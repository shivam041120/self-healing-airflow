import os
import re
import asyncio
from typing import TypedDict, Literal, Optional
from langgraph.graph import StateGraph, END, START
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from app.core.mcp_client import get_airflow_mcp_tools, find_tool
from app.services.airflow_api import get_task_logs, clear_task_instance, get_task_instance_state
from app.services.decision_log import log_decision
from app.services.schema_healer import diagnose_and_heal
from app.core.specialists.code_specialist import code_specialist_node
from app.core.critic_node import critic_node
from app.core.open_pr_node import open_pr_node

# How long (seconds) to wait between polls, and how many polls, when
# checking whether a cleared task instance finished after a RETRY.
VERIFY_POLL_INTERVAL_SECONDS = float(os.getenv("VERIFY_POLL_INTERVAL_SECONDS", "3"))
VERIFY_POLL_MAX_ATTEMPTS = int(os.getenv("VERIFY_POLL_MAX_ATTEMPTS", "10"))
# One retry, not three: if a clear-and-rerun doesn't fix it the first time,
# looping again is just repeating the same guess against a failure that's
# probably not transient. Better to stop and hand it to a human.
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "1"))
TERMINAL_STATES = {"success", "failed", "upstream_failed", "skipped"}

# The small local model is inconsistent at classifying SQL syntax/reference
# errors as CODE_FIX vs RETRY — it sometimes reads a Postgres error as
# "just try again" when it's actually a typo'd/wrong SQL string that will
# fail identically every time. These are Postgres's own well-known error
# message shapes for exactly that failure class (not the missing-table
# shape, which schema_healer already owns): a bad keyword/typo in the
# statement, or a column that doesn't exist. Deterministic, so it doesn't
# depend on the model getting it right.
_SQL_CODE_BUG_RE = re.compile(
    r'syntax error at or near|column "[^"]+" does not exist',
    re.IGNORECASE,
)

# Same idea, for plain Python bugs: these exception types are essentially
# always a real bug in the code, not a transient/infra condition, so
# there's no ambiguity worth leaving to the small model's judgment.
# Deliberately NOT included: OperationalError, ConnectionError,
# TimeoutError, InterfaceError and friends — those genuinely can be
# transient, so RETRY should stay on the table for them.
_PYTHON_CODE_BUG_RE = re.compile(
    r'^(AttributeError|KeyError|IndexError|TypeError|NameError|'
    r'ZeroDivisionError|UnboundLocalError):',
    re.MULTILINE,
)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi4-mini")


# 1. State Definition
class AgentState(TypedDict):
    task_id: str
    dag_id: str
    dag_run_id: str
    logs: str
    action_decision: str
    reasoning: Optional[str]
    attempts: int
    is_fixed: bool
    # --- multi-agent (CODE_FIX) fields, unused by the RETRY/ESCALATE path ---
    repo_path: Optional[str]
    original_content: Optional[str]
    proposed_fix: Optional[str]
    fix_summary: Optional[str]
    revision_count: int
    critic_verdict: Optional[str]
    critic_concerns: Optional[str]
    pr_url: Optional[str]


# 2. Decision Schema for LLM
class LLMDecision(BaseModel):
    action: Literal["RETRY", "ESCALATE", "CODE_FIX", "NONE"] = Field(
        description=(
            "RETRY: transient/infra failure, safe to just clear and re-run "
            "(network blip, timeout, flaky dependency) — no code is wrong. "
            "CODE_FIX: the failure is caused by wrong code (SQL syntax "
            "error, wrong column/table reference, a Python bug in the DAG "
            "file) — clearing and re-running would just fail identically; "
            "the fix has to change the code itself. Routes to a dedicated "
            "specialist agent, not handled directly here. "
            "ESCALATE: needs a human decision the agent can't safely make "
            "(auth/permissions, data correctness, ambiguous cause). "
            "NONE: no action needed."
        )
    )
    reasoning: str = Field(description="Explanation for the choice")


# 3. Nodes
async def _fetch_logs_via_mcp(state: AgentState) -> str:
    """
    Tries to fetch logs through the Airflow MCP server first (real tool use).
    Falls back to the direct REST call if the MCP tool isn't found or the
    call fails, so the agent keeps working even before tool names are
    confirmed against your live instance.
    """
    try:
        tools = await get_airflow_mcp_tools()
        log_tool = find_tool(tools, "task", "log")
        if log_tool:
            result = await log_tool.ainvoke({
                "dag_id": state["dag_id"],
                "dag_run_id": state["dag_run_id"],
                "task_id": state["task_id"],
            })
            result_str = str(result)
            # Some MCP tools return their own errors as normal content
            # instead of raising, so a wrong/incompatible tool call can
            # silently "succeed" with an error message as the "logs". Catch
            # the obvious case and fall back to REST instead of feeding
            # that error text to the LLM as if it were the task's log.
            if "validation error" in result_str.lower() or "is a required property" in result_str.lower():
                print(f"[mcp] Log tool returned a tool-level error, using REST fallback: {result_str}")
            else:
                return result_str
        else:
            print("[mcp] No log-fetching tool found by keyword match; using REST fallback.")
    except Exception as e:
        print(f"[mcp] Log fetch via MCP failed, using REST fallback: {e}")
    return get_task_logs(state["dag_id"], state["dag_run_id"], state["task_id"])


async def analyze_node(state: AgentState):
    # Log that the node started BEFORE anything risky runs, so a crash
    # anywhere below this line still leaves a visible trace instead of
    # total silence on the dashboard.
    await log_decision(
        dag_id=state["dag_id"],
        task_id=state["task_id"],
        dag_run_id=state["dag_run_id"],
        node="analyze_start",
        attempt=state.get("attempts", 0),
    )

    logs = await _fetch_logs_via_mcp(state)

    try:
        llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL).with_structured_output(LLMDecision)
        decision = llm.invoke(
            "Analyze this Airflow task failure log and decide the next action "
            f"(RETRY, ESCALATE, CODE_FIX, or NONE):\n\n{logs}"
        )
        action = decision.action
        reasoning = decision.reasoning
    except Exception as e:
        # Previously this exception would propagate uncaught, crash the
        # FastAPI request, and leave zero trace in the dashboard. Now it's
        # visible: we log the failure itself and escalate, rather than
        # silently doing nothing.
        error_msg = f"LLM call failed ({type(e).__name__}): {e}"
        print(f"[analyze_node] {error_msg}")
        await log_decision(
            dag_id=state["dag_id"],
            task_id=state["task_id"],
            dag_run_id=state["dag_run_id"],
            node="analyze_error",
            attempt=state.get("attempts", 0),
            logs_excerpt=logs,
            reasoning=error_msg,
            action_decision="ESCALATE",
        )
        return {"logs": logs, "action_decision": "ESCALATE", "reasoning": error_msg}

    await log_decision(
        dag_id=state["dag_id"],
        task_id=state["task_id"],
        dag_run_id=state["dag_run_id"],
        node="analyze",
        attempt=state.get("attempts", 0),
        logs_excerpt=logs,
        reasoning=reasoning,
        action_decision=action,
    )

    return {"logs": logs, "action_decision": action, "reasoning": reasoning}


async def diagnose_node(state: AgentState):
    """
    Runs after the LLM's analysis, before any action is taken. Checks
    whether this failure matches a well-understood, mechanically fixable
    shape (a missing table) using schema_healer — deterministic root-cause
    detection rather than relying on a small LLM to both diagnose *and*
    decide correctly every time.

    - If a fix was applied: force action_decision to RETRY (regardless of
      what the LLM said) so action_node clears the task and it now
      actually has a chance to succeed.
    - If only a suggestion was produced (no confident fix): force
      action_decision to ESCALATE with the suggestion attached, so a human
      sees a concrete next step instead of the loop burning retries on a
      failure that can't fix itself.
    - If the log doesn't match this failure shape at all: pass the LLM's
      original decision through unchanged, then check for the second,
      separate SQL-code-bug shape below.
    """
    heal_result = await diagnose_and_heal(state.get("logs", ""))

    if heal_result.matched:
        reasoning = f"{state.get('reasoning', '')}\n\n[schema_healer] {heal_result.message}".strip()
        new_action = "RETRY" if heal_result.fixed else "ESCALATE"

        await log_decision(
            dag_id=state["dag_id"],
            task_id=state["task_id"],
            dag_run_id=state["dag_run_id"],
            node="diagnose",
            attempt=state.get("attempts", 0),
            reasoning=heal_result.message,
            action_decision="AUTO_FIX_APPLIED" if heal_result.fixed else "FIX_SUGGESTED",
        )

        return {"action_decision": new_action, "reasoning": reasoning}

    # Not a missing-table shape. Separately: if this looks like a SQL
    # syntax/column-reference error, or a Python exception type that's
    # essentially always a real code bug, and the LLM didn't already call
    # it CODE_FIX, force it — these aren't transient, so RETRY would just
    # fail identically, and "NONE"/"ESCALATE" would leave a fixable bug
    # sitting unfixed. This needs the code_specialist, not a clear-and-
    # rerun or a human paged for something the agent can actually try.
    current_action = state.get("action_decision")
    logs_text = state.get("logs", "") or ""
    is_sql_bug = _SQL_CODE_BUG_RE.search(logs_text)
    is_python_bug = _PYTHON_CODE_BUG_RE.search(logs_text)

    if current_action != "CODE_FIX" and (is_sql_bug or is_python_bug):
        shape = "SQL syntax/column-reference" if is_sql_bug else "Python"
        reasoning = (
            f"{state.get('reasoning', '')}\n\n[diagnose] Log matches a {shape} error, "
            f"which isn't transient — routing to CODE_FIX instead of {current_action}."
        ).strip()

        await log_decision(
            dag_id=state["dag_id"],
            task_id=state["task_id"],
            dag_run_id=state["dag_run_id"],
            node="diagnose",
            attempt=state.get("attempts", 0),
            reasoning=reasoning,
            action_decision="CODE_FIX_FORCED",
        )

        return {"action_decision": "CODE_FIX", "reasoning": reasoning}

    return {}


async def _clear_task_via_mcp(state: AgentState):
    """Returns the clear-tool result, or None if no MCP tool was found/usable."""
    tools = await get_airflow_mcp_tools()
    clear_tool = find_tool(tools, "clear", "task")
    if not clear_tool:
        print("[action_node] No clear-task tool found by keyword match; using REST fallback.")
        return None
    result = await clear_tool.ainvoke({
        "dag_id": state["dag_id"],
        "dag_run_id": state["dag_run_id"],
        "task_id": state["task_id"],
    })
    print(f"[action_node] Cleared task via MCP: {result}")
    return result


async def action_node(state: AgentState):
    action_taken = "NONE"
    if state["action_decision"] == "RETRY":
        mcp_result = None
        try:
            mcp_result = await _clear_task_via_mcp(state)
        except Exception as e:
            print(f"[action_node] MCP clear-task call failed, using REST fallback: {e}")

        if mcp_result is not None:
            action_taken = "CLEAR_AND_RETRY"
        else:
            # MCP tool wasn't found or the call failed — fall back to the
            # direct REST call so RETRY still actually clears the task
            # instance instead of silently doing nothing.
            rest_result = clear_task_instance(
                state["dag_id"], state["dag_run_id"], state["task_id"]
            )
            if "error" in rest_result:
                print(f"[action_node] REST clear-task call failed: {rest_result['error']}")
                action_taken = f"CLEAR_FAILED: {rest_result['error']}"
            else:
                print(f"[action_node] Cleared task via REST: {rest_result}")
                action_taken = "CLEAR_AND_RETRY"

    new_attempts = state.get("attempts", 0) + 1

    await log_decision(
        dag_id=state["dag_id"],
        task_id=state["task_id"],
        dag_run_id=state["dag_run_id"],
        node="action",
        attempt=new_attempts,
        reasoning=state.get("reasoning"),
        action_decision=action_taken,
    )

    return {"attempts": new_attempts}


async def verify_node(state: AgentState):
    """
    Checks the task instance's *actual* state after an action, rather than
    assuming success. If action_node just cleared the task (RETRY), give
    Airflow a little time to re-run it and poll for a terminal state. For
    ESCALATE/NONE, no retry happened, so just read the current state once.
    """
    final_state = None
    try:
        if state["action_decision"] == "RETRY":
            for _ in range(VERIFY_POLL_MAX_ATTEMPTS):
                await asyncio.sleep(VERIFY_POLL_INTERVAL_SECONDS)
                final_state = get_task_instance_state(
                    state["dag_id"], state["dag_run_id"], state["task_id"]
                )
                if final_state in TERMINAL_STATES:
                    break
        else:
            final_state = get_task_instance_state(
                state["dag_id"], state["dag_run_id"], state["task_id"]
            )
        success = final_state == "success"
    except Exception as e:
        print(f"[verify_node] verification check failed: {e}")
        success = False

    print(f"[verify_node] task instance state after action: {final_state}")

    await log_decision(
        dag_id=state["dag_id"],
        task_id=state["task_id"],
        dag_run_id=state["dag_run_id"],
        node="verify",
        attempt=state.get("attempts", 0),
        verification_result=success,
    )

    return {"is_fixed": success}


async def escalate_after_retry_node(state: AgentState):
    """
    Reached only when the one retry we allow still didn't fix the task.
    Logs a distinct, human-visible decision instead of just letting the
    loop stop quietly — so this shows up as "Escalated" on the dashboard,
    not "Unresolved" (which reads the same as "still in progress").
    """
    await log_decision(
        dag_id=state["dag_id"],
        task_id=state["task_id"],
        dag_run_id=state["dag_run_id"],
        node="escalate_after_retry",
        attempt=state.get("attempts", 0),
        reasoning="Retried once and the task is still failing — this doesn't "
                   "look transient. Escalating instead of retrying again.",
        action_decision="ESCALATE_AFTER_RETRY",
    )
    return {}


# 4. Graph Construction
builder = StateGraph(AgentState)
builder.add_node("analyze", analyze_node)          # router
builder.add_node("diagnose", diagnose_node)        # deterministic override for the missing-table shape
builder.add_node("code_specialist", code_specialist_node)
builder.add_node("critic", critic_node)
builder.add_node("open_pr", open_pr_node)
builder.add_node("action", action_node)
builder.add_node("verify", verify_node)
builder.add_node("escalate_after_retry", escalate_after_retry_node)

builder.add_edge(START, "analyze")
builder.add_edge("analyze", "diagnose")

# diagnose_node only overrides RETRY/ESCALATE for the schema-healer shape
# (missing table) — it never produces CODE_FIX, so this is exactly the
# router's original classification for anything code-shaped.
builder.add_conditional_edges(
    "diagnose",
    lambda state: state["action_decision"],
    {
        "CODE_FIX": "code_specialist",
        "RETRY": "action",
        "ESCALATE": "action",
        "NONE": "action",
    },
)

builder.add_edge("code_specialist", "critic")


def after_critic(state: AgentState):
    verdict = state.get("critic_verdict")
    if verdict == "approved":
        return "open_pr"
    if verdict == "revise":
        return "code_specialist"
    return END  # "rejected" — escalate, no PR opened


builder.add_conditional_edges("critic", after_critic)
builder.add_edge("open_pr", END)

builder.add_edge("action", "verify")


# Conditional Router — only the RETRY/ESCALATE/NONE branch ever reaches
# verify; CODE_FIX exits through open_pr or the critic's rejection instead.
def should_continue(state: AgentState):
    if state["is_fixed"]:
        return END
    # ESCALATE/NONE mean no retry was actually attempted this round, so
    # looping back to "analyze" would just re-read the same failed task
    # and burn attempts for nothing. Only RETRY should loop.
    if state["action_decision"] != "RETRY":
        return END
    if state["attempts"] < MAX_RETRY_ATTEMPTS:
        return "analyze"
    # The one retry we allow didn't fix it — a second identical guess
    # isn't going to do better. Escalate explicitly rather than looping
    # again or quietly stopping.
    return "escalate_after_retry"


builder.add_conditional_edges("verify", should_continue)
builder.add_edge("escalate_after_retry", END)
agent_graph = builder.compile()