import os
import asyncio
from typing import TypedDict, Literal, Optional
from langgraph.graph import StateGraph, END, START
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from app.core.mcp_client import get_airflow_mcp_tools, find_tool
from app.services.airflow_api import get_task_logs, clear_task_instance, get_task_instance_state
from app.services.decision_log import log_decision
from app.services.schema_healer import diagnose_and_heal

# How long (seconds) to wait between polls, and how many polls, when
# checking whether a cleared task instance finished after a RETRY.
VERIFY_POLL_INTERVAL_SECONDS = float(os.getenv("VERIFY_POLL_INTERVAL_SECONDS", "3"))
VERIFY_POLL_MAX_ATTEMPTS = int(os.getenv("VERIFY_POLL_MAX_ATTEMPTS", "10"))
TERMINAL_STATES = {"success", "failed", "upstream_failed", "skipped"}

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


# 2. Decision Schema for LLM
class LLMDecision(BaseModel):
    action: Literal["RETRY", "ESCALATE", "NONE"] = Field(description="Action to take")
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
            f"(RETRY, ESCALATE, or NONE):\n\n{logs}"
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
      original decision through unchanged.
    """
    heal_result = await diagnose_and_heal(state.get("logs", ""))

    if not heal_result.matched:
        return {}

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


# 4. Graph Construction
builder = StateGraph(AgentState)
builder.add_node("analyze", analyze_node)
builder.add_node("diagnose", diagnose_node)
builder.add_node("action", action_node)
builder.add_node("verify", verify_node)

builder.add_edge(START, "analyze")
builder.add_edge("analyze", "diagnose")
builder.add_edge("diagnose", "action")
builder.add_edge("action", "verify")


# Conditional Router
def should_continue(state: AgentState):
    if state["is_fixed"]:
        return END
    # ESCALATE/NONE mean no retry was actually attempted this round, so
    # looping back to "analyze" would just re-read the same failed task
    # and burn attempts for nothing. Only RETRY should loop.
    if state["action_decision"] != "RETRY":
        return END
    if state["attempts"] >= 3:
        return END
    return "analyze"


builder.add_conditional_edges("verify", should_continue)
agent_graph = builder.compile()
