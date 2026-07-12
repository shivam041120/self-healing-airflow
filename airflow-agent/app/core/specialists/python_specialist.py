"""
Code-fix specialist. Runs only after the router (analyze_node) has already
classified a failure as CODE_FIX — this agent's whole job is narrower than
the router's: given the DAG file that actually failed and the traceback,
propose a corrected version of that file. It never opens a PR itself
(that's open_pr_node, and only after critic_node approves) and never
decides RETRY/ESCALATE (that's the router's job) — one job, one prompt.
"""

import os
from typing import Optional

from langchain_ollama import ChatOllama
from pydantic import BaseModel, Field

from app.services.airflow_api import get_dag_fileloc
from app.services.decision_log import log_decision
from app.services import github_pr

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi4-mini")

# Hard cap so a critic "revise" verdict can't ping-pong forever between
# this node and critic_node — see agent.py's conditional edge.
MAX_REVISIONS = 1


class ProposedFix(BaseModel):
    corrected_content: str = Field(
        description="The FULL corrected file content — not a diff, not just the changed lines. "
                     "Must be valid, complete Python (or the file's existing language) that a linter "
                     "could parse, since this replaces the file wholesale in the PR."
    )
    summary: str = Field(description="One or two sentences: what was wrong, what changed.")


async def python_specialist_node(state: dict):
    dag_id, task_id, dag_run_id = state["dag_id"], state["task_id"], state["dag_run_id"]
    logs = state.get("logs", "")
    # A "revision pass" is specifically a call triggered by the critic's
    # "revise" verdict — not just "any call after the first". Gating on
    # that (rather than incrementing unconditionally) is what lets the
    # cap in critic_node actually work: the ORIGINAL attempt must not
    # count against the revision budget.
    is_revision = state.get("critic_verdict") == "revise"
    revision_count = state.get("revision_count", 0) + (1 if is_revision else 0)

    fileloc = get_dag_fileloc(dag_id)
    if not fileloc:
        return await _bail(state, "Could not determine the DAG's source file via the Airflow API — "
                                   "no fix can be proposed without knowing what to patch.")

    repo_path = github_pr.fileloc_to_repo_path(fileloc)
    file_info = github_pr.get_file_contents(repo_path)
    if "error" in file_info:
        return await _bail(state, f"Could not read '{repo_path}' from GitHub: {file_info['error']}")

    original_content = file_info["content"]

    # On a revision pass, include the critic's concerns and the previous
    # attempt so the model doesn't just regenerate the same fix.
    critic_feedback = ""
    if is_revision and state.get("critic_concerns"):
        critic_feedback = (
            f"\n\nA previous proposed fix was reviewed and sent back for revision. "
            f"Reviewer's concerns:\n{state['critic_concerns']}\n\n"
            f"Previous attempt (do not just repeat this):\n{state.get('proposed_fix', '')}"
        )

    prompt = (
        f"This Airflow DAG file failed with the error below. Propose a corrected version "
        f"of the ENTIRE file — preserve everything that isn't related to the bug, fix only "
        f"what's actually broken.\n\n"
        f"File: {repo_path}\n\n"
        f"Current content:\n```python\n{original_content}\n```\n\n"
        f"Failure log:\n{logs}"
        f"{critic_feedback}"
    )

    try:
        llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL).with_structured_output(ProposedFix)
        fix = llm.invoke(prompt)
    except Exception as e:
        return await _bail(state, f"Fix-generation LLM call failed ({type(e).__name__}): {e}")

    await log_decision(
        dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
        node="python_specialist", attempt=state.get("attempts", 0),
        reasoning=fix.summary, action_decision="FIX_PROPOSED",
    )

    return {
        "repo_path": repo_path,
        "original_content": original_content,
        "proposed_fix": fix.corrected_content,
        "fix_summary": fix.summary,
        "revision_count": revision_count,
    }


async def _bail(state: dict, reason: str):
    """
    A specialist that can't do its job should hand back to a human, not
    crash the run or silently do nothing — same principle as analyze_node's
    LLM-failure handling.
    """
    await log_decision(
        dag_id=state["dag_id"], task_id=state["task_id"], dag_run_id=state["dag_run_id"],
        node="python_specialist", attempt=state.get("attempts", 0),
        reasoning=reason, action_decision="ESCALATE",
    )
    return {"action_decision": "ESCALATE", "reasoning": reason, "proposed_fix": None}