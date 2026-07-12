"""
Reviews code_specialist's proposed fix before it's allowed to become a
PR. Deliberately a separate LLM call with a skeptical prompt, rather than
trusting the specialist to grade its own work — a generator that also
approves itself has no actual check on it.

Three possible verdicts:
  approved - proceed to open_pr_node
  revise   - send back to code_specialist once, with concerns attached
  rejected - give up automatically proposing a fix; escalate to a human
             instead of opening a PR. This also covers the case where
             code_specialist itself couldn't produce a fix at all.
"""

import os
from typing import Literal

from langchain_ollama import ChatOllama
from pydantic import BaseModel, Field

from app.services.decision_log import log_decision
from app.core.specialists.code_specialist import MAX_REVISIONS

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi4-mini")


class CriticVerdict(BaseModel):
    verdict: Literal["approved", "revise", "rejected"] = Field(
        description="approved: the fix directly addresses the exact error in the failure log, "
                     "and doesn't touch code unrelated to that error. This is the default for a "
                     "small, targeted change — approve it even if a more elegant fix exists. "
                     "revise: has a SPECIFIC, concrete problem you can name — e.g. it still "
                     "references the same missing key/attribute, it introduces a real syntax "
                     "error, it changes behavior for inputs the log never showed failing. "
                     "'could be improved' or 'might have other issues' is not concrete enough — "
                     "if you can't name an actual problem with actual evidence, approve instead. "
                     "rejected: the fix doesn't address the root cause at all, or revise was "
                     "already used once — a human should look at this instead."
    )
    concerns: str = Field(
        description="If revise/rejected: the SPECIFIC problem, in one sentence, referencing "
                     "exactly what's wrong. If approved: a brief one-sentence confirmation."
    )


async def critic_node(state: dict):
    dag_id, task_id, dag_run_id = state["dag_id"], state["task_id"], state["dag_run_id"]
    revision_count = state.get("revision_count", 0)

    # code_specialist already bailed (couldn't read the file, LLM
    # failed, etc.) — nothing to critique, go straight to escalation.
    if not state.get("proposed_fix"):
        await log_decision(
            dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
            node="critic", attempt=state.get("attempts", 0),
            reasoning="No fix was proposed to review.", action_decision="rejected",
        )
        return {"critic_verdict": "rejected"}

    prompt = (
        f"A specialist proposed a fix for this Airflow task failure. Your job is narrow: "
        f"catch a fix that's actually wrong before it reaches a human as a pull request — "
        f"NOT to hold out for the most elegant or defensive version of the fix. A small, "
        f"targeted change that directly resolves the exact error below should be approved, "
        f"even if you can imagine a more thorough version.\n\n"
        f"Failure log:\n{state.get('logs', '')}\n\n"
        f"Original file:\n```python\n{state.get('original_content', '')}\n```\n\n"
        f"Proposed fix:\n```python\n{state['proposed_fix']}\n```\n\n"
        f"Specialist's own summary of the change: {state.get('fix_summary', '')}\n\n"
        f"Two questions only:\n"
        f"1. Does the proposed fix eliminate the exact error shown in the failure log above?\n"
        f"2. Does it change any code NOT related to that error?\n"
        f"If (1) is yes and (2) is no, approve it. Only choose revise/rejected if you can "
        f"point to a specific line that's still broken or a specific unrelated change — "
        f"not a general sense that more could be done."
    )

    try:
        llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL).with_structured_output(CriticVerdict)
        result = llm.invoke(prompt)
        verdict, concerns = result.verdict, result.concerns
    except Exception as e:
        # A critic that can't run is not a safe reason to auto-approve a
        # PR — fail closed to escalation instead.
        verdict, concerns = "rejected", f"Critic LLM call failed ({type(e).__name__}): {e}"

    # Cap revision ping-pong regardless of what the critic wants — see
    # MAX_REVISIONS in code_specialist.py.
    if verdict == "revise" and revision_count >= MAX_REVISIONS:
        verdict = "rejected"
        concerns = f"{concerns}\n\n(Revision limit reached — escalating instead of looping further.)"

    await log_decision(
        dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
        node="critic", attempt=state.get("attempts", 0),
        reasoning=concerns, action_decision=verdict,
    )

    return {"critic_verdict": verdict, "critic_concerns": concerns}