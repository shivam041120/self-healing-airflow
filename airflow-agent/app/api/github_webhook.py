"""
Closes the loop on the CODE_FIX path: once a human merges the PR
open_pr_node opened, the task should be cleared and retried exactly
once — not sent back through the router/specialist/critic again, since
the fix already happened as real code in the merged PR, not as another
agent decision.

Two ways to trigger this:
  POST /api/github/webhook   - real GitHub webhook (pull_request event),
                                verified with GITHUB_WEBHOOK_SECRET
  POST /api/prs/{pr_number}/mark-merged
                              - manual fallback for local dev, since a
                                webhook needs a publicly reachable URL
                                that a local docker-compose stack usually
                                doesn't have
"""

import asyncio
import hashlib
import hmac
import os

from fastapi import APIRouter, Header, HTTPException, Request

from app.services.airflow_api import clear_task_instance, get_task_instance_state
from app.services.decision_log import get_pending_pr, mark_pr_status, log_decision

router = APIRouter()

GITHUB_WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET")

# Same polling shape as agent.py's verify_node — kept as separate
# constants (not imported) since this path has nothing else to do with
# the LangGraph loop and shouldn't need to import from it.
VERIFY_POLL_INTERVAL_SECONDS = float(os.getenv("VERIFY_POLL_INTERVAL_SECONDS", "3"))
VERIFY_POLL_MAX_ATTEMPTS = int(os.getenv("VERIFY_POLL_MAX_ATTEMPTS", "10"))
TERMINAL_STATES = {"success", "failed", "upstream_failed", "skipped"}


def _verify_signature(secret: str, payload_body: bytes, signature_header: str) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), payload_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature_header)


async def _retry_after_merge(pr_number: int) -> dict:
    """
    The actual merge-gated single retry. Shared by both the real webhook
    and the manual fallback endpoint so they can't drift apart.
    """
    pending = await get_pending_pr(pr_number)
    if not pending:
        return {"status": "ignored", "reason": f"No pending fix tracked for PR #{pr_number}"}
    if pending["status"] != "open":
        return {"status": "ignored", "reason": f"PR #{pr_number} already handled (status={pending['status']})"}

    dag_id, task_id, dag_run_id = pending["dag_id"], pending["task_id"], pending["dag_run_id"]

    result = clear_task_instance(dag_id, dag_run_id, task_id)
    if "error" in result:
        await mark_pr_status(pr_number, "merge_retry_failed")
        await log_decision(
            dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
            node="retry_after_merge", reasoning=f"PR merged, but clearing the task failed: {result['error']}",
            action_decision="CLEAR_FAILED",
        )
        return {"status": "error", "reason": result["error"]}

    await mark_pr_status(pr_number, "merged_retried")
    await log_decision(
        dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
        node="retry_after_merge", reasoning=f"PR #{pr_number} merged — task cleared for its one post-merge retry.",
        action_decision="CLEAR_AND_RETRY",
    )

    # Without this, a fully successful merge+retry has no way to ever
    # show as "Fixed" on the dashboard — it would just sit at
    # CLEAR_AND_RETRY forever, indistinguishable from one that's still
    # pending or one that quietly failed again. Reusing node="verify"
    # (rather than inventing a new status) means the existing, generic
    # status logic in incident_view.py picks this up for free.
    final_state = None
    for _ in range(VERIFY_POLL_MAX_ATTEMPTS):
        await asyncio.sleep(VERIFY_POLL_INTERVAL_SECONDS)
        final_state = get_task_instance_state(dag_id, dag_run_id, task_id)
        if final_state in TERMINAL_STATES:
            break
    success = final_state == "success"

    await log_decision(
        dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
        node="verify", reasoning=f"Post-merge retry finished in state: {final_state}",
        verification_result=success,
    )

    return {
        "status": "retried", "dag_id": dag_id, "task_id": task_id,
        "dag_run_id": dag_run_id, "verified": success, "final_state": final_state,
    }


@router.post("/github/webhook")
async def github_webhook(request: Request, x_hub_signature_256: str = Header(default=None), x_github_event: str = Header(default=None)):
    body = await request.body()

    if GITHUB_WEBHOOK_SECRET:
        if not _verify_signature(GITHUB_WEBHOOK_SECRET, body, x_hub_signature_256):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")
    else:
        print("[github_webhook] WARNING: GITHUB_WEBHOOK_SECRET not set — accepting unverified webhook payload.")

    if x_github_event != "pull_request":
        return {"status": "ignored", "reason": f"Not a pull_request event ({x_github_event})"}

    payload = await request.json()
    action = payload.get("action")
    pr = payload.get("pull_request", {})

    if action != "closed" or not pr.get("merged"):
        return {"status": "ignored", "reason": f"action={action}, merged={pr.get('merged')}"}

    return await _retry_after_merge(pr["number"])


@router.post("/prs/{pr_number}/mark-merged")
async def mark_merged_manually(pr_number: int):
    """Local-dev fallback — see module docstring. Does exactly what the real webhook does."""
    return await _retry_after_merge(pr_number)