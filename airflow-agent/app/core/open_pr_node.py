"""
Final step of the CODE_FIX path — only reached after critic_node returns
"approved". Opens the real GitHub PR and records it in pending_fix_prs so
the merge webhook (app/api/github_webhook.py) knows which incident it
belongs to. The agent's job ends here: a human reviews and merges (or
closes) the PR through GitHub as normal.
"""

from app.services.decision_log import log_decision, create_pending_pr
from app.services import github_pr


async def open_pr_node(state: dict):
    dag_id, task_id, dag_run_id = state["dag_id"], state["task_id"], state["dag_run_id"]

    if not github_pr.is_configured():
        await log_decision(
            dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
            node="open_pr", attempt=state.get("attempts", 0),
            reasoning="Fix was approved by the critic, but GITHUB_TOKEN/GITHUB_REPO "
                      "aren't configured, so no PR could be opened.",
            action_decision="ESCALATE",
        )
        return {"is_fixed": False, "action_decision": "ESCALATE"}

    result = github_pr.open_fix_pr(
        dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
        repo_path=state["repo_path"],
        original_content=state["original_content"],
        fixed_content=state["proposed_fix"],
        reasoning=f"{state.get('fix_summary', '')}\n\nReviewer notes: {state.get('critic_concerns', '')}",
    )

    if "error" in result:
        await log_decision(
            dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
            node="open_pr", attempt=state.get("attempts", 0),
            reasoning=f"Approved fix, but opening the PR failed: {result['error']}",
            action_decision="ESCALATE",
        )
        return {"is_fixed": False, "action_decision": "ESCALATE"}

    await create_pending_pr(
        pr_number=result["pr_number"], pr_url=result["pr_url"],
        dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
        repo_path=state["repo_path"],
    )

    await log_decision(
        dag_id=dag_id, task_id=task_id, dag_run_id=dag_run_id,
        node="open_pr", attempt=state.get("attempts", 0),
        reasoning=f"PR opened: {result['pr_url']}. Awaiting human review/merge — "
                  f"the agent will retry exactly once after it's merged.",
        action_decision="PR_OPENED",
    )

    # Not fixed yet — a human still has to merge it. This run's job is
    # done; app/api/github_webhook.py handles what happens after merge.
    return {"is_fixed": False, "action_decision": "PR_OPENED", "pr_url": result["pr_url"]}
