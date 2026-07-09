import asyncio
import os

from fastapi import APIRouter
from app.core.agent import agent_graph
from app.services.decision_log import log_decision

router = APIRouter()

# Scenarios like agent_error_scenarios deliberately fail several
# independent tasks at once, and each on_failure_callback hits this
# endpoint. Without a cap, that means N concurrent full LangGraph runs —
# each hammering the local Ollama model and each writing/notifying its own
# burst of decision rows — piling on top of each other. Queue them instead
# so incidents are processed a couple at a time rather than all at once.
MAX_CONCURRENT_AGENT_RUNS = int(os.getenv("MAX_CONCURRENT_AGENT_RUNS", "2"))
_agent_semaphore = asyncio.Semaphore(MAX_CONCURRENT_AGENT_RUNS)


@router.post("/analyze-failure")
async def trigger_agent(data: dict):
    dag_id = data.get("dag_id", "unknown")
    task_id = data.get("task_id", "unknown")
    dag_run_id = data.get("dag_run_id", "unknown")

    # Log that the webhook was received at all, before anything else runs.
    # If nothing below this succeeds, this row alone still proves the
    # callback reached the agent — narrowing "did it even arrive" out of
    # the list of things to debug.
    await log_decision(
        dag_id=dag_id,
        task_id=task_id,
        dag_run_id=dag_run_id,
        node="webhook_received",
    )

    input_state = {
        "task_id": task_id,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "attempts": 0,
        "is_fixed": False,
    }

    try:
        async with _agent_semaphore:
            result = await agent_graph.ainvoke(input_state)
        return {"status": "completed", "result": result}
    except Exception as e:
        error_msg = f"Agent graph crashed ({type(e).__name__}): {e}"
        print(f"[routes] {error_msg}")
        await log_decision(
            dag_id=dag_id,
            task_id=task_id,
            dag_run_id=dag_run_id,
            node="graph_crashed",
            reasoning=error_msg,
        )
        return {"status": "error", "message": error_msg}
