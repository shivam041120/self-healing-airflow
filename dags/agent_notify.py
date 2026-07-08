"""
Shared failure-notification callback used by every DAG in this project.
Centralized here so any DAG that wants self-healing behavior just imports
this one function, instead of each DAG redefining its own copy of the
HTTP call / error handling / AGENT_URL logic — which is exactly the kind
of duplication that quietly drifts out of sync once a second DAG exists.

Airflow's dag-processor adds the dags/ folder itself to sys.path, so any
.py file directly inside dags/ is importable by plain module name from
sibling DAG files — no __init__.py or package setup needed.
"""

import os
import httpx

AGENT_URL = os.getenv("AGENT_URL", "http://airflow-agent:8000")


def agent_failure_callback(context):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    dag_run_id = context["dag_run"].run_id

    print(f"Task failed. Notifying agent for dag={dag_id} task={task_id} run={dag_run_id}")

    try:
        response = httpx.post(
            f"{AGENT_URL}/api/analyze-failure",
            json={"dag_id": dag_id, "task_id": task_id, "dag_run_id": dag_run_id},
            timeout=60,
        )
        print(f"Agent responded: {response.status_code} {response.text}")
    except Exception as e:
        # Never let a notification failure crash the callback itself
        print(f"Could not reach self-healing agent at {AGENT_URL}: {e}")
