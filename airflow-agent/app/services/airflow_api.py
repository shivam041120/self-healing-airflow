import httpx
import os
from typing import Optional

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-apiserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "airflow")


def get_auth_token() -> str:
    """
    Airflow 3's API server requires a JWT bearer token rather than basic auth.
    Used both by the direct REST fallback below and by the MCP client, which
    needs a fresh token each time it spawns the airflow-mcp-server subprocess.
    """
    resp = httpx.post(
        f"{AIRFLOW_URL}/auth/token",
        json={"username": AIRFLOW_USER, "password": AIRFLOW_PASS},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_task_logs(dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
    """
    Direct REST fallback for fetching task logs. Used if the MCP tool call
    (see app/core/mcp_client.py) fails or the expected tool isn't found.
    """
    try:
        token = get_auth_token()
    except Exception as e:
        return f"Could not authenticate with Airflow API: {e}"

    url = (
        f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
        f"/taskInstances/{task_id}/logs/{try_number}"
    )
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = httpx.get(url, headers=headers, timeout=15)
    except Exception as e:
        return f"Could not reach Airflow API: {e}"

    if response.status_code == 200:
        return response.text
    return f"Could not fetch logs (status {response.status_code}): {response.text}"


def clear_task_instance(dag_id: str, dag_run_id: str, task_id: str) -> dict:
    """
    Direct REST fallback for clearing (and thereby retrying) a failed task
    instance. Used if the MCP "clear task" tool call (see
    app/core/mcp_client.py) fails or the expected tool isn't found.

    Without this, RETRY decisions had no way to actually take effect unless
    the MCP subprocess happened to work — the agent would say "RETRY" but
    nothing in Airflow would change.
    """
    try:
        token = get_auth_token()
    except Exception as e:
        return {"error": f"Could not authenticate with Airflow API: {e}"}

    url = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/clearTaskInstances"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "dry_run": False,
        "task_ids": [task_id],
        "dag_run_id": dag_run_id,
        "only_failed": True,
        "reset_dag_runs": True,
    }

    try:
        response = httpx.post(url, headers=headers, json=payload, timeout=15)
    except Exception as e:
        return {"error": f"Could not reach Airflow API: {e}"}

    if response.status_code in (200, 201):
        return {"status": "cleared", "detail": response.text}
    return {"error": f"Could not clear task (status {response.status_code}): {response.text}"}


def get_dag_fileloc(dag_id: str) -> Optional[str]:
    """
    Returns the DAG's source file path as Airflow itself sees it (e.g.
    '/opt/airflow/dags/dag_syntax_error_sql.py'). Used by the code-fix
    flow to find which file in the repo actually needs patching, instead
    of hardcoding a task_id -> filename table that would silently go
    stale the moment DAG files get renamed or split.
    """
    try:
        token = get_auth_token()
    except Exception as e:
        print(f"[airflow_api] Could not authenticate to read DAG fileloc: {e}")
        return None

    url = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = httpx.get(url, headers=headers, timeout=15)
    except Exception as e:
        print(f"[airflow_api] Could not reach Airflow API for DAG fileloc: {e}")
        return None

    if response.status_code == 200:
        return response.json().get("fileloc")
    print(f"[airflow_api] Could not fetch DAG fileloc (status {response.status_code}): {response.text}")
    return None


def get_task_instance_state(dag_id: str, dag_run_id: str, task_id: str) -> Optional[str]:
    """
    Reads back a task instance's current state (e.g. 'success', 'failed',
    'running', 'queued'). Used by verify_node to check whether a RETRY
    actually fixed things, instead of blindly assuming success.
    """
    try:
        token = get_auth_token()
    except Exception as e:
        print(f"[airflow_api] Could not authenticate to read task state: {e}")
        return None

    url = (
        f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
        f"/taskInstances/{task_id}"
    )
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = httpx.get(url, headers=headers, timeout=15)
    except Exception as e:
        print(f"[airflow_api] Could not reach Airflow API for task state: {e}")
        return None

    if response.status_code == 200:
        return response.json().get("state")
    print(f"[airflow_api] Could not fetch task state (status {response.status_code}): {response.text}")
    return None