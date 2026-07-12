import httpx
import json
import os
from typing import Optional

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-apiserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "airflow")


def _flatten_structured_log(raw: str) -> str:
    """
    Airflow 3's task-log API returns structured JSON — a single-line blob
    shaped like {"content": [{"event": "...", "timestamp": ..., ...}, ...]}
    — not a plain-text traceback. Feeding that raw JSON straight to the
    LLM (or to any regex expecting real line breaks, like the SQL/Python
    code-bug detectors in agent.py) buries the actual exception in JSON
    punctuation and boilerplate (group markers, timestamps, logger names),
    with no line breaks for a small model — or a regex — to anchor on.

    Critically, the actual exception is NOT in the generic "event" text
    ("Task failed with exception") — it's under a separate "error_detail"
    field: a list of {exc_type, exc_value, frames: [{filename, lineno,
    name}, ...]} (a list because Python exception chains — __cause__/
    __context__ — can nest more than one). Without reading this, the
    flattened log has all the boilerplate but never the actual error.

    Flattens both into one human-readable line per event/exception when
    the response matches this shape. Falls through unchanged for anything
    else (plain-text log, error string, different Airflow version's
    format), so this never turns a working plain-text path into a broken
    one.
    """
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw

    content = data.get("content") if isinstance(data, dict) else None
    if not isinstance(content, list):
        return raw

    lines = []
    for entry in content:
        if isinstance(entry, str):
            lines.append(entry)
            continue
        if not isinstance(entry, dict):
            continue

        if "event" in entry:
            lines.append(str(entry["event"]))

        for exc in entry.get("error_detail") or []:
            if not isinstance(exc, dict):
                continue
            exc_type = exc.get("exc_type", "Exception")
            exc_value = exc.get("exc_value", "")
            # Unindented "ExceptionType: message" — matches the same
            # shape a plain-text traceback ends with, so the existing
            # SQL/Python code-bug regexes in agent.py match it correctly.
            lines.append(f"{exc_type}: {exc_value}")
            for frame in exc.get("frames") or []:
                lines.append(
                    f"  at {frame.get('filename', '?')}:{frame.get('lineno', '?')} "
                    f"in {frame.get('name', '?')}"
                )

    return "\n".join(lines) if lines else raw


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
        return _flatten_structured_log(response.text)
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