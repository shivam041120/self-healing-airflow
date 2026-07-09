"""
Standalone single-task DAG isolating the permission-denied failure
scenario.

Split out of agent_error_scenarios so each error type can be triggered on
its own — one on_failure_callback fires per run instead of several at
once, which keeps this from piling concurrent load onto the agent/Ollama.

| Task                     | Category      | Expected agent decision |
|----------------------------|----------------|----------------------------|
| permission_denied_error   | auth / access  | ESCALATE                   |
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from agent_notify import agent_failure_callback

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}


def _permission_denied_error(**context):
    # Wording deliberately reads as an access/auth problem — retrying
    # won't fix this, and neither will a code patch; it needs a human.
    raise PermissionError(
        "Access denied: role 'agent_service_account' does not have SELECT "
        "privilege on table 'restricted_financial_data'."
    )


with DAG(
    dag_id="agent_error_permission_denied",
    default_args=default_args,
    description="Isolated failure scenario: an auth/access error that should be escalated, not retried",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    permission_denied_error = PythonOperator(
        task_id="permission_denied_error",
        python_callable=_permission_denied_error,
        on_failure_callback=agent_failure_callback,
    )
