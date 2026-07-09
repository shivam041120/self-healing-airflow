"""
Standalone single-task DAG isolating the transient-connection failure
scenario.

Split out of agent_error_scenarios so each error type can be triggered on
its own — one on_failure_callback fires per run instead of several at
once, which keeps this from piling concurrent load onto the agent/Ollama.

| Task                         | Category          | Expected agent decision |
|--------------------------------|--------------------|----------------------------|
| transient_connection_error    | transient / infra  | RETRY                      |
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


def _transient_connection_error(**context):
    # Wording deliberately reads as infra/transient, not a code bug —
    # this is what should push the agent toward RETRY.
    raise ConnectionError(
        "Could not connect to upstream service at sales-api.internal:5432 — "
        "connection reset by peer. This is typically transient."
    )


with DAG(
    dag_id="agent_error_transient_connection",
    default_args=default_args,
    description="Isolated failure scenario: a simulated transient upstream connection failure",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    transient_connection_error = PythonOperator(
        task_id="transient_connection_error",
        python_callable=_transient_connection_error,
        on_failure_callback=agent_failure_callback,
    )
