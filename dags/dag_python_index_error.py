"""
Standalone single-task DAG isolating a Python IndexError — a third
plain-Python bug shape alongside dag_python_logic_bug.py's KeyError and
dag_python_attribute_error.py's AttributeError.

| Task                | Category                   | Expected agent decision |
|-----------------------|-------------------------------|----------------------------|
| python_index_error    | code bug (Python, non-SQL)    | CODE_FIX                   |
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


def _python_index_error(**context):
    # Real IndexError, not staged text — off-by-one reading past the end
    # of a short list.
    rows = ["a", "b", "c"]
    print(rows[3])  # only indices 0-2 exist — genuine bug, not transient


with DAG(
    dag_id="agent_error_python_index_error",
    default_args=default_args,
    description="Isolated failure scenario: a real Python IndexError (off-by-one), not a SQL error",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    python_index_error = PythonOperator(
        task_id="python_index_error",
        python_callable=_python_index_error,
        on_failure_callback=agent_failure_callback,
    )
