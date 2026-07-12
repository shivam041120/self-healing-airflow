"""
Standalone single-task DAG isolating a Python AttributeError — a different
bug shape than dag_python_logic_bug.py's typo'd dict key (KeyError), so the
router/code_specialist are exercised against more than one flavor of
plain-Python bug, not just SQL-shaped ones.

| Task                    | Category                   | Expected agent decision |
|---------------------------|-------------------------------|----------------------------|
| python_attribute_error    | code bug (Python, non-SQL)    | CODE_FIX                   |
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


def _python_attribute_error(**context):
    # Real AttributeError, not staged text — calling a method that
    # doesn't exist on a plain dict.
    record = {"amount": 100.0}
    record.total()  # dict has no .total() — genuine bug, not transient


with DAG(
    dag_id="agent_error_python_attribute_error",
    default_args=default_args,
    description="Isolated failure scenario: a real Python AttributeError, not a SQL error",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    python_attribute_error = PythonOperator(
        task_id="python_attribute_error",
        python_callable=_python_attribute_error,
        on_failure_callback=agent_failure_callback,
    )
