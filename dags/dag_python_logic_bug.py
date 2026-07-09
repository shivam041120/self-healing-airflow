"""
Standalone single-task DAG isolating the plain-Python-bug failure
scenario (as opposed to a SQL-shaped error).

Split out of agent_error_scenarios so each error type can be triggered on
its own — one on_failure_callback fires per run instead of several at
once, which keeps this from piling concurrent load onto the agent/Ollama.

| Task              | Category                    | Expected agent decision |
|--------------------|-------------------------------|----------------------------|
| python_logic_bug   | code bug (Python, non-SQL)    | SUGGEST_FIX                |
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


def _python_logic_bug(**context):
    # A real Python bug, not a SQL problem — tests whether SUGGEST_FIX
    # generalizes beyond SQL-shaped errors to plain code bugs too.
    config = {"environment": "production"}
    value = config["enviroment"]  # typo'd key — real KeyError, not staged text
    print(value)


with DAG(
    dag_id="agent_error_python_logic_bug",
    default_args=default_args,
    description="Isolated failure scenario: a real Python bug (typo'd dict key), not a SQL error",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    python_logic_bug = PythonOperator(
        task_id="python_logic_bug",
        python_callable=_python_logic_bug,
        on_failure_callback=agent_failure_callback,
    )
