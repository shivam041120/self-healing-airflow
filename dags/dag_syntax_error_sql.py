"""
Standalone single-task DAG isolating the SQL syntax-error failure scenario.

Split out of agent_error_scenarios so each error type can be triggered on
its own — one on_failure_callback fires per run instead of several at
once, which keeps this from piling concurrent load onto the agent/Ollama.

| Task              | Category         | Expected agent decision |
|--------------------|-------------------|----------------------------|
| syntax_error_sql   | code bug (SQL)    | SUGGEST_FIX                |
"""

from airflow import DAG
from datetime import datetime
from agent_notify import agent_failure_callback

try:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
except ImportError as e:
    raise ImportError("SQL provider missing. Install apache-airflow-providers-common-sql.") from e

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="agent_error_syntax_error_sql",
    default_args=default_args,
    description="Isolated failure scenario: SQL statement with a syntax error",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    syntax_error_sql = SQLExecuteQueryOperator(
        task_id="syntax_error_sql",
        conn_id="postgres_default",
        sql="SELEKT * FROM sales_records;",  # deliberate typo — real SQL syntax error
        on_failure_callback=agent_failure_callback,
    )
