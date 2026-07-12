"""
Standalone single-task DAG isolating a SQL syntax error caused by a
dangling trailing comma in a column list — a different syntax-error shape
than dag_syntax_error_sql.py's misspelled keyword, so the router/
code_specialist are exercised against more than one flavor of "invalid
SQL".

| Task                     | Category      | Expected agent decision |
|----------------------------|----------------|----------------------------|
| sql_trailing_comma_error   | code bug (SQL) | CODE_FIX                   |
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
    dag_id="agent_error_sql_trailing_comma",
    default_args=default_args,
    description="Isolated failure scenario: SQL statement with a dangling trailing comma",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    sql_trailing_comma_error = SQLExecuteQueryOperator(
        task_id="sql_trailing_comma_error",
        conn_id="postgres_default",
        # Trailing comma before FROM — a genuine syntax error, not just an
        # implicit alias (unlike "SELECT id amount FROM ...", which
        # Postgres actually accepts and so wouldn't fail at all).
        sql="SELECT id, FROM sales_records;",
        on_failure_callback=agent_failure_callback,
    )

