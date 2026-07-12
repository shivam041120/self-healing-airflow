"""
Standalone single-task DAG isolating a SQL wrong-column-reference error —
distinct from both the missing-table shape (schema_healer's territory) and
the syntax-typo shape (dag_syntax_error_sql.py). Here the table exists and
the SQL is syntactically valid, but it references a column that was never
created — a genuine code bug in the DAG file, not an infra/schema issue.

| Task                   | Category      | Expected agent decision |
|-------------------------|----------------|----------------------------|
| sql_wrong_column_error  | code bug (SQL) | CODE_FIX                   |
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
    dag_id="agent_error_sql_wrong_column",
    default_args=default_args,
    description="Isolated failure scenario: query references a column that doesn't exist",
    schedule=None,  # manual/on-demand only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    # sales_records only has id/amount (see sales_pipeline_dag.py) — this
    # column never existed, it's not a typo of an existing one, so this
    # can't be confused with the missing-table auto-heal path.
    sql_wrong_column_error = SQLExecuteQueryOperator(
        task_id="sql_wrong_column_error",
        conn_id="postgres_default",
        sql="SELECT revenue FROM sales_records;",
        on_failure_callback=agent_failure_callback,
    )
