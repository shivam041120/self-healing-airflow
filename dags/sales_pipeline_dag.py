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
    dag_id="sales_pipeline",
    default_args=default_args,
    description="A sales pipeline DAG with self-healing capabilities",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
) as dag:

    create_sales_table = SQLExecuteQueryOperator(
        task_id="create_sales_table",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS sales_records (
                id INTEGER PRIMARY KEY,
                amount NUMERIC
            );
        """,
        on_failure_callback=agent_failure_callback,
    )

    create_sample_record = SQLExecuteQueryOperator(
        task_id="create_sample_record",
        conn_id="postgres_default",
        sql="INSERT INTO sales_records (id, amount) VALUES (1, 100.00) ON CONFLICT DO NOTHING;",
        on_failure_callback=agent_failure_callback,
    )

    check_sales_data = SQLExecuteQueryOperator(
        task_id="check_sales_data",
        conn_id="postgres_default",
        sql="SELECT count(*) FROM sales_records;",
        on_failure_callback=agent_failure_callback,
    )

    create_sales_table >> create_sample_record >> check_sales_data