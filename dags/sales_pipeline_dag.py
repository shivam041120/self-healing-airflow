import os
import json
import httpx
from airflow import DAG
from datetime import datetime

# URL of the FastAPI self-healing agent (set in docker-compose environment)
AGENT_URL = os.getenv("AGENT_URL", "http://airflow-agent:8000")

# Path setup to dynamic config file (assumes config dir parallel to dags folder)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'query_config.json')


def get_query_from_config():
    """Dynamically loads the sql query from configuration to avoid hardcoding."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
        return config['sql_query']
    except Exception as e:
        # Fallback query if file layout isn't ready yet so the DAG still parses
        return "SELECT 'Config file missing or unreadable';"


def agent_failure_callback(context):
    """
    Fires when a task fails. Notifies the FastAPI self-healing agent
    so it can analyze logs and decide whether to retry/escalate/fix.
    """
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    dag_run_id = context["dag_run"].run_id

    print(f"Task failed. Notifying agent for dag={dag_id} task={task_id} run={dag_run_id}")

    try:
        response = httpx.post(
            f"{AGENT_URL}/api/analyze-failure",
            json={"dag_id": dag_id, "task_id": task_id, "dag_run_id": dag_run_id},
            timeout=60,
        )
        print(f"Agent responded: {response.status_code} {response.text}")
    except Exception as e:
        # Never let a notification failure crash the callback itself
        print(f"Could not reach self-healing agent at {AGENT_URL}: {e}")


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

    # NEW: creates the table if it doesn't exist yet. Without this, every
    # run fails immediately since sales_records never existed.
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

    # Added task to process the wrong SQL query from the schema configuration
    execute_faulty_sql = SQLExecuteQueryOperator(
        task_id="execute_faulty_sql",
        conn_id="postgres_default",
        sql=get_query_from_config(),
        on_failure_callback=agent_failure_callback,
    )

    # Original execution order preserved, appending the faulty query execution to the end
    create_sales_table >> create_sample_record >> check_sales_data >> execute_faulty_sql