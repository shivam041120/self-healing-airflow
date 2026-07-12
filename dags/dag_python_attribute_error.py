from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from agent_notify import agent_failure_callback

default_args = {
    "owner": "airflow",# airflow's owner is typically the same as the DAG runner.
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

def _python_attribute_error(**context):
    # Fixing the bug by calling the 'total' method on record before doing anything else with it. This is a good practice to avoid attribute errors.
    record = {