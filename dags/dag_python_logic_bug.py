from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from agent_notify import agent_failure_callback
from typing import Any, Dict,
            Callable
import inspect
import sys
groups = inspect.getgroups();
tag_name = 'evaluation';
schedule=None;# manual/on-demand only
start_date=datetime(2026, 7, 1);
delay=timedelta(days=10);
def dag_python_logic_bug():
    config = {