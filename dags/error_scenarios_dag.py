"""
A dedicated DAG of deliberate, independent failure scenarios — the
foundation for evaluating the self-healing agent's judgment, not its
"happy path" wiring.

Each task fails in a different CATEGORY of way on purpose, and each
carries an EXPECTED classification as a docstring so this file doubles as
a human-readable evaluation spec once real scoring logic gets built on
top of it (compare the agent's actual decision per incident against the
"expected" column below).

Tasks are independent (no dependencies between them) so every scenario
reproduces cleanly on every DAG run, and a failure in one never blocks
the others from running and being observed.

| Task                        | Category                | Expected agent decision            |
|------------------------------|--------------------------|-------------------------------------|
| missing_table_error          | schema / infra           | RETRY or auto-fix (schema_healer)   |
| syntax_error_sql              | code bug (SQL)           | SUGGEST_FIX                         |
| wrong_column_error            | code bug (schema mismatch)| SUGGEST_FIX                        |
| transient_connection_error    | transient / infra        | RETRY                               |
| permission_denied_error       | auth / access             | ESCALATE                            |
| flaky_then_succeeds           | genuinely transient       | RETRY (and should actually resolve) |
| python_logic_bug              | code bug (Python, non-SQL)| SUGGEST_FIX                        |

Nothing here has retries=0 overridden per-task; Airflow's own retry
mechanism is intentionally left at 0 (see default_args) so every retry
that happens is the AGENT's decision, not Airflow silently retrying on
its own — otherwise we couldn't tell which system actually caused a
recovery.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
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


def _transient_connection_error(**context):
    # Wording deliberately reads as infra/transient, not a code bug —
    # this is what should push the agent toward RETRY.
    raise ConnectionError(
        "Could not connect to upstream service at sales-api.internal:5432 — "
        "connection reset by peer. This is typically transient."
    )


def _permission_denied_error(**context):
    # Wording deliberately reads as an access/auth problem — retrying
    # won't fix this, and neither will a code patch; it needs a human.
    raise PermissionError(
        "Access denied: role 'agent_service_account' does not have SELECT "
        "privilege on table 'restricted_financial_data'."
    )


def _flaky_then_succeeds(**context):
    # Genuinely transient: fails on the first two attempts, succeeds on
    # the third. Lets us verify the FULL retry loop end-to-end — not just
    # that the agent decides RETRY, but that clearing the task instance
    # via MCP/REST actually results in a real, verified success rather
    # than looping forever or giving up too early.
    ti = context["ti"]
    if ti.try_number < 3:
        raise TimeoutError(
            f"Upstream request timed out (attempt {ti.try_number}/3). Retrying usually resolves this."
        )
    print("Succeeded on attempt 3 — simulated flakiness resolved.")


def _python_logic_bug(**context):
    # A real Python bug, not a SQL problem — tests whether SUGGEST_FIX
    # generalizes beyond SQL-shaped errors to plain code bugs too.
    config = {"environment": "production"}
    value = config["enviroment"]  # typo'd key — real KeyError, not staged text
    print(value)


with DAG(
    dag_id="agent_error_scenarios",
    default_args=default_args,
    description="Deliberate, independent failure scenarios for evaluating the self-healing agent",
    schedule=None,  # manual/on-demand only — this DAG exists to be triggered deliberately for testing
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["evaluation", "self-healing-test"],
) as dag:

    missing_table_error = SQLExecuteQueryOperator(
        task_id="missing_table_error",
        conn_id="postgres_default",
        sql="SELECT * FROM nonexistent_scenario_table;",
        on_failure_callback=agent_failure_callback,
    )

    syntax_error_sql = SQLExecuteQueryOperator(
        task_id="syntax_error_sql",
        conn_id="postgres_default",
        sql="SELEKT * FROM sales_records;",  # deliberate typo — real SQL syntax error
        on_failure_callback=agent_failure_callback,
    )

    wrong_column_error = SQLExecuteQueryOperator(
        task_id="wrong_column_error",
        conn_id="postgres_default",
        sql="SELECT nonexistent_column FROM sales_records;",
        on_failure_callback=agent_failure_callback,
    )

    transient_connection_error = PythonOperator(
        task_id="transient_connection_error",
        python_callable=_transient_connection_error,
        on_failure_callback=agent_failure_callback,
    )

    permission_denied_error = PythonOperator(
        task_id="permission_denied_error",
        python_callable=_permission_denied_error,
        on_failure_callback=agent_failure_callback,
    )

    flaky_then_succeeds = PythonOperator(
        task_id="flaky_then_succeeds",
        python_callable=_flaky_then_succeeds,
        on_failure_callback=agent_failure_callback,
    )

    python_logic_bug = PythonOperator(
        task_id="python_logic_bug",
        python_callable=_python_logic_bug,
        on_failure_callback=agent_failure_callback,
    )

    # Deliberately no >> chaining — every scenario is independent so one
    # failing never blocks the others from running and being observed.
