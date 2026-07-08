"""
Decision-trace logging. Every node in the LangGraph loop (analyze, action,
verify) writes a row here — not just the final result — so a human can see
exactly what the agent saw, what it decided, and why, per incident.

Reuses the existing Airflow metadata Postgres (same container, same
credentials) with its own table, rather than standing up a second database.
Fine for this project's scope; a production system would likely want a
separate database so agent write load never touches Airflow's own metadata
tables.
"""

import os
import json
import asyncpg

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

# Channel used for Postgres LISTEN/NOTIFY so the live dashboard can push
# updates the instant a decision is written, instead of polling.
NOTIFY_CHANNEL = "agent_decisions_channel"

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            min_size=1,
            max_size=5,
        )
    return _pool


async def get_raw_connection():
    """
    A standalone (non-pooled) connection, for the SSE endpoint to hold open
    long-term and LISTEN on. Pooled connections shouldn't be held for the
    lifetime of a streaming HTTP response.
    """
    return await asyncpg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_decisions (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                dag_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                dag_run_id TEXT NOT NULL,
                node TEXT NOT NULL,
                attempt INTEGER,
                logs_excerpt TEXT,
                reasoning TEXT,
                action_decision TEXT,
                verification_result BOOLEAN
            );
            """
        )


async def log_decision(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    node: str,
    attempt: int = None,
    logs_excerpt: str = None,
    reasoning: str = None,
    action_decision: str = None,
    verification_result: bool = None,
):
    """
    Never raises — a logging failure should not take down the agent loop.
    Falls back to printing so you still see it in container logs.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_decisions
                  (dag_id, task_id, dag_run_id, node, attempt, logs_excerpt,
                   reasoning, action_decision, verification_result)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                dag_id,
                task_id,
                dag_run_id,
                node,
                attempt,
                (logs_excerpt or "")[:2000],
                reasoning,
                action_decision,
                verification_result,
            )
            # Best-effort push notification. Payload stays tiny (just the
            # incident key) — listeners re-fetch that incident's rows
            # rather than trusting the notify payload as the source of
            # truth, so this can never desync from the actual table.
            try:
                await conn.execute(
                    "SELECT pg_notify($1, $2)",
                    NOTIFY_CHANNEL,
                    json.dumps({"dag_id": dag_id, "task_id": task_id, "dag_run_id": dag_run_id}),
                )
            except Exception as notify_err:
                print(f"[decision_log] Notify failed (non-fatal): {notify_err}")
    except Exception as e:
        print(f"[decision_log] Failed to write decision trace: {e}")


async def get_decisions(
    dag_id: str = None,
    task_id: str = None,
    status_hint: str = None,
    since_hours: int = None,
    limit: int = 500,
):
    """
    Filtered fetch used by the JSON API. Filters are applied at the SQL
    level where possible (dag_id/task_id/time range); status is a derived
    concept computed later in incident_view, so it isn't filterable here —
    the API layer filters on it after grouping instead.
    """
    query = "SELECT * FROM agent_decisions WHERE 1=1"
    params = []
    if dag_id:
        params.append(dag_id)
        query += f" AND dag_id = ${len(params)}"
    if task_id:
        params.append(task_id)
        query += f" AND task_id = ${len(params)}"
    if since_hours:
        params.append(since_hours)
        query += f" AND created_at >= now() - make_interval(hours => ${len(params)})"
    query += " ORDER BY created_at DESC"
    params.append(limit)
    query += f" LIMIT ${len(params)}"

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(r) for r in rows]


async def get_recent_decisions(limit: int = 100):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_decisions ORDER BY created_at DESC LIMIT $1", limit
        )
        return [dict(r) for r in rows]
