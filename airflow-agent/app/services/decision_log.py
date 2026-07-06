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
import asyncpg

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

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
    except Exception as e:
        print(f"[decision_log] Failed to write decision trace: {e}")


async def get_recent_decisions(limit: int = 100):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_decisions ORDER BY created_at DESC LIMIT $1", limit
        )
        return [dict(r) for r in rows]
