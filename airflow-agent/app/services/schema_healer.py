"""
Deterministic root-cause healing for a common, very fixable failure class:
a SQL task failing because it queries a table that doesn't exist yet (typo'd
name, renamed table, config drift, etc.).

Why this exists separately from the LLM: a small local model (e.g.
llama3.2:1b) can *describe* an error reasonably well but is unreliable at
deciding precisely when it's safe to act automatically. For this one
well-understood failure shape, we don't need to ask it — we can look at
Postgres's own error, look at what tables actually exist, and act (or
clearly recommend an action) deterministically. Everything else still goes
through the LLM's RETRY/ESCALATE/NONE judgment in agent.py.

Two outcomes only:
  - AUTO_FIX:  a confidently similar existing table was found, we cloned its
               structure under the missing name, and it's now safe to retry.
  - SUGGESTED: we found the missing table but no confident match (or the fix
               itself failed) — we hand back a concrete, human-readable
               suggestion instead of guessing.
If the log doesn't match this failure shape at all, we do nothing and let
the existing LLM-driven flow proceed untouched.
"""

import os
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Optional

import asyncpg

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
# NOTE: intentionally the *airflow* database, not a separate one — that's
# the same Postgres schema the sales_pipeline DAG's own tasks read/write,
# which is exactly what needs inspecting/fixing here.
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

# Below this similarity score we refuse to guess — we only suggest.
AUTO_FIX_SIMILARITY_THRESHOLD = 0.55

_MISSING_RELATION_RE = re.compile(r'relation "([^"]+)" does not exist', re.IGNORECASE)

_pool = None


async def _get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            min_size=1,
            max_size=3,
        )
    return _pool


@dataclass
class HealResult:
    matched: bool  # did the log even look like a missing-relation error?
    fixed: bool  # did we actually apply a fix?
    missing_table: Optional[str] = None
    candidate_table: Optional[str] = None
    similarity: Optional[float] = None
    message: str = ""


def _extract_missing_table(log_text: str) -> Optional[str]:
    match = _MISSING_RELATION_RE.search(log_text or "")
    if not match:
        return None
    # Postgres sometimes qualifies with a schema, e.g. "public.foo"
    return match.group(1).split(".")[-1]


async def _find_similar_table(missing_table: str) -> tuple[Optional[str], float]:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name != $1
            """,
            missing_table,
        )
    best_table, best_score = None, 0.0
    for row in rows:
        candidate = row["table_name"]
        score = SequenceMatcher(None, missing_table, candidate).ratio()
        if score > best_score:
            best_table, best_score = candidate, score
    return best_table, best_score


async def _clone_table(missing_table: str, source_table: str) -> None:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        # LIKE ... INCLUDING ALL copies columns, defaults, and constraints
        # (not data) — enough for the missing table to satisfy the query.
        await conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{missing_table}" '
            f'(LIKE "{source_table}" INCLUDING ALL);'
        )
        await conn.execute(
            f'INSERT INTO "{missing_table}" SELECT * FROM "{source_table}";'
        )


async def diagnose_and_heal(log_text: str) -> HealResult:
    """
    Looks at a task's failure log. If it's a missing-relation error, tries
    to find a confidently similar existing table and clone it into place.
    Returns a HealResult describing what was found and/or done — never
    raises, so a healing failure can't take down the agent loop.
    """
    missing_table = _extract_missing_table(log_text)
    if not missing_table:
        return HealResult(matched=False, fixed=False)

    try:
        candidate, score = await _find_similar_table(missing_table)
    except Exception as e:
        return HealResult(
            matched=True,
            fixed=False,
            missing_table=missing_table,
            message=f"Detected missing table '{missing_table}' but could not "
                     f"inspect the database to find a fix candidate: {e}",
        )

    if not candidate:
        return HealResult(
            matched=True,
            fixed=False,
            missing_table=missing_table,
            message=f"Detected missing table '{missing_table}', but no existing "
                     f"table looked similar enough to clone. This likely needs "
                     f"a real migration, not an automated fix.",
        )

    if score < AUTO_FIX_SIMILARITY_THRESHOLD:
        return HealResult(
            matched=True,
            fixed=False,
            missing_table=missing_table,
            candidate_table=candidate,
            similarity=round(score, 2),
            message=f"Detected missing table '{missing_table}'. Closest existing "
                     f"table is '{candidate}' (similarity {score:.2f}), but that's "
                     f"below the auto-fix confidence threshold "
                     f"({AUTO_FIX_SIMILARITY_THRESHOLD}). Suggest reviewing "
                     f"whether '{candidate}' is the intended table, or whether "
                     f"'{missing_table}' needs to be created via a real migration.",
        )

    try:
        await _clone_table(missing_table, candidate)
    except Exception as e:
        return HealResult(
            matched=True,
            fixed=False,
            missing_table=missing_table,
            candidate_table=candidate,
            similarity=round(score, 2),
            message=f"Found a confident match ('{candidate}', similarity "
                     f"{score:.2f}) for missing table '{missing_table}', but "
                     f"applying the fix failed: {e}",
        )

    return HealResult(
        matched=True,
        fixed=True,
        missing_table=missing_table,
        candidate_table=candidate,
        similarity=round(score, 2),
        message=f"Created table '{missing_table}' by cloning the structure and "
                 f"data of the similar existing table '{candidate}' "
                 f"(similarity {score:.2f}). Retrying the task.",
    )
