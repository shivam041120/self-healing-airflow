"""
JSON API backing the live dashboard (and usable by anything else — a CLI,
a Slack bot, a status page). Also exposes /api/stream: a Server-Sent-Events
endpoint that pushes the moment a decision is written, using Postgres's
native LISTEN/NOTIFY rather than having every open dashboard poll on a
timer.
"""

import asyncio
import json

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.services.decision_log import get_decisions, get_raw_connection, NOTIFY_CHANNEL
from app.services.incident_view import group_into_incidents, compute_summary, compute_daily_trend, compute_heatmap_days

router = APIRouter(prefix="/api")


@router.get("/incidents")
async def list_incidents(
    dag_id: str = None,
    task_id: str = None,
    status: str = None,
    since_hours: int = Query(default=None, ge=1),
    limit: int = Query(default=200, le=2000),
):
    decisions = await get_decisions(dag_id=dag_id, task_id=task_id, since_hours=since_hours, limit=limit)
    incidents = group_into_incidents(decisions)
    if status:
        incidents = [i for i in incidents if i.status == status]
    return {
        "summary": compute_summary(incidents),
        "incidents": [i.to_dict() for i in incidents],
    }


@router.get("/incidents/{dag_id}/{task_id}/{dag_run_id}")
async def incident_detail(dag_id: str, task_id: str, dag_run_id: str):
    decisions = await get_decisions(dag_id=dag_id, task_id=task_id, limit=500)
    decisions = [d for d in decisions if d["dag_run_id"] == dag_run_id]
    incidents = group_into_incidents(decisions)
    if not incidents:
        return {"error": "not found"}
    return incidents[0].to_dict(include_rows=True)


@router.get("/stats")
async def stats(days: int = Query(default=14, ge=1, le=90), since_hours: int = None):
    decisions = await get_decisions(since_hours=since_hours or days * 24, limit=5000)
    incidents = group_into_incidents(decisions)
    return {
        "summary": compute_summary(incidents),
        "trend": compute_daily_trend(incidents, days=days),
    }


@router.get("/heatmap")
async def heatmap(days: int = Query(default=90, ge=1, le=365)):
    # This route didn't exist before — the frontend was already calling
    # it (loadHeatmap() in app.js), silently 404ing, and leaving the
    # "Incident density" card rendered with a header and legend but no
    # actual grid, since safeRun() swallows the failed fetch.
    decisions = await get_decisions(since_hours=days * 24, limit=5000)
    incidents = group_into_incidents(decisions)
    return {"days": compute_heatmap_days(incidents, days=days)}


@router.get("/stream")
async def stream():
    """
    Server-Sent Events endpoint. Holds one dedicated Postgres connection
    LISTENing on the decisions channel; every NOTIFY (fired by
    decision_log.log_decision right after an insert) is forwarded to the
    browser as an event within milliseconds — no polling delay, no wasted
    queries when nothing is happening.
    """
    async def event_generator():
        conn = await get_raw_connection()
        queue: asyncio.Queue = asyncio.Queue()

        def _on_notify(_connection, _pid, _channel, payload):
            queue.put_nowait(payload)

        await conn.add_listener(NOTIFY_CHANNEL, _on_notify)
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"event: incident_update\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    # Keepalive so proxies/browsers don't consider the
                    # connection dead during quiet periods.
                    yield ": keepalive\n\n"
        finally:
            await conn.remove_listener(NOTIFY_CHANNEL, _on_notify)
            await conn.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")