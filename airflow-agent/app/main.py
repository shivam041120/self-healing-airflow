from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.api.routes import router
from app.api.dashboard import router as dashboard_router
from app.api.incidents_api import router as incidents_router
from app.api.github_webhook import router as github_webhook_router
from app.services.decision_log import init_db

app = FastAPI()

app.include_router(router, prefix="/api")
app.include_router(dashboard_router)  # legacy server-rendered /dashboard
app.include_router(incidents_router)  # JSON API + /api/stream (SSE) backing the live console
app.include_router(github_webhook_router, prefix="/api")  # PR-merge-gated single retry for CODE_FIX incidents

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/console")
def console():
    # The live console (sidebar, filters, animated pipeline, stat cards,
    # trend chart, incident table, drawer) — a static single-page app that
    # talks to /api/incidents, /api/stats, and /api/stream. No build step:
    # plain HTML/CSS/JS served directly.
    return FileResponse("app/static/dashboard/index.html")


@app.on_event("startup")
async def startup():
    try:
        await init_db()
        print("[startup] agent_decisions table ready")
    except Exception as e:
        # Don't crash the whole app if Postgres isn't reachable yet at
        # startup — the agent can still run, decision logging just won't
        # persist until the DB is available.
        print(f"[startup] Could not initialize decision log DB: {e}")


@app.get("/health")
def health_check():
    return {"status": "ok"}
