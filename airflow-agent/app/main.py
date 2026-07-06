from fastapi import FastAPI
from app.api.routes import router
from app.api.dashboard import router as dashboard_router
from app.services.decision_log import init_db

app = FastAPI()

app.include_router(router, prefix="/api")
app.include_router(dashboard_router)


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
