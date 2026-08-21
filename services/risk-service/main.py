"""
risk-service: scores bot/scalper risk asynchronously and writes results to
Redis for fast lookup by the waiting-room and checkout APIs.

This is a stub. Real scoring logic and the Kafka consumer land in Phase 10.
"""

from fastapi import FastAPI

app = FastAPI(title="risk-service")


@app.get("/healthz")
def healthz():
    return {"status": "ok", "note": "stub, not implemented yet"}
