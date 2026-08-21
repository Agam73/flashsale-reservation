from fastapi import FastAPI

app = FastAPI(title="risk-service")


@app.get("/healthz")
def healthz():
    return {"status": "ok", "note": "stub, not implemented yet"}
