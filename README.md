# flashsale-reservation

An event-driven flash-sale / ticket reservation backend, built to learn
distributed systems, Kafka, Go, and AI service integration by building
something with real failure modes -- not another CRUD app.

See [`docs/phase-plan.md`](docs/phase-plan.md) for the full build plan and
the design decisions locked in during Phase 1.

## Services

| Service | Language | Role |
|---|---|---|
| `cmd/waiting-room-api` | Go | Admits buyers, tracks queue position |
| `cmd/checkout-api` | Go | Fast Redis inventory check, publishes purchase attempts to Kafka |
| `cmd/decision-service` | Go | Authoritative Kafka consumer, writes reservations to Postgres |
| `cmd/expiry-worker` | Go | Releases inventory from unpaid, expired reservations |
| `services/risk-service` | Python (FastAPI) | Scores bot/scalper risk asynchronously |

All services are currently stubs -- health endpoints only. Real logic is
built phase by phase per the build plan.

## Running the local infra

```bash
docker compose up -d
docker compose ps
```

This brings up Postgres (`localhost:5432`), Redis (`localhost:6379`),
Kafka in KRaft mode (`localhost:9092`), and Kafka UI
(`http://localhost:8090`) for inspecting topics and consumer groups.

## Running a Go service stub

```bash
go run ./cmd/waiting-room-api
curl localhost:8081/healthz
```

## Running the risk-service stub

```bash
cd services/risk-service
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
curl localhost:8000/healthz
```
