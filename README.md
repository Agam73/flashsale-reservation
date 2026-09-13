# flashsale-reservation

An event-driven flash-sale / ticket reservation backend, built to learn
distributed systems, Kafka, Go, and AI service integration by building
something with real failure modes -- not another CRUD app.

See [`docs/phase-plan.md`](docs/phase-plan.md) for the full build plan
and the design decisions locked in during Phase 1,
[`docs/phase8.md`](docs/phase8.md) for the reliability layer, and
[`docs/phase9.md`](docs/phase9.md) for the current Redis layer.

## Status: Phases 1-9 complete (through Redis)

| Service | Language | Role | Status |
|---|---|---|---|
| `cmd/waiting-room-api` | Go | Admits buyers via a per-item FIFO queue, grants short-lived Redis admission tokens, publishes queue depth to Redis | Implemented (Phase 2, 4, 9) |
| `cmd/checkout-api` | Go | Fast Redis inventory check, publishes purchase attempts to Kafka, reconciles Redis inventory from Postgres on startup and on a schedule | Implemented (Phase 4, 6, 9) |
| `cmd/decision-service` | Go | Authoritative Kafka consumer, writes reservations to Postgres, classifies failures and dead-letters bad messages | Implemented (Phase 6, 8) |
| `cmd/expiry-worker` | Go | Worker-pool releasing inventory from unpaid, expired reservations | Implemented (Phase 7) |
| `services/risk-service` | Python (FastAPI) | Scores bot/scalper risk asynchronously | Stub -- health endpoint only, logic is Phase 10 (Redis cache it'll write to is built, see `docs/phase9.md`) |

Correctness properties that are actually verified, not just claimed:
- **Never oversells inventory** -- `decision.ProcessAttempt` proven
  under a 50-goroutine concurrency test against 10 units of stock.
- **Never double-releases a reservation** -- `expiry.ExpireReservation`
  proven under the same kind of concurrency test.
- **Survives a Postgres outage without wrongly dead-lettering
  messages, and survives a bad message without crashing or blocking
  the partition** -- see [`docs/phase8.md`](docs/phase8.md).
- **Redis inventory is always rebuildable from Postgres** --
  `internal/reconcile` re-seeds it at startup and on a schedule,
  Postgres always winning over whatever Redis currently has -- see
  [`docs/phase9.md`](docs/phase9.md).

## Running the local infra

```bash
docker compose up -d
docker compose ps
```

This brings up Postgres (`localhost:5432`), Redis (`localhost:6379`),
Kafka in KRaft mode (`localhost:9092`), and Kafka UI
(`http://localhost:8090`) for inspecting topics and consumer groups.

Create the Kafka topics before starting `checkout-api` or
`decision-service` for the first time:

```bash
bash scripts/setup_kafka_topic.sh
```

Apply migrations:

```bash
migrate -path migrations -database "$DATABASE_URL" up
```

## Running a Go service

```bash
go run ./cmd/waiting-room-api
curl localhost:8081/healthz
```

Same pattern for `checkout-api` (`:8082`), `decision-service`
(`:8083`), and `expiry-worker` (`:8084`) -- see `.env.example` for the
full port/config list. `checkout-api` now also needs `DATABASE_URL`
set (Phase 9): it reconciles Redis inventory from Postgres once at
startup and again every `INVENTORY_RECONCILE_INTERVAL_SECONDS`.

## Running the risk-service stub

```bash
cd services/risk-service
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
curl localhost:8000/healthz
```

This one is still a stub -- health endpoint only. Real risk-scoring
logic is Phase 10.

## Running the tests

```bash
go test ./... -p 1
```

`-p 1` serializes package execution against the live Postgres --
running `internal/decision`'s concurrency test and `internal/expiry`'s
`TRUNCATE` calls in parallel corrupts each other's fixtures. Tests
that need Postgres/Redis/Kafka skip cleanly if that infra isn't up.
