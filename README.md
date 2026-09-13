# flashsale-reservation

An event-driven flash-sale / ticket reservation backend, built to learn
distributed systems, Kafka, Go, and AI service integration by building
something with real failure modes -- not another CRUD app.

See [`docs/phase-plan.md`](docs/phase-plan.md) for the full build plan
and the design decisions locked in during Phase 1,
[`docs/phase8.md`](docs/phase8.md) for the reliability layer,
[`docs/phase9.md`](docs/phase9.md) for the Redis layer, and
[`docs/phase10.md`](docs/phase10.md) for the risk-scoring service, and
[`docs/TEST_RESULTS.md`](docs/TEST_RESULTS.md) for a running log of
actual test suite runs (real output, not descriptions).

## Status: Phases 1-10 complete (through the risk-scoring service)

| Service | Language | Role | Status |
|---|---|---|---|
| `cmd/waiting-room-api` | Go | Admits buyers via a per-item FIFO queue, grants short-lived Redis admission tokens, publishes queue depth to Redis and a BuyerJoined event to Kafka | Implemented (Phase 2, 4, 9, 10) |
| `cmd/checkout-api` | Go | Fast Redis inventory check, publishes purchase attempts to Kafka, reconciles Redis inventory from Postgres on startup and on a schedule | Implemented (Phase 4, 6, 9) |
| `cmd/decision-service` | Go | Authoritative Kafka consumer, writes reservations to Postgres, classifies failures and dead-letters bad messages | Implemented (Phase 6, 8) |
| `cmd/expiry-worker` | Go | Worker-pool releasing inventory from unpaid, expired reservations | Implemented (Phase 7) |
| `services/risk-service` | Python (FastAPI) | Consumes BuyerJoined events, scores bot/scalper risk with a documented heuristic, caches the score in Redis | Implemented (Phase 10) -- nothing consumes the score yet, by design; see `docs/phase10.md` |

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
- **Risk scoring can never slow down or block admission** -- publishing
  the event that drives it happens off the request path entirely,
  proven by pointing the Kafka writer at an unreachable broker and
  confirming admission still returns in ~0.1s -- see
  [`docs/phase10.md`](docs/phase10.md).

## Running the local infra

```bash
docker compose up -d
docker compose ps
```

This brings up Postgres (`localhost:5432`), Redis (`localhost:6379`),
Kafka in KRaft mode (`localhost:9092`), and Kafka UI
(`http://localhost:8090`) for inspecting topics and consumer groups.

Create the Kafka topics before starting `waiting-room-api`,
`checkout-api`, `decision-service`, or `risk-service` for the first
time:

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

## Running risk-service

```bash
cd services/risk-service
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8085
curl localhost:8085/healthz
```

As of Phase 10 this consumes `waiting-room-joins` (make sure
`scripts/setup_kafka_topic.sh` has been run, and `waiting-room-api` is
actually producing to it) and writes a risk score per buyer to Redis.
Nothing else in this project reads that score yet -- see
`docs/phase10.md` for why. `GET /items/{item_id}/users/{user_id}/risk-score`
is a read-only endpoint for checking a cached score by hand.

## Running the tests

```bash
go test ./... -p 1
```

`-p 1` serializes package execution against the live Postgres --
running `internal/decision`'s concurrency test and `internal/expiry`'s
`TRUNCATE` calls in parallel corrupts each other's fixtures. Tests
that need Postgres/Redis/Kafka skip cleanly if that infra isn't up.

```bash
cd services/risk-service
pytest
```

Same skip-cleanly-if-unavailable approach as the Go tests, against the
same local Redis (`REDIS_ADDR`/`localhost:6379` by default) -- no
Kafka broker required, since `pytest` only exercises `process_event`
and the FastAPI app directly, never the real `AIOKafkaConsumer` loop.
