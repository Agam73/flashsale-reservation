# Phase 4 — Go API service

`waiting-room-api` and `checkout-api` over HTTP, Redis-only fast path,
no Kafka yet. This doc explains how the two services fit together and
why.

## The flow

```
buyer                 waiting-room-api              checkout-api
  |                          |                            |
  |-- POST /items/{id}/join->|                            |
  |   (blocks in FIFO queue) |                            |
  |                          |-- GrantAdmission (Redis) -->|
  |<---- 200 admitted -------|                            |
  |                                                        |
  |------------------- POST /items/{id}/checkout --------->|
  |                          |     ConsumeAdmission --------|
  |                          |     (must exist, single-use) |
  |                          |     TryDecrementInventory ----|
  |                          |     (atomic Lua script)       |
  |<------------------------------- 200 / 409 / 403 --------|
```

## waiting-room-api

`internal/admission` (Phase 2) is untouched -- `Admitter` is a single
FIFO queue admitting at a fixed rate, already built and tested. This
phase adds `internal/admission/registry.go`: a `Registry` that lazily
creates one `Admitter` per item ID, so a flash sale for item A doesn't
make buyers for item B wait behind it.

`POST /items/{itemID}/join` calls `Admitter.Join(r.Context())`, which
**doesn't return until the buyer is admitted or their connection
drops**. The HTTP request itself is the wait -- there's no separate
"check my position" endpoint to keep in sync with it, and no polling.
Go's `net/http` server already runs each request in its own goroutine
and cancels `r.Context()` when the client disconnects, so this maps
onto the existing Phase 2 API with zero changes to it.

On admission, the handler writes a short-lived token to Redis
(`admission:{itemID}:{userID}`, default TTL 120s) via
`redisx.GrantAdmission`. That token is the only thing that makes it
into checkout-api's world -- waiting-room-api never talks to Postgres
or Kafka.

## checkout-api

`POST /items/{itemID}/checkout` does two things, in order:

1. **`redisx.ConsumeAdmission`** -- atomically checks for and deletes
   the buyer's token (Redis `GETDEL`, one round trip). No token, no
   checkout: `403`. This is what makes the waiting room's fairness rule
   actually enforced, not just a suggestion a buyer can route around by
   calling checkout-api directly. It's also single-use, so a buyer
   can't reuse one trip through the waiting room to check out twice.

2. **`redisx.TryDecrementInventory`** -- a Lua script that checks and
   decrements `inventory:{itemID}` in one atomic round trip, so two
   concurrent requests can never both read "1 left" and both succeed.
   `ErrInsufficientInventory` → `409`, `ErrInventoryNotFound` → `404`.

`scripts/redisx smoke-tested this directly`: 50 goroutines racing to
decrement 1 unit of stock, exactly 1 succeeds (see
`internal/redisx/inventory_test.go`,
`TestTryDecrementInventoryNeverOversells`).

`POST /items/{itemID}/inventory` and `GET /items/{itemID}/inventory`
are dev/test-only endpoints to seed and read the Redis counter directly
-- there's no Postgres wiring in this phase to seed it from, so
something has to. Phase 9 replaces this with real seeding and
reconciliation from `items.available_inventory`.

## What a 200 from checkout does *not* mean

`checkout-api`'s response includes `"status": "fast_path_admitted"` on
purpose, not `"confirmed"` or `"reserved"`. Per the Phase 1 design
decision, Postgres is the source of truth; this phase never writes a
`reservations` row. A 200 here means "the fast path let this purchase
through," which is honest about what's actually been verified so far.
The durable, authoritative reservation -- the one an unpaid buyer can
actually lose to a TTL expiry, the one an oversell gets rejected against
for real -- doesn't exist until Phase 6 wires up the Kafka producer and
decision-service consuming it into Postgres.

## What's shared across services (`internal/`)

- **`internal/admission`** -- the Phase 2 `Admitter`, plus the new
  per-item `Registry`.
- **`internal/redisx`** -- Redis client setup, the key formats
  (`AdmissionKey`, `InventoryKey`) both services need to agree on, and
  the inventory/token operations themselves. Centralized so the two
  services can't drift into using different key formats.
- **`internal/httpx`** -- a `WriteJSON`/`WriteError` pair so every
  endpoint across both services returns the same `{"error": "..."}`
  shape on failure.
- **`internal/config`** -- tiny env-var helpers (`config.String`,
  `config.Int`) so every `main.go` reads configuration the same way.

## Known simplifications (intentional, revisited in later phases)

- One admission rate for every item, set once via
  `ADMIT_RATE_PER_SEC`. Per-item rates need a reason to talk to
  Postgres, which this phase doesn't have.
- Inventory is seeded manually via `POST /items/{id}/inventory`
  instead of from Postgres. Phase 9.
- No graceful "still waiting, retry" response for buyers who've been in
  line a long time -- `/join` blocks unbounded (bounded only by the
  client's own timeout). Revisit if it becomes a real problem once load
  testing (Phase 13) exercises this path.

## Testing this phase

```bash
make db-up   # if you want Postgres running too, not required for this phase
docker compose up -d redis   # or however your compose file names it
go test ./...

go build -o bin/waiting-room-api ./cmd/waiting-room-api
go build -o bin/checkout-api ./cmd/checkout-api
ADMIT_RATE_PER_SEC=50 ./bin/waiting-room-api &
./bin/checkout-api &

curl -X POST localhost:8082/items/concert-ticket/inventory -d '{"available": 2}'
curl -X POST localhost:8081/items/concert-ticket/join -d '{"user_id":"alice"}'
curl -X POST localhost:8082/items/concert-ticket/checkout -d '{"user_id":"alice","quantity":1}'
```

Everything above was actually run end to end while building this phase
(FIFO ordering under concurrent joins, oversell prevention under
concurrent checkouts, replay/bypass rejection, and graceful shutdown on
SIGTERM) -- not just written and assumed to work.
