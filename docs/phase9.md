# Phase 9 — Redis

Three separate pieces of Redis-adjacent work, tied together only by
living in the same phase of the plan: making Redis's fast-path
inventory counters actually rebuildable from Postgres (the thing
Phase 4 explicitly deferred), exposing the waiting room's queue depth
through Redis, and building the risk-score cache Phase 10's
risk-service will need.

## Inventory reconciliation

Through Phase 8, Redis's fast-path inventory counter was seeded exactly
once, by hand, through a dev-only endpoint (`POST /items/{id}/inventory`
with an arbitrary `{"available": N}` body -- see `docs/phase4.md`).
Nothing kept it in sync with Postgres afterward, and nothing rebuilt it
if it was ever lost. That's a real gap against the Phase 1 design
decision that Redis is supposed to be "fast, disposable, derived state
-- rebuildable from Postgres if lost." Phase 9 closes it.

**`internal/reconcile/reconcile.go`** -- the correctness logic, no
scheduling concept in it at all:

- `Item(ctx, db, redisClient, itemID)` -- reads `available_inventory`
  for one item from Postgres and overwrites Redis's copy
  (`redisx.SeedInventory`) to match, returning the value written.
  Postgres always wins: this never merges the two values, it just makes
  Redis say what Postgres says right now.
- `ActiveItemIDs(ctx, db)` -- every item with status `on_sale` or
  `scheduled`. `draft`/`closed`/`sold_out` items don't need a live
  fast-path counter.
- `All(ctx, db, redisClient)` -- reconciles every active item, returning
  one `Outcome{ItemID, Available, Err}` per item. A single item failing
  doesn't stop the rest -- same "don't let one bad apple block a batch"
  instinct as `expiry-worker`'s scanner (Phase 7), just without that
  package's channel/worker-pool machinery, since this runs occasionally
  over a handful of items rather than against a backlog that can pile
  up under load.

`cmd/checkout-api/main.go` calls `reconcile.All` once, synchronously,
before it starts accepting HTTP traffic -- so the first buyer after a
restart doesn't hit a cold, empty Redis counter and get a false "item
not found or not on sale." A background goroutine then calls it again
every `INVENTORY_RECONCILE_INTERVAL_SECONDS` (default 30s) for as long
as the service runs, correcting whatever drift has accumulated since
the last pass.

This is checkout-api's first Postgres dependency. Through Phase 8 it
only ever talked to Redis and Kafka; it now calls `pgdb.New` at startup
like `decision-service` and `expiry-worker` already do, and fails fast
(same as those two) if Postgres isn't reachable.

The old manual seed endpoint is gone. In its place,
`POST /items/{id}/reconcile` does the same on-demand refresh as the
scheduled loop, but for one item immediately -- useful right after
adjusting an item in Postgres directly, without waiting up to 30
seconds for the next scheduled pass. Unlike the endpoint it replaces,
the only input is *which* item to refresh; the value itself always
comes from Postgres, never from the request body.

### What this doesn't fix

A checkout that decrements Redis but is later rejected by
decision-service -- because by the time its Kafka message was actually
processed, Postgres was already sold out -- leaves Redis and Postgres
briefly disagreeing about how much stock is left. Reconciliation
doesn't erase that window; Kafka delivery and decision-service's own
processing time mean Postgres itself can lag slightly behind what
checkout-api's fast path has already provisionally decremented in
Redis. `docs/phase6.md` flagged this as an open question ("what a
false-positive fast-path admission means for the buyer") for this phase
to address. The honest answer: it doesn't change, because it doesn't
need to. Checkout-api's response was already "pending_confirmation",
never "confirmed" -- a later rejection from decision-service is the
same outcome the system has always been designed to produce for an
oversold attempt, fast-path or not. What reconciliation actually buys
is bounding how long *persistent* drift (a bug, a crash, Redis losing
its data entirely) can survive, rather than letting it accumulate
forever. It is not, and was never meant to be, a fix for the inherent
lag between an optimistic synchronous path and an asynchronous
authoritative one.

## Waiting-room queue state

The in-memory `admission.Admitter` (Phase 2) has always known how many
buyers are waiting -- `len(queue)` inside its single owning goroutine
-- but nothing outside that goroutine could ask. `Depth(ctx)` adds a
non-blocking way to ask: a `depthRequest` sent over a new channel,
answered the same way `Join`'s position is, by the loop itself, so it's
always consistent with what `Join` and the admit ticker actually see.

`waiting-room-api`'s new `GET /items/{id}/queue` calls `Depth`, then
publishes the result into Redis via `redisx.SetQueueDepth` before
responding. The Admitter is still the actual source of truth for who's
waiting and in what order; Redis just caches a number read from it --
the same authoritative-store/disposable-cache shape the Phase 1 design
decision uses for Postgres/Redis inventory, with the Admitter playing
Postgres's role here.

Known simplifications, left as-is rather than solved in this phase:

- If `waiting-room-api` restarts, the in-memory queue resets to zero --
  buyers who were waiting have to rejoin. That's not new (the Admitter
  has always worked this way); it's just now visible through Redis's
  queue-depth key too, which will read 0 until the next `GET .../queue`
  call after restart.
- Running more than one `waiting-room-api` instance per item still
  isn't supported: each instance owns its own independent `Admitter`
  (`internal/admission/registry.go`), so two instances behind a load
  balancer would run two separate lines for the same item, not one.
  Moving the line itself into Redis (rather than just caching its
  depth there) is the kind of change that would fix that -- explicitly
  out of scope here, since it would mean giving up the "one goroutine
  owns the queue, no locks" design Phase 2 deliberately chose.

## Risk-score cache

`redisx/risk.go` adds `SetRiskScore`/`GetRiskScore`, keyed per
item+user (`risk:{itemID}:{userID}`), with a required TTL on writes.
Per the Phase 1 design decision -- "the risk model runs asynchronously
and writes to Redis... never called synchronously in the checkout hot
path" -- this is write-only from the scorer's side. Nothing in
checkout-api's request path calls `GetRiskScore` yet, and nothing
should until there's an actual consumer of the score; a missing or
expired entry is treated as "unknown," never as an error, precisely so
that a cache miss can never accidentally block a checkout.

Nothing populates this cache yet either -- that's `risk-service`,
Phase 10. This phase only builds the cache and locks in the key format
the two services need to agree on.

## Testing

Everything in this phase was tested against real Postgres and Redis,
not mocks:

- `internal/reconcile` -- seeding from a fresh Postgres row,
  Postgres-wins-over-drift, unknown item, status filtering, and a full
  batch across multiple active items.
- `internal/redisx` (`queue.go`, `risk.go`) -- set/get round-trips, the
  zero-by-default behavior for an item nobody's queued for, TTL
  expiry for risk scores, and per-item/per-user isolation.
- `internal/admission` -- `Depth` reflects buyers actually waiting
  (verified by joining several buyers behind a slow admit rate and
  polling until the count catches up), and returns an error rather than
  hanging if the Admitter has already shut down.
- `cmd/checkout-api` -- the new `/reconcile` endpoint end to end,
  including overwriting a deliberately-wrong cached Redis value.
- `cmd/waiting-room-api` -- the new `/queue` endpoint, including that it
  actually writes what it reports into Redis.

Not tested here, same gap as Phase 8: this project's `checkout-api`
Kafka-publish tests still need a real broker, which isn't available in
every environment this gets built in. That's an existing limitation,
not something Phase 9 introduces or was meant to fix.
