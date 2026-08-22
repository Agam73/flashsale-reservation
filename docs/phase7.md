# Phase 7 — Worker architecture

`expiry-worker` as a proper worker-pool pattern: one scanner goroutine
finds expired reservations, a fixed pool of worker goroutines releases
them concurrently. This is the compensating-transaction half of the
Phase 1 TTL decision -- Phase 6 built the "hold inventory" side
(`decision.ProcessAttempt`), this phase builds the "give it back if
nobody paid" side.

## Why a worker pool, and not a queue or a Kafka consumer

Every other concurrent thing in this project so far has an ordering
requirement: Phase 2's `Admitter` is a single serialized FIFO queue
(buyer order matters). Phase 6's Kafka consumer processes one
partition's messages in order (a given item's attempts must be decided
in sequence). Expiring reservations has no such requirement --
reservation A expiring before or after reservation B makes no
difference to anyone. That's exactly the shape a worker pool fits:
a fixed number of goroutines pulling arbitrary units of work off a
shared channel, with no ordering guarantee needed or wanted.

## The two pieces

**`internal/expiry/processor.go`** -- the correctness logic, no worker
concept in it at all:

- `FindExpiredReservations(ctx, db, limit)` -- one query, using the
  partial index built in Phase 3
  (`idx_reservations_expiry_sweep ... WHERE status = 'reserved'`), so
  this stays cheap no matter how many `completed`/`expired`/`cancelled`
  rows pile up over time.
- `ExpireReservation(ctx, db, reservationID)` -- one transaction:
  `UPDATE reservations SET status = 'expired' WHERE id = $1 AND status
  = 'reserved' AND expires_at < now() RETURNING item_id, quantity`,
  then `UPDATE items SET available_inventory = available_inventory +
  quantity`. Same idempotency trick as `decision.ProcessAttempt`, just
  via a conditional `UPDATE` instead of `ON CONFLICT DO NOTHING`:
  whichever caller's `UPDATE` actually matches a row is the one that
  releases inventory. Everyone else -- another worker that got there
  first, a reservation that got paid in the meantime, a stale scan
  result for something not actually expired yet -- gets zero rows
  affected, `Released: false`, no error, no double-release.

  `TestExpireReservationNeverDoubleReleasesUnderConcurrency` is this
  phase's version of the oversell test: 50 goroutines racing to expire
  the *same* reservation ID, exactly 1 succeeds, inventory goes back
  exactly once.

**`cmd/expiry-worker/main.go`** -- the worker-pool wiring:

```
runScanner (1 goroutine, ticker-driven)
  -> finds up to batchSize expired reservation IDs
  -> pushes each onto `jobs` (buffered channel, backpressure if full)
  -> closes `jobs` once ctx is cancelled (sole producer, sole closer)

worker x N (goroutine pool)
  -> range over jobs until the channel closes
  -> calls ExpireReservation per ID, logs the outcome
```

`jobs` is sized to one full scan batch (`EXPIRY_SCAN_BATCH_SIZE`), so a
single batch can be handed off without the scanner blocking, but a
second batch's send blocks naturally until the pool has made room --
backpressure instead of an unbounded queue.

## Graceful shutdown

`SIGTERM`/`SIGINT` cancels the shared context. The scanner's `select`
is watching `ctx.Done()` both while waiting for the next tick and while
pushing individual IDs onto `jobs`, so it stops promptly either way,
then closes `jobs`. Workers exit naturally when `range jobs` ends on
the closed channel. `main` blocks on a `sync.WaitGroup` until every
worker has actually returned before shutting down the health server --
verified directly: `SIGTERM` while a scan cycle had just completed
logged `scanner stopped, waiting for in-flight jobs to finish...` then
`stopped`, with the process actually exiting, not hanging.

## Verified end to end, not just unit-tested

Beyond the 8 `internal/expiry` tests (including the concurrency one),
I seeded a realistic mix directly into Postgres and ran the actual
compiled binary against it:

- 3 genuinely expired `reserved` reservations (quantities 1, 2, 1)
- 1 `reserved` reservation that hasn't expired yet
- 1 `completed` reservation whose `expires_at` is also in the past (the
  case that would be wrong to touch)

With `EXPIRY_WORKER_POOL_SIZE=3`, all three expired reservations got
picked up by three *different* worker goroutines in the same scan
cycle (`worker[1]`, `worker[2]`, `worker[3]` each logged a different
reservation) -- real evidence of the pool actually parallelizing work,
not just a single goroutine pretending. Inventory went from 10 to 14
(`10 + 1 + 2 + 1`), and the not-yet-expired and already-completed rows
were confirmed untouched by querying the table directly afterward.

## Config

| var | default | meaning |
|---|---|---|
| `EXPIRY_WORKER_PORT` | `8084` | health endpoint |
| `EXPIRY_SCAN_INTERVAL_SECONDS` | `5` | how often the scanner ticks |
| `EXPIRY_SCAN_BATCH_SIZE` | `100` | max reservations pulled per scan |
| `EXPIRY_WORKER_POOL_SIZE` | `4` | number of worker goroutines |

## Known gaps (intentional)

- **No metrics on pool utilization or scan lag.** You can't currently
  see "how far behind is the scanner" or "how full is the jobs
  channel" without reading logs. Phase 11 (observability) is where
  this gets real instrumentation.
- **A worker error just logs and moves to the next job** -- there's no
  retry for a reservation whose `ExpireReservation` call failed (e.g. a
  transient DB blip mid-transaction). It'll simply get picked up again
  on the *next* scan tick, since it's still sitting in `reserved` with
  a past `expires_at` -- which is a natural, if slow (bounded by
  `EXPIRY_SCAN_INTERVAL_SECONDS`), form of retry, but not an immediate
  one the way decision-service's backoff loop is in Phase 6.
