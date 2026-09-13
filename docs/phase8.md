# Phase 8 — Reliability & Failure Handling

Status: **Implemented and verified against live infra.** Chaos-testing
scenarios (kill mid-batch, stop Postgres, duplicate event) are written
up but not yet individually executed — see "What's left" below.

## What this phase adds

`decision-service` now classifies every processing error into one of
four outcomes before deciding what to do, instead of treating all
failures identically:

| Error | Outcome |
|---|---|
| `nil` | Commit — success, or a safely-replayed redelivery |
| `decision.ErrItemNotFound` / `ErrInvalidQuantity` | Dead-letter immediately, no retry budget spent — these are data problems, not infrastructure ones |
| Anything that isn't a `*pq.Error` (`isConnectivityError`) | Retry forever, **uncounted** against the retry budget |
| Anything else (a real `*pq.Error`, e.g. a serialization failure) | Governed by `retry.Policy.ShouldRetry` as before |

**Why this matters:** without it, an extended Postgres outage would
dead-letter every in-flight message identically — none of which are
actually bad, they're just unlucky about when they arrived.

## What's new

- `internal/retry` — a `Policy` type (`MaxAttempts`, `MaxElapsed`,
  exponential backoff), fully unit-tested.
- `internal/kafkax/dlq.go` — `DeadLetter` struct, `NewDLQWriter`,
  `PublishDeadLetter`, tested.
- `classifyOutcome` / `isConnectivityError` in
  `cmd/decision-service/main.go` — the actual new decision logic this
  phase adds.
- `checkout-attempts-dlq` Kafka topic (1 partition — an audit log, not
  a work queue, so no per-item ordering to preserve).

## Idempotency: already done, not touched this phase

Per the Phase 1 decision ("at-least-once delivery, all consumers
idempotent by design"), this was built in Phase 6/7:

- `decision.ProcessAttempt` — `INSERT ... ON CONFLICT (item_id,
  idempotency_key) DO NOTHING`, tested under 50-goroutine concurrent
  redelivery.
- `expiry.ExpireReservation` — conditional `UPDATE ... WHERE status =
  'reserved'`, tested the same way.

## A real deadlock found and fixed along the way

`ProcessAttempt` locked the item row with `SELECT ... FOR UPDATE`.
Separately, every `INSERT INTO reservations` implicitly takes a
`FOR KEY SHARE` lock on that same `items` row via the foreign key
check. `FOR UPDATE` conflicts with everything, including
`FOR KEY SHARE`; `FOR NO KEY UPDATE` does not, and is semantically
correct here since `ProcessAttempt` never modifies the row's key.
Fixed in `internal/decision/processor.go`.

Full writeup, including how this was found (concurrency tests failing
with real `pq: deadlock detected` errors once run with enough CPU
parallelism): [`docs/phase8.md`](phase8.md).

## What's verified

- `go build ./...`, `go vet ./...` — clean.
- All pure-logic unit tests (`retry`, `kafkax`, `classifyOutcome`,
  `isConnectivityError`) — pass with no infra needed.
- Full integration suite against live Postgres/Redis/Kafka — pass
  (run with `go test ./... -p 1`; see note below).
- Migrations applied, both Kafka topics present with correct
  partition counts.
- `decision-service` run live against the real stack: picked up a bad
  message (invalid UUID), retried 5 times with exponential backoff
  over ~31s per the retry policy, then dead-lettered it correctly
  instead of crashing or blocking the partition.

**Note on `-p 1`:** `internal/decision`'s 50-goroutine row-locking test
and `internal/expiry`'s `TRUNCATE` calls will corrupt each other's
fixtures if Go runs those packages' tests concurrently against the
same live Postgres. Serializing package execution (`-p 1`) avoids
this; it isn't a bug in the reliability logic itself.

## What's left

- **Chaos testing**, run individually and confirmed:
  1. Kill `decision-service` mid-batch (`kill -9`), confirm no
     double-reservation on restart.
  2. Stop Postgres while messages are flowing, confirm retries never
     dead-letter and processing resumes cleanly when it's back.
  3. Replay a duplicate event, confirm no double-reservation and no
     second inventory decrement.

  The live run above exercised the retry → dead-letter path with a
  real bad message, which is adjacent to but not the same as any of
  these three scenarios — they still need to be run individually.

- **Known, intentional gaps** (see `docs/phase8.md` for reasoning):
  retry/attempt counters aren't durable across a decision-service
  crash; `expiry-worker` has no equivalent error classification (its
  natural retry-on-next-scan-tick doesn't need one); no metrics on DLQ
  depth or retry rate yet (Phase 11).
