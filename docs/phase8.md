# Phase 8 — Reliability & failure handling

Idempotency, retries, dead-letter topic. Then deliberately break things
(kill a consumer mid-batch, stop Postgres, duplicate an event) and
confirm the system recovers the way it's supposed to.

## A note on how this phase came together

Before writing anything, I found this repo already had real work in it
I hadn't built: `internal/retry` (a `Policy` type -- `MaxAttempts`,
`MaxElapsed`, backoff calculation -- fully tested) and
`internal/kafkax/dlq.go` (a `DeadLetter` struct and `NewDLQWriter`,
also tested). I'd started writing a parallel, conflicting
implementation before noticing. What's in this phase now is the result
of reconciling the two: the existing `retry.Policy` and `DeadLetter`
stayed as-is, and what I added on top is the one piece that was
missing -- see below.

## Idempotency: already done, verified again

Per the Phase 1 decision ("at-least-once delivery, all consumers
idempotent by design"), this was actually built in Phase 6/7, not this
phase:

- `decision.ProcessAttempt` -- `INSERT ... ON CONFLICT (item_id,
  idempotency_key) DO NOTHING`, tested under 50-goroutine concurrent
  redelivery.
- `expiry.ExpireReservation` -- conditional `UPDATE ... WHERE status =
  'reserved'`, tested the same way.

Phase 8 doesn't change either. Nothing here needed to.

## What was missing: distinguishing *why* something failed

The existing `retry.Policy` treats every processing failure the same
way: count attempts, count elapsed time, give up once either cap is
hit. That's correct for a message that's genuinely bad -- but it has a
real gap for a message that's failing because **Postgres itself is
temporarily unreachable**. During an outage, *every* in-flight message
would fail identically and hit the same cap at the same time, dead-
lettering all of them -- none of which are actually bad, they're just
unlucky about when they arrived.

`cmd/decision-service/main.go` now classifies every `ProcessAttempt`
error into one of four outcomes before deciding what to do
(`classifyOutcome`):

| error | outcome |
|---|---|
| `nil` | commit -- success, or a safely-replayed redelivery |
| `decision.ErrItemNotFound` / `ErrInvalidQuantity` | dead-letter immediately, no retry budget spent -- these are data problems, not infrastructure ones |
| anything that **isn't** a `*pq.Error` (`isConnectivityError`) | retry, **uncounted** against `retry.Policy`'s budget |
| anything else (a real `*pq.Error` -- e.g. a serialization failure) | governed by `retry.Policy.ShouldRetry` as before |

`isConnectivityError` is the actual mechanism: `lib/pq` wraps a real
response *from* Postgres (a constraint violation, a serialization
failure) in `*pq.Error`. A connection refused, a timeout, a dropped
connection -- none of those ever got far enough to receive a Postgres
response, so they surface as a plain `error` instead.
`internal/decision`'s consistent `%w` wrapping means `errors.As` still
finds the `*pq.Error` (or doesn't) despite the extra layers of
`fmt.Errorf` on the way up.

This means an extended Postgres outage now behaves correctly: every
message retries forever with capped backoff, nothing gets
dead-lettered, and normal processing resumes the moment Postgres comes
back. That claim is verified by the unit tests below at the
decision-logic level; the chaos-testing section further down is what
actually proves it against the real stack -- not yet run.

## Dead-lettering, end to end

Uses the existing `kafkax.DeadLetter` / `NewDLQWriter` /
`PublishDeadLetter`, unchanged. Two paths lead here:

1. **A message that can't even be decoded** (`kafkax.DecodeAttempt`
   fails) -- immediate, `AttemptCount: 0`.
2. **A message whose processing was classified as permanent**, either
   immediately (`ErrItemNotFound`/`ErrInvalidQuantity`) or after
   `retry.Policy.ShouldRetry` says to stop.

If the DLQ publish itself fails, the original offset is still
committed -- the alternative (refusing to commit, retrying forever)
would let a single bad message block its entire partition, exactly
what dead-lettering exists to prevent. This is logged loudly rather
than silently swallowed.

`scripts/setup_kafka_topic.sh` now creates both `checkout-attempts`
(3 partitions, unchanged) and `checkout-attempts-dlq` (1 partition --
there's no per-item ordering to preserve in an audit log).

## What's tested where

- `internal/retry` -- unchanged, already had full coverage of
  `ShouldRetry`/`NextBackoff` before this phase.
- `internal/kafkax` -- unchanged, `dlq_test.go` already covered
  `DeadLetter`'s JSON round-trip and `NewDLQWriter`'s topic.
- `cmd/decision-service/main_test.go` -- new. `classifyOutcome` and
  `isConnectivityError` are pure functions (no broker, no database),
  so the actual reliability *decision* is fully unit-tested: nil is a
  commit, the two sentinel errors dead-letter immediately regardless of
  attempt count, a connectivity error stays uncounted even at 100
  attempts and 24 hours elapsed, and an unknown `*pq.Error` is bounded
  correctly by the policy's `MaxAttempts` and `MaxElapsed` (including
  the "elapsed cap fires even with attempts remaining" case).

## Chaos testing: deliberately breaking things

I don't have a live Kafka broker or the ability to run multiple
processes against shared infrastructure in the sandbox I built this
in, so this part has to be run on your machine, where the full stack
is already up. Three scenarios, each proving a specific claim this doc
makes:

### 1. Kill a consumer mid-batch

**Claim:** a crash between "processed" and "committed" never causes a
double-reservation, because redelivery hits the same `ON CONFLICT DO
NOTHING` idempotency that redelivery-under-normal-operation already
relies on.

Run several checkouts to get messages flowing, then `kill -9` (not
`SIGTERM` -- a hard kill, so there's no graceful shutdown to interfere
with the test) `decision-service` mid-processing, restart it, and
confirm: no reservation was double-counted, and inventory matches what
it should be.

### 2. Stop Postgres

**Claim:** decision-service retries forever without dead-lettering
anything while Postgres is down, and resumes normal processing once
it's back -- this is the specific gap `classifyOutcome` closes.

Stop the `flashsale-postgres` container while decision-service is
running and messages are flowing. Watch the logs: every failure should
say `"Postgres unreachable, retrying ... (infra outage, not counted
against retry budget)"`, never `"DEAD-LETTERING"`. Restart Postgres and
confirm processing resumes and the `checkout-attempts-dlq` topic stays
empty.

### 3. Duplicate an event

**Claim:** replaying the exact same message doesn't create a second
reservation or double-decrement inventory.

Manually re-produce a message with the *same* key and value onto
`checkout-attempts` (the Kafka console producer, or a small script) and
confirm `reservations` still has exactly one row for that
`idempotency_key`, and `items.available_inventory` didn't move.

Full step-by-step commands for all three are in the testing runbook --
ask for it and I'll write it out.

## A real deadlock found by actually running this under concurrency

`TestProcessAttemptNeverOversells` -- the test that proves overselling
can't happen -- itself started failing with real `pq: deadlock detected
(40P01)` errors once it ran on a machine with enough CPU cores for the
50 goroutines to genuinely race, rather than my sandbox's more
constrained concurrency. Worth writing up because it's exactly the
kind of thing this phase is about, and the fix matters for production
correctness, not just passing tests.

**The mechanism:** `ProcessAttempt` locked the item row with `SELECT
... FOR UPDATE`. Separately, every `INSERT INTO reservations` -- which
has `item_id REFERENCES items(id)` -- implicitly takes a `FOR KEY
SHARE` lock on that same `items` row, as part of Postgres's own
foreign-key check. Per Postgres's documented lock compatibility
matrix, `FOR UPDATE` conflicts with everything, including `FOR KEY
SHARE`; `FOR NO KEY UPDATE` does not. Since `ProcessAttempt` never
modifies the row's key (only `available_inventory`), `FOR NO KEY
UPDATE` is both semantically correct and avoids this entire class of
conflict, rather than just making it less likely. Fixed in
`internal/decision/processor.go`; `docs/schema.md` updated to match.

The same test run also showed `internal/expiry` tests failing with
unrelated-looking symptoms (a query returning 0 rows instead of 3, a
`TRUNCATE` itself deadlocking). Best working theory, not fully proven:
Go runs different packages' tests concurrently by default, and
`TRUNCATE` takes an `ACCESS EXCLUSIVE` lock that conflicts with
everything -- if `internal/expiry`'s `TRUNCATE` ran while
`internal/decision`'s deadlock storm was actively holding locks on the
same live Postgres instance, that would produce exactly these symptoms
as collateral damage, not a second bug in `expiry.go` itself. Consistent
with this: after the fix, 3 consecutive full-suite runs (`go test
./...`, default parallelism, matching how it's normally run) all
passed clean, `internal/expiry` included.

Verified: 8 consecutive clean runs of the concurrency test alone, plus
3 consecutive clean full-suite runs, after the fix -- versus reliably
reproducing dozens of deadlocks per run before it (on a machine with
enough real parallelism to hit the race).

## Known gaps (intentional)

- **The retry/attempt counters aren't durable across a
  decision-service crash.** If the process crashes mid-retry-loop
  (not yet dead-lettered, not yet committed), the counters are gone
  on restart -- the message just looks like a fresh at-least-once
  redelivery, which the idempotency layer already handles correctly.
  Making the retry budget itself survive a crash (e.g. via message
  headers) is real added complexity for a benefit this project's scale
  doesn't need yet.
- **`expiry-worker` doesn't have an equivalent classification.** It
  doesn't need one the same way: a failed `ExpireReservation` call
  just gets picked up again on the next scan tick, which is already a
  natural (if slower) retry with no ordering or partition-blocking
  concerns to worry about.
- **No metrics on DLQ depth or retry rate.** You can currently only see
  this by reading logs or consuming the DLQ topic directly. Phase 11
  (observability) is where this gets real instrumentation.
