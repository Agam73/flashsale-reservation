# Phase 6 — Producers/consumers

Real Kafka producer in `checkout-api` (partitioned by item ID). Real
consumer in `decision-service`. This is where overselling gets
prevented for real -- everything before this point (the waiting room,
the Redis fast path) was optimistic and provisional.

## The flow, end to end

```
buyer -> waiting-room-api -> [Redis token] -> checkout-api
                                                   |
                                    Redis fast-path check (Phase 4,
                                    unchanged) -- optimistic, fast,
                                    disposable
                                                   |
                                    publish PurchaseAttempted to Kafka
                                    (key = item_id) <---- NEW
                                                   |
                                          [ Kafka: checkout-attempts ]
                                                   |
                                    decision-service consumes, one
                                    partition (= one item) at a time
                                                   |
                                    internal/decision.ProcessAttempt:
                                    SELECT ... FOR UPDATE on the item,
                                    INSERT the reservation, decrement
                                    Postgres inventory -- authoritative,
                                    durable, exactly-once effect despite
                                    at-least-once delivery
```

## Where the actual correctness logic lives

`internal/decision/processor.go` -- and nowhere else. `ProcessAttempt`
has no Kafka dependency at all; it just takes an `Attempt` and a
`*sql.DB`. That's deliberate: it means the "does this prevent
overselling" question can be answered by testing this one function
directly against Postgres, with no broker anywhere in the loop.

The core moves, all inside one transaction:

1. `SELECT available_inventory FROM items WHERE id = $1 FOR UPDATE` --
   locks the item's row. Kafka's partition-by-item-ID design already
   means one consumer processes a given item's attempts at a time; this
   lock is the backstop that holds even if that invariant is ever
   violated (e.g. two decision-service replicas briefly overlapping
   during a rebalance).
2. Decide `reserved` or `rejected` in Go, based on the locked row.
3. `INSERT INTO reservations (...) ON CONFLICT (item_id,
   idempotency_key) DO NOTHING RETURNING id, status`. If Kafka
   redelivers the same message, this insert is a no-op and the
   already-recorded outcome is read back and returned -- same input,
   same answer, every time.
4. Only decrement `items.available_inventory` when a genuinely new row
   was inserted with status `reserved`. A replayed delivery never
   double-decrements.

`internal/decision/processor_test.go` proves this against a real
Postgres instance, including the test that actually matters:
`TestProcessAttemptNeverOversells` fires 50 concurrent attempts at 10
units of stock and asserts exactly 10 succeed, 0 get oversold. That's
not a mocked assertion -- it's 50 real goroutines racing against a real
transactional row lock.

## checkout-api's new step

After the existing Redis fast-path decrement succeeds (unchanged from
Phase 4), `checkout-api` now:

1. Generates a fresh `idempotency_key` per attempt
   (`kafkax.NewIdempotencyKey`, `crypto/rand`-backed, no new dependency
   needed for it).
2. Publishes a `PurchaseAttempted` event keyed by `item_id`
   (`kafkax.PublishAttempt`).
3. Returns `"status": "pending_confirmation"` instead of Phase 4's
   `"fast_path_admitted"` -- the fast path passed, but the durable
   decision is now decided asynchronously by decision-service. There's
   no status-check endpoint yet, so the response says so honestly
   rather than implying a confirmed reservation exists.

**If the Kafka publish itself fails** (broker unreachable, etc.), the
Redis decrement already happened -- left alone, that inventory would be
silently stranded, held against an attempt nobody will ever
authoritatively decide. So a failed publish triggers
`releaseAndRegrant`: the Redis inventory is given back, and the buyer's
admission token is re-granted (same TTL) so they can retry the
checkout call without rejoining the waiting room queue.

## Why the Balancer setting matters

`kafka-go`'s `Writer` defaults to a `LeastBytes` balancer, which
ignores the message key entirely and just spreads messages across
partitions by size. `internal/kafkax.NewWriter` explicitly sets
`Balancer: &kafka.Hash{}` -- without this one line, every
`PurchaseAttempted` event would scatter across partitions regardless of
item ID, and decision-service's one-consumer-per-item guarantee would
silently stop holding. This is the single most important line in the
producer code and the easiest one to get wrong by omission.

## Required setup: create the topic yourself first

Kafka can auto-create a topic on first publish, but it'll use the
broker's default partition count -- almost certainly not what you want
for item-ID partitioning to mean anything. Create it explicitly before
running either service, the same way migrations are applied explicitly
before running anything that depends on the schema:

```bash
docker exec -it flashsale-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic checkout-attempts --partitions 3 --replication-factor 1
```

If you already have a `checkout-attempts` topic left over from Phase
5's manual exploration, delete and recreate it first so there's no
leftover test data or an accidental wrong partition count from an
earlier auto-create:

```bash
docker exec -it flashsale-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic checkout-attempts
```

## decision-service's consume loop

`cmd/decision-service/main.go` fetches one message at a time and
retries that *same* message against Postgres (with exponential backoff,
capped at 30s) until it succeeds -- it does not call `FetchMessage`
again to "retry", since that would advance to the next message
regardless of whether the current one was committed. Only two things
end a message's retry loop:

- **Success** -- offset committed, move on.
- **`decision.ErrItemNotFound`** -- treated as permanent (retrying
  won't make a nonexistent item exist), logged loudly, offset
  committed anyway rather than blocking the partition forever.

Everything else (a Postgres connection blip, for example) retries
forever with backoff. `Ctrl+C` / `SIGTERM` cancels the context, which
`FetchMessage` and the backoff `select` both respect, so shutdown is
still clean.

## Known gaps (intentional, later phases close them)

- **No dead-letter topic.** A message that fails for a reason other
  than `ErrItemNotFound` -- some other genuinely permanent error --
  retries forever and blocks the rest of its partition. Phase 8 is
  where retries/DLQ get designed properly; this phase's retry loop is
  honest about not solving that yet.
- **No status-check endpoint.** A buyer who gets
  `"pending_confirmation"` from checkout-api has no way to later ask
  "did I actually get a reservation?" Natural next addition, not built
  here to keep this phase's scope to producer + consumer + Postgres
  write.
- **checkout-api's Redis fast path is unchanged from Phase 4** and can
  still admit a purchase that decision-service later rejects (Redis and
  Postgres inventory can drift apart under enough concurrent load,
  since only the Postgres path has the row lock). This is expected and
  matches the Phase 1 design: Redis is optimistic, Postgres is
  authoritative. Reconciling the two -- and what a false-positive
  fast-path admission means for the buyer -- is Phase 9's job.

## Testing this phase

I could not run a real Kafka broker in the sandbox I built this in (no
Docker, no reachable Apache mirrors) -- so the two things verified
directly, end to end, against real infrastructure are:

- **`internal/decision`** against a real Postgres instance, all 5
  tests, including the concurrency test.
- **`internal/kafkax`**'s pure logic (ID generation, JSON round-trip,
  object construction) with no broker involved.
- The whole module builds and vets clean (`go build ./...`, `go vet
  ./...`).

The actual Kafka wire-level behavior -- does checkout-api's publish
really land on the partition you'd expect, does decision-service's
consumer group really split partitions the way Phase 5 demonstrated,
does an end-to-end buyer flow actually produce a `reservations` row --
needs to be run on your machine, where Kafka is already up. See the
runbook for the exact commands.
