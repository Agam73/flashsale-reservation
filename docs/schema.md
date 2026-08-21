# Phase 3 — data model

Three tables: `items`, `reservations`, `orders`. This doc explains the
*why* behind each column, tied back to the decisions locked in during
Phase 1.

## items

The thing being sold. `available_inventory` is the number that
actually matters, and it's **authoritative in Postgres** — per the
Phase 1 decision, Redis (Phase 9) only ever holds a fast, disposable
copy of this value that can be rebuilt from this row if it's lost.

```
available_inventory <= total_inventory   (CHECK)
available_inventory >= 0                 (CHECK)
```

The `>= 0` check is the last line of defense against overselling. The
real prevention mechanism is upstream — one consumer instance owns each
item's decision stream (partition key = item ID, Phase 6) — but the
constraint means a bug in that logic fails loudly with a rejected
transaction instead of silently going negative.

`status` tracks the *sale's* lifecycle (`draft` → `scheduled` →
`on_sale` → `sold_out`/`closed`), separate from any individual
reservation's status.

## reservations

One row per purchase attempt that made it through the waiting room and
into the authoritative path. This is the table that encodes the
at-least-once + idempotent-consumer decision from Phase 1:

| status | meaning |
|---|---|
| `pending` | checkout-api produced the Kafka event; decision-service hasn't consumed it yet |
| `reserved` | decision-service admitted it, decremented `items.available_inventory`, TTL clock started |
| `completed` | an order was paid against it (terminal) |
| `expired` | TTL elapsed before payment; `expiry-worker` released the inventory (terminal) |
| `cancelled` | user backed out before the TTL; inventory released (terminal) |
| `rejected` | decision-service denied it — item was already sold out; inventory untouched (terminal) |

**Idempotency.** `idempotency_key` is the checkout-attempt ID the
client (or checkout-api) generates before the event ever touches
Kafka. `UNIQUE (item_id, idempotency_key)` is what makes "insert the
reservation" the idempotent operation itself — decision-service can
reprocess a redelivered message and just hit the constraint instead of
needing its own dedup table. This is the "idempotent by design, not
retrofitted" requirement from Phase 1, expressed as a schema
constraint rather than application logic that's easy to forget.

**TTL / compensating transaction.** `expires_at` is set the moment a
reservation becomes `reserved`. `CHECK (status <> 'reserved' OR
expires_at IS NOT NULL)` makes it structurally impossible to hold
inventory without a TTL attached — you can't accidentally lock stock
indefinitely, which is the whole point of the reservation-TTL decision
from Phase 1. The partial index

```sql
CREATE INDEX idx_reservations_expiry_sweep
    ON reservations (expires_at)
    WHERE status = 'reserved';
```

exists because `expiry-worker` (Phase 7) has exactly one query —
"give me `reserved` rows whose TTL passed" — and this index keeps that
query cheap forever, regardless of how many `completed`/`expired`
rows accumulate.

## orders

Created once a reservation is paid. `reservation_id UNIQUE` encodes
"a reservation converts into at most one order" directly in the
schema. Inventory was already decremented when the reservation became
`reserved`, so an order never touches `items.available_inventory` —
that column has exactly one writer path (reservation admission /
release), which keeps the oversell-prevention story simple to reason
about.

`idempotency_key` here is separate from the reservation's — it covers
retried payment confirmations (e.g. a webhook firing twice), the same
"let a UNIQUE constraint make retries safe" pattern.

## What's deliberately *not* here yet

- No `users` table — `user_id` is a bare `TEXT` for now. Auth/identity
  isn't in scope for this project.
- No dedicated Kafka-offset/dedup table. The idempotency keys on
  `reservations` and `orders` cover Phase 3–6's needs; Phase 8
  (reliability) is where dead-letter handling gets designed, and it
  may add something here if application-level dedup isn't enough.
- `available_inventory` is a plain column, not a version/optimistic-lock
  counter. Because exactly one consumer instance ever decides a given
  item's outcome at a time (item-ID partition key), a simple
  `SELECT ... FOR UPDATE` inside decision-service's transaction is
  enough to serialize writes — no optimistic-concurrency dance needed
  at the schema level.

## Verifying it

`scripts/smoke_test.sql` inserts sample rows inside a transaction it
rolls back, and checks:

1. A reservation decrements `available_inventory`.
2. A redelivered event (same `item_id` + `idempotency_key`) is rejected
   by the unique index, not double-reserved.
3. A `reserved` row without `expires_at` is rejected by the CHECK.
4. Selling out two units brings `available_inventory` to exactly 0.
5. A direct attempt to push `available_inventory` negative is rejected.
6. A second order against the same `reservation_id` is rejected.
7. The `updated_at` trigger actually advances the timestamp on `UPDATE`.

Run it with `make smoke-test` after `make migrate-up`.
