-- reservations is the record of one purchase attempt moving through
-- the async, authoritative path:
--
--   pending   -> checkout-api accepted the attempt and produced the
--                Kafka event; decision-service hasn't consumed it yet.
--   reserved  -> decision-service admitted it, decremented
--                items.available_inventory, and started the TTL clock.
--   completed -> an order was paid against this reservation (terminal).
--   expired   -> expiry-worker reaped it after the TTL elapsed and
--                released the inventory it held (terminal).
--   cancelled -> the user backed out before the TTL elapsed and the
--                inventory was released (terminal).
--   rejected  -> decision-service denied it because the item was sold
--                out; no inventory was ever touched (terminal).
--
-- idempotency_key is the checkout-attempt request ID the client (or
-- checkout-api) generates. Kafka's at-least-once delivery means
-- decision-service can see the same event more than once; the unique
-- constraint below is what makes "insert the reservation" itself the
-- idempotent operation, rather than trusting the consumer to remember
-- what it already processed.
CREATE TABLE reservations (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id          UUID NOT NULL REFERENCES items (id),
    user_id          TEXT NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    status           TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'reserved', 'completed', 'expired', 'cancelled', 'rejected')),
    idempotency_key  TEXT NOT NULL,
    expires_at       TIMESTAMPTZ,
    decided_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT reservations_reserved_requires_expiry
        CHECK (status <> 'reserved' OR expires_at IS NOT NULL)
);

-- One outcome per (item, client-supplied attempt). A redelivered Kafka
-- message hits this constraint and decision-service just reads back
-- the existing row instead of double-reserving.
CREATE UNIQUE INDEX uq_reservations_item_idempotency
    ON reservations (item_id, idempotency_key);

CREATE INDEX idx_reservations_item_id ON reservations (item_id);
CREATE INDEX idx_reservations_user_id ON reservations (user_id);

-- expiry-worker's only query is "give me reserved rows whose TTL has
-- passed" — a partial index keeps that sweep cheap regardless of how
-- many completed/expired/cancelled rows pile up over time.
CREATE INDEX idx_reservations_expiry_sweep
    ON reservations (expires_at)
    WHERE status = 'reserved';

CREATE TRIGGER reservations_set_updated_at
    BEFORE UPDATE ON reservations
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
