-- orders is created once a reservation is paid for. reservation_id is
-- UNIQUE: a reservation converts into at most one order, ever. Inventory
-- was already decremented when the reservation became 'reserved', so
-- creating an order never touches items.available_inventory itself.
--
-- idempotency_key here covers the payment step separately from the
-- reservation step (e.g. a payment webhook firing twice) — same
-- "let the unique constraint make retries safe" pattern as reservations.
CREATE TABLE orders (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id   UUID NOT NULL UNIQUE REFERENCES reservations (id),
    item_id          UUID NOT NULL REFERENCES items (id),
    user_id          TEXT NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    amount_cents     BIGINT NOT NULL CHECK (amount_cents >= 0),
    status           TEXT NOT NULL DEFAULT 'pending_payment'
                          CHECK (status IN ('pending_payment', 'paid', 'failed', 'refunded', 'cancelled')),
    idempotency_key  TEXT NOT NULL UNIQUE,
    paid_at          TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT orders_paid_requires_paid_at
        CHECK (status <> 'paid' OR paid_at IS NOT NULL)
);

CREATE INDEX idx_orders_item_id ON orders (item_id);
CREATE INDEX idx_orders_user_id ON orders (user_id);

CREATE TRIGGER orders_set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
