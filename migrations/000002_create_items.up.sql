-- items is the thing being sold (a ticket type, a drop, etc).
--
-- available_inventory is the AUTHORITATIVE count. Postgres is the
-- source of truth per the Phase 1 design decision; Redis (Phase 9) only
-- ever holds a fast, disposable copy that can be rebuilt from this row.
-- decision-service is the only writer that ever decrements it, and it
-- does so inside the same transaction that flips a reservation to
-- 'reserved' (see 000003), using SELECT ... FOR UPDATE on this row to
-- serialize concurrent decrements even if the item-partitioned
-- consumer guarantee is ever violated (extra belt, extra suspenders).
CREATE TABLE items (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT NOT NULL,
    description          TEXT NOT NULL DEFAULT '',
    price_cents          BIGINT NOT NULL CHECK (price_cents >= 0),
    total_inventory      INTEGER NOT NULL CHECK (total_inventory >= 0),
    available_inventory  INTEGER NOT NULL CHECK (available_inventory >= 0),
    status               TEXT NOT NULL DEFAULT 'draft'
                             CHECK (status IN ('draft', 'scheduled', 'on_sale', 'sold_out', 'closed')),
    sale_starts_at       TIMESTAMPTZ,
    sale_ends_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT items_available_within_total
        CHECK (available_inventory <= total_inventory),
    CONSTRAINT items_sale_window_valid
        CHECK (sale_ends_at IS NULL OR sale_starts_at IS NULL OR sale_ends_at > sale_starts_at)
);

CREATE INDEX idx_items_status ON items (status);

CREATE TRIGGER items_set_updated_at
    BEFORE UPDATE ON items
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
