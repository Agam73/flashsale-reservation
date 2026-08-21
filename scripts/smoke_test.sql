-- Exercises the invariants Phase 3 exists to enforce. Run after
-- `make migrate-up`:
--   make psql < scripts/smoke_test.sql
-- Every step prints what it's checking; a RAISE EXCEPTION means the
-- schema didn't hold up its end.

BEGIN;

-- 1. Create an item with 2 units of inventory.
INSERT INTO items (id, name, price_cents, total_inventory, available_inventory, status)
VALUES ('00000000-0000-0000-0000-000000000001', 'Test Concert Ticket', 5000, 2, 2, 'on_sale');

-- 2. Happy path: a reservation gets admitted and holds one unit.
INSERT INTO reservations (id, item_id, user_id, quantity, status, idempotency_key, expires_at)
VALUES (
    '00000000-0000-0000-0000-0000000000a1',
    '00000000-0000-0000-0000-000000000001',
    'user-alice', 1, 'reserved', 'attempt-alice-1', now() + interval '2 minutes'
);
UPDATE items SET available_inventory = available_inventory - 1
WHERE id = '00000000-0000-0000-0000-000000000001';

DO $$
BEGIN
    IF (SELECT available_inventory FROM items WHERE id = '00000000-0000-0000-0000-000000000001') <> 1 THEN
        RAISE EXCEPTION 'FAIL: expected 1 unit left after first reservation';
    END IF;
    RAISE NOTICE 'PASS: reservation decremented available_inventory to 1';
END $$;

-- 3. Kafka redelivers the SAME checkout attempt (at-least-once). The
--    idempotency constraint must stop decision-service from double-booking
--    even if it blindly retries the insert.
DO $$
BEGIN
    BEGIN
        INSERT INTO reservations (item_id, user_id, quantity, status, idempotency_key, expires_at)
        VALUES ('00000000-0000-0000-0000-000000000001', 'user-alice', 1, 'reserved', 'attempt-alice-1', now() + interval '2 minutes');
        RAISE EXCEPTION 'FAIL: duplicate idempotency_key for same item was allowed to insert twice';
    EXCEPTION WHEN unique_violation THEN
        RAISE NOTICE 'PASS: duplicate (item_id, idempotency_key) rejected as expected';
    END;
END $$;

-- 4. A 'reserved' row without an expiry is a bug waiting to strand
--    inventory forever — the CHECK constraint should refuse it.
DO $$
BEGIN
    BEGIN
        INSERT INTO reservations (item_id, user_id, quantity, status, idempotency_key, expires_at)
        VALUES ('00000000-0000-0000-0000-000000000001', 'user-bob', 1, 'reserved', 'attempt-bob-1', NULL);
        RAISE EXCEPTION 'FAIL: reserved row with no expires_at was allowed';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: reserved-without-expiry rejected as expected';
    END;
END $$;

-- 5. Second buyer takes the last unit.
INSERT INTO reservations (id, item_id, user_id, quantity, status, idempotency_key, expires_at)
VALUES (
    '00000000-0000-0000-0000-0000000000b1',
    '00000000-0000-0000-0000-000000000001',
    'user-bob', 1, 'reserved', 'attempt-bob-2', now() + interval '2 minutes'
);
UPDATE items SET available_inventory = available_inventory - 1
WHERE id = '00000000-0000-0000-0000-000000000001';

-- 6. A third buyer is rejected by decision-service — no inventory
--    column is touched for a 'rejected' row.
INSERT INTO reservations (item_id, user_id, quantity, status, idempotency_key)
VALUES ('00000000-0000-0000-0000-000000000001', 'user-carol', 1, 'rejected', 'attempt-carol-1');

DO $$
BEGIN
    IF (SELECT available_inventory FROM items WHERE id = '00000000-0000-0000-0000-000000000001') <> 0 THEN
        RAISE EXCEPTION 'FAIL: expected 0 units left after second reservation';
    END IF;
    RAISE NOTICE 'PASS: available_inventory floors at 0, oversell blocked at the app layer next (CHECK backstops >= 0)';
END $$;

-- 7. Oversell attempt: try to push available_inventory negative
--    directly. This is the last line of defense if application logic
--    ever has a bug.
DO $$
BEGIN
    BEGIN
        UPDATE items SET available_inventory = available_inventory - 1
        WHERE id = '00000000-0000-0000-0000-000000000001';
        RAISE EXCEPTION 'FAIL: available_inventory was allowed to go negative';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: CHECK (available_inventory >= 0) blocked the oversell';
    END;
END $$;

-- 8. Alice pays. Reservation converts to a completed order; the
--    one-order-per-reservation UNIQUE constraint should hold.
INSERT INTO orders (reservation_id, item_id, user_id, quantity, amount_cents, status, idempotency_key, paid_at)
VALUES ('00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-000000000001', 'user-alice', 1, 5000, 'paid', 'payment-alice-1', now());
UPDATE reservations SET status = 'completed' WHERE id = '00000000-0000-0000-0000-0000000000a1';

DO $$
BEGIN
    BEGIN
        INSERT INTO orders (reservation_id, item_id, user_id, quantity, amount_cents, status, idempotency_key, paid_at)
        VALUES ('00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-000000000001', 'user-alice', 1, 5000, 'paid', 'payment-alice-2', now());
        RAISE EXCEPTION 'FAIL: a second order against the same reservation was allowed';
    EXCEPTION WHEN unique_violation THEN
        RAISE NOTICE 'PASS: reservation_id UNIQUE blocked a second order on the same reservation';
    END;
END $$;

ROLLBACK; -- leave the database clean; everything above only proves the schema works

-- 9. updated_at trigger actually fires. now() is fixed for the
--    lifetime of a transaction in Postgres, so this has to run as two
--    separate (autocommit) statements rather than inside steps 1-8's
--    transaction, or the "before" and "after" timestamps would be
--    identical by construction, not because the trigger failed.
INSERT INTO items (id, name, price_cents, total_inventory, available_inventory, status)
VALUES ('00000000-0000-0000-0000-000000000002', 'Trigger Test Item', 100, 1, 1, 'draft');

SELECT pg_sleep(0.05);

UPDATE items SET status = 'on_sale' WHERE id = '00000000-0000-0000-0000-000000000002';

DO $$
DECLARE
    c_ts TIMESTAMPTZ;
    u_ts TIMESTAMPTZ;
BEGIN
    SELECT created_at, updated_at INTO c_ts, u_ts FROM items WHERE id = '00000000-0000-0000-0000-000000000002';
    IF u_ts <= c_ts THEN
        RAISE EXCEPTION 'FAIL: updated_at trigger did not advance on UPDATE';
    END IF;
    RAISE NOTICE 'PASS: updated_at trigger fired on UPDATE (updated_at % > created_at %)', u_ts, c_ts;
END $$;

DELETE FROM items WHERE id = '00000000-0000-0000-0000-000000000002'; -- clean up

SELECT 'smoke test finished — see NOTICEs above for pass/fail' AS result;
