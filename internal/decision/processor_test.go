package decision

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const testTTL = 2 * time.Minute

// testDB connects to the Postgres migrated by this repo's Phase 3
// migrations and truncates the tables this package touches before each
// test, so tests don't interfere with each other.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", "postgres://flashsale:flashsale@localhost:5432/flashsale?sslmode=disable")
	if err != nil {
		t.Skipf("skipping: no local Postgres available: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("skipping: no local Postgres available: %v", err)
	}
	if _, err := db.Exec(`TRUNCATE reservations, orders, items CASCADE`); err != nil {
		t.Fatalf("truncating tables: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// seedItem inserts an item with the given available inventory and
// returns its ID.
func seedItem(t *testing.T, db *sql.DB, available int) string {
	t.Helper()
	var id string
	err := db.QueryRow(`
		INSERT INTO items (name, price_cents, total_inventory, available_inventory, status)
		VALUES ('Test Item', 1000, $1, $1, 'on_sale')
		RETURNING id
	`, available).Scan(&id)
	if err != nil {
		t.Fatalf("seeding item: %v", err)
	}
	return id
}

func TestProcessAttemptReserves(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 5)

	outcome, err := ProcessAttempt(ctx, db, Attempt{
		ItemID: itemID, UserID: "alice", Quantity: 2, IdempotencyKey: "attempt-1",
	}, testTTL)
	if err != nil {
		t.Fatalf("ProcessAttempt: %v", err)
	}
	if outcome.Status != "reserved" {
		t.Errorf("expected status 'reserved', got %q", outcome.Status)
	}
	if outcome.Replayed {
		t.Error("expected Replayed=false for a genuinely new attempt")
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 3 {
		t.Errorf("expected 3 remaining after reserving 2 of 5, got %d", available)
	}
}

func TestProcessAttemptRejectsWhenSoldOut(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 1)

	outcome, err := ProcessAttempt(ctx, db, Attempt{
		ItemID: itemID, UserID: "alice", Quantity: 5, IdempotencyKey: "attempt-1",
	}, testTTL)
	if err != nil {
		t.Fatalf("ProcessAttempt: %v", err)
	}
	if outcome.Status != "rejected" {
		t.Errorf("expected status 'rejected' (requesting 5 of 1 available), got %q", outcome.Status)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 1 {
		t.Errorf("expected inventory untouched at 1 after a rejected attempt, got %d", available)
	}
}

func TestProcessAttemptItemNotFound(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	_, err := ProcessAttempt(ctx, db, Attempt{
		ItemID: "00000000-0000-0000-0000-000000000000", UserID: "alice", Quantity: 1, IdempotencyKey: "attempt-1",
	}, testTTL)
	if !errors.Is(err, ErrItemNotFound) {
		t.Errorf("expected ErrItemNotFound, got %v", err)
	}
}

// TestProcessAttemptIsIdempotentUnderRedelivery is the test for the
// specific guarantee Phase 1 committed to: "at-least-once delivery,
// all consumers idempotent by design." Processing the exact same
// attempt (same idempotency key) twice must decrement inventory
// exactly once and return the same outcome both times.
func TestProcessAttemptIsIdempotentUnderRedelivery(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 5)

	attempt := Attempt{ItemID: itemID, UserID: "alice", Quantity: 2, IdempotencyKey: "attempt-1"}

	first, err := ProcessAttempt(ctx, db, attempt, testTTL)
	if err != nil {
		t.Fatalf("first ProcessAttempt: %v", err)
	}
	if first.Replayed {
		t.Error("expected first delivery to have Replayed=false")
	}

	// Simulate Kafka redelivering the exact same message.
	second, err := ProcessAttempt(ctx, db, attempt, testTTL)
	if err != nil {
		t.Fatalf("second (redelivered) ProcessAttempt: %v", err)
	}
	if !second.Replayed {
		t.Error("expected redelivered attempt to have Replayed=true")
	}
	if second.ReservationID != first.ReservationID {
		t.Errorf("expected redelivery to return the same reservation ID, got %s vs %s", second.ReservationID, first.ReservationID)
	}
	if second.Status != first.Status {
		t.Errorf("expected redelivery to return the same status, got %s vs %s", second.Status, first.Status)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 3 {
		t.Errorf("expected inventory decremented exactly ONCE (5 -> 3), got %d -- redelivery caused a double decrement", available)
	}

	var reservationCount int
	if err := db.QueryRow(`SELECT count(*) FROM reservations WHERE item_id = $1`, itemID).Scan(&reservationCount); err != nil {
		t.Fatalf("counting reservations: %v", err)
	}
	if reservationCount != 1 {
		t.Errorf("expected exactly 1 reservation row despite 2 deliveries, got %d", reservationCount)
	}
}

// TestProcessAttemptNeverOversells is the test that matters most in
// this whole project: hammer a small amount of stock with concurrent,
// distinct attempts and verify Postgres's row lock (SELECT ... FOR
// UPDATE) serializes them correctly -- exactly the number of units
// available get reserved, never more, no matter how many goroutines
// race for them at once.
func TestProcessAttemptNeverOversells(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	const stock = 10
	const attempts = 50
	itemID := seedItem(t, db, stock)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var reserved, rejected int

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcome, err := ProcessAttempt(ctx, db, Attempt{
				ItemID:         itemID,
				UserID:         fmt.Sprintf("buyer-%d", i),
				Quantity:       1,
				IdempotencyKey: fmt.Sprintf("attempt-%d", i),
			}, testTTL)
			if err != nil {
				t.Errorf("ProcessAttempt for buyer-%d: %v", i, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if outcome.Status == "reserved" {
				reserved++
			} else {
				rejected++
			}
		}(i)
	}
	wg.Wait()

	if reserved != stock {
		t.Errorf("expected exactly %d reservations to succeed (one per unit of stock), got %d", stock, reserved)
	}
	if rejected != attempts-stock {
		t.Errorf("expected %d rejections, got %d", attempts-stock, rejected)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 0 {
		t.Errorf("expected exactly 0 remaining after selling all %d units under concurrent load, got %d -- this is an oversell", stock, available)
	}

	var reservedCount int
	if err := db.QueryRow(`SELECT count(*) FROM reservations WHERE item_id = $1 AND status = 'reserved'`, itemID).Scan(&reservedCount); err != nil {
		t.Fatalf("counting reserved rows: %v", err)
	}
	if reservedCount != stock {
		t.Errorf("expected exactly %d reserved rows in Postgres, got %d", stock, reservedCount)
	}
}

// TestProcessAttemptExactStockBoundary checks the boundary directly:
// requesting exactly the remaining stock should reserve (available >=
// quantity is true when they're equal), leaving zero behind. An
// off-by-one here (> instead of >=) would wrongly reject the buyer who
// should get the very last unit.
func TestProcessAttemptExactStockBoundary(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 3)

	outcome, err := ProcessAttempt(ctx, db, Attempt{
		ItemID: itemID, UserID: "alice", Quantity: 3, IdempotencyKey: "attempt-1",
	}, testTTL)
	if err != nil {
		t.Fatalf("ProcessAttempt: %v", err)
	}
	if outcome.Status != "reserved" {
		t.Errorf("expected exact-match quantity to reserve, got status %q", outcome.Status)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 0 {
		t.Errorf("expected 0 remaining after reserving exactly all 3 units, got %d", available)
	}
}

// TestProcessAttemptZeroQuantityIsAcceptedAsReserved documents current
// behavior rather than asserting it's correct: ProcessAttempt does not
// itself validate Quantity > 0. checkout-api's HTTP handler rejects
// quantity <= 0 before ever publishing to Kafka, so this is
// unreachable through the real buyer flow today -- but ProcessAttempt
// has no guard of its own, and a Kafka message doesn't know or enforce
// that its producer validated anything.
func TestProcessAttemptRejectsZeroQuantity(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 5)

	_, err := ProcessAttempt(ctx, db, Attempt{
		ItemID: itemID, UserID: "alice", Quantity: 0, IdempotencyKey: "attempt-1",
	}, testTTL)
	if !errors.Is(err, ErrInvalidQuantity) {
		t.Errorf("expected ErrInvalidQuantity for a zero-quantity attempt, got %v", err)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 5 {
		t.Errorf("expected inventory unchanged at 5 for a rejected zero-quantity attempt, got %d", available)
	}
}

// TestProcessAttemptNegativeQuantityIncreasesInventory documents a real
// gap, not a recommendation: a negative Quantity passes `available >=
// quantity` trivially (available is always >= any negative number),
// then *subtracts a negative*, INCREASING available_inventory. This
// only stays unreachable as long as every future producer of this
// topic (backfill scripts, replay tools, a future admin action) also
// happens to validate quantity upstream -- the same kind of assumption
// Phase 1 explicitly rejected by requiring every consumer to be
// idempotent rather than trusting producers to behave. Recommend
// adding `if attempt.Quantity <= 0 { return Outcome{}, fmt.Errorf(...) }`
// directly in ProcessAttempt as part of Phase 8 hardening, so this
// invariant doesn't live solely in the HTTP layer.
func TestProcessAttemptRejectsNegativeQuantity(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 5)

	_, err := ProcessAttempt(ctx, db, Attempt{
		ItemID: itemID, UserID: "alice", Quantity: -10, IdempotencyKey: "attempt-1",
	}, testTTL)
	if !errors.Is(err, ErrInvalidQuantity) {
		t.Fatalf("expected ErrInvalidQuantity, got %v", err)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 5 {
		t.Errorf("expected inventory untouched at 5 after a rejected negative-quantity attempt, got %d", available)
	}
}
