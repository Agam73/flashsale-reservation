package expiry

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// testDB connects to the Postgres migrated by this repo's Phase 3
// migrations and truncates the tables this package touches before each
// test.
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

func seedItem(t *testing.T, db *sql.DB, available int) string {
	t.Helper()
	var id string
	err := db.QueryRow(`
		INSERT INTO items (name, price_cents, total_inventory, available_inventory, status)
		VALUES ('Test Item', 1000, 100, $1, 'on_sale')
		RETURNING id
	`, available).Scan(&id)
	if err != nil {
		t.Fatalf("seeding item: %v", err)
	}
	return id
}

// seedReservation inserts a reservation with an arbitrary status and
// expiry, for setting up specific test scenarios directly (bypassing
// internal/decision entirely, since this package doesn't depend on it).
func seedReservation(t *testing.T, db *sql.DB, itemID string, quantity int64, status string, expiresAt *time.Time) string {
	t.Helper()
	var id string
	err := db.QueryRow(`
		INSERT INTO reservations (item_id, user_id, quantity, status, idempotency_key, expires_at, decided_at)
		VALUES ($1, 'test-user', $2, $3, $4, $5, now())
		RETURNING id
	`, itemID, quantity, status, fmt.Sprintf("seed-%d", time.Now().UnixNano()), expiresAt).Scan(&id)
	if err != nil {
		t.Fatalf("seeding reservation: %v", err)
	}
	return id
}

func past(d time.Duration) *time.Time {
	t := time.Now().Add(-d)
	return &t
}

func future(d time.Duration) *time.Time {
	t := time.Now().Add(d)
	return &t
}

func TestFindExpiredReservationsReturnsOnlyExpiredReserved(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 10)

	expiredReserved := seedReservation(t, db, itemID, 1, "reserved", past(time.Minute))
	seedReservation(t, db, itemID, 1, "reserved", future(time.Minute))  // not yet expired
	seedReservation(t, db, itemID, 1, "completed", past(time.Minute))   // wrong status, even though "expired" in time
	seedReservation(t, db, itemID, 1, "expired", past(time.Minute))     // already handled
	seedReservation(t, db, itemID, 1, "rejected", nil)                  // never had an expiry at all

	ids, err := FindExpiredReservations(ctx, db, 10)
	if err != nil {
		t.Fatalf("FindExpiredReservations: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected exactly 1 expired reservation, got %d: %v", len(ids), ids)
	}
	if ids[0] != expiredReserved {
		t.Errorf("expected the one expired-and-reserved reservation, got a different ID")
	}
}

func TestFindExpiredReservationsRespectsLimit(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 10)

	for i := 0; i < 5; i++ {
		seedReservation(t, db, itemID, 1, "reserved", past(time.Minute))
	}

	ids, err := FindExpiredReservations(ctx, db, 3)
	if err != nil {
		t.Fatalf("FindExpiredReservations: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("expected exactly 3 (the limit), got %d", len(ids))
	}
}

func TestFindExpiredReservationsOrdersOldestFirst(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 10)

	newest := seedReservation(t, db, itemID, 1, "reserved", past(1*time.Minute))
	oldest := seedReservation(t, db, itemID, 1, "reserved", past(10*time.Minute))
	middle := seedReservation(t, db, itemID, 1, "reserved", past(5*time.Minute))

	ids, err := FindExpiredReservations(ctx, db, 10)
	if err != nil {
		t.Fatalf("FindExpiredReservations: %v", err)
	}
	want := []string{oldest, middle, newest}
	if len(ids) != len(want) {
		t.Fatalf("expected %d results, got %d", len(want), len(ids))
	}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("position %d: expected %s, got %s (expected oldest-expiry-first order)", i, want[i], id)
		}
	}
}

func TestExpireReservationReleasesInventory(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 3) // 3 available, 2 held by the reservation below

	reservationID := seedReservation(t, db, itemID, 2, "reserved", past(time.Minute))

	outcome, err := ExpireReservation(ctx, db, reservationID)
	if err != nil {
		t.Fatalf("ExpireReservation: %v", err)
	}
	if !outcome.Released {
		t.Error("expected Released=true")
	}
	if outcome.Quantity != 2 {
		t.Errorf("expected outcome quantity 2, got %d", outcome.Quantity)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 5 {
		t.Errorf("expected 3 + 2 released = 5, got %d", available)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM reservations WHERE id = $1`, reservationID).Scan(&status); err != nil {
		t.Fatalf("reading reservation status: %v", err)
	}
	if status != "expired" {
		t.Errorf("expected status 'expired', got %q", status)
	}
}

func TestExpireReservationNotYetExpiredIsNoOp(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 3)
	reservationID := seedReservation(t, db, itemID, 2, "reserved", future(time.Hour))

	outcome, err := ExpireReservation(ctx, db, reservationID)
	if err != nil {
		t.Fatalf("ExpireReservation: %v", err)
	}
	if outcome.Released {
		t.Error("expected Released=false for a reservation that hasn't actually expired yet")
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 3 {
		t.Errorf("expected inventory untouched at 3, got %d", available)
	}
}

func TestExpireReservationAlreadyCompletedIsNoOp(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 3)
	// A reservation that got paid before its TTL lapsed: status moved
	// to 'completed', but expires_at (if still set) could still be in
	// the past. expiry-worker must never touch this.
	reservationID := seedReservation(t, db, itemID, 2, "completed", past(time.Minute))

	outcome, err := ExpireReservation(ctx, db, reservationID)
	if err != nil {
		t.Fatalf("ExpireReservation: %v", err)
	}
	if outcome.Released {
		t.Error("expected Released=false for an already-completed reservation")
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 3 {
		t.Errorf("expected inventory untouched at 3 for a completed reservation, got %d", available)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM reservations WHERE id = $1`, reservationID).Scan(&status); err != nil {
		t.Fatalf("reading reservation status: %v", err)
	}
	if status != "completed" {
		t.Errorf("expected status to remain 'completed', got %q", status)
	}
}

func TestExpireReservationCalledTwiceOnlyReleasesOnce(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 3)
	reservationID := seedReservation(t, db, itemID, 2, "reserved", past(time.Minute))

	first, err := ExpireReservation(ctx, db, reservationID)
	if err != nil {
		t.Fatalf("first ExpireReservation: %v", err)
	}
	if !first.Released {
		t.Fatal("expected first call to release")
	}

	second, err := ExpireReservation(ctx, db, reservationID)
	if err != nil {
		t.Fatalf("second ExpireReservation: %v", err)
	}
	if second.Released {
		t.Error("expected second call on an already-expired reservation to be a no-op")
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 5 {
		t.Errorf("expected inventory released exactly once (3 + 2 = 5), got %d -- second call double-released", available)
	}
}

// TestExpireReservationNeverDoubleReleasesUnderConcurrency is this
// phase's version of the oversell test: many goroutines racing to
// expire the exact same reservation ID at once (simulating multiple
// worker-pool workers, or overlapping scanner ticks, both grabbing the
// same stale reservation ID). Inventory must go back exactly once.
func TestExpireReservationNeverDoubleReleasesUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	itemID := seedItem(t, db, 3)
	reservationID := seedReservation(t, db, itemID, 2, "reserved", past(time.Minute))

	const attempts = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	var releasedCount int

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outcome, err := ExpireReservation(ctx, db, reservationID)
			if err != nil {
				t.Errorf("ExpireReservation: %v", err)
				return
			}
			if outcome.Released {
				mu.Lock()
				releasedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if releasedCount != 1 {
		t.Errorf("expected exactly 1 of %d concurrent attempts to actually release, got %d", attempts, releasedCount)
	}

	var available int
	if err := db.QueryRow(`SELECT available_inventory FROM items WHERE id = $1`, itemID).Scan(&available); err != nil {
		t.Fatalf("reading inventory: %v", err)
	}
	if available != 5 {
		t.Errorf("expected inventory released exactly once under concurrent load (3 + 2 = 5), got %d -- this is a double-release", available)
	}
}
