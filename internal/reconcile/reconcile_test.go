package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"

	_ "github.com/lib/pq"

	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

// testDB connects to the same Postgres instance internal/decision and
// internal/expiry's tests target, truncating the tables this package
// touches before each test.
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

// testRedis connects to the same local Redis internal/redisx's tests
// target, flushing the DB before each test.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("skipping: no local Redis available: %v", err)
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushing test db: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// seedItem inserts an item with the given available inventory and
// status, returning its ID.
func seedItem(t *testing.T, db *sql.DB, available int, status string) string {
	t.Helper()
	var id string
	err := db.QueryRow(`
		INSERT INTO items (name, price_cents, total_inventory, available_inventory, status)
		VALUES ('Test Item', 1000, $1, $1, $2)
		RETURNING id
	`, available, status).Scan(&id)
	if err != nil {
		t.Fatalf("seeding item: %v", err)
	}
	return id
}

func TestItem_SeedsRedisFromPostgres(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	redisClient := testRedis(t)

	itemID := seedItem(t, db, 7, "on_sale")

	available, err := Item(ctx, db, redisClient, itemID)
	if err != nil {
		t.Fatalf("Item: %v", err)
	}
	if available != 7 {
		t.Errorf("expected 7, got %d", available)
	}

	n, err := redisx.GetInventory(ctx, redisClient, itemID)
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if n != 7 {
		t.Errorf("expected redis to read 7 after reconciling, got %d", n)
	}
}

// TestItem_PostgresAlwaysWins is the actual point of this package:
// however Redis got out of sync with Postgres -- a crash, a bug, a
// manual fix gone wrong -- reconciling overwrites it with the
// authoritative value rather than trying to merge the two.
func TestItem_PostgresAlwaysWins(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	redisClient := testRedis(t)

	itemID := seedItem(t, db, 9, "on_sale")

	// Simulate drift: Redis thinks there are only 2 left.
	if err := redisx.SeedInventory(ctx, redisClient, itemID, 2); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}

	available, err := Item(ctx, db, redisClient, itemID)
	if err != nil {
		t.Fatalf("Item: %v", err)
	}
	if available != 9 {
		t.Errorf("expected reconcile to return postgres's value 9, got %d", available)
	}

	n, err := redisx.GetInventory(ctx, redisClient, itemID)
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if n != 9 {
		t.Errorf("expected redis corrected to postgres's 9, got %d", n)
	}
}

func TestItem_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	redisClient := testRedis(t)

	_, err := Item(ctx, db, redisClient, "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, ErrItemNotFound) {
		t.Errorf("expected ErrItemNotFound, got %v", err)
	}
}

func TestActiveItemIDs_FiltersByStatus(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	onSale := seedItem(t, db, 5, "on_sale")
	scheduled := seedItem(t, db, 5, "scheduled")
	seedItem(t, db, 5, "draft")
	seedItem(t, db, 0, "sold_out")
	seedItem(t, db, 5, "closed")

	ids, err := ActiveItemIDs(ctx, db)
	if err != nil {
		t.Fatalf("ActiveItemIDs: %v", err)
	}

	want := map[string]bool{onSale: true, scheduled: true}
	if len(ids) != len(want) {
		t.Fatalf("expected %d active items, got %d: %v", len(want), len(ids), ids)
	}
	for _, id := range ids {
		if !want[id] {
			t.Errorf("unexpected item %s in active set (should only include on_sale/scheduled)", id)
		}
	}
}

func TestAll_ReconcilesEveryActiveItem(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	redisClient := testRedis(t)

	item1 := seedItem(t, db, 3, "on_sale")
	item2 := seedItem(t, db, 10, "scheduled")
	draftItem := seedItem(t, db, 99, "draft")

	outcomes, err := All(ctx, db, redisClient)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("expected 2 outcomes (draft item excluded), got %d", len(outcomes))
	}

	byID := make(map[string]Outcome, len(outcomes))
	for _, o := range outcomes {
		if o.Err != nil {
			t.Errorf("unexpected error reconciling item %s: %v", o.ItemID, o.Err)
		}
		byID[o.ItemID] = o
	}
	if byID[item1].Available != 3 {
		t.Errorf("expected item1 available=3, got %d", byID[item1].Available)
	}
	if byID[item2].Available != 10 {
		t.Errorf("expected item2 available=10, got %d", byID[item2].Available)
	}

	n1, err := redisx.GetInventory(ctx, redisClient, item1)
	if err != nil {
		t.Fatalf("GetInventory item1: %v", err)
	}
	if n1 != 3 {
		t.Errorf("expected redis item1=3, got %d", n1)
	}

	n2, err := redisx.GetInventory(ctx, redisClient, item2)
	if err != nil {
		t.Fatalf("GetInventory item2: %v", err)
	}
	if n2 != 10 {
		t.Errorf("expected redis item2=10, got %d", n2)
	}

	if _, err := redisx.GetInventory(ctx, redisClient, draftItem); !errors.Is(err, redisx.ErrInventoryNotFound) {
		t.Errorf("expected the draft item to never be seeded in redis, got err=%v", err)
	}
}
