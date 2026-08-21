package redisx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testClient connects to the Redis started by the test environment
// (REDIS_ADDR, defaulting to localhost:6379) and flushes the DB before
// each test so tests don't interfere with each other.
func testClient(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()
	client, err := NewClient(ctx, Config{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("skipping: no local Redis available: %v", err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushing test db: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestSeedAndGetInventory(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SeedInventory(ctx, client, "item-1", 5); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}
	n, err := GetInventory(ctx, client, "item-1")
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5, got %d", n)
	}
}

func TestGetInventoryNotFound(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	_, err := GetInventory(ctx, client, "never-seeded")
	if !errors.Is(err, ErrInventoryNotFound) {
		t.Errorf("expected ErrInventoryNotFound, got %v", err)
	}
}

func TestTryDecrementInventorySuccess(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SeedInventory(ctx, client, "item-1", 3); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}

	remaining, err := TryDecrementInventory(ctx, client, "item-1", 2)
	if err != nil {
		t.Fatalf("TryDecrementInventory: %v", err)
	}
	if remaining != 1 {
		t.Errorf("expected 1 remaining, got %d", remaining)
	}
}

func TestTryDecrementInventoryInsufficientLeavesCountUnchanged(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SeedInventory(ctx, client, "item-1", 1); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}

	_, err := TryDecrementInventory(ctx, client, "item-1", 5)
	if !errors.Is(err, ErrInsufficientInventory) {
		t.Fatalf("expected ErrInsufficientInventory, got %v", err)
	}

	n, err := GetInventory(ctx, client, "item-1")
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if n != 1 {
		t.Errorf("expected count untouched at 1 after failed decrement, got %d", n)
	}
}

func TestTryDecrementInventoryNotFound(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	_, err := TryDecrementInventory(ctx, client, "never-seeded", 1)
	if !errors.Is(err, ErrInventoryNotFound) {
		t.Errorf("expected ErrInventoryNotFound, got %v", err)
	}
}

// TestTryDecrementInventoryNeverOversells is the test that actually
// matters: it hammers a single unit of inventory with concurrent
// decrements and checks that at most one of them ever succeeds. This is
// the same class of bug the whole project's Kafka/decision-service
// design (Phase 6) exists to prevent authoritatively -- here it's Redis
// atomicity doing the same job for the fast path.
func TestTryDecrementInventoryNeverOversells(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SeedInventory(ctx, client, "hot-item", 1); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}

	const attempts = 50
	var wg sync.WaitGroup
	var successes int64
	var mu sync.Mutex

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := TryDecrementInventory(ctx, client, "hot-item", 1); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Errorf("expected exactly 1 success out of %d concurrent attempts on 1 unit of stock, got %d", attempts, successes)
	}

	n, err := GetInventory(ctx, client, "hot-item")
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 remaining after selling the only unit, got %d", n)
	}
}

func TestReleaseInventory(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SeedInventory(ctx, client, "item-1", 0); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}
	if err := ReleaseInventory(ctx, client, "item-1", 3); err != nil {
		t.Fatalf("ReleaseInventory: %v", err)
	}
	n, err := GetInventory(ctx, client, "item-1")
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 after releasing, got %d", n)
	}
}

func TestGrantAndConsumeAdmission(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := GrantAdmission(ctx, client, "item-1", "user-alice", time.Minute); err != nil {
		t.Fatalf("GrantAdmission: %v", err)
	}

	found, err := ConsumeAdmission(ctx, client, "item-1", "user-alice")
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if !found {
		t.Error("expected token to be found")
	}

	// Single-use: consuming again should find nothing.
	found, err = ConsumeAdmission(ctx, client, "item-1", "user-alice")
	if err != nil {
		t.Fatalf("ConsumeAdmission (second call): %v", err)
	}
	if found {
		t.Error("expected token to be gone after being consumed once")
	}
}

func TestConsumeAdmissionWithoutGrant(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	found, err := ConsumeAdmission(ctx, client, "item-1", "user-never-admitted")
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if found {
		t.Error("expected no token for a user who never joined the waiting room")
	}
}

func TestAdmissionTokenExpires(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := GrantAdmission(ctx, client, "item-1", "user-slow", 50*time.Millisecond); err != nil {
		t.Fatalf("GrantAdmission: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	found, err := ConsumeAdmission(ctx, client, "item-1", "user-slow")
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if found {
		t.Error("expected token to have expired before it was consumed")
	}
}
