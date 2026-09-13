package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

// TestRunReconciler_CorrectsDriftOnEachTick is new coverage for Phase 9:
// reconcile.Item and reconcile.All (internal/reconcile) were tested
// directly, and handleReconcileItem's on-demand HTTP path was tested,
// but nothing exercised runReconciler itself -- the background loop
// cmd/checkout-api actually starts in main() to correct drift for as
// long as the service runs. This confirms the loop really does call
// reconcile on its own, on a schedule, without a manual trigger.
func TestRunReconciler_CorrectsDriftOnEachTick(t *testing.T) {
	db := testDB(t)
	redisClient := testRedis(t)
	itemID := seedPGItem(t, db, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A short interval keeps the test fast; runReconciler doesn't
	// treat this specially, it just ticks faster.
	go runReconciler(ctx, db, redisClient, 20*time.Millisecond)

	// Wait for the first scheduled tick to seed Redis, independent of
	// any startup call -- runReconciler's own first tick, not a
	// synchronous pre-seed.
	waitForInventory(t, redisClient, itemID, 10)

	// Now drift Postgres out from under Redis, the same way an order
	// fulfilled by decision-service or an operator's manual DB edit
	// would -- and confirm the *next* tick corrects it without anyone
	// calling reconcile.Item or /reconcile directly.
	if _, err := db.Exec(`UPDATE items SET available_inventory = 4 WHERE id = $1`, itemID); err != nil {
		t.Fatalf("simulating drift: %v", err)
	}
	waitForInventory(t, redisClient, itemID, 4)
}

// TestRunReconciler_StopsOnContextCancellation confirms the loop is a
// well-behaved background goroutine: it must not keep reconciling (or
// leak) once its context is cancelled, since cmd/checkout-api relies
// on ctx cancellation for graceful shutdown everywhere else.
func TestRunReconciler_StopsOnContextCancellation(t *testing.T) {
	db := testDB(t)
	redisClient := testRedis(t)
	itemID := seedPGItem(t, db, 10)

	ctx, cancel := context.WithCancel(context.Background())
	go runReconciler(ctx, db, redisClient, 15*time.Millisecond)
	waitForInventory(t, redisClient, itemID, 10)

	cancel()
	time.Sleep(50 * time.Millisecond) // let the loop actually exit

	// Drift Postgres after cancellation -- if the loop is truly
	// stopped, this must NOT get picked up.
	if _, err := db.Exec(`UPDATE items SET available_inventory = 1 WHERE id = $1`, itemID); err != nil {
		t.Fatalf("simulating drift: %v", err)
	}
	time.Sleep(80 * time.Millisecond) // long enough for a tick if the loop were still running

	n, err := redisx.GetInventory(context.Background(), redisClient, itemID)
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if n != 10 {
		t.Errorf("expected stale value 10 (loop should have stopped), got %d", n)
	}
}

// waitForInventory polls Redis until itemID's cached inventory matches
// want or the timeout elapses, failing the test either way. Polling
// (rather than a fixed sleep) is what makes the "does the loop
// actually tick" test fast without being flaky -- it succeeds the
// instant the tick lands rather than on a guessed delay.
func waitForInventory(t *testing.T, redisClient *redis.Client, itemID string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last int64 = -1
	var lastErr error
	for time.Now().Before(deadline) {
		n, err := redisx.GetInventory(context.Background(), redisClient, itemID)
		if err == nil {
			last = n
			if n == want {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for redis inventory=%d for item %s (last seen: %d, last err: %v)", want, itemID, last, lastErr)
}
