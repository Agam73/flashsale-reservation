package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Agam73/flashsale-reservation/internal/admission"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

// testRedis connects to the same local Redis this service targets by
// default, skipping cleanly if it's not reachable -- same pattern
// cmd/checkout-api's tests already use.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("skipping: no local Redis available: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func doQueueDepth(t *testing.T, registry *admission.Registry, redisClient *redis.Client, itemID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", "/items/"+itemID+"/queue", nil)
	req.SetPathValue("itemID", itemID)
	rec := httptest.NewRecorder()
	handleQueueDepth(registry, redisClient)(rec, req)
	return rec
}

func TestHandleQueueDepth_MissingItemID(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 5)

	req := httptest.NewRequest("GET", "/items//queue", nil)
	req.SetPathValue("itemID", "")
	rec := httptest.NewRecorder()
	handleQueueDepth(registry, redisClient)(rec, req)

	if rec.Code != 400 {
		t.Errorf("expected 400 for missing item id, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleQueueDepth_ZeroBeforeAnyoneJoins(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 5)

	itemID := "queue-test-empty"
	rec := doQueueDepth(t, registry, redisClient, itemID)
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp queueDepthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Waiting != 0 {
		t.Errorf("expected 0 waiting before anyone joins, got %d", resp.Waiting)
	}
}

// TestHandleQueueDepth_ReflectsWaitingBuyersAndCachesToRedis checks
// both halves of this handler: it reports the Admitter's real depth,
// and it publishes that same number into Redis for other readers.
func TestHandleQueueDepth_ReflectsWaitingBuyersAndCachesToRedis(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 1) // slow admit rate so joins outpace it

	itemID := "queue-test-waiting"
	a := registry.For(itemID)

	for i := 0; i < 2; i++ {
		go func() {
			_, admitted, err := a.Join(context.Background())
			if err == nil {
				<-admitted
			}
		}()
	}

	// Wait for both joins to register before asserting -- the
	// Admitter serializes them one at a time over its requests
	// channel.
	deadline := time.After(500 * time.Millisecond)
	var rec *httptest.ResponseRecorder
	for {
		rec = doQueueDepth(t, registry, redisClient, itemID)
		var resp queueDepthResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err == nil && resp.Waiting == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected queue depth to reach 2, last response: %s", rec.Body.String())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	n, err := redisx.GetQueueDepth(context.Background(), redisClient, itemID)
	if err != nil {
		t.Fatalf("GetQueueDepth: %v", err)
	}
	if n != 2 {
		t.Errorf("expected redis to cache queue depth 2, got %d", n)
	}
}
