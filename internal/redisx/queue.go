package redisx

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// SetQueueDepth records the number of buyers currently waiting in
// itemID's waiting room. This is purely observational: the in-memory
// admission.Admitter (Phase 2) remains the actual source of truth for
// who's waiting and in what order. Redis just caches a number read
// from it -- the same "authoritative store / disposable cached copy"
// shape the Phase 1 design decision uses for Postgres/Redis inventory,
// just with the Admitter playing the authoritative role instead of
// Postgres.
//
// No TTL on purpose: this is overwritten every time something reads
// the Admitter's depth (see cmd/waiting-room-api's queue handler), so
// a stale value just means nobody's asked recently, not that the value
// is wrong or that a key needs to expire out from under a reader.
func SetQueueDepth(ctx context.Context, client *redis.Client, itemID string, depth int64) error {
	if depth < 0 {
		return fmt.Errorf("redisx: queue depth must not be negative, got %d", depth)
	}
	return client.Set(ctx, QueueDepthKey(itemID), depth, 0).Err()
}

// GetQueueDepth returns the last-recorded queue depth for itemID, or 0
// if nobody has ever recorded one. Unlike inventory -- which must be
// deliberately seeded before it means anything -- "nobody's queued for
// this item (yet)" is a legitimate, common default, not a missing-data
// error.
func GetQueueDepth(ctx context.Context, client *redis.Client, itemID string) (int64, error) {
	n, err := client.Get(ctx, QueueDepthKey(itemID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("redisx: reading queue depth for item %s: %w", itemID, err)
	}
	return n, nil
}
