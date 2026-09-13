package redisx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// SetRiskScore caches a bot/scalper risk score for one buyer's pass
// through one item's waiting room. Per the Phase 1 design decision --
// "the risk model runs asynchronously and writes to Redis... it is
// never called synchronously in the checkout hot path" -- this is
// write-only from risk-service's side (Phase 10); nothing in
// checkout-api's request path blocks waiting for a score to exist.
//
// ttl bounds how long a cached score is trusted before it's treated as
// stale rather than wrong. risk-service is expected to keep refreshing
// it for as long as a buyer's waiting-room session is active, not to
// write it once and leave it.
func SetRiskScore(ctx context.Context, client *redis.Client, itemID, userID string, score float64, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("redisx: risk score ttl must be positive, got %s", ttl)
	}
	return client.Set(ctx, RiskScoreKey(itemID, userID), strconv.FormatFloat(score, 'f', -1, 64), ttl).Err()
}

// GetRiskScore returns the cached risk score for itemID/userID.
// found=false (no error) means nobody has scored this buyer yet, or
// the cached score expired -- both are "treat as unknown," not a
// failure, since a missing score should never be able to block a
// checkout attempt.
//
// Nothing reads this yet: consuming it to actually influence a
// decision is Phase 10's job. This phase just builds the cache and the
// key format checkout-api and risk-service need to agree on.
func GetRiskScore(ctx context.Context, client *redis.Client, itemID, userID string) (score float64, found bool, err error) {
	v, err := client.Get(ctx, RiskScoreKey(itemID, userID)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("redisx: reading risk score for item %s user %s: %w", itemID, userID, err)
	}

	parsed, parseErr := strconv.ParseFloat(v, 64)
	if parseErr != nil {
		return 0, false, fmt.Errorf("redisx: parsing cached risk score for item %s user %s: %w", itemID, userID, parseErr)
	}
	return parsed, true, nil
}
