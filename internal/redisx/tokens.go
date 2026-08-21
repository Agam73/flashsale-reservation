package redisx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// GrantAdmission records that userID has been let through the waiting
// room for itemID, valid for ttl. checkout-api requires this token to
// exist -- and consumes it -- before doing the fast-path inventory
// check. That's what makes the waiting room's fairness gate actually
// enforced rather than advisory: skipping straight to checkout-api
// without a token gets rejected.
func GrantAdmission(ctx context.Context, client *redis.Client, itemID, userID string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("redisx: admission ttl must be positive, got %s", ttl)
	}
	return client.Set(ctx, AdmissionKey(itemID, userID), time.Now().UTC().Format(time.RFC3339), ttl).Err()
}

// ConsumeAdmission atomically checks for and deletes a user's admission
// token in one round trip (Redis's GETDEL). Single-use: once
// checkout-api consumes it, the same buyer can't check out twice off
// one trip through the waiting room. Returns found=false (no error) if
// there was no valid token -- either the buyer never joined the
// waiting room, or the token already expired/was already consumed.
func ConsumeAdmission(ctx context.Context, client *redis.Client, itemID, userID string) (found bool, err error) {
	err = client.GetDel(ctx, AdmissionKey(itemID, userID)).Err()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redisx: consuming admission token for item %s user %s: %w", itemID, userID, err)
	}
	return true, nil
}
