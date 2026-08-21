package redisx

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ErrInventoryNotFound means InventoryKey(itemID) hasn't been seeded
// yet -- there's nothing to check out against.
var ErrInventoryNotFound = errors.New("redisx: inventory not found for item")

// ErrInsufficientInventory means the requested quantity is more than
// what's left; no decrement was made.
var ErrInsufficientInventory = errors.New("redisx: insufficient inventory")

// decrementScript checks and decrements in a single round trip, so two
// concurrent checkouts can never both read "1 left" and both succeed --
// exactly the oversell scenario this whole project exists to prevent,
// demonstrated here with Redis's own atomicity instead of the Postgres
// row lock decision-service will use from Phase 6 on.
//
// Return values: -1 = key not seeded, -2 = insufficient stock (no
// change made), >=0 = new remaining count after a successful decrement.
var decrementScript = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1]))
if current == nil then
  return -1
end
local qty = tonumber(ARGV[1])
if current < qty then
  return -2
end
return redis.call('DECRBY', KEYS[1], qty)
`)

// SeedInventory sets the fast-path available count for an item. This is
// a Phase 4 stand-in for real seeding from Postgres -- Phase 9 is where
// this gets wired up properly and made rebuildable from
// items.available_inventory if Redis state is ever lost.
func SeedInventory(ctx context.Context, client *redis.Client, itemID string, available int64) error {
	if available < 0 {
		return fmt.Errorf("redisx: cannot seed negative inventory (%d) for item %s", available, itemID)
	}
	return client.Set(ctx, InventoryKey(itemID), available, 0).Err()
}

// GetInventory returns the current fast-path count for an item.
func GetInventory(ctx context.Context, client *redis.Client, itemID string) (int64, error) {
	n, err := client.Get(ctx, InventoryKey(itemID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, ErrInventoryNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("redisx: reading inventory for item %s: %w", itemID, err)
	}
	return n, nil
}

// TryDecrementInventory atomically decrements itemID's fast-path count
// by quantity, but only if enough is available. Returns the count
// remaining after a successful decrement, ErrInventoryNotFound if the
// item was never seeded, or ErrInsufficientInventory if there wasn't
// enough left (in which case nothing was changed).
func TryDecrementInventory(ctx context.Context, client *redis.Client, itemID string, quantity int64) (remaining int64, err error) {
	if quantity <= 0 {
		return 0, fmt.Errorf("redisx: quantity must be positive, got %d", quantity)
	}

	res, err := decrementScript.Run(ctx, client, []string{InventoryKey(itemID)}, quantity).Int64()
	if err != nil {
		return 0, fmt.Errorf("redisx: decrementing inventory for item %s: %w", itemID, err)
	}

	switch res {
	case -1:
		return 0, ErrInventoryNotFound
	case -2:
		return 0, ErrInsufficientInventory
	default:
		return res, nil
	}
}

// ReleaseInventory adds quantity back to an item's fast-path count.
// Used to undo a decrement if checkout fails after the Redis step, and
// later by expiry-worker (Phase 7) when a reservation's TTL lapses
// unpaid.
func ReleaseInventory(ctx context.Context, client *redis.Client, itemID string, quantity int64) error {
	if quantity <= 0 {
		return fmt.Errorf("redisx: quantity must be positive, got %d", quantity)
	}
	return client.IncrBy(ctx, InventoryKey(itemID), quantity).Err()
}
