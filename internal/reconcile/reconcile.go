// Package reconcile keeps Redis's fast-path inventory counters in sync
// with Postgres's authoritative items.available_inventory column.
//
// Per the Phase 1 design decision, Redis is fast, disposable, derived
// state -- rebuildable from Postgres if it's ever lost. Through Phase 8
// nothing actually did that rebuilding: checkout-api's Redis counters
// were seeded once, by hand, through a dev-only HTTP endpoint
// (see docs/phase4.md). This package is the real thing: given an item
// ID, read the authoritative count and overwrite Redis's copy to
// match. Postgres always wins -- there's no merge logic, just "make
// Redis say what Postgres says right now."
//
// Like internal/decision and internal/expiry, this has no dependency
// on how or how often it gets called. cmd/checkout-api's job is to
// call it once at startup (so Redis is warm before the first buyer
// arrives) and again on a schedule (so drift doesn't accumulate
// forever); everything about correctness lives here, where it's tested
// directly against Postgres and Redis.
//
// What this does NOT fix: a checkout that decrements Redis but is
// later rejected by decision-service (because Postgres was already
// sold out by the time the Kafka message was processed) leaves
// Postgres's available_inventory temporarily lower than what Redis's
// fast path is still willing to sell against, or vice versa while a
// message is still in flight and hasn't reached decision-service yet.
// That's an accepted characteristic of an optimistic fast path sitting
// in front of an asynchronous authoritative path (see docs/phase6.md,
// "Known gaps") -- reconciliation bounds how long any given drift can
// persist, it doesn't claim the two stores are never briefly out of
// step.
package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

// ErrItemNotFound means itemID isn't in the items table at all --
// nothing to reconcile against.
var ErrItemNotFound = errors.New("reconcile: item not found")

// Item reads itemID's authoritative available_inventory from Postgres
// and overwrites Redis's fast-path copy to match, returning the value
// written.
func Item(ctx context.Context, db *sql.DB, redisClient *redis.Client, itemID string) (int64, error) {
	var available int64
	err := db.QueryRowContext(ctx,
		`SELECT available_inventory FROM items WHERE id = $1`, itemID,
	).Scan(&available)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrItemNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("reconcile: reading item %s: %w", itemID, err)
	}

	if err := redisx.SeedInventory(ctx, redisClient, itemID, available); err != nil {
		return 0, fmt.Errorf("reconcile: seeding redis for item %s: %w", itemID, err)
	}
	return available, nil
}

// ActiveItemIDs returns every item currently on sale or scheduled to
// go on sale -- the set worth keeping a warm Redis counter for.
// draft/sold_out/closed items don't need a fast path at all: draft and
// closed items aren't being sold, and sold_out items should already be
// rejecting fast-path checkouts at 0, which reconciling can only
// confirm, not change.
func ActiveItemIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id FROM items WHERE status IN ('scheduled', 'on_sale') ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("reconcile: listing active items: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("reconcile: reading active item ids: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reconcile: reading active item ids: %w", err)
	}
	return ids, nil
}

// Outcome is one item's result from an All pass. Kept even for items
// that failed, so a caller can log or alert on the specific item
// without the whole pass aborting over one bad item -- the same
// "don't let one bad apple block a batch" instinct as expiry-worker's
// scanner, just without that package's channel/worker-pool machinery:
// this runs occasionally, over a handful of items, not against a
// backlog of work that can pile up under load.
type Outcome struct {
	ItemID    string
	Available int64
	Err       error
}

// All reconciles every active item and returns one Outcome per item.
// The only error All itself returns is a failure to even list active
// items; a failure reconciling one specific item shows up in that
// item's Outcome.Err instead, so the rest of the batch still runs.
func All(ctx context.Context, db *sql.DB, redisClient *redis.Client) ([]Outcome, error) {
	ids, err := ActiveItemIDs(ctx, db)
	if err != nil {
		return nil, err
	}

	outcomes := make([]Outcome, 0, len(ids))
	for _, id := range ids {
		available, err := Item(ctx, db, redisClient, id)
		outcomes = append(outcomes, Outcome{ItemID: id, Available: available, Err: err})
	}
	return outcomes, nil
}
