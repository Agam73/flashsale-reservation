// Package decision holds the one piece of logic this whole project
// exists to get right: given a purchase attempt, decide -- once,
// durably, and safely under both concurrent access and Kafka's
// at-least-once redelivery -- whether that item still has stock.
//
// This has no Kafka dependency at all. decision-service's job is just
// to pull messages off a partition and hand them to ProcessAttempt;
// everything about correctness lives here, where it can be tested
// directly against Postgres without a broker in the loop.
package decision

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrItemNotFound means the attempt referenced an item ID that isn't
// in the items table at all. This is different from "sold out": the
// reservations.item_id foreign key means we can't even attempt an
// insert for a nonexistent item, so this is treated as a distinct,
// non-retryable error rather than a "rejected" outcome.
var ErrItemNotFound = errors.New("decision: item not found")

// ErrInvalidQuantity means the attempt requested zero or a negative
// quantity. checkout-api's HTTP handler already rejects this at the
// edge, but ProcessAttempt is what Kafka messages actually reach, and
// nothing guarantees every producer is checkout-api forever -- without
// this guard, a negative quantity would make
// "available_inventory - quantity" increase stock instead of
// decrementing it. Validated here, not just at the HTTP layer, because
// this is the function that's supposed to be authoritative.
var ErrInvalidQuantity = errors.New("decision: quantity must be positive")

// Attempt is what a PurchaseAttempted Kafka message decodes into.
// IdempotencyKey is what makes reprocessing the same message (Kafka's
// at-least-once guarantee) safe: the same key always produces the same
// recorded outcome, never a second decrement.
type Attempt struct {
	ItemID         string
	UserID         string
	Quantity       int64
	IdempotencyKey string
}

// Outcome is the durable result of processing an Attempt.
type Outcome struct {
	ReservationID string
	// Status is "reserved" (inventory held, buyer has until the TTL to
	// pay) or "rejected" (not enough stock left).
	Status string
	// Replayed is true when this outcome was NOT decided just now --
	// it's what was already recorded from an earlier delivery of the
	// same idempotency key. Useful for logging/metrics, not for
	// deciding behavior; the outcome itself is already correct either
	// way.
	Replayed bool
}

// ProcessAttempt is the entire "prevent overselling for real" story in
// one function. It:
//
//  0. Rejects a non-positive quantity outright (ErrInvalidQuantity),
//     before opening a transaction at all -- a negative quantity would
//     otherwise make step 4's decrement increase inventory instead.
//  1. Locks the item's row (SELECT ... FOR UPDATE) for the duration of
//     the transaction. Kafka's partition-by-item-ID design already
//     ensures one consumer processes a given item's attempts at a
//     time; this lock is the belt-and-suspenders backstop that holds
//     even if that invariant is ever violated (e.g. two decision-service
//     replicas briefly overlapping during a rebalance).
//  2. Decides reserved vs rejected based on the locked row's current
//     available_inventory.
//  3. Inserts the reservation with ON CONFLICT (item_id,
//     idempotency_key) DO NOTHING -- if this exact attempt was already
//     recorded (Kafka redelivered the message), the insert is a no-op
//     and the existing outcome is read back and returned unchanged.
//     Inventory is only ever decremented on a genuinely new insert,
//     never on a replay.
//  4. Only decrements items.available_inventory when a NEW reservation
//     was actually inserted with status 'reserved'.
//
// All of this happens in one transaction, so a crash between steps
// leaves nothing half-applied.
func ProcessAttempt(ctx context.Context, db *sql.DB, attempt Attempt, reservationTTL time.Duration) (Outcome, error) {
	if attempt.Quantity <= 0 {
		return Outcome{}, fmt.Errorf("%w: got %d", ErrInvalidQuantity, attempt.Quantity)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Outcome{}, fmt.Errorf("decision: beginning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op if already committed

	var available int64
	err = tx.QueryRowContext(ctx,
		`SELECT available_inventory FROM items WHERE id = $1 FOR UPDATE`,
		attempt.ItemID,
	).Scan(&available)
	if errors.Is(err, sql.ErrNoRows) {
		return Outcome{}, fmt.Errorf("%w: %s", ErrItemNotFound, attempt.ItemID)
	}
	if err != nil {
		return Outcome{}, fmt.Errorf("decision: locking item %s: %w", attempt.ItemID, err)
	}

	status := "rejected"
	var expiresAt sql.NullTime
	if available >= attempt.Quantity {
		status = "reserved"
		expiresAt = sql.NullTime{Time: time.Now().Add(reservationTTL), Valid: true}
	}

	var reservationID string
	var insertedStatus string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO reservations (item_id, user_id, quantity, status, idempotency_key, expires_at, decided_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (item_id, idempotency_key) DO NOTHING
		RETURNING id, status
	`, attempt.ItemID, attempt.UserID, attempt.Quantity, status, attempt.IdempotencyKey, expiresAt,
	).Scan(&reservationID, &insertedStatus)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		// ON CONFLICT DO NOTHING fired: this idempotency_key was
		// already processed by an earlier delivery of the same
		// message. Read back what actually happened so the caller
		// gets a consistent answer, and skip the inventory update --
		// it was already applied (or correctly not applied) the first
		// time.
		var existing Outcome
		lookupErr := tx.QueryRowContext(ctx, `
			SELECT id, status FROM reservations
			WHERE item_id = $1 AND idempotency_key = $2
		`, attempt.ItemID, attempt.IdempotencyKey).Scan(&existing.ReservationID, &existing.Status)
		if lookupErr != nil {
			return Outcome{}, fmt.Errorf("decision: looking up existing reservation for replayed attempt: %w", lookupErr)
		}
		existing.Replayed = true
		if err := tx.Commit(); err != nil {
			return Outcome{}, fmt.Errorf("decision: committing replayed attempt: %w", err)
		}
		return existing, nil

	case err != nil:
		return Outcome{}, fmt.Errorf("decision: inserting reservation: %w", err)
	}

	// Genuinely new attempt. Only now, having actually inserted a new
	// 'reserved' row, do we touch inventory.
	if insertedStatus == "reserved" {
		if _, err := tx.ExecContext(ctx,
			`UPDATE items SET available_inventory = available_inventory - $1 WHERE id = $2`,
			attempt.Quantity, attempt.ItemID,
		); err != nil {
			return Outcome{}, fmt.Errorf("decision: decrementing inventory for item %s: %w", attempt.ItemID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return Outcome{}, fmt.Errorf("decision: committing: %w", err)
	}

	return Outcome{ReservationID: reservationID, Status: insertedStatus}, nil
}
