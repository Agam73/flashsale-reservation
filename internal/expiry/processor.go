// Package expiry holds the compensating-transaction logic from the
// Phase 1 design decision: an unpaid reservation's TTL lapses, its
// inventory goes back to the item, and the reservation is marked
// expired -- all without ever locking stock indefinitely.
//
// Like internal/decision, this has no dependency on how it gets
// called. cmd/expiry-worker's job is just to find candidate
// reservation IDs on a schedule and hand them to a pool of workers;
// everything about correctness -- including staying safe when the same
// reservation gets handed to two workers at once -- lives here, where
// it's tested directly against Postgres.
package expiry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// FindExpiredReservations returns up to limit reservation IDs that are
// still 'reserved' but whose expires_at has passed, oldest first. Uses
// the partial index built in Phase 3
// (idx_reservations_expiry_sweep ... WHERE status = 'reserved') so this
// stays cheap regardless of how many completed/expired/cancelled rows
// have piled up.
func FindExpiredReservations(ctx context.Context, db *sql.DB, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("expiry: limit must be positive, got %d", limit)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT id FROM reservations
		WHERE status = 'reserved' AND expires_at < now()
		ORDER BY expires_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("expiry: scanning for expired reservations: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("expiry: reading scan results: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("expiry: reading scan results: %w", err)
	}
	return ids, nil
}

// Outcome is the result of attempting to expire one reservation.
type Outcome struct {
	ReservationID string
	// Released is true only if THIS call actually performed the
	// reserved -> expired transition and gave the inventory back.
	ItemID   string
	Quantity int64
	Released bool
}

// ExpireReservation transitions one reservation from 'reserved' to
// 'expired' and releases its held quantity back to the item's
// available_inventory, in a single transaction.
//
// Safe to call concurrently on the same reservation ID from multiple
// workers, or after another worker already got to it: the UPDATE's
// WHERE clause (status = 'reserved' AND expires_at < now()) is the
// same "only one caller's write actually changes the row" guard
// decision.ProcessAttempt uses for idempotency, just via a conditional
// UPDATE instead of ON CONFLICT DO NOTHING. Whichever worker's UPDATE
// actually matches a row is the one that releases inventory; every
// other worker (or a stale scan result for a reservation that's since
// been paid) gets zero rows affected and Released=false, no inventory
// change, no error.
func ExpireReservation(ctx context.Context, db *sql.DB, reservationID string) (Outcome, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Outcome{}, fmt.Errorf("expiry: beginning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op if already committed

	var itemID string
	var quantity int64
	err = tx.QueryRowContext(ctx, `
		UPDATE reservations
		SET status = 'expired'
		WHERE id = $1 AND status = 'reserved' AND expires_at < now()
		RETURNING item_id, quantity
	`, reservationID).Scan(&itemID, &quantity)

	if errors.Is(err, sql.ErrNoRows) {
		// Nothing to do: either another worker already expired this
		// reservation, the buyer paid in the meantime (status moved to
		// 'completed'), or it genuinely isn't expired yet. All three
		// are a correct, silent no-op -- not an error.
		if err := tx.Commit(); err != nil {
			return Outcome{}, fmt.Errorf("expiry: committing no-op: %w", err)
		}
		return Outcome{ReservationID: reservationID, Released: false}, nil
	}
	if err != nil {
		return Outcome{}, fmt.Errorf("expiry: expiring reservation %s: %w", reservationID, err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE items SET available_inventory = available_inventory + $1 WHERE id = $2`,
		quantity, itemID,
	); err != nil {
		return Outcome{}, fmt.Errorf("expiry: releasing inventory for item %s: %w", itemID, err)
	}

	if err := tx.Commit(); err != nil {
		return Outcome{}, fmt.Errorf("expiry: committing: %w", err)
	}

	return Outcome{ReservationID: reservationID, ItemID: itemID, Quantity: quantity, Released: true}, nil
}
