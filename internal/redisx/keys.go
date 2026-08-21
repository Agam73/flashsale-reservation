package redisx

import "fmt"

// Key formats live here, in one place, because waiting-room-api writes
// admission keys and checkout-api reads them -- if the two services
// built these strings independently they'd eventually drift out of
// sync in a way that fails silently (checkout-api just never finding a
// token that's actually there under a slightly different key).

// AdmissionKey is where waiting-room-api records that a user has been
// let through the waiting room for a given item. checkout-api checks
// for this key's existence (and deletes it, single-use) before doing
// the fast-path inventory check.
func AdmissionKey(itemID, userID string) string {
	return fmt.Sprintf("admission:%s:%s", itemID, userID)
}

// InventoryKey holds the fast-path available-inventory counter for an
// item. This is Redis's own copy for the synchronous hot path -- per
// the Phase 1 design decision, Postgres (items.available_inventory,
// Phase 3) remains the source of truth. Nothing in this phase seeds
// this key from Postgres yet; that reconciliation is Phase 9's job.
func InventoryKey(itemID string) string {
	return fmt.Sprintf("inventory:%s", itemID)
}
