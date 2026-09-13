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
// Phase 3) remains the source of truth. internal/reconcile (Phase 9)
// is what keeps this key in sync with that row.
func InventoryKey(itemID string) string {
	return fmt.Sprintf("inventory:%s", itemID)
}

// QueueDepthKey holds the last-observed number of buyers waiting in
// itemID's waiting room. The in-memory Admitter (Phase 2) remains the
// actual source of truth for admission order; this is a disposable
// cache of its queue length, published whenever waiting-room-api reads
// it (Phase 9), so other services/tools can see roughly how busy an
// item's line is without talking to a specific waiting-room-api
// instance directly.
func QueueDepthKey(itemID string) string {
	return fmt.Sprintf("queue_depth:%s", itemID)
}

// RiskScoreKey holds the cached bot/scalper risk score for one buyer's
// pass through one item's waiting room. Written by risk-service
// (Phase 10); this phase (9) only builds the cache itself and the
// key format checkout-api and risk-service need to agree on.
func RiskScoreKey(itemID, userID string) string {
	return fmt.Sprintf("risk:%s:%s", itemID, userID)
}
