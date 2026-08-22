// Package kafkax centralizes the Kafka wiring shared by checkout-api
// (producer) and decision-service (consumer): the topic name, the
// event schema, and how messages get keyed/partitioned -- the same
// role internal/redisx plays for Redis. It's named kafkax rather than
// kafka to avoid colliding with the imported package name from
// segmentio/kafka-go.
package kafkax

import "time"

// CheckoutAttemptsTopic holds every PurchaseAttempted event. It must
// be created with more than one partition before either service runs
// (see docs/phase6.md) -- if Kafka auto-creates it on first use, it
// gets the broker's default partition count, which defeats the whole
// point of partitioning by item ID.
const CheckoutAttemptsTopic = "checkout-attempts"

// PurchaseAttempted is what checkout-api publishes after its Redis
// fast path admits a purchase, and what decision-service consumes to
// make the durable, authoritative decision.
//
// ItemID doubles as the Kafka message key (see NewWriter's Balancer).
// Every attempt for the same item lands on the same partition, which
// is what lets decision-service rely on one consumer instance
// processing a given item's attempts in order -- the actual mechanism
// behind the Phase 1 "partition key = item ID" design decision.
type PurchaseAttempted struct {
	ItemID         string    `json:"item_id"`
	UserID         string    `json:"user_id"`
	Quantity       int64     `json:"quantity"`
	IdempotencyKey string    `json:"idempotency_key"`
	AttemptedAt    time.Time `json:"attempted_at"`
}
