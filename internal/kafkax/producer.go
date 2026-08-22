package kafkax

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

// NewWriter returns a Writer that partitions by message key using
// kafka-go's Hash balancer -- the same "hash the key to a partition"
// strategy Kafka's own default Java client partitioner uses. This is
// not the kafka-go default (its Writer defaults to LeastBytes, which
// ignores keys entirely and just balances by message size); without
// explicitly setting this, every PurchaseAttempted event would scatter
// across partitions regardless of item ID, and decision-service's
// one-consumer-per-item guarantee would quietly stop holding.
func NewWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        CheckoutAttemptsTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
}

// PublishAttempt marshals and writes a PurchaseAttempted event, keyed
// by item ID. Blocks until the broker acknowledges the write (Async is
// false on the Writer returned by NewWriter) -- checkout-api treats a
// failed publish as a failed checkout, not a fire-and-forget best
// effort, since a lost event means a buyer's fast-path admission was
// never durably recorded.
func PublishAttempt(ctx context.Context, w *kafka.Writer, attempt PurchaseAttempted) error {
	value, err := json.Marshal(attempt)
	if err != nil {
		return fmt.Errorf("kafkax: marshaling attempt: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(attempt.ItemID),
		Value: value,
	}

	if err := w.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafkax: publishing attempt for item %s: %w", attempt.ItemID, err)
	}
	return nil
}

// NewIdempotencyKey returns a random, URL-safe identifier for one
// checkout attempt. Generated fresh per HTTP request in checkout-api --
// this is what reservations.idempotency_key ultimately stores, and
// what makes Kafka's at-least-once redelivery safe to process twice.
func NewIdempotencyKey() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("kafkax: generating idempotency key: %w", err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
