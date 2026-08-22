package kafkax

import (
	"encoding/json"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

// NewReader returns a Reader belonging to the given consumer group,
// subscribed to CheckoutAttemptsTopic. Multiple decision-service
// instances started with the same groupID have the topic's partitions
// split between them automatically by Kafka -- Phase 5's
// consumer-group behavior, now driving real code instead of the CLI.
func NewReader(brokers []string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    CheckoutAttemptsTopic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
}

// DecodeAttempt parses a Kafka message's value into a
// PurchaseAttempted. A decode failure is treated as a permanent,
// non-retryable error by the caller (see cmd/decision-service) --
// there's no dead-letter topic yet (that's Phase 8), so for now a
// malformed message gets logged loudly and its offset committed anyway
// rather than blocking the partition forever.
func DecodeAttempt(value []byte) (PurchaseAttempted, error) {
	var attempt PurchaseAttempted
	if err := json.Unmarshal(value, &attempt); err != nil {
		return PurchaseAttempted{}, fmt.Errorf("kafkax: decoding attempt: %w", err)
	}
	return attempt, nil
}
