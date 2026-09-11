package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// DeadLetterTopic holds messages decision-service gave up on: ones
// that failed to even decode, and ones whose processing error was
// classified as permanent (not just a transient infrastructure blip)
// after exhausting the retry policy's budget. See docs/phase8.md for
// the classification logic. One partition is enough -- nothing here
// needs per-item ordering, it's an audit/inspection log, not a work
// queue.
const DeadLetterTopic = "checkout-attempts-dlq"

// DeadLetter is what lands on DeadLetterTopic. The Original* fields
// carry the source message verbatim (not just a parsed
// PurchaseAttempted) specifically so a message that failed to decode
// at all is still fully recoverable for inspection or manual replay --
// a human reading the DLQ later shouldn't need to already know why a
// message was bad just to see what it was.
type DeadLetter struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	OriginalKey       string    `json:"original_key"`
	OriginalValue     string    `json:"original_value"`
	Reason            string    `json:"reason"`
	AttemptCount      int       `json:"attempt_count"`
	FailedAt          time.Time `json:"failed_at"`
}

// NewDLQWriter returns a Writer for DeadLetterTopic. Deliberately no
// Balancer/key-partitioning configuration the way NewWriter has for
// CheckoutAttemptsTopic -- there's no per-item ordering requirement
// here, so the default balancer is fine.
func NewDLQWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        DeadLetterTopic,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
}

// PublishDeadLetter marshals and writes a DeadLetter, keyed by the
// original message's key when known (so all of one item's
// dead-lettered messages land together in the topic, purely for
// convenience when inspecting it -- not a correctness requirement).
func PublishDeadLetter(ctx context.Context, w *kafka.Writer, dl DeadLetter) error {
	value, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("kafkax: marshaling dead letter: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(dl.OriginalKey),
		Value: value,
	}

	if err := w.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafkax: publishing dead letter: %w", err)
	}
	return nil
}
