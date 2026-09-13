package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// NewWaitingRoomWriter returns a Writer for WaitingRoomJoinsTopic,
// partitioned by user ID rather than item ID (see BuyerJoined's doc
// comment for why the key differs from NewWriter's). If risk-service
// is ever run as more than one instance, keying by user means one
// user's events always land on the same partition and get processed
// in order by the same consumer instance -- useful for the
// sliding-window join-velocity tracking risk-service does per user,
// without needing cross-instance coordination.
//
// Same retry settings as NewWriter (Phase 8): a handful of retries
// with backoff before a produce failure is surfaced to the caller.
func NewWaitingRoomWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:            kafka.TCP(brokers...),
		Topic:           WaitingRoomJoinsTopic,
		Balancer:        &kafka.Hash{},
		RequiredAcks:    kafka.RequireOne,
		Async:           false,
		MaxAttempts:     5,
		WriteBackoffMin: 100 * time.Millisecond,
		WriteBackoffMax: 2 * time.Second,
	}
}

// PublishBuyerJoined marshals and writes a BuyerJoined event, keyed by
// user ID. Unlike PublishAttempt, a failure here is not treated as a
// failure of the operation it's attached to: waiting-room-api calls
// this from a background goroutine after a buyer is already admitted
// (see cmd/waiting-room-api), because risk scoring is a secondary,
// asynchronous signal per the Phase 1 design decision -- it must never
// be able to slow down or block the actual admission a buyer is
// waiting on.
func PublishBuyerJoined(ctx context.Context, w *kafka.Writer, event BuyerJoined) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("kafkax: marshaling buyer-joined event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.UserID),
		Value: value,
	}

	if err := w.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafkax: publishing buyer-joined event for user %s: %w", event.UserID, err)
	}
	return nil
}

// DecodeBuyerJoined parses a Kafka message's value into a BuyerJoined.
// Go has no consumer for this topic (risk-service, Python, is the
// consumer) -- this exists for symmetry with DecodeAttempt and so this
// package's own tests can verify the wire format without a second
// language involved.
func DecodeBuyerJoined(value []byte) (BuyerJoined, error) {
	var event BuyerJoined
	if err := json.Unmarshal(value, &event); err != nil {
		return BuyerJoined{}, fmt.Errorf("kafkax: decoding buyer-joined event: %w", err)
	}
	return event, nil
}
