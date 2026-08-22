package kafkax

import (
	"encoding/json"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func TestNewIdempotencyKeyIsUniqueAndWellFormed(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		key, err := NewIdempotencyKey()
		if err != nil {
			t.Fatalf("NewIdempotencyKey: %v", err)
		}
		if key == "" {
			t.Fatal("expected a non-empty key")
		}
		if seen[key] {
			t.Fatalf("generated a duplicate key after %d iterations: %s", i, key)
		}
		seen[key] = true
	}
}

func TestPurchaseAttemptedJSONRoundTrip(t *testing.T) {
	original := PurchaseAttempted{
		ItemID:         "concert-ticket",
		UserID:         "alice",
		Quantity:       2,
		IdempotencyKey: "abc-123",
		AttemptedAt:    time.Now().UTC().Truncate(time.Second),
	}

	value, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshaling: %v", err)
	}

	decoded, err := DecodeAttempt(value)
	if err != nil {
		t.Fatalf("DecodeAttempt: %v", err)
	}

	if decoded != original {
		t.Errorf("round trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestDecodeAttemptRejectsMalformedJSON(t *testing.T) {
	_, err := DecodeAttempt([]byte("not json"))
	if err == nil {
		t.Error("expected an error decoding malformed JSON, got nil")
	}
}

// TestNewWriterAndReaderConstructWithoutDialing checks that building
// these objects doesn't itself require a reachable broker -- kafka-go
// connects lazily on first actual use (WriteMessages / FetchMessage),
// not at construction time. This can't verify wire-level behavior
// against a real broker (no Kafka available in this environment); it
// only proves the wrapper functions build valid, correctly-configured
// clients.
func TestNewWriterAndReaderConstructWithoutDialing(t *testing.T) {
	w := NewWriter([]string{"localhost:9092"})
	defer w.Close()
	if w.Topic != CheckoutAttemptsTopic {
		t.Errorf("expected writer topic %q, got %q", CheckoutAttemptsTopic, w.Topic)
	}
	if _, ok := w.Balancer.(*kafka.Hash); !ok {
		t.Errorf("expected Balancer to be *kafka.Hash (key-based partitioning), got %T -- without this, same-item events would scatter across partitions", w.Balancer)
	}
	if w.RequiredAcks != kafka.RequireOne {
		t.Errorf("expected RequiredAcks=RequireOne, got %v", w.RequiredAcks)
	}

	r := NewReader([]string{"localhost:9092"}, "test-group")
	defer r.Close()
	cfg := r.Config()
	if cfg.Topic != CheckoutAttemptsTopic {
		t.Errorf("expected reader topic %q, got %q", CheckoutAttemptsTopic, cfg.Topic)
	}
	if cfg.GroupID != "test-group" {
		t.Errorf("expected reader group %q, got %q", "test-group", cfg.GroupID)
	}
}
