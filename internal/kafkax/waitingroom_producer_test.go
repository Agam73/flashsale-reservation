package kafkax

import (
	"encoding/json"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func TestBuyerJoinedJSONRoundTrip(t *testing.T) {
	original := BuyerJoined{
		ItemID:     "concert-ticket",
		UserID:     "alice",
		RemoteAddr: "203.0.113.7:54321",
		JoinedAt:   time.Now().UTC().Truncate(time.Second),
	}

	value, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshaling: %v", err)
	}

	decoded, err := DecodeBuyerJoined(value)
	if err != nil {
		t.Fatalf("DecodeBuyerJoined: %v", err)
	}

	if decoded != original {
		t.Errorf("round trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestDecodeBuyerJoinedRejectsMalformedJSON(t *testing.T) {
	_, err := DecodeBuyerJoined([]byte("not json"))
	if err == nil {
		t.Error("expected an error decoding malformed JSON, got nil")
	}
}

// TestNewWaitingRoomWriterConstructsWithoutDialing mirrors
// TestNewWriterAndReaderConstructWithoutDialing: it can only check
// that the Writer is configured correctly, not that it can actually
// reach a broker (none is available in this environment).
func TestNewWaitingRoomWriterConstructsWithoutDialing(t *testing.T) {
	w := NewWaitingRoomWriter([]string{"localhost:9092"})
	defer w.Close()

	if w.Topic != WaitingRoomJoinsTopic {
		t.Errorf("expected writer topic %q, got %q", WaitingRoomJoinsTopic, w.Topic)
	}
	if _, ok := w.Balancer.(*kafka.Hash); !ok {
		t.Errorf("expected Balancer to be *kafka.Hash (key-based partitioning), got %T", w.Balancer)
	}
	if w.RequiredAcks != kafka.RequireOne {
		t.Errorf("expected RequiredAcks=RequireOne, got %v", w.RequiredAcks)
	}
}
