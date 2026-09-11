package kafkax

import (
	"encoding/json"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func TestDeadLetterJSONRoundTrip(t *testing.T) {
	original := DeadLetter{
		OriginalTopic:     CheckoutAttemptsTopic,
		OriginalPartition: 2,
		OriginalOffset:    42,
		OriginalKey:       "concert-ticket",
		OriginalValue:     `{"item_id":"concert-ticket","user_id":"alice"}`,
		Reason:            "decision: item not found: concert-ticket",
		AttemptCount:      1,
		FailedAt:          time.Now().UTC().Truncate(time.Second),
	}

	value, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshaling: %v", err)
	}

	var decoded DeadLetter
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatalf("unmarshaling: %v", err)
	}

	if decoded != original {
		t.Errorf("round trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestNewDLQWriterConstructsWithoutDialing(t *testing.T) {
	w := NewDLQWriter([]string{"localhost:9092"})
	defer w.Close()
	if w.Topic != DeadLetterTopic {
		t.Errorf("expected topic %q, got %q", DeadLetterTopic, w.Topic)
	}
	if w.RequiredAcks != kafka.RequireOne {
		t.Errorf("expected RequiredAcks=RequireOne, got %v", w.RequiredAcks)
	}
}
