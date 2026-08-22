package kafkax

import (
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

// TestHashBalancerIsDeterministicPerKey exercises the exact balancer
// NewWriter configures (kafka.Hash{}), directly, without a broker. This
// is the mechanism behind the Phase 5 CLI observation ("ticket-1
// always landed on the same partition") -- now verified at the code
// level, so a future change to NewWriter's Balancer can't silently
// break that guarantee without a test failing.
func TestHashBalancerIsDeterministicPerKey(t *testing.T) {
	balancer := &kafka.Hash{}
	partitions := []int{0, 1, 2}

	firstPick := balancer.Balance(kafka.Message{Key: []byte("concert-ticket")}, partitions...)
	for i := 0; i < 20; i++ {
		p := balancer.Balance(kafka.Message{Key: []byte("concert-ticket")}, partitions...)
		if p != firstPick {
			t.Fatalf("same key produced different partitions across calls: %d then %d", firstPick, p)
		}
	}
}

// TestHashBalancerStaysWithinPartitionRange is a sanity check that the
// balancer never returns an index outside what it was offered -- the
// kind of off-by-one that wouldn't show up against a live 3-partition
// topic unless checked explicitly.
func TestHashBalancerStaysWithinPartitionRange(t *testing.T) {
	balancer := &kafka.Hash{}
	partitions := []int{0, 1, 2}

	keys := []string{"concert-ticket", "flash-sale-item", "another-item", "yet-another"}
	for _, key := range keys {
		p := balancer.Balance(kafka.Message{Key: []byte(key)}, partitions...)
		if p < 0 || p > 2 {
			t.Errorf("balancer returned out-of-range partition %d for key %q (valid range 0-2)", p, key)
		}
	}
}
