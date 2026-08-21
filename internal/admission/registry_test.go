package admission

import (
	"context"
	"testing"
	"time"
)

// TestRegistryReusesAdmitterPerItem checks that repeated calls to For
// with the same item ID return the same Admitter, so a buyer's queue
// position is tracked against one shared queue rather than a fresh one
// per request.
func TestRegistryReusesAdmitterPerItem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry(ctx, 100)

	a1 := reg.For("concert-ticket")
	a2 := reg.For("concert-ticket")
	if a1 != a2 {
		t.Error("expected the same Admitter instance for repeated calls with the same item ID")
	}
}

// TestRegistryIsolatesQueuesPerItem checks that two different items get
// independent queues -- a buyer joining item B shouldn't wait behind
// buyers who joined item A.
func TestRegistryIsolatesQueuesPerItem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Slow enough that if the items shared a queue, itemB's buyer would
	// visibly wait behind itemA's three buyers.
	reg := NewRegistry(ctx, 20) // ~50ms between admits

	for i := 0; i < 3; i++ {
		go func() {
			_, admitted, err := reg.For("itemA").Join(context.Background())
			if err == nil {
				<-admitted
			}
		}()
	}
	time.Sleep(10 * time.Millisecond) // let itemA's buyers enqueue first

	start := time.Now()
	position, admitted, err := reg.For("itemB").Join(context.Background())
	if err != nil {
		t.Fatalf("Join returned unexpected error: %v", err)
	}
	<-admitted
	elapsed := time.Since(start)

	if position != 0 {
		t.Errorf("expected itemB's first buyer to be position 0 in its own queue, got %d", position)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("itemB's buyer took %v to be admitted; queues should be isolated per item, not shared", elapsed)
	}
}

// TestRegistryShutdownWaitsForAllAdmitters checks that Shutdown doesn't
// return until every Admitter the registry created has actually
// stopped.
func TestRegistryShutdownWaitsForAllAdmitters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	reg := NewRegistry(ctx, 100)
	reg.For("itemA")
	reg.For("itemB")
	reg.For("itemC")

	cancel()

	done := make(chan struct{})
	go func() {
		reg.Shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Shutdown did not return after context cancellation")
	}
}
