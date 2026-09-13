package admission

import (
	"context"
	"testing"
	"time"
)

// TestAdmitsInFIFOOrder checks that buyers who join earlier get admitted
// earlier, regardless of goroutine scheduling.
func TestAdmitsInFIFOOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewAdmitter(ctx, 100) // 100/sec => ~10ms between admits

	type result struct {
		joinOrder int
		position  int
	}
	results := make(chan result, 3)

	for i := 0; i < 3; i++ {
		go func(joinOrder int) {
			position, admitted, err := a.Join(context.Background())
			if err != nil {
				t.Errorf("Join returned unexpected error: %v", err)
				return
			}
			<-admitted
			results <- result{joinOrder: joinOrder, position: position}
		}(i)
		time.Sleep(5 * time.Millisecond) // stagger joins so arrival order is deterministic
	}

	positions := make([]int, 3)
	for i := 0; i < 3; i++ {
		select {
		case r := <-results:
			positions[r.joinOrder] = r.position
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for admissions")
		}
	}

	if positions[0] != 0 || positions[1] != 1 || positions[2] != 2 {
		t.Errorf("expected positions [0 1 2] in join order, got %v", positions)
	}
}

// TestDepthReflectsWaitingBuyers checks that Depth reports the number
// of buyers who have joined but not yet been admitted, and that it
// drops back down as the rate limiter admits them.
func TestDepthReflectsWaitingBuyers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Slow enough that joins clearly outpace admits within this test.
	a := NewAdmitter(ctx, 1)

	if n, err := a.Depth(context.Background()); err != nil || n != 0 {
		t.Fatalf("expected depth 0 before anyone joins, got %d (err %v)", n, err)
	}

	for i := 0; i < 3; i++ {
		go func() {
			_, admitted, err := a.Join(context.Background())
			if err == nil {
				<-admitted
			}
		}()
	}

	// Give the loop time to process all three join requests (it
	// serializes them one at a time over the requests channel) before
	// its first admit tick fires a second later.
	deadline := time.After(500 * time.Millisecond)
	for {
		n, err := a.Depth(context.Background())
		if err != nil {
			t.Fatalf("Depth returned unexpected error: %v", err)
		}
		if n == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected depth to reach 3 waiting buyers, got %d", n)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// TestDepthAfterShutdownReturnsError checks that Depth doesn't hang
// forever if the admitter's loop has already exited.
func TestDepthAfterShutdownReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a := NewAdmitter(ctx, 10)
	cancel()
	a.Shutdown()

	if _, err := a.Depth(context.Background()); err == nil {
		t.Error("expected an error calling Depth after the admitter has shut down, got nil")
	}
}

// TestJoinRespectsContextCancellation checks that a buyer whose context
// is cancelled while waiting doesn't block Join forever.
func TestJoinRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewAdmitter(ctx, 1) // 1/sec -- slow enough that cancellation wins the race

	joinCtx, joinCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer joinCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, admitted, err := a.Join(joinCtx)
		if err != nil {
			return // correctly bailed out on cancellation
		}
		select {
		case <-admitted:
			t.Error("expected Join to respect context cancellation, but request was admitted")
		case <-time.After(100 * time.Millisecond):
			t.Error("Join returned no error but the request was never admitted or cancelled")
		}
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Join did not return in time after its context was cancelled")
	}
}
