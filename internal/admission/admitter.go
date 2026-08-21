// Package admission implements a rate-limited, fair waiting-room admitter.
//
// Buyers call Join to enter the line. A single background goroutine owns
// the FIFO queue and admits one waiter per tick of an internal rate
// limiter -- so there's no mutex guarding the queue: only one goroutine
// ever touches it, and everyone else talks to it through channels.
package admission

import (
	"context"
	"time"
)

// joinRequest is what a caller sends to the admitter's loop when it wants
// to join the line.
type joinRequest struct {
	position chan int      // loop sends this request's queue position once known
	admitted chan struct{} // loop closes this when the request is admitted
}

// Admitter admits buyers into the sale at a fixed rate, first come,
// first served.
type Admitter struct {
	requests chan joinRequest
	rate     time.Duration
	done     chan struct{} // closed once the loop has exited
}

// NewAdmitter starts the admitter's background loop and returns a ready
// to use Admitter. The loop runs until ctx is cancelled. Call Shutdown to
// block until it has fully stopped.
func NewAdmitter(ctx context.Context, ratePerSecond int) *Admitter {
	a := &Admitter{
		requests: make(chan joinRequest),
		rate:     time.Second / time.Duration(ratePerSecond),
		done:     make(chan struct{}),
	}
	go a.run(ctx)
	return a
}

// Join enqueues the caller and blocks until either they're admitted or
// ctx is cancelled (e.g. the buyer's HTTP request disconnects). It
// returns the caller's queue position at the moment they joined.
//
// TODO(you): implement Join. It needs to:
//
//  1. Build a joinRequest with buffered (size 1) position and admitted
//     channels, so the loop's sends never block on a caller who already
//     gave up.
//  2. Send that request on a.requests -- in a select, also watching
//     ctx.Done(), so a caller can't block forever if the loop isn't
//     ready to receive.
//  3. Wait for a value on position (also selecting on ctx.Done()), then
//     return it along with the admitted channel so the caller can wait
//     on that separately.
func (a *Admitter) Join(ctx context.Context) (position int, admitted <-chan struct{}, err error) {
	panic("not implemented")
}

// run is the admitter's single owning goroutine. It is the only code in
// this package that ever touches the queue -- that's what makes it safe
// without a mutex.
//
// TODO(you): implement run. It needs to:
//
//  1. Hold a FIFO queue of joinRequest (a plain slice is fine).
//  2. Create a time.Ticker at a.rate and defer ticker.Stop().
//  3. Loop on a select with three cases:
//     - a new request arrives on a.requests: append it to the queue and
//     send it its position (len(queue)-1) on its position channel.
//     - the ticker fires: if the queue is non-empty, pop the front
//     request and close its admitted channel.
//     - ctx.Done(): stop looping, close a.done, and return. (Don't worry
//     about notifying requests still stuck in the queue -- Phase 8
//     covers shutdown semantics for in-flight work properly.)
func (a *Admitter) run(ctx context.Context) {
	panic("not implemented")
}

// Shutdown blocks until the admitter's loop has fully exited.
func (a *Admitter) Shutdown() {
	<-a.done
}
