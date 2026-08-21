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
// returns the caller's queue position at the moment they joined, and the
// (already-closed, by the time Join returns successfully) admitted
// channel for the caller's own bookkeeping.
func (a *Admitter) Join(ctx context.Context) (position int, admitted <-chan struct{}, err error) {
	req := joinRequest{
		position: make(chan int, 1),
		admitted: make(chan struct{}),
	}

	select {
	case a.requests <- req:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}

	var pos int
	select {
	case pos = <-req.position:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}

	select {
	case <-req.admitted:
		return pos, req.admitted, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

// run is the admitter's single owning goroutine. It is the only code in
// this package that ever touches the queue -- that's what makes it safe
// without a mutex. It holds a FIFO queue, admits the front of the queue
// once per tick of the rate limiter, and exits when ctx is cancelled.
//
// Known limitation, deliberately unhandled here: a caller already
// blocked in Join's first select (trying to enqueue) with a ctx that
// never cancels will hang if this loop exits first, since nobody is left
// to receive on a.requests. Phase 8 covers proper shutdown semantics for
// in-flight work.
func (a *Admitter) run(ctx context.Context) {
	defer close(a.done)

	ticker := time.NewTicker(a.rate)
	defer ticker.Stop()

	var queue []joinRequest
	var nextPosition int // monotonic: len(queue) shrinks as people get admitted, this doesn't

	for {
		select {
		case req := <-a.requests:
			queue = append(queue, req)
			req.position <- nextPosition
			nextPosition++

		case <-ticker.C:
			if len(queue) > 0 {
				next := queue[0]
				queue = queue[1:]
				close(next.admitted)
			}

		case <-ctx.Done():
			return
		}
	}
}

// Shutdown blocks until the admitter's loop has fully exited.
func (a *Admitter) Shutdown() {
	<-a.done
}
