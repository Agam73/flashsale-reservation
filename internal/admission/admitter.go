// Package admission implements a rate-limited, fair waiting-room admitter.
//
// Buyers call Join to enter the line. A single background goroutine owns
// the FIFO queue and admits one waiter per tick of an internal rate
// limiter -- so there's no mutex guarding the queue: only one goroutine
// ever touches it, and everyone else talks to it through channels.
package admission

import (
	"context"
	"errors"
	"time"
)

// errStopped is returned by Depth if the admitter's loop has already
// exited (ctx was cancelled) by the time a depth request would be sent
// -- there's nobody left to answer it.
var errStopped = errors.New("admission: admitter has stopped")

// joinRequest is what a caller sends to the admitter's loop when it wants
// to join the line.
type joinRequest struct {
	position chan int      // loop sends this request's queue position once known
	admitted chan struct{} // loop closes this when the request is admitted
}

// depthRequest is what a caller sends to the admitter's loop to ask how
// many buyers are currently waiting. Separate from joinRequest because
// it's a read-only snapshot, not an attempt to join -- Phase 9 adds
// this so waiting-room-api can publish queue depth into Redis (fast,
// disposable, derived state, same relationship Postgres/Redis has
// elsewhere in this project -- here the Admitter's own queue plays
// Postgres's role, and Redis just caches a number read from it).
type depthRequest struct {
	result chan int
}

// Admitter admits buyers into the sale at a fixed rate, first come,
// first served.
type Admitter struct {
	requests chan joinRequest
	depths   chan depthRequest
	rate     time.Duration
	done     chan struct{} // closed once the loop has exited
}

// NewAdmitter starts the admitter's background loop and returns a ready
// to use Admitter. The loop runs until ctx is cancelled. Call Shutdown to
// block until it has fully stopped.
func NewAdmitter(ctx context.Context, ratePerSecond int) *Admitter {
	a := &Admitter{
		requests: make(chan joinRequest),
		depths:   make(chan depthRequest),
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

// Depth returns the number of buyers currently waiting (already
// joined, not yet admitted). Unlike Join, it never blocks on
// admission -- it's a snapshot read of the loop's own queue length,
// answered by the one goroutine that's allowed to touch the queue, so
// it's always consistent with what Join/run actually see.
func (a *Admitter) Depth(ctx context.Context) (int, error) {
	req := depthRequest{result: make(chan int, 1)}

	select {
	case a.depths <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-a.done:
		return 0, errStopped
	}

	select {
	case n := <-req.result:
		return n, nil
	case <-ctx.Done():
		return 0, ctx.Err()
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

		case req := <-a.depths:
			req.result <- len(queue)

		case <-ctx.Done():
			return
		}
	}
}

// Shutdown blocks until the admitter's loop has fully exited.
func (a *Admitter) Shutdown() {
	<-a.done
}
