package admission

import (
	"context"
	"sync"
)

// Registry lazily creates and reuses one Admitter per item ID, so each
// flash-sale item gets its own independent FIFO queue and admission
// rate instead of buyers for different items competing in the same
// line.
//
// This is additive to admitter.go: the Admitter type and its single-
// queue behavior are untouched. Registry just owns a map of them.
//
// Known simplification: every Admitter created by a Registry shares the
// same ratePerSecond. Per-item rates (e.g. read from Postgres) are a
// later-phase concern, once cmd/waiting-room-api has a reason to talk
// to the database.
type Registry struct {
	ctx           context.Context
	ratePerSecond int

	mu        sync.Mutex
	admitters map[string]*Admitter
}

// NewRegistry returns a Registry whose Admitters all run under ctx with
// the given rate. Cancel ctx to stop every Admitter the registry has
// created (past and future); call Shutdown afterwards to block until
// they've all actually exited.
func NewRegistry(ctx context.Context, ratePerSecond int) *Registry {
	return &Registry{
		ctx:           ctx,
		ratePerSecond: ratePerSecond,
		admitters:     make(map[string]*Admitter),
	}
}

// For returns the Admitter for itemID, starting it on first use. Later
// calls for the same itemID return the same Admitter, so its queue and
// position counter persist across requests.
func (reg *Registry) For(itemID string) *Admitter {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if a, ok := reg.admitters[itemID]; ok {
		return a
	}

	a := NewAdmitter(reg.ctx, reg.ratePerSecond)
	reg.admitters[itemID] = a
	return a
}

// Shutdown blocks until every Admitter created so far has fully
// stopped. Callers should cancel the registry's context first, or this
// blocks forever.
func (reg *Registry) Shutdown() {
	reg.mu.Lock()
	admitters := make([]*Admitter, 0, len(reg.admitters))
	for _, a := range reg.admitters {
		admitters = append(admitters, a)
	}
	reg.mu.Unlock()

	for _, a := range admitters {
		a.Shutdown()
	}
}
