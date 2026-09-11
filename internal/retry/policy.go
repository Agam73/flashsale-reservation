// Package retry holds a small, reusable retry-timing policy: how many
// attempts, for how long, before giving up, and how long to wait
// between attempts. It knows nothing about Kafka, Postgres, or what
// "giving up" means to a caller -- that's deliberate. Decisions like
// "should this specific kind of failure even count against the
// budget" belong to the caller (see cmd/decision-service's
// classifyOutcome), not here.
package retry

import "time"

// Policy bounds how long a caller keeps retrying something, along two
// independent dimensions -- whichever cap is hit first wins. Zero
// value for either field means that dimension never caps (only makes
// sense if the other one does).
type Policy struct {
	MaxAttempts int
	MaxElapsed  time.Duration
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// DefaultPolicy is a reasonable starting point: up to 5 attempts, no
// longer than 2 minutes total, starting at a 1 second backoff and
// doubling up to a 30 second cap.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 5,
		MaxElapsed:  2 * time.Minute,
		BaseBackoff: time.Second,
		MaxBackoff:  30 * time.Second,
	}
}

// ShouldRetry reports whether another attempt should be made, given how
// many attempts have happened so far (starting at 1 for the first,
// already-made attempt) and how much time has elapsed since the first
// attempt began. Either cap being reached is enough to stop -- a
// caller that wants only one dimension to matter should leave the
// other at its zero value.
func (p Policy) ShouldRetry(attemptCount int, elapsed time.Duration) bool {
	if p.MaxAttempts > 0 && attemptCount >= p.MaxAttempts {
		return false
	}
	if p.MaxElapsed > 0 && elapsed >= p.MaxElapsed {
		return false
	}
	return true
}

// NextBackoff returns exponential backoff for the given 1-indexed
// attempt number, doubling from BaseBackoff and capped at MaxBackoff.
// Falls back to sane defaults (1s base, 30s cap) if a Policy was
// constructed without setting them -- a Policy built just to express
// MaxAttempts/MaxElapsed (as in several tests) shouldn't need to also
// think about backoff pacing to remain usable.
func (p Policy) NextBackoff(attemptCount int) time.Duration {
	base := p.BaseBackoff
	if base <= 0 {
		base = time.Second
	}
	maxBackoff := p.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	backoff := base
	for i := 1; i < attemptCount; i++ {
		backoff *= 2
		if backoff >= maxBackoff {
			return maxBackoff
		}
	}
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}
