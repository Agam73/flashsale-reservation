package retry

import (
	"testing"
	"time"
)

func TestDefaultPolicyValues(t *testing.T) {
	p := DefaultPolicy()
	if p.MaxAttempts != 5 {
		t.Errorf("expected MaxAttempts=5, got %d", p.MaxAttempts)
	}
	if p.MaxElapsed != 2*time.Minute {
		t.Errorf("expected MaxElapsed=2m, got %s", p.MaxElapsed)
	}
	if p.BaseBackoff != time.Second {
		t.Errorf("expected BaseBackoff=1s, got %s", p.BaseBackoff)
	}
	if p.MaxBackoff != 30*time.Second {
		t.Errorf("expected MaxBackoff=30s, got %s", p.MaxBackoff)
	}
}

func TestShouldRetryWithinBothCaps(t *testing.T) {
	p := Policy{MaxAttempts: 5, MaxElapsed: time.Minute}
	if !p.ShouldRetry(2, 10*time.Second) {
		t.Error("expected ShouldRetry to be true when under both caps")
	}
}

func TestShouldRetryFalseAtAttemptCap(t *testing.T) {
	p := Policy{MaxAttempts: 5, MaxElapsed: time.Hour}
	if p.ShouldRetry(5, 0) {
		t.Error("expected ShouldRetry to be false once attemptCount reaches MaxAttempts")
	}
	if p.ShouldRetry(6, 0) {
		t.Error("expected ShouldRetry to be false once attemptCount exceeds MaxAttempts")
	}
}

func TestShouldRetryFalseAtElapsedCapEvenWithAttemptsRemaining(t *testing.T) {
	p := Policy{MaxAttempts: 100, MaxElapsed: time.Minute}
	if p.ShouldRetry(1, time.Minute) {
		t.Error("expected ShouldRetry to be false once elapsed reaches MaxElapsed, regardless of attempt count")
	}
}

func TestShouldRetryZeroValueMeansNoCapOnThatDimension(t *testing.T) {
	// MaxAttempts unset (0): only MaxElapsed should matter.
	p := Policy{MaxElapsed: time.Minute}
	if !p.ShouldRetry(1000, 0) {
		t.Error("expected ShouldRetry to ignore attempt count when MaxAttempts is 0")
	}
	if p.ShouldRetry(0, time.Hour) {
		t.Error("expected ShouldRetry to still respect MaxElapsed")
	}
}

func TestNextBackoffDoublesAndCaps(t *testing.T) {
	p := Policy{BaseBackoff: time.Second, MaxBackoff: 8 * time.Second}
	cases := map[int]time.Duration{
		1: 1 * time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,  // hits the cap
		5: 8 * time.Second,  // stays capped
		9: 8 * time.Second,
	}
	for attempt, want := range cases {
		if got := p.NextBackoff(attempt); got != want {
			t.Errorf("attempt=%d: expected %s, got %s", attempt, want, got)
		}
	}
}

func TestNextBackoffFallsBackToDefaultsWhenUnset(t *testing.T) {
	// A Policy built just to express MaxAttempts/MaxElapsed (common in
	// tests elsewhere in this repo) shouldn't produce a zero or
	// negative backoff just because BaseBackoff/MaxBackoff were never
	// set.
	p := Policy{MaxAttempts: 5, MaxElapsed: time.Minute}
	backoff := p.NextBackoff(1)
	if backoff <= 0 {
		t.Errorf("expected a positive backoff even with BaseBackoff unset, got %s", backoff)
	}
	if backoff != time.Second {
		t.Errorf("expected the 1s default base backoff, got %s", backoff)
	}
}
