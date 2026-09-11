package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/Agam73/flashsale-reservation/internal/decision"
	"github.com/Agam73/flashsale-reservation/internal/retry"
)

func TestIsConnectivityErrorTrueForPlainErrors(t *testing.T) {
	// A connection-refused / timeout / EOF style failure never reaches
	// Postgres at all, so it's never wrapped as *pq.Error -- it just
	// surfaces (wrapped by our own %w chain) as a plain error.
	err := fmt.Errorf("decision: beginning transaction: %w", errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"))
	if !isConnectivityError(err) {
		t.Error("expected a plain wrapped error to be classified as a connectivity error")
	}
}

func TestIsConnectivityErrorFalseForPqError(t *testing.T) {
	// A real response FROM Postgres (e.g. a serialization failure) is a
	// data/contention-level problem, not an infrastructure one.
	pqErr := &pq.Error{Code: "40001", Message: "could not serialize access due to concurrent update"}
	err := fmt.Errorf("decision: inserting reservation: %w", pqErr)
	if isConnectivityError(err) {
		t.Error("expected a *pq.Error to NOT be classified as a connectivity error")
	}
}

func TestClassifyOutcomeNilIsCommit(t *testing.T) {
	if got := classifyOutcome(nil, retry.DefaultPolicy(), 0, 0); got != outcomeCommit {
		t.Errorf("expected outcomeCommit for nil error, got %v", got)
	}
}

func TestClassifyOutcomeItemNotFoundIsImmediateDeadLetter(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", decision.ErrItemNotFound)
	// Even at attemptCount=0, elapsed=0, a data-level sentinel error
	// goes straight to the dead letter -- no retry budget spent on
	// something retrying can never fix.
	if got := classifyOutcome(err, retry.DefaultPolicy(), 0, 0); got != outcomeDeadLetter {
		t.Errorf("expected outcomeDeadLetter for ErrItemNotFound, got %v", got)
	}
}

func TestClassifyOutcomeInvalidQuantityIsImmediateDeadLetter(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", decision.ErrInvalidQuantity)
	if got := classifyOutcome(err, retry.DefaultPolicy(), 0, 0); got != outcomeDeadLetter {
		t.Errorf("expected outcomeDeadLetter for ErrInvalidQuantity, got %v", got)
	}
}

func TestClassifyOutcomeConnectivityIsUncountedRegardlessOfPolicyBudget(t *testing.T) {
	err := errors.New("dial tcp: connection refused")
	policy := retry.Policy{MaxAttempts: 5, MaxElapsed: time.Minute}

	// Even well past what the policy would normally allow, a
	// connectivity error must never dead-letter -- an extended
	// Postgres outage would otherwise dead-letter every single
	// in-flight message, none of which are actually bad.
	cases := []struct {
		attemptCount int
		elapsed      time.Duration
	}{
		{0, 0},
		{5, time.Minute},          // exactly at both caps
		{100, 24 * time.Hour},     // wildly past both caps
	}
	for _, c := range cases {
		if got := classifyOutcome(err, policy, c.attemptCount, c.elapsed); got != outcomeRetryUncounted {
			t.Errorf("attemptCount=%d elapsed=%s: expected outcomeRetryUncounted for a connectivity error, got %v", c.attemptCount, c.elapsed, got)
		}
	}
}

func TestClassifyOutcomeUnknownPqErrorRetriesThenDeadLettersPerPolicy(t *testing.T) {
	pqErr := &pq.Error{Code: "40001", Message: "could not serialize access due to concurrent update"}
	policy := retry.Policy{MaxAttempts: 3, MaxElapsed: time.Hour}

	// Below the policy's attempt cap: keep retrying.
	if got := classifyOutcome(pqErr, policy, 0, 0); got != outcomeRetryCounted {
		t.Errorf("attemptCount=0: expected outcomeRetryCounted, got %v", got)
	}
	if got := classifyOutcome(pqErr, policy, 2, 0); got != outcomeRetryCounted {
		t.Errorf("attemptCount=2: expected outcomeRetryCounted, got %v", got)
	}

	// At the policy's attempt cap: give up.
	if got := classifyOutcome(pqErr, policy, 3, 0); got != outcomeDeadLetter {
		t.Errorf("attemptCount=3 (== MaxAttempts): expected outcomeDeadLetter, got %v", got)
	}
}

func TestClassifyOutcomeUnknownPqErrorDeadLettersOnElapsedCapEvenWithAttemptsLeft(t *testing.T) {
	pqErr := &pq.Error{Code: "40001", Message: "could not serialize access due to concurrent update"}
	policy := retry.Policy{MaxAttempts: 100, MaxElapsed: time.Minute}

	// Attempt count is nowhere near its cap, but elapsed time is --
	// the policy's OR-of-two-caps behavior (already tested in
	// internal/retry) should still apply here through classifyOutcome.
	if got := classifyOutcome(pqErr, policy, 2, 2*time.Minute); got != outcomeDeadLetter {
		t.Errorf("expected outcomeDeadLetter when the elapsed cap is hit, even with attempts remaining, got %v", got)
	}
}
