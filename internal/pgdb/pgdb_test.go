package pgdb

import (
	"context"
	"testing"
	"time"
)

func TestConfigWithDefaults(t *testing.T) {
	cfg := Config{DSN: "postgres://example"}.withDefaults()

	if cfg.MaxOpenConns != 10 {
		t.Errorf("expected default MaxOpenConns=10, got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 5 {
		t.Errorf("expected default MaxIdleConns=5, got %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != 30*time.Minute {
		t.Errorf("expected default ConnMaxLifetime=30m, got %s", cfg.ConnMaxLifetime)
	}
	if cfg.DSN != "postgres://example" {
		t.Errorf("expected DSN to be preserved untouched, got %q", cfg.DSN)
	}
}

func TestConfigWithDefaultsPreservesExplicitValues(t *testing.T) {
	cfg := Config{
		DSN:             "postgres://example",
		MaxOpenConns:    99,
		MaxIdleConns:    42,
		ConnMaxLifetime: time.Hour,
	}.withDefaults()

	if cfg.MaxOpenConns != 99 {
		t.Errorf("expected explicit MaxOpenConns=99 to be preserved, got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 42 {
		t.Errorf("expected explicit MaxIdleConns=42 to be preserved, got %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != time.Hour {
		t.Errorf("expected explicit ConnMaxLifetime=1h to be preserved, got %s", cfg.ConnMaxLifetime)
	}
}

// TestNewFailsFastOnUnreachablePostgres checks the actual promise New's
// doc comment makes: a bad connection fails with a clear error
// quickly, rather than the caller discovering it deep inside request
// handling. Port 1 is a reserved port nothing listens on, so this
// needs no live Postgres to run -- it runs (and matters) everywhere.
func TestNewFailsFastOnUnreachablePostgres(t *testing.T) {
	ctx := context.Background()
	start := time.Now()

	_, err := New(ctx, Config{DSN: "postgres://flashsale:flashsale@127.0.0.1:1/flashsale?sslmode=disable"})

	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected an error connecting to an unreachable Postgres, got nil")
	}
	if elapsed > 4*time.Second {
		t.Errorf("expected fast failure on connection refused, took %s -- did this hit the 5s ping timeout instead of failing fast?", elapsed)
	}
}

// TestNewRespectsParentContextCancellation checks an already-cancelled
// parent context is honored rather than silently ignored.
func TestNewRespectsParentContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(ctx, Config{DSN: "postgres://flashsale:flashsale@127.0.0.1:1/flashsale?sslmode=disable"})
	if err == nil {
		t.Fatal("expected an error when parent context is already cancelled, got nil")
	}
}
