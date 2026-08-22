// Package pgdb centralizes Postgres connection setup, the same way
// internal/redisx centralizes Redis. decision-service is the only
// caller today, but expiry-worker (Phase 7) will need the same
// connect-and-fail-fast behavior.
package pgdb

import (
	"context"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"database/sql"
)

// Config holds what's needed to connect.
type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 10
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 5
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 30 * time.Minute
	}
	return c
}

// New opens a connection pool and fails fast with a clear error if
// Postgres isn't reachable, rather than letting the first query return
// a confusing connection error deep in request handling.
func New(ctx context.Context, cfg Config) (*sql.DB, error) {
	cfg = cfg.withDefaults()

	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("pgdb: opening connection: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("pgdb: connecting: %w", err)
	}

	return db, nil
}
