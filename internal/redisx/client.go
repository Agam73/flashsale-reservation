// Package redisx centralizes the Redis wiring shared by waiting-room-api
// and checkout-api: connecting, key formats, and the atomic inventory
// operations both services need to agree on. It's named redisx rather
// than redis to avoid colliding with the imported github.com/redis/go-redis/v9
// package name in every file that uses both.
//
// Everything Postgres-authoritative (the real reservation/order rows)
// stays out of this package on purpose -- per the Phase 1 design
// decision, Redis here is fast, disposable, fast-path state, not the
// source of truth. decision-service (Phase 6) is what writes to
// Postgres.
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds what's needed to connect. Addr is required; the rest
// have sane defaults for local dev.
type Config struct {
	Addr         string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.DialTimeout == 0 {
		c.DialTimeout = 2 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = time.Second
	}
	return c
}

// NewClient connects to Redis and fails fast with a clear error if it's
// unreachable, rather than letting the first request-time command
// return a confusing connection error.
func NewClient(ctx context.Context, cfg Config) (*redis.Client, error) {
	cfg = cfg.withDefaults()

	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redisx: connecting to %s: %w", cfg.Addr, err)
	}

	return client, nil
}
