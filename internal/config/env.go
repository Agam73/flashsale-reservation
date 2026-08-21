// Package config holds small, dependency-free helpers for reading
// configuration from environment variables with a fallback default.
// Each service's main.go calls these directly rather than every
// service reinventing "parse this env var as an int, or use N".
package config

import (
	"log"
	"os"
	"strconv"
)

// String returns the environment variable's value, or fallback if unset
// or empty.
func String(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Int returns the environment variable parsed as an int, or fallback if
// unset, empty, or not a valid integer (in which case it logs a warning
// rather than crashing the service over a typo'd env var).
func Int(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("config: %s=%q is not a valid integer, using default %d", key, v, fallback)
		return fallback
	}
	return n
}
