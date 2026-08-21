# internal/

Shared Go packages used across the `cmd/` services live here — things like
`internal/config`, `internal/kafka`, `internal/redis`, `internal/db`.

Nothing lives here yet. We add packages as later phases need them, rather
than pre-building an abstraction layer before we know what it needs to do.
