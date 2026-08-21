# Build plan

Flash-sale / ticket reservation system: an event-driven backend built to
learn distributed systems, Kafka, Go, and AI service integration end to end.

Full architecture and requirements discussion lives in the design
conversation this repo came out of -- this doc tracks the concrete,
checkable path through it.

- [x] **Phase 1 -- Architecture & requirements**
      Requirements drafted (throughput, latency, delivery guarantee,
      consistency model, fairness rule, reservation TTL). Repo scaffolded.
- [ ] **Phase 2 -- Go fundamentals**
      Waiting-room admission logic: goroutines, channels, context,
      graceful shutdown. Built as real code for this project, not toy
      examples.
- [ ] **Phase 3 -- Postgres & data model**
      Schema for items, reservations, orders. Migrations.
- [ ] **Phase 4 -- Go API service**
      waiting-room-api + checkout-api over HTTP. Redis-only fast path,
      no Kafka yet.
- [ ] **Phase 5 -- Kafka fundamentals**
      Manually produce/consume messages via the CLI and Kafka UI. Build
      intuition for topics, partitions, offsets, consumer groups before
      writing a line of Go against them.
- [ ] **Phase 6 -- Producers/consumers**
      Real Kafka producer in checkout-api (partitioned by item ID). Real
      consumer in decision-service. This is where overselling gets
      prevented for real.
- [ ] **Phase 7 -- Worker architecture**
      expiry-worker as a proper worker-pool pattern.
- [ ] **Phase 8 -- Reliability & failure handling**
      Idempotency, retries, dead-letter topic. Then deliberately break
      things (kill a consumer mid-batch, stop Postgres, duplicate an
      event) and confirm the system recovers the way it's supposed to.
- [ ] **Phase 9 -- Redis**
      Atomic inventory counters, waiting-room queue state, risk-score
      cache.
- [ ] **Phase 10 -- FastAPI AI service**
      risk-service: consumes waiting-room events, scores bot/scalper
      risk asynchronously, writes to Redis.
- [ ] **Phase 11 -- Observability**
      Structured logs, Prometheus metrics, Grafana dashboards,
      OpenTelemetry traces, correlation IDs across every service.
- [ ] **Phase 12 -- Docker & deployment**
      Full docker-compose stack for all services (not just infra),
      health checks, graceful shutdown verified under `docker stop`.
- [ ] **Phase 13 -- Load testing & benchmarking**
      Simulate a burst of ~2,000 concurrent buyers against ~200 tickets.
      Measure checkout p99 latency and Kafka consumer lag under load.
- [ ] **Phase 14 -- System design / interview prep**
      Write up the design as a story: what problem it solves, the key
      tradeoffs (sync fast path vs. async authoritative path,
      partition-key choice, idempotency strategy), and what broke during
      testing and how it was fixed.

## Key design decisions locked in during Phase 1

- **Delivery guarantee:** at-least-once. All consumers must be idempotent
  by design -- not retrofitted later.
- **Source of truth:** Postgres. Redis is fast, disposable, derived state
  -- rebuildable from Postgres if lost.
- **Partition key:** item ID. Guarantees one ordered stream of purchase
  attempts per item, so exactly one consumer instance ever decides that
  item's outcome at a time.
- **AI placement:** the risk model runs asynchronously and writes to
  Redis. It is never called synchronously in the checkout hot path --
  that would trade fairness/latency for a marginal risk-scoring benefit.
- **Reservation TTL:** unpaid reservations expire and release inventory
  automatically (compensating-transaction pattern), rather than locking
  stock indefinitely.
