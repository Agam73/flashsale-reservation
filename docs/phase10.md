# Phase 10 — FastAPI AI service

The phase plan describes this phase in one sentence: risk-service
consumes waiting-room events, scores bot/scalper risk asynchronously,
writes to Redis. Actually building it surfaced a gap immediately --
"waiting-room events" didn't exist. Through Phase 9, waiting-room-api
had never published anything to Kafka at all; it only ever touched
Redis and its own in-memory `admission.Admitter`. So this phase is two
pieces, not one: giving waiting-room-api something to publish, and
building the service that consumes it.

## The missing event (Go side)

**`internal/kafkax/waitingroom_event.go`** defines `BuyerJoined` and
`WaitingRoomJoinsTopic`. `waiting-room-api` publishes one `BuyerJoined`
event per buyer *admitted* off the queue (not per join request --
publishing at request time would mean scoring buyers who never
actually made it off the queue, which risk-service has no use for).

```go
type BuyerJoined struct {
	ItemID     string
	UserID     string
	RemoteAddr string // from r.RemoteAddr, not client-controlled
	JoinedAt   time.Time
}
```

`RemoteAddr` is the one new piece of data collected here -- a cheap,
widely-used signal for spotting many identities driven from one
machine, with the usual caveat that it's unreliable behind NAT or a
shared proxy. It's server-observed, not taken from the request body,
so a client can't spoof it.

The topic is keyed by user ID (`internal/kafkax/waitingroom_producer.go`
-- `NewWaitingRoomWriter`), unlike `CheckoutAttemptsTopic`'s item-ID
keying: if risk-service is ever scaled to more than one instance,
keying by user means one user's events always land on the same
partition and get processed in order by the same instance, which is
what the sliding-window join-counting below actually needs.

### Publishing without blocking admission

This is the one correctness property that actually matters in this
half of the phase, spelled out explicitly because it's easy to get
wrong by accident: **publishing this event must never be able to slow
down or block a buyer's admission.** Per the Phase 1 design decision,
risk scoring is asynchronous and secondary; a buyer waiting in line
must not wait one millisecond longer because Kafka is slow or down.

`handleJoin` grants the admission token, builds the `joinResponse`,
and only *then* fires the publish -- in a background goroutine, with
its own 5-second timeout, tracked by a `sync.WaitGroup` so shutdown can
wait for in-flight publishes before closing the writer out from under
them. The HTTP response never touches this goroutine at all.

This is verified directly, not just asserted:
`TestHandleJoin_AdmitsEvenWhenKafkaUnreachable` points the writer at a
closed port and confirms `handleJoin` still returns a 200 in about
0.1s -- indistinguishable from the broker being healthy, from the
buyer's point of view.

`scripts/setup_kafka_topic.sh` now also creates
`waiting-room-joins` alongside the existing two topics.

## risk-service (Python side)

`services/risk-service` was a `/healthz`-only stub through Phase 9.
This phase gives it an actual job:

```
config.py        env vars, same names as the Go services (REDIS_ADDR,
                  KAFKA_BROKERS) so one .env configures everything
schemas.py        BuyerJoined, field-for-field compatible with the Go
                  struct
redis_client.py   the risk-score cache -- same risk:{item}:{user} key
                  format as internal/redisx/risk.go (Phase 9)
risk_window.py     two Redis-sorted-set sliding windows: joins per
                  user, distinct users per IP
scoring.py        pure function turning window counts into a score
consumer.py       process_event/_handle_message (testable, no Kafka
                  needed) + run() (the actual aiokafka consume loop)
main.py           FastAPI app: /healthz, a debug read endpoint, and a
                  lifespan that starts/stops the consumer as a
                  background task
```

Split the same way `internal/decision` is split from
`cmd/decision-service`: everything that matters for correctness
(`process_event`, the window counters, the scoring function) is a
plain function tested directly against real Redis; only `run()` touches
`AIOKafkaConsumer`, and nothing in this environment can test that
against a real broker (no Docker, no reachable Kafka mirror -- the
same gap `docs/phase6.md` and `docs/phase8.md` already noted for the Go
side).

### Cross-language wire compatibility, actually verified

Both languages have to agree on the JSON shape of `BuyerJoined` and the
Redis key format of a cached score. Rather than trust that by
inspection, this was checked directly: a real Go program marshaled a
`BuyerJoined` value, and the exact bytes it produced --
`{"item_id":"concert-ticket","user_id":"alice","remote_addr":"203.0.113.7:54321","joined_at":"2026-09-13T11:33:37.785109783Z"}`
-- were fed straight into `schemas.BuyerJoined.model_validate_json`
and then through `consumer._handle_message`, ending in a real score
written to a real Redis under `risk:concert-ticket:alice`. Nanosecond
timestamp precision from Go collapses to microsecond precision in
Python's `datetime`, which is expected and irrelevant here -- nothing
about this event needs sub-microsecond resolution.

### The scoring heuristic (and a bug caught while testing it)

`scoring.py` combines two signals -- see its module doc for the full
reasoning -- by taking whichever is worse, not an average, since
averaging would let one severe signal get diluted by an unremarkable
one. It's explicitly a heuristic: risk-service is a "FastAPI AI
service" per the phase plan, but there's no labeled data anywhere in
this project to train an actual model on, and a heuristic that says
exactly why a score came out the way it did is more useful than a
model dressed up to look more rigorous than it is.

The first version of this heuristic had a real bug, caught by its own
test suite rather than by inspection: `count / threshold` scores a
buyer's very first, completely normal join as `1/threshold` instead of
0, because the join itself was being counted as partial evidence of
suspicion. One person joining once from one IP is exactly what a real
buyer looks like -- it should never carry a nonzero baseline score.
The fix ramps from `(count - 1)`, so a lone occurrence always scores 0
and the climb toward 1.0 only starts once something actually repeats.
`test_process_event_writes_a_score` is what caught the original
`0.333` instead of `0.0`.

### What this doesn't do

- **Nothing consumes a risk score to change a decision.** Per the
  Phase 1 design decision, that would mean calling into risk-service
  (or reading its Redis output) from checkout-api's hot path -- exactly
  what "the risk model runs asynchronously ... never called
  synchronously in the checkout hot path" rules out. This phase builds
  the producer, the scorer, and the shared cache format; consuming the
  score is deliberately not in scope.
- **No DLQ for risk-service.** Unlike `checkout-attempts`, nothing
  downstream depends on every `BuyerJoined` event being scored -- a
  buyer who never gets one just reads as "unknown" via
  `get_risk_score`, which is already the safe default. A malformed
  message is logged and skipped (`consumer._handle_message`), not
  dead-lettered; building DLQ machinery for a best-effort side signal
  would be effort spent on the wrong priority.
- **Single risk-service instance assumed.** The user-keyed partitioning
  scheme would support more than one consumer instance without
  cross-instance coordination, but nothing here actually runs more
  than one, and `docker-compose.yml` doesn't have a `risk-service`
  entry yet -- that's Phase 12's job ("Full docker-compose stack for
  all services"), not this one's.
- **IP-based signals are exactly as unreliable as IP-based signals
  always are** -- shared corporate NAT, mobile carrier-grade NAT, and
  VPNs all make "many users, one IP" ambiguous. `risk_window.py`'s doc
  comment says this plainly rather than pretending it's a stronger
  signal than it is.

## Testing

Go: `TestBuyerJoinedJSONRoundTrip`, `TestNewWaitingRoomWriterConstructsWithoutDialing`,
`TestHandleJoin_AdmitsEvenWhenKafkaUnreachable` (proves publishing
can't block admission), and `TestHandleJoin_PublishesReadableBuyerJoinedEvent`
(proves a real, correctly-keyed `BuyerJoined` message actually lands on
the topic when a broker is reachable -- mirrors `checkout-api`'s own
`TestHandleCheckout_SuccessPublishesToKafka` pattern exactly: scan
forward from `FirstOffset` on a fresh consumer group, match by
idempotent field rather than assuming message order). Both
Kafka-dependent tests skip cleanly via a `requireKafka` helper (a quick
TCP dial with a timeout) rather than hanging or failing outright if no
broker is reachable -- `TestHandleCheckout_SuccessPublishesToKafka`
didn't do this before Phase 10 despite the comment above it claiming
otherwise; fixed as part of this phase since `waiting-room-api`'s new
test needed the same skip behavior anyway.

Neither of these two tests could actually be run against a real broker
in the environment this phase was built in -- no Docker, no reachable
Apache mirror, the same gap `docs/phase6.md` and `docs/phase8.md`
already documented. They're written to run for real given a broker,
same as `checkout-api`'s equivalent test always was; what's new here is
that "no broker" now reads as a clean skip instead of a build-breaking
failure either way.

Python (`services/risk-service/tests`, 30 tests, run with
`cd services/risk-service && pytest`, against a real local Redis):

- `test_scoring.py` -- pure logic, including the zero-baseline
  regression test above.
- `test_redis_client.py` / `test_risk_window.py` -- round-trips, TTL
  expiry, per-key isolation, sliding-window eviction, IP/port
  handling, all against real Redis.
- `test_consumer.py` -- `process_event` and `_handle_message` against
  real Redis, including the malformed-message-doesn't-crash case.
- `test_main.py` -- the FastAPI app's endpoints via `httpx.AsyncClient`
  with `get_redis` dependency-overridden to the test fixture, so these
  never trigger the real lifespan (and therefore never need a live
  Kafka broker to run).

Plus a manual smoke test: the real app, started with `uvicorn`, with no
Kafka broker reachable at all -- `/healthz` and the risk-score read
endpoint both work normally, and the unreachable broker only shows up
as a logged connection error, never a crash.
