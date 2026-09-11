#!/usr/bin/env bash
# verify_phase8.sh — staged health check for flashsale-reservation, Phase 1-8.
# Run from the repo root. Each stage prints PASS/FAIL and stops on the first
# failure, so whatever breaks tells you exactly which layer the error is in.
set -uo pipefail

pass() { echo "[PASS] $1"; }
fail() { echo "[FAIL] $1"; exit 1; }

echo "== Stage 0: toolchain =="
command -v go >/dev/null || fail "go not installed"
go version
pass "go present"

echo
echo "== Stage 1: build =="
go build ./... 2>&1 && pass "go build ./..." || fail "build error — check output above, likely a missing dep or syntax error"

echo
echo "== Stage 2: vet =="
go vet ./... 2>&1 && pass "go vet ./..." || fail "vet found suspicious code"

echo
echo "== Stage 3: pure-logic unit tests (no infra needed) =="
go test ./internal/retry/... ./internal/kafkax/... ./internal/admission/... ./cmd/decision-service/... -run . -v 2>&1 \
  | tee /tmp/stage3.log | grep -E "^(--- FAIL|FAIL)" && fail "unit test failure — see /tmp/stage3.log" \
  || pass "all pure-logic tests pass"

echo
echo "== Stage 4: infra containers up =="
docker compose ps 2>&1
for svc in postgres redis kafka; do
  status=$(docker compose ps --format json 2>/dev/null | grep -i "$svc" | grep -io '"State":"[a-z]*"' || true)
  echo "  $svc: ${status:-not found in docker compose ps}"
done
echo "  (confirm postgres/redis/kafka all show 'running' above before continuing)"

echo
echo "== Stage 5: DB/Redis/Kafka-backed tests against live stack =="
# -p 1 serializes package execution -- internal/decision's 50-goroutine
# row-locking test and internal/expiry's TRUNCATE calls collide when run
# concurrently against the same live Postgres (see docs/phase8.md).
go test ./internal/decision/... ./internal/expiry/... ./internal/redisx/... ./cmd/checkout-api/... -p 1 -v 2>&1 \
  | tee /tmp/stage5.log | grep -E "^(--- FAIL|--- SKIP|FAIL)" 
grep -q "^--- FAIL" /tmp/stage5.log && fail "integration test failure — see /tmp/stage5.log"
grep -q "^--- SKIP" /tmp/stage5.log && echo "  [WARN] some tests still skipped — check DB/Redis env vars (see .env)"
pass "integration tests ran against live infra (check WARN above for skips)"

echo
echo "== Stage 6: migrations applied =="
docker compose exec -T postgres psql -U flashsale -d flashsale -c "\dt" 2>&1 || fail "could not list tables — migrations not applied or wrong DB creds"
pass "tables present (verify items/reservations/orders are in the list above)"

echo
echo "== Stage 7: Kafka topics exist =="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1 | tee /tmp/topics.log
grep -q "checkout-attempts$" /tmp/topics.log || fail "checkout-attempts topic missing — run scripts/setup_kafka_topic.sh"
grep -q "checkout-attempts-dlq$" /tmp/topics.log || fail "checkout-attempts-dlq topic missing — run scripts/setup_kafka_topic.sh"
pass "both topics present"

echo
echo "== Stage 8: service boot smoke test =="
echo "  Manually run each of these in its own terminal, confirm no panic on startup:"
echo "    go run ./cmd/waiting-room-api   && curl localhost:8081/healthz"
echo "    go run ./cmd/checkout-api       && curl localhost:8082/healthz"
echo "    go run ./cmd/decision-service"
echo "    go run ./cmd/expiry-worker"

echo
echo "All automated stages passed. If step 5 showed [WARN] skips or step 8 panics,"
echo "that's your actual bug — paste that specific output back and we'll dig in."
