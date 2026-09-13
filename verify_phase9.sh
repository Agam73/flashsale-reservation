#!/usr/bin/env bash
# verify_phase9.sh — staged health check for flashsale-reservation, Phase 9.
# Run from the repo root (git bash: MSYS_NO_PATHCONV=1 bash verify_phase9.sh).
# No new Kafka topics or containers needed for Phase 9 itself -- it's
# Postgres + Redis only -- but decision-service/checkout-api's existing
# Kafka-dependent tests will still show up if you run the whole suite.
set -uo pipefail

pass() { echo "[PASS] $1"; }
fail() { echo "[FAIL] $1"; exit 1; }

echo "== Stage 0: toolchain =="
command -v go >/dev/null || fail "go not installed"
go version
pass "go present"

echo
echo "== Stage 1: build =="
go build ./... 2>&1 && pass "go build ./..." || fail "build error — check output above"

echo
echo "== Stage 2: vet =="
go vet ./... 2>&1 && pass "go vet ./..." || fail "vet found suspicious code"

echo
echo "== Stage 3: infra containers up =="
docker compose ps 2>&1
echo "  (confirm postgres and redis show 'healthy' above — Phase 9 doesn't need kafka to be up)"

echo
echo "== Stage 4: reconcile package (Postgres + Redis) =="
go test ./internal/reconcile/... -v -count=1 2>&1 | tee /tmp/phase9_reconcile.log
grep -qE "^(--- FAIL|FAIL)" /tmp/phase9_reconcile.log && fail "reconcile test failure — see /tmp/phase9_reconcile.log"
grep -q "^--- SKIP" /tmp/phase9_reconcile.log && fail "tests SKIPPED, not run — Postgres/Redis not reachable, check Stage 3"
pass "reconcile: seed-from-postgres, drift-correction, unknown item, status filtering, batch all pass"

echo
echo "== Stage 5: redisx additions (queue depth, risk score) =="
go test ./internal/redisx/... -v -count=1 2>&1 | tee /tmp/phase9_redisx.log
grep -qE "^(--- FAIL|FAIL)" /tmp/phase9_redisx.log && fail "redisx test failure — see /tmp/phase9_redisx.log"
grep -q "^--- SKIP" /tmp/phase9_redisx.log && fail "tests SKIPPED, not run — Redis not reachable, check Stage 3"
pass "redisx: queue depth + risk score tests pass"

echo
echo "== Stage 6: admission.Depth (no infra needed) =="
go test ./internal/admission/... -run Depth -v -count=1 2>&1 | tee /tmp/phase9_admission.log
grep -qE "^(--- FAIL|FAIL)" /tmp/phase9_admission.log && fail "admission.Depth test failure — see /tmp/phase9_admission.log"
pass "Depth reflects queue length; returns error after shutdown"

echo
echo "== Stage 7: checkout-api's background reconciler loop =="
go test ./cmd/checkout-api/... -run TestRunReconciler -v -count=1 2>&1 | tee /tmp/phase9_reconciler_loop.log
grep -qE "^(--- FAIL|FAIL)" /tmp/phase9_reconciler_loop.log && fail "runReconciler test failure — see /tmp/phase9_reconciler_loop.log"
grep -q "^--- SKIP" /tmp/phase9_reconciler_loop.log && fail "tests SKIPPED, not run — Postgres not reachable, check Stage 3"
pass "background reconciler loop ticks on schedule and stops on context cancellation"

echo
echo "== Stage 8: race detector across everything Phase 9 touched =="
go test ./internal/reconcile/... ./internal/redisx/... ./internal/admission/... ./cmd/checkout-api/... \
  -run 'Reconcile|Depth|Risk|Queue|Item|All' -race -count=1 2>&1 | tee /tmp/phase9_race.log
grep -qE "^(WARNING: DATA RACE|--- FAIL|FAIL)" /tmp/phase9_race.log && fail "race detected or test failure — see /tmp/phase9_race.log"
pass "no data races detected"

echo
echo "All Phase 9 stages passed. Note: cmd/checkout-api's Kafka-publish"
echo "test will still fail if you run 'go test ./...' broadly without a"
echo "live Kafka broker — that's a pre-existing Phase 8 gap, not Phase 9."
