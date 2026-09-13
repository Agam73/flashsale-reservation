# Test results log

A running record of actual test suite runs against this repo -- real
command output, not a description of what the tests are supposed to
do (that lives in each phase's own `docs/phaseN.md`). Newest entry on
top.

**How to add an entry:** after running the full suite (Go and/or
Python), paste the real terminal output below under a new dated
heading. Don't summarize or clean it up -- a genuine `FAIL` or `SKIP`
here is more useful than a tidied-up "all green." Note the environment
(OS, whether Kafka/Docker was up) since that changes which tests run
vs. skip.

---

## 2026-09-13 -- Windows, full stack (Postgres + Redis + Kafka via Docker)

**Environment:** Windows (Git Bash / PowerShell), `docker-compose` for
Postgres, Redis, and Kafka, topics created via
`scripts/setup_kafka_topic.sh`. Go tests run from repo root; Python
tests run from `services/risk-service` with a Python 3.12 venv.

**Command:** `go test ./... -p 1`

```
ok      github.com/Agam73/flashsale-reservation/cmd/checkout-api        13.188s
ok      github.com/Agam73/flashsale-reservation/cmd/decision-service    (cached)
?       github.com/Agam73/flashsale-reservation/cmd/expiry-worker       [no test files]
ok      github.com/Agam73/flashsale-reservation/cmd/waiting-room-api    13.118s
ok      github.com/Agam73/flashsale-reservation/internal/admission      (cached)
?       github.com/Agam73/flashsale-reservation/internal/config [no test files]
ok      github.com/Agam73/flashsale-reservation/internal/decision       (cached)
ok      github.com/Agam73/flashsale-reservation/internal/expiry (cached)
?       github.com/Agam73/flashsale-reservation/internal/httpx  [no test files]
ok      github.com/Agam73/flashsale-reservation/internal/kafkax (cached)
ok      github.com/Agam73/flashsale-reservation/internal/pgdb   (cached)
ok      github.com/Agam73/flashsale-reservation/internal/reconcile      (cached)
ok      github.com/Agam73/flashsale-reservation/internal/redisx (cached)
ok      github.com/Agam73/flashsale-reservation/internal/retry  (cached)
```

Notably, `checkout-api` and `waiting-room-api` took ~13s each rather
than skipping -- that's `TestHandleCheckout_SuccessPublishesToKafka`
and `TestHandleJoin_PublishesReadableBuyerJoinedEvent` actually
connecting to a live Kafka broker and reading a real message back,
not just constructing objects.

**Command:** `pytest` (from `services/risk-service`, Python 3.12 venv)

```
======================================================================= test session starts ========================================================================
platform win32 -- Python 3.12.10, pytest-8.3.3, pluggy-1.6.0
rootdir: D:\Github\flashsale-reservation\services\risk-service
configfile: pytest.ini
plugins: anyio-4.15.1, asyncio-0.24.0
asyncio: mode=Mode.AUTO, default_loop_scope=function
collected 30 items

tests\test_consumer.py .....                                                                                                                                  [ 16%]
tests\test_main.py ...                                                                                                                                        [ 26%]
tests\test_redis_client.py .......                                                                                                                            [ 50%]
tests\test_risk_window.py .........                                                                                                                           [ 80%]
tests\test_scoring.py ......                                                                                                                                  [100%]

======================================================================== 30 passed in 1.09s ========================================================================
```

**Verdict:** full stack green -- every Go package passes or correctly
reports no test files; all 30 Python tests pass; both Kafka-dependent
tests ran for real (not skipped) and passed.

**Gotchas hit getting here** (kept for anyone repeating this setup):
- `psql` isn't on Windows PATH by default -- ran migrations via
  `docker exec -i flashsale-postgres psql ...` instead of a local
  install.
- Git Bash auto-converts Unix-style paths, which broke
  `scripts/setup_kafka_topic.sh`'s `docker exec` calls until run with
  `MSYS_NO_PATHCONV=1`.
- `pip install -r requirements.txt` failed to build `aiokafka` and
  `pydantic-core` from source under Python 3.14 (no prebuilt Windows
  wheels yet for that version, and no MSVC/Rust toolchain to compile
  them locally). Recreating the venv with Python 3.12 fixed it --
  prebuilt wheels exist for 3.12.

---
