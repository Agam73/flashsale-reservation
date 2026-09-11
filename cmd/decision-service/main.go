// decision-service is the authoritative consumer: one Kafka partition
// per item (see internal/kafkax), a single writer per item at a time,
// writing the final reservation to Postgres via internal/decision.
// This is where overselling actually gets prevented -- everything
// before this point (the waiting room, the Redis fast path in
// checkout-api) is optimistic and provisional; this is the durable
// source of truth per the Phase 1 design decision.
//
// Phase 8's reliability layer sits on top of Phase 6's correctness
// layer: internal/retry.Policy decides how many times, and for how
// long, to keep retrying a message against Postgres before giving up;
// internal/kafkax.DeadLetter + NewDLQWriter is where a given-up-on
// message goes instead of blocking its partition forever.
//
// This file adds one more layer on top of both: classifyOutcome
// decides whether a given failure should count against the retry
// policy's budget at all. A message that fails because Postgres is
// temporarily unreachable is not the same kind of problem as a message
// that references an item that doesn't exist -- see classifyOutcome's
// doc comment for why conflating the two would make an extended
// Postgres outage dead-letter every message in flight, none of which
// are actually bad.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"

	"github.com/Agam73/flashsale-reservation/internal/config"
	"github.com/Agam73/flashsale-reservation/internal/decision"
	"github.com/Agam73/flashsale-reservation/internal/httpx"
	"github.com/Agam73/flashsale-reservation/internal/kafkax"
	"github.com/Agam73/flashsale-reservation/internal/pgdb"
	"github.com/Agam73/flashsale-reservation/internal/retry"

	"database/sql"
)

func main() {
	addr := ":" + config.String("DECISION_SERVICE_PORT", "8083")
	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:9092"), ",")
	groupID := config.String("KAFKA_CONSUMER_GROUP", "decision-service")
	dsn := config.String("DATABASE_URL", "postgres://flashsale:flashsale@localhost:5432/flashsale?sslmode=disable")
	reservationTTL := time.Duration(config.Int("RESERVATION_TTL_SECONDS", 120)) * time.Second

	policy := retry.DefaultPolicy()
	if v := config.Int("DECISION_MAX_RETRIES", 0); v > 0 {
		policy.MaxAttempts = v
	}
	if v := config.Int("DECISION_MAX_RETRY_ELAPSED_SECONDS", 0); v > 0 {
		policy.MaxElapsed = time.Duration(v) * time.Second
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgdb.New(ctx, pgdb.Config{DSN: dsn})
	if err != nil {
		log.Fatalf("decision-service: %v", err)
	}
	defer db.Close()

	reader := kafkax.NewReader(brokers, groupID)
	defer reader.Close()

	dlqWriter := kafkax.NewDLQWriter(brokers)
	defer dlqWriter.Close()

	healthSrv := newHealthServer(addr)
	go func() {
		log.Printf("decision-service health endpoint on %s", addr)
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("decision-service: health server error: %v", err)
		}
	}()

	log.Printf("decision-service consuming topic=%s group=%s brokers=%v (reservation ttl: %s, retry policy: max %d attempts / %s elapsed)",
		kafkax.CheckoutAttemptsTopic, groupID, brokers, reservationTTL, policy.MaxAttempts, policy.MaxElapsed)
	runConsumeLoop(ctx, reader, dlqWriter, db, reservationTTL, policy)

	log.Println("decision-service: shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("decision-service: error during health server shutdown: %v", err)
	}
	log.Println("decision-service: stopped")
}

func newHealthServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "decision-service"})
	})
	return &http.Server{Addr: addr, Handler: mux}
}

// outcome is what classifyOutcome reduces a ProcessAttempt result down
// to. Separating "what should happen" from "how it happens" this way
// is what makes the decision testable without a live broker or
// database -- classifyOutcome takes plain values in and returns a
// plain value out.
type outcome int

const (
	outcomeCommit outcome = iota
	outcomeDeadLetter
	outcomeRetryUncounted // infra failure: retry, don't touch the policy's budget
	outcomeRetryCounted   // everything else: subject to the policy's MaxAttempts/MaxElapsed
)

// classifyOutcome decides what to do with a ProcessAttempt result.
//
//   - nil error -> outcomeCommit (success, or a safely-replayed
//     redelivery; nothing left to do).
//   - ErrItemNotFound / ErrInvalidQuantity -> outcomeDeadLetter
//     immediately, without spending any retry budget. These are
//     properties of the message's data, not the infrastructure --
//     retrying won't help, and every retry just delays an operator
//     finding out about a bad event.
//   - A connectivity-shaped error (anything that ISN'T a *pq.Error --
//     see isConnectivityError) -> outcomeRetryUncounted, never counted
//     against the policy. If Postgres is down, EVERY in-flight message
//     fails identically; dead-lettering them via a MaxAttempts/
//     MaxElapsed cap meant for "is this one bad message" would be
//     wrong, since none of them are actually bad -- they're all
//     waiting on infrastructure that isn't back yet.
//   - Anything else (a real Postgres-level error that isn't one of the
//     two known sentinels -- e.g. a serialization failure or an
//     unexpected constraint violation) -> outcomeRetryCounted,
//     governed by policy.ShouldRetry.
func classifyOutcome(err error, policy retry.Policy, attemptCount int, elapsed time.Duration) outcome {
	if err == nil {
		return outcomeCommit
	}
	if errors.Is(err, decision.ErrItemNotFound) || errors.Is(err, decision.ErrInvalidQuantity) {
		return outcomeDeadLetter
	}
	if isConnectivityError(err) {
		return outcomeRetryUncounted
	}
	if !policy.ShouldRetry(attemptCount, elapsed) {
		return outcomeDeadLetter
	}
	return outcomeRetryCounted
}

// isConnectivityError distinguishes "Postgres responded with a real
// error" from "we couldn't even talk to Postgres". lib/pq wraps actual
// database-level errors (constraint violations, serialization
// failures, syntax errors, etc.) in *pq.Error; a connection failure,
// timeout, or dropped connection surfaces as a plain error instead,
// since there was never a Postgres response to wrap. internal/decision's
// error wrapping (%w throughout) preserves whichever one is at the
// root, so errors.As still finds it here despite the extra
// fmt.Errorf layers.
func isConnectivityError(err error) bool {
	var pqErr *pq.Error
	return !errors.As(err, &pqErr)
}

// runConsumeLoop fetches messages one at a time. kafka-go's
// FetchMessage advances to the next message on every call regardless
// of commit state, so "retry the same message" has to happen in this
// inner loop, not by calling FetchMessage again.
func runConsumeLoop(ctx context.Context, reader *kafka.Reader, dlqWriter *kafka.Writer, db *sql.DB, reservationTTL time.Duration, policy retry.Policy) {
messages:
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("decision-service: fetching message: %v", err)
			continue messages
		}

		attempt, decodeErr := kafkax.DecodeAttempt(msg.Value)
		if decodeErr != nil {
			log.Printf("decision-service: DEAD-LETTERING unparseable message at partition %d offset %d: %v", msg.Partition, msg.Offset, decodeErr)
			sendToDeadLetter(ctx, dlqWriter, msg, decodeErr, 0)
			commitOrLog(ctx, reader, msg)
			continue messages
		}

		start := time.Now()
		attemptCount := 0        // counted attempts, subject to policy.ShouldRetry
		uncountedRetries := 0    // connectivity retries, own backoff curve, never capped

		for {
			out, procErr := decision.ProcessAttempt(ctx, db, decision.Attempt{
				ItemID:         attempt.ItemID,
				UserID:         attempt.UserID,
				Quantity:       attempt.Quantity,
				IdempotencyKey: attempt.IdempotencyKey,
			}, reservationTTL)

			switch classifyOutcome(procErr, policy, attemptCount, time.Since(start)) {
			case outcomeCommit:
				logOutcome(attempt, out)
				commitOrLog(ctx, reader, msg)
				continue messages

			case outcomeDeadLetter:
				log.Printf("decision-service: DEAD-LETTERING attempt for item %s after %d attempt(s) over %s (partition %d offset %d): %v",
					attempt.ItemID, attemptCount, time.Since(start), msg.Partition, msg.Offset, procErr)
				sendToDeadLetter(ctx, dlqWriter, msg, procErr, attemptCount)
				commitOrLog(ctx, reader, msg)
				continue messages

			case outcomeRetryUncounted:
				uncountedRetries++
				backoff := policy.NextBackoff(uncountedRetries)
				log.Printf("decision-service: Postgres unreachable, retrying item %s in %s (infra outage, not counted against retry budget): %v",
					attempt.ItemID, backoff, procErr)
				if !sleepOrDone(ctx, backoff) {
					return
				}

			case outcomeRetryCounted:
				attemptCount++
				backoff := policy.NextBackoff(attemptCount)
				log.Printf("decision-service: processing attempt for item %s failed (attempt %d/%d), retrying in %s: %v",
					attempt.ItemID, attemptCount, policy.MaxAttempts, backoff, procErr)
				if !sleepOrDone(ctx, backoff) {
					return
				}
			}
		}
	}
}

// sleepOrDone waits for d, or returns false early if ctx is cancelled
// first (so shutdown during a retry backoff is still prompt).
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// sendToDeadLetter publishes to the DLQ and logs (but does not treat
// as fatal) a failure to do so. If the DLQ publish itself fails, the
// caller still commits the original offset -- deliberately: the
// alternative (refusing to commit, retrying forever) would mean a
// single bad message can block its whole partition, exactly the
// scenario dead-lettering exists to prevent.
func sendToDeadLetter(ctx context.Context, dlqWriter *kafka.Writer, msg kafka.Message, cause error, attemptCount int) {
	dl := kafkax.DeadLetter{
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		OriginalKey:       string(msg.Key),
		OriginalValue:     string(msg.Value),
		Reason:            cause.Error(),
		AttemptCount:      attemptCount,
		FailedAt:          time.Now().UTC(),
	}
	if err := kafkax.PublishDeadLetter(ctx, dlqWriter, dl); err != nil {
		log.Printf("decision-service: FAILED to publish to dead-letter topic (partition %d offset %d will be committed anyway): %v",
			msg.Partition, msg.Offset, err)
	}
}

func commitOrLog(ctx context.Context, reader *kafka.Reader, msg kafka.Message) {
	if err := reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("decision-service: committing offset (partition %d offset %d): %v", msg.Partition, msg.Offset, err)
	}
}

func logOutcome(attempt kafkax.PurchaseAttempted, out decision.Outcome) {
	replayedNote := ""
	if out.Replayed {
		replayedNote = " (replayed delivery, no new decision made)"
	}
	log.Printf("decision-service: item=%s user=%s qty=%d -> %s reservation=%s%s",
		attempt.ItemID, attempt.UserID, attempt.Quantity, out.Status, out.ReservationID, replayedNote)
}
