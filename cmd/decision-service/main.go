// decision-service is the authoritative consumer: one Kafka partition
// per item (see internal/kafkax), a single writer per item at a time,
// writing the final reservation to Postgres via internal/decision.
// This is where overselling actually gets prevented -- everything
// before this point (the waiting room, the Redis fast path in
// checkout-api) is optimistic and provisional; this is the durable
// source of truth per the Phase 1 design decision.
//
// The correctness logic itself lives in internal/decision and is
// tested directly against Postgres with no Kafka involved. This file
// is just the consume loop: fetch a message, decode it, hand it to
// decision.ProcessAttempt, commit the offset once it succeeds.
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

	kafka "github.com/segmentio/kafka-go"

	"github.com/Agam73/flashsale-reservation/internal/config"
	"github.com/Agam73/flashsale-reservation/internal/decision"
	"github.com/Agam73/flashsale-reservation/internal/httpx"
	"github.com/Agam73/flashsale-reservation/internal/kafkax"
	"github.com/Agam73/flashsale-reservation/internal/pgdb"

	"database/sql"
)

func main() {
	addr := ":" + config.String("DECISION_SERVICE_PORT", "8083")
	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:9092"), ",")
	groupID := config.String("KAFKA_CONSUMER_GROUP", "decision-service")
	dsn := config.String("DATABASE_URL", "postgres://flashsale:flashsale@localhost:5432/flashsale?sslmode=disable")
	reservationTTL := time.Duration(config.Int("RESERVATION_TTL_SECONDS", 120)) * time.Second

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgdb.New(ctx, pgdb.Config{DSN: dsn})
	if err != nil {
		log.Fatalf("decision-service: %v", err)
	}
	defer db.Close()

	reader := kafkax.NewReader(brokers, groupID)
	defer reader.Close()

	healthSrv := newHealthServer(addr)
	go func() {
		log.Printf("decision-service health endpoint on %s", addr)
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("decision-service: health server error: %v", err)
		}
	}()

	log.Printf("decision-service consuming topic=%s group=%s brokers=%v (reservation ttl: %s)",
		kafkax.CheckoutAttemptsTopic, groupID, brokers, reservationTTL)
	runConsumeLoop(ctx, reader, db, reservationTTL)

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

// runConsumeLoop fetches messages one at a time and retries a message
// against Postgres (with backoff) until it succeeds, rather than
// advancing past it -- kafka-go's FetchMessage moves to the next
// message on every call regardless of whether the previous one was
// committed, so "retry the same message" has to happen in this inner
// loop, not by calling FetchMessage again.
//
// Known gap (documented, not solved here): there's no dead-letter
// topic yet (that's Phase 8). A message that fails for a genuinely
// permanent reason other than ErrItemNotFound -- which is the one
// permanent-failure case this loop does special-case -- would retry
// forever and block the rest of its partition. Acceptable for this
// phase; not acceptable to ship past Phase 8.
func runConsumeLoop(ctx context.Context, reader *kafka.Reader, db *sql.DB, reservationTTL time.Duration) {
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
			log.Printf("decision-service: SKIPPING unparseable message at partition %d offset %d: %v", msg.Partition, msg.Offset, decodeErr)
			commitOrLog(ctx, reader, msg)
			continue messages
		}

		backoff := time.Second
		for {
			outcome, procErr := decision.ProcessAttempt(ctx, db, decision.Attempt{
				ItemID:         attempt.ItemID,
				UserID:         attempt.UserID,
				Quantity:       attempt.Quantity,
				IdempotencyKey: attempt.IdempotencyKey,
			}, reservationTTL)

			switch {
			case procErr == nil:
				logOutcome(attempt, outcome)
				commitOrLog(ctx, reader, msg)
				continue messages
			case errors.Is(procErr, decision.ErrItemNotFound):
				log.Printf("decision-service: SKIPPING attempt for unknown item %s (partition %d offset %d): %v",
					attempt.ItemID, msg.Partition, msg.Offset, procErr)
				commitOrLog(ctx, reader, msg)
				continue messages
			}

			log.Printf("decision-service: processing attempt for item %s failed, retrying in %s: %v", attempt.ItemID, backoff, procErr)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}
}

func commitOrLog(ctx context.Context, reader *kafka.Reader, msg kafka.Message) {
	if err := reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("decision-service: committing offset (partition %d offset %d): %v", msg.Partition, msg.Offset, err)
	}
}

func logOutcome(attempt kafkax.PurchaseAttempted, outcome decision.Outcome) {
	replayedNote := ""
	if outcome.Replayed {
		replayedNote = " (replayed delivery, no new decision made)"
	}
	log.Printf("decision-service: item=%s user=%s qty=%d -> %s reservation=%s%s",
		attempt.ItemID, attempt.UserID, attempt.Quantity, outcome.Status, outcome.ReservationID, replayedNote)
}
