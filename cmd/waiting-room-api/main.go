// waiting-room-api admits buyers into a flash sale at a controlled,
// fair (FIFO) rate per item, and gates access to checkout-api. The
// admission logic itself (internal/admission) is Phase 2 work, built
// and tested standalone; this is Phase 4's job -- put an HTTP face on
// it and hand each admitted buyer a short-lived Redis token that
// checkout-api requires before it will do the fast-path inventory
// check. That handoff is what makes the waiting room's fairness rule
// actually enforced rather than advisory.
//
// Phase 9 adds queue-depth caching (see handleQueueDepth). Phase 10
// adds this service's first Kafka dependency: every buyer admitted
// off the queue also gets a BuyerJoined event published to
// kafkax.WaitingRoomJoinsTopic, which risk-service consumes to score
// bot/scalper risk asynchronously. Publishing happens in a background
// goroutine, off the request path entirely -- per the Phase 1 design
// decision that risk scoring must never be able to slow down or block
// admission, a slow or unreachable Kafka broker cannot make a buyer
// wait any longer than they already were.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"

	"github.com/Agam73/flashsale-reservation/internal/admission"
	"github.com/Agam73/flashsale-reservation/internal/config"
	"github.com/Agam73/flashsale-reservation/internal/httpx"
	"github.com/Agam73/flashsale-reservation/internal/kafkax"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

func main() {
	addr := ":" + config.String("WAITING_ROOM_API_PORT", "8081")
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	ratePerSecond := config.Int("ADMIT_RATE_PER_SEC", 5)
	admissionTTL := time.Duration(config.Int("ADMISSION_TTL_SECONDS", 120)) * time.Second
	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:9092"), ",")

	// ctx is the service's own lifetime: cancelled on SIGINT/SIGTERM,
	// which in turn stops every Admitter the registry owns, since they
	// all run under this same context.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisx.NewClient(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		log.Fatalf("waiting-room-api: %v", err)
	}
	defer redisClient.Close()

	kafkaWriter := kafkax.NewWaitingRoomWriter(brokers)
	defer kafkaWriter.Close()

	// Tracks in-flight BuyerJoined publishes so shutdown can wait for
	// them to finish (or hit their own timeout) before closing the
	// writer out from under them -- see handleJoin.
	var publishWG sync.WaitGroup

	registry := admission.NewRegistry(ctx, ratePerSecond)
	srv := newServer(addr, registry, redisClient, kafkaWriter, &publishWG, admissionTTL)

	go func() {
		log.Printf("waiting-room-api listening on %s (admit rate: %d/sec/item, admission ttl: %s, kafka brokers: %v)", addr, ratePerSecond, admissionTTL, brokers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("waiting-room-api: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("waiting-room-api: shutting down...")

	// Give in-flight requests (including buyers still waiting in line)
	// a window to unwind before the socket is pulled out from under
	// them.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("waiting-room-api: error during HTTP shutdown: %v", err)
	}

	// Every Admitter's loop is already watching ctx (cancelled above);
	// this just blocks until they've actually finished exiting.
	registry.Shutdown()

	// Let any BuyerJoined publishes still in flight finish (each has
	// its own bounded timeout, see handleJoin) before the writer they
	// depend on gets closed by the deferred kafkaWriter.Close() above.
	publishWG.Wait()
	log.Println("waiting-room-api: stopped")
}

func newServer(addr string, registry *admission.Registry, redisClient *redis.Client, kafkaWriter *kafka.Writer, publishWG *sync.WaitGroup, admissionTTL time.Duration) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /items/{itemID}/join", handleJoin(registry, redisClient, kafkaWriter, publishWG, admissionTTL))
	mux.HandleFunc("GET /items/{itemID}/queue", handleQueueDepth(registry, redisClient))

	return &http.Server{
		Addr:        addr,
		Handler:     mux,
		ReadTimeout: 5 * time.Second,
		// No WriteTimeout: /join intentionally blocks for as long as a
		// buyer waits in line. A client that wants a bound on that
		// should set its own request timeout/context deadline -- the
		// handler already respects r.Context() being cancelled.
		IdleTimeout: 60 * time.Second,
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "waiting-room-api"})
}

type joinRequest struct {
	UserID string `json:"user_id"`
}

type joinResponse struct {
	ItemID              string `json:"item_id"`
	UserID              string `json:"user_id"`
	Position            int    `json:"position"`
	Status              string `json:"status"`
	AdmissionTTLSeconds int    `json:"admission_ttl_seconds"`
}

// handleJoin blocks until the caller is admitted or their connection
// drops -- the HTTP request itself is the wait, so there's no separate
// "check my position" endpoint to keep in sync with it.
func handleJoin(registry *admission.Registry, redisClient *redis.Client, kafkaWriter *kafka.Writer, publishWG *sync.WaitGroup, admissionTTL time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		itemID := r.PathValue("itemID")
		if itemID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "item id is required")
			return
		}

		var req joinRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if req.UserID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "user_id is required")
			return
		}

		// Join doesn't return at all until this buyer is admitted or
		// ctx is cancelled -- by the time it returns without error, the
		// admitted channel is already closed (see admitter.go).
		position, _, err := registry.For(itemID).Join(r.Context())
		if err != nil {
			// Client disconnected, or the service is shutting down.
			// Nobody is listening for a response anymore.
			return
		}

		if err := redisx.GrantAdmission(r.Context(), redisClient, itemID, req.UserID, admissionTTL); err != nil {
			log.Printf("waiting-room-api: granting admission token for item %s user %s: %v", itemID, req.UserID, err)
			httpx.WriteError(w, http.StatusInternalServerError, "admitted, but failed to issue a checkout token -- try again")
			return
		}

		// Tell risk-service this buyer made it off the queue. Fired
		// off the request path: publishWG lets shutdown wait for it,
		// but nothing about this response depends on it succeeding --
		// see this file's package doc for why.
		event := kafkax.BuyerJoined{
			ItemID:     itemID,
			UserID:     req.UserID,
			RemoteAddr: r.RemoteAddr,
			JoinedAt:   time.Now().UTC(),
		}
		publishWG.Add(1)
		go func() {
			defer publishWG.Done()
			publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := kafkax.PublishBuyerJoined(publishCtx, kafkaWriter, event); err != nil {
				log.Printf("waiting-room-api: publishing buyer-joined event for item %s user %s: %v", itemID, req.UserID, err)
			}
		}()

		httpx.WriteJSON(w, http.StatusOK, joinResponse{
			ItemID:              itemID,
			UserID:              req.UserID,
			Position:            position,
			Status:              "admitted",
			AdmissionTTLSeconds: int(admissionTTL.Seconds()),
		})
	}
}

type queueDepthResponse struct {
	ItemID  string `json:"item_id"`
	Waiting int    `json:"waiting"`
}

// handleQueueDepth answers "how many buyers are currently waiting for
// this item" -- Phase 9's "waiting-room queue state" piece. The
// in-memory Admitter (Phase 2) remains the actual source of truth; this
// just reads its current depth and also publishes it into Redis
// (redisx.SetQueueDepth) so the number is visible to anything that
// isn't this specific waiting-room-api instance, without that caller
// needing its own admission.Registry.
//
// Known simplification: if this service restarts, the in-memory queue
// (and therefore this number) resets to zero even though buyers who
// were waiting haven't actually been admitted anywhere else -- the
// same restart behavior the Admitter has always had, just now visible
// through Redis too. Running more than one waiting-room-api instance
// per item isn't supported yet either; each instance still owns its
// own independent queue (see internal/admission/registry.go).
func handleQueueDepth(registry *admission.Registry, redisClient *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		itemID := r.PathValue("itemID")
		if itemID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "item id is required")
			return
		}

		depth, err := registry.For(itemID).Depth(r.Context())
		if err != nil {
			log.Printf("waiting-room-api: reading queue depth for item %s: %v", itemID, err)
			httpx.WriteError(w, http.StatusInternalServerError, "failed to read queue depth")
			return
		}

		if err := redisx.SetQueueDepth(r.Context(), redisClient, itemID, int64(depth)); err != nil {
			// Non-fatal: the caller still gets an accurate answer
			// straight from the Admitter. Redis is just a cache of it.
			log.Printf("waiting-room-api: caching queue depth for item %s: %v", itemID, err)
		}

		httpx.WriteJSON(w, http.StatusOK, queueDepthResponse{ItemID: itemID, Waiting: depth})
	}
}
