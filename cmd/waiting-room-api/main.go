// waiting-room-api admits buyers into a flash sale at a controlled,
// fair (FIFO) rate per item, and gates access to checkout-api. The
// admission logic itself (internal/admission) is Phase 2 work, built
// and tested standalone; this is Phase 4's job -- put an HTTP face on
// it and hand each admitted buyer a short-lived Redis token that
// checkout-api requires before it will do the fast-path inventory
// check. That handoff is what makes the waiting room's fairness rule
// actually enforced rather than advisory.
//
// No Kafka, no Postgres here yet: this service only ever touches
// Redis. Phase 6 adds the durable, authoritative path through
// decision-service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Agam73/flashsale-reservation/internal/admission"
	"github.com/Agam73/flashsale-reservation/internal/config"
	"github.com/Agam73/flashsale-reservation/internal/httpx"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

func main() {
	addr := ":" + config.String("WAITING_ROOM_API_PORT", "8081")
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	ratePerSecond := config.Int("ADMIT_RATE_PER_SEC", 5)
	admissionTTL := time.Duration(config.Int("ADMISSION_TTL_SECONDS", 120)) * time.Second

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

	registry := admission.NewRegistry(ctx, ratePerSecond)
	srv := newServer(addr, registry, redisClient, admissionTTL)

	go func() {
		log.Printf("waiting-room-api listening on %s (admit rate: %d/sec/item, admission ttl: %s)", addr, ratePerSecond, admissionTTL)
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
	log.Println("waiting-room-api: stopped")
}

func newServer(addr string, registry *admission.Registry, redisClient *redis.Client, admissionTTL time.Duration) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /items/{itemID}/join", handleJoin(registry, redisClient, admissionTTL))

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
func handleJoin(registry *admission.Registry, redisClient *redis.Client, admissionTTL time.Duration) http.HandlerFunc {
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

		httpx.WriteJSON(w, http.StatusOK, joinResponse{
			ItemID:              itemID,
			UserID:              req.UserID,
			Position:            position,
			Status:              "admitted",
			AdmissionTTLSeconds: int(admissionTTL.Seconds()),
		})
	}
}
