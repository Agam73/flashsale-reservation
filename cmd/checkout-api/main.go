// checkout-api does the synchronous, Redis-only fast path for a
// purchase attempt -- confirm the buyer actually came through the
// waiting room, then atomically decrement the item's fast-path
// inventory counter -- and then publishes the attempt to Kafka so
// decision-service can make the durable, authoritative call.
//
// A 200 here means "the fast path admitted this purchase and durably
// recorded the attempt", not "you have a confirmed reservation". Per
// the Phase 1 design decision, Postgres is the source of truth; the
// actual reservation only exists once decision-service consumes this
// event and writes it. This service has no way to report that final
// outcome back yet -- there's no GET-status endpoint -- so the
// response is honest about being provisional.
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
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"

	"github.com/Agam73/flashsale-reservation/internal/config"
	"github.com/Agam73/flashsale-reservation/internal/httpx"
	"github.com/Agam73/flashsale-reservation/internal/kafkax"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

func main() {
	addr := ":" + config.String("CHECKOUT_API_PORT", "8082")
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:9092"), ",")
	admissionTTL := time.Duration(config.Int("ADMISSION_TTL_SECONDS", 120)) * time.Second

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisx.NewClient(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		log.Fatalf("checkout-api: %v", err)
	}
	defer redisClient.Close()

	kafkaWriter := kafkax.NewWriter(brokers)
	defer kafkaWriter.Close()

	srv := newServer(addr, redisClient, kafkaWriter, admissionTTL)

	go func() {
		log.Printf("checkout-api listening on %s (kafka brokers: %v)", addr, brokers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("checkout-api: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("checkout-api: shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("checkout-api: error during HTTP shutdown: %v", err)
	}
	log.Println("checkout-api: stopped")
}

func newServer(addr string, redisClient *redis.Client, kafkaWriter *kafka.Writer, admissionTTL time.Duration) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /items/{itemID}/checkout", handleCheckout(redisClient, kafkaWriter, admissionTTL))
	mux.HandleFunc("POST /items/{itemID}/inventory", handleSeedInventory(redisClient))
	mux.HandleFunc("GET /items/{itemID}/inventory", handleGetInventory(redisClient))

	return &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "checkout-api"})
}

type checkoutRequest struct {
	UserID   string `json:"user_id"`
	Quantity int64  `json:"quantity"`
}

type checkoutResponse struct {
	ItemID         string `json:"item_id"`
	UserID         string `json:"user_id"`
	Quantity       int64  `json:"quantity"`
	Remaining      int64  `json:"remaining_inventory"`
	IdempotencyKey string `json:"idempotency_key"`
	Status         string `json:"status"`
	Note           string `json:"note"`
}

// handleCheckout: verify the admission token, atomically decrement the
// Redis fast-path counter, then durably publish the attempt to Kafka.
//
// If the Kafka publish fails after the Redis decrement already
// succeeded, that inventory would otherwise be silently stranded --
// held in Redis against an attempt nobody will ever authoritatively
// decide. So on publish failure this releases the Redis inventory back
// and re-grants the buyer's admission token (same TTL), so they can
// retry the checkout call without rejoining the waiting room queue.
func handleCheckout(redisClient *redis.Client, kafkaWriter *kafka.Writer, admissionTTL time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		itemID := r.PathValue("itemID")
		if itemID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "item id is required")
			return
		}

		var req checkoutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if req.UserID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "user_id is required")
			return
		}
		if req.Quantity <= 0 {
			httpx.WriteError(w, http.StatusBadRequest, "quantity must be positive")
			return
		}

		found, err := redisx.ConsumeAdmission(r.Context(), redisClient, itemID, req.UserID)
		if err != nil {
			log.Printf("checkout-api: consuming admission token for item %s user %s: %v", itemID, req.UserID, err)
			httpx.WriteError(w, http.StatusInternalServerError, "failed to verify admission")
			return
		}
		if !found {
			httpx.WriteError(w, http.StatusForbidden, "not admitted -- join the waiting room for this item first")
			return
		}

		remaining, err := redisx.TryDecrementInventory(r.Context(), redisClient, itemID, req.Quantity)
		switch {
		case errors.Is(err, redisx.ErrInventoryNotFound):
			httpx.WriteError(w, http.StatusNotFound, "item not found or not on sale")
			return
		case errors.Is(err, redisx.ErrInsufficientInventory):
			httpx.WriteError(w, http.StatusConflict, "sold out")
			return
		case err != nil:
			log.Printf("checkout-api: decrementing inventory for item %s: %v", itemID, err)
			httpx.WriteError(w, http.StatusInternalServerError, "failed to check inventory")
			return
		}

		idempotencyKey, err := kafkax.NewIdempotencyKey()
		if err != nil {
			log.Printf("checkout-api: generating idempotency key: %v", err)
			releaseAndRegrant(r.Context(), redisClient, itemID, req.UserID, req.Quantity, admissionTTL)
			httpx.WriteError(w, http.StatusInternalServerError, "failed to submit purchase attempt -- try again")
			return
		}

		event := kafkax.PurchaseAttempted{
			ItemID:         itemID,
			UserID:         req.UserID,
			Quantity:       req.Quantity,
			IdempotencyKey: idempotencyKey,
			AttemptedAt:    time.Now().UTC(),
		}
		if err := kafkax.PublishAttempt(r.Context(), kafkaWriter, event); err != nil {
			log.Printf("checkout-api: publishing attempt for item %s user %s: %v", itemID, req.UserID, err)
			releaseAndRegrant(r.Context(), redisClient, itemID, req.UserID, req.Quantity, admissionTTL)
			httpx.WriteError(w, http.StatusServiceUnavailable, "failed to submit purchase attempt -- try again")
			return
		}

		httpx.WriteJSON(w, http.StatusOK, checkoutResponse{
			ItemID:         itemID,
			UserID:         req.UserID,
			Quantity:       req.Quantity,
			Remaining:      remaining,
			IdempotencyKey: idempotencyKey,
			Status:         "pending_confirmation",
			Note:           "fast path admitted and the attempt was durably recorded -- decision-service decides the actual reservation asynchronously; there's no status-check endpoint yet",
		})
	}
}

// releaseAndRegrant undoes the Redis-side effects of an admission that
// didn't make it all the way to a durable Kafka publish, so a
// transient Kafka blip doesn't cost the buyer their spot in line or
// strand inventory that was never actually recorded as attempted.
func releaseAndRegrant(ctx context.Context, redisClient *redis.Client, itemID, userID string, quantity int64, admissionTTL time.Duration) {
	if err := redisx.ReleaseInventory(ctx, redisClient, itemID, quantity); err != nil {
		log.Printf("checkout-api: releasing inventory for item %s after failed publish: %v", itemID, err)
	}
	if err := redisx.GrantAdmission(ctx, redisClient, itemID, userID, admissionTTL); err != nil {
		log.Printf("checkout-api: re-granting admission for item %s user %s after failed publish: %v", itemID, userID, err)
	}
}

type seedInventoryRequest struct {
	Available int64 `json:"available"`
}

// handleSeedInventory exists only because this phase has no Postgres
// wiring to seed Redis from yet -- Phase 9 replaces this with real
// seeding/reconciliation from items.available_inventory. Treat this as
// a test/dev-only endpoint, not part of the real buyer flow.
func handleSeedInventory(redisClient *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		itemID := r.PathValue("itemID")
		if itemID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "item id is required")
			return
		}

		var req seedInventoryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if req.Available < 0 {
			httpx.WriteError(w, http.StatusBadRequest, "available must not be negative")
			return
		}

		if err := redisx.SeedInventory(r.Context(), redisClient, itemID, req.Available); err != nil {
			log.Printf("checkout-api: seeding inventory for item %s: %v", itemID, err)
			httpx.WriteError(w, http.StatusInternalServerError, "failed to seed inventory")
			return
		}

		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"item_id":   itemID,
			"available": req.Available,
		})
	}
}

func handleGetInventory(redisClient *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		itemID := r.PathValue("itemID")
		if itemID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "item id is required")
			return
		}

		n, err := redisx.GetInventory(r.Context(), redisClient, itemID)
		if errors.Is(err, redisx.ErrInventoryNotFound) {
			httpx.WriteError(w, http.StatusNotFound, "item not found or not on sale")
			return
		}
		if err != nil {
			log.Printf("checkout-api: reading inventory for item %s: %v", itemID, err)
			httpx.WriteError(w, http.StatusInternalServerError, "failed to read inventory")
			return
		}

		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"item_id":   itemID,
			"available": n,
		})
	}
}
