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
//
// Phase 9 adds this service's first Postgres dependency: on startup,
// and then on a schedule, internal/reconcile reads every on-sale/
// scheduled item's authoritative available_inventory and (re)seeds
// Redis's fast-path copy from it, replacing the dev-only manual seed
// endpoint Phase 4 shipped as a stand-in (see docs/phase9.md).
package main

import (
	"context"
	"database/sql"
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
	"github.com/Agam73/flashsale-reservation/internal/pgdb"
	"github.com/Agam73/flashsale-reservation/internal/reconcile"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

func main() {
	addr := ":" + config.String("CHECKOUT_API_PORT", "8082")
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	dsn := config.String("DATABASE_URL", "postgres://flashsale:flashsale@localhost:5432/flashsale?sslmode=disable")
	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:9092"), ",")
	admissionTTL := time.Duration(config.Int("ADMISSION_TTL_SECONDS", 120)) * time.Second
	reconcileInterval := time.Duration(config.Int("INVENTORY_RECONCILE_INTERVAL_SECONDS", 30)) * time.Second

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisx.NewClient(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		log.Fatalf("checkout-api: %v", err)
	}
	defer redisClient.Close()

	db, err := pgdb.New(ctx, pgdb.Config{DSN: dsn})
	if err != nil {
		log.Fatalf("checkout-api: %v", err)
	}
	defer db.Close()

	kafkaWriter := kafkax.NewWriter(brokers)
	defer kafkaWriter.Close()

	// Seed Redis from Postgres before accepting any traffic, so the
	// first buyer after a restart doesn't hit a cold/empty counter and
	// get a false "item not found or not on sale".
	log.Println("checkout-api: seeding Redis inventory from Postgres...")
	reconcileOnce(ctx, db, redisClient)

	// Then keep correcting drift on a schedule for as long as the
	// service runs -- see internal/reconcile's package doc for what
	// this does and doesn't fix.
	go runReconciler(ctx, db, redisClient, reconcileInterval)

	srv := newServer(addr, db, redisClient, kafkaWriter, admissionTTL)

	go func() {
		log.Printf("checkout-api listening on %s (kafka brokers: %v, reconcile interval: %s)", addr, brokers, reconcileInterval)
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

// runReconciler re-seeds Redis inventory from Postgres every interval,
// until ctx is cancelled. The very first pass is run synchronously in
// main before this goroutine starts, so this loop's job is purely
// correcting drift that accumulates afterward.
func runReconciler(ctx context.Context, db *sql.DB, redisClient *redis.Client, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcileOnce(ctx, db, redisClient)
		}
	}
}

// reconcileOnce runs a single reconciliation pass over every active
// item and logs a summary. One item's failure doesn't stop the rest
// (see reconcile.All) -- this just reports whatever it found.
func reconcileOnce(ctx context.Context, db *sql.DB, redisClient *redis.Client) {
	outcomes, err := reconcile.All(ctx, db, redisClient)
	if err != nil {
		log.Printf("checkout-api: reconciling inventory: %v", err)
		return
	}

	var failed int
	for _, o := range outcomes {
		if o.Err != nil {
			failed++
			log.Printf("checkout-api: reconciling item %s: %v", o.ItemID, o.Err)
		}
	}
	log.Printf("checkout-api: reconciled %d item(s), %d failed", len(outcomes), failed)
}

func newServer(addr string, db *sql.DB, redisClient *redis.Client, kafkaWriter *kafka.Writer, admissionTTL time.Duration) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /items/{itemID}/checkout", handleCheckout(redisClient, kafkaWriter, admissionTTL))
	mux.HandleFunc("POST /items/{itemID}/reconcile", handleReconcileItem(db, redisClient))
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

// handleReconcileItem forces an on-demand reconciliation of one item,
// re-reading its authoritative available_inventory from Postgres and
// overwriting Redis's fast-path copy to match. This is Phase 9's
// replacement for Phase 4's manual seed endpoint: instead of trusting
// an arbitrary number from the request body, the only input is which
// item to refresh -- the value itself always comes from Postgres.
// Useful right after seeding/adjusting an item in Postgres directly,
// without waiting for the next scheduled reconciliation pass.
func handleReconcileItem(db *sql.DB, redisClient *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		itemID := r.PathValue("itemID")
		if itemID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "item id is required")
			return
		}

		available, err := reconcile.Item(r.Context(), db, redisClient, itemID)
		if errors.Is(err, reconcile.ErrItemNotFound) {
			httpx.WriteError(w, http.StatusNotFound, "item not found")
			return
		}
		if err != nil {
			log.Printf("checkout-api: reconciling item %s: %v", itemID, err)
			httpx.WriteError(w, http.StatusInternalServerError, "failed to reconcile inventory")
			return
		}

		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"item_id":   itemID,
			"available": available,
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
