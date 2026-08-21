// checkout-api does the synchronous, Redis-only fast path for a
// purchase attempt: confirm the buyer actually came through the
// waiting room, then atomically decrement the item's fast-path
// inventory counter.
//
// This is deliberately NOT the authoritative decision. Per the Phase 1
// design decision, Postgres is the source of truth and Redis here is
// fast, disposable, derived state. A 200 from POST /items/{id}/checkout
// means "the fast path admitted this purchase", not "a durable
// reservation exists" -- there is no reservations row yet, because that
// requires the Kafka producer (Phase 6) and decision-service consuming
// it and writing to Postgres. Until then this is intentionally an
// optimistic, in-memory-speed gate in front of the real thing.
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

	"github.com/Agam73/flashsale-reservation/internal/config"
	"github.com/Agam73/flashsale-reservation/internal/httpx"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

func main() {
	addr := ":" + config.String("CHECKOUT_API_PORT", "8082")
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisx.NewClient(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		log.Fatalf("checkout-api: %v", err)
	}
	defer redisClient.Close()

	srv := newServer(addr, redisClient)

	go func() {
		log.Printf("checkout-api listening on %s", addr)
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

func newServer(addr string, redisClient *redis.Client) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /items/{itemID}/checkout", handleCheckout(redisClient))
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
	ItemID    string `json:"item_id"`
	UserID    string `json:"user_id"`
	Quantity  int64  `json:"quantity"`
	Remaining int64  `json:"remaining_inventory"`
	Status    string `json:"status"`
	Note      string `json:"note"`
}

// handleCheckout is the fast path: verify the admission token, then
// atomically decrement inventory. If the decrement fails after the
// token was already consumed, the token is intentionally not restored
// -- it's single-use by design, and Phase 6's Kafka retry path is where
// "the buyer's attempt didn't survive the fast path" gets handled
// properly rather than by trying to un-consume state here.
func handleCheckout(redisClient *redis.Client) http.HandlerFunc {
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

		httpx.WriteJSON(w, http.StatusOK, checkoutResponse{
			ItemID:    itemID,
			UserID:    req.UserID,
			Quantity:  req.Quantity,
			Remaining: remaining,
			Status:    "fast_path_admitted",
			Note:      "no durable reservation created yet -- Kafka + decision-service land in Phase 6",
		})
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
