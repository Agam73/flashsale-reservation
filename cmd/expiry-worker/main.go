// expiry-worker is this project's first worker-pool: one scanner
// goroutine periodically finds reservations whose TTL has lapsed
// (internal/expiry.FindExpiredReservations) and feeds their IDs into a
// channel; a fixed pool of worker goroutines drains that channel
// concurrently, each releasing one reservation's held inventory back
// to its item (internal/expiry.ExpireReservation).
//
// This is a different concurrency shape than anything earlier in the
// project: Phase 2's Admitter is a single serialized queue: Phase 6's
// Kafka consumer processes one partition's messages in order. Here,
// by contrast, there's no ordering requirement between reservations at
// all -- expiring reservation A before or after reservation B makes no
// difference -- so a worker pool (N goroutines pulling arbitrary work
// off a shared channel) is the right fit, and it's safe specifically
// because internal/expiry.ExpireReservation is safe to call
// concurrently on any reservation, including duplicates.
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Agam73/flashsale-reservation/internal/config"
	"github.com/Agam73/flashsale-reservation/internal/expiry"
	"github.com/Agam73/flashsale-reservation/internal/httpx"
	"github.com/Agam73/flashsale-reservation/internal/pgdb"
)

func main() {
	addr := ":" + config.String("EXPIRY_WORKER_PORT", "8084")
	dsn := config.String("DATABASE_URL", "postgres://flashsale:flashsale@localhost:5432/flashsale?sslmode=disable")
	scanInterval := time.Duration(config.Int("EXPIRY_SCAN_INTERVAL_SECONDS", 5)) * time.Second
	batchSize := config.Int("EXPIRY_SCAN_BATCH_SIZE", 100)
	poolSize := config.Int("EXPIRY_WORKER_POOL_SIZE", 4)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgdb.New(ctx, pgdb.Config{DSN: dsn})
	if err != nil {
		log.Fatalf("expiry-worker: %v", err)
	}
	defer db.Close()

	healthSrv := newHealthServer(addr)
	go func() {
		log.Printf("expiry-worker health endpoint on %s", addr)
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("expiry-worker: health server error: %v", err)
		}
	}()

	log.Printf("expiry-worker starting: pool size=%d, scan interval=%s, batch size=%d", poolSize, scanInterval, batchSize)

	// jobs is sized to one full scan batch: the scanner can hand off an
	// entire batch without blocking on the pool keeping up, but a
	// second batch's send will naturally block (backpressure) until
	// workers have made room, rather than growing without bound.
	jobs := make(chan string, batchSize)

	var wg sync.WaitGroup
	for i := 1; i <= poolSize; i++ {
		wg.Add(1)
		go worker(ctx, i, db, jobs, &wg)
	}

	// Blocks until ctx is cancelled, then closes jobs (it's the sole
	// producer, so it's the only goroutine allowed to close it) and
	// returns.
	runScanner(ctx, db, jobs, scanInterval, batchSize)

	log.Println("expiry-worker: scanner stopped, waiting for in-flight jobs to finish...")
	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("expiry-worker: error during health server shutdown: %v", err)
	}
	log.Println("expiry-worker: stopped")
}

func newHealthServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "expiry-worker"})
	})
	return &http.Server{Addr: addr, Handler: mux}
}

// runScanner is the pool's sole producer. On every tick it finds up to
// batchSize expired reservations and pushes each ID onto jobs,
// blocking (applying backpressure) if the pool hasn't drained the
// previous batch yet. Returns once ctx is cancelled, having closed
// jobs so the worker pool can drain and exit.
func runScanner(ctx context.Context, db *sql.DB, jobs chan<- string, interval time.Duration, batchSize int) {
	defer close(jobs)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ids, err := expiry.FindExpiredReservations(ctx, db, batchSize)
			if err != nil {
				log.Printf("expiry-worker: scanning for expired reservations: %v", err)
				continue
			}
			for _, id := range ids {
				select {
				case jobs <- id:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// worker drains jobs until the channel is closed (normal shutdown) and
// reports what it did for each one. Multiple workers may occasionally
// be handed the same reservation ID (e.g. two scan ticks overlapping a
// slow batch) -- expiry.ExpireReservation is what makes that safe; this
// loop doesn't need to know or care.
func worker(ctx context.Context, id int, db *sql.DB, jobs <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for reservationID := range jobs {
		outcome, err := expiry.ExpireReservation(ctx, db, reservationID)
		if err != nil {
			log.Printf("expiry-worker[%d]: expiring reservation %s: %v", id, reservationID, err)
			continue
		}
		if !outcome.Released {
			log.Printf("expiry-worker[%d]: reservation %s already handled or not actually expired, skipped", id, reservationID)
			continue
		}
		log.Printf("expiry-worker[%d]: expired reservation %s, released %d unit(s) back to item %s",
			id, reservationID, outcome.Quantity, outcome.ItemID)
	}
}
