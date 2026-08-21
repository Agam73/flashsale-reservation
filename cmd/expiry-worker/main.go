package main

import (
	"log"
	"time"
)

// expiry-worker watches unpaid reservations past their TTL, releases
// inventory back, and publishes ReservationExpired. Real logic lands in
// Phase 7 (worker pool pattern).

func main() {
	log.Println("expiry-worker: starting (stub, not implemented yet)")
	for {
		log.Println("expiry-worker: tick (no-op)")
		time.Sleep(10 * time.Second)
	}
}
