package main

import (
	"fmt"
	"log"
	"net/http"
)

// checkout-api does the fast Redis inventory check and publishes
// PurchaseAttempted events to Kafka. Real logic lands in Phase 4 (HTTP)
// and Phase 6 (Kafka producer).

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "checkout-api: ok (stub, not implemented yet)")
	})

	addr := ":8082"
	log.Printf("checkout-api listening on %s (stub)", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
