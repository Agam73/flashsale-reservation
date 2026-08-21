package main

import (
	"fmt"
	"log"
	"net/http"
)

// decision-service is the authoritative Kafka consumer: one partition per
// item, single writer per item, writes the final reservation to Postgres.
// Real logic lands in Phase 6.

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "decision-service: ok (stub, not implemented yet)")
	})

	addr := ":8083"
	log.Printf("decision-service health endpoint on %s (stub, Kafka consumer not yet running)", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
