package main

import (
	"fmt"
	"log"
	"net/http"
)

// waiting-room-api admits buyers into the sale at a controlled rate and
// tracks queue position. Real logic lands in Phase 2.

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "waiting-room-api: ok (stub, not implemented yet)")
	})

	addr := ":8081"
	log.Printf("waiting-room-api listening on %s (stub)", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
