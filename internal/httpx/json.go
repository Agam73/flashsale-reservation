// Package httpx holds tiny helpers shared by waiting-room-api and
// checkout-api so JSON responses look the same shape across both
// services instead of each main.go inventing its own.
package httpx

import (
	"encoding/json"
	"log"
	"net/http"
)

// WriteJSON encodes v as JSON and writes it with the given status code.
// Errors encoding v are logged rather than surfaced to the client --
// the status line and headers are already sent by the time encoding
// could fail.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("httpx: encoding JSON response: %v", err)
	}
}

// ErrorResponse is the JSON body shape for every error response, so
// clients can rely on a consistent {"error": "..."} regardless of which
// service or endpoint returned it.
type ErrorResponse struct {
	Error string `json:"error"`
}

// WriteError writes a JSON error body with the given status code.
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, ErrorResponse{Error: message})
}
