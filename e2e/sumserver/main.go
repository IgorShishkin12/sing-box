package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
)

type sumRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type sumResponse struct {
	Sum int `json:"sum"`
}

func sumHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sumRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	resp := sumResponse{Sum: req.A + req.B}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	log.Printf("sum: %d + %d = %d", req.A, req.B, resp.Sum)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
}

func main() {
	port := 8080
	if p := os.Getenv("PORT"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil {
			port = parsed
		}
	}

	http.HandleFunc("/sum", sumHandler)
	http.HandleFunc("/health", healthHandler)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	log.Printf("sum server listening on %s", addr)
	// DisableKeepAlives ensures each HTTP response closes the TCP connection.
	// Without this, the Reticulum tunnel's data relay never terminates cleanly.
	server := &http.Server{Addr: addr}
	server.SetKeepAlivesEnabled(false)
	log.Fatal(server.ListenAndServe())
}