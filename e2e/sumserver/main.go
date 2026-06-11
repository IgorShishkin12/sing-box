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

type sumTermsRequest struct {
	Terms []int `json:"terms"`
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

// sumTermsHandler accepts {"terms":[...]} and returns their sum.
func sumTermsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sumTermsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	result := 0
	for _, t := range req.Terms {
		result += t
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sumResponse{Sum: result})

	log.Printf("sum-terms: %d terms = %d", len(req.Terms), result)
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
	http.HandleFunc("/sum-terms", sumTermsHandler)
	http.HandleFunc("/health", healthHandler)

	listenHost := "127.0.0.1"
	if h := os.Getenv("LISTEN_ADDR"); h != "" {
		listenHost = h
	}
	addr := fmt.Sprintf("%s:%d", listenHost, port)
	log.Printf("sum server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
