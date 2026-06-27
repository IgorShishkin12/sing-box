package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

// bodySnippet returns a short, printable head or tail of b for diagnostics.
func bodySnippet(b []byte, head bool) string {
	const n = 96
	if len(b) <= n {
		return string(b)
	}
	if head {
		return string(b[:n])
	}
	return string(b[len(b)-n:])
}

// readBody reads the full request body and logs loudly if the number of bytes
// received does not match the advertised Content-Length (a strong signal of a
// truncated/corrupted transport stream).
func readBody(r *http.Request, route string) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	cl := r.Header.Get("Content-Length")
	if err != nil {
		log.Printf("%s: ERROR reading body after %d bytes (Content-Length=%s): %v", route, len(body), cl, err)
		return body, err
	}
	if want, perr := strconv.Atoi(cl); perr == nil && want != len(body) {
		log.Printf("%s: WARN body length mismatch: read %d bytes but Content-Length=%d (transport truncated/corrupted?)",
			route, len(body), want)
	}
	return body, nil
}

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

	body, err := readBody(r, "sum")
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}
	var req sumRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("sum: BAD REQUEST invalid JSON: %v | received %d bytes | head=%q tail=%q",
			err, len(body), bodySnippet(body, true), bodySnippet(body, false))
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

	body, err := readBody(r, "sum-terms")
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}
	var req sumTermsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		// Loud, diagnostic: the parse error + head/tail of the received body
		// reveal *where* the transport corrupted/truncated the stream.
		log.Printf("sum-terms: BAD REQUEST invalid JSON: %v | received %d bytes | head=%q tail=%q",
			err, len(body), bodySnippet(body, true), bodySnippet(body, false))
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	result := 0
	for _, t := range req.Terms {
		result += t
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sumResponse{Sum: result})

	log.Printf("sum-terms: %d terms = %d (%d bytes)", len(req.Terms), result, len(body))
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
