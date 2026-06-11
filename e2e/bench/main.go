// e2e-bench measures latency and throughput of the reticulum proxy across
// multiple load profiles, with support for a direct (no-proxy) baseline mode.
//
// Mode "direct": plain HTTP to -url (no SOCKS5). Used for baseline measurements.
// Mode "proxy":  HTTP over SOCKS5 tunnel to -url. Used for reticulum measurements.
//
// After an initial warm-up, five profiles run in sequence:
//
//	c=1  r=30   small payload (/sum)
//	c=5  r=50   small payload
//	c=20 r=100  small payload
//	c=1  r=10   large payload (/sum-terms, 1000 terms)
//	c=5  r=30   large payload
//
// Each profile prints one row: req/s, mean, p50, p95, p99, max.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type profile struct {
	concurrency int
	requests    int
	largeTerms  int // 0 = small /sum payload, >0 = /sum-terms with N terms
}

var profiles = []profile{
	{1, 100, 0},
	{1, 100, 3000},
	{5, 100, 3000},
	{20, 100, 3000},
	{1, 100, 300},
	{1, 100, 3000},
	{20, 100, 300},
	{20, 100, 3000},
}

func main() {
	mode := flag.String("mode", "proxy", "Connection mode: direct or proxy")
	socksAddr := flag.String("socks", "127.0.0.1:1080", "SOCKS5 proxy address (mode=proxy)")
	targetURL := flag.String("url", "http://127.0.0.1:8080", "Base URL of the sum-server")
	warmupTimeout := flag.Duration("warmup-timeout", 60*time.Second, "Max time for warm-up phase")
	outputFile := flag.String("output", "", "Write results table to this file in addition to stdout")
	flag.Parse()

	if *mode != "direct" && *mode != "proxy" {
		log.Fatalf("unknown mode %q: must be direct or proxy", *mode)
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// ── Warm-up ──────────────────────────────────────────────────────────────
	log.Printf("Warm-up: mode=%s url=%s (max %v) ...", *mode, *targetURL, *warmupTimeout)
	warmupStart := time.Now()
	for {
		var err error
		if *mode == "proxy" {
			err = doProxySum(*socksAddr, *targetURL+"/sum", 3, 0)
		} else {
			err = doDirectHealth(*targetURL + "/health")
		}
		if err == nil {
			log.Printf("Warm-up succeeded in %v", time.Since(warmupStart).Round(time.Millisecond))
			break
		}
		if time.Since(warmupStart) >= *warmupTimeout {
			log.Fatalf("Warm-up timed out after %v: %v", *warmupTimeout, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// ── Benchmark profiles ───────────────────────────────────────────────────
	out := io.Writer(os.Stdout)
	if *outputFile != "" {
		f, err := os.Create(*outputFile)
		if err != nil {
			log.Fatalf("open output file: %v", err)
		}
		defer f.Close()
		out = io.MultiWriter(os.Stdout, f)
	}

	fmt.Fprintf(out, "\n=== Benchmark: %s ===\n", *mode)
	printHeader(out)

	for it, p := range profiles {
		label := profileLabel(p)
		latencies, elapsed, err := runProfile(*mode, *socksAddr, *targetURL, p)
		if err != nil {
			log.Fatalf("Profile %s failed: %v", label, err)
		}
		printRow(out, label, latencies, elapsed)
		log.Printf("Completed %d steps of bench", it)
	}

	fmt.Fprintln(out)
	log.Printf("BENCHMARKS COMPLETE")
}

// runProfile executes one benchmark profile. Returns per-request latencies and
// the wall-clock duration of the concurrent run (for accurate throughput).
func runProfile(mode, socksAddr, baseURL string, p profile) ([]time.Duration, time.Duration, error) {
	latencies := make([]time.Duration, 0, p.requests)
	var mu sync.Mutex
	var failures atomic.Int64

	var wg sync.WaitGroup
	perGoroutine := p.requests / p.concurrency
	if perGoroutine < 1 {
		perGoroutine = 1
	}

	wallStart := time.Now()
	for g := 0; g < p.concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				start := time.Now()
				var err error
				if p.largeTerms > 0 {
					terms := makeTerms(p.largeTerms)
					expected := sumTerms(terms)
					if mode == "proxy" {
						err = doProxySumTerms(socksAddr, baseURL+"/sum-terms", terms, expected)
					} else {
						err = doDirectSumTerms(baseURL+"/sum-terms", terms, expected)
					}
				} else {
					if mode == "proxy" {
						err = doProxySum(socksAddr, baseURL+"/sum", 3, 5)
					} else {
						err = doDirectSum(baseURL+"/sum", 3, 5)
					}
				}
				elapsed := time.Since(start)
				if err != nil {
					failures.Add(1)
					log.Printf("request failed: %v", err)
					continue
				}
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	wallElapsed := time.Since(wallStart)

	if failures.Load() > 0 {
		return nil, 0, fmt.Errorf("%d requests failed", failures.Load())
	}
	return latencies, wallElapsed, nil
}

// ── Output helpers ────────────────────────────────────────────────────────────

func profileLabel(p profile) string {
	payload := "small"
	if p.largeTerms > 0 {
		payload = fmt.Sprintf("large(%d)", p.largeTerms)
	}
	return fmt.Sprintf("c=%-2d r=%-3d %s", p.concurrency, p.requests, payload)
}

func printHeader(w io.Writer) {
	fmt.Fprintf(w, "%-26s | %-7s | %-7s | %-7s | %-7s | %-7s | %s\n",
		"Profile", "Req/s", "Mean", "p50", "p95", "p99", "Max")
	fmt.Fprintf(w, "%s\n", repeatStr("-", 90))
}

func printRow(w io.Writer, label string, latencies []time.Duration, wallElapsed time.Duration) {
	if len(latencies) == 0 {
		fmt.Fprintf(w, "%-26s | no data\n", label)
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	total := time.Duration(0)
	for _, d := range latencies {
		total += d
	}
	mean := total / time.Duration(len(latencies))
	throughput := float64(len(latencies)) / wallElapsed.Seconds()

	fmt.Fprintf(w, "%-26s | %-7s | %-7s | %-7s | %-7s | %-7s | %s\n",
		label,
		fmt.Sprintf("%.1f", throughput),
		fmtDur(mean),
		fmtDur(percentile(latencies, 50)),
		fmtDur(percentile(latencies, 95)),
		fmtDur(percentile(latencies, 99)),
		fmtDur(latencies[len(latencies)-1]),
	)
}

func percentile(sorted []time.Duration, pct int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted)*pct + 99) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func fmtDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Microseconds()))
	}
	return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
}

func repeatStr(s string, n int) string {
	out := make([]byte, len(s)*n)
	for i := 0; i < n; i++ {
		copy(out[i*len(s):], s)
	}
	return string(out)
}

// ── HTTP helpers (direct mode) ────────────────────────────────────────────────

func doDirectHealth(rawURL string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(rawURL)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health: status %d", resp.StatusCode)
	}
	return nil
}

func doDirectSum(rawURL string, a, b int) error {
	body, _ := json.Marshal(map[string]int{"a": a, "b": b})
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(rawURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var result struct {
		Sum int `json:"sum"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if result.Sum != a+b {
		return fmt.Errorf("wrong sum: got %d want %d", result.Sum, a+b)
	}
	return nil
}

func doDirectSumTerms(rawURL string, terms []int, expected int) error {
	body, _ := json.Marshal(map[string]any{"terms": terms})
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Post(rawURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var result struct {
		Sum int `json:"sum"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if result.Sum != expected {
		return fmt.Errorf("wrong sum: got %d want %d", result.Sum, expected)
	}
	return nil
}

// ── HTTP helpers (proxy mode, via SOCKS5) ─────────────────────────────────────

func doProxySum(socksAddr, rawURL string, a, b int) error {
	conn, err := proxyConn(socksAddr, rawURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	body, _ := json.Marshal(map[string]int{"a": a, "b": b})
	req, _ := http.NewRequest("POST", rawURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Transport: &http.Transport{Dial: func(_, _ string) (net.Conn, error) { return conn, nil }},
		Timeout:   30 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var result struct {
		Sum int `json:"sum"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if result.Sum != a+b {
		return fmt.Errorf("wrong sum: got %d want %d", result.Sum, a+b)
	}
	return nil
}

func doProxySumTerms(socksAddr, rawURL string, terms []int, expected int) error {
	conn, err := proxyConn(socksAddr, rawURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	body, _ := json.Marshal(map[string]any{"terms": terms})
	req, _ := http.NewRequest("POST", rawURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Transport: &http.Transport{Dial: func(_, _ string) (net.Conn, error) { return conn, nil }},
		Timeout:   60 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var result struct {
		Sum int `json:"sum"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if result.Sum != expected {
		return fmt.Errorf("wrong sum: got %d want %d", result.Sum, expected)
	}
	return nil
}

// proxyConn opens a SOCKS5 CONNECT to the host:port parsed from rawURL.
func proxyConn(socksAddr, rawURL string) (net.Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	destHost := u.Hostname()
	destPort := 80
	if p := u.Port(); p != "" {
		destPort, _ = strconv.Atoi(p)
	}
	return socks5Connect(socksAddr, destHost, destPort)
}

// socks5Connect performs a SOCKS5 CONNECT handshake (no-auth, RFC 1928).
func socks5Connect(socksAddr, destHost string, destPort int) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", socksAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}

	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("greeting: %w", err)
	}
	choice := make([]byte, 2)
	if _, err := io.ReadFull(conn, choice); err != nil {
		conn.Close()
		return nil, fmt.Errorf("greeting response: %w", err)
	}
	if choice[0] != 5 || choice[1] != 0 {
		conn.Close()
		return nil, fmt.Errorf("proxy requires auth (method 0x%02x)", choice[1])
	}

	host := []byte(destHost)
	req := make([]byte, 0, 7+len(host))
	req = append(req, 5, 1, 0, 3, byte(len(host)))
	req = append(req, host...)
	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, uint16(destPort))
	req = append(req, portBuf...)
	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, fmt.Errorf("CONNECT request: %w", err)
	}

	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		conn.Close()
		return nil, fmt.Errorf("CONNECT response header: %w", err)
	}
	if hdr[1] != 0 {
		conn.Close()
		return nil, fmt.Errorf("CONNECT failed: reply code 0x%02x", hdr[1])
	}
	switch hdr[3] {
	case 1:
		tail := make([]byte, 4+2)
		_, _ = io.ReadFull(conn, tail)
	case 3:
		lenBuf := make([]byte, 1)
		_, _ = io.ReadFull(conn, lenBuf)
		tail := make([]byte, int(lenBuf[0])+2)
		_, _ = io.ReadFull(conn, tail)
	case 4:
		tail := make([]byte, 16+2)
		_, _ = io.ReadFull(conn, tail)
	}

	return conn, nil
}

// ── Payload helpers ───────────────────────────────────────────────────────────

func makeTerms(n int) []int {
	terms := make([]int, n)
	for i := range terms {
		terms[i] = 1
	}
	return terms
}

func sumTerms(terms []int) int {
	s := 0
	for _, t := range terms {
		s += t
	}
	return s
}
