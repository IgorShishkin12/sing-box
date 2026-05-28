// e2e-loadtest exercises the reticulum tunnel under concurrent load.
//
// Phase 1 — warm-up: retry SOCKS5 CONNECT + POST /sum until the first success.
// This single request triggers name resolution, dial, and the full SBRT-AUTH-1
// handshake, populating the persistent trust store on both sides.
//
// Phase 2 — load: immediately fan out N concurrent goroutines each making M
// requests via SOCKS5 CONNECT. These should hit the cached-trust fast path
// (both sides send trust=1), skipping the full handshake. All responses are
// verified to be {"sum":8}.
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
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	socksAddr := flag.String("socks", "127.0.0.1:1080", "SOCKS5 proxy address")
	targetURL := flag.String("url", "http://127.0.0.1:8080", "Base URL of the sum-server")
	concurrency := flag.Int("concurrency", 20, "Number of concurrent goroutines in phase 2")
	requests := flag.Int("requests", 100, "Total requests to send in phase 2")
	warmupTimeout := flag.Duration("warmup-timeout", 120*time.Second, "Max time for phase 1 warm-up")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// ── Phase 1: warm-up ────────────────────────────────────────────────────
	log.Printf("Phase 1: warming up (max %v) ...", *warmupTimeout)
	warmupStart := time.Now()
	for {
		err := doRequest(*socksAddr, *targetURL+"/sum", 3, 5)
		if err == nil {
			log.Printf("Phase 1: warm-up succeeded in %v", time.Since(warmupStart).Round(time.Millisecond))
			break
		}
		if time.Since(warmupStart) >= *warmupTimeout {
			log.Fatalf("Phase 1: warm-up timed out after %v: %v", *warmupTimeout, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// ── Phase 2: concurrent load ─────────────────────────────────────────────
	log.Printf("Phase 2: %d concurrent goroutines × %d requests each ...", *concurrency, *requests / *concurrency)
	var (
		wg      sync.WaitGroup
		success atomic.Int64
		failure atomic.Int64
	)
	start := time.Now()
	perGoroutine := *requests / *concurrency
	if perGoroutine < 1 {
		perGoroutine = 1
	}

	for g := 0; g < *concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if err := doRequest(*socksAddr, *targetURL+"/sum", 3, 5); err != nil {
					log.Printf("request failed: %v", err)
					failure.Add(1)
				} else {
					success.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := success.Load() + failure.Load()
	log.Printf("Phase 2: %d/%d requests succeeded in %v (%.1f req/s)",
		success.Load(), total, elapsed.Round(time.Millisecond),
		float64(total)/elapsed.Seconds())

	if failure.Load() > 0 {
		log.Printf("FAIL: %d requests failed", failure.Load())
		os.Exit(1)
	}
	log.Printf("ALL E2E TESTS PASSED")
}

// doRequest sends POST /sum via the SOCKS5 proxy and verifies the response.
func doRequest(socksAddr, rawURL string, a, b int) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	destHost := u.Hostname()
	destPort := 80
	if p := u.Port(); p != "" {
		destPort, _ = strconv.Atoi(p)
	}

	conn, err := socks5Connect(socksAddr, destHost, destPort)
	if err != nil {
		return fmt.Errorf("socks5 connect: %w", err)
	}
	defer conn.Close()

	body, _ := json.Marshal(map[string]int{"a": a, "b": b})
	req, _ := http.NewRequest("POST", rawURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	transport := &http.Transport{
		Dial: func(network, addr string) (net.Conn, error) {
			return conn, nil
		},
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var result struct {
		Sum int `json:"sum"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	expected := a + b
	if result.Sum != expected {
		return fmt.Errorf("wrong sum: got %d, want %d", result.Sum, expected)
	}
	return nil
}

// socks5Connect performs a SOCKS5 CONNECT to the given host:port through the
// proxy at socksAddr. Returns the established net.Conn on success.
// Implements RFC 1928 SOCKS5 CONNECT with no-auth method.
func socks5Connect(socksAddr, destHost string, destPort int) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", socksAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}

	// Greeting: VER=5, NMETHODS=1, METHOD=0 (no auth)
	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("greeting: %w", err)
	}
	// Server choice
	choice := make([]byte, 2)
	if _, err := io.ReadFull(conn, choice); err != nil {
		conn.Close()
		return nil, fmt.Errorf("greeting response: %w", err)
	}
	if choice[0] != 5 || choice[1] != 0 {
		conn.Close()
		return nil, fmt.Errorf("proxy requires auth (method 0x%02x)", choice[1])
	}

	// CONNECT request: VER=5, CMD=CONNECT, RSV=0, ATYP=DOMAINNAME
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

	// Read response (at least 10 bytes for IPv4 response)
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		conn.Close()
		return nil, fmt.Errorf("CONNECT response header: %w", err)
	}
	if hdr[1] != 0 {
		conn.Close()
		return nil, fmt.Errorf("CONNECT failed: reply code 0x%02x", hdr[1])
	}
	// Consume the bound address/port from the response.
	switch hdr[3] {
	case 1: // IPv4
		tail := make([]byte, 4+2)
		_, _ = io.ReadFull(conn, tail)
	case 3: // domain
		lenBuf := make([]byte, 1)
		_, _ = io.ReadFull(conn, lenBuf)
		tail := make([]byte, int(lenBuf[0])+2)
		_, _ = io.ReadFull(conn, tail)
	case 4: // IPv6
		tail := make([]byte, 16+2)
		_, _ = io.ReadFull(conn, tail)
	}

	return conn, nil
}
