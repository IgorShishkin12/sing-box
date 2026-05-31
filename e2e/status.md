# E2E Test Status

## 2026-05-31 — Current state

### TCP simple
**PASSES** — single sequential connection, warm-up ~1s.

### TCP length test
**PASSES** — all lengths including 512, 128, 118.
Fix: fragment encoding rewritten (mux.go). Old 3-bit totalCode field couldn't represent 8–15
total fragments; replaced with 1-bit isLast + 6-bit partIndex scheme.

### TCP loadtest (5 goroutines × 4 requests)
**PASSES** — 20/20 requests succeed.

### UDP loadtest
**PASSES** — blazingly fast.
Fix: server-udp.json and client-udp.json both have explicit forward_ip/forward_port so the
UDPInterface is bidirectional (previously server had no forward address and couldn't reply).

### Auto loadtest (AutoInterface / multicast)
**PASSES** — warm-up ~1.3s, 20/20 requests at ~460 req/s.
Fix: AutoInterface now uses UDP multicast (239.255.0.1) instead of broadcast (255.255.255.255).
The underlying socket2 socket never sets SO_BROADCAST, so sends to 255.255.255.255 were
silently dropped (EACCES). Multicast requires no extra socket options and is reliably flooded
by Linux bridges (Docker/Podman) when IGMP snooping is off.

---

## Phase 1 (framedConn + AuthIO)
Status: **pending** — TCP/UDP/auto tests all pass without it; revisit if concurrent load
reveals auth-ordering issues at higher concurrency.

## Phase 2 (async Rust bridge)
Status: **pending**

## Phase 3 (H1 dial serialization)
Status: **pending** (re-evaluate after Phase 2)
