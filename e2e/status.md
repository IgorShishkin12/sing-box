# E2E Test Status

## 2026-05-29 — Baseline (Phase 0)

### TCP simple (phase-1 warmup)
**PASSES** — single sequential connection, auth + sum request succeeds in ~5.5 s.

### TCP Length test
**PASSES**: 4, 16, 64
**FAILS** : 512, 128, 118
Formula of actual length: 10 + n*3 - 2 + 2 = 10 + n*3

### TCP loadtest (phase-2 concurrent, 5 goroutines × 4 requests)
**FAILS** — 0/20 requests succeed.

Symptom (client): `reticulum auth failed: auth: read header: auth timeout`
Symptom (loadtest): `CONNECT failed: reply code 0x01` (SOCKS5 general error)

Root cause: **serial accept loop in inbound.go**. While the server is handling connection
N (auth → destHeader → route), no new `BridgeAccept` call is outstanding. The 5 concurrent
phase-2 connections establish at the Reticulum level (client logs "dial successful" for all)
but the server only accepts the first one; the other 4 time out after 10 s waiting for the
`SBRT-AUTH-1` header that the server never sends them.

---

## Phase 1 (framedConn + AuthIO)
Status: **in progress**

Changes:
- [ ] 1.1 conn.go: WriteMessage / ReadMessage
- [ ] 1.2 framed_conn.go
- [ ] 1.3 auth.go: AuthIO interface
- [ ] 1.4 trust_store.go
- [ ] 1.5 inbound.go: framedConn + goroutine-per-conn
- [ ] 1.6 outbound.go: framedConn + negotiateAuth
- [ ] 1.7 tests

## Phase 2 (async Rust bridge)
Status: **pending**

## Phase 3 (H1 dial serialization)
Status: **pending** (re-evaluate after Phase 2)
