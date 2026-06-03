# Reticulum Tunnel Latency Analysis

## Benchmark summary

Baseline (direct HTTP) vs. SOCKS5 → Reticulum tunnel, Docker localhost:

```
================================================================
BASELINE (direct HTTP, no proxy)
================================================================

=== Benchmark: direct ===
Profile                    | Req/s   | Mean    | p50     | p95     | p99     | Max
------------------------------------------------------------------------------------------
c=1  r=100 small           | 3359.3  | 297.00µs | 244.00µs | 459.00µs | 3.72ms  | 3.72ms
c=1  r=100 large(3000)     | 672.4   | 1.49ms  | 1.41ms  | 1.94ms  | 5.70ms  | 5.70ms
c=5  r=100 large(3000)     | 1349.2  | 3.45ms  | 1.79ms  | 24.55ms | 26.46ms | 26.46ms
c=20 r=100 large(3000)     | 2712.5  | 6.59ms  | 5.27ms  | 18.91ms | 21.19ms | 21.19ms
c=1  r=100 large(300)      | 2412.6  | 414.00µs | 461.00µs | 549.00µs | 956.00µs | 956.00µs
c=1  r=100 large(3000)     | 812.2   | 1.23ms  | 1.22ms  | 1.69ms  | 2.16ms  | 2.16ms
c=20 r=100 large(300)      | 8538.2  | 1.96ms  | 1.94ms  | 3.80ms  | 4.38ms  | 4.38ms
c=20 r=100 large(3000)     | 2265.3  | 6.96ms  | 5.89ms  | 17.58ms | 21.44ms | 21.44ms


================================================================
RETICULUM (SOCKS5 → reticulum tunnel)
================================================================

=== Benchmark: proxy ===
Profile                    | Req/s   | Mean    | p50     | p95     | p99     | Max
------------------------------------------------------------------------------------------
c=1  r=100 small           | 12.0    | 83.00ms | 83.09ms | 84.72ms | 85.24ms | 85.24ms
c=1  r=100 large(3000)     | 18.6    | 53.64ms | 54.79ms | 63.65ms | 82.21ms | 82.21ms
c=5  r=100 large(3000)     | 69.4    | 68.73ms | 66.84ms | 98.16ms | 111.90ms | 111.90ms
c=20 r=100 large(3000)     | 79.5    | 230.88ms | 224.15ms | 338.63ms | 366.05ms | 366.05ms
c=1  r=100 large(300)      | 11.9    | 83.75ms | 84.09ms | 86.21ms | 86.81ms | 86.81ms
c=1  r=100 large(3000)     | 19.0    | 52.68ms | 53.38ms | 61.42ms | 63.91ms | 63.91ms
c=20 r=100 large(300)      | 320.1   | 50.03ms | 46.42ms | 77.63ms | 99.00ms | 99.00ms
c=20 r=100 large(3000)     | 84.6    | 217.86ms | 208.66ms | 336.18ms | 365.12ms | 365.12ms

```

Key observations:
- CPU at ~0% for c=1 runs — all time is waiting, not computing
- CPU at ~95% for c=20 large — but "not at max speed": lock contention, not useful work
- `small` and `large(300)` have **identical** 83ms despite 36× different payload sizes
- `large(3000)` is paradoxically **faster** (53ms) than smaller payloads

## Root causes

### 1. Cold-start tokio task wakeup (the 83ms floor)

Data delivery from the library goes through **4 separate tokio tasks**:

```
TCP rx_task
  → rx_channel (mpsc)
  → manage_transport task  (holds handler_arc lock)
  → post_event → link_event_tx broadcast
  → spawn_link_data_forwarder task
  → received_data_tx broadcast
  → spawn_link_data_reader task (bridge)
  → call_on_data() → Go
```

With c=1 sequential requests, after the request is sent the runtime goes idle.
Tokio workers park. When the response arrives, each task boundary requires an OS
futex wakeup (~5–30ms on lightly loaded Linux). Four hops × two directions = the
~80ms base cost every request pays when the pipeline is cold.

Why `large(3000)` is faster: sending ~92 mux fragments (~18 KB) takes ~50ms of
continuous activity, keeping all tasks warm. The small response arrives while
tasks are still scheduled, paying no cold-start penalty. Small and large(300)
(1–10 fragments) finish quickly and let the runtime park before the response
arrives — paying the full cold-start both ways.

This is a **library architecture issue** (`transport/core.rs:96–141` in
reticulum-rs-transport): the intermediate `spawn_link_data_forwarder` task adds
an extra broadcast hop that can be eliminated.

### 2. `writeMu` global serialization (the c=20 bottleneck)

All mux virtual connections share a single write mutex (`mux.go`). Under
c=20 × 92 fragments, 20 goroutines queue behind one lock for sequential
BridgeWrite calls. This is why CPU hits 95% — threads are spinning on futex
waits rather than doing crypto. The fix: move `writeMu` into `muxConn` (per
stream) rather than `muxSession` (per physical link).

### 3. Unnecessary proof packets on every send

`connection.rs` uses `link.data_packet()` which sets `PacketContext::None`.
The library's receiver unconditionally sends a proof packet back for this context
(`link.rs:455`). The bridge never waits for or uses these proofs. At c=20 with
large payloads this roughly doubles the Reticulum packet count, contributing to
CPU load and handler lock contention.

Fix: add a public method to the library:
```rust
pub fn no_proof_packet(&self, data: &[u8]) -> Result<Packet, RnsError> {
    self.packet_with_context(data, PacketContext::Request)
}
```
`PacketContext::Request` delivers data identically but skips the proof response
(`link.rs:458`). Change `connection.rs:92` to call `no_proof_packet` instead of
`data_packet`.

### 4. TCP_NODELAY not set

The library's `tcp_client.rs` and `tcp_server.rs` never call `set_nodelay(true)`.
With multi-fragment payloads, Nagle's algorithm can buffer small writes until a
TCP ACK arrives, adding up to 200ms on the second write. Add after connect:
```rust
stream.set_nodelay(true).ok();
```

### 5. Dial polling (100ms sleep loop)

`transport.rs` polls link activation every 100ms instead of waking on the
`LinkEvent::Activated` broadcast. This only affects first-connection setup, not
per-request latency. The library does fire `LinkEvent::Activated` and
`LinkEvent::Closed`, so the polling loop can be replaced with `tokio::select!`
on `link_events.recv()`.

## Timeouts inventory

| Location | Value | Necessary? |
|---|---|---|
| `auth.go` `authTimeout` | 10s | Yes — dead peer after TCP connect |
| `transport.rs` `DIAL_TIMEOUT` | 30s | Yes (while polling loop exists) |
| `transport.rs` `DIAL_POLL_INTERVAL` | 100ms | No — replace with event |
| `transport.rs` `ANNOUNCE_INTERVAL` | 5s | Yes — periodic re-announce |
| `transport.rs` `exchange_identify_on_link` | 5s | Yes — peer crash after activate |
| `c_api.rs` name resolve retry | 3s→6s→12s | Yes — cold path |
| library `LOCAL_PATH_RESPONSE_COOLDOWN` | 750ms | LoRa-specific, irrelevant on TCP |
| library channel retry floor | 25ms | LoRa-specific floor |
| library channel send window start | 2 | LoRa-conservative, should be higher on TCP |

LoRa-specific values in the library are hardcoded and not configurable. They
should be exposed as `TransportConfig` fields so TCP deployments can use
appropriate values (e.g. window start = 48, cooldown = 0).

## Fixes ranked by impact

1. **Move `writeMu` to per-stream** — fixes c=20 CPU and latency. Your code.
2. **Add `no_proof_packet()` to library** — halves packet count, halves CPU at
   high concurrency. Library fork (one-liner).
3. **TCP_NODELAY in library** — eliminates Nagle buffering. Library fork
   (one-liner per interface).
4. **Replace dial polling with event** — cleaner code, minor latency win on first
   connect. Your code (TODO already noted in `transport.rs`).
5. **Reduce 4-hop chain in library** — removes the fundamental ~30ms cold-start
   floor. Requires more invasive library changes.
6. **Expose LoRa timing as config** — allows tuning for TCP. Library change.

## How to instrument

Add Go timestamps in `mux.go` around each BridgeWrite call and around the full
`Write`→`Read` cycle. Five sequential c=1 requests with `log.Printf` will
immediately show whether the 83ms is dominated by write latency (Rust/TCP path)
or read latency (response delivery, i.e. the cold-start).

Enable `RETICULUMD_DIAGNOSTICS=1` to get the library's own packet-level tracing
on stderr (gated `eprintln!` calls in `tcp_client.rs`).
