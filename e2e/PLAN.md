# Plan: Fix Loadtest — framedConn, Async Bridge, H1 Dial Serialization

## Process notes
- Use **podman** / **podman-compose** for all container operations
- When running containers: redirect output to a `.log` file, then grep the file — never pipe container output into search commands (rebuilds are expensive)
- **Commit after each verified phase** with a message noting what was checked
- Keep this plan file in the repo (`e2e/PLAN.md`) and mark each step `[x]` once done
- Add `e2e/status.md` to track what fails and at which step after each run

---

## Context

**Branch**: `async_second_try` (HEAD = `cf1518a`, the E2E infra commit from last session).

**Failure**: The loadtest (phase-2 concurrent requests) fails with auth timeouts. The simple TCP e2e (one sequential connection) works fine. Two independent root causes have been identified by debugging on `reticulum-dev`:

1. **Protocol fragility**: The current auth uses `bufio.Reader` over a raw byte stream with no message-type framing. Auth messages and data bytes are indistinguishable, making the protocol fragile to ordering/timing under concurrent load.

2. **Serial accept loop**: `inbound.go`'s `acceptLoop` blocks on `BridgePollTask` + `ServerAuth` for connection N before calling `BridgeAccept` for connection N+1. Under concurrent load, incoming connections pile up unaccepted.

3. **H1 — Concurrent dials share a Reticulum link** (tentative, from debug doc `ee29cd0`): Multiple goroutines dialing the same destination simultaneously may race at the Reticulum link level.

**Reference commits on `reticulum-dev`**:
- `3bb9e6a` — framedConn + AuthIO + TrustStore
- `b09749189` — async Rust callbacks replacing polling
- `da79a801` — test updates for async
- `ee29cd0` — debug logging + H1 analysis (loadtest still failing after above changes)

**Strategy**: Three phases, each verified against the simple TCP e2e before proceeding. Full diff for each commit is available via `git show <hash>` in the repo.

---

## Phase 0: Baseline health-check (do first)

Before any code changes, verify the current state so we know what we're starting from.

- [ ] 0.1 — Build images: `podman-compose -f e2e/docker-compose.tcp.yml build 2>&1 | tee /tmp/build.log`
- [ ] 0.2 — Run TCP e2e, capture output: `podman-compose -f e2e/docker-compose.tcp.yml up --exit-code-from e2e-client ... 2>&1 | tee /tmp/tcp.log`
- [ ] 0.3 — Grep logs for PASS/FAIL, errors, auth messages
- [ ] 0.4 — Document findings in `e2e/status.md`
- [ ] 0.5 — Commit `e2e/status.md` + this plan as `e2e/PLAN.md` so progress is tracked in git

---

## Phase 1: framedConn + AuthIO (Go only, no Rust changes)

**Goal**: Add a 1-byte type prefix to every message so control traffic (auth) and data are never confused. Trust store avoids redundant full re-auth for known peers.

### 1.1 — `conn.go`: add `WriteMessage` / `ReadMessage`

Reticulum preserves message boundaries (each BridgeWrite → one BridgeRead), so no length prefix is needed.

Type byte encoding (high bit determines class):
```
0b0_______  (0x00)        — data / pass-through. Low 7 bits MUST be 0.
0b1_______  (0x80–0xFF)   — control. Low 7 bits encode sub-type.
  0x80                      AUTH_CTRL   (auth exchange message)
  0x81                      REAUTH_REQ  (peer requests re-auth)
```

Message format: `[1 byte type][payload bytes (rest of BridgeRead result)]`

```
const maxMessagePayload = <Reticulum MTU - 1>   // enforce on WriteMessage

WriteMessage(typ byte, payload []byte) error
  → if len(payload) > maxMessagePayload: log error, return error
  → single BridgeWrite([typ] + payload)

ReadMessage() (typ byte, payload []byte, err error)
  → single BridgeRead; byte[0] = type, byte[1:] = payload
```

These are the atomic I/O primitives that framedConn is built on top of.

### 1.2 — new `framed_conn.go`

```
const TypeData      = 0x00
const TypeAuthCtrl  = 0x80
const TypeReauthReq = 0x81

type AuthIO interface {
    ReadMsg()  ([]byte, error)
    WriteMsg([]byte) error
}

type framedConn struct {
    inner   *reticulumConn
    ctrlCh  chan []byte   // auth control messages
    dataCh  chan []byte   // data frames (buffered until OpenGate)
    gate    chan struct{}  // closed by OpenGate()
}
```

Reader goroutine (started in constructor): continuously calls `inner.ReadMessage()`, routes by type.  
`OpenGate()`: closes `gate`; the `Read()` method blocks on `gate` before serving from `dataCh`.  
`Write()`: calls `inner.WriteMessage(TypeData, b)`.  
`ReadMsg()` / `WriteMsg()`: use `ctrlCh` / `TypeAuthCtrl`.  
Also implements `net.Conn` (Read/Write/Close/LocalAddr/RemoteAddr/SetDeadline*).

### 1.3 — `auth.go`: switch to `AuthIO`

Replace `bufio.Reader` / `writeLine` / `readLine` with `AuthIO.ReadMsg` / `AuthIO.WriteMsg`.  
Auth messages become raw binary blobs (no `\n` delimiter needed).  
Signatures become:
```
ServerAuth(rw AuthIO, password string) error
ClientAuth(rw AuthIO, password string) error
```
Keep the same HMAC-SHA256 challenge-response logic; only the I/O layer changes.

### 1.4 — new `trust_store.go`

```
Token(password, peerHash string) []byte  // HMAC-SHA256(password, peerHash)

type TrustStore struct { mu sync.RWMutex; m map[string][]byte }
func (ts *TrustStore) Check(peerHash string, tok []byte) bool
func (ts *TrustStore) Store(peerHash string, tok []byte)
```

In-memory only (disk persistence is a nice-to-have, not required).

### 1.5 — `inbound.go`: wrap conn + negotiateAuth + OpenGate

```go
raw := newReticulumConn(handle, ...)
fc  := newFramedConn(raw)            // starts reader goroutine

if password != "" {
    if err := negotiateAuth(fc, password, peerHash, serverTrustStore); err != nil {
        fc.Close(); continue
    }
}
fc.OpenGate()

destAddr, err := readDestHeader(fc)  // fc now acts as net.Conn
router.RouteConnectionEx(..., fc, ...)
```

`negotiateAuth` (server side): send 1-byte hint (`0x01` = trusted / `0x00` = full auth needed), then branch.

**Also**: move the per-connection work into a goroutine so the accept loop immediately loops back to `BridgeAccept`:

```go
go func() {
    defer fc.Close()
    // negotiateAuth, OpenGate, readDestHeader, RouteConnectionEx
}()
```

### 1.6 — `outbound.go`: same pattern

Wrap conn in framedConn, call `negotiateAuth` (client side), `OpenGate`, then `writeDestHeader(fc, ...)`.

### 1.7 — Tests

- `auth_test.go`: use `chanAuthIO` test helper (pair of channels) instead of `net.Pipe` — no network involved
- `conn_test.go`: test `WriteMessage` / `ReadMessage` round-trip via an in-process `net.Pipe`
- Add framed_conn tests: verify DATA vs AUTH_CTRL routing, OpenGate unblocks Read

**[VERIFY]** `go test ./protocol/reticulum/...` passes; simple TCP e2e passes 5/5 runs.

---

## Phase 2: Async Rust bridge (from commit `b09749189`)

**Goal**: Replace the Go-side polling loop with Rust callbacks. Eliminates the serial `BridgePollTask` bottleneck and potential deadlocks under load.

### 2.1 — Rust `c_api.rs`

Replace task-polling model with four callback function pointers registered at init:
```rust
on_accept(listener_id: u64, conn_id: u64)
on_connect(dial_task_id: i32, conn_id: u64)   // or error
on_data(conn_id: u64, data: *const u8, len: usize)
on_close(conn_id: u64)
```

`reticulum_init` now takes these four callbacks.  
`reticulum_dial` / `reticulum_accept` no longer return task IDs — they fire callbacks when ready.  
`reticulum_poll` is removed or made a no-op.

### 2.2 — Rust `connection.rs`, `listener.rs`, `store.rs`

Simplified state: connections store only id + link. No internal read buffer (data arrives via `on_data`).

### 2.3 — `bridge_stub_reticulum.go`

Add exported Go callbacks (`//export goOnAccept` etc.) that populate Go channels:
```go
var globalAcceptCh = make(chan acceptEvent, 64)
var pendingDials    sync.Map  // dialTaskID → chan connResult
var connDataChs     sync.Map  // connID → chan []byte
```

Register callbacks in `BridgeInit`.

### 2.4 — `conn.go`: `reticulumConn` reads from `dataCh`

`Read()` now reads from `connDataChs[handle]` channel instead of polling `BridgeRead`.

### 2.5 — `inbound.go`: read from `globalAcceptCh`

`acceptLoop` becomes:
```go
for ev := range globalAcceptCh {
    go h.handleConn(ev.connID)
}
```

### 2.6 — `outbound.go`: channel wait for `on_connect`

`DialContext` registers a channel in `pendingDials[taskID]`, calls `BridgeDial`, then blocks on the channel.

### 2.7 — Rust tests (from `da79a801`)

Update all tests to use the callback model: atomic variables capture callback results; `wait_atomic_set` replaces `poll_until_done`.

**[VERIFY]** `cargo test -p sing_box_reticulum_bridge` passes; `go test ./protocol/reticulum/...` passes; simple TCP e2e passes 5/5 runs.

---

## Phase 3: H1 — Concurrent dial serialization (if still needed after Phase 2)

**Goal**: Prevent multiple goroutines from racing to establish a link to the same destination simultaneously.

### 3.1 — `outbound.go`

```go
var dialMu sync.Map  // key: destHash, value: *sync.Mutex

func perDestMu(destHash string) *sync.Mutex {
    mu, _ := dialMu.LoadOrStore(destHash, &sync.Mutex{})
    return mu.(*sync.Mutex)
}
```

In `DialContext`, lock `perDestMu(destHash)` before `BridgeDial`, unlock after connection handle is returned (or on error).

**[VERIFY]** Loadtest (phase-2 concurrent) passes without auth timeouts.

---

## Verification sequence

```bash
# After each phase:
cd sing-box
go test ./protocol/reticulum/...  # unit tests
go build ./...                    # must compile

# Full e2e (requires Docker):
cd e2e && bash runner.sh          # all three transports
```

For loadtest specifically: run `e2e-loadtest` directly with `--concurrency 10 --requests 50` via the TCP compose stack and check exit code 0.
