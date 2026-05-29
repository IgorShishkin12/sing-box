# E2E Auth Failure Debug

## Observed symptoms

Client-side warnings (from `docker-compose.tcp.yml` full load test):
```
WARN outbound/reticulum[reticulum-out]: [rns_transport::destination::link] can't create data packet for closed link
WARN outbound/reticulum[reticulum-out]: [rns_transport::destination::link] close /b92545c2916fe0b88c9628682469fab7/
```

## What these messages mean (confirmed by reading source)

- `can't create data packet for closed link` — `link.rs:843-844`, fires when `link.status` is not
  `Active` or `Stale` and we attempt a write. It is just a warning; the encrypt call follows and
  fails, causing `connection.write()` to return `Err`, `BridgeWrite` to return -1.
- `close /hash/` — `link.rs:1152` inside `finalize_local_close()`. Called from our
  `reticulum_close` → `link.close()` → `finalize_local_close()`.

ORDER: `can't create` appears BEFORE `close /hash/`. Therefore the link was **already Closed**
before Go called `BridgeClose`. The second message is from our own cleanup.

## Code path summary

```
Go DialContext
  → BridgeDial → reticulum_dial (Rust, async task)
    → dial_and_wait: polls link.status() until Active, returns Ok((link, link_id))
  → call_on_connect → Go goOnConnect → resultCh receives connID
Go creates reticulumConn + framedConn
Go calls framedConn.Write (trust hint)
  → reticulumConn.Write → BridgeWrite → reticulum_write → conn.write → link.data_packet
    → packet_with_context: warns if link not Active/Stale, then tries encrypt
```

```
Go Close (deferred in DialContext on error, or after request done)
  → framedConn.Close → reticulumConn.Close → BridgeClose → reticulum_close
    → store.remove(handle)
    → conn.link().lock().close()   ← calls finalize_local_close → logs "close /hash/"
    → call_on_close(handle)
```

## Fixes already applied (confirmed in current code)

1. `c_api.rs reticulum_close`: calls `conn.link().lock().close()` after removing from store.
   Intent: force a fresh link on the next dial (so `tp.link()` sees Closed and creates new one).
2. `framed_conn.go Write`: splits payloads > 400 B into multiple frames (MDU headroom).

## Hypotheses

---

### H1 — Concurrent dials all share the same Reticulum link [NOT DEBUNKED]

**Claim**: `tp.link(desc)` (in `transport/links.rs:452`) returns the existing `Arc<Mutex<Link>>`
if its status is not `Closed`. Multiple concurrent `BridgeDial` calls for the same destination
therefore all get the same link object.

**Consequence**:
- Server receives ONE `LinkEvent::Activated` → ONE `call_on_accept` → ONE server-side conn.
- All N client conn-handles point to the same `Arc<Mutex<Link>>` with the same `link_id`.
- `spawn_link_data_reader` for each conn filters on the same `link_id` → every received packet
  is pushed to ALL N data channels simultaneously → multiplexing is broken.
- When ANY one connection closes (`reticulum_close` → `link.close()`), the link becomes Closed
  for ALL. Any subsequent write by the remaining connections gets "can't create data packet for
  closed link".

**Evidence**: Confirmed by reading `transport/links.rs:464`:
```rust
if status != LinkStatus::Closed {
    return link;  // reuses existing link
}
```

**Applies to**: Phase 2 (5 concurrent goroutines). Does NOT explain simple-tcp failure (1 request).

**Status**: CONFIRMED BY CODE READING (the sharing happens). Whether it is THE cause of the
observed failure depends on whether Phase 2 is what's failing. Needs a test run to confirm.

---

### H2 — Docker build cache served stale binary [PARTIALLY DEBUNKED]

**Claim**: During the previous debug session (at 01:49), the docker Go-build step (`COPY .
/workspace/sing-box` then `go build`) was served from cache despite Rust source changes.

**Analysis**: The Dockerfile copies `./bridge` FIRST and runs `cargo build --release` in its own
layer. Then it copies the full source (`. /workspace/sing-box`) and runs `go build`. If `bridge/`
files changed on disk, the Rust layer would rerun and invalidate the Go layer too.

The 01:49 observation (same cache hash) was true AT THAT MOMENT because the Rust source edits had
not yet been written to disk at that point in the session. The fixes at 01:23 ARE in the current
files. A fresh `podman-compose ... --build` will pick them up.

**Status**: DEBUNKED for the CURRENT state. The fixes from 01:23 ARE in c_api.rs and
framed_conn.go. A fresh build will compile them. Run `podman-compose -f docker-compose.tcp.yml up
--build --no-cache` to be safe.

---

### H3 — simple-tcp test (single request) still fails for a different reason [NOT DEBUNKED]

**Claim**: Even without concurrency, the single-request test might fail because the server closes
the link for an unrelated reason before the client's first write.

**Sub-hypotheses for WHY the link closes on the server side**:

#### H3a — Server auth fails and server closes the conn, but client link stays alive

When the server calls `framedConn.Close()` (because auth failed), it calls
`reticulumConn.Close()` → `BridgeClose` → `reticulum_close` → `link.close()`. This closes the
SERVER's view of the link. But does it notify the CLIENT?

`link.close()` calls `finalize_local_close()` which does NOT send a `LinkClose` packet over the
wire. So the CLIENT's link stays Active.

The client would then try to write data. The client's link is still Active (not Closed). So the
client would NOT get "can't create data packet for closed link".

**Status**: This sub-hypothesis does not explain the observed error message (client's link must
be Closed). DEBUNKED BY LOGIC for the single-request case.

#### H3b — Client link is prematurely Closed via receiving a LinkClose packet from the server

For the server to send a `LinkClose` to the client, the server's code must call `teardown()` (not
just `close()`). Looking at `reticulum_close` → `link.close()` (not `teardown()`). And
`reticulumConn.Close()` → `BridgeClose` → `reticulum_close` → `link.close()` (still not
`teardown()`).

There is no call to `teardown()` in our bridge code. So the server never sends a LinkClose
packet to the client. Client's link cannot become Closed via a server-initiated teardown.

**Status**: DEBUNKED BY LOGIC. Server never calls teardown(), never sends LinkClose wire packet.

#### H3c — Watchdog closes the client link during auth [DEBUNKED]

Default keepalive is 360s, stale_time = 720s. Auth is way faster than that.

**Status**: DEBUNKED BY LOGIC.

#### H3d — Discovery knock closes the service link [NOT DEBUNKED]

`reticulum_resolve_name` spawns a discovery knock (`dial_discovery_and_wait`) to the DISCOVERY
destination (different hash). This creates a discovery link. The service link is a different link.
They should not interfere.

**Status**: VERY UNLIKELY. But not tested. Low priority.

---

### H4 — For the simple-tcp test: link is genuinely Active but encrypt fails due to library bug [NOT DEBUNKED]

**Claim**: The link's `can_exchange_data()` returns false for some reason other than Closed status.
Looking at the enum: `Active | Stale` → can exchange data. `Pending | Closed` → cannot. The link
was Active when `dial_and_wait` returned it. 

Could it have transitioned Pending → Closed (short-circuit without going through Active)?
`dial_and_wait` returns an error if `Closed | Stale` is seen during polling. So it only returns
`Ok` when `Active`. The returned link IS active.

But between returning `Ok` and Go executing the first write, could the link go Active → Closed?
The only things that can do this are:
- Our own `close()` call: Not yet called.
- Receiving a LinkClose wire packet: Server doesn't send one (H3b debunked).
- Watchdog: Too slow (H3c debunked).

So if the link was Active when returned and stays Active until the first write: no problem.

But WHAT IF `data_packet()` itself transitions the link? Looking at `packet_with_context`: it only
warns, it doesn't change status. But `encrypt_packet_data_into` could panic if `session_cipher` is
None... Let me check if there's a case where Active link has no cipher.

A link is Active only after the LINK_RTT exchange, which sets up the session key. So an Active
link should always have a valid `session_cipher`. This should not cause the error.

**Status**: UNLIKELY. Needs investigation only if H1 is fully debunked.

---

## Root cause found (2026-05-29, session 3)

### H5 — CLIENT's trust-hint write fails silently → negotiateAuth fails → link closes

**Claim**: The `conn.write` log is emitted BEFORE `data_packet()` is called. If
`data_packet()` returns `Err`, `reticulum_write` returns -1. Go sees a `WriteMsg` error →
`negotiateAuth` returns error → `fc.Close()` → `reticulumConn.Close()` → link closes.

**Observed evidence** (from goOnClose / dispatch tracing, timestamp logs):
```
12:07:31 [client] conn.write: conn=1 link=/0bf5d3d.../ status=Active len=2   ← logged BEFORE data_packet
12:07:31 [client] [rns] close /0bf5d3d.../                                    ← link.close() from reticulum_close(1)
12:07:31 [client] [bridge] goOnClose: no dataCh found for conn 1 (already closed or never registered)
             ← reticulumConn.Close() already deleted dataCh from map before BridgeClose
12:07:31 [client] [framed_conn] dispatch: ReadMessage error: EOF
             ← c.closed was closed by reticulumConn.Close(); dispatch calls fc.close()
12:07:31 [server] conn.write: len=2, 12, 65, 3, 65 (server auth)
12:07:36 [server] outbound connection to 127.0.0.1:8080
12:07:36 [server] close /link/
12:07:36 [server] dispatch: ReadMessage error: EOF
```

**Sequence on CLIENT**:
1. conn.write log fires (before packet creation)
2. `data_packet()` returns Err → `reticulum_write` returns -1 → Go `reticulumConn.Write` returns error
3. `framedConn.WriteMsg` returns error → `negotiateAuth` returns error → `fc.Close()` called
4. `fc.Close()` → `reticulumConn.Close()` → `connDataChans.Delete(1)` + `BridgeClose(1)` + `close(c.closed)`
5. `close(c.closed)` → dispatch sees `c.closed` → "dispatch: ReadMessage error: EOF" + `fc.close()`
6. `BridgeClose(1)` → `reticulum_close(1)` → `link.close()` → "close /link/" + `call_on_close(1)`
7. `goOnClose(1)` → `LoadAndDelete(1)` → not found (already deleted in step 4) → "no dataCh found"

**The "no dataCh found" is NOT a bug — it's the expected result of `reticulumConn.Close()` deleting the
channel before the subsequent `call_on_close` fires.**

**Open question**: WHY does `data_packet()` fail when status=Active? Two sub-hypotheses:

#### H5a — Encryption fails because `ingress_iface` is None

`conn.write()` calls `link_guard.ingress_iface()` AFTER `data_packet()`. If `data_packet()` itself
fails, the iface doesn't matter. But if `data_packet()` succeeds and only `send_direct` fails…
actually `send_direct` failure is silent (no error return). So this can't be the cause.

#### H5b — link status is NOT Active at the time of `data_packet()`

The log says `status=Active` but the status is read SEPARATELY from `data_packet()`. Both hold the
mutex, but they're sequential reads. The status could change between the log and `data_packet()`.

Actually: the mutex IS held across BOTH lines:
```rust
let link_guard = self.link.lock().await;    // mutex acquired
let status = link_guard.status();           // read 1
log::warn!(..., status, ...);               // log
let packet = link_guard.data_packet(data)  // read 2 (same lock)
    .map_err(...)?;
```
`link_guard` holds the mutex for both reads. So status CANNOT change between log and
`data_packet()`. If status=Active is logged, `can_exchange_data()` is true and the warning is not
fired.

#### H5c — Encryption fails because `ingress_iface` is None, causing `send_broadcast` to not send

Wait, `data_packet()` → `encrypt_packet_data_into()` → `encrypt()`. If `session_cipher` is Some
and functional (Active link should have it), encryption succeeds. `data_packet()` should return Ok.

#### H5d — The trust hint write SUCCEEDS but a subsequent write fails

Maybe it's NOT the trust hint write (len=2) that fails. Maybe a LATER write fails (client salt
len=65 or client HMAC len=65) and we're misreading the log order. The `conn.write` log for len=2
appears in the output, but the subsequent writes (len=65 × 2) do NOT appear.

**CONFIRMED: this is the most likely explanation.** The CLIENT's auth sequence should be:
- Write trust hint (len=2) ← LOGGED
- Read server trust hint (ReadMsg, via dispatch → ctrlCh)
- Write client salt (len=65) ← NOT LOGGED → this is where failure occurs
- ...

But `ReadMsg` reads from `ctrlCh`. `ctrlCh` is filled by `dispatch()` from link data. If the
SERVER's auth messages arrive before `ReadMsg` is called, they're buffered. But if `dispatch()` is
already closed (fc.closed), `ReadMsg` returns EOF immediately without waiting.

**But**: the "dispatch: ReadMessage error: EOF" appears AFTER "close /link/". The dispatch is
closed by `reticulumConn.Close()` (step 4 above). Step 4 happens only after `fc.Close()` in step
3. Step 3 happens only after `negotiateAuth` returns error. So dispatch is closed AFTER auth fails.

This means `ReadMsg()` in step "Read server trust hint" ALSO returns EOF. The auth fails on the
READ step, not the write step. The trust hint write succeeded, then `ReadMsg` returned EOF because
`fc.closed` was already set from somewhere.

#### H5e — `fc.closed` is set BEFORE `ReadMsg` is called, but AFTER the trust hint write

For `fc.closed` to be set, `fc.close()` must be called. `fc.close()` is called by `dispatch()` on
error. For dispatch to error, `ReadMessage()` must fail. For `ReadMessage()` to fail, `dataCh`
(the `reticulumConn.dataCh`, NOT `fc.dataCh`) must be closed, OR `c.closed` must fire.

**`c.closed`** can only be fired by `reticulumConn.Close()`. But that's in step 4, after auth fails.
**`dataCh`** can be closed by `goOnClose(1)`. But `goOnClose(1)` shows "no dataCh found" — meaning
the channel was already deleted. It was deleted by `reticulumConn.Close()` (in step 4). But that's
a chicken-and-egg problem again.

**UNLESS**: there's a DIFFERENT `reticulumConn.Close()` call from a PRIOR connection with the same
conn_id=1. If a previous dial left a conn_id=1 in a bad state...

Actually: `goOnConnect` stores a NEW channel for conn_id=1. Then `newReticulumConn(1)` reuses it.
If `goOnClose(1)` fired from a PREVIOUS call and closed+deleted the channel, then `goOnConnect`
for conn_id=1 would store a NEW channel. But `newReticulumConn(1)` would load THIS new channel.
So as long as the channel stored in `connDataChans[1]` is the same instance that `reticulumConn.dataCh`
points to, there should be no issue.

The "no dataCh found" proves `reticulumConn.Close()` ran (which deletes the entry). This proves
`fc.Close()` was called. And `fc.Close()` is only called on error. So auth DID fail.

**STILL OPEN**: what caused the FIRST failure that triggered `fc.Close()`? The auth error path is:
- Write trust hint → ReadMsg → FAILS → auth error → `fc.Close()`

`ReadMsg` fails when `fc.closed` fires before the server's trust hint arrives in `ctrlCh`.

`fc.closed` fires from `fc.close()` from `dispatch()` when `ReadMessage()` fails.

`dispatch()` runs in a separate goroutine. Is there a race where `dispatch()` errors BEFORE the server's auth data arrives?

If `dispatch()` is the FIRST goroutine to run after `go fc.dispatch()` is called, it immediately tries to call `ReadMessage()`. At that point, the server's trust hint might not have arrived yet in `dataCh`. So `ReadMessage()` would BLOCK (not error), waiting for data.

The only way `ReadMessage()` returns an error is if `dataCh` or `c.closed` signals. But `dataCh` is a fresh channel (from `goOnConnect`) and `c.closed` is a fresh channel. Neither is closed.

**FINAL HYPOTHESIS**: There must be a timing issue where the CLIENT closes the conn BEFORE the server's first auth message arrives. If the pipe terminates (no response from server in time) OR if there's a context cancellation, `fc.Close()` could be called while dispatch is waiting.

**NEXT STEP**: add logging at the actual WriteMsg/ReadMsg call sites in negotiateAuth to confirm WHICH message fails and with what error.

## Confirmed findings (empirical, from actual test runs)

### simple-tcp test: PASSES after fixes

Added `conn.write: status={:?}` logging (WARN level) in `connection.rs write()`.
Full write sequence for a successful request:
```
link /7a4b.../ activated (event) cur_status=Active, dial successful
conn.write: conn=1 link=/7a4b.../ status=Active len=2    ← trust hint
conn.write: conn=1 link=/7a4b.../ status=Active len=65   ← HMAC challenge
conn.write: conn=1 link=/7a4b.../ status=Active len=65   ← HMAC response
conn.write: conn=1 link=/7a4b.../ status=Active len=15   ← dest header
conn.write: conn=1 link=/7a4b.../ status=Active len=154  ← HTTP request (curl payload)
can't create data packet for closed link                  ← teardown, after request done
```

The warning fires AFTER the HTTP data was delivered, during connection teardown.
Root cause of the warning at teardown: sing-box's connection pipe may still attempt writes after
`reticulum_close` marks the link Closed. Harmless — request completed before the link closed.

### Previous "can't create on first write" failures
Those failures were from the old binary (before the `reticulum_close → link.close()` fix). With
the fix, the link is properly closed after each connection so `tp.link()` creates a fresh one on
the next dial, and the SERVER fires a new `call_on_accept` each time.

## Next steps (in order)

1. **DONE**: simple-tcp passes — the single-request case is fixed.
2. **Run full loadtest** (docker-compose.tcp.yml) to verify H1 (concurrent link sharing) is
   also fixed. If 5 concurrent goroutines work correctly, the issue is resolved end-to-end.
3. **Cleanup**: Remove the temporary WARN logging from `connection.rs write()` or downgrade to
   DEBUG.

## Proposed fix for H1 (concurrent link sharing)

In `reticulum_dial` (transport.rs, `dial_and_wait`), before calling `tp.link()`, if an existing
non-Closed link is found, call `teardown()` on it and wait for it to become Closed. This forces
each dial to create a fresh link.

But this breaks concurrent use (closing an in-progress connection to start a new one).

**Better fix**: Serialize dial attempts to the same destination with a per-destination async
`Mutex`. Only one dial can be in-flight at a time. Others queue behind it. This doesn't enable
true concurrent multi-link (which would require protocol multiplexing), but makes sequential
reuse correct.
