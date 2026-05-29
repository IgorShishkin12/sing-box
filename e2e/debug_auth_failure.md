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

## Next steps (in order)

1. **Run a SINGLE-REQUEST test first** (simple-tcp), with a fresh `--no-cache` build, and collect
   full logs. Determine if the single request (non-concurrent) passes after the 01:23 fixes.

2. **If single request passes**: The concurrent load test (H1) is the remaining issue. Fix: prevent
   link sharing between concurrent dials. Options:
   - Serialize dials (allow only one active link per destination at a time)
   - In `reticulum_dial`, before calling `tp.link()`, check if there's an existing non-Closed link
     and wait/close it first.
   - Multiplex multiple logical connections over one link (major protocol change).

3. **If single request fails**: Collect full trace-level logs. Add logging around the
   `link.data_packet()` call to print the link status at the exact moment of failure.

## Proposed fix for H1 (concurrent link sharing)

In `reticulum_dial` (transport.rs, `dial_and_wait`), before calling `tp.link()`, if an existing
non-Closed link is found, call `teardown()` on it and wait for it to become Closed. This forces
each dial to create a fresh link.

But this breaks concurrent use (closing an in-progress connection to start a new one).

**Better fix**: Serialize dial attempts to the same destination with a per-destination async
`Mutex`. Only one dial can be in-flight at a time. Others queue behind it. This doesn't enable
true concurrent multi-link (which would require protocol multiplexing), but makes sequential
reuse correct.
