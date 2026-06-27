# Reticulum Protocol Plugin

A sing-box inbound/outbound plugin that tunnels TCP streams over the [Reticulum Network Stack](https://reticulum.network/). Multiple virtual connections are multiplexed over a single Reticulum link with optional mutual password authentication.

---

## Architecture

```
┌──────────────────────────────────────┐
│           sing-box router            │
└──────────────────┬───────────────────┘
                   │ net.Conn
┌──────────────────▼───────────────────┐
│   muxConn  (virtual TCP stream)      │
│   one per proxied connection         │
└──────────────────┬───────────────────┘
                   │ assembled []byte messages
┌──────────────────▼───────────────────┐
│   muxSession  (Go)                   │
│   multiplexes N virtual conns over   │
│   one Reticulum link; handles        │
│   fragmentation and reassembly       │
└──────────────────┬───────────────────┘
                   │ raw Reticulum packets (≤200 B)
┌──────────────────▼───────────────────┐
│   framedConn  (Go)                   │
│   separates auth control messages    │
│   from data; gates data until auth   │
│   completes                          │
└──────────────────┬───────────────────┘
                   │ message-boundary reads
┌──────────────────▼───────────────────┐
│   reticulumConn  (Go)                │
│   wraps Rust async callbacks into    │
│   synchronous Go channel reads       │
└──────────────────┬───────────────────┘
                   │ CGO / FFI
┌──────────────────▼───────────────────┐
│   Rust bridge  (libreticulumbridge)  │
│   async Tokio runtime; drives RNS    │
└──────────────────┬───────────────────┘
                   │ UDP / TCP / AutoInterface
       Reticulum network (mesh)
```

---

## Connection Flow

### Outbound (client side)

```
Outbound.DialContext(ctx, destination)
  │
  ├─ getOrCreateSession()          ← one session per dest hash (cached)
  │    │
  │    ├─ BridgeResolveName(name)  ← if Name set; blocks up to 3×15 s
  │    ├─ BridgeDial(destHash)     ← async; fires goOnConnect on success
  │    ├─ newReticulumConn()
  │    ├─ newFramedConn()          ← starts demux goroutine
  │    ├─ Auth / AuthWithRetry()   ← if password configured
  │    ├─ framedConn.OpenGate()    ← data unblocked
  │    └─ newMuxSessionClient()    ← starts readLoop goroutine
  │
  └─ session.OpenConn(destination)
       ├─ TypeNewConn → peer       ← "host:port" in payload
       └─ returns muxConn          ← caller reads/writes this
```

### Inbound (server side)

```
Inbound.Start()
  ├─ BridgeInit(configJSON)
  ├─ BridgeListen(listenHash)
  └─ acceptLoop()  ← goroutine
       │
       └─ globalAcceptCh ← goOnAccept callback fires here
            │
            └─ newReticulumConn()
                 └─ newFramedConn()         ← demux goroutine starts
                      └─ Auth / AuthWithRetry()
                           └─ framedConn.OpenGate()
                                └─ newMuxSessionServer()  ← readLoop starts
                                     │
                                     └─ incomingCh ← TypeNewConn packets
                                          │
                                          └─ handleConn() → sing-box router
```

---

## Auth Protocol

Password authentication uses a 2-round concurrent HMAC-SHA256 exchange. Both sides send and receive simultaneously so there is no designated initiator.

```
Client                                      Server
  │                                           │
  ├──[TypeRequestAuth=0x84][clientSalt]──────►│
  │◄─────────────[TypeRequestAuth=0x84][serverSalt]──┤
  │                                           │
  ├──[TypeResponseAuth=0x85][clientMAC]──────►│
  │◄────────────[TypeResponseAuth=0x85][serverMAC]───┤

clientMAC = HMAC-SHA256(key=password, data=clientID || serverSalt)
serverMAC = HMAC-SHA256(key=password, data=serverID || clientSalt)

Client verifies: serverMAC == HMAC-SHA256(password, serverID, clientSalt)
Server verifies: clientMAC == HMAC-SHA256(password, clientID, serverSalt)
```

Identity binding (committing both peer IDs into the MAC) prevents replay across different connections or peers.

**Salts** are 32 random bytes, generated fresh per connection but held stable across retry attempts.

**Timeout:** each round has a 30 s idle deadline (timer starts after WriteMsg, not before) (`authTimeout`).

**Retry policies** (`auth_retry` field):

| Value | Behavior |
|---|---|
| `""` / `"none"` | One attempt; fail on mismatch |
| `"linear"` | Retry forever with 5 s fixed delay |
| `"exp"` | Retry with exponential backoff: 4 s, 8 s, 16 s, 32 s, … |

---

## Packet Wire Format

All packets share a 3-byte header:

```
 0        1        2        3 …
┌────────┬────────┬────────┬──────────────────────┐
│typeByte│  connID (uint16 big-endian)  │  payload │
└────────┴────────┴────────┴──────────────────────┘
```

Maximum packet size: **200 bytes** (enforced by Reticulum message limit).  
Maximum payload: **197 bytes** (200 − 3 header bytes).

### typeByte encoding

**Control packets** — high bit set (0x80–0xFF):

| Byte | Constant | Payload |
|---|---|---|
| `0x80` | `TypeAuthCtrl` | Auth exchange |
| `0x81` | `TypeReauthReq` | Re-auth request |
| `0x82` | `TypeNewConn` | `"host:port"` string |
| `0x83` | `TypeCloseConn` | empty |
| `0x84` | `TypeRequestAuth` | 32-byte salt |
| `0x85` | `TypeResponseAuth` | 32-byte HMAC-SHA256 |

**Data packets** — high bit clear (0x00–0x7F):

```
bit 7: always 0
bit 6: isLast  — 1 if this is the final fragment
bits 5-0: partIndex — 0-indexed fragment number (0–63)
```

Single-message example (13 bytes, one fragment):
```
typeByte = 0b01000000 = 0x40   (isLast=1, partIndex=0)
packet:  [0x40][0x12][0x34]["Hello, world!"]
```

Two-fragment example (200 bytes split into 197 + 3):
```
fragment 0:  [0x00][connID][197 bytes]   (isLast=0, partIndex=0)
fragment 1:  [0x41][connID][3 bytes]     (isLast=1, partIndex=1)
```

### Fragmentation limits

| Limit | Value |
|---|---|
| Max fragments per message | 64 (6-bit partIndex) |
| Max fragment payload | 197 B |
| Max reassembled message | 64 × 197 = **12,608 B** |

Fragments are reassembled in-order within each virtual connection. A lost fragment stalls that message but does not affect other virtual connections on the same session.

---

## Configuration Reference

### Inbound

```jsonc
{
  "type": "reticulum",
  // --- listen options (standard sing-box) ---
  "listen": "127.0.0.1",
  "listen_port": 1080,

  // --- reticulum identity / network ---
  "reticulum_config": { /* ReticulumConfig, see below */ },
  "reticulum_config_path": "/path/to/reticulum.json", // alternative to inline

  // --- destination ---
  "destination": "aabbccdd...",  // hex hash to listen on (takes priority over name)
  "name": "myservice.rns",       // resolved to hash on start

  // --- auth ---
  "password": "s3cr3t",          // optional; both sides must use same password
  "auth_retry": "linear"         // "", "none", "linear", or "exp"
}
```

### Outbound

```jsonc
{
  "type": "reticulum",
  // --- dialer options (standard sing-box) ---

  "reticulum_config": { /* ReticulumConfig, see below */ },
  "reticulum_config_path": "/path/to/reticulum.json",

  "destination": "aabbccdd...",  // hex hash to dial
  "name": "myservice.rns",       // resolved async; cached after first resolution

  "password": "s3cr3t",
  "auth_retry": "exp",
  "auth_on_start": true          // eagerly dial and auth when sing-box starts
}
```

### ReticulumConfig

```jsonc
{
  "identity_path": "/var/lib/rns/identity",   // directory holding the identity key
  "storage_path":  "/var/lib/rns/storage",    // RNS persistent storage
  "config_dir":    "/var/lib/rns",            // RNS config directory
  "identity_key":  "base64...",               // inline identity (alternative to path)
  "identity_name": "mynode",                  // human-readable label
  "reticulum_config_path": "/etc/rns.cfg",    // path to native RNS config file
  "interfaces": [ /* []ReticulumInterface */ ]
}
```

### ReticulumInterface

`type` is required; other fields depend on the type.

```jsonc
// UDP broadcast/unicast
{
  "name": "udp0",
  "type": "UDPInterface",
  "listen_ip":    "0.0.0.0",
  "listen_port":  4242,
  "forward_ip":   "255.255.255.255",  // unicast or broadcast target
  "forward_port": 4242
}

// Outbound TCP connection to a hub
{
  "name": "tcp0",
  "type": "TCPClientInterface",
  "target_host": "hub.example.com",
  "target_port": 4965
}

// Local-network auto-discovery (mDNS-like)
{
  "name": "auto0",
  "type": "AutoInterface",
  "data_port": 4242   // optional; 0 = OS-assigned
}
```

---

## Hardcoded Internal Limits

| Constant | Value | Source | Description |
|---|---|---|---|
| `MaxReticulumMessage` | 200 B | `mux.go:16` | Hard Reticulum message size cap |
| `muxHeaderSize` | 3 B | `mux.go:17` | 1 typeByte + 2 connID |
| `maxFragPayload` | 197 B | `mux.go:18` | `MaxReticulumMessage − muxHeaderSize` |
| Max fragments / message | 64 | `mux.go:40–48` | 6-bit `partIndex` field |
| Max reassembled payload | 12,608 B | derived | 64 × 197 |
| Max virtual connections | 65,535 | `mux.go:192` | uint16 connID space |
| `authTimeout` | 30 s | `auth.go` | Idle deadline for ReadMsg (starts after WriteMsg completes) |
| Linear retry delay | 5 s | `auth.go:40` | Fixed inter-attempt pause |
| Exp retry base | 4 s × 2ⁿ | `auth.go:43` | 4 s → 8 s → 16 s → 32 s … |
| Salt / MAC size | 32 B | `auth.go:50,90` | Challenge and response lengths |
| `globalAcceptCh` buffer | 256 | `conn.go:36` | Rust→Go inbound connection events |
| Per-conn receive buffer | 256 | `conn.go:68` | Packet queue per `reticulumConn` |
| `incomingCh` buffer | 64 | `mux.go:178` | Pending virtual conns (server side) |
| `muxConn.readCh` buffer | 64 | `mux.go:387` | Assembled messages per virtual conn |
| `framedConn.ctrlCh` buffer | 8 | `framed_conn.go:47` | Auth control messages |
| `framedConn.dataCh` buffer | 128 | `framed_conn.go:48` | Data messages buffered before gate opens |
| `DIAL_TIMEOUT` | 30 s | `bridge/src/transport.rs:36` | Link activation deadline |
| `DIAL_POLL_INTERVAL` | 100 ms | `bridge/src/transport.rs:38` | Poll cadence during dial |
| `ANNOUNCE_INTERVAL` | 5 s | `bridge/src/transport.rs:41` | Service re-announce period |
| `ANNOUNCE_WAIT` | 15 s | `bridge/src/c_api.rs:551` | Per-attempt name resolve wait |
| `MAX_ATTEMPTS` | 3 | `bridge/src/c_api.rs:552` | Name resolution retry count |
| `INITIAL_BACKOFF` | 3 s | `bridge/src/c_api.rs:553` | First retry delay for name resolve |
| Max resolve backoff | 30 s | `bridge/src/c_api.rs:567` | Backoff cap (doubles each attempt) |
| Identify timeout | 5 s | `bridge/src/transport.rs:696` | Peer LinkIdentify exchange deadline |
