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
                   │ raw Reticulum packets (≤120 B)
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
                   │ UDP / TCP / AutoInterface / RNode (LoRa)
       Reticulum network (mesh)
```

Ordering, acknowledgement, flow control and retransmission are handled by the
native Reticulum **Channel** that sits under the bridge. The mux keeps no send
window, in-flight table, fragment ACKs, or retransmit loop of its own — it only
splits messages into Channel-sized fragments and reassembles them in order.

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

**Timeout:** each round has a 40 s idle deadline (timer starts after WriteMsg, not before) (`authTimeout`). Once the peer starts responding, `ReadMsg` waits the *sooner* of the global 40 s deadline or a 20 s per-message inactivity deadline (`authInactivityTimeout`).

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

Maximum packet size: **120 bytes**.  
Maximum payload: **117 bytes** (120 − 3 header bytes).

The 120-byte cap is derived from the LoRa/RNode serial link MTU of 220 bytes:
after Reticulum's per-packet overhead (header, IFAC, IV, worst-case AES padding,
HMAC) the largest plaintext that still fits one on-wire packet is 120 bytes (see
`mux.go:16–27`). Links with a larger MTU negotiate a bigger per-session payload
via the bridge (`reticulum_get_conn_max_payload`); 120 is the conservative default.

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
| `0xC0` | `TypeLargeData` | whole oversized message (see below) |

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

Two-fragment example (120 bytes split into 117 + 3):
```
fragment 0:  [0x00][connID][117 bytes]   (isLast=0, partIndex=0)
fragment 1:  [0x41][connID][3 bytes]     (isLast=1, partIndex=1)
```

### Fragmentation limits

| Limit | Value |
|---|---|
| Max fragments per message | 64 (6-bit partIndex) |
| Max fragment payload | 117 B |
| Max reassembled message | 64 × 117 = **7,488 B** |

Fragments are reassembled in-order within each virtual connection. A lost fragment stalls that message but does not affect other virtual connections on the same session.

### Large messages (`TypeLargeData`)

A message that exceeds the 64-fragment limit (> ~7.5 KB) is not fragmented by the
mux. Instead it is sent whole as a single `TypeLargeData` (`0xC0`) control packet,
which the Rust bridge ships over the Reticulum **Resource** protocol (bulk
transfer with its own segmentation and reliability) rather than as ordinary
Channel packets. The receiver delivers the reassembled payload to the target
virtual connection as one message. See `mux.go` (`writeData`, `TypeLargeData`
handling).

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
  "rust_log": "info",                         // RUST_LOG tracing filter for the bridge
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

// RNode LoRa radio over a serial device
{
  "name": "lora0",
  "type": "RNodeSerial",
  "device": "/dev/ttyUSB0",   // required
  // shared LoRa radio params (unset → US915 band defaults):
  "frequency_hz":     915000000,
  "bandwidth_hz":     125000,
  "tx_power_dbm":     17,
  "spreading_factor": 8,
  "coding_rate":      5           // 5–8, maps to 4/5 … 4/8
}

// RNode LoRa radio over BLE (Android / btleplug)
{
  "name": "lora-ble0",
  "type": "RNodeBLE",
  "peripheral_id": "RNode 1234",  // required; BLE name or MAC address
  // same shared LoRa radio params as RNodeSerial
  "frequency_hz": 915000000
}
```

---

## Hardcoded Internal Limits

| Constant | Value | Source | Description |
|---|---|---|---|
| `MaxReticulumMessage` | 120 B | `mux.go:28` | Default (LoRa) mux-packet size cap |
| `muxHeaderSize` | 3 B | `mux.go:29` | 1 typeByte + 2 connID |
| `maxFragPayload` | 117 B | `mux.go:30` | `MaxReticulumMessage − muxHeaderSize` |
| Max fragments / message | 64 | `mux.go` (`encodeDataByte`) | 6-bit `partIndex` field |
| Max reassembled payload | 7,488 B | derived | 64 × 117 (before `TypeLargeData` kicks in) |
| Max virtual connections | 65,535 | `mux.go` (`OpenConn`) | uint16 connID space |
| `authTimeout` | 40 s | `auth.go:31` | Global idle deadline for ReadMsg (starts after WriteMsg completes) |
| `authInactivityTimeout` | 20 s | `framed_conn.go:35` | Per-message idle deadline once the peer starts responding |
| Linear retry delay | 5 s | `auth.go:48` | Fixed inter-attempt pause |
| Exp retry base | 4 s × 2ⁿ | `auth.go:51` | 4 s → 8 s → 16 s → 32 s … |
| Salt / MAC size | 32 B | `auth.go:58` | Challenge and response lengths |
| `globalAcceptCh` buffer | 256 | `conn.go:83` | Rust→Go inbound connection events |
| Per-conn receive buffer | 256 | `conn.go:115` | Packet queue per `reticulumConn` |
| `incomingCh` buffer | 1024 | `mux.go:272` | Pending virtual conns (server side) |
| `muxConn.readCh` buffer | 1024 | `mux.go:626` | Assembled messages per virtual conn |
| `framedConn.ctrlCh` buffer | 8 | `framed_conn.go:73` | Auth control messages |
| `framedConn.dataCh` buffer | 128 | `framed_conn.go:74` | Data messages buffered before gate opens |
| `DIAL_TIMEOUT` | 10 s | `bridge/src/transport.rs:47` | Link activation deadline |
| `DIAL_POLL_INTERVAL` | 100 ms | `bridge/src/transport.rs:49` | Poll cadence during dial |
| `ANNOUNCE_INTERVAL` | 5 s | `bridge/src/transport.rs:52` | Service re-announce period |
| `IDENTIFY_TIMEOUT` | 30 s | `bridge/src/transport.rs:836` | Peer LinkIdentify exchange deadline |
| `IDENTIFY_RETRY_INTERVAL` | 3 s | `bridge/src/transport.rs:837` | LinkIdentify re-send cadence |
| `ANNOUNCE_WAIT` | 15 s | `bridge/src/c_api.rs:686` | Per-attempt name resolve wait |
| `MAX_ATTEMPTS` | 3 | `bridge/src/c_api.rs:687` | Name resolution retry count |
| `INITIAL_BACKOFF` | 3 s | `bridge/src/c_api.rs:688` | First retry delay for name resolve |
| Max resolve backoff | 30 s | `bridge/src/c_api.rs:702` | Backoff cap (doubles each attempt) |
