# E2E Tests

End-to-end tests for the Reticulum sing-box protocol. Each scenario brings up a server
and a client, runs a 3-phase load test (warm-up → concurrent load → optional large
messages), and exits non-zero on failure.

## Running all Docker tests

```bash
cd sing-box/e2e
./runner.sh
```

## Scenarios

| Compose file / script | What it tests | Config files |
|-----------------------|--------------|--------------|
| `docker-compose.simple-tcp.yml` | Single sequential connection over TCP | `configs/server.json`, `configs/client.json` |
| `docker-compose.length-test.yml` | Large message fragmentation and reassembly | `configs/server.json`, `configs/client.json` |
| `docker-compose.tcp.yml` | Concurrent load (5 goroutines × 4 requests), TCP interface | `configs/server.json`, `configs/client.json` |
| `docker-compose.udp.yml` | Concurrent load, UDP interface | `configs/server-udp.json`, `configs/client-udp.json` |
| `docker-compose.auto.yml` | Concurrent load, AutoInterface (link-local discovery) | `configs/server-auto.json`, `configs/client-auto.json` |
| `docker-compose.android-tcp.yml` | Android x86_64 emulator as client, TCP interface | `Dockerfile.android-client`, `android-entrypoint.sh` |
| `docker-compose.internet-proxy.yml` | Internet proxy isolation: client routes HTTP via SOCKS5 through Reticulum to the real internet | `configs/server.json`, `configs/client.json` |
| `real-device-test.sh` | Real arm64 Android phone over LAN ADB, TCP interface | `android-bins/` (built on first run) |
| `runner-hw.sh serial\|ble\|mixed` | Physical RNode LoRa modems: serial, BLE, or the mixed container-serial↔native-BLE path | `configs/{server,client}-{serial,ble}.json` |

## Pass / fail conditions

Each scenario documents its expected happy path, known protocol limits that are *supposed*
to fail, and known bugs where the test fails when it shouldn't.

### `simple-tcp`

**Passes:** always — baseline single connection; if this fails everything else is broken.

### `length-test`

**Passes:** current payload (512 × `1` in the `terms` array).

**Expected to fail (protocol limit, not a bug):** payloads large enough to require more
than 64 fragments. The fragment encoding in `mux.go` uses a 6-bit `partIndex` field
(max value 63) with a 1-bit `isLast` flag, so the hard limit is 64 fragments per
message. Exceeding it raises an explicit "too many fragments" error. Raising the limit
requires a protocol-breaking change to the frame format.

### `tcp` / `udp` / `auto`

**Passes:** all 20 requests (5 goroutines × 4 each) succeed. These are the primary
regression tests for concurrent correctness and interface-specific behaviour.

### `android-tcp`

**Passes:** when `/dev/kvm` is available on the Docker host (required for KVM
acceleration). Emulator boot takes ~1 minute; the image build is slow the first time
but cached on subsequent runs.

### `internet-proxy`

**Passes:** fetching a small HTTP response through the SOCKS5 proxy, e.g. `example.com`
(~1 KB, plain `Example Domain` page).

**Bug — fails when it shouldn't:** fetching `www.google.com` fails. Google's homepage is
substantially larger (full HTML with inline resources), so the response arrives in
multiple TCP segments and requires multi-packet stream reassembly. The failure does *not*
produce a "too many fragments" error, which distinguishes it from the length-test limit —
the fragment count is not the bottleneck. The likely cause is a bug in stream reassembly
or proxy buffering when a proxied HTTP response spans many Reticulum packets. Needs
investigation.

### `real-device-test.sh`

**Passes:** when a real arm64 Android device is connected via ADB and reachable over LAN.
Not included in `runner.sh`; run manually.

## Android tests

Two modes — emulator for CI, real device for local development.

### Emulator (`docker-compose.android-tcp.yml`)

KVM-accelerated x86_64 Android emulator runs inside the client container. The Android
SDK and system image are downloaded during `docker build` and cached in image layers, so
the slow initial build pays once and subsequent runs just boot the emulator (~1 min).

```bash
# Build (once)
docker compose -f docker-compose.android-tcp.yml build

# Run
docker compose -f docker-compose.android-tcp.yml up \
  --exit-code-from e2e-android-client --abort-on-container-exit
docker compose -f docker-compose.android-tcp.yml down
```

Requires `/dev/kvm` on the Docker host.

### Real device (`real-device-test.sh`)

Runs sing-box and the loadtest binary directly on a physical arm64 phone via ADB.
The PC acts as the Reticulum server; the script discovers the right PC IP automatically.

```bash
# One-time: connect phone (USB or network ADB)
adb connect <phone-ip>:5555   # if over Wi-Fi
adb devices                   # confirm one real device

# Run (builds arm64 binaries via Docker on first run, ~3 min)
./real-device-test.sh

# Override PC IP if auto-discovery fails (e.g. multi-homed host)
SERVER_IP=192.168.1.42 ./real-device-test.sh
```

Arm64 binaries are cached in `android-bins/` after the first build.

## Hardware RNode tests (`runner-hw.sh`)

Real-radio E2E over physical RNode LoRa modems. Not run in CI — invoke manually with
the device env vars set. All modes reuse the same sum-server / loadtest exchange; the
loadtest exit code is the verdict.

```bash
cd sing-box/e2e

# Both ends in containers, over USB serial (2× RNode on USB):
RNODE_SERIAL_SERVER=/dev/ttyUSB0 RNODE_SERIAL_CLIENT=/dev/ttyUSB1 ./runner-hw.sh serial

# Both ends in containers, over Bluetooth (2× RNode over BLE):
RNODE_BLE_SERVER="RNode 9999" RNODE_BLE_CLIENT="RNode 98EF" ./runner-hw.sh ble

# Mixed path: container/serial server ↔ LoRa ↔ native/BLE client:
RNODE_SERIAL_SERVER=/dev/ttyUSB0 RNODE_BLE_CLIENT="RNode 98EF" ./runner-hw.sh mixed
```

### `mixed` mode

Exercises the full path
**container → serial → RNode A → LoRa → RNode B → BLE → native host**.

- The **serial server** runs in a container ([docker-compose.mixed.yml](docker-compose.mixed.yml),
  server-only) with the host RNode A mapped to `/dev/ttyUSB0`.
- The **BLE client** runs **natively on the host** (BlueZ), not in a container —
  BLE-from-container is a pain, and the two ends communicate over LoRa RF only, so no
  shared network is needed. The runner builds/uses a native `sing-box`
  (`make build_with_bridge`, `rnode-ble` is a default Cargo feature) and native
  `e2e-loadtest`, then runs the client with `configs/client-ble.json`.
- Override the native binaries with `SINGBOX_BIN` / `LOADTEST_BIN`. Per-run logs land in
  `logs/mixed-<timestamp>/`.

> **LoRa params must match on both ends.** `frequency_hz`, `bandwidth_hz`,
> `spreading_factor`, and `coding_rate` must be identical or the link silently fails to
> form. The default pair `server-serial.json` ↔ `client-ble.json` is aligned at
> 433 MHz / 500 kHz / SF8 / CR6. The peripheral ID in `client-ble.json` /
> `server-ble.json` is a `${RNODE_BLE_CLIENT}` / `${RNODE_BLE_SERVER}` placeholder
> resolved by `envsubst` at run time.

## Key support files

| File | Purpose |
|------|---------|
| `Dockerfile.server` | Builds the server container (sing-box + sum-server) |
| `Dockerfile.client` | Builds the Linux client container (sing-box + loadtest) |
| `Dockerfile.android-client` | Builds the Android client container (emulator + Android binaries). Accepts `GOARCH`, `RUST_TARGET`, `NDK_CC` build args to target arm64 instead of x86_64 |
| `android-entrypoint.sh` | Entrypoint for the Android emulator container |
| `sumserver/main.go` | Simple HTTP server: `POST /sum {"a":N,"b":M}` → `{"sum":N+M}` |
| `loadtest/main.go` | 3-phase load test client (warm-up, concurrent, long-message) |

## Future: APK test (Phase 2)

When the sing-box-for-android app is built, `real-device-test.sh` can be switched to
install the APK and launch its VPN service instead of pushing a raw binary. See the
stub comment near the top of that script.
