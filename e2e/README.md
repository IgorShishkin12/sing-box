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
| `real-device-test.sh` | Real arm64 Android phone over LAN ADB, TCP interface | `android-bins/` (built on first run) |

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
