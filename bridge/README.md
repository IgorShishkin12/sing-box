# sing-box-reticulum-bridge

Rust bridge crate providing a C FFI layer between the [sing-box](https://github.com/sagernet/sing-box) Go core and the [Reticulum](https://github.com/markqvist/Reticulum) network stack (via [`reticulum-rs`](https://crates.io/crates/reticulum-rs)).

## Features

- ~~**`real-reticulum`**~~: **Deprecated and removed.** The reticulum transport layer is now always compiled in; there is no longer a stub/mock alternative controlled by this flag.

## Architecture

### Sync/async boundary

Go calls the bridge via synchronous C FFI. Internally the bridge runs a **multi-thread Tokio runtime** (`Builder::new_multi_thread().enable_all()`). All FFI entry points cross the boundary using `runtime::block_on()` — with one key exception:

- **`reticulum_dial()` is non-blocking.** It spawns a Tokio task and returns immediately. When the link comes up (or fails), Rust fires the `on_connect(task_id, conn_id)` callback from a Tokio worker thread. `conn_id = 0` signals failure.
- **`reticulum_listen()` is blocking.** It waits synchronously until the listener is registered and returns the listener handle (or `-1` on error).
- **`reticulum_resolve_name()` is non-blocking.** It spawns a Tokio task and returns immediately. Result is delivered via `on_resolve(task_id, hash)` — `hash` is `NULL` on timeout, otherwise malloc'd (free with `reticulum_free`). Register the callback with `reticulum_set_resolve_callback()` before calling. Retries up to 3× with exponential backoff (3 s → 6 s → 12 s), 15 s per attempt.

### Reentrant `block_on` protection

`block_on()` is not re-entrant. A thread-local `Cell<bool>` (`IN_BLOCK_ON`) detects recursive calls on the same thread and skips the internal serial lock (which would deadlock). Calls from different threads serialize via a global `BLOCK_ON_LOCK` mutex.

`reticulum_shutdown()` aborts all registered background tasks, then drops the `Arc<Runtime>`, fully releasing the runtime. All three transport singletons (`TRANSPORT`, `TRANSPORT_IDENTITY`, `TRANSPORT_IDENTITY_HASH`) are cleared so a subsequent `reticulum_init()` starts fresh. All six callback atomics are zeroed last — after tasks are dead — to prevent any surviving task from invoking a dangling function pointer.

### Event delivery

Link data and close events are delivered via **Tokio broadcast channels**. Receivers that fall behind lose events:

- Data reader: lagged events are silently dropped (the link is treated as active).
- Service link monitor: lagged events emit a `warn!` log and the loop continues.

No backpressure is applied — if the Go side cannot consume events fast enough, events are lost.

### Service announce loop

A long-lived Tokio task sends periodic announces every 5 s. It is controlled via a `watch::Receiver<bool>` kill signal. If the sender is dropped, the task keeps sleeping rather than exiting.

### Logging

`logger.rs` installs a `tracing_subscriber::Layer` (`CLogLayer`) that converts every Rust `tracing` event into a call to the Go log callback registered via `reticulum_set_log_callback()`. Log levels map as: 1=Error, 2=Warn, 3=Info, 4=Debug, 5=Trace. If no callback is registered, log events are silently discarded.

## FFI Safety

**Callbacks are called from Tokio worker threads**, not from Go-started OS threads. Go's CGO runtime does not know about these threads. Callback implementations must be minimal — write to a channel and return. Doing heavy work or calling back into Rust from a callback is unsafe.

Six callback function pointers (`on_accept`, `on_connect`, `on_data`, `on_close`, `on_resolve`, `on_log`) are stored as `usize` atomics and transmuted to function pointers at call time. This is safe as long as:
1. Callbacks are registered before the first `reticulum_dial` / `reticulum_listen` / `reticulum_resolve_name` call.
2. `reticulum_shutdown()` is called before the Go side unloads any callback function.

All six atomics are zeroed at the end of `reticulum_shutdown()`, after all background tasks are aborted and the runtime is dropped, so no surviving task can call into freed Go memory.

**Memory ownership**: values returned by `reticulum_get_listener_hash()`, `reticulum_get_conn_peer_hash()`, `reticulum_get_conn_identified_peer()`, `reticulum_get_transport_hash()`, and `get_hash()` are allocated with `libc::malloc`. The caller must free them with `reticulum_free()`. Do **not** use Go's `C.free` — it uses a different allocator.

**Error conventions**:
- `reticulum_listen()` returns `-1` synchronously on error.
- `reticulum_dial()` fires `on_connect(task_id, 0)` asynchronously on failure.
- `reticulum_resolve_name()` fires `on_resolve(task_id, NULL)` on timeout or if the bridge is not initialized.
- Hash getter functions return `NULL` if the handle is invalid or the hash is not yet known.

**Mutex poison recovery**: if a Rust thread panics while holding an internal store or runtime lock, subsequent callers recover via `unwrap_or_else(|p| p.into_inner())` and continue. State may be partially cleared, but the process will not deadlock.

## Identity & Config

Service identity is resolved in this order:

1. **Explicit key**: `identity_key` field in the JSON config (128-char hex, 64-byte raw key).
2. **Persisted name**: `identity_name` field — loads or creates `<config_dir>/<name>-service.key`.
3. **Ephemeral**: generated fresh on every start, not saved.

Config directory search order: explicit `config_dir` → legacy `storage_path` → `$HOME/.reticulum` → `/etc/reticulum`.

`reticulum_resolve_name(task_id, name)` resolves a name via announce broadcast, non-blocking. It retries up to **3 times** with exponential backoff (3 s → 6 s → 12 s, capped at 30 s), with a 15 s timeout per attempt — up to ~69 s worst case. Result arrives via `on_resolve(task_id, hash)`; `hash` is `NULL` on timeout, otherwise malloc'd — free with `reticulum_free()`.

## Building

### Build the Rust bridge static library

```bash
cargo build --release
```

This produces `target/release/libsing_box_reticulum_bridge.a` (static library).

### Build sing-box with the bridge statically linked

```bash
# Using Makefile targets
make bridge          # Build the Rust bridge
make build_with_bridge  # Build sing-box with bridge statically linked

# Or manually
CGO_ENABLED=1 go build -v -trimpath -tags "$(cat release/DEFAULT_BUILD_TAGS_OTHERS),with_reticulum" ./cmd/sing-box
```

The `with_reticulum` Go build tag gates the bridge code. Without it, stub implementations are used that return `ErrBridgeNotAvailable`.

### Run tests

```bash
# Rust tests (single-threaded to avoid shared global runtime races)
cargo test -- --test-threads=1

# Go tests (requires bridge static library)
CGO_ENABLED=1 go test -v -tags with_reticulum ./protocol/reticulum/...
```

Rust unit tests use an **in-memory `Connection`** variant (`ConnectionInner::Memory`, `#[cfg(test)]`) backed by `Arc<RwLock<Vec<u8>>>` instead of a real Reticulum link. Integration tests in `tests/` exercise the full stack.

## Build tag: `with_reticulum`

The Go code uses a build tag `with_reticulum` to conditionally compile the CGO bridge code:

- **With `-tags with_reticulum`**: The real CGO bridge is compiled, linking against the Rust static library.
- **Without the tag**: Stub implementations are used that return `ErrBridgeNotAvailable`. This allows the rest of sing-box to compile without the Rust toolchain.

The build tag is added to the existing tag list (e.g., `with_gvisor,with_quic,...,with_reticulum`).

## Supported architectures

Currently, the bridge is supported for:
- `linux/amd64` (x86_64-unknown-linux-gnu)
- `linux/arm64` (aarch64-unknown-linux-gnu)

Support for additional architectures can be added by installing the appropriate Rust targets and cross-compilation toolchains.

### AutoInterface limitations

The `AutoInterface` network interface type is **experimental**. The implementation uses a single site-local UDP multicast group (`239.255.0.1`) rather than enumerating interfaces or using IPv6 link-local multicast. It works on most LANs and Docker/Podman bridge networks but is **not compatible with real Reticulum's AutoInterface** in multi-interface or IPv6 scenarios.

## Android Cross-Compilation

The crate can be cross-compiled for `aarch64-linux-android` (64-bit ARM Android).

### Prerequisites

1. **Rust target**: Install the Android target:
   ```bash
   rustup target add aarch64-linux-android
   ```

2. **Android NDK**: Install the Android NDK (r28 or later recommended) and set the `ANDROID_NDK_HOME` environment variable:
   ```bash
   export ANDROID_NDK_HOME=/path/to/android-ndk
   ```
   Common NDK locations:
   - `$HOME/Android/Sdk/ndk/<version>/`
   - `/opt/android-ndk/`
   - GitHub Actions: set automatically by `nttld/setup-ndk@v1`

### Build Script

Use the provided build script:

```bash
./scripts/build-android.sh          # debug build
./scripts/build-android.sh --release # release build
```

The script will:
- Verify `ANDROID_NDK_HOME` is set and points to a valid NDK
- Verify the `aarch64-linux-android` Rust target is installed
- Run `cargo build --target aarch64-linux-android`
- Print the location of build artifacts on success

### Manual Build

Alternatively, build manually:

```bash
export ANDROID_NDK_HOME=/path/to/android-ndk
export PATH="$PWD/scripts:$PATH"
export RUSTFLAGS="-C link-arg=-fPIC"
cargo build --target aarch64-linux-android
```

### CI

The GitHub Actions workflow (`.github/workflows/bridge.yml`) automatically:
- Runs tests without default features and with default features on every push/PR
- Cross-compiles for Android `aarch64` using the NDK installed via `nttld/setup-ndk@v1`
- Can be called as a reusable workflow from other workflows (e.g., `build.yml`, `linux.yml`, `docker.yml`)

## Project Structure

```
bridge/
├── .cargo/
│   └── config.toml          # Cargo linker config for Android targets
├── scripts/
│   ├── android-linker.sh    # NDK linker wrapper (reads $ANDROID_NDK_HOME)
│   └── build-android.sh     # Android cross-compile build script
├── src/
│   ├── lib.rs               # Crate root; global config singleton
│   ├── c_api.rs             # C FFI exports (14 public functions)
│   ├── config.rs            # JSON config parsing
│   ├── connection.rs        # Connection handle (real link or test buffer)
│   ├── listener.rs          # Listener handle
│   ├── logger.rs            # CLogLayer: tracing → Go log callback
│   ├── runtime.rs           # Tokio runtime singleton + block_on/spawn
│   ├── store.rs             # Global handle store (connections + listeners)
│   └── transport.rs         # Reticulum transport, dial, listen, announce
├── tests/                   # Integration tests
├── include/
│   └── reticulum_bridge.h   # C header for FFI
├── Cargo.toml
└── README.md
```

## E2E Testing

The E2E test setup uses Docker Compose to create two containers:

```bash
docker compose -f sing-box/docker-compose.e2e.yml up --build
```

- **Server container**: Runs a sum HTTP server on localhost:8080 and a bridge listener that forwards reticulum connections to it.
- **Client container**: Dials the server via reticulum, sends an HTTP POST with two numbers, and verifies the sum response.

No ports are exposed to the host — all communication happens through the reticulum tunnel over a shared Docker network.
