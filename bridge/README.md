# sing-box-reticulum-bridge

Rust bridge crate providing a C FFI layer between the [sing-box](https://github.com/sagernet/sing-box) Go core and the [Reticulum](https://github.com/markqvist/Reticulum) network stack (via [`reticulum-rs`](https://crates.io/crates/reticulum-rs)).

## Features

- **`real-reticulum`** (default): Enables integration with the actual `reticulum-rs` transport layer. Without this feature, the crate compiles with stub/mock implementations suitable for unit testing.

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
# Rust tests
cargo test --features real-reticulum -- --test-threads=1

# Go tests (requires bridge static library)
CGO_ENABLED=1 go test -v -tags with_reticulum ./protocol/reticulum/...
```

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
- Runs tests without and with the `real-reticulum` feature on every push/PR
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
│   ├── lib.rs               # Crate root
│   ├── c_api.rs             # C FFI exports
│   ├── config.rs            # Configuration parsing
│   ├── connection.rs        # Connection management
│   ├── listener.rs          # Listener management
│   ├── runtime.rs           # Tokio runtime management
│   ├── store.rs             # Identity/destination store
│   ├── task.rs              # Async task registry
│   └── transport.rs         # Reticulum transport layer
├── tests/                   # Integration tests
├── include/
│   └── reticulum_bridge.h   # C header for FFI
├── Cargo.toml
└── README.md
```

## E2E Testing

The E2E test setup uses Docker Compose to create two containers:

```bash
docker compose -f docker-compose.e2e.yml up --build
```

- **Server container**: Runs a sum HTTP server on localhost:8080 and a bridge listener that forwards reticulum connections to it.
- **Client container**: Dials the server via reticulum, sends an HTTP POST with two numbers, and verifies the sum response.

No ports are exposed to the host — all communication happens through the reticulum tunnel over a shared Docker network.