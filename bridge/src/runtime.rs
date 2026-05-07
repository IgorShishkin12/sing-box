use once_cell::sync::OnceCell;
use tokio::runtime::{Builder, Runtime};
use std::sync::Arc;

static RUNTIME: OnceCell<Arc<Runtime>> = OnceCell::new();

/// Initialize the Tokio runtime if not already initialized.
/// Returns 0 on success, -1 on error.
pub fn init_runtime() -> i32 {
    eprintln!("init_runtime called");
    RUNTIME.get_or_try_init(|| {
        eprintln!("Building runtime");
        Builder::new_current_thread()
            .enable_all()
            .build()
            .map(Arc::new)
            .map_err(|e| {
                eprintln!("Failed to create Tokio runtime: {}", e);
                e
            })
    }).map(|_| {
        eprintln!("Runtime created successfully");
        0
    }).unwrap_or_else(|e| {
        eprintln!("Failed to create runtime: {:?}", e);
        -1
    })
}

/// Execute a future on the runtime and block on it.
/// This is used by the CGO layer to run async operations.
pub fn block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let rt = RUNTIME.get().expect("Runtime not initialized");
    rt.block_on(f)
}

/// Shutdown the runtime.
pub fn shutdown() {
    // No-op for stub.
}
