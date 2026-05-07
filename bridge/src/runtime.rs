use std::sync::Arc;
use std::sync::Mutex;
use tokio::runtime::{Builder, Runtime};

static RUNTIME: once_cell::sync::OnceCell<Mutex<Option<Arc<Runtime>>>> = once_cell::sync::OnceCell::new();

fn get_runtime_lock() -> &'static Mutex<Option<Arc<Runtime>>> {
    RUNTIME.get_or_init(|| Mutex::new(None))
}

/// Initialize the Tokio runtime if not already initialized.
/// Returns 0 on success, -1 on error.
pub fn init_runtime() -> i32 {
    eprintln!("init_runtime called");
    let mut guard = get_runtime_lock().lock().unwrap();
    if guard.is_some() {
        eprintln!("Runtime already initialized");
        return 0;
    }
    eprintln!("Building runtime");
    match Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => {
            *guard = Some(Arc::new(rt));
            eprintln!("Runtime created successfully");
            0
        }
        Err(e) => {
            eprintln!("Failed to create Tokio runtime: {}", e);
            -1
        }
    }
}

/// Execute a future on the runtime and block on it.
/// This is used by the CGO layer to run async operations.
/// If the runtime has been shut down, it will be re-initialized automatically.
pub fn block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // Fast path: runtime is already initialized
    {
        let guard = get_runtime_lock().lock().unwrap();
        if let Some(rt) = guard.as_ref() {
            return rt.block_on(f);
        }
    }
    // Runtime was shut down — re-initialize
    init_runtime();
    let guard = get_runtime_lock().lock().unwrap();
    let rt = guard.as_ref().expect("Runtime not initialized after re-init");
    rt.block_on(f)
}

/// Shutdown the runtime and release all resources.
pub fn shutdown() {
    eprintln!("shutdown called");
    let mut guard = get_runtime_lock().lock().unwrap();
    if let Some(rt) = guard.take() {
        // Drop the Arc<Runtime> — this will shut down the runtime
        drop(rt);
        eprintln!("Runtime shut down");
    } else {
        eprintln!("Runtime was not initialized, nothing to shut down");
    }
}