use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::runtime::{Builder, Runtime};

static RUNTIME: once_cell::sync::OnceCell<Mutex<Option<Arc<Runtime>>>> =
    once_cell::sync::OnceCell::new();

fn get_runtime_lock() -> &'static Mutex<Option<Arc<Runtime>>> {
    RUNTIME.get_or_init(|| Mutex::new(None))
}

/// Lock the runtime mutex, recovering from a poisoned state if a previous
/// test panicked while holding the lock.
fn lock_runtime() -> std::sync::MutexGuard<'static, Option<Arc<Runtime>>> {
    get_runtime_lock().lock().unwrap_or_else(|poisoned| {
        log::warn!("runtime mutex was poisoned, recovering");
        poisoned.into_inner()
    })
}

/// Initialize the Tokio runtime if not already initialized.
pub fn init_runtime() -> Result<(), String> {
    log::debug!("init_runtime called");
    let mut guard = lock_runtime();
    if guard.is_some() {
        log::debug!("Runtime already initialized");
        return Ok(());
    }
    log::debug!("Building runtime (multi-thread)");
    match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => {
            *guard = Some(Arc::new(rt));
            log::info!("Runtime created successfully");
            Ok(())
        }
        Err(e) => {
            log::error!("Failed to create Tokio runtime: {}", e);
            Err(e.to_string())
        }
    }
}

/// Thread-local flag to detect reentrant `block_on` calls.
///
/// `current_thread` runtime's `block_on` is NOT thread-safe — calling it
/// concurrently from multiple threads causes data races on the runtime's
/// internal state. However, calling it reentrantly from the *same* thread
/// is safe (e.g., `reticulum_listen` calls `block_on` twice in sequence).
///
/// We use a global `Mutex<()>` to serialize across threads, but skip it
/// when we detect we're already inside a `block_on` on this thread.
thread_local! {
    static IN_BLOCK_ON: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

static BLOCK_ON_LOCK: once_cell::sync::OnceCell<Mutex<()>> = once_cell::sync::OnceCell::new();

fn get_block_on_lock() -> &'static Mutex<()> {
    BLOCK_ON_LOCK.get_or_init(|| Mutex::new(()))
}

/// Check if the runtime has been initialized.
pub fn has_runtime() -> bool {
    let guard = match get_runtime_lock().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.is_some()
}

/// Execute a future on the runtime and block on it.
/// This is used by the CGO layer to run async operations.
/// If the runtime has been shut down, it will be re-initialized automatically.
///
/// NOTE: `block_on` does not require `Send + 'static` — unlike `tokio::spawn`,
/// this runs the future directly on the current thread. The `'static` bound
/// was previously present and caused spurious memory issues (SIGSEGV under
/// test parallelism with OnceCell).
pub fn block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    // Check if we're already inside a block_on on this thread.
    // If so, skip the serial lock to avoid deadlock on reentrant calls.
    let is_reentrant = IN_BLOCK_ON.with(|cell| cell.replace(true));
    let _serial = if !is_reentrant {
        Some(get_block_on_lock().lock().unwrap_or_else(|poisoned| {
            log::warn!("block_on mutex was poisoned, recovering");
            poisoned.into_inner()
        }))
    } else {
        None
    };

    // Fast path: runtime is already initialized
    {
        let guard = lock_runtime();
        if let Some(rt) = guard.as_ref() {
            let result = rt.block_on(f);
            if !is_reentrant {
                IN_BLOCK_ON.with(|cell| cell.set(false));
            }
            return result;
        }
    }
    // Runtime was shut down — re-initialize; error already logged inside.
    let _ = init_runtime();
    let guard = lock_runtime();
    let rt = guard
        .as_ref()
        .expect("Runtime not initialized after re-init");
    let result = rt.block_on(f);
    if !is_reentrant {
        IN_BLOCK_ON.with(|cell| cell.set(false));
    }
    result
}

/// Spawn a future on the runtime without blocking the calling thread.
///
/// Panics if the runtime is not initialized. Call `init_runtime` first.
pub fn spawn<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let guard = lock_runtime();
    guard
        .as_ref()
        .expect("runtime not initialized; call reticulum_init first")
        .spawn(f)
}

/// Shutdown the bridge runtime.
///
/// Tokio's `current_thread` runtime has thread-local state that cannot be
/// safely recreated after being dropped — doing so causes SIGSEGV/SIGABRT
/// on subsequent `Runtime::block_on` calls. Therefore we *do not* drop the
/// runtime. We simply mark it as "logically shut down" so that `block_on`
/// knows to re-init (which is a no-op since the runtime still exists).
///
/// The store and registry are cleared by the caller (reticulum_shutdown).
pub fn shutdown() {
    log::debug!("shutdown called");
    // Keep the runtime alive — don't drop it.
    // The Mutex will always hold Some(Arc<Runtime>) after first init.
    let guard = lock_runtime();
    if guard.is_some() {
        log::debug!("Runtime shut down (logical)");
    } else {
        log::warn!("Runtime was not initialized, nothing to shut down");
    }
}
