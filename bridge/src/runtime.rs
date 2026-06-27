use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

static RUNTIME: once_cell::sync::OnceCell<Mutex<Option<Arc<Runtime>>>> =
    once_cell::sync::OnceCell::new();

static TASK_HANDLES: once_cell::sync::OnceCell<Mutex<Vec<JoinHandle<()>>>> =
    once_cell::sync::OnceCell::new();

fn get_task_handles() -> &'static Mutex<Vec<JoinHandle<()>>> {
    TASK_HANDLES.get_or_init(|| Mutex::new(Vec::new()))
}

/// Register a task handle so it is aborted when `shutdown()` is called.
pub fn register_task(handle: JoinHandle<()>) {
    get_task_handles()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(handle);
}

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
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();

    // On Android with BLE enabled, attach each Tokio worker thread to the JVM as
    // a daemon thread so btleplug can invoke BLE callbacks from worker context.
    #[cfg(all(feature = "rnode-ble", target_os = "android"))]
    if let Some(jvm_addr) = crate::transport::android_jvm_addr() {
        builder.on_thread_start(move || {
            unsafe {
                let raw_jvm = jvm_addr as *mut jni::sys::JavaVM;
                if let Ok(jvm) = jni::JavaVM::from_raw(raw_jvm) {
                    let _ = jvm.attach_current_thread_as_daemon();
                }
            }
        });
    }

    match builder.build() {
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

// Thread-local flag to detect reentrant `block_on` calls.
//
// `current_thread` runtime's `block_on` is NOT thread-safe — calling it
// concurrently from multiple threads causes data races on the runtime's
// internal state. However, calling it reentrantly from the *same* thread
// is safe (e.g., `reticulum_listen` calls `block_on` twice in sequence).
//
// We use a global `Mutex<()>` to serialize across threads, but skip it
// when we detect we're already inside a `block_on` on this thread.
thread_local! {
    static IN_BLOCK_ON: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

static BLOCK_ON_LOCK: once_cell::sync::OnceCell<Mutex<()>> = once_cell::sync::OnceCell::new();

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
        Some(
            BLOCK_ON_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|poisoned| {
                    log::warn!("block_on mutex was poisoned, recovering");
                    poisoned.into_inner()
                }),
        )
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
/// Aborts all registered background tasks, then drops the Tokio runtime.
/// After this call, `has_runtime()` returns `false` and `init_runtime()` can
/// be used to start a fresh runtime.
///
/// The store is cleared by the caller (`reticulum_shutdown`) before this is
/// called.
pub fn shutdown() {
    log::debug!("shutdown called");
    {
        let mut handles = get_task_handles().lock().unwrap_or_else(|p| p.into_inner());
        log::debug!("aborting {} registered task(s)", handles.len());
        for handle in handles.drain(..) {
            handle.abort();
        }
    }
    let mut guard = lock_runtime();
    if guard.is_some() {
        *guard = None;
        log::info!("Runtime shut down");
    } else {
        log::warn!("Runtime was not initialized, nothing to shut down");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_clears_runtime() {
        init_runtime().unwrap();
        assert!(has_runtime());
        shutdown();
        assert!(!has_runtime(), "shutdown must clear the runtime");
        // Leave runtime initialized for subsequent tests.
        init_runtime().unwrap();
    }

    #[test]
    fn test_reinit_after_shutdown() {
        init_runtime().unwrap();
        shutdown();
        assert!(!has_runtime());
        init_runtime().unwrap();
        assert!(has_runtime());
        let v = block_on(async { 7u64 });
        assert_eq!(v, 7);
        shutdown();
        // Leave initialized.
        init_runtime().unwrap();
    }

    #[test]
    fn test_register_task_aborted_on_shutdown() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };
        init_runtime().unwrap();

        let ran = Arc::new(AtomicBool::new(false));
        let ran2 = ran.clone();
        let handle = spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            ran2.store(true, Ordering::SeqCst);
        });
        register_task(handle);
        shutdown();

        // The long-running task was aborted; the flag was never set.
        assert!(!ran.load(Ordering::SeqCst));
        assert!(!has_runtime());
        // Leave initialized.
        init_runtime().unwrap();
    }
}
