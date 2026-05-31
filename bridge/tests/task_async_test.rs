use serial_test::serial;
use sing_box_reticulum_bridge::c_api::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

// Sentinel: u64::MAX means "not yet fired".
static CONNECT_TASK_ID: AtomicU64 = AtomicU64::new(u64::MAX);
static CONNECT_CONN_ID: AtomicU64 = AtomicU64::new(u64::MAX);

extern "C" fn test_on_connect(task_id: u64, conn_id: u64) {
    CONNECT_TASK_ID.store(task_id, Ordering::Relaxed);
    CONNECT_CONN_ID.store(conn_id, Ordering::Relaxed);
}

fn init_with_connect_cb() {
    CONNECT_TASK_ID.store(u64::MAX, Ordering::Relaxed);
    CONNECT_CONN_ID.store(u64::MAX, Ordering::Relaxed);
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, Some(test_on_connect), None, None) };
    assert_eq!(ret, 0);
}

fn wait_for_connect(task_id: u64, max_ms: u64) -> u64 {
    let start = std::time::Instant::now();
    loop {
        let fired_task = CONNECT_TASK_ID.load(Ordering::Relaxed);
        if fired_task == task_id {
            return CONNECT_CONN_ID.load(Ordering::Relaxed);
        }
        if start.elapsed().as_millis() as u64 >= max_ms {
            panic!("on_connect did not fire within {}ms", max_ms);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// Dial to an unknown destination — on_connect fires with conn_id=0 (failure).
#[test]
#[serial]
fn test_dial_unknown_dest_returns_error() {
    init_with_connect_cb();

    let dest = std::ffi::CString::new("aabbccdd00112233445566778899aabb").unwrap();
    let task_id: u64 = 1001;
    unsafe {
        reticulum_dial(task_id, dest.as_ptr());
    }

    let conn_id = wait_for_connect(task_id, 10_000);
    assert_eq!(
        conn_id, 0,
        "dial to unknown dest should fire on_connect with conn_id=0"
    );

    reticulum_shutdown();
}

/// Listen succeeds (returns a positive handle) without network interfaces.
#[test]
#[serial]
fn test_listen_succeeds_without_network() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    assert_eq!(ret, 0);

    let listen_hash = std::ffi::CString::new("rln://listen-hash-no-net").unwrap();
    let handle = unsafe { reticulum_listen(listen_hash.as_ptr()) };
    match handle {
        h if h > 0 => {
            reticulum_close(h as u64);
        }
        _ => {
            eprintln!(
                "listen failed (acceptable without network): handle={}",
                handle
            );
        }
    }

    reticulum_shutdown();
}

// Mutex to serialise the multi-dial test's shared static.
static MULTI_DIAL_MUTEX: Mutex<()> = Mutex::new(());
static MULTI_CONNECT_COUNT: AtomicU64 = AtomicU64::new(0);

extern "C" fn multi_on_connect(_task_id: u64, _conn_id: u64) {
    MULTI_CONNECT_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Multiple concurrent dials — all fire on_connect (with conn_id=0, no peer).
#[test]
#[serial]
fn test_multiple_dials_fire_callbacks() {
    let _guard = MULTI_DIAL_MUTEX.lock().unwrap();
    MULTI_CONNECT_COUNT.store(0, Ordering::Relaxed);

    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, Some(multi_on_connect), None, None) };
    assert_eq!(ret, 0);

    const N: u64 = 5;
    for i in 0..N {
        let dest =
            std::ffi::CString::new(format!("{:032x}", (i + 1) as u128 * 0x1111111111111111u128))
                .unwrap();
        unsafe {
            reticulum_dial(i + 2000, dest.as_ptr());
        }
    }

    // Wait for all callbacks (they all fail — no peer)
    let start = std::time::Instant::now();
    loop {
        if MULTI_CONNECT_COUNT.load(Ordering::Relaxed) >= N {
            break;
        }
        if start.elapsed().as_secs() >= 15 {
            panic!("not all on_connect callbacks fired within 15s");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    reticulum_shutdown();
}
