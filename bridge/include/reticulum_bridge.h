/*
 * Reticulum Bridge C API for sing-box
 *
 * Callback-based, non-blocking API. Rust fires the four callbacks when events
 * occur; Go registers exported functions as those callbacks at init time.
 *
 * Thread-safety: all functions are safe to call from any goroutine / C thread.
 */

#ifndef RETICULUM_BRIDGE_H
#define RETICULUM_BRIDGE_H

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * Callback types (Rust → Go)
 * ------------------------------------------------------------------------- */

/* New inbound connection on a listener. peer_hash is a hex-encoded Reticulum
 * identity address hash; the string is valid only for the duration of the call. */
typedef void (*reticulum_on_accept_fn)(uint64_t listener_id, uint64_t conn_id,
                                       char *peer_hash);

/* Outbound dial completed. conn_id == 0 means failure. */
typedef void (*reticulum_on_connect_fn)(uint64_t task_id, uint64_t conn_id);

/* Data arrived on a connection. data/len are valid only for the duration of
 * the call — the callback must copy the bytes if it needs them afterwards. */
typedef void (*reticulum_on_data_fn)(uint64_t conn_id, uint8_t *data,
                                     size_t len);

/* Connection has been closed (by either side). */
typedef void (*reticulum_on_close_fn)(uint64_t conn_id);

/* Log event from Rust. level: 1=Error 2=Warn 3=Info 4=Debug 5=Trace.
 * target is the tracing/log crate target (e.g. "reticulum_rs::transport").
 * Both strings are valid only for the duration of the call. */
typedef void (*reticulum_log_fn)(uint8_t level, const char *target,
                                 const char *message);

/* -------------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------------- */

/*
 * Initialize the bridge. Must be called before any other function.
 * config_json: JSON configuration string (at least "{}").
 * The four callbacks are called from tokio worker threads; they must not block.
 * Returns 0 on success, -1 on error.
 */
int reticulum_init(const char *config_json,
                   reticulum_on_accept_fn  on_accept,
                   reticulum_on_connect_fn on_connect,
                   reticulum_on_data_fn    on_data,
                   reticulum_on_close_fn   on_close);

/*
 * Register the Go log callback. Call before reticulum_init to capture
 * early initialisation events. Safe to call from any thread.
 */
void reticulum_set_log_callback(reticulum_log_fn on_log);

/*
 * Shut down the bridge and release all resources.
 */
void reticulum_shutdown(void);

/* -------------------------------------------------------------------------
 * Server side
 * ------------------------------------------------------------------------- */

/*
 * Register a named service listener. Blocks until the listener is announced
 * on the network (up to ~30 s). Returns the listener_id (> 0) on success,
 * or -1 on error. on_accept is called for each incoming connection.
 */
int64_t reticulum_listen(const char *name);

/* -------------------------------------------------------------------------
 * Client side
 * ------------------------------------------------------------------------- */

/*
 * Initiate a connection to dest_hash. Non-blocking: returns immediately.
 * on_connect is called with task_id and the new conn_id when the link is
 * established, or with conn_id == 0 on failure.
 */
void reticulum_dial(uint64_t task_id, const char *dest_hash);

/*
 * Resolve a human-readable service name to its Reticulum address hash.
 * Blocks up to ~30 s (3 retries). Returns a malloc'd hex string on success,
 * or NULL on timeout. Caller must free with reticulum_free.
 */
char *reticulum_resolve_name(const char *name);

/* -------------------------------------------------------------------------
 * Data transfer
 * ------------------------------------------------------------------------- */

/*
 * Write data to a connection. Returns bytes written, or -1 on error.
 */
int reticulum_write(uint64_t conn_id, const uint8_t *data, size_t len); /* data not modified */

/*
 * Close a connection or listener handle. Triggers on_close for connections.
 */
void reticulum_close(uint64_t handle);

/* -------------------------------------------------------------------------
 * Memory
 * ------------------------------------------------------------------------- */

/*
 * Free a string returned by reticulum_resolve_name.
 */
void reticulum_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif /* RETICULUM_BRIDGE_H */
