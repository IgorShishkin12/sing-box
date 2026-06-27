/*
 * Reticulum Bridge C API for sing-box
 *
 * All functions are thread-safe and may be called from any goroutine/thread.
 * Data flows via four Go callbacks registered at init; Rust never blocks waiting
 * for Go to consume data.
 */

#ifndef RETICULUM_BRIDGE_H
#define RETICULUM_BRIDGE_H

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Callback types (called from Rust tokio threads — keep implementations minimal) */
typedef void (*reticulum_on_accept_fn) (uint64_t listener_id, uint64_t conn_id, const char* peer_hash);
typedef void (*reticulum_on_connect_fn)(uint64_t task_id,     uint64_t conn_id);
typedef void (*reticulum_on_data_fn)   (uint64_t conn_id,     const uint8_t* data, size_t len);
typedef void (*reticulum_on_close_fn)  (uint64_t conn_id);
/*
 * Fires when reticulum_resolve_name completes. hash is NULL on timeout; when
 * non-NULL it is malloc'd — caller must free with reticulum_free.
 */
typedef void (*reticulum_on_resolve_fn)(uint64_t task_id, const char* hash);
/* Fires when reticulum_write completes. bytes is the number of bytes written, or -1 on error. */
typedef void (*reticulum_on_write_fn)(uint64_t task_id, int32_t bytes);

/* Log callback type: (level, target, message) */
typedef void (*reticulum_log_fn)(uint8_t, const char*, const char*);

/*
 * Register a Go log callback. Call before reticulum_init.
 */
void reticulum_set_log_callback(reticulum_log_fn on_log);

/*
 * Register the name-resolution callback.
 * Must be set before calling reticulum_resolve_name.
 */
void reticulum_set_resolve_callback(reticulum_on_resolve_fn on_resolve);

/*
 * Register the write-completion callback.
 * Must be set before calling reticulum_write.
 */
void reticulum_set_write_callback(reticulum_on_write_fn on_write);

/*
 * Initialize the bridge.
 * config_json  — JSON configuration string (must not be NULL; pass "{}" for defaults).
 * on_accept    — called from Rust when an inbound connection arrives.
 * on_connect   — called from Rust when an outbound dial completes (conn_id==0 on failure).
 * on_data      — called from Rust when data arrives on a connection.
 * on_close     — called from Rust when a connection is closed by the remote side.
 * Returns 0 on success, -1 on error.
 */
int reticulum_init(
    const char*              config_json,
    reticulum_on_accept_fn   on_accept,
    reticulum_on_connect_fn  on_connect,
    reticulum_on_data_fn     on_data,
    reticulum_on_close_fn    on_close
);

/*
 * Shutdown the bridge and release all resources.
 */
void reticulum_shutdown(void);

/*
 * Dial a destination hash. Non-blocking.
 * Fires on_connect(task_id, conn_id) when done; conn_id==0 means failure.
 */
void reticulum_dial(uint64_t task_id, const char* destination_hash);

/*
 * Listen on a hash. Blocks until the listener is registered.
 * Returns the listener handle (>0) on success, -1 on error.
 * Incoming connections are delivered via on_accept(listener_handle, conn_id, peer_hash).
 */
int64_t reticulum_listen(const char* listen_hash);

/*
 * Close a connection or listener handle.
 */
void reticulum_close(uint64_t handle);

/*
 * Write data to a connection. Non-blocking.
 * Fires on_write(task_id, bytes) when done; bytes is -1 on error.
 */
void reticulum_write(uint64_t task_id, uint64_t conn_handle, const uint8_t* data, size_t len);

/*
 * Get the destination hash of a listener as a hex string.
 * Caller must free with reticulum_free. Returns NULL if not found.
 */
char* reticulum_get_listener_hash(uint64_t listener_handle);

/*
 * Get the peer identity hash for a connection (from link.peer_identity()).
 * Caller must free with reticulum_free. Returns NULL if unavailable.
 */
char* reticulum_get_conn_peer_hash(uint64_t conn_handle);

/*
 * Get the verified persistent identity hash of the remote peer (from LinkIdentify exchange).
 * Caller must free with reticulum_free. Returns NULL if unavailable.
 */
char* reticulum_get_conn_identified_peer(uint64_t conn_handle);

/*
 * Get the local transport identity hash.
 * Caller must free with reticulum_free. Returns NULL if not initialized.
 */
char* reticulum_get_transport_hash(void);

/*
 * Register a name→hash mapping for later lookup via get_hash.
 * Returns 0 on success, -1 on error.
 */
int reticulum_register_name(const char* name, const char* hash);

/*
 * Get the destination hash for a registered name.
 * The caller must free *hash with reticulum_free.
 * Returns 0 on success, -1 if the name is unknown.
 */
int get_hash(char** hash, const char* name);

/*
 * Resolve a service name to its address hash via network announcements. Non-blocking.
 * Fires on_resolve(task_id, hash) when done; hash is NULL on timeout.
 * When non-NULL, hash is malloc'd — caller must free with reticulum_free.
 * Retries up to 3× with exponential backoff (3 s → 6 s → 12 s), 15 s per attempt.
 */
void reticulum_resolve_name(uint64_t task_id, const char* name);

/*
 * Get the maximum plaintext bytes that fit in a single data_packet for this connection.
 * Computed as link.packet_mdu() - FERNET_OVERHEAD_SIZE - FERNET_MAX_PADDING_SIZE.
 * Returns -1 if the connection is not a link or if the bridge is not initialized.
 */
int32_t reticulum_get_conn_max_payload(uint64_t conn_handle);

/*
 * Free memory allocated by the bridge.
 */
void reticulum_free(void* ptr);

#ifdef __cplusplus
}
#endif

#endif /* RETICULUM_BRIDGE_H */
