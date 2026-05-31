/*
 * Reticulum Bridge C API for sing-box
 *
 * This header defines the C interface between Go (sing-box) and Rust.
 * All functions are thread-safe and may be called from any goroutine.
 */

#ifndef RETICULUM_BRIDGE_H
#define RETICULUM_BRIDGE_H

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Initialize the reticulum bridge with a JSON configuration string.
 * Returns 0 on success, -1 on error.
 */
int reticulum_init(const char* config_json);

/*
 * Get the destination hash for a given name.
 * The caller must free `*hash` with reticulum_free after use.
 * Returns 0 on success, -1 if the name is unknown.
 */
int get_hash(char** hash, const char* name);

/*
 * Register a name→hash mapping for later lookup via get_hash.
 * Returns 0 on success, -1 on error.
 */
int reticulum_register_name(const char* name, const char* hash);

/*
 * Shutdown the bridge and release all resources.
 */
void reticulum_shutdown(void);

/*
 * Dial a destination hash, returning a task ID.
 * Use reticulum_poll to check for completion and get the connection handle.
 * Returns -1 on error, otherwise a positive task ID.
 */
int32_t reticulum_dial(const char* destination_hash);

/*
 * Listen on a hash, returning a task ID.
 * Use reticulum_poll to check for completion and get the listener handle.
 * Returns -1 on error, otherwise a positive task ID.
 */
int32_t reticulum_listen(const char* listen_hash);

/*
 * Accept a pending connection from a listener.
 * Returns a task ID. Use reticulum_poll to get the new connection handle.
 * Returns -1 on error, otherwise a positive task ID.
 */
int32_t reticulum_accept(uint64_t listener_handle);

/*
 * Get the address hash of a listener as a hex string.
 * The caller must free the returned string with reticulum_free.
 * Returns NULL if the listener is not found or has no hash.
 */
char* reticulum_get_listener_hash(uint64_t listener_handle);

/*
 * Get the identity hash of the remote peer for a connection handle.
 * Returns a null-terminated hex string (caller must free with reticulum_free),
 * or NULL if the handle is not found or has no peer identity (e.g. outbound connections).
 */
char* reticulum_get_conn_peer_hash(uint64_t conn_handle);

/*
 * Close a connection or listener handle.
 */
void reticulum_close(uint64_t handle);

/*
 * Write data to a connection.
 * Returns number of bytes written, or -1 on error.
 */
int reticulum_write(uint64_t conn_handle, const uint8_t* data, size_t len);

/*
 * Read data from a connection.
 * Returns number of bytes read, or -1 on error.
 */
int reticulum_read(uint64_t conn_handle, uint8_t* buffer, size_t max_len);

/*
 * Poll for completion of a task.
 * Returns 0=pending, 1=done, -1=error.
 */
int reticulum_poll(int task_id, void** result_out, size_t* len_out);

/*
 * Free memory allocated by the bridge.
 */
void reticulum_free(void* ptr);

/*
 * Resolve a human-readable name to a deterministic address hash.
 * Both listener and dialer can call this independently to get the same
 * 32-char hex address hash from the same name, without any shared state.
 * The caller must free the returned string with reticulum_free.
 * Returns NULL if real-reticulum is not available or the name is empty.
 */
char* reticulum_resolve_name(const char* name);

/* Log callback type: (level, target, message) */
typedef void (*reticulum_log_fn)(uint8_t, const char*, const char*);

/* Register a Go log callback. Call before reticulum_init. */
void reticulum_set_log_callback(reticulum_log_fn on_log);

#ifdef __cplusplus
}
#endif

#endif /* RETICULUM_BRIDGE_H */

