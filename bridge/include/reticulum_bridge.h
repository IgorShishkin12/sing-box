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
 */
void get_hash(char** hash, const char* name);

/*
 * Shutdown the bridge and release all resources.
 */
void reticulum_shutdown(void);

/*
 * Dial a destination hash, returning a connection handle.
 * Returns 0 on error, otherwise a positive handle.
 */
uint64_t reticulum_dial(const char* destination_hash);

/*
 * Listen on a hash, returning a listener handle.
 * Returns 0 on error, otherwise a positive handle.
 */
uint64_t reticulum_listen(const char* listen_hash);

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

#ifdef __cplusplus
}
#endif

#endif /* RETICULUM_BRIDGE_H */
