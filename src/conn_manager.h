#ifndef CONN_MAN_H
#define CONN_MAN_H

#include <stddef.h>
#include <stdint.h>

#include "conn_catalog.h"
#include "db_backend.h"
#include "secret_store.h"

typedef struct ConnManager ConnManager;

/* Borrowed view over a ready-to-use connection managed by ConnManager.
 * - `db` and `profile` are owned by ConnManager.
 * - Both pointers are valid only while ConnManager is alive. */
typedef struct ConnView {
  DbBackend *db;
  const ConnProfile *profile;
} ConnView;

/**
 * Creates and returns a ConnManager.
 *
 * Ownership:
 * - `cat` is owned by ConnManager after creation.
 * - `secrets` is owned by ConnManager after creation.
 */
ConnManager *connm_create(ConnCatalog *cat, SecretStore *secrets);

/**
 * Creates a ConnManager using a custom backend factory (useful for tests).
 *
 * Ownership:
 * - `cat` is owned by ConnManager after creation.
 * - `secrets` is owned by ConnManager after creation.
 * - `factory` is borrowed and must remain valid for the lifetime of
 * ConnManager.
 *
 * Error semantics: returns NULL on invalid input or allocation failure.
 */
ConnManager *connm_create_with_factory(ConnCatalog *cat, SecretStore *secrets,
                                       DbBackend *(*factory)(DbKind kind));

/**
 * Destroy ConnManager and all allocated backends. Closes connections before
 * destroying.
 */
void connm_destroy(ConnManager *m);

/**
 * Get a ready-to-use connection view for `connection_name`.
 * - Performs lazy connect on first use.
 * - May reconnect if the previous connection is broken.
 * - May reap idle connections before acquiring.
 *
 * Ownership:
 * - `out->db` and `out->profile` are borrowed from ConnManager.
 * - caller must not free or mutate them.
 *
 * Error semantics:
 * - YES: found and connected, `out` populated with valid pointers.
 * - NO:  connection_name not found.
 * - ERR: internal error (e.g. cannot connect).
 */
int connm_get_connection(ConnManager *m, const char *connection_name,
                         ConnView *out);

/**
 * Marks a connection as "used now" (e.g., call this after exec completes,
 * regardless of success/failure).
 */
void connm_mark_used(ConnManager *m, const char *connection_name);

/**
 * Overrides the idle TTL (milliseconds) for reaping connections.
 *
 * Ownership: borrows 'm'.
 * Side effects: affects when connm_get_backend will disconnect idle sessions.
 * Error semantics: no return value.
 */
void connm_set_ttl_ms(ConnManager *m, uint64_t ttl_ms);

#endif
