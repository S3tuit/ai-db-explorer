#ifndef CONN_MAN_H
#define CONN_MAN_H

#include <stdint.h>
#include <stddef.h>

#include "conn_catalog.h"
#include "secret_store.h"
#include "db_backend.h"


typedef struct ConnManager ConnManager;

/**
 * Creates and returns a ConnManager.
 *
 * Ownership:
 * - `cat` is borrowed and must outlive ConnManager.
 * - `secrets` is borrowed and must outlive ConnManager.
 * - the policy is borrowed from the catalog and must outlive ConnManager (v1).
 */
ConnManager *connm_create(ConnCatalog *cat, SecretStore *secrets);

/**
 * Destroy ConnManager and all allocated backends. Closes connections before
 * destroying.
 */
void connm_destroy(ConnManager *m);

/**
 * Get a ready-to-use backend for `connection_name`, or NULL if there is no
 * backend for that 'connection_name' or if error.
 * - Performs lazy connect on first use.
 * - May reconnect if the previous connection is broken.
 * - May reap idle connections before acquiring.
 *
 * The returned DbBackend* is owned by ConnManager and remains valid until:
 * - connm_get_backend or connm_destroy is called.
 */
DbBackend *connm_get_backend(ConnManager *m, const char *connection_name);

/**
 * Marks a connection as "used now" (e.g., call this after exec completes,
 * regardless of success/failure).
 */
void connm_mark_used(ConnManager *m, const char *connection_name);

#endif
