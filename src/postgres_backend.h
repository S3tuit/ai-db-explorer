#ifndef POSTGRES_BACKEND_H
#define POSTGRES_BACKEND_H

#include "db_backend.h"

/* Creates a Postgres DbBackend (caller must free with db->vt->close + free).
 * Will not return NULL. */
DbBackend *postgres_backend_create(void);

#endif

