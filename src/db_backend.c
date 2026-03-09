#include "db_backend.h"
#include "postgres_backend.h"

DbBackend *db_backend_create(DbKind kind) {
  switch (kind) {
  case DB_KIND_POSTGRES:
    return postgres_backend_create();
  default:
    return NULL;
  }
}
