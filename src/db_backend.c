#include "db_backend.h"
#include "postgres_backend.h"

#ifdef ADBX_TEST_MODE
static DbBackendFactory g_db_test_factory = NULL;

void db_backend_set_test_factory(DbBackendFactory factory) {
  g_db_test_factory = factory;
}
#endif

DbBackend *db_backend_create(DbKind kind) {
#ifdef ADBX_TEST_MODE
  if (g_db_test_factory)
    return g_db_test_factory(kind);
#endif

  switch (kind) {
  case DB_KIND_POSTGRES:
    return postgres_backend_create();
  default:
    return NULL;
  }
}
