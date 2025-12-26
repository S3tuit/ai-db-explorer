#include "session_manager.h"
#include "postgres_backend.h"
#include "safety_policy.h"
#include "utils.h"

#include <stdio.h>

static SafetyPolicy policy_default(void) {
    SafetyPolicy p = {0};
    p.read_only = 1;
    p.statement_timeout_ms = 5000;
    p.max_rows = 200;
    p.max_cell_bytes = 4096;
    return p;
}

static const char *test_conninfo(void) {
    return "host=localhost port=5432 dbname=postgres user=postgres password=postgres";
}

int main(void) {
    SafetyPolicy policy = policy_default();

    DbBackend *pg = postgres_backend_create();
    if (!pg) return 1;

    if (pg->vt->connect(pg, test_conninfo(), &policy) != OK) {
        fprintf(stderr, "Can't connect to: %s", test_conninfo());
        pg->vt->destroy(pg);
        return 1;
    }

    SessionManager sm;
    if (session_init(&sm, stdin, stdout, pg) != OK) {
        pg->vt->destroy(pg);
        return 1;
    }

    int rc = session_run(&sm);

    session_clean(&sm);
    pg->vt->destroy(pg);

    return (rc == OK) ? 0 : 1;
}

