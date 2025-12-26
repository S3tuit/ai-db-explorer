#include "test.h"
#include "session_manager.h"
#include "postgres_backend.h"
#include "safety_policy.h"
#include "utils.h"

#include <stdio.h>

/* ------------------------------ Real Backend ----------------------------- */

static DbBackend *db_get_pg() {
    DbBackend *pg = postgres_backend_create();
    ASSERT_TRUE(pg != NULL);

    SafetyPolicy policy;
    safety_policy_init(&policy, NULL, NULL, NULL, NULL);
    pg->vt->connect(pg, "dbname=postgres", &policy);

    return pg;
}

/* ------------------------ Fake catastrophic backend ---------------------- */

typedef struct FakeImpl { int unused; } FakeImpl;

static int fake_connect(DbBackend *db, const char *conninfo, const SafetyPolicy *policy) {
    (void)conninfo; (void)policy;
    return (db && db->impl) ? OK : ERR;
}

static void fake_destroy(DbBackend *db) {
    if (!db) return;
    free(db->impl);
    free(db);
}

static int fake_exec_catastrophic(DbBackend *db, uint32_t request_id, const char *sql, QueryResult **out_qr) {
    (void)db; (void)request_id; (void)sql;
    if (!out_qr) return ERR;
    *out_qr = NULL;
    return ERR;
}

static DbBackend *db_get_fake_catastrophic(void) {
    DbBackend *db = xcalloc(1, sizeof(DbBackend));

    FakeImpl *impl = xcalloc(1, sizeof(FakeImpl));

    static const DbBackendVTable VT = {
        .connect = fake_connect,
        .destroy = fake_destroy,
        .exec = fake_exec_catastrophic
    };

    db->vt = &VT;
    db->impl = impl;

    SafetyPolicy policy;
    safety_policy_init(&policy, NULL, NULL, NULL, NULL);
    ASSERT_TRUE(db->vt->connect(db, "", &policy) == OK);

    return db;
}

/* ----------------------------------- tests ------------------------------- */

void test_basic_input_to_out() {
    DbBackend *pg = db_get_pg();

    FILE *in = MEMFILE_IN("SELECT 1;");
    FILE *out = MEMFILE_OUT();

    SessionManager sm;
    ASSERT_TRUE(session_init(&sm, in, out, pg) == OK);
    ASSERT_TRUE(session_run(&sm) == OK);

    ASSERT_TRUE(session_last_error(&sm) != NULL);
    ASSERT_TRUE(session_last_error(&sm)[0] == '\0');
    
    fflush(out);
    fseek(out, 0, SEEK_SET);

    char *out_all = read_all(out);

    ASSERT_TRUE(strstr(out_all, "Content-Length:") != NULL);
    ASSERT_TRUE(strstr(out_all, "\"id\":1") != NULL);

    free(out_all);
    session_clean(&sm);
    fclose(in);
    fclose(out);
    pg->vt->destroy(pg);
}

void test_session_manager_writes_even_on_catastrophic_backend(void) {
    DbBackend *fake = db_get_fake_catastrophic();

    FILE *in = MEMFILE_IN("SELECT 1;");
    FILE *out = MEMFILE_OUT();

    SessionManager sm;
    ASSERT_TRUE(session_init(&sm, in, out, fake) == OK);

    // session manager still won't fail
    ASSERT_TRUE(session_run(&sm) == OK);

    fflush(out);
    fseek(out, 0, SEEK_SET);

    char *out_all = read_all(out);
    ASSERT_TRUE(out_all != NULL);

    ASSERT_TRUE(strstr(out_all, "Content-Length:") != NULL);
    ASSERT_TRUE(strstr(out_all, "\"id\":1") != NULL);
    ASSERT_TRUE(strstr(out_all, "\"error\":") != NULL);

    // still no internal errors
    ASSERT_TRUE(session_last_error(&sm)[0] == '\0');

    free(out_all);
    session_clean(&sm);
    fclose(in);
    fclose(out);
    fake->vt->destroy(fake);
}

void test_bad_input() {
    FILE *in = MEMFILE_IN("SELECT 1;");
    FILE *out = MEMFILE_OUT();

    SessionManager sm;
    ASSERT_TRUE(session_init(&sm, in, out, NULL) == ERR);

    DbBackend *fake = db_get_fake_catastrophic();
    ASSERT_TRUE(session_init(&sm, in, NULL, fake) == ERR);

    fclose(in);
    fclose(out);
    session_clean(&sm);
    fake->vt->destroy(fake);
}

void test_internal_failure() {
    FILE *in = MEMFILE_IN("SELECT 1;");
    FILE *out = MEMFILE_OUT();
    DbBackend *fake = db_get_fake_catastrophic();

    SessionManager sm;
    ASSERT_TRUE(session_init(&sm, in, out, fake) == OK);
    
    // transport writer should give error
    fclose(out);
    ASSERT_TRUE(session_run(&sm) == ERR);
    ASSERT_TRUE(session_last_error(&sm)[0] != '\0');

    fclose(in);
    session_clean(&sm);
    fake->vt->destroy(fake);
}

int main (void) {
    test_basic_input_to_out();
    test_session_manager_writes_even_on_catastrophic_backend();
    test_bad_input();

    fprintf(stderr, "OK: test_session_manager\n");
    return(0);
}
