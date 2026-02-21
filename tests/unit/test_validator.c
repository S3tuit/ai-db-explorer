#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "conn_catalog.h"
#include "postgres_backend.h"
#include "test.h"
#include "validator.h"

/* Runs a single validation and checks for accept/reject + error code.
 * Side effects: initializes and cleans one request-scoped validator output. */
static void assert_validate_at(DbBackend *db, const ConnProfile *cp,
                               const char *sql, int expect_ok,
                               ValidatorErrCode expect_code,
                               const char *expect_substr, const char *file,
                               int line) {
  ValidateQueryOut out = {0};
  ASSERT_TRUE(vq_out_init(&out) == OK);
  ValidatorRequest req = {
      .db = db,
      .profile = cp,
      .sql = sql,
  };

  int rc = validate_query(&req, &out);
  if (expect_ok) {
    if (rc != OK) {
      fprintf(stderr, "validate_query failed: code=%d msg=", (int)out.err.code);
      if (out.err.msg.data && out.err.msg.len > 0) {
        fprintf(stderr, "%.*s", (int)out.err.msg.len, out.err.msg.data);
      } else {
        fprintf(stderr, "(null)");
      }
      fprintf(stderr, "\n");
    }
    ASSERT_TRUE_AT(rc == OK, file, line);
    ASSERT_TRUE_AT(out.err.code == VERR_NONE, file, line);
  } else {
    if (rc != ERR) {
      fprintf(stderr, "validate_query unexpectedly OK\n");
    } else if (out.err.code != expect_code) {
      fprintf(stderr, "validate_query wrong code: got=%d expected=%d msg=",
              (int)out.err.code, (int)expect_code);
      if (out.err.msg.data && out.err.msg.len > 0) {
        fprintf(stderr, "%.*s", (int)out.err.msg.len, out.err.msg.data);
      } else {
        fprintf(stderr, "(null)");
      }
      fprintf(stderr, "\n");
    }
    ASSERT_TRUE_AT(rc == ERR, file, line);
    ASSERT_TRUE_AT(out.err.code == expect_code, file, line);
    if (expect_substr) {
      ASSERT_TRUE_AT(out.err.msg.data != NULL, file, line);
      size_t hay_len = out.err.msg.len;
      size_t needle_len = strlen(expect_substr);
      int found = 0;
      if (needle_len == 0) {
        found = 1;
      } else if (hay_len >= needle_len) {
        for (size_t i = 0; i + needle_len <= hay_len; i++) {
          if (memcmp(out.err.msg.data + i, expect_substr, needle_len) == 0) {
            found = 1;
            break;
          }
        }
      }
      ASSERT_TRUE_AT(found, file, line);
    }
  }

  vq_out_clean(&out);
}
#define ASSERT_VALIDATE(db, cp, policy, sql, ok, code)                         \
  assert_validate_at((db), (cp), (sql), (ok), (code), NULL, __FILE__, __LINE__)
#define ASSERT_VALIDATE_MSG(db, cp, policy, sql, ok, code, substr)             \
  assert_validate_at((db), (cp), (sql), (ok), (code), (substr), __FILE__,      \
                     __LINE__)

/* Runs one validation expected to succeed and checks full ValidatorPlan shape.
 * It borrows all inputs and owns temporary ValidateQueryOut for this call.
 */
static void assert_validate_plan_at(DbBackend *db, const ConnProfile *cp,
                                    const char *sql,
                                    const ValidatorColOutKind *exp_kinds,
                                    const char *const *exp_col_ids,
                                    uint32_t exp_ncols, const char *file,
                                    int line) {
  ValidateQueryOut out = {0};
  ASSERT_TRUE_AT(vq_out_init(&out) == OK, file, line);

  ValidatorRequest req = {
      .db = db,
      .profile = cp,
      .sql = sql,
  };

  int rc = validate_query(&req, &out);
  if (rc != OK) {
    fprintf(stderr, "validate_query failed: code=%d msg=", (int)out.err.code);
    if (out.err.msg.data && out.err.msg.len > 0) {
      fprintf(stderr, "%.*s", (int)out.err.msg.len, out.err.msg.data);
    } else {
      fprintf(stderr, "(null)");
    }
    fprintf(stderr, "\n");
  }
  ASSERT_TRUE_AT(rc == OK, file, line);
  ASSERT_TRUE_AT(out.err.code == VERR_NONE, file, line);
  ASSERT_TRUE_AT(out.plan.cols != NULL, file, line);
  ASSERT_TRUE_AT(parr_len(out.plan.cols) == exp_ncols, file, line);

  for (uint32_t i = 0; i < exp_ncols; i++) {
    const ValidatorColPlan *col =
        (const ValidatorColPlan *)parr_cat(out.plan.cols, i);
    ASSERT_TRUE_AT(col != NULL, file, line);
    ASSERT_TRUE_AT(col->kind == exp_kinds[i], file, line);

    if (exp_kinds[i] == VCOL_OUT_PLAINTEXT) {
      ASSERT_TRUE_AT(col->col_id == NULL, file, line);
      ASSERT_TRUE_AT(col->col_id_len == 0, file, line);
      continue;
    }

    ASSERT_TRUE_AT(col->col_id != NULL, file, line);
    ASSERT_TRUE_AT(col->col_id_len > 0, file, line);
    ASSERT_TRUE_AT(strlen(col->col_id) == col->col_id_len, file, line);
    if (exp_col_ids && exp_col_ids[i]) {
      ASSERT_TRUE_AT(strcmp(col->col_id, exp_col_ids[i]) == 0, file, line);
    }
  }

  vq_out_clean(&out);
}
#define ASSERT_VALIDATE_PLAN(db, cp, sql, exp_kinds, exp_ids, exp_ncols)       \
  assert_validate_plan_at((db), (cp), (sql), (exp_kinds), (exp_ids),           \
                          (exp_ncols), __FILE__, __LINE__)

/* Runs one validation expected to fail and asserts that the output plan is
 * empty after failure.
 * It borrows all inputs and owns temporary ValidateQueryOut for this call.
 * Side effects: executes validate_query() and allocates/frees per-call output.
 * Error semantics: test helper (asserts on mismatch and does not return ERR).
 */
static void assert_validate_reject_empty_plan_at(DbBackend *db,
                                                 const ConnProfile *cp,
                                                 const char *sql,
                                                 ValidatorErrCode expect_code,
                                                 const char *file, int line) {
  ValidateQueryOut out = {0};
  ASSERT_TRUE_AT(vq_out_init(&out) == OK, file, line);

  ValidatorRequest req = {
      .db = db,
      .profile = cp,
      .sql = sql,
  };

  int rc = validate_query(&req, &out);
  ASSERT_TRUE_AT(rc == ERR, file, line);
  ASSERT_TRUE_AT(out.err.code == expect_code, file, line);
  ASSERT_TRUE_AT(out.plan.cols != NULL, file, line);
  ASSERT_TRUE_AT(parr_len(out.plan.cols) == 0, file, line);

  vq_out_clean(&out);
}
#define ASSERT_VALIDATE_REJECT_EMPTY_PLAN(db, cp, sql, code)                   \
  assert_validate_reject_empty_plan_at((db), (cp), (sql), (code), __FILE__,    \
                                       __LINE__)

/* Basic validation cases for the policy rules and error codes. */
static void test_validator_accepts(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);

  ConnProfile *cp = NULL;
  ASSERT_TRUE(catalog_list(cat, &cp, 1) == 1);
  ASSERT_TRUE(cp != NULL);

  SafetyPolicy *policy = &cp->safe_policy;
  ASSERT_TRUE(policy != NULL);

  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);

  ASSERT_VALIDATE(db, cp, policy, "SELECT u.name FROM users u WHERE u.id = 1;",
                  1, VERR_NONE);

  db_destroy(db);
  catalog_destroy(cat);
}

static void test_validator_rejects_rules(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);

  ConnProfile *cp = NULL;
  ASSERT_TRUE(catalog_list(cat, &cp, 1) == 1);
  ASSERT_TRUE(cp != NULL);

  SafetyPolicy *policy = &cp->safe_policy;
  ASSERT_TRUE(policy != NULL);

  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);

  ASSERT_VALIDATE(db, cp, policy, "SELECT * FROM users u WHERE u.id = 1;", 0,
                  VERR_STAR);

  ASSERT_VALIDATE_MSG(db, cp, policy,
                      "SELECT u.name FROM users u JOIN balance ON u.id = "
                      "u.user_id WHERE u.id = 1;",
                      0, VERR_NO_TABLE_ALIAS, "Missing alias in JOIN item");

  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.status = $1;", 0,
                  VERR_PARAM_NON_SENSITIVE);

  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u LEFT JOIN expenses e ON "
                  "e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 10;",
                  0, VERR_JOIN_NOT_INNER);

  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.receiver = u.fiscal_code "
                  "WHERE u.id = 1 LIMIT 10;",
                  0, VERR_JOIN_ON_SENSITIVE);

  ASSERT_VALIDATE_MSG(
      db, cp, policy,
      "WITH tab1 AS (SELECT u.fiscal_code FROM users u WHERE u.id = 1) "
      "SELECT t.fiscal_code FROM tab1 t LIMIT 10;",
      0, VERR_SENSITIVE_OUTSIDE_MAIN, "fiscal_code");

  db_destroy(db);
  catalog_destroy(cat);
}

/* Runs the full set of validator_tests cases. */
static void test_validator_from_notes(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);
  ConnProfile *cp = NULL;
  ASSERT_TRUE(catalog_list(cat, &cp, 1) == 1);
  ASSERT_TRUE(cp != NULL);
  SafetyPolicy *policy = &cp->safe_policy;
  ASSERT_TRUE(policy != NULL);
  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);

  /* ACCEPT cases */
  ASSERT_VALIDATE(db, cp, policy, "SELECT u.name FROM users u WHERE u.id = 1;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.country, COUNT(*) FROM users u INNER JOIN expenses "
                  "e ON e.user_id = u.id "
                  "WHERE u.status = true GROUP BY u.country;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT c.id, c.type FROM cards c WHERE c.type = (SELECT "
                  "ct.code FROM card_types ct WHERE ct.id = 1) "
                  "ORDER BY c.id LIMIT 50;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT LOWER(u.email) AS email_lc FROM users u WHERE u.id = 1;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.fiscal_code, u.name FROM users u WHERE u.id = 1 LIMIT 10;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code = $1 LIMIT 200;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code IN ($1, $2, $3) LIMIT 200;",
      1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT e.category, COUNT(*) FROM users u INNER JOIN "
                  "expenses e ON e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 GROUP BY e.category LIMIT 200;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT DISTINCT u.status FROM users u WHERE u.status = true;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.status = false LIMIT 10 OFFSET 10;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT transfer_amount(u.id, (SELECT u2.id FROM users u2 "
                  "WHERE u2.amount = 123), 50) AS bal "
                  "FROM users u WHERE u.id = 1;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT users.calc_balance(u.id) AS bal FROM users u WHERE u.id = 1;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT LOWER(c.balance) AS lb FROM public.cards c LIMIT 10;",
                  1, VERR_NONE);

  /* REJECT cases */
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.status, COUNT(*) FROM users u WHERE u.fiscal_code = $1 "
      "GROUP BY u.status HAVING COUNT(*) > $2 LIMIT 200;",
      0, VERR_PARAM_OUTSIDE_WHERE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT LOWER(c.balance) AS lb FROM private.cards c LIMIT 10;", 0,
      VERR_SENSITIVE_SELECT_EXPR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, u.name FROM users u WHERE u.status = $1 AND "
                  "u.fiscal_code = $2 LIMIT 200;",
                  0, VERR_PARAM_NON_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy, "SELECT * FROM users u WHERE u.id = 1;", 0,
                  VERR_STAR);
  ASSERT_VALIDATE(db, cp, policy, "SELECT u.* FROM users u WHERE u.id = 101;",
                  0, VERR_STAR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.name, e.* FROM users u INNER JOIN expenses e ON "
                  "e.user_id = u.id WHERE u.id = $1;",
                  0, VERR_STAR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.name FROM users u CROSS JOIN cards WHERE u.id = 1;",
                  0, VERR_NO_TABLE_ALIAS);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.name, e.amount FROM users u INNER JOIN expenses ON "
                  "expenses.user_id = u.id WHERE u.id = 1;",
                  0, VERR_ANALYZE_FAIL);
  ASSERT_VALIDATE_MSG(db, cp, policy,
                      "SELECT LOWER2(u.fiscal_code) FROM users u LIMIT 199;", 0,
                      VERR_FUNC_UNSAFE, "lower2");
  ASSERT_VALIDATE_MSG(db, cp, policy,
                      "SELECT u.id FROM users u WHERE u.fiscal_code = "
                      "'ABCDEF12G34H567I' LIMIT 200;",
                      0, VERR_SENSITIVE_CMP, "u.fiscal_code");
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code IN ('A', 'B') LIMIT 200;",
      0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code = $1 OR "
                  "u.status = false LIMIT 200;",
                  0, VERR_WHERE_NOT_CONJ);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE NOT (u.fiscal_code = $1) LIMIT 200;", 0,
      VERR_WHERE_NOT_CONJ);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code <> $1 LIMIT 200;", 0,
      VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code LIKE $1 LIMIT 200;", 0,
      VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code BETWEEN $1 AND "
                  "$2 LIMIT 200;",
                  0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code = (SELECT 'A' "
                  "FROM users x WHERE x.id = 1) "
                  "LIMIT 200;",
                  0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.receiver = u.fiscal_code "
                  "WHERE u.id = 1 LIMIT 200;",
                  0, VERR_JOIN_ON_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u LEFT JOIN expenses e ON "
                  "e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 10;",
                  0, VERR_JOIN_NOT_INNER);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u CROSS JOIN expenses e "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  0, VERR_JOIN_NOT_INNER);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.user_id = u.id OR e.user_id = 1 "
                  "WHERE u.fiscal_code = $2 LIMIT 200;",
                  0, VERR_JOIN_ON_INVALID);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.user_id > u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  0, VERR_JOIN_ON_INVALID);
  // TODO: we might relax this; allow subqueries and functions inside JOIN ON
  // if both side are non-sensitive, func are safe and JOIN ON still has only
  // = and AND.
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "LOWER(e.category) = u.status "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  0, VERR_JOIN_ON_INVALID);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code, COUNT(*) FROM users u WHERE u.status "
                  "= true GROUP BY u.fiscal_code LIMIT 200;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code = $1 ORDER BY "
                  "u.fiscal_code LIMIT 200;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.status, COUNT(*) FROM users u WHERE u.status = "
                  "false GROUP BY u.status "
                  "HAVING MAX(u.fiscal_code) IS NOT NULL LIMIT 200;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT DISTINCT u.status FROM users u WHERE u.fiscal_code = $1;", 0,
      VERR_DISTINCT_SENSITIVE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code = $1 LIMIT 10 OFFSET 10;",
      0, VERR_OFFSET_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy,
                  "WITH tab1 AS (SELECT u.fiscal_code FROM users u WHERE u.id "
                  "= 2) SELECT t.fiscal_code FROM tab1 t LIMIT 10;",
                  0, VERR_SENSITIVE_OUTSIDE_MAIN);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.id = (SELECT x.id FROM "
                  "users x WHERE x.fiscal_code = $1) LIMIT 10;",
                  0, VERR_SENSITIVE_OUTSIDE_MAIN);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT t.id FROM (SELECT u.id FROM users u WHERE "
                  "u.fiscal_code = $1) t LIMIT 10;",
                  0, VERR_SENSITIVE_OUTSIDE_MAIN);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT public.u FROM users u WHERE u.id = 1;", 0,
                  VERR_NO_COLUMN_ALIAS);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.\"name\" FROM \"users\" u WHERE u.\"fiscaL_code\" "
                  "= 'A' LIMIT 10;",
                  0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy, "SELECT u.fiscal_code FROM users u;", 0,
                  VERR_LIMIT_REQUIRED);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code IN ($1, 'X') LIMIT 10;", 0,
      VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy, "SELECT $1 AS trick FROM users u LIMIT 1;", 0,
                  VERR_PARAM_OUTSIDE_WHERE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code FROM users u LIMIT 201;", 0,
                  VERR_LIMIT_EXCEEDS);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT calc_balance(u.id) AS bal FROM users u WHERE u.id = 1;", 0,
      VERR_FUNC_UNSAFE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code AS fc FROM users u WHERE u.fiscal_code "
                  "= $1 ORDER BY fc LIMIT 10;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code AS fc, COUNT(*) FROM users u WHERE "
                  "u.status = true GROUP BY fc LIMIT 10;",
                  0, VERR_NO_COLUMN_ALIAS);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.status = $1 LIMIT 10;", 0,
                  VERR_PARAM_NON_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.id = (SELECT x.id FROM "
                  "users x WHERE x.id = $1) LIMIT 10;",
                  0, VERR_PARAM_NON_SENSITIVE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.status, MAX(u.fiscal_code) AS m FROM users u GROUP BY u.status "
      "HAVING m IS NOT NULL LIMIT 10;",
      0, VERR_NO_COLUMN_ALIAS);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT row_number() OVER (ORDER BY u.fiscal_code) AS rn FROM users u "
      "WHERE u.fiscal_code = $1 LIMIT 10;",
      0, VERR_SENSITIVE_SELECT_EXPR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.receiver = $1 "
                  "WHERE u.fiscal_code = $2 LIMIT 10;",
                  0, VERR_PARAM_OUTSIDE_WHERE);

  db_destroy(db);
  catalog_destroy(cat);
}

/* Verifies ValidatorPlan output-column mapping for plaintext/token columns,
 * canonical token identifiers, reset semantics, and failure-plan invariants.
 */
static void test_validator_plan(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);

  ConnProfile *cp = NULL;
  ASSERT_TRUE(catalog_list(cat, &cp, 1) == 1);
  ASSERT_TRUE(cp != NULL);

  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);

  // all plaintext
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
    };
    const char *ids[] = {NULL, NULL, NULL, NULL};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT u.name, u.age, u.eye_color, u.hair_color FROM users u WHERE "
        "u.id = 1;",
        kinds, ids, 4);
  }

  // all plaintext (aggregates with mixed casing)
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
    };
    const char *ids[] = {NULL, NULL, NULL, NULL};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT u.country, COUNT(*), count(u.name), CounT(*) FROM users u "
        "INNER "
        "JOIN expenses e ON e.user_id = u.id WHERE u.status = true GROUP BY "
        "u.country;",
        kinds, ids, 4);
  }

  // one tokenized output (unqualified table)
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_TOKEN,
        VCOL_OUT_PLAINTEXT,
    };
    const char *ids[] = {NULL, "users.fiscal_code", NULL};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT u.eye_color, u.fiscal_code, u.name FROM users u WHERE u.id = "
        "1 LIMIT 10;",
        kinds, ids, 3);
  }

  // one tokenized output (schema-qualified base relation)
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_TOKEN,
    };
    const char *ids[] = {NULL, "private.users.fiscal_code"};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT u.eye_color, u.fiscal_code FROM private.users u WHERE u.id = "
        "1 LIMIT 10;",
        kinds, ids, 2);
  }

  // tokenized output from join rhs (private schema)
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_TOKEN,
    };
    const char *ids[] = {NULL, NULL, NULL, "private.users.fiscal_code"};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT e.amount, e.date, u.name, u.fiscal_COde FROM expenses e INNER "
        "JOIN private.users u ON e.user_id = u.id LIMIT 10;",
        kinds, ids, 4);
  }

  // tokenized output from secondary join alias (users)
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_TOKEN,
    };
    const char *ids[] = {NULL, NULL, NULL, "users.fiscal_code"};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT e.amount, e.date, u2.name, u1.fiscal_COde FROM expenses e "
        "INNER JOIN private.users u2 ON e.user_id = u2.id INNER JOIN users u1 "
        "ON u1.id = u2.id LIMIT 10;",
        kinds, ids, 4);
  }

  // duplicate sensitive column appears twice in output plan
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_TOKEN,
        VCOL_OUT_TOKEN,
    };
    const char *ids[] = {"users.fiscal_code", "users.fiscal_code"};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT u.fiscal_code, u.fiscal_code FROM users u WHERE u.id = 1 "
        "LIMIT 10;",
        kinds, ids, 2);
  }

  // output alias should not alter canonical source identifier
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_TOKEN,
        VCOL_OUT_PLAINTEXT,
    };
    const char *ids[] = {"users.fiscal_code", NULL};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT u.fiscal_code AS fc, u.name FROM users u WHERE u.id = 1 LIMIT "
        "10;",
        kinds, ids, 2);
  }

  // sensitive outputs from two different configured sources
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_TOKEN,
        VCOL_OUT_TOKEN,
    };
    const char *ids[] = {"users.fiscal_code", "expenses.receiver"};
    ASSERT_VALIDATE_PLAN(
        db, cp,
        "SELECT u.fiscal_code, e.receiver FROM users u INNER JOIN expenses e "
        "ON e.user_id = u.id LIMIT 10;",
        kinds, ids, 2);
  }

  // non-colref SELECT expressions are always plaintext in the plan
  {
    const ValidatorColOutKind kinds[] = {
        VCOL_OUT_PLAINTEXT,
        VCOL_OUT_PLAINTEXT,
    };
    const char *ids[] = {NULL, NULL};
    ASSERT_VALIDATE_PLAN(
        db, cp, "SELECT LOWER(u.name), u.name FROM users u WHERE u.id = 1;",
        kinds, ids, 2);
  }

  // sensitive mode at LIMIT boundary keeps plaintext output mapping
  {
    const ValidatorColOutKind kinds[] = {VCOL_OUT_PLAINTEXT};
    const char *ids[] = {NULL};
    ASSERT_VALIDATE_PLAN(
        db, cp, "SELECT u.id FROM users u WHERE u.fiscal_code = $1 LIMIT 200;",
        kinds, ids, 1);
  }

  // failure paths must not leave partial plan state
  ASSERT_VALIDATE_REJECT_EMPTY_PLAN(
      db, cp, "SELECT u.fiscal_code FROM users u;", VERR_LIMIT_REQUIRED);
  ASSERT_VALIDATE_REJECT_EMPTY_PLAN(
      db, cp, "SELECT u.id FROM users u WHERE u.fiscal_code = $1 LIMIT 201;",
      VERR_LIMIT_EXCEEDS);

  // reuse the same ValidateQueryOut to ensure reset removes old token plan.
  ValidateQueryOut out = {0};
  ASSERT_TRUE(vq_out_init(&out) == OK);
  ValidatorRequest req_token = {
      .db = db,
      .profile = cp,
      .sql = "SELECT u.fiscal_code FROM users u WHERE u.id = 1 LIMIT 10;",
  };
  ASSERT_TRUE(validate_query(&req_token, &out) == OK);
  ASSERT_TRUE(out.err.code == VERR_NONE);
  ASSERT_TRUE(out.plan.cols != NULL);
  ASSERT_TRUE(parr_len(out.plan.cols) == 1);
  const ValidatorColPlan *c0 =
      (const ValidatorColPlan *)parr_cat(out.plan.cols, 0);
  ASSERT_TRUE(c0 != NULL);
  ASSERT_TRUE(c0->kind == VCOL_OUT_TOKEN);
  ASSERT_TRUE(c0->col_id != NULL);
  ASSERT_STREQ(c0->col_id, "users.fiscal_code");

  ValidatorRequest req_plain = {
      .db = db,
      .profile = cp,
      .sql = "SELECT u.name FROM users u WHERE u.id = 1;",
  };
  ASSERT_TRUE(validate_query(&req_plain, &out) == OK);
  ASSERT_TRUE(out.err.code == VERR_NONE);
  ASSERT_TRUE(out.plan.cols != NULL);
  ASSERT_TRUE(parr_len(out.plan.cols) == 1);
  const ValidatorColPlan *c1 =
      (const ValidatorColPlan *)parr_cat(out.plan.cols, 0);
  ASSERT_TRUE(c1 != NULL);
  ASSERT_TRUE(c1->kind == VCOL_OUT_PLAINTEXT);
  ASSERT_TRUE(c1->col_id == NULL);
  ASSERT_TRUE(c1->col_id_len == 0);

  vq_out_clean(&out);
  db_destroy(db);
  catalog_destroy(cat);
}

int main(void) {
  test_validator_accepts();
  test_validator_rejects_rules();
  test_validator_from_notes();
  test_validator_plan();
  fprintf(stderr, "OK: test_validator\n");
  return 0;
}
