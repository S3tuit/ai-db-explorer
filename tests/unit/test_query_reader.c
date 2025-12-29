#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "test.h"
#include "query_reader.h"
#include "stdio_byte_channel.h"

/* -------------------------------- helpers -------------------------------- */

// Each helper function takes 'file' and 'line' in input so we can log the
// line and file of the callee.

/* Tests if the reader consumes 'input' and gives back 'expected'. */
static void read_one_impl(const char *input, const char *expected, const char *file, int line) {
  FILE *f = memfile_impl(input, file, line);

  ByteChannel *ch = stdio_bytechannel_create(f, NULL, 0);
  QueryReader *r = query_reader_create(ch);

  char *res = NULL;
  int rc = query_reader_read_sql(r, &res);

  ASSERT_TRUE_AT(rc == YES, file, line);
  ASSERT_TRUE_AT(res != NULL, file, line);
  ASSERT_STREQ_AT(res, expected, file, line);

  free(res);
  query_reader_destroy(r);
  fclose(f);
}

/* Tests if the reader consumes 'input' and gives back 2 statements, 'e1' and
 * 'e2' in order. Then tests if the reader give back a NULL statement the 3rd
 * time. */
static void read_two_impl(const char *input, const char *e1, const char *e2, const char *file, int line) {
  FILE *f = memfile_impl(input, file, line);

  ByteChannel *ch = stdio_bytechannel_create(f, NULL, 0);
  QueryReader *r = query_reader_create(ch);

  char *res1 = NULL;
  int rc1 = query_reader_read_sql(r, &res1);
  ASSERT_TRUE_AT(rc1 == YES, file, line);
  ASSERT_STREQ_AT(res1, e1, file, line);
  free(res1);

  char *res2 = NULL;
  int rc2 = query_reader_read_sql(r, &res2);
  ASSERT_TRUE_AT(rc2 == YES, file, line);
  ASSERT_STREQ_AT(res2, e2, file, line);
  free(res2);

  // then EOF
  char *res3 = NULL;
  int rc3 = query_reader_read_sql(r, &res3);
  ASSERT_TRUE_AT(rc3 == NO, file, line);
  ASSERT_TRUE_AT(res3 == NULL, file, line);

  query_reader_destroy(r);
  fclose(f);
}

/* Tests if the reader fails reading 'input'. */
static void expect_error_impl(const char *input, const char *file, int line) {
  FILE *f = memfile_impl(input, file, line);

  ByteChannel *ch = stdio_bytechannel_create(f, NULL, 0);
  QueryReader *r = query_reader_create(ch);

  char *res = NULL;
  int rc = query_reader_read_sql(r, &res);

  ASSERT_TRUE_AT(rc == ERR, file, line);
  ASSERT_TRUE_AT(res == NULL, file, line);

  query_reader_destroy(r);
  fclose(f);
}

// help log the line of the callee
#define read_one(input, expected) read_one_impl((input), (expected), __FILE__, __LINE__)
#define read_two(input, e1, e2) read_two_impl((input), (e1), (e2), __FILE__, __LINE__)
#define expect_error(input) expect_error_impl((input), __FILE__, __LINE__)

/* --------------------------------- tests --------------------------------- */
static void test_cases(void) {
  // basic
  read_one("SELECT 1;\n", "SELECT 1;");

  // semicolon inside quotes should not terminate early
  read_one("SELECT ';' as x;\n", "SELECT ';' as x;");
  read_one("SELECT \"a;\" as x;\n", "SELECT \"a;\" as x;");

  // escaped quotes (doubling) inside single/double quotes
  read_one("SELECT 'it''s; ok' as s;\n", "SELECT 'it''s; ok' as s;");
  read_one("SELECT \"a\"\";\"\"b\" as i;\n", "SELECT \"a\"\";\"\"b\" as i;");

  // multiline SQL
  read_one("SELECT\n  1\n;\n", "SELECT\n  1\n;");

  // whitespace after terminator on same line should not be included
  read_one("SELECT 1;   \t \n", "SELECT 1;");

  // multiple statements
  read_two("SELECT 1;\nSELECT 2;\n", "SELECT 1;", "SELECT 2;");

  // two statements on one line
  read_two("SELECT 1; SELECT 2;\n", "SELECT 1;", "SELECT 2;");

  // empty statement is allowed
  read_one(";\n", ";");

  // EOF without terminator -> error (per our earlier contract)
  expect_error("SELECT 1\n");

  // unterminated quote -> error at EOF
  expect_error("SELECT 'oops;\n");
}

int main(void) {
  test_cases();
  fprintf(stderr, "OK: test_query_reader\n");
  return 0;
}
