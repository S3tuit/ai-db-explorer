#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "test.h"
#include "command_reader.h"
#include "stdio_byte_channel.h"

/* -------------------------------- helpers -------------------------------- */

// Each helper function takes 'file' and 'line' in input so we can log the
// line and file of the callee.

/* Tests if the reader consumes 'input' and gives back SQL 'expected'. */
static void read_one_sql_impl(const char *input, const char *expected,
                              const char *file, int line) {
  FILE *f = memfile_impl(input, file, line);

  ByteChannel *ch = stdio_bytechannel_create(f, NULL, 0);
  CommandReader *r = command_reader_create(ch);

  Command *cmd = NULL;
  int rc = command_reader_read_cmd(r, &cmd);

  ASSERT_TRUE_AT(rc == YES, file, line);
  ASSERT_TRUE_AT(cmd != NULL, file, line);
  ASSERT_TRUE_AT(cmd->type == CMD_SQL, file, line);
  ASSERT_TRUE_AT(cmd->raw_sql != NULL, file, line);
  ASSERT_STREQ_AT(cmd->raw_sql, expected, file, line);

  Command *ignored = NULL;
  ASSERT_TRUE_AT(command_reader_read_cmd(r, &ignored) == NO, file, line);

  command_destroy(cmd);
  command_reader_destroy(r);
  fclose(f);
}

/* Tests if the reader consumes 'input' and gives back 2 statements, 'e1' and
 * 'e2' in order. Then tests if the reader give back a NULL statement the 3rd
 * time. */
static void read_two_sql_impl(const char *input, const char *e1, const char *e2,
                              const char *file, int line) {
  FILE *f = memfile_impl(input, file, line);

  ByteChannel *ch = stdio_bytechannel_create(f, NULL, 0);
  CommandReader *r = command_reader_create(ch);

  Command *res1 = NULL;
  int rc1 = command_reader_read_cmd(r, &res1);
  ASSERT_TRUE_AT(rc1 == YES, file, line);
  ASSERT_TRUE_AT(res1->type == CMD_SQL, file, line);
  ASSERT_STREQ_AT(res1->raw_sql, e1, file, line);
  command_destroy(res1);

  Command *res2 = NULL;
  int rc2 = command_reader_read_cmd(r, &res2);
  ASSERT_TRUE_AT(rc2 == YES, file, line);
  ASSERT_TRUE_AT(res2->type == CMD_SQL, file, line);
  ASSERT_STREQ_AT(res2->raw_sql, e2, file, line);
  command_destroy(res2);

  // then EOF
  Command *res3 = NULL;
  int rc3 = command_reader_read_cmd(r, &res3);
  ASSERT_TRUE_AT(rc3 == NO, file, line);
  ASSERT_TRUE_AT(res3 == NULL, file, line);

  command_reader_destroy(r);
  fclose(f);
}

/* Tests if the reader consumes 'input' and gives back meta command fields. */
static void read_one_meta_impl(const char *input, const char *expected_cmd,
                               const char *expected_args,
                               const char *file, int line) {
  FILE *f = memfile_impl(input, file, line);

  ByteChannel *ch = stdio_bytechannel_create(f, NULL, 0);
  CommandReader *r = command_reader_create(ch);

  Command *cmd = NULL;
  int rc = command_reader_read_cmd(r, &cmd);
  ASSERT_TRUE_AT(rc == YES, file, line);
  ASSERT_TRUE_AT(cmd != NULL, file, line);
  ASSERT_TRUE_AT(cmd->type == CMD_META, file, line);
  ASSERT_STREQ_AT(cmd->cmd, expected_cmd, file, line);
  if (expected_args) {
    ASSERT_STREQ_AT(cmd->args, expected_args, file, line);
  } else {
    ASSERT_TRUE_AT(cmd->args == NULL, file, line);
  }

  command_destroy(cmd);
  command_reader_destroy(r);
  fclose(f);
}

/* Tests if the reader fails reading 'input'. */
static void expect_error_impl(const char *input, const char *file, int line) {
  FILE *f = memfile_impl(input, file, line);

  ByteChannel *ch = stdio_bytechannel_create(f, NULL, 0);
  CommandReader *r = command_reader_create(ch);

  Command *res = NULL;
  int rc = command_reader_read_cmd(r, &res);

  ASSERT_TRUE_AT(rc == ERR, file, line);
  ASSERT_TRUE_AT(res == NULL, file, line);

  command_reader_destroy(r);
  fclose(f);
}

// help log the line of the callee
#define read_one_sql(input, expected) read_one_sql_impl((input), (expected), __FILE__, __LINE__)
#define read_two_sql(input, e1, e2) read_two_sql_impl((input), (e1), (e2), __FILE__, __LINE__)
#define read_one_meta(input, cmd, args) \
  read_one_meta_impl((input), (cmd), (args), __FILE__, __LINE__)
#define expect_error(input) expect_error_impl((input), __FILE__, __LINE__)

/* --------------------------------- tests --------------------------------- */
static void test_cases(void) {
  // basic
  read_one_sql("SELECT 1;\n", "SELECT 1");

  // meta command (leading whitespace ignored)
  read_one_meta("  \\connect name=\"main\";\n", "connect", "name=\"main\"");
  read_one_meta("\\status;\n", "status", NULL);

  // semicolon inside quotes should not terminate early
  read_one_sql("SELECT ';' as x;\n", "SELECT ';' as x");
  read_one_sql("SELECT \"a;\" as x;\n", "SELECT \"a;\" as x");

  // escaped quotes (doubling) inside single/double quotes
  read_one_sql("SELECT 'it''s; ok' as s;\n", "SELECT 'it''s; ok' as s");
  read_one_sql("SELECT \"a\"\";\"\"b\" as i;\n", "SELECT \"a\"\";\"\"b\" as i");

  // multiline SQL
  read_one_sql("SELECT\n  1\n;\n", "SELECT\n  1\n");

  // whitespace after terminator on same line should not be included
  read_one_sql("SELECT 1;   \t \n", "SELECT 1");

  // whitespace before terminator should be preserved
  read_one_sql("SELECT 1   ;\n", "SELECT 1   ");

  // multiple statements
  read_two_sql("SELECT 1;\nSELECT 2;\n", "SELECT 1", "SELECT 2");

  // two statements on one line
  read_two_sql("SELECT 1; SELECT 2;\n", "SELECT 1", "SELECT 2");

  // empty statement is allowed
  read_one_sql(";\n", "");

  // EOF without terminator -> error (per our earlier contract)
  expect_error("SELECT 1\n");

  // unterminated quote -> error at EOF
  expect_error("SELECT 'oops;\n");
}

int main(void) {
  test_cases();
  fprintf(stderr, "OK: test_command_reader\n");
  return 0;
}
