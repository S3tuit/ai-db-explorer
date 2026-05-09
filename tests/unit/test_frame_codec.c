#include <signal.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "bufio.h"
#include "frame_codec.h"
#include "stdio_byte_channel.h"
#include "test.h"

static void write_be_u32(unsigned char *dst, uint32_t n) {
  dst[0] = (unsigned char)((n >> 24) & 0xFF);
  dst[1] = (unsigned char)((n >> 16) & 0xFF);
  dst[2] = (unsigned char)((n >> 8) & 0xFF);
  dst[3] = (unsigned char)(n & 0xFF);
}

static void test_frame_write_len(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_wrap_fd(-1, fileno(out));
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  const char *payload = "hello";
  ASSERT_TRUE(frame_write_len(bc, payload, 5) == OK);
  bufch_destroy(bc);

  char *raw = read_all(out);
  ASSERT_TRUE(raw != NULL);
  ASSERT_TRUE((unsigned char)raw[0] == 0);
  ASSERT_TRUE((unsigned char)raw[1] == 0);
  ASSERT_TRUE((unsigned char)raw[2] == 0);
  ASSERT_TRUE((unsigned char)raw[3] == 5);
  ASSERT_TRUE(memcmp(raw + 4, "hello", 5) == 0);

  free(raw);
  fclose(out);
}

static void test_frame_read_len(void) {
  unsigned char buf[4 + 5];
  write_be_u32(buf, 5);
  memcpy(buf + 4, "hello", 5);

  FILE *in = MEMFILE_OUT();
  fwrite(buf, 1, sizeof(buf), in);
  fflush(in);
  fseek(in, 0, SEEK_SET);

  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  StrBuf payload;
  sb_init(&payload);
  ASSERT_TRUE(frame_read_len(bc, &payload, 0) == FRAME_READ_LEN_OK);
  ASSERT_TRUE(payload.len == 5);
  ASSERT_TRUE(memcmp(payload.data, "hello", 5) == 0);

  sb_clean(&payload);
  bufch_destroy(bc);
  fclose(in);
}

static void test_frame_read_len_truncated(void) {
  unsigned char buf[4];
  write_be_u32(buf, 9);

  FILE *in = MEMFILE_OUT();
  fwrite(buf, 1, sizeof(buf), in);
  fflush(in);
  fseek(in, 0, SEEK_SET);

  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  StrBuf payload;
  sb_init(&payload);
  ASSERT_TRUE(frame_read_len(bc, &payload, 0) == FRAME_READ_LEN_ERR_IO);

  sb_clean(&payload);
  bufch_destroy(bc);
  fclose(in);
}

static void test_frame_read_len_cap_rejects_before_payload(void) {
  unsigned char buf[4 + 6];
  write_be_u32(buf, 6);
  memcpy(buf + 4, "abcdef", 6);

  FILE *in = MEMFILE_OUT();
  fwrite(buf, 1, sizeof(buf), in);
  fflush(in);
  fseek(in, 0, SEEK_SET);

  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  StrBuf payload;
  sb_init(&payload);
  ASSERT_TRUE(frame_read_len(bc, &payload, 5) ==
              FRAME_READ_LEN_ERR_OVERSIZE);
  ASSERT_TRUE(payload.len == 0);

  unsigned char body[6] = {0};
  ASSERT_TRUE(bufch_read_exact(bc, body, sizeof(body)) == OK);
  ASSERT_TRUE(memcmp(body, "abcdef", sizeof(body)) == 0);

  sb_clean(&payload);
  bufch_destroy(bc);
  fclose(in);
}

static void test_frame_write_cl(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_wrap_fd(-1, fileno(out));
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  const char *payload = "abc";
  ASSERT_TRUE(frame_write_rpc(bc, payload, 3, FRAME_RPC_STYLE_CONTENT_LENGTH) ==
              OK);
  bufch_destroy(bc);

  char *res = read_all(out);
  ASSERT_STREQ(res, "Content-Length: 3\r\n\r\nabc");

  free(res);
  fclose(out);
}

static AdbxTriStatus frame_read_cl(BufChannel *bc, StrBuf *out_payload) {
  FrameRpcStyle style = FRAME_RPC_STYLE_UNKNOWN;
  AdbxTriStatus rc = frame_read_rpc(bc, out_payload, &style);
  ASSERT_TRUE(style == FRAME_RPC_STYLE_CONTENT_LENGTH);
  return rc;
}

/* Reads one Content-Length framed payload from 'raw' and asserts successful
 * decoding with payload bytes equal to 'expected_payload'. It allocates one
 * temporary memfile, channel, and StrBuf, and frees them before returning.
 * Side effects: consumes one frame through frame_read_cl().
 * Error semantics: test helper; aborts when decoding fails or payload differs.
 */
static void assert_frame_read_cl_ok(const char *raw,
                                    const char *expected_payload) {
  FILE *in = MEMFILE_IN(raw);
  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  StrBuf payload;
  sb_init(&payload);
  ASSERT_TRUE(frame_read_cl(bc, &payload) == YES);
  ASSERT_TRUE(expected_payload != NULL);
  ASSERT_TRUE(payload.len == strlen(expected_payload));
  ASSERT_TRUE(memcmp(payload.data, expected_payload, payload.len) == 0);

  sb_clean(&payload);
  bufch_destroy(bc);
  fclose(in);
}

/* Reads one Content-Length frame from a live pipe whose write end remains
 * open after the full frame is written.
 * It uses a child process so the parent can fail quickly if decoding blocks.
 * Side effects: creates one pipe, forks one child, and keeps the writer open
 * until the child completes or times out.
 * Error semantics: test helper; aborts when decoding fails or blocks past the
 * timeout.
 */
static void assert_frame_read_cl_ok_live_pipe(const char *raw,
                                              const char *expected_payload) {
  int pipefd[2] = {-1, -1};
  ASSERT_TRUE(pipe(pipefd) == 0);

  size_t raw_len = strlen(raw);
  size_t off = 0;
  while (off < raw_len) {
    ssize_t nw = write(pipefd[1], raw + off, raw_len - off);
    ASSERT_TRUE(nw > 0);
    off += (size_t)nw;
  }

  pid_t pid = fork();
  ASSERT_TRUE(pid >= 0);

  if (pid == 0) {
    ASSERT_TRUE(close(pipefd[1]) == 0);

    ByteChannel *ch = stdio_bytechannel_wrap_fd(pipefd[0], -1);
    BufChannel *bc = bufch_create(ch);
    ASSERT_TRUE(bc != NULL);

    StrBuf payload;
    sb_init(&payload);

    AdbxTriStatus rc = frame_read_cl(bc, &payload);
    int ok = (rc == YES && expected_payload != NULL &&
              payload.len == strlen(expected_payload) &&
              memcmp(payload.data, expected_payload, payload.len) == 0);

    sb_clean(&payload);
    bufch_destroy(bc);
    (void)close(pipefd[0]);
    _exit(ok ? 0 : 1);
  }

  ASSERT_TRUE(close(pipefd[0]) == 0);

  int status = 0;
  int done = 0;
  struct timespec delay = {.tv_sec = 0, .tv_nsec = 10 * 1000 * 1000};
  for (int i = 0; i < 200; i++) {
    pid_t w = waitpid(pid, &status, WNOHANG);
    ASSERT_TRUE(w >= 0);
    if (w == pid) {
      done = 1;
      break;
    }
    ASSERT_TRUE(nanosleep(&delay, NULL) == 0);
  }

  if (!done) {
    (void)kill(pid, SIGKILL);
    (void)waitpid(pid, &status, 0);
    ASSERT_TRUE(!"frame_read_cl blocked on live pipe");
  }

  ASSERT_TRUE(close(pipefd[1]) == 0);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_TRUE(WEXITSTATUS(status) == 0);
}

/* Reads one Content-Length framed payload from 'raw' and asserts decode
 * rejection.
 * It allocates one temporary memfile, channel, and StrBuf, and frees them
 * before returning.
 * Side effects: consumes one frame through frame_read_cl().
 * Error semantics: test helper; aborts when frame_read_cl() does not return
 * ERR.
 */
static void assert_frame_read_cl_err(const char *raw) {
  FILE *in = MEMFILE_IN(raw);
  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  StrBuf payload;
  sb_init(&payload);
  ASSERT_TRUE(frame_read_cl(bc, &payload) == ERR);

  sb_clean(&payload);
  bufch_destroy(bc);
  fclose(in);
}

/* Reads one auto-detected MCP stdio frame from 'raw' and asserts both payload
 * bytes and detected framing style.
 * It allocates one temporary memfile, channel, and StrBuf, and frees them
 * before returning.
 * Side effects: consumes exactly one frame through frame_read_rpc().
 * Error semantics: test helper; aborts when decode or style detection differs
 * from expectations.
 */
static void assert_frame_read_rpc_ok(const char *raw, FrameRpcStyle style,
                                     const char *expected_payload) {
  FILE *in = MEMFILE_IN(raw);
  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  StrBuf payload;
  sb_init(&payload);
  FrameRpcStyle got_style = FRAME_RPC_STYLE_UNKNOWN;
  ASSERT_TRUE(frame_read_rpc(bc, &payload, &got_style) == YES);
  ASSERT_TRUE(got_style == style);
  ASSERT_TRUE(expected_payload != NULL);
  ASSERT_TRUE(payload.len == strlen(expected_payload));
  ASSERT_TRUE(memcmp(payload.data, expected_payload, payload.len) == 0);

  sb_clean(&payload);
  bufch_destroy(bc);
  fclose(in);
}

static void test_frame_read_cl(void) {
  const char *raw = "Content-Length: 5\r\n\r\nhello";
  FILE *in = MEMFILE_OUT();
  fwrite(raw, 1, strlen(raw), in);
  fflush(in);
  fseek(in, 0, SEEK_SET);

  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  StrBuf payload;
  sb_init(&payload);
  ASSERT_TRUE(frame_read_cl(bc, &payload) == YES);
  ASSERT_TRUE(payload.len == 5);
  ASSERT_TRUE(memcmp(payload.data, "hello", 5) == 0);

  sb_clean(&payload);
  bufch_destroy(bc);
  fclose(in);
}

/* Accepts a lowercase content-length header name. */
static void test_frame_read_cl_lowercase_content_length(void) {
  assert_frame_read_cl_ok("content-length: 5\r\n\r\nhello", "hello");
}

/* Accepts a mixed-case Content-Length header name. */
static void test_frame_read_cl_mixed_case_content_length(void) {
  assert_frame_read_cl_ok("CoNtEnT-LeNgTh: 5\r\n\r\nhello", "hello");
}

/* Accepts Content-Type alongside Content-Length. */
static void test_frame_read_cl_with_content_type(void) {
  assert_frame_read_cl_ok(
      "Content-Length: 5\r\n"
      "Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n"
      "\r\n"
      "hello",
      "hello");
}

/* Accepts Content-Type before Content-Length. */
static void test_frame_read_cl_content_type_before_length(void) {
  assert_frame_read_cl_ok(
      "Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n"
      "Content-Length: 5\r\n"
      "\r\n"
      "hello",
      "hello");
}

/* Ignores one unknown header while decoding Content-Length frames. */
static void test_frame_read_cl_with_unknown_header(void) {
  assert_frame_read_cl_ok("Content-Length: 5\r\n"
                          "X-Test-Trace: abc123\r\n"
                          "\r\n"
                          "hello",
                          "hello");
}

/* Accepts headers that exceed the historical 52-byte scan cap. */
static void test_frame_read_cl_header_longer_than_old_cap(void) {
  assert_frame_read_cl_ok(
      "Content-Length: 5\r\n"
      "X-Really-Long-Debug-Header: 1234567890123456789012345678901234567890\r\n"
      "\r\n"
      "hello",
      "hello");
}

/* Accepts longer headers even when the peer keeps the pipe open after the
 * request, matching stdio transports used by integration tests and MCP hosts.
 */
static void test_frame_read_cl_header_longer_than_old_cap_live_pipe(void) {
  assert_frame_read_cl_ok_live_pipe(
      "Content-Length: 5\r\n"
      "X-Really-Long-Debug-Header: 1234567890123456789012345678901234567890\r\n"
      "\r\n"
      "hello",
      "hello");
}

/* Rejects duplicate Content-Length headers to avoid ambiguous framing. */
static void test_frame_read_cl_duplicate_content_length_rejected(void) {
  assert_frame_read_cl_err("Content-Length: 5\r\n"
                           "Content-Length: 5\r\n"
                           "\r\n"
                           "hello");
}

/* Auto-detects Content-Length framing for header-prefixed payloads. */
static void test_frame_read_rpc_content_length_style(void) {
  assert_frame_read_rpc_ok("Content-Length: 5\r\n\r\nhello",
                           FRAME_RPC_STYLE_CONTENT_LENGTH, "hello");
}

/* Auto-detects JSONL framing for line-delimited payloads. */
static void test_frame_read_rpc_jsonl_style(void) {
  assert_frame_read_rpc_ok("{\"jsonrpc\":\"2.0\",\"id\":1}\n",
                           FRAME_RPC_STYLE_JSONL,
                           "{\"jsonrpc\":\"2.0\",\"id\":1}");
}

/* Writes JSONL framing when requested by the caller. */
static void test_frame_write_rpc_jsonl(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_wrap_fd(-1, fileno(out));
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  const char *payload = "{\"jsonrpc\":\"2.0\",\"id\":1}";
  ASSERT_TRUE(frame_write_rpc(bc, payload, strlen(payload),
                              FRAME_RPC_STYLE_JSONL) == OK);
  bufch_destroy(bc);

  char *res = read_all(out);
  ASSERT_STREQ(res, "{\"jsonrpc\":\"2.0\",\"id\":1}\n");

  free(res);
  fclose(out);
}

int main(void) {
  test_frame_write_len();
  test_frame_read_len();
  test_frame_read_len_truncated();
  test_frame_read_len_cap_rejects_before_payload();
  test_frame_write_cl();
  test_frame_read_cl();
  test_frame_read_cl_lowercase_content_length();
  test_frame_read_cl_mixed_case_content_length();
  test_frame_read_cl_with_content_type();
  test_frame_read_cl_content_type_before_length();
  test_frame_read_cl_with_unknown_header();
  test_frame_read_cl_header_longer_than_old_cap();
  test_frame_read_cl_header_longer_than_old_cap_live_pipe();
  test_frame_read_cl_duplicate_content_length_rejected();
  test_frame_read_rpc_content_length_style();
  test_frame_read_rpc_jsonl_style();
  test_frame_write_rpc_jsonl();

  fprintf(stderr, "OK: test_frame_codec\n");
  return 0;
}
