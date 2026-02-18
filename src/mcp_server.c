#define _GNU_SOURCE

#include "mcp_server.h"
#include "file_io.h"
#include "frame_codec.h"
#include "handshake_codec.h"
#include "json_codec.h"
#include "log.h"
#include "mcp_id.h"
#include "resume_token.h"
#include "utils.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static void mcpser_set_err(McpServer *s, const char *msg) {
  if (!s)
    return;
  if (!msg)
    msg = "unknown error";
  snprintf(s->last_err, sizeof(s->last_err), "%s", msg);
}

static int mcpser_send_error(McpServer *s, const McpId *id, long code,
                             const char *msg, const char *requested);

/* Marks broker channel as unavailable and drops its socket/channel resources.
 * Ownership: borrows 's'.
 * Side effects: closes broker channel and clears runtime-ready state bit.
 * Error semantics: returns OK on state update, ERR on invalid input.
 */
static int mcpser_invalidate_broker(McpServer *s) {
  if (!s)
    return ERR;
  s->flags &= ~MCPSER_F_BROKER_READY;
  bufch_clean(&s->brok_bc);
  return OK;
}

/* Connects to the Unix-domain socket at 'path'.
 * Ownership: borrows 'path'; returns a new fd owned by caller.
 * Side effects: creates and connects a socket.
 * Error semantics: returns connected fd on success, -1 on invalid input or
 * socket/connect failures.
 */
static int connect_unix_socket(const char *path) {
  if (!path)
    return -1;
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  if (strlen(path) >= sizeof(addr.sun_path)) {
    close(fd);
    return -1;
  }
  strcpy(addr.sun_path, path);

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

/* Opens and installs a broker channel connected to 'sock_path'.
 * Ownership: borrows 's' and 'sock_path'; stores owned channel in 's'.
 * Side effects: creates/connects a Unix socket and mutates 's->brok_bc'.
 * Error semantics: returns OK on success, ERR on invalid input or connection/
 * channel creation failure.
 */
static int mcpser_connect_broker_channel(McpServer *s, const char *sock_path) {
  if (!s || !sock_path)
    return ERR;

  (void)mcpser_invalidate_broker(s);

  int brok_fd = connect_unix_socket(sock_path);
  if (brok_fd < 0)
    return ERR;
  if (bufch_stdio_openfd_init(&s->brok_bc, brok_fd, brok_fd) != OK)
    return ERR;

  return OK;
}

/* Writes one framed broker handshake request using provided credentials.
 * Ownership: borrows all inputs.
 * Side effects: writes one binary frame to 's->brok_bc'.
 * Error semantics: returns OK on success, ERR on invalid input or write
 * failure.
 */
static int mcpser_send_broker_handshake_req(
    McpServer *s, const uint8_t secret_token[SECRET_TOKEN_LEN], int use_resume,
    const uint8_t resume_token[RESUME_TOKEN_LEN]) {
  if (!s || !s->brok_bc.ch || !secret_token)
    return ERR;
  if (use_resume == YES && !resume_token)
    return ERR;

  handshake_req_t req = {0};
  req.magic = HANDSHAKE_MAGIC;
  req.version = HANDSHAKE_VERSION;
  memcpy(req.secret_token, secret_token, SECRET_TOKEN_LEN);
  if (use_resume == YES) {
    req.flags |= HANDSHAKE_FLAG_RESUME;
    memcpy(req.resume_token, resume_token, RESUME_TOKEN_LEN);
  }

  uint8_t wire[HANDSHAKE_REQ_WIRE_SIZE];
  if (handshake_req_encode(&req, wire) != OK)
    return ERR;
  return frame_write_len(&s->brok_bc, wire, (uint32_t)sizeof(wire));
}

/* Reads one framed broker handshake response and validates envelope fields.
 * Ownership: borrows 's'; writes parsed response into caller-owned 'out'.
 * Side effects: reads one binary frame from 's->brok_bc'.
 * Error semantics: returns OK on valid response frame, ERR on invalid input,
 * framing I/O failure, payload-size mismatch, or bad magic/version.
 */
static int mcpser_read_broker_handshake_resp(McpServer *s,
                                             handshake_resp_t *out) {
  if (!s || !s->brok_bc.ch || !out)
    return ERR;

  StrBuf payload;
  sb_init(&payload);
  int rc = frame_read_len(&s->brok_bc, &payload);
  if (rc != OK) {
    sb_clean(&payload);
    return ERR;
  }

  if (handshake_resp_decode(out, (const uint8_t *)payload.data, payload.len) !=
      OK) {
    sb_clean(&payload);
    return ERR;
  }
  sb_clean(&payload);

  if (out->magic != HANDSHAKE_MAGIC || out->version != HANDSHAKE_VERSION)
    return ERR;
  return OK;
}

/* Maps broker handshake status code to human-readable reason.
 * Ownership: pure helper, borrows no resources.
 * Side effects: none.
 * Error semantics: returns static string for any input status value.
 */
static const char *mcpser_hs_status_desc(handshake_status st) {
  switch (st) {
  case HS_OK:
    return "ok";
  case HS_ERR_BAD_MAGIC:
    return "bad magic";
  case HS_ERR_BAD_VERSION:
    return "bad version";
  case HS_ERR_TOKEN_EXPIRED:
    return "token expired";
  case HS_ERR_TOKEN_UNKNOWN:
    return "token unknown";
  case HS_ERR_FULL:
    return "broker full";
  case HS_ERR_REQ:
    return "bad request";
  case HS_ERR_INTERNAL:
    return "broker internal";
  default:
    return "unknown status";
  }
}

/* Ensures broker channel is connected and handshake-complete for this server.
 * Ownership: borrows 's'; uses borrowed private-dir paths and owned resume
 * token store contained in 's'.
 * Side effects: may read secret token from disk, open/close broker sockets,
 * perform framed handshake I/O, and update persisted resume token state.
 * Error semantics: returns OK when broker is ready (already ready or newly
 * connected), ERR on missing/invalid inputs or failed connect/handshake flow.
 */
static int mcpser_connect_and_handshake_broker(McpServer *s) {
  if (!s || !s->privd || !s->privd->sock_path)
    return ERR;

  // Ready means we already have a connected channel that completed handshake.
  if ((s->flags & MCPSER_F_BROKER_READY) != 0 && s->brok_bc.ch != NULL)
    return OK;
  if ((s->flags & MCPSER_F_BROKER_READY) != 0 && s->brok_bc.ch == NULL)
    s->flags &= ~MCPSER_F_BROKER_READY;

  uint8_t secret_token[SECRET_TOKEN_LEN] = {0};
  if (privdir_read_token(s->privd, secret_token) != OK) {
    TLOG("ERROR - failed to read broker secret token before reconnect");
    return ERR;
  }

  uint8_t resume_token[RESUME_TOKEN_LEN] = {0};
  int have_resume = (restok_load(&s->restok, resume_token) == YES) ? YES : NO;

  for (int attempt = 0; attempt < 2; attempt++) {
    // Broker requires handshake as first frame, so each retry uses a fresh
    // socket/channel.
    if (mcpser_connect_broker_channel(s, s->privd->sock_path) != OK)
      return ERR;

    // try handshake once
    handshake_resp_t resp = {0};
    int rc = mcpser_send_broker_handshake_req(s, secret_token, have_resume,
                                              resume_token);
    if (rc == OK)
      rc = mcpser_read_broker_handshake_resp(s, &resp);
    if (rc != OK) {
      (void)mcpser_invalidate_broker(s);
      return ERR;
    }

    if (resp.status == HS_OK) {
      if (restok_store(&s->restok, resp.resume_token) != OK) {
        fprintf(stderr,
                "Failed to write token file: session resume disabled\n");
      }
      s->flags |= MCPSER_F_BROKER_READY;
      return OK;
    }

    // Broker closes on handshake errors; mirror state locally before retry/fail.
    (void)mcpser_invalidate_broker(s);

    if (have_resume == YES && (resp.status == HS_ERR_TOKEN_EXPIRED ||
                               resp.status == HS_ERR_TOKEN_UNKNOWN)) {
      fprintf(stderr, "Broker rejected resume token, starting fresh session\n");
      (void)restok_delete(&s->restok);
      have_resume = NO;
      memset(resume_token, 0, sizeof(resume_token));
      continue;
    }

    fprintf(stderr, "Broker handshake failed: %s\n",
            mcpser_hs_status_desc(resp.status));
    return ERR;
  }

  return ERR;
}

/* Emits a JSON-RPC error explaining broker unavailability.
 * Ownership: borrows all inputs.
 * Side effects: writes one error frame to the MCP host output channel.
 * Error semantics: returns OK on successful write, ERR on invalid input or
 * write failure.
 */
static int mcpser_send_broker_unavailable(McpServer *s, const McpId *id) {
  return mcpser_send_error(
      s, id, -32600,
      "Unable to reach broker. Please, try again. If the issue persists, ask "
      "the user to check for broker health.",
      NULL);
}

/* Writes a JSON-RPC error response to the MCP host output channel.
 * Ownership: borrows 's', optional 'id', and message pointers; temporary JSON
 * buffer is internal and freed before return.
 * Side effects: serializes JSON and writes one Content-Length frame to
 * 's->out_bc'.
 * Error semantics: returns OK on successful serialization/write, ERR on invalid
 * input, allocation failure, or output I/O failure.
 */
static int mcpser_send_error(McpServer *s, const McpId *id, long code,
                             const char *msg, const char *requested) {
  if (!s || !s->out_bc.ch || !msg)
    return ERR;

  StrBuf sb;
  sb_init(&sb);
  if (json_rpc_begin(&sb) != OK)
    goto err;
  if (id) {
    if (id->kind == MCP_ID_STR) {
      if (json_kv_str(&sb, "id", id->str ? id->str : "") != OK)
        goto err;
    } else {
      if (json_kv_u64(&sb, "id", id->u32) != OK)
        goto err;
    }
  } else {
    if (json_kv_null(&sb, "id") != OK)
      goto err;
  }
  if (json_kv_obj_begin(&sb, "error") != OK)
    goto err;
  if (json_kv_l(&sb, "code", code) != OK)
    goto err;
  if (json_kv_str(&sb, "message", msg) != OK)
    goto err;

  if (requested) {
    if (json_kv_obj_begin(&sb, "data") != OK)
      goto err;
    if (json_kv_arr_begin(&sb, "supported") != OK)
      goto err;
    if (json_arr_elem_str(&sb, MCP_PROTOCOL_VERSION) != OK)
      goto err;
    if (json_arr_end(&sb) != OK)
      goto err;
    if (json_kv_str(&sb, "requested", requested) != OK)
      goto err;
    if (json_obj_end(&sb) != OK)
      goto err;
  }

  if (json_obj_end(&sb) != OK)
    goto err;
  if (json_obj_end(&sb) != OK)
    goto err;

  int rc = frame_write_cl(&s->out_bc, sb.data, sb.len);
  sb_clean(&sb);
  return rc;

err:
  sb_clean(&sb);
  return ERR;
}

int mcpser_init(McpServer *s, const McpServerInit *init) {
  if (!s || !init || !init->in || !init->out || !init->privd)
    return ERR;
  memset(s, 0, sizeof(*s));
  s->last_err[0] = '\0';

  int in_fd = fileno(init->in);
  int out_fd = fileno(init->out);
  if (in_fd < 0 || out_fd < 0)
    return ERR;

  // The McpServer doesn't own the input/output file descriptors, so it wraps
  // them as non-owning channels.
  if (bufch_stdio_wrapfd_init(&s->in_bc, in_fd, -1) != OK)
    return ERR;
  if (bufch_stdio_wrapfd_init(&s->out_bc, -1, out_fd) != OK) {
    bufch_clean(&s->in_bc);
    return ERR;
  }

  s->privd = init->privd;
  s->flags = 0;

  if (restok_init(&s->restok) == ERR) {
    mcpser_clean(s);
    mcpser_set_err(s, "failed to initialize resume token store");
    return ERR;
  }

  // Best-effort eager connect: startup remains available even if broker is not
  // ready yet. Each request retries via mcpser_connect_and_handshake_broker().
  if (mcpser_connect_and_handshake_broker(s) != OK) {
    TLOG("INFO - broker not ready during server init; requests will retry");
  }

  return OK;
}

/* Validates and answers the user-facing MCP "initialize" JSON-RPC request.
 * Ownership: borrows 's'; allocates temporary request/response buffers and
 * frees them before return.
 * Side effects: reads one Content-Length-framed request from stdin channel and
 * writes one framed response (or error) to stdout channel.
 * Error semantics: returns OK on valid initialize flow, ERR on malformed input
 * framing/JSON, write failure, or protocol mismatch.
 */
static int mcpser_user_initialize_handshake(McpServer *s) {
  if (!s)
    return ERR;

  StrBuf req;
  sb_init(&req);
  int rc = frame_read_cl(&s->in_bc, &req);
  if (rc != YES) {
    sb_clean(&req);
    return ERR;
  }

  const char *json = req.data;

  JsonGetter jg;
  int irc = jsget_init(&jg, json, req.len);
  // if it's not a valid JSON-RPC, we still try to find a top-leve "id" key
  // before returning the error
  int vrc = (irc == OK) ? jsget_simple_rpc_validation(&jg) : ERR;

  McpId id = {0};
  const McpId *idp = NULL;
  if (irc == OK) {
    if (jsget_u32(&jg, "id", &id.u32) == YES) {
      id.kind = MCP_ID_INT;
      idp = &id;
    } else {
      char *id_str = NULL;
      int src = jsget_string_decode_alloc(&jg, "id", &id_str);
      if (src == YES) {
        id.kind = MCP_ID_STR;
        id.str = id_str;
        idp = &id;
      }
    }
  }

  if (vrc != YES) {
    sb_clean(&req);
    (void)mcpser_send_error(s, idp, -32600, "Invalid Request.", NULL);
    TLOG("ERROR - handshake: invalid JSON-RPC");
    mcpser_set_err(s, "handshake rejected invalid request");
    if (idp)
      mcp_id_clean(&id);
    return ERR;
  }

  JsonStrSpan method = {0};
  JsonStrSpan proto = {0};
  int mrc = jsget_string_span(&jg, "method", &method);
  // If the server supports the requested protocol version, it MUST respond
  // with the same version. Otherwise, the server MUST respond with another
  // protocol version it supports.
  int prc = jsget_string_span(&jg, "params.protocolVersion", &proto);
  if (mrc != YES || prc != YES || method.len == 0 ||
      method.len != strlen("initialize") ||
      memcmp(method.ptr, "initialize", method.len) != 0) {
    sb_clean(&req);
    (void)mcpser_send_error(s, idp, -32600, "Invalid Request.", NULL);
    TLOG("ERROR - handshake: invalid initialize request");
    mcpser_set_err(s, "handshake rejected invalid initialize");
    if (idp)
      mcp_id_clean(&id);
    return ERR;
  }

  sb_clean(&req);

  StrBuf sb;
  sb_init(&sb);
  if (json_rpc_begin(&sb) != OK)
    goto fail;
  if (id.kind == MCP_ID_STR) {
    if (json_kv_str(&sb, "id", id.str ? id.str : "") != OK)
      goto fail;
  } else {
    if (json_kv_u64(&sb, "id", id.u32) != OK)
      goto fail;
  }
  if (json_kv_obj_begin(&sb, "result") != OK)
    goto fail;
  if (json_kv_str(&sb, "protocolVersion", MCP_PROTOCOL_VERSION) != OK)
    goto fail;
  if (json_kv_obj_begin(&sb, "capabilities") != OK)
    goto fail;
  if (json_kv_obj_begin(&sb, "tools") != OK)
    goto fail;
  if (json_obj_end(&sb) != OK)
    goto fail;
  if (json_kv_obj_begin(&sb, "resources") != OK)
    goto fail;
  if (json_obj_end(&sb) != OK)
    goto fail;
  if (json_obj_end(&sb) != OK)
    goto fail;
  if (json_kv_obj_begin(&sb, "serverInfo") != OK)
    goto fail;
  if (json_kv_str(&sb, "name", "ai-db-explorer") != OK)
    goto fail;
  if (json_kv_str(&sb, "version", "0.0.1") != OK)
    goto fail;
  if (json_obj_end(&sb) != OK)
    goto fail;
  if (json_obj_end(&sb) != OK)
    goto fail;
  if (json_obj_end(&sb) != OK)
    goto fail;

  int wrc = frame_write_cl(&s->out_bc, sb.data, sb.len);
  sb_clean(&sb);
  if (id.kind == MCP_ID_STR)
    mcp_id_clean(&id);
  return wrc;

fail:
  sb_clean(&sb);
  if (id.kind == MCP_ID_STR)
    mcp_id_clean(&id);
  return ERR;
}

/* Validates one user JSON-RPC request frame before broker forwarding.
 * Ownership: borrows 's' and 'req'; writes parsed id to caller-owned 'id_out'
 * (string id ownership transfers to caller when return is YES).
 * Side effects: may write JSON-RPC error replies to 's->out_bc' for invalid
 * requests.
 * Error semantics: returns YES for valid request, NO for non-fatal
 * reject/ignore (error already sent or notification without id), ERR when
 * processing must stop (typically failed error-response write or invalid input
 * pointers).
 */
static int mcpser_validate_user_req(McpServer *s, const StrBuf *req,
                                    McpId *id_out, const McpId **idp_out) {
  if (!s || !req || !id_out || !idp_out)
    return ERR;

  memset(id_out, 0, sizeof(*id_out));
  *idp_out = NULL;

  JsonGetter jg;
  int irc = jsget_init(&jg, req->data, req->len);
  if (irc != OK) {
    fprintf(stderr, "McpServer: malformed input\n");
    TLOG("ERROR - invalid JSON in MCP input");
    if (mcpser_send_error(s, NULL, -32600, "Malformed JSON-RPC request",
                          NULL) != OK) {
      mcpser_set_err(s, "failed to write error response");
      return ERR;
    }
    return NO;
  }

  if (jsget_u32(&jg, "id", &id_out->u32) == YES) {
    id_out->kind = MCP_ID_INT;
    *idp_out = id_out;
  } else {
    char *id_str = NULL;
    int src = jsget_string_decode_alloc(&jg, "id", &id_str);
    if (src == YES) {
      id_out->kind = MCP_ID_STR;
      id_out->str = id_str;
      *idp_out = id_out;
    } else if (src == NO) {
      // Notifications have no id; ignore them for now.
      return NO;
    } else {
      TLOG("ERROR - invalid id in JSON-RPC request");
      if (mcpser_send_error(s, NULL, -32600, "Invalid JSON-RPC request.",
                            NULL) != OK) {
        mcpser_set_err(s, "failed to write error response");
        return ERR;
      }
      return NO;
    }
  }

  int vrc = jsget_simple_rpc_validation(&jg);
  if (vrc != YES) {
    fprintf(stderr, "McpServer: invalid input\n");
    TLOG("ERROR - invalid JSON-RPC envelope");
    if (mcpser_send_error(s, *idp_out, -32600, "Invalid JSON-RPC request.",
                          NULL) != OK) {
      mcpser_set_err(s, "failed to write error response");
      if (id_out->kind == MCP_ID_STR)
        mcp_id_clean(id_out);
      return ERR;
    }
    if (id_out->kind == MCP_ID_STR)
      mcp_id_clean(id_out);
    *idp_out = NULL;
    return NO;
  }

  // Overflow guard before narrowing request length to uint32 for broker frame.
  if (req->len > UINT32_MAX) {
    fprintf(stderr, "McpServer: request too large\n");
    TLOG("ERROR - request too large: len=%zu", req->len);
    if (mcpser_send_error(s, *idp_out, -32600, "Request too large.", NULL) !=
        OK) {
      mcpser_set_err(s, "failed to write error response");
      if (id_out->kind == MCP_ID_STR)
        mcp_id_clean(id_out);
      return ERR;
    }
    if (id_out->kind == MCP_ID_STR)
      mcp_id_clean(id_out);
    *idp_out = NULL;
    return NO;
  }

  return YES;
}

int mcpser_run(McpServer *s) {
  // This is the flow of McpServer:
  // handshake -> read JSON-RPC -> validate -> write to broker -> read from
  // broker -> write to out channel
  if (!s || !s->in_bc.ch || !s->out_bc.ch || !s->privd)
    return ERR;

  int hrc = mcpser_user_initialize_handshake(s);
  if (hrc != OK)
    return ERR;
  TLOG("INFO - handshake complete, entering main loop");

  for (;;) {
    // McpServer reads JSON-RPC request
    StrBuf req;
    sb_init(&req);
    int rc = frame_read_cl(&s->in_bc, &req);
    TLOG("INFO - frame_read_cl rc=%d len=%zu", rc, req.len);
    if (rc == NO) {
      // EOF
      sb_clean(&req);
      TLOG("INFO - EOF on MCP stdin.");
      return OK;
    }
    if (rc == ERR) {
      sb_clean(&req);
      TLOG("ERROR - frame_read_cl failed while reading MCP input");
      mcpser_set_err(s, "failed to read input");
      return ERR;
    }

    McpId id = {0};
    const McpId *idp = NULL;
    int vrc = mcpser_validate_user_req(s, &req, &id, &idp);
    if (vrc == ERR) {
      sb_clean(&req);
      return ERR;
    }
    if (vrc == NO) {
      sb_clean(&req);
      continue;
    }

    // Keep fail-closed semantics: never process requests without a live broker
    // handshake, but keep MCP server alive and reply with explicit errors.
    if (mcpser_connect_and_handshake_broker(s) != OK) {
      fprintf(stderr, "McpServer: broker unavailable\n");
      TLOG("ERROR - broker connect+handshake failed for request");
      sb_clean(&req);
      if (mcpser_send_broker_unavailable(s, idp) != OK) {
        mcpser_set_err(s, "failed to write error response");
        if (idp)
          mcp_id_clean(&id);
        return ERR;
      }
      if (idp)
        mcp_id_clean(&id);
      continue;
    }

    // McpServer sends request to the broker.
    if (frame_write_len(&s->brok_bc, req.data, (uint32_t)req.len) != OK) {
      fprintf(stderr, "McpServer: broker write failed\n");
      sb_clean(&req);
      TLOG("ERROR - failed to write request to broker");
      (void)mcpser_invalidate_broker(s);
      if (mcpser_send_broker_unavailable(s, idp) != OK) {
        mcpser_set_err(s, "failed to write error response");
        if (idp)
          mcp_id_clean(&id);
        return ERR;
      }
      if (idp)
        mcp_id_clean(&id);
      continue;
    }
    sb_clean(&req);

    // McpServer reads broker response
    //
    StrBuf resp;
    sb_init(&resp);
    if (frame_read_len(&s->brok_bc, &resp) != OK) {
      fprintf(stderr, "McpServer: broker read failed\n");
      sb_clean(&resp);
      TLOG("ERROR - failed to read response from broker");
      (void)mcpser_invalidate_broker(s);
      if (mcpser_send_broker_unavailable(s, idp) != OK) {
        mcpser_set_err(s, "failed to write error response");
        if (idp)
          mcp_id_clean(&id);
        return ERR;
      }
      if (idp)
        mcp_id_clean(&id);
      continue;
    }

    // McpServer writes response to user
    if (frame_write_cl(&s->out_bc, resp.data, resp.len) != OK) {
      fprintf(stderr, "McpServer: stdout write failed\n");
      sb_clean(&resp);
      TLOG("ERROR - failed to write response to stdout");
      mcpser_set_err(s, "failed to write to stdout");
      if (idp)
        mcp_id_clean(&id);
      return ERR;
    }

    sb_clean(&resp);
    if (idp)
      mcp_id_clean(&id);
  }
}

void mcpser_clean(McpServer *s) {
  if (!s)
    return;
  bufch_clean(&s->in_bc);
  bufch_clean(&s->brok_bc);
  bufch_clean(&s->out_bc);
  restok_clean(&s->restok);
  s->privd = NULL;
  s->flags = 0;
  s->last_err[0] = '\0';
}

const char *mcpser_last_error(const McpServer *s) {
  if (!s)
    return "McpServer is NULL";
  return s->last_err;
}
