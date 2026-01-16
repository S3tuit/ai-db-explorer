#define _GNU_SOURCE

#include "mcp_server.h"
#include "log.h"
#include "frame_codec.h"
#include "json_codec.h"
#include "mcp_id.h"
#include "stdio_byte_channel.h"
#include "utils.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>

#define MCP_PROTOCOL_VERSION "2025-11-25"

static void mcpser_set_err(McpServer *s, const char *msg) {
    if (!s) return;
    if (!msg) msg = "unknown error";
    snprintf(s->last_err, sizeof(s->last_err), "%s", msg);
}

/* Connects to the socket located at 'path' and returns the new endpoint to
 * use for communication. On error returns -1. */
static int connect_unix_socket(const char *path) {
    if (!path) return -1;
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

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

/* Writes a JSON-RPC error response. If 'requested' is non-NULL, include data
 * with supported/requested protocol versions. */
static int mcpser_send_error(McpServer *s, const McpId *id, long code,
                             const char *msg, const char *requested) {
    if (!s || !s->out_bc || !msg) return ERR;

    StrBuf sb = {0};
    if (json_rpc_begin(&sb) != OK) goto err;
    if (id) {
        if (id->kind == MCP_ID_STR) {
            if (json_kv_str(&sb, "id", id->str ? id->str : "") != OK) goto err;
        } else {
            if (json_kv_u64(&sb, "id", id->u32) != OK) goto err;
        }
    } else {
        if (json_kv_null(&sb, "id") != OK) goto err;
    }
    if (json_kv_obj_begin(&sb, "error") != OK) goto err;
    if (json_kv_l(&sb, "code", code) != OK) goto err;
    if (json_kv_str(&sb, "message", msg) != OK) goto err;

    if (requested) {
        if (json_kv_obj_begin(&sb, "data") != OK) goto err;
        if (json_kv_arr_begin(&sb, "supported") != OK) goto err;
        if (json_arr_elem_str(&sb, MCP_PROTOCOL_VERSION) != OK) goto err;
        if (json_arr_end(&sb) != OK) goto err;
        if (json_kv_str(&sb, "requested", requested) != OK) goto err;
        if (json_obj_end(&sb) != OK) goto err;
    }

    if (json_obj_end(&sb) != OK) goto err;
    if (json_obj_end(&sb) != OK) goto err;

    int rc = frame_write_cl(s->out_bc, sb.data, sb.len);
    sb_clean(&sb);
    return rc;

err:
    sb_clean(&sb);
    return ERR;
}

int mcpser_init(McpServer *s, FILE *in, FILE *out, const char *sock_path) {
    if (!s || !in || !out || !sock_path) return ERR;
    memset(s, 0, sizeof(*s));
    s->next_id = 1;
    s->last_err[0] = '\0';

    int in_fd = fileno(in);
    int out_fd = fileno(out);
    if (in_fd < 0 || out_fd < 0) return ERR;

    // The McpServer doesn't own the input file destriptors, so it won't cloes
    // them. That's because usually they're stdin and stdout
    ByteChannel *stdin_ch = stdio_bytechannel_wrap_fd(in_fd, -1);
    if (!stdin_ch) return ERR;
    s->in_bc = bufch_create(stdin_ch);
    if (!s->in_bc) return ERR;

    ByteChannel *out_ch = stdio_bytechannel_wrap_fd(-1, out_fd);
    if (!out_ch) {
        bufch_destroy(s->in_bc);
        s->in_bc = NULL;
        return ERR;
    }
    s->out_bc = bufch_create(out_ch);
    if (!s->out_bc) {
        bytech_destroy(out_ch);
        bufch_destroy(s->in_bc);
        s->in_bc = NULL;
        return ERR;
    }

    // tries to connect to the Broker
    int brok_fd = connect_unix_socket(sock_path);
    if (brok_fd < 0) {
        mcpser_clean(s);
        return ERR;
    }
    ByteChannel *brok_ch = stdio_bytechannel_open_fd(brok_fd, brok_fd);
    if (!brok_ch) {
        close(brok_fd);
        mcpser_clean(s);
        return ERR;
    }
    s->brok_bc = bufch_create(brok_ch);
    if (!s->brok_bc) {
        bytech_destroy(brok_ch);
        mcpser_clean(s);
        return ERR;
    }

    return OK;
}

/* Handles the MCP initialize handshake. */
static int mcpser_handshake(McpServer *s) {
    if (!s) return ERR;

    StrBuf req = {0};
    int rc = frame_read_cl(s->in_bc, &req);
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
        if (idp) mcp_id_clean(&id);
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
        if (idp) mcp_id_clean(&id);
        return ERR;
    }

    sb_clean(&req);

    StrBuf sb = {0};
    if (json_rpc_begin(&sb) != OK) goto fail;
    if (id.kind == MCP_ID_STR) {
        if (json_kv_str(&sb, "id", id.str ? id.str : "") != OK) goto fail;
    } else {
        if (json_kv_u64(&sb, "id", id.u32) != OK) goto fail;
    }
    if (json_kv_obj_begin(&sb, "result") != OK) goto fail;
    if (json_kv_str(&sb, "protocolVersion", MCP_PROTOCOL_VERSION) != OK) goto fail;
    if (json_kv_obj_begin(&sb, "capabilities") != OK) goto fail;
    if (json_kv_obj_begin(&sb, "tools") != OK) goto fail;
    if (json_obj_end(&sb) != OK) goto fail;
    if (json_kv_obj_begin(&sb, "resources") != OK) goto fail;
    if (json_obj_end(&sb) != OK) goto fail;
    if (json_obj_end(&sb) != OK) goto fail;
    if (json_kv_obj_begin(&sb, "serverInfo") != OK) goto fail;
    if (json_kv_str(&sb, "name", "ai-db-explorer") != OK) goto fail;
    if (json_kv_str(&sb, "version", "0.0.1") != OK) goto fail;
    if (json_obj_end(&sb) != OK) goto fail;
    if (json_obj_end(&sb) != OK) goto fail;
    if (json_obj_end(&sb) != OK) goto fail;

    int wrc = frame_write_cl(s->out_bc, sb.data, sb.len);
    sb_clean(&sb);
    if (id.kind == MCP_ID_STR) mcp_id_clean(&id);
    return wrc;

fail:
    sb_clean(&sb);
    if (id.kind == MCP_ID_STR) mcp_id_clean(&id);
    return ERR;
}

int mcpser_run(McpServer *s) {
    // This is the flow of McpServer:
    // handshake -> read JSON-RPC -> validate -> write to broker -> read from
    // broker -> write to out channel
    if (!s || !s->in_bc || !s->brok_bc || !s->out_bc) return ERR;

    int hrc = mcpser_handshake(s);
    if (hrc != OK) return ERR;
    TLOG("INFO - handshake complete, entering main loop");

    for (;;) {
        // McpServer reads JSON-RPC request
        StrBuf req = {0};
        int rc = frame_read_cl(s->in_bc, &req);
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

        JsonGetter jg;
        int irc = jsget_init(&jg, req.data, req.len);
        if (irc != OK) {
            fprintf(stderr, "McpServer: malformed input\n");
            sb_clean(&req);
            TLOG("ERROR - invalid JSON in MCP input");
            if (mcpser_send_error(s, NULL, -32600, "Malformed JSON-RPC request", NULL) != OK) {
                mcpser_set_err(s, "failed to write error response");
                return ERR;
            }
            continue;
        }

        McpId id = {0};
        const McpId *idp = NULL;
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
            } else if (src == NO) {
                // Notifications have no id; ignore them for now.
                sb_clean(&req);
                continue;
            } else {
                sb_clean(&req);
                TLOG("ERROR - invalid id in JSON-RPC request");
                if (mcpser_send_error(s, NULL, -32600,
                        "Invalid JSON-RPC request.", NULL) != OK) {
                    mcpser_set_err(s, "failed to write error response");
                    return ERR;
                }
                continue;
            }
        }

        int vrc = jsget_simple_rpc_validation(&jg);

        if (vrc != YES) {
            fprintf(stderr, "McpServer: invalid input\n");
            sb_clean(&req);
            TLOG("ERROR - invalid JSON-RPC envelope");
            if (mcpser_send_error(s, idp, -32600, "Invalid JSON-RPC request.", NULL) != OK) {
                mcpser_set_err(s, "failed to write error response");
                if (idp) mcp_id_clean(&id);
                return ERR;
            }
            if (idp) mcp_id_clean(&id);
            continue;
        }

        // overflow
        if (req.len > UINT32_MAX) {
            fprintf(stderr, "McpServer: request too large\n");
            sb_clean(&req);
            TLOG("ERROR - request too large: len=%zu", req.len);
            if (mcpser_send_error(s, idp, -32600, "Request too large.", NULL) != OK) {
                mcpser_set_err(s, "failed to write error response");
                if (idp) mcp_id_clean(&id);
                return ERR;
            }
            if (idp) mcp_id_clean(&id);
            continue;
        }

        // McpServer sends request to the broker
        //
        if (frame_write_len(s->brok_bc, req.data, (uint32_t)req.len) != OK) {
            fprintf(stderr, "McpServer: broker write failed\n");
            sb_clean(&req);
            TLOG("ERROR - failed to write request to broker");
            if (mcpser_send_error(s, idp, -32600, "Unable to reach broker.", NULL) != OK) {
                mcpser_set_err(s, "failed to write error response");
                if (idp) mcp_id_clean(&id);
                return ERR;
            }
            mcpser_set_err(s, "failed to write to broker");
            if (idp) mcp_id_clean(&id);
            return ERR;
        }
        sb_clean(&req);

        // McpServer reads broker response
        //
        StrBuf resp = {0};
        if (frame_read_len(s->brok_bc, &resp) != OK) {
            fprintf(stderr, "McpServer: broker read failed\n");
            sb_clean(&resp);
            TLOG("ERROR - failed to read response from broker");
            if (mcpser_send_error(s, idp, -32600, "Unable to read broker response.", NULL) != OK) {
                mcpser_set_err(s, "failed to write error response");
                if (idp) mcp_id_clean(&id);
                return ERR;
            }
            mcpser_set_err(s, "failed to read from broker");
            if (idp) mcp_id_clean(&id);
            return ERR;
        }

        // McpServer writes response to user
        if (frame_write_cl(s->out_bc, resp.data, resp.len) != OK) {
            fprintf(stderr, "McpServer: stdout write failed\n");
            sb_clean(&resp);
            TLOG("ERROR - failed to write response to stdout");
            mcpser_set_err(s, "failed to write to stdout");
            if (idp) mcp_id_clean(&id);
            return ERR;
        }

        sb_clean(&resp);
        if (idp) mcp_id_clean(&id);
    }
}

void mcpser_clean(McpServer *s) {
    if (!s) return;
    if (s->in_bc) {
        bufch_destroy(s->in_bc);
        s->in_bc = NULL;
    }
    if (s->brok_bc) {
        bufch_destroy(s->brok_bc);
        s->brok_bc = NULL;
    }
    if (s->out_bc) {
        bufch_destroy(s->out_bc);
        s->out_bc = NULL;
    }
    s->next_id = 0;
    s->last_err[0] = '\0';
}

const char *mcpser_last_error(const McpServer *s) {
    if (!s) return "McpServer is NULL";
    return s->last_err;
}
