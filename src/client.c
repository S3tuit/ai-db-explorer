#define _GNU_SOURCE

#include "client.h"
#include "frame_codec.h"
#include "json_codec.h"
#include "query_result.h"
#include "stdio_byte_channel.h"
#include "utils.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
// TODO: when session_manager was alive we had test_tty_input.c. Get it back
// from github and adapt that test to the new architecture
static void client_set_err(Client *c, const char *msg) {
    if (!c) return;
    if (!msg) msg = "unknown error";
    snprintf(c->last_err, sizeof(c->last_err), "%s", msg);
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

/* Creates a QueryResult representing an error using 'id' and 'msg'. Writes it
 * to 'c'->out_bc. Returns ok/err. */
static int client_send_error(Client *c, uint32_t id, const char *msg) {
    if (!c || !c->out_bc) return ERR;
    QueryResult *qr = qr_create_err(id, msg ? msg : "");
    char *payload = NULL;
    size_t payload_len = 0;
    if (qr_to_jsonrpc(qr, &payload, &payload_len) != OK || !payload) {
        qr_destroy(qr);
        return ERR;
    }
    int rc = frame_write_cl(c->out_bc, payload, payload_len);
    free(payload);
    qr_destroy(qr);
    return rc;
}

int client_init(Client *c, FILE *in, FILE *out, const char *sock_path) {
    if (!c || !in || !out || !sock_path) return ERR;
    memset(c, 0, sizeof(*c));
    c->next_id = 1;
    c->last_err[0] = '\0';

    int in_fd = fileno(in);
    int out_fd = fileno(out);
    if (in_fd < 0 || out_fd < 0) return ERR;

    // The Client doesn't own the input file destriptors, so it won't cloes
    // them. That's because usually they're stdin and stdout
    ByteChannel *stdin_ch = stdio_bytechannel_wrap_fd(in_fd, -1);
    if (!stdin_ch) return ERR;
    c->r = command_reader_create(stdin_ch);
    if (!c->r) return ERR;

    ByteChannel *out_ch = stdio_bytechannel_wrap_fd(-1, out_fd);
    if (!out_ch) {
        command_reader_destroy(c->r);
        c->r = NULL;
        return ERR;
    }
    c->out_bc = bufch_create(out_ch);
    if (!c->out_bc) {
        bytech_destroy(out_ch);
        command_reader_destroy(c->r);
        c->r = NULL;
        return ERR;
    }

    // tries to connect to the Broker
    int brok_fd = connect_unix_socket(sock_path);
    if (brok_fd < 0) {
        client_clean(c);
        return ERR;
    }
    ByteChannel *brok_ch = stdio_bytechannel_open_fd(brok_fd, brok_fd);
    if (!brok_ch) {
        close(brok_fd);
        client_clean(c);
        return ERR;
    }
    c->brok_bc = bufch_create(brok_ch);
    if (!c->brok_bc) {
        bytech_destroy(brok_ch);
        client_clean(c);
        return ERR;
    }

    return OK;
}

int client_run(Client *c) {
    // This is the flow of Client:
    // read command -> encode into json -> write to broker -> read from broker
    // -> write to out channel
    if (!c || !c->r || !c->brok_bc || !c->out_bc) return ERR;

    for (;;) {
        // Clients reads command
        //
        Command *cmd = NULL;
        int rc = command_reader_read_cmd(c->r, &cmd);
        if (rc == NO) {
            // EOF
            return OK;
        }
        if (rc == ERR) {
            client_set_err(c, "command reader failed");
            return ERR;
        }

        uint32_t id = c->next_id++;
        char *payload = NULL;
        size_t payload_len = 0;

        if (command_to_jsonrpc(cmd, id, &payload, &payload_len) != OK || !payload) {
            fprintf(stderr, "Client: json encoding failed\n");
            if (client_send_error(c, id, "Unable to encode command.") != OK) {
                // fatal
                command_destroy(cmd);
                client_set_err(c, "failed to write error response");
                return ERR;
            }
            command_destroy(cmd);
            continue;
        }
        
        // overflow
        if (payload_len > UINT32_MAX) {
            fprintf(stderr, "Client: request too large\n");
            free(payload);
            command_destroy(cmd);
            if (client_send_error(c, id, "Request too large.") != OK) {
                client_set_err(c, "failed to write error response");
                return ERR;
            }
            continue;
        }

        // Client sends command to the broker
        //
        if (frame_write_len(c->brok_bc, payload, (uint32_t)payload_len) != OK) {
            fprintf(stderr, "Client: broker write failed\n");
            free(payload);
            command_destroy(cmd);
            if (client_send_error(c, id, "Unable to reach broker.") != OK) {
                client_set_err(c, "failed to write error response");
                return ERR;
            }
            client_set_err(c, "failed to write to broker");
            return ERR;
        }
        free(payload);
        command_destroy(cmd);

        // Client read broker response
        //
        StrBuf resp = {0};
        if (frame_read_len(c->brok_bc, &resp) != OK) {
            fprintf(stderr, "Client: broker read failed\n");
            sb_clean(&resp);
            if (client_send_error(c, id, "Unable to read broker response.") != OK) {
                client_set_err(c, "failed to write error response");
                return ERR;
            }
            client_set_err(c, "failed to read from broker");
            return ERR;
        }

        // Client writes response to user
        if (frame_write_cl(c->out_bc, resp.data, resp.len) != OK) {
            fprintf(stderr, "Client: stdout write failed\n");
            sb_clean(&resp);
            client_set_err(c, "failed to write to stdout");
            return ERR;
        }

        sb_clean(&resp);
    }
}

void client_clean(Client *c) {
    if (!c) return;
    if (c->r) {
        command_reader_destroy(c->r);
        c->r = NULL;
    }
    if (c->brok_bc) {
        bufch_destroy(c->brok_bc);
        c->brok_bc = NULL;
    }
    if (c->out_bc) {
        bufch_destroy(c->out_bc);
        c->out_bc = NULL;
    }
    c->next_id = 0;
    c->last_err[0] = '\0';
}

const char *client_last_error(const Client *c) {
    if (!c) return "Client is NULL";
    return c->last_err;
}
