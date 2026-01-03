#include "command_reader.h"
#include "utils.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Discard the first 'n' bytes from the stash buffer.
static void stash_consume_prefix(CommandReader *r, size_t n) {
    if (n == 0 || !r) return;
    if (n >= r->stash.len) {
        sb_clean(&r->stash);
        return;
    }
    memmove(r->stash.data, r->stash.data + n, r->stash.len - n);
    r->stash.len -= n;
}

CommandReader *command_reader_create(ByteChannel *ch) {
    if (!ch) return NULL;
    BufChannel *bc = bufch_create(ch);
    if (!bc) {
        bytech_destroy(ch);
        return NULL;
    }
    CommandReader *r = (CommandReader *)xmalloc(sizeof(*r));
    r->bc = bc;
    r->stash.data = NULL;
    r->stash.len = 0;
    r->stash.cap = 0;
    return r;
}

void command_reader_destroy(CommandReader *r) {
    if (!r) return;

    sb_clean(&r->stash);
    if (r->bc) {
        bufch_destroy(r->bc);
        r->bc = NULL;
    }
    free(r);
}

/* Stores inside 'acc' all the bytes until, not including, a ';' that's not
 * enclosed in quotes. The remaining bytes of r->bc are appended to r->stash. */
static int command_reader_read_stmt(CommandReader *r, StrBuf *acc) {
    if (!r || !acc) return ERR;
    acc->len = 0;

    // 0 if we're outside any quotes, "\'" if inside single quotes, "\"" if
    // inside double quotes
    char quote_inside = 0;
    int terminator_found = 0;

    while (1) {
        const unsigned char *src = NULL;
        size_t src_len = 0;
        unsigned char *tmp = NULL;

        // we always put the line we read inside stash.
        // then, we remove the bytes up until the terminator and keep the
        // others to be consumed in the next iteration
        if (r->stash.len > 0) {
            src = (const unsigned char *)r->stash.data;
            src_len = r->stash.len;
        } else {
            size_t avail = 0;
            const uint8_t *peek = bufch_peek(r->bc, &avail);

            // no more bytes buffered, see if it's EOF or there are more bytes
            if (peek == NULL || avail == 0) {
                int rc = bufch_ensure(r->bc, 1);
                if (rc == ERR) return ERR;
                if (rc == NO) {
                    if (acc->len == 0) return NO;
                    return ERR;
                }
                continue;
            }

            tmp = (unsigned char *)xmalloc(avail);
            if (bufch_read_n(r->bc, tmp, avail) != OK) {
                free(tmp);
                return ERR;
            }
            src = tmp;
            src_len = avail;
        }

        size_t i = 0;
        while (i < src_len) {
            if (terminator_found) {
                // skip trailing whitespaces after terminating ';'
                while (i < src_len && isspace((unsigned char)src[i])) i++;
                break;
            }

            unsigned char c = src[i];

            if (quote_inside) {
                if (c == (unsigned char)quote_inside) {
                    // a double quote, inside the same quote, it treated as
                    // escaped. e.g. " "" " is still a single statement
                    if (i + 1 < src_len && src[i + 1] == (unsigned char)quote_inside) {
                        if (sb_append_bytes(acc, src + i, 2) != OK) {
                            free(tmp);
                            return ERR;
                        }
                        i += 2;
                        continue;
                    }
                    quote_inside = 0;
                }
                if (sb_append_bytes(acc, &c, 1) != OK) {
                    free(tmp);
                    return ERR;
                }
                i++;
                continue;
            }

            if (c == '\'' || c == '\"') {
                quote_inside = (char)c;
                if (sb_append_bytes(acc, &c, 1) != OK) {
                    free(tmp);
                    return ERR;
                }
                i++;
                continue;
            }

            // terminator detected outside quotes
            if (c == ';') {
                terminator_found = 1;
                i++;
                continue;
            }

            // normal char
            if (sb_append_bytes(acc, &c, 1) != OK) {
                free(tmp);
                return ERR;
            }
            i++;
        }

        if (r->stash.len > 0) {
            // remove up until, and including, terminator
            stash_consume_prefix(r, i);
        } else if (i < src_len) {
            // we have unconsumed bytes
            if (sb_append_bytes(&r->stash, src + i, src_len - i) != OK) {
                free(tmp);
                return ERR;
            }
        }

        free(tmp);

        if (terminator_found) return YES;
    }
}

/* Creates a CMD_SQL Command and allocates it inside 'out_cmd'. The 'raw_sql'
 * of the Command will be all the 'len' char starting from 'stmt'. */
static int command_reader_make_sql(const unsigned char *stmt, size_t len, Command **out_cmd) {
    Command *cmd = (Command *)xcalloc(1, sizeof(*cmd));
    cmd->type = CMD_SQL;
    cmd->raw_sql = (char *)xmalloc(len + 1);
    if (len > 0) memcpy(cmd->raw_sql, stmt, len);
    cmd->raw_sql[len] = '\0';
    *out_cmd = cmd;
    return YES;
}

/* Creates a CMD_META Commant and allocates it inside 'out_cmd' reading 'len'
 * bytes from 'stmt'. */
static int command_reader_make_meta(const unsigned char *stmt, size_t len, Command **out_cmd) {
    size_t i = 0;
    while (i < len && isspace((unsigned char)stmt[i])) i++;
    // there are just spaces after '\'
    if (i >= len) return ERR;

    size_t name_start = i;
    while (i < len && !isspace((unsigned char)stmt[i])) i++;
    size_t name_len = i - name_start;
    if (name_len == 0) return ERR;

    while (i < len && isspace((unsigned char)stmt[i])) i++;
    // everything from the first non space char after the command name is part
    // of the args
    size_t args_start = i;
    size_t args_len = (args_start < len) ? (len - args_start) : 0;

    Command *cmd = (Command *)xcalloc(1, sizeof(*cmd));
    cmd->type = CMD_META;
    cmd->cmd = (char *)xmalloc(name_len + 1);
    memcpy(cmd->cmd, stmt + name_start, name_len);
    cmd->cmd[name_len] = '\0';

    if (args_len > 0) {
        cmd->args = (char *)xmalloc(args_len + 1);
        memcpy(cmd->args, stmt + args_start, args_len);
        cmd->args[args_len] = '\0';
    }

    *out_cmd = cmd;
    return YES;
}

int command_reader_read_cmd(CommandReader *r, Command **out_cmd) {
    if (!r || !out_cmd) return ERR;
    *out_cmd = NULL;

    // we'll use this to store bytes as we consume
    StrBuf acc = {0};
    int rc = command_reader_read_stmt(r, &acc);
    if (rc != YES) {
        sb_clean(&acc);
        return rc;
    }

    size_t start = 0;
    // skip trailing spaces before the first non-space char
    while (start < acc.len && isspace((unsigned char)acc.data[start])) start++;

    if (start >= acc.len) {
        rc = command_reader_make_sql((const unsigned char *)"", 0, out_cmd);
        sb_clean(&acc);
        return rc;
    }

    // if the command starts with '\' then it's an internal command
    if ((unsigned char)acc.data[start] == '\\') {
        // we don't include the initial '\'
        rc = command_reader_make_meta((const unsigned char *)acc.data + start + 1,
                                      acc.len - start - 1, out_cmd);
    } else {
        rc = command_reader_make_sql((const unsigned char *)acc.data + start,
                                     acc.len - start, out_cmd);
    }

    sb_clean(&acc);
    return rc;
}

void command_destroy(Command *cmd) {
    if (!cmd) return;
    free(cmd->raw_sql);
    free(cmd->cmd);
    free(cmd->args);
    free(cmd);
}
