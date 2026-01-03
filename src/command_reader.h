#ifndef COMMAND_READER_H
#define COMMAND_READER_H

#include <stddef.h>

#include "bufio.h"
#include "string_op.h"

/* This entity reads statements from a buffered byte stream. */
typedef struct CommandReader {
    BufChannel *bc;  // owned
    StrBuf stash;   // unconsumed bytes from previous reads (after ';')
} CommandReader;

typedef enum {
    CMD_SQL = 1,        // user wants to run a query
    CMD_META = 2        // user wants to run one of our internal commands
} CommandType;

// Entity that will be sent to the Broker to do something
typedef struct {
    CommandType type;
    // Owned by this entity.
    char *raw_sql;      // Valid if CMD_SQL. Statement without trailing ';'.
    char *cmd;          // Valid if CMD_META. Name of the internal command.
    char *args;         // Valid if CMD_META. Raw arguments (may be NULL).
} Command;

/* Creates the reader and takes ownership of 'ch' (via BufChannel). */
CommandReader *command_reader_create(ByteChannel *ch);

/* Frees internal buffers and owned BufChannel/ByteChannel. */
void command_reader_destroy(CommandReader *r);

/* Reads next statement from its stream. A statement ends when a semicolon ';'
 * is found that is not inside:
 *  - single quotes
 *  - double quotes
 *
 * Any non-whitespace bytes after the semicolon stay in the stash for the next
 * iteration, which allows multiple statements on the same line.
 *
 * Returns:
 *  YES -> success, *out_cmd is malloc'd.
 *  NO  -> clean EOF with no pending statement.
 *  ERR -> error (malloc/read). */
int command_reader_read_cmd(CommandReader *r, Command **out_cmd);

/* Frees a Command and its owned fields. */
void command_destroy(Command *cmd);

#endif
