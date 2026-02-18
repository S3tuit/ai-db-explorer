## list_database_connections
TBD

## run_sql_query

Tool definition aligned with MCP `2025-11-25`:

```json
{
  "name": "run_sql_query",
  "title": "Run SQL Query",
  "description": "Execute a read-only SQL query against a configured database connection.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "connectionName": {
        "type": "string",
        "minLength": 1,
        "description": "Database connection name, specified inside list_database_connections, to use (for example: MyPostgres)."
      },
      "query": {
        "type": "string",
        "minLength": 1,
        "description": "SQL statement to validate and execute under broker safety policy."
      }
    },
    "required": ["connectionName", "query"],
    "additionalProperties": false
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "exec_ms": {
        "type": "integer",
        "minimum": 0,
        "description": "Execution time in milliseconds."
      },
      "columns": {
        "type": "array",
        "description": "Result-set columns in order.",
        "items": {
          "type": "object",
          "properties": {
            "name": {
              "type": "string",
              "description": "Column name."
            },
            "type": {
              "type": "string",
              // WARN: RIGHT NOW WE ACTUALLY PUT POSTGRES OID IN HERE
              "description": "Database type name (for example: int4, text, date)."
            }
          },
          "required": ["name", "type"],
          "additionalProperties": false
        }
      },
      "rows": {
        "type": "array",
        "description": "Tabular rows; each cell is stringified DB output or null.",
        "items": {
          "type": "array",
          "items": {
            "type": ["string", "null"]
          }
        }
      },
      "rowcount": {
        "type": "integer",
        "minimum": 0,
        "description": "Number of rows returned in this response."
      },
      "resultTruncated": {
        "type": "boolean",
        "description": "True when row/cell output was truncated by safety limits."
      },
      "warnings": {
        "type": "array",
        "description": "Optional non-fatal warnings emitted while preparing the response.",
        "items": {
          "type": "string"
        }
      }
    },
    "required": [
      "exec_ms",
      "columns",
      "rows",
      "rowcount",
      "resultTruncated"
    ],
    "additionalProperties": false
  }
}
```

## run_sql_query_tokens

Future tool definition aligned with MCP `2025-11-25`:

```json
{
  "name": "run_sql_query_tokens",
  "title": "Run SQL Query With Parameters",
  "description": "Execute a read-only SQL query against a configured database connection using positional text parameters (for example PostgreSQL $1, $2).",
  "inputSchema": {
    "type": "object",
    "properties": {
      "connectionName": {
        "type": "string",
        "minLength": 1,
        "description": "Database connection name, specified inside list_database_connections, to use (for example: MyPostgres)."
      },
      "query": {
        "type": "string",
        "minLength": 1,
        "description": "SQL statement that may contain database-specific positional parameters."
      },
      "parameters": {
        "type": "array",
        "description": "Positional parameter values for query placeholders. Values must be tokens returned by the server and are interpreted by the backend.",
        "maxItems": 10,
        "items": {
          "type": "string"
        }
      }
    },
    "required": ["connectionName", "query", "parameters"],
    "additionalProperties": false
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "exec_ms": {
        "type": "integer",
        "minimum": 0,
        "description": "Execution time in milliseconds."
      },
      "columns": {
        "type": "array",
        "description": "Result-set columns in order.",
        "items": {
          "type": "object",
          "properties": {
            "name": {
              "type": "string",
              "description": "Column name."
            },
            "type": {
              "type": "string",
              // WARN: RIGHT NOW WE ACTUALLY PUT POSTGRES OID IN HERE
              "description": "Database type name (for example: int4, text, date)."
            }
          },
          "required": ["name", "type"],
          "additionalProperties": false
        }
      },
      "rows": {
        "type": "array",
        "description": "Tabular rows; each cell is stringified DB output or null.",
        "items": {
          "type": "array",
          "items": {
            "type": ["string", "null"]
          }
        }
      },
      "rowcount": {
        "type": "integer",
        "minimum": 0,
        "description": "Number of rows returned in this response."
      },
      "resultTruncated": {
        "type": "boolean",
        "description": "True when row/cell output was truncated by safety limits."
      },
      "warnings": {
        "type": "array",
        "description": "Optional non-fatal warnings emitted while preparing the response.",
        "items": {
          "type": "string"
        }
      }
    },
    "required": [
      "exec_ms",
      "columns",
      "rows",
      "rowcount",
      "resultTruncated"
    ],
    "additionalProperties": false
  }
}
```
