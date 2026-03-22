## list_database_connections
Tool definition aligned with MCP `2025-11-25`. The `inputSchema` and
`outputSchema` documents below explicitly target JSON Schema 2020-12:

```json
{
  "name": "list_database_connections",
  "title": "List Database Connections",
  "description": "List configured database connections together with the effective broker safety metadata exposed to the agent for each one.",
  "inputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": false
  },
  "outputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
      "connections": {
        "type": "array",
        "description": "Configured database connections available for other tool calls.",
        "items": {
          "type": "object",
          "properties": {
            "connectionName": {
              "type": "string",
              "minLength": 1,
              "description": "Stable connection identifier used by tools such as run_sql_query."
            },
            "type": {
              "type": "string",
              "enum": ["postgres"],
              "description": "Database backend type."
            },
            "readOnly": {
              "type": "boolean",
              "description": "True when the effective safety policy enforces read-only access for this connection."
            }
          },
          "required": ["connectionName", "type", "readOnly"],
          "additionalProperties": false
        }
      }
    },
    "required": ["connections"],
    "additionalProperties": false
  }
}
```

## describe_relation

Tool definition aligned with MCP `2025-11-25`. The `inputSchema` and
`outputSchema` documents below explicitly target JSON Schema 2020-12:

```json
{
  "name": "describe_relation",
  "title": "Describe Relation",
  "description": "Describe one database relation available on a configured connection. Relations include base tables, views, materialized views, and foreign tables.",
  "inputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
      "connectionName": {
        "type": "string",
        "minLength": 1,
        "description": "Database connection name, specified inside list_database_connections, to use."
      },
      "schemaName": {
        "type": "string",
        "minLength": 1,
        "description": "Schema that owns the relation."
      },
      "relationName": {
        "type": "string",
        "minLength": 1,
        "description": "Unqualified relation name."
      }
    },
    "required": ["connectionName", "schemaName", "relationName"],
    "additionalProperties": false
  },
  "outputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
      "schemaName": {
        "type": "string",
        "minLength": 1,
        "description": "Schema that owns the described relation."
      },
      "relationName": {
        "type": "string",
        "minLength": 1,
        "description": "Unqualified relation name."
      },
      "relationKind": {
        "type": "string",
        "enum": ["table", "view", "materialized_view", "foreign_table"],
        "description": "Backend relation kind."
      },
      "columns": {
        "type": "array",
        "description": "Columns in physical relation order. The broker computes sensitivity metadata centrally.",
        "items": {
          "type": "object",
          "properties": {
            "name": {
              "type": "string",
              "minLength": 1,
              "description": "Column name."
            },
            "type": {
              "type": "string",
              "minLength": 1,
              "description": "Human-readable database type name."
            },
            "sensitive": {
              "type": "boolean",
              "description": "True when the broker safety policy marks this column as sensitive."
            },
            "isPrimaryKey": {
              "type": "boolean",
              "description": "True when the column participates in the relation primary key."
            },
            "isForeignKey": {
              "type": "boolean",
              "description": "True when the column participates in at least one foreign-key constraint."
            },
            "references": {
              "type": ["object", "null"],
              "description": "Referenced target for a foreign key, or null when the column is not a foreign key. When one column participates in multiple foreign-key constraints, returns only the first referenced target.",
              "properties": {
                "schemaName": {
                  "type": "string",
                  "minLength": 1,
                  "description": "Schema that owns the referenced relation."
                },
                "relationName": {
                  "type": "string",
                  "minLength": 1,
                  "description": "Referenced relation name."
                },
                "columnName": {
                  "type": "string",
                  "minLength": 1,
                  "description": "Referenced column name."
                }
              },
              "required": ["schemaName", "relationName", "columnName"],
              "additionalProperties": false
            }
          },
          "required": [
            "name",
            "type",
            "sensitive",
            "isPrimaryKey",
            "isForeignKey",
            "references"
          ],
          "additionalProperties": false,
          "allOf": [
            {
              "if": {
                "properties": {
                  "isForeignKey": {
                    "const": true
                  }
                },
                "required": ["isForeignKey"]
              },
              "then": {
                "properties": {
                  "references": {
                    "type": "object"
                  }
                }
              },
              "else": {
                "properties": {
                  "references": {
                    "type": "null"
                  }
                }
              }
            }
          ]
        }
      }
    },
    "required": ["schemaName", "relationName", "relationKind", "columns"],
    "additionalProperties": false
  }
}
```

## run_sql_query

Tool definition aligned with MCP `2025-11-25`. The `inputSchema` and
`outputSchema` documents below explicitly target JSON Schema 2020-12:

```json
{
  "name": "run_sql_query",
  "title": "Run SQL Query",
  "description": "Execute a read-only SQL query against a configured database connection.",
  "inputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
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
    "$schema": "https://json-schema.org/draft/2020-12/schema",
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

Future tool definition aligned with MCP `2025-11-25`. The `inputSchema` and
`outputSchema` documents below explicitly target JSON Schema 2020-12:

```json
{
  "name": "run_sql_query_tokens",
  "title": "Run SQL Query With Parameters",
  "description": "Execute a read-only SQL query against a configured database connection using positional text parameters (for example PostgreSQL $1, $2).",
  "inputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
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
    "$schema": "https://json-schema.org/draft/2020-12/schema",
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
