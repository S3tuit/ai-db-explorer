#!/bin/sh
set -e

# Create primary test database owned by the bootstrap superuser.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE DATABASE "${MY_DB}" OWNER "${POSTGRES_USER}";
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$MY_DB" <<SQL
CREATE ROLE "${MY_USER}" LOGIN PASSWORD '${MY_PASSWORD}';
SQL

# Seed primary test DB.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$MY_DB" \
  -f /seeds/zfighters.sql

# Create a read-only role and grant it to the bootstrap user.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$MY_DB" <<SQL
CREATE ROLE mcp_readonly NOLOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_readonly;

GRANT mcp_readonly TO "${MY_USER}";
SQL
