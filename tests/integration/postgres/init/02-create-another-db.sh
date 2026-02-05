#!/bin/sh
set -e

# Create secondary database owned by another_user.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE DATABASE "${ANOTHER_DB}" OWNER "${POSTGRES_USER}";
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$ANOTHER_DB" <<SQL
CREATE ROLE "${ANOTHER_USER}" LOGIN PASSWORD '${ANOTHER_PASSWORD}';
SQL

# Seed another_db
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$ANOTHER_DB" \
  -f /seeds/gym.sql

# Give read-only permissions to another_user via mcp_readonly role
# created by 01-create-my-db.sh.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$ANOTHER_DB" <<SQL
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_readonly;
GRANT mcp_readonly TO "${ANOTHER_USER}";
SQL
