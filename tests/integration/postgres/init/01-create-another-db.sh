#!/bin/sh
set -e

# Create another_user/another_db and seed it with gym.sql.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE USER "${ANOTHER_USER}" WITH PASSWORD '${ANOTHER_PASSWORD}';
CREATE DATABASE "${ANOTHER_DB}" OWNER "${ANOTHER_USER}";
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$ANOTHER_DB" \
  -f /docker-entrypoint-initdb.d/gym.sql
