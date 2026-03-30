# Acknowledgements

`adbxplorer` uses and benefits from several upstream projects.

This file is intentionally informal. It is here to say thank you, to make the
upstream dependencies visible to users, and to point readers at the relevant
code in this repository.

## Included Upstream Projects

### libpg_query

`adbxplorer` uses `libpg_query` for PostgreSQL parsing support.

- Upstream: https://github.com/pganalyze/libpg_query
- Vendored here: `third_party/libpg_query/`
- License text in this repository: `third_party/libpg_query/LICENSE`

Thanks to the `libpg_query` maintainers and contributors for making the
PostgreSQL parser available as a reusable library. Without it, `adbxplorer`
wouldn't have been able to claim to never expose sensitive data.

### jsmn

`adbxplorer` uses `jsmn` as a minimal JSON parser.

- Upstream: https://github.com/zserge/jsmn
- Vendored here: `src/jsmn.h` and `src/jsmn.c`
- License header in this repository: `src/jsmn.h`

Thanks to Serge Zaitsev and the `jsmn` contributors for making a simple and
fast json parser. 

### rapidhash

`adbxplorer` uses `rapidhash` for fast hashing in internal code paths.

- Upstream: https://github.com/Nicoshev/rapidhash
- Vendored here: `src/rapidhash.h`
- License header in this repository: `src/rapidhash.h`

Thanks to Nicolas De Carli and the upstream `rapidhash` project for making an
hashtable possible in this project.

## Notes

This file is only an acknowledgement and attribution aid.

For actual license terms:

- files authored for `adbxplorer` are covered by the repository `LICENSE`
- `third_party/libpg_query/` remains under its own upstream license
- `jsmn` and `rapidhash` retain the license notices embedded in their vendored
  source files
