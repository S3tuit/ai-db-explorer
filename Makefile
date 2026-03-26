CC      := gcc
PKG_CONFIG ?= pkg-config
UNAME_S := $(shell uname -s)
LIBPQ_CFLAGS := $(shell $(PKG_CONFIG) --cflags libpq 2>/dev/null)
LIBPQ_LIBS   := $(shell $(PKG_CONFIG) --libs   libpq 2>/dev/null)
LIBSECRET_CFLAGS := $(shell $(PKG_CONFIG) --cflags libsecret-1 2>/dev/null)
LIBSECRET_LIBS   := $(shell $(PKG_CONFIG) --libs   libsecret-1 2>/dev/null)

# Third party flags, these are built separately to allow docker cache in
# integration tests
LIBPG_QUERY_DIR := third_party/libpg_query
LIBPG_QUERY_LIB := $(LIBPG_QUERY_DIR)/libpg_query.a
LIBPG_QUERY_INC := -I$(LIBPG_QUERY_DIR)

# Build flags
CFLAGS  := -Wall -Wextra -Werror -Wenum-conversion -std=c11 -g -O2 -flto=auto
CFLAGS  += -D_POSIX_C_SOURCE=200809L
CFLAGS  += -DNDEBUG
ifneq ($(strip $(LIBSECRET_LIBS)),)
CFLAGS += -DHAVE_LIBSECRET
endif
INCLUDES := -Isrc -Itests/unit $(LIBPQ_CFLAGS) $(LIBSECRET_CFLAGS) $(LIBPG_QUERY_INC)
LDFLAGS := $(LIBPQ_LIBS) $(LIBSECRET_LIBS) $(LIBPG_QUERY_LIB)
ifeq ($(UNAME_S),Darwin)
LDFLAGS += -framework Security -framework CoreFoundation
endif

# Benchmark flags (optimized, no sanitizers)
BENCH_CFLAGS := -Wall -Wextra -Werror -std=c11 -O3 -DNDEBUG -flto \
                -D_POSIX_C_SOURCE=200809L -Isrc

# Test flags
EXTRA_TCFLAGS ?= -DADBX_TEST_MODE
TCFLAGS = -Wall -Wextra -Werror -Wenum-conversion -std=c11 -g -O1 $(INCLUDES) \
          -D_POSIX_C_SOURCE=200809L $(EXTRA_TCFLAGS)
TSAN    := -fsanitize=address,undefined -fno-omit-frame-pointer
TLDFLAGS := $(TSAN) $(LDFLAGS) $(PIE_LDFLAGS)
ASAN_RUN_OPTS ?= detect_leaks=1:abort_on_error=1:halt_on_error=1:fast_unwind_on_malloc=0

# App sources (exclude main.c for reuse in tests)
APP_MAIN := src/main.c
APP_SRC  := $(filter-out $(APP_MAIN),$(wildcard src/*.c))

# App bin
APP_OBJ := $(APP_SRC:src/%.c=build/%.o) build/main.o
BIN := build/adbxplorer
ASAN_BIN := build/adbxplorer-asan
PG_DUMP_AST_BIN := build/pg_dump_ast
PG_DUMP_AST_SRC := py_utils/pg_dump_ast.c

# Unit tests: each tests/unit/test_foo.c -> build/tests/unit/test_foo
UNIT_TEST_SRC := $(filter-out tests/unit/test_env.c tests/unit/test_broker_run_utils.c,$(wildcard tests/unit/test_*.c))
UNIT_TEST_BINS := $(patsubst tests/unit/%.c,build/tests/unit/%,$(UNIT_TEST_SRC))
TEST_HELPER_OBJ := build/tests/unit/test.o
TEST_ENV_HELPER_OBJ := build/tests/unit/test_env.o
TEST_BROKER_RUN_UTILS_OBJ := build/tests/unit/test_broker_run_utils.o

# Integration tests: tests/integration/*/test_foo.c -> build/tests/integration/*/test_foo
INTEGRATION_TEST_SRC := $(wildcard tests/integration/*/test_*.c)
INTEGRATION_TEST_BINS := $(patsubst tests/integration/%.c,build/tests/integration/%,$(INTEGRATION_TEST_SRC))
POSTGRES_SEED_BIN := build/tests/integration/postgres/seed_file_secret_store

# Benchmarks: each benchmarks/bench_foo.c -> build/benchmarks/bench_foo
BENCH_SRC := $(wildcard benchmarks/bench_*.c)
BENCH_BINS := $(patsubst benchmarks/%.c,build/benchmarks/%,$(BENCH_SRC))
BENCH_COMMON_SRC := src/arena.c src/utils.c

.PHONY: all clean run test test-unit test-unit-notty test-integration docker-test-postgres test-build compdb asan clean-testobj pg-dump-ast bench gen-tools

all: $(BIN)

gen-tools:
	python3 py_utils/gen_tool_artifacts.py

# Build vendored libpg_query (static).
$(LIBPG_QUERY_LIB):
	@$(MAKE) -C $(LIBPG_QUERY_DIR)

# Build app objects
build/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Link app binary
$(BIN): $(APP_OBJ) $(LIBPG_QUERY_LIB)
	@mkdir -p $(dir $@)
	$(CC) $(APP_OBJ) -o $@ $(LDFLAGS)

run: $(BIN)
	./$(BIN) $(RUN_ARGS)

# Build AST dumper used by py_utils/pg_dump_ast.py
$(PG_DUMP_AST_BIN): $(PG_DUMP_AST_SRC) $(LIBPG_QUERY_LIB)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(LIBPG_QUERY_INC) $< -o $@ $(LIBPG_QUERY_LIB)

pg-dump-ast: $(PG_DUMP_AST_BIN)

# Build one benchmark binary with shared benchmark sources.
build/benchmarks/%: benchmarks/%.c $(BENCH_COMMON_SRC)
	@mkdir -p $(dir $@)
	$(CC) $(BENCH_CFLAGS) $< $(BENCH_COMMON_SRC) -o $@

# Build and run all benchmarks in benchmarks/.
bench: $(BENCH_BINS)
	@set -e; \
	for b in $(BENCH_BINS); do \
	  echo "==> $$b"; \
	  $$b; \
	done

# ASAN-instrumented app binary for debugging, used inside integration tests.
ASAN_CFLAGS = $(TCFLAGS) $(TSAN)
ASAN_OBJ := $(APP_SRC:src/%.c=build/asan/%.o) build/asan/main.o

build/asan/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(ASAN_CFLAGS) -c $< -o $@

$(ASAN_BIN): $(ASAN_OBJ) $(LIBPG_QUERY_LIB)
	@mkdir -p $(dir $@)
	$(CC) $(ASAN_OBJ) -o $@ $(TLDFLAGS)

asan: $(ASAN_BIN)

# --- Tests build rules ---
# Compile each test source to an object with sanitizers enabled
build/tests/%.o: tests/%.c
	@mkdir -p $(dir $@)
	$(CC) $(TCFLAGS) $(TSAN) -c $< -o $@

# Compile src objects for tests (sanitized), but exclude main.c
build/testobj/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(TCFLAGS) $(TSAN) -c $< -o $@

TEST_APP_OBJ := $(APP_SRC:src/%.c=build/testobj/%.o)

# Narrow object slices for secret-store tests so they don't drag libpq/libpg_query
# into containers that only need the secret-store backends.
TEST_CORE_OBJ := \
	build/testobj/arena.o \
	build/testobj/config_dir.o \
	build/testobj/file_io.o \
	build/testobj/hash_table.o \
	build/testobj/json_codec.o \
	build/testobj/jsmn.o \
	build/testobj/mcp_id.o \
	build/testobj/packed_array.o \
	build/testobj/query_result.o \
	build/testobj/sensitive_tok.o \
	build/testobj/spool.o \
	build/testobj/string_op.o \
	build/testobj/utils.o

TEST_SECRET_STORE_OBJ := \
	$(TEST_CORE_OBJ) \
	build/testobj/secret_store.o \
	build/testobj/secret_store_file.o \
	build/testobj/secret_store_keychain.o \
	build/testobj/secret_store_libsecret.o

SECRET_STORE_TEST_LDFLAGS := $(TSAN) $(LIBSECRET_LIBS) $(PIE_LDFLAGS)
ifeq ($(UNAME_S),Darwin)
SECRET_STORE_TEST_LDFLAGS += -framework Security -framework CoreFoundation
endif

# Link each test binary from its test object + shared test helper + sanitized app objects
build/tests/%: build/tests/%.o $(TEST_HELPER_OBJ) $(TEST_ENV_HELPER_OBJ) $(TEST_APP_OBJ) $(LIBPG_QUERY_LIB)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(TLDFLAGS)

build/tests/unit/test_broker_tool_calls: build/tests/unit/test_broker_tool_calls.o $(TEST_BROKER_RUN_UTILS_OBJ) $(TEST_HELPER_OBJ) $(TEST_ENV_HELPER_OBJ) $(TEST_APP_OBJ) $(LIBPG_QUERY_LIB)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(TLDFLAGS)

# Secret-store tests use only the secret-store slice and provide their own
# lightweight helpers, so they do not need test.o, libpq, or libpg_query.
build/tests/unit/test_secret_store_contract: build/tests/unit/test_secret_store_contract.o $(TEST_ENV_HELPER_OBJ) $(TEST_SECRET_STORE_OBJ)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(SECRET_STORE_TEST_LDFLAGS)

build/tests/unit/test_secret_store_factory: build/tests/unit/test_secret_store_factory.o $(TEST_ENV_HELPER_OBJ) $(TEST_SECRET_STORE_OBJ)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(SECRET_STORE_TEST_LDFLAGS)

build/tests/unit/test_secret_store_file: build/tests/unit/test_secret_store_file.o $(TEST_ENV_HELPER_OBJ) $(TEST_SECRET_STORE_OBJ)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(SECRET_STORE_TEST_LDFLAGS)

# Postgres integration seeding uses only the file secret-store backend and the
# core filesystem/config helpers. It must not pull libpq/libpg_query or the
# full app object set into the container.
$(POSTGRES_SEED_BIN): build/tests/integration/postgres/seed_file_secret_store.o $(TEST_CORE_OBJ) build/testobj/secret_store_file.o
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(TSAN) $(PIE_LDFLAGS)

# Run unit tests
test-unit: EXTRA_TCFLAGS=-DADBX_TEST_MODE
test-unit: $(UNIT_TEST_BINS)
	@set -e; \
	for t in $(UNIT_TEST_BINS); do \
	  echo "==> $$t"; \
	  ASAN_OPTIONS=$(ASAN_RUN_OPTS) $$t; \
	done; \
	echo "ALL TESTS PASSED"

# Some of our unit tests assume to run inside a non tty env
test-unit-notty:
	+@setsid -w $(MAKE) test-unit

# Integration compose file used by docker targets.
DOCKER_POSTGRES_COMPOSE := tests/integration/postgres/postgres.test.yml
DOCKER_LIBSECRET_COMPOSE := tests/integration/libsecret/libsecret.test.yml

test-integration-postgres:
	@set -e; \
	trap 'docker compose -f $(DOCKER_POSTGRES_COMPOSE) down -v' EXIT; \
	docker compose -f $(DOCKER_POSTGRES_COMPOSE) up --build --abort-on-container-exit --exit-code-from test

test-integration-libsecret:
	@set -e; \
	trap 'docker compose -f $(DOCKER_LIBSECRET_COMPOSE) down -v' EXIT; \
	docker compose -f $(DOCKER_LIBSECRET_COMPOSE) up --build --abort-on-container-exit --exit-code-from test

# Run integration tests (docker) and always rebuild the image.
# This ensures the container sees the current working tree state.
test-integration: test-integration-postgres test-integration-libsecret

# Run all tests
test: test-unit test-integration

# Run postgres integration tests (used by docker) and run the .py tests.
# We always use the ASAN binary for tests.
docker-test-postgres: EXTRA_TCFLAGS=-DADBX_TEST_MODE
docker-test-postgres: clean-testobj $(INTEGRATION_TEST_BINS) $(ASAN_BIN)
# We use a symlink so the integration tests always run the ASAN binary.
# We use set -e so failures in .py tests stop the target.
	@set -e; \
	$(MAKE) $(POSTGRES_SEED_BIN); \
	ASAN_OPTIONS=$(ASAN_RUN_OPTS) $(POSTGRES_SEED_BIN); \
	ln -sf adbxplorer-asan build/adbxplorer; \
	for t in $(INTEGRATION_TEST_BINS); do \
	  echo "==> $$t"; \
	  ASAN_OPTIONS=$(ASAN_RUN_OPTS) $$t; \
	done; \
	for t in tests/integration/postgres/*.py; do \
	  echo "==> $$t"; \
	  python3 $$t; \
	done; \
	echo "ALL INTEGRATION TESTS PASSED"

# Only builds tests, usefull for making the LSP recognize the header files
# inside tests/
test-build: $(UNIT_TEST_BINS) $(INTEGRATION_TEST_BINS)

# Rebuild compile_commands.json for this checkout so ccls/clangd see the real
# compiler flags and include paths used by the project sources.
compdb:
	rm -f compile_commands.json
	bear -- $(MAKE) -j6 -B build/adbxplorer

clean-testobj:
	# Force rebuild of sanitized objects.
	# We don't generate header dependency files today, so after API/layout
	# changes in headers (e.g. struct fields), stale .o files can survive and
	# cause hard-to-debug ASAN crashes from ABI mismatches.
	rm -f build/testobj/*.o
	rm -f build/asan/*.o

clean:
	rm -rf build
