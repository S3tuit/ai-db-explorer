CC      := gcc
PKG_CONFIG ?= pkg-config
LIBPQ_CFLAGS := $(shell $(PKG_CONFIG) --cflags libpq 2>/dev/null)
LIBPQ_LIBS   := $(shell $(PKG_CONFIG) --libs   libpq 2>/dev/null)

# Third party flags, these are built separately to allow docker cache in
# integration tests
LIBPG_QUERY_DIR := third_party/libpg_query
LIBPG_QUERY_LIB := $(LIBPG_QUERY_DIR)/libpg_query.a
LIBPG_QUERY_INC := -I$(LIBPG_QUERY_DIR)

# Build flags
CFLAGS  := -Wall -Wextra -Werror -std=c11 -g -O2
CFLAGS  += -D_POSIX_C_SOURCE=200809L
CFLAGS  += -DNDEBUG
INCLUDES := -Isrc -Itests/unit $(LIBPQ_CFLAGS) $(LIBPG_QUERY_INC)
LDFLAGS := $(LIBPQ_LIBS) $(LIBPG_QUERY_LIB)

# Test flags
EXTRA_TCFLAGS ?=
TCFLAGS = -Wall -Wextra -Werror -std=c11 -g -O1 $(INCLUDES) \
          -D_POSIX_C_SOURCE=200809L $(EXTRA_TCFLAGS)
TSAN    := -fsanitize=address,undefined -fno-omit-frame-pointer
TLDFLAGS := $(TSAN) $(LDFLAGS) $(PIE_LDFLAGS)
ASAN_RUN_OPTS ?= detect_leaks=1:abort_on_error=1:halt_on_error=1:fast_unwind_on_malloc=0

# App sources (exclude main.c for reuse in tests)
APP_MAIN := src/main.c
APP_SRC  := $(filter-out $(APP_MAIN),$(wildcard src/*.c))

# App bin
APP_OBJ := $(APP_SRC:src/%.c=build/%.o) build/main.o
BIN := build/ai-db-explorer
ASAN_BIN := build/ai-db-explorer-asan
PG_DUMP_AST_BIN := build/pg_dump_ast
PG_DUMP_AST_SRC := py_utils/pg_dump_ast.c

# Unit tests: each tests/unit/test_foo.c -> build/tests/unit/test_foo
UNIT_TEST_SRC := $(wildcard tests/unit/test_*.c)
UNIT_TEST_BINS := $(patsubst tests/unit/%.c,build/tests/unit/%,$(UNIT_TEST_SRC))
TEST_HELPER_OBJ := build/tests/unit/test.o

# Integration tests: tests/integration/*/test_foo.c -> build/tests/integration/*/test_foo
INTEGRATION_TEST_SRC := $(wildcard tests/integration/*/test_*.c)
INTEGRATION_TEST_BINS := $(patsubst tests/integration/%.c,build/tests/integration/%,$(INTEGRATION_TEST_SRC))

.PHONY: all clean run test test-unit test-integration test-integration-cached test-postgres test-build asan clean-testobj pg-dump-ast

all: $(BIN)

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

# Link each test binary from its test object + shared test helper + sanitized app objects
build/tests/%: build/tests/%.o $(TEST_HELPER_OBJ) $(TEST_APP_OBJ) $(LIBPG_QUERY_LIB)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(TLDFLAGS)

# Run unit tests
test-unit: EXTRA_TCFLAGS=-DADBX_TEST_MODE
test-unit: $(UNIT_TEST_BINS)
	@set -e; \
	for t in $(UNIT_TEST_BINS); do \
	  echo "==> $$t"; \
	  ASAN_OPTIONS=$(ASAN_RUN_OPTS) $$t; \
	done; \
	echo "ALL TESTS PASSED"

# Integration compose file used by docker targets.
DOCKER_POSTGRES_COMPOSE := tests/integration/postgres/postgres.test.yml

# Run integration tests (docker) and always rebuild the image.
# This ensures the container sees the current working tree state.
test-integration:
	@set -e; \
	trap 'docker compose -f $(DOCKER_POSTGRES_COMPOSE) down -v' EXIT; \
	docker compose -f $(DOCKER_POSTGRES_COMPOSE) up --build --abort-on-container-exit --exit-code-from test

# Run integration tests (docker) using cached images (no rebuild).
# Use this only when you know the image is already up-to-date.
test-integration-cached:
	@set -e; \
	trap 'docker compose -f $(DOCKER_POSTGRES_COMPOSE) down -v' EXIT; \
	docker compose -f $(DOCKER_POSTGRES_COMPOSE) up --abort-on-container-exit --exit-code-from test

# Run all tests
test: test-unit test-integration

# Run postgres integration tests (used by docker) and run the .py tests.
# We always use the ASAN binary for tests.
test-postgres: EXTRA_TCFLAGS=-DADBX_TEST_MODE -DDUMMY_SECRET_STORE_WARNING
test-postgres: clean-testobj $(INTEGRATION_TEST_BINS) $(ASAN_BIN)
# We use a symlink so the integration tests always run the ASAN binary.
# We use set -e so failures in .py tests stop the target.
	@set -e; \
	ln -sf ai-db-explorer-asan build/ai-db-explorer; \
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

clean-testobj:
	# Force rebuild of sanitized objects.
	# We don't generate header dependency files today, so after API/layout
	# changes in headers (e.g. struct fields), stale .o files can survive and
	# cause hard-to-debug ASAN crashes from ABI mismatches.
	rm -f build/testobj/*.o
	rm -f build/asan/*.o

clean:
	rm -rf build
