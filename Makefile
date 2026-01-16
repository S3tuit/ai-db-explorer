CC      := gcc
PKG_CONFIG ?= pkg-config
LIBPQ_CFLAGS := $(shell $(PKG_CONFIG) --cflags libpq 2>/dev/null)
LIBPQ_LIBS   := $(shell $(PKG_CONFIG) --libs   libpq 2>/dev/null)

# Build flags
CFLAGS  := -Wall -Wextra -Werror -std=c11 -g -O2
CFLAGS  += -D_POSIX_C_SOURCE=200809L
INCLUDES := -Isrc -Itests/unit $(LIBPQ_CFLAGS)
LDFLAGS := $(LIBPQ_LIBS)

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

# Unit tests: each tests/unit/test_foo.c -> build/tests/unit/test_foo
UNIT_TEST_SRC := $(wildcard tests/unit/test_*.c)
UNIT_TEST_BINS := $(patsubst tests/unit/%.c,build/tests/unit/%,$(UNIT_TEST_SRC))

# Integration tests: tests/integration/*/test_foo.c -> build/tests/integration/*/test_foo
INTEGRATION_TEST_SRC := $(wildcard tests/integration/*/test_*.c)
INTEGRATION_TEST_BINS := $(patsubst tests/integration/%.c,build/tests/integration/%,$(INTEGRATION_TEST_SRC))

.PHONY: all clean run test test-unit test-integration test-postgres test-build asan clean-testobj

all: $(BIN)

# Build app objects
build/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Link app binary
$(BIN): $(APP_OBJ)
	@mkdir -p $(dir $@)
	$(CC) $(APP_OBJ) -o $@ $(LDFLAGS)

run: $(BIN)
	./$(BIN) $(RUN_ARGS)

# ASAN-instrumented app binary for debugging, used inside integration tests.
ASAN_CFLAGS = $(TCFLAGS) $(TSAN)
ASAN_OBJ := $(APP_SRC:src/%.c=build/asan/%.o) build/asan/main.o

build/asan/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(ASAN_CFLAGS) -c $< -o $@

$(ASAN_BIN): $(ASAN_OBJ)
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

# Link each test binary from its test object + sanitized app objects
build/tests/%: build/tests/%.o $(TEST_APP_OBJ)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(TLDFLAGS)

# Run unit tests
test-unit: $(UNIT_TEST_BINS)
	@set -e; \
	for t in $(UNIT_TEST_BINS); do \
	  echo "==> $$t"; \
	  ASAN_OPTIONS=$(ASAN_RUN_OPTS) $$t; \
	done; \
	echo "ALL TESTS PASSED"

# Run integration tests (docker)
test-integration:
	@set -e; \
	docker compose -f tests/integration/postgres/postgres.test.yml up --build --abort-on-container-exit --exit-code-from test; \
	docker compose -f tests/integration/postgres/postgres.test.yml down -v

# Run all tests
test: test-unit test-integration

# Run postgres integration tests (used by docker) and run the .py tests.
# We always use the ASAN binary for tests.
test-postgres: EXTRA_TCFLAGS=-DADBX_TESTLOG -DDUMMY_SECRET_STORE_WARNING
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
	# Force rebuild of secret_store objects when switching test-only flags.
	rm -f build/testobj/secret_store.o build/testobj/dummy_secret_store.o
	rm -f build/asan/secret_store.o build/asan/dummy_secret_store.o

clean:
	rm -rf build
