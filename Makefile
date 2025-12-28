CC      := gcc
PKG_CONFIG ?= pkg-config
LIBPQ_CFLAGS := $(shell $(PKG_CONFIG) --cflags libpq 2>/dev/null)
LIBPQ_LIBS   := $(shell $(PKG_CONFIG) --libs   libpq 2>/dev/null)

# Build flags
CFLAGS  := -Wall -Wextra -std=c11 -g -O2
CFLAGS  += -D_POSIX_C_SOURCE=200809L
INCLUDES := -Isrc -Itests/unit $(LIBPQ_CFLAGS)
LDFLAGS := $(LIBPQ_LIBS)

# Test flags
TCFLAGS := -Wall -Wextra -std=c11 -g -O1 $(INCLUDES)
TCFLAGS += -D_POSIX_C_SOURCE=200809L
TSAN    := -fsanitize=address,undefined -fno-omit-frame-pointer
TLDFLAGS := $(TSAN) $(LDFLAGS) $(PIE_LDFLAGS)
ASAN_RUN_OPTS ?= detect_leaks=1:abort_on_error=1

# App sources (exclude main.c for reuse in tests)
APP_MAIN := src/main.c
APP_SRC  := $(filter-out $(APP_MAIN),$(wildcard src/*.c))

# App bin
APP_OBJ := $(APP_SRC:src/%.c=build/%.o) build/main.o
BIN := build/ai-db-explorer

# Unit tests: each tests/unit/test_foo.c -> build/tests/unit/test_foo
UNIT_TEST_SRC := $(wildcard tests/unit/test_*.c)
UNIT_TEST_BINS := $(patsubst tests/unit/%.c,build/tests/unit/%,$(UNIT_TEST_SRC))

# Integration tests: tests/integration/*/test_foo.c -> build/tests/integration/*/test_foo
INTEGRATION_TEST_SRC := $(wildcard tests/integration/*/test_*.c)
INTEGRATION_TEST_BINS := $(patsubst tests/integration/%.c,build/tests/integration/%,$(INTEGRATION_TEST_SRC))

.PHONY: all clean run test test-unit test-integration test-postgres test-build

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
	./$(BIN)

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

# Run postgres integration tests (used by docker)
test-postgres: $(INTEGRATION_TEST_BINS)
	@set -e; \
	for t in $(INTEGRATION_TEST_BINS); do \
	  echo "==> $$t"; \
	  ASAN_OPTIONS=$(ASAN_RUN_OPTS) $$t; \
	done; \
	echo "ALL TESTS PASSED"

# Only builds tests, usefull for making the LSP recognize the header files
# inside tests/
test-build: $(UNIT_TEST_BINS) $(INTEGRATION_TEST_BINS)

clean:
	rm -rf build
