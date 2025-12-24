CC      := gcc
PKG_CONFIG ?= pkg-config
LIBPQ_CFLAGS := $(shell $(PKG_CONFIG) --cflags libpq 2>/dev/null)
LIBPQ_LIBS   := $(shell $(PKG_CONFIG) --libs   libpq 2>/dev/null)

# Build flags
CFLAGS  := -Wall -Wextra -std=c11 -g -O2
CFLAGS  += -D_POSIX_C_SOURCE=200809L
INCLUDES := -Isrc $(LIBPQ_CFLAGS)
LDFLAGS := $(LIBPQ_LIBS)

# Test flags
TCFLAGS := -Wall -Wextra -std=c11 -g -O1 $(INCLUDES)
TCFLAGS += -D_POSIX_C_SOURCE=200809L
TSAN    := -fsanitize=address,undefined -fno-omit-frame-pointer
TLDFLAGS := $(TSAN) $(LDFLAGS)

# App sources (exclude main.c for reuse in tests)
APP_MAIN := src/main.c
APP_SRC  := $(filter-out $(APP_MAIN),$(wildcard src/*.c))

# App bin
APP_OBJ := $(APP_SRC:src/%.c=build/%.o) build/main.o
BIN := build/ai-db-explorer

# Tests: each tests/test_foo.c -> build/tests/test_foo
TEST_SRC := $(wildcard tests/test_*.c)
TEST_BINS := $(patsubst tests/%.c,build/tests/%,$(TEST_SRC))

.PHONY: all clean run test

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

# Run all tests
test: $(TEST_BINS)
	@set -e; \
	for t in $(TEST_BINS); do \
	  echo "==> $$t"; \
	  ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 $$t; \
	done; \
	echo "ALL TESTS PASSED"

clean:
	rm -rf build
