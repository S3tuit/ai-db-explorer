# Compiler and flags
CC      := gcc
CFLAGS  := -Wall -Wextra -std=c11 -g -O2
CFLAGS  += -D_POSIX_C_SOURCE=200809L
LDFLAGS :=

# Include paths
INCLUDES := -Isrc

# Sources
SRC := \
    src/main.c \
    src/utils.c \
    src/transport_reader.c

# Object files go into build/
OBJ := $(SRC:src/%.c=build/%.o)

BIN := build/ai-db-explorer

.PHONY: all clean run

all: $(BIN)

# Link step
$(BIN): build $(OBJ)
	$(CC) $(OBJ) -o $(BIN) $(LDFLAGS)

# Generic rule: src/foo.c -> build/foo.o
build/%.o: src/%.c | build
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Ensure build/ exists
build:
	mkdir -p build

run: $(BIN)
	./$(BIN)

clean:
	rm -rf build
