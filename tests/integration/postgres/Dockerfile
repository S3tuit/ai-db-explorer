# we need an image that allow libasan/libubsan packages
FROM debian:bookworm-slim

# install what we need to run the test
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        pkg-config \
        ca-certificates \
        python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

# PIE linking may give error since we don't compile with -fPIE,
# that why we put "-no-pie"
CMD ["sh", "-lc", "make PIE_LDFLAGS=-no-pie test-postgres"]
