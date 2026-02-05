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

# copy vendored libpg_query first so docker can cache this layer
COPY third_party/libpg_query third_party/libpg_query

# Always rebuild libpg_query inside the container toolchain.
RUN make -C third_party/libpg_query clean && make -C third_party/libpg_query

COPY . .
