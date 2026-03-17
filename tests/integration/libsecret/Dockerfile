# same as the integration test for postgres, we just change the packages to install
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libsecret-1-dev \
        libglib2.0-dev \
        dbus-user-session \
        gnome-keyring \
        libsecret-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .
