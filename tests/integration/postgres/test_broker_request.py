#!/usr/bin/env python3
import shutil
import struct
import sys

from test_broker_mcp_handshake import (
    _assert_broker_usable,
    _assert_server_usable,
    _build_handshake_req_bytes,
    _connect_raw_broker_client,
    _read_broker_secret_token,
    _read_len_prefixed,
    _send_len_prefixed,
    _wait_broker_close,
    HANDSHAKE_MAGIC,
    HANDSHAKE_VERSION,
)
from test_user_mcp_handshake import make_runtime_dir, make_temp_privdir, start_broker, stop_proc

HS_OK = 0


def _open_broker_session(privdir):
    secret = _read_broker_secret_token(privdir)
    req = _build_handshake_req_bytes(secret)
    client = _connect_raw_broker_client(privdir)

    _send_len_prefixed(client, len(req), req)
    payload = _read_len_prefixed(client, timeout_sec=6.0)
    assert len(payload) >= 8

    magic, version, status = struct.unpack(">IHH", payload[:8])
    assert magic == HANDSHAKE_MAGIC
    assert version == HANDSHAKE_VERSION
    assert status == HS_OK
    return client


def test_post_handshake_truncated_request_frame_drops_session():
    privdir = make_temp_privdir("post-req-truncated")
    runtime_dir = make_runtime_dir("post-req-truncated-rt")
    broker = None
    raw = None
    try:
        broker = start_broker(privdir)

        raw = _open_broker_session(privdir)
        payload = b'{"jsonrpc":"2.0","id":1,"method":"tools/call"'
        _send_len_prefixed(raw, len(payload) + 2, payload)

        _wait_broker_close(raw)
        raw.close()
        raw = None

        _assert_broker_usable(privdir)
        _assert_server_usable(privdir, {"XDG_RUNTIME_DIR": runtime_dir})
    finally:
        if raw is not None:
            raw.close()
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_post_handshake_oversized_request_frame_drops_session():
    privdir = make_temp_privdir("post-req-oversized")
    runtime_dir = make_runtime_dir("post-req-oversized-rt")
    broker = None
    raw = None
    try:
        broker = start_broker(privdir)

        raw = _open_broker_session(privdir)
        _send_len_prefixed(raw, 0xFFFFFFFF, b"")

        _wait_broker_close(raw)
        raw.close()
        raw = None

        _assert_broker_usable(privdir)
        _assert_server_usable(privdir, {"XDG_RUNTIME_DIR": runtime_dir})
    finally:
        if raw is not None:
            raw.close()
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def main():
    test_post_handshake_truncated_request_frame_drops_session()
    test_post_handshake_oversized_request_frame_drops_session()
    print("OK: test_broker_request")


if __name__ == "__main__":
    sys.exit(main())
