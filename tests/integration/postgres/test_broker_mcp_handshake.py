#!/usr/bin/env python3
import os
import socket
import struct
import shutil
import sys
import time

from test_user_mcp_handshake import (
    MCP_PROTOCOL_VERSION,
    do_user_handshake,
    make_runtime_dir,
    make_temp_privdir,
    secret_token_path,
    start_broker,
    start_server,
    stop_proc,
)

SECRET_TOKEN_LEN = 32
RESUME_TOKEN_LEN = 32
RESTOK_DIR_NAME = "ai-dbexplorer-mcp"
HANDSHAKE_MAGIC = 0x4D435042
HANDSHAKE_VERSION = 1
HANDSHAKE_REQ_WIRE_SIZE = 4 + 2 + 2 + RESUME_TOKEN_LEN + SECRET_TOKEN_LEN
HANDSHAKE_RESP_WIRE_SIZE = 4 + 2 + 2 + RESUME_TOKEN_LEN + 4 + 4


# Linux /proc stat field #22 (starttime) extraction.
def _proc_start_ticks(pid):
    with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as f:
        line = f.read().strip()
    rparen = line.rfind(")")
    if rparen <= 0:
        raise RuntimeError("unexpected /proc stat format")
    fields_after_comm = line[rparen + 2 :].split()
    if len(fields_after_comm) < 20:
        raise RuntimeError("unexpected /proc stat fields")
    return int(fields_after_comm[19])


def _resume_store_dir(runtime_dir):
    return os.path.join(runtime_dir, RESTOK_DIR_NAME)


def _resume_token_path(runtime_dir):
    # resume_token.c scopes token filename by parent pid + parent start ticks.
    pid = os.getpid()
    ticks = _proc_start_ticks(pid)
    return os.path.join(_resume_store_dir(runtime_dir), f"token-{pid}-{ticks}")


def _seed_resume_token_file(runtime_dir, token):
    if len(token) != RESUME_TOKEN_LEN:
        raise ValueError("resume token must be exactly 32 bytes")

    store_dir = _resume_store_dir(runtime_dir)
    os.makedirs(store_dir, mode=0o700, exist_ok=True)
    os.chmod(store_dir, 0o700)

    token_path = _resume_token_path(runtime_dir)
    with open(token_path, "wb") as f:
        f.write(token)
    os.chmod(token_path, 0o600)
    return token_path


def _read_resume_token_file(runtime_dir):
    token_path = _resume_token_path(runtime_dir)
    if not os.path.exists(token_path):
        return None
    with open(token_path, "rb") as f:
        return f.read()


def _prepare_privdir_for_server_start(privdir, token):
    if len(token) != SECRET_TOKEN_LEN:
        raise ValueError("secret token must be exactly 32 bytes")

    run_dir = os.path.join(privdir, "run")
    sec_dir = os.path.join(privdir, "secret")
    os.makedirs(run_dir, mode=0o700, exist_ok=True)
    os.makedirs(sec_dir, mode=0o700, exist_ok=True)
    os.chmod(run_dir, 0o700)
    os.chmod(sec_dir, 0o700)

    tok_path = secret_token_path(privdir)
    with open(tok_path, "wb") as f:
        f.write(token)
    os.chmod(tok_path, 0o600)


def _wait_start_failure(proc, timeout_sec=3.0):
    rc = proc.wait(timeout=timeout_sec)
    assert rc != 0
    # Negative return codes mean signal-based termination (crash).
    assert rc > 0


def _assert_server_usable(privdir, server_env):
    server = start_server(privdir, env=server_env)
    try:
        resp = do_user_handshake(server, "healthy", MCP_PROTOCOL_VERSION)
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        assert resp["id"] == "healthy"
    finally:
        stop_proc(server)


def _assert_broker_usable(privdir):
    secret = _read_broker_secret_token(privdir)
    req = _build_handshake_req_bytes(secret)
    client = _connect_raw_broker_client(privdir)
    try:
        _send_len_prefixed(client, len(req), req)
        payload = _read_len_prefixed(client, timeout_sec=6.0)
        assert len(payload) == HANDSHAKE_RESP_WIRE_SIZE
        magic, version, status = struct.unpack(">IHH", payload[:8])
        assert magic == HANDSHAKE_MAGIC
        assert version == HANDSHAKE_VERSION
        assert status == 0  # HS_OK
    finally:
        client.close()


def _read_broker_secret_token(privdir):
    token_path = secret_token_path(privdir)
    with open(token_path, "rb") as f:
        token = f.read()
    assert len(token) == SECRET_TOKEN_LEN
    return token


def _build_handshake_req_bytes(secret_token, magic=HANDSHAKE_MAGIC):
    assert len(secret_token) == SECRET_TOKEN_LEN
    # Wire scalar fields are big-endian.
    req = struct.pack(
        ">IHH32s32s",
        magic,
        HANDSHAKE_VERSION,
        0,
        b"\x00" * RESUME_TOKEN_LEN,
        secret_token,
    )
    assert len(req) == HANDSHAKE_REQ_WIRE_SIZE
    return req


def _write_all(sock, data):
    view = memoryview(data)
    while view:
        sent = sock.send(view)
        if sent <= 0:
            raise RuntimeError("socket write failed")
        view = view[sent:]


def _send_len_prefixed(sock, declared_len, payload):
    if declared_len < 0 or declared_len > 0xFFFFFFFF:
        raise ValueError("declared_len must fit uint32")
    if len(payload) > declared_len:
        raise ValueError("payload cannot exceed declared length")
    _write_all(sock, struct.pack(">I", declared_len))
    if payload:
        _write_all(sock, payload)


def _connect_raw_broker_client(privdir):
    sock_path = os.path.join(privdir, "run", "broker.sock")
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(sock_path)
    return client


def _wait_broker_close(client, timeout_sec=6.0):
    client.settimeout(timeout_sec)
    deadline = time.time() + timeout_sec
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise AssertionError("broker did not close malformed client in time")
        client.settimeout(remaining)
        chunk = client.recv(4096)
        if chunk == b"":
            return


def _recv_exact(client, n, timeout_sec=6.0):
    if n < 0:
        raise ValueError("n must be >= 0")
    client.settimeout(timeout_sec)
    data = bytearray()
    while len(data) < n:
        chunk = client.recv(n - len(data))
        if chunk == b"":
            raise AssertionError("unexpected EOF while reading broker frame")
        data.extend(chunk)
    return bytes(data)


def _read_len_prefixed(client, timeout_sec=6.0):
    hdr = _recv_exact(client, 4, timeout_sec=timeout_sec)
    declared = struct.unpack(">I", hdr)[0]
    return _recv_exact(client, declared, timeout_sec=timeout_sec)


def do_full_handshake(
    req_id=1,
    protocol_version=MCP_PROTOCOL_VERSION,
    privdir=None,
    broker_env=None,
    server_env=None,
):
    created_privdir = False
    created_runtime = False
    if privdir is None:
        privdir = make_temp_privdir("full-hs")
        created_privdir = True

    merged_server_env = dict(server_env or {})
    runtime_dir = merged_server_env.get("XDG_RUNTIME_DIR")
    if not runtime_dir:
        runtime_dir = make_runtime_dir("full-hs-rt")
        merged_server_env["XDG_RUNTIME_DIR"] = runtime_dir
        created_runtime = True

    broker = None
    server = None
    try:
        broker = start_broker(privdir, env=broker_env)
        server = start_server(privdir, env=merged_server_env)
        resp = do_user_handshake(server, req_id, protocol_version)
        return broker, server, privdir, runtime_dir, resp
    except Exception:
        stop_proc(server)
        stop_proc(broker)
        if created_privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if created_runtime:
            shutil.rmtree(runtime_dir, ignore_errors=True)
        raise


def test_broker_absent_server_fails_to_start():
    privdir = make_temp_privdir("broker-absent")
    runtime_dir = make_runtime_dir("restok-absent")
    server = None
    try:
        _prepare_privdir_for_server_start(privdir, os.urandom(SECRET_TOKEN_LEN))
        server = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        _wait_start_failure(server)
    finally:
        stop_proc(server)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_secret_token_mismatch_server_fails_to_start():
    privdir = make_temp_privdir("secret-mismatch")
    runtime_dir = make_runtime_dir("restok-mismatch")
    broker = None
    server = None
    try:
        broker = start_broker(privdir)

        token_path = secret_token_path(privdir)
        with open(token_path, "rb") as f:
            good_secret = f.read()
        assert len(good_secret) == SECRET_TOKEN_LEN

        with open(token_path, "wb") as f:
            f.write(os.urandom(SECRET_TOKEN_LEN))
        os.chmod(token_path, 0o600)

        server = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        _wait_start_failure(server)

        with open(token_path, "wb") as f:
            f.write(good_secret)
        os.chmod(token_path, 0o600)

        _assert_broker_usable(privdir)
        _assert_server_usable(privdir, {"XDG_RUNTIME_DIR": runtime_dir})
    finally:
        stop_proc(server)
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_unknown_resume_token_fallback_still_starts():
    privdir = make_temp_privdir("resume-unknown")
    runtime_dir = make_runtime_dir("restok-unknown")
    broker = None
    server = None
    try:
        broker = start_broker(privdir)

        unknown = os.urandom(RESUME_TOKEN_LEN)
        _seed_resume_token_file(runtime_dir, unknown)

        server = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        resp = do_user_handshake(server, "u-unknown", MCP_PROTOCOL_VERSION)
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION

        fresh = _read_resume_token_file(runtime_dir)
        assert fresh is not None
        assert len(fresh) == RESUME_TOKEN_LEN
        assert fresh != unknown
    finally:
        stop_proc(server)
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_resume_happy_path_across_restart_rotates_token():
    privdir = make_temp_privdir("resume-rotate")
    runtime_dir = make_runtime_dir("restok-rotate")
    broker = None
    server1 = None
    server2 = None
    try:
        broker = start_broker(privdir)

        server1 = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        resp = do_user_handshake(server1, "first", MCP_PROTOCOL_VERSION)
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        token1 = _read_resume_token_file(runtime_dir)
        assert token1 is not None
        assert len(token1) == RESUME_TOKEN_LEN

        stop_proc(server1)
        server1 = None
        # Give broker poll loop enough time to move active session into idle.
        time.sleep(0.2)

        server2 = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        resp = do_user_handshake(server2, "second", MCP_PROTOCOL_VERSION)
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        token2 = _read_resume_token_file(runtime_dir)
        assert token2 is not None
        assert len(token2) == RESUME_TOKEN_LEN
        assert token2 != token1
    finally:
        stop_proc(server2)
        stop_proc(server1)
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_wrong_resume_dir_permissions_disable_resume_but_starts():
    privdir = make_temp_privdir("resume-perms")
    runtime_dir = make_runtime_dir("restok-perms")
    broker = None
    server = None
    try:
        os.makedirs(_resume_store_dir(runtime_dir), mode=0o755, exist_ok=True)
        os.chmod(_resume_store_dir(runtime_dir), 0o755)

        broker = start_broker(privdir)
        server = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        resp = do_user_handshake(server, "perms", MCP_PROTOCOL_VERSION)
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION

        # Resume store is fail-disabled; token persistence should be inactive.
        assert _read_resume_token_file(runtime_dir) is None
    finally:
        stop_proc(server)
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_server_cannot_read_secret_token_fails_to_start():
    privdir = make_temp_privdir("secret-unreadable")
    runtime_dir = make_runtime_dir("restok-secret")
    broker = None
    server = None
    try:
        broker = start_broker(privdir)

        token_path = secret_token_path(privdir)
        with open(token_path, "rb") as f:
            token = f.read()
        assert len(token) == SECRET_TOKEN_LEN

        # Do not rely on chmod-based denial: in containers tests may run as
        # root, which can still read mode-000 files. Deleting the token makes
        # read fail deterministically for all UIDs.
        os.unlink(token_path)

        server = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        _wait_start_failure(server)

        with open(token_path, "wb") as f:
            f.write(token)
        os.chmod(token_path, 0o600)
        _assert_broker_usable(privdir)
        _assert_server_usable(privdir, {"XDG_RUNTIME_DIR": runtime_dir})
    finally:
        stop_proc(server)
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_expired_resume_token_fallback_still_starts():
    privdir = make_temp_privdir("resume-expired")
    runtime_dir = make_runtime_dir("restok-expired")
    broker = None
    server1 = None
    server2 = None
    try:
        broker = start_broker(privdir, env={"ADBX_TEST_IDLE_TTL_SEC": "1"})

        server1 = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        resp = do_user_handshake(server1, "exp-1", MCP_PROTOCOL_VERSION)
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        token1 = _read_resume_token_file(runtime_dir)
        assert token1 is not None
        assert len(token1) == RESUME_TOKEN_LEN

        stop_proc(server1)
        server1 = None
        time.sleep(2.2)

        server2 = start_server(privdir, env={"XDG_RUNTIME_DIR": runtime_dir})
        resp = do_user_handshake(server2, "exp-2", MCP_PROTOCOL_VERSION)
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        token2 = _read_resume_token_file(runtime_dir)
        assert token2 is not None
        assert len(token2) == RESUME_TOKEN_LEN
        assert token2 != token1
    finally:
        stop_proc(server2)
        stop_proc(server1)
        stop_proc(broker)
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(privdir, ignore_errors=True)


def test_broker_survives_bad_magic_raw_handshake():
    privdir = make_temp_privdir("raw-bad-magic")
    runtime_dir = make_runtime_dir("raw-bad-magic-rt")
    broker = None
    raw = None
    try:
        broker = start_broker(privdir)
        secret = _read_broker_secret_token(privdir)
        req = _build_handshake_req_bytes(secret, magic=HANDSHAKE_MAGIC ^ 1)

        raw = _connect_raw_broker_client(privdir)
        _send_len_prefixed(raw, len(req), req)
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


def test_broker_survives_len_mismatch_raw_handshake():
    privdir = make_temp_privdir("raw-len-mismatch")
    runtime_dir = make_runtime_dir("raw-len-mismatch-rt")
    broker = None
    raw = None
    try:
        broker = start_broker(privdir)
        secret = _read_broker_secret_token(privdir)
        req = _build_handshake_req_bytes(secret)
        short_payload = req[:-1]

        raw = _connect_raw_broker_client(privdir)
        _send_len_prefixed(raw, len(short_payload), short_payload)
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


def test_broker_survives_truncated_keep_open_raw_handshake():
    privdir = make_temp_privdir("raw-truncated")
    runtime_dir = make_runtime_dir("raw-truncated-rt")
    broker = None
    raw = None
    try:
        broker = start_broker(privdir)
        secret = _read_broker_secret_token(privdir)
        req = _build_handshake_req_bytes(secret)
        partial_payload = req[:-2]

        raw = _connect_raw_broker_client(privdir)
        _send_len_prefixed(raw, len(req), partial_payload)
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


def test_broker_survives_no_bytes_raw_handshake():
    privdir = make_temp_privdir("raw-no-bytes")
    runtime_dir = make_runtime_dir("raw-no-bytes-rt")
    broker = None
    raw = None
    try:
        broker = start_broker(privdir)
        raw = _connect_raw_broker_client(privdir)
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
    test_broker_absent_server_fails_to_start()
    test_secret_token_mismatch_server_fails_to_start()
    test_unknown_resume_token_fallback_still_starts()
    test_resume_happy_path_across_restart_rotates_token()
    test_wrong_resume_dir_permissions_disable_resume_but_starts()
    test_server_cannot_read_secret_token_fails_to_start()
    test_expired_resume_token_fallback_still_starts()
    test_broker_survives_bad_magic_raw_handshake()
    test_broker_survives_len_mismatch_raw_handshake()
    test_broker_survives_truncated_keep_open_raw_handshake()
    test_broker_survives_no_bytes_raw_handshake()
    print("OK: test_broker_mcp_handshake")


if __name__ == "__main__":
    sys.exit(main())
