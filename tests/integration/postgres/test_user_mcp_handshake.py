#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
import time

# root is not '/', but is the root of our repo copied inside the docker
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(ROOT, "build", "ai-db-explorer-asan")
DEFAULT_PRIVDIR = os.path.join(ROOT, "build", "privdir")
CONFIG = os.path.join(ROOT, "tests", "integration", "postgres", "config.json")
MCP_PROTOCOL_VERSION = "2025-11-25"


def broker_sock_path(privdir):
    return os.path.join(privdir, "run", "broker.sock")


def secret_token_path(privdir):
    return os.path.join(privdir, "secret", "token")


def make_temp_privdir(prefix="mcp-it"):
    build_dir = os.path.join(ROOT, "build")
    os.makedirs(build_dir, exist_ok=True)
    return tempfile.mkdtemp(prefix=f"{prefix}-", dir=build_dir)


def make_runtime_dir(prefix="mcp-rt"):
    path = tempfile.mkdtemp(prefix=f"{prefix}-")
    os.chmod(path, 0o700)
    return path


def merge_env(extra_env):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return env


def write_frame(proc, payload_bytes):
    hdr = f"Content-Length: {len(payload_bytes)}\r\n\r\n".encode("ascii")
    proc.stdin.write(hdr + payload_bytes)
    proc.stdin.flush()


def read_frame(proc):
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = proc.stdout.read(1)
        if not chunk:
            raise RuntimeError("unexpected EOF while reading header")
        buf += chunk
        if len(buf) > 256:
            raise RuntimeError("header too large")
    hdr, rest = buf.split(b"\r\n\r\n", 1)
    hdr_text = hdr.decode("ascii", errors="replace")
    prefix = "Content-Length:"
    if prefix not in hdr_text:
        raise RuntimeError("missing Content-Length")
    for line in hdr_text.splitlines():
        if line.startswith(prefix):
            n = int(line[len(prefix):].strip())
            break
    else:
        raise RuntimeError("missing Content-Length line")
    payload = rest
    while len(payload) < n:
        chunk = proc.stdout.read(n - len(payload))
        if not chunk:
            raise RuntimeError("unexpected EOF while reading payload")
        payload += chunk
    return payload


def start_broker(privdir=DEFAULT_PRIVDIR, env=None):
    sock = broker_sock_path(privdir)
    if os.path.exists(sock):
        os.unlink(sock)

    proc = subprocess.Popen(
        [BIN, "-broker", "-privdir", privdir, "-config", CONFIG],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        # Forward broker logs to the test runner so failures aren't silent.
        stderr=None,
        env=merge_env(env),
    )

    for _ in range(50):
        if proc.poll() is not None:
            raise RuntimeError("broker exited before creating socket")
        if os.path.exists(sock):
            return proc
        time.sleep(0.05)

    stop_proc(proc)
    raise RuntimeError("broker did not create socket")


def start_server(privdir=DEFAULT_PRIVDIR, env=None):
    return subprocess.Popen(
        [BIN, "-privdir", privdir],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        # Forward MCP server logs to the test runner for easier debugging.
        stderr=None,
        env=merge_env(env),
    )


def stop_proc(proc):
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        proc.kill()


def do_user_handshake(server, req_id, protocol_version):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "initialize",
        "params": {
            "protocolVersion": protocol_version,
            "capabilities": {"elicitation": {}},
            "clientInfo": {"name": "example-client", "version": "1.0.0"},
        },
    }
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def test_handshake_ok():
    server = start_server()
    try:
        resp = do_user_handshake(server, 1, MCP_PROTOCOL_VERSION)
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 1
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        assert "tools" in resp["result"]["capabilities"]
        assert "resources" in resp["result"]["capabilities"]
        assert resp["result"]["serverInfo"]["name"] == "ai-db-explorer"
        assert resp["result"]["serverInfo"]["version"] == "0.0.1"

        # Double initialize should fail.
        resp = do_user_handshake(server, "second", MCP_PROTOCOL_VERSION)
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == "second"
        assert "error" in resp
    finally:
        stop_proc(server)


# This should not error: when client asks for an unsupported version, the
# server replies with a supported one.
def test_handshake_bad_version():
    server = start_server()
    try:
        resp = do_user_handshake(server, 2, "2020-01-01")
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 2
        assert resp["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
    finally:
        stop_proc(server)


# Invalid JSON should return JSON-RPC Invalid Request.
def test_handshake_invalid_json():
    server = start_server()
    try:
        bad = b'{"jsonrpc":"2.0","id":3,"method":"initialize"'
        write_frame(server, bad)
        resp = json.loads(read_frame(server).decode("utf-8"))
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] is None
        assert resp["error"]["code"] == -32600
    finally:
        stop_proc(server)


def test_notification_invalid_request():
    server = start_server()
    try:
        resp = do_user_handshake(server, 4, MCP_PROTOCOL_VERSION)
        assert resp["jsonrpc"] == "2.0"

        # Notifications must not include an id; server should ignore them.
        note = {
            "jsonrpc": "2.0",
            "method": "notifications/test",
            "params": {"x": 1},
        }
        write_frame(server, json.dumps(note).encode("utf-8"))

        # Verify connection still alive by issuing initialize again.
        resp = do_user_handshake(server, 5, MCP_PROTOCOL_VERSION)
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 5
    finally:
        stop_proc(server)


def main():
    broker = start_broker()
    try:
        test_handshake_ok()
        test_handshake_bad_version()
        test_handshake_invalid_json()
        test_notification_invalid_request()
        print("OK: test_user_mcp_handshake")
    finally:
        stop_proc(broker)


if __name__ == "__main__":
    sys.exit(main())
