#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time

# root is not '/', but is the root of our repo copied inside the docker
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(ROOT, "build", "ai-db-explorer-asan")
PRIVDIR = os.path.join(ROOT, "build", "privdir")
SOCK = os.path.join(PRIVDIR, "run", "broker.sock")
CONFIG = os.path.join(ROOT, "tests", "integration", "postgres", "config.json")


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


def start_broker():
    if os.path.exists(SOCK):
        os.unlink(SOCK)
    proc = subprocess.Popen(
        [BIN, "-broker", "-privdir", PRIVDIR, "-config", CONFIG],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        # Forward broker logs to the test runner so failures aren't silent.
        stderr=None,
    )
    for _ in range(50):
        if os.path.exists(SOCK):
            return proc
        time.sleep(0.05)
    proc.terminate()
    raise RuntimeError("broker did not create socket")


def start_server():
    return subprocess.Popen(
        [BIN, "-privdir", PRIVDIR],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        # Forward MCP server logs to the test runner for easier debugging.
        stderr=None,
    )


def stop_proc(proc):
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        proc.kill()


def do_handshake(server, req_id, protocol_version):
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


def test_handshake_ok(broker):
    server = start_server()
    try:
        resp = do_handshake(server, 1, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 1
        assert resp["result"]["protocolVersion"] == "2025-11-25"
        assert "tools" in resp["result"]["capabilities"]
        assert "resources" in resp["result"]["capabilities"]
        assert resp["result"]["serverInfo"]["name"] == "ai-db-explorer"
        assert resp["result"]["serverInfo"]["version"] == "0.0.1"
        # double handshake should fail
        resp = do_handshake(server, "second", "2025-11-25")
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == "second"
        assert "error" in resp
    finally:
        stop_proc(server)

# this should not give error, the broker should respond with the laster version
# available to it
def test_handshake_bad_version(broker):
    server = start_server()
    try:
        resp = do_handshake(server, 2, "2020-01-01")
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 2
        assert resp["result"]["protocolVersion"] == "2025-11-25"
    finally:
        stop_proc(server)

# this should error
def test_handshake_invalid_json(broker):
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

def test_notification_invalid_request(broker):
    server = start_server()
    try:
        resp = do_handshake(server, 4, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"

        # Notifications must not include an id; server should ignore them.
        note = {
            "jsonrpc": "2.0",
            "method": "notifications/test",
            "params": {"x": 1},
        }
        write_frame(server, json.dumps(note).encode("utf-8"))

        # A valid server should not respond to notifications; verify
        # the connection is still alive by sending another handshake.
        resp = do_handshake(server, 5, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 5
    finally:
        stop_proc(server)


def main():
    broker = start_broker()
    try:
        test_handshake_ok(broker)
        test_handshake_bad_version(broker)
        test_handshake_invalid_json(broker)
        test_notification_invalid_request(broker)
        print("OK: test_mcp_handshake")
    finally:
        stop_proc(broker)


if __name__ == "__main__":
    sys.exit(main())
