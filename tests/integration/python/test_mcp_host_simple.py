#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(ROOT, "build", "ai-db-explorer")
SOCK = os.path.join(ROOT, "build", "aidbexplorer.sock")


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
        [BIN, "-broker", "-sock", SOCK],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for _ in range(50):
        if os.path.exists(SOCK):
            return proc
        time.sleep(0.05)
    proc.terminate()
    raise RuntimeError("broker did not create socket")


def start_server():
    return subprocess.Popen(
        [BIN, "-sock", SOCK],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def stop_proc(proc):
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        proc.kill()


def test_handshake_ok():
    broker = start_broker()
    server = start_server()
    try:
        req = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {"elicitation": {}},
                "clientInfo": {"name": "example-client", "version": "1.0.0"},
            },
        }
        write_frame(server, json.dumps(req).encode("utf-8"))
        resp = json.loads(read_frame(server).decode("utf-8"))
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 1
        assert resp["result"]["protocolVersion"] == "2025-11-25"
        assert "tools" in resp["result"]["capabilities"]
        assert "resources" in resp["result"]["capabilities"]
        assert resp["result"]["serverInfo"]["name"] == "ai-db-explorer"
        assert resp["result"]["serverInfo"]["version"] == "0.0.1"
    finally:
        stop_proc(server)
        stop_proc(broker)


def test_handshake_bad_version():
    broker = start_broker()
    server = start_server()
    try:
        req = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "initialize",
            "params": {"protocolVersion": "2020-01-01"},
        }
        write_frame(server, json.dumps(req).encode("utf-8"))
        resp = json.loads(read_frame(server).decode("utf-8"))
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 2
        assert resp["result"]["protocolVersion"] == "2025-06-18"
    finally:
        stop_proc(server)
        stop_proc(broker)


def test_handshake_invalid_json():
    broker = start_broker()
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
        stop_proc(broker)


def main():
    test_handshake_ok()
    test_handshake_bad_version()
    test_handshake_invalid_json()
    print("OK: test_mcp_host_simple")


if __name__ == "__main__":
    sys.exit(main())
