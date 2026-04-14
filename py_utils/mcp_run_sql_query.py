#!/usr/bin/env python3
"""
Sends one 'tools/call' for the 'run_sql_query' MCP tool to a local adbxplorer
MCP server and prints the JSON-RPC response.

Preconditions:
  - The adbxplorer broker is already running, with its runtime directory at
    /run/user/1000/adbxplorer (the default when XDG_RUNTIME_DIR=/run/user/1000).
  - build/adbxplorer (or the binary passed via --bin) exists.

Usage:
  py_utils/mcp_run_sql_query.py <connectionName> <filename>
  py_utils/mcp_run_sql_query.py MyPostgres ./query.sql
  py_utils/mcp_run_sql_query.py --bin build/adbxplorer-asan MyPostgres q.sql

This mirrors tests/integration/postgres/test_mcp_run_sql.py::send_tools_call,
but as a standalone driver against an already-running broker instead of the
test harness that also spawns the broker.
"""
import argparse
import json
import os
import subprocess
import sys


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_BIN = os.path.join(REPO_ROOT, "build", "adbxplorer")
DEFAULT_APPDIR = "/run/user/1000/adbxplorer"
MCP_PROTOCOL_VERSION = "2025-11-25"


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
    n = None
    for line in hdr_text.splitlines():
        if line.startswith(prefix):
            n = int(line[len(prefix):].strip())
            break
    if n is None:
        raise RuntimeError("missing Content-Length line")
    payload = rest
    while len(payload) < n:
        chunk = proc.stdout.read(n - len(payload))
        if not chunk:
            raise RuntimeError("unexpected EOF while reading payload")
        payload += chunk
    return payload


def send_initialize(server, req_id, protocol_version):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "initialize",
        "params": {
            "protocolVersion": protocol_version,
            "capabilities": {"elicitation": {}},
            "clientInfo": {"name": "mcp-run-sql-query", "version": "1.0.0"},
        },
    }
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def send_initialized_notification(server):
    note = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    write_frame(server, json.dumps(note).encode("utf-8"))


def send_tools_call(server, req_id, connection_name, query):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {
            "name": "run_sql_query",
            "arguments": {
                "connectionName": connection_name,
                "query": query,
            },
        },
    }
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def start_server(bin_path, appdir):
    # Derive XDG_RUNTIME_DIR from the parent of --appdir so the server's
    # resume-token store (which reads XDG_RUNTIME_DIR independently of
    # -appdir) stays consistent with the appdir we're pointing at.
    env = os.environ.copy()
    env["XDG_RUNTIME_DIR"] = os.path.dirname(os.path.abspath(appdir))
    return subprocess.Popen(
        [bin_path, "-appdir", appdir],
        cwd=REPO_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        env=env,
    )


def stop_proc(proc):
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        proc.kill()


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    p.add_argument("connection_name", help="MCP connectionName (e.g. 'MyPostgres')")
    p.add_argument("filename", help="Path to a file containing the SQL query")
    p.add_argument("--bin", default=DEFAULT_BIN, help=f"adbxplorer binary (default: {DEFAULT_BIN})")
    p.add_argument("--appdir", default=DEFAULT_APPDIR, help=f"adbxplorer appdir (default: {DEFAULT_APPDIR})")
    p.add_argument("--req-id", default="mcp-run-sql-query-1", help="JSON-RPC request id for tools/call")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    if not os.path.exists(args.bin):
        sys.stderr.write(f"error: binary not found: {args.bin}\n")
        return 2
    if not os.path.exists(args.appdir):
        sys.stderr.write(f"error: appdir not found: {args.appdir}\n")
        return 2
    if not os.path.isfile(args.filename):
        sys.stderr.write(f"error: query file not found: {args.filename}\n")
        return 2

    with open(args.filename, "r", encoding="utf-8") as f:
        query = f.read()
    if not query.strip():
        sys.stderr.write(f"error: query file is empty: {args.filename}\n")
        return 2

    server = None
    try:
        server = start_server(args.bin, args.appdir)

        init_resp = send_initialize(server, "mcp-run-sql-query-init", MCP_PROTOCOL_VERSION)
        if "result" not in init_resp:
            sys.stderr.write("error: initialize failed:\n")
            json.dump(init_resp, sys.stderr, indent=2)
            sys.stderr.write("\n")
            return 1
        send_initialized_notification(server)

        resp = send_tools_call(server, args.req_id, args.connection_name, query)
        json.dump(resp, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    finally:
        stop_proc(server)


if __name__ == "__main__":
    sys.exit(main())
