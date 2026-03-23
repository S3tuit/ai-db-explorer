#!/usr/bin/env python3
import json
import shutil
import sys

from json_utils_tests import assert_tool_payload_valid

from test_broker_mcp_handshake import (
    do_full_handshake,
)

from test_user_mcp_handshake import (
    read_frame,
    read_proc_stderr,
    stop_proc,
    write_frame,
)


def send_list_connections_call(server, req_id):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {
            "name": "list_database_connections",
            "arguments": {},
        },
    }
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def send_faulty_list_connections_call(server, req_id):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {
            "name": "list_database_connections"
        },
    }
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def test_list_database_connections_ok():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    config_home = None
    try:
        broker, server, privdir, runtime_dir, resp = do_full_handshake(req_id=2)
        assert resp["jsonrpc"] == "2.0"

        assert_tool_payload_valid("list_database_connections", "input", {})
        resp = send_list_connections_call(server, "list-connections-ok")
        assert resp["jsonrpc"] == "2.0"

        structured = resp["result"]["structuredContent"]
        assert_tool_payload_valid("list_database_connections", "output", structured)
        assert structured["connections"]
        expected = {"SuperPostgres", "MyPostgres", "AnotherPostgres"}
        actual = {c["connectionName"] for c in structured["connections"]}
        assert actual == expected

    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_list_database_connections_faulty_request():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    config_home = None
    try:
        broker, server, privdir, runtime_dir, resp = do_full_handshake(req_id=2)
        assert resp["jsonrpc"] == "2.0"

        resp = send_faulty_list_connections_call(server, 302)
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 302
        assert resp["error"]["message"] is not None

    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def main():
    test_list_database_connections_ok()
    test_list_database_connections_faulty_request()
    print("OK: test_list_connections")


if __name__ == "__main__":
    sys.exit(main())
