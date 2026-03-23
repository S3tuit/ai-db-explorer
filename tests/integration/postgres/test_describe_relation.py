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
    stop_proc,
    write_frame,
)


def send_describe_relation_call(server, req_id, connection_name, schema_name, relation_name):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {
            "name": "describe_relation",
            "arguments": {
                "connectionName": connection_name,
                "schemaName": schema_name,
                "relationName": relation_name,
            },
        },
    }
    assert_tool_payload_valid("describe_relation", "input", req["params"]["arguments"])
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def _find_column(columns, name):
    for col in columns:
        if col["name"] == name:
            return col
    return None


def test_describe_relation_ok():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, resp = do_full_handshake(req_id=21)
        assert resp["jsonrpc"] == "2.0"

        resp = send_describe_relation_call(
            server,
            "describe-relation-ok",
            "MyPostgres",
            "public",
            "zfighter_intel",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == "describe-relation-ok"

        data = resp["result"]["structuredContent"]
        assert_tool_payload_valid("describe_relation", "output", data)
        assert data["schemaName"] == "public"
        assert data["relationName"] == "zfighter_intel"
        assert data["relationKind"] == "table"

        fighter_id = _find_column(data["columns"], "fighter_id")
        assert fighter_id is not None
        assert fighter_id["isPrimaryKey"] is True
        assert fighter_id["isForeignKey"] is True
        assert fighter_id["references"] is not None
        assert fighter_id["references"]["schemaName"] == "public"
        assert fighter_id["references"]["relationName"] == "zfighters"
        assert fighter_id["references"]["columnName"] == "id"

        scouter_serial = _find_column(data["columns"], "scouter_serial")
        assert scouter_serial is not None
        assert scouter_serial["sensitive"] is True

        codename = _find_column(data["columns"], "codename")
        assert codename is not None
        assert codename["sensitive"] is False

    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_describe_relation_missing_relation():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, resp = do_full_handshake(req_id=22)
        assert resp["jsonrpc"] == "2.0"

        resp = send_describe_relation_call(
            server,
            "describe-relation-missing",
            "MyPostgres",
            "public",
            "unknown_relation",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == "describe-relation-missing"
        assert resp["result"]["isError"] is True
        assert resp["result"]["content"][0]["type"] == "text"
        assert "unknown_relation" in resp["result"]["content"][0]["text"]

    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def main():
    test_describe_relation_ok()
    test_describe_relation_missing_relation()
    print("OK: test_describe_relation")


if __name__ == "__main__":
    sys.exit(main())
