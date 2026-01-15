#!/usr/bin/env python3
import json
import sys

from test_mcp_handshake import (
    do_handshake,
    read_frame,
    start_broker,
    start_server,
    stop_proc,
    write_frame,
)


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


def test_run_sql_my_db():
    broker = start_broker()
    server = start_server()
    try:
        resp = do_handshake(server, 1, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"
        print("DEBUG: stdin_closed=", server.stdin.closed)
        print("DEBUG: server_poll=", server.poll())

        resp = send_tools_call(
            server,
            3,
            "MyPostgres",
            "SELECT height_cm FROM zfighters WHERE name = 'Broly'",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 3
        assert resp["result"]["columns"][0]["name"] == "height_cm"
        assert resp["result"]["rows"] == [["220"]]
    finally:
        stop_proc(server)
        stop_proc(broker)


def test_run_sql_another_db():
    broker = start_broker()
    server = start_server()
    try:
        resp = do_handshake(server, 2, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"

        resp = send_tools_call(
            server,
            4,
            "AnotherPostgres",
            "SELECT name FROM gym_exercise WHERE noob_weight = 40",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 4
        rows = resp["result"]["rows"]
        names = [row[0] for row in rows]
        assert "Bench Press" in names
        assert "Barbell Row" in names
    finally:
        stop_proc(server)
        stop_proc(broker)


def test_run_sql_unknown_db():
    broker = start_broker()
    server = start_server()
    try:
        resp = do_handshake(server, 5, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"

        resp = send_tools_call(
            server,
            6,
            "Random",
            "SELECT name FROM gym_exercise WHERE noob_weight = 40",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 6
        assert "error" in resp
    finally:
        stop_proc(server)
        stop_proc(broker)


def main():
    test_run_sql_my_db()
    test_run_sql_another_db()
    test_run_sql_unknown_db()
    print("OK: test_mcp_run_sql")


if __name__ == "__main__":
    sys.exit(main())
