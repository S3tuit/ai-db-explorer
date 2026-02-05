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

        resp = send_tools_call(
            server,
            "req-3",
            "MyPostgres",
            "SELECT z.height_cm FROM zfighters z WHERE z.name = 'Broly'",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == "req-3"
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
            "SELECT g.name FROM gym_exercise g WHERE g.noob_weight = 40",
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
            "SELECT g.name FROM gym_exercise g WHERE g.noob_weight = 40",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 6
        assert "error" in resp
    finally:
        stop_proc(server)
        stop_proc(broker)


def test_run_sql_unsafe_role():
    broker = start_broker()
    server = start_server()
    try:
        resp = do_handshake(server, 5, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"

        resp = send_tools_call(
            server,
            6,
            "SuperPostgres",
            "SELECT 1;",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 6
        assert "error" in resp
    finally:
        stop_proc(server)
        stop_proc(broker)


def test_run_sql_sensitive():
    broker = start_broker()
    server = start_server()
    try:
        resp = do_handshake(server, 5, "2025-11-25")
        assert resp["jsonrpc"] == "2.0"

        resp = send_tools_call(
            server,
            6,
            "AnotherPostgres",
            "SELECT g.nickname FROM gym_bros g WHERE g.real_name = 'Angelo' LIMIT 1;",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 6
        assert "error" in resp
        assert "message" in resp["error"]
        assert "real_name" in resp["error"]["message"]

        resp = send_tools_call(
            server,
            "seven",
            "AnotherPostgres",
            "SELECT g.real_name FROM gym_bros g LIMIT 1;",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == "seven"
        assert resp["result"]["columns"][0]["name"] == "real_name"

        resp = send_tools_call(
            server,
            8,
            "AnotherPostgres",
            "SELECT get_weight(24, 16) AS w;",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 8
        row = resp["result"]["rows"][0]
        # libpq can return numeric values as strings; accept both.
        assert any(str(v) == "40" or str(v) == "40.0" for v in row)

        resp = send_tools_call(
            server,
            9,
            "AnotherPostgres",
            "SELECT unsafe_get_weight(20, 10) AS w;",
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 9
        assert "error" in resp
        assert "message" in resp["error"]
        assert "unsafe_get_weight" in resp["error"]["message"]

    finally:
        stop_proc(server)
        stop_proc(broker)


def main():
    test_run_sql_my_db()
    test_run_sql_another_db()
    test_run_sql_unknown_db()
    test_run_sql_unsafe_role()
    test_run_sql_sensitive()
    print("OK: test_mcp_run_sql")


if __name__ == "__main__":
    sys.exit(main())
