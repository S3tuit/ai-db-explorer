#!/usr/bin/env python3
import json
import shutil
import sys

from test_broker_mcp_handshake import do_full_handshake
from test_mcp_run_sql import send_tools_call
from test_user_mcp_handshake import read_frame, stop_proc, write_frame


def send_tokens_tools_call(server, req_id, connection_name, query, parameters):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {
            "name": "run_sql_query_tokens",
            "arguments": {
                "connectionName": connection_name,
                "query": query,
                "parameters": parameters,
            },
        },
    }
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def _assert_tools_call_ok(resp, req_id):
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == req_id
    assert "result" in resp
    assert resp["result"].get("isError") is not True
    return resp["result"]["structuredContent"]


def _assert_tools_call_failed(resp, req_id):
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == req_id
    if "error" in resp:
        return
    assert "result" in resp
    assert resp["result"].get("isError") is True


def _extract_one_token(server, req_id, connection_name, query):
    resp = send_tools_call(server, req_id, connection_name, query)
    data = _assert_tools_call_ok(resp, req_id)
    tok = data["rows"][0][0]
    assert isinstance(tok, str)
    assert tok.startswith("tok_")
    return tok


def test_token_input_happy_path_one_param():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=200)

        tok = _extract_one_token(
            server,
            "tok-happy-1-src",
            "MyPostgres",
            "SELECT i.scouter_serial FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;",
        )
        resp = send_tokens_tools_call(
            server,
            "tok-happy-1-run",
            "MyPostgres",
            "SELECT i.fighter_id FROM zfighter_intel i WHERE i.scouter_serial = $1 LIMIT 1;",
            [tok],
        )
        data = _assert_tools_call_ok(resp, "tok-happy-1-run")
        assert data["rows"] == [["1"]]
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_token_input_happy_path_two_params_from_two_queries():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=210)

        tok_serial = _extract_one_token(
            server,
            "tok-happy-2-src-a",
            "MyPostgres",
            "SELECT i.scouter_serial FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;",
        )
        tok_home = _extract_one_token(
            server,
            "tok-happy-2-src-b",
            "MyPostgres",
            "SELECT i.home_coordinates FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;",
        )

        resp = send_tokens_tools_call(
            server,
            "tok-happy-2-run",
            "MyPostgres",
            "SELECT i.codename FROM zfighter_intel i "
            "WHERE i.scouter_serial = $1 AND i.home_coordinates = $2 LIMIT 1;",
            [tok_serial, tok_home],
        )
        data = _assert_tools_call_ok(resp, "tok-happy-2-run")
        assert data["rows"] == [["Kakarot"]]
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_token_input_wrong_column_fails():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=220)

        tok_home = _extract_one_token(
            server,
            "tok-bad-col-src",
            "MyPostgres",
            "SELECT i.home_coordinates FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;",
        )

        resp = send_tokens_tools_call(
            server,
            "tok-bad-col-run",
            "MyPostgres",
            "SELECT i.fighter_id FROM zfighter_intel i WHERE i.scouter_serial = $1 LIMIT 1;",
            [tok_home],
        )
        _assert_tools_call_failed(resp, "tok-bad-col-run")
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_token_input_fewer_tokens_than_params_fails():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=230)

        tok_serial = _extract_one_token(
            server,
            "tok-fewer-src",
            "MyPostgres",
            "SELECT i.scouter_serial FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;",
        )

        resp = send_tokens_tools_call(
            server,
            "tok-fewer-run",
            "MyPostgres",
            "SELECT i.codename FROM zfighter_intel i "
            "WHERE i.scouter_serial = $1 AND i.home_coordinates = $2 LIMIT 1;",
            [tok_serial],
        )
        _assert_tools_call_failed(resp, "tok-fewer-run")
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_token_input_more_tokens_than_params_fails():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=240)

        tok_serial = _extract_one_token(
            server,
            "tok-more-src-a",
            "MyPostgres",
            "SELECT i.scouter_serial FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;",
        )
        tok_home = _extract_one_token(
            server,
            "tok-more-src-b",
            "MyPostgres",
            "SELECT i.home_coordinates FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;",
        )

        resp = send_tokens_tools_call(
            server,
            "tok-more-run",
            "MyPostgres",
            "SELECT i.codename FROM zfighter_intel i WHERE i.scouter_serial = $1 LIMIT 1;",
            [tok_serial, tok_home],
        )
        _assert_tools_call_failed(resp, "tok-more-run")
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_token_input_no_tokens_no_params_fails():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=250)

        resp = send_tokens_tools_call(
            server,
            "tok-empty-run",
            "MyPostgres",
            "SELECT i.codename FROM zfighter_intel i WHERE i.scouter_serial = $1 LIMIT 1;",
            [],
        )
        _assert_tools_call_failed(resp, "tok-empty-run")
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_token_input_cross_connection_token_fails():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=260)

        tok = _extract_one_token(
            server,
            "tok-cross-src",
            "AnotherPostgres",
            "SELECT g.real_name FROM gym_bros g WHERE g.id = 2 LIMIT 1;",
        )

        resp = send_tokens_tools_call(
            server,
            "tok-cross-run",
            "MyPostgres",
            "SELECT i.codename FROM zfighter_intel i WHERE i.scouter_serial = $1 LIMIT 1;",
            [tok],
        )
        _assert_tools_call_failed(resp, "tok-cross-run")
        assert "connection mismatch" in resp["result"]["content"][0]["text"]
        assert resp["result"]["isError"] == True
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def main():
    test_token_input_happy_path_one_param()
    test_token_input_happy_path_two_params_from_two_queries()
    test_token_input_wrong_column_fails()
    test_token_input_fewer_tokens_than_params_fails()
    test_token_input_more_tokens_than_params_fails()
    test_token_input_no_tokens_no_params_fails()
    test_token_input_cross_connection_token_fails()
    print("OK: test_token_input")


if __name__ == "__main__":
    sys.exit(main())
