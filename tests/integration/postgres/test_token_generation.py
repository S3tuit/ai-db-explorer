#!/usr/bin/env python3
import shutil
import sys

from test_broker_mcp_handshake import do_full_handshake
from test_mcp_run_sql import send_tools_call
from test_user_mcp_handshake import stop_proc


def _assert_tools_call_ok(resp, req_id):
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == req_id
    assert "result" in resp
    assert resp["result"].get("isError") is not True
    return resp["result"]["structuredContent"]


def _parse_token(token):
    assert isinstance(token, str)
    assert token.startswith("tok_")
    tail = token[len("tok_") :]
    conn_name, generation_txt, index_txt = tail.rsplit("_", 2)
    generation = int(generation_txt)
    index = int(index_txt)
    assert generation >= 0
    assert index >= 0
    return conn_name, generation, index


def _assert_is_token(token, exp_connection):
    conn_name, generation, index = _parse_token(token)
    assert conn_name == exp_connection
    assert generation >= 0
    assert index >= 0


def test_my_postgres_randomized_same_value_yields_different_tokens():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=100)

        q = (
            "SELECT i.scouter_serial "
            "FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;"
        )
        resp1 = send_tools_call(server, "my-rand-1", "MyPostgres", q)
        data1 = _assert_tools_call_ok(resp1, "my-rand-1")

        resp2 = send_tools_call(server, "my-rand-2", "MyPostgres", q)
        data2 = _assert_tools_call_ok(resp2, "my-rand-2")

        tok1 = data1["rows"][0][0]
        tok2 = data2["rows"][0][0]
        assert tok1 != "SCT-9001-A"
        assert tok2 != "SCT-9001-A"
        _assert_is_token(tok1, "MyPostgres")
        _assert_is_token(tok2, "MyPostgres")
        assert tok1 != tok2
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_my_postgres_only_sensitive_columns_are_tokenized():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=110)

        q = (
            "SELECT i.codename, i.scouter_serial, i.home_coordinates "
            "FROM zfighter_intel i WHERE i.fighter_id = 1 LIMIT 1;"
        )
        resp = send_tools_call(server, "my-mixed", "MyPostgres", q)
        data = _assert_tools_call_ok(resp, "my-mixed")

        row = data["rows"][0]
        assert row[0] == "Kakarot"
        assert row[1] != "SCT-9001-A"
        assert row[2] != "X:120-Y:450"
        _assert_is_token(row[1], "MyPostgres")
        _assert_is_token(row[2], "MyPostgres")
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_another_postgres_deterministic_same_value_yields_same_token():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=120)

        q = "SELECT g.real_name FROM gym_bros g WHERE g.id = 2 LIMIT 1;"
        resp1 = send_tools_call(server, "another-det-1", "AnotherPostgres", q)
        data1 = _assert_tools_call_ok(resp1, "another-det-1")

        resp2 = send_tools_call(server, "another-det-2", "AnotherPostgres", q)
        data2 = _assert_tools_call_ok(resp2, "another-det-2")

        tok1 = data1["rows"][0][0]
        tok2 = data2["rows"][0][0]
        assert tok1 != "Angelo"
        assert tok2 != "Angelo"
        _assert_is_token(tok1, "AnotherPostgres")
        _assert_is_token(tok2, "AnotherPostgres")
        assert tok1 == tok2
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def test_another_postgres_only_sensitive_columns_are_tokenized():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, _ = do_full_handshake(req_id=130)

        q = (
            "SELECT g.nickname, g.real_name "
            "FROM gym_bros g WHERE g.id = 2 LIMIT 1;"
        )
        resp = send_tools_call(server, "another-mixed", "AnotherPostgres", q)
        data = _assert_tools_call_ok(resp, "another-mixed")

        row = data["rows"][0]
        assert row[0] == "Black Panther"
        assert row[1] != "Angelo"
        _assert_is_token(row[1], "AnotherPostgres")
    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def main():
    test_my_postgres_randomized_same_value_yields_different_tokens()
    test_my_postgres_only_sensitive_columns_are_tokenized()
    test_another_postgres_deterministic_same_value_yields_same_token()
    test_another_postgres_only_sensitive_columns_are_tokenized()
    print("OK: test_token_generation")


if __name__ == "__main__":
    sys.exit(main())
