#!/usr/bin/env python3
import json
import shutil
import sys
from pathlib import Path

from test_broker_mcp_handshake import do_full_handshake
from test_user_mcp_handshake import read_frame, stop_proc, write_frame

ROOT = Path(__file__).resolve().parents[3]
TOOLS_JSON = ROOT / "meta" / "tools.json"


def send_tools_list_request(server, req_id, params=None):
    req = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/list",
    }
    if params is not None:
        req["params"] = params
    write_frame(server, json.dumps(req).encode("utf-8"))
    return json.loads(read_frame(server).decode("utf-8"))


def load_expected_tools():
    manifest = json.loads(TOOLS_JSON.read_text(encoding="utf-8"))
    return manifest["tools"]


def test_tools_list_ok_and_ignores_unused_params():
    broker = None
    server = None
    privdir = None
    runtime_dir = None
    try:
        broker, server, privdir, runtime_dir, resp = do_full_handshake(req_id=31)
        assert resp["jsonrpc"] == "2.0"

        expected_tools = load_expected_tools()
        resp = send_tools_list_request(
            server,
            "tools-list-ok",
            {
                "cursor": "unused-host-cursor",
                "_meta": {"transportHint": "host-added"},
            },
        )
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == "tools-list-ok"
        assert "error" not in resp
        assert "nextCursor" not in resp["result"]
        assert resp["result"]["tools"] == expected_tools

    finally:
        stop_proc(server)
        stop_proc(broker)
        if privdir:
            shutil.rmtree(privdir, ignore_errors=True)
        if runtime_dir:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def main():
    test_tools_list_ok_and_ignores_unused_params()
    print("OK: test_tools_list")


if __name__ == "__main__":
    sys.exit(main())
