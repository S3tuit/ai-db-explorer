#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import tempfile
import time

from test_user_mcp_handshake import (
    BIN,
    CONFIG,
    ROOT,
    make_temp_privdir,
    merge_env,
    start_broker,
    stop_proc,
)


def _make_tmpdir(prefix):
    build_dir = os.path.join(ROOT, "build")
    os.makedirs(build_dir, exist_ok=True)
    return tempfile.mkdtemp(prefix=f"{prefix}-", dir=build_dir)


def _wait_for_file(path, timeout_sec=3.0):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if os.path.isfile(path):
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for file: {path}")


def test_xdg_empty_dir_creates_default_config():
    xdg_home = _make_tmpdir("cfg-xdg-empty")
    privdir = make_temp_privdir("cfg-xdg-empty")
    broker = None
    try:
        broker = start_broker(privdir, env={"XDG_CONFIG_HOME": xdg_home})

        expected = os.path.join(xdg_home, "adbxplorer", "config.json")
        _wait_for_file(expected)
        assert os.path.getsize(expected) > 0
    finally:
        stop_proc(broker)
        shutil.rmtree(privdir, ignore_errors=True)
        shutil.rmtree(xdg_home, ignore_errors=True)


def test_explicit_config_overrides_xdg_default_path():
    xdg_home = _make_tmpdir("cfg-explicit-xdg")
    privdir = make_temp_privdir("cfg-explicit")
    broker = None
    try:
        broker = start_broker(
            privdir,
            env={"XDG_CONFIG_HOME": xdg_home},
            config_path=CONFIG,
        )

        default_path = os.path.join(xdg_home, "adbxplorer", "config.json")
        assert not os.path.exists(default_path)
    finally:
        stop_proc(broker)
        shutil.rmtree(privdir, ignore_errors=True)
        shutil.rmtree(xdg_home, ignore_errors=True)


def test_home_fallback_creates_default_config_without_xdg():
    home_dir = _make_tmpdir("cfg-home-fallback")
    privdir = make_temp_privdir("cfg-home-fallback")
    broker = None
    try:
        broker = start_broker(
            privdir,
            env={
                "HOME": home_dir,
                "XDG_CONFIG_HOME": None,
            },
        )

        expected = os.path.join(home_dir, ".config", "adbxplorer", "config.json")
        _wait_for_file(expected)
        assert os.path.getsize(expected) > 0
    finally:
        stop_proc(broker)
        shutil.rmtree(privdir, ignore_errors=True)
        shutil.rmtree(home_dir, ignore_errors=True)


def main():
    test_xdg_empty_dir_creates_default_config()
    test_explicit_config_overrides_xdg_default_path()
    test_home_fallback_creates_default_config_without_xdg()
    print("OK: test_config_dir")


if __name__ == "__main__":
    sys.exit(main())
