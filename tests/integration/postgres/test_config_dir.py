#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import tempfile

from test_user_mcp_handshake import (
    BIN,
    CONFIG,
    ROOT,
    make_temp_appdir,
    merge_env,
    start_broker,
    stop_proc,
)


def _make_tmpdir(prefix):
    build_dir = os.path.join(ROOT, "build")
    os.makedirs(build_dir, exist_ok=True)
    return tempfile.mkdtemp(prefix=f"{prefix}-", dir=build_dir)


def test_explicit_config_overrides_xdg_default_path():
    xdg_home = _make_tmpdir("cfg-explicit-xdg")
    appdir = make_temp_appdir("cfg-explicit")
    broker = None
    try:
        broker = start_broker(
            appdir,
            env={"XDG_CONFIG_HOME": xdg_home},
            config_path=CONFIG,
        )

        default_path = os.path.join(xdg_home, "adbxplorer", "config.json")
        assert not os.path.exists(default_path)
    finally:
        stop_proc(broker)
        shutil.rmtree(appdir, ignore_errors=True)
        shutil.rmtree(xdg_home, ignore_errors=True)


def test_home_fallback_missing_default_config_fails_without_xdg():
    home_dir = _make_tmpdir("cfg-home-fallback")
    appdir = make_temp_appdir("cfg-home-fallback")
    try:
        proc = subprocess.run(
            [BIN, "-broker", "-appdir", appdir],
            cwd=ROOT,
            capture_output=True,
            text=True,
            env=merge_env(
                {
                    "HOME": home_dir,
                    "XDG_CONFIG_HOME": None,
                }
            ),
        )

        expected = os.path.join(home_dir, ".config", "adbxplorer", "config.json")
        assert proc.returncode != 0
        assert "default configuration file does not exist" in proc.stderr
        assert expected in proc.stderr
        assert not os.path.exists(expected)
    finally:
        shutil.rmtree(appdir, ignore_errors=True)
        shutil.rmtree(home_dir, ignore_errors=True)


def main():
    test_explicit_config_overrides_xdg_default_path()
    test_home_fallback_missing_default_config_fails_without_xdg()
    print("OK: test_config_dir")


if __name__ == "__main__":
    sys.exit(main())
