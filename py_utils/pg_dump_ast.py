#!/usr/bin/env python3
"""
Runs the local libpg_query-based AST dumper and prints the JSON parse tree.
Usage:
  echo "SELECT 1" | py_utils/pg_dump_ast.py
  py_utils/pg_dump_ast.py "SELECT 1"
"""
import os
import json
import subprocess
import sys


def main() -> int:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    bin_path = os.path.join(repo_root, "build", "pg_dump_ast")
    if not os.path.exists(bin_path):
        sys.stderr.write(
            "error: build/pg_dump_ast not found. Run `make pg-dump-ast` first.\n"
        )
        return 2

    if len(sys.argv) > 1:
        sql = " ".join(sys.argv[1:]).encode("utf-8")
    else:
        sql = sys.stdin.buffer.read()

    if not sql:
        sys.stderr.write("error: no SQL provided (use stdin or args)\n")
        return 2

    proc = subprocess.run(
        [bin_path],
        input=sql,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        check=False,
    )
    if proc.returncode != 0:
        sys.stdout.buffer.write(proc.stdout)
        return proc.returncode

    raw = proc.stdout.decode("utf-8", errors="replace")
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        sys.stdout.write(raw)
        return 0

    sys.stdout.write(json.dumps(obj, indent=2, ensure_ascii=False))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
