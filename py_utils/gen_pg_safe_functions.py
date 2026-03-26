#!/usr/bin/env python3
"""Generate the Postgres safe-function include from docs/pg_safe_functions.json.

The JSON file is the canonical manually-edited source. In normal mode this
script validates that the file is already normalized, then emits the C include.
With --normalize it sorts, deduplicates, rewrites the JSON file, and then emits
the generated include.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


DEFAULT_SAFE_FUNCTIONS_JSON = (
    Path(__file__).resolve().parent.parent / "docs" / "pg_safe_functions.json"
)
DEFAULT_SAFE_FUNCTIONS_INC = (
    Path(__file__).resolve().parent.parent / "src" / "pg_safe_func.generated.inc"
)
SAFE_FUNC_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate src/pg_safe_func.generated.inc from "
            "docs/pg_safe_functions.json."
        )
    )
    parser.add_argument(
        "--safe-functions-json",
        default=str(DEFAULT_SAFE_FUNCTIONS_JSON),
        help="Path to the canonical Postgres safe-function JSON file.",
    )
    parser.add_argument(
        "--safe-functions-inc-out",
        default=str(DEFAULT_SAFE_FUNCTIONS_INC),
        help="Path to the generated C include output.",
    )
    parser.add_argument(
        "--normalize",
        action="store_true",
        help="Sort, deduplicate, and rewrite the JSON source before emitting C.",
    )
    return parser.parse_args()


def _load_safe_functions(path: Path) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"safe-function file '{path}' must decode to an object")
    if set(data.keys()) != {"safeFunctions"}:
        raise RuntimeError(
            f"safe-function file '{path}' must contain only 'safeFunctions'"
        )

    items = data["safeFunctions"]
    if not isinstance(items, list):
        raise RuntimeError(f"'safeFunctions' in '{path}' must be an array")
    if len(items) == 0:
        raise RuntimeError(f"'safeFunctions' in '{path}' must not be empty")

    out: list[str] = []
    for idx, item in enumerate(items):
        if not isinstance(item, str):
            raise RuntimeError(
                f"'safeFunctions[{idx}]' in '{path}' must be a string"
            )
        if item == "":
            raise RuntimeError(
                f"'safeFunctions[{idx}]' in '{path}' must not be empty"
            )
        if not SAFE_FUNC_RE.match(item):
            raise RuntimeError(
                f"'safeFunctions[{idx}]' in '{path}' must be lowercase and "
                f"unqualified, got '{item}'"
            )
        out.append(item)
    return out


def _canonical_json_text(functions: list[str]) -> str:
    payload = {"safeFunctions": sorted(set(functions))}
    return json.dumps(payload, indent=2) + "\n"


def _render_include(functions: list[str]) -> str:
    parts = [
        "/* Generated from docs/pg_safe_functions.json by "
        "py_utils/gen_pg_safe_functions.py.",
        " * Do not edit manually.",
        " */",
        "static const char *PG_SAFE_FUNCS[] = {",
    ]
    for name in sorted(set(functions)):
        parts.append(f'    "{name}",')
    parts.append("};")
    parts.append("")
    return "\n".join(parts)


def main() -> int:
    args = _parse_args()
    json_path = Path(args.safe_functions_json)
    inc_path = Path(args.safe_functions_inc_out)

    functions = _load_safe_functions(json_path)
    canonical_json = _canonical_json_text(functions)
    if args.normalize:
        json_path.write_text(canonical_json, encoding="utf-8")
    else:
        actual_json = json_path.read_text(encoding="utf-8")
        if actual_json != canonical_json:
            raise RuntimeError(
                f"safe-function file '{json_path}' is not normalized. "
                "Run `make gen-files` or re-run this script with --normalize."
            )

    inc_path.write_text(_render_include(functions), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
