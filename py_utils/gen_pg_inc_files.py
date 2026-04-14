#!/usr/bin/env python3
"""Generate Postgres safe-allowlist includes from canonical JSON files.

The JSON files under meta/ are the canonical manually-edited sources. In normal
mode this script validates that those files are already normalized, then emits
the generated C includes. With --normalize it sorts, deduplicates, rewrites the
JSON sources, and then emits the generated includes.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Callable


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SAFE_FUNCTIONS_JSON = REPO_ROOT / "meta" / "pg_safe_functions.json"
DEFAULT_SAFE_FUNCTIONS_INC = REPO_ROOT / "src" / "pg_safe_func.generated.inc"
DEFAULT_SAFE_OPERATORS_JSON = REPO_ROOT / "meta" / "pg_safe_operators.json"
DEFAULT_SAFE_OPERATORS_INC = REPO_ROOT / "src" / "pg_safe_operator.generated.inc"

SAFE_FUNC_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
SAFE_OPERATOR_WORD_RE = re.compile(r"^[A-Z]+(?: [A-Z]+)*$")
SAFE_OPERATOR_SYMBOL_RE = re.compile(r"^[!#%&*+\-./<=>?@^|~]+$")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate Postgres safe-function and safe-operator includes from "
            "their canonical JSON files."
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
        help="Path to the generated safe-function include output.",
    )
    parser.add_argument(
        "--safe-operators-json",
        default=str(DEFAULT_SAFE_OPERATORS_JSON),
        help="Path to the canonical Postgres safe-operator JSON file.",
    )
    parser.add_argument(
        "--safe-operators-inc-out",
        default=str(DEFAULT_SAFE_OPERATORS_INC),
        help="Path to the generated safe-operator include output.",
    )
    parser.add_argument(
        "--normalize",
        action="store_true",
        help="Sort, deduplicate, and rewrite the JSON sources before emitting C.",
    )
    return parser.parse_args()


def _load_json_array(
    path: Path, key: str, validate_item: Callable[[str, Path, int], None]
) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"file '{path}' must decode to an object")
    if set(data.keys()) != {key}:
        raise RuntimeError(f"file '{path}' must contain only '{key}'")

    items = data[key]
    if not isinstance(items, list):
        raise RuntimeError(f"'{key}' in '{path}' must be an array")
    if len(items) == 0:
        raise RuntimeError(f"'{key}' in '{path}' must not be empty")

    out: list[str] = []
    for idx, item in enumerate(items):
        if not isinstance(item, str):
            raise RuntimeError(f"'{key}[{idx}]' in '{path}' must be a string")
        if item == "":
            raise RuntimeError(f"'{key}[{idx}]' in '{path}' must not be empty")
        validate_item(item, path, idx)
        out.append(item)
    return out


def _validate_safe_function(name: str, path: Path, idx: int) -> None:
    if SAFE_FUNC_RE.match(name):
        return
    raise RuntimeError(
        f"'safeFunctions[{idx}]' in '{path}' must be lowercase and "
        f"unqualified, got '{name}'"
    )


def _validate_safe_operator(token: str, path: Path, idx: int) -> None:
    if token != token.strip() or "  " in token:
        raise RuntimeError(
            f"'safeOperators[{idx}]' in '{path}' must not have extra spaces"
        )
    if SAFE_OPERATOR_WORD_RE.match(token) or SAFE_OPERATOR_SYMBOL_RE.match(token):
        return
    raise RuntimeError(
        f"'safeOperators[{idx}]' in '{path}' must be an uppercase keyword token "
        f"or symbolic operator, got '{token}'"
    )


def _canonical_json_text(key: str, items: list[str]) -> str:
    payload = {key: sorted(set(items))}
    return json.dumps(payload, indent=2) + "\n"


def _c_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _render_include(
    source_json: str, array_decl: str, items: list[str]
) -> str:
    parts = [
        f"/* Generated from {source_json} by py_utils/gen_pg_inc_files.py.",
        " * Do not edit manually.",
        " */",
        array_decl,
    ]
    for item in sorted(set(items)):
        parts.append(f"    {_c_string_literal(item)},")
    parts.append("};")
    parts.append("")
    return "\n".join(parts)


def _normalize_or_validate_json(
    path: Path, key: str, items: list[str], normalize: bool
) -> None:
    canonical_json = _canonical_json_text(key, items)
    if normalize:
        path.write_text(canonical_json, encoding="utf-8")
        return

    actual_json = path.read_text(encoding="utf-8")
    if actual_json != canonical_json:
        raise RuntimeError(
            f"file '{path}' is not normalized. "
            "Run `make gen-files` or re-run this script with --normalize."
        )


def main() -> int:
    args = _parse_args()

    functions_json_path = Path(args.safe_functions_json)
    functions_inc_path = Path(args.safe_functions_inc_out)
    operators_json_path = Path(args.safe_operators_json)
    operators_inc_path = Path(args.safe_operators_inc_out)

    functions = _load_json_array(
        functions_json_path, "safeFunctions", _validate_safe_function
    )
    operators = _load_json_array(
        operators_json_path, "safeOperators", _validate_safe_operator
    )

    _normalize_or_validate_json(
        functions_json_path, "safeFunctions", functions, args.normalize
    )
    _normalize_or_validate_json(
        operators_json_path, "safeOperators", operators, args.normalize
    )

    functions_inc_path.write_text(
        _render_include(
            "meta/pg_safe_functions.json",
            "static const char *PG_SAFE_FUNCS[] = {",
            functions,
        ),
        encoding="utf-8",
    )
    operators_inc_path.write_text(
        _render_include(
            "meta/pg_safe_operators.json",
            "static const char *const PG_SAFE_OPERATOR_TOKENS[] = {",
            operators,
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
