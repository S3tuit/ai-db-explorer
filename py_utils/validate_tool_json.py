#!/usr/bin/env python3
"""Validate tool payload JSON against the schemas documented in docs/tools.md.

This script treats the fenced `json` blocks in docs/tools.md as the canonical
tool definitions. Each block must decode to an object with a unique `name`
field and both `inputSchema` and `outputSchema` objects.

Usage examples:
  echo '{"connections":[]}' | python3 py_utils/validate_tool_json.py \
      list_database_connections output

  python3 py_utils/validate_tool_json.py describe_relation input \
      '{"connectionName":"prod","schemaName":"public","relationName":"users"}'
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

try:
    from jsonschema import Draft202012Validator
except ImportError as exc:  # pragma: no cover - environment issue
    print(f"Error: missing dependency 'jsonschema': {exc}", file=sys.stderr)
    raise SystemExit(2)


DEFAULT_TOOLS_MD = Path(__file__).resolve().parent.parent / "docs" / "tools.md"
JSON_BLOCK_RE = re.compile(r"```json\s*\n(.*?)\n```", re.DOTALL)


class ToolSchemaError(RuntimeError):
    """Raised when docs/tools.md cannot be parsed into tool definitions."""


def _format_error_path(error_path: list[Any]) -> str:
    if not error_path:
        return "$"
    parts = ["$"]
    for part in error_path:
        if isinstance(part, int):
            parts.append(f"[{part}]")
        else:
            parts.append(f".{part}")
    return "".join(parts)


def load_tool_definitions(tools_md_path: Path = DEFAULT_TOOLS_MD) -> dict[str, dict[str, Any]]:
    """Load all tool definition JSON objects from docs/tools.md."""
    try:
        text = tools_md_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ToolSchemaError(f"unable to read '{tools_md_path}': {exc}") from exc

    tool_defs: dict[str, dict[str, Any]] = {}
    for match in JSON_BLOCK_RE.finditer(text):
        block = match.group(1)
        try:
            tool_def = json.loads(block)
        except json.JSONDecodeError as exc:
            raise ToolSchemaError(
                f"invalid JSON code block in '{tools_md_path}': {exc}"
            ) from exc

        if not isinstance(tool_def, dict):
            raise ToolSchemaError("each tool JSON block must decode to an object")

        name = tool_def.get("name")
        if not isinstance(name, str) or not name:
            raise ToolSchemaError("each tool JSON block must contain a non-empty 'name'")
        if name in tool_defs:
            raise ToolSchemaError(f"duplicate tool definition for '{name}'")

        if not isinstance(tool_def.get("inputSchema"), dict):
            raise ToolSchemaError(f"tool '{name}' is missing an object 'inputSchema'")
        if not isinstance(tool_def.get("outputSchema"), dict):
            raise ToolSchemaError(f"tool '{name}' is missing an object 'outputSchema'")

        tool_defs[name] = tool_def

    if not tool_defs:
        raise ToolSchemaError(f"no tool definition JSON blocks found in '{tools_md_path}'")
    return tool_defs


def get_tool_schema(
    tool_name: str, schema_kind: str, tools_md_path: Path = DEFAULT_TOOLS_MD
) -> dict[str, Any]:
    """Return one schema from docs/tools.md after validating the schema itself."""
    if schema_kind not in {"input", "output"}:
        raise ToolSchemaError(f"unsupported schema kind '{schema_kind}'")

    tool_defs = load_tool_definitions(tools_md_path)
    tool_def = tool_defs.get(tool_name)
    if tool_def is None:
        known = ", ".join(sorted(tool_defs))
        raise ToolSchemaError(f"unknown tool '{tool_name}'. Known tools: {known}")

    schema_key = "inputSchema" if schema_kind == "input" else "outputSchema"
    schema = tool_def[schema_key]
    Draft202012Validator.check_schema(schema)
    return schema


def validate_instance(
    instance: Any, tool_name: str, schema_kind: str, tools_md_path: Path = DEFAULT_TOOLS_MD
) -> list[str]:
    """Validate one JSON instance and return human-readable error strings."""
    schema = get_tool_schema(tool_name, schema_kind, tools_md_path)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(instance), key=lambda err: list(err.absolute_path))
    return [f"{_format_error_path(list(err.absolute_path))}: {err.message}" for err in errors]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a JSON payload against one tool schema in docs/tools.md."
    )
    parser.add_argument("tool_name", help="Tool name from docs/tools.md")
    parser.add_argument(
        "schema_kind",
        choices=("input", "output"),
        help="Validate against the tool inputSchema or outputSchema",
    )
    parser.add_argument(
        "json_text",
        nargs="?",
        help="JSON instance to validate. If omitted, the script reads JSON from stdin.",
    )
    parser.add_argument(
        "--tools-md",
        default=str(DEFAULT_TOOLS_MD),
        help="Path to the tools.md file to load. Defaults to %(default)s",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    if args.json_text is not None:
        payload_text = args.json_text
    else:
        if sys.stdin.isatty():
            print(
                "Error: missing JSON instance. Pass it as an argument or via stdin.",
                file=sys.stderr,
            )
            return 2
        payload_text = sys.stdin.read()

    try:
        instance = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        print(f"Error: invalid JSON instance: {exc}", file=sys.stderr)
        return 2

    try:
        errors = validate_instance(
            instance,
            args.tool_name,
            args.schema_kind,
            Path(args.tools_md),
        )
    except ToolSchemaError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Error: schema validation failed unexpectedly: {exc}", file=sys.stderr)
        return 2

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    print("VALID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
