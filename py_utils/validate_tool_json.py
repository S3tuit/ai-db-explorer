#!/usr/bin/env python3
"""Validate tool payload JSON against the canonical meta/tools.json manifest.

This script treats meta/tools.json as the canonical tool manifest. The manifest
itself is first validated against meta/tool_manifest.schema.json, then every
embedded inputSchema and outputSchema is validated against JSON Schema
2020-12, and only then the selected payload instance is checked.

Usage examples:
  echo '{"connections":[]}' | python3 py_utils/validate_tool_json.py \
      list_database_connections output

  python3 py_utils/validate_tool_json.py describe_relation input \
      '{"connectionName":"prod","schemaName":"public","relationName":"users"}'
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from jsonschema import Draft202012Validator
except ImportError as exc:  # pragma: no cover - environment issue
    print(f"Error: missing dependency 'jsonschema': {exc}", file=sys.stderr)
    raise SystemExit(2)


META_DIR = Path(__file__).resolve().parent.parent / "meta"
DEFAULT_TOOLS_JSON = META_DIR / "tools.json"
DEFAULT_MANIFEST_SCHEMA = META_DIR / "tool_manifest.schema.json"


class ToolSchemaError(RuntimeError):
    """Raised when the canonical tool manifest cannot be loaded or validated."""


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


def _load_json_file(path: Path) -> Any:
    """Load one JSON file from disk.

    It borrows 'path' and returns the decoded JSON value.
    Raises ToolSchemaError on I/O or JSON parse failure.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ToolSchemaError(f"unable to read '{path}': {exc}") from exc

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ToolSchemaError(f"invalid JSON in '{path}': {exc}") from exc


def load_tool_definitions(
    tools_json_path: Path = DEFAULT_TOOLS_JSON,
    manifest_schema_path: Path = DEFAULT_MANIFEST_SCHEMA,
) -> dict[str, dict[str, Any]]:
    """Load and validate all tool definitions from meta/tools.json.

    It validates the manifest structure against tool_manifest.schema.json, then
    validates each embedded inputSchema and outputSchema against JSON Schema
    2020-12. Returns a dict keyed by tool name.
    Raises ToolSchemaError on malformed manifest content or invalid schemas.
    """
    manifest_schema = _load_json_file(manifest_schema_path)
    tool_manifest = _load_json_file(tools_json_path)

    try:
        Draft202012Validator.check_schema(manifest_schema)
        Draft202012Validator(manifest_schema).validate(tool_manifest)
    except Exception as exc:
        raise ToolSchemaError(
            f"tool manifest '{tools_json_path}' does not match "
            f"'{manifest_schema_path}': {exc}"
        ) from exc

    tools = tool_manifest.get("tools")
    if not isinstance(tools, list) or not tools:
        raise ToolSchemaError(f"manifest '{tools_json_path}' is missing tools[]")

    tool_defs: dict[str, dict[str, Any]] = {}
    for tool_def in tools:
        if not isinstance(tool_def, dict):
            raise ToolSchemaError("each manifest tool entry must be an object")

        name = tool_def.get("name")
        if not isinstance(name, str) or not name:
            raise ToolSchemaError("each tool must contain a non-empty 'name'")
        if name in tool_defs:
            raise ToolSchemaError(f"duplicate tool definition for '{name}'")

        input_schema = tool_def.get("inputSchema")
        output_schema = tool_def.get("outputSchema")
        if not isinstance(input_schema, dict):
            raise ToolSchemaError(f"tool '{name}' is missing an object 'inputSchema'")
        if not isinstance(output_schema, dict):
            raise ToolSchemaError(
                f"tool '{name}' is missing an object 'outputSchema'"
            )

        try:
            Draft202012Validator.check_schema(input_schema)
        except Exception as exc:
            raise ToolSchemaError(
                f"tool '{name}' has invalid inputSchema: {exc}"
            ) from exc
        try:
            Draft202012Validator.check_schema(output_schema)
        except Exception as exc:
            raise ToolSchemaError(
                f"tool '{name}' has invalid outputSchema: {exc}"
            ) from exc

        tool_defs[name] = tool_def

    return tool_defs


def get_tool_schema(
    tool_name: str,
    schema_kind: str,
    tools_json_path: Path = DEFAULT_TOOLS_JSON,
    manifest_schema_path: Path = DEFAULT_MANIFEST_SCHEMA,
) -> dict[str, Any]:
    """Return one validated tool schema from the canonical tool manifest."""
    if schema_kind not in {"input", "output"}:
        raise ToolSchemaError(f"unsupported schema kind '{schema_kind}'")

    tool_defs = load_tool_definitions(tools_json_path, manifest_schema_path)
    tool_def = tool_defs.get(tool_name)
    if tool_def is None:
        known = ", ".join(sorted(tool_defs))
        raise ToolSchemaError(f"unknown tool '{tool_name}'. Known tools: {known}")

    schema_key = "inputSchema" if schema_kind == "input" else "outputSchema"
    return tool_def[schema_key]


def validate_instance(
    instance: Any,
    tool_name: str,
    schema_kind: str,
    tools_json_path: Path = DEFAULT_TOOLS_JSON,
    manifest_schema_path: Path = DEFAULT_MANIFEST_SCHEMA,
) -> list[str]:
    """Validate one JSON instance and return human-readable error strings."""
    schema = get_tool_schema(
        tool_name,
        schema_kind,
        tools_json_path,
        manifest_schema_path,
    )
    validator = Draft202012Validator(schema)
    errors = sorted(
        validator.iter_errors(instance), key=lambda err: list(err.absolute_path)
    )
    return [
        f"{_format_error_path(list(err.absolute_path))}: {err.message}"
        for err in errors
    ]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a JSON payload against one tool schema in meta/tools.json."
    )
    parser.add_argument("tool_name", help="Tool name from meta/tools.json")
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
        "--tools-json",
        default=str(DEFAULT_TOOLS_JSON),
        help="Path to the tools.json manifest to load. Defaults to %(default)s",
    )
    parser.add_argument(
        "--manifest-schema",
        default=str(DEFAULT_MANIFEST_SCHEMA),
        help="Path to the tool manifest schema. Defaults to %(default)s",
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
            Path(args.tools_json),
            Path(args.manifest_schema),
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
