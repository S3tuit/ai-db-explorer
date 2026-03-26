#!/usr/bin/env python3
"""Generate human-readable and C-consumable tool artifacts from docs/tools.json.

This script treats docs/tools.json as the canonical MCP tool manifest. It
validates the manifest and all embedded schemas via validate_tool_json.py, then
emits:
  - docs/tools.md for human-readable review
  - src/tool_defs.generated.inc for broker_response.c consumption
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from validate_tool_json import DEFAULT_MANIFEST_SCHEMA, DEFAULT_TOOLS_JSON, load_tool_definitions


DEFAULT_TOOLS_MD = Path(__file__).resolve().parent.parent / "docs" / "tools.md"
DEFAULT_TOOL_INC = (
    Path(__file__).resolve().parent.parent / "src" / "tool_defs.generated.inc"
)


def _load_manifest(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise RuntimeError(f"manifest '{path}' must decode to an object")
    return data


def _render_tools_md(manifest: dict[str, Any]) -> str:
    version = manifest["mcpSpecVersion"]
    tools = manifest["tools"]

    parts = [
        "<!-- Generated from docs/tools.json by py_utils/gen_tool_artifacts.py. Do not edit manually. -->",
        "",
        "# Tool Definitions",
        "",
        f"Generated from `docs/tools.json` and aligned with MCP `{version}`.",
        "",
    ]

    for tool in tools:
        parts.extend(
            [
                f"## {tool['name']}",
                "",
                "```json",
                json.dumps(tool, indent=2, ensure_ascii=False),
                "```",
                "",
            ]
        )

    return "\n".join(parts)


def _c_string_literal_chunks(text: str, width: int = 76) -> str:
    chunks = [text[i : i + width] for i in range(0, len(text), width)] or [""]
    return "\n".join(f"        {json.dumps(chunk, ensure_ascii=False)}" for chunk in chunks)


def _render_tool_defs_inc(manifest: dict[str, Any]) -> str:
    tools = manifest["tools"]
    parts = [
        "/* Generated from docs/tools.json by py_utils/gen_tool_artifacts.py.",
        " * Do not edit manually.",
        " */",
        "static const BrespToolJsonDef bresp_tool_defs[] = {",
    ]

    for tool in tools:
        tool_json = json.dumps(tool, separators=(",", ":"), ensure_ascii=False)
        parts.extend(
            [
                "    {",
                f"        .name = {json.dumps(tool['name'], ensure_ascii=False)},",
                "        .tool_json =",
                _c_string_literal_chunks(tool_json),
                "    },",
            ]
        )

    parts.append("};")
    parts.append("")
    return "\n".join(parts)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate docs/tools.md and src/tool_defs.generated.inc from docs/tools.json."
    )
    parser.add_argument(
        "--tools-json",
        default=str(DEFAULT_TOOLS_JSON),
        help="Path to the canonical tool manifest. Defaults to %(default)s",
    )
    parser.add_argument(
        "--manifest-schema",
        default=str(DEFAULT_MANIFEST_SCHEMA),
        help="Path to the tool manifest schema. Defaults to %(default)s",
    )
    parser.add_argument(
        "--tools-md-out",
        default=str(DEFAULT_TOOLS_MD),
        help="Path to the generated Markdown output. Defaults to %(default)s",
    )
    parser.add_argument(
        "--tool-inc-out",
        default=str(DEFAULT_TOOL_INC),
        help="Path to the generated C include. Defaults to %(default)s",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    tools_json = Path(args.tools_json)
    manifest_schema = Path(args.manifest_schema)
    tools_md_out = Path(args.tools_md_out)
    tool_inc_out = Path(args.tool_inc_out)

    load_tool_definitions(tools_json, manifest_schema)
    manifest = _load_manifest(tools_json)

    tools_md_out.write_text(_render_tools_md(manifest), encoding="utf-8")
    tool_inc_out.write_text(_render_tool_defs_inc(manifest), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
