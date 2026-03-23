#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "py_utils"))

from validate_tool_json import validate_instance


def assert_tool_payload_valid(tool_name, schema_kind, payload):
    errors = validate_instance(payload, tool_name, schema_kind)
    assert not errors, "\n".join(errors)
