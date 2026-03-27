#!/usr/bin/env python3
"""Checks vendored dependencies against docs/vendor_manifest.json.

This script supports:
  - verify: ensure the working tree matches the pinned upstream state
  - freshness: compare pinned refs against the latest stable GitHub releases
  - all: run both checks in sequence

The manifest is intentionally strict and only supports the GitHub-backed vendor
forms currently used by this repository.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST = ROOT / "docs" / "vendor_manifest.json"
GITHUB_UA = "adbxplorer-vendor-check"
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
SERIES_TAG_RE = re.compile(r"^(?P<series>\d+)-(?P<version>\d+\.\d+\.\d+)$")


class ManifestError(RuntimeError):
    """Raised when docs/vendor_manifest.json is malformed."""


class CheckError(RuntimeError):
    """Raised when one vendor check cannot be completed safely."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify vendored files and compare them with upstream releases."
    )
    parser.add_argument(
        "command",
        choices=("verify", "freshness", "all"),
        help="Which check to run.",
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST),
        help="Path to docs/vendor_manifest.json.",
    )
    parser.add_argument(
        "--vendor",
        action="append",
        dest="vendors",
        default=[],
        help="Limit checks to one or more vendor names from the manifest.",
    )
    return parser.parse_args()


def _validate_rel_path(raw: Any, field_name: str) -> str:
    if not isinstance(raw, str) or raw == "":
        raise ManifestError(f"{field_name} must be a non-empty string")
    pp = PurePosixPath(raw)
    if pp.is_absolute() or ".." in pp.parts:
        raise ManifestError(f"{field_name} must stay within the repository")
    return raw


def _validate_repo(raw: Any, field_name: str) -> str:
    if not isinstance(raw, str) or not REPO_RE.match(raw):
        raise ManifestError(f"{field_name} must be in 'owner/name' format")
    return raw


def _validate_manifest(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ManifestError("vendor manifest must decode to an object")
    if set(data.keys()) != {"version", "vendors"}:
        raise ManifestError("vendor manifest must contain only 'version' and 'vendors'")
    if data["version"] != "1":
        raise ManifestError("vendor manifest version must be '1'")
    vendors = data["vendors"]
    if not isinstance(vendors, list) or len(vendors) == 0:
        raise ManifestError("'vendors' must be a non-empty array")

    seen_names: set[str] = set()
    for idx, vendor in enumerate(vendors):
        where = f"vendors[{idx}]"
        if not isinstance(vendor, dict):
            raise ManifestError(f"{where} must be an object")
        required = {"name", "kind", "path", "source", "pin", "freshness"}
        if set(vendor.keys()) != required:
            raise ManifestError(f"{where} must contain only {sorted(required)}")

        name = vendor["name"]
        if not isinstance(name, str) or name == "":
            raise ManifestError(f"{where}.name must be a non-empty string")
        if name in seen_names:
            raise ManifestError(f"{where}.name '{name}' is duplicated")
        seen_names.add(name)

        kind = vendor["kind"]
        if kind not in ("git_submodule", "single_file"):
            raise ManifestError(f"{where}.kind must be 'git_submodule' or 'single_file'")
        vendor["path"] = _validate_rel_path(vendor["path"], f"{where}.path")

        source = vendor["source"]
        if not isinstance(source, dict):
            raise ManifestError(f"{where}.source must be an object")
        if source.get("host") != "github":
            raise ManifestError(f"{where}.source.host must be 'github'")
        source["repo"] = _validate_repo(source.get("repo"), f"{where}.source.repo")
        if kind == "single_file":
            if set(source.keys()) != {"host", "repo", "file"}:
                raise ManifestError(
                    f"{where}.source must contain only 'host', 'repo', and 'file'"
                )
            source["file"] = _validate_rel_path(source["file"], f"{where}.source.file")
        else:
            if set(source.keys()) != {"host", "repo"}:
                raise ManifestError(f"{where}.source must contain only 'host' and 'repo'")

        pin = vendor["pin"]
        if not isinstance(pin, dict):
            raise ManifestError(f"{where}.pin must be an object")
        if kind == "git_submodule":
            if set(pin.keys()) != {"commit", "tag"}:
                raise ManifestError(
                    f"{where}.pin must contain only 'commit' and 'tag' for submodules"
                )
            if not isinstance(pin["commit"], str) or re.fullmatch(r"[0-9a-f]{40}", pin["commit"]) is None:
                raise ManifestError(f"{where}.pin.commit must be a 40-char hex sha")
            if not isinstance(pin["tag"], str) or pin["tag"] == "":
                raise ManifestError(f"{where}.pin.tag must be a non-empty string")
        else:
            if set(pin.keys()) != {"ref", "sha256"}:
                raise ManifestError(
                    f"{where}.pin must contain only 'ref' and 'sha256' for single files"
                )
            if not isinstance(pin["ref"], str) or pin["ref"] == "":
                raise ManifestError(f"{where}.pin.ref must be a non-empty string")
            if not isinstance(pin["sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", pin["sha256"]) is None:
                raise ManifestError(f"{where}.pin.sha256 must be a 64-char hex sha256")

        freshness = vendor["freshness"]
        if not isinstance(freshness, dict):
            raise ManifestError(f"{where}.freshness must be an object")
        allowed = {"mode", "includePrereleases", "series"}
        if any(key not in allowed for key in freshness.keys()):
            raise ManifestError(f"{where}.freshness contains unsupported keys")
        if freshness.get("mode") != "github_releases":
            raise ManifestError(f"{where}.freshness.mode must be 'github_releases'")
        if not isinstance(freshness.get("includePrereleases"), bool):
            raise ManifestError(f"{where}.freshness.includePrereleases must be boolean")
        if "series" in freshness:
            if not isinstance(freshness["series"], str) or not freshness["series"].isdigit():
                raise ManifestError(f"{where}.freshness.series must be a numeric string")
    return vendors


def _select_vendors(vendors: list[dict[str, Any]], names: list[str]) -> list[dict[str, Any]]:
    if not names:
        return vendors
    by_name = {vendor["name"]: vendor for vendor in vendors}
    selected: list[dict[str, Any]] = []
    for name in names:
        vendor = by_name.get(name)
        if not vendor:
            raise ManifestError(f"unknown vendor '{name}'")
        selected.append(vendor)
    return selected


def _git(*args: str, cwd: Path | None = None) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd) if cwd else str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise CheckError(proc.stderr.strip() or "git command failed")
    return proc.stdout


def _github_json(url: str) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": GITHUB_UA,
            "Accept": "application/vnd.github+json",
        },
    )
    with urlopen(req, timeout=20) as resp:
        return json.load(resp)


def _github_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": GITHUB_UA})
    with urlopen(req, timeout=20) as resp:
        return resp.read()


def _github_raw_file(repo: str, ref: str, rel_path: str) -> bytes:
    url = (
        f"https://raw.githubusercontent.com/{repo}/{quote(ref, safe='')}/"
        f"{quote(rel_path, safe='/')}"
    )
    return _github_bytes(url)


def _github_resolve_commit(repo: str, ref: str) -> str:
    data = _github_json(
        f"https://api.github.com/repos/{repo}/commits/{quote(ref, safe='')}"
    )
    sha = data.get("sha")
    if not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{40}", sha) is None:
        raise CheckError(f"unable to resolve commit for {repo}@{ref}")
    return sha


def _github_releases(repo: str, include_prereleases: bool) -> list[dict[str, Any]]:
    data = _github_json(f"https://api.github.com/repos/{repo}/releases?per_page=100")
    if not isinstance(data, list):
        raise CheckError(f"invalid releases payload for {repo}")
    out: list[dict[str, Any]] = []
    for rel in data:
        if not isinstance(rel, dict):
            continue
        if rel.get("draft"):
            continue
        if rel.get("prerelease") and not include_prereleases:
            continue
        tag = rel.get("tag_name")
        if not isinstance(tag, str) or tag == "":
            continue
        out.append(rel)
    return out


def _release_time(rel: dict[str, Any]) -> datetime:
    stamp = rel.get("published_at") or rel.get("created_at")
    if not isinstance(stamp, str):
        raise CheckError("release is missing published_at/created_at")
    return datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def _verify_submodule(vendor: dict[str, Any]) -> tuple[int, int]:
    failures = 0
    notices = 0
    path = ROOT / vendor["path"]
    if not path.exists():
        print(f"FAIL verify {vendor['name']}: missing path {vendor['path']}")
        return 1, 0

    raw_status = _git("submodule", "status", "--", vendor["path"])
    lines = raw_status.splitlines()
    if not lines:
        print(f"FAIL verify {vendor['name']}: {vendor['path']} is not a submodule")
        return 1, 0

    status = lines[0]
    current_commit = status[1:].lstrip().split()[0]
    pinned_commit = vendor["pin"]["commit"]
    if current_commit != pinned_commit:
        print(
            f"FAIL verify {vendor['name']}: submodule commit {current_commit} "
            f"!= pinned {pinned_commit}"
        )
        failures += 1
    else:
        print(
            f"OK verify {vendor['name']}: submodule commit matches {pinned_commit}"
        )

    dirty = _git("-C", str(path), "status", "--porcelain", "--untracked-files=normal")
    if dirty.strip():
        print(f"NOTICE verify {vendor['name']}: submodule worktree is dirty")
        notices += 1
    return failures, notices


def _verify_single_file(vendor: dict[str, Any]) -> tuple[int, int]:
    failures = 0
    path = ROOT / vendor["path"]
    if not path.is_file():
        print(f"FAIL verify {vendor['name']}: missing file {vendor['path']}")
        return 1, 0

    local_bytes = path.read_bytes()
    local_sha = hashlib.sha256(local_bytes).hexdigest()
    pinned_sha = vendor["pin"]["sha256"]
    if local_sha != pinned_sha:
        print(
            f"FAIL verify {vendor['name']}: local sha256 {local_sha} != pinned {pinned_sha}"
        )
        failures += 1
    else:
        print(f"OK verify {vendor['name']}: local sha256 matches manifest")

    upstream_bytes = _github_raw_file(
        vendor["source"]["repo"], vendor["pin"]["ref"], vendor["source"]["file"]
    )
    if upstream_bytes != local_bytes:
        print(
            f"FAIL verify {vendor['name']}: local file differs from upstream "
            f"{vendor['source']['repo']}@{vendor['pin']['ref']}/{vendor['source']['file']}"
        )
        failures += 1
    else:
        print(
            f"OK verify {vendor['name']}: local file matches upstream ref "
            f"{vendor['pin']['ref']}"
        )
    return failures, 0


def _run_verify(vendors: list[dict[str, Any]]) -> int:
    failures = 0
    notices = 0
    for vendor in vendors:
        try:
            if vendor["kind"] == "git_submodule":
                vf, vn = _verify_submodule(vendor)
            else:
                vf, vn = _verify_single_file(vendor)
            failures += vf
            notices += vn
        except CheckError as exc:
            print(f"FAIL verify {vendor['name']}: {exc}")
            failures += 1
    if failures:
        return 1
    if notices:
        return 0
    return 0


def _pinned_commit_for_freshness(vendor: dict[str, Any]) -> str:
    if vendor["kind"] == "git_submodule":
        return vendor["pin"]["commit"]
    return _github_resolve_commit(vendor["source"]["repo"], vendor["pin"]["ref"])


def _freshness_latest_release(vendor: dict[str, Any]) -> tuple[dict[str, Any], str]:
    repo = vendor["source"]["repo"]
    releases = _github_releases(repo, vendor["freshness"]["includePrereleases"])
    if not releases:
        raise CheckError(f"no matching releases found for {repo}")

    series = vendor["freshness"].get("series")
    if not series:
        latest = max(releases, key=_release_time)
        return latest, latest["tag_name"]

    matching: list[tuple[tuple[int, int, int], dict[str, Any]]] = []
    higher: list[tuple[int, tuple[int, int, int], dict[str, Any]]] = []
    for rel in releases:
        match = SERIES_TAG_RE.fullmatch(rel["tag_name"])
        if not match:
            continue
        rel_series = int(match.group("series"))
        rel_version = tuple(int(part) for part in match.group("version").split("."))
        if rel_series == int(series):
            matching.append((rel_version, rel))
        elif rel_series > int(series):
            higher.append((rel_series, rel_version, rel))

    if not matching:
        raise CheckError(f"no stable releases found for series {series} in {repo}")

    latest_same = max(matching, key=lambda item: item[0])[1]
    if higher:
        latest_major = max(higher, key=lambda item: (item[0], item[1]))[2]
        print(
            f"NOTICE freshness {vendor['name']}: newer major series release exists: "
            f"{latest_major['tag_name']}"
        )
    return latest_same, latest_same["tag_name"]


def _run_freshness(vendors: list[dict[str, Any]]) -> int:
    failures = 0
    for vendor in vendors:
        try:
            latest_release, latest_tag = _freshness_latest_release(vendor)
            latest_commit = _github_resolve_commit(vendor["source"]["repo"], latest_tag)
            pinned_commit = _pinned_commit_for_freshness(vendor)
            if pinned_commit == latest_commit:
                print(
                    f"OK freshness {vendor['name']}: pinned ref matches latest stable "
                    f"release {latest_tag}"
                )
                continue

            series = vendor["freshness"].get("series")
            if series:
                print(
                    f"FAIL freshness {vendor['name']}: pinned commit "
                    f"{pinned_commit} does not match latest stable release in "
                    f"series {series}: {latest_tag} ({latest_commit})"
                )
            else:
                print(
                    f"FAIL freshness {vendor['name']}: pinned ref does not match "
                    f"latest stable release {latest_tag} ({latest_commit})"
                )
            failures += 1
        except CheckError as exc:
            print(f"FAIL freshness {vendor['name']}: {exc}")
            failures += 1
    return 1 if failures else 0


def main() -> int:
    args = _parse_args()
    manifest_path = Path(args.manifest)
    vendors = _validate_manifest(manifest_path)
    vendors = _select_vendors(vendors, args.vendors)

    if args.command == "verify":
        return _run_verify(vendors)
    if args.command == "freshness":
        return _run_freshness(vendors)

    verify_rc = _run_verify(vendors)
    freshness_rc = _run_freshness(vendors)
    return 1 if verify_rc != 0 or freshness_rc != 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
