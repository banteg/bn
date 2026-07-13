#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


VERSION_PATTERN = re.compile(r"\d+\.\d+\.\d+(?:[a-zA-Z0-9.+-]*)?")


def _replace_once(path: Path, pattern: str, replacement: str) -> None:
    source = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, replacement, source, count=1, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"Could not find version field in {path}")
    path.write_text(updated, encoding="utf-8")


def versions(root: Path) -> dict[str, str]:
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    package_version = (root / "src/bn/version.py").read_text(encoding="utf-8")
    plugin = json.loads(
        (root / "src/bn/assets/plugin/bn_agent_bridge/plugin.json").read_text(encoding="utf-8")
    )
    lock = (root / "uv.lock").read_text(encoding="utf-8")

    patterns = {
        "pyproject.toml": (pyproject, r'^version = "([^"]+)"'),
        "src/bn/version.py": (package_version, r'^VERSION = "([^"]+)"'),
        "uv.lock": (lock, r'^\[\[package\]\]\nname = "bn"\nversion = "([^"]+)"'),
    }
    result = {name: re.search(pattern, source, re.MULTILINE).group(1) for name, (source, pattern) in patterns.items()}
    result["plugin.json"] = str(plugin["version"])
    return result


def check(root: Path) -> str:
    found = versions(root)
    unique = set(found.values())
    if len(unique) != 1:
        details = "\n".join(f"- {name}: {version}" for name, version in found.items())
        raise RuntimeError(f"Release versions are out of sync:\n{details}")
    return unique.pop()


def set_version(root: Path, version: str) -> None:
    if VERSION_PATTERN.fullmatch(version) is None:
        raise ValueError(f"Invalid version: {version}")

    _replace_once(root / "pyproject.toml", r'^version = "[^"]+"', f'version = "{version}"')
    _replace_once(root / "src/bn/version.py", r'^VERSION = "[^"]+"', f'VERSION = "{version}"')
    _replace_once(
        root / "uv.lock",
        r'^(\[\[package\]\]\nname = "bn"\nversion = ")[^"]+("$)',
        rf'\g<1>{version}\2',
    )

    plugin_path = root / "src/bn/assets/plugin/bn_agent_bridge/plugin.json"
    plugin = json.loads(plugin_path.read_text(encoding="utf-8"))
    plugin["version"] = version
    plugin_path.write_text(json.dumps(plugin, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Update or verify bn release metadata")
    parser.add_argument("version", nargs="?", help="Version to write, for example 0.14.0")
    parser.add_argument("--check", action="store_true", help="Only verify that release metadata agrees")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1], help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    try:
        if args.check:
            if args.version is not None:
                parser.error("version cannot be combined with --check")
            version = check(args.root)
            print(f"release metadata: {version}")
            return 0
        if args.version is None:
            parser.error("provide a version or use --check")
        set_version(args.root, args.version)
        version = check(args.root)
        print(f"updated release metadata to {version}")
        return 0
    except (KeyError, AttributeError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
