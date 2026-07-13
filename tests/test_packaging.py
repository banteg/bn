from __future__ import annotations

import ast
import json
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

from bn.paths import plugin_source_dir, skill_source_dir
from bn.version import VERSION


def test_packaged_plugin_assets_are_complete():
    source = plugin_source_dir()

    assert (source / "__init__.py").is_file()
    assert (source / "bridge.py").is_file()
    assert (source / "paths.py").is_file()
    assert (source / "version.py").is_file()
    metadata = json.loads((source / "plugin.json").read_text(encoding="utf-8"))
    assert metadata["version"] == VERSION


def test_packaged_skill_assets_are_complete():
    source = skill_source_dir()

    assert (source / "SKILL.md").is_file()
    assert (source / "agents" / "openai.yaml").is_file()


def test_release_metadata_is_in_sync():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/release.py"),
            "--check",
            "--tag",
            f"v{VERSION}",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert f"release metadata: {VERSION}" in result.stdout


def test_distribution_metadata_matches_public_install_name():
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"]

    assert project["name"] == "bn-cli"
    assert project["requires-python"] == ">=3.12"


def test_cli_and_plugin_protocol_versions_match():
    root = Path(__file__).resolve().parents[1]

    def assigned_integer(path: Path, name: str) -> int:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, int)
            ):
                return node.value.value
        raise AssertionError(f"{name} not found in {path}")

    cli_protocol = assigned_integer(root / "src/bn/version.py", "PROTOCOL_VERSION")
    plugin_protocol = assigned_integer(
        root / "src/bn/assets/plugin/bn_agent_bridge/version.py",
        "PROTOCOL_VERSION",
    )

    assert plugin_protocol == cli_protocol


def test_release_script_updates_all_metadata(tmp_path):
    root = Path(__file__).resolve().parents[1]
    paths = (
        Path("pyproject.toml"),
        Path("uv.lock"),
        Path("src/bn/version.py"),
        Path("src/bn/assets/plugin/bn_agent_bridge/plugin.json"),
    )
    for relative in paths:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / relative, destination)

    result = subprocess.run(
        [sys.executable, str(root / "scripts/release.py"), "1.2.3", "--root", str(tmp_path)],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert 'version = "1.2.3"' in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    assert 'VERSION = "1.2.3"' in (tmp_path / "src/bn/version.py").read_text(encoding="utf-8")
    plugin = json.loads(
        (tmp_path / "src/bn/assets/plugin/bn_agent_bridge/plugin.json").read_text(encoding="utf-8")
    )
    assert plugin["version"] == "1.2.3"
    assert 'name = "bn-cli"\nversion = "1.2.3"' in (tmp_path / "uv.lock").read_text(encoding="utf-8")
