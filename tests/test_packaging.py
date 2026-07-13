from __future__ import annotations

import json

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
