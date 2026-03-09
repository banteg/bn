from __future__ import annotations

import os
import platform
import tempfile
from pathlib import Path


PLUGIN_NAME = "bn_agent_bridge"
SKILL_NAME = "bn"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def codex_home() -> Path:
    env = os.environ.get("CODEX_HOME")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".codex"


def cache_home() -> Path:
    env = os.environ.get("BN_CACHE_DIR")
    if env:
        return Path(env).expanduser()

    system = platform.system()
    home = Path.home()
    if system == "Darwin":
        return home / "Library" / "Caches" / "bn"
    if system == "Windows":
        base = os.environ.get("LOCALAPPDATA")
        if base:
            return Path(base) / "bn"
    xdg = os.environ.get("XDG_CACHE_HOME")
    if xdg:
        return Path(xdg) / "bn"
    return home / ".cache" / "bn"


def bridge_registry_path() -> Path:
    return cache_home() / f"{PLUGIN_NAME}.json"


def bridge_socket_path() -> Path:
    return cache_home() / f"{PLUGIN_NAME}.sock"


def spill_root() -> Path:
    root = Path(tempfile.gettempdir()) / "bn-spills"
    root.mkdir(parents=True, exist_ok=True)
    return root


def plugin_source_dir() -> Path:
    return repo_root() / "plugin" / PLUGIN_NAME


def binary_ninja_plugin_dir() -> Path:
    env = os.environ.get("BN_PLUGIN_DIR")
    if env:
        return Path(env).expanduser()

    system = platform.system()
    home = Path.home()
    if system == "Darwin":
        return home / "Library" / "Application Support" / "Binary Ninja" / "plugins"
    if system == "Windows":
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "Binary Ninja" / "plugins"
    return home / ".binaryninja" / "plugins"


def plugin_install_dir() -> Path:
    return binary_ninja_plugin_dir() / PLUGIN_NAME


def codex_skills_dir() -> Path:
    return codex_home() / "skills"


def skill_source_dir() -> Path:
    return repo_root() / "skills" / SKILL_NAME


def skill_install_dir() -> Path:
    return codex_skills_dir() / SKILL_NAME
