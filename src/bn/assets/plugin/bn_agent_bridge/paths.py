from __future__ import annotations

import os
import platform
from pathlib import Path


PLUGIN_NAME = "bn_agent_bridge"


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
