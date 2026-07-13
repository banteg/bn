from __future__ import annotations

import hashlib
import json
from pathlib import Path


PROTOCOL_VERSION = 1


def _plugin_version() -> str:
    metadata_path = Path(__file__).with_name("plugin.json")
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        return str(metadata["version"])
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        return "0+unknown"


VERSION = _plugin_version()


def build_id_for_file(path: Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data).hexdigest()[:12]
