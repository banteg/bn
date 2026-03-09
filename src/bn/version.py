from __future__ import annotations

import hashlib
from pathlib import Path


VERSION = "0.6.1"


def build_id_for_file(path: Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data).hexdigest()[:12]
