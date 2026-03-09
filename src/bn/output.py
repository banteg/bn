from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import spill_root


DEFAULT_SPILL_THRESHOLD = 64 * 1024


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return repr(value)


def render_value(value: Any, fmt: str) -> str:
    if fmt == "json":
        return json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n"

    if fmt == "ndjson":
        if isinstance(value, list):
            lines = [
                json.dumps(item, sort_keys=True, default=_json_default) for item in value
            ]
            return "\n".join(lines) + ("\n" if lines else "")
        return json.dumps(value, sort_keys=True, default=_json_default) + "\n"

    if isinstance(value, str):
        return value if value.endswith("\n") else value + "\n"
    return json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n"


def _summary(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {"kind": "object", "keys": sorted(value.keys())[:10], "count": len(value)}
    if isinstance(value, list):
        return {"kind": "array", "count": len(value)}
    if isinstance(value, str):
        return {"kind": "string", "chars": len(value)}
    return {"kind": type(value).__name__}


def _spill_path(stem: str, suffix: str) -> Path:
    now = datetime.now(timezone.utc)
    directory = spill_root() / now.strftime("%Y%m%d")
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{stem}-{now.strftime('%H%M%S')}{suffix}"


def write_output(
    value: Any,
    *,
    fmt: str,
    out_path: Path | None,
    stem: str,
    spill_threshold: int = DEFAULT_SPILL_THRESHOLD,
) -> str:
    rendered = render_value(value, fmt)
    encoded = rendered.encode("utf-8")

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(encoded)
        return json.dumps(
            {
                "ok": True,
                "artifact_path": str(out_path),
                "format": fmt,
                "bytes": len(encoded),
                "sha256": hashlib.sha256(encoded).hexdigest(),
                "summary": _summary(value),
            },
            indent=2,
            sort_keys=True,
        ) + "\n"

    if len(encoded) <= spill_threshold:
        return rendered

    suffix = ".ndjson" if fmt == "ndjson" else ".txt" if fmt == "text" else ".json"
    spill_path = _spill_path(stem, suffix)
    spill_path.write_bytes(encoded)
    return json.dumps(
        {
            "ok": True,
            "artifact_path": str(spill_path),
            "format": fmt,
            "bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "summary": _summary(value),
        },
        indent=2,
        sort_keys=True,
    ) + "\n"
