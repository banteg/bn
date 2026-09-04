from __future__ import annotations

import functools
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import spill_root


DEFAULT_SPILL_BYTE_LIMIT = 40_000
TOKENIZER = "o200k_base"


@dataclass(frozen=True)
class OutputWriteResult:
    rendered: str
    artifact: dict[str, Any] | None = None
    spilled: bool = False


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
    fd, name = tempfile.mkstemp(prefix=f"{stem}-{now.strftime('%H%M%S')}-", suffix=suffix, dir=directory)
    os.close(fd)
    return Path(name)


@functools.cache
def _token_encoding():
    import tiktoken

    return tiktoken.get_encoding(TOKENIZER)


def _artifact_payload(
    *,
    artifact_path: Path,
    fmt: str,
    encoded: bytes,
    count_tokens: bool,
    value: Any,
) -> dict[str, Any]:
    payload = {
        "ok": True,
        "artifact_path": str(artifact_path),
        "format": fmt,
        "bytes": len(encoded),

        "sha256": hashlib.sha256(encoded).hexdigest(),
        "summary": _summary(value),
    }
    if count_tokens:
        try:
            payload["tokens"] = len(_token_encoding().encode(encoded.decode("utf-8"), disallowed_special=()))
            payload["tokenizer"] = TOKENIZER
        except Exception as exc:
            payload["token_count_warning"] = f"Token count unavailable: {exc}. Install bn-cli[tokens] for exact counts."
    return payload


def _artifact_envelope(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def write_output_result(
    value: Any,
    *,
    fmt: str,
    out_path: Path | None,
    stem: str,
    spill_byte_limit: int = DEFAULT_SPILL_BYTE_LIMIT,
    allow_spill: bool | None = None,
    count_tokens: bool = False,
) -> OutputWriteResult:
    rendered = render_value(value, fmt)
    encoded = rendered.encode("utf-8")
    if allow_spill is None:
        allow_spill = fmt == "text"

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(encoded)
        artifact = _artifact_payload(
            artifact_path=out_path,
            fmt=fmt,
            encoded=encoded,
            count_tokens=count_tokens,
            value=value,
        )
        return OutputWriteResult(
            rendered=_artifact_envelope(artifact),
            artifact=artifact,
            spilled=False,
        )

    if not allow_spill or len(encoded) <= spill_byte_limit:
        return OutputWriteResult(rendered=rendered)

    suffix = ".ndjson" if fmt == "ndjson" else ".txt" if fmt == "text" else ".json"
    spill_path = _spill_path(stem, suffix)
    spill_path.write_bytes(encoded)
    artifact = _artifact_payload(
        artifact_path=spill_path,
        fmt=fmt,
        encoded=encoded,
        count_tokens=count_tokens,
        value=value,
    )
    return OutputWriteResult(
        rendered=_artifact_envelope(artifact),
        artifact=artifact,
        spilled=True,
    )


def write_output(
    value: Any,
    *,
    fmt: str,
    out_path: Path | None,
    stem: str,
    spill_byte_limit: int = DEFAULT_SPILL_BYTE_LIMIT,
    allow_spill: bool | None = None,
    count_tokens: bool = False,
) -> str:
    return write_output_result(
        value,
        fmt=fmt,
        out_path=out_path,
        stem=stem,
        spill_byte_limit=spill_byte_limit,
        allow_spill=allow_spill,
        count_tokens=count_tokens,
    ).rendered
