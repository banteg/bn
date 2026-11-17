from __future__ import annotations

import contextlib
import errno
import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import bridge_registry_path
from .version import PROTOCOL_VERSION


class BridgeError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or ({"code": code, "message": message} if code else {})


TRANSIENT_SOCKET_ERRNOS = {
    errno.ECONNREFUSED,
    errno.ENOENT,
}

DENIED_SOCKET_ERRNOS = {
    errno.EACCES,
    errno.EPERM,
}


@dataclass(slots=True)
class BridgeInstance:
    pid: int
    socket_path: Path
    registry_path: Path
    plugin_name: str
    plugin_version: str
    started_at: str | None
    meta: dict[str, Any]
    protocol_version: int | None = None


def _purge_stale_registry(registry_path: Path) -> None:
    with contextlib.suppress(OSError):
        registry_path.unlink()


def _socket_probe_error(socket_path: Path, timeout: float = 0.2) -> OSError | None:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect(str(socket_path))
        return None
    except OSError as exc:
        return exc


def _denied_error(path: Path, exc: OSError) -> BridgeError:
    if os.environ.get("CODEX_SANDBOX"):
        hint = "Retry this authorized bn command with bridge socket access outside the Codex sandbox."
    else:
        hint = "Check the permissions on the Binary Ninja bridge socket and its registry."
    return BridgeError(f"Access denied to the Binary Ninja bridge at {path}: {exc}. {hint}", code="bridge_denied")


def _load_instance(path: Path) -> BridgeInstance | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        socket_path = Path(payload["socket_path"])
        pid = int(payload["pid"])
    except PermissionError as exc:
        raise _denied_error(path, exc) from exc
    except (OSError, ValueError, KeyError):
        return None

    probe_error = _socket_probe_error(socket_path)
    if probe_error is not None:
        if probe_error.errno in TRANSIENT_SOCKET_ERRNOS:
            _purge_stale_registry(path)
            return None
        payload["socket_probe_error"] = str(probe_error)

    protocol_value = payload.get("protocol_version")
    try:
        protocol_version = int(protocol_value) if protocol_value is not None else None
    except (TypeError, ValueError):
        protocol_version = None

    return BridgeInstance(
        pid=pid,
        socket_path=socket_path,
        registry_path=path,
        plugin_name=str(payload.get("plugin_name", "bn_agent_bridge")),
        plugin_version=str(payload.get("plugin_version", "0")),
        started_at=payload.get("started_at"),
        meta=payload,
        protocol_version=protocol_version,
    )


def list_instances() -> list[BridgeInstance]:
    fixed_registry = bridge_registry_path()
    instances: list[BridgeInstance] = []
    instance = _load_instance(fixed_registry)
    if instance is not None:
        instances.append(instance)
    return instances


def absent_bridge_error() -> BridgeError:
    return BridgeError(
        "No running Binary Ninja bridge instances found. Open a database and start the companion in Binary Ninja.",
        code="bridge_absent",
    )


def choose_instance() -> BridgeInstance:
    instances = list_instances()
    if not instances:
        raise absent_bridge_error()
    return instances[0]


def _send_request_to_instance(
    instance: BridgeInstance,
    op: str,
    *,
    params: dict[str, Any] | None = None,
    target: str | None = None,
    timeout: float | None = None,
    connect_retries: int = 4,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "protocol_version": PROTOCOL_VERSION,
        "op": op,
        "params": params or {},
    }
    if target is not None:
        payload["target"] = target

    encoded = (json.dumps(payload) + "\n").encode("utf-8")

    chunks: list[bytes] = []
    deadline = time.monotonic() + timeout if timeout is not None else None
    connected = False

    def set_remaining_timeout(sock):
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("bridge request deadline expired")
            sock.settimeout(remaining)

    for attempt in range(connect_retries):
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                set_remaining_timeout(sock)
                sock.connect(str(instance.socket_path))
                connected = True
                set_remaining_timeout(sock)
                sock.sendall(encoded)
                with contextlib.suppress(OSError):
                    sock.shutdown(socket.SHUT_WR)
                while True:
                    set_remaining_timeout(sock)
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
            break
        except TimeoutError as exc:
            state = "Execution may still be in progress." if connected else "The request was not sent."
            raise BridgeError(
                f"Timed out waiting for Binary Ninja bridge pid {instance.pid} at {instance.socket_path}. {state}",
                code="bridge_timeout",
            ) from exc
        except OSError as exc:
            if exc.errno in DENIED_SOCKET_ERRNOS:
                raise _denied_error(instance.socket_path, exc) from exc
            if not connected and exc.errno in TRANSIENT_SOCKET_ERRNOS:
                if attempt + 1 < connect_retries:
                    time.sleep(0.05 * (attempt + 1))
                    continue
                raise absent_bridge_error() from exc
            raise BridgeError(
                f"Failed to contact Binary Ninja bridge pid {instance.pid} at {instance.socket_path}: {exc}",
                code="bridge_transport_error",
            ) from exc

    if not chunks:
        raise BridgeError("Binary Ninja bridge returned an empty response")

    try:
        response = json.loads(b"".join(chunks).decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise BridgeError("Binary Ninja bridge returned invalid JSON") from exc

    if not isinstance(response, dict):
        raise BridgeError("Binary Ninja bridge returned a malformed response")

    if response.get("ok"):
        return response

    error = response.get("error") or "Unknown Binary Ninja bridge error"
    if isinstance(error, dict):
        message = str(error.get("message") or error.get("code") or "Unknown Binary Ninja bridge error")
        observed = error.get("observed")
        if isinstance(observed, dict) and isinstance(observed.get("stdout"), str) and observed["stdout"]:
            stdout = observed["stdout"]
            if len(stdout) > 2000:
                stdout = "… [earlier stdout omitted]\n" + stdout[-2000:]
            message += "\nCaptured stdout:\n" + stdout.rstrip("\n")
        if isinstance(observed, dict) and observed.get("traceback"):
            message += "\n" + str(observed["traceback"]).rstrip()
        if isinstance(observed, dict) and observed.get("suggestions"):
            message += "\nSuggestions: " + ", ".join(str(item) for item in observed["suggestions"])
        raise BridgeError(
            message,
            code=str(error.get("code")) if error.get("code") is not None else None,
            details=error,
        )
    raise BridgeError(str(error))


def send_request(
    op: str,
    *,
    params: dict[str, Any] | None = None,
    target: str | None = None,
    timeout: float | None = None,
    connect_retries: int = 4,
) -> dict[str, Any]:
    instance = choose_instance()
    return _send_request_to_instance(
        instance,
        op,
        params=params,
        target=target,
        timeout=timeout,
        connect_retries=connect_retries,
    )
