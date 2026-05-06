from __future__ import annotations

import json
import os
import errno
import socket
import socketserver
import threading
import uuid
from pathlib import Path

import pytest

from bn.paths import bridge_registry_path
from bn.transport import choose_instance, list_instances, send_request


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        raw = self.rfile.readline()
        if not raw:
            return
        payload = json.loads(raw.decode("utf-8"))
        response = {
            "ok": True,
            "result": {
                "op": payload["op"],
                "target": payload.get("target"),
                "params": payload.get("params"),
            },
        }
        self.wfile.write(json.dumps(response).encode("utf-8"))


class _Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


def test_send_request_uses_registry_and_socket(tmp_path, monkeypatch):
    monkeypatch.setenv("BN_CACHE_DIR", str(tmp_path))
    pid = os.getpid()
    socket_path = Path("/tmp") / f"bn-test-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"
    registry_path = bridge_registry_path()
    registry_path.parent.mkdir(parents=True, exist_ok=True)

    server = _Server(str(socket_path), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    registry_path.write_text(
        json.dumps(
            {
                "pid": pid,
                "socket_path": str(socket_path),
                "plugin_name": "bn_agent_bridge",
                "plugin_version": "0.1.0",
            }
        ),
        encoding="utf-8",
    )

    try:
        instances = list_instances()
        assert len(instances) == 1
        instance = choose_instance()
        assert instance.pid == pid

        response = send_request("ping", params={"hello": "world"}, target=f"{pid}:1:999")
        assert response["result"]["op"] == "ping"
        assert response["result"]["params"] == {"hello": "world"}
    finally:
        server.shutdown()
        server.server_close()


def test_list_instances_prunes_stale_registry_and_socket(tmp_path, monkeypatch):
    monkeypatch.setenv("BN_CACHE_DIR", str(tmp_path))
    registry_path = bridge_registry_path()
    registry_path.parent.mkdir(parents=True, exist_ok=True)

    stale_socket_path = Path("/tmp") / f"bn-stale-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"
    stale_server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale_server.bind(str(stale_socket_path))
    stale_server.listen(1)
    stale_server.close()

    registry_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "socket_path": str(stale_socket_path),
                "plugin_name": "bn_agent_bridge",
                "plugin_version": "0.1.0",
            }
        ),
        encoding="utf-8",
    )

    assert stale_socket_path.exists()

    instances = list_instances()

    assert instances == []
    assert not registry_path.exists()
    assert stale_socket_path.exists()


def test_list_instances_preserves_permission_denied_registry(tmp_path, monkeypatch):
    monkeypatch.setenv("BN_CACHE_DIR", str(tmp_path))
    registry_path = bridge_registry_path()
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    socket_path = tmp_path / "bridge.sock"
    socket_path.write_text("", encoding="utf-8")
    registry_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "socket_path": str(socket_path),
                "plugin_name": "bn_agent_bridge",
                "plugin_version": "0.1.0",
            }
        ),
        encoding="utf-8",
    )

    class _DeniedSocket:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def settimeout(self, timeout):
            self.timeout = timeout

        def connect(self, path):
            raise PermissionError(errno.EPERM, "Operation not permitted")

    monkeypatch.setattr("bn.transport.socket.socket", lambda *args, **kwargs: _DeniedSocket())

    instances = list_instances()

    assert len(instances) == 1
    assert instances[0].socket_path == socket_path
    assert "Operation not permitted" in instances[0].meta["socket_probe_error"]
    assert registry_path.exists()


def test_send_request_wraps_socket_errors(tmp_path, monkeypatch):
    from bn.transport import BridgeError, BridgeInstance

    instance = BridgeInstance(
        pid=999,
        socket_path=tmp_path / "missing.sock",
        registry_path=tmp_path / "missing.json",
        plugin_name="bn_agent_bridge",
        plugin_version="0.1.0",
        started_at=None,
        meta={},
    )
    monkeypatch.setattr("bn.transport.choose_instance", lambda: instance)

    with pytest.raises(BridgeError, match="Failed to contact Binary Ninja bridge pid 999"):
        send_request("doctor")


def test_send_request_retries_transient_connect_failures(tmp_path, monkeypatch):
    from bn.transport import BridgeInstance

    instance = BridgeInstance(
        pid=999,
        socket_path=tmp_path / "bridge.sock",
        registry_path=tmp_path / "bridge.json",
        plugin_name="bn_agent_bridge",
        plugin_version="0.1.0",
        started_at=None,
        meta={},
    )
    monkeypatch.setattr("bn.transport.choose_instance", lambda: instance)

    class _FakeSocket:
        attempts = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def settimeout(self, timeout):
            self.timeout = timeout

        def connect(self, path):
            type(self).attempts += 1
            if type(self).attempts == 1:
                raise ConnectionRefusedError(61, "Connection refused")

        def sendall(self, payload):
            self.payload = payload

        def shutdown(self, how):
            self.how = how

        def recv(self, size):
            if not hasattr(self, "_sent"):
                self._sent = True
                return json.dumps({"ok": True, "result": {"pong": True}}).encode("utf-8")
            return b""

    monkeypatch.setattr("bn.transport.socket.socket", lambda *args, **kwargs: _FakeSocket())

    response = send_request("ping")

    assert response["result"]["pong"] is True
    assert _FakeSocket.attempts == 2


def test_send_request_uses_blocking_socket_by_default(tmp_path, monkeypatch):
    from bn.transport import BridgeInstance

    instance = BridgeInstance(
        pid=999,
        socket_path=tmp_path / "bridge.sock",
        registry_path=tmp_path / "bridge.json",
        plugin_name="bn_agent_bridge",
        plugin_version="0.1.0",
        started_at=None,
        meta={},
    )
    monkeypatch.setattr("bn.transport.choose_instance", lambda: instance)

    class _FakeSocket:
        timeout_calls = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def settimeout(self, timeout):
            type(self).timeout_calls += 1
            self.timeout = timeout

        def connect(self, path):
            self.path = path

        def sendall(self, payload):
            self.payload = payload

        def shutdown(self, how):
            self.how = how

        def recv(self, size):
            if not hasattr(self, "_sent"):
                self._sent = True
                return json.dumps({"ok": True, "result": {"pong": True}}).encode("utf-8")
            return b""

    monkeypatch.setattr("bn.transport.socket.socket", lambda *args, **kwargs: _FakeSocket())

    response = send_request("ping")

    assert response["result"]["pong"] is True
    assert _FakeSocket.timeout_calls == 0


def test_send_request_reports_timeout_waiting_for_response(tmp_path, monkeypatch):
    from bn.transport import BridgeError, BridgeInstance

    instance = BridgeInstance(
        pid=999,
        socket_path=tmp_path / "bridge.sock",
        registry_path=tmp_path / "bridge.json",
        plugin_name="bn_agent_bridge",
        plugin_version="0.1.0",
        started_at=None,
        meta={},
    )
    monkeypatch.setattr("bn.transport.choose_instance", lambda: instance)

    class _FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def settimeout(self, timeout):
            self.timeout = timeout

        def connect(self, path):
            self.path = path

        def sendall(self, payload):
            self.payload = payload

        def shutdown(self, how):
            self.how = how

        def recv(self, size):
            raise socket.timeout("timed out")

    monkeypatch.setattr("bn.transport.socket.socket", lambda *args, **kwargs: _FakeSocket())

    with pytest.raises(BridgeError, match="Timed out waiting for Binary Ninja bridge pid 999"):
        send_request("ping", timeout=12.5)


def test_list_instances_trusts_live_socket_even_with_stale_pid(tmp_path, monkeypatch):
    monkeypatch.setenv("BN_CACHE_DIR", str(tmp_path))
    registry_path = bridge_registry_path()
    registry_path.parent.mkdir(parents=True, exist_ok=True)

    socket_path = Path("/tmp") / f"bn-live-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"
    server = _Server(str(socket_path), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    registry_path.write_text(
        json.dumps(
            {
                "pid": 111,
                "socket_path": str(socket_path),
                "plugin_name": "bn_agent_bridge",
                "plugin_version": "0.1.0",
            }
        ),
        encoding="utf-8",
    )

    try:
        instances = list_instances()

        assert len(instances) == 1
        assert instances[0].pid == 111
        assert registry_path.exists()
    finally:
        server.shutdown()
        server.server_close()


def test_list_instances_reads_fixed_registry_path(tmp_path, monkeypatch):
    monkeypatch.setenv("BN_CACHE_DIR", str(tmp_path))
    pid = os.getpid()
    socket_path = Path("/tmp") / f"bn-fixed-{pid}-{uuid.uuid4().hex[:8]}.sock"
    server = _Server(str(socket_path), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    registry_path = bridge_registry_path()
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(
            {
                "pid": pid,
                "socket_path": str(socket_path),
                "plugin_name": "bn_agent_bridge",
                "plugin_version": "0.1.0",
            }
        ),
        encoding="utf-8",
    )

    try:
        instances = list_instances()

        assert len(instances) == 1
        assert instances[0].pid == pid
        assert instances[0].registry_path == registry_path
    finally:
        server.shutdown()
        server.server_close()
