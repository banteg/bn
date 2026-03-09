from __future__ import annotations

import atexit
import contextlib
import difflib
import hashlib
import io
import json
import os
import re
import socketserver
import threading
import traceback
import weakref
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import binaryninja as bn
from binaryninja.mainthread import execute_on_main_thread_and_wait, is_main_thread
from binaryninja.plugin import PluginCommand

try:
    import binaryninjaui as ui
except ImportError:  # pragma: no cover - GUI plugin only
    ui = None


VERSION = "0.1.0"
PLUGIN_NAME = "bn_agent_bridge"


def _cache_home() -> Path:
    base = os.environ.get("BN_CACHE_DIR")
    if base:
        return Path(base).expanduser()
    return Path.home() / "Library" / "Caches" / "bn"


def _registry_path() -> Path:
    return _cache_home() / f"{PLUGIN_NAME}.json"


def _socket_path() -> Path:
    return _cache_home() / f"{PLUGIN_NAME}.sock"


def _json_response(*, ok: bool, result: Any = None, error: str | None = None) -> dict[str, Any]:
    return {"ok": ok, "result": result, "error": error}


class OperationFailure(RuntimeError):
    def __init__(
        self,
        status: str,
        message: str,
        *,
        requested: dict[str, Any] | None = None,
        observed: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.message = message
        self.requested = requested or {}
        self.observed = observed or {}


class _ReadWriteLock:
    def __init__(self):
        self._condition = threading.Condition()
        self._readers = 0
        self._writer = False

    @contextlib.contextmanager
    def read(self):
        with self._condition:
            while self._writer:
                self._condition.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextlib.contextmanager
    def write(self):
        with self._condition:
            while self._writer or self._readers:
                self._condition.wait()
            self._writer = True
        try:
            yield
        finally:
            with self._condition:
                self._writer = False
                self._condition.notify_all()


READ_LOCKED_OPS = {
    "function_info",
    "list_functions",
    "search_functions",
    "decompile",
    "il",
    "disasm",
    "xrefs",
    "field_xrefs",
    "types",
    "type_info",
    "strings",
    "imports",
    "data",
    "bundle_function",
    "bundle_corpus",
    "get_comment",
}


WRITE_LOCKED_OPS = {
    "py_exec",
    "rename_symbol",
    "set_comment",
    "delete_comment",
    "set_prototype",
    "local_rename",
    "local_retype",
    "struct_field_set",
    "struct_field_rename",
    "struct_field_delete",
    "struct_replace",
    "types_declare",
    "patch_bytes",
    "batch_apply",
    "refresh",
}


def _run_on_main_thread(func):
    if is_main_thread():
        return func()

    holder: dict[str, Any] = {}

    def wrapper():
        try:
            holder["result"] = func()
        except Exception as exc:  # pragma: no cover - exercised inside GUI
            holder["error"] = exc
            holder["traceback"] = traceback.format_exc()

    execute_on_main_thread_and_wait(wrapper)
    if "error" in holder:
        exc = holder["error"]
        if "traceback" in holder:
            bn.log_error(holder["traceback"])
        raise exc
    return holder.get("result")


def _parse_address(value: Any) -> int:
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.lower().startswith("0x"):
        return int(text, 16)
    return int(text, 10)


def _clean_bytes(text: str) -> bytes:
    cleaned = re.sub(r"[^0-9a-fA-F]", "", text)
    if len(cleaned) % 2:
        raise ValueError("Hex byte string must have an even number of nybbles")
    return bytes.fromhex(cleaned)


def _write_text_artifact(path_text: str | None, payload: Any) -> dict[str, Any] | None:
    if not path_text:
        return None

    path = Path(path_text).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    path.write_bytes(data)
    return {
        "artifact_path": str(path),
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _active_binary_view():
    if ui is None:
        return None

    def resolve():
        try:
            context = ui.UIContext.activeContext()
            if context is not None:
                frame = context.getCurrentViewFrame()
                view = frame.getCurrentBinaryView() if frame is not None else None
                if view is not None:
                    return view

            contexts = list(ui.UIContext.allContexts())
            if len(contexts) == 1:
                frame = contexts[0].getCurrentViewFrame()
                return frame.getCurrentBinaryView() if frame is not None else None
        except Exception:
            return None
        return None

    return _run_on_main_thread(resolve)


def _collect_open_views() -> list[Any]:
    if ui is None:
        active = _active_binary_view()
        return [active] if active is not None else []

    def collect():
        found: list[Any] = []
        try:
            contexts = list(ui.UIContext.allContexts())
        except Exception:
            contexts = []
        if not contexts:
            active_context = ui.UIContext.activeContext()
            if active_context is not None:
                contexts = [active_context]

        def collect_binary_view(view):
            if view is not None:
                found.append(view)

        def collect_from_frame(frame):
            if frame is None:
                return
            collect_binary_view(frame.getCurrentBinaryView())

        def collect_from_tab(context, tab):
            try:
                collect_from_frame(context.getViewFrameForTab(tab))
            except Exception:
                pass
            try:
                view = context.getViewForTab(tab)
                collect_binary_view(view.getData() if view is not None else None)
            except Exception:
                pass

        for context in contexts:
            try:
                collect_from_frame(context.getCurrentViewFrame())
            except Exception:
                pass
            try:
                tabs = list(context.getTabs())
            except Exception:
                tabs = []
            for tab in tabs:
                collect_from_tab(context, tab)

        unique: list[Any] = []
        seen: set[int] = set()
        for bv in found:
            marker = id(bv)
            if marker not in seen:
                seen.add(marker)
                unique.append(bv)
        return unique

    return _run_on_main_thread(collect)


@dataclass(slots=True)
class TargetRecord:
    view_id: str
    ref: weakref.ReferenceType
    session_id: str
    filename: str
    basename: str
    view_name: str

    def target_id(self) -> str:
        return f"{os.getpid()}:{self.view_id}:{self.session_id}"


class TargetManager:
    def __init__(self):
        self._lock = threading.RLock()
        self._records: dict[str, TargetRecord] = {}
        self._ids_by_object: dict[int, str] = {}
        self._next_id = 1

    def _view_name(self, bv) -> str:
        for attr in ("view_type", "name"):
            try:
                value = getattr(bv, attr, None)
                if value:
                    return str(getattr(value, "name", value))
            except Exception:
                continue
        return type(bv).__name__

    def _preferred_selector(self, record: TargetRecord, basename_counts: dict[str, int]) -> str:
        if record.basename and basename_counts.get(record.basename, 0) == 1:
            return record.basename
        return record.target_id()

    def _matches_record(self, record: TargetRecord, selector: str | None) -> bool:
        if selector is None:
            return False
        candidate = str(selector).strip()
        if candidate in ("", "active"):
            return False
        return candidate in (
            record.target_id(),
            record.view_id,
            record.filename,
            record.basename,
        )

    def _default_view(self):
        active = _active_binary_view()
        if active is not None:
            return active

        with self._lock:
            live_views = [record.ref() for record in self._records.values()]
        live_views = [view for view in live_views if view is not None]
        if len(live_views) == 1:
            return live_views[0]
        return None

    def refresh(self) -> list[dict[str, Any]]:
        views = _collect_open_views()
        focused = _active_binary_view()

        with self._lock:
            alive: dict[str, TargetRecord] = {}
            for bv in views:
                key = id(bv)
                view_id = self._ids_by_object.get(key)
                if view_id is None:
                    view_id = str(self._next_id)
                    self._next_id += 1
                    self._ids_by_object[key] = view_id

                try:
                    session_id = str(bv.file.session_id)
                except Exception:
                    session_id = str(key)
                try:
                    filename = str(getattr(bv.file, "filename", "")) if bv.file else ""
                except Exception:
                    filename = ""

                alive[view_id] = TargetRecord(
                    view_id=view_id,
                    ref=weakref.ref(bv),
                    session_id=session_id,
                    filename=filename,
                    basename=os.path.basename(filename) if filename else "",
                    view_name=self._view_name(bv),
                )

            self._records = alive
            active = focused
            if active is None and len(self._records) == 1:
                active = next(iter(self._records.values())).ref()
            basename_counts: dict[str, int] = {}
            for record in self._records.values():
                if record.basename:
                    basename_counts[record.basename] = basename_counts.get(record.basename, 0) + 1

            result = []
            for view_id in sorted(self._records, key=lambda item: int(item)):
                record = self._records[view_id]
                view = record.ref()
                if view is None:
                    continue
                result.append(
                    {
                        "target_id": record.target_id(),
                        "view_id": record.view_id,
                        "session_id": record.session_id,
                        "filename": record.filename,
                        "basename": record.basename,
                        "selector": self._preferred_selector(record, basename_counts),
                        "view_name": record.view_name,
                        "active": bool(view is active),
                    }
                )
            return result

    def resolve(self, selector: str | None):
        targets = self.refresh()
        if not targets:
            raise RuntimeError("No BinaryView targets are open in the GUI")

        if selector in (None, "", "active"):
            active = self._default_view()
            if active is None:
                raise RuntimeError("No active BinaryView is selected and multiple targets are open")
            return active

        with self._lock:
            for record in self._records.values():
                if self._matches_record(record, selector):
                    view = record.ref()
                    if view is not None:
                        return view
        raise RuntimeError(f"Unknown target selector: {selector}")


class BridgeHandler(socketserver.StreamRequestHandler):
    def handle(self):  # pragma: no cover - exercised from CLI
        raw = self.rfile.readline()
        if not raw:
            return
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            response = _json_response(ok=False, error="Invalid JSON request")
        else:
            response = self.server.bridge.dispatch(payload)
        encoded = json.dumps(response, sort_keys=True, default=str).encode("utf-8")
        self.wfile.write(encoded)


class ThreadedUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64

    def __init__(self, socket_path: str, handler, bridge):
        self.bridge = bridge
        super().__init__(socket_path, handler)


class BinaryNinjaBridge:
    def __init__(self):
        self.targets = TargetManager()
        self.socket_path = _socket_path()
        self.registry_path = _registry_path()
        self._server: ThreadedUnixServer | None = None
        self._thread: threading.Thread | None = None
        self._target_lock = _ReadWriteLock()

    def start(self):  # pragma: no cover - requires GUI runtime
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        if self.socket_path.exists():
            self.socket_path.unlink()

        self._server = ThreadedUnixServer(str(self.socket_path), BridgeHandler, self)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        self._write_registry()
        bn.log_info(f"BN Agent Bridge listening on {self.socket_path}")

    def stop(self):  # pragma: no cover - requires GUI runtime
        if self._server is not None:
            with contextlib.suppress(Exception):
                self._server.shutdown()
            with contextlib.suppress(Exception):
                self._server.server_close()
        if self.socket_path.exists():
            with contextlib.suppress(OSError):
                self.socket_path.unlink()
        if self.registry_path.exists():
            with contextlib.suppress(OSError):
                self.registry_path.unlink()

    def _write_registry(self):
        payload = {
            "pid": os.getpid(),
            "socket_path": str(self.socket_path),
            "plugin_name": PLUGIN_NAME,
            "plugin_version": VERSION,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self.registry_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def dispatch(self, payload: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover - GUI runtime
        op = payload.get("op")
        params = payload.get("params") or {}
        target = payload.get("target")
        try:
            lock = contextlib.nullcontext()
            if op in WRITE_LOCKED_OPS:
                lock = self._target_lock.write()
            elif op in READ_LOCKED_OPS:
                lock = self._target_lock.read()
            with lock:
                result = self._dispatch_on_main(op, params, target)
            return _json_response(ok=True, result=result)
        except Exception as exc:
            return _json_response(ok=False, error=f"{type(exc).__name__}: {exc}")

    def _dispatch_on_main(self, op: str, params: dict[str, Any], target: str | None):
        if op == "doctor":
            return self._doctor()
        if op == "list_targets":
            return self.targets.refresh()
        if op == "target_info":
            return self._target_info(params.get("selector") or target)
        if op == "refresh":
            return self._refresh(target)

        if op == "list_functions":
            return self._list_functions(target, params.get("offset", 0), params.get("limit", 100))
        if op == "search_functions":
            return self._search_functions(
                target,
                str(params.get("query", "")),
                params.get("offset", 0),
                params.get("limit", 100),
            )
        if op == "function_info":
            return self._function_info(target, params["identifier"])
        if op == "decompile":
            return self._decompile(target, params["identifier"])
        if op == "il":
            return self._il(target, params["identifier"], str(params.get("view", "hlil")), bool(params.get("ssa")))
        if op == "disasm":
            return self._disasm(target, params["identifier"])
        if op == "xrefs":
            return self._xrefs(target, params["identifier"])
        if op == "field_xrefs":
            return self._field_xrefs(target, str(params["field"]))
        if op == "types":
            return self._types(
                target,
                query=params.get("query"),
                offset=int(params.get("offset", 0)),
                limit=int(params.get("limit", 100)),
            )
        if op == "type_info":
            return self._type_info(
                target,
                str(params["type_name"]),
                require_struct=bool(params.get("require_struct")),
            )
        if op == "strings":
            return self._strings(
                target,
                query=params.get("query"),
                offset=int(params.get("offset", 0)),
                limit=int(params.get("limit", 100)),
            )
        if op == "imports":
            return self._imports(target)
        if op == "data":
            return self._data(
                target,
                offset=int(params.get("offset", 0)),
                limit=int(params.get("limit", 100)),
            )
        if op == "bundle_function":
            return self._bundle_function(target, params["identifier"], params.get("out_path"))
        if op == "bundle_corpus":
            return self._bundle_corpus(
                target,
                str(params["kind"]),
                query=params.get("query"),
                limit=int(params.get("limit", 500)),
                out_path=params.get("out_path"),
            )
        if op == "py_exec":
            return self._py_exec(target, str(params["script"]), params.get("out_path"))

        if op == "rename_symbol":
            return self._mutation(target, bool(params.get("preview")), [params])
        if op == "get_comment":
            return self._get_comment(target, params.get("address"), params.get("function"))
        if op == "set_comment":
            return self._mutation(target, bool(params.get("preview")), [{"op": "set_comment", **params}])
        if op == "delete_comment":
            return self._mutation(target, bool(params.get("preview")), [{"op": "delete_comment", **params}])
        if op == "set_prototype":
            return self._mutation(target, bool(params.get("preview")), [{"op": "set_prototype", **params}])
        if op == "local_rename":
            return self._mutation(target, bool(params.get("preview")), [{"op": "local_rename", **params}])
        if op == "local_retype":
            return self._mutation(target, bool(params.get("preview")), [{"op": "local_retype", **params}])
        if op == "struct_field_set":
            return self._mutation(target, bool(params.get("preview")), [{"op": "struct_field_set", **params}])
        if op == "struct_field_rename":
            return self._mutation(target, bool(params.get("preview")), [{"op": "struct_field_rename", **params}])
        if op == "struct_field_delete":
            return self._mutation(target, bool(params.get("preview")), [{"op": "struct_field_delete", **params}])
        if op == "struct_replace":
            return self._mutation(target, bool(params.get("preview")), [{"op": "struct_replace", **params}])
        if op == "types_declare":
            return self._mutation(target, bool(params.get("preview")), [{"op": "types_declare", **params}])
        if op == "patch_bytes":
            return self._mutation(target, bool(params.get("preview")), [{"op": "patch_bytes", **params}])
        if op == "batch_apply":
            manifest = dict(params)
            preview = bool(manifest.get("preview"))
            target = str(manifest.get("target") or target)
            operations = list(manifest.get("ops") or [])
            return self._mutation(target, preview, operations)

        raise ValueError(f"Unknown operation: {op}")

    def _doctor(self):
        return {
            "plugin_name": PLUGIN_NAME,
            "plugin_version": VERSION,
            "pid": os.getpid(),
            "socket_path": str(self.socket_path),
            "targets": self.targets.refresh(),
        }

    def _target_info(self, selector: str | None):
        bv = self.targets.resolve(selector)
        record = None
        for item in self.targets.refresh():
            if item["active"] and selector in (None, "", "active"):
                record = item
                break
            if selector and any(
                self.targets._matches_record(target_record, selector)
                for target_record in self.targets._records.values()
                if target_record.target_id() == item["target_id"]
            ):
                record = item
                break
        return {
            **(record or {}),
            "arch": str(getattr(bv, "arch", "")),
            "platform": str(getattr(bv, "platform", "")),
            "entry_point": hex(getattr(bv, "entry_point", 0)),
        }

    def _refresh(self, selector: str | None):
        bv = self._resolve_view(selector)
        bv.update_analysis_and_wait()
        return {
            "refreshed": True,
            "target": self._target_info(selector),
        }

    def _resolve_view(self, selector: str | None):
        return self.targets.resolve(selector)

    def _find_function(self, bv, identifier):
        try:
            addr = _parse_address(identifier)
            fn = bv.get_function_at(addr)
            if fn is not None:
                return fn
        except Exception:
            pass

        text = str(identifier)
        exact = self._find_functions_by_name(bv, text, case_sensitive=True)
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            raise RuntimeError(f"Ambiguous function identifier: {identifier}")

        folded = self._find_functions_by_name(bv, text, case_sensitive=False)
        if len(folded) == 1:
            return folded[0]
        if len(folded) > 1:
            raise RuntimeError(f"Ambiguous function identifier: {identifier}")

        symbol = bv.get_symbol_by_raw_name(text)
        if symbol is not None:
            fn = bv.get_function_at(symbol.address)
            if fn is not None:
                return fn
        raise RuntimeError(f"Function not found: {identifier}")

    def _find_functions_by_name(self, bv, text: str, *, case_sensitive: bool) -> list[Any]:
        matches = []
        needle = text if case_sensitive else text.lower()
        seen: set[int] = set()
        for fn in list(bv.functions):
            names = [str(fn.name), str(getattr(fn, "raw_name", fn.name))]
            haystacks = names if case_sensitive else [name.lower() for name in names]
            if needle not in haystacks:
                continue
            marker = int(fn.start)
            if marker in seen:
                continue
            seen.add(marker)
            matches.append(fn)
        return matches

    def _find_symbols_by_name(self, bv, text: str, *, case_sensitive: bool) -> list[Any]:
        matches = []
        seen: set[tuple[int, str]] = set()

        if case_sensitive:
            candidates = list(bv.get_symbols_by_name(text))
            raw_match = bv.get_symbol_by_raw_name(text)
            if raw_match is not None:
                candidates.append(raw_match)
        else:
            folded = text.lower()
            candidates = []
            for symbol in list(bv.get_symbols()):
                names = [str(getattr(symbol, "name", "")), str(getattr(symbol, "raw_name", ""))]
                if folded in {name.lower() for name in names if name}:
                    candidates.append(symbol)

        for symbol in candidates:
            marker = (int(symbol.address), str(symbol.type))
            if marker in seen:
                continue
            seen.add(marker)
            matches.append(symbol)
        return matches

    def _resolve_rename_target(self, bv, identifier: Any, kind: str) -> dict[str, Any]:
        requested = {
            "kind": kind,
            "identifier": str(identifier),
        }

        try:
            address = _parse_address(identifier)
        except Exception:
            address = None

        if address is not None:
            fn = bv.get_function_at(address)
            symbol = bv.get_symbol_at(address)
            if kind == "function":
                if fn is None:
                    raise OperationFailure("unsupported", f"Function not found: {identifier}", requested=requested)
                return {
                    "kind": "function",
                    "address": int(fn.start),
                    "before_name": str(fn.name),
                }
            if kind == "data":
                return {
                    "kind": "data",
                    "address": int(address),
                    "before_name": str(symbol.name) if symbol is not None else None,
                }
            if fn is not None:
                return {
                    "kind": "function",
                    "address": int(fn.start),
                    "before_name": str(fn.name),
                }
            return {
                "kind": "data",
                "address": int(address),
                "before_name": str(symbol.name) if symbol is not None else None,
            }

        if kind in {"auto", "function"}:
            exact_functions = self._find_functions_by_name(bv, str(identifier), case_sensitive=True)
            if len(exact_functions) == 1:
                fn = exact_functions[0]
                return {
                    "kind": "function",
                    "address": int(fn.start),
                    "before_name": str(fn.name),
                }
            if len(exact_functions) > 1:
                raise OperationFailure("unsupported", f"Ambiguous function identifier: {identifier}", requested=requested)

            folded_functions = self._find_functions_by_name(bv, str(identifier), case_sensitive=False)
            if len(folded_functions) == 1:
                fn = folded_functions[0]
                return {
                    "kind": "function",
                    "address": int(fn.start),
                    "before_name": str(fn.name),
                }
            if len(folded_functions) > 1:
                raise OperationFailure("unsupported", f"Ambiguous function identifier: {identifier}", requested=requested)

        if kind == "function":
            raise OperationFailure("unsupported", f"Function not found: {identifier}", requested=requested)

        exact_symbols = [
            symbol
            for symbol in self._find_symbols_by_name(bv, str(identifier), case_sensitive=True)
            if symbol.type != bn.SymbolType.FunctionSymbol
        ]
        if len(exact_symbols) == 1:
            symbol = exact_symbols[0]
            return {
                "kind": "data",
                "address": int(symbol.address),
                "before_name": str(symbol.name),
            }
        if len(exact_symbols) > 1:
            raise OperationFailure("unsupported", f"Ambiguous symbol identifier: {identifier}", requested=requested)

        folded_symbols = [
            symbol
            for symbol in self._find_symbols_by_name(bv, str(identifier), case_sensitive=False)
            if symbol.type != bn.SymbolType.FunctionSymbol
        ]
        if len(folded_symbols) == 1:
            symbol = folded_symbols[0]
            return {
                "kind": "data",
                "address": int(symbol.address),
                "before_name": str(symbol.name),
            }
        if len(folded_symbols) > 1:
            raise OperationFailure("unsupported", f"Ambiguous symbol identifier: {identifier}", requested=requested)

        raise OperationFailure("unsupported", f"Symbol not found: {identifier}", requested=requested)

    def _functions_containing(self, bv, address: int):
        try:
            return list(bv.get_functions_containing(address))
        except Exception:
            fn = bv.get_function_at(address)
            return [fn] if fn is not None else []

    def _find_variable_by_storage(self, func, storage: int, *, is_parameter: bool | None = None):
        collections = []
        if is_parameter is True:
            collections = [(func.parameter_vars, True)]
        elif is_parameter is False:
            collections = [(func.stack_layout, False)]
        else:
            collections = [(func.parameter_vars, True), (func.stack_layout, False)]

        for collection, marker in collections:
            for var in list(collection):
                if int(var.storage) == int(storage):
                    return var, marker
        raise RuntimeError(f"Variable not found at storage {storage}")

    def _function_text(self, bv, func, *, view: str = "hlil", ssa: bool = False) -> str:
        il_name = {"hlil": "hlil", "mlil": "mlil", "llil": "llil"}.get(view, "hlil")
        try:
            il = getattr(func, il_name)
            if ssa and hasattr(il, "ssa_form") and il.ssa_form is not None:
                il = il.ssa_form
            lines = []
            for ins in il.instructions:
                address = getattr(ins, "address", func.start)
                lines.append(f"{int(address):08x}        {ins}")
            if lines:
                return "\n".join(lines)
        except Exception:
            pass
        return str(func)

    def _disasm_text(self, bv, func) -> str:
        lines = []
        for block in list(func.basic_blocks):
            addr = block.start
            while addr < block.end:
                length = max(1, int(bv.get_instruction_length(addr)))
                disasm = bv.get_disassembly(addr) or ""
                raw = bv.read(addr, length)
                hex_bytes = raw.hex(" ") if raw else ""
                lines.append(f"{addr:08x}  {hex_bytes:<16} {disasm}")
                addr += length
        return "\n".join(lines)

    def _list_locals(self, func) -> list[dict[str, Any]]:
        variables = []
        seen: set[tuple[str, int]] = set()
        for collection, is_parameter in ((func.parameter_vars, True), (func.stack_layout, False)):
            for var in list(collection):
                marker = (str(var.name), int(var.storage))
                if marker in seen:
                    continue
                seen.add(marker)
                variables.append(
                    {
                        "name": var.name,
                        "storage": int(var.storage),
                        "type": str(var.type),
                        "is_parameter": is_parameter,
                    }
                )
        return variables

    def _comment_map(self, bv, func) -> dict[str, str]:
        comments: dict[str, str] = {}
        for block in list(func.basic_blocks):
            addr = block.start
            while addr < block.end:
                text = bv.get_comment_at(addr)
                if text:
                    comments[hex(addr)] = text
                addr += max(1, int(bv.get_instruction_length(addr)))
        return comments

    def _xrefs_to_address(self, bv, address: int) -> dict[str, Any]:
        code_refs = []
        data_refs = []
        for ref in list(bv.get_code_refs(address)):
            code_refs.append(
                {
                    "function": ref.function.name if getattr(ref, "function", None) else None,
                    "address": hex(ref.address),
                }
            )
        for ref_addr in list(bv.get_data_refs(address)):
            functions = self._functions_containing(bv, ref_addr)
            data_refs.append(
                {
                    "function": functions[0].name if functions else None,
                    "address": hex(ref_addr),
                }
            )
        return {"address": hex(address), "code_refs": code_refs, "data_refs": data_refs}

    def _list_functions(self, selector: str | None, offset: int, limit: int):
        bv = self._resolve_view(selector)
        items = [
            {"name": fn.name, "address": hex(fn.start), "raw_name": getattr(fn, "raw_name", fn.name)}
            for fn in list(bv.functions)
        ]
        return items[offset : offset + limit]

    def _search_functions(self, selector: str | None, query: str, offset: int, limit: int):
        bv = self._resolve_view(selector)
        items = []
        needle = query.lower()
        for fn in list(bv.functions):
            if needle in fn.name.lower():
                items.append({"name": fn.name, "address": hex(fn.start)})
        return items[offset : offset + limit]

    def _decompile(self, selector: str | None, identifier):
        bv = self._resolve_view(selector)
        func = self._find_function(bv, identifier)
        text = self._function_text(bv, func, view="hlil")
        warnings = self._render_warnings(text)
        return {
            "function": {"name": func.name, "address": hex(func.start)},
            "text": text,
            "warnings": warnings,
        }

    def _function_info(self, selector: str | None, identifier):
        bv = self._resolve_view(selector)
        func = self._find_function(bv, identifier)
        variables = self._list_locals(func)
        parameters = [item for item in variables if item["is_parameter"]]
        locals_only = [item for item in variables if not item["is_parameter"]]
        return {
            "function": {
                "name": func.name,
                "address": hex(func.start),
                "raw_name": getattr(func, "raw_name", func.name),
            },
            "prototype": str(func.type),
            "parameters": parameters,
            "locals": locals_only,
            "stack_vars": locals_only,
        }

    def _il(self, selector: str | None, identifier, view: str, ssa: bool):
        bv = self._resolve_view(selector)
        func = self._find_function(bv, identifier)
        text = self._function_text(bv, func, view=view, ssa=ssa)
        return {
            "function": {"name": func.name, "address": hex(func.start)},
            "view": view,
            "ssa": ssa,
            "text": text,
            "warnings": self._render_warnings(text),
        }

    def _disasm(self, selector: str | None, identifier):
        bv = self._resolve_view(selector)
        func = self._find_function(bv, identifier)
        return {
            "function": {"name": func.name, "address": hex(func.start)},
            "text": self._disasm_text(bv, func),
        }

    def _xrefs(self, selector: str | None, identifier):
        bv = self._resolve_view(selector)
        try:
            address = _parse_address(identifier)
        except Exception:
            address = self._find_function(bv, identifier).start
        return self._xrefs_to_address(bv, address)

    def _resolve_type_field(self, bv, field_spec: str):
        type_name, sep, field_name = str(field_spec).rpartition(".")
        if not sep or not type_name or not field_name:
            raise RuntimeError("Field selector must be in the form Struct.field")

        resolved_name, type_obj = self._find_type(bv, type_name)
        members = getattr(type_obj, "members", None)
        if members is None:
            raise RuntimeError(f"Type is not a struct-like type: {resolved_name}")

        for index, member in enumerate(list(members)):
            if str(getattr(member, "name", "")) != field_name:
                continue
            return {
                "type_name": resolved_name,
                "field_name": field_name,
                "offset": int(getattr(member, "offset", 0)),
                "member_index": index,
                "field_type": str(getattr(member, "type", "")),
            }
        raise RuntimeError(f"Field not found: {resolved_name}.{field_name}")

    def _field_xrefs(self, selector: str | None, field_spec: str):
        bv = self._resolve_view(selector)
        field = self._resolve_type_field(bv, field_spec)

        code_refs = []
        for ref in bv.get_code_refs_for_type_field(field["type_name"], field["offset"]):
            func = getattr(ref, "func", None)
            address = int(getattr(ref, "address", 0))
            code_refs.append(
                {
                    "function": func.name if func is not None else None,
                    "address": hex(address),
                    "size": int(getattr(ref, "size", 0)),
                    "incoming_type": str(getattr(ref, "incomingType", "")) or None,
                    "disasm": bv.get_disassembly(address) or "",
                }
            )

        data_refs = []
        for address in list(bv.get_data_refs_for_type_field(field["type_name"], field["offset"])):
            symbol = bv.get_symbol_at(address)
            type_obj = bv.get_type_at(address)
            data_refs.append(
                {
                    "address": hex(address),
                    "symbol": symbol.name if symbol is not None else None,
                    "type": str(type_obj) if type_obj is not None else None,
                }
            )

        return {
            "field": field,
            "code_refs": code_refs,
            "data_refs": data_refs,
        }

    def _types(self, selector: str | None, *, query, offset: int, limit: int):
        bv = self._resolve_view(selector)
        items = []
        needle = str(query).lower() if query else None
        for name, type_obj in list(bv.types.items()):
            entry = self._type_entry(name, type_obj)
            if needle and needle not in entry["name"].lower() and needle not in entry["decl"].lower():
                continue
            items.append(entry)
        return items[offset : offset + limit]

    def _find_type(self, bv, type_name: str):
        type_obj = bv.get_type_by_name(type_name)
        if type_obj is not None:
            return type_name, type_obj

        needle = str(type_name).lower()
        for name, candidate in list(bv.types.items()):
            if str(name).lower() == needle:
                return str(name), candidate
        raise RuntimeError(f"Type not found: {type_name}")

    def _type_entry(self, type_name, type_obj):
        return {
            "name": str(type_name),
            "kind": str(getattr(type_obj, "type_class", "unknown")),
            "decl": str(type_obj),
            "layout": self._render_type_layout(type_obj),
        }

    def _type_info(self, selector: str | None, type_name: str, *, require_struct: bool = False):
        bv = self._resolve_view(selector)
        resolved_name, type_obj = self._find_type(bv, type_name)
        members = getattr(type_obj, "members", None)
        if require_struct and members is None:
            raise RuntimeError(f"Type is not a struct-like type: {resolved_name}")
        return self._type_entry(resolved_name, type_obj)

    def _strings(self, selector: str | None, *, query, offset: int, limit: int):
        bv = self._resolve_view(selector)
        items = []
        needle = str(query).lower() if query else None
        for item in list(getattr(bv, "strings", [])):
            value = str(getattr(item, "value", ""))
            entry = {
                "address": hex(int(getattr(item, "start", 0))),
                "length": int(getattr(item, "length", 0)),
                "type": str(getattr(item, "type", "")),
                "value": value,
            }
            if needle and needle not in value.lower():
                continue
            items.append(entry)
        return items[offset : offset + limit]

    def _imports(self, selector: str | None):
        bv = self._resolve_view(selector)
        items = []
        for sym in list(bv.get_symbols_of_type(bn.SymbolType.ImportedFunctionSymbol)):
            items.append({"name": sym.name, "address": hex(sym.address)})
        return items

    def _get_comment(self, selector: str | None, address, function):
        bv = self._resolve_view(selector)
        if function:
            fn = self._find_function(bv, function)
            comment = bv.get_comment_at(fn.start)
            return {
                "function": fn.name,
                "address": hex(fn.start),
                "comment": comment or "",
                "has_comment": bool(comment),
            }

        if address is None:
            raise RuntimeError("comment get requires --address or --function")

        comment_address = _parse_address(address)
        comment = bv.get_comment_at(comment_address)
        return {
            "address": hex(comment_address),
            "comment": comment or "",
            "has_comment": bool(comment),
        }

    def _data(self, selector: str | None, *, offset: int, limit: int):
        bv = self._resolve_view(selector)
        items = []
        for addr in list(bv.data_vars):
            symbol = bv.get_symbol_at(addr)
            type_obj = bv.get_type_at(addr)
            items.append(
                {
                    "address": hex(addr),
                    "name": symbol.name if symbol else None,
                    "type": str(type_obj) if type_obj is not None else None,
                }
            )
        return items[offset : offset + limit]

    def _bundle_function(self, selector: str | None, identifier, out_path: str | None):
        bv = self._resolve_view(selector)
        func = self._find_function(bv, identifier)
        decompile = self._function_text(bv, func, view="hlil")
        bundle = {
            "target": self._target_info(selector),
            "function": {
                "name": func.name,
                "address": hex(func.start),
                "raw_name": getattr(func, "raw_name", func.name),
                "type": str(func.type),
            },
            "decompile": decompile,
            "warnings": self._render_warnings(decompile),
            "il": {
                "hlil": decompile,
                "mlil": self._function_text(bv, func, view="mlil"),
            },
            "disassembly": self._disasm_text(bv, func),
            "locals": self._list_locals(func),
            "comments": self._comment_map(bv, func),
            "xrefs": self._xrefs_to_address(bv, func.start),
        }
        artifact = _write_text_artifact(out_path, bundle)
        if artifact:
            bundle["artifact"] = artifact
        return bundle

    def _bundle_corpus(self, selector: str | None, kind: str, *, query, limit: int, out_path: str | None):
        if kind == "functions":
            payload = self._list_functions(selector, 0, limit)
        elif kind == "types":
            payload = self._types(selector, query=query, offset=0, limit=limit)
        elif kind == "strings":
            payload = self._strings(selector, query=query, offset=0, limit=limit)
        else:
            raise ValueError(f"Unsupported corpus kind: {kind}")

        result = {"kind": kind, "items": payload}
        artifact = _write_text_artifact(out_path, result)
        if artifact:
            result["artifact"] = artifact
        return result

    def _py_exec(self, selector: str | None, script: str, out_path: str | None):
        bv = self._resolve_view(selector)
        stdout = io.StringIO()
        scope = {
            "bn": bn,
            "binaryninja": bn,
            "bv": bv,
            "result": None,
        }
        with contextlib.redirect_stdout(stdout):
            exec(script, scope, scope)
        result = {
            "stdout": stdout.getvalue(),
            "result": scope.get("result"),
        }
        artifact = _write_text_artifact(out_path, result)
        if artifact:
            result["artifact"] = artifact
        return result

    def _render_warnings(self, text: str) -> list[str]:
        warnings: list[str] = []
        if "__offset(" in text:
            warnings.append(
                "Decompile still contains raw __offset(...) expressions; use `bn types show` or `bn struct show` as the authoritative layout until Binary Ninja refreshes the presentation."
            )
        return warnings

    def _guess_type_affected_functions(self, bv, type_name: str, limit: int = 10):
        matches = []
        needle = type_name.lower()
        for fn in list(bv.functions):
            text = str(fn.type).lower()
            if needle in text:
                matches.append(fn)
                if len(matches) >= limit:
                    break
        return matches

    def _parse_declaration_source(self, bv, declaration: str, *, source_path: str | None = None):
        parse_result = None
        source_error: Exception | None = None
        platform = getattr(bv, "platform", None)
        if platform is not None and hasattr(platform, "parse_types_from_source"):
            kwargs: dict[str, Any] = {}
            if source_path:
                kwargs["filename"] = source_path
                kwargs["include_dirs"] = [str(Path(source_path).expanduser().resolve().parent)]
            try:
                parse_result = platform.parse_types_from_source(declaration, **kwargs)
            except Exception as exc:
                source_error = exc

        if parse_result is None:
            try:
                parse_result = bv.parse_types_from_string(declaration)
            except Exception:
                if source_error is not None:
                    raise source_error
                raise

        return {
            "types": [(str(name), type_obj) for name, type_obj in list(getattr(parse_result, "types", {}).items())],
            "variables": [(str(name), type_obj) for name, type_obj in list(getattr(parse_result, "variables", {}).items())],
            "functions": [(str(name), type_obj) for name, type_obj in list(getattr(parse_result, "functions", {}).items())],
        }

    def _operation_type_names(self, bv, op: dict[str, Any]) -> list[str]:
        kind = op.get("op") or "rename_symbol"
        if kind.startswith("struct_") and op.get("struct_name"):
            return [str(op["struct_name"])]
        if kind in {"struct_replace", "types_declare"}:
            return [name for name, _ in self._parse_declaration_source(
                bv,
                str(op["declaration"]),
                source_path=op.get("source_path"),
            )["types"]]
        return []

    def _guess_affected_functions(self, bv, operations: list[dict[str, Any]]):
        affected = []
        seen = set()
        for op in operations:
            kind = op.get("op") or "rename_symbol"
            functions = []
            try:
                if kind == "rename_symbol" and op.get("kind") != "data":
                    functions = [self._find_function(bv, op["identifier"])]
                elif kind in {"set_prototype", "local_rename", "local_retype"}:
                    ident = op.get("identifier") or op.get("function")
                    functions = [self._find_function(bv, ident)]
                elif kind in {"set_comment", "delete_comment"}:
                    if op.get("function"):
                        functions = [self._find_function(bv, op["function"])]
                    elif op.get("address"):
                        functions = self._functions_containing(bv, _parse_address(op["address"]))
                elif kind == "patch_bytes":
                    functions = self._functions_containing(bv, _parse_address(op["address"]))
                elif kind.startswith("struct_") or kind == "types_declare":
                    for type_name in self._operation_type_names(bv, op):
                        functions.extend(self._guess_type_affected_functions(bv, type_name))
            except Exception:
                functions = []

            for fn in functions:
                if fn is None:
                    continue
                marker = int(fn.start)
                if marker not in seen:
                    seen.add(marker)
                    affected.append(fn)
        return affected

    def _affected_type_names(self, bv, operations: list[dict[str, Any]]) -> list[str]:
        names: list[str] = []
        seen: set[str] = set()
        for op in operations:
            for type_name in self._operation_type_names(bv, op):
                if type_name not in seen:
                    seen.add(type_name)
                    names.append(type_name)
        return names

    def _render_type_layout(self, type_obj) -> str:
        header = str(type_obj)
        try:
            width = int(getattr(type_obj, "width", 0))
            header = f"{header} // size=0x{width:x}"
        except Exception:
            pass

        members = getattr(type_obj, "members", None)
        if members is None:
            return header

        lines = [header]
        for member in list(members):
            try:
                offset = int(getattr(member, "offset", 0))
            except Exception:
                offset = 0
            name = str(getattr(member, "name", "<anonymous>"))
            member_type = str(getattr(member, "type", "<unknown>"))
            lines.append(f"0x{offset:04x}: {member_type} {name}")
        return "\n".join(lines)

    def _capture_type_snapshots(self, bv, operations: list[dict[str, Any]]):
        snapshots: dict[str, dict[str, Any]] = {}
        for type_name in self._affected_type_names(bv, operations):
            type_obj = bv.get_type_by_name(type_name)
            if type_obj is None:
                continue
            snapshots[type_name] = {
                "type_name": type_name,
                "decl": str(type_obj),
                "layout": self._render_type_layout(type_obj),
            }
        return snapshots

    def _diff_type_snapshots(self, before: dict[str, Any], after: dict[str, Any]):
        diffs = []
        for type_name in sorted(set(before) | set(after)):
            old = before.get(type_name, {"decl": "", "layout": ""})
            new = after.get(type_name, {"decl": "", "layout": ""})
            layout_diff = "\n".join(
                difflib.unified_diff(
                    old["layout"].splitlines(),
                    new["layout"].splitlines(),
                    fromfile=f"before:{type_name}",
                    tofile=f"after:{type_name}",
                    lineterm="",
                )
            )
            changed = old["decl"] != new["decl"] or old["layout"] != new["layout"]
            entry = {
                "type_name": type_name,
                "before_decl": old["decl"],
                "after_decl": new["decl"],
                "before_layout": old["layout"],
                "after_layout": new["layout"],
                "layout_diff": layout_diff,
                "changed": changed,
            }
            if not changed:
                entry["message"] = "No effective change detected"
            diffs.append(entry)
        return diffs

    def _annotate_operation_results(self, results: list[dict[str, Any]], type_diffs: list[dict[str, Any]]):
        type_changes = {item["type_name"]: item for item in type_diffs}
        annotated = []
        for result in results:
            item = dict(result)
            type_name = item.get("struct_name")
            if type_name and type_name in type_changes:
                change = type_changes[type_name]
                item["changed"] = bool(change["changed"])
                if not change["changed"]:
                    item["message"] = change["message"]
                    if item.get("status") == "verified":
                        item["status"] = "noop"
            defined_types = dict(item.get("defined_types") or {})
            if defined_types:
                changed_types = {name: bool(type_changes.get(name, {}).get("changed")) for name in defined_types}
                item["changed_types"] = changed_types
                if item.get("status") == "verified" and not any(changed_types.values()):
                    item["status"] = "noop"
                    item["message"] = "No effective change detected"
            annotated.append(item)
        return annotated

    def _capture_function_snapshots(self, bv, functions):
        snapshots = {}
        for fn in functions:
            snapshots[int(fn.start)] = {
                "name": fn.name,
                "address": hex(fn.start),
                "text": self._function_text(bv, fn, view="hlil"),
            }
        return snapshots

    def _snippet_for_change(self, before_text: str, after_text: str, *, context_lines: int = 3, max_lines: int = 10):
        before_lines = before_text.splitlines()
        after_lines = after_text.splitlines()
        line_count = max(len(before_lines), len(after_lines))

        changed_line = None
        for index in range(line_count):
            before_line = before_lines[index] if index < len(before_lines) else None
            after_line = after_lines[index] if index < len(after_lines) else None
            if before_line != after_line:
                changed_line = index
                break

        if changed_line is None:
            return None

        start = max(0, changed_line - context_lines)
        end = min(line_count, start + max_lines)
        return {
            "start_line": start + 1,
            "before_excerpt": "\n".join(before_lines[start:end]),
            "after_excerpt": "\n".join(after_lines[start:end]),
        }

    def _diff_snapshots(self, before: dict[int, Any], after: dict[int, Any]):
        diffs = []
        snippets_added = 0
        for address in sorted(set(before) | set(after)):
            old = before.get(address, {"text": ""})
            new = after.get(address, {"text": ""})
            text_changed = old.get("text", "") != new.get("text", "")
            name_changed = old.get("name") != new.get("name")
            diff = "\n".join(
                difflib.unified_diff(
                    old["text"].splitlines(),
                    new["text"].splitlines(),
                    fromfile=f"before:{old.get('name', hex(address))}",
                    tofile=f"after:{new.get('name', hex(address))}",
                    lineterm="",
                )
            )
            if not diff and name_changed:
                diff = "\n".join(
                    [
                        f"--- before:{old.get('name', hex(address))}",
                        f"+++ after:{new.get('name', hex(address))}",
                    ]
                )
            diffs.append(
                {
                    "address": hex(address),
                    "before_name": old.get("name"),
                    "after_name": new.get("name"),
                    "changed": bool(text_changed or name_changed),
                    "diff": diff,
                }
            )
            if text_changed and snippets_added < 3:
                snippet = self._snippet_for_change(old.get("text", ""), new.get("text", ""))
                if snippet is not None:
                    diffs[-1].update(snippet)
                    snippets_added += 1
        return diffs

    def _find_variable(self, func, name: str):
        for collection in (func.parameter_vars, func.stack_layout):
            for var in list(collection):
                if var.name == name:
                    return var
        raise RuntimeError(f"Variable not found: {name}")

    def _operation_requested(self, op: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in op.items() if key != "preview"}

    def _operation_failure_result(self, op: dict[str, Any], exc: OperationFailure) -> dict[str, Any]:
        result = {
            "op": str(op.get("op") or "rename_symbol"),
            "status": exc.status,
            "message": exc.message,
            "requested": exc.requested or self._operation_requested(op),
        }
        if exc.observed:
            result["observed"] = exc.observed
        return result

    def _mark_unverified_results(self, results: list[dict[str, Any]], message: str) -> list[dict[str, Any]]:
        annotated = []
        for result in results:
            item = dict(result)
            item["status"] = "unsupported"
            item["message"] = message
            annotated.append(item)
        return annotated

    def _has_failed_results(self, results: list[dict[str, Any]]) -> bool:
        return any(item.get("status") in {"unsupported", "verification_failed"} for item in results)

    def _find_member(self, type_obj, *, offset: int | None = None, name: str | None = None):
        members = getattr(type_obj, "members", None)
        if members is None:
            return None
        for member in list(members):
            member_offset = int(getattr(member, "offset", 0))
            member_name = str(getattr(member, "name", ""))
            if offset is not None and member_offset != int(offset):
                continue
            if name is not None and member_name != name:
                continue
            return member
        return None

    def _verify_operation(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        op = result.get("op")
        try:
            if op == "rename_symbol":
                return self._verify_rename_symbol(bv, result)
            if op == "set_comment":
                return self._verify_set_comment(bv, result)
            if op == "delete_comment":
                return self._verify_delete_comment(bv, result)
            if op == "set_prototype":
                return self._verify_set_prototype(bv, result)
            if op == "local_rename":
                return self._verify_local_rename(bv, result)
            if op == "local_retype":
                return self._verify_local_retype(bv, result)
            if op == "struct_field_set":
                return self._verify_struct_field_set(bv, result)
            if op == "struct_field_rename":
                return self._verify_struct_field_rename(bv, result)
            if op == "struct_field_delete":
                return self._verify_struct_field_delete(bv, result)
            if op == "struct_replace":
                return self._verify_declared_types(bv, result)
            if op == "types_declare":
                return self._verify_declared_types(bv, result)
            if op == "patch_bytes":
                return self._verify_patch_bytes(bv, result)
            raise OperationFailure("unsupported", f"Unsupported verification path: {op}", requested=result.get("requested"))
        except OperationFailure as exc:
            item = dict(result)
            item["status"] = exc.status
            item["message"] = exc.message
            if exc.requested:
                item["requested"] = exc.requested
            if exc.observed:
                item["observed"] = exc.observed
            return item
        except Exception as exc:
            item = dict(result)
            item["status"] = "verification_failed"
            item["message"] = f"{type(exc).__name__}: {exc}"
            if item.get("requested") is None:
                item["requested"] = {}
            return item

    def _verify_rename_symbol(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        address = _parse_address(item["address"])
        requested_name = str(item["new_name"])
        before_name = item.get("before_name")
        observed_name = None
        if item.get("kind") == "function":
            fn = bv.get_function_at(address)
            if fn is None:
                raise OperationFailure(
                    "verification_failed",
                    f"Function missing after rename at {item['address']}",
                    requested=item.get("requested"),
                    observed={"address": item["address"], "name": None},
                )
            observed_name = str(fn.name)
        else:
            symbol = bv.get_symbol_at(address)
            observed_name = str(symbol.name) if symbol is not None else None
        item["observed"] = {"address": item["address"], "name": observed_name}
        if observed_name != requested_name:
            raise OperationFailure(
                "verification_failed",
                f"Live rename verification failed at {item['address']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "noop" if before_name == requested_name else "verified"
        return item

    def _verify_set_comment(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        address = _parse_address(item["address"])
        expected = str(item["requested"]["comment"])
        observed = bv.get_comment_at(address) or ""
        item["observed"] = {"address": item["address"], "comment": observed}
        if observed != expected:
            raise OperationFailure(
                "verification_failed",
                f"Live comment verification failed at {item['address']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "noop" if item.get("before_comment", "") == expected else "verified"
        return item

    def _verify_delete_comment(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        address = _parse_address(item["address"])
        observed = bv.get_comment_at(address) or ""
        item["observed"] = {"address": item["address"], "comment": observed}
        if observed:
            raise OperationFailure(
                "verification_failed",
                f"Live comment deletion verification failed at {item['address']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "noop" if not item.get("before_comment") else "verified"
        return item

    def _verify_set_prototype(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        address = _parse_address(item["address"])
        fn = bv.get_function_at(address)
        if fn is None:
            raise OperationFailure(
                "verification_failed",
                f"Function missing after prototype change at {item['address']}",
                requested=item.get("requested"),
                observed={"address": item["address"], "prototype": None},
            )
        observed = str(fn.type)
        item["observed"] = {"address": item["address"], "prototype": observed}
        if observed != item["expected_prototype"]:
            raise OperationFailure(
                "verification_failed",
                f"Live prototype verification failed at {item['address']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "noop" if item.get("before_prototype") == item["expected_prototype"] else "verified"
        return item

    def _verify_local_rename(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        address = _parse_address(item["address"])
        fn = bv.get_function_at(address)
        if fn is None:
            raise OperationFailure(
                "verification_failed",
                f"Function missing after local rename at {item['address']}",
                requested=item.get("requested"),
                observed={"address": item["address"], "variable": None},
            )
        var, _ = self._find_variable_by_storage(
            fn,
            int(item["storage"]),
            is_parameter=bool(item["is_parameter"]),
        )
        observed_name = str(var.name)
        item["observed"] = {"address": item["address"], "variable": observed_name, "storage": int(item["storage"])}
        if observed_name != item["new_name"]:
            raise OperationFailure(
                "verification_failed",
                f"Live local rename verification failed at {item['address']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "noop" if item.get("before_name") == item["new_name"] else "verified"
        return item

    def _verify_local_retype(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        address = _parse_address(item["address"])
        fn = bv.get_function_at(address)
        if fn is None:
            raise OperationFailure(
                "verification_failed",
                f"Function missing after local retype at {item['address']}",
                requested=item.get("requested"),
                observed={"address": item["address"], "type": None},
            )
        var, _ = self._find_variable_by_storage(
            fn,
            int(item["storage"]),
            is_parameter=bool(item["is_parameter"]),
        )
        observed_type = str(var.type)
        item["observed"] = {"address": item["address"], "variable": str(var.name), "type": observed_type}
        if observed_type != item["expected_type"]:
            raise OperationFailure(
                "verification_failed",
                f"Live local retype verification failed at {item['address']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "noop" if item.get("before_type") == item["expected_type"] else "verified"
        return item

    def _verify_struct_field_set(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        type_obj = bv.get_type_by_name(item["struct_name"])
        if type_obj is None:
            raise OperationFailure(
                "verification_failed",
                f"Struct missing after field set: {item['struct_name']}",
                requested=item.get("requested"),
                observed={"type_name": item["struct_name"]},
            )
        member = self._find_member(type_obj, offset=int(item["member_offset"]), name=item["field_name"])
        observed = {
            "type_name": item["struct_name"],
            "offset": item["offset"],
            "field_name": getattr(member, "name", None),
            "field_type": str(getattr(member, "type", "")) if member is not None else None,
        }
        item["observed"] = observed
        if member is None or observed["field_type"] != item["field_type"]:
            raise OperationFailure(
                "verification_failed",
                f"Live struct field verification failed for {item['struct_name']} at {item['offset']}",
                requested=item.get("requested"),
                observed=observed,
            )
        previous = item.get("before_member")
        if previous and previous.get("field_name") == item["field_name"] and previous.get("field_type") == item["field_type"]:
            item["status"] = "noop"
        else:
            item["status"] = "verified"
        return item

    def _verify_struct_field_rename(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        type_obj = bv.get_type_by_name(item["struct_name"])
        if type_obj is None:
            raise OperationFailure(
                "verification_failed",
                f"Struct missing after field rename: {item['struct_name']}",
                requested=item.get("requested"),
                observed={"type_name": item["struct_name"]},
            )
        member = self._find_member(type_obj, name=item["new_name"])
        old_member = self._find_member(type_obj, name=item["old_name"])
        observed = {
            "type_name": item["struct_name"],
            "new_name": getattr(member, "name", None),
            "old_name_present": old_member is not None,
        }
        item["observed"] = observed
        if member is None or old_member is not None:
            raise OperationFailure(
                "verification_failed",
                f"Live struct field rename verification failed for {item['struct_name']}",
                requested=item.get("requested"),
                observed=observed,
            )
        item["status"] = "noop" if item["old_name"] == item["new_name"] else "verified"
        return item

    def _verify_struct_field_delete(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        type_obj = bv.get_type_by_name(item["struct_name"])
        if type_obj is None:
            raise OperationFailure(
                "verification_failed",
                f"Struct missing after field delete: {item['struct_name']}",
                requested=item.get("requested"),
                observed={"type_name": item["struct_name"]},
            )
        member = self._find_member(type_obj, name=item["field_name"])
        item["observed"] = {"type_name": item["struct_name"], "field_present": member is not None}
        if member is not None:
            raise OperationFailure(
                "verification_failed",
                f"Live struct field delete verification failed for {item['struct_name']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "verified"
        return item

    def _verify_declared_types(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        defined_types = dict(item.get("defined_types") or {})
        if not defined_types:
            item["observed"] = {
                "defined_types": {},
                "parsed_functions": list(item.get("parsed_functions") or []),
                "parsed_variables": list(item.get("parsed_variables") or []),
            }
            item["status"] = "noop"
            item["message"] = "Parsed declarations but no named types were defined."
            return item
        observed_types: dict[str, str | None] = {}
        for name, expected in defined_types.items():
            type_obj = bv.get_type_by_name(name)
            observed_types[name] = str(type_obj) if type_obj is not None else None
            if observed_types[name] != expected:
                raise OperationFailure(
                    "verification_failed",
                    f"Live type verification failed for {name}",
                    requested=item.get("requested"),
                    observed={"defined_types": observed_types},
                )
        item["observed"] = {"defined_types": observed_types}
        before = dict(item.get("before_defined_types") or {})
        item["status"] = "noop" if before and all(before.get(name) == expected for name, expected in defined_types.items()) else "verified"
        return item

    def _verify_patch_bytes(self, bv, result: dict[str, Any]) -> dict[str, Any]:
        item = dict(result)
        address = _parse_address(item["address"])
        expected = bytes.fromhex(item["patched"])
        observed_bytes = bv.read(address, len(expected)) or b""
        observed_hex = observed_bytes.hex()
        item["observed"] = {"address": item["address"], "patched": observed_hex}
        if observed_hex != item["patched"]:
            raise OperationFailure(
                "verification_failed",
                f"Live patch verification failed at {item['address']}",
                requested=item.get("requested"),
                observed=item["observed"],
            )
        item["status"] = "noop" if item.get("original") == item["patched"] else "verified"
        return item

    def _apply_operation(self, bv, op: dict[str, Any]):
        kind = op.get("op") or "rename_symbol"
        try:
            if kind == "rename_symbol":
                return self._op_rename_symbol(bv, op)
            if kind == "set_comment":
                return self._op_set_comment(bv, op)
            if kind == "delete_comment":
                return self._op_delete_comment(bv, op)
            if kind == "set_prototype":
                return self._op_set_prototype(bv, op)
            if kind == "local_rename":
                return self._op_local_rename(bv, op)
            if kind == "local_retype":
                return self._op_local_retype(bv, op)
            if kind == "struct_field_set":
                return self._op_struct_field_set(bv, op)
            if kind == "struct_field_rename":
                return self._op_struct_field_rename(bv, op)
            if kind == "struct_field_delete":
                return self._op_struct_field_delete(bv, op)
            if kind == "struct_replace":
                return self._op_struct_replace(bv, op)
            if kind == "types_declare":
                return self._op_types_declare(bv, op)
            if kind == "patch_bytes":
                return self._op_patch_bytes(bv, op)
            raise OperationFailure("unsupported", f"Unsupported batch operation: {kind}", requested=self._operation_requested(op))
        except OperationFailure:
            raise
        except Exception as exc:
            raise OperationFailure(
                "unsupported",
                f"{type(exc).__name__}: {exc}",
                requested=self._operation_requested(op),
            ) from exc

    def _mutation(self, selector: str | None, preview: bool, operations: list[dict[str, Any]]):
        if not operations:
            raise ValueError("Batch operation list is empty")

        bv = self._resolve_view(selector)
        affected = self._guess_affected_functions(bv, operations)
        before = self._capture_function_snapshots(bv, affected)
        type_before = self._capture_type_snapshots(bv, operations)
        state = bv.begin_undo_actions()
        results = []
        try:
            for op in operations:
                results.append(self._apply_operation(bv, op))
        except OperationFailure as exc:
            with contextlib.suppress(Exception):
                bv.revert_undo_actions(state)
            return {
                "preview": preview,
                "success": False,
                "committed": False,
                "message": "Rolled back before post-state verification because an operation failed to apply.",
                "results": self._mark_unverified_results(results, "Rolled back before post-state verification.")
                + [self._operation_failure_result(operations[len(results)], exc)],
                "affected_functions": [],
                "affected_types": [],
            }

        try:
            bv.update_analysis_and_wait()
            after = self._capture_function_snapshots(bv, affected)
            type_after = self._capture_type_snapshots(bv, operations)
            diffs = self._diff_snapshots(before, after)
            type_diffs = self._diff_type_snapshots(type_before, type_after)
            verified_results = [self._verify_operation(bv, result) for result in results]
            annotated_results = self._annotate_operation_results(verified_results, type_diffs)
            failed = self._has_failed_results(annotated_results)
            if preview or failed:
                bv.revert_undo_actions(state)
            else:
                bv.commit_undo_actions(state)
            message = None
            if preview:
                message = "Preview verified and reverted."
            elif failed:
                message = "Rolled back because live-session verification failed."
            else:
                message = "Applied and verified in the live Binary Ninja session."
            return {
                "preview": preview,
                "success": not failed,
                "committed": bool((not preview) and (not failed)),
                "message": message,
                "results": annotated_results,
                "affected_functions": diffs,
                "affected_types": type_diffs,
            }
        except Exception:
            with contextlib.suppress(Exception):
                bv.revert_undo_actions(state)
            raise

    def _op_rename_symbol(self, bv, op: dict[str, Any]):
        kind = str(op.get("kind", "auto"))
        identifier = op["identifier"]
        new_name = str(op["new_name"])
        target = self._resolve_rename_target(bv, identifier, kind)
        requested = self._operation_requested(op)
        if target["kind"] == "function":
            fn = bv.get_function_at(target["address"])
            if fn is None:
                raise OperationFailure("unsupported", f"Function not found: {identifier}", requested=requested)
            if target["before_name"] != new_name:
                fn.name = new_name
            return {
                "op": "rename_symbol",
                "kind": "function",
                "address": hex(target["address"]),
                "before_name": target["before_name"],
                "new_name": new_name,
                "requested": requested,
            }
        address = int(target["address"])
        if target["before_name"] != new_name:
            bv.define_user_symbol(bn.Symbol(bn.SymbolType.DataSymbol, address, new_name))
        return {
            "op": "rename_symbol",
            "kind": "data",
            "address": hex(address),
            "before_name": target["before_name"],
            "new_name": new_name,
            "requested": requested,
        }

    def _op_set_comment(self, bv, op: dict[str, Any]):
        comment = str(op["comment"])
        if op.get("function"):
            fn = self._find_function(bv, op["function"])
            before_comment = bv.get_comment_at(fn.start) or ""
            if before_comment != comment:
                bv.set_comment_at(fn.start, comment)
            return {
                "op": "set_comment",
                "address": hex(fn.start),
                "function": fn.name,
                "before_comment": before_comment,
                "requested": self._operation_requested(op),
            }
        address = _parse_address(op["address"])
        before_comment = bv.get_comment_at(address) or ""
        if before_comment != comment:
            bv.set_comment_at(address, comment)
        return {
            "op": "set_comment",
            "address": hex(address),
            "before_comment": before_comment,
            "requested": self._operation_requested(op),
        }

    def _op_delete_comment(self, bv, op: dict[str, Any]):
        if op.get("function"):
            fn = self._find_function(bv, op["function"])
            before_comment = bv.get_comment_at(fn.start) or ""
            if before_comment:
                bv.set_comment_at(fn.start, None)
            return {
                "op": "delete_comment",
                "address": hex(fn.start),
                "function": fn.name,
                "before_comment": before_comment,
                "requested": self._operation_requested(op),
            }
        address = _parse_address(op["address"])
        before_comment = bv.get_comment_at(address) or ""
        if before_comment:
            bv.set_comment_at(address, None)
        return {
            "op": "delete_comment",
            "address": hex(address),
            "before_comment": before_comment,
            "requested": self._operation_requested(op),
        }

    def _op_set_prototype(self, bv, op: dict[str, Any]):
        fn = self._find_function(bv, op["identifier"])
        expected_type, _ = bv.parse_type_string(str(op["prototype"]))
        before_prototype = str(fn.type)
        if before_prototype != str(expected_type):
            fn.set_user_type(expected_type)
        return {
            "op": "set_prototype",
            "function": fn.name,
            "address": hex(fn.start),
            "before_prototype": before_prototype,
            "expected_prototype": str(expected_type),
            "requested": self._operation_requested(op),
        }

    def _op_local_rename(self, bv, op: dict[str, Any]):
        fn = self._find_function(bv, op["function"])
        var = self._find_variable(fn, str(op["variable"]))
        new_name = str(op["new_name"])
        if str(var.name) != new_name:
            fn.create_user_var(var, var.type, new_name)
        return {
            "op": "local_rename",
            "function": fn.name,
            "address": hex(fn.start),
            "variable": str(op["variable"]),
            "storage": int(var.storage),
            "is_parameter": var in list(fn.parameter_vars),
            "before_name": str(var.name),
            "new_name": new_name,
            "requested": self._operation_requested(op),
        }

    def _op_local_retype(self, bv, op: dict[str, Any]):
        fn = self._find_function(bv, op["function"])
        var = self._find_variable(fn, str(op["variable"]))
        expected_type, _ = bv.parse_type_string(str(op["new_type"]))
        if str(var.type) != str(expected_type):
            fn.create_user_var(var, expected_type, var.name)
        return {
            "op": "local_retype",
            "function": fn.name,
            "address": hex(fn.start),
            "variable": str(op["variable"]),
            "storage": int(var.storage),
            "is_parameter": var in list(fn.parameter_vars),
            "before_type": str(var.type),
            "expected_type": str(expected_type),
            "requested": self._operation_requested(op),
        }

    def _struct_builder(self, bv, struct_name: str):
        type_obj = bv.get_type_by_name(struct_name)
        if type_obj is None:
            raise RuntimeError(f"Struct not found: {struct_name}")
        return type_obj.mutable_copy()

    def _commit_struct_builder(self, bv, struct_name: str, builder):
        bv.define_user_type(struct_name, builder)

    def _op_struct_field_set(self, bv, op: dict[str, Any]):
        struct_name = str(op["struct_name"])
        builder = self._struct_builder(bv, struct_name)
        field_type, _ = bv.parse_type_string(str(op["field_type"]))
        offset = _parse_address(op["offset"])
        overwrite = bool(op.get("overwrite_existing", True))
        before_type = bv.get_type_by_name(struct_name)
        before_member = None
        if before_type is not None:
            member = self._find_member(before_type, offset=offset)
            if member is not None:
                before_member = {
                    "field_name": str(getattr(member, "name", "")),
                    "field_type": str(getattr(member, "type", "")),
                    "offset": hex(int(getattr(member, "offset", offset))),
                }
        builder.add_member_at_offset(str(op["field_name"]), field_type, offset, overwrite)
        try:
            builder.width = max(int(builder.width), int(offset) + int(field_type.width))
        except Exception:
            pass
        self._commit_struct_builder(bv, struct_name, builder)
        return {
            "op": "struct_field_set",
            "struct_name": struct_name,
            "offset": hex(offset),
            "field_name": str(op["field_name"]),
            "field_type": str(field_type),
            "member_offset": int(offset),
            "before_member": before_member,
            "requested": self._operation_requested(op),
        }

    def _op_struct_field_rename(self, bv, op: dict[str, Any]):
        struct_name = str(op["struct_name"])
        builder = self._struct_builder(bv, struct_name)
        index = builder.index_by_name(str(op["old_name"]))
        if index is None:
            raise RuntimeError(f"Field not found: {op['old_name']}")
        member = builder[str(op["old_name"])]
        if member is None:
            raise RuntimeError(f"Field not found: {op['old_name']}")
        builder.replace(index, member.type, str(op["new_name"]), True)
        self._commit_struct_builder(bv, struct_name, builder)
        return {
            "op": "struct_field_rename",
            "struct_name": struct_name,
            "old_name": str(op["old_name"]),
            "new_name": str(op["new_name"]),
            "requested": self._operation_requested(op),
        }

    def _op_struct_field_delete(self, bv, op: dict[str, Any]):
        struct_name = str(op["struct_name"])
        builder = self._struct_builder(bv, struct_name)
        index = builder.index_by_name(str(op["field_name"]))
        if index is None:
            raise RuntimeError(f"Field not found: {op['field_name']}")
        builder.remove(index)
        self._commit_struct_builder(bv, struct_name, builder)
        return {
            "op": "struct_field_delete",
            "struct_name": struct_name,
            "field_name": str(op["field_name"]),
            "requested": self._operation_requested(op),
        }

    def _op_struct_replace(self, bv, op: dict[str, Any]):
        parsed = self._parse_declaration_source(
            bv,
            str(op["declaration"]),
            source_path=op.get("source_path"),
        )
        named_types = list(parsed["types"])
        if not named_types:
            raise OperationFailure("unsupported", "No named types found in declaration", requested=self._operation_requested(op))
        defined_types = {}
        before_defined_types = {}
        for name, type_obj in named_types:
            existing = bv.get_type_by_name(name)
            before_defined_types[str(name)] = str(existing) if existing is not None else None
            bv.define_user_type(name, type_obj)
            defined_types[str(name)] = str(type_obj)
        return {
            "op": "struct_replace",
            "defined_types": defined_types,
            "before_defined_types": before_defined_types,
            "parsed_functions": [name for name, _ in parsed["functions"]],
            "parsed_variables": [name for name, _ in parsed["variables"]],
            "requested": self._operation_requested(op),
        }

    def _op_types_declare(self, bv, op: dict[str, Any]):
        parsed = self._parse_declaration_source(
            bv,
            str(op["declaration"]),
            source_path=op.get("source_path"),
        )
        named_types = list(parsed["types"])
        defined_types = {}
        before_defined_types = {}
        for name, type_obj in named_types:
            existing = bv.get_type_by_name(name)
            before_defined_types[str(name)] = str(existing) if existing is not None else None
            bv.define_user_type(name, type_obj)
            defined_types[str(name)] = str(type_obj)
        return {
            "op": "types_declare",
            "defined_types": defined_types,
            "before_defined_types": before_defined_types,
            "count": len(defined_types),
            "parsed_functions": [name for name, _ in parsed["functions"]],
            "parsed_variables": [name for name, _ in parsed["variables"]],
            "parsed_type_count": len(named_types),
            "parsed_function_count": len(parsed["functions"]),
            "parsed_variable_count": len(parsed["variables"]),
            "requested": self._operation_requested(op),
        }

    def _op_patch_bytes(self, bv, op: dict[str, Any]):
        address = _parse_address(op["address"])
        data = _clean_bytes(str(op["data"]))
        original = bv.read(address, len(data))
        written = bv.write(address, data)
        if written != len(data):
            raise RuntimeError(f"Patched {written} bytes but expected {len(data)}")
        return {
            "op": "patch_bytes",
            "address": hex(address),
            "original": original.hex() if original else None,
            "patched": data.hex(),
            "requested": self._operation_requested(op),
        }


_bridge: BinaryNinjaBridge | None = None


def _start_bridge_command(_):  # pragma: no cover - GUI runtime
    start_bridge()


def start_bridge():  # pragma: no cover - GUI runtime
    global _bridge
    if ui is None:
        bn.log_warn("BN Agent Bridge requires the Binary Ninja GUI")
        return
    if _bridge is not None:
        return
    _bridge = BinaryNinjaBridge()
    _bridge.start()


def _stop_bridge():  # pragma: no cover - GUI runtime
    global _bridge
    if _bridge is not None:
        _bridge.stop()
        _bridge = None


atexit.register(_stop_bridge)

PluginCommand.register(
    "BN Agent Bridge\\Restart Bridge",
    "Restart the bn CLI socket bridge",
    _start_bridge_command,
)
