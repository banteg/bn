from __future__ import annotations

import os
import threading
import traceback
import weakref
from dataclasses import dataclass
from typing import Any

import binaryninja as bn
from binaryninja.mainthread import execute_on_main_thread_and_wait, is_main_thread

from .errors import OperationFailure

try:
    import binaryninjaui as ui
except ImportError:  # pragma: no cover - GUI plugin only
    ui = None


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
            if frame is not None:
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


def _render_target_choices(targets: list[dict[str, Any]]) -> str:
    lines = []
    for target in targets:
        label = str(target.get("selector") or target.get("basename") or target.get("target_id") or "<unknown>")
        if target.get("active"):
            label += " [active]"
        target_id = target.get("target_id")
        if target_id:
            label += f" (target_id: {target_id})"
        lines.append(f"- {label}")
    return "\n".join(lines)


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
            raise OperationFailure("no_targets", "No BinaryView targets are open in the GUI")

        if selector in (None, ""):
            if len(targets) != 1:
                raise OperationFailure(
                    "target_required",
                    f"This command requires --target when multiple targets are open.\nOpen targets:\n{_render_target_choices(targets)}",
                    observed={"targets": targets},
                )
            selector = str(targets[0]["target_id"])

        if selector == "active":
            active = self._default_view()
            if active is None:
                raise OperationFailure(
                    "target_required",
                    f"No active BinaryView is selected; pass an explicit --target.\nOpen targets:\n{_render_target_choices(targets)}",
                    requested={"target": "active"},
                    observed={"targets": targets},
                )
            return active

        with self._lock:
            for record in self._records.values():
                if self._matches_record(record, selector):
                    view = record.ref()
                    if view is not None:
                        return view
        raise OperationFailure(
            "unknown_target",
            f"Unknown target selector: {selector}\nOpen targets:\n{_render_target_choices(targets)}",
            requested={"target": selector},
            observed={"targets": targets},
        )
