from __future__ import annotations

import os
import threading
import traceback
from collections import Counter
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

        return list(dict.fromkeys(found))

    return _run_on_main_thread(collect)


def _render_target_choices(targets: list[dict[str, Any]]) -> str:
    lines = []
    for target in targets:
        label = str(target.get("selector") or target.get("basename") or target.get("target_id") or "<unknown>")
        if target.get("active"):
            label += " [active]"
        target_id = target.get("target_id")
        if target_id:
            label += f" (target_id: {target_id})"
        if target.get("filename"):
            label += f"  {target['filename']}"
        lines.append(f"- {label}")
    return "\n".join(lines)


class TargetManager:
    def __init__(self):
        self._lock = threading.RLock()
        self._view_ids: dict[tuple[str, int], int] = {}
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

    def _snapshot(self) -> list[tuple[Any, dict[str, Any]]]:
        views, focused = _run_on_main_thread(lambda: (_collect_open_views(), _active_binary_view()))
        snapshot = []
        alias_counts: Counter[str] = Counter()
        with self._lock:
            live_ids = {}
            for bv in views:
                # BinaryView hashes its native handle. Qt can return fresh wrappers for one view.
                handle_hash = hash(bv)
                try:
                    session_id = str(bv.file.session_id)
                except Exception:
                    session_id = str(handle_hash)
                key = (session_id, handle_hash)
                view_id = self._view_ids.get(key)
                if view_id is None:
                    view_id = self._next_id
                    self._next_id += 1
                live_ids[key] = view_id
                try:
                    filename = str(bv.file.filename or "")
                except Exception:
                    filename = ""
                target_id = f"{os.getpid()}:{view_id}:{session_id}"
                basename = os.path.basename(filename)
                alias_counts.update({basename, filename, str(view_id), target_id})
                info = {
                    "target_id": target_id,
                    "view_id": str(view_id),
                    "session_id": session_id,
                    "filename": filename,
                    "basename": basename,
                    "view_name": self._view_name(bv),
                    "active": bv == focused or (focused is None and len(views) == 1),
                }
                snapshot.append((bv, info))
            self._view_ids = live_ids

        for _, info in snapshot:
            basename = info["basename"]
            info["selector"] = info["target_id"]
            if basename.strip() and basename == basename.strip() and basename != "active" and alias_counts[basename] == 1:
                info["selector"] = basename
        return sorted(snapshot, key=lambda pair: int(pair[1]["view_id"]))

    def refresh(self) -> list[dict[str, Any]]:
        return [info for _, info in self._snapshot()]

    def resolve_with_info(self, selector: str | None) -> tuple[Any, dict[str, Any]]:
        snapshot = self._snapshot()
        targets = [info for _, info in snapshot]
        if not targets:
            raise OperationFailure("no_targets", "No BinaryView targets are open in the GUI")
        if selector in (None, ""):
            if len(snapshot) == 1:
                return snapshot[0]
            raise OperationFailure(
                "target_required",
                f"This command requires --target when multiple targets are open.\nOpen targets:\n{_render_target_choices(targets)}",
                observed={"targets": targets},
            )
        selector = selector.strip()
        if selector == "active":
            for bv, info in snapshot:
                if info["active"]:
                    return bv, info
            raise OperationFailure(
                "target_required",
                f"No active BinaryView is selected; pass an explicit --target.\nOpen targets:\n{_render_target_choices(targets)}",
                requested={"target": selector}, observed={"targets": targets},
            )
        matches = []
        for bv, info in snapshot:
            if selector == info["target_id"]:
                return bv, info
            if selector and selector in (info["view_id"], info["filename"], info["basename"]):
                matches.append((bv, info))
        if len(matches) == 1:
            return matches[0]
        if matches:
            choices = [info for _, info in matches]
            raise OperationFailure(
                "ambiguous_target",
                f"Ambiguous target selector {selector!r}. Use a full target ID:\n{_render_target_choices(choices)}",
                requested={"target": selector}, observed={"targets": choices},
            )
        raise OperationFailure(
            "unknown_target",
            f"Unknown target selector: {selector}\nOpen targets:\n{_render_target_choices(targets)}",
            requested={"target": selector}, observed={"targets": targets},
        )

    def resolve(self, selector: str | None):
        bv, _ = self.resolve_with_info(selector)
        return bv
