from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Callable

from .output import write_output_result
from .paths import plugin_install_dir, plugin_source_dir, skill_install_dir, skill_source_dir
from .transport import BridgeError, _send_request_to_instance, list_instances, send_request
from .version import VERSION, build_id_for_file

FAILED_MUTATION_STATUSES = {"unsupported", "verification_failed"}


def _package_version() -> str:
    return VERSION


def _common_io_options(
    parser: argparse.ArgumentParser,
    *,
    default_format: str = "text",
) -> None:
    parser.add_argument(
        "--format",
        choices=("json", "text", "ndjson"),
        default=default_format,
        help="Output format",
    )
    parser.add_argument("--out", type=Path, help="Write output to a file instead of stdout")


def _target_option(
    parser: argparse.ArgumentParser,
    *,
    required: bool,
    default: str | None = None,
) -> None:
    kwargs: dict[str, Any] = {
        "help": "Target selector from `bn target list` (`selector`, `target_id`, basename, filename, or view id) or `active`",
        "required": required,
    }
    if default is not None:
        kwargs["default"] = default
        kwargs["required"] = False
    parser.add_argument("--target", **kwargs)


def _render_result(
    value: Any,
    *,
    fmt: str,
    out_path: Path | None,
    stem: str,
    spill_label: str | None = None,
    spill_context: Any = None,
) -> None:
    result = write_output_result(value, fmt=fmt, out_path=out_path, stem=stem)
    sys.stdout.write(result.rendered)
    if result.spilled and result.artifact:
        artifact = result.artifact
        details = [
            f"format={artifact['format']}",
            f"tokens={artifact['tokens']}",
            f"bytes={artifact['bytes']}",
            f"tokenizer={artifact['tokenizer']}",
        ]
        if isinstance(spill_context, list):
            details.append(f"items={len(spill_context)}")
        if isinstance(value, str):
            details.append(f"lines={len(value.splitlines())}")
        label = spill_label or stem.replace("_", " ")
        print(
            f"warning: {label} output spilled to {artifact['artifact_path']} ({', '.join(details)})",
            file=sys.stderr,
        )


def _implicit_target(args: argparse.Namespace) -> str:
    response = send_request(
        "list_targets",
        params={},
        target=None,
    )
    targets = list(response["result"])
    if len(targets) == 1:
        return "active"
    if not targets:
        raise BridgeError("No BinaryView targets are open in the GUI")
    raise BridgeError("This command requires --target when multiple targets are open")


def _resolve_target(
    args: argparse.Namespace,
    *,
    require_target: bool,
    allow_implicit_target: bool = False,
) -> str | None:
    target = getattr(args, "target", None)
    if require_target and not target:
        if allow_implicit_target:
            return _implicit_target(args)
        raise BridgeError("This command requires --target")
    return target


def _mutation_exit_code(result: Any) -> int:
    if not isinstance(result, dict):
        return 0
    results = list(result.get("results") or [])
    if any(isinstance(item, dict) and item.get("status") in FAILED_MUTATION_STATUSES for item in results):
        return 3
    if result.get("success") is False:
        return 3
    return 0


def _call(
    args: argparse.Namespace,
    op: str,
    params: dict[str, Any] | None = None,
    *,
    require_target: bool,
    allow_implicit_target: bool = False,
    text_renderer: Callable[[Any], str] | None = None,
    page_limit: int | None = None,
    page_offset: int = 0,
    page_label: str | None = None,
    stem: str,
    result_exit_code: Callable[[Any], int] | None = None,
    bridge_writes_output: bool = False,
) -> int:
    request_params = dict(params or {})
    effective_page_limit = None
    if page_limit is not None and page_limit >= 0:
        effective_page_limit = page_limit
        request_params["limit"] = page_limit + 1

    target = _resolve_target(
        args,
        require_target=require_target,
        allow_implicit_target=allow_implicit_target,
    )
    response = send_request(
        op,
        params=request_params,
        target=target,
    )
    result = response["result"]
    exit_code = result_exit_code(result) if result_exit_code is not None else 0
    if effective_page_limit is not None and isinstance(result, list) and len(result) > effective_page_limit:
        result = result[:effective_page_limit]
        label = page_label or op
        next_offset = page_offset + effective_page_limit
        print(
            f"warning: {label} output truncated to {effective_page_limit} items; rerun with --offset {next_offset} or a larger --limit",
            file=sys.stderr,
        )
    spill_context = result
    if text_renderer is not None and args.format == "text":
        result = text_renderer(result)
    _render_result(
        result,
        fmt=args.format,
        out_path=None if bridge_writes_output else args.out,
        stem=stem,
        spill_label=page_label or op.replace("_", " "),
        spill_context=spill_context,
    )
    return exit_code


def _render_fallback_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=2, sort_keys=True)


def _format_local_entry(item: dict[str, Any]) -> str:
    role = "param" if item.get("is_parameter") else "local"
    details = [f"storage={item.get('storage', '?')}"]
    if item.get("source_type"):
        details.append(f"source={item['source_type']}")
    if item.get("index") is not None:
        details.append(f"index={item['index']}")
    if item.get("identifier") is not None:
        details.append(f"identifier={item['identifier']}")
    if item.get("local_id"):
        details.append(f"id={item['local_id']}")
    return (
        f"- {item.get('type', '<unknown>')} {item.get('name', '<unknown>')} "
        f"[{role}; {'; '.join(details)}]"
    )


def _text_field(field: str) -> Callable[[Any], str]:
    def render(value: Any) -> str:
        if isinstance(value, dict):
            text = value.get(field)
            if isinstance(text, str):
                return text
        return _render_fallback_text(value)

    return render


def _render_function_info_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)

    function = value.get("function") or {}
    lines = [
        f"{function.get('name', '<unknown>')} @ {function.get('address', '<unknown>')}",
        str(value.get("prototype", "")),
        f"return: {value.get('return_type', '<unknown>')}",
        f"calling convention: {value.get('calling_convention', '<unknown>')}",
        f"size: {value.get('size', '<unknown>')}",
        "",
        "parameters:",
    ]
    parameters = list(value.get("parameters") or [])
    if parameters:
        for item in parameters:
            lines.append(_format_local_entry(item))
    else:
        lines.append("- none")

    lines.extend(["", "locals:"])
    locals_only = list(value.get("locals") or [])
    if locals_only:
        for item in locals_only:
            lines.append(_format_local_entry(item))
    else:
        lines.append("- none")
    return "\n".join(lines)


def _render_proto_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    prototype = value.get("prototype")
    if isinstance(prototype, str):
        return prototype
    return _render_fallback_text(value)


def _render_local_list_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    function = value.get("function") or {}
    lines = [f"{function.get('name', '<unknown>')} @ {function.get('address', '<unknown>')}", ""]
    locals_only = list(value.get("locals") or [])
    if not locals_only:
        lines.append("locals: none")
        return "\n".join(lines)
    lines.append("locals:")
    for item in locals_only:
        lines.append(_format_local_entry(item))
    return "\n".join(lines)


def _render_type_info_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    layout = value.get("layout")
    if isinstance(layout, str) and layout:
        return layout
    decl = value.get("decl")
    if isinstance(decl, str) and decl:
        return decl
    return _render_fallback_text(value)


def _render_field_xrefs_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)

    field = value.get("field") or {}
    lines = [
        f"{field.get('type_name', '<unknown>')}.{field.get('field_name', '<unknown>')} @ +0x{int(field.get('offset', 0)):x}",
        f"type: {field.get('field_type', '<unknown>')}",
        "",
        "code refs:",
    ]
    code_refs = list(value.get("code_refs") or [])
    if code_refs:
        for ref in code_refs:
            details = [ref.get("address", "<unknown>")]
            if ref.get("function"):
                details.append(ref["function"])
            if ref.get("incoming_type"):
                details.append(f"type={ref['incoming_type']}")
            if ref.get("disasm"):
                details.append(ref["disasm"])
            lines.append("- " + " | ".join(details))
    else:
        lines.append("- none")

    lines.extend(["", "data refs:"])
    data_refs = list(value.get("data_refs") or [])
    if data_refs:
        for ref in data_refs:
            details = [ref.get("address", "<unknown>")]
            if ref.get("symbol"):
                details.append(ref["symbol"])
            if ref.get("type"):
                details.append(f"type={ref['type']}")
            lines.append("- " + " | ".join(details))
    else:
        lines.append("- none")

    return "\n".join(lines)


def _render_comment_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    comment = value.get("comment")
    if isinstance(comment, str):
        return comment
    return _render_fallback_text(value)


def _render_refresh_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    target = value.get("target")
    if isinstance(target, dict):
        return f"refreshed: true\n\n{_render_target_summary(target)}"
    return _render_fallback_text(value)


def _render_target_summary(value: dict[str, Any]) -> str:
    label = value.get("selector") or value.get("target_id") or "<unknown>"
    lines = [str(label)]
    if value.get("active"):
        lines[0] += " [active]"

    details = [
        ("target", value.get("target_id")),
        ("view", value.get("view_id")),
        ("kind", value.get("view_name")),
        ("file", value.get("filename")),
        ("arch", value.get("arch")),
        ("platform", value.get("platform")),
        ("entry", value.get("entry_point")),
    ]
    for key, item in details:
        if item not in (None, ""):
            lines.append(f"{key}: {item}")
    return "\n".join(lines)


def _render_target_list_text(value: Any) -> str:
    if not isinstance(value, list):
        return _render_fallback_text(value)
    if not value:
        return "no targets"
    return "\n\n".join(
        _render_target_summary(item) if isinstance(item, dict) else _render_fallback_text(item)
        for item in value
    )


def _render_target_info_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    return _render_target_summary(value)


def _render_name_address_list_text(value: Any) -> str:
    if not isinstance(value, list):
        return _render_fallback_text(value)
    if not value:
        return "none"

    lines = []
    for item in value:
        if not isinstance(item, dict):
            lines.append(_render_fallback_text(item))
            continue
        address = item.get("address", "<unknown>")
        name = item.get("name") or item.get("function") or "<unknown>"
        line = f"{address}  {name}"
        library = item.get("library")
        if library:
            line += f" [{library}]"
        raw_name = item.get("raw_name")
        if raw_name and raw_name != name:
            line += f" (raw: {raw_name})"
        lines.append(line)
    return "\n".join(lines)


def _render_xrefs_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)

    lines = [
        f"xrefs to {value.get('address', '<unknown>')}",
        "",
        "code refs:",
    ]
    code_refs = list(value.get("code_refs") or [])
    if code_refs:
        for ref in code_refs:
            if not isinstance(ref, dict):
                lines.append("- " + _render_fallback_text(ref))
                continue
            details = [str(ref.get("address", "<unknown>"))]
            if ref.get("function"):
                details.append(str(ref["function"]))
            lines.append("- " + " | ".join(details))
    else:
        lines.append("- none")

    lines.extend(["", "data refs:"])
    data_refs = list(value.get("data_refs") or [])
    if data_refs:
        for ref in data_refs:
            if not isinstance(ref, dict):
                lines.append("- " + _render_fallback_text(ref))
                continue
            details = [str(ref.get("address", "<unknown>"))]
            if ref.get("function"):
                details.append(str(ref["function"]))
            lines.append("- " + " | ".join(details))
    else:
        lines.append("- none")
    return "\n".join(lines)


def _render_type_list_text(value: Any) -> str:
    if not isinstance(value, list):
        return _render_fallback_text(value)
    if not value:
        return "none"

    lines = []
    for item in value:
        if not isinstance(item, dict):
            lines.append(_render_fallback_text(item))
            continue
        name = item.get("name", "<unknown>")
        kind = item.get("kind", "<unknown>")
        decl = item.get("decl")
        line = f"{name} | {kind}"
        if decl:
            line += f" | {decl}"
        lines.append(line)
    return "\n".join(lines)


def _render_strings_text(value: Any) -> str:
    if not isinstance(value, list):
        return _render_fallback_text(value)
    if not value:
        return "none"

    lines = []
    for item in value:
        if not isinstance(item, dict):
            lines.append(_render_fallback_text(item))
            continue
        address = item.get("address", "<unknown>")
        length = item.get("length", "?")
        string_type = item.get("type", "")
        rendered = json.dumps(item.get("value", ""), ensure_ascii=True)
        lines.append(f"{address}  len={length}  {string_type}  {rendered}".rstrip())
    return "\n".join(lines)


def _render_doctor_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)

    lines = [
        f"cli version: {value.get('cli_version', '<unknown>')}",
        f"plugin source: {value.get('plugin_source_dir', '<unknown>')}",
        f"plugin install: {value.get('plugin_install_dir', '<unknown>')}",
        f"plugin source build: {value.get('plugin_source_build_id', '<unknown>')}",
        f"plugin install build: {value.get('plugin_install_build_id', '<unknown>')}",
        "",
        "instances:",
    ]
    instances = list(value.get("instances") or [])
    if not instances:
        lines.append("- none")
        return "\n".join(lines)

    for item in instances:
        if not isinstance(item, dict):
            lines.append("- " + _render_fallback_text(item))
            continue
        doctor = item.get("doctor") if isinstance(item.get("doctor"), dict) else {}
        status = "ok" if doctor and not doctor.get("error") else "error"
        lines.append(
            "- "
            + f"pid={item.get('pid', '<unknown>')} plugin={item.get('plugin_version', '<unknown>')} status={status}"
        )
        build_id = item.get("plugin_build_id")
        if build_id:
            lines.append(f"  build: {build_id}")
        if item.get("stale_plugin_version"):
            lines.append("  stale: loaded plugin version differs from CLI version")
        if item.get("stale_plugin_code"):
            lines.append("  stale: loaded plugin code does not match installed plugin file")
        if item.get("started_at"):
            lines.append(f"  started: {item['started_at']}")
        if item.get("socket_path"):
            lines.append(f"  socket: {item['socket_path']}")
        error = doctor.get("error")
        if error:
            lines.append(f"  error: {error}")
    return "\n".join(lines)


def _format_operation_result(item: dict[str, Any]) -> str:
    op = item.get("op", "<unknown>")
    if op == "rename_symbol":
        return f"rename_symbol {item.get('kind', 'auto')} {item.get('address', '<unknown>')} -> {item.get('new_name', '<unknown>')}"
    if op == "set_comment":
        target = item.get("function") or item.get("address", "<unknown>")
        return f"set_comment {target}"
    if op == "delete_comment":
        target = item.get("function") or item.get("address", "<unknown>")
        return f"delete_comment {target}"
    if op == "set_prototype":
        return f"set_prototype {item.get('function', '<unknown>')} @ {item.get('address', '<unknown>')}"
    if op in {"local_rename", "local_retype"}:
        target = item.get("local_id") or item.get("variable", "<unknown>")
        return f"{op} {item.get('function', '<unknown>')}::{target}"
    if op == "struct_field_set":
        return (
            f"struct_field_set {item.get('struct_name', '<unknown>')} "
            f"{item.get('offset', '<unknown>')} {item.get('field_name', '<unknown>')} {item.get('field_type', '<unknown>')}"
        )
    if op == "struct_field_rename":
        return (
            f"struct_field_rename {item.get('struct_name', '<unknown>')} "
            f"{item.get('old_name', '<unknown>')} -> {item.get('new_name', '<unknown>')}"
        )
    if op == "struct_field_delete":
        return f"struct_field_delete {item.get('struct_name', '<unknown>')}::{item.get('field_name', '<unknown>')}"
    if op == "types_declare":
        return (
            f"types_declare {item.get('count', 0)} types"
            f" (parsed functions={item.get('parsed_function_count', len(item.get('parsed_functions') or []))},"
            f" variables={item.get('parsed_variable_count', len(item.get('parsed_variables') or []))})"
        )
    return _render_fallback_text(item)


def _render_mutation_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)

    lines = [
        f"preview: {bool(value.get('preview'))}",
        f"success: {bool(value.get('success', True))}",
        f"committed: {bool(value.get('committed', False))}",
    ]
    if value.get("message"):
        lines.append(f"message: {value['message']}")
    lines.extend(["", "results:"])
    results = list(value.get("results") or [])
    if results:
        for item in results:
            if isinstance(item, dict):
                summary = _format_operation_result(item)
                if item.get("status"):
                    summary += f" [status={item['status']}]"
                if "changed" in item:
                    summary += f" [changed={bool(item['changed'])}]"
                if item.get("message"):
                    summary += f" ({item['message']})"
                lines.append("- " + summary)
                if item.get("requested"):
                    lines.append("  requested: " + json.dumps(item["requested"], sort_keys=True))
                if item.get("observed"):
                    lines.append("  observed: " + json.dumps(item["observed"], sort_keys=True))
            else:
                lines.append("- " + _render_fallback_text(item))
    else:
        lines.append("- none")

    lines.extend(["", "affected functions:"])
    affected_functions = list(value.get("affected_functions") or [])
    if affected_functions:
        for item in affected_functions:
            if not isinstance(item, dict):
                lines.append("- " + _render_fallback_text(item))
                continue
            before_name = item.get("before_name") or item.get("after_name") or "<unknown>"
            after_name = item.get("after_name") or before_name
            summary = f"{item.get('address', '<unknown>')} {before_name}"
            if after_name != before_name:
                summary += f" -> {after_name}"
            summary += f" [changed={bool(item.get('changed'))}]"
            lines.append("- " + summary)
            if item.get("diff"):
                lines.append(str(item["diff"]))
    else:
        lines.append("- none")

    lines.extend(["", "affected types:"])
    affected_types = list(value.get("affected_types") or [])
    if affected_types:
        for item in affected_types:
            if not isinstance(item, dict):
                lines.append("- " + _render_fallback_text(item))
                continue
            summary = f"{item.get('type_name', '<unknown>')} [changed={bool(item.get('changed'))}]"
            if item.get("message"):
                summary += f" ({item['message']})"
            lines.append("- " + summary)
            if item.get("layout_diff"):
                lines.append(str(item["layout_diff"]))
    else:
        lines.append("- none")
    return "\n".join(lines)


def _render_py_exec_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)

    parts: list[str] = []
    stdout = value.get("stdout")
    if isinstance(stdout, str) and stdout:
        parts.append(stdout.rstrip("\n"))

    result = value.get("result")
    if result is not None:
        body = result if isinstance(result, str) else json.dumps(result, indent=2, sort_keys=True)
        prefix = "result:\n" if parts else "result:\n"
        parts.append(prefix + body)

    warnings = list(value.get("warnings") or [])
    if warnings:
        parts.append("warnings:\n" + "\n".join(f"- {warning}" for warning in warnings))

    artifact = value.get("artifact")
    if isinstance(artifact, dict) and artifact.get("artifact_path"):
        parts.append(f"artifact: {artifact['artifact_path']}")

    if not parts:
        return ""
    return "\n\n".join(parts)


def _doctor(args: argparse.Namespace) -> int:
    install_dir = plugin_install_dir()
    source_dir = plugin_source_dir()
    install_bridge = install_dir / "bridge.py"
    source_bridge = source_dir / "bridge.py"
    install_build_id = build_id_for_file(install_bridge)
    source_build_id = build_id_for_file(source_bridge)
    instances = []
    for instance in list_instances():
        ping: dict[str, Any]
        try:
            response = _send_request_to_instance(
                instance,
                "doctor",
                params={},
                target=None,
            )
            ping = response["result"]
        except Exception as exc:
            ping = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

        loaded_version = ping.get("plugin_version") if isinstance(ping, dict) else None
        loaded_build_id = ping.get("plugin_build_id") if isinstance(ping, dict) else None
        instances.append(
            {
                "pid": instance.pid,
                "socket_path": str(instance.socket_path),
                "plugin_version": instance.plugin_version,
                "plugin_build_id": loaded_build_id,
                "installed_plugin_build_id": install_build_id,
                "source_plugin_build_id": source_build_id,
                "stale_plugin_version": (
                    bool(loaded_version)
                    and str(loaded_version) != _package_version()
                ),
                "stale_plugin_code": (
                    bool(loaded_build_id)
                    and install_build_id is not None
                    and loaded_build_id != install_build_id
                ),
                "started_at": instance.started_at,
                "doctor": ping,
            }
        )

    result = {
        "cli_version": _package_version(),
        "plugin_source_dir": str(source_dir),
        "plugin_install_dir": str(install_dir),
        "plugin_source_build_id": source_build_id,
        "plugin_install_build_id": install_build_id,
        "instances": instances,
    }
    if args.format == "text":
        result = _render_doctor_text(result)
    _render_result(result, fmt=args.format, out_path=args.out, stem="doctor")
    return 0


def _plugin_install(args: argparse.Namespace) -> int:
    source = plugin_source_dir()
    dest = args.dest or plugin_install_dir()
    _install_tree(source, dest, mode=args.mode, force=args.force)

    _render_result(
        {
            "installed": True,
            "mode": args.mode,
            "source": str(source),
            "destination": str(dest),
        },
        fmt=args.format,
        out_path=args.out,
        stem="plugin-install",
    )
    return 0


def _install_tree(source: Path, dest: Path, *, mode: str, force: bool) -> None:
    if not source.exists():
        raise BridgeError(f"Source directory is missing: {source}")

    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() or dest.is_symlink():
        if not force:
            raise BridgeError(f"Destination already exists: {dest}")
        if dest.is_symlink() or dest.is_file():
            dest.unlink()
        else:
            shutil.rmtree(dest)

    if mode == "copy":
        shutil.copytree(source, dest)
    else:
        os.symlink(source, dest, target_is_directory=True)


def _skill_install(args: argparse.Namespace) -> int:
    source = skill_source_dir()
    dest = args.dest or skill_install_dir()
    _install_tree(source, dest, mode=args.mode, force=args.force)

    _render_result(
        {
            "installed": True,
            "mode": args.mode,
            "skill": source.name,
            "source": str(source),
            "destination": str(dest),
        },
        fmt=args.format,
        out_path=args.out,
        stem="skill-install",
    )
    return 0


def _target_list(args: argparse.Namespace) -> int:
    return _call(
        args,
        "list_targets",
        {},
        require_target=False,
        text_renderer=_render_target_list_text,
        stem="targets",
    )


def _target_info(args: argparse.Namespace) -> int:
    return _call(
        args,
        "target_info",
        {"selector": args.target},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_target_info_text,
        stem="target-info",
    )


def _refresh(args: argparse.Namespace) -> int:
    return _call(
        args,
        "refresh",
        {},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_refresh_text,
        stem="refresh",
    )


def _function_list(args: argparse.Namespace) -> int:
    params: dict[str, Any] = {}
    if args.min_address is not None:
        params["min_address"] = args.min_address
    if args.max_address is not None:
        params["max_address"] = args.max_address
    return _call(
        args,
        "list_functions",
        params,
        require_target=True,
        text_renderer=_render_name_address_list_text,
        page_label="function list",
        stem="functions",
    )


def _function_search(args: argparse.Namespace) -> int:
    params = {
        "query": args.query,
        "regex": bool(args.regex),
    }
    if args.min_address is not None:
        params["min_address"] = args.min_address
    if args.max_address is not None:
        params["max_address"] = args.max_address
    return _call(
        args,
        "search_functions",
        params,
        require_target=True,
        text_renderer=_render_name_address_list_text,
        page_label="function search",
        stem="function-search",
    )


def _function_info(args: argparse.Namespace) -> int:
    return _call(
        args,
        "function_info",
        {"identifier": args.identifier},
        require_target=True,
        text_renderer=_render_function_info_text,
        stem="function-info",
    )


def _decompile(args: argparse.Namespace) -> int:
    return _call(
        args,
        "decompile",
        {"identifier": args.identifier},
        require_target=True,
        text_renderer=_text_field("text"),
        stem="decompile",
    )


def _il(args: argparse.Namespace) -> int:
    return _call(
        args,
        "il",
        {"identifier": args.identifier, "view": args.view, "ssa": bool(args.ssa)},
        require_target=True,
        text_renderer=_text_field("text"),
        stem="il",
    )


def _disasm(args: argparse.Namespace) -> int:
    return _call(
        args,
        "disasm",
        {"identifier": args.identifier},
        require_target=True,
        text_renderer=_text_field("text"),
        stem="disasm",
    )


def _xrefs(args: argparse.Namespace) -> int:
    if args.identifier == "field":
        if len(args.extra) != 1:
            raise BridgeError("Usage: bn xrefs field <Struct.field>")
        return _call(
            args,
            "field_xrefs",
            {"field": args.extra[0]},
            require_target=True,
            text_renderer=_render_field_xrefs_text,
            stem="field-xrefs",
        )
    if not args.identifier:
        raise BridgeError("xrefs requires an identifier")
    return _call(
        args,
        "xrefs",
        {"identifier": args.identifier},
        require_target=True,
        text_renderer=_render_xrefs_text,
        stem="xrefs",
    )


def _types(args: argparse.Namespace) -> int:
    return _call(
        args,
        "types",
        {"query": args.query, "offset": args.offset, "limit": args.limit},
        require_target=True,
        text_renderer=_render_type_list_text,
        page_limit=args.limit,
        page_offset=args.offset,
        page_label="types",
        stem="types",
    )


def _types_show(args: argparse.Namespace) -> int:
    return _call(
        args,
        "type_info",
        {
            "type_name": args.type_name,
            "require_struct": bool(getattr(args, "require_struct", False)),
        },
        require_target=True,
        text_renderer=_render_type_info_text,
        stem="type-show",
    )


def _types_declare(args: argparse.Namespace) -> int:
    source_path = None
    if args.file is not None:
        if not args.file.exists():
            raise BridgeError(f"Declaration file not found: {args.file}")
        declaration = args.file.read_text(encoding="utf-8")
        source_path = str(args.file)
    elif args.stdin:
        declaration = sys.stdin.read()
    elif args.declaration:
        declaration = args.declaration
    else:
        raise BridgeError("Provide a declaration string, --file, or --stdin")

    return _call(
        args,
        "types_declare",
        {
            "declaration": declaration,
            "source_path": source_path,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="types-declare",
        result_exit_code=_mutation_exit_code,
    )


def _strings(args: argparse.Namespace) -> int:
    return _call(
        args,
        "strings",
        {"query": args.query, "offset": args.offset, "limit": args.limit},
        require_target=True,
        text_renderer=_render_strings_text,
        page_limit=args.limit,
        page_offset=args.offset,
        page_label="strings",
        stem="strings",
    )


def _imports(args: argparse.Namespace) -> int:
    return _call(
        args,
        "imports",
        {},
        require_target=True,
        text_renderer=_render_name_address_list_text,
        stem="imports",
    )


def _bundle_function(args: argparse.Namespace) -> int:
    return _call(
        args,
        "bundle_function",
        {"identifier": args.identifier, "out_path": str(args.out) if args.out else None},
        require_target=True,
        allow_implicit_target=True,
        stem="function-bundle",
        bridge_writes_output=bool(args.out),
    )


def _py_exec(args: argparse.Namespace) -> int:
    if getattr(args, "code", None) is not None:
        script = args.code
    elif args.script:
        if not args.script.exists():
            raise BridgeError(f"Script file not found: {args.script}. Use --code for inline Python.")
        script = args.script.read_text(encoding="utf-8")
    else:
        script = sys.stdin.read()

    return _call(
        args,
        "py_exec",
        {"script": script},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_py_exec_text,
        stem="py-exec",
    )


def _symbol_rename(args: argparse.Namespace) -> int:
    return _call(
        args,
        "rename_symbol",
        {
            "kind": args.kind,
            "identifier": args.identifier,
            "new_name": args.new_name,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="symbol-rename",
        result_exit_code=_mutation_exit_code,
    )


def _comment_set(args: argparse.Namespace) -> int:
    return _call(
        args,
        "set_comment",
        {
            "address": args.address,
            "function": args.function,
            "comment": args.comment,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="comment-set",
        result_exit_code=_mutation_exit_code,
    )


def _comment_get(args: argparse.Namespace) -> int:
    return _call(
        args,
        "get_comment",
        {
            "address": args.address,
            "function": args.function,
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_comment_text,
        stem="comment-get",
    )


def _comment_delete(args: argparse.Namespace) -> int:
    return _call(
        args,
        "delete_comment",
        {
            "address": args.address,
            "function": args.function,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="comment-delete",
        result_exit_code=_mutation_exit_code,
    )


def _proto_set(args: argparse.Namespace) -> int:
    return _call(
        args,
        "set_prototype",
        {
            "identifier": args.identifier,
            "prototype": args.prototype,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="prototype-set",
        result_exit_code=_mutation_exit_code,
    )


def _proto_get(args: argparse.Namespace) -> int:
    return _call(
        args,
        "get_prototype",
        {"identifier": args.identifier},
        require_target=True,
        text_renderer=_render_proto_text,
        stem="prototype-get",
    )


def _local_list(args: argparse.Namespace) -> int:
    return _call(
        args,
        "list_locals",
        {"identifier": args.function},
        require_target=True,
        text_renderer=_render_local_list_text,
        stem="local-list",
    )


def _local_rename(args: argparse.Namespace) -> int:
    return _call(
        args,
        "local_rename",
        {
            "function": args.function,
            "variable": args.variable,
            "new_name": args.new_name,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="local-rename",
        result_exit_code=_mutation_exit_code,
    )


def _local_retype(args: argparse.Namespace) -> int:
    return _call(
        args,
        "local_retype",
        {
            "function": args.function,
            "variable": args.variable,
            "new_type": args.new_type,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="local-retype",
        result_exit_code=_mutation_exit_code,
    )


def _struct_field_set(args: argparse.Namespace) -> int:
    return _call(
        args,
        "struct_field_set",
        {
            "struct_name": args.struct_name,
            "offset": args.offset,
            "field_name": args.field_name,
            "field_type": args.field_type,
            "overwrite_existing": not args.no_overwrite,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="struct-field-set",
        result_exit_code=_mutation_exit_code,
    )


def _struct_show(args: argparse.Namespace) -> int:
    return _call(
        args,
        "type_info",
        {
            "type_name": args.struct_name,
            "require_struct": True,
        },
        require_target=True,
        text_renderer=_render_type_info_text,
        stem="struct-show",
    )


def _struct_field_rename(args: argparse.Namespace) -> int:
    return _call(
        args,
        "struct_field_rename",
        {
            "struct_name": args.struct_name,
            "old_name": args.old_name,
            "new_name": args.new_name,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="struct-field-rename",
        result_exit_code=_mutation_exit_code,
    )


def _struct_field_delete(args: argparse.Namespace) -> int:
    return _call(
        args,
        "struct_field_delete",
        {
            "struct_name": args.struct_name,
            "field_name": args.field_name,
            "preview": bool(args.preview),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_mutation_text,
        stem="struct-field-delete",
        result_exit_code=_mutation_exit_code,
    )


def _batch_apply(args: argparse.Namespace) -> int:
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if args.preview:
        manifest["preview"] = True
    return _call(
        args,
        "batch_apply",
        manifest,
        require_target=False,
        text_renderer=_render_mutation_text,
        stem="batch-apply",
        result_exit_code=_mutation_exit_code,
    )


def _add_paged_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=100)


def _add_function_address_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--min-address",
        help="Only include functions whose start address is at or above this address",
    )
    parser.add_argument(
        "--max-address",
        help="Only include functions whose start address is at or below this address",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bn", description="Agent-friendly Binary Ninja CLI")
    parser.set_defaults(handler=None, _parser=parser)

    subparsers = parser.add_subparsers(dest="command")

    doctor = subparsers.add_parser("doctor", help="Validate bridge discovery and installation")
    _common_io_options(doctor)
    doctor.set_defaults(handler=_doctor)

    plugin = subparsers.add_parser("plugin", help="Install the Binary Ninja companion plugin")
    plugin.set_defaults(_parser=plugin)
    plugin_sub = plugin.add_subparsers(dest="plugin_command")
    plugin_install = plugin_sub.add_parser("install", help="Install the GUI plugin")
    plugin_install.add_argument("--dest", type=Path, help="Custom install destination")
    plugin_install.add_argument("--mode", choices=("symlink", "copy"), default="symlink")
    plugin_install.add_argument("--force", action="store_true")
    _common_io_options(plugin_install, default_format="json")
    plugin_install.set_defaults(handler=_plugin_install)

    skill = subparsers.add_parser("skill", help="Install the bundled Codex skill")
    skill.set_defaults(_parser=skill)
    skill_sub = skill.add_subparsers(dest="skill_command")
    skill_install = skill_sub.add_parser("install", help="Install the bundled Codex skill")
    skill_install.add_argument("--dest", type=Path, help="Custom install destination")
    skill_install.add_argument("--mode", choices=("symlink", "copy"), default="symlink")
    skill_install.add_argument("--force", action="store_true")
    _common_io_options(skill_install, default_format="json")
    skill_install.set_defaults(handler=_skill_install)

    target = subparsers.add_parser("target", help="Inspect Binary Ninja targets")
    target.set_defaults(_parser=target)
    target_sub = target.add_subparsers(dest="target_command")
    target_list = target_sub.add_parser("list", help="List open BinaryView targets")
    _common_io_options(target_list)
    target_list.set_defaults(handler=_target_list)
    target_info = target_sub.add_parser("info", help="Show one target")
    _common_io_options(target_info)
    _target_option(target_info, required=False)
    target_info.set_defaults(handler=_target_info)

    refresh = subparsers.add_parser("refresh", help="Refresh analysis for the selected target")
    _common_io_options(refresh)
    _target_option(refresh, required=False)
    refresh.set_defaults(handler=_refresh)

    function = subparsers.add_parser("function", help="Function discovery helpers")
    function.set_defaults(_parser=function)
    function_sub = function.add_subparsers(dest="function_command")
    function_list = function_sub.add_parser("list", help="List functions")
    _common_io_options(function_list)
    _target_option(function_list, required=False, default="active")
    _add_function_address_args(function_list)
    function_list.set_defaults(handler=_function_list)
    function_search = function_sub.add_parser("search", help="Search functions by substring or regex")
    _common_io_options(function_search)
    _target_option(function_search, required=False, default="active")
    _add_function_address_args(function_search)
    function_search.add_argument(
        "--regex",
        action="store_true",
        help="Interpret query as a case-insensitive regular expression",
    )
    function_search.add_argument("query")
    function_search.set_defaults(handler=_function_search)
    function_info = function_sub.add_parser("info", help="Show function prototype and variables")
    _common_io_options(function_info)
    _target_option(function_info, required=False, default="active")
    function_info.add_argument("identifier")
    function_info.set_defaults(handler=_function_info)

    decompile = subparsers.add_parser("decompile", help="Render HLIL-style decompile text for a function")
    _common_io_options(decompile)
    _target_option(decompile, required=False, default="active")
    decompile.add_argument("identifier")
    decompile.set_defaults(handler=_decompile)

    il = subparsers.add_parser("il", help="Dump IL for a function")
    _common_io_options(il)
    _target_option(il, required=False, default="active")
    il.add_argument("identifier")
    il.add_argument("--view", choices=("hlil", "mlil", "llil"), default="hlil")
    il.add_argument("--ssa", action="store_true")
    il.set_defaults(handler=_il)

    disasm = subparsers.add_parser("disasm", help="Disassemble a function")
    _common_io_options(disasm)
    _target_option(disasm, required=False, default="active")
    disasm.add_argument("identifier")
    disasm.set_defaults(handler=_disasm)

    xrefs = subparsers.add_parser("xrefs", help="List xrefs to an address or function, or `field <Struct.field>`")
    _common_io_options(xrefs)
    _target_option(xrefs, required=False, default="active")
    xrefs.add_argument("identifier", nargs="?")
    xrefs.add_argument("extra", nargs="*")
    xrefs.set_defaults(handler=_xrefs)

    types = subparsers.add_parser("types", help="List or search types")
    _common_io_options(types)
    _target_option(types, required=False, default="active")
    _add_paged_args(types)
    types.add_argument("--query")
    types.set_defaults(handler=_types)
    types_sub = types.add_subparsers(dest="types_command")
    types_show = types_sub.add_parser("show", help="Show one type")
    _common_io_options(types_show)
    _target_option(types_show, required=False, default="active")
    types_show.add_argument("type_name")
    types_show.set_defaults(handler=_types_show)
    types_declare = types_sub.add_parser("declare", help="Import C declarations as user types")
    _common_io_options(types_declare, default_format="json")
    _target_option(types_declare, required=False)
    types_declare.add_argument("--preview", action="store_true")
    types_declare.add_argument("--file", type=Path, help="Read declarations from a file")
    types_declare.add_argument("--stdin", action="store_true", help="Read declarations from stdin")
    types_declare.add_argument("declaration", nargs="?")
    types_declare.set_defaults(handler=_types_declare)

    strings = subparsers.add_parser("strings", help="List or search strings")
    _common_io_options(strings)
    _target_option(strings, required=False, default="active")
    _add_paged_args(strings)
    strings.add_argument("--query")
    strings.set_defaults(handler=_strings)

    imports = subparsers.add_parser("imports", help="List imports")
    _common_io_options(imports)
    _target_option(imports, required=False, default="active")
    imports.set_defaults(handler=_imports)

    bundle = subparsers.add_parser("bundle", help="Export reusable bundles")
    bundle.set_defaults(_parser=bundle)
    bundle_sub = bundle.add_subparsers(dest="bundle_command")
    bundle_function = bundle_sub.add_parser("function", help="Export a function bundle")
    _common_io_options(bundle_function, default_format="json")
    _target_option(bundle_function, required=False)
    bundle_function.add_argument("identifier")
    bundle_function.set_defaults(handler=_bundle_function)

    py = subparsers.add_parser("py", help="Execute Python inside Binary Ninja")
    py.set_defaults(_parser=py)
    py_sub = py.add_subparsers(dest="py_command")
    py_exec = py_sub.add_parser("exec", help="Execute a Python snippet")
    _common_io_options(py_exec)
    _target_option(py_exec, required=False)
    source = py_exec.add_mutually_exclusive_group(required=True)
    source.add_argument("--script", type=Path, help="Read Python code from a file")
    source.add_argument("--code", help="Inline Python code")
    source.add_argument("--stdin", action="store_true")
    py_exec.set_defaults(handler=_py_exec)

    symbol = subparsers.add_parser("symbol", help="Rename functions or data")
    symbol.set_defaults(_parser=symbol)
    symbol_sub = symbol.add_subparsers(dest="symbol_command")
    symbol_rename = symbol_sub.add_parser("rename", help="Rename a symbol")
    _common_io_options(symbol_rename, default_format="json")
    _target_option(symbol_rename, required=False)
    symbol_rename.add_argument("--kind", choices=("auto", "function", "data"), default="auto")
    symbol_rename.add_argument("--preview", action="store_true")
    symbol_rename.add_argument("identifier")
    symbol_rename.add_argument("new_name")
    symbol_rename.set_defaults(handler=_symbol_rename)

    comment = subparsers.add_parser("comment", help="Set or delete comments")
    comment.set_defaults(_parser=comment)
    comment_sub = comment.add_subparsers(dest="comment_command")
    comment_get = comment_sub.add_parser("get", help="Get a comment")
    _common_io_options(comment_get)
    _target_option(comment_get, required=False)
    comment_get.add_argument("--address")
    comment_get.add_argument("--function")
    comment_get.set_defaults(handler=_comment_get)
    comment_set = comment_sub.add_parser("set", help="Set a comment")
    _common_io_options(comment_set, default_format="json")
    _target_option(comment_set, required=False)
    comment_set.add_argument("--preview", action="store_true")
    comment_set.add_argument("--address")
    comment_set.add_argument("--function")
    comment_set.add_argument("comment")
    comment_set.set_defaults(handler=_comment_set)
    comment_delete = comment_sub.add_parser("delete", help="Delete a comment")
    _common_io_options(comment_delete, default_format="json")
    _target_option(comment_delete, required=False)
    comment_delete.add_argument("--preview", action="store_true")
    comment_delete.add_argument("--address")
    comment_delete.add_argument("--function")
    comment_delete.set_defaults(handler=_comment_delete)

    proto = subparsers.add_parser("proto", help="Inspect or set a user prototype")
    proto.set_defaults(_parser=proto)
    proto_sub = proto.add_subparsers(dest="proto_command")
    proto_get = proto_sub.add_parser("get", help="Show the current prototype")
    _common_io_options(proto_get)
    _target_option(proto_get, required=False, default="active")
    proto_get.add_argument("identifier")
    proto_get.set_defaults(handler=_proto_get)
    proto_set = proto_sub.add_parser("set", help="Set a prototype")
    _common_io_options(proto_set, default_format="json")
    _target_option(proto_set, required=False)
    proto_set.add_argument("--preview", action="store_true")
    proto_set.add_argument("identifier")
    proto_set.add_argument("prototype")
    proto_set.set_defaults(handler=_proto_set)

    local = subparsers.add_parser("local", help="Inspect, rename, or retype locals")
    local.set_defaults(_parser=local)
    local_sub = local.add_subparsers(dest="local_command")
    local_list = local_sub.add_parser("list", help="List locals with stable IDs")
    _common_io_options(local_list)
    _target_option(local_list, required=False, default="active")
    local_list.add_argument("function")
    local_list.set_defaults(handler=_local_list)
    local_rename = local_sub.add_parser("rename", help="Rename a local")
    _common_io_options(local_rename, default_format="json")
    _target_option(local_rename, required=False)
    local_rename.add_argument("--preview", action="store_true")
    local_rename.add_argument("function")
    local_rename.add_argument("variable", help="Stable local_id or legacy variable name")
    local_rename.add_argument("new_name")
    local_rename.set_defaults(handler=_local_rename)
    local_retype = local_sub.add_parser("retype", help="Retype a local")
    _common_io_options(local_retype, default_format="json")
    _target_option(local_retype, required=False)
    local_retype.add_argument("--preview", action="store_true")
    local_retype.add_argument("function")
    local_retype.add_argument("variable", help="Stable local_id or legacy variable name")
    local_retype.add_argument("new_type")
    local_retype.set_defaults(handler=_local_retype)

    struct = subparsers.add_parser("struct", help="Field-first structure editing")
    struct.set_defaults(_parser=struct)
    struct_sub = struct.add_subparsers(dest="struct_command")
    struct_show = struct_sub.add_parser("show", help="Show one struct layout")
    _common_io_options(struct_show)
    _target_option(struct_show, required=False, default="active")
    struct_show.add_argument("struct_name")
    struct_show.set_defaults(handler=_struct_show)
    field = struct_sub.add_parser("field", help="Operate on struct fields")
    field.set_defaults(_parser=field)
    field_sub = field.add_subparsers(dest="struct_field_command")
    field_set = field_sub.add_parser("set", help="Set or replace a field")
    _common_io_options(field_set, default_format="json")
    _target_option(field_set, required=False)
    field_set.add_argument("--preview", action="store_true")
    field_set.add_argument("--no-overwrite", action="store_true")
    field_set.add_argument("struct_name")
    field_set.add_argument("offset")
    field_set.add_argument("field_name")
    field_set.add_argument("field_type")
    field_set.set_defaults(handler=_struct_field_set)
    field_rename = field_sub.add_parser("rename", help="Rename a field")
    _common_io_options(field_rename, default_format="json")
    _target_option(field_rename, required=False)
    field_rename.add_argument("--preview", action="store_true")
    field_rename.add_argument("struct_name")
    field_rename.add_argument("old_name")
    field_rename.add_argument("new_name")
    field_rename.set_defaults(handler=_struct_field_rename)
    field_delete = field_sub.add_parser("delete", help="Delete a field")
    _common_io_options(field_delete, default_format="json")
    _target_option(field_delete, required=False)
    field_delete.add_argument("--preview", action="store_true")
    field_delete.add_argument("struct_name")
    field_delete.add_argument("field_name")
    field_delete.set_defaults(handler=_struct_field_delete)
    batch = subparsers.add_parser("batch", help="Apply a batch manifest")
    batch.set_defaults(_parser=batch)
    batch_sub = batch.add_subparsers(dest="batch_command")
    batch_apply = batch_sub.add_parser("apply", help="Apply a JSON manifest")
    _common_io_options(batch_apply, default_format="json")
    batch_apply.add_argument("--preview", action="store_true")
    batch_apply.add_argument("manifest", type=Path)
    batch_apply.set_defaults(handler=_batch_apply)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler: Callable[[argparse.Namespace], int] | None = getattr(args, "handler", None)
    if handler is None:
        selected_parser = getattr(args, "_parser", parser)
        selected_parser.print_help()
        return 1

    try:
        return handler(args)
    except BridgeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
