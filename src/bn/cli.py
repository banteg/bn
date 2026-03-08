from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Callable

from .output import write_output
from .paths import PLUGIN_NAME, plugin_install_dir, plugin_source_dir
from .transport import BridgeError, choose_instance, list_instances, send_request


def _package_version() -> str:
    try:
        return version("bn")
    except PackageNotFoundError:
        return "0.0.0"


def _common_io_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--format",
        choices=("json", "text", "md", "ndjson"),
        default="json",
        help="Output format",
    )
    parser.add_argument("--out", type=Path, help="Write output to a file instead of stdout")
    parser.add_argument(
        "--instance",
        type=int,
        help="Binary Ninja bridge pid when multiple GUI instances are running",
    )


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
) -> None:
    sys.stdout.write(write_output(value, fmt=fmt, out_path=out_path, stem=stem))


def _call(
    args: argparse.Namespace,
    op: str,
    params: dict[str, Any] | None = None,
    *,
    require_target: bool,
    stem: str,
) -> int:
    target = getattr(args, "target", None)
    if require_target and not target:
        raise BridgeError("This command requires --target")
    response = send_request(
        op,
        params=params,
        target=target,
        instance_pid=getattr(args, "instance", None),
    )
    _render_result(
        response["result"],
        fmt=args.format,
        out_path=args.out,
        stem=stem,
    )
    return 0


def _doctor(args: argparse.Namespace) -> int:
    instances = []
    for instance in list_instances():
        ping: dict[str, Any]
        try:
            response = send_request(
                "doctor",
                params={},
                target=None,
                instance_pid=instance.pid,
            )
            ping = response["result"]
        except Exception as exc:
            ping = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

        instances.append(
            {
                "pid": instance.pid,
                "socket_path": str(instance.socket_path),
                "plugin_version": instance.plugin_version,
                "started_at": instance.started_at,
                "doctor": ping,
            }
        )

    result = {
        "cli_version": _package_version(),
        "plugin_source_dir": str(plugin_source_dir()),
        "plugin_install_dir": str(plugin_install_dir()),
        "instances": instances,
    }
    _render_result(result, fmt=args.format, out_path=args.out, stem="doctor")
    return 0


def _plugin_install(args: argparse.Namespace) -> int:
    source = plugin_source_dir()
    if not source.exists():
        raise BridgeError(f"Plugin source directory is missing: {source}")

    dest = args.dest or plugin_install_dir()
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() or dest.is_symlink():
        if not args.force:
            raise BridgeError(f"Destination already exists: {dest}")
        if dest.is_symlink() or dest.is_file():
            dest.unlink()
        else:
            shutil.rmtree(dest)

    if args.mode == "copy":
        shutil.copytree(source, dest)
    else:
        os.symlink(source, dest, target_is_directory=True)

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


def _target_list(args: argparse.Namespace) -> int:
    if args.instance is not None:
        choose_instance(instance_pid=args.instance)
    return _call(args, "list_targets", {}, require_target=False, stem="targets")


def _target_info(args: argparse.Namespace) -> int:
    return _call(
        args,
        "target_info",
        {"selector": args.target},
        require_target=True,
        stem="target-info",
    )


def _function_list(args: argparse.Namespace) -> int:
    return _call(
        args,
        "list_functions",
        {"offset": args.offset, "limit": args.limit},
        require_target=True,
        stem="functions",
    )


def _function_search(args: argparse.Namespace) -> int:
    return _call(
        args,
        "search_functions",
        {"query": args.query, "offset": args.offset, "limit": args.limit},
        require_target=True,
        stem="function-search",
    )


def _decompile(args: argparse.Namespace) -> int:
    return _call(
        args,
        "decompile",
        {"identifier": args.identifier},
        require_target=True,
        stem="decompile",
    )


def _il(args: argparse.Namespace) -> int:
    return _call(
        args,
        "il",
        {"identifier": args.identifier, "view": args.view, "ssa": bool(args.ssa)},
        require_target=True,
        stem="il",
    )


def _disasm(args: argparse.Namespace) -> int:
    return _call(
        args,
        "disasm",
        {"identifier": args.identifier},
        require_target=True,
        stem="disasm",
    )


def _xrefs(args: argparse.Namespace) -> int:
    return _call(
        args,
        "xrefs",
        {"identifier": args.identifier},
        require_target=True,
        stem="xrefs",
    )


def _types(args: argparse.Namespace) -> int:
    return _call(
        args,
        "types",
        {"query": args.query, "offset": args.offset, "limit": args.limit},
        require_target=True,
        stem="types",
    )


def _strings(args: argparse.Namespace) -> int:
    return _call(
        args,
        "strings",
        {"query": args.query, "offset": args.offset, "limit": args.limit},
        require_target=True,
        stem="strings",
    )


def _imports(args: argparse.Namespace) -> int:
    return _call(args, "imports", {}, require_target=True, stem="imports")


def _data(args: argparse.Namespace) -> int:
    return _call(
        args,
        "data",
        {"offset": args.offset, "limit": args.limit},
        require_target=True,
        stem="data",
    )


def _bundle_function(args: argparse.Namespace) -> int:
    return _call(
        args,
        "bundle_function",
        {"identifier": args.identifier, "out_path": str(args.out) if args.out else None},
        require_target=True,
        stem="function-bundle",
    )


def _bundle_corpus(args: argparse.Namespace) -> int:
    return _call(
        args,
        "bundle_corpus",
        {
            "kind": args.kind,
            "query": args.query,
            "limit": args.limit,
            "out_path": str(args.out) if args.out else None,
        },
        require_target=True,
        stem="corpus-bundle",
    )


def _py_exec(args: argparse.Namespace) -> int:
    if args.script:
        script = args.script.read_text(encoding="utf-8")
    else:
        script = sys.stdin.read()

    return _call(
        args,
        "py_exec",
        {"script": script, "out_path": str(args.out) if args.out else None},
        require_target=True,
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
        stem="symbol-rename",
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
        stem="comment-set",
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
        stem="comment-delete",
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
        stem="prototype-set",
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
        stem="local-rename",
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
        stem="local-retype",
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
        stem="struct-field-set",
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
        stem="struct-field-rename",
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
        stem="struct-field-delete",
    )


def _struct_replace(args: argparse.Namespace) -> int:
    return _call(
        args,
        "struct_replace",
        {
            "declaration": args.declaration,
            "preview": bool(args.preview),
        },
        require_target=True,
        stem="struct-replace",
    )


def _patch_bytes(args: argparse.Namespace) -> int:
    return _call(
        args,
        "patch_bytes",
        {
            "address": args.address,
            "data": args.data,
            "preview": bool(args.preview),
        },
        require_target=True,
        stem="patch-bytes",
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
        stem="batch-apply",
    )


def _add_paged_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=100)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bn", description="Agent-friendly Binary Ninja CLI")
    parser.set_defaults(handler=None)

    subparsers = parser.add_subparsers(dest="command")

    doctor = subparsers.add_parser("doctor", help="Validate bridge discovery and installation")
    _common_io_options(doctor)
    doctor.set_defaults(handler=_doctor)

    plugin = subparsers.add_parser("plugin", help="Install the Binary Ninja companion plugin")
    plugin_sub = plugin.add_subparsers(dest="plugin_command")
    plugin_install = plugin_sub.add_parser("install", help="Install the GUI plugin")
    plugin_install.add_argument("--dest", type=Path, help="Custom install destination")
    plugin_install.add_argument("--mode", choices=("symlink", "copy"), default="symlink")
    plugin_install.add_argument("--force", action="store_true")
    _common_io_options(plugin_install)
    plugin_install.set_defaults(handler=_plugin_install)

    target = subparsers.add_parser("target", help="Inspect Binary Ninja targets")
    target_sub = target.add_subparsers(dest="target_command")
    target_list = target_sub.add_parser("list", help="List open BinaryView targets")
    _common_io_options(target_list)
    target_list.set_defaults(handler=_target_list)
    target_info = target_sub.add_parser("info", help="Show one target")
    _common_io_options(target_info)
    _target_option(target_info, required=True)
    target_info.set_defaults(handler=_target_info)

    function = subparsers.add_parser("function", help="Function discovery helpers")
    function_sub = function.add_subparsers(dest="function_command")
    function_list = function_sub.add_parser("list", help="List functions")
    _common_io_options(function_list)
    _target_option(function_list, required=False, default="active")
    _add_paged_args(function_list)
    function_list.set_defaults(handler=_function_list)
    function_search = function_sub.add_parser("search", help="Search functions by substring")
    _common_io_options(function_search)
    _target_option(function_search, required=False, default="active")
    _add_paged_args(function_search)
    function_search.add_argument("query")
    function_search.set_defaults(handler=_function_search)

    decompile = subparsers.add_parser("decompile", help="Decompile a function")
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

    xrefs = subparsers.add_parser("xrefs", help="List xrefs to an address or function")
    _common_io_options(xrefs)
    _target_option(xrefs, required=False, default="active")
    xrefs.add_argument("identifier")
    xrefs.set_defaults(handler=_xrefs)

    types = subparsers.add_parser("types", help="List or search types")
    _common_io_options(types)
    _target_option(types, required=False, default="active")
    _add_paged_args(types)
    types.add_argument("--query")
    types.set_defaults(handler=_types)

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

    data = subparsers.add_parser("data", help="List defined data")
    _common_io_options(data)
    _target_option(data, required=False, default="active")
    _add_paged_args(data)
    data.set_defaults(handler=_data)

    bundle = subparsers.add_parser("bundle", help="Export reusable bundles")
    bundle_sub = bundle.add_subparsers(dest="bundle_command")
    bundle_function = bundle_sub.add_parser("function", help="Export a function bundle")
    _common_io_options(bundle_function)
    _target_option(bundle_function, required=True)
    bundle_function.add_argument("identifier")
    bundle_function.set_defaults(handler=_bundle_function)
    bundle_corpus = bundle_sub.add_parser("corpus", help="Export a corpus bundle")
    _common_io_options(bundle_corpus)
    _target_option(bundle_corpus, required=True)
    bundle_corpus.add_argument("--kind", choices=("functions", "types", "strings"), required=True)
    bundle_corpus.add_argument("--query")
    bundle_corpus.add_argument("--limit", type=int, default=500)
    bundle_corpus.set_defaults(handler=_bundle_corpus)

    py = subparsers.add_parser("py", help="Execute Python inside Binary Ninja")
    py_sub = py.add_subparsers(dest="py_command")
    py_exec = py_sub.add_parser("exec", help="Execute a Python snippet")
    _common_io_options(py_exec)
    _target_option(py_exec, required=True)
    source = py_exec.add_mutually_exclusive_group(required=True)
    source.add_argument("--script", type=Path)
    source.add_argument("--stdin", action="store_true")
    py_exec.set_defaults(handler=_py_exec)

    symbol = subparsers.add_parser("symbol", help="Rename functions or data")
    symbol_sub = symbol.add_subparsers(dest="symbol_command")
    symbol_rename = symbol_sub.add_parser("rename", help="Rename a symbol")
    _common_io_options(symbol_rename)
    _target_option(symbol_rename, required=True)
    symbol_rename.add_argument("--kind", choices=("auto", "function", "data"), default="auto")
    symbol_rename.add_argument("--preview", action="store_true")
    symbol_rename.add_argument("identifier")
    symbol_rename.add_argument("new_name")
    symbol_rename.set_defaults(handler=_symbol_rename)

    comment = subparsers.add_parser("comment", help="Set or delete comments")
    comment_sub = comment.add_subparsers(dest="comment_command")
    comment_set = comment_sub.add_parser("set", help="Set a comment")
    _common_io_options(comment_set)
    _target_option(comment_set, required=True)
    comment_set.add_argument("--preview", action="store_true")
    comment_set.add_argument("--address")
    comment_set.add_argument("--function")
    comment_set.add_argument("comment")
    comment_set.set_defaults(handler=_comment_set)
    comment_delete = comment_sub.add_parser("delete", help="Delete a comment")
    _common_io_options(comment_delete)
    _target_option(comment_delete, required=True)
    comment_delete.add_argument("--preview", action="store_true")
    comment_delete.add_argument("--address")
    comment_delete.add_argument("--function")
    comment_delete.set_defaults(handler=_comment_delete)

    proto = subparsers.add_parser("proto", help="Set a user prototype")
    proto_sub = proto.add_subparsers(dest="proto_command")
    proto_set = proto_sub.add_parser("set", help="Set a prototype")
    _common_io_options(proto_set)
    _target_option(proto_set, required=True)
    proto_set.add_argument("--preview", action="store_true")
    proto_set.add_argument("identifier")
    proto_set.add_argument("prototype")
    proto_set.set_defaults(handler=_proto_set)

    local = subparsers.add_parser("local", help="Rename or retype locals")
    local_sub = local.add_subparsers(dest="local_command")
    local_rename = local_sub.add_parser("rename", help="Rename a local")
    _common_io_options(local_rename)
    _target_option(local_rename, required=True)
    local_rename.add_argument("--preview", action="store_true")
    local_rename.add_argument("function")
    local_rename.add_argument("variable")
    local_rename.add_argument("new_name")
    local_rename.set_defaults(handler=_local_rename)
    local_retype = local_sub.add_parser("retype", help="Retype a local")
    _common_io_options(local_retype)
    _target_option(local_retype, required=True)
    local_retype.add_argument("--preview", action="store_true")
    local_retype.add_argument("function")
    local_retype.add_argument("variable")
    local_retype.add_argument("new_type")
    local_retype.set_defaults(handler=_local_retype)

    struct = subparsers.add_parser("struct", help="Field-first structure editing")
    struct_sub = struct.add_subparsers(dest="struct_command")
    field = struct_sub.add_parser("field", help="Operate on struct fields")
    field_sub = field.add_subparsers(dest="struct_field_command")
    field_set = field_sub.add_parser("set", help="Set or replace a field")
    _common_io_options(field_set)
    _target_option(field_set, required=True)
    field_set.add_argument("--preview", action="store_true")
    field_set.add_argument("--no-overwrite", action="store_true")
    field_set.add_argument("struct_name")
    field_set.add_argument("offset")
    field_set.add_argument("field_name")
    field_set.add_argument("field_type")
    field_set.set_defaults(handler=_struct_field_set)
    field_rename = field_sub.add_parser("rename", help="Rename a field")
    _common_io_options(field_rename)
    _target_option(field_rename, required=True)
    field_rename.add_argument("--preview", action="store_true")
    field_rename.add_argument("struct_name")
    field_rename.add_argument("old_name")
    field_rename.add_argument("new_name")
    field_rename.set_defaults(handler=_struct_field_rename)
    field_delete = field_sub.add_parser("delete", help="Delete a field")
    _common_io_options(field_delete)
    _target_option(field_delete, required=True)
    field_delete.add_argument("--preview", action="store_true")
    field_delete.add_argument("struct_name")
    field_delete.add_argument("field_name")
    field_delete.set_defaults(handler=_struct_field_delete)
    struct_replace = struct_sub.add_parser("replace", help="Whole-struct replacement escape hatch")
    _common_io_options(struct_replace)
    _target_option(struct_replace, required=True)
    struct_replace.add_argument("--preview", action="store_true")
    struct_replace.add_argument("declaration")
    struct_replace.set_defaults(handler=_struct_replace)

    patch = subparsers.add_parser("patch", help="Patch raw bytes")
    patch_sub = patch.add_subparsers(dest="patch_command")
    patch_bytes = patch_sub.add_parser("bytes", help="Patch bytes at an address")
    _common_io_options(patch_bytes)
    _target_option(patch_bytes, required=True)
    patch_bytes.add_argument("--preview", action="store_true")
    patch_bytes.add_argument("address")
    patch_bytes.add_argument("data")
    patch_bytes.set_defaults(handler=_patch_bytes)

    batch = subparsers.add_parser("batch", help="Apply a batch manifest")
    batch_sub = batch.add_subparsers(dest="batch_command")
    batch_apply = batch_sub.add_parser("apply", help="Apply a JSON manifest")
    _common_io_options(batch_apply)
    batch_apply.add_argument("--preview", action="store_true")
    batch_apply.add_argument("manifest", type=Path)
    batch_apply.set_defaults(handler=_batch_apply)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler: Callable[[argparse.Namespace], int] | None = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1

    try:
        return handler(args)
    except BridgeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
