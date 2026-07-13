from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Callable

from .output import write_output_result
from .paths import plugin_install_dir, plugin_source_dir, skill_install_dir, skill_source_dir
from .transport import BridgeError, _send_request_to_instance, list_instances, send_request
from .version import PROTOCOL_VERSION, VERSION, build_id_for_file

FAILED_MUTATION_STATUSES = {"unsupported", "verification_failed"}


class _HelpFullAction(argparse.Action):
    def __init__(
        self,
        option_strings: list[str],
        dest: str = argparse.SUPPRESS,
        default: str = argparse.SUPPRESS,
        help: str | None = None,
    ) -> None:
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help,
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | list[str] | None,
        option_string: str | None = None,
    ) -> None:
        if isinstance(parser, BnArgumentParser):
            parser.print_full_help()
        else:
            parser.print_help()
        parser.exit()


class BnArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.set_defaults(_parser=self)
        self.add_argument(
            "--help-full",
            action=_HelpFullAction,
            help="Show help for this command and all subcommands",
        )

    def _iter_full_help_parsers(self) -> list[argparse.ArgumentParser]:
        parsers: list[argparse.ArgumentParser] = [self]
        for action in self._actions:
            if isinstance(action, argparse._SubParsersAction):
                for parser in action.choices.values():
                    if isinstance(parser, BnArgumentParser):
                        parsers.extend(parser._iter_full_help_parsers())
                    else:
                        parsers.append(parser)
        return parsers

    def _full_help_actions(self) -> tuple[type[argparse.Action], ...]:
        return (argparse._HelpAction, _HelpFullAction)

    def format_help_for_full(self) -> str:
        formatter = self._get_formatter()
        help_action_types = self._full_help_actions()
        actions = [action for action in self._actions if not isinstance(action, help_action_types)]

        formatter.add_usage(self.usage, actions, self._mutually_exclusive_groups)
        formatter.add_text(self.description)

        for action_group in self._action_groups:
            group_actions = [
                action
                for action in action_group._group_actions
                if not isinstance(action, help_action_types)
            ]
            if not group_actions:
                continue
            formatter.start_section(action_group.title)
            formatter.add_text(action_group.description)
            formatter.add_arguments(group_actions)
            formatter.end_section()

        formatter.add_text(self.epilog)
        return formatter.format_help()

    def format_full_help(self) -> str:
        sections: list[str] = []
        seen: set[int] = set()
        for parser in self._iter_full_help_parsers():
            parser_id = id(parser)
            if parser_id in seen:
                continue
            seen.add(parser_id)
            if isinstance(parser, BnArgumentParser):
                sections.append(parser.format_help_for_full().rstrip())
            else:
                sections.append(parser.format_help().rstrip())
        return "\n\n".join(sections) + "\n"

    def print_full_help(self, file: Any = None) -> None:
        if file is None:
            file = sys.stdout
        self._print_message(self.format_full_help(), file)


def _package_version() -> str:
    return VERSION


def _common_io_options(
    parser: argparse.ArgumentParser,
    *,
    default_format: str = "text",
    match_context: bool = True,
) -> None:
    parser.add_argument(
        "--format",
        choices=("json", "text", "ndjson"),
        default=default_format,
        help="Output format",
    )
    parser.add_argument("--out", type=Path, help="Write output to a file instead of stdout")
    parser.add_argument(
        "--match",
        help="Keep matching text lines (regular expression); use --before/--after for context",
    )
    if match_context:
        parser.add_argument(
            "--before",
            dest="match_before",
            type=int,
            default=0,
            help="Context lines before each --match",
        )
        parser.add_argument(
            "--after",
            dest="match_after",
            type=int,
            default=0,
            help="Context lines after each --match",
        )
    parser.add_argument(
        "--no-spill",
        action="store_true",
        help="Stream the complete result to stdout even above the automatic spill limit",
    )


def _target_option(
    parser: argparse.ArgumentParser,
    *,
    required: bool,
) -> None:
    kwargs: dict[str, Any] = {
        "help": (
            "Target selector from `bn target list` (`selector`, `target_id`, basename, filename, or view id); "
            "omit only when exactly one target is open, or use `active` to follow the GUI-selected target explicitly"
        ),
        "required": required,
    }
    parser.add_argument("--target", **kwargs)


def _render_result(
    value: Any,
    *,
    fmt: str,
    out_path: Path | None,
    stem: str,
    spill_label: str | None = None,
    spill_context: Any = None,
    allow_spill: bool = True,
) -> None:
    output_args: dict[str, Any] = {
        "fmt": fmt,
        "out_path": out_path,
        "stem": stem,
    }
    if not allow_spill:
        output_args["allow_spill"] = False
    result = write_output_result(value, **output_args)
    if result.spilled and result.artifact:
        label = spill_label or stem.replace("_", " ")
        artifact = result.artifact
        lines = [
            f"warning: {label} output spilled",
            f"path: {artifact['artifact_path']}",
            f"format: {artifact['format']}",
            f"bytes: {artifact['bytes']}",
            f"tokens: {artifact['tokens']}",
            f"tokenizer: {artifact['tokenizer']}",
        ]
        if isinstance(artifact.get("sha256"), str):
            lines.append(f"sha256: {artifact['sha256']}")
        summary = artifact.get("summary")
        if isinstance(summary, dict):
            summary_parts = []
            kind = summary.get("kind")
            if kind is not None:
                summary_parts.append(f"kind={kind}")
            for key in sorted(summary):
                if key == "kind":
                    continue
                summary_parts.append(
                    f"{key}={json.dumps(summary[key], sort_keys=True, default=str)}"
                )
            if summary_parts:
                lines.append(f"summary: {', '.join(summary_parts)}")
        if isinstance(spill_context, list):
            lines.append(f"items: {len(spill_context)}")
        if isinstance(value, str):
            lines.append(f"lines: {len(value.splitlines())}")
        print("\n".join(lines), file=sys.stderr)
        return
    sys.stdout.write(result.rendered)


def _render_target_choice(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)

    label = str(value.get("selector") or value.get("target_id") or "<unknown>")
    if value.get("active"):
        label += " [active]"

    target_id = value.get("target_id")
    if target_id not in (None, "", value.get("selector")):
        label += f" (target_id: {target_id})"
    return label


def _render_target_choices(value: Any) -> str:
    if not isinstance(value, list):
        return _render_fallback_text(value)
    if not value:
        return "none"
    return "\n".join(f"- {_render_target_choice(item)}" for item in value)


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
    raise BridgeError(
        "This command requires --target when multiple targets are open.\n"
        f"Open targets:\n{_render_target_choices(targets)}"
    )


def _resolve_target(
    args: argparse.Namespace,
    *,
    require_target: bool,
    allow_implicit_target: bool = False,
) -> str | None:
    local_target = getattr(args, "target", None)
    global_target = getattr(args, "global_target", None)
    env_target = os.environ.get("BN_TARGET")
    if local_target and global_target and local_target != global_target:
        raise BridgeError(
            f"Conflicting target selectors: --target {global_target!r} before the command and "
            f"--target {local_target!r} after it"
        )
    target = local_target or global_target or env_target
    if require_target and not target:
        if allow_implicit_target:
            return _implicit_target(args)
        raise BridgeError("This command requires --target")
    return target


def _filter_text_result(value: str, pattern: str, *, before: int, after: int) -> str:
    if before < 0 or after < 0:
        raise BridgeError("--before and --after must be non-negative")
    try:
        regex = re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        raise BridgeError(f"Invalid --match regular expression: {exc}") from exc

    lines = value.splitlines()
    selected: set[int] = set()
    for index, line in enumerate(lines):
        if not regex.search(line):
            continue
        selected.update(range(max(0, index - before), min(len(lines), index + after + 1)))
    return "\n".join(lines[index] for index in sorted(selected))


def _render_direct_result(args: argparse.Namespace, value: Any, *, stem: str) -> None:
    match = getattr(args, "match", None)
    if match:
        if args.format != "text":
            raise BridgeError("--match is only valid with --format text")
        if not isinstance(value, str):
            value = _render_fallback_text(value)
        value = _filter_text_result(
            value,
            match,
            before=int(getattr(args, "match_before", 0)),
            after=int(getattr(args, "match_after", 0)),
        )
    elif getattr(args, "match_before", 0) or getattr(args, "match_after", 0):
        raise BridgeError("--before and --after require --match")
    _render_result(
        value,
        fmt=args.format,
        out_path=args.out,
        stem=stem,
        allow_spill=not bool(getattr(args, "no_spill", False)),
    )


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
    match = getattr(args, "match", None)
    if match:
        if args.format != "text":
            raise BridgeError("--match is only valid with --format text")
        if not isinstance(result, str):
            result = _render_fallback_text(result)
        result = _filter_text_result(
            result,
            match,
            before=int(getattr(args, "match_before", 0)),
            after=int(getattr(args, "match_after", 0)),
        )
    elif getattr(args, "match_before", 0) or getattr(args, "match_after", 0):
        raise BridgeError("--before and --after require --match")
    _render_result(
        result,
        fmt=args.format,
        out_path=None if bridge_writes_output else args.out,
        stem=stem,
        spill_label=page_label or op.replace("_", " "),
        spill_context=spill_context,
        allow_spill=not bool(getattr(args, "no_spill", False)),
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


def _render_callsites_text(value: Any, *, prefer_caller_static: bool = False) -> str:
    if not isinstance(value, list):
        return _render_fallback_text(value)
    if not value:
        return "none"

    blocks = []
    for row in value:
        if not isinstance(row, dict):
            blocks.append(_render_fallback_text(row))
            continue

        callee = row.get("callee") if isinstance(row.get("callee"), dict) else {}
        containing = row.get("containing_function") if isinstance(row.get("containing_function"), dict) else {}
        call_addr = row.get("call_addr", "<unknown>")
        caller_static = row.get("caller_static", "<unknown>")
        call_index = row.get("call_index")
        primary = (
            f"caller_static {caller_static} | call {call_addr}"
            if prefer_caller_static
            else f"call {call_addr} | caller_static {caller_static}"
        )
        lines = [
            primary,
            (
                f"within: {containing.get('name', '<unknown>')} @ "
                f"{containing.get('address', '<unknown>')}"
            ),
            f"callee: {callee.get('name', '<unknown>')} @ {callee.get('address', '<unknown>')}",
        ]
        if call_index is not None:
            lines.append(f"call-index: {call_index}")
        if row.get("within_query"):
            lines.append(f"within-query: {row['within_query']}")
        if row.get("hlil_statement"):
            lines.append(f"hlil: {row['hlil_statement']}")
        if row.get("pre_branch_condition"):
            lines.append(f"pre-branch: {row['pre_branch_condition']}")

        call_instruction = row.get("call_instruction") if isinstance(row.get("call_instruction"), dict) else {}
        previous = list(row.get("previous_instructions") or [])
        next_instructions = list(row.get("next_instructions") or [])
        lines.append("context:")
        for item in previous:
            if isinstance(item, dict):
                lines.append(f"  {item.get('address', '<unknown>')}  {item.get('text', '')}".rstrip())
        lines.append(
            f"> {call_instruction.get('address', '<unknown>')}  {call_instruction.get('text', '')}".rstrip()
        )
        for item in next_instructions:
            if isinstance(item, dict):
                lines.append(f"  {item.get('address', '<unknown>')}  {item.get('text', '')}".rstrip())
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


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
        sandbox_hint = item.get("sandbox_hint")
        if sandbox_hint:
            lines.append(f"  hint: {sandbox_hint}")
    return "\n".join(lines)


def _sandbox_socket_hint(instance: Any, error: str) -> str | None:
    if not os.environ.get("CODEX_SANDBOX"):
        return None
    lowered = error.lower()
    if "operation not permitted" not in lowered and "permission denied" not in lowered:
        return None
    return (
        "Codex sandbox blocked the Binary Ninja bridge socket at "
        f"{instance.socket_path}. `bn` must run outside the Codex sandbox to reach the "
        "live Binary Ninja GUI bridge. Add `prefix_rule(pattern=[\"bn\"], decision=\"allow\")` "
        "to `~/.codex/rules/default.rules`, then restart Codex or reload rules."
    )


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


def _render_function_identity_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    function = value.get("function") if isinstance(value.get("function"), dict) else value
    name = function.get("name", "<unknown>")
    address = function.get("address", "<unknown>")
    return f"{address}  {name}"


def _render_address_info_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    lines = [f"address: {value.get('address', '<unknown>')}"]
    symbol = value.get("symbol")
    if isinstance(symbol, dict):
        lines.append(f"symbol: {symbol.get('name', '<unknown>')} ({symbol.get('type', '<unknown>')})")
    functions = list(value.get("functions") or [])
    lines.append("functions: " + (", ".join(str(item.get("name", "<unknown>")) for item in functions if isinstance(item, dict)) or "none"))
    data = value.get("data_variable")
    if isinstance(data, dict):
        lines.append(f"data: {data.get('type', '<unknown>')} width={data.get('width', '<unknown>')}")
    section = value.get("section")
    if isinstance(section, dict):
        lines.append(f"section: {section.get('name', '<unknown>')}")
    segment = value.get("segment")
    if isinstance(segment, dict):
        permissions = "".join(
            key for key, enabled in (("r", segment.get("readable")), ("w", segment.get("writable")), ("x", segment.get("executable"))) if enabled
        )
        lines.append(f"segment: {segment.get('start', '?')}..{segment.get('end', '?')} {permissions or '-'}")
    return "\n".join(lines)


def _render_data_read_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    values = value.get("values")
    if isinstance(values, list):
        def render_item(item: dict[str, Any]) -> str:
            decoded = item.get("value", "<unknown>")
            if isinstance(decoded, int):
                decoded = f"{decoded} ({decoded:#x})"
            return f"{item.get('address', '<unknown>')}  {decoded}"

        return "\n".join(
            render_item(item)
            for item in values
            if isinstance(item, dict)
        )
    return str(value.get("value", ""))


def _render_search_text(value: Any) -> str:
    metadata = None
    if isinstance(value, dict) and isinstance(value.get("results"), list):
        metadata = value
        value = value["results"]
    if not isinstance(value, list):
        return _render_fallback_text(value)
    if not value:
        body = "none"
    else:
        body = "\n".join(
        f"{item.get('address', '<unknown>')}  {item.get('function', '<unknown>')}  {item.get('text', '')}".rstrip()
        for item in value
        if isinstance(item, dict)
        )
    if metadata is not None and not metadata.get("complete", True):
        body += (
            f"\nwarning: search stopped by {metadata.get('stopped_reason', 'limit')} "
            f"after {metadata.get('elapsed_seconds', '?')}s"
        )
    return body


def _render_refs_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _render_fallback_text(value)
    function = value.get("function") if isinstance(value.get("function"), dict) else {}
    lines = [f"{function.get('name', '<unknown>')} @ {function.get('address', '<unknown>')}"]
    for label, key in (("code", "code_refs"), ("data", "data_refs")):
        rows = list(value.get(key) or [])
        lines.append(f"{label} refs:")
        if not rows:
            lines.append("- none")
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            target = row.get("symbol") or row.get("function") or row.get("target", "<unknown>")
            lines.append(f"- {row.get('source', '<unknown>')} -> {row.get('target', '<unknown>')} {target}")
    return "\n".join(lines)


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
        sandbox_hint = _sandbox_socket_hint(instance, str(ping.get("error", "")))

        loaded_version = ping.get("plugin_version") if isinstance(ping, dict) else None
        loaded_build_id = ping.get("plugin_build_id") if isinstance(ping, dict) else None
        instance_info = {
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
        if sandbox_hint:
            instance_info["sandbox_hint"] = sandbox_hint
        instances.append(instance_info)

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
    _render_direct_result(args, result, stem="doctor")
    return 0


def _plugin_install(args: argparse.Namespace) -> int:
    source = plugin_source_dir()
    dest = args.dest or plugin_install_dir()
    _install_tree(source, dest, mode=args.mode, force=args.force)

    _render_direct_result(
        args,
        {
            "installed": True,
            "mode": args.mode,
            "source": str(source),
            "destination": str(dest),
        },
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

    _render_direct_result(
        args,
        {
            "installed": True,
            "mode": args.mode,
            "skill": source.name,
            "source": str(source),
            "destination": str(dest),
        },
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
        allow_implicit_target=True,
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
        allow_implicit_target=True,
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
        allow_implicit_target=True,
        text_renderer=_render_function_info_text,
        stem="function-info",
    )


def _function_containing(args: argparse.Namespace) -> int:
    return _call(
        args,
        "function_containing",
        {"address": args.address},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_function_identity_text,
        stem="function-containing",
    )


def _decompile(args: argparse.Namespace) -> int:
    return _call(
        args,
        "decompile",
        {"identifier": args.identifier},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_text_field("text"),
        stem="decompile",
    )


def _il(args: argparse.Namespace) -> int:
    return _call(
        args,
        "il",
        {"identifier": args.identifier, "view": args.view, "ssa": bool(args.ssa)},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_text_field("text"),
        stem="il",
    )


def _disasm(args: argparse.Namespace) -> int:
    return _call(
        args,
        "disasm",
        {
            "identifier": args.identifier,
            "before": args.before_instructions,
            "after": args.after_instructions,
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_text_field("text"),
        stem="disasm",
    )


def _address_info(args: argparse.Namespace) -> int:
    return _call(
        args,
        "address_info",
        {"address": args.address},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_address_info_text,
        stem="address-info",
    )


def _data_read(args: argparse.Namespace) -> int:
    return _call(
        args,
        "data_read",
        {"address": args.address, "type": args.type, "count": args.count},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_data_read_text,
        stem="data-read",
    )


def _refs(args: argparse.Namespace) -> int:
    return _call(
        args,
        "refs_from",
        {"identifier": args.identifier},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_refs_text,
        stem="refs-from",
    )


def _search_text(args: argparse.Namespace) -> int:
    return _call(
        args,
        "search_text",
        {
            "query": args.query,
            "view": args.view,
            "regex": bool(args.regex),
            "max_results": args.max_results,
            "max_seconds": args.timeout,
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_search_text,
        stem="search-text",
    )


def _search_constant(args: argparse.Namespace) -> int:
    return _call(
        args,
        "search_constant",
        {"value": args.value, "max_results": args.max_results, "max_seconds": args.timeout},
        require_target=True,
        allow_implicit_target=True,
        text_renderer=_render_search_text,
        stem="search-constant",
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
            allow_implicit_target=True,
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
        allow_implicit_target=True,
        text_renderer=_render_xrefs_text,
        stem="xrefs",
    )


def _load_within_identifiers(path: Path) -> list[str]:
    identifiers = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        identifiers.append(line)
    return identifiers


def _callsites(args: argparse.Namespace) -> int:
    if args.within is not None:
        within_identifiers = [args.within]
    elif args.within_file is not None:
        if not args.within_file.exists():
            raise BridgeError(f"Scope file not found: {args.within_file}")
        within_identifiers = _load_within_identifiers(args.within_file)
        if not within_identifiers:
            raise BridgeError(f"Scope file did not contain any function identifiers: {args.within_file}")
    else:
        within_identifiers = []

    return _call(
        args,
        "callsites",
        {
            "callee": args.callee,
            "within_identifiers": within_identifiers,
            "context": args.context,
            "caller_static": bool(args.caller_static),
        },
        require_target=True,
        allow_implicit_target=True,
        text_renderer=lambda value: _render_callsites_text(
            value,
            prefer_caller_static=bool(args.caller_static),
        ),
        stem="callsites",
    )


def _types(args: argparse.Namespace) -> int:
    return _call(
        args,
        "types",
        {"query": args.query, "offset": args.offset, "limit": args.limit},
        require_target=True,
        allow_implicit_target=True,
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
        allow_implicit_target=True,
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
        allow_implicit_target=True,
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
        allow_implicit_target=True,
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
        allow_implicit_target=True,
        text_renderer=_render_proto_text,
        stem="prototype-get",
    )


def _local_list(args: argparse.Namespace) -> int:
    return _call(
        args,
        "list_locals",
        {"identifier": args.function},
        require_target=True,
        allow_implicit_target=True,
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
        allow_implicit_target=True,
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
    if args.stdin and args.manifest is not None:
        raise BridgeError("Pass either a manifest path or --stdin, not both")
    if args.manifest is None and not args.stdin:
        raise BridgeError("batch apply requires a manifest path or --stdin")
    if args.stdin or str(args.manifest) == "-":
        source = sys.stdin.read()
        source_name = "stdin"
    else:
        if not args.manifest.exists():
            raise BridgeError(f"Batch manifest not found: {args.manifest}")
        source = args.manifest.read_text(encoding="utf-8")
        source_name = str(args.manifest)
    try:
        payload = json.loads(source)
    except json.JSONDecodeError:
        try:
            payload = [json.loads(line) for line in source.splitlines() if line.strip()]
        except json.JSONDecodeError as exc:
            raise BridgeError(f"Invalid JSON batch input from {source_name}: {exc}") from exc
    if isinstance(payload, list):
        manifest = {"ops": payload}
    elif isinstance(payload, dict):
        manifest = dict(payload)
    else:
        raise BridgeError("Batch input must be a JSON object, an operation array, or NDJSON operations")
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


def _schema_action(action: argparse.Action) -> dict[str, Any]:
    value: dict[str, Any] = {
        "dest": action.dest,
        "required": bool(getattr(action, "required", False)),
    }
    if action.option_strings:
        value["options"] = list(action.option_strings)
    else:
        value["positional"] = True
    if action.nargs is not None:
        value["nargs"] = action.nargs
    if action.default not in (None, argparse.SUPPRESS):
        value["default"] = action.default
    choices = getattr(action, "choices", None)
    if choices is not None and not isinstance(action, argparse._SubParsersAction):
        value["choices"] = list(choices)
    if action.help not in (None, argparse.SUPPRESS):
        value["help"] = action.help
    return value


def _command_schema(parser: argparse.ArgumentParser) -> dict[str, Any]:
    commands: dict[str, Any] = {}

    def visit(current: argparse.ArgumentParser, path: list[str]) -> None:
        actions = [
            _schema_action(action)
            for action in current._actions
            if not isinstance(action, (argparse._HelpAction, _HelpFullAction, argparse._SubParsersAction))
        ]
        if path:
            commands[" ".join(path)] = {
                "description": current.description,
                "arguments": actions,
            }
        for action in current._actions:
            if not isinstance(action, argparse._SubParsersAction):
                continue
            for name, child in action.choices.items():
                visit(child, [*path, name])

    visit(parser, [])
    global_arguments = [
        _schema_action(action)
        for action in parser._actions
        if not isinstance(action, (argparse._HelpAction, _HelpFullAction, argparse._SubParsersAction))
    ]
    return {
        "name": "bn",
        "version": VERSION,
        "protocol_version": PROTOCOL_VERSION,
        "global_arguments": global_arguments,
        "environment": {"BN_TARGET": "Default target selector"},
        "commands": commands,
    }


def _schema(args: argparse.Namespace) -> int:
    value = _command_schema(build_parser())
    if args.format == "text":
        value = "\n".join(sorted(value["commands"]))
    _render_direct_result(args, value, stem="schema")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = BnArgumentParser(prog="bn", description="Agent-friendly Binary Ninja CLI")
    parser.set_defaults(handler=None)
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    parser.add_argument(
        "--target",
        dest="global_target",
        help="Default target selector for the selected command (also available as BN_TARGET)",
    )

    subparsers = parser.add_subparsers(dest="command")

    schema = subparsers.add_parser("schema", help="Describe the complete command surface as data")
    _common_io_options(schema, default_format="json")
    schema.set_defaults(handler=_schema)

    doctor = subparsers.add_parser("doctor", help="Validate bridge discovery and installation")
    _common_io_options(doctor)
    doctor.set_defaults(handler=_doctor)

    plugin = subparsers.add_parser("plugin", help="Install the Binary Ninja companion plugin")
    plugin_sub = plugin.add_subparsers(dest="plugin_command")
    plugin_install = plugin_sub.add_parser("install", help="Install the GUI plugin")
    plugin_install.add_argument("--dest", type=Path, help="Custom install destination")
    plugin_install.add_argument("--mode", choices=("symlink", "copy"), default="symlink")
    plugin_install.add_argument("--force", action="store_true")
    _common_io_options(plugin_install, default_format="json")
    plugin_install.set_defaults(handler=_plugin_install)

    skill = subparsers.add_parser("skill", help="Install the bundled Codex skill")
    skill_sub = skill.add_subparsers(dest="skill_command")
    skill_install = skill_sub.add_parser("install", help="Install the bundled Codex skill")
    skill_install.add_argument("--dest", type=Path, help="Custom install destination")
    skill_install.add_argument("--mode", choices=("symlink", "copy"), default="symlink")
    skill_install.add_argument("--force", action="store_true")
    _common_io_options(skill_install, default_format="json")
    skill_install.set_defaults(handler=_skill_install)

    target = subparsers.add_parser("target", help="Inspect Binary Ninja targets")
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
    function_sub = function.add_subparsers(dest="function_command")
    function_list = function_sub.add_parser("list", help="List functions")
    _common_io_options(function_list)
    _target_option(function_list, required=False)
    _add_function_address_args(function_list)
    function_list.set_defaults(handler=_function_list)
    function_search = function_sub.add_parser("search", help="Search functions by substring or regex")
    _common_io_options(function_search)
    _target_option(function_search, required=False)
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
    _target_option(function_info, required=False)
    function_info.add_argument("identifier")
    function_info.set_defaults(handler=_function_info)
    function_containing = function_sub.add_parser(
        "containing",
        help="Resolve the function containing an arbitrary address",
    )
    _common_io_options(function_containing)
    _target_option(function_containing, required=False)
    function_containing.add_argument("address")
    function_containing.set_defaults(handler=_function_containing)

    decompile = subparsers.add_parser("decompile", help="Render HLIL-style decompile text for a function")
    _common_io_options(decompile)
    _target_option(decompile, required=False)
    decompile.add_argument("identifier")
    decompile.set_defaults(handler=_decompile)

    il = subparsers.add_parser("il", help="Dump IL for a function")
    _common_io_options(il)
    _target_option(il, required=False)
    il.add_argument("identifier")
    il.add_argument("--view", choices=("hlil", "mlil", "llil"), default="hlil")
    il.add_argument("--ssa", action="store_true")
    il.set_defaults(handler=_il)

    disasm = subparsers.add_parser("disasm", help="Disassemble a function")
    _common_io_options(disasm, match_context=False)
    _target_option(disasm, required=False)
    disasm.add_argument("identifier")
    disasm.add_argument(
        "--before",
        dest="before_instructions",
        type=int,
        help="Instructions before an address identifier (requires an address)",
    )
    disasm.add_argument(
        "--after",
        dest="after_instructions",
        type=int,
        help="Instructions after an address identifier (requires an address)",
    )
    disasm.set_defaults(handler=_disasm)

    address = subparsers.add_parser("address", help="Inspect arbitrary addresses")
    address_sub = address.add_subparsers(dest="address_command")
    address_info = address_sub.add_parser("info", help="Show symbols, functions, data, and mapping metadata")
    _common_io_options(address_info)
    _target_option(address_info, required=False)
    address_info.add_argument("address")
    address_info.set_defaults(handler=_address_info)

    data = subparsers.add_parser("data", help="Read typed data from the selected BinaryView")
    data_sub = data.add_subparsers(dest="data_command")
    data_read = data_sub.add_parser("read", help="Read bytes, integers, pointers, floats, or C strings")
    _common_io_options(data_read)
    _target_option(data_read, required=False)
    data_read.add_argument("address")
    data_read.add_argument(
        "--type",
        choices=("bytes", "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "ptr", "cstr"),
        default="bytes",
    )
    data_read.add_argument("--count", type=int)
    data_read.set_defaults(handler=_data_read)

    xrefs = subparsers.add_parser("xrefs", help="List xrefs to an address or function, or `field <Struct.field>`")
    _common_io_options(xrefs)
    _target_option(xrefs, required=False)
    xrefs.add_argument("identifier", nargs="?")
    xrefs.add_argument("extra", nargs="*")
    xrefs.set_defaults(handler=_xrefs)

    refs = subparsers.add_parser("refs", help="List outbound code and data references from a function")
    _common_io_options(refs)
    _target_option(refs, required=False)
    refs.add_argument("identifier")
    refs.set_defaults(handler=_refs)

    search = subparsers.add_parser("search", help="Search IL/disassembly text or integer constants")
    search_sub = search.add_subparsers(dest="search_command")
    search_text = search_sub.add_parser("text", help="Search rendered IL or disassembly lines")
    _common_io_options(search_text)
    _target_option(search_text, required=False)
    search_text.add_argument("query")
    search_text.add_argument("--view", choices=("hlil", "mlil", "llil", "disasm"), default="hlil")
    search_text.add_argument("--regex", action="store_true")
    search_text.add_argument("--max-results", type=int, default=200)
    search_text.add_argument("--timeout", type=float, default=5.0, help="Stop the search after this many seconds")
    search_text.set_defaults(handler=_search_text)
    search_constant = search_sub.add_parser("constant", help="Find IL instructions containing an integer constant")
    _common_io_options(search_constant)
    _target_option(search_constant, required=False)
    search_constant.add_argument("value")
    search_constant.add_argument("--max-results", type=int, default=200)
    search_constant.add_argument("--timeout", type=float, default=5.0, help="Stop the search after this many seconds")
    search_constant.set_defaults(handler=_search_constant)

    callsites = subparsers.add_parser("callsites", help="Find direct native callsites and exact caller_static addresses")
    _common_io_options(callsites)
    _target_option(callsites, required=False)
    callsites.add_argument("callee")
    scope = callsites.add_mutually_exclusive_group()
    scope.add_argument("--within", help="Containing function to search for callsites")
    scope.add_argument("--within-file", type=Path, help="Text file with one containing-function identifier per line")
    callsites.add_argument(
        "--context",
        type=int,
        default=3,
        help="Number of previous and next instructions to include around each callsite",
    )
    callsites.add_argument(
        "--caller-static",
        action="store_true",
        help="Prefer caller_static-first text output for return-address mapping workflows",
    )
    callsites.set_defaults(handler=_callsites)

    types = subparsers.add_parser("types", help="List or search types")
    _common_io_options(types)
    _target_option(types, required=False)
    _add_paged_args(types)
    types.add_argument("--query")
    types.set_defaults(handler=_types)
    types_sub = types.add_subparsers(dest="types_command")
    types_show = types_sub.add_parser("show", help="Show one type")
    _common_io_options(types_show)
    _target_option(types_show, required=False)
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
    _target_option(strings, required=False)
    _add_paged_args(strings)
    strings.add_argument("--query")
    strings.set_defaults(handler=_strings)

    imports = subparsers.add_parser("imports", help="List imports")
    _common_io_options(imports)
    _target_option(imports, required=False)
    imports.set_defaults(handler=_imports)

    bundle = subparsers.add_parser("bundle", help="Export reusable bundles")
    bundle_sub = bundle.add_subparsers(dest="bundle_command")
    bundle_function = bundle_sub.add_parser("function", help="Export a function bundle")
    _common_io_options(bundle_function, default_format="json")
    _target_option(bundle_function, required=False)
    bundle_function.add_argument("identifier")
    bundle_function.set_defaults(handler=_bundle_function)

    py = subparsers.add_parser("py", help="Execute Python inside Binary Ninja")
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
    proto_sub = proto.add_subparsers(dest="proto_command")
    proto_get = proto_sub.add_parser("get", help="Show the current prototype")
    _common_io_options(proto_get)
    _target_option(proto_get, required=False)
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
    local_sub = local.add_subparsers(dest="local_command")
    local_list = local_sub.add_parser("list", help="List locals with stable IDs")
    _common_io_options(local_list)
    _target_option(local_list, required=False)
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
    struct_sub = struct.add_subparsers(dest="struct_command")
    struct_show = struct_sub.add_parser("show", help="Show one struct layout")
    _common_io_options(struct_show)
    _target_option(struct_show, required=False)
    struct_show.add_argument("struct_name")
    struct_show.set_defaults(handler=_struct_show)
    field = struct_sub.add_parser("field", help="Operate on struct fields")
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
    batch_sub = batch.add_subparsers(dest="batch_command")
    batch_apply = batch_sub.add_parser("apply", help="Apply a JSON manifest")
    _common_io_options(batch_apply, default_format="json")
    batch_apply.add_argument("--preview", action="store_true")
    batch_apply.add_argument("--stdin", action="store_true", help="Read a manifest, operation array, or NDJSON from stdin")
    batch_apply.add_argument("manifest", type=Path, nargs="?")
    batch_apply.set_defaults(handler=_batch_apply)

    return parser


def _normalize_target_argv(argv: list[str]) -> list[str]:
    targets: list[str] = []
    remaining: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        if token == "--target":
            if index + 1 >= len(argv):
                raise BridgeError("--target requires a selector")
            targets.append(argv[index + 1])
            index += 2
            continue
        if token.startswith("--target="):
            targets.append(token.split("=", 1)[1])
            index += 1
            continue
        remaining.append(token)
        index += 1
    unique = list(dict.fromkeys(targets))
    if len(unique) > 1:
        raise BridgeError("Conflicting target selectors: " + ", ".join(repr(item) for item in unique))
    if unique:
        return ["--target", unique[0], *remaining]
    return remaining


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    try:
        normalized_argv = _normalize_target_argv(raw_argv)
    except BridgeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    args = parser.parse_args(normalized_argv)
    handler: Callable[[argparse.Namespace], int] | None = getattr(args, "handler", None)
    if handler is None:
        selected_parser = getattr(args, "_parser", parser)
        selected_parser.print_help()
        return 1

    try:
        return handler(args)
    except BridgeError as exc:
        if getattr(args, "format", "text") in {"json", "ndjson"} and exc.details:
            payload = {"ok": False, "error": exc.details}
            rendered = json.dumps(payload, sort_keys=True)
            if args.format == "json":
                rendered = json.dumps(payload, indent=2, sort_keys=True)
            print(rendered, file=sys.stderr)
        else:
            print(str(exc), file=sys.stderr)
        return 2
    except BrokenPipeError:
        return 0
