---
name: bn
description: Use the local bn CLI for Binary Ninja reversing work when a Binary Ninja GUI session is already open. Prefer this skill for decompilation, function search, IL/disassembly, xrefs, type inspection, struct field edits, previewed mutations, and inline Python execution through the bn bridge.
---

# bn

Use this skill when the user wants reverse-engineering work against an already-open Binary Ninja database and the local `bn` CLI is available.

## Workflow

1. Start with target discovery:

```bash
bn target list
bn doctor
```

Use `bn doctor` when bridge state is unclear or `bn target list` does not show what you expect.

2. Pick a target:
- If there is exactly one open BinaryView, target-scoped commands can omit `--target` entirely.
- If multiple targets are open, pass `--target <selector>` from `bn target list`.
- Use `--target active` only when you explicitly mean the GUI-selected target.

3. Pick the right output mode:
- Read commands default to `text`.
- Mutation, preview, setup, and export commands default to `json`.
- Other options: `--format json`, `--format ndjson`, `--out <path>`.

`--out` writes rendered output to the path and usually returns a compact JSON envelope. `bn bundle function` writes the artifact from inside the bridge and returns the envelope.

## High-Value Read Commands

```bash
bn target list
bn target info
bn function list
bn function list --min-address 0x401000 --max-address 0x40ffff
bn function search attachment
bn function search --regex 'attach|detach|follow'
bn function info sample_track_floor_height_at_position
bn proto get sample_track_floor_height_at_position
bn local list sample_track_floor_height_at_position
bn decompile sample_track_floor_height_at_position
bn il sample_track_floor_height_at_position
bn disasm sample_track_floor_height_at_position
bn xrefs sample_track_floor_height_at_position
bn xrefs field TrackRowCell.tile_type
bn comment get --address 0x401000
bn types --query Player
bn types show Player
bn struct show Player
bn strings --query follow
bn imports
```

`bn function search` is case-insensitive substring matching by default. Add `--regex` when you need regular expressions. `bn function list` and `bn function search` both accept `--min-address` and `--max-address`.

## Bundles

Use bundles when you want a reusable artifact instead of pasting long output into context:

```bash
bn bundle function sample_track_floor_height_at_position --out /tmp/floor.json
```

With `--out`, the CLI returns a JSON envelope for the written artifact instead of dumping the whole bundle to stdout.

## Python Escape Hatch

Use inline Python as a normal lane for one-off Binary Ninja inspection that is awkward to express as a built-in command:

```bash
bn py exec --code "print(hex(bv.entry_point)); result = {'functions': len(list(bv.functions))}"
```

For multiline snippets, prefer `--stdin` with a quoted heredoc:

Shell details matter here:
- Quote the heredoc delimiter as `<<'PY'` so the shell does not expand `$vars`, backticks, or backslashes before Binary Ninja sees the Python.
- Keep the closing `PY` on its own line with no indentation or trailing spaces.
- Use `--script <file>` only for real files you want to keep on disk.
- Avoid ordinary double-quoted multiline `--code "... \n ..."` strings; `--code` receives one shell argument, so `"\n"` stays a literal backslash-`n` pair instead of becoming a newline.

Use this pattern for larger inspection snippets:

```bash
bn py exec --stdin <<'PY'
out = []
for f in bv.functions:
    if 0x416000 <= f.start < 0x41C000:
        out.append((f.start, f.symbol.short_name))
out.sort()
print("\n".join(f"{addr:#x} {name}" for addr, name in out))
PY
```

If you really need inline multiline code without a heredoc, use ANSI-C quoting instead:

```bash
bn py exec --code $'print(hex(bv.entry_point))\nresult = {"functions": len(list(bv.functions))}'
```

The `py exec` environment includes:`bn`, `binaryninja`, `bv`, `result`.

`py exec` always returns `stdout` and `result`. If `result` is not JSON-serializable, the CLI returns `repr(result)` plus a warning instead of silently flattening it.

## Mutation Workflow

Prefer preview first:

```bash
bn types declare "typedef struct Player { int hp; } Player;" --preview
bn types declare --file /path/to/win32_min.h --preview
bn struct field set Player 0x308 movement_flag_selector uint32_t --preview
bn symbol rename sub_401000 player_update --preview
bn proto get sub_401000
bn local list sub_401000
bn proto set sub_401000 "int __cdecl player_update(Player* self)" --preview
```

Preview mode applies the change, refreshes analysis, captures affected decompile diffs, and then reverts the mutation.

For struct previews, inspect:`results`, `affected_types`, `affected_functions`.

For the first few changed functions, `affected_functions` may also include `before_excerpt` and `after_excerpt` HLIL snippets around the first changed lines.

If a struct edit is already identical, preview may report `changed: false` with `No effective change detected`.

`bn types declare` uses Binary Ninja's source parser when available. With `--file`, it forwards the real source path so relative includes work like GUI header import.

If a declaration only introduces functions or extern variables and no named types, `types declare` now reports a no-op instead of failing with `No named types found in declaration`.

Non-preview writes are live-verified by default. If the requested state does not read back from Binary Ninja, the command exits nonzero and the whole mutation or batch is reverted.

Key result statuses:
- `verified`
- `noop`
- `unsupported`
- `verification_failed`

When verification fails, JSON output also includes the requested and observed state for the failed operation.

If you need to force BN to recalculate presentation after a type change, run:

```bash
bn refresh
```

