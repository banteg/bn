---
name: binary-ninja-cli
description: Use the local bn CLI for Binary Ninja reversing work when a Binary Ninja GUI session is already open. Prefer this skill for decompilation, function search, IL/disassembly, xrefs, type inspection, struct field edits, previewed mutations, and inline Python execution through the bn bridge.
---

# Binary Ninja CLI

Use this skill when the user wants reverse-engineering work against an already-open Binary Ninja database and the local `bn` CLI is available.

## Workflow

1. Check bridge state first:

```bash
bn doctor
bn target list
```

2. Pick a target:
- If there is exactly one open BinaryView, target-scoped commands can omit `--target` entirely.
- Do not repeat `--target active` in normal examples or routine usage.
- Otherwise prefer the `selector` field from `bn target list`.
- If multiple targets are open, pass an explicit `--target`.

3. Use `bn` directly when it is on PATH.
- If running from this repo without the global tool installed, use `uv run bn`.

## High-Value Read Commands

```bash
bn function list
bn function search attachment
bn function info sample_track_floor_height_at_position
bn proto get sample_track_floor_height_at_position
bn local list sample_track_floor_height_at_position
bn refresh
bn decompile sample_track_floor_height_at_position
bn il sample_track_floor_height_at_position
bn disasm sample_track_floor_height_at_position
bn xrefs sample_track_floor_height_at_position
bn xrefs field TrackRowCell.tile_type
bn comment get --address 0x401000
bn types --query Player
bn types show Player --format text
bn struct show Player --format text
bn types declare --file /path/to/win32_min.h --preview
bn strings --query follow
bn bundle function sample_track_floor_height_at_position --out /tmp/floor.json
```

## Python Escape Hatch

Use inline Python as a normal lane for one-off Binary Ninja inspection that is awkward to express as a built-in command:

```bash
bn py exec --code "print(hex(bv.entry_point)); result = {'functions': len(list(bv.functions))}"
```

Use `--stdin` for larger snippets. Use `--script <file>` only for real files.

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

For struct previews, inspect:
- `results`
- `affected_types`
- `affected_functions`

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

If you need to force BN to recalculate presentation after a type change, run:

```bash
bn refresh
```

## Practical Guidance

- Prefer `bn` over MCP for shell-driven decompilation, search, bundles, and large outputs.
- Prefer stable `local_id` values from `bn local list` or `bn function info` when renaming or retyping locals.
- `bn decompile` does not always rewrite post-hoc struct-growth sites away from raw `__offset(...)` expressions, even after a manual analysis refresh.
- Treat `bn decompile` as the HLIL-text convenience lane; typed layouts live in `bn types show ...` and `bn struct show ...`.
- Treat `bn types show ...` and `bn struct show ...` as the authoritative typed layouts when decompile output lags behind type recovery.
- Keep writes sequential when you care about trustworthy preview diffs. Read-side concurrency is much safer than write-side concurrency.
- Use `--out` when output may be long or when you want a stable artifact.
- Use `bn target list` again if target selection is ambiguous.
- If multiple targets are open, be explicit and pass `--target <selector>`.
- If `bn target list` is empty, the Binary Ninja plugin is not live. Check `bn plugin install`, then reload Binary Ninja plugins or restart Binary Ninja.
