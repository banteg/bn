---
name: bn
description: Use the local bn CLI for Binary Ninja reversing work when a Binary Ninja GUI session is already open. Prefer this skill for decompilation, function search, callsite recovery, IL/disassembly, xrefs, type inspection, struct field edits, previewed mutations, and inline Python execution through the bn bridge.
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
- If multiple targets are open, commands that omit `--target` fail; pass `--target <selector>` from `bn target list`.
- Use `--target active` only when you explicitly mean the GUI-selected target.
- `--target` can appear anywhere in the command. Set `BN_TARGET` when a whole shell session should use one database.

3. Pick the right output mode:
- Read commands default to `text`.
- Mutation, preview, setup, and export commands default to `json`.
- Other options: `--format json`, `--format ndjson`, `--out <path>`.

Outputs above `10_000` `o200k_base` tokens auto-spill to disk. When that happens, stdout is empty and stderr carries the spill metadata as plain text, so do not chain `bn ... | rg ...` and expect to search the real output. Use `--out <path>` when you want the full body written to a known file.

Use `--match <regex>` with `--before`/`--after` to keep relevant text lines before spill accounting. Use `--no-spill` when a downstream command must receive the complete stdout stream.

## High-Value Read Commands

```bash
bn target list
bn target info
bn function list
bn function list --min-address 0x401000 --max-address 0x40ffff
bn function search attachment
bn function search --regex 'attach|detach|follow'
bn function info sample_track_floor_height_at_position
bn function containing 0x401234
bn callsites crt_rand --within bonus_pick_random_type
bn callsites crt_rand --within-file /tmp/rng-functions.txt --format ndjson
bn proto get sample_track_floor_height_at_position
bn local list sample_track_floor_height_at_position
bn decompile sample_track_floor_height_at_position
bn il sample_track_floor_height_at_position
bn disasm sample_track_floor_height_at_position
bn disasm 0x401234 --before 5 --after 10
bn address info global_player+0x308
bn data read global_player+0x308 --type u32 --count 4
bn xrefs sample_track_floor_height_at_position
bn refs sample_track_floor_height_at_position
bn xrefs field TrackRowCell.tile_type
bn comment get --address 0x401000
bn types --query Player
bn types show Player
bn struct show Player
bn strings --query follow
bn imports
bn search text crt_rand --view hlil --max-results 50
bn search constant 0x370 --max-results 50
```

`bn function search` is case-insensitive substring matching by default. Add `--regex` when you need regular expressions. `bn function list` and `bn function search` both accept `--min-address` and `--max-address`.

Function-read commands accept either a function identifier or any address contained by that function. Address commands also resolve data/import symbols and `symbol+offset` expressions.

Searches have a five-second default time budget and report whether results are complete. Increase `--timeout` deliberately instead of leaving an unbounded whole-database IL scan running.

## Caller-Static Mapping

Prefer `bn callsites` over ad hoc `py exec` when the task is "find exact native RNG return-address callers" or any similar direct-call mapping workflow.

`bn callsites` reports both:
- `call_addr`: the native `call ...` instruction address
- `caller_static`: the exact post-call return address

The key rule is:
- `caller_static = call_addr + instruction_length`

Use it like this:

```bash
bn callsites crt_rand --within bonus_pick_random_type --caller-static
bn callsites crt_rand --within fx_queue_add_random --caller-static
bn callsites crt_rand --within-file /tmp/rng-functions.txt --format json
bn callsites crt_rand --format ndjson
```

Omit the scope when you want Binary Ninja's inbound reference index to derive all caller functions. Add `--within` or `--within-file` when you already know the relevant callers and want the fastest narrow result.

The `--within-file` format is one function identifier per non-empty line. Lines beginning with `#` are ignored.

For close-together callsites, `bn callsites` also returns:
- previous instructions
- next instructions
- `call_index` within the containing function
- `within_query` with the original unresolved scope token
- a local-or-null HLIL statement
- a best-effort `pre_branch_condition`

`hlil_statement` is intentionally local-or-null. If Binary Ninja only exposes a coarse enclosing region instead of the smallest call-containing expression or statement, expect `hlil_statement: null` rather than a noisy whole-function blob.

`pre_branch_condition` means the nearest enclosing pre-call HLIL condition when it can be recovered confidently. It is not a generic "related branch" field, so `null` is normal when the condition cannot be derived cleanly.

Use `bn xrefs` when you only need inbound references. Use `bn callsites` when you need exact return-address recovery and local context around the call.

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

Use `--stdin` with a quoted heredoc for multiline Python snippets:

Shell details matter here:
- Quote the heredoc delimiter as `<<'PY'` so the shell does not expand `$vars`, backticks, or backslashes before Binary Ninja sees the Python.
- Keep the closing `PY` on its own line with no indentation or trailing spaces.
- Use `--script <file>` only for real files you want to keep on disk.
- Use `--code` for true one-liners only.
- If you are counting or collecting BN iterators such as `f.hlil.instructions`, materialize them explicitly with `list(...)` or a generator consumption pattern instead of assuming random-access behavior.

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

The `py exec` environment includes the unrestricted `bn`/`binaryninja` module and `bv`, plus `current_view`, `address`, `function`, `functions_containing`, typed `read_u*`/`read_i*` helpers, `read_ptr`, `read_f32`, `read_f64`, `read_cstr`, and `result`. These are small functions, not a wrapper API.

`py exec` is a first-class lane for arbitrary-complexity analysis, not a deprecated fallback. It always returns `stdout` and `result`. If `result` is not JSON-serializable, the CLI returns `repr(result)` plus a warning instead of silently flattening it. Failures include the Binary Ninja-process traceback.

## Machine Discovery

Use `bn schema --format json` to discover commands, arguments, defaults, choices, and `BN_TARGET` support without a live bridge.

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

Non-preview writes are live-verified by default. If the requested state does not read back from Binary Ninja, the command exits nonzero and the mutation is reverted.

After any live type or prototype mutation, do an explicit readback:

```bash
bn proto get sub_401000
bn struct show Player
bn types show Player
bn decompile sub_401000
```

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
