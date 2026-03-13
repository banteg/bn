# bn

`bn` is a coding agent-first CLI for Binary Ninja. It gives a shell session or tool-calling agent stable commands, structured output, and access to the same live Binary Ninja database you already have open in the GUI.

## Headline Features

- Query live Binary Ninja state from the shell: targets, functions, callsites, decompile text, IL, disassembly, xrefs, types, strings, imports, and reusable bundles.
- Execute Python inside the Binary Ninja process instead of maintaining a separate headless workflow.
- Apply mutations with `--preview`, capture decompile diffs, and verify the live post-state before reporting success.
- Emit structured `json` or `ndjson` output, auto-spill large results to files, and return token counts so agents can budget context intelligently.

## Install

Recommended setup: install the CLI, the Binary Ninja companion plugin, and the bundled Codex skill.

Install the CLI on your PATH:

```bash
uv tool install -e .
```

Install the Binary Ninja companion plugin:

```bash
bn plugin install
```

That links [`plugin/bn_agent_bridge`](/Users/banteg/dev/banteg/bn/plugin/bn_agent_bridge) into your Binary Ninja plugins directory.

Install the bundled Codex skill:

```bash
bn skill install
```

That symlinks [`skills/bn`](/Users/banteg/dev/banteg/bn/skills/bn) into `$CODEX_HOME/skills/bn` by default. Use `--mode copy` if you want a standalone copy instead. Restart Codex to pick up a new or renamed skill.

If the plugin code changes, reload Binary Ninja Python plugins or restart Binary Ninja.

## How It Works

- `bn` has two parts:
  - a normal Python CLI that you can run from your shell or agent tool harness
  - a Binary Ninja GUI plugin that owns all Binary Ninja API access
- The plugin creates one fixed bridge socket and one fixed registry file.
- The CLI discovers that bridge, connects to it, and forwards commands into the live GUI session.
- Because the Binary Ninja side runs as a GUI plugin, it works with a personal license and does not require a commercial headless license.

## Quick Start

Open a binary or `.bndb` in Binary Ninja, then run:

```bash
bn doctor
bn target list
bn refresh
bn function list
bn decompile sub_401000
```

If exactly one BinaryView is open, target-specific commands can omit `--target` entirely. If multiple targets are open, pass `--target <selector>` from `bn target list`.

## Target Selection

Use `bn target list` to see available targets:

```bash
bn target list
```

Targets can be selected with:

- the `selector` field from `bn target list`
- the full `target_id`
- the BinaryView basename
- the full filename
- the view id
- `active` when you explicitly want the GUI-selected target

In normal use, prefer the `selector` field. For a single open database, this is usually just the `.bndb` basename:

```bash
bn decompile update_player_movement_flags --target SnailMail_unwrapped.exe.bndb
```

Omitting `--target` only works when exactly one target is open. If multiple targets are open, the CLI rejects the command instead of silently falling back to `active`.

## Output Behavior

Every command supports:

- `--format json`
- `--format text`
- `--format ndjson`
- `--out <path>`

Interactive read commands default to `text`. Mutation, setup, and export commands default to `json`.
Add `--format json` when you need stable fields for automation or piping into structured tooling.

Examples:

```bash
bn function list --format ndjson
bn function list --min-address 0x401000 --max-address 0x40ffff
bn function search --regex 'attach|detach'
bn decompile sample_track_floor_height_at_position --out /tmp/floor.json
```

If `--out` is set, the command writes the rendered result to that path and prints a compact JSON envelope with the artifact path, byte size, token count, tokenizer, hash, and summary. Agents can use that envelope to decide whether to read the full artifact, keep a summary, or defer loading it into context.

The only exception is `bn bundle function`, which writes the bundle artifact from inside the bridge and prints the envelope back to the CLI.

`bn function list` and `bn function search` return the full matching set for the selected target or address range. Large results auto-spill to an artifact instead of forcing manual pagination. Spill is token-based and currently triggers above 10,000 tokens. When that happens, stdout stays empty and stderr carries the spill metadata as plain text, including the artifact path and size counts.

## Extraction Commands

Common read-only commands:

```bash
bn target list
bn target info

bn function list
bn function list --min-address 0x401000 --max-address 0x40ffff
bn function search attachment
bn function search --regex 'attach|detach|follow'
bn function info end_track_attachment_follow_state
bn callsites crt_rand --within bonus_pick_random_type
bn callsites crt_rand --within-file /tmp/rng-functions.txt --format ndjson
bn proto get end_track_attachment_follow_state
bn local list end_track_attachment_follow_state
bn refresh

bn decompile end_track_attachment_follow_state
bn il end_track_attachment_follow_state
bn disasm end_track_attachment_follow_state
bn xrefs end_track_attachment_follow_state
bn xrefs field TrackRowCell.tile_type
bn comment get --address 0x401000

bn types --query Player
bn types show Player
bn struct show Player
bn types declare --file /path/to/win32_min.h --preview
bn strings --query follow
bn imports
```

`bn function search` stays case-insensitive substring matching by default. Add `--regex` when you need regular expressions. `bn function list` and `bn function search` both accept `--min-address` and `--max-address` to filter by function start address.

`bn callsites` is the direct-call lane for exact return-address recovery. It reports both the native `call_addr` and the post-call `caller_static`, where `caller_static = call_addr + instruction_length`. Scope it with `--within <function>` or `--within-file <path>`; the file format is one function identifier per non-empty line, with `#` comments ignored.

Each callsite row also includes:

- `call_index`: zero-based ordinal for matching callsites in the containing function, ordered by `call_addr`
- `within_query`: the original unresolved scope token from `--within` or `--within-file`
- `hlil_statement`: the smallest recoverable HLIL expression or statement containing the call, or `null` when Binary Ninja only exposes a coarse enclosing region
- `pre_branch_condition`: the nearest enclosing pre-call HLIL condition when it can be recovered confidently, otherwise `null`

`hlil_statement` is intentionally local-or-null. If the best available HLIL mapping expands to a broad whole-function or multi-statement blob, `bn callsites` suppresses it instead of returning noisy context.

## Bundles And Python

`bn decompile` is the HLIL-text convenience lane. It is useful for quick function reading, but typed layouts remain authoritative in `bn types show` and `bn struct show`.

Export a reusable function bundle:

```bash
bn bundle function end_track_attachment_follow_state --out /tmp/end_track_attachment_follow_state.json
```

Run Python inside the Binary Ninja process for one-off inspection and BN-native scripting:

```bash
bn py exec --code "print(hex(bv.entry_point)); result = {'functions': len(list(bv.functions))}"

bn py exec --stdin <<'PY'
print(hex(bv.entry_point))
result = {"functions": len(list(bv.functions))}
PY
```

Use `--stdin` or `--script` for multiline Python snippets. Use `--code` for true one-liners only.

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

Use a quoted heredoc for multiline Python snippets.

When you need counts from BN iterators such as `f.hlil.instructions`, materialize them explicitly with `list(...)` or consume them with `sum(1 for ...)` instead of assuming sequence semantics.

The `py exec` environment includes:

- `bn`
- `binaryninja`
- `bv`
- `result`

Stdout and `result` are both returned. If `result` is not JSON-serializable, `bn` returns `repr(result)` and includes a warning instead of silently stringifying the whole response.

## Mutation Commands

Mutations follow the same target-selection rules as other target-specific commands.

Examples:

```bash
bn symbol rename sub_401000 player_update --preview
bn comment set --address 0x401000 "interesting branch" --preview
bn comment get --address 0x401000
bn proto get sub_401000
bn proto set sub_401000 "int __cdecl player_update(Player* self)" --preview
bn local list sub_401000
bn local rename sub_401000 0x401000:local:StackVariableSourceType:-20:2:12345 speed --preview
bn local retype sub_401000 0x401000:local:StackVariableSourceType:-20:2:12345 float --preview
bn types declare "typedef struct Player { int hp; } Player;" --preview
bn struct field set Player 0x308 movement_flag_selector uint32_t --preview
```

Preview mode applies the change, refreshes analysis, captures affected decompile diffs, and then reverts the mutation.

Non-preview writes only report success after reading the live BN session back and verifying that the requested post-state actually landed. If verification fails, the CLI returns a nonzero exit code and reverts the whole mutation or batch.

After any live type or prototype mutation, do an explicit readback:

```bash
bn proto get sub_401000
bn struct show Player
bn types show Player
bn decompile sub_401000
```

For declaration and struct mutations, preview results also include `affected_types` with before/after layouts and a unified diff. If a field edit is already identical, the result is marked with `changed: false` and a `No effective change detected` message.

For the first few changed functions, `affected_functions` also includes short `before_excerpt` and `after_excerpt` snippets around the first changed HLIL lines.

Mutation results now distinguish:

- `verified`
- `noop`
- `unsupported`
- `verification_failed`

When verification fails, JSON output also includes `requested` and `observed` state for the failed op.

`bn types declare` now uses Binary Ninja's source parser when available. When you pass `--file`, the CLI also forwards the source path so relative includes resolve the same way they would during header import in the GUI.

If a declaration only parses functions or extern variables and introduces no named types to persist, `types declare` returns a verified no-op instead of failing with `No named types found in declaration`.

`bn local list` and `bn function info` return stable `local_id` values for parameters and locals. Prefer those IDs for `bn local rename`, `bn local retype`, and batch manifests; legacy name-based targeting still works for compatibility.

## Batch Manifests

`bn batch apply` accepts a JSON manifest:

```json
{
  "target": "SnailMail_unwrapped.exe.bndb",
  "preview": true,
  "ops": [
    {
      "op": "rename_symbol",
      "kind": "function",
      "identifier": "sub_401000",
      "new_name": "player_update"
    },
    {
      "op": "set_prototype",
      "identifier": "player_update",
      "prototype": "int __cdecl player_update(Player* self)"
    }
  ]
}
```

Apply it with:

```bash
bn batch apply manifest.json
```

Batch apply verifies the live session by default. If any op fails to apply or fails post-state verification, the entire batch is reverted.

## Troubleshooting

Check bridge state:

```bash
bn doctor
```

If `bn target list` is empty:

- make sure Binary Ninja is open
- make sure a binary or `.bndb` is open
- make sure the plugin is installed with `bn plugin install`
- reload Binary Ninja plugins or restart Binary Ninja after plugin changes

If multiple targets are open, omitted `--target` is rejected. Pass `--target <selector>` from `bn target list`, or use `--target active` only when you intentionally want the GUI-selected target.

If decompile text still looks stale after a type change, run:

```bash
bn refresh
```

That forces an analysis refresh, but it still may not fully eliminate Binary Ninja's stale `__offset(...)` presentation in every case.

## Development

Run tests with:

```bash
uv run pytest
```

Run the CLI from the repo without installing it globally:

```bash
uv run bn --help
```
