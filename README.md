# bn

`bn` is an agent-friendly Binary Ninja CLI.

It splits into two parts:

- a normal Python CLI that you can run from your shell
- a Binary Ninja GUI plugin that owns all Binary Ninja API access

The CLI talks to the GUI plugin over a local Unix socket. This avoids the headless `binaryninja` limitations in this environment and makes it easy to spill large results to files instead of truncating them in a tool harness.

## Install

Install the CLI on your PATH:

```bash
uv tool install -e .
```

Install the Binary Ninja companion plugin:

```bash
bn plugin install
```

That links [`plugin/bn_agent_bridge`](/Users/banteg/dev/banteg/bn/plugin/bn_agent_bridge) into your Binary Ninja plugins directory.

If the plugin code changes, reload Binary Ninja Python plugins or restart Binary Ninja.

## How It Works

- The plugin creates one fixed bridge socket and one fixed registry file.
- The CLI discovers that bridge, connects to it, and forwards commands.
- Read commands return structured data.
- Large outputs can spill to artifacts with `--out`, and some large stdout responses auto-spill to a temp directory.
- Mutations support `--preview` so you can inspect the effect before making a permanent change.
- Mutations verify live Binary Ninja post-state before they report success.

This version assumes one Binary Ninja/plugin instance per machine, which keeps discovery simple.

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

- `active`
- the `selector` field from `bn target list`
- the full `target_id`
- the BinaryView basename
- the full filename
- the view id

In normal use, prefer the `selector` field. For a single open database, this is usually just the `.bndb` basename:

```bash
bn decompile update_player_movement_flags --target SnailMail_unwrapped.exe.bndb
```

## Output Behavior

Every command supports:

- `--format json`
- `--format text`
- `--format md`
- `--format ndjson`
- `--out <path>`

Examples:

```bash
bn function list --format ndjson
bn decompile sample_track_floor_height_at_position --out /tmp/floor.json
```

If `--out` is set, the command writes the rendered result to that path and prints a compact JSON envelope with the artifact path, size, hash, and summary.

## Extraction Commands

Common read-only commands:

```bash
bn target list
bn target info

bn function list
bn function search attachment
bn function info end_track_attachment_follow_state
bn refresh

bn decompile end_track_attachment_follow_state
bn il end_track_attachment_follow_state
bn disasm end_track_attachment_follow_state
bn xrefs end_track_attachment_follow_state
bn xrefs field TrackRowCell.tile_type
bn comment get --address 0x401000

bn types --query Player
bn types show Player --format text
bn struct show Player --format text
bn types declare --file /path/to/win32_min.h --preview
bn strings --query follow
bn imports
bn data
```

## Typing Caveat

One local tooling caveat remains:

- `bn decompile` does not always rewrite post-hoc struct-growth sites away from raw `__offset(...)` expressions, even after a manual analysis refresh.
- For now, `bn types show Player`, `bn types show Game`, and `bn types show PathTemplate` are the authoritative typed layouts.

## Bundles And Python

Export a reusable function bundle:

```bash
bn bundle function end_track_attachment_follow_state --out /tmp/end_track_attachment_follow_state.json
```

Run Python inside the Binary Ninja process:

```bash
bn py exec --code "print(hex(bv.entry_point)); result = {'functions': len(list(bv.functions))}"

bn py exec --stdin <<'PY'
print(hex(bv.entry_point))
result = {"functions": len(list(bv.functions))}
PY
```

The `py exec` environment includes:

- `bn`
- `binaryninja`
- `bv`
- `result`

Stdout and `result` are both returned.

## Mutation Commands

Mutations can omit `--target` when exactly one BinaryView is open. If multiple targets are open, pass an explicit selector.

Examples:

```bash
bn symbol rename sub_401000 player_update --preview
bn comment set --address 0x401000 "interesting branch" --preview
bn proto set sub_401000 "int __cdecl player_update(Player* self)" --preview
bn local rename sub_401000 var_14 speed --preview
bn local retype sub_401000 var_14 float --preview
bn types declare "typedef struct Player { int hp; } Player;" --preview
bn struct field set Player 0x308 movement_flag_selector uint32_t --preview
bn patch bytes 0x401000 "90 90" --preview
```

Preview mode applies the change, refreshes analysis, captures affected decompile diffs, and then reverts the mutation.

Non-preview writes only report success after reading the live BN session back and verifying that the requested post-state actually landed. If verification fails, the CLI returns a nonzero exit code and reverts the whole mutation or batch.

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

If `active` is ambiguous, pass `--target <selector>` from `bn target list`.

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
