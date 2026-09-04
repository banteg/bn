---
name: bn
description: Inspect and edit an already-open Binary Ninja database through the local bn CLI, including decompilation, disassembly, callsites, types, verified mutations, and unrestricted in-process Python.
---

# bn

Use the live GUI database through `bn`. Start with `bn target list`; use `bn doctor` if bridge state is unclear. With multiple views open, pass the returned `--target <selector>` or set `BN_TARGET`. Omission works only with one view. `--target active` deliberately follows the GUI tab.

## Read only what the task needs

```bash
bn function search player
bn function info player_update
bn decompile player_update
bn disasm player_update
bn disasm 0x401234 --count 20
bn disasm 0x401234 --end 0x401280
bn xrefs player_update
bn refs player_update
bn bundle function player_update projectile_update --include decompile,disasm --out /tmp/functions.json
```

`function info` is a compact summary; add `--locals` or use `local list` for full variable details. Bundles default to decompile, disassembly, and outbound references. `--include all` exports every section without duplicating HLIL. Multiple-function bundles retain successful reads alongside per-function errors and exit nonzero if any fail.

Linear `disasm --count/--end` works without a containing function and reports where decoding stops. `--end` is exclusive. For windows in an analyzed function use `--before-instructions/--after-instructions`. With `--match`, `--before/--after` always count text lines; without `--match`, disassembly also accepts them as legacy instruction-window aliases.

Use `bn schema disasm` or `bn schema function info` for scoped argument discovery. `--help` works on each command. Whole-database `search text` and `search constant` have a five-second default analysis budget; `function search` does not take `--timeout`.

## Output

- JSON and NDJSON stream complete results by default. Use `--spill` explicitly to receive an artifact envelope for large results.
- Text over 40,000 bytes spills to a unique file, with a short preview on stdout and metadata on stderr. Use `--no-spill` for text pipelines.
- `--out <path>` writes the full result and returns an artifact envelope with size and hash.
- `--match <regex>` filters text before spill accounting. `--before/--after` supply matching-line context.
- Optional `--tokens` adds artifact token counts if the `bn-cli[tokens]` extra and tokenizer are available. Normal output needs neither.

## Python is a first-class analysis path

Use BN directly for custom analysis. `bn py exec` remains an alias. Pipe multiline code with a quoted heredoc:

```bash
bn py <<'PY'
f = function("player_update")
result = {"address": hex(f.start), "hlil_count": sum(1 for _ in f.hlil.instructions)}
PY
```

Use `--code` for one-liners or `--script` for saved scripts. Shell quoting is not JSON quoting; do not pass multiline code through `JSON.stringify` as shell escaping. Inputs are syntax-checked before contacting BN.

The scope includes unrestricted `bn`/`binaryninja`, `bv`/`current_view`, `address`, `function`, `functions_containing`, typed `read_u*`/`read_i*`, `read_ptr`, `read_f32`, `read_f64`, `read_cstr`, and `result`. BN iterators may need explicit materialization. Stdout and result are returned; non-JSON results use repr with a warning. Python writes are unrestricted and do not inherit the built-in mutation transaction guarantees.

## Types and mutations

Use `types show`/`struct show` as authoritative layouts when decompile presentation is stale. For edits, prefer the built-in preview and verification path:

```bash
bn proto set player_update "void player_update(Player* self)" --preview
bn struct field set Player 0x308 movement_flag_selector uint32_t --preview
```

Preview applies, verifies, and reverts. Non-preview writes verify the live post-state and revert on failure. Inspect `results`, `affected_functions`, and `affected_types`; use `local_id` from `local list` for local edits. Read back relevant types/prototypes and decompile after changes. `bn refresh` can refresh stale analysis presentation. User-requested raw Python writes still require appropriate readback.

For exact native call/return addresses, use `callsites` rather than inferring from HLIL. Read [references/callsites.md](references/callsites.md) for caller-static mapping and scoped recovery.
