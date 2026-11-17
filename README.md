# bn

An agent-friendly CLI for the Binary Ninja database already open in your GUI. Read decompilation, assembly, references, and types; run unrestricted in-process Python; apply mutations with preview and live verification. The GUI companion works with a personal license and requires no headless license.

## Install

```bash
uv tool install bn-cli
bn plugin install
bn skill install
```

The last two commands link the packaged plugin into Binary Ninja and the skill into `$CODEX_HOME/skills` (default `~/.codex/skills`). Use `--mode copy` for standalone copies. Restart Binary Ninja after installing or upgrading the plugin; restart Codex to discover a newly installed skill.

Upgrade with `uv tool upgrade bn-cli`. Reload Python plugins or restart Binary Ninja after code changes: **Restart Bridge** alone restarts the socket with the already-loaded Python code.

## Start with a target

```bash
bn target list
bn decompile player_update --target crimsonland.exe.bndb
```

With exactly one open view, omit `--target`. With several, copy the selector from the row with the intended file path. An ambiguous basename or path fails before the operation starts and lists the matching target IDs. `--target` works anywhere in the command, and `BN_TARGET` sets a shell default. Use `--target active` to deliberately follow the selected GUI tab.

## Common reads

```bash
bn function search player
bn function search --regex 'attach|detach'
bn function info player_update
bn function info player_update --locals
bn decompile player_update
bn il player_update --view mlil --ssa
bn disasm player_update
bn disasm 0x401234 --count 20
bn disasm 0x401234 --end 0x401280
bn xrefs player_update
bn refs player_update
bn callsites crt_rand --within bonus_pick_random_type
bn address info global_player+0x308
bn data read global_player+0x308 --type u32 --count 4
bn struct show Player
bn types show Player
bn search text crt_rand --view hlil --max-results 50
```

Function reads accept a name or an address inside a function. Address reads also accept symbols and `symbol+offset`. Function lists/searches support `--min-address` and `--max-address`.

`function info` returns a compact summary; `--locals` or `local list` expands variables with stable IDs. `xrefs` finds inbound references; `refs` finds outbound references. `callsites` recovers native call addresses and exact post-call return addresses, with optional local HLIL/branch context. See the [callsite reference](src/bn/assets/skills/bn/references/callsites.md) for scoped and caller-static workflows.

Linear `disasm --count/--end` works without function analysis; `--end` is exclusive. Results report unmapped or undecodable stopping points. For analyzed-function windows, use `--before-instructions`/`--after-instructions`. Legacy `--before`/`--after` still select instruction windows without `--match`; with `--match`, they consistently mean text-line context.

Whole-database `search text` and `search constant` have a five-second default analysis budget and report incomplete results. Increase their `--timeout` explicitly when needed; `function search` does not take that option.

For the full command surface, use `bn --help`. Use scoped help or machine-readable discovery for details:

```bash
bn disasm --help
bn schema disasm
bn schema function info
```

Schema output defines common arguments once and references them from each command, with per-command defaults. It requires no live bridge.

## Output and bundles

Read commands default to text; mutations, setup, and bundles default to JSON.

| Option | Behavior |
|---|---|
| `--format json` / `ndjson` | Complete structured stdout by default |
| `--out <path>` | Write the full result; return its artifact envelope |
| `--spill` | Explicitly allow large structured results to become artifact envelopes |
| `--no-spill` | Stream complete text for pipelines |
| `--match <regex>` | Keep matching text with optional `--before`/`--after` context |

Text above 40,000 bytes spills to a unique file, leaving a short preview on stdout and metadata on stderr. Explicit structured spills return an envelope on stdout. Artifact metadata includes path, format, bytes, SHA-256, and a summary. Version 0.16 deprecates `--tokens` and the `bn-cli[tokens]` extra. Existing callers can still request exact counts during the deprecation window.

```bash
bn decompile player_update --match 'health|damage' --before 2 --after 3
bn bundle function player_update projectile_update --out /tmp/functions.json
```

Bundles pin one view for the whole request and default to decompilation and disassembly. Use `--include decompile,disasm,refs` for the 0.15 default or `--include all` for every section. Other available sections are `mlil,llil,locals,comments,xrefs`. A partial bundle retains successful sections, records failed sections in `section_errors`, sets `success` to false and exits nonzero. For multiple identifiers, check each row's `ok` field. With `--out`, the artifact contains these results.

`decompile` and `il` report the requested function, IL level and available reason when Binary Ninja returns unavailable or empty IL, or raises while rendering it. Inspect the function's analysis state in Binary Ninja or read its native instructions with `disasm`.

## Python

`bn py` runs Python with unrestricted `bn`/`binaryninja` and `bv`/`current_view` access. `bn py exec` remains an alias. Use `--code` for one-liners, `--script` for saved files, or pipe multiline code with a quoted heredoc:

```bash
bn py <<'PY'
f = function("player_update")
il = f.hlil
if il is None:
    raise ValueError(f"HLIL unavailable for {f.name}: {f.analysis_skip_reason}")
result = {"address": hex(f.start), "instructions": sum(1 for _ in il.instructions)}
PY
```

Helpers include `address`, `function`, `functions_containing`, typed `read_u*`/`read_i*`, `read_ptr`, `read_f32`, `read_f64`, and `read_cstr`. BN iterators may need materialization. Both stdout and `result` are returned. Non-JSON results use `repr` with a warning. Syntax errors are caught locally. On a runtime error, text stderr includes the last 2,000 characters of captured stdout and the traceback. Structured errors retain the full captured stdout. Raw Python writes do not inherit built-in mutation rollback or verification.

## Mutations

```bash
bn symbol rename sub_401000 player_update --preview
bn proto set player_update 'void player_update(Player* self)' --preview
bn struct field set Player 0x308 movement_flag_selector uint32_t --preview
bn types declare --file /path/to/types.h --preview
```

Preview applies, refreshes, verifies, captures diffs, then reverts. Omit `--preview` to commit a verified change. Failures roll back; results distinguish `verified`, `noop`, `unsupported`, and `verification_failed`, with requested/observed state where relevant. Use IDs from `local list` for local edits. Type imports preserve the source path for relative includes; declarations with no named types to persist report a no-op.

Read back the relevant prototype/type and decompile after changes. `types show` and `struct show` are authoritative when HLIL still displays stale `__offset(...)` expressions. `bn refresh` refreshes analysis but may not eliminate every presentation artifact.

## Troubleshooting and development

`bn doctor` checks the companion with a two-second request deadline. It exits nonzero for an absent, denied, timed-out or busy bridge. While work is running, it reports the operation and elapsed time without querying GUI views. `target list` reports `bridge_busy` if it cannot start within two seconds. Other analysis and mutation requests queue normally and have no default execution deadline. A client timeout after submission can leave the operation running.

An absent bridge (`bridge_absent`) needs a running GUI companion. If the bridge responds but lists no targets, open a database. For `bridge_denied` under Codex, retry the authorized command with bridge socket access.

```bash
uv sync --locked
uv run pytest
uv run bn --help
uv run python scripts/release.py --check
uv build
```

For editable development, use `uv tool install -e .`. To prepare a release, run `uv run python scripts/release.py <version>`, update [CHANGELOG.md](CHANGELOG.md), commit, and push the matching `v<version>` tag. CI tests Python 3.12/3.14, verifies the wheel, publishes to PyPI, and creates the GitHub release. The CLI and companion must use the same protocol version; restart/reload the companion after upgrades.
