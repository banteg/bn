# Changelog

All notable changes to `bn` are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added GitHub Actions CI for Python 3.14 with locked dependency sync, the full test suite, and distribution builds.
- Added a CI status badge to the README.
- Added a release helper and CI check that keep the package, plugin, and lockfile versions synchronized.

### Changed

- Centralized bridge operation dispatch so executable handlers also define the advertised protocol capabilities.
- Moved target discovery, socket serving, and Python execution helpers into focused bridge modules.
- Simplified mutations to one operation per request and serialized Binary Ninja access with one reentrant mutex.
- Unified bundle artifact writing with the CLI's standard output, hashing, token, and summary envelope.

### Fixed

- Made the transient socket retry test portable across macOS and Linux errno values.
- Included the companion Binary Ninja plugin and Codex skill in built wheels and source distributions.
- Enforced protocol and ambiguous-target checks in the bridge without a CLI preflight request.

## [0.13.1] - 2026-07-14

### Removed

- Removed the unused `bn batch apply` CLI command, bridge operation, capability advertisement, tests, and documentation.

### Changed

- Kept atomic multi-operation rollback as an internal mutation primitive and added regression coverage for it.

## [0.13.0] - 2026-07-14

### Added

- Added global target selection through `--target` anywhere in a command and the `BN_TARGET` environment variable.
- Added arbitrary-address inspection, typed data reads, outbound references, bounded searches, and unscoped callsite discovery.
- Added machine-readable command schema, live capability discovery, structured bridge errors, and concise recovery suggestions.
- Added `py exec` helpers for common Binary Ninja reads while retaining unrestricted `binaryninja` and `bv` access.
- Added output filtering with `--match`, `--before`, and `--after`, plus `--no-spill` for complete stdout streaming.

### Changed

- Promoted `py exec` as a first-class analysis lane with preserved tracebacks and safe representation of non-JSON results.
- Improved mutation verification, affected-function/type reporting, and no-op handling for declarations.

### Fixed

- Removed the default bridge request timeout and improved Codex sandbox socket-denial diagnostics.
- Fixed bridge restart behavior and stale plugin reporting.

## [0.12.2] - 2026-03-14

### Fixed

- Included the open target list in ambiguous multi-target errors so callers can choose a valid selector directly.

## [0.12.1] - 2026-03-13

### Fixed

- Sent automatic spill metadata to stderr, keeping stdout clean for structured command consumers.

## [0.12.0] - 2026-03-12

### Changed

- Rejected implicit target selection when multiple Binary Ninja views are open instead of silently choosing the active tab.

## [0.11.0] - 2026-03-12

### Added

- Added direct callsite discovery with exact native call addresses and post-call `caller_static` return addresses.
- Added scoped caller searches, nearby instruction context, local HLIL statements, and best-effort pre-branch conditions.

## [0.10.1] - 2026-03-10

### Fixed

- Hardened bridge mutation verification and failure reporting.

## [0.10.0] - 2026-03-10

### Added

- Added exact subcommand help and recursive `--help-full` output for machine and human discovery.

### Changed

- Lowered automatic output spilling to a 10,000-token threshold.
- Tightened the bundled agent skill and protocol guidance, including safe heredoc usage.

## [0.9.0] - 2026-03-09

### Added

- Added explicit warnings when command output is automatically spilled to an artifact.

### Fixed

- Handled bridge client timeouts with clear errors.

## [0.8.0] - 2026-03-09

### Added

- Added token-budget-based spilling for large outputs.

### Changed

- Simplified function discovery output and rewrote the README around coding-agent workflows.

## [0.7.0] - 2026-03-09

### Added

- Added installation support for the bundled Codex `bn` skill.

### Changed

- Clarified `py exec` escaping, target selection, and target-implicit examples.

## [0.6.1] - 2026-03-09

### Fixed

- Corrected text status rendering in `bn doctor`.

## [0.6.0] - 2026-03-09

### Added

- Added exact token counts to spill metadata.

### Changed

- Made read commands default to text output and aligned the documentation with those defaults.
- Removed the duplicate `stack_vars` alias from function information.

## [0.5.1] - 2026-03-09

### Fixed

- Detected stale plugin loads and deduplicated local variable aliases.

## [0.5.0] - 2026-03-09

### Changed

- Narrowed the CLI to a stable, agent-oriented command surface.

## [0.4.0] - 2026-03-09

### Changed

- Used Binary Ninja's source parser for type declarations.

### Fixed

- Enumerated targets from open Binary Ninja tabs.

## [0.3.0] - 2026-03-09

### Added

- Added live readback verification for Binary Ninja write operations.

### Fixed

- Marked rename previews as changed and removed vestigial bridge fallback paths.

## [0.2.0] - 2026-03-09

### Added

- Added improved inline Python execution and mutation preview output.
- Added sole-target defaults, the initial Codex skill, type inspection and declaration import, field xrefs, and comment reads.

### Fixed

- Avoided analysis waits on the UI thread, completed text rendering paths, warned on truncated paged output, and hardened bridge transport and spill handling.

## [0.1.0] - 2026-03-09

### Added

- Added the initial agent-friendly Binary Ninja CLI and GUI companion bridge.
- Added target discovery with preferred basename selectors and stale bridge-instance pruning.

### Fixed

- Simplified bridge discovery, trusted registered bridge sockets, resolved active targets consistently, and removed legacy debug paths.

[Unreleased]: https://github.com/banteg/bn/compare/v0.13.1...HEAD
[0.13.1]: https://github.com/banteg/bn/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/banteg/bn/compare/v0.12.2...v0.13.0
[0.12.2]: https://github.com/banteg/bn/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/banteg/bn/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/banteg/bn/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/banteg/bn/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/banteg/bn/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/banteg/bn/compare/5b11dcc...v0.10.0
[0.9.0]: https://github.com/banteg/bn/compare/v0.8.0...5b11dcc
[0.8.0]: https://github.com/banteg/bn/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/banteg/bn/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/banteg/bn/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/banteg/bn/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/banteg/bn/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/banteg/bn/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/banteg/bn/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/banteg/bn/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/banteg/bn/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/banteg/bn/tree/v0.1.0
