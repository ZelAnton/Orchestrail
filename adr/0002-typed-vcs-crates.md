# 0002. Typed VCS crates instead of shell CLI wrappers

Status: Accepted · Date: 2026-04-22

## Decision

`engine/src/vcs.rs` is the Rust engine's route for Git and Jujutsu repository orchestration operations. It uses the typed `vcs-core`, `vcs-git`, `vcs-jj`, `vcs-cli-support`, and `vcs-diff` crates rather than assembling shell commands or maintaining shell CLI wrappers. `engine/src/legacy_fingerprint.rs` is a deliberate exception for differential fingerprint comparison: it uses `vcs-core`, `vcs-git`, `vcs-jj`, and `vcs-diff` directly for repository discovery, complete committed-tree inventory, and typed blob reads.

The module builds on the common Git/JJ facade, rejects flag-like VCS input with `vcs_cli_support::reject_flag_like`, and verifies each managed worktree or workspace after creation.

## Why

- Typed repository operations make branch, revision, merge, diff, and worktree intent explicit at the engine boundary.
- Keeping VCS-like user input out of argv prevents it from being interpreted as an option.
- A single repository orchestration adapter makes managed-worktree verification part of the operation rather than an optional caller convention.
- The module documentation explicitly excludes shell strings and direct process spawning.

## Consequences

- New Rust-engine repository orchestration behaviour belongs in `engine/src/vcs.rs` and must use the published VCS abstractions; the direct typed-crate use in `legacy_fingerprint.rs` remains limited to differential fingerprint comparison.
- Callers receive typed repository data such as backend-neutral snapshots and verified workspace coordinates instead of parsing command output.
- This separates repository concerns from the direct tool-execution boundary in [ADR 0001](0001-processkit-sole-boundary.md).
