# 0002. Typed VCS crates instead of shell CLI wrappers

Status: Accepted · Date: 2026-04-22

## Decision

`engine/src/vcs.rs` is the Rust engine's only route for Git and Jujutsu repository operations. It uses the typed `vcs-core`, `vcs-git`, `vcs-jj`, `vcs-cli-support`, and `vcs-diff` crates rather than assembling shell commands or maintaining shell CLI wrappers.

The module builds on the common Git/JJ facade, rejects flag-like VCS input with `vcs_cli_support::reject_flag_like`, and verifies each managed worktree or workspace after creation.

## Why

- Typed repository operations make branch, revision, merge, diff, and worktree intent explicit at the engine boundary.
- Keeping VCS-like user input out of argv prevents it from being interpreted as an option.
- A single repository adapter makes managed-worktree verification part of the operation rather than an optional caller convention.
- The module documentation explicitly excludes shell strings and direct process spawning.

## Consequences

- New Rust-engine repository behaviour belongs in `engine/src/vcs.rs` and must use the published VCS abstractions.
- Callers receive typed repository data such as backend-neutral snapshots and verified workspace coordinates instead of parsing command output.
- This separates repository concerns from the direct tool-execution boundary in [ADR 0001](0001-processkit-sole-boundary.md).
