# 0004. Fail closed for unported integrations

Status: Accepted · Date: 2026-05-10

## Decision

The native processor path fails closed when an integration does not have a native, typed implementation. It does not silently fall back to the legacy sandbox mode: `engine/src/lib.rs` identifies `engine run` as a sandbox-only compatibility fixture and the native processor as the production-oriented path.

Model calls and forge CI are injected at the native-port edge because projects differ in authentication and provider policy. All other native processor operations use the durable `ControlPlane` and `VcsService` layers. Publication CI accepts only the typed GitHub, GitLab, and Gitea backends; an unrecognised `FORGE` value is rejected during configuration rather than allowing publication without observed CI.

## Why

- A partially ported integration must not bypass the durable reducer ledger or degrade into an unsafe compatibility path.
- Project-specific model and provider policy remains an explicit injected dependency instead of an implicit global assumption.
- The forge CI gate can report a passing result only after positive, commit-SHA-bound confirmation of the selected checks.
- Rejecting unknown forge configuration avoids guessing a provider from a remote URL and polling the wrong API.

## Consequences

- Unsupported native integrations stop with explicit error context and require a native implementation before they can proceed.
- Extending publication CI requires an explicit typed forge implementation and configuration support; it cannot rely on a generic fallback.
- GitHub, GitLab, and Gitea configuration are supported; values outside `github`, `gitlab`, and `gitea` are invalid.
- Native effects flow through `ControlPlane` and `VcsService`, reinforcing [ADR 0001](0001-processkit-sole-boundary.md) and [ADR 0002](0002-typed-vcs-crates.md).
