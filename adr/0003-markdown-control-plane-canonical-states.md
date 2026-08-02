# 0003. Markdown control plane with canonical ASCII states

Status: Accepted · Date: 2026-04-23

## Decision

The orchestrator's durable control plane is represented by human-readable Markdown artifacts for queue, task descriptor, cohort, integration, and batch state. `engine/src/state/mod.rs` parses those artifacts into the typed, deterministic `Snapshot` model.

`engine/src/state/canonical.rs` is the engine's immutable compatibility mapping from Cyrillic Markdown status literals to the canonical ASCII state vocabulary used by the contract and `tools/state-tx.ps1`: for example, `не начата` maps to `not-started`, `в работе` to `working`, `на ревью` to `in-review`, and `готова к слиянию` to `ready`. The state module is read-only: it observes the control plane but does not write files, acquire the orchestrator lease, or validate state transitions.

## Why

- Markdown control-plane files are inspectable and can represent the backlog and live lifecycle in durable repository artifacts.
- The contract fixes canonical ASCII names while allowing human-readable Cyrillic task and cohort literals as a compatible representation.
- A byte-for-byte mapping shared with the contract tables and transition validator prevents consumers from assigning different meanings to the same status label.
- Separating parsing from transition mutation lets the engine and TUI consume a common snapshot without bypassing the transition authority.

## Consequences

- Consumers use canonical names such as `not-started`, `working`, `in-review`, `ready`, `merged`, `published`, `done`, `escalated`, and `conflict` after parsing task literals.
- Cohort admission uses the canonical `open` and `closed` names; integration state has its own canonical vocabulary, including `none` and `in-progress`.
- State changes and transition validation remain the responsibility of `tools/state-tx.ps1`, not the state snapshot layer.
- The explicit read-only boundary complements the durable native processor described in [ADR 0004](0004-fail-closed-unported-integrations.md).
