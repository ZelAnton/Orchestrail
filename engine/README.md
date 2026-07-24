# orchestrail-engine

`orchestrail-engine` is the headless, crash-safe implementation of the Orchestra processor
contract. The Rust reducer owns orchestration decisions; leaf models, VCS/forge adapters, the
control plane, and the operator TUI exchange typed commands, effects, evidence, and events.

The native processor covers the complete cohort path:

1. owner lease, PAUSE, and Phase-0 recovery;
2. inbox/dependency reconciliation, planning, transactional capture, and rolling admission;
3. contained coder/reviewer execution with bounded retries and exact VCS evidence;
4. sequential typed merge, conflict repair, integration review/fix, and verification;
5. publication, exact-SHA CI observation/repair, and knowledge curation;
6. CI re-confirmation, immutable task archival, guarded cleanup, final inbox delivery, and the
   next-cohort boundary.

Every impure effect is recorded in `processor-runtime.json` before dispatch. Restart classifies a
pending effect as either guarded/idempotent or inspect-before-continuing; it never guesses that an
unknown model, merge, push, or delivery may be replayed.

## Architecture

| Area | Main modules |
|---|---|
| Pure lifecycle reducer and durable effect vocabulary | `processor.rs`, `runtime.rs`, `execution.rs` |
| Native scheduler, adapters, and recovery import | `native_loop.rs`, `native.rs`, `native_port.rs`, `recovery.rs` |
| ProcessKit-contained model/tool boundaries | `headless.rs`, `supervise.rs`, `claude.rs`, `codex.rs`, `verification.rs` |
| Typed Git/Jujutsu and forge boundaries | `vcs.rs` and the published `vcs-*` crates |
| Markdown control plane and policy | `control.rs`, `config.rs`, `policy.rs`, `approval.rs`, `roadmap.rs` |
| Inbox, dependency graph, and release fan-out | `inbox.rs`, `queue_inbox.rs`, `dependency_graph.rs`, `release.rs` |
| Event contract and immutable metrics | `events/`, `telemetry.rs`, `legacy_fingerprint.rs` |

`engine` stays headless. It emits state/events and accepts named commands; orchestration policy
does not live in `tui`.

## Process and VCS boundaries

Production child processes run through the published `processkit` crate with typed argv, bounded
output, deadlines/cancellation, and process-tree containment. Product code does not use shell
command strings or `std::process::Command`.

Git, Jujutsu, GitHub, and other forge operations go through the published `vcs-core`,
`vcs-cli-support`, `vcs-diff`, and relevant `vcs-*` forge crates. Native code proves exact refs,
workspaces, changed paths, publication ancestry, and CI SHA before acknowledging the reducer.

## Events and task accounting

`.work/events.jsonl` is an append-only, crash-tail-repairing outbox with deterministic UUIDv5
identities. `operation.completed` is a strict scalar-only timing spine materialized on real task
IDs. It joins `usage.recorded` by replay-stable role/mode/attempt coordinates and divides shared
cohort/integration calls by `shared_task_count`.

Before a task descriptor is removed, Phase 6 projects its full descriptor plus exactly one
`orchestra/task-execution-metrics@1` block into `Tasks_Done.md`. The projection reports
`ok|partial|no_data|error`, never invents missing tokens as zero, and atomically repairs the
header-only crash window. Recovery also accepts an already `выполнена` descriptor without
repeating its publication CI gate.

## Validation

From the repository root:

```pwsh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Tests are hermetic by default. Real Git/Jujutsu fixtures use disposable repositories; model
provider calls remain explicit and opt-in. The read-only legacy checkout is a behavioral oracle
for catch-up work, never a runtime dependency and never a writable test fixture.

The detailed responsibility/evidence matrix lives in
[`../.work/processor_coverage.md`](../.work/processor_coverage.md). The recorded comparison
baseline remains [`../.work/orchestra-sync.json`](../.work/orchestra-sync.json) and is advanced
only by the explicit synchronization workflow.
