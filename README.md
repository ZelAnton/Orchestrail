# Orchestrail

Orchestrail is a deterministic, compiled orchestrator and its operator TUI. It
evolves the control loop currently implemented by the legacy `orchestra`
repository while keeping agent judgment outside the deterministic core.

The workspace contains two private crates:

- `engine/` — headless orchestration state, decision resolvers, event readers,
  and supervised external-tool integration.
- `tui/` — the ratatui operator console over the engine's state and event
  contracts. See the [TUI operator guide](docs/tui-guide.md) for running,
  navigation, and command safety semantics.

The intended architecture and staged migration are recorded in
[`plans/DETERMINISTIC_ORCHESTRATOR_INTENT.md`](plans/DETERMINISTIC_ORCHESTRATOR_INTENT.md).
Each developer's local `.work/orchestra-sync.json` records the provenance and
next synchronization point with the legacy source.

## Development

```pwsh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

`engine` and `tui` launch external programs through the published `processkit`
crate. Product code uses the published `vcs-core`, `vcs-cli-support`, and
`vcs-diff` crates for VCS work; it does not shell out to Git or Jujutsu directly.
Typed forge integration is a tracked native-port gap, not an implicit CLI
fallback.

The new native control-path is deliberately opt-in while it catches up with the
legacy implementation:

```pwsh
cargo run -p orchestrail-engine -- processor --once --live --work .work --root .
```

It uses the native lease, reducer checkpoint/effect ledger, ProcessKit-contained
headless agents, and typed VCS operations. One invocation keeps its owner lease across
successive cohorts until the normal delivery lane is exhausted, without opening an empty cohort.

Adding `--watch` turns that single drain into a long-lived local service: when the lane is
exhausted the process keeps its lease and waits at that settled boundary for new current-lane
queue rows, then drains them as the next delivery wave under a distinct cohort id. Waiting is a
bounded backoff poll of the queue (5 s after a drained lane, doubling up to 60 s, reset after every
drained lane), so populators can simply append tasks instead of restarting the engine per wave.
Create `.work/WATCH_STOP` to end the waiting cleanly at the next poll boundary, or the ordinary
`.work/PAUSE` to hold the pipeline; both are observed only between waves and never interrupt a
running cohort. A `.work` that can no longer be read ends the waiting too, but as a non-zero
failure naming the read error — never as a clean stop attributed to a marker that was not observed,
so a supervisor sees an incident instead of a silent shutdown. Escalated tasks still stop the run
instead of being hidden behind an endless wait.

After the final integration review and immediately
before publication, `VERIFICATION_COMMANDS` (or the `SMOKE_CMD` fallback) runs sequentially
against that exact integration tip; `VERIFICATION_MODE: auto|required` without a profile is held
rather than treated as success. With `CI_WATCH: on`, GitHub commit checks are polled through `vcs-github`
against the exact published SHA; an unsupported forge, empty/unknown checks, transport failure,
or timeout remains fail-closed rather than becoming an implicit success.

`REVIEW_CYCLE_VERIFICATION: on` additionally runs that profile inside each task's worktree on
every review/fix cycle, under the same containment and `CALL_OUTPUT_MAX_BYTES` ceiling, and mixes
a failing result into the task's `review.md` as an open finding before that round's reviewer is
dispatched — so a broken build reaches the fixer in the round that caused it instead of at the
publication gate several rounds later. `REVIEW_CYCLE_VERIFICATION_COMMANDS` narrows the per-round
profile (for example lint and build only, without the heavy tests). The option is off by default,
and leaving it off keeps verification a Phase-4-only gate.

A new repository can bootstrap that profile instead of writing it by hand:

```pwsh
cargo run -p orchestrail-engine -- config discover --live --work .work --root .
```

Like every other real model call in this binary, discovery is opt-in via `--live` and consumes
tokens. It makes exactly one read-only Codex call proposing format/lint/test candidates, then
validates each one locally before it can reach `config.md`: the executable must be one of a fixed
allowlist of recognized verification tools (`cargo`/`rustfmt`, the `npm`/`pnpm`/`yarn`/`bun`/`npx`
family, `just`, `make`, `go`, and the `python` toolchain), it must be on `PATH`, and its declared
witness file (e.g. `Cargo.toml` for `cargo`) must actually exist in the repository — a program
outside the allowlist is never accepted, no matter what file it names as evidence. A rejected
candidate is never dropped silently: it is written as an explicit `off`-keyed comment naming the
reason. If any of `VERIFICATION_COMMANDS`, `REVIEW_CYCLE_VERIFICATION_COMMANDS`, `SMOKE_CMD`, or
`VERIFICATION_MODE` is already present — including a bare fail-closed `VERIFICATION_MODE:
required` with no profile yet — discovery leaves `config.md` untouched instead of spending the
call. An existing `config.md` that already fails to parse (for example an empty
`VERIFICATION_COMMANDS: []`, which the engine's own loader rejects) is reported as invalid and
never has a call spent on it or a second, duplicate key layered on top. A backend failure or
unparsable model response also aborts before any write.

Published releases use a separate mode and never enter the task queue:

```pwsh
cargo run -p orchestrail-engine -- release-sync --live --work .work --root . --version 1.2.3
```

The mode takes the owner lease, performs a typed fast-forward-only trunk sync, proves the release
tag, refreshes the dependency graph, creates content-bound notes under
`.work/release_notifications/`, and idempotently notifies the frozen dependent audience. Use
`--resume` after a partial delivery; replacement notes, products, subject, or URL are then refused.
