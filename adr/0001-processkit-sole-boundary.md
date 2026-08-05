# 0001. ProcessKit as the sole process execution boundary

Status: Accepted · Date: 2026-04-22

## Decision

The engine uses ProcessKit as its sole direct production process-execution boundary. `engine/src/supervise.rs` owns the contained invocation of an external tool and preserves the engine's synchronous `Verdict` contract. The single-call `run()` path hosts its asynchronous ProcessKit call on a current-thread Tokio runtime; `run_batch()` uses a bounded multi-thread Tokio runtime so independent supervised calls can run concurrently.

The boundary applies deadlines, cancellation, and bounded output capture to each invocation. Direct process callers receive the stable `Reason` mapping `ok=0`, `timeout=3`, `cancelled=4`, `crash=5`, and `error=6`.

## Why

- ProcessKit drains standard streams without deadlock and owns the child tree in an OS containment primitive.
- It tears down the contained process tree on timeout, cancellation, or drop. Abrupt-parent-death cleanup is platform-specific: Windows Job Objects reap the whole tree, Linux `kill_on_parent_death` reaches the direct child, and macOS/BSD provide no guaranteed abrupt-parent-death cleanup.
- A synchronous verdict and legacy-compatible exit codes give callers deterministic failure handling while the implementation can use asynchronous ProcessKit APIs.
- Repository operations have their own typed boundary; see [ADR 0002](0002-typed-vcs-crates.md).

## Consequences

- Production code that directly runs a tool must go through `engine/src/supervise.rs` instead of constructing an independent process-launch path.
- Output is deliberately bounded; exceeding the configured capture ceiling is an `error` verdict rather than a successful result with a truncated transcript.
- The synchronous API remains a compatibility constraint for callers even though ProcessKit itself is asynchronous.
- Batch execution uses a multi-thread runtime to drive concurrent supervised calls while retaining a separate ProcessKit container, deadline, cancellation token, and output bound for each call.

## ProcessKit 3.x migration review (2026-08-05)

### Decision

Do not upgrade the workspace from ProcessKit 2.3.2 to 3.2.0 in this change. This is a conscious, evidence-based refusal rather than a conclusion that the direct supervisor API is too difficult to port.

The direct `supervise.rs` surface is almost entirely source-compatible, but all locked `vcs-*` crates require `processkit = "2.1"`. A direct dependency bump therefore resolves both ProcessKit 2.3.2 and 3.2.0. The engine intentionally uses the direct `processkit::Error` type at its VCS and forge boundaries, so the two major versions are not interchangeable. An offline `cargo check --workspace --all-targets` of that exact candidate produced 803 diagnostics across `headless.rs`, `native_port.rs`, and `vcs.rs`; the primary failures were futures returning ProcessKit 2.x errors where the engine expected the direct ProcessKit 3.x error. Fixing that would require either upgrading the whole published `vcs-*` family or changing integration modules and error contracts outside this decision's conflict domain. Keeping two direct aliases would also undermine the single-boundary intent of this ADR.

Reconsider the upgrade when the selected `vcs-*` release train accepts ProcessKit 3.x, or in a separately scoped cross-boundary migration that is allowed to change the VCS/forge error adapters. The latter must also decide how shutdown grace enters `SpawnSpec` without silently changing its public contract.

### Published-source API audit

The audit compared the published crates.io source payloads, not release notes: [ProcessKit 2.3.2 source](https://docs.rs/crate/processkit/2.3.2/source/) and [ProcessKit 3.2.0 source](https://docs.rs/crate/processkit/3.2.0/source/). Both manifests declare `rust-version = "1.88"`, matching this workspace.

| Used element | ProcessKit 2.3.2 | ProcessKit 3.2.0 assessment |
| --- | --- | --- |
| `CancellationToken` | Re-export of `tokio_util::sync::CancellationToken` | Unchanged re-export and compatible with `new`, `clone`, and `cancel` ([2.3 `lib.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/lib.rs), [3.2 `lib.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/lib.rs)). |
| `Command` | `new(impl AsRef<OsStr>)`, consuming `args`, `current_dir`, `stdin`, `timeout_opt`, `timeout_grace`, `cancel_on`, `create_no_window`, `kill_on_parent_death`, `output_buffer`, and async `output_string(&self)` | Every used signature remains compatible. New shutdown controls are assessed separately below ([2.3 `command.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/command.rs), [3.2 `command.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/command.rs)). |
| `Error` | A non-exhaustive enum; `supervise.rs` directly matches `Error::NotFound`, `Error::Spawn`, and `Error::OutputTooLarge`. Accessors such as `stdout`, `stderr`, `code`, `is_timeout`, and `is_cancelled` are present. | Breaking change: `Error` is now a pointer-sized wrapper around non-exhaustive `ErrorReason`. Accessors remain, but variant matching must use `error.reason()`/`error.into_reason()` and `ErrorReason`, or the new coarse `ErrorKind`. The direct supervisor has three affected matches; the larger blocker is the incompatible 2.x error returned by the locked VCS crates ([2.3 `error.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/error.rs), [3.2 `error.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/error.rs)). |
| `JobRunner` | Cloneable zero-sized default runner with `new() -> Self`. | Unchanged for the used construction and runner role ([2.3 `runner.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/runner.rs), [3.2 `runner.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/runner.rs)). |
| `OutputBufferPolicy` | Public `max_lines`, `max_bytes`, and `overflow`; `fail_loud(usize) -> Self`; `with_max_bytes(usize) -> Self`. | Used fields and signatures are unchanged, so the line-plus-byte fail-loud policy and its assertions remain compatible ([2.3 `buffer.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/buffer.rs), [3.2 `buffer.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/buffer.rs)). |
| `ProcessResult<String>` | `timed_out() -> bool`, `code() -> Option<i32>`, `signal() -> Option<i32>`, `stdout() -> &String`, `stderr() -> &str`, and `duration() -> Duration`. | All used accessors and return shapes remain compatible. 3.x adds outcome variants and accessors that this boundary does not need ([2.3 `result.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/result.rs), [3.2 `result.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/result.rs)). |
| `Stdin` | `from_string(impl Into<String>) -> Self`. | Unchanged for the used path ([2.3 `stdin.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/stdin.rs), [3.2 `stdin.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/stdin.rs)). |
| `output_all` | `async fn output_all<R, I>(commands: I, concurrency: usize, runner: &R) -> Vec<Result<ProcessResult<String>>>`, with `R: ProcessRunner + ?Sized` and `I: IntoIterator<Item = Command>`. | Signature, bounded fan-out, no-short-circuit behavior, and input-order collection remain compatible. 3.x adds streaming siblings but they are neither required nor adopted ([2.3 `batch.rs`](https://docs.rs/crate/processkit/2.3.2/source/src/batch.rs), [3.2 `batch.rs`](https://docs.rs/crate/processkit/3.2.0/source/src/batch.rs)). |

No new optional 3.x feature (JSON, PTY, metrics, tracing, record/replay, limits, or report serialization) is needed for this boundary and none was enabled.

### Graceful signal ladder evaluation

ProcessKit 3.2 does provide a configurable two-tier shutdown sequence per `Command`: one caller-selected soft signal, a caller-selected grace duration, then a hard kill for survivors. It is not an arbitrary multi-step list of signals.

- Timeout: `timeout_grace(Duration)` selects the per-command grace window and `timeout_signal(Signal)` selects the Unix soft signal before `SIGKILL`. These APIs already exist in 2.3.2; Orchestrail currently supplies a fixed 200 ms timeout grace and leaves the signal at `SIGTERM`.
- Cancellation: 3.2 adds `cancel_grace(Duration)` and `cancel_signal(Signal)`, independently configurable from the timeout pair. This is the material new capability. The 2.3.2 source shows that `cancel_on` hard-kills immediately; the current `timeout_grace(200 ms)` does **not** apply to cancellation.
- Windows: Unix signal selection is ignored. ProcessKit 3.2 can post `WM_CLOSE` to windowed children or, with `windows_graceful_ctrl_break`, send `CTRL_BREAK` to a console child. The latter explicitly cannot reach a child launched with `create_no_window`, which is a non-negotiable Orchestrail invariant. A typical headless, windowless agent therefore still has no usable soft tier and is hard-killed by the Job Object.

The 3.2 API can close the hardcoded-grace gap for Unix timeouts and cancellations only after Orchestrail exposes or derives a per-call grace value and calls both grace builders. It does not close that gap merely by upgrading, and it does not provide an equivalent graceful path for the current Windows `create_no_window` launch. Adding a grace field or builder to public `SpawnSpec`, or enabling a different Windows console policy, is intentionally not smuggled into this dependency review.

### Behavioral invariants and lock-file impact

Because the candidate was refused and fully restored, `Cargo.toml`, `Cargo.lock`, `engine/src/supervise.rs`, and the fixtures retain their existing behavior:

- reason exit codes remain `ok=0`, `timeout=3`, `cancelled=4`, `crash=5`, and `error=6`;
- output still fails loud at both the line and byte ceilings;
- timeouts retain the fixed 200 ms grace and cancellations retain their actual 2.3.2 immediate-hard-kill behavior;
- file-watcher and in-process-probe cancellation, Windows `create_no_window`, parent-death containment, private process groups, and input-ordered `output_all` fan-out through a shared `JobRunner` are unchanged;
- containment and headless fixture assertions are unchanged.

The rejected lock candidate retained ProcessKit 2.3.2 for the locked VCS family and added ProcessKit 3.2.0 for the engine. Both package entries used the same already-resolved transitive set: `async-trait`, `encoding_rs`, `libc`, `mutants`, `thiserror`, `tokio`, `tokio-stream`, `tokio-util`, and `windows-sys 0.61.2`. Consequently the candidate added exactly one package entry (`processkit 3.2.0`) and added, removed, or upgraded no transitive packages. Restoration removed that extra entry; the committed lock outcome remains the single ProcessKit 2.3.2 package.

Validation of the restored decision state passed `cargo fmt --all --check`, `cargo build --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings`. The focused supervisor unit tests, the original Windows nested-descendant containment fixture, and both headless fixture suites also passed with their assertions unchanged. A full `cargo test --workspace` reached 857 passing tests but was not green: 17 unrelated JJ integration tests failed because JJ could not write temporary repository index/operation files under the sandbox's `.work/codex-cache/tmp`; representative failures reproduced individually after ProcessKit 2.3.2 and its single lock entry had been restored. This environmental baseline failure neither motivated the refusal nor supplies evidence for accepting 3.x.
