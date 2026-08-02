# 0001. ProcessKit as the sole process execution boundary

Status: Accepted · Date: 2026-04-22

## Decision

The engine uses ProcessKit as its sole direct production process-execution boundary. `engine/src/supervise.rs` owns the contained invocation of an external tool and preserves the engine's synchronous `Verdict` contract by running the asynchronous ProcessKit call on a current-thread Tokio runtime.

The boundary applies deadlines, cancellation, and bounded output capture to each invocation. Direct process callers receive the stable `Reason` mapping `ok=0`, `timeout=3`, `cancelled=4`, `crash=5`, and `error=6`.

## Why

- ProcessKit drains standard streams without deadlock and owns the child tree in an OS containment primitive.
- It tears down the contained process tree on timeout, cancellation, or drop, including support for parent-death cleanup.
- A synchronous verdict and legacy-compatible exit codes give callers deterministic failure handling while the implementation can use asynchronous ProcessKit APIs.
- Repository operations have their own typed boundary; see [ADR 0002](0002-typed-vcs-crates.md).

## Consequences

- Production code that directly runs a tool must go through `engine/src/supervise.rs` instead of constructing an independent process-launch path.
- Output is deliberately bounded; exceeding the configured capture ceiling is an `error` verdict rather than a successful result with a truncated transcript.
- The synchronous API remains a compatibility constraint for callers even though ProcessKit itself is asynchronous.
