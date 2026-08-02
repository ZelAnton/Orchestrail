# orchestrail-engine-fuzz

`cargo-fuzz` (libFuzzer) targets for `orchestrail-engine`'s untrusted-input parsers: the
stream-json leaf-agent transcript, the structured `ИТОГ:`/`SUMMARY-R`/`R-NN`/`F-NN` markers, the
`.work/events.jsonl` outbox line decoder, the Markdown control-plane parsers (`Tasks_Queue.md`,
task descriptors, `batch.md`, `cohort_state.md`, `integration_state.md`), and `config.md`. Each
target feeds raw, adversarial bytes straight into the same `pub fn` the engine calls in
production. A **panic, hang, or unbounded allocation is a defect**; a structural `None`/`Err`/
empty result is the expected, already-tested outcome (see `engine/tests/parser_properties.rs` for
the proptest-class coverage this complements).

This crate is deliberately **outside** the root Cargo workspace (`Cargo.toml` at the repo root
declares `members = ["engine", "tui"]` — an explicit list, no globs — and is not edited by this
task). `engine/fuzz/Cargo.toml` carries its own empty `[workspace]` table and its own
`Cargo.lock`, so `cargo build --workspace` / `cargo test --workspace` run from the repo root never
touch it, and it needs its own toolchain/setup below.

## Targets

| Target | Function under test |
|---|---|
| `claude_parse_transcript` | `orchestrail_engine::claude::parse_transcript` |
| `contract_parse_outcome` | `orchestrail_engine::contract::parse_outcome` |
| `contract_parse_review` | `orchestrail_engine::contract::parse_review` |
| `contract_detect_sentinel` | `orchestrail_engine::contract::detect_sentinel` |
| `contract_parse_changed_files` | `orchestrail_engine::contract::parse_changed_files` |
| `events_parse_line` | `orchestrail_engine::events::parse_line` |
| `state_parse_queue` | `orchestrail_engine::state::parse_queue` |
| `state_parse_descriptor` | `orchestrail_engine::state::parse_descriptor` (`id` fixed to `"T-1"` — it is never itself parsed, only echoed into the result; `text` is the fuzzed surface) |
| `state_parse_batch` | `orchestrail_engine::state::parse_batch` |
| `state_parse_cohort` | `orchestrail_engine::state::parse_cohort` |
| `state_parse_integration` | `orchestrail_engine::state::parse_integration` |
| `config_parse` | `orchestrail_engine::config::parse` |

Each `corpus/<target>/` directory carries a small seed set distilled from the fixtures already
exercised in `engine/tests/parser_properties.rs`, `engine/tests/state_fixture.rs`, and the
in-module `#[cfg(test)]` blocks (`engine/src/claude.rs`, `engine/src/contract.rs`,
`engine/src/events/parse.rs`, `engine/src/config.rs`) — happy-path input plus known edge shapes
(a torn/truncated tail, partially-malformed `key=value` suffixes, missing required fields,
malformed marker ids). It is deliberately tracked in Git (unlike `target/`/`artifacts/`/
`coverage/`, see `.gitignore`): coverage-guided fuzzing grows a target's corpus with new inputs it
discovers locally, but only a deliberately curated seed belongs in the repository.

## One-time setup

```pwsh
rustup toolchain install nightly       # cargo-fuzz needs nightly for sanitizer/coverage codegen
cargo install cargo-fuzz --locked
```

**Windows note.** `cargo +nightly fuzz check`/`build` (type-checking / producing the
AddressSanitizer-instrumented binary) work as-is. Actually *running* an instrumented binary
(`cargo fuzz run`) additionally needs the Clang ASan runtime DLL
(`clang_rt.asan_dynamic-x86_64.dll`) on `PATH`, shipped by Visual Studio's "C++ Clang tools for
Windows" component (`VC\Tools\MSVC\<ver>\bin\Hostx64\x64\`) — and it must match the LLVM version
the active nightly's sanitizer codegen expects, which is not guaranteed for an independently
installed MSVC toolset (`STATUS_ENTRYPOINT_NOT_FOUND` at that mismatch). This is a known rough
edge of `cargo-fuzz` on `-msvc` targets, not specific to this crate. Linux/WSL (also what the
scheduled CI job in `fuzz.yml` uses — `ubuntu-latest`) has no such caveat: the toolchain's own
sanitizer runtime is used directly.

## Running a target locally

From `engine/fuzz/`:

```pwsh
# Type-check every target without instrumenting/running it (fast; this is what CI without a
# fuzzing budget should run — see below).
cargo +nightly fuzz check

# Build one target with libFuzzer instrumentation.
cargo +nightly fuzz build events_parse_line

# Fuzz one target for a short, bounded budget (seconds), replaying the seed corpus first.
cargo +nightly fuzz run events_parse_line -- -max_total_time=60

# Fuzz every target for a short budget in turn.
cargo fuzz list | ForEach-Object { cargo +nightly fuzz run $_ -- -max_total_time=60 }
```

A crash writes its minimized reproducer under `artifacts/<target>/`; replay it with
`cargo +nightly fuzz run <target> artifacts/<target>/<crash-file>` once fixed, then (optionally)
copy it into `corpus/<target>/` as a permanent regression seed.

## CI

Regular `ci.yml` neither builds nor runs this crate (network + nightly cost); an optional,
explicitly non-blocking scheduled job may exercise `cargo +nightly fuzz run <target> --
-max_total_time=60` per target, pinning any new GitHub Action by commit SHA the same way
`ci.yml` already does.
