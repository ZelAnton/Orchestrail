# Contributing to orchestrail

Thanks for your interest in improving **orchestrail**.

## Prerequisites

- A stable Rust toolchain. The repo pins it via
  [`rust-toolchain.toml`](rust-toolchain.toml) (channel `stable`, with `rustfmt`
  and `clippy`), so `rustup` installs the right components automatically the
  first time you build.
- Your toolchain must be at least the project's **Minimum Supported Rust
  Version (MSRV)**, declared as `rust-version` in [`Cargo.toml`](Cargo.toml) and
  verified by the `msrv` CI job. `stable` is normally newer than the floor, so
  this only matters if you adopt a newer language or `std` feature — bump
  `rust-version` and the `msrv` job's toolchain together if you do.

## Build and test

```sh
cargo build
cargo test
```

Generate a local HTML code coverage report for the entire workspace with
[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov):

```sh
cargo llvm-cov --workspace --html
```

Run a single test (substring match on the test name) with:

```sh
cargo test <name>
```

Before opening a pull request, make sure the same gates CI enforces pass
locally — CI treats clippy warnings as errors, so a clean run is required:

```sh
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo deny check advisories bans
```

## Fuzzing untrusted-input parsers

`engine/fuzz/` (a separate `cargo-fuzz` crate, deliberately outside this repo's Cargo
workspace) fuzzes the parsers that consume bytes the engine does not control (leaf-agent
transcripts, `events.jsonl`, the Markdown control plane, `config.md`). See
[`engine/fuzz/README.md`](engine/fuzz/README.md) for setup and how to run a target locally.

## Mutation testing

Mutation testing checks whether the test suite catches deliberately introduced logical
errors, rather than only measuring whether code was executed. Orchestrail uses
[`cargo-mutants`](https://mutants.rs/) for deterministic resolver, state, contract, event,
processor, and time code; expensive process, browser, filesystem, and VCS boundaries are excluded.
Run it locally with `bash ./scripts/run-mutants.sh` on Linux/macOS or
`.\scripts\run-mutants.ps1` on Windows. For a verified resolver-only smoke run,
append `--quick` to either command; it first validates the production
`.cargo-mutants.toml` selection and integration-boundary exclusions, then mutates
only `engine/src/resolvers/tiering.rs`.

The production configuration explicitly excludes the following integration boundaries:

- `engine/src/vcs.rs` — VCS operations.
- `engine/src/headless.rs` and `engine/src/supervise.rs` — headless agent and process supervision.
- `engine/src/run.rs` — process execution.
- `engine/src/notification.rs` — external notification dispatch.
- `engine/src/verification.rs` and `engine/src/legacy_fingerprint.rs` — VCS-backed verification and fingerprinting.

These files remain listed even when current `examine_globs` patterns do not select
them: explicit denylists guard against a future pattern broadening accidentally
bringing an integration boundary into the mutation scope. Event and state files
are likewise listed individually so filesystem adapters remain outside that scope.

A surviving mutant is not automatically a defect: it may affect irrelevant or unused
utility behavior. Investigate each survivor, then strengthen the tests, exclude a harmless
mutation in the configuration, or document the limitation. See the
[`cargo-mutants` result guide](https://mutants.rs/using-results.html) for filtering and
inspection details.

## Conventions

- **Formatting** is governed by `rustfmt` (run `cargo fmt`); non-Rust files
  follow [`.editorconfig`](.editorconfig) (LF line endings, final newline). Do
  not reformat code you are not changing.
- **Dependencies** — every entry in [`Cargo.toml`](Cargo.toml) carries an inline
  comment explaining *why* it is there; pin major versions and enable only the
  features you use. `Cargo.lock` is committed for reproducible builds.
- **Commit subjects** are conventional-commit style (`type(scope): summary`) —
  they feed the changelog auto-fill via [`cliff.toml`](cliff.toml).
- See [`AGENTS.md`](AGENTS.md) for the full, authoritative set of conventions
  (code style, dependency management, supply chain/MSRV, version control).

## Changelog

Every user-visible change ships its [`CHANGELOG.md`](CHANGELOG.md) entry in the
same change set, under `## [Unreleased]`. Write the bullet for a consumer of the
crate, not the implementer. Pure internal refactors are exempt.

## Pull requests

- Keep changes focused; unrelated cleanups belong in their own PR.
- Ensure CI (fmt, clippy, build/test on Linux, Windows, and macOS, cargo-deny,
  MSRV) passes.
- Fill in the pull-request checklist.
