//! orchestrail-engine — T-097 Stage 1 de-risking spike.
//!
//! Purpose (intent doc §8.1, risk R1): prove, OUTSIDE Claude Code, that a compiled engine
//! can spawn and supervise ONE `claude` leaf-agent call and ONE `codex exec` call with a
//! deadline / maxTurns, capture their structured output, and handle permission/consent
//! correctly — the single true unknown before any headless engine is worth building.
//!
//! Every module is a small, unit-tested primitive. The engine uses `serde_json` for durable
//! event decoding and ProcessKit for its one permitted process-execution boundary:
//!
//! * [`supervise`] — ProcessKit-contained spawn + deadline + cancellation + bounded capture,
//!   with a verdict contract compatible with `tools/supervisor.ps1`.
//! * [`claude`] — headless `claude -p --output-format stream-json` argv + transcript
//!   parse + explicit per-call permission posture (the T-057 lesson).
//! * [`codex`] — fail-closed `codex exec` argv mirroring `tools/codex-runtime.ps1`.
//! * [`contract`] — deterministic parse of leaf-agent structured markers (§8.2).
//! * [`jsonline`] — minimal top-level JSON field scanner for stream-json lines.
//! * [`time`] — dependency-free Unix epoch to ISO-8601 UTC formatting shared with the TUI.
//!
//! Beyond the original spike, the crate now also carries [`events`] — typed, read-only access
//! to the `.work/events.jsonl` durable event outbox (contract `docs/queue_contract.md` §19).
//! This is the first module to grow the crate from spike toward engine and the first to pull
//! in `serde_json` (see Cargo.toml / README "Spike outcome"). It only *reads* the journal; it
//! is not wired into the running orchestrator.
//!
//! [`state`] extends that read-only direction to the **control plane** (contract §13): it parses
//! the queue / task-descriptor / cohort / integration / batch Markdown artifacts into one typed
//! [`state::Snapshot`], mapping the Cyrillic status literals onto their canonical ASCII names.
//! Like [`events`], it only *reads* `.work/` — no mutation, no lock — and is not wired into the
//! running orchestrator; it is the model layer future resolvers and the TUI build on.
//!
//! [`resolvers`] is that first layer of resolvers: the processor's per-task decision trees
//! (`agents/processor.md` phases 2.x — reviewer tiering, Codex maker/checker routing, the clean
//! gate, review-cycle limits) compiled into deterministic pure functions over typed inputs (and
//! reusing [`contract::ReviewParse::is_clean_pass`]). Like the layers below it, it performs no
//! I/O and no mutation and is not wired into the running orchestrator.
//!
//! [`ownership`] and [`lease`] provide the engine's native owner-lease interlock (contract
//! §14–§17, task T-107). They read and write the interoperable `orchestra/lease@1` record under an
//! owner-checked, liveness-gated protocol; they never force-remove a foreign lease. The `lease`
//! module keeps CLI vocabulary and exit-code compatibility, while [`ownership`] is the durable
//! implementation used by `engine lease` and the native processor.
//!
//! [`processor`], [`runtime`], [`native_loop`], [`native_port`], and [`headless`] now compose the
//! durable reducer into the opt-in `engine processor --once --live` path. This path owns the
//! checkpoint/effect ledger, control-plane transitions, typed VCS actions and ProcessKit-contained
//! model leaves without invoking legacy scripts. It also has a durable local verification gate
//! for the final reviewed integration tip immediately before publication. It is intentionally fail-closed for integrations
//! that do not yet have a native port (for example non-GitHub forge CI polling).
//!
//! [`run`] is the first module that *composes* all the layers above into a real control loop —
//! but ONLY over a hermetic **sandbox** `.work` handed in as `--work <dir>` (task T-109): it takes
//! the [`lease`], admits a cohort with the [`resolvers`], captures each task through
//! `tools/queue-tx.ps1`, runs ONE supervised leaf round ([`supervise`] + [`claude`] + [`contract`]),
//! validates each descriptor/cohort transition through `tools/state-tx.ps1`, and emits the round's
//! events through `tools/outbox.ps1`. It remains a sandbox-only compatibility fixture exercised by
//! `engine run --once` and the hermetic `run_fixture` test; the native `processor` path is the
//! production-oriented implementation.

pub mod approval;
pub mod checkpoint;
pub mod claude;
pub mod codex;
pub mod command_line;
pub mod config;
pub mod config_discovery;
pub mod contract;
pub mod control;
pub mod dependency_graph;
pub mod events;
pub mod execution;
pub mod headless;
pub mod inbox;
pub mod jsonline;
pub mod lease;
pub mod legacy_fingerprint;
pub mod native;
pub mod native_loop;
pub mod native_port;
pub mod notification;
pub mod outcome_adapter;
pub mod ownership;
pub mod policy;
pub mod processor;
pub mod queue_inbox;
pub mod recovery;
pub mod release;
pub mod resolvers;
pub mod roadmap;
pub mod run;
pub mod runtime;
pub mod state;
pub mod supervise;
pub mod task_id;
pub mod telemetry;
pub mod time;
pub mod toolscript;
pub mod vcs;
pub mod verification;
/// Confined, bounded filesystem primitives shared by the headless engine and its operator TUI.
///
/// Keeping this module public inside the private workspace crates prevents the TUI from growing
/// a second, weaker implementation for the same authority-bearing `.work` artifacts.
pub mod work_fs;
