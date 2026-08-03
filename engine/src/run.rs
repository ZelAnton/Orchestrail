//! `engine run --once --work <sandbox>` — the engine's FIRST real end-to-end orchestration of
//! ONE cohort/phase (task T-109), driven strictly over a **sandbox** `.work` handed in as
//! `--work <dir>` and never against a live repository `.work`.
//!
//! This is where the primitives built up by the spike and the T-103…T-107 layers are finally
//! *composed* into a control loop, but only in the hermetic sandbox `tools/harness.ps1` builds:
//!
//! 1. **Lease.** Take the engine's owner lease (role `engine`) through [`crate::lease`] /
//!    `tools/state-tx.ps1` (never a direct lock write, never `--force`; K-003), heartbeat it on
//!    every step boundary, and release it on completion OR error (no dangling owner lease).
//! 2. **Cohort open.** Read a control-plane [`Snapshot`], admit ready, non-overlapping candidates
//!    via the T-106 [`plan_admission`] resolver, and COMMIT the decision transactionally —
//!    `tools/queue-tx.ps1 capture` for each admitted task and the cohort admission transition
//!    validated by `tools/state-tx.ps1 check-transition --kind cohort` — never by hand-editing the
//!    queue / control-plane state.
//! 3. **One execution round.** For each captured task, run ONE supervised leaf call — the
//!    deterministic, offline `__fake-agent` stand-in by default, OR (opt-in `--live`, task T-244)
//!    the REAL `claude -p`/`codex exec` child routed through the executor resolvers — parse its
//!    structured report with [`crate::contract`], apply the T-105
//!    per-task decision (reviewer tier via [`base_reviewer`]), validate the descriptor status
//!    transition through `state-tx check-transition --kind task`, write the descriptor, and emit
//!    the round/task events through `tools/outbox.ps1` in the §19 outbox format.
//! 4. **Per-task review fix cycle (opt-in, `--review`).** After the execution round, every task
//!    that reached `in-review` enters the phase-2.4…2.9 review fix cycle: the base reviewer tier is
//!    picked by [`base_reviewer`] and the concrete reviewer by [`reelect_reviewer`], a supervised
//!    reviewer stand-in writes the task's `review.md`, [`crate::contract::parse_review`] parses it,
//!    and [`review_gate`] names the branch. A **clean** pass promotes `in-review -> ready`;
//!    **findings** dispatch a supervised deterministic coder fix round and RE-REVIEW, looping under
//!    [`review_cycle_decision`] (`REVIEW_LOOP_MAX`); an **incomplete** pass re-runs the reviewer
//!    unchanged (phase 2.7). Exhausting the cycle budget is a CLEAN terminal escalation
//!    (`in-review -> escalated`, `не сходится ревью после N циклов`) through the SAME
//!    `state-tx check-transition` + `queue-tx escalate` path the execution round uses — never a
//!    re-interpretation. Every cycle transition is validated through `state-tx check-transition`
//!    before the descriptor is written, is recorded on the descriptor as `Циклов-ревью: N`, and is
//!    emitted through `tools/outbox.ps1` keyed by the cycle number (so each cycle is a DISTINCT
//!    observable fact by fingerprint). The phase is off by default so the T-109 execution-only
//!    baseline (which stops at `in-review`) is unchanged.
//! 5. **Join barrier (opt-in, `--join`, implies `--review`; task T-243).** After the review round
//!    promotes the ready tasks, drive `agents/processor.md` phases 4–6 in the SAME hermetic
//!    sandbox: enter the join (integration `none -> in-progress`, `cohort.join_started`), merge the
//!    ready branches sequentially through a supervised `merger` stand-in whose `merge_report.md`
//!    the engine decides off ([`crate::contract::parse_merge_report`]) — `ready -> merged` per
//!    merged branch, `ready -> conflict` + `queue-tx return` per quarantined one — then run the
//!    bounded integration review cycle (a supervised `full_reviewer` stand-in + [`integration_gate`]
//!    over `F-`/`SUMMARY-F`, capped by `INTEGRATION_LOOP_MAX` via the SAME [`review_cycle_decision`]
//!    as the per-task cycle). A converged review publishes (integration `reviewed -> published`,
//!    each task `merged -> published`, `cohort.published`) and archives (`published -> done`,
//!    `queue-tx archive`, integration `published -> cleaned`); a non-converged one STOPS unpublished
//!    (integration `-> failed`, tasks stay `merged`). One `cohort.closed` terminally processes the
//!    cohort in every case. Every integration transition is validated through
//!    `state-tx check-transition --kind integration` before it is written; the merge/publish is
//!    hermetically SIMULATED (the sandbox `.work` has no repository — no live VCS mutation). Off by
//!    default so the T-127/T-128 review baseline (which stops at `ready`) is unchanged.
//!
//! **Boundaries this module keeps.** It touches ONLY the `.work` passed as `--work` (the run
//! subcommand has NO default work dir, so it can never silently resolve the repo's live `.work`);
//! it CALLS `tools/*.ps1` as they are and never edits them; every mutation of the queue / lease /
//! outbox goes through those transactional tools; and it is not wired into `agents/processor.md`
//! or any launcher. It is exercised by `engine run --once` and the hermetic `run_fixture` /
//! `review_fixture` / `join_fixture` e2e tests.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;

use crate::claude::{ClaudeCall, PermissionPosture};
use crate::codex::{CodexCall, Sandbox};
use crate::contract::{
    MergeOutcome, parse_changed_files, parse_merge_report, parse_outcome, parse_review,
};
use crate::lease::{self, AcquireVerdict};
use crate::resolvers::{
    ActiveClass, ActiveTask, AdmissionOutcome, BaseReviewer, Candidate, CloseReason, CoderRoute,
    CoderRouteInput, CodexCoder, CodexReviewer, CycleDecision, Domain, ImplBy, Level, ReviewGate,
    ReviewerRoute, base_reviewer, integration_gate, is_ready, plan_admission, reelect_reviewer,
    review_cycle_decision, review_gate, route_coder,
};
use crate::state::{
    Snapshot, TaskState, archive_header_task_id, now_epoch_secs, try_completed_ids,
};
use crate::supervise::{self, Reason, SpawnSpec};
use crate::time::epoch_to_iso;

/// Engine-side exit codes for `engine run` (a small, documented vocabulary).
pub mod exit {
    /// Success: the run completed (a cohort was opened and one round ran, or there was nothing
    /// admissible — an idle run is not an error).
    pub const OK: i32 = 0;
    /// A tool / supervision failure (a `queue-tx`/`state-tx`/`outbox` call failed, a leaf could
    /// not be supervised, or a descriptor could not be written). The lease is released first.
    pub const FAILED: i32 = 1;
    /// Usage / argument error.
    pub const USAGE: i32 = 2;
    /// Refused: a live foreign lease (a running `processor`) holds `.work/orchestrator.lock`, or
    /// the lock is legacy/corrupt. A clean refusal — the engine never forces its way in.
    pub const LEASE_REFUSED: i32 = 3;
}

/// A structured, non-panicking run failure: an engine exit code plus a human diagnostic.
#[derive(Debug, Clone)]
pub struct RunError {
    pub code: i32,
    pub message: String,
}

impl RunError {
    fn new(code: i32, message: impl Into<String>) -> RunError {
        RunError {
            code,
            message: message.into(),
        }
    }
}

/// Everything `run --once` needs, resolved from the CLI by the caller.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// The **sandbox** `.work` directory to orchestrate over (required — no default).
    pub work: PathBuf,
    /// The project root recorded in the lease (defaults to the work dir's parent).
    pub root: PathBuf,
    /// The directory holding `state-tx.ps1` / `queue-tx.ps1` / `outbox.ps1` (defaults to
    /// `<root>/tools`).
    pub tools: PathBuf,
    /// This binary's path, so the engine can spawn itself as the deterministic `__fake-agent`.
    pub self_exe: String,
    /// The batch id to open (defaults to a fresh `B-<epoch>`).
    pub batch_id: String,
    /// The base ref recorded in `batch.md`.
    pub base: String,
    /// Admission capacity for this cohort (`COHORT_SIZE`).
    pub cohort_size: usize,
    /// Whether the reviewer tiering resolver is on (`REVIEWER_TIERING`, default true).
    pub reviewer_tiering: bool,
    /// The lease TTL in seconds.
    pub ttl_secs: u64,
    /// Optional task id whose leaf call emits an `эскалация` verdict — deterministic
    /// fault-injection for the escalation branch of the round.
    pub inject_escalate: Option<String>,
    /// Whether the per-task review round (phases 2.4–2.6) runs after the execution round. Off by
    /// default so the T-109 execution-only baseline (which stops at `in-review`) is unchanged.
    pub review: bool,
    /// Optional task id whose reviewer stand-in writes an open `R-` finding instead of a fresh
    /// clean `SUMMARY-R` — deterministic fault-injection for the with-findings review branch.
    pub inject_findings: Option<String>,
    /// The review-cycle limit (`REVIEW_LOOP_MAX`, default 8): how many review→fix→re-review cycles a
    /// single task may run before the fix cycle escalates it `не сходится ревью после N циклов`.
    pub review_loop_max: u32,
    /// Optional convergence point for the `inject_findings` task: its reviewer stand-in yields
    /// findings while `cycle < converge_after_cycles` and a clean pass from that cycle onward — the
    /// deterministic "the fix worked at cycle N" knob for the CONVERGING branch. `None` = the fix
    /// never converges, so persistent findings drive the DIVERGING (escalation) branch.
    pub converge_after_cycles: Option<u32>,
    /// Whether the join-barrier segment (phases 4–6: sequential merge into `_integration`, the
    /// bounded integration review cycle, ff-merge/publication and archival) runs after the review
    /// round. Off by default (implies `--review`), so the T-127/T-128 review-only baseline (which
    /// stops at `готова к слиянию`) is unchanged. Every step is hermetic — deterministic
    /// `__fake-agent` stand-ins for the `merger` and `full_reviewer`, no live model call and no real
    /// VCS mutation (the sandbox `.work` has no repository).
    pub join: bool,
    /// The integration review-cycle limit (`INTEGRATION_LOOP_MAX`, default 8): how many
    /// integration review→fix→re-review cycles the join barrier may run before it stops WITHOUT
    /// publishing (batch left unpublished, integration state `failed`).
    pub integration_loop_max: u32,
    /// Optional task id whose branch the merger stand-in quarantines (`quarantined=…`) instead of
    /// merging — deterministic fault-injection for the merge/quarantine decision branch.
    pub inject_merge_conflict: Option<String>,
    /// Whether the `full_reviewer` stand-in yields an open integration `F-` finding instead of a
    /// fresh clean `SUMMARY-F` — deterministic fault-injection for the with-findings integration
    /// review branch.
    pub inject_f_findings: bool,
    /// Optional convergence point for `inject_f_findings`: the integration review yields findings
    /// while `cycle < integration_converge_after` and a clean pass from that cycle onward. `None`
    /// with `inject_f_findings` = never converges → drives the `INTEGRATION_LOOP_MAX` stop branch.
    pub integration_converge_after: Option<u32>,
    /// **Live mode** (opt-in, `--live`, task T-244). When set, every supervised leaf round spawns
    /// the REAL model child — `claude -p --output-format stream-json` (via [`crate::claude`]) or
    /// `codex exec` (via [`crate::codex`]) — with its permission posture stated explicitly on its
    /// OWN argv (the T-057 lesson: consent lives in the context of the call, never inherited).
    /// Off by default: the round drives the deterministic offline `__fake-agent` stand-in exactly
    /// as before, so the hermetic tests / CI stay token-free and the fake baseline is unchanged.
    /// Only the SPAWN TARGET changes — supervision (deadline/drain/tree-kill via [`supervise`]) and
    /// every transactional mutation (queue-tx/state-tx/outbox, K-006) are identical to fake mode.
    pub live: bool,
    /// `CODEX_CODER` maker-routing flag (default `off`). Fed to the [`route_coder`] resolver so a
    /// LIVE coder/fix leaf is routed to the `codex exec` maker when the operator opts in
    /// (`--codex-coder`); `off` keeps the leaf on Claude (the sandbox's Claude-only default). Inert
    /// in fake mode — the offline stand-in is spawned regardless of the routed executor.
    pub codex_coder: CodexCoder,
    /// `CODEX_NETWORK` availability, fed to [`route_coder`]'s network gate (default off). Only
    /// consulted when `codex_coder` routes a live leaf toward Codex.
    pub codex_network: bool,
    /// Wall-clock bound on one supervised leaf call.
    pub leaf_deadline: Duration,
}

impl RunConfig {
    fn state_tx(&self) -> PathBuf {
        self.tools.join("state-tx.ps1")
    }
    fn queue_tx(&self) -> PathBuf {
        self.tools.join("queue-tx.ps1")
    }
    fn outbox(&self) -> PathBuf {
        self.tools.join("outbox.ps1")
    }
}

// --- leaf spawn: the ONE place the live vs offline switch lives (task T-244) ---------------

/// Which real model backs a LIVE leaf call. Selects how the child's output is read back: a
/// `claude -p` child speaks stream-json (the `result` event carries the report); a `codex exec`
/// child streams plain text whose final agent message is the report (both end with the T-111
/// `ИТОГ:` line — the contract covers the Codex variants too).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Executor {
    Claude,
    Codex,
}

/// One supervised leaf invocation, describing BOTH the real child (`program`/`argv`/`stdin`, run
/// only under `--live`) and the deterministic `__fake-agent` stand-in (`fake_args`, the offline
/// default). Every leaf call in the run loop routes through this one shape so the two paths can
/// never diverge in supervision: [`leaf_spec`] picks the spawn target, but the deadline / output
/// drain / tree-kill always come from the SAME [`supervise::run`], live or offline.
struct LeafPlan {
    /// The real child program for a live run (`"claude"` or `"codex"`); ignored offline.
    program: &'static str,
    /// The real child argv (the headless `claude`/`codex` command); used only live.
    argv: Vec<String>,
    /// The real child's stdin (the codex prompt; empty for claude, whose prompt is on argv).
    stdin: String,
    /// How to read the live child's output back.
    executor: Executor,
    /// The `__fake-agent` argv (after the engine's own path) for the offline default run.
    fake_args: Vec<String>,
}

/// The default wall-clock budget for one **offline** `__fake-agent` leaf. The stand-in finishes in
/// well under a second, so the fake baseline keeps its historical 60s cap unchanged.
pub const LEAF_DEADLINE_FAKE_SECS: u64 = 60;

/// The default wall-clock budget for one **live** (`--live`) leaf. A real `claude -p`/`codex exec`
/// child that implements or reviews a whole task in a worktree runs minutes, not seconds, so the
/// live default is a much larger budget — the 60s fake cap would otherwise time-out→tree-kill every
/// real leaf into a `supervised_ok=false` escalation (T-244 R-01), while also burning partial model
/// spend on each doomed call.
pub const LEAF_DEADLINE_LIVE_SECS: u64 = 1800;

/// Resolve the wall-clock budget for one supervised leaf. Fake and live get **separate** defaults
/// ([`LEAF_DEADLINE_FAKE_SECS`] vs [`LEAF_DEADLINE_LIVE_SECS`]) so the offline baseline is untouched
/// while a real leaf gets a budget adequate for a headless call; an explicit `--leaf-deadline <sec>`
/// override (clamped to ≥1s) wins for either mode, so an operator can retune without a rebuild. Pure
/// and total, so the fake-vs-live default and the clamp are unit-testable (T-244 R-01).
pub fn resolve_leaf_deadline(override_secs: Option<u64>, live: bool) -> Duration {
    let secs = override_secs.map(|s| s.max(1)).unwrap_or(if live {
        LEAF_DEADLINE_LIVE_SECS
    } else {
        LEAF_DEADLINE_FAKE_SECS
    });
    Duration::from_secs(secs)
}

/// Build the [`SpawnSpec`] for one leaf: the REAL child under `--live`, the deterministic
/// `__fake-agent` stand-in otherwise. The deadline is identical either way — only the spawn
/// target differs. Pure and total so the live/offline switch is unit-testable without spawning.
fn leaf_spec(live: bool, self_exe: &str, deadline: Duration, plan: &LeafPlan) -> SpawnSpec {
    let spec = if live {
        SpawnSpec::new(plan.program, plan.argv.clone()).stdin(plan.stdin.clone())
    } else {
        SpawnSpec::new(self_exe, plan.fake_args.clone())
    };
    spec.deadline(Some(deadline))
}

/// The Claude model for a coder/fix leaf of the given executor level — the SINGLE source of the
/// level→model mapping (the live path never re-hardcodes it): `coder_deep` gets Opus, the
/// shallower levels Sonnet. Keyed off the resolver's typed [`Level`], never re-derived inline.
fn claude_model_for_level(level: Level) -> &'static str {
    match level {
        Level::CoderDeep => "opus",
        Level::Coder | Level::CoderFast => "sonnet",
    }
}

/// The Claude model for a reviewer leaf named by the tiering/re-election resolvers: the cheaper
/// Sonnet for `reviewer_std`, Opus for the base `reviewer` (the sandbox is Claude-only, so the
/// name is never `reviewer_codex` at runtime — the mapping stays keyed off the resolver output).
fn claude_reviewer_model(reviewer_name: &str) -> &'static str {
    if reviewer_name == "reviewer_std" {
        "sonnet"
    } else {
        "opus"
    }
}

/// The explicit tool allowlist for a live Claude leaf working in a hermetic worktree. Enumerated
/// (`--permission-mode acceptEdits` + `--allowedTools`, not blanket `bypassPermissions`) so the
/// posture is auditable ON THE CALL'S OWN ARGV — the engine states what the child may do on the
/// very command it runs, never inheriting consent from a parent context (the T-057 lesson).
fn leaf_allowed_tools() -> Vec<String> {
    ["Read", "Edit", "Write", "Bash", "Grep", "Glob"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// The Codex reasoning effort / model for a live Codex maker leaf (mirrors the `coder_codex`
/// default). Kept as a role constant, not a level mapping — Codex is the opt-in maker accelerator.
const CODEX_CODER_MODEL: &str = "gpt-5-codex";

/// Route a live coder/fix leaf to its real executor THROUGH the [`route_coder`] resolver
/// (coder.rs) — never a duplicated inline choice. `off` (the default) keeps it on the Claude
/// coder of its level with an explicit permission posture on argv; an opted-in `CODEX_CODER`
/// routes it to the fail-closed `codex exec` maker (prompt on stdin). Pure over its inputs, so the
/// routing→executor mapping is unit-testable without spawning either child.
fn plan_coder_executor(
    codex_coder: CodexCoder,
    codex_network: bool,
    level: Level,
    prompt: &str,
    worktree: &Path,
) -> (&'static str, Vec<String>, String, Executor) {
    let route = route_coder(&CoderRouteInput {
        codex_coder,
        level,
        codex_network,
        // The sandbox does not parse the descriptor's `Сеть:`/`Экосистема:` or KB pitfalls into the
        // gate here (mirroring the run loop's existing simplifications); the level×flag stage is
        // what selects the maker. Absent inputs read as "no extra gate", the pre-T-064 behavior.
        network: None,
        kb_pitfall: None,
    });
    match route {
        CoderRoute::Codex => {
            let mut c = CodexCall::new(
                worktree.to_string_lossy().into_owned(),
                Sandbox::WorkspaceWrite,
            );
            c.model = Some(CODEX_CODER_MODEL.to_string());
            // The engine's worktree is a real git worktree in live mode; in a repo-less sandbox the
            // check would abort, so skip it — the fail-closed `--sandbox`/`approval_policy=never`
            // contract still holds either way (T-069).
            c.skip_git_repo_check = true;
            // Propagate the resolved network posture onto the argv (T-063/T-283): the same
            // `codex_network` gate that `route_coder` consulted above also flips codex's
            // outbound-network + openssl git-TLS overrides, keeping the engine argv in parity
            // with tools/codex-runtime.ps1 `-Network on`.
            c.network = codex_network;
            ("codex", c.to_argv(), prompt.to_string(), Executor::Codex)
        }
        CoderRoute::Claude(_) => {
            let mut c = ClaudeCall::new(prompt.to_string());
            c.model = Some(claude_model_for_level(level).to_string());
            c.max_turns = Some(40);
            c.allowed_tools = leaf_allowed_tools();
            c.posture = PermissionPosture::Allowlisted;
            ("claude", c.to_argv(), String::new(), Executor::Claude)
        }
    }
}

/// The distilled body + supervision verdict of one leaf's output.
struct LeafReport {
    /// The report text scanned for `ИТОГ:` / `Изменённые файлы:` (contract.rs).
    text: String,
    /// Whether the child was supervised cleanly (exit 0, and for a stream-json child no `is_error`).
    supervised_ok: bool,
}

/// The outcome of driving one task through the round.
#[derive(Debug, Clone)]
pub struct TaskOutcome {
    pub id: String,
    /// The executor level the task was planned for (default `coder` in the sandbox).
    pub level: Level,
    /// The base Claude reviewer tier the T-105 tiering resolver picks for it.
    pub reviewer: BaseReviewer,
    /// The parsed terminal `ИТОГ:` verdict from the leaf report (empty if none).
    pub verdict: String,
    /// The supervised leaf-call stop reason (`ok` / `timeout` / …).
    pub supervised: &'static str,
    /// The descriptor state it started the round in (`working`).
    pub from: TaskState,
    /// The descriptor state it ended the round in (`in-review` on success, else `escalated`).
    pub to: TaskState,
    /// The files the leaf reported changing (`Изменённые файлы:`), if any.
    pub changed_files: Vec<String>,
    /// The per-task review-round outcome, when the review phase (`--review`) ran for this task
    /// (only tasks that reached `in-review` after execution are reviewed).
    pub review: Option<ReviewOutcome>,
}

/// The outcome of driving one `in-review` task through the phase-2.4…2.6 review round.
#[derive(Debug, Clone)]
pub struct ReviewOutcome {
    /// The concrete reviewer the tiering + Codex-routing resolvers elected (e.g. `reviewer`).
    pub reviewer: &'static str,
    /// The clean / findings / incomplete gate branch [`review_gate`] named.
    pub gate: &'static str,
    /// The supervised reviewer-leaf stop reason (`ok` / `timeout` / …) of the LAST review pass.
    pub supervised: &'static str,
    /// The descriptor state the review fix cycle finally transitioned to: `ready` on a converging
    /// clean pass, `escalated` on exhausting `REVIEW_LOOP_MAX` (or a fail-closed supervision
    /// failure). The cycle never leaves a task stuck at `in-review`.
    pub to: TaskState,
    /// How many review cycles ran (the terminal `Циклов-ревью: N` value): the cycle a converging
    /// clean pass landed on, or the number of cycles completed before the budget escalation.
    pub cycles: u32,
}

/// The outcome of the join-barrier segment (phases 4–6), when it ran (`--join`).
#[derive(Debug, Clone)]
pub struct JoinReport {
    /// The tasks that entered the join (`готова к слиянию` after the review round).
    pub ready: Vec<String>,
    /// The tasks the merger stand-in merged clean into the integration branch.
    pub merged: Vec<String>,
    /// The tasks the merger stand-in quarantined (`quarantined=…`) — not merged.
    pub quarantined: Vec<String>,
    /// The merged tasks that were published (ff-merged into main) and archived (`выполнена`).
    pub published: Vec<String>,
    /// The terminal integration state (§13.3 canonical name): `cleaned` on a fully published +
    /// archived batch, `failed` if the integration review did not converge (batch unpublished),
    /// `none` if the join ran but nothing was ready to merge.
    pub integration: &'static str,
    /// How many integration review cycles ran before the gate resolved.
    pub integration_cycles: u32,
}

/// The report of one `run --once`.
#[derive(Debug, Clone)]
pub struct RunReport {
    pub owner: String,
    pub batch_id: String,
    pub admitted: Vec<String>,
    /// Set when nothing was admitted: the planner/close reason.
    pub idle_reason: Option<String>,
    pub tasks: Vec<TaskOutcome>,
    /// The join-barrier outcome, when the join segment (`--join`) ran.
    pub join: Option<JoinReport>,
    pub events_appended: usize,
    pub lease_released: bool,
}

impl RunReport {
    /// Render the report as a human-readable block (ends with a newline).
    pub fn to_human(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "Engine run --once (owner={})", self.owner);
        if let Some(r) = &self.idle_reason {
            let _ = writeln!(s, "Cohort: nothing admitted · причина={r}");
        } else {
            let _ = writeln!(
                s,
                "Cohort: {} opened · admitted={} [{}]",
                self.batch_id,
                self.admitted.len(),
                self.admitted.join(", ")
            );
        }
        for t in &self.tasks {
            let _ = writeln!(
                s,
                "  {} · level={} · reviewer={} · leaf={} · verdict={} · {} -> {}",
                t.id,
                t.level.as_str(),
                t.reviewer.as_str(),
                t.supervised,
                if t.verdict.is_empty() {
                    "(none)"
                } else {
                    &t.verdict
                },
                t.from.as_str(),
                t.to.as_str(),
            );
            if let Some(r) = &t.review {
                let _ = writeln!(
                    s,
                    "      review · reviewer={} · gate={} · leaf={} · cycles={} · {} -> {}",
                    r.reviewer,
                    r.gate,
                    r.supervised,
                    r.cycles,
                    t.to.as_str(),
                    r.to.as_str(),
                );
            }
        }
        if let Some(j) = &self.join {
            let _ = writeln!(
                s,
                "Join barrier · integration={} · ready=[{}] · merged=[{}] · quarantined=[{}] · published=[{}] · integration_cycles={}",
                j.integration,
                j.ready.join(", "),
                j.merged.join(", "),
                j.quarantined.join(", "),
                j.published.join(", "),
                j.integration_cycles,
            );
        }
        let _ = writeln!(
            s,
            "Events appended: {} · lease released: {}",
            self.events_appended, self.lease_released
        );
        s
    }

    /// Render the report as one compact JSON object (stable field order).
    pub fn to_json(&self) -> String {
        json!({
            "owner": self.owner,
            "batch_id": self.batch_id,
            "admitted": self.admitted,
            "idle_reason": self.idle_reason,
            "events_appended": self.events_appended,
            "lease_released": self.lease_released,
            "join": self.join.as_ref().map(|j| json!({
                "integration": j.integration,
                "ready": j.ready,
                "merged": j.merged,
                "quarantined": j.quarantined,
                "published": j.published,
                "integration_cycles": j.integration_cycles,
            })),
            "tasks": self.tasks.iter().map(|t| json!({
                "id": t.id,
                "level": t.level.as_str(),
                "reviewer": t.reviewer.as_str(),
                "verdict": t.verdict,
                "supervised": t.supervised,
                "from": t.from.as_str(),
                "to": t.to.as_str(),
                "changed_files": t.changed_files,
                "review": t.review.as_ref().map(|r| json!({
                    "reviewer": r.reviewer,
                    "gate": r.gate,
                    "supervised": r.supervised,
                    "to": r.to.as_str(),
                    "cycles": r.cycles,
                })),
            })).collect::<Vec<_>>(),
        })
        .to_string()
    }
}

// ---------------------------------------------------------------------------------------
// Pure helpers (unit-tested) — the byte-level Markdown the engine writes and the small
// decisions the round makes. Kept pure so a test pins them without any tool call.
// ---------------------------------------------------------------------------------------

/// The Cyrillic status literal for a canonical [`TaskState`] — the inverse of
/// `TaskState::from_markdown`, used to write the descriptor `Статус:` line and the outbox
/// event `--from`/`--to` coordinates, byte-for-byte with §13.1.
fn task_literal(state: TaskState) -> &'static str {
    match state {
        TaskState::NotStarted => "не начата",
        TaskState::Working => "в работе",
        TaskState::InReview => "на ревью",
        TaskState::Ready => "готова к слиянию",
        TaskState::Merged => "слита",
        TaskState::Published => "опубликована",
        TaskState::Done => "выполнена",
        TaskState::Escalated => "эскалирована",
        TaskState::Conflict => "конфликт",
    }
}

/// The round's per-task decision: a supervised, cleanly-`готово` leaf advances `working ->
/// in-review`; anything else (an `эскалация` verdict, a supervision timeout/crash, or a missing
/// terminal `ИТОГ:` line) is a `working -> escalated` — a fail-closed transition, never a silent
/// "assume success".
fn decide_to_state(supervised_ok: bool, verdict: &str) -> TaskState {
    if supervised_ok && verdict == "готово" {
        TaskState::InReview
    } else {
        TaskState::Escalated
    }
}

/// The review round's per-task decision over the [`review_gate`] branch (phases 2.6/2.7/2.8): a
/// supervised **clean** pass promotes `in-review -> ready`; **findings** and an **incomplete**
/// pass both keep the task `in-review` for the fix cycle (the T-128 successor owns the loop); a
/// reviewer that could not be supervised is a fail-closed `in-review -> escalated`, never a
/// silent "assume clean".
fn decide_review_state(supervised_ok: bool, gate: ReviewGate) -> TaskState {
    if !supervised_ok {
        return TaskState::Escalated;
    }
    match gate {
        ReviewGate::Clean => TaskState::Ready,
        ReviewGate::Findings | ReviewGate::Incomplete => TaskState::InReview,
    }
}

/// The stable label of a [`review_gate`] branch for the report / event payload.
fn review_gate_label(gate: ReviewGate) -> &'static str {
    match gate {
        ReviewGate::Clean => "clean",
        ReviewGate::Findings => "findings",
        ReviewGate::Incomplete => "incomplete",
    }
}

/// The concrete reviewer name a [`ReviewerRoute`] dispatches to (the tier for a Claude route /
/// diversity augment, `reviewer_codex` for a full Codex replacement).
fn route_reviewer_name(route: ReviewerRoute) -> &'static str {
    match route {
        ReviewerRoute::Claude(base) | ReviewerRoute::Augment(base) => base.as_str(),
        ReviewerRoute::CodexFull => "reviewer_codex",
    }
}

/// The cohort-admission close reason for a `--once` run that admitted `admitted` of a `capacity`
/// window: filling the window closes on `COHORT_SIZE`; admitting fewer (the queue ran dry of
/// admissible candidates) closes on `очередь-пуста` (§13.2 vocabulary).
fn close_reason(admitted: usize, capacity: usize) -> CloseReason {
    if capacity > 0 && admitted >= capacity {
        CloseReason::CohortSize
    } else {
        CloseReason::QueueEmpty
    }
}

/// Render the descriptor `task.md` the engine writes on capture / on a status transition. When the
/// task is inside the review fix cycle, `review_cycles` carries the current `Циклов-ревью: N` value
/// (written as an ADDITIVE trailing field so the `Статус:` / `Предпосылки:` / `Конфликт-домен:`
/// bytes are byte-for-byte unchanged; capture and the execution round pass `None`, so a
/// pre-review descriptor never grows the field).
fn descriptor_md(
    id: &str,
    state: TaskState,
    batch: &str,
    prerequisites: &[String],
    conflict_domain: Option<&[String]>,
    review_cycles: Option<u32>,
) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# {id}");
    let _ = writeln!(s, "Статус: {}", task_literal(state));
    let _ = writeln!(s, "Ветка: task/{id}");
    let _ = writeln!(s, "Батч: {batch}");
    if !prerequisites.is_empty() {
        let _ = writeln!(s, "Предпосылки: {}", prerequisites.join(", "));
    }
    if let Some(domain) = conflict_domain {
        let _ = writeln!(s, "Конфликт-домен: {}", domain.join(", "));
    }
    if let Some(n) = review_cycles {
        let _ = writeln!(s, "Циклов-ревью: {n}");
    }
    s
}

/// Render `batch.md` (the §13 manifest the snapshot / TUI read back).
fn batch_md(batch: &str, base: &str, tasks: &[String]) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# Batch {batch}");
    let _ = writeln!(s, "База: {base}");
    let _ = writeln!(s, "Интеграционная ветка: integration/{batch}");
    let _ = writeln!(s);
    let _ = writeln!(s, "## Задачи");
    for id in tasks {
        let _ = writeln!(
            s,
            "- [{id}] уровень=coder ветка=task/{id} worktree=.work/worktrees/{id} волна=1"
        );
    }
    s
}

/// Render `cohort_state.md` (`Приём:` admission state, §13.2).
fn cohort_md(batch: &str, admission_literal: &str, admitted: usize) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# Cohort state — Batch {batch}");
    let _ = writeln!(s, "Приём: {admission_literal}");
    let _ = writeln!(s, "Волна: 1");
    let _ = writeln!(s, "Admitted всего: {admitted}");
    s
}

/// Render `integration_state.md` (§13.3 join bookkeeping): `Ревью-SHA:` (the integration tip the
/// reviewer saw) and `F-циклов:` (the integration review-cycle counter). In the hermetic sandbox
/// the SHA is a fixed placeholder (there is no real repository); the counter is load-bearing.
fn integration_state_md(batch: &str, f_cycles: u32) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# Integration state — Batch {batch}");
    let _ = writeln!(s, "Ревью-SHA: sandbox-integration-tip");
    let _ = writeln!(s, "F-циклов: {f_cycles}");
    s
}

/// The current wall clock as an ISO-8601 UTC `date -u` mark — the freshness cutoff the review
/// round records JUST BEFORE a reviewer call (a clean `SUMMARY-R` must be newer than this).
fn now_utc_iso() -> String {
    epoch_to_iso(now_epoch_secs())
}

/// The review gate observes the child after it exits, but this legacy runner's wall clock has only
/// whole-second precision. Let a real same-second millisecond summary fit within the observed
/// second without accepting arbitrary future timestamps.
fn review_window_end() -> String {
    let second = now_utc_iso();
    format!("{}.999Z", second.trim_end_matches('Z'))
}

/// A fresh, sandbox-only batch id when the caller did not pin one.
pub fn default_batch_id() -> String {
    format!("B-{}", now_epoch_secs())
}

/// The first non-empty trimmed stderr line — a tool's refusal diagnostic.
fn first_line(text: &str) -> &str {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("(no diagnostic)")
}

/// The planner's typed domain for one descriptor, when it exists and is usable.
fn descriptor_globs<'a>(snap: &'a Snapshot, id: &str) -> Option<&'a [String]> {
    snap.descriptors
        .iter()
        .find(|descriptor| descriptor.id == id)
        .and_then(|descriptor| descriptor.conflict_domain.as_deref())
}

/// Resolve a descriptor domain fail-closed: absent or malformed fields block packing with every
/// other task instead of behaving like an empty, conflict-free domain.
fn descriptor_domain(snap: &Snapshot, id: &str) -> Domain {
    descriptor_globs(snap, id)
        .map(Domain::from_globs)
        .unwrap_or_else(Domain::unknown)
}

/// A task's prerequisite ids from the queue snapshot (the descriptor the engine writes on capture
/// and on each transition carries them forward).
fn prereqs_of(snap: &Snapshot, id: &str) -> Vec<String> {
    snap.queue
        .iter()
        .find(|e| e.id == id)
        .map(|e| e.prerequisites.clone())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------------------
// The orchestrator. `run_once` acquires the lease, runs the body, and ALWAYS releases the
// lease (success or error) so no owner lease is left dangling in the sandbox.
// ---------------------------------------------------------------------------------------

/// Drive ONE cohort/phase end-to-end over the sandbox `.work` in `cfg.work`.
pub fn run_once(cfg: &RunConfig) -> Result<RunReport, RunError> {
    if !cfg.work.is_dir() {
        return Err(RunError::new(
            exit::USAGE,
            format!("work directory not found: {}", cfg.work.display()),
        ));
    }
    for (name, path) in [
        ("state-tx.ps1", cfg.state_tx()),
        ("queue-tx.ps1", cfg.queue_tx()),
        ("outbox.ps1", cfg.outbox()),
    ] {
        if !path.exists() {
            return Err(RunError::new(
                exit::USAGE,
                format!(
                    "{name} not found at {} (pass --tools <dir> or --root <project root>)",
                    path.display()
                ),
            ));
        }
    }

    let mut r = Runner::new(cfg);
    let owner = r.acquire_lease()?;
    r.owner = owner.clone();

    // Run the body; whatever happens, release the lease afterwards.
    let body = r.run_body();
    let released = r.release_lease().unwrap_or(false);

    match body {
        Ok(mut report) => {
            report.owner = owner;
            report.events_appended = r.events;
            report.lease_released = released;
            Ok(report)
        }
        Err(mut e) => {
            if !released {
                e.message
                    .push_str(" (warning: lease could not be released cleanly)");
            }
            Err(e)
        }
    }
}

/// Mutable orchestration state for one run.
struct Runner<'a> {
    cfg: &'a RunConfig,
    work_s: String,
    root_s: String,
    owner: String,
    events: usize,
}

impl<'a> Runner<'a> {
    fn new(cfg: &'a RunConfig) -> Runner<'a> {
        Runner {
            cfg,
            work_s: cfg.work.to_string_lossy().into_owned(),
            root_s: cfg.root.to_string_lossy().into_owned(),
            owner: String::new(),
            events: 0,
        }
    }

    // --- tool spawning -----------------------------------------------------------------

    /// Run a `.ps1` tool under supervision and require a clean exit 0; otherwise a `RunError`.
    fn tool_ok(
        &self,
        script: &Path,
        args: &[String],
        ctx: &str,
    ) -> Result<supervise::Verdict, RunError> {
        let s = script.to_string_lossy().into_owned();
        let v = lease::run_state_tx(&s, args, lease::STATE_TX_DEADLINE)
            .map_err(|e| RunError::new(exit::FAILED, format!("{ctx}: {e}")))?;
        match v.exit_code {
            Some(0) => Ok(v),
            Some(code) => Err(RunError::new(
                exit::FAILED,
                format!(
                    "{ctx}: {} exit {code} — {}",
                    script.display(),
                    first_line(&v.stderr)
                ),
            )),
            None => Err(RunError::new(
                exit::FAILED,
                format!(
                    "{ctx}: {} did not complete ({}) — {}",
                    script.display(),
                    v.reason.as_str(),
                    v.outcome_reason
                ),
            )),
        }
    }

    // --- lease -------------------------------------------------------------------------

    /// Take the engine's owner lease via `state-tx.ps1` (role `engine`): acquire a free lock, or
    /// adopt a provably-stale one via the liveness-gated `takeover` (never `--force`). A live
    /// foreign lease, a legacy lock, or a corrupt record is a CLEAN refusal, not a crash.
    fn acquire_lease(&mut self) -> Result<String, RunError> {
        let script = self.cfg.state_tx();
        let s = script.to_string_lossy().into_owned();
        let ttl = self.cfg.ttl_secs.to_string();
        let argv = lease::acquire_argv(&self.work_s, &self.root_s, Some(&ttl), None, None, None);
        let v = lease::run_state_tx(&s, &argv, lease::STATE_TX_DEADLINE)
            .map_err(|e| RunError::new(exit::FAILED, format!("lease acquire: {e}")))?;
        let code = v.exit_code.ok_or_else(|| {
            RunError::new(
                exit::FAILED,
                format!(
                    "lease acquire: state-tx did not complete ({})",
                    v.reason.as_str()
                ),
            )
        })?;
        match lease::acquire_verdict(code) {
            AcquireVerdict::Acquired => Self::owner_from(&v.stdout),
            AcquireVerdict::Stale => {
                // A provably-stale lease: adopt it via the liveness-gated takeover.
                let targv = lease::takeover_argv(&self.work_s, &self.root_s, Some(&ttl), None);
                let tv = lease::run_state_tx(&s, &targv, lease::STATE_TX_DEADLINE)
                    .map_err(|e| RunError::new(exit::FAILED, format!("lease takeover: {e}")))?;
                let tcode = tv.exit_code.ok_or_else(|| {
                    RunError::new(
                        exit::FAILED,
                        format!(
                            "lease takeover: state-tx did not complete ({})",
                            tv.reason.as_str()
                        ),
                    )
                })?;
                match lease::takeover_verdict(tcode) {
                    AcquireVerdict::Acquired => Self::owner_from(&tv.stdout),
                    other => Err(Self::refusal(other, &tv.stderr)),
                }
            }
            other => Err(Self::refusal(other, &v.stderr)),
        }
    }

    fn owner_from(stdout: &str) -> Result<String, RunError> {
        lease::extract_owner(stdout).ok_or_else(|| {
            RunError::new(
                exit::FAILED,
                "lease acquired but owner id was not parseable from state-tx output".to_string(),
            )
        })
    }

    fn refusal(v: AcquireVerdict, stderr: &str) -> RunError {
        let code = match v {
            AcquireVerdict::HeldLive | AcquireVerdict::Legacy | AcquireVerdict::Corrupt => {
                exit::LEASE_REFUSED
            }
            AcquireVerdict::Usage => exit::USAGE,
            _ => exit::FAILED,
        };
        RunError::new(
            code,
            format!(
                "lease refused (a live/legacy/corrupt lock is present) — {}",
                first_line(stderr)
            ),
        )
    }

    /// Renew the engine's own lease at a step boundary. A heartbeat failure is a hard error (a
    /// lost lease means we can no longer safely mutate the sandbox).
    fn heartbeat(&self) -> Result<(), RunError> {
        let argv = lease::heartbeat_argv(&self.work_s, &self.owner);
        self.tool_ok(&self.cfg.state_tx(), &argv, "lease heartbeat")?;
        Ok(())
    }

    /// Release the engine's own owner lease (owner-checked; never `--force`). Best-effort:
    /// returns whether it was released.
    fn release_lease(&self) -> Result<bool, RunError> {
        if self.owner.is_empty() {
            return Ok(false);
        }
        let argv = lease::release_argv(&self.work_s, &self.owner);
        match self.tool_ok(&self.cfg.state_tx(), &argv, "lease release") {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    // --- state-tx transition validation ------------------------------------------------

    /// Validate a lifecycle transition through `state-tx check-transition` (read-only). The
    /// engine validates BEFORE it writes the descriptor / cohort file, exactly as the processor
    /// does — a rejected transition aborts the run rather than writing an illegal state.
    fn check_transition(&self, kind: &str, from: &str, to: &str) -> Result<(), RunError> {
        let argv = vec![
            "check-transition".into(),
            "--kind".into(),
            kind.into(),
            "--from".into(),
            from.into(),
            "--to".into(),
            to.into(),
        ];
        self.tool_ok(
            &self.cfg.state_tx(),
            &argv,
            &format!("check-transition {kind} {from}->{to}"),
        )?;
        Ok(())
    }

    // --- outbox -----------------------------------------------------------------------

    /// Append one event through `outbox.ps1`, owner-bound (single-writer invariant, §19).
    fn emit(&mut self, args: &[&str], ctx: &str) -> Result<(), RunError> {
        let mut a: Vec<String> = vec![
            "append".into(),
            "--work".into(),
            self.work_s.clone(),
            "--owner".into(),
            self.owner.clone(),
        ];
        a.extend(args.iter().map(|s| s.to_string()));
        self.tool_ok(&self.cfg.outbox(), &a, ctx)?;
        self.events += 1;
        Ok(())
    }

    // --- descriptor / coordination-file writes -----------------------------------------

    fn write_descriptor(
        &self,
        id: &str,
        state: TaskState,
        prerequisites: &[String],
        conflict_domain: Option<&[String]>,
        review_cycles: Option<u32>,
    ) -> Result<(), RunError> {
        let dir = self.cfg.work.join("tasks").join(id);
        fs::create_dir_all(&dir)
            .map_err(|e| RunError::new(exit::FAILED, format!("create {}: {e}", dir.display())))?;
        let path = dir.join("task.md");
        fs::write(
            &path,
            descriptor_md(
                id,
                state,
                &self.cfg.batch_id,
                prerequisites,
                conflict_domain,
                review_cycles,
            ),
        )
        .map_err(|e| RunError::new(exit::FAILED, format!("write {}: {e}", path.display())))
    }

    fn write_file(&self, rel: &str, contents: &str) -> Result<(), RunError> {
        let path = self.cfg.work.join(rel);
        fs::write(&path, contents)
            .map_err(|e| RunError::new(exit::FAILED, format!("write {}: {e}", path.display())))
    }

    // --- the body ----------------------------------------------------------------------

    fn run_body(&mut self) -> Result<RunReport, RunError> {
        let snap = Snapshot::try_load(&self.cfg.work).map_err(|error| {
            RunError::new(
                exit::FAILED,
                format!(
                    "read control-plane snapshot from {}: {error}",
                    self.cfg.work.display()
                ),
            )
        })?;
        let completed = try_completed_ids(&self.cfg.work, &snap).map_err(|error| {
            RunError::new(
                exit::FAILED,
                format!(
                    "read Tasks_Done.md from {}: {error}",
                    self.cfg.work.display()
                ),
            )
        })?;

        // Candidates: every not-started queue entry, ready iff its prerequisites are complete and
        // carrying its delivery lane (§11.1 — a `next_major` entry is parked out of ordinary
        // admission). Their planner-created descriptors carry the conflict-domain. A missing or
        // malformed descriptor field is deliberately an unknown domain, which blocks packing
        // fail-closed.
        let candidates: Vec<Candidate> = snap
            .queue
            .iter()
            .filter(|e| e.state == Some(TaskState::NotStarted))
            .map(|e| Candidate {
                id: e.id.clone(),
                ready: is_ready(&e.prerequisites, &completed),
                domain: descriptor_domain(&snap, &e.id),
                delivery: e.delivery_target,
            })
            .collect();

        // Active descriptors use the same state-layer domains, so ongoing work also blocks an
        // overlapping candidate. Missing/malformed values remain conservative here as well.
        let active: Vec<ActiveTask> = snap
            .descriptors
            .iter()
            .filter_map(|d| {
                d.state
                    .and_then(ActiveClass::from_state)
                    .map(|class| ActiveTask {
                        domain: descriptor_domain(&snap, &d.id),
                        class,
                    })
            })
            .collect();

        let admitted = match plan_admission(&candidates, &active, self.cfg.cohort_size) {
            AdmissionOutcome::Admitted(ids) => ids,
            AdmissionOutcome::Empty(reason) => {
                // Nothing to do — an idle run is not an error. Release happens in `run_once`.
                return Ok(RunReport {
                    owner: self.owner.clone(),
                    batch_id: self.cfg.batch_id.clone(),
                    admitted: Vec::new(),
                    idle_reason: Some(reason.as_str().to_string()),
                    tasks: Vec::new(),
                    join: None,
                    events_appended: self.events,
                    lease_released: false,
                });
            }
        };

        // --- open the cohort ------------------------------------------------------------
        self.write_file(
            "batch.md",
            &batch_md(&self.cfg.batch_id, &self.cfg.base, &admitted),
        )?;
        self.write_file(
            "cohort_state.md",
            &cohort_md(&self.cfg.batch_id, "открыт", admitted.len()),
        )?;
        self.emit(
            &[
                "--type",
                "cohort.opened",
                "--batch-id",
                &self.cfg.batch_id,
                "--payload",
                "{\"wave\":1}",
            ],
            "emit cohort.opened",
        )?;
        self.heartbeat()?;

        // --- capture each admitted task transactionally through queue-tx ----------------
        for id in &admitted {
            let capture_argv = vec![
                "capture".into(),
                "--work".into(),
                self.work_s.clone(),
                "--id".into(),
                id.clone(),
                "--batch".into(),
                self.cfg.batch_id.clone(),
            ];
            self.tool_ok(
                &self.cfg.queue_tx(),
                &capture_argv,
                &format!("queue-tx capture {id}"),
            )?;
            self.write_descriptor(
                id,
                TaskState::Working,
                &prereqs_of(&snap, id),
                descriptor_globs(&snap, id),
                None,
            )?;
            self.emit(
                &[
                    "--type",
                    "task.captured",
                    "--batch-id",
                    &self.cfg.batch_id,
                    "--task-id",
                    id,
                    "--attempt",
                    "1",
                    "--payload",
                    "{\"level\":\"coder\"}",
                ],
                &format!("emit task.captured {id}"),
            )?;
            self.heartbeat()?;
        }

        // --- one round of execution -----------------------------------------------------
        self.emit(
            &[
                "--type",
                "cohort.round_started",
                "--batch-id",
                &self.cfg.batch_id,
                "--wave",
                "1",
                "--payload",
                "{}",
            ],
            "emit cohort.round_started",
        )?;

        let mut outcomes = Vec::new();
        for id in &admitted {
            let outcome =
                self.run_one_task(id, &prereqs_of(&snap, id), descriptor_globs(&snap, id))?;
            outcomes.push(outcome);
            self.heartbeat()?;
        }

        self.emit(
            &[
                "--type",
                "cohort.round_closed",
                "--batch-id",
                &self.cfg.batch_id,
                "--wave",
                "1",
                "--payload",
                "{}",
            ],
            "emit cohort.round_closed",
        )?;

        // --- the opt-in per-task review round (phases 2.4–2.6) --------------------------
        self.run_review_round(&snap, &mut outcomes)?;

        // --- close the cohort's admission (validated cohort transition) -----------------
        let reason = close_reason(admitted.len(), self.cfg.cohort_size);
        self.check_transition("cohort", "open", "closed")?;
        self.write_file(
            "cohort_state.md",
            &cohort_md(
                &self.cfg.batch_id,
                &format!("закрыт · причина={}", reason.as_str()),
                admitted.len(),
            ),
        )?;
        self.emit(
            &[
                "--type",
                "cohort.admission_closed",
                "--batch-id",
                &self.cfg.batch_id,
                "--payload",
                "{}",
            ],
            "emit cohort.admission_closed",
        )?;

        // --- the opt-in join barrier (phases 4–6) ---------------------------------------
        let join = self.run_join_barrier(&snap, &outcomes)?;

        Ok(RunReport {
            owner: self.owner.clone(),
            batch_id: self.cfg.batch_id.clone(),
            admitted,
            idle_reason: None,
            tasks: outcomes,
            join,
            events_appended: self.events,
            lease_released: false,
        })
    }

    // --- leaf spawning (live vs offline) -----------------------------------------------

    /// Supervise ONE leaf, live or offline, through the SAME [`supervise::run`]: [`leaf_spec`]
    /// picks the spawn target (`--live` → the real `claude`/`codex` child, else the deterministic
    /// `__fake-agent` stand-in), and the deadline / output drain / tree-kill come from the shared
    /// supervisor either way — the live and offline paths never diverge in supervision (T-244).
    fn run_leaf(&self, plan: &LeafPlan) -> supervise::Verdict {
        let spec = leaf_spec(
            self.cfg.live,
            &self.cfg.self_exe,
            self.cfg.leaf_deadline,
            plan,
        );
        supervise::run(&spec)
    }

    /// Read one leaf's output back into a [`LeafReport`]. A live Codex child streams plain text
    /// (its final agent message is the report body, `supervised_ok` == a clean exit); a Claude
    /// child or the offline stand-in speaks stream-json (the `result` event carries the report and
    /// an explicit `is_error`). The downstream `parse_outcome`/`parse_changed_files` scanners
    /// (contract.rs) read the same `ИТОГ:`/`Изменённые файлы:` markers from either body.
    fn leaf_report(&self, plan: &LeafPlan, v: &supervise::Verdict) -> LeafReport {
        if self.cfg.live && plan.executor == Executor::Codex {
            LeafReport {
                text: v.stdout.clone(),
                supervised_ok: v.reason == Reason::Ok,
            }
        } else {
            let parsed = crate::claude::parse_transcript(&v.stdout);
            LeafReport {
                text: parsed.result_text.unwrap_or_default(),
                supervised_ok: v.reason == Reason::Ok && parsed.is_error != Some(true),
            }
        }
    }

    /// Build the leaf plan for a coder / R-fix call. The live executor is routed through
    /// [`route_coder`] (coder.rs) via [`plan_coder_executor`] — Claude by default, the `codex exec`
    /// maker when `--codex-coder` opts in — while the offline stand-in is always
    /// `__fake-agent --mode leaf` with the given terminal `verdict`, so the fake baseline is
    /// unchanged regardless of the routed live executor.
    fn coder_leaf_plan(&self, id: &str, verdict: &str) -> LeafPlan {
        let worktree = self.cfg.work.join("worktrees").join(id);
        let prompt = format!(
            "Use the coder subagent to implement task {id}. Worktree={} WORK={}",
            worktree.display(),
            self.cfg.work.display()
        );
        let (program, argv, stdin, executor) = plan_coder_executor(
            self.cfg.codex_coder,
            self.cfg.codex_network,
            Level::Coder,
            &prompt,
            &worktree,
        );
        LeafPlan {
            program,
            argv,
            stdin,
            executor,
            fake_args: vec![
                "__fake-agent".into(),
                "--mode".into(),
                "leaf".into(),
                "--task".into(),
                id.to_string(),
                "--verdict".into(),
                verdict.to_string(),
            ],
        }
    }

    /// Run ONE task's round: a supervised leaf call (live `claude`/`codex` or the deterministic
    /// `__fake-agent` stand-in), contract parse, T-105 reviewer-tier decision, a validated
    /// descriptor transition, and the `task.status_changed` event.
    fn run_one_task(
        &mut self,
        id: &str,
        prerequisites: &[String],
        conflict_domain: Option<&[String]>,
    ) -> Result<TaskOutcome, RunError> {
        // The executor level in the sandbox defaults to `coder`; the T-105 tiering resolver still
        // runs over it to pick the reviewer tier (the one per-task decision derivable statically).
        let level = Level::Coder;
        let reviewer = base_reviewer(self.cfg.reviewer_tiering, level);

        // Build the leaf plan: under `--live` this carries the REAL `claude`/`codex` argv (routed
        // through the coder.rs resolver), offline the deterministic `__fake-agent --mode leaf`
        // stand-in. Either way it is supervised by the SAME `supervise::run` (deadline/tree-kill).
        let verdict_arg = if self.cfg.inject_escalate.as_deref() == Some(id) {
            "эскалация"
        } else {
            "готово"
        };
        let plan = self.coder_leaf_plan(id, verdict_arg);
        let v = self.run_leaf(&plan);

        // Read the leaf's report body (stream-json `result` for claude/stand-in, raw stdout for a
        // live codex child), then parse its structured markers (contract.rs).
        let leaf = self.leaf_report(&plan, &v);
        let report = leaf.text;
        let outcome = parse_outcome(&report);
        let verdict = outcome
            .as_ref()
            .map(|o| o.verdict.clone())
            .unwrap_or_default();
        let changed_files = parse_changed_files(&report).unwrap_or_default();

        let to = decide_to_state(leaf.supervised_ok, &verdict);

        // Validate working -> {in-review|escalated} through the state machine, then write it.
        self.check_transition("task", TaskState::Working.as_str(), to.as_str())?;
        self.write_descriptor(id, to, prerequisites, conflict_domain, None)?;

        // On an escalation, also reflect it in the queue transactionally (queue-tx escalate) so
        // the queue and descriptor agree; the happy path leaves the queue as captured (в работе).
        if to == TaskState::Escalated {
            let esc_argv = vec![
                "escalate".into(),
                "--work".into(),
                self.work_s.clone(),
                "--id".into(),
                id.to_string(),
                "--reason".into(),
                "leaf escalated in sandbox round".into(),
            ];
            self.tool_ok(
                &self.cfg.queue_tx(),
                &esc_argv,
                &format!("queue-tx escalate {id}"),
            )?;
        }

        let from_lit = task_literal(TaskState::Working);
        let to_lit = task_literal(to);
        let payload = format!("{{\"from\":\"{from_lit}\",\"to\":\"{to_lit}\"}}");
        self.emit(
            &[
                "--type",
                "task.status_changed",
                "--task-id",
                id,
                "--from",
                from_lit,
                "--to",
                to_lit,
                "--attempt",
                "1",
                "--round",
                "1",
                "--payload",
                &payload,
            ],
            &format!("emit task.status_changed {id}"),
        )?;

        Ok(TaskOutcome {
            id: id.to_string(),
            level,
            reviewer,
            verdict,
            supervised: v.reason.as_str(),
            from: TaskState::Working,
            to,
            changed_files,
            review: None,
        })
    }

    /// The opt-in per-task review round (phases 2.4–2.6). Every task that reached `in-review`
    /// after the execution round is reviewed by a supervised reviewer stand-in; the round is
    /// bracketed by `cohort.round_started`/`cohort.round_closed` (wave 2, the review wave) in the
    /// same §19 envelope the execution round uses. A no-op when `--review` is off or no task
    /// reached review (e.g. every task escalated), so no empty review round is emitted.
    fn run_review_round(
        &mut self,
        snap: &Snapshot,
        outcomes: &mut [TaskOutcome],
    ) -> Result<(), RunError> {
        if !self.cfg.review {
            return Ok(());
        }
        if !outcomes.iter().any(|o| o.to == TaskState::InReview) {
            return Ok(());
        }

        self.emit(
            &[
                "--type",
                "cohort.round_started",
                "--batch-id",
                &self.cfg.batch_id,
                "--wave",
                "2",
                "--payload",
                "{}",
            ],
            "emit cohort.round_started (review)",
        )?;

        for outcome in outcomes.iter_mut() {
            // Only a successfully-executed (`in-review`) task is reviewed; an escalated one is not.
            if outcome.to != TaskState::InReview {
                continue;
            }
            let id = outcome.id.clone();
            let level = outcome.level;
            let prereqs = prereqs_of(snap, &id);
            let domain = descriptor_globs(snap, &id);
            let review = self.run_one_review(&id, level, &prereqs, domain)?;
            outcome.review = Some(review);
            self.heartbeat()?;
        }

        self.emit(
            &[
                "--type",
                "cohort.round_closed",
                "--batch-id",
                &self.cfg.batch_id,
                "--wave",
                "2",
                "--payload",
                "{}",
            ],
            "emit cohort.round_closed (review)",
        )?;
        Ok(())
    }

    /// Drive ONE `in-review` task through the phase-2.6…2.9 **review fix cycle** (task T-128).
    ///
    /// Resolver 1 (tiering) picks the base Claude reviewer tier for the level and resolver 2b
    /// (reviewer re-election) the concrete reviewer — through the real resolvers, never a
    /// hard-coded name; the sandbox is Claude-only (`CODEX_REVIEWER=off`), so this resolves to the
    /// base Claude reviewer, deterministically the same every cycle.
    ///
    /// Each cycle runs a supervised reviewer pass ([`run_review_pass`]) and branches on the
    /// [`review_gate`]: a **clean** pass promotes `in-review -> ready`; **findings** dispatch a
    /// supervised deterministic coder fix round ([`run_fix_round`]) and re-review; an **incomplete**
    /// pass re-runs the reviewer unchanged (phase 2.7 — never hands the coder an empty list). The
    /// loop repeats under [`review_cycle_decision`] (`REVIEW_LOOP_MAX`); exhausting the budget is a
    /// CLEAN terminal escalation (`не сходится ревью после N циклов`) through the SAME
    /// `state-tx check-transition` + `queue-tx escalate` path the execution round uses, never a
    /// re-interpretation. A reviewer or fix leaf that cannot be supervised is a fail-closed
    /// escalation. The cycle number is written to the descriptor (`Циклов-ревью: N`) and is the
    /// `round` coordinate of every emitted `task.status_changed`, so each cycle is a DISTINCT
    /// observable fact by fingerprint.
    fn run_one_review(
        &mut self,
        id: &str,
        level: Level,
        prerequisites: &[String],
        conflict_domain: Option<&[String]>,
    ) -> Result<ReviewOutcome, RunError> {
        let base = base_reviewer(self.cfg.reviewer_tiering, level);
        let route = reelect_reviewer(CodexReviewer::Off, base, level, &[ImplBy::Claude]);
        let reviewer_name = route_reviewer_name(route);

        let mut cycle: u32 = 1;
        loop {
            // Resolver 4 (cycle limit) BEFORE running the cycle: an exhausted budget is a clean
            // terminal escalation, not another loop iteration — the deterministic cycle handles
            // exactly the encoded case and stops cleanly (intent doc §11, R2/R3).
            if let CycleDecision::Escalate { after_cycles } =
                review_cycle_decision(cycle, self.cfg.review_loop_max)
            {
                let reason = CycleDecision::Escalate { after_cycles }
                    .escalation_reason()
                    .unwrap_or_else(|| format!("не сходится ревью после {after_cycles} циклов"));
                // `after_cycles` (= REVIEW_LOOP_MAX) cycles ran without converging; record that
                // count as the terminal `Циклов-ревью`/`round` coordinate.
                let esc_cycle = after_cycles.max(1);
                self.commit_review_escalation(
                    id,
                    esc_cycle,
                    prerequisites,
                    conflict_domain,
                    &reason,
                    ReviewGate::Findings,
                    Some(self.cfg.review_loop_max),
                )?;
                return Ok(ReviewOutcome {
                    reviewer: reviewer_name,
                    gate: "findings",
                    supervised: "ok",
                    to: TaskState::Escalated,
                    cycles: esc_cycle,
                });
            }

            let pass = self.run_review_pass(id, reviewer_name, cycle)?;
            let to = decide_review_state(pass.supervised_ok, pass.gate);

            match to {
                TaskState::Ready => {
                    // A converging clean pass: promote `in-review -> ready` at this cycle.
                    self.commit_review_transition(
                        id,
                        TaskState::Ready,
                        cycle,
                        prerequisites,
                        conflict_domain,
                        pass.gate,
                    )?;
                    return Ok(ReviewOutcome {
                        reviewer: reviewer_name,
                        gate: review_gate_label(pass.gate),
                        supervised: pass.supervised,
                        to: TaskState::Ready,
                        cycles: cycle,
                    });
                }
                TaskState::Escalated => {
                    // The reviewer leaf could not be supervised — fail-closed escalation.
                    self.commit_review_escalation(
                        id,
                        cycle,
                        prerequisites,
                        conflict_domain,
                        "reviewer leaf failed in sandbox review cycle",
                        pass.gate,
                        None,
                    )?;
                    return Ok(ReviewOutcome {
                        reviewer: reviewer_name,
                        gate: review_gate_label(pass.gate),
                        supervised: pass.supervised,
                        to: TaskState::Escalated,
                        cycles: cycle,
                    });
                }
                TaskState::InReview => {
                    // Findings (2.8) dispatch a supervised coder fix before re-review; an
                    // incomplete pass (2.7) re-runs the reviewer unchanged. A fix leaf that cannot
                    // be supervised is itself a fail-closed escalation at this cycle.
                    if pass.gate == ReviewGate::Findings && !self.run_fix_round(id) {
                        self.commit_review_escalation(
                            id,
                            cycle,
                            prerequisites,
                            conflict_domain,
                            "fix leaf failed in sandbox review cycle",
                            pass.gate,
                            None,
                        )?;
                        return Ok(ReviewOutcome {
                            reviewer: reviewer_name,
                            gate: review_gate_label(pass.gate),
                            supervised: pass.supervised,
                            to: TaskState::Escalated,
                            cycles: cycle,
                        });
                    }
                    // Record the cycle (`in-review -> in-review`, `Циклов-ревью: N`) and re-review.
                    self.commit_review_transition(
                        id,
                        TaskState::InReview,
                        cycle,
                        prerequisites,
                        conflict_domain,
                        pass.gate,
                    )?;
                    cycle += 1;
                    self.heartbeat()?;
                }
                _ => unreachable!("decide_review_state yields only ready/in-review/escalated"),
            }
        }
    }

    /// Run ONE supervised reviewer pass over an `in-review` task: take the phase-2.6 freshness mark,
    /// spawn the reviewer leaf (live `claude` reviewer of the tier the resolvers named, or the
    /// deterministic offline `__fake-agent --mode review` stand-in), and name the [`review_gate`]
    /// branch off the parsed `review.md` — the terminal `ИТОГ:` line is NOT what decides
    /// clean/with-findings here. (The production adapter reads the same three branches in the same
    /// order and additionally requires that tail before promoting a CLEAN pass; an artifact
    /// without it is `Incomplete` there — a bounded re-run — never a terminal escalation. See
    /// [`crate::resolvers::gate`]'s module docs for the full statement of that one divergence.)
    /// The `inject_findings` task yields findings until it converges
    /// (`converge_after_cycles`), the deterministic "the fix worked at cycle N" (or never) knob.
    /// Robust to a live child that does not speak stream-json: the gate reads `review.md` from
    /// disk (the real reviewer writes it), and `supervised_ok` only needs a clean exit.
    fn run_review_pass(
        &self,
        id: &str,
        reviewer_name: &str,
        cycle: u32,
    ) -> Result<ReviewPass, RunError> {
        // The phase-2.6 freshness cutoff: the UTC mark taken JUST BEFORE the reviewer call. A clean
        // `SUMMARY-R` must be newer than this; the clean stand-in stamps a same-second fractional
        // mark, which lies within the observed invocation window without manufacturing a future
        // whole-second timestamp.
        let since = now_utc_iso();
        let summary_ts = format!("{}.001Z", now_utc_iso().trim_end_matches('Z'));
        let want_findings = self.cfg.inject_findings.as_deref() == Some(id)
            && match self.cfg.converge_after_cycles {
                // Clean from `threshold` onward — the fix converged; findings before it.
                Some(threshold) => cycle < threshold,
                // No convergence point — persistent findings drive the escalation branch.
                None => true,
            };

        // The reviewer leaf is Claude (the sandbox is Claude-only); its model comes from the tier
        // the tiering/re-election resolvers named — never a hard-coded model choice.
        let prompt = format!(
            "Use the {reviewer_name} subagent to review task {id}. Worktree={} WORK={}",
            self.cfg.work.join("worktrees").join(id).display(),
            self.cfg.work.display()
        );
        let mut call = ClaudeCall::new(prompt);
        call.model = Some(claude_reviewer_model(reviewer_name).to_string());
        call.max_turns = Some(40);
        call.allowed_tools = leaf_allowed_tools();
        call.posture = PermissionPosture::Allowlisted;

        let outcome_arg = if want_findings { "findings" } else { "clean" };
        let plan = LeafPlan {
            program: "claude",
            argv: call.to_argv(),
            stdin: String::new(),
            executor: Executor::Claude,
            fake_args: vec![
                "__fake-agent".into(),
                "--mode".into(),
                "review".into(),
                "--task".into(),
                id.to_string(),
                "--work".into(),
                self.work_s.clone(),
                "--outcome".into(),
                outcome_arg.to_string(),
                "--summary-ts".into(),
                summary_ts,
            ],
        };
        let v = self.run_leaf(&plan);
        let until = review_window_end();
        let supervised_ok = self.leaf_report(&plan, &v).supervised_ok;

        let review_md = Self::read_optional_artifact(
            &self.cfg.work.join("tasks").join(id).join("review.md"),
            "review.md",
        )?;
        let gate = review_gate(&parse_review(&review_md), &since, &until);

        Ok(ReviewPass {
            gate,
            supervised: v.reason.as_str(),
            supervised_ok,
        })
    }

    /// Run ONE coder-side fix round within the fix cycle: supervise the coder/fix leaf (live
    /// `claude`/`codex` routed through [`route_coder`], or the deterministic offline
    /// `__fake-agent --mode leaf` stand-in, verdict `готово`). Returns whether the fix converged
    /// cleanly (a supervised, cleanly-`готово` leaf); anything else fail-closes the task, exactly
    /// like the execution round's [`decide_to_state`].
    fn run_fix_round(&self, id: &str) -> bool {
        let plan = self.coder_leaf_plan(id, "готово");
        let v = self.run_leaf(&plan);
        let leaf = self.leaf_report(&plan, &v);
        let verdict = parse_outcome(&leaf.text)
            .map(|o| o.verdict)
            .unwrap_or_default();
        leaf.supervised_ok && verdict == "готово"
    }

    /// Validate + write ONE review-cycle descriptor transition (`in-review -> {ready|in-review}`),
    /// recording the cycle counter as `Циклов-ревью: N`, and emit its `task.status_changed` event
    /// keyed by the cycle number (the `round` coordinate, so cycle N's transition is a DISTINCT
    /// observable fact by fingerprint even when the status word is unchanged).
    fn commit_review_transition(
        &mut self,
        id: &str,
        to: TaskState,
        cycle: u32,
        prerequisites: &[String],
        conflict_domain: Option<&[String]>,
        gate: ReviewGate,
    ) -> Result<(), RunError> {
        self.check_transition("task", TaskState::InReview.as_str(), to.as_str())?;
        self.write_descriptor(id, to, prerequisites, conflict_domain, Some(cycle))?;
        let from_lit = task_literal(TaskState::InReview);
        let to_lit = task_literal(to);
        let round = cycle.to_string();
        let payload = json!({
            "from": from_lit,
            "to": to_lit,
            "gate": review_gate_label(gate),
            "cycle": cycle,
        })
        .to_string();
        self.emit(
            &[
                "--type",
                "task.status_changed",
                "--task-id",
                id,
                "--from",
                from_lit,
                "--to",
                to_lit,
                "--attempt",
                "1",
                "--round",
                &round,
                "--payload",
                &payload,
            ],
            &format!("emit task.status_changed (review cycle {cycle}) {id}"),
        )?;
        Ok(())
    }

    /// Fail-closed escalation of a task inside the review fix cycle: validate `in-review ->
    /// escalated`, write the descriptor (with the cycle counter), reflect it in the queue
    /// transactionally (`queue-tx escalate`) so the queue and descriptor agree, and emit the
    /// `task.status_changed` event keyed by the cycle number. `cap` carries the ASCII
    /// `REVIEW_LOOP_MAX` marker + limit for the budget-exhaustion escalation (host-agnostic
    /// observability; a supervision-failure escalation passes `None`).
    #[allow(clippy::too_many_arguments)]
    fn commit_review_escalation(
        &mut self,
        id: &str,
        cycle: u32,
        prerequisites: &[String],
        conflict_domain: Option<&[String]>,
        reason: &str,
        gate: ReviewGate,
        cap: Option<u32>,
    ) -> Result<(), RunError> {
        self.check_transition(
            "task",
            TaskState::InReview.as_str(),
            TaskState::Escalated.as_str(),
        )?;
        self.write_descriptor(
            id,
            TaskState::Escalated,
            prerequisites,
            conflict_domain,
            Some(cycle),
        )?;
        let esc_argv = vec![
            "escalate".into(),
            "--work".into(),
            self.work_s.clone(),
            "--id".into(),
            id.to_string(),
            "--reason".into(),
            reason.to_string(),
        ];
        self.tool_ok(
            &self.cfg.queue_tx(),
            &esc_argv,
            &format!("queue-tx escalate {id}"),
        )?;
        let from_lit = task_literal(TaskState::InReview);
        let to_lit = task_literal(TaskState::Escalated);
        let round = cycle.to_string();
        let mut payload = json!({
            "from": from_lit,
            "to": to_lit,
            "gate": review_gate_label(gate),
            "cycle": cycle,
            "reason": reason,
        });
        if let Some(limit) = cap {
            payload["cap"] = json!("REVIEW_LOOP_MAX");
            payload["limit"] = json!(limit);
        }
        let payload = payload.to_string();
        self.emit(
            &[
                "--type",
                "task.status_changed",
                "--task-id",
                id,
                "--from",
                from_lit,
                "--to",
                to_lit,
                "--attempt",
                "1",
                "--round",
                &round,
                "--payload",
                &payload,
            ],
            &format!("emit task.status_changed (review escalate cycle {cycle}) {id}"),
        )?;
        Ok(())
    }

    // --- the opt-in join barrier (phases 4–6) ------------------------------------------

    /// Drive the batch through the **join barrier** (`agents/processor.md` phases 4–6) after the
    /// review round, over the hermetic sandbox `.work`. A no-op when `--join` is off. Every step
    /// runs a deterministic `__fake-agent` stand-in (the `merger` and `full_reviewer`); the sandbox
    /// `.work` has NO repository, so the merge and ff-publish are hermetically SIMULATED — the
    /// engine drives the §13 state transitions and §19 events, never a real VCS mutation.
    ///
    /// * **4.1** enter the join — integration `none → in-progress`, seed `integration_state.md`,
    ///   emit `cohort.join_started` (payload: the ready task list).
    /// * **4.2/4.3** sequential merge — the merger stand-in writes `merge_report.md`; the engine
    ///   decides merged (`готова к слиянию → слита`) vs quarantined (`→ конфликт`, re-queued via
    ///   `queue-tx return`) per task off that report, never free text.
    /// * **5.1/5.2** the bounded integration review cycle ([`run_integration_review`]) with the
    ///   `INTEGRATION_LOOP_MAX` cap and the same clean/findings/incomplete gate as the per-task
    ///   cycle (adapted to `F-`/`SUMMARY-F` via [`integration_gate`]).
    /// * **5.3** publish — integration `reviewed → published`, each task `слита → опубликована`,
    ///   one `cohort.published`.
    /// * **6.1** archive — each task `опубликована → выполнена` (`queue-tx archive`), integration
    ///   `published → cleaned`.
    /// * **6** one `cohort.closed` closes the cohort regardless of outcome (even when nothing was
    ///   ready to merge — the terminal batch event).
    fn run_join_barrier(
        &mut self,
        snap: &Snapshot,
        outcomes: &[TaskOutcome],
    ) -> Result<Option<JoinReport>, RunError> {
        if !self.cfg.join {
            return Ok(None);
        }

        // Ready = the tasks the review round promoted to `готова к слиянию`; escalated = anything
        // that fell out in the execution OR review round. Both in stable admitted order.
        let ready: Vec<String> = outcomes
            .iter()
            .filter(|o| matches!(&o.review, Some(r) if r.to == TaskState::Ready))
            .map(|o| o.id.clone())
            .collect();
        let escalated = outcomes
            .iter()
            .filter(|o| {
                o.to == TaskState::Escalated
                    || matches!(&o.review, Some(r) if r.to == TaskState::Escalated)
            })
            .count();

        let mut merged: Vec<String> = Vec::new();
        let mut quarantined: Vec<String> = Vec::new();
        let mut published: Vec<String> = Vec::new();
        let mut integration: &'static str = "none";
        let mut integration_cycles: u32 = 0;

        if !ready.is_empty() {
            // --- 4.1: enter the join --------------------------------------------------------
            self.check_transition("integration", "none", "in-progress")?;
            self.write_integration_state(1)?;
            let started_payload = json!({ "ready_tasks": ready.clone() }).to_string();
            self.emit(
                &[
                    "--type",
                    "cohort.join_started",
                    "--batch-id",
                    &self.cfg.batch_id,
                    "--payload",
                    &started_payload,
                ],
                "emit cohort.join_started",
            )?;
            self.heartbeat()?;

            // --- 4.2/4.3: sequential merge, decided off merge_report.md ----------------------
            let report = self.run_merge_round(&ready)?;
            for line in &report {
                let prereqs = prereqs_of(snap, &line.id);
                let domain = descriptor_globs(snap, &line.id);
                match &line.outcome {
                    MergeOutcome::Merged { .. } => {
                        self.check_transition(
                            "task",
                            TaskState::Ready.as_str(),
                            TaskState::Merged.as_str(),
                        )?;
                        self.write_descriptor(&line.id, TaskState::Merged, &prereqs, domain, None)?;
                        self.emit_join_task_status(&line.id, TaskState::Ready, TaskState::Merged)?;
                        merged.push(line.id.clone());
                    }
                    MergeOutcome::Quarantined { reason } => {
                        // Rolled-back merge: `готова к слиянию → конфликт`, then re-queued
                        // transactionally (bounded requeue) and the descriptor dropped (Phase 6.2).
                        self.check_transition(
                            "task",
                            TaskState::Ready.as_str(),
                            TaskState::Conflict.as_str(),
                        )?;
                        self.write_descriptor(
                            &line.id,
                            TaskState::Conflict,
                            &prereqs,
                            domain,
                            None,
                        )?;
                        self.emit_join_task_status(
                            &line.id,
                            TaskState::Ready,
                            TaskState::Conflict,
                        )?;
                        let ret_argv = vec![
                            "return".into(),
                            "--work".into(),
                            self.work_s.clone(),
                            "--id".into(),
                            line.id.clone(),
                            "--reason".into(),
                            reason.clone(),
                            "--max-attempts".into(),
                            "3".into(),
                        ];
                        self.tool_ok(
                            &self.cfg.queue_tx(),
                            &ret_argv,
                            &format!("queue-tx return {}", line.id),
                        )?;
                        self.remove_task_descriptor(&line.id, "quarantine")?;
                        quarantined.push(line.id.clone());
                    }
                }
            }
            self.heartbeat()?;

            if merged.is_empty() {
                // Every ready branch was quarantined — nothing to review/publish.
                self.check_transition("integration", "in-progress", "failed")?;
                integration = "failed";
            } else {
                // --- 5.1/5.2: the bounded integration review cycle --------------------------
                let (clean, cycles) = self.run_integration_review(&merged)?;
                integration_cycles = cycles;
                if !clean {
                    // Did not converge within INTEGRATION_LOOP_MAX (or a stand-in failed): stop
                    // WITHOUT publishing — merged tasks stay `слита`, branch/worktree kept.
                    self.check_transition("integration", "in-progress", "failed")?;
                    integration = "failed";
                } else {
                    self.check_transition("integration", "in-progress", "reviewed")?;
                    // --- 5.3: publish -------------------------------------------------------
                    self.check_transition("integration", "reviewed", "published")?;
                    for id in &merged {
                        let prereqs = prereqs_of(snap, id);
                        let domain = descriptor_globs(snap, id);
                        self.check_transition(
                            "task",
                            TaskState::Merged.as_str(),
                            TaskState::Published.as_str(),
                        )?;
                        self.write_descriptor(id, TaskState::Published, &prereqs, domain, None)?;
                        self.emit_join_task_status(id, TaskState::Merged, TaskState::Published)?;
                    }
                    let published_payload =
                        json!({ "pushed": false, "tasks": merged.clone() }).to_string();
                    self.emit(
                        &[
                            "--type",
                            "cohort.published",
                            "--batch-id",
                            &self.cfg.batch_id,
                            "--payload",
                            &published_payload,
                        ],
                        "emit cohort.published",
                    )?;
                    self.heartbeat()?;

                    // --- 6.1: archive each published task -----------------------------------
                    for id in merged.clone() {
                        self.archive_task(&id)?;
                        published.push(id);
                    }
                    self.check_transition("integration", "published", "cleaned")?;
                    integration = "cleaned";
                }
            }
        }

        // --- 6: cohort.closed (batch processing complete for this run) ----------------------
        let closed_payload = json!({
            "merged": merged.len(),
            "quarantined": quarantined.len(),
            "escalated": escalated,
        })
        .to_string();
        self.emit(
            &[
                "--type",
                "cohort.closed",
                "--batch-id",
                &self.cfg.batch_id,
                "--payload",
                &closed_payload,
            ],
            "emit cohort.closed",
        )?;

        Ok(Some(JoinReport {
            ready,
            merged,
            quarantined,
            published,
            integration,
            integration_cycles,
        }))
    }

    /// Run ONE supervised merger pass: supervise the merger leaf (live `claude` merger, or the
    /// deterministic offline `__fake-agent --mode merge` stand-in, which writes `merge_report.md`),
    /// and return that report's parsed per-task lines. The `inject_merge_conflict` task (if any of
    /// the ready set) is quarantined by the stand-in — the deterministic "this branch conflicted"
    /// knob. The merger is a Claude role; its `merge_report.md` is read from disk either way.
    fn run_merge_round(
        &self,
        ready: &[String],
    ) -> Result<Vec<crate::contract::MergeLine>, RunError> {
        let prompt = format!(
            "Use the merger subagent to integrate the ready task branches of this batch into integration/{} in {}. WORK={}",
            self.cfg.batch_id,
            self.cfg
                .work
                .join("worktrees")
                .join("_integration")
                .display(),
            self.cfg.work.display()
        );
        let mut call = ClaudeCall::new(prompt);
        call.model = Some("sonnet".to_string());
        call.max_turns = Some(100);
        call.allowed_tools = leaf_allowed_tools();
        call.posture = PermissionPosture::Allowlisted;

        let mut fake_args: Vec<String> = vec![
            "__fake-agent".into(),
            "--mode".into(),
            "merge".into(),
            "--work".into(),
            self.work_s.clone(),
            "--batch".into(),
            self.cfg.batch_id.clone(),
            "--tasks".into(),
            ready.join(","),
        ];
        if let Some(q) = self.cfg.inject_merge_conflict.as_deref()
            && ready.iter().any(|id| id == q)
        {
            fake_args.push("--quarantine".into());
            fake_args.push(q.to_string());
        }
        let plan = LeafPlan {
            program: "claude",
            argv: call.to_argv(),
            stdin: String::new(),
            executor: Executor::Claude,
            fake_args,
        };
        let v = self.run_leaf(&plan);
        let parsed = crate::claude::parse_transcript(&v.stdout);
        if v.reason != Reason::Ok || parsed.is_error == Some(true) {
            return Err(RunError::new(
                exit::FAILED,
                format!(
                    "merger stand-in did not complete cleanly ({})",
                    v.reason.as_str()
                ),
            ));
        }
        let report = fs::read_to_string(self.cfg.work.join("merge_report.md"))
            .map_err(|e| RunError::new(exit::FAILED, format!("read merge_report.md: {e}")))?;
        Ok(parse_merge_report(&report))
    }

    /// Drive the **integration review fix cycle** (`agents/processor.md` phase 5.2) over the merged
    /// batch: each cycle runs a supervised `full_reviewer` pass ([`run_integration_review_pass`]) and
    /// branches on the [`integration_gate`] — a **clean** pass (fresh `SUMMARY-F`, no open `F-`)
    /// converges; **findings** dispatch a supervised deterministic integration fix (a coder stand-in
    /// in `_integration`) and re-review; an **incomplete** pass re-runs the reviewer unchanged. The
    /// loop is bounded by `INTEGRATION_LOOP_MAX` ([`review_cycle_decision`], the SAME resolver the
    /// per-task cycle uses); exhausting it — or a stand-in that cannot be supervised — is a
    /// fail-closed stop that leaves the batch UNPUBLISHED. Returns `(clean, cycles)`.
    fn run_integration_review(&mut self, merged: &[String]) -> Result<(bool, u32), RunError> {
        let _ = merged;
        let mut cycle: u32 = 1;
        loop {
            if let CycleDecision::Escalate { after_cycles } =
                review_cycle_decision(cycle, self.cfg.integration_loop_max)
            {
                // INTEGRATION_LOOP_MAX exhausted: do NOT publish; record the completed count.
                let done = after_cycles.max(1);
                self.write_integration_state(done)?;
                return Ok((false, done));
            }
            let pass = self.run_integration_review_pass(cycle)?;
            if !pass.supervised_ok {
                // full_reviewer stand-in failed — fail-closed, batch unpublished.
                return Ok((false, cycle));
            }
            match pass.gate {
                ReviewGate::Clean => {
                    self.write_integration_state(cycle)?;
                    return Ok((true, cycle));
                }
                ReviewGate::Findings => {
                    // Dispatch a deterministic integration fix in `_integration`, then re-review.
                    // A fix leaf that cannot be supervised is itself a fail-closed stop.
                    if !self.run_fix_round("_integration") {
                        return Ok((false, cycle));
                    }
                    self.write_integration_state(cycle + 1)?;
                    cycle += 1;
                    self.heartbeat()?;
                }
                ReviewGate::Incomplete => {
                    // full_reviewer cut short — re-run it unchanged (never fabricate a fix list).
                    // The deterministic stand-in always writes a fresh summary OR an open F-, so
                    // this is defensive; bump the counter to keep the loop INTEGRATION_LOOP_MAX-bounded.
                    self.write_integration_state(cycle + 1)?;
                    cycle += 1;
                    self.heartbeat()?;
                }
            }
        }
    }

    /// Run ONE supervised `full_reviewer` pass over the integration branch: take the phase-5.2
    /// freshness mark, spawn the full_reviewer leaf (live `claude` full_reviewer, or the
    /// deterministic offline `__fake-agent --mode integration-review` stand-in, which writes
    /// `review_integration.md`), and name the [`integration_gate`] branch off the parsed
    /// `F-`/`SUMMARY-F`. The `inject_f_findings` knob yields findings until it converges
    /// (`integration_converge_after`), or never. The `full_reviewer` is a Claude role; its
    /// `review_integration.md` is read from disk either way.
    fn run_integration_review_pass(&self, cycle: u32) -> Result<IntegrationPass, RunError> {
        let since = now_utc_iso();
        let summary_ts = format!("{}.001Z", now_utc_iso().trim_end_matches('Z'));
        let want_findings = self.cfg.inject_f_findings
            && match self.cfg.integration_converge_after {
                Some(threshold) => cycle < threshold,
                None => true,
            };

        let prompt = format!(
            "Use the full_reviewer subagent to review the integration branch integration/{} in worktree {}. WORK={}",
            self.cfg.batch_id,
            self.cfg
                .work
                .join("worktrees")
                .join("_integration")
                .display(),
            self.cfg.work.display()
        );
        let mut call = ClaudeCall::new(prompt);
        call.model = Some("opus".to_string());
        call.max_turns = Some(40);
        call.allowed_tools = leaf_allowed_tools();
        call.posture = PermissionPosture::Allowlisted;

        let outcome_arg = if want_findings { "findings" } else { "clean" };
        let plan = LeafPlan {
            program: "claude",
            argv: call.to_argv(),
            stdin: String::new(),
            executor: Executor::Claude,
            fake_args: vec![
                "__fake-agent".into(),
                "--mode".into(),
                "integration-review".into(),
                "--work".into(),
                self.work_s.clone(),
                "--outcome".into(),
                outcome_arg.to_string(),
                "--summary-ts".into(),
                summary_ts,
            ],
        };
        let v = self.run_leaf(&plan);
        let until = review_window_end();
        let supervised_ok = self.leaf_report(&plan, &v).supervised_ok;

        let review_md = Self::read_optional_artifact(
            &self.cfg.work.join("review_integration.md"),
            "review_integration.md",
        )?;
        let gate = integration_gate(&parse_review(&review_md), &since, &until);
        Ok(IntegrationPass {
            gate,
            supervised_ok,
        })
    }

    /// Phase-6.1 archive of ONE published task: validate `опубликована → выполнена`, emit the
    /// `task.status_changed` event BEFORE removing the descriptor dir, archive the queue entry
    /// transactionally (`queue-tx archive`), record the id in `Tasks_Done.md`, and drop the
    /// descriptor dir.
    fn archive_task(&mut self, id: &str) -> Result<(), RunError> {
        self.check_transition(
            "task",
            TaskState::Published.as_str(),
            TaskState::Done.as_str(),
        )?;
        self.emit_join_task_status(id, TaskState::Published, TaskState::Done)?;
        let archive_argv = vec![
            "archive".into(),
            "--work".into(),
            self.work_s.clone(),
            "--id".into(),
            id.to_string(),
        ];
        self.tool_ok(
            &self.cfg.queue_tx(),
            &archive_argv,
            &format!("queue-tx archive {id}"),
        )?;
        self.append_done(id)?;
        self.remove_task_descriptor(id, "archive")?;
        Ok(())
    }

    /// Read a reviewer artifact. A missing file is an incomplete pass (the caller chooses that
    /// gate branch); every other I/O failure is a real control-plane failure and must not be
    /// misreported as an empty review.
    fn read_optional_artifact(path: &Path, label: &str) -> Result<String, RunError> {
        match fs::metadata(path) {
            Ok(metadata) if metadata.is_file() => fs::read_to_string(path).map_err(|error| {
                RunError::new(
                    exit::FAILED,
                    format!("read {label} {}: {error}", path.display()),
                )
            }),
            Ok(_) => Err(RunError::new(
                exit::FAILED,
                format!("read {label} {}: expected a regular file", path.display()),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(error) => Err(RunError::new(
                exit::FAILED,
                format!("read {label} {}: {error}", path.display()),
            )),
        }
    }

    /// Remove a terminal descriptor only after its queue/archive transaction completed. Surface
    /// a failure immediately: continuing with a stale descriptor would claim a clean terminal
    /// transition while leaving contradictory durable state behind.
    fn remove_task_descriptor(&self, id: &str, phase: &str) -> Result<(), RunError> {
        let path = self.cfg.work.join("tasks").join(id);
        match fs::remove_dir_all(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RunError::new(
                exit::FAILED,
                format!(
                    "{phase}: remove task descriptor {}: {error}",
                    path.display()
                ),
            )),
        }
    }

    /// Append `### [T-ID] done` to `Tasks_Done.md` (idempotent — never a duplicate id), the
    /// header-anchored archive record `completed_ids` reads back.
    fn append_done(&self, id: &str) -> Result<(), RunError> {
        let path = self.cfg.work.join("Tasks_Done.md");
        let mut cur = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(RunError::new(
                    exit::FAILED,
                    format!("read Tasks_Done.md: {error}"),
                ));
            }
        };
        if has_archive_record(&cur, id) {
            return Ok(());
        }
        if !cur.is_empty() && !cur.ends_with('\n') {
            cur.push('\n');
        }
        cur.push_str(&format!("### [{id}] done\n"));
        fs::write(&path, cur)
            .map_err(|e| RunError::new(exit::FAILED, format!("write Tasks_Done.md: {e}")))
    }

    /// Write `integration_state.md` (§13.3 join bookkeeping): a stable sandbox `Ревью-SHA:` marker
    /// and the `F-циклов:` integration review-cycle counter (the `INTEGRATION_LOOP_MAX` guard).
    fn write_integration_state(&self, f_cycles: u32) -> Result<(), RunError> {
        self.write_file(
            "integration_state.md",
            &integration_state_md(&self.cfg.batch_id, f_cycles),
        )
    }

    /// Emit one join-barrier `task.status_changed` (`from → to`), keyed `attempt=1 round=1` — the
    /// §19 envelope the publication/archival transitions share with the execution round.
    fn emit_join_task_status(
        &mut self,
        id: &str,
        from: TaskState,
        to: TaskState,
    ) -> Result<(), RunError> {
        let from_lit = task_literal(from);
        let to_lit = task_literal(to);
        let payload = format!("{{\"from\":\"{from_lit}\",\"to\":\"{to_lit}\"}}");
        self.emit(
            &[
                "--type",
                "task.status_changed",
                "--task-id",
                id,
                "--from",
                from_lit,
                "--to",
                to_lit,
                "--attempt",
                "1",
                "--round",
                "1",
                "--payload",
                &payload,
            ],
            &format!("emit task.status_changed (join {from_lit}->{to_lit}) {id}"),
        )
    }
}

/// Whether an archive already has the task's **normative header record**.  A body reference such
/// as `depends on [T-123]` cannot make an archival operation idempotent: prerequisite resolution
/// intentionally ignores such prose, so the writer must use the same parser or it can remove a
/// descriptor without leaving the archive record that makes the task durably completed.
fn has_archive_record(archive: &str, id: &str) -> bool {
    archive
        .lines()
        .filter_map(archive_header_task_id)
        .any(|archived| archived == id)
}

/// The outcome of ONE reviewer pass inside the fix cycle (the [`review_gate`] branch plus the
/// reviewer-leaf supervision result).
struct ReviewPass {
    gate: ReviewGate,
    supervised: &'static str,
    supervised_ok: bool,
}

/// The outcome of ONE `full_reviewer` pass inside the integration review cycle (the
/// [`integration_gate`] branch plus the reviewer-leaf supervision result).
struct IntegrationPass {
    gate: ReviewGate,
    supervised_ok: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_literal_round_trips_through_canonical() {
        for st in [
            TaskState::NotStarted,
            TaskState::Working,
            TaskState::InReview,
            TaskState::Ready,
            TaskState::Merged,
            TaskState::Published,
            TaskState::Done,
            TaskState::Escalated,
            TaskState::Conflict,
        ] {
            assert_eq!(TaskState::from_markdown(task_literal(st)), Some(st));
        }
    }

    #[test]
    fn decide_to_state_is_fail_closed() {
        // Only a supervised, cleanly-готово leaf advances to review.
        assert_eq!(decide_to_state(true, "готово"), TaskState::InReview);
        // Anything else escalates: an escalation verdict, a supervision failure, a missing ИТОГ.
        assert_eq!(decide_to_state(true, "эскалация"), TaskState::Escalated);
        assert_eq!(decide_to_state(false, "готово"), TaskState::Escalated);
        assert_eq!(decide_to_state(true, ""), TaskState::Escalated);
    }

    #[test]
    fn close_reason_matches_capacity() {
        assert_eq!(close_reason(3, 3).as_str(), "COHORT_SIZE");
        assert_eq!(close_reason(2, 3).as_str(), "очередь-пуста");
        assert_eq!(close_reason(0, 0).as_str(), "очередь-пуста");
    }

    #[test]
    fn decide_review_state_maps_every_gate_branch_fail_closed() {
        // A supervised clean pass promotes to `ready`; findings/incomplete keep the task in review
        // for the fix cycle; a reviewer that could not be supervised fail-closes to `escalated`.
        assert_eq!(
            decide_review_state(true, ReviewGate::Clean),
            TaskState::Ready
        );
        assert_eq!(
            decide_review_state(true, ReviewGate::Findings),
            TaskState::InReview
        );
        assert_eq!(
            decide_review_state(true, ReviewGate::Incomplete),
            TaskState::InReview
        );
        // Supervision failure escalates regardless of what (if anything) landed in review.md.
        for gate in [
            ReviewGate::Clean,
            ReviewGate::Findings,
            ReviewGate::Incomplete,
        ] {
            assert_eq!(decide_review_state(false, gate), TaskState::Escalated);
        }
    }

    #[test]
    fn review_gate_labels_are_stable() {
        assert_eq!(review_gate_label(ReviewGate::Clean), "clean");
        assert_eq!(review_gate_label(ReviewGate::Findings), "findings");
        assert_eq!(review_gate_label(ReviewGate::Incomplete), "incomplete");
    }

    #[test]
    fn archive_idempotency_uses_normative_headers_not_body_mentions() {
        let archive = "# Completed\n\nRelated task [T-203] is still pending.\n";
        assert!(
            !has_archive_record(archive, "T-203"),
            "a body reference must not suppress the header this writer owes"
        );

        let archived = "### [T-203] done\n\nRelated task [T-204] is still pending.\n";
        assert!(has_archive_record(archived, "T-203"));
        assert!(!has_archive_record(archived, "T-204"));
    }

    #[test]
    fn unreadable_review_artifact_is_not_reclassified_as_an_incomplete_pass() {
        // A directory is never a valid review artifact. Treating this I/O error as an empty
        // document would silently turn a broken control-plane path into a retry loop.
        let error = Runner::read_optional_artifact(&std::env::temp_dir(), "review.md")
            .expect_err("a directory cannot be read as review.md");
        assert_eq!(error.code, exit::FAILED);
        assert!(error.message.contains("review.md"));
    }

    #[test]
    fn route_reviewer_name_maps_each_route() {
        use crate::resolvers::ReviewerRoute::*;
        assert_eq!(
            route_reviewer_name(Claude(BaseReviewer::Reviewer)),
            "reviewer"
        );
        assert_eq!(
            route_reviewer_name(Claude(BaseReviewer::ReviewerStd)),
            "reviewer_std"
        );
        assert_eq!(route_reviewer_name(CodexFull), "reviewer_codex");
        assert_eq!(
            route_reviewer_name(Augment(BaseReviewer::Reviewer)),
            "reviewer"
        );
    }

    // --- live mode (task T-244) --------------------------------------------------------

    #[test]
    fn leaf_spec_selects_live_vs_offline_target() {
        // A leaf plan carrying BOTH the real child argv and the offline stand-in argv.
        let plan = LeafPlan {
            program: "claude",
            argv: vec!["-p".into(), "do the task".into(), "--verbose".into()],
            stdin: String::new(),
            executor: Executor::Claude,
            fake_args: vec!["__fake-agent".into(), "--mode".into(), "leaf".into()],
        };
        // Offline default: spawn THIS engine binary as the deterministic `__fake-agent` stand-in.
        let offline = leaf_spec(false, "/path/to/engine", Duration::from_secs(1), &plan);
        assert_eq!(offline.program, "/path/to/engine");
        assert_eq!(
            offline.args.first().map(String::as_str),
            Some("__fake-agent")
        );
        // Live: spawn the REAL child with the built argv (never the engine's own path).
        let live = leaf_spec(true, "/path/to/engine", Duration::from_secs(1), &plan);
        assert_eq!(live.program, "claude");
        assert_eq!(live.args, plan.argv);
    }

    #[test]
    fn leaf_deadline_defaults_split_fake_and_live() {
        // No override: fake keeps the historical 60s cap, live gets the much larger budget so a real
        // headless leaf can actually finish instead of timing out→tree-kill→escalation (R-01).
        assert_eq!(
            resolve_leaf_deadline(None, false),
            Duration::from_secs(LEAF_DEADLINE_FAKE_SECS)
        );
        assert_eq!(
            resolve_leaf_deadline(None, true),
            Duration::from_secs(LEAF_DEADLINE_LIVE_SECS)
        );
        // A live leaf must get a strictly larger default budget than the fake stand-in — otherwise
        // the shared 60s cap would time-out every real leaf (R-01).
        const _: () = assert!(LEAF_DEADLINE_LIVE_SECS > LEAF_DEADLINE_FAKE_SECS);
        // An explicit `--leaf-deadline` override wins for either mode…
        assert_eq!(
            resolve_leaf_deadline(Some(300), false),
            Duration::from_secs(300)
        );
        assert_eq!(
            resolve_leaf_deadline(Some(7200), true),
            Duration::from_secs(7200)
        );
        // …and is clamped to a floor of 1s so `--leaf-deadline 0` can never mean "kill immediately".
        assert_eq!(resolve_leaf_deadline(Some(0), true), Duration::from_secs(1));
    }

    #[test]
    fn leaf_spec_carries_codex_prompt_on_stdin_when_live() {
        let plan = LeafPlan {
            program: "codex",
            argv: vec!["exec".into(), "-".into()],
            stdin: "implement T-1".into(),
            executor: Executor::Codex,
            fake_args: vec!["__fake-agent".into(), "--mode".into(), "leaf".into()],
        };
        // Live codex: the prompt rides stdin (never a shell fragment).
        let live = leaf_spec(true, "/engine", Duration::from_secs(1), &plan);
        assert_eq!(live.program, "codex");
        assert_eq!(live.stdin, "implement T-1");
        // Offline: the codex prompt is irrelevant — the stand-in gets no stdin.
        let offline = leaf_spec(false, "/engine", Duration::from_secs(1), &plan);
        assert!(offline.stdin.is_empty());
        assert_eq!(offline.program, "/engine");
    }

    #[test]
    fn coder_executor_routes_through_route_coder_with_explicit_posture() {
        let wt = Path::new("/abs/worktree/T-1");
        // Default `off` → the Claude coder of the level, with the permission posture stated
        // EXPLICITLY on argv (`--permission-mode` + an enumerated `--allowedTools`), never inherited.
        let (prog, argv, stdin, ex) =
            plan_coder_executor(CodexCoder::Off, false, Level::Coder, "implement T-1", wt);
        assert_eq!(prog, "claude");
        assert_eq!(ex, Executor::Claude);
        assert!(stdin.is_empty(), "claude's prompt is on argv, not stdin");
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--permission-mode" && w[1] == "acceptEdits"),
            "explicit permission mode on the call's own argv (T-057): {argv:?}"
        );
        assert!(
            argv.iter().any(|s| s == "--allowedTools"),
            "explicit tool allowlist on argv: {argv:?}"
        );
        assert!(
            argv.iter().any(|s| s == "implement T-1"),
            "the prompt is passed as an argv element: {argv:?}"
        );

        // Opted-in `fast+std` at `coder` level → the fail-closed `codex exec` maker, prompt on stdin.
        let (prog, argv, stdin, ex) = plan_coder_executor(
            CodexCoder::FastStd,
            false,
            Level::Coder,
            "implement T-1",
            wt,
        );
        assert_eq!(prog, "codex");
        assert_eq!(ex, Executor::Codex);
        assert_eq!(stdin, "implement T-1", "codex reads the prompt from stdin");
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "approval_policy=never"),
            "the fail-closed approval policy is pinned on argv (T-069): {argv:?}"
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--sandbox" && w[1] == "workspace-write"),
            "an explicit sandbox is on argv: {argv:?}"
        );
        // With the network gate OFF the codex argv carries no outbound-network override (T-283).
        assert!(
            !argv
                .iter()
                .any(|s| s == "sandbox_workspace_write.network_access=true"),
            "codex_network=false must not open the sandbox network: {argv:?}"
        );

        // The resolved `codex_network` posture is propagated onto the codex argv (T-063/T-283).
        let (_prog, argv, _stdin, _ex) =
            plan_coder_executor(CodexCoder::FastStd, true, Level::Coder, "implement T-1", wt);
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "sandbox_workspace_write.network_access=true"),
            "codex_network=true must open the sandbox network on argv: {argv:?}"
        );

        // `coder_deep` is always Claude even under an opted-in flag (the resolver's stage-1 rule).
        let (prog, _argv, _stdin, ex) =
            plan_coder_executor(CodexCoder::FastStd, false, Level::CoderDeep, "x", wt);
        assert_eq!(prog, "claude");
        assert_eq!(ex, Executor::Claude);
    }

    #[test]
    fn claude_models_key_off_the_resolved_tier() {
        // Level → model (single source; coder_deep is Opus, the shallower levels Sonnet).
        assert_eq!(claude_model_for_level(Level::CoderDeep), "opus");
        assert_eq!(claude_model_for_level(Level::Coder), "sonnet");
        assert_eq!(claude_model_for_level(Level::CoderFast), "sonnet");
        // Reviewer tier → model (keyed off the tiering resolver's name).
        assert_eq!(claude_reviewer_model("reviewer_std"), "sonnet");
        assert_eq!(claude_reviewer_model("reviewer"), "opus");
    }

    #[test]
    fn descriptor_md_parses_back() {
        // No review-cycle counter on a pre-review (captured/executed) descriptor.
        let md = descriptor_md(
            "T-201",
            TaskState::Working,
            "B-1",
            &["T-200".to_string()],
            Some(&["engine/src/**".to_string()]),
            None,
        );
        let d = crate::state::parse_descriptor("T-201", &md);
        assert_eq!(d.state, Some(TaskState::Working));
        assert_eq!(d.prerequisites, vec!["T-200"]);
        assert!(
            !md.contains("Циклов-ревью:"),
            "a pre-review descriptor carries no cycle counter"
        );
        assert_eq!(crate::state::parse_review_cycles(&md), None);
    }

    #[test]
    fn descriptor_md_records_review_cycle_counter() {
        // Inside the review fix cycle the counter is written as an additive `Циклов-ревью: N` field
        // and round-trips through the state-layer reader, WITHOUT disturbing the existing fields.
        let md = descriptor_md(
            "T-201",
            TaskState::InReview,
            "B-1",
            &[],
            Some(&["engine/src/**".to_string()]),
            Some(3),
        );
        assert_eq!(crate::state::parse_review_cycles(&md), Some(3));
        let d = crate::state::parse_descriptor("T-201", &md);
        assert_eq!(d.state, Some(TaskState::InReview));
        assert_eq!(
            d.conflict_domain.as_deref(),
            Some(&["engine/src/**".to_string()][..])
        );
    }

    #[test]
    fn batch_md_parses_back() {
        let md = batch_md(
            "B-20260712T000000Z",
            "abc123",
            &["T-201".to_string(), "T-202".to_string()],
        );
        let b = crate::state::parse_batch(&md);
        assert_eq!(b.batch_id.as_deref(), Some("B-20260712T000000Z"));
        assert_eq!(b.base.as_deref(), Some("abc123"));
        assert_eq!(b.tasks.len(), 2);
        assert_eq!(b.tasks[0].id, "T-201");
        assert_eq!(b.tasks[0].level.as_deref(), Some("coder"));
    }

    #[test]
    fn cohort_md_parses_back_open_and_closed() {
        let open = crate::state::parse_cohort(&cohort_md("B-1", "открыт", 2));
        assert_eq!(open.admission.map(|a| a.as_str()), Some("open"));
        assert_eq!(open.admitted_total, Some(2));
        let closed =
            crate::state::parse_cohort(&cohort_md("B-1", "закрыт · причина=COHORT_SIZE", 3));
        assert_eq!(closed.admission.map(|a| a.as_str()), Some("closed"));
        assert_eq!(closed.admission_reason.as_deref(), Some("COHORT_SIZE"));
    }
}
