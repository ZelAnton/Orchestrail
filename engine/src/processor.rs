//! Deterministic phase-0–6 processor state machine.
//!
//! `agents/processor.md` is deliberately a behavioural oracle during migration.  This module
//! contains its *orchestration* decisions, not an LLM substitute: leaf agents, VCS and CI return
//! typed [`ProcessorCommand`] values, then the reducer validates the current phase and produces
//! the next concrete [`Effect`].  The caller persists [`ProcessorState`] after every successful
//! command before executing an effect, which makes a crash/restart an ordinary `Recover` command
//! instead of an invitation to replay an unknown model call.
//!
//! The reducer is intentionally free of filesystem, VCS and process I/O.  `run` (and eventually
//! the daemon) owns those integrations through `processkit` and [`crate::vcs`].  Keeping the
//! phase logic here lets the same transition tests cover Git, JJ and scripted leaf fixtures.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::notification::NotificationEvent;

use crate::dependency_graph::RefreshBoundary;
use crate::resolvers::{
    ActiveClass, ActiveTask, AdmissionGate, AttemptSignature, Candidate, CloseReason,
    CohortCounters, CohortThresholds, Domain, EmptyReason, Level, Risk, StagnationDecision,
    admission_gate, empty_fixed_set_decision, plan_admission, review_cycle_decision,
    stagnation_decision,
};
use crate::session::{LeafSessionKey, LeafSessionUpdate, is_valid_session_id};
use crate::state::DeliveryTarget;
use crate::task_id::is_task_id;

/// Version of the persisted processor checkpoint.  Bump only with an explicit migration.
pub const PROCESSOR_STATE_VERSION: u32 = 1;

/// Validated deterministic limits normally decoded from `.work/config.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorConfig {
    /// At most this many non-terminal task leaves may be in a rolling wave.
    pub max_parallel: usize,
    /// Maximum task count admitted into the current cohort across all waves.
    pub cohort_size: u32,
    /// Stop rolling admission after this many minutes.
    pub cohort_max_age_minutes: u64,
    /// Optional total cohort wall-clock circuit breaker.
    pub cohort_budget_secs: Option<u64>,
    /// Optional post-charge ceiling for deduplicated, provider-actual `usage.recorded` tokens.
    /// `None` is the documented `COHORT_TOKEN_BUDGET: 0` unlimited mode.
    pub cohort_token_budget: Option<u64>,
    /// Whether an explicit unmetered model-call marker makes token telemetry unavailable.
    /// The legacy-compatible default keeps the marker visible but gates on known actuals.
    pub cohort_token_budget_strict: bool,
    /// Whether the event outbox is enabled. A non-zero token budget refuses model dispatch when
    /// this is false because it cannot establish a trustworthy actual-usage snapshot.
    pub events_outbox_enabled: bool,
    /// Maximum per-task review/fix cycles.
    pub review_loop_max: u32,
    /// Maximum integration-review/fix cycles.
    pub integration_loop_max: u32,
    /// Maximum CI repair attempts after publication verification fails.
    pub ci_fix_max: u32,
    /// Maximum repeat of one identical finding/error before the loop escalates.
    pub stagnation_limit: u32,
    /// Maximum total transient execution attempts for an individual leaf kind, including the
    /// first launch. This is the direct meaning of legacy `CALL_MAX_ATTEMPTS`.
    pub leaf_max_attempts: u32,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            max_parallel: 3,
            cohort_size: 9,
            cohort_max_age_minutes: 90,
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            events_outbox_enabled: true,
            review_loop_max: 8,
            integration_loop_max: 8,
            ci_fix_max: 3,
            stagnation_limit: 2,
            leaf_max_attempts: 2,
        }
    }
}

impl ProcessorConfig {
    pub fn validate(&self) -> Result<(), ProcessorError> {
        if self.max_parallel == 0 {
            return Err(ProcessorError::InvalidConfig(
                "MAX_PARALLEL must be at least 1".into(),
            ));
        }
        if self.cohort_size == 0 {
            return Err(ProcessorError::InvalidConfig(
                "COHORT_SIZE must be at least 1".into(),
            ));
        }
        if self.cohort_max_age_minutes == 0 {
            return Err(ProcessorError::InvalidConfig(
                "COHORT_MAX_AGE must be at least one minute".into(),
            ));
        }
        if self.stagnation_limit < 2 {
            return Err(ProcessorError::InvalidConfig(
                "STAGNATION_LIMIT must be at least 2".into(),
            ));
        }
        if self.review_loop_max == 0
            || self.integration_loop_max == 0
            || self.ci_fix_max == 0
            || self.leaf_max_attempts == 0
        {
            return Err(ProcessorError::InvalidConfig(
                "review, integration, CI, and leaf-attempt limits must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// Coarse, durable phase of one processor lease/session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    /// Phase 0: inspect durable state before any mutation or leaf dispatch.
    Recovery,
    /// Phase 1: a batch exists but its first rolling wave has not been admitted yet.
    Opening,
    /// Phase 2: top up free slots and advance each active task by exactly one step.
    Rolling,
    /// Phase 4: merge ready task branches into the integration worktree.
    Joining,
    /// Phase 5: integration review, publication and required CI verification.
    Publishing,
    /// Phase 5.5/6: knowledge/journal/cleanup and terminal accounting.
    Cleaning,
    /// No active cohort. The lease may be released or a new cohort may be opened.
    Idle,
    /// Operator pause, observed at every effect boundary.
    Paused,
    /// Fail-closed state requiring an operator decision; never silently re-dispatches a leaf.
    Blocked,
}

/// Per-task state internal to the deterministic engine.
///
/// The former `Returned` variant represented a task re-queued for a later cohort, but no active
/// transition ever constructed it: re-queueing is a control-plane effect while the durable engine
/// task remains in `Conflict`. Removing that dead state keeps every phase match exhaustive and
/// prevents it from being projected as an escalation. A legacy checkpoint containing the
/// kebab-case value `"returned"` now fails native runtime resume with
/// [`crate::runtime::RuntimeError::CorruptCheckpoint`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskPhase {
    Capturing,
    Implementing,
    Committing,
    Reviewing,
    Fixing,
    Ready,
    /// A typed VCS merge is deliberately left in its conflict state while the checkpointed
    /// merger leaf prepares the exact resolution.  No other task may merge from this state.
    ResolvingMerge,
    Merged,
    Published,
    Done,
    Conflict,
    Escalated,
}

/// A one-shot, checkpointed continuation created only by a strictly proved legacy import.  It is
/// consumed by Phase 0 into the ordinary native effect ledger before any agent/VCS work begins;
/// a later crash therefore follows the same inspection protocol as a natively scheduled leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportedRecoveryIntent {
    EnsureWorkspace,
    /// The legacy descriptor was already in review and its task branch retains the reviewed
    /// candidate. Recreate only the managed workspace, then resume review at that exact tip.
    EnsureWorkspaceForReview,
    DispatchImplementation,
    DispatchReview,
    /// Phase 4 recorded a merge quarantine in the descriptor/report before Phase 6 could return
    /// the queue row.  The recovery importer may schedule that idempotent control-plane half
    /// once, but must retain the marker until the return effect is durably acknowledged so a
    /// restart cannot increment the quarantine counter twice.
    ReturnConflictToQueue,
}

impl TaskPhase {
    fn is_active(self) -> bool {
        matches!(
            self,
            TaskPhase::Capturing
                | TaskPhase::Implementing
                | TaskPhase::Committing
                | TaskPhase::Reviewing
                | TaskPhase::Fixing
        )
    }

    fn blocks_admission(self) -> Option<ActiveClass> {
        match self {
            TaskPhase::Capturing
            | TaskPhase::Implementing
            | TaskPhase::Committing
            | TaskPhase::Reviewing
            | TaskPhase::Fixing => Some(ActiveClass::Active),
            TaskPhase::Ready
            | TaskPhase::ResolvingMerge
            | TaskPhase::Escalated
            | TaskPhase::Conflict => Some(ActiveClass::Terminal),
            TaskPhase::Merged | TaskPhase::Published | TaskPhase::Done => None,
        }
    }
}

/// A supervised leaf role.  The state machine never derives the role from free-form agent text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LeafKind {
    Implement,
    Review,
    Fix,
    Merger,
    IntegrationReview,
    IntegrationFix,
    CiFix,
    KnowledgeCurator,
}

/// Which narrow cross-project inbox operation an explicitly checkpointed curator may perform.
/// The intake flow deliberately precedes every planner wave; final reply routing is a separate
/// Phase-6 boundary and cannot be accidentally started while a task cohort is still rolling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InboxCurationMode {
    Intake,
    Finalize,
}

/// A task admitted or proposed for admission by the deterministic planner boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionCandidate {
    pub id: String,
    pub conflict_domain: String,
    /// Planner-selected executor level. It is persisted with the task so a restarted run never
    /// re-derives reviewer/Codex routing from stale descriptor prose.
    pub level: Level,
    /// Descriptor-backed initial risk classification proposed by the planner. The native
    /// admission boundary rechecks it against `task.md` before copying it into the checkpoint,
    /// so a coder's later `риск=` elevation remains monotonic across a restart.
    pub risk: Risk,
    pub ready: bool,
    pub current_delivery_lane: bool,
}

/// Durable runtime details for one task.  The fields are intentionally the coordinates needed to
/// resume review/fix cycles without replaying a leaf call from chat history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRuntime {
    pub id: String,
    pub conflict_domain: String,
    /// `None` is an intentionally fail-closed representation of a checkpoint written before
    /// executor level became durable. Native agent adapters must hold rather than guess a route.
    #[serde(default)]
    pub level: Option<Level>,
    /// The planner classification, possibly raised by a completed coder report. `None` keeps
    /// old legacy recovery checkpoints readable without fabricating an initial risk.
    #[serde(default)]
    pub risk: Option<Risk>,
    pub wave: u32,
    pub phase: TaskPhase,
    pub leaf_attempts: BTreeMap<String, u32>,
    /// Number of completed per-task reviewer passes, including `Incomplete` passes. This is the
    /// durable `Циклов-ревью` coordinate used to enforce `REVIEW_LOOP_MAX` across recovery.
    pub review_cycles: u32,
    pub review_signatures: Vec<String>,
    /// The open-finding count (`ReviewOutcome::Findings`'s own `open_findings`) of the review
    /// round that most recently dispatched a fix leaf, or `None` outside a fix round / on a
    /// checkpoint written before task T-014. Consumed (and cleared) the moment the fix leaf
    /// returns, so it never survives stale across an unrelated later round; a resumed checkpoint
    /// missing it simply skips the empty-fixed-set early exit for that one round and falls back to
    /// the unaffected `stagnation_decision` path.
    #[serde(default)]
    pub pending_fix_open_findings: Option<u32>,
    /// The exact ids (`ReviewOutcome::Findings::open_finding_ids`) counted in
    /// `pending_fix_open_findings` for the same round (R-06). Consumed (cleared) alongside it the
    /// moment the fix leaf returns.
    ///
    /// `Some` vs `None` is the ABSENCE of the coordinate, never its emptiness (R-08):
    /// * `Some(ids)` — this round reported its open set, and it is authoritative. `Some(vec![])` is
    ///   a legitimate value of that kind ("this round has nothing a fixer may decline"), and it
    ///   validates STRICTLY like any other known set: no `не исправлено=` entry can be a member of
    ///   it, so none is counted. Emptiness must NOT be re-read as "unknown" — that would restore
    ///   exactly the unbounded count R-06 was filed against, on the one route that can produce it
    ///   (`native_port::enforce_review_cycle_gate` over a reviewer's clean pass).
    /// * `None` — no such coordinate exists at all: a checkpoint written before R-06, the
    ///   count-only `#[serde(default)]` degradation described on `pending_fix_open_findings`, or a
    ///   leaf role that never runs a task fix cycle. Only then does the adapter that computes
    ///   `wont_fixed` fall back to an unvalidated (but still deduplicated) count, because there is
    ///   nothing to validate against — it never fabricates membership.
    #[serde(default)]
    pub pending_fix_open_finding_ids: Option<Vec<String>>,
    /// The review dimensions (`resolvers::reviewer::ReviewerRoster`) that reported at least one
    /// finding in the MOST RECENT completed review round, by dimension name. It is the durable,
    /// per-task coordinate for the selective-repeat optimization (orca's
    /// `ReviewerSelector.onlyPreviouslyReporting`): a repeat round may narrow the roster to just
    /// these still-"talking" dimensions via `reviewer::narrow_roster_to_previously_reporting`,
    /// skipping the ones that were silent last time. It is additive per-task state, exactly like
    /// `review_signatures` above.
    ///
    /// `#[serde(default)]` (empty) so a checkpoint written before this field — every pre-T-018
    /// checkpoint — deserializes without panic and without fabricating a dimension. An empty set is
    /// read as "no prior per-dimension finding signal recorded"; on the review-authority side that
    /// degrades fail-OPEN — the narrow helper then runs the FULL eligible roster rather than
    /// silently narrowing review to nothing (a skipped review is an un-audited merge), the same
    /// safe-path degradation the adjacent `Option` fields take when their coordinate is absent.
    #[serde(default)]
    pub dimensions_with_findings_last_round: Vec<String>,
    pub implementation_author: Option<String>,
    /// The last committed tip that a reviewer completed before the current `review_sha` was
    /// created. `None` identifies the first full review from the immutable cohort base.  It is
    /// deliberately durable: a resumed fixer must not turn a required range review into a
    /// second broad review merely because process-local context was lost.
    #[serde(default)]
    pub previous_review_sha: Option<String>,
    pub review_sha: Option<String>,
    pub reason: Option<String>,
    /// Set only while adopting a legacy active descriptor. Native cohorts never manufacture this
    /// marker: their durable effect ledger already records the exact outstanding action.
    #[serde(default)]
    pub imported_recovery_intent: Option<ImportedRecoveryIntent>,
    /// Provider conversation ids for repeated calls of the same leaf lineage, keyed by
    /// [`LeafSessionKey::as_durable_key`] (`claude:coder`, `codex:reviewer`). It is durable for
    /// the same reason `review_sha` is — a resumed cycle must not lose the coordinate — but it is
    /// deliberately ORTHOGONAL: nothing in this reducer reads it, so no phase, transition, or
    /// escalation can depend on whether a conversation happens to still exist. A checkpoint
    /// written before durable sessions simply has no map and re-seeds full context, exactly as
    /// the engine behaved before.
    ///
    /// `Processor::record_leaf_session` keeps at most one entry per lineage: the two providers'
    /// keys for one lineage are mutually exclusive, because only the provider that last ran can
    /// know what the working tree now contains.
    #[serde(default)]
    pub leaf_sessions: BTreeMap<String, String>,
}

impl TaskRuntime {
    fn new(candidate: &AdmissionCandidate, wave: u32) -> Self {
        Self {
            id: candidate.id.clone(),
            conflict_domain: candidate.conflict_domain.clone(),
            level: Some(candidate.level),
            risk: Some(candidate.risk),
            wave,
            phase: TaskPhase::Capturing,
            leaf_attempts: BTreeMap::new(),
            review_cycles: 0,
            review_signatures: Vec::new(),
            pending_fix_open_findings: None,
            pending_fix_open_finding_ids: None,
            dimensions_with_findings_last_round: Vec::new(),
            implementation_author: None,
            previous_review_sha: None,
            review_sha: None,
            reason: None,
            imported_recovery_intent: None,
            leaf_sessions: BTreeMap::new(),
        }
    }

    fn leaf_attempt(&mut self, kind: LeafKind) -> u32 {
        let attempts = self.leaf_attempts.entry(kind.as_str().into()).or_default();
        *attempts = attempts.saturating_add(1);
        *attempts
    }

    /// Durable conversation id for one leaf lineage, when a previous call recorded one. The
    /// caller must still prove the conversation exists before resuming it.
    pub fn leaf_session(&self, key: LeafSessionKey) -> Option<&str> {
        self.leaf_sessions
            .get(&key.as_durable_key())
            .map(String::as_str)
    }
}

/// Accept only a strictly higher coder-reported classification. The planner's classification is
/// durable metadata, so accepting a lower/equal value after a restart would let an agent erase
/// the more cautious assessment already committed by a prior leaf.
fn raise_task_risk(task: &mut TaskRuntime, reported: Risk) -> Result<(), String> {
    validate_task_risk_elevation(task.risk, reported)?;
    task.risk = Some(reported);
    Ok(())
}

/// Validate a reported elevation without changing a checkpoint. Native adapters use this before
/// they patch a descriptor, while the reducer uses [`raise_task_risk`] before it commits state.
pub(crate) fn validate_task_risk_elevation(
    current: Option<Risk>,
    reported: Risk,
) -> Result<(), String> {
    let current = current.ok_or_else(|| {
        format!(
            "coder reported риск={} but the task has no durable planner risk; cannot prove a strictly higher classification",
            reported.as_str()
        )
    })?;
    if reported <= current {
        return Err(format!(
            "coder reported риск={} but the durable task risk is {}; only a strictly higher risk may be reported",
            reported.as_str(),
            current.as_str()
        ));
    }
    Ok(())
}

/// Convert non-task leaf outcomes into a durable diagnostic. Task risk elevation is deliberately
/// not generic success metadata: only the implementation/fix continuation owns a descriptor it
/// may update.
fn non_success_leaf_reason(outcome: LeafOutcome, role: &str) -> Option<String> {
    match outcome {
        LeafOutcome::Completed { .. } => None,
        LeafOutcome::RetryableFailure { reason } | LeafOutcome::Escalated { reason } => {
            Some(reason)
        }
        LeafOutcome::RiskElevated { risk, .. } => Some(format!(
            "{role} reported unsupported task risk elevation {}",
            risk.as_str()
        )),
        // Won't-fix metadata (task T-014) is, like risk elevation above, a task-fix-cycle-only
        // extension: only `TaskLeaf`'s fix path owns the descriptor it correlates against.
        LeafOutcome::CompletedWithWontFix { .. } => Some(format!(
            "{role} reported unsupported fix-cycle won't-fix metadata"
        )),
    }
}

impl LeafKind {
    /// Stable durable key used in the checkpoint's per-leaf attempt ledger and evidence names.
    pub fn as_str(self) -> &'static str {
        match self {
            LeafKind::Implement => "implement",
            LeafKind::Review => "review",
            LeafKind::Fix => "fix",
            LeafKind::Merger => "merger",
            LeafKind::IntegrationReview => "integration-review",
            LeafKind::IntegrationFix => "integration-fix",
            LeafKind::CiFix => "ci-fix",
            LeafKind::KnowledgeCurator => "knowledge-curator",
        }
    }
}

/// A model-bearing effect that must pass the cohort wall-clock and, when configured, post-charge
/// token safety checks immediately before it starts. Keeping the continuation as typed data makes
/// each check durable and lets a restart re-observe the guard without replaying an unknown model
/// call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelCall {
    Planner {
        free_slots: usize,
    },
    /// Critical evaluation of untrusted inter-project inbox messages. The model never chooses an
    /// admission directly: after it returns, the native queue inbox drain and planner still
    /// validate the resulting durable task records.
    InboxCurator {
        free_slots: usize,
        mode: InboxCurationMode,
    },
    /// The model-derived graph candidate is later validated and atomically applied by the native
    /// registry adapter. A refresh failure is recorded as a post-publication degradation rather
    /// than allowing the model to hand-edit the registry.
    DependencyCurator {
        boundary: RefreshBoundary,
    },
    /// Optional diversity review that must complete before the authoritative task-review leaf.
    /// It is its own continuation so an enabled token budget is checked between the two calls.
    TaskReviewPreparation {
        task_id: String,
    },
    /// Optional Codex implementation/fix before the ordinary Claude maker fallback.  It is a
    /// distinct continuation so the token preflight sits between the two possible model calls.
    TaskLeafPreparation {
        task_id: String,
        kind: LeafKind,
    },
    CiFixPreparation,
    Task {
        task_id: String,
        kind: LeafKind,
    },
    Integration {
        kind: LeafKind,
    },
}

impl ModelCall {
    fn from_effect(effect: &Effect) -> Option<Self> {
        match effect {
            Effect::PlanNextWave { free_slots } => Some(Self::Planner {
                free_slots: *free_slots,
            }),
            Effect::DispatchInboxCurator { free_slots, mode } => Some(Self::InboxCurator {
                free_slots: *free_slots,
                mode: *mode,
            }),
            Effect::DispatchDependencyCurator { boundary } => Some(Self::DependencyCurator {
                boundary: *boundary,
            }),
            Effect::PrepareTaskReview { task_id } => Some(Self::TaskReviewPreparation {
                task_id: task_id.clone(),
            }),
            Effect::PrepareTaskLeaf { task_id, kind } => Some(Self::TaskLeafPreparation {
                task_id: task_id.clone(),
                kind: *kind,
            }),
            Effect::PrepareCiFix => Some(Self::CiFixPreparation),
            Effect::DispatchTask { task_id, kind }
                if matches!(kind, LeafKind::Implement | LeafKind::Review | LeafKind::Fix) =>
            {
                Some(Self::Task {
                    task_id: task_id.clone(),
                    kind: *kind,
                })
            }
            Effect::DispatchIntegration { kind }
                if matches!(
                    kind,
                    LeafKind::Merger
                        | LeafKind::IntegrationReview
                        | LeafKind::IntegrationFix
                        | LeafKind::CiFix
                ) =>
            {
                Some(Self::Integration { kind: *kind })
            }
            _ => None,
        }
    }

    fn into_effect(self) -> Effect {
        match self {
            Self::Planner { free_slots } => Effect::PlanNextWave { free_slots },
            Self::InboxCurator { free_slots, mode } => {
                Effect::DispatchInboxCurator { free_slots, mode }
            }
            Self::DependencyCurator { boundary } => Effect::DispatchDependencyCurator { boundary },
            Self::TaskReviewPreparation { task_id } => Effect::PrepareTaskReview { task_id },
            Self::TaskLeafPreparation { task_id, kind } => {
                Effect::PrepareTaskLeaf { task_id, kind }
            }
            Self::CiFixPreparation => Effect::PrepareCiFix,
            Self::Task { task_id, kind } => Effect::DispatchTask { task_id, kind },
            Self::Integration { kind } => Effect::DispatchIntegration { kind },
        }
    }

    pub(crate) fn ledger_key(&self) -> String {
        match self {
            Self::Planner { .. } => "check-token-budget:planner".into(),
            Self::InboxCurator { mode, .. } => {
                format!(
                    "check-token-budget:inbox-curator:{}",
                    inbox_curation_key(*mode)
                )
            }
            Self::DependencyCurator { boundary } => {
                format!(
                    "check-token-budget:dependency-curator:{}",
                    boundary.as_str()
                )
            }
            Self::TaskReviewPreparation { task_id } => {
                format!("check-token-budget:task:{task_id}:review-augment")
            }
            Self::TaskLeafPreparation { task_id, kind } => {
                format!("check-token-budget:task:{task_id}:{}-codex", kind.as_str())
            }
            Self::CiFixPreparation => "check-token-budget:integration:ci-fix-codex".into(),
            Self::Task { task_id, kind } => {
                format!("check-token-budget:task:{task_id}:{}", kind.as_str())
            }
            Self::Integration { kind } => {
                format!("check-token-budget:integration:{}", kind.as_str())
            }
        }
    }

    pub(crate) fn cohort_budget_ledger_key(&self) -> String {
        self.ledger_key()
            .replacen("check-token-budget:", "check-cohort-budget:", 1)
    }
}

fn inbox_curation_key(mode: InboxCurationMode) -> &'static str {
    match mode {
        InboxCurationMode::Intake => "intake",
        InboxCurationMode::Finalize => "finalize",
    }
}

/// Read-only result of the actual-token telemetry preflight. An unavailable snapshot is an
/// explicit fail-closed input, not an adapter error: its reducer transition records the safe
/// halt before any model call is attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenBudgetObservation {
    Actual { tokens: u64 },
    Unavailable,
}

/// Current cohort metadata.  It is preserved in the checkpoint in addition to the human-visible
/// `batch.md` because it contains no inferred values that could change during a resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortRuntime {
    pub id: String,
    pub base: String,
    pub started_at_secs: u64,
    pub wave: u32,
    pub admitted_total: u32,
    pub admission_closed: Option<CloseReasonWire>,
    /// Immutable `COHORT_BUDGET_SEC` snapshot for this cohort. An old checkpoint without this
    /// field can be resumed only when the current configuration also keeps the gate disabled.
    #[serde(default)]
    pub cohort_budget_secs: Option<u64>,
    /// Immutable `COHORT_TOKEN_BUDGET` snapshot for this cohort. An old checkpoint without this
    /// field can be resumed only when the current configuration also keeps the gate disabled.
    #[serde(default)]
    pub cohort_token_budget: Option<u64>,
    /// Immutable `COHORT_TOKEN_BUDGET_STRICT` snapshot for resume-stable admission policy.
    #[serde(default)]
    pub cohort_token_budget_strict: bool,
    /// Most recent trusted post-charge actual total. `None` means telemetry was unavailable or
    /// the cohort has not yet reached a token-gate boundary.
    #[serde(default)]
    pub token_budget_actual_tokens: Option<u64>,
    /// Immutable event-outbox setting needed to validate the token telemetry source.
    #[serde(default = "default_events_outbox_enabled")]
    pub events_outbox_enabled: bool,
}

fn default_events_outbox_enabled() -> bool {
    true
}

/// Serializable counterpart of the resolver's intentionally non-serializable `CloseReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloseReasonWire {
    CohortSize,
    CohortMaxAge,
    CohortTokenBudget,
    /// Legacy Phase 0.3b treats a missing `cohort_state.md` as closed so a restart may finish
    /// already active work but cannot admit another task into an unproven cohort.
    LegacyCohortStateAbsent,
    QueueEmpty,
    OnlyConflictsWithReady,
}

impl From<CloseReason> for CloseReasonWire {
    fn from(value: CloseReason) -> Self {
        match value {
            CloseReason::CohortSize => Self::CohortSize,
            CloseReason::CohortMaxAge => Self::CohortMaxAge,
            CloseReason::CohortTokenBudget => Self::CohortTokenBudget,
            CloseReason::QueueEmpty => Self::QueueEmpty,
            CloseReason::OnlyConflictsWithReady => Self::OnlyConflictsWithReady,
        }
    }
}

impl CloseReasonWire {
    pub fn as_legacy_literal(self) -> &'static str {
        match self {
            Self::CohortSize => "COHORT_SIZE",
            Self::CohortMaxAge => "COHORT_MAX_AGE",
            Self::CohortTokenBudget => "COHORT_TOKEN_BUDGET",
            Self::LegacyCohortStateAbsent => "LEGACY_COHORT_STATE_ABSENT",
            Self::QueueEmpty => "очередь-пуста",
            Self::OnlyConflictsWithReady => "только-конфликты-с-готовыми",
        }
    }
}

/// Integration progress across phases 4–6.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IntegrationRuntime {
    pub workspace_prepared: bool,
    /// A Phase-0 legacy import proved a material integration/report but found its registered
    /// workspace path absent.  Unlike a normal checkpoint with `workspace_prepared == false`,
    /// this requires exactly one typed workspace reconstruction before integration review can
    /// resume.  Older native checkpoints did not carry this import-only distinction.
    #[serde(default)]
    pub imported_workspace_restore_pending: bool,
    pub merged_tasks: BTreeSet<String>,
    pub f_cycles: u32,
    pub ci_cycles: u32,
    /// The optional Codex CI adapter actually yielded to the Claude fixer for the current
    /// logical iteration. This survives the reducer boundary so task telemetry can fold both
    /// provider usage records into one `operation.completed` outcome.
    #[serde(default)]
    pub ci_fix_provider_fallback: bool,
    /// Durable Phase-5.4 result for the exact published head. `None` means CI has not reached a
    /// terminal observation yet; older checkpoints predate this explicit distinction.
    #[serde(default)]
    pub ci_disposition: Option<CiDisposition>,
    /// Durable launch ledger for integration leaves. It gives telemetry events a replay-stable
    /// attempt coordinate independent of the semantic review/F/CI cycle counters.
    #[serde(default)]
    pub leaf_attempts: BTreeMap<String, u32>,
    /// Completed final-verification invocations in this batch. Unlike the semantic F-cycle
    /// counter, this coordinate never resets after a publication re-anchor.
    #[serde(default)]
    pub verification_attempts: u32,
    /// Actual publication attempts that reached either success or a proved re-anchor result.
    #[serde(default)]
    pub publish_attempts: u32,
    /// Completed Phase-5.4 CI observations, including unconfirmed waits retried after resume.
    #[serde(default)]
    pub ci_wait_attempts: u32,
    /// Completed Phase-6 archive CI reconfirmations. This mode has its own event coordinate.
    #[serde(default)]
    pub archive_ci_wait_attempts: u32,
    /// One deliberately-started typed merge which has unresolved paths.  The VCS workspace is
    /// intentionally dirty only while this coordinate exists; a recovery without this durable
    /// proof must hold rather than guess whether it is safe to abort or continue.
    #[serde(default)]
    pub pending_merge_resolution: Option<MergeResolutionRuntime>,
    pub signatures: Vec<String>,
    /// Current committed integration branch tip, updated after every merger or F-fix commit.
    pub integration_head: Option<String>,
    /// The integration tip that most recently passed a complete full-review protocol.  A later
    /// F-fix must commit before it can be re-reviewed against this durable coordinate.
    pub review_sha: Option<String>,
    /// Exact reviewed integration tip that passed the final pre-publication verification profile.
    /// A later merger or F-fix clears it, so an old successful run can never authorize a changed
    /// integration branch for publication.
    #[serde(default)]
    pub verification_head: Option<String>,
    pub published_head: Option<String>,
    /// Whether the exact published head crossed the configured remote boundary. This is retained
    /// until terminal CI projection so `cohort.published` never guesses from later configuration.
    #[serde(default)]
    pub publication_pushed: Option<bool>,
    /// A rejected remote publication was proved to have diverged after the local primary ref
    /// advanced.  This durable marker routes recovery through the idempotent re-anchor boundary
    /// rather than retrying the same push against a different remote history.
    #[serde(default)]
    pub publication_reanchor_reason: Option<String>,
    /// Which primary coordinate is authoritative for the pending re-anchor. A missing value is
    /// read as `RemotePublication` for compatibility with checkpoints written before local
    /// fast-forward divergence had its own typed recovery path.
    #[serde(default)]
    pub publication_reanchor_target: Option<PublicationReanchorTarget>,
    /// Number of primary-divergence re-anchors already admitted for this integration. This is
    /// independent from review/F cycles: an external writer can make every otherwise clean
    /// review stale, so it has its own finite convergence budget.
    #[serde(default)]
    pub publication_reanchor_cycles: u32,
    /// The Phase-6 journal/status materialization was durably acknowledged. This is separate
    /// from the cleanup-effect ledger because a process may restart after that acknowledgement
    /// and before `CleanupComplete`; replaying an append-only journal entry would otherwise
    /// duplicate the same cohort accounting record.
    #[serde(default)]
    pub cleanup_journaled: bool,
    /// The archive boundary is separate from the initial Phase-5.4 CI result: required checks
    /// must be re-observed for the exact published head after the journal and immediately before
    /// any task/control artifact is deleted.
    #[serde(default)]
    pub archive_ci_gate: Option<ArchiveCiGate>,
    /// Each dependency refresh is attempted once at its deterministic cohort boundary. The
    /// outcome may be a non-fatal degradation, but persisting the acknowledgement prevents a
    /// Phase-0 recovery from launching a second unknown curator call.
    #[serde(default)]
    pub dependency_graph_refreshed_open: bool,
    #[serde(default)]
    pub dependency_graph_refreshed_post_archive: bool,
    /// Published batches whose non-gating knowledge-curator attempt failed before writing its
    /// sentinel. The compact coordinates survive Phase-6 cleanup so a later cohort can retry the
    /// harvest without retaining task worktrees or relying on chat history.
    #[serde(default)]
    pub pending_knowledge_curations: BTreeMap<String, PendingKnowledgeCuration>,
    pub failed_reason: Option<String>,
    /// Non-fatal post-publication degradation notes (currently knowledge curation).  They are
    /// journal/status material, not a reason to strand an already-published batch.
    pub degradations: Vec<String>,
}

/// Compact retry context for a published batch whose knowledge sentinel is still absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingKnowledgeCuration {
    pub base: String,
    pub published_head: String,
    pub merged_tasks: BTreeSet<String>,
    pub fixed_task_findings: u32,
    pub integration_or_ci_signatures: u32,
    pub ci_failure_cycles: u32,
    pub quarantined_tasks: BTreeSet<String>,
    pub escalated_tasks: BTreeSet<String>,
    pub degradations: u32,
}

/// A cryptographic snapshot of one cleanly auto-merged path while a conflicting merge is
/// deliberately paused. `None` is an expected absence (for example, a recorded rename source).
/// The snapshot prevents a merger leaf from silently rewriting a non-conflicting merge path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergePathFingerprint {
    pub path: String,
    pub sha256: Option<String>,
}

/// Durable coordinates of one integration merge conflict. Paths are evidence for the merger
/// leaf and operator only; VCS finalization independently re-reads the actual conflict set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeResolutionRuntime {
    pub task_id: String,
    pub pre_merge_head: String,
    /// Typed merge surface recorded before the leaf ran. Git records all staged merge paths and
    /// rejects a model-added path at finalization; JJ currently records its exact conflict set
    /// because a multi-parent conflict has no gap-free typed range-diff surface.
    #[serde(default)]
    pub merge_paths: Vec<String>,
    pub paths: Vec<String>,
    /// Every non-conflicting path in `merge_paths`, fingerprinted after the typed merge began.
    /// An old checkpoint omits this only for the JJ compatibility case where `merge_paths` is
    /// exactly the conflict set; new Git conflicts must supply the complete complement.
    #[serde(default)]
    pub protected_paths: Vec<MergePathFingerprint>,
}

impl IntegrationRuntime {
    fn leaf_attempt(&mut self, kind: LeafKind) -> u32 {
        let attempts = self.leaf_attempts.entry(kind.as_str().into()).or_default();
        *attempts = attempts.saturating_add(1);
        *attempts
    }
}

/// The complete durable checkpoint.  Serialize it atomically after each accepted command before
/// beginning the returned effect; a later `Recover` can thus tell exactly what was pending.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorState {
    pub schema_version: u32,
    pub phase: Phase,
    pub paused_from: Option<Phase>,
    pub batch: Option<CohortRuntime>,
    pub tasks: BTreeMap<String, TaskRuntime>,
    pub integration: IntegrationRuntime,
    pub blocked_reason: Option<String>,
}

impl Default for ProcessorState {
    fn default() -> Self {
        Self {
            schema_version: PROCESSOR_STATE_VERSION,
            phase: Phase::Recovery,
            paused_from: None,
            batch: None,
            tasks: BTreeMap::new(),
            integration: IntegrationRuntime::default(),
            blocked_reason: None,
        }
    }
}

/// A structured leaf completion.  Unexpected/free-form output is converted at the adapter
/// boundary to `Escalated`, never guessed at by this reducer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafOutcome {
    Completed {
        author: Option<String>,
    },
    /// A successful task coder reported a strictly higher blast-radius classification in its
    /// machine-readable tail. Only `TaskLeaf` may consume this variant; all other leaf roles
    /// reject it rather than treating task metadata as a generic success signal.
    RiskElevated {
        author: Option<String>,
        risk: Risk,
        /// Mode-2 findings explicitly left unfixed in the same successful report. Keeping this
        /// payload optional lets risk elevation and empty-fixed-set detection remain independent.
        wont_fixed: Option<u32>,
    },
    /// A successful FIX round (task T-014) that additionally declared `wont_fixed` findings it
    /// did NOT fix via the fixer's additive `не исправлено` outcome field
    /// (`crate::contract::Outcome::wont_fix`). Kept distinct from the plain `Completed` variant so
    /// only `TaskLeaf`'s fix path — the one place that tracks a round's open-finding count
    /// (`TaskRuntime::pending_fix_open_findings`) — needs to correlate it; every other leaf role
    /// (Implement, merger, curators, ...) that can never emit it treats it exactly like an
    /// ordinary completion wherever it is matched.
    CompletedWithWontFix {
        author: Option<String>,
        wont_fixed: u32,
    },
    RetryableFailure {
        reason: String,
    },
    Escalated {
        reason: String,
    },
}

/// Structured outcome of a reviewer pass. `signature` must be a normalized deterministic
/// fingerprint (for example [`AttemptSignature::as_str`]), not arbitrary prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReviewOutcome {
    Clean {
        review_sha: String,
    },
    /// A clean reviewer pass also proved that the descriptor's informational risk is too low.
    /// The exact evidence remains in `review.md`; this typed value carries only the validated
    /// classification through the durable reducer.
    CleanRiskElevated {
        review_sha: String,
        risk: Risk,
    },
    Findings {
        signature: String,
        /// Count of open (`статус: новая`) findings this round (task T-014). Threaded through so
        /// the per-task fix path can correlate it against the fixer's own `не исправлено` count
        /// and detect an empty fixed set without waiting for a repeat review pass — see
        /// `TaskRuntime::pending_fix_open_findings` and
        /// [`crate::resolvers::empty_fixed_set_decision`]. The integration (`F-`) loop threads the
        /// same field but does not (yet) act on it; only the per-task loop (phases 2.5/2.8) does.
        /// `#[serde(default)]` (0) so a durable Codex replay receipt written before task T-014
        /// still deserializes — a defaulted 0 simply never satisfies `open_findings > 0` in
        /// [`crate::resolvers::empty_fixed_set_decision`], degrading that one round back to the
        /// unaffected `stagnation_decision` path rather than failing to load.
        #[serde(default)]
        open_findings: u32,
        /// The exact `R-`/`F-` ids counted in `open_findings` this round (R-06). Additive sibling
        /// of the bare count: it lets the per-task fix path (`TaskRuntime::pending_fix_open_finding_ids`)
        /// validate that a fixer's `не исправлено` entries actually name findings THIS round was
        /// dispatched to address, rather than trusting an unverified id it may have copied from a
        /// stale/unrelated prior finding. `#[serde(default)]` (empty) for the same replay-compat
        /// reason as `open_findings`: an old durable receipt without this field simply degrades the
        /// one round's id-membership check to "unknown", not a load failure.
        #[serde(default)]
        open_finding_ids: Vec<String>,
    },
    /// An open R-finding concurrently records a strict risk elevation. It follows the ordinary
    /// repair loop and never introduces a separate human gate.
    FindingsRiskElevated {
        signature: String,
        risk: Risk,
        /// Same meaning and same `#[serde(default)]` compatibility rationale as
        /// [`ReviewOutcome::Findings::open_findings`] (task T-014).
        #[serde(default)]
        open_findings: u32,
        /// Same meaning and compatibility rationale as [`ReviewOutcome::Findings::open_finding_ids`]
        /// (R-06).
        #[serde(default)]
        open_finding_ids: Vec<String>,
    },
    Incomplete,
    Escalated {
        reason: String,
    },
}

/// Result returned by a merger after it has run in the integration workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    Merged {
        integration_sha: String,
    },
    /// The typed VCS boundary has proved and deliberately started an abortable conflict merge.
    /// The reducer persists this before any merger model call, so restart cannot replay an
    /// unknown resolution against an arbitrary worktree state.
    NeedsResolution {
        pre_merge_head: String,
        merge_paths: Vec<String>,
        paths: Vec<String>,
        protected_paths: Vec<MergePathFingerprint>,
    },
    Quarantined {
        reason: String,
    },
    Failed {
        reason: String,
    },
}

/// Durable classification of the publication CI boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CiDisposition {
    Confirmed,
    Disabled,
    UnconfirmedDegraded,
}

/// Durable result of the Phase-6 archive preflight.  `None` means the current published head
/// has not yet crossed that boundary; a new CI repair head clears the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArchiveCiGate {
    /// No second CI observation is required by the effective PUSH/CI_WATCH/policy combination.
    Skipped,
    /// Required checks were observed green again for the exact head immediately before archive.
    Confirmed,
}

/// CI status after publication. Only `Failed` proves a red required check and therefore carries
/// a normalized failure signature for the shared stagnation detector. Missing/outage/timeout and
/// an optional best-effort watch are distinct so neither can dispatch an unjustified CI fixer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiOutcome {
    Passed,
    LocalOnly,
    Disabled,
    BestEffortDegraded { reason: String },
    RequiredUnconfirmed { reason: String },
    Failed { signature: String, reason: String },
}

/// The authoritative primary coordinate for a bounded publication re-anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PublicationReanchorTarget {
    /// The local fast-forward succeeded, but the subsequent typed push did not make the
    /// integration durable. Re-anchor only after fetching and proving the remote divergence.
    RemotePublication,
    /// The local fast-forward itself failed because an external writer advanced the primary.
    /// Keep that local primary target intact and replay the integration on top of it.
    LocalPrimary,
}

/// Result of the optional Codex Mode-3 CI repair. A disabled route, runner failure, or explicit
/// fallback sentinel reaches the separately gated Claude repair; a completed or protocol-terminal
/// outcome remains explicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiFixPreparationOutcome {
    Skipped,
    Completed,
    Fallback,
    /// A no-model sandbox probe proved that this session's host cannot start the Codex
    /// restricted-token sandbox. The reducer records the degradation before dispatching the
    /// ordinary Claude CI fixer.
    SandboxDowngraded {
        scope: CodexSandboxDowngrade,
    },
    Escalated {
        reason: String,
    },
}

/// Scope of a live, session-local Codex sandbox preflight downgrade. These are deliberately
/// closed values: operator status and journal text must never incorporate a child transcript.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodexSandboxDowngrade {
    /// The restricted-token sandbox cannot start on this host; all Codex routes are disabled for
    /// the rest of this native engine session.
    Host,
    /// Only the nested task-worktree shape is rejected; reviewer and main-tree CI routes remain
    /// eligible.
    Worktree,
}

impl CodexSandboxDowngrade {
    fn degradation(self) -> &'static str {
        match self {
            Self::Host => {
                "Codex sandbox-init preflight: live host limit; Codex routing downgraded for this session"
            }
            Self::Worktree => {
                "Codex sandbox-init-worktree preflight: live task-worktree limit; Codex coder routing downgraded for this session"
            }
        }
    }
}

/// Result of the deterministic Phase-5.5 preflight.  The filesystem/VCS adapter decides whether
/// this cohort has any harvest or invalidation input before the reducer spends a model call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCurationPreparationOutcome {
    Required,
    /// Every deferred/current sentinel already exists, typically after a crash between the
    /// curator write and reducer acknowledgement. Clear retry metadata without another model.
    AlreadyCompleted,
    Skipped,
}

/// Native Phase-6 policy preflight result.  Policy and effective runtime switches stay outside
/// the reducer, while the resulting archive authority is checkpointed here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchivalPreparationOutcome {
    ReconfirmRequired { required_checks: Vec<String> },
    Skipped,
}

/// Result of the optional Codex implementation/fix attempt. A disabled/ineligible route,
/// runner failure, or explicit fallback sentinel reaches the separately gated Claude maker;
/// a protocol-terminal outcome is an escalation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskLeafPreparationOutcome {
    Skipped,
    Completed,
    /// Codex completed a fix round and explicitly left findings unfixed. This mirrors
    /// [`LeafOutcome::CompletedWithWontFix`] across the preparation/replay boundary.
    CompletedWithWontFix {
        wont_fixed: u32,
    },
    /// Codex completed a task and reported a strict risk elevation. The reducer records it
    /// before committing, exactly as it does for the ordinary Claude path.
    RiskElevated {
        risk: Risk,
        wont_fixed: Option<u32>,
    },
    Fallback,
    SandboxDowngraded {
        scope: CodexSandboxDowngrade,
    },
    Escalated {
        reason: String,
    },
}

/// Result of the optional Codex task-review preparation.  A full Codex review may satisfy the
/// authoritative gate itself; its sentinel falls through to a separately dispatched Claude
/// reviewer, while an augment pass is intentionally non-authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskReviewPreparationOutcome {
    DispatchClaude,
    SandboxDowngraded { scope: CodexSandboxDowngrade },
    Completed(ReviewOutcome),
}

/// Result of the mandatory local verification profile immediately before publication of the
/// final full-reviewed integration tip. `Exempt` is an explicit `VERIFICATION_MODE=disabled`
/// decision, never an inference from missing evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationOutcome {
    Passed,
    Exempt { reason: String },
    Failed { signature: String, reason: String },
    Blocked { reason: String },
}

/// One externally observed, fully structured processor input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorCommand {
    /// Establish a new cohort after the recovery gate has reached `Idle`.
    Open {
        batch_id: String,
        base: String,
        now_secs: u64,
    },
    /// Restore a persisted state after verifying the physical control-plane/VCS observations.
    /// `workspaces_present` supplies the task ids proven to have their managed workspace.
    Recover {
        workspaces_present: BTreeSet<String>,
    },
    /// Admit a rolling wave. Only candidates that the resolver selects produce workspace effects.
    Admit {
        candidates: Vec<AdmissionCandidate>,
        now_secs: u64,
    },
    /// The mechanical inbox reconciliation completed before a planner wave. `curation_required`
    /// is derived by the native adapter from the validated actionable projection, never from
    /// message prose in the reducer.
    InboxReconciled {
        free_slots: usize,
        curation_required: bool,
    },
    /// The post-archive inbox reconciliation completed. Unlike planner-wave reconciliation it
    /// considers only terminal conversations that must be completed or have a missing final
    /// reply, and it never opens another admission slot.
    InboxFinalizationReconciled { curation_required: bool },
    /// A dependency curator returned after its candidate was validated and atomically graph-synced
    /// by the native adapter. Failure is a recorded release-notification degradation, not a
    /// license for a manual registry edit or a reason to strand an otherwise independent cohort.
    DependencyGraphRefreshed {
        boundary: RefreshBoundary,
        outcome: LeafOutcome,
    },
    /// A checkpointed inbox curator either completed its narrow intake/finalization operation or
    /// produced a terminal, explicit reason. A transient process failure is deliberately held
    /// rather than retried blindly after an unknown cross-repository side effect.
    InboxCurated {
        free_slots: usize,
        mode: InboxCurationMode,
        outcome: LeafOutcome,
    },
    /// Queue-inbox records were consumed and inbox provenance was reconciled after curation.
    /// This is the only transition that is allowed to reach the planner for the same wave.
    InboxDrained { free_slots: usize },
    /// A read-only token telemetry check for one exact pending model continuation.
    TokenBudgetChecked {
        next: ModelCall,
        observation: TokenBudgetObservation,
    },
    /// A read-only wall-clock check immediately before one exact model continuation. The
    /// reducer records the cohort's configured budget in its checkpoint, so a resume cannot
    /// silently run with a changed `COHORT_BUDGET_SEC` policy.
    CohortBudgetChecked { next: ModelCall, now_secs: u64 },
    /// The VCS guard created or verified the requested managed task workspace.
    WorkspaceReady { task_id: String },
    /// The VCS guard refused/failed the requested workspace; this is a clean task escalation.
    WorkspaceFailed { task_id: String, reason: String },
    /// Optional Codex implementation/fix completed, was ineligible, or requested Claude
    /// fallback.  The ordinary Claude leaf is intentionally a separate command/effect.
    TaskLeafPrepared {
        task_id: String,
        outcome: TaskLeafPreparationOutcome,
    },
    /// A coder/fixer leaf completed, retried transiently, or escalated.
    TaskLeaf {
        task_id: String,
        outcome: LeafOutcome,
    },
    /// The task's VCS commit returned a durable commit/change id.
    TaskCommitted { task_id: String, commit: String },
    /// Optional Codex review preparation completed. A full Codex review may carry its verified
    /// outcome; otherwise Claude is scheduled separately so a token preflight sits between them.
    TaskReviewPrepared {
        task_id: String,
        outcome: TaskReviewPreparationOutcome,
    },
    /// A per-task reviewer result.
    TaskReview {
        task_id: String,
        outcome: ReviewOutcome,
    },
    /// Re-evaluate age/budget and move to the join barrier once phase 2 has quiesced.
    Advance { now_secs: u64 },
    /// The integration worktree/workspace was created and root-verified.
    IntegrationWorkspaceReady,
    /// One typed merger result.
    TaskMerged {
        task_id: String,
        outcome: MergeOutcome,
    },
    /// The checkpointed merger leaf completed against the exact typed conflict state.
    MergeResolution {
        task_id: String,
        outcome: LeafOutcome,
    },
    /// Typed VCS finalization committed/recorded the resolved merge and, where configured,
    /// completed its per-merge verification/rollback boundary.
    MergeResolutionFinalized {
        task_id: String,
        outcome: MergeOutcome,
    },
    /// Typed VCS abort restored the exact pre-merge integration tip after the merger leaf could
    /// not produce a trustworthy resolution.
    MergeResolutionAborted { task_id: String, reason: String },
    /// The configured final pre-publication verification profile completed for this exact
    /// integration tip. The SHA travels back with the result so stale evidence cannot authorize
    /// a later F-fix or merge.
    IntegrationVerified {
        head: String,
        outcome: VerificationOutcome,
    },
    /// One typed integration-review result.
    IntegrationReview { outcome: ReviewOutcome },
    /// A full-review finding was fixed and committed in the integration workspace.
    IntegrationFix { outcome: LeafOutcome },
    /// The VCS boundary committed an F-fix and returned the new integration tip.
    IntegrationFixCommitted { head: String },
    /// Publication (fast-forward/move main and push) has completed.
    Published { head: String, pushed: bool },
    /// A durable policy approval was rejected, expired, or became stale before publication. The
    /// native port has not moved the primary checkout; the reducer terminally escalates exactly
    /// the affected merged cohort and cleans its isolated integration surface.
    PublicationRejected { reason: String },
    /// A durable policy approval is still undecided. This acknowledges the safe, read-only
    /// publication probe and stops the current invocation; Phase-0 recovery later schedules the
    /// same verified publish effect again so an external operator decision can be observed.
    PublicationAwaitingApproval { reason: String },
    /// A typed publication boundary proved either a rejected remote push or a local primary
    /// divergence. The durable target tells the following effect whether a freshly proved remote
    /// ref or the externally advanced local primary is authoritative before merger/review/
    /// verification starts again.
    PublicationReanchorRequired {
        reason: String,
        target: PublicationReanchorTarget,
    },
    /// The retry-safe re-anchor effect either reset only the remote-rejected candidate, retained
    /// an externally advanced local primary, or discovered that a concurrent publication already
    /// made the exact integration durable.
    PublicationReanchored,
    /// Required CI result for the published head.
    CiVerified { outcome: CiOutcome },
    /// A CI repair agent result.
    CiFix { outcome: LeafOutcome },
    /// Optional Codex Mode-3 CI repair completed, was disabled, or requested Claude fallback.
    CiFixPrepared { outcome: CiFixPreparationOutcome },
    /// The VCS/forge boundary committed and published the CI repair, returning the exact head
    /// whose required checks must now be verified.
    CiFixCommitted { head: String },
    /// The typed filesystem/VCS preflight decided whether Phase 5.5 has useful work.
    KnowledgeCurationPrepared {
        outcome: KnowledgeCurationPreparationOutcome,
    },
    /// Knowledge curator completed after a `Required` preflight.
    KnowledgeCurated { outcome: LeafOutcome },
    /// The native policy preflight decided whether the exact published head needs a second
    /// required-CI observation immediately before physical archival.
    ArchivalPrepared { outcome: ArchivalPreparationOutcome },
    /// Required checks were re-observed for the exact head named by the originating effect.
    ArchiveCiReconfirmed { head: String, outcome: CiOutcome },
    /// All required task workspace/descriptor/archive cleanup has completed.
    CleanupComplete,
    /// An operator PAUSE marker was observed at an effect boundary.
    Pause,
    /// Resume from the exact phase captured by [`ProcessorCommand::Pause`].
    Resume,
    /// A non-recoverable adapter contradiction; fail closed and retain the reason.
    Block { reason: String },
}

/// A concrete ordered effect for the impure runner.  Effects are deliberately narrow enough to
/// map directly onto ProcessKit leaf calls, `vcs-*` operations, durable state writes and TUI events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    PersistCheckpoint,
    Reconcile {
        task_id: String,
    },
    /// Reconcile the optional cross-project inbox and inspect its actionable projection before a
    /// planner wave. This effect is idempotent and uses only a checkpointed cohort timestamp.
    ReconcileInbox {
        free_slots: usize,
    },
    /// Reconcile inbox provenance after archival and inspect only the finalization projection.
    /// The effect is separate from a rolling wave so a final reply can never accidentally admit
    /// another task into a cohort that is already being cleaned up.
    ReconcileInboxFinalization,
    /// Build and atomically apply the current project's dependency graph snapshot. The external
    /// curator may create only a candidate under `.work`; native code owns registry validation,
    /// CAS and replacement.
    DispatchDependencyCurator {
        boundary: RefreshBoundary,
    },
    /// Invoke the critical inbox curator only when validated actionable inbox records exist.
    /// Its output may only populate `.work/queue_inbox`; a later native drain owns queue writes.
    DispatchInboxCurator {
        free_slots: usize,
        mode: InboxCurationMode,
    },
    /// Drain `.work/queue_inbox` and reconnect `Inbox message: msg-*` provenance before the
    /// planner observes the queue. The native transaction makes this retry-safe.
    DrainQueueInbox {
        free_slots: usize,
    },
    PlanNextWave {
        free_slots: usize,
    },
    /// Durable preflight directly before a planner/coder/reviewer/fix/full-reviewer/CI-fix model
    /// call. The contained continuation is executed only after a trusted under-limit result.
    CheckTokenBudget {
        next: ModelCall,
    },
    /// Durable wall-clock preflight before every model call when `COHORT_BUDGET_SEC` is enabled.
    /// It runs before the token gate so an expired cohort cannot start another provider process.
    CheckCohortBudget {
        next: ModelCall,
    },
    EnsureTaskWorkspace {
        task_id: String,
        branch: String,
    },
    /// Run at most one optional diversity reviewer before the authoritative per-task review.
    /// It has its own durable boundary because it can itself be a model call.
    PrepareTaskReview {
        task_id: String,
    },
    /// Run an optional Codex implementation/fix before the ordinary Claude maker fallback.
    /// Its own durable boundary keeps the token gate between the two model invocations.
    PrepareTaskLeaf {
        task_id: String,
        kind: LeafKind,
    },
    DispatchTask {
        task_id: String,
        kind: LeafKind,
    },
    CommitTask {
        task_id: String,
    },
    PrepareIntegrationWorkspace {
        branch: String,
    },
    MergeTask {
        task_id: String,
    },
    FinalizeMergeResolution {
        task_id: String,
    },
    AbortMergeResolution {
        task_id: String,
        reason: String,
    },
    VerifyIntegration {
        head: String,
    },
    DispatchIntegration {
        kind: LeafKind,
    },
    CommitIntegrationFix,
    Publish {
        batch_id: String,
    },
    /// Re-anchor an un-published integration after a remote divergence.  The concrete port must
    /// prove the current remote relation before it resets the primary checkout, and must preserve
    /// every task branch/workspace for the ordinary re-integration path.
    ReanchorPublication {
        batch_id: String,
    },
    VerifyCi {
        head: String,
    },
    /// Send a best-effort, redacted operator notification only after the triggering command has
    /// entered the durable runtime ledger. Its port is explicitly non-gating and holds an
    /// at-most-once receipt, so recovery cannot repeat a delivered alert.
    Notify {
        event: NotificationEvent,
        subject: String,
    },
    /// Optional Codex Mode-3 CI repair before the ordinary Claude repair fallback.
    PrepareCiFix,
    CommitCiFix,
    /// Inspect the configured KB, batch sentinel, durable findings and exact batch diff before
    /// deciding whether a knowledge-curator model call is useful.
    PrepareKnowledgeCuration,
    /// Resolve current policy/runtime switches only after the Phase-6 journal was acknowledged.
    PrepareArchival,
    /// Re-observe required checks for the exact durable publication head before deletion.
    ReconfirmCiBeforeArchive {
        head: String,
        required_checks: Vec<String>,
    },
    ReturnTask {
        task_id: String,
        reason: String,
    },
    /// Persist a terminal escalation in the descriptor and queue without re-queuing it. This is
    /// distinct from merge quarantine and is used by the token-safety terminal boundary.
    EscalateTask {
        task_id: String,
        reason: String,
    },
    ArchiveTask {
        task_id: String,
    },
    CleanupTaskWorkspace {
        task_id: String,
    },
    CleanupIntegrationWorkspace,
    CleanupCohortControlPlane,
    WriteJournalAndStatus,
    ReleaseLease,
    WaitForOperator {
        reason: String,
    },
}

/// Reducer failure: an impossible or stale external result is not silently tolerated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorError {
    InvalidConfig(String),
    InvalidCommand(String),
    MissingTask(String),
    UnexpectedTaskPhase {
        task_id: String,
        expected: &'static str,
        actual: TaskPhase,
    },
    CorruptCheckpoint(String),
}

impl std::fmt::Display for ProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(message)
            | Self::InvalidCommand(message)
            | Self::CorruptCheckpoint(message) => f.write_str(message),
            Self::MissingTask(task_id) => write!(f, "unknown task {task_id}"),
            Self::UnexpectedTaskPhase {
                task_id,
                expected,
                actual,
            } => write!(
                f,
                "task {task_id} is {actual:?}; expected state compatible with {expected}"
            ),
        }
    }
}

impl std::error::Error for ProcessorError {}

/// The deterministic processor reducer.
#[derive(Debug, Clone)]
pub struct Processor {
    config: ProcessorConfig,
    state: ProcessorState,
    /// The durable phase before this process entered phase 0. This is intentionally not
    /// serialized: every new process must pass the recovery gate before dispatching work again.
    recovery_target: Option<Phase>,
}

impl Processor {
    pub fn new(config: ProcessorConfig) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self {
            config,
            state: ProcessorState::default(),
            recovery_target: None,
        })
    }

    pub fn from_checkpoint(
        config: ProcessorConfig,
        mut state: ProcessorState,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        if state.schema_version != PROCESSOR_STATE_VERSION {
            return Err(ProcessorError::CorruptCheckpoint(format!(
                "unsupported processor checkpoint version {}",
                state.schema_version
            )));
        }
        if let Some(batch) = &state.batch {
            validate_batch_id(&batch.id).map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid active batch id {:?}: {error}",
                    batch.id
                ))
            })?;
            validate_ref(&batch.base, "active batch base").map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid active batch {}: {error}",
                    batch.id
                ))
            })?;
            if batch.wave == 0 {
                return Err(ProcessorError::CorruptCheckpoint(format!(
                    "active batch {} has zero wave",
                    batch.id
                )));
            }
        }
        for (stored_id, task) in &state.tasks {
            validate_task_id(stored_id).map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid task map key {stored_id:?}: {error}"
                ))
            })?;
            validate_task_id(&task.id).map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!("invalid task id {:?}: {error}", task.id))
            })?;
            if stored_id != &task.id {
                return Err(ProcessorError::CorruptCheckpoint(format!(
                    "task map key {stored_id:?} does not match embedded id {:?}",
                    task.id
                )));
            }
            if task.wave == 0 {
                return Err(ProcessorError::CorruptCheckpoint(format!(
                    "task {} has zero admission wave",
                    task.id
                )));
            }
            if task.conflict_domain.trim().is_empty() {
                return Err(ProcessorError::CorruptCheckpoint(format!(
                    "task {} has an empty conflict domain",
                    task.id
                )));
            }
            for (label, coordinate) in [
                ("previous review", task.previous_review_sha.as_deref()),
                ("review", task.review_sha.as_deref()),
            ] {
                if let Some(coordinate) = coordinate {
                    validate_ref(coordinate, label).map_err(|error| {
                        ProcessorError::CorruptCheckpoint(format!(
                            "invalid {label} coordinate for {}: {error}",
                            task.id
                        ))
                    })?;
                }
            }
        }
        for (label, coordinate) in [
            ("integration", state.integration.integration_head.as_deref()),
            (
                "integration review",
                state.integration.review_sha.as_deref(),
            ),
            (
                "integration verification",
                state.integration.verification_head.as_deref(),
            ),
            ("publication", state.integration.published_head.as_deref()),
        ] {
            if let Some(coordinate) = coordinate {
                validate_ref(coordinate, label).map_err(|error| {
                    ProcessorError::CorruptCheckpoint(format!(
                        "invalid {label} coordinate: {error}"
                    ))
                })?;
            }
        }
        for task_id in &state.integration.merged_tasks {
            validate_task_id(task_id).map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid merged task id {task_id:?}: {error}"
                ))
            })?;
            if !state.tasks.contains_key(task_id) {
                return Err(ProcessorError::CorruptCheckpoint(format!(
                    "merged task set references missing task {task_id}"
                )));
            }
        }
        if let Some(pending) = &state.integration.pending_merge_resolution {
            validate_task_id(&pending.task_id).map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid pending merge task {:?}: {error}",
                    pending.task_id
                ))
            })?;
            validate_ref(&pending.pre_merge_head, "pending merge base").map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid pending merge for {}: {error}",
                    pending.task_id
                ))
            })?;
            let task = state.tasks.get(&pending.task_id).ok_or_else(|| {
                ProcessorError::CorruptCheckpoint(format!(
                    "pending merge references missing task {}",
                    pending.task_id
                ))
            })?;
            if task.phase != TaskPhase::ResolvingMerge {
                return Err(ProcessorError::CorruptCheckpoint(format!(
                    "pending merge task {} is {:?}, not ResolvingMerge",
                    pending.task_id, task.phase
                )));
            }
        }
        if let Some(batch) = &state.batch
            && (batch.cohort_budget_secs != config.cohort_budget_secs
                || batch.cohort_token_budget != config.cohort_token_budget
                || batch.cohort_token_budget_strict != config.cohort_token_budget_strict
                || batch.events_outbox_enabled != config.events_outbox_enabled)
        {
            return Err(ProcessorError::CorruptCheckpoint(
                "processor configuration affecting cohort safety differs from the active cohort snapshot"
                    .into(),
            ));
        }
        for (batch_id, pending) in &state.integration.pending_knowledge_curations {
            validate_batch_id(batch_id).map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid deferred knowledge batch {batch_id:?}: {error}"
                ))
            })?;
            validate_ref(&pending.base, "deferred knowledge base").map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid deferred knowledge batch {batch_id}: {error}"
                ))
            })?;
            validate_ref(&pending.published_head, "deferred knowledge head").map_err(|error| {
                ProcessorError::CorruptCheckpoint(format!(
                    "invalid deferred knowledge batch {batch_id}: {error}"
                ))
            })?;
            for task_id in pending
                .merged_tasks
                .iter()
                .chain(&pending.quarantined_tasks)
                .chain(&pending.escalated_tasks)
            {
                validate_task_id(task_id).map_err(|error| {
                    ProcessorError::CorruptCheckpoint(format!(
                        "invalid deferred knowledge task {task_id:?} in {batch_id}: {error}"
                    ))
                })?;
            }
        }
        if let Some(gate) = state.integration.archive_ci_gate {
            let cleaning = state.phase == Phase::Cleaning
                || (state.phase == Phase::Paused && state.paused_from == Some(Phase::Cleaning));
            if !cleaning || state.batch.is_none() || !state.integration.cleanup_journaled {
                return Err(ProcessorError::CorruptCheckpoint(
                    "archive CI gate exists outside a journaled active cleaning boundary".into(),
                ));
            }
            if state
                .tasks
                .values()
                .any(|task| task.phase == TaskPhase::Merged)
            {
                return Err(ProcessorError::CorruptCheckpoint(
                    "archive CI gate exists while merged work is still unpublished".into(),
                ));
            }
            for task_id in &state.integration.merged_tasks {
                let task = state.tasks.get(task_id).ok_or_else(|| {
                    ProcessorError::CorruptCheckpoint(format!(
                        "archive CI gate references missing task {task_id}"
                    ))
                })?;
                if task.phase != TaskPhase::Published {
                    return Err(ProcessorError::CorruptCheckpoint(format!(
                        "archive CI gate references {task_id} in {:?}, not Published",
                        task.phase
                    )));
                }
            }
            if let Some(task) = state.tasks.values().find(|task| {
                task.phase == TaskPhase::Published
                    && !state.integration.merged_tasks.contains(&task.id)
            }) {
                return Err(ProcessorError::CorruptCheckpoint(format!(
                    "archive CI gate omits published task {}",
                    task.id
                )));
            }
            if !state.integration.merged_tasks.is_empty()
                && (state.integration.published_head.is_none()
                    || state.integration.publication_pushed.is_none()
                    || state.integration.ci_disposition.is_none())
            {
                return Err(ProcessorError::CorruptCheckpoint(
                    "archive CI gate lacks its terminal publication context".into(),
                ));
            }
            if gate == ArchiveCiGate::Confirmed
                && (state.integration.publication_pushed != Some(true)
                    || state.integration.ci_disposition != Some(CiDisposition::Confirmed)
                    || state.integration.published_head.is_none())
            {
                return Err(ProcessorError::CorruptCheckpoint(
                    "confirmed archive CI gate lacks its exact remote publication evidence".into(),
                ));
            }
            if gate == ArchiveCiGate::Confirmed {
                validate_ref(
                    state
                        .integration
                        .published_head
                        .as_deref()
                        .unwrap_or_default(),
                    "confirmed archive CI head",
                )
                .map_err(|error| {
                    ProcessorError::CorruptCheckpoint(format!(
                        "invalid confirmed archive CI evidence: {error}"
                    ))
                })?;
            }
        }
        let recovery_target = state.phase;
        state.phase = Phase::Recovery;
        Ok(Self {
            config,
            state,
            recovery_target: Some(recovery_target),
        })
    }

    pub fn state(&self) -> &ProcessorState {
        &self.state
    }

    /// Apply one structured input. The caller must persist the returned state before carrying out
    /// any returned effect; every non-empty result therefore begins with `PersistCheckpoint`.
    pub fn apply(&mut self, command: ProcessorCommand) -> Result<Vec<Effect>, ProcessorError> {
        let was_model_budget_check = matches!(
            &command,
            ProcessorCommand::TokenBudgetChecked { .. }
                | ProcessorCommand::CohortBudgetChecked { .. }
        );
        if matches!(self.state.phase, Phase::Blocked)
            && !matches!(command, ProcessorCommand::Recover { .. })
        {
            return Ok(vec![Effect::WaitForOperator {
                reason: self
                    .state
                    .blocked_reason
                    .clone()
                    .unwrap_or_else(|| "processor is blocked".into()),
            }]);
        }
        if matches!(self.state.phase, Phase::Paused)
            && !matches!(
                command,
                ProcessorCommand::Resume | ProcessorCommand::Block { .. }
            )
        {
            return Ok(vec![Effect::WaitForOperator {
                reason: "PAUSE is active".into(),
            }]);
        }

        let mut effects = match command {
            ProcessorCommand::Pause => self.pause()?,
            ProcessorCommand::Resume => self.resume()?,
            ProcessorCommand::Block { reason } => {
                self.state.phase = Phase::Blocked;
                self.state.blocked_reason = Some(reason.clone());
                vec![Effect::WaitForOperator { reason }]
            }
            ProcessorCommand::Recover { workspaces_present } => self.recover(workspaces_present)?,
            ProcessorCommand::Open {
                batch_id,
                base,
                now_secs,
            } => self.open(batch_id, base, now_secs)?,
            ProcessorCommand::Admit {
                candidates,
                now_secs,
            } => self.admit(candidates, now_secs)?,
            ProcessorCommand::InboxReconciled {
                free_slots,
                curation_required,
            } => self.inbox_reconciled(free_slots, curation_required)?,
            ProcessorCommand::InboxFinalizationReconciled { curation_required } => {
                self.inbox_finalization_reconciled(curation_required)?
            }
            ProcessorCommand::DependencyGraphRefreshed { boundary, outcome } => {
                self.dependency_graph_refreshed(boundary, outcome)?
            }
            ProcessorCommand::InboxCurated {
                free_slots,
                mode,
                outcome,
            } => self.inbox_curated(free_slots, mode, outcome)?,
            ProcessorCommand::InboxDrained { free_slots } => self.inbox_drained(free_slots)?,
            ProcessorCommand::TokenBudgetChecked { next, observation } => {
                self.token_budget_checked(next, observation)?
            }
            ProcessorCommand::CohortBudgetChecked { next, now_secs } => {
                self.cohort_budget_checked(next, now_secs)?
            }
            ProcessorCommand::WorkspaceReady { task_id } => self.workspace_ready(&task_id)?,
            ProcessorCommand::WorkspaceFailed { task_id, reason } => {
                self.workspace_failed(&task_id, reason)?
            }
            ProcessorCommand::TaskLeafPrepared { task_id, outcome } => {
                self.task_leaf_prepared(&task_id, outcome)?
            }
            ProcessorCommand::TaskLeaf { task_id, outcome } => self.task_leaf(&task_id, outcome)?,
            ProcessorCommand::TaskCommitted { task_id, commit } => {
                self.task_committed(&task_id, commit)?
            }
            ProcessorCommand::TaskReviewPrepared { task_id, outcome } => {
                self.task_review_prepared(&task_id, outcome)?
            }
            ProcessorCommand::TaskReview { task_id, outcome } => {
                self.task_review(&task_id, outcome)?
            }
            ProcessorCommand::Advance { now_secs } => self.advance(now_secs)?,
            ProcessorCommand::IntegrationWorkspaceReady => self.integration_workspace_ready()?,
            ProcessorCommand::TaskMerged { task_id, outcome } => {
                self.task_merged(&task_id, outcome)?
            }
            ProcessorCommand::MergeResolution { task_id, outcome } => {
                self.merge_resolution(&task_id, outcome)?
            }
            ProcessorCommand::MergeResolutionFinalized { task_id, outcome } => {
                self.merge_resolution_finalized(&task_id, outcome)?
            }
            ProcessorCommand::MergeResolutionAborted { task_id, reason } => {
                self.merge_resolution_aborted(&task_id, reason)?
            }
            ProcessorCommand::IntegrationVerified { head, outcome } => {
                self.integration_verified(head, outcome)?
            }
            ProcessorCommand::IntegrationReview { outcome } => self.integration_review(outcome)?,
            ProcessorCommand::IntegrationFix { outcome } => self.integration_fix(outcome)?,
            ProcessorCommand::IntegrationFixCommitted { head } => {
                self.integration_fix_committed(head)?
            }
            ProcessorCommand::Published { head, pushed } => self.published(head, pushed)?,
            ProcessorCommand::PublicationRejected { reason } => {
                self.publication_rejected(reason)?
            }
            ProcessorCommand::PublicationAwaitingApproval { reason } => {
                self.publication_awaiting_approval(reason)?
            }
            ProcessorCommand::PublicationReanchorRequired { reason, target } => {
                self.publication_reanchor_required(reason, target)?
            }
            ProcessorCommand::PublicationReanchored => self.publication_reanchored()?,
            ProcessorCommand::CiVerified { outcome } => self.ci_verified(outcome)?,
            ProcessorCommand::CiFix { outcome } => self.ci_fix(outcome)?,
            ProcessorCommand::CiFixPrepared { outcome } => self.ci_fix_prepared(outcome)?,
            ProcessorCommand::CiFixCommitted { head } => self.ci_fix_committed(head)?,
            ProcessorCommand::KnowledgeCurationPrepared { outcome } => {
                self.knowledge_curation_prepared(outcome)?
            }
            ProcessorCommand::KnowledgeCurated { outcome } => self.knowledge_curated(outcome)?,
            ProcessorCommand::ArchivalPrepared { outcome } => self.archival_prepared(outcome)?,
            ProcessorCommand::ArchiveCiReconfirmed { head, outcome } => {
                self.archive_ci_reconfirmed(head, outcome)?
            }
            ProcessorCommand::CleanupComplete => self.cleanup_complete()?,
        };
        // A budget check returns the original continuation only after it has been durably
        // acknowledged. Do not wrap that continuation a second time; every other model-bearing
        // effect is guarded here in one place, including retry and resume paths.
        if !was_model_budget_check {
            effects = self.guard_model_effects(effects);
        }
        if !effects.is_empty() && !matches!(effects.first(), Some(Effect::WaitForOperator { .. })) {
            effects.insert(0, Effect::PersistCheckpoint);
        }
        Ok(effects)
    }

    fn guard_model_effects(&self, effects: Vec<Effect>) -> Vec<Effect> {
        effects
            .into_iter()
            .map(|effect| match ModelCall::from_effect(&effect) {
                Some(next) if self.config.cohort_budget_secs.is_some() => {
                    Effect::CheckCohortBudget { next }
                }
                Some(next) if self.config.cohort_token_budget.is_some() => {
                    Effect::CheckTokenBudget { next }
                }
                None => effect,
                Some(_) => effect,
            })
            .collect()
    }

    fn cohort_budget_checked(
        &mut self,
        next: ModelCall,
        now_secs: u64,
    ) -> Result<Vec<Effect>, ProcessorError> {
        let limit = self.config.cohort_budget_secs.ok_or_else(|| {
            ProcessorError::InvalidCommand(
                "received cohort-budget check while COHORT_BUDGET_SEC is disabled".into(),
            )
        })?;
        let batch = self.state.batch.as_ref().ok_or_else(|| {
            ProcessorError::InvalidCommand("cohort-budget check requires an active cohort".into())
        })?;
        if batch.cohort_budget_secs != Some(limit) {
            return Err(ProcessorError::CorruptCheckpoint(
                "active cohort wall-clock budget snapshot differs from processor configuration"
                    .into(),
            ));
        }
        let elapsed = now_secs.saturating_sub(batch.started_at_secs);
        if elapsed >= limit {
            return self.halt_for_cohort_budget(format!(
                "COHORT_BUDGET_SEC elapsed={elapsed} limit={limit}"
            ));
        }
        Ok(vec![if self.config.cohort_token_budget.is_some() {
            Effect::CheckTokenBudget { next }
        } else {
            next.into_effect()
        }])
    }

    fn token_budget_checked(
        &mut self,
        next: ModelCall,
        observation: TokenBudgetObservation,
    ) -> Result<Vec<Effect>, ProcessorError> {
        let limit = self.config.cohort_token_budget.ok_or_else(|| {
            ProcessorError::InvalidCommand(
                "received token-budget check while COHORT_TOKEN_BUDGET is disabled".into(),
            )
        })?;
        let batch = self.state.batch.as_mut().ok_or_else(|| {
            ProcessorError::InvalidCommand("token-budget check requires an active cohort".into())
        })?;
        if batch.cohort_token_budget != Some(limit) {
            return Err(ProcessorError::CorruptCheckpoint(
                "active cohort token-budget snapshot differs from processor configuration".into(),
            ));
        }

        match observation {
            TokenBudgetObservation::Actual { tokens } if tokens < limit => {
                batch.token_budget_actual_tokens = Some(tokens);
                Ok(vec![next.into_effect()])
            }
            TokenBudgetObservation::Actual { tokens } => {
                batch.token_budget_actual_tokens = Some(tokens);
                self.halt_for_token_budget(format!(
                    "COHORT_TOKEN_BUDGET actual={tokens} limit={limit}"
                ))
            }
            TokenBudgetObservation::Unavailable => {
                batch.token_budget_actual_tokens = None;
                self.halt_for_token_budget("COHORT_TOKEN_BUDGET telemetry-unavailable".into())
            }
        }
    }

    fn halt_for_token_budget(&mut self, reason: String) -> Result<Vec<Effect>, ProcessorError> {
        if self
            .state
            .batch
            .as_ref()
            .is_some_and(|batch| batch.admission_closed.is_none())
        {
            self.close_admission(CloseReason::CohortTokenBudget);
        }

        let active_ids: Vec<String> = self
            .state
            .tasks
            .values()
            .filter(|task| task.phase.is_active())
            .map(|task| task.id.clone())
            .collect();
        let mut effects = Vec::new();
        for task_id in active_ids {
            let task = self.task_mut(&task_id)?;
            task.phase = TaskPhase::Escalated;
            task.reason = Some(reason.clone());
            effects.push(Effect::EscalateTask {
                task_id,
                reason: reason.clone(),
            });
        }
        effects.extend(self.after_admission_closed(0)?);
        Ok(effects)
    }

    fn halt_for_cohort_budget(&mut self, reason: String) -> Result<Vec<Effect>, ProcessorError> {
        if self
            .state
            .batch
            .as_ref()
            .is_some_and(|batch| batch.admission_closed.is_none())
        {
            self.close_admission(CloseReason::CohortMaxAge);
        }

        let active_ids: Vec<String> = self
            .state
            .tasks
            .values()
            .filter(|task| task.phase.is_active())
            .map(|task| task.id.clone())
            .collect();
        let mut effects = Vec::new();
        for task_id in active_ids {
            let task = self.task_mut(&task_id)?;
            task.phase = TaskPhase::Escalated;
            task.reason = Some(reason.clone());
            effects.push(Effect::EscalateTask {
                task_id,
                reason: reason.clone(),
            });
        }
        effects.extend(self.after_admission_closed(0)?);
        Ok(effects)
    }

    fn pause(&mut self) -> Result<Vec<Effect>, ProcessorError> {
        if matches!(self.state.phase, Phase::Paused) {
            return Ok(vec![Effect::WaitForOperator {
                reason: "PAUSE is already active".into(),
            }]);
        }
        self.state.paused_from = Some(self.state.phase);
        self.state.phase = Phase::Paused;
        Ok(vec![Effect::WriteJournalAndStatus])
    }

    fn resume(&mut self) -> Result<Vec<Effect>, ProcessorError> {
        if !matches!(self.state.phase, Phase::Paused) {
            return Err(ProcessorError::InvalidCommand(
                "resume requires PAUSE".into(),
            ));
        }
        self.state.phase = self.state.paused_from.take().unwrap_or(Phase::Recovery);
        let effects = match self.state.phase {
            Phase::Rolling => vec![self.rolling_boundary_effect()],
            Phase::Joining => self.next_merge_or_review()?,
            Phase::Publishing => vec![self.publishing_resume_effect()?],
            Phase::Cleaning => vec![Effect::WriteJournalAndStatus],
            Phase::Recovery | Phase::Opening | Phase::Idle | Phase::Paused | Phase::Blocked => {
                vec![Effect::WriteJournalAndStatus]
            }
        };
        Ok(effects)
    }

    fn recover(
        &mut self,
        workspaces_present: BTreeSet<String>,
    ) -> Result<Vec<Effect>, ProcessorError> {
        if !matches!(self.state.phase, Phase::Recovery) {
            return Err(ProcessorError::InvalidCommand(
                "recovery is allowed only in phase 0".into(),
            ));
        }

        // `Blocked` is an explicit, durable operator decision. `from_checkpoint` intentionally
        // routes every restart through Phase 0, but recovery must not reinterpret a blocked
        // idle checkpoint as permission to open a new cohort merely because it has no active
        // batch. In particular, scheduler guards use this state to prevent a no-progress planner
        // result from repeatedly spending model calls after restart.
        if matches!(self.recovery_target, Some(Phase::Blocked)) {
            self.recovery_target = None;
            self.state.phase = Phase::Blocked;
            return Ok(vec![Effect::WaitForOperator {
                reason: self
                    .state
                    .blocked_reason
                    .clone()
                    .unwrap_or_else(|| "processor is blocked".into()),
            }]);
        }

        let mut effects = Vec::new();
        for task in self.state.tasks.values_mut() {
            if let Some(intent) = task.imported_recovery_intent {
                let workspace_present = workspaces_present.contains(&task.id);
                match intent {
                    ImportedRecoveryIntent::EnsureWorkspace => {
                        if !matches!(task.phase, TaskPhase::Capturing) {
                            return Err(ProcessorError::CorruptCheckpoint(format!(
                                "legacy recovery intent for {} requires Capturing, found {:?}",
                                task.id, task.phase
                            )));
                        }
                        // Retain this marker until `WorkspaceReady` durably acknowledges the
                        // effect. Unlike a native admission, the legacy control plane already
                        // owns the queue/descriptor capture; the VCS port uses the marker to
                        // avoid trying to capture that task for a second time. Keeping it also
                        // makes a crash between scheduling and acknowledgement retry the same
                        // idempotent operation with the same authority boundary.
                        effects.push(Effect::EnsureTaskWorkspace {
                            task_id: task.id.clone(),
                            branch: format!("task/{}", task.id),
                        });
                        continue;
                    }
                    ImportedRecoveryIntent::EnsureWorkspaceForReview => {
                        if !matches!(task.phase, TaskPhase::Reviewing) {
                            return Err(ProcessorError::CorruptCheckpoint(format!(
                                "legacy recovery intent for {} requires Reviewing, found {:?}",
                                task.id, task.phase
                            )));
                        }
                        // As with `EnsureWorkspace`, retain the marker until the pending
                        // workspace effect is acknowledged so the port can retain the already
                        // captured legacy control-plane coordinates across a restart.
                        effects.push(Effect::EnsureTaskWorkspace {
                            task_id: task.id.clone(),
                            branch: format!("task/{}", task.id),
                        });
                        continue;
                    }
                    ImportedRecoveryIntent::DispatchImplementation => {
                        if !matches!(task.phase, TaskPhase::Implementing) || !workspace_present {
                            self.state.phase = Phase::Blocked;
                            self.state.blocked_reason = Some(format!(
                                "recovery: imported implementation {} has no verified managed workspace",
                                task.id
                            ));
                            return Ok(vec![Effect::WaitForOperator {
                                reason: self.state.blocked_reason.clone().unwrap_or_default(),
                            }]);
                        }
                        task.imported_recovery_intent = None;
                        task.leaf_attempt(LeafKind::Implement);
                        effects.push(Effect::PrepareTaskLeaf {
                            task_id: task.id.clone(),
                            kind: LeafKind::Implement,
                        });
                        continue;
                    }
                    ImportedRecoveryIntent::DispatchReview => {
                        if !matches!(task.phase, TaskPhase::Reviewing) || !workspace_present {
                            self.state.phase = Phase::Blocked;
                            self.state.blocked_reason = Some(format!(
                                "recovery: imported review {} has no verified managed workspace",
                                task.id
                            ));
                            return Ok(vec![Effect::WaitForOperator {
                                reason: self.state.blocked_reason.clone().unwrap_or_default(),
                            }]);
                        }
                        task.imported_recovery_intent = None;
                        task.leaf_attempt(LeafKind::Review);
                        effects.push(Effect::PrepareTaskReview {
                            task_id: task.id.clone(),
                        });
                        continue;
                    }
                    ImportedRecoveryIntent::ReturnConflictToQueue => {
                        if !matches!(task.phase, TaskPhase::Conflict) {
                            return Err(ProcessorError::CorruptCheckpoint(format!(
                                "legacy recovery intent for {} requires Conflict, found {:?}",
                                task.id, task.phase
                            )));
                        }
                        let reason = task.reason.clone().ok_or_else(|| {
                            ProcessorError::CorruptCheckpoint(format!(
                                "legacy conflict {} has no quarantine reason",
                                task.id
                            ))
                        })?;
                        // The queue return is intentionally a normal keyed effect.  Its marker is
                        // cleared only by `acknowledge_non_command_effect` after the control-plane
                        // write has succeeded and the reduced ledger is persisted atomically.
                        effects.push(Effect::ReturnTask {
                            task_id: task.id.clone(),
                            reason,
                        });
                        continue;
                    }
                }
            }
            if matches!(
                task.phase,
                TaskPhase::Capturing
                    | TaskPhase::Implementing
                    | TaskPhase::Committing
                    | TaskPhase::Reviewing
                    | TaskPhase::Fixing
            ) && !workspaces_present.contains(&task.id)
            {
                self.state.phase = Phase::Blocked;
                self.state.blocked_reason = Some(format!(
                    "recovery: active task {} has no verified managed workspace",
                    task.id
                ));
                return Ok(vec![Effect::WaitForOperator {
                    reason: self.state.blocked_reason.clone().unwrap_or_default(),
                }]);
            }
            if !matches!(
                task.phase,
                TaskPhase::Done | TaskPhase::Conflict | TaskPhase::Escalated
            ) && workspaces_present.contains(&task.id)
            {
                effects.push(Effect::Reconcile {
                    task_id: task.id.clone(),
                });
            }
        }

        let target = self.recovery_target.take();
        let restore_integration_before_publishing = matches!(target, Some(Phase::Publishing))
            && self.state.integration.imported_workspace_restore_pending;
        self.state.phase = if self.state.batch.is_none() {
            Phase::Idle
        } else if matches!(target, Some(Phase::Paused)) {
            Phase::Paused
        } else if matches!(target, Some(Phase::Cleaning)) {
            Phase::Cleaning
        } else if matches!(target, Some(Phase::Publishing)) {
            // A legacy Phase-0 import can prove a material integration branch/report while its
            // registered `_integration` checkout was removed by the crash.  The legacy
            // processor recreates that checkout from the durable branch before it resumes
            // Phase 5.  Route through Joining so the normal, keyed workspace-preparation effect
            // owns the typed VCS reconstruction; it will return to Publishing after the
            // `IntegrationWorkspaceReady` acknowledgement.
            if !restore_integration_before_publishing {
                Phase::Publishing
            } else {
                Phase::Joining
            }
        } else if self.state.integration.workspace_prepared
            || self.tasks_ready_to_merge().next().is_some()
        {
            Phase::Joining
        } else {
            Phase::Rolling
        };

        if matches!(self.state.phase, Phase::Cleaning) {
            // A native checkpoint normally carries the remaining cleanup effects in its ledger.
            // An imported legacy checkpoint does not, and a crash exactly before that ledger was
            // persisted is indistinguishable from one. Rebuild the complete, idempotent cleanup
            // sequence rather than writing a journal and immediately accepting `CleanupComplete`.
            // If Phase 0 also has a durable conflict return to finish, preserve Phase-6 ordering:
            // journal first, return the queue row, then remove task/integration artifacts.
            // A native runtime can already have one of those effects in its durable ledger.  Do
            // not return it twice to the driver: after the first acknowledgement the second copy
            // would otherwise try to acknowledge a now-absent key.  Start from the canonical
            // cleanup sequence, then insert only non-cleanup recovery work (notably a proved
            // legacy conflict return) in its Phase-6 position.
            let cleanup = self.cleanup_effects();
            let mut ordered = Vec::new();
            let mut cleanup_without_journal = cleanup.as_slice();
            if matches!(cleanup.first(), Some(Effect::WriteJournalAndStatus)) {
                push_unique_effect(&mut ordered, cleanup[0].clone());
                cleanup_without_journal = &cleanup[1..];
            }
            for effect in effects {
                if !cleanup.contains(&effect) {
                    push_unique_effect(&mut ordered, effect);
                }
            }
            for effect in cleanup_without_journal {
                push_unique_effect(&mut ordered, effect.clone());
            }
            effects = ordered;
        } else if restore_integration_before_publishing {
            // A reported legacy merger can also carry a Phase-6 quarantine return. Preserve
            // that return and the independently idempotent workspace reconstruction in the same
            // checkpointed recovery turn; otherwise the first acknowledgement would leave the
            // reducer at Joining with no next durable effect.
            effects.extend(self.next_merge_or_review()?);
        } else if effects.is_empty() {
            effects = match self.state.phase {
                // An idle recovery is the normal entry point for a long-lived processor: the
                // runner may immediately open the next cohort while it still owns the lease.
                // Releasing here made that safe continuation impossible because `Open` is
                // correctly rejected while the durable `release-lease` effect is pending.
                // The outer run loop releases the lease only when it decides to stop.
                Phase::Idle => vec![Effect::WriteJournalAndStatus],
                Phase::Rolling => vec![self.rolling_boundary_effect()],
                Phase::Joining => self.next_merge_or_review()?,
                Phase::Publishing => vec![self.publishing_resume_effect()?],
                _ => vec![Effect::WriteJournalAndStatus],
            };
        } else if matches!(self.state.phase, Phase::Publishing) {
            // A reported legacy merger may need to finish one or more already-proven queue
            // returns before resuming the integration review.  Keep both effects in the same
            // checkpointed recovery turn: a crash after a return leaves the review effect in the
            // ledger, while a crash before it cannot lose the return.
            effects.push(self.publishing_resume_effect()?);
        }
        Ok(effects)
    }

    /// Record or forget one task's provider conversation coordinate.
    ///
    /// This deliberately is NOT a [`ProcessorCommand`]. A session id is not a decision: it
    /// produces no [`Effect`], acknowledges no pending ledger key, and — as the sibling assertions
    /// in this module's tests pin down — cannot move `TaskPhase`, change a transition, or affect
    /// an escalation. Threading it through the command surface would enlarge the deterministic
    /// contract with a value the reducer never reads and would make every acknowledgement carry
    /// an irrelevant field. It joins [`Self::acknowledge_non_command_effect`] as the narrow,
    /// typed, effect-free mutation path instead.
    ///
    /// It is also safe outside the effect ledger precisely because it is orthogonal: a write lost
    /// to a crash, or one durable for a call that was never acknowledged, can at worst cost one
    /// re-seeded leaf call — never a replayed model call or a skipped review.
    ///
    /// One lineage keeps at most ONE resumable conversation, and it belongs to the provider whose
    /// call last ran. Routing may hand the same task's coder lineage to Claude in one round and to
    /// Codex in the next (`route_coder` reacts to durable descriptor metadata, and a Codex fallback
    /// hands the round to Claude outright), and the provider that did not run has no way to learn
    /// that the working tree moved underneath its conversation. Continuing such a peer would let it
    /// re-apply a change that is already in the tree or "fix" code that no longer exists — a silent
    /// corruption inside the fix cycle rather than a failed call. Forgetting it instead costs one
    /// re-seeded call, which is exactly the behaviour that predates durable sessions.
    pub(crate) fn record_leaf_session(
        &mut self,
        task_id: &str,
        update: &LeafSessionUpdate,
    ) -> Result<(), ProcessorError> {
        let key = update.key().as_durable_key();
        let peer_key = update.key().peer().as_durable_key();
        let task = self.task_mut(task_id)?;
        match update {
            LeafSessionUpdate::Observed { id, .. } => {
                // Defence in depth: the adapter already refuses a malformed provider id, and the
                // durable checkpoint must never become a carrier for one either. Reject before any
                // mutation, so a rejected write leaves the map exactly as it was.
                if !is_valid_session_id(id) {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "task {task_id} reported a malformed provider session id for {key}"
                    )));
                }
                task.leaf_sessions.remove(&peer_key);
                task.leaf_sessions.insert(key, id.clone());
            }
            LeafSessionUpdate::Invalidated { .. } => {
                // A call that ran and failed may still have edited the tree before failing, so the
                // peer is no less stale here than after a successful round.
                task.leaf_sessions.remove(&peer_key);
                task.leaf_sessions.remove(&key);
            }
        }
        Ok(())
    }

    /// Retire recovery-only metadata after a non-command effect has completed.  Most effects do
    /// not change reducer state at acknowledgement time; a legacy conflict return is the narrow
    /// exception because retaining its import marker would schedule another counter increment on
    /// the next Phase-0 pass.
    pub(crate) fn acknowledge_non_command_effect(
        &mut self,
        effect: &Effect,
    ) -> Result<(), ProcessorError> {
        match effect {
            Effect::WriteJournalAndStatus if matches!(self.state.phase, Phase::Cleaning) => {
                self.state.integration.cleanup_journaled = true;
            }
            Effect::ReturnTask { task_id, .. } => {
                let task = self.task_mut(task_id)?;
                if task.imported_recovery_intent
                    == Some(ImportedRecoveryIntent::ReturnConflictToQueue)
                {
                    if !matches!(task.phase, TaskPhase::Conflict) {
                        return Err(ProcessorError::CorruptCheckpoint(format!(
                            "legacy conflict return for {task_id} was acknowledged from {:?}",
                            task.phase
                        )));
                    }
                    task.imported_recovery_intent = None;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn open(
        &mut self,
        batch_id: String,
        base: String,
        now_secs: u64,
    ) -> Result<Vec<Effect>, ProcessorError> {
        if !matches!(self.state.phase, Phase::Idle | Phase::Opening) || self.state.batch.is_some() {
            return Err(ProcessorError::InvalidCommand(
                "open requires an idle processor with no existing cohort".into(),
            ));
        }
        validate_batch_id(&batch_id)?;
        validate_ref(&base, "base")?;
        self.state.batch = Some(CohortRuntime {
            id: batch_id,
            base,
            started_at_secs: now_secs,
            wave: 1,
            admitted_total: 0,
            admission_closed: None,
            cohort_budget_secs: self.config.cohort_budget_secs,
            cohort_token_budget: self.config.cohort_token_budget,
            cohort_token_budget_strict: self.config.cohort_token_budget_strict,
            token_budget_actual_tokens: None,
            events_outbox_enabled: self.config.events_outbox_enabled,
        });
        let pending_knowledge_curations =
            std::mem::take(&mut self.state.integration.pending_knowledge_curations);
        self.state.integration = IntegrationRuntime {
            pending_knowledge_curations,
            ..IntegrationRuntime::default()
        };
        self.state.tasks.clear();
        self.state.phase = Phase::Rolling;
        Ok(vec![Effect::DispatchDependencyCurator {
            boundary: RefreshBoundary::CohortOpen,
        }])
    }

    fn inbox_reconciliation_effect(&self) -> Effect {
        Effect::ReconcileInbox {
            free_slots: self.free_slots(),
        }
    }

    fn rolling_boundary_effect(&self) -> Effect {
        if self.state.integration.dependency_graph_refreshed_open {
            self.inbox_reconciliation_effect()
        } else {
            Effect::DispatchDependencyCurator {
                boundary: RefreshBoundary::CohortOpen,
            }
        }
    }

    fn dependency_graph_refreshed(
        &mut self,
        boundary: RefreshBoundary,
        outcome: LeafOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        match boundary {
            RefreshBoundary::CohortOpen => {
                self.require_phase(Phase::Rolling, "opening dependency graph refresh")?;
                if self.state.integration.dependency_graph_refreshed_open {
                    return Err(ProcessorError::InvalidCommand(
                        "opening dependency graph refresh was already acknowledged".into(),
                    ));
                }
                self.state.integration.dependency_graph_refreshed_open = true;
                if let Some(reason) = non_success_leaf_reason(outcome, "dependency graph curator") {
                    self.state.integration.degradations.push(format!(
                        "dependency graph refresh at cohort open failed: {reason}"
                    ));
                    return Ok(vec![
                        Effect::WriteJournalAndStatus,
                        self.inbox_reconciliation_effect(),
                    ]);
                }
                Ok(vec![self.inbox_reconciliation_effect()])
            }
            RefreshBoundary::PostArchive => {
                self.require_phase(Phase::Cleaning, "post-archive dependency graph refresh")?;
                if self
                    .state
                    .integration
                    .dependency_graph_refreshed_post_archive
                {
                    return Err(ProcessorError::InvalidCommand(
                        "post-archive dependency graph refresh was already acknowledged".into(),
                    ));
                }
                self.state
                    .integration
                    .dependency_graph_refreshed_post_archive = true;
                if let Some(reason) = non_success_leaf_reason(outcome, "dependency graph curator") {
                    self.state.integration.degradations.push(format!(
                        "dependency graph refresh after archive failed: {reason}"
                    ));
                    return Ok(vec![
                        Effect::WriteJournalAndStatus,
                        Effect::ReconcileInboxFinalization,
                    ]);
                }
                Ok(vec![Effect::ReconcileInboxFinalization])
            }
        }
    }

    fn validate_inbox_wave(
        &self,
        free_slots: usize,
        operation: &str,
    ) -> Result<(), ProcessorError> {
        self.require_phase(Phase::Rolling, operation)?;
        if free_slots != self.free_slots() {
            return Err(ProcessorError::InvalidCommand(format!(
                "{operation} free_slots={free_slots} does not match the current deterministic capacity {}",
                self.free_slots()
            )));
        }
        Ok(())
    }

    fn inbox_reconciled(
        &mut self,
        free_slots: usize,
        curation_required: bool,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.validate_inbox_wave(free_slots, "inbox reconciliation")?;
        Ok(vec![if curation_required {
            Effect::DispatchInboxCurator {
                free_slots,
                mode: InboxCurationMode::Intake,
            }
        } else {
            Effect::DrainQueueInbox { free_slots }
        }])
    }

    fn inbox_curated(
        &mut self,
        free_slots: usize,
        mode: InboxCurationMode,
        outcome: LeafOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        match mode {
            InboxCurationMode::Intake => self.validate_inbox_wave(free_slots, "inbox curation")?,
            InboxCurationMode::Finalize => {
                self.require_phase(Phase::Cleaning, "post-archive inbox finalization")?;
                if free_slots != 0 {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "inbox finalization free_slots={free_slots} must be zero"
                    )));
                }
            }
        }
        match outcome {
            LeafOutcome::Completed { .. } => match mode {
                InboxCurationMode::Intake => Ok(vec![Effect::DrainQueueInbox { free_slots }]),
                // The finalizer has already made its own durable terminal-status and reply
                // mutations. Do not drain queue-inbox here: Phase 6 must not create work after
                // its archive boundary.
                InboxCurationMode::Finalize => Ok(Vec::new()),
            },
            LeafOutcome::RetryableFailure { reason } | LeafOutcome::Escalated { reason } => {
                self.state.phase = Phase::Blocked;
                self.state.blocked_reason = Some(format!("inbox curator {mode:?}: {reason}"));
                Ok(vec![Effect::WaitForOperator {
                    reason: self
                        .state
                        .blocked_reason
                        .clone()
                        .expect("set immediately above"),
                }])
            }
            LeafOutcome::RiskElevated { risk, .. } => {
                let reason = format!(
                    "inbox curator reported unsupported task risk elevation {}",
                    risk.as_str()
                );
                self.state.phase = Phase::Blocked;
                self.state.blocked_reason = Some(reason.clone());
                Ok(vec![Effect::WaitForOperator { reason }])
            }
            LeafOutcome::CompletedWithWontFix { .. } => {
                let reason =
                    "inbox curator reported unsupported fix-cycle won't-fix metadata".to_string();
                self.state.phase = Phase::Blocked;
                self.state.blocked_reason = Some(reason.clone());
                Ok(vec![Effect::WaitForOperator { reason }])
            }
        }
    }

    fn inbox_drained(&mut self, free_slots: usize) -> Result<Vec<Effect>, ProcessorError> {
        self.validate_inbox_wave(free_slots, "queue inbox drain")?;
        Ok(vec![Effect::PlanNextWave { free_slots }])
    }

    fn inbox_finalization_reconciled(
        &mut self,
        curation_required: bool,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Cleaning, "post-archive inbox reconciliation")?;
        if curation_required {
            Ok(vec![Effect::DispatchInboxCurator {
                // Finalization is not an admission wave. Keeping this coordinate explicitly
                // zero makes a malformed/stale acknowledgement unable to authorize planning.
                free_slots: 0,
                mode: InboxCurationMode::Finalize,
            }])
        } else {
            Ok(Vec::new())
        }
    }

    fn admit(
        &mut self,
        candidates: Vec<AdmissionCandidate>,
        now_secs: u64,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Rolling, "rolling admission")?;
        for candidate in &candidates {
            validate_conflict_domain(&candidate.conflict_domain)?;
        }
        self.close_for_budget_if_needed(now_secs);
        let Some(batch) = self.state.batch.as_ref() else {
            return Err(ProcessorError::InvalidCommand(
                "rolling requires a cohort".into(),
            ));
        };
        if batch.admission_closed.is_some() {
            return self.after_admission_closed(now_secs);
        }
        let free_slots = self.free_slots();
        let remaining =
            usize::try_from(self.config.cohort_size.saturating_sub(batch.admitted_total))
                .unwrap_or(usize::MAX);
        let capacity = free_slots.min(remaining);
        let active = self.active_tasks();
        let resolver_candidates: Vec<Candidate> = candidates
            .iter()
            .map(|candidate| Candidate {
                id: candidate.id.clone(),
                ready: candidate.ready,
                domain: Domain::parse(&candidate.conflict_domain),
                delivery: if candidate.current_delivery_lane {
                    DeliveryTarget::Current
                } else {
                    DeliveryTarget::NextMajor
                },
            })
            .collect();
        let outcome = plan_admission(&resolver_candidates, &active, capacity);
        match outcome {
            crate::resolvers::AdmissionOutcome::Admitted(ids) => {
                let admitted: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
                let wave = self.state.batch.as_ref().expect("checked above").wave;
                let mut effects = Vec::new();
                for candidate in candidates
                    .iter()
                    .filter(|c| admitted.contains(c.id.as_str()))
                {
                    validate_task_id(&candidate.id)?;
                    if self.state.tasks.contains_key(&candidate.id) {
                        return Err(ProcessorError::InvalidCommand(format!(
                            "task {} was admitted twice in one cohort",
                            candidate.id
                        )));
                    }
                    self.state
                        .tasks
                        .insert(candidate.id.clone(), TaskRuntime::new(candidate, wave));
                    effects.push(Effect::EnsureTaskWorkspace {
                        task_id: candidate.id.clone(),
                        branch: format!("task/{}", candidate.id),
                    });
                }
                let batch = self.state.batch.as_mut().expect("checked above");
                batch.admitted_total = batch.admitted_total.saturating_add(ids.len() as u32);
                batch.wave = batch.wave.saturating_add(1);
                self.close_for_budget_if_needed(now_secs);
                Ok(effects)
            }
            crate::resolvers::AdmissionOutcome::Empty(reason) => {
                if let Some(close) = reason.to_close_reason() {
                    self.close_admission(close);
                    self.after_admission_closed(now_secs)
                } else {
                    debug_assert_eq!(reason, EmptyReason::OnlyConflictsWithActive);
                    Ok(Vec::new())
                }
            }
        }
    }

    fn workspace_ready(&mut self, task_id: &str) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Rolling, "task workspace completion")?;
        let task = self.task_mut(task_id)?;
        match task.imported_recovery_intent {
            Some(ImportedRecoveryIntent::EnsureWorkspaceForReview) => {
                expect_task_phase(task, TaskPhase::Reviewing, "review workspace creation")?;
                task.imported_recovery_intent = None;
                task.leaf_attempt(LeafKind::Review);
                Ok(vec![Effect::PrepareTaskReview {
                    task_id: task_id.into(),
                }])
            }
            Some(ImportedRecoveryIntent::EnsureWorkspace) | None => {
                expect_task_phase(task, TaskPhase::Capturing, "workspace creation")?;
                task.imported_recovery_intent = None;
                task.phase = TaskPhase::Implementing;
                task.leaf_attempt(LeafKind::Implement);
                Ok(vec![Effect::PrepareTaskLeaf {
                    task_id: task_id.into(),
                    kind: LeafKind::Implement,
                }])
            }
            Some(
                ImportedRecoveryIntent::DispatchImplementation
                | ImportedRecoveryIntent::DispatchReview
                | ImportedRecoveryIntent::ReturnConflictToQueue,
            ) => Err(ProcessorError::CorruptCheckpoint(format!(
                "task {task_id} received WorkspaceReady for an incompatible legacy recovery intent"
            ))),
        }
    }

    fn workspace_failed(
        &mut self,
        task_id: &str,
        reason: String,
    ) -> Result<Vec<Effect>, ProcessorError> {
        let task = self.task_mut(task_id)?;
        expect_task_phase(task, TaskPhase::Capturing, "workspace failure")?;
        task.phase = TaskPhase::Escalated;
        task.reason = Some(reason.clone());
        Ok(vec![Effect::EscalateTask {
            task_id: task_id.into(),
            reason,
        }])
    }

    fn task_leaf(
        &mut self,
        task_id: &str,
        outcome: LeafOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Rolling, "task leaf result")?;
        let leaf_max_attempts = self.config.leaf_max_attempts;
        let task = self.task_mut(task_id)?;
        let kind = match task.phase {
            TaskPhase::Implementing => LeafKind::Implement,
            TaskPhase::Fixing => LeafKind::Fix,
            actual => {
                return Err(ProcessorError::UnexpectedTaskPhase {
                    task_id: task_id.into(),
                    expected: "implementation or fix leaf",
                    actual,
                });
            }
        };
        match outcome {
            LeafOutcome::Completed { author } => {
                if author.is_some() {
                    task.implementation_author = author;
                }
                task.pending_fix_open_findings = None;
                task.pending_fix_open_finding_ids = None;
                task.phase = TaskPhase::Committing;
                Ok(vec![Effect::CommitTask {
                    task_id: task_id.into(),
                }])
            }
            LeafOutcome::RiskElevated {
                author,
                risk,
                wont_fixed,
            } => {
                if let Err(reason) = raise_task_risk(task, risk) {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                let open_findings = task.pending_fix_open_findings.take();
                // Cleared alongside the count it was captured with (R-06) — see
                // `TaskRuntime::pending_fix_open_finding_ids`. This reducer never reads its value:
                // when the round did report won't-fix entries, the adapter that produced
                // `wont_fixed` had already consumed it; when it did not (`wont_fixed: None` — an
                // ordinary risk elevation), there was nothing to validate. Either way the round
                // this coordinate belonged to ends here, so it must not outlive it (R-07).
                task.pending_fix_open_finding_ids = None;
                if let Some(wont_fixed) = wont_fixed {
                    if kind != LeafKind::Fix {
                        return Err(ProcessorError::InvalidCommand(format!(
                            "task {task_id} reported fix-cycle won't-fix metadata outside a fix leaf"
                        )));
                    }
                    if let Some(open_findings) = open_findings
                        && let Some(reason) =
                            empty_fixed_set_decision(wont_fixed, open_findings).escalation_reason()
                    {
                        task.phase = TaskPhase::Escalated;
                        task.reason = Some(reason.clone());
                        return Ok(vec![Effect::EscalateTask {
                            task_id: task_id.into(),
                            reason,
                        }]);
                    }
                }
                if author.is_some() {
                    task.implementation_author = author;
                }
                task.phase = TaskPhase::Committing;
                Ok(vec![Effect::CommitTask {
                    task_id: task_id.into(),
                }])
            }
            LeafOutcome::CompletedWithWontFix { author, wont_fixed } => {
                if kind != LeafKind::Fix {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "task {task_id} reported fix-cycle won't-fix metadata outside a fix leaf"
                    )));
                }
                // Consumed (and cleared) here so it never survives stale into a later,
                // unrelated round; `None` (no durable coordinate — a checkpoint predating
                // T-014, or a defensive gap) means the empty-fixed-set signal simply cannot be
                // judged this round, so it is skipped and the ordinary path (below,
                // `stagnation_decision` on the NEXT review pass) remains the sole backstop.
                let open_findings = task.pending_fix_open_findings.take();
                task.pending_fix_open_finding_ids = None;
                if let Some(open_findings) = open_findings
                    && let Some(reason) =
                        empty_fixed_set_decision(wont_fixed, open_findings).escalation_reason()
                {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                if author.is_some() {
                    task.implementation_author = author;
                }
                task.phase = TaskPhase::Committing;
                Ok(vec![Effect::CommitTask {
                    task_id: task_id.into(),
                }])
            }
            LeafOutcome::RetryableFailure { reason } => {
                if schedule_leaf_retry(task, kind, leaf_max_attempts) {
                    Ok(vec![Effect::PrepareTaskLeaf {
                        task_id: task_id.into(),
                        kind,
                    }])
                } else {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(format!("{reason}; retry limit exhausted"));
                    Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason: task.reason.clone().unwrap_or_default(),
                    }])
                }
            }
            LeafOutcome::Escalated { reason } => {
                task.phase = TaskPhase::Escalated;
                task.reason = Some(reason.clone());
                Ok(vec![Effect::EscalateTask {
                    task_id: task_id.into(),
                    reason,
                }])
            }
        }
    }

    fn task_leaf_prepared(
        &mut self,
        task_id: &str,
        outcome: TaskLeafPreparationOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Rolling, "Codex task leaf preparation")?;
        if let TaskLeafPreparationOutcome::SandboxDowngraded { scope } = &outcome {
            let degradation = scope.degradation().to_owned();
            if !self.state.integration.degradations.contains(&degradation) {
                self.state.integration.degradations.push(degradation);
            }
        }
        let task = self.task_mut(task_id)?;
        let kind = match task.phase {
            TaskPhase::Implementing => LeafKind::Implement,
            TaskPhase::Fixing => LeafKind::Fix,
            actual => {
                return Err(ProcessorError::UnexpectedTaskPhase {
                    task_id: task_id.into(),
                    expected: "implementation or fix Codex preparation",
                    actual,
                });
            }
        };
        match outcome {
            TaskLeafPreparationOutcome::Completed => {
                task.implementation_author = Some("coder_codex".into());
                task.pending_fix_open_findings = None;
                task.pending_fix_open_finding_ids = None;
                task.phase = TaskPhase::Committing;
                Ok(vec![Effect::CommitTask {
                    task_id: task_id.into(),
                }])
            }
            TaskLeafPreparationOutcome::CompletedWithWontFix { wont_fixed } => {
                if kind != LeafKind::Fix {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "task {task_id} reported fix-cycle won't-fix metadata outside a fix leaf"
                    )));
                }
                let open_findings = task.pending_fix_open_findings.take();
                task.pending_fix_open_finding_ids = None;
                if let Some(open_findings) = open_findings
                    && let Some(reason) =
                        empty_fixed_set_decision(wont_fixed, open_findings).escalation_reason()
                {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                task.implementation_author = Some("coder_codex".into());
                task.phase = TaskPhase::Committing;
                Ok(vec![Effect::CommitTask {
                    task_id: task_id.into(),
                }])
            }
            TaskLeafPreparationOutcome::RiskElevated { risk, wont_fixed } => {
                if let Err(reason) = raise_task_risk(task, risk) {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                let open_findings = task.pending_fix_open_findings.take();
                task.pending_fix_open_finding_ids = None;
                if let Some(wont_fixed) = wont_fixed {
                    if kind != LeafKind::Fix {
                        return Err(ProcessorError::InvalidCommand(format!(
                            "task {task_id} reported fix-cycle won't-fix metadata outside a fix leaf"
                        )));
                    }
                    if let Some(open_findings) = open_findings
                        && let Some(reason) =
                            empty_fixed_set_decision(wont_fixed, open_findings).escalation_reason()
                    {
                        task.phase = TaskPhase::Escalated;
                        task.reason = Some(reason.clone());
                        return Ok(vec![Effect::EscalateTask {
                            task_id: task_id.into(),
                            reason,
                        }]);
                    }
                }
                task.implementation_author = Some("coder_codex".into());
                task.phase = TaskPhase::Committing;
                Ok(vec![Effect::CommitTask {
                    task_id: task_id.into(),
                }])
            }
            TaskLeafPreparationOutcome::Skipped | TaskLeafPreparationOutcome::Fallback => {
                Ok(vec![Effect::DispatchTask {
                    task_id: task_id.into(),
                    kind,
                }])
            }
            TaskLeafPreparationOutcome::SandboxDowngraded { .. } => Ok(vec![
                Effect::WriteJournalAndStatus,
                Effect::DispatchTask {
                    task_id: task_id.into(),
                    kind,
                },
            ]),
            TaskLeafPreparationOutcome::Escalated { reason } => {
                task.phase = TaskPhase::Escalated;
                task.reason = Some(reason.clone());
                Ok(vec![Effect::EscalateTask {
                    task_id: task_id.into(),
                    reason,
                }])
            }
        }
    }

    fn task_committed(
        &mut self,
        task_id: &str,
        commit: String,
    ) -> Result<Vec<Effect>, ProcessorError> {
        validate_ref(&commit, "task commit")?;
        let review_loop_max = self.config.review_loop_max;
        let task = self.task_mut(task_id)?;
        expect_task_phase(task, TaskPhase::Committing, "task commit")?;
        task.phase = TaskPhase::Reviewing;
        task.previous_review_sha = task.review_sha.clone();
        task.review_sha = Some(commit);
        // The final permitted findings pass may still run its fixer, but the legacy processor
        // never launches one more review to validate that fix.
        if task.review_cycles >= review_loop_max {
            let reason = format!("не сходится ревью после {} циклов", task.review_cycles);
            task.phase = TaskPhase::Escalated;
            task.reason = Some(reason.clone());
            return Ok(vec![Effect::EscalateTask {
                task_id: task_id.into(),
                reason,
            }]);
        }
        task.leaf_attempt(LeafKind::Review);
        Ok(vec![Effect::PrepareTaskReview {
            task_id: task_id.into(),
        }])
    }

    fn task_review_prepared(
        &mut self,
        task_id: &str,
        outcome: TaskReviewPreparationOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Rolling, "task review preparation")?;
        let review_loop_max = self.config.review_loop_max;
        {
            let task = self.task_mut(task_id)?;
            expect_task_phase(task, TaskPhase::Reviewing, "task review preparation")?;
            if task.review_cycles >= review_loop_max {
                let reason = format!("не сходится ревью после {} циклов", task.review_cycles);
                task.phase = TaskPhase::Escalated;
                task.reason = Some(reason.clone());
                return Ok(vec![Effect::EscalateTask {
                    task_id: task_id.into(),
                    reason,
                }]);
            }
            if task
                .leaf_attempts
                .get(LeafKind::Review.as_str())
                .copied()
                .unwrap_or_default()
                == 0
            {
                // Checkpoints written before the Codex-full preparation became an actual model
                // boundary stored `prepare-task-review` before reserving its review attempt.
                // Preserve their one safe continuation as attempt 1; all newly issued effects
                // reserve before spawn above.
                task.leaf_attempt(LeafKind::Review);
            }
        }
        if let TaskReviewPreparationOutcome::SandboxDowngraded { scope } = &outcome {
            let degradation = scope.degradation().to_owned();
            if !self.state.integration.degradations.contains(&degradation) {
                self.state.integration.degradations.push(degradation);
            }
        }
        match outcome {
            TaskReviewPreparationOutcome::Completed(outcome) => self.task_review(task_id, outcome),
            TaskReviewPreparationOutcome::DispatchClaude => Ok(vec![Effect::DispatchTask {
                task_id: task_id.into(),
                kind: LeafKind::Review,
            }]),
            TaskReviewPreparationOutcome::SandboxDowngraded { .. } => Ok(vec![
                Effect::WriteJournalAndStatus,
                Effect::DispatchTask {
                    task_id: task_id.into(),
                    kind: LeafKind::Review,
                },
            ]),
        }
    }

    fn task_review(
        &mut self,
        task_id: &str,
        outcome: ReviewOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Rolling, "task review")?;
        let review_loop_max = self.config.review_loop_max;
        let stagnation_limit = self.config.stagnation_limit;
        let task = self.task_mut(task_id)?;
        expect_task_phase(task, TaskPhase::Reviewing, "task review")?;
        match outcome {
            ReviewOutcome::Clean { review_sha } => {
                validate_ref(&review_sha, "review SHA")?;
                if task.review_sha.as_deref() != Some(review_sha.as_str()) {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "task {task_id} review belongs to {review_sha:?}, not committed tip {:?}",
                        task.review_sha
                    )));
                }
                task.review_cycles = task.review_cycles.saturating_add(1);
                if task.review_cycles > review_loop_max {
                    task.phase = TaskPhase::Escalated;
                    let reason = format!("не сходится ревью после {review_loop_max} циклов");
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                task.phase = TaskPhase::Ready;
                task.review_sha = Some(review_sha);
                Ok(Vec::new())
            }
            ReviewOutcome::CleanRiskElevated { review_sha, risk } => {
                validate_ref(&review_sha, "review SHA")?;
                if task.review_sha.as_deref() != Some(review_sha.as_str()) {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "task {task_id} review belongs to {review_sha:?}, not committed tip {:?}",
                        task.review_sha
                    )));
                }
                if let Err(reason) = raise_task_risk(task, risk) {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                task.review_cycles = task.review_cycles.saturating_add(1);
                if task.review_cycles > review_loop_max {
                    task.phase = TaskPhase::Escalated;
                    let reason = format!("не сходится ревью после {review_loop_max} циклов");
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                task.phase = TaskPhase::Ready;
                task.review_sha = Some(review_sha);
                Ok(Vec::new())
            }
            ReviewOutcome::Findings {
                signature,
                open_findings,
                open_finding_ids,
            } => {
                validate_signature(&signature)?;
                task.review_cycles = task.review_cycles.saturating_add(1);
                task.review_signatures.push(signature);
                let cycle = review_cycle_decision(task.review_cycles, review_loop_max);
                if let Some(reason) = cycle.escalation_reason() {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                let signatures = signatures_from(&task.review_signatures);
                if let Some(reason) = stagnation_reason(&signatures, stagnation_limit) {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                // Durable coordinate for T-014's empty-fixed-set early exit: correlated against
                // the fixer's own `не исправлено` count the moment this fix round returns (see
                // `task_leaf`'s `LeafOutcome::CompletedWithWontFix` arm). The id set is recorded
                // exactly as the round reported it, INCLUDING an empty one: emptiness here is a
                // known fact about this round ("nothing a fixer may decline"), never the absence of
                // the coordinate — see `TaskRuntime::pending_fix_open_finding_ids` (R-08).
                task.pending_fix_open_findings = Some(open_findings);
                task.pending_fix_open_finding_ids = Some(open_finding_ids);
                task.phase = TaskPhase::Fixing;
                task.leaf_attempt(LeafKind::Fix);
                Ok(vec![Effect::PrepareTaskLeaf {
                    task_id: task_id.into(),
                    kind: LeafKind::Fix,
                }])
            }
            ReviewOutcome::FindingsRiskElevated {
                signature,
                risk,
                open_findings,
                open_finding_ids,
            } => {
                validate_signature(&signature)?;
                if let Err(reason) = raise_task_risk(task, risk) {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                task.review_cycles = task.review_cycles.saturating_add(1);
                task.review_signatures.push(signature);
                let cycle = review_cycle_decision(task.review_cycles, review_loop_max);
                if let Some(reason) = cycle.escalation_reason() {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                let signatures = signatures_from(&task.review_signatures);
                if let Some(reason) = stagnation_reason(&signatures, stagnation_limit) {
                    task.phase = TaskPhase::Escalated;
                    task.reason = Some(reason.clone());
                    return Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }]);
                }
                // Same durable coordinates, and the same "empty is known, not unknown" rule as the
                // plain `Findings` arm above (R-08).
                task.pending_fix_open_findings = Some(open_findings);
                task.pending_fix_open_finding_ids = Some(open_finding_ids);
                task.phase = TaskPhase::Fixing;
                task.leaf_attempt(LeafKind::Fix);
                Ok(vec![Effect::PrepareTaskLeaf {
                    task_id: task_id.into(),
                    kind: LeafKind::Fix,
                }])
            }
            // Phase 2.7 — the reviewer pass did not conclude (cut short before its `ИТОГ:` tail,
            // no fresh `SUMMARY-R` to prove a clean gate, or no report of its own at all). Re-run
            // the SAME reviewer; never dispatch a fixer against an empty finding list.
            //
            // This arm is the only budget holder for such rounds, and T-026 routed the remaining
            // truncation shapes here instead of into a terminal escalation, so its accounting is
            // load-bearing: `Циклов-ревью` counts every COMPLETED pass including this one (see
            // `TaskRuntime::review_cycles`), which is what keeps `REVIEW_LOOP_MAX` finite for a
            // reviewer that is repeatedly interrupted, and crash-safe across resume. The `>=`
            // comparison (rather than the `>` used when concluding a clean pass) is deliberate: it
            // decides whether ANOTHER cycle may run, matching the identical guard in
            // `prepare_task_review`, so no review is dispatched that preparation would reject.
            // `CALL_MAX_ATTEMPTS` (`leaf_max_attempts`) is untouched by this route and stays what
            // it is — the transient-retry budget of implement/fix leaves; a reviewer's supervision
            // failure remains terminal (`ReviewOutcome::Escalated`) rather than a retryable one.
            ReviewOutcome::Incomplete => {
                task.review_cycles = task.review_cycles.saturating_add(1);
                if task.review_cycles >= review_loop_max {
                    task.phase = TaskPhase::Escalated;
                    let reason = format!("не сходится ревью после {} циклов", task.review_cycles);
                    task.reason = Some(reason.clone());
                    Ok(vec![Effect::EscalateTask {
                        task_id: task_id.into(),
                        reason,
                    }])
                } else {
                    task.leaf_attempt(LeafKind::Review);
                    Ok(vec![Effect::PrepareTaskReview {
                        task_id: task_id.into(),
                    }])
                }
            }
            ReviewOutcome::Escalated { reason } => {
                task.review_cycles = task.review_cycles.saturating_add(1);
                task.phase = TaskPhase::Escalated;
                task.reason = Some(reason.clone());
                Ok(vec![Effect::EscalateTask {
                    task_id: task_id.into(),
                    reason,
                }])
            }
        }
    }

    fn advance(&mut self, now_secs: u64) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Rolling, "advance")?;
        self.close_for_budget_if_needed(now_secs);
        if self
            .state
            .batch
            .as_ref()
            .is_some_and(|b| b.admission_closed.is_none())
        {
            return Ok(vec![self.inbox_reconciliation_effect()]);
        }
        self.after_admission_closed(now_secs)
    }

    fn after_admission_closed(&mut self, _now_secs: u64) -> Result<Vec<Effect>, ProcessorError> {
        if self.state.tasks.values().any(|task| task.phase.is_active()) {
            return Ok(Vec::new());
        }
        if self.tasks_ready_to_merge().next().is_some() {
            self.state.phase = Phase::Joining;
            return Ok(vec![Effect::PrepareIntegrationWorkspace {
                branch: self.integration_branch()?,
            }]);
        }
        self.state.phase = Phase::Cleaning;
        Ok(self.cleanup_effects())
    }

    fn integration_workspace_ready(&mut self) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Joining, "integration workspace")?;
        self.state.integration.workspace_prepared = true;
        self.state.integration.imported_workspace_restore_pending = false;
        self.next_merge_or_review()
    }

    fn next_merge_or_review(&mut self) -> Result<Vec<Effect>, ProcessorError> {
        // Recovery can reconstruct a cohort after the legacy control plane had already closed
        // admission, but before the integration workspace was created.  Do not let the presence
        // of a ready task skip that durable preparation boundary: `MergeTask` is only valid once
        // the integration workspace has been root-verified and recorded.
        if !self.state.integration.workspace_prepared {
            return Ok(vec![Effect::PrepareIntegrationWorkspace {
                branch: self.integration_branch()?,
            }]);
        }
        if self.state.integration.pending_merge_resolution.is_some() {
            // `dispatch_integration` reserves a fresh attempt before the model is exposed. This
            // branch is reached only from the typed `NeedsResolution` acknowledgement or from a
            // recovery that has no outstanding runtime-ledger entry; the latter is deliberately
            // conservative and leaves the pending model call visible for normal token gating.
            return Ok(vec![self.dispatch_integration(LeafKind::Merger)]);
        }
        if let Some(task) = self.tasks_ready_to_merge().next() {
            Ok(vec![Effect::MergeTask {
                task_id: task.id.clone(),
            }])
        } else if self.state.integration.merged_tasks.is_empty() {
            self.state.phase = Phase::Cleaning;
            Ok(vec![Effect::WriteJournalAndStatus])
        } else {
            // Full review may require one or more F-fixes, so verification belongs at the
            // publication boundary rather than immediately after the merge sequence. A passing
            // profile for an earlier integration tip must never authorize a later fix.
            self.state.phase = Phase::Publishing;
            Ok(vec![self.dispatch_integration(LeafKind::IntegrationReview)])
        }
    }

    fn task_merged(
        &mut self,
        task_id: &str,
        outcome: MergeOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Joining, "merger result")?;
        let task = self
            .state
            .tasks
            .get(task_id)
            .ok_or_else(|| ProcessorError::MissingTask(task_id.into()))?;
        expect_task_phase(task, TaskPhase::Ready, "merger result")?;
        match outcome {
            MergeOutcome::Merged { integration_sha } => {
                self.record_merged_task(task_id, integration_sha)
            }
            MergeOutcome::NeedsResolution {
                pre_merge_head,
                merge_paths,
                paths,
                protected_paths,
            } => {
                validate_ref(&pre_merge_head, "pre-merge integration SHA")?;
                validate_merge_conflict_paths(&merge_paths)?;
                validate_merge_conflict_paths(&paths)?;
                if paths.iter().any(|path| !merge_paths.contains(path)) {
                    return Err(ProcessorError::InvalidCommand(
                        "merge conflict paths are not contained in the typed merge surface".into(),
                    ));
                }
                validate_merge_protected_paths(&merge_paths, &paths, &protected_paths)?;
                let expected = self
                    .state
                    .integration
                    .integration_head
                    .as_deref()
                    .or_else(|| self.state.batch.as_ref().map(|batch| batch.base.as_str()))
                    .ok_or_else(|| {
                        ProcessorError::InvalidCommand(
                            "merge conflict has no durable cohort base".into(),
                        )
                    })?;
                if expected != pre_merge_head {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "merge conflict pre-merge SHA {pre_merge_head:?} differs from durable integration tip {expected:?}"
                    )));
                }
                if self.state.integration.pending_merge_resolution.is_some() {
                    return Err(ProcessorError::InvalidCommand(
                        "another merge resolution is already pending".into(),
                    ));
                }
                self.task_mut(task_id)?.phase = TaskPhase::ResolvingMerge;
                self.state.integration.pending_merge_resolution = Some(MergeResolutionRuntime {
                    task_id: task_id.into(),
                    pre_merge_head,
                    merge_paths,
                    paths,
                    protected_paths,
                });
                Ok(vec![self.dispatch_integration(LeafKind::Merger)])
            }
            MergeOutcome::Quarantined { reason } => self.quarantine_merged_task(task_id, reason),
            MergeOutcome::Failed { reason } => {
                // `Failed` is intentionally not a quarantine. A merger that could not produce
                // a trustworthy per-task outcome (for example, a supervisor failure, malformed
                // report, or unexpected VCS error) has not proved that this branch is safe to
                // re-queue. The legacy processor preserves the integration workspace/report for
                // operator recovery in this case. Returning the queue row here was worse than a
                // no-op: the native port had correctly left the descriptor `ready`, so Phase 6
                // would later try to clean incompatible control-plane states.
                self.fail_integration(format!(
                    "merger did not produce a reliable result for {task_id}: {reason}"
                ))
            }
        }
    }

    fn merge_resolution(
        &mut self,
        task_id: &str,
        outcome: LeafOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Joining, "merge resolution")?;
        self.require_pending_merge_resolution(task_id)?;
        let task = self.task_mut(task_id)?;
        expect_task_phase(task, TaskPhase::ResolvingMerge, "merge resolution")?;
        match outcome {
            LeafOutcome::Completed { .. } => Ok(vec![Effect::FinalizeMergeResolution {
                task_id: task_id.into(),
            }]),
            LeafOutcome::RetryableFailure { reason } | LeafOutcome::Escalated { reason } => {
                Ok(vec![Effect::AbortMergeResolution {
                    task_id: task_id.into(),
                    reason: format!("merger could not resolve conflict: {reason}"),
                }])
            }
            LeafOutcome::RiskElevated { risk, .. } => Ok(vec![Effect::AbortMergeResolution {
                task_id: task_id.into(),
                reason: format!(
                    "merger reported unsupported task risk elevation {}",
                    risk.as_str()
                ),
            }]),
            LeafOutcome::CompletedWithWontFix { .. } => Ok(vec![Effect::AbortMergeResolution {
                task_id: task_id.into(),
                reason: "merger reported unsupported fix-cycle won't-fix metadata".into(),
            }]),
        }
    }

    fn merge_resolution_finalized(
        &mut self,
        task_id: &str,
        outcome: MergeOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Joining, "resolved merger result")?;
        self.require_pending_merge_resolution(task_id)?;
        let task = self
            .state
            .tasks
            .get(task_id)
            .ok_or_else(|| ProcessorError::MissingTask(task_id.into()))?;
        expect_task_phase(task, TaskPhase::ResolvingMerge, "resolved merger result")?;
        match outcome {
            MergeOutcome::Merged { integration_sha } => {
                self.state.integration.pending_merge_resolution = None;
                self.record_merged_task(task_id, integration_sha)
            }
            MergeOutcome::Quarantined { reason } => {
                self.state.integration.pending_merge_resolution = None;
                self.quarantine_merged_task(task_id, reason)
            }
            MergeOutcome::Failed { reason } => self.fail_integration(format!(
                "resolved merger did not produce a reliable result for {task_id}: {reason}"
            )),
            MergeOutcome::NeedsResolution { .. } => Err(ProcessorError::InvalidCommand(
                "resolved merger cannot request another conflict resolution".into(),
            )),
        }
    }

    fn merge_resolution_aborted(
        &mut self,
        task_id: &str,
        reason: String,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Joining, "merge resolution abort")?;
        self.require_pending_merge_resolution(task_id)?;
        let task = self
            .state
            .tasks
            .get(task_id)
            .ok_or_else(|| ProcessorError::MissingTask(task_id.into()))?;
        expect_task_phase(task, TaskPhase::ResolvingMerge, "merge resolution abort")?;
        self.state.integration.pending_merge_resolution = None;
        self.quarantine_merged_task(task_id, reason)
    }

    fn require_pending_merge_resolution(&self, task_id: &str) -> Result<(), ProcessorError> {
        let pending = self
            .state
            .integration
            .pending_merge_resolution
            .as_ref()
            .ok_or_else(|| {
                ProcessorError::InvalidCommand("no merge resolution is pending".into())
            })?;
        if pending.task_id == task_id {
            Ok(())
        } else {
            Err(ProcessorError::InvalidCommand(format!(
                "merge resolution for {task_id} does not match pending task {}",
                pending.task_id
            )))
        }
    }

    fn record_merged_task(
        &mut self,
        task_id: &str,
        integration_sha: String,
    ) -> Result<Vec<Effect>, ProcessorError> {
        validate_ref(&integration_sha, "integration SHA")?;
        let task = self.task_mut(task_id)?;
        task.phase = TaskPhase::Merged;
        task.review_sha = Some(integration_sha.clone());
        self.state.integration.integration_head = Some(integration_sha);
        self.state.integration.verification_head = None;
        self.state.integration.merged_tasks.insert(task_id.into());
        self.next_merge_or_review()
    }

    fn quarantine_merged_task(
        &mut self,
        task_id: &str,
        reason: String,
    ) -> Result<Vec<Effect>, ProcessorError> {
        let task = self.task_mut(task_id)?;
        task.phase = TaskPhase::Conflict;
        task.reason = Some(reason.clone());
        let mut effects = vec![Effect::ReturnTask {
            task_id: task_id.into(),
            reason,
        }];
        effects.extend(self.next_merge_or_review()?);
        Ok(effects)
    }

    fn integration_verified(
        &mut self,
        head: String,
        outcome: VerificationOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "integration verification")?;
        validate_ref(&head, "integration verification head")?;
        if self.state.integration.integration_head.as_deref() != Some(head.as_str()) {
            return Err(ProcessorError::InvalidCommand(format!(
                "integration verification belongs to {head:?}, not current tip {:?}",
                self.state.integration.integration_head
            )));
        }
        if let VerificationOutcome::Failed { signature, .. } = &outcome {
            validate_signature(signature)?;
        }
        self.state.integration.verification_attempts = self
            .state
            .integration
            .verification_attempts
            .saturating_add(1);
        match outcome {
            VerificationOutcome::Passed | VerificationOutcome::Exempt { .. } => {
                self.state.integration.verification_head = Some(head);
                Ok(vec![Effect::Publish {
                    batch_id: self.batch_id()?.to_string(),
                }])
            }
            VerificationOutcome::Failed { signature, reason } => self.fail_integration(format!(
                "integration verification failed ({signature}): {reason}"
            )),
            VerificationOutcome::Blocked { reason } => {
                self.state.phase = Phase::Blocked;
                self.state.blocked_reason =
                    Some(format!("integration verification blocked: {reason}"));
                Ok(vec![Effect::WaitForOperator { reason }])
            }
        }
    }

    fn integration_review(
        &mut self,
        outcome: ReviewOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "integration review")?;
        match outcome {
            ReviewOutcome::Clean { review_sha } => {
                validate_ref(&review_sha, "integration review SHA")?;
                if self.state.integration.integration_head.as_deref() != Some(review_sha.as_str()) {
                    return Err(ProcessorError::InvalidCommand(format!(
                        "integration review belongs to {review_sha:?}, not current tip {:?}",
                        self.state.integration.integration_head
                    )));
                }
                self.state.integration.f_cycles = self.state.integration.f_cycles.saturating_add(1);
                if self.state.integration.f_cycles > self.config.integration_loop_max {
                    return self.fail_integration(format!(
                        "не сходится ревью после {} циклов",
                        self.config.integration_loop_max
                    ));
                }
                self.state.integration.review_sha = Some(review_sha);
                self.state.integration.verification_head = None;
                let head = self
                    .state
                    .integration
                    .integration_head
                    .clone()
                    .ok_or_else(|| {
                        ProcessorError::InvalidCommand(
                            "clean integration review has no exact integration tip to verify"
                                .into(),
                        )
                    })?;
                Ok(vec![Effect::VerifyIntegration { head }])
            }
            // `open_findings` (task T-014) is not (yet) consumed here — the empty-fixed-set
            // early exit is scoped to the per-task loop (phases 2.5/2.8); this batch-level loop
            // keeps `stagnation_decision` as its sole stall detector.
            ReviewOutcome::Findings {
                signature,
                open_findings: _,
                open_finding_ids: _,
            } => {
                validate_signature(&signature)?;
                self.state.integration.verification_head = None;
                self.state.integration.f_cycles = self.state.integration.f_cycles.saturating_add(1);
                if self.state.integration.f_cycles > self.config.integration_loop_max {
                    return self.fail_integration(format!(
                        "не сходится ревью после {} циклов",
                        self.config.integration_loop_max
                    ));
                }
                self.state.integration.signatures.push(signature);
                let signatures = signatures_from(&self.state.integration.signatures);
                if let Some(reason) = stagnation_reason(&signatures, self.config.stagnation_limit) {
                    return self.fail_integration(reason);
                }
                Ok(vec![self.dispatch_integration(LeafKind::IntegrationFix)])
            }
            ReviewOutcome::Incomplete => {
                // `F-циклов` counts every completed full-review pass, including a pass that
                // produced neither a fresh SUMMARY-F nor an F-* list.  Otherwise an absent or
                // stale `review_integration.md` would re-dispatch the reviewer forever and
                // bypass `INTEGRATION_LOOP_MAX`.
                self.state.integration.f_cycles = self.state.integration.f_cycles.saturating_add(1);
                if self.state.integration.f_cycles >= self.config.integration_loop_max {
                    return self.fail_integration(format!(
                        "не сходится ревью после {} циклов",
                        self.state.integration.f_cycles
                    ));
                }
                Ok(vec![self.dispatch_integration(LeafKind::IntegrationReview)])
            }
            ReviewOutcome::Escalated { reason } => self.fail_integration(reason),
            ReviewOutcome::CleanRiskElevated { risk, .. }
            | ReviewOutcome::FindingsRiskElevated { risk, .. } => self.fail_integration(format!(
                "integration reviewer reported unsupported task risk elevation {}",
                risk.as_str()
            )),
        }
    }

    fn integration_fix(&mut self, outcome: LeafOutcome) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "integration fix")?;
        match outcome {
            // An integration (`F-`) fixer runs the same Mode-2 contract as a per-task fixer (both
            // are `agents/coder.md` "Режим 2") and may equally report `не исправлено` entries.
            // Unlike the per-task loop (phases 2.5/2.8), this batch-level loop does not (yet)
            // correlate them against an open-finding count — task T-014 scopes the early-exit
            // signal to the per-task loop only — so a won't-fix report here is treated exactly
            // like an ordinary completion; the batch's own review-signature `stagnation_decision`
            // remains the sole stall detector for this loop.
            LeafOutcome::Completed { .. } | LeafOutcome::CompletedWithWontFix { .. } => {
                Ok(vec![Effect::CommitIntegrationFix])
            }
            LeafOutcome::RetryableFailure { reason } | LeafOutcome::Escalated { reason } => {
                self.fail_integration(format!("integration fix failed: {reason}"))
            }
            LeafOutcome::RiskElevated { risk, .. } => self.fail_integration(format!(
                "integration fix reported unsupported task risk elevation {}",
                risk.as_str()
            )),
        }
    }

    fn integration_fix_committed(&mut self, head: String) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "integration fix commit")?;
        validate_ref(&head, "integration fix head")?;
        self.state.integration.integration_head = Some(head);
        self.state.integration.verification_head = None;
        // The legacy processor completes the fixer selected by the final permitted full-review
        // pass, but it never spends an additional (over-limit) review call to validate it.
        if self.state.integration.f_cycles >= self.config.integration_loop_max {
            return self.fail_integration(format!(
                "не сходится ревью после {} циклов",
                self.state.integration.f_cycles
            ));
        }
        Ok(vec![self.dispatch_integration(LeafKind::IntegrationReview)])
    }

    fn published(&mut self, head: String, pushed: bool) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "publication result")?;
        validate_ref(&head, "published head")?;
        if self.state.integration.merged_tasks.is_empty() {
            return Err(ProcessorError::InvalidCommand(
                "cannot publish a batch with no merged tasks".into(),
            ));
        }
        if self.state.integration.integration_head.as_deref() != Some(head.as_str())
            || self.state.integration.verification_head.as_deref() != Some(head.as_str())
        {
            return Err(ProcessorError::InvalidCommand(format!(
                "publication head {head:?} lacks matching final verification for integration tip {:?}",
                self.state.integration.integration_head
            )));
        }
        self.state.integration.publish_attempts =
            self.state.integration.publish_attempts.saturating_add(1);
        self.state.integration.publication_reanchor_reason = None;
        self.state.integration.publication_reanchor_target = None;
        for task_id in self.state.integration.merged_tasks.clone() {
            let task = self.task_mut(&task_id)?;
            expect_task_phase(task, TaskPhase::Merged, "publication")?;
            task.phase = TaskPhase::Published;
        }
        self.state.integration.published_head = Some(head.clone());
        self.state.integration.publication_pushed = Some(pushed);
        self.state.integration.ci_disposition = None;
        self.state.integration.archive_ci_gate = None;
        Ok(vec![Effect::VerifyCi { head }])
    }

    fn publication_rejected(&mut self, reason: String) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "publication rejection")?;
        if reason.trim().is_empty() || reason.contains('\0') {
            return Err(ProcessorError::InvalidCommand(
                "publication rejection must carry a non-empty safe reason".into(),
            ));
        }
        if self.state.integration.merged_tasks.is_empty() {
            return Err(ProcessorError::InvalidCommand(
                "cannot reject publication for a batch with no merged tasks".into(),
            ));
        }

        let mut escalations = Vec::new();
        for task_id in self.state.integration.merged_tasks.clone() {
            let task = self.task_mut(&task_id)?;
            expect_task_phase(task, TaskPhase::Merged, "publication rejection")?;
            task.phase = TaskPhase::Escalated;
            task.reason = Some(reason.clone());
            escalations.push(Effect::EscalateTask {
                task_id,
                reason: reason.clone(),
            });
        }
        self.state.integration.merged_tasks.clear();
        self.state.integration.failed_reason = Some(reason);
        self.state.phase = Phase::Cleaning;

        // The normal Phase-6 journal must describe the reducer's terminal task states before
        // physical cleanup begins. Escalations then materialize descriptor/queue truth before
        // their worktrees are removed, followed by the ordinary cleanup chain.
        let mut cleanup = self.cleanup_effects();
        let journal = match cleanup.first() {
            Some(Effect::WriteJournalAndStatus) => cleanup.remove(0),
            _ => {
                return Err(ProcessorError::InvalidCommand(
                    "publication rejection cleanup must start with a journal boundary".into(),
                ));
            }
        };
        let mut effects = vec![journal];
        effects.extend(escalations);
        effects.extend(cleanup);
        Ok(effects)
    }

    fn publication_awaiting_approval(
        &mut self,
        reason: String,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "publication approval hold")?;
        if reason.trim().is_empty() || reason.contains('\0') {
            return Err(ProcessorError::InvalidCommand(
                "publication approval hold must carry a non-empty safe reason".into(),
            ));
        }
        if self.state.integration.merged_tasks.is_empty()
            || self.state.integration.published_head.is_some()
        {
            return Err(ProcessorError::InvalidCommand(
                "publication approval hold requires a verified but unpublished merged batch".into(),
            ));
        }
        Ok(vec![Effect::WaitForOperator { reason }])
    }

    /// Record a proven publication divergence as a separate durable command/effect pair. A failed
    /// remote push may already have advanced the local primary ref, while a local fast-forward
    /// refusal must retain an external primary advance; the target prevents Phase 0 from treating
    /// either pre-publication shape as a completed release.
    fn publication_reanchor_required(
        &mut self,
        reason: String,
        target: PublicationReanchorTarget,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "publication re-anchor requirement")?;
        if reason.trim().is_empty() || reason.contains('\0') {
            return Err(ProcessorError::InvalidCommand(
                "publication re-anchor must carry a non-empty safe reason".into(),
            ));
        }
        if self.state.integration.merged_tasks.is_empty()
            || self.state.integration.published_head.is_some()
            || self.state.integration.integration_head.is_none()
            || self.state.integration.verification_head != self.state.integration.integration_head
        {
            return Err(ProcessorError::InvalidCommand(
                "publication re-anchor requires a verified but unpublished merged batch".into(),
            ));
        }
        // The external publication attempt completed even when its re-anchor budget is already
        // exhausted. Reserve its event coordinate before the convergence decision so the final
        // failed call cannot collide with the preceding one.
        self.state.integration.publish_attempts =
            self.state.integration.publish_attempts.saturating_add(1);
        if self.state.integration.publication_reanchor_cycles >= self.config.integration_loop_max {
            let blocked_reason = format!(
                "publication re-anchor did not converge after {} attempts; manual intervention is required",
                self.state.integration.publication_reanchor_cycles
            );
            // Do not call `fail_integration` here.  The legacy fallback preserves the current
            // integration branch/worktree and merged candidates for an operator to inspect; it
            // must not start cleanup or pretend that the current integration was discarded.
            self.state.phase = Phase::Blocked;
            self.state.blocked_reason = Some(blocked_reason.clone());
            return Ok(vec![
                Effect::WriteJournalAndStatus,
                Effect::WaitForOperator {
                    reason: blocked_reason,
                },
            ]);
        }
        self.state.integration.publication_reanchor_cycles += 1;
        self.state.integration.publication_reanchor_reason = Some(reason);
        self.state.integration.publication_reanchor_target = Some(target);
        Ok(vec![Effect::ReanchorPublication {
            batch_id: self.batch_id()?.to_string(),
        }])
    }

    /// Return the merged task candidates to the exact pre-merge readiness state after the VCS
    /// port has either reset the primary checkout to a freshly fetched remote base or retained a
    /// proved external local primary, then removed only the integration surface. The task
    /// worktrees/branches and their reviewed SHA remain intact; they are intentionally replayed
    /// through merger, full review and final verification.
    fn publication_reanchored(&mut self) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "publication re-anchor completion")?;
        if self.state.integration.publication_reanchor_reason.is_none() {
            return Err(ProcessorError::InvalidCommand(
                "publication re-anchor completed without a durable requirement".into(),
            ));
        }
        if self.state.integration.merged_tasks.is_empty() {
            return Err(ProcessorError::InvalidCommand(
                "cannot re-anchor a batch with no merged tasks".into(),
            ));
        }
        for task_id in self.state.integration.merged_tasks.clone() {
            let task = self.task_mut(&task_id)?;
            expect_task_phase(task, TaskPhase::Merged, "publication re-anchor")?;
            task.phase = TaskPhase::Ready;
            task.reason = None;
        }

        self.state.integration.workspace_prepared = false;
        self.state.integration.imported_workspace_restore_pending = false;
        self.state.integration.merged_tasks.clear();
        self.state.integration.f_cycles = 0;
        self.state.integration.ci_cycles = 0;
        self.state.integration.ci_disposition = None;
        // Leaf attempts are telemetry call coordinates for the whole batch, not per-reanchor
        // semantic counters. Preserve them so a repeated merger/reviewer/fixer call cannot
        // collide with a completed pre-reanchor event or usage fact.
        self.state.integration.pending_merge_resolution = None;
        self.state.integration.signatures.clear();
        self.state.integration.integration_head = None;
        self.state.integration.review_sha = None;
        self.state.integration.verification_head = None;
        self.state.integration.published_head = None;
        self.state.integration.publication_pushed = None;
        self.state.integration.publication_reanchor_reason = None;
        self.state.integration.publication_reanchor_target = None;
        self.state.integration.failed_reason = None;
        self.state.phase = Phase::Joining;
        Ok(vec![Effect::PrepareIntegrationWorkspace {
            branch: self.integration_branch()?,
        }])
    }

    fn ci_verified(&mut self, outcome: CiOutcome) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "CI result")?;
        let published_head = self
            .state
            .integration
            .published_head
            .as_deref()
            .ok_or_else(|| {
                ProcessorError::InvalidCommand(
                    "CI result arrived without a durable published head".into(),
                )
            })?;
        validate_ref(published_head, "published CI head")?;
        match &outcome {
            CiOutcome::BestEffortDegraded { reason }
            | CiOutcome::RequiredUnconfirmed { reason } => {
                validate_ci_observation_reason(reason)?;
            }
            CiOutcome::Failed { signature, .. } => validate_signature(signature)?,
            CiOutcome::Passed | CiOutcome::LocalOnly | CiOutcome::Disabled => {}
        }
        let inferred_pushed = !matches!(&outcome, CiOutcome::LocalOnly);
        let publication_pushed = self
            .state
            .integration
            .publication_pushed
            .unwrap_or(inferred_pushed);
        if (!publication_pushed && !matches!(&outcome, CiOutcome::LocalOnly))
            || (publication_pushed && matches!(&outcome, CiOutcome::LocalOnly))
        {
            return Err(ProcessorError::InvalidCommand(
                "CI disposition contradicts the durable local/remote publication route".into(),
            ));
        }
        if self.state.integration.publication_pushed.is_none() {
            self.state.integration.publication_pushed = Some(inferred_pushed);
        }
        self.state.integration.ci_wait_attempts =
            self.state.integration.ci_wait_attempts.saturating_add(1);
        match outcome {
            CiOutcome::Passed => {
                self.state.integration.ci_disposition = Some(CiDisposition::Confirmed);
                self.state.phase = Phase::Cleaning;
                Ok(vec![Effect::PrepareKnowledgeCuration])
            }
            CiOutcome::LocalOnly | CiOutcome::Disabled => {
                self.state.integration.ci_disposition = Some(CiDisposition::Disabled);
                self.state.phase = Phase::Cleaning;
                Ok(vec![Effect::PrepareKnowledgeCuration])
            }
            CiOutcome::BestEffortDegraded { reason } => {
                self.state.integration.ci_disposition = Some(CiDisposition::UnconfirmedDegraded);
                let degradation = format!("publication CI was not confirmed: {reason}");
                if !self.state.integration.degradations.contains(&degradation) {
                    self.state.integration.degradations.push(degradation);
                }
                self.state.phase = Phase::Cleaning;
                Ok(vec![Effect::PrepareKnowledgeCuration])
            }
            CiOutcome::RequiredUnconfirmed { reason } => {
                self.state.integration.ci_disposition = Some(CiDisposition::UnconfirmedDegraded);
                let degradation = format!("required publication CI is unconfirmed: {reason}");
                if !self.state.integration.degradations.contains(&degradation) {
                    self.state.integration.degradations.push(degradation);
                }
                let hold_reason = format!("published CI requires manual confirmation: {reason}");
                Ok(vec![
                    Effect::WriteJournalAndStatus,
                    Effect::WaitForOperator {
                        reason: hold_reason,
                    },
                ])
            }
            CiOutcome::Failed {
                signature,
                reason: _,
            } => {
                self.state.integration.ci_disposition = None;
                let published_head =
                    self.state
                        .integration
                        .published_head
                        .clone()
                        .ok_or_else(|| {
                            ProcessorError::InvalidCommand(
                                "CI failure arrived without a durable published head".into(),
                            )
                        })?;
                validate_ref(&published_head, "published CI head")?;
                let notification = Effect::Notify {
                    event: NotificationEvent::PublishCiFailed,
                    subject: published_head,
                };
                self.state.integration.ci_cycles =
                    self.state.integration.ci_cycles.saturating_add(1);
                self.state.integration.signatures.push(signature);
                let decision =
                    review_cycle_decision(self.state.integration.ci_cycles, self.config.ci_fix_max);
                if let Some(reason_from_cap) = decision.escalation_reason() {
                    let mut effects = self.hold_failed_published_ci(format!(
                        "required CI repair limit reached: {reason_from_cap}"
                    ))?;
                    effects.insert(0, notification);
                    return Ok(effects);
                }
                let signatures = signatures_from(&self.state.integration.signatures);
                if let Some(reason_from_stagnation) =
                    stagnation_reason(&signatures, self.config.stagnation_limit)
                {
                    let mut effects = self.hold_failed_published_ci(format!(
                        "required CI repair stagnated: {reason_from_stagnation}"
                    ))?;
                    effects.insert(0, notification);
                    return Ok(effects);
                }
                Ok(vec![notification, Effect::PrepareCiFix])
            }
        }
    }

    fn ci_fix_prepared(
        &mut self,
        outcome: CiFixPreparationOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "Codex CI fix preparation")?;
        match outcome {
            CiFixPreparationOutcome::Completed => {
                self.state.integration.ci_fix_provider_fallback = false;
                Ok(vec![Effect::CommitCiFix])
            }
            CiFixPreparationOutcome::Skipped => {
                self.state.integration.ci_fix_provider_fallback = false;
                Ok(vec![self.dispatch_integration(LeafKind::CiFix)])
            }
            CiFixPreparationOutcome::Fallback => {
                self.state.integration.ci_fix_provider_fallback = true;
                Ok(vec![self.dispatch_integration(LeafKind::CiFix)])
            }
            CiFixPreparationOutcome::SandboxDowngraded { scope } => {
                self.state.integration.ci_fix_provider_fallback = false;
                let degradation = scope.degradation().to_owned();
                if !self.state.integration.degradations.contains(&degradation) {
                    self.state.integration.degradations.push(degradation);
                }
                Ok(vec![
                    Effect::WriteJournalAndStatus,
                    self.dispatch_integration(LeafKind::CiFix),
                ])
            }
            CiFixPreparationOutcome::Escalated { reason } => {
                self.state.integration.ci_fix_provider_fallback = false;
                self.hold_failed_published_ci(format!("Codex CI fix failed: {reason}"))
            }
        }
    }

    fn ci_fix(&mut self, outcome: LeafOutcome) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "CI fix")?;
        let effects = match outcome {
            LeafOutcome::Completed { .. } => Ok(vec![Effect::CommitCiFix]),
            LeafOutcome::RetryableFailure { reason } | LeafOutcome::Escalated { reason } => {
                self.hold_failed_published_ci(format!("CI fix failed: {reason}"))
            }
            LeafOutcome::RiskElevated { risk, .. } => self.hold_failed_published_ci(format!(
                "CI fix reported unsupported task risk elevation {}",
                risk.as_str()
            )),
            // A CI fixer runs the Mode-3 point-fix contract (`агент.md` "Режим 3"), never
            // Mode 2, so `не исправлено` metadata cannot legitimately originate here — held to
            // the same "unsupported extension" treatment as an out-of-scope risk elevation above.
            LeafOutcome::CompletedWithWontFix { .. } => self.hold_failed_published_ci(
                "CI fix reported unsupported fix-cycle won't-fix metadata".into(),
            ),
        };
        self.state.integration.ci_fix_provider_fallback = false;
        effects
    }

    fn ci_fix_committed(&mut self, head: String) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Publishing, "CI fix commit")?;
        validate_ref(&head, "CI fix head")?;
        self.state.integration.published_head = Some(head.clone());
        self.state.integration.ci_disposition = None;
        self.state.integration.archive_ci_gate = None;
        Ok(vec![Effect::VerifyCi { head }])
    }

    fn hold_failed_published_ci(&mut self, reason: String) -> Result<Vec<Effect>, ProcessorError> {
        validate_ci_observation_reason(&reason)?;
        if self.state.integration.published_head.is_none()
            || self.state.integration.publication_pushed != Some(true)
        {
            return Err(ProcessorError::InvalidCommand(
                "terminal CI repair failure requires a remotely published head".into(),
            ));
        }
        self.state.integration.ci_disposition = Some(CiDisposition::UnconfirmedDegraded);
        let degradation = format!("required publication CI remains unconfirmed: {reason}");
        if !self.state.integration.degradations.contains(&degradation) {
            self.state.integration.degradations.push(degradation);
        }
        self.state.blocked_reason = Some(format!(
            "published CI requires manual intervention: {reason}"
        ));
        self.state.phase = Phase::Blocked;
        Ok(vec![
            Effect::WriteJournalAndStatus,
            Effect::WaitForOperator {
                reason: self.state.blocked_reason.clone().unwrap_or_default(),
            },
        ])
    }

    fn knowledge_curated(&mut self, outcome: LeafOutcome) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Cleaning, "knowledge curation")?;
        let (batch_id, mut pending) = self.current_knowledge_curation_retry()?;
        match outcome {
            LeafOutcome::Completed { .. } => {
                self.state.integration.pending_knowledge_curations.clear();
                Ok(self.cleanup_effects())
            }
            LeafOutcome::RetryableFailure { reason } | LeafOutcome::Escalated { reason } => {
                pending.degradations = pending.degradations.saturating_add(1);
                self.state
                    .integration
                    .degradations
                    .push(format!("knowledge curation failed: {reason}"));
                self.state
                    .integration
                    .pending_knowledge_curations
                    .insert(batch_id, pending);
                Ok(self.cleanup_effects())
            }
            LeafOutcome::RiskElevated { risk, .. } => {
                pending.degradations = pending.degradations.saturating_add(1);
                self.state.integration.degradations.push(format!(
                    "knowledge curator reported unsupported task risk elevation {}",
                    risk.as_str()
                ));
                self.state
                    .integration
                    .pending_knowledge_curations
                    .insert(batch_id, pending);
                Ok(self.cleanup_effects())
            }
            LeafOutcome::CompletedWithWontFix { .. } => {
                pending.degradations = pending.degradations.saturating_add(1);
                self.state.integration.degradations.push(
                    "knowledge curator reported unsupported fix-cycle won't-fix metadata".into(),
                );
                self.state
                    .integration
                    .pending_knowledge_curations
                    .insert(batch_id, pending);
                Ok(self.cleanup_effects())
            }
        }
    }

    fn knowledge_curation_prepared(
        &mut self,
        outcome: KnowledgeCurationPreparationOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Cleaning, "knowledge curation preflight")?;
        // Validate the complete publication context before `AlreadyCompleted` is allowed to
        // clear durable retry metadata or `Skipped` is allowed to enter destructive cleanup.
        let _ = self.current_knowledge_curation_retry()?;
        Ok(match outcome {
            KnowledgeCurationPreparationOutcome::Required => {
                vec![self.dispatch_integration(LeafKind::KnowledgeCurator)]
            }
            KnowledgeCurationPreparationOutcome::AlreadyCompleted => {
                self.state.integration.pending_knowledge_curations.clear();
                self.cleanup_effects()
            }
            KnowledgeCurationPreparationOutcome::Skipped => self.cleanup_effects(),
        })
    }

    fn archival_prepared(
        &mut self,
        outcome: ArchivalPreparationOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Cleaning, "archive preflight")?;
        if !self.state.integration.cleanup_journaled {
            return Err(ProcessorError::InvalidCommand(
                "archive preflight requires the Phase-6 journal acknowledgement".into(),
            ));
        }
        let batch = self.state.batch.as_ref().ok_or_else(|| {
            ProcessorError::InvalidCommand("archive preflight requires an active batch".into())
        })?;
        validate_batch_id(&batch.id)?;
        validate_ref(&batch.base, "archive preflight base")?;
        if let Some(task) = self
            .state
            .tasks
            .values()
            .find(|task| matches!(task.phase, TaskPhase::Merged))
        {
            return Err(ProcessorError::InvalidCommand(format!(
                "cannot prepare archive while {} is merged but not published",
                task.id
            )));
        }
        for task_id in &self.state.integration.merged_tasks {
            let task = self
                .state
                .tasks
                .get(task_id)
                .ok_or_else(|| ProcessorError::MissingTask(task_id.clone()))?;
            if !matches!(task.phase, TaskPhase::Published | TaskPhase::Done) {
                return Err(ProcessorError::InvalidCommand(format!(
                    "archive preflight requires {} to be published or terminal-recovery done, found {:?}",
                    task.id, task.phase
                )));
            }
        }
        if let Some(task) = self.state.tasks.values().find(|task| {
            task.phase == TaskPhase::Published
                && !self.state.integration.merged_tasks.contains(&task.id)
        }) {
            return Err(ProcessorError::InvalidCommand(format!(
                "published task {} is absent from the archive cohort",
                task.id
            )));
        }
        if !self.state.integration.merged_tasks.is_empty() {
            let head = self
                .state
                .integration
                .published_head
                .as_deref()
                .ok_or_else(|| {
                    ProcessorError::InvalidCommand(
                        "published archive cohort requires an exact head".into(),
                    )
                })?;
            validate_ref(head, "archive preflight published head")?;
            if self.state.integration.publication_pushed.is_none()
                || self.state.integration.ci_disposition.is_none()
            {
                return Err(ProcessorError::InvalidCommand(
                    "published archive cohort lacks its terminal publication/CI disposition".into(),
                ));
            }
        }
        if self.state.integration.archive_ci_gate.is_some() {
            return Err(ProcessorError::InvalidCommand(
                "archive preflight was already completed for this published head".into(),
            ));
        }
        match outcome {
            ArchivalPreparationOutcome::Skipped => {
                self.state.integration.archive_ci_gate = Some(ArchiveCiGate::Skipped);
                Ok(self.cleanup_effects())
            }
            ArchivalPreparationOutcome::ReconfirmRequired { required_checks } => {
                validate_required_ci_check_names(&required_checks)?;
                if self.state.integration.publication_pushed != Some(true)
                    || self.state.integration.ci_disposition != Some(CiDisposition::Confirmed)
                {
                    return Err(ProcessorError::InvalidCommand(
                        "required archive CI reconfirmation needs a remotely published head with confirmed Phase-5.4 CI".into(),
                    ));
                }
                let head = self
                    .state
                    .integration
                    .published_head
                    .clone()
                    .ok_or_else(|| {
                        ProcessorError::InvalidCommand(
                            "archive CI reconfirmation requires a published head".into(),
                        )
                    })?;
                validate_ref(&head, "archive CI head")?;
                Ok(vec![Effect::ReconfirmCiBeforeArchive {
                    head,
                    required_checks,
                }])
            }
        }
    }

    fn archive_ci_reconfirmed(
        &mut self,
        head: String,
        outcome: CiOutcome,
    ) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Cleaning, "archive CI reconfirmation")?;
        let published_head = self
            .state
            .integration
            .published_head
            .as_deref()
            .ok_or_else(|| {
                ProcessorError::InvalidCommand(
                    "archive CI result arrived without a durable published head".into(),
                )
            })?;
        validate_ref(&head, "archive CI result head")?;
        if head != published_head {
            return Err(ProcessorError::InvalidCommand(format!(
                "archive CI result names stale head {head}, expected {published_head}"
            )));
        }
        if self.state.integration.archive_ci_gate.is_some() {
            return Err(ProcessorError::InvalidCommand(
                "archive CI gate was already completed for this published head".into(),
            ));
        }
        if !self.state.integration.cleanup_journaled
            || self.state.integration.publication_pushed != Some(true)
            || self.state.integration.ci_disposition != Some(CiDisposition::Confirmed)
        {
            return Err(ProcessorError::InvalidCommand(
                "archive CI result requires a journaled remotely published head with confirmed Phase-5.4 CI".into(),
            ));
        }
        match &outcome {
            CiOutcome::RequiredUnconfirmed { reason } => validate_ci_observation_reason(reason)?,
            CiOutcome::Failed { signature, .. } => validate_signature(signature)?,
            CiOutcome::Passed => {}
            CiOutcome::LocalOnly | CiOutcome::Disabled | CiOutcome::BestEffortDegraded { .. } => {
                return Err(ProcessorError::InvalidCommand(
                    "required archive CI reconfirmation returned a non-required disposition".into(),
                ));
            }
        }
        self.state.integration.archive_ci_wait_attempts = self
            .state
            .integration
            .archive_ci_wait_attempts
            .saturating_add(1);
        match outcome {
            CiOutcome::Passed => {
                self.state.integration.archive_ci_gate = Some(ArchiveCiGate::Confirmed);
                Ok(self.cleanup_effects())
            }
            CiOutcome::RequiredUnconfirmed { reason } => {
                self.state.integration.cleanup_journaled = false;
                self.state.phase = Phase::Publishing;
                self.ci_verified(CiOutcome::RequiredUnconfirmed { reason })
            }
            CiOutcome::Failed { signature, reason } => {
                self.state.integration.cleanup_journaled = false;
                self.state.phase = Phase::Publishing;
                self.ci_verified(CiOutcome::Failed { signature, reason })
            }
            CiOutcome::LocalOnly | CiOutcome::Disabled | CiOutcome::BestEffortDegraded { .. } => {
                unreachable!("non-required archive CI outcomes were rejected before mutation")
            }
        }
    }

    fn current_knowledge_curation_retry(
        &self,
    ) -> Result<(String, PendingKnowledgeCuration), ProcessorError> {
        let batch = self.state.batch.as_ref().ok_or_else(|| {
            ProcessorError::InvalidCommand(
                "knowledge curation retry requires an active batch".into(),
            )
        })?;
        validate_batch_id(&batch.id)?;
        validate_ref(&batch.base, "knowledge curation base")?;
        let published_head = self
            .state
            .integration
            .published_head
            .clone()
            .ok_or_else(|| {
                ProcessorError::InvalidCommand(
                    "knowledge curation retry requires a published head".into(),
                )
            })?;
        validate_ref(&published_head, "knowledge curation published head")?;
        let mut fixed_task_findings = 0u32;
        for task_id in &self.state.integration.merged_tasks {
            let task = self
                .state
                .tasks
                .get(task_id)
                .ok_or_else(|| ProcessorError::MissingTask(task_id.clone()))?;
            expect_task_phase(task, TaskPhase::Published, "knowledge curation")?;
            fixed_task_findings = fixed_task_findings
                .saturating_add(u32::try_from(task.review_signatures.len()).unwrap_or(u32::MAX));
        }
        let quarantined_tasks = self
            .state
            .tasks
            .values()
            .filter(|task| task.phase == TaskPhase::Conflict)
            .map(|task| task.id.clone())
            .collect();
        let escalated_tasks = self
            .state
            .tasks
            .values()
            .filter(|task| task.phase == TaskPhase::Escalated)
            .map(|task| task.id.clone())
            .collect();
        Ok((
            batch.id.clone(),
            PendingKnowledgeCuration {
                base: batch.base.clone(),
                published_head,
                merged_tasks: self.state.integration.merged_tasks.clone(),
                fixed_task_findings,
                integration_or_ci_signatures: u32::try_from(
                    self.state.integration.signatures.len(),
                )
                .unwrap_or(u32::MAX),
                ci_failure_cycles: self.state.integration.ci_cycles,
                quarantined_tasks,
                escalated_tasks,
                degradations: u32::try_from(self.state.integration.degradations.len())
                    .unwrap_or(u32::MAX),
            },
        ))
    }

    fn cleanup_complete(&mut self) -> Result<Vec<Effect>, ProcessorError> {
        self.require_phase(Phase::Cleaning, "cleanup")?;
        if let Some(task) = self
            .state
            .tasks
            .values()
            .find(|task| matches!(task.phase, TaskPhase::Merged))
        {
            return Err(ProcessorError::InvalidCommand(format!(
                "cannot clean batch while {} is merged but not published",
                task.id
            )));
        }
        if self.state.integration.archive_ci_gate.is_none() {
            return Err(ProcessorError::InvalidCommand(
                "cannot complete cleanup before the archive CI preflight".into(),
            ));
        }
        // Phase 6.7 follows physical archival and owned-worktree/control cleanup, rather than
        // sharing their ledger turn.  A crash while a previous cleanup effect is outstanding
        // must not make a *future* curator invocation look as though it had already started.
        // The scheduler will call this boundary again after the curator/finalizer chain settles.
        if !self
            .state
            .integration
            .dependency_graph_refreshed_post_archive
        {
            return Ok(vec![Effect::DispatchDependencyCurator {
                boundary: RefreshBoundary::PostArchive,
            }]);
        }
        for task in self.state.tasks.values_mut() {
            if matches!(task.phase, TaskPhase::Published) {
                task.phase = TaskPhase::Done;
            }
        }
        self.state.phase = Phase::Idle;
        self.state.batch = None;
        let pending_knowledge_curations =
            std::mem::take(&mut self.state.integration.pending_knowledge_curations);
        self.state.integration = IntegrationRuntime {
            pending_knowledge_curations,
            ..IntegrationRuntime::default()
        };
        Ok(vec![Effect::WriteJournalAndStatus, Effect::ReleaseLease])
    }

    fn fail_integration(&mut self, reason: String) -> Result<Vec<Effect>, ProcessorError> {
        self.state.integration.failed_reason = Some(reason.clone());
        self.state.blocked_reason = Some(format!("integration failed: {reason}"));
        self.state.phase = Phase::Blocked;
        Ok(vec![
            Effect::WriteJournalAndStatus,
            Effect::WaitForOperator {
                reason: self.state.blocked_reason.clone().unwrap_or_default(),
            },
        ])
    }

    fn cleanup_effects(&self) -> Vec<Effect> {
        // Journal before terminal deletions.  It is the human audit record for the current
        // cohort; losing it after worktrees/descriptors disappeared would make a crash during
        // Phase 6 irrecoverable from the control plane alone.
        let mut effects = Vec::new();
        if !self.state.integration.cleanup_journaled {
            effects.push(Effect::WriteJournalAndStatus);
        }
        if self.state.integration.archive_ci_gate.is_none() {
            effects.push(Effect::PrepareArchival);
            return effects;
        }
        for task in self.state.tasks.values() {
            match task.phase {
                TaskPhase::Published | TaskPhase::Done => {
                    effects.push(Effect::ArchiveTask {
                        task_id: task.id.clone(),
                    });
                    effects.push(Effect::CleanupTaskWorkspace {
                        task_id: task.id.clone(),
                    });
                }
                TaskPhase::Conflict | TaskPhase::Escalated => {
                    effects.push(Effect::CleanupTaskWorkspace {
                        task_id: task.id.clone(),
                    });
                }
                TaskPhase::Capturing
                | TaskPhase::Implementing
                | TaskPhase::Committing
                | TaskPhase::Reviewing
                | TaskPhase::Fixing
                | TaskPhase::Ready
                | TaskPhase::ResolvingMerge
                | TaskPhase::Merged => {}
            }
        }
        effects.push(Effect::CleanupIntegrationWorkspace);
        effects.push(Effect::CleanupCohortControlPlane);
        // Phase 6.7 is scheduled by the later `CleanupComplete` turn after every physical
        // cleanup effect above has been acknowledged.  Never pre-ledger an unknown curator
        // invocation while a preceding archive/worktree effect can still be interrupted.
        effects
    }

    fn close_for_budget_if_needed(&mut self, now_secs: u64) {
        let Some(batch) = self.state.batch.as_ref() else {
            return;
        };
        if batch.admission_closed.is_some() {
            return;
        }
        let elapsed = now_secs.saturating_sub(batch.started_at_secs);
        let counters = CohortCounters {
            admitted_total: batch.admitted_total,
            age_minutes: elapsed / 60,
            elapsed_sec: elapsed,
        };
        let thresholds = CohortThresholds {
            size: self.config.cohort_size,
            max_age_minutes: self.config.cohort_max_age_minutes,
            budget_sec: self.config.cohort_budget_secs,
        };
        if let AdmissionGate::Close(reason) = admission_gate(counters, thresholds) {
            self.close_admission(reason);
        }
    }

    fn close_admission(&mut self, reason: CloseReason) {
        if let Some(batch) = self.state.batch.as_mut() {
            batch.admission_closed = Some(reason.into());
        }
    }

    fn active_tasks(&self) -> Vec<ActiveTask> {
        self.state
            .tasks
            .values()
            .filter_map(|task| {
                task.phase.blocks_admission().map(|class| ActiveTask {
                    domain: Domain::parse(&task.conflict_domain),
                    class,
                })
            })
            .collect()
    }

    fn free_slots(&self) -> usize {
        self.config.max_parallel.saturating_sub(
            self.state
                .tasks
                .values()
                .filter(|task| task.phase.is_active())
                .count(),
        )
    }

    fn tasks_ready_to_merge(&self) -> impl Iterator<Item = &TaskRuntime> {
        self.state
            .tasks
            .values()
            .filter(|task| matches!(task.phase, TaskPhase::Ready))
    }

    fn integration_branch(&self) -> Result<String, ProcessorError> {
        Ok(format!("integration/{}", self.batch_id()?))
    }

    /// Resume Phase 5 from durable evidence only. Re-running review is conservative when no
    /// final verification exists; once it does, publication is idempotently retried; and a
    /// recorded publication resumes its exact-SHA CI gate rather than silently treating it as
    /// complete.
    fn publishing_resume_effect(&mut self) -> Result<Effect, ProcessorError> {
        if self.state.integration.publication_reanchor_reason.is_some() {
            return Ok(Effect::ReanchorPublication {
                batch_id: self.batch_id()?.to_string(),
            });
        }
        if let Some(head) = self.state.integration.published_head.clone() {
            return Ok(Effect::VerifyCi { head });
        }
        if let (Some(integration), Some(verified)) = (
            self.state.integration.integration_head.as_deref(),
            self.state.integration.verification_head.as_deref(),
        ) && integration == verified
        {
            return Ok(Effect::Publish {
                batch_id: self.batch_id()?.to_string(),
            });
        }
        // A recovered legacy integration state may already record every permitted full-review
        // cycle but have no durable verification/publish effect. Never spend a new reviewer call
        // merely because the native runtime checkpoint was interrupted at that boundary.
        if self.state.integration.f_cycles >= self.config.integration_loop_max {
            let reason = format!(
                "не сходится ревью после {} циклов",
                self.state.integration.f_cycles
            );
            self.state.phase = Phase::Blocked;
            self.state.blocked_reason = Some(format!("integration review failed: {reason}"));
            return Ok(Effect::WaitForOperator { reason });
        }
        Ok(self.dispatch_integration(LeafKind::IntegrationReview))
    }

    /// Persist the attempt before exposing an integration leaf to the effect executor. This is
    /// intentionally separate from review/CI cycle counters: a transient incomplete review is a
    /// new model invocation even though it is not a new findings/fix cycle.
    fn dispatch_integration(&mut self, kind: LeafKind) -> Effect {
        self.state.integration.leaf_attempt(kind);
        Effect::DispatchIntegration { kind }
    }

    fn batch_id(&self) -> Result<&str, ProcessorError> {
        self.state
            .batch
            .as_ref()
            .map(|batch| batch.id.as_str())
            .ok_or_else(|| ProcessorError::InvalidCommand("active cohort is missing".into()))
    }

    fn task_mut(&mut self, id: &str) -> Result<&mut TaskRuntime, ProcessorError> {
        self.state
            .tasks
            .get_mut(id)
            .ok_or_else(|| ProcessorError::MissingTask(id.into()))
    }

    fn require_phase(&self, expected: Phase, operation: &str) -> Result<(), ProcessorError> {
        if self.state.phase == expected {
            Ok(())
        } else {
            Err(ProcessorError::InvalidCommand(format!(
                "{operation} requires {expected:?}, current phase is {:?}",
                self.state.phase
            )))
        }
    }
}

/// Preserve Phase-6 order while removing an effect that was already restored from the native
/// checkpoint.  Equality is intentional: an incompatible effect with the same durable key is
/// rejected by `ProcessorRuntime` before this reducer result can be driven.
fn push_unique_effect(effects: &mut Vec<Effect>, effect: Effect) {
    if !effects.contains(&effect) {
        effects.push(effect);
    }
}

fn expect_task_phase(
    task: &TaskRuntime,
    expected: TaskPhase,
    operation: &'static str,
) -> Result<(), ProcessorError> {
    if task.phase == expected {
        Ok(())
    } else {
        Err(ProcessorError::UnexpectedTaskPhase {
            task_id: task.id.clone(),
            expected: operation,
            actual: task.phase,
        })
    }
}

/// Record and schedule one additional transient attempt only when the number of *already
/// dispatched* attempts is below the configured inclusive cap.  The initial attempt is recorded
/// at dispatch time, so `CALL_MAX_ATTEMPTS=2` permits exactly two launches, never three.
fn schedule_leaf_retry(task: &mut TaskRuntime, kind: LeafKind, max_attempts: u32) -> bool {
    if !can_schedule_leaf(task, kind, max_attempts) {
        false
    } else {
        task.leaf_attempt(kind);
        true
    }
}

fn can_schedule_leaf(task: &TaskRuntime, kind: LeafKind, max_attempts: u32) -> bool {
    task.leaf_attempts
        .get(kind.as_str())
        .copied()
        .unwrap_or_default()
        < max_attempts
}

fn signatures_from(raw: &[String]) -> Vec<AttemptSignature> {
    raw.iter()
        .map(|signature| AttemptSignature::of(signature))
        .collect()
}

fn stagnation_reason(signatures: &[AttemptSignature], limit: u32) -> Option<String> {
    match stagnation_decision(signatures, limit) {
        StagnationDecision::Stagnated { .. } => {
            stagnation_decision(signatures, limit).escalation_reason()
        }
        StagnationDecision::Progressing => None,
    }
}

fn validate_task_id(id: &str) -> Result<(), ProcessorError> {
    if is_task_id(id) {
        Ok(())
    } else {
        Err(ProcessorError::InvalidCommand(format!(
            "invalid task id {id:?}; expected T- followed by ASCII digits"
        )))
    }
}

fn validate_conflict_domain(value: &str) -> Result<(), ProcessorError> {
    if crate::state::descriptor::parse_conflict_domain(value).is_some() {
        Ok(())
    } else {
        Err(ProcessorError::InvalidCommand(format!(
            "invalid conflict domain {value:?}; expected non-empty repository-relative path globs"
        )))
    }
}

fn validate_batch_id(id: &str) -> Result<(), ProcessorError> {
    let valid = id
        .strip_prefix("B-")
        .is_some_and(|suffix| !suffix.is_empty() && !suffix.chars().any(char::is_whitespace));
    if valid && !id.starts_with('-') && !id.contains('\0') {
        Ok(())
    } else {
        Err(ProcessorError::InvalidCommand(format!(
            "invalid batch id {id:?}"
        )))
    }
}

fn validate_ref(value: &str, what: &str) -> Result<(), ProcessorError> {
    if value.trim().is_empty() || value.starts_with('-') || value.contains('\0') {
        Err(ProcessorError::InvalidCommand(format!(
            "invalid {what} {value:?}"
        )))
    } else {
        Ok(())
    }
}

fn validate_required_ci_check_names(checks: &[String]) -> Result<(), ProcessorError> {
    if checks.is_empty() {
        return Err(ProcessorError::InvalidCommand(
            "archive CI reconfirmation requires at least one check name".into(),
        ));
    }
    let mut unique = BTreeSet::new();
    for check in checks {
        if check.trim() != check
            || check.is_empty()
            || check.chars().any(|ch| matches!(ch, '\0' | '\r' | '\n'))
            || !unique.insert(check)
        {
            return Err(ProcessorError::InvalidCommand(
                "archive CI reconfirmation contains an invalid or duplicate check name".into(),
            ));
        }
    }
    Ok(())
}

fn validate_ci_observation_reason(reason: &str) -> Result<(), ProcessorError> {
    if reason.trim().is_empty() || reason.contains('\0') {
        return Err(ProcessorError::InvalidCommand(
            "CI observation must carry a non-empty safe reason".into(),
        ));
    }
    Ok(())
}

fn validate_merge_conflict_paths(paths: &[String]) -> Result<(), ProcessorError> {
    if paths.is_empty() {
        return Err(ProcessorError::InvalidCommand(
            "merge conflict must include at least one relative path".into(),
        ));
    }
    for path in paths {
        let bytes = path.as_bytes();
        let has_windows_prefix =
            bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';
        if path.is_empty()
            || path.starts_with('/')
            || path.starts_with('\\')
            || path.contains('\0')
            || has_windows_prefix
            || path.split(['/', '\\']).any(|part| {
                part.is_empty() || part == "." || part == ".." || part == ".git" || part == ".jj"
            })
        {
            return Err(ProcessorError::InvalidCommand(format!(
                "merge conflict path is not a safe repository-relative path: {path:?}"
            )));
        }
    }
    Ok(())
}

fn validate_merge_protected_paths(
    merge_paths: &[String],
    conflict_paths: &[String],
    protected_paths: &[MergePathFingerprint],
) -> Result<(), ProcessorError> {
    let expected = merge_paths
        .iter()
        .filter(|path| !conflict_paths.contains(path))
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for protected in protected_paths {
        validate_merge_conflict_paths(std::slice::from_ref(&protected.path))?;
        if !protected.sha256.as_deref().is_none_or(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        }) {
            return Err(ProcessorError::InvalidCommand(format!(
                "merge-path fingerprint for {:?} is not a SHA-256 digest",
                protected.path
            )));
        }
        if !actual.insert(protected.path.as_str()) {
            return Err(ProcessorError::InvalidCommand(format!(
                "merge-path fingerprint repeats {:?}",
                protected.path
            )));
        }
    }
    if actual != expected {
        return Err(ProcessorError::InvalidCommand(
            "merge-path fingerprints do not cover exactly the clean portion of the typed merge surface"
                .into(),
        ));
    }
    Ok(())
}

fn validate_signature(value: &str) -> Result<(), ProcessorError> {
    if value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ProcessorError::InvalidCommand(
            "finding/error signature must be 16 ASCII hex characters".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn processor() -> Processor {
        Processor::new(ProcessorConfig {
            max_parallel: 2,
            cohort_size: 3,
            review_loop_max: 2,
            integration_loop_max: 2,
            ci_fix_max: 2,
            stagnation_limit: 2,
            leaf_max_attempts: 1,
            ..ProcessorConfig::default()
        })
        .unwrap()
    }

    #[test]
    fn stagnation_limit_requires_at_least_two_attempts() {
        let invalid = ProcessorConfig {
            stagnation_limit: 1,
            ..ProcessorConfig::default()
        };
        assert!(matches!(
            invalid.validate(),
            Err(ProcessorError::InvalidConfig(message))
                if message == "STAGNATION_LIMIT must be at least 2"
        ));

        let valid = ProcessorConfig {
            stagnation_limit: 2,
            ..ProcessorConfig::default()
        };
        valid.validate().unwrap();
    }

    fn candidate(id: &str, domain: &str) -> AdmissionCandidate {
        AdmissionCandidate {
            id: id.into(),
            conflict_domain: domain.into(),
            level: Level::Coder,
            risk: Risk::Medium,
            ready: true,
            current_delivery_lane: true,
        }
    }

    fn signature(value: &str) -> String {
        AttemptSignature::of(value).as_str().to_string()
    }

    fn open(p: &mut Processor) {
        assert_eq!(
            p.apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap(),
            vec![Effect::PersistCheckpoint, Effect::WriteJournalAndStatus]
        );
        let effects = p
            .apply(ProcessorCommand::Open {
                batch_id: "B-20260724T120000Z".into(),
                base: "abc123".into(),
                now_secs: 100,
            })
            .unwrap();
        assert_eq!(
            effects,
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchDependencyCurator {
                    boundary: RefreshBoundary::CohortOpen,
                }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::DependencyGraphRefreshed {
                boundary: RefreshBoundary::CohortOpen,
                outcome: LeafOutcome::Completed {
                    author: Some("dependency_curator".into()),
                },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::ReconcileInbox { free_slots: 2 }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::InboxReconciled {
                free_slots: 2,
                curation_required: false,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DrainQueueInbox { free_slots: 2 }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::InboxDrained { free_slots: 2 })
                .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::PlanNextWave { free_slots: 2 }
            ]
        );
    }

    fn acknowledge_journal_and_skip_archive_ci(p: &mut Processor) -> Vec<Effect> {
        p.acknowledge_non_command_effect(&Effect::WriteJournalAndStatus)
            .unwrap();
        p.apply(ProcessorCommand::ArchivalPrepared {
            outcome: ArchivalPreparationOutcome::Skipped,
        })
        .unwrap()
    }

    fn finish_open_dependency_refresh(p: &mut Processor) {
        p.apply(ProcessorCommand::DependencyGraphRefreshed {
            boundary: RefreshBoundary::CohortOpen,
            outcome: LeafOutcome::Completed {
                author: Some("dependency_curator".into()),
            },
        })
        .unwrap();
    }

    #[test]
    fn planner_wave_reconciles_curates_and_drains_inbox_before_dispatch() {
        let mut p = processor();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        let opened = p
            .apply(ProcessorCommand::Open {
                batch_id: "B-20260725T120000Z".into(),
                base: "base".into(),
                now_secs: 1,
            })
            .unwrap();
        assert_eq!(
            opened,
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchDependencyCurator {
                    boundary: RefreshBoundary::CohortOpen,
                }
            ]
        );
        finish_open_dependency_refresh(&mut p);
        assert_eq!(
            p.apply(ProcessorCommand::InboxReconciled {
                free_slots: 2,
                curation_required: true,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchInboxCurator {
                    free_slots: 2,
                    mode: InboxCurationMode::Intake,
                }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::InboxCurated {
                free_slots: 2,
                mode: InboxCurationMode::Intake,
                outcome: LeafOutcome::Completed {
                    author: Some("inbox_curator".into()),
                },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DrainQueueInbox { free_slots: 2 }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::InboxDrained { free_slots: 2 })
                .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::PlanNextWave { free_slots: 2 }
            ]
        );
    }

    #[test]
    fn failed_inbox_curation_holds_the_current_cohort_instead_of_replaying_unknown_work() {
        let mut p = processor();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120000Z".into(),
            base: "base".into(),
            now_secs: 1,
        })
        .unwrap();
        finish_open_dependency_refresh(&mut p);
        p.apply(ProcessorCommand::InboxReconciled {
            free_slots: 2,
            curation_required: true,
        })
        .unwrap();
        let effects = p
            .apply(ProcessorCommand::InboxCurated {
                free_slots: 2,
                mode: InboxCurationMode::Intake,
                outcome: LeafOutcome::RetryableFailure {
                    reason: "contained process timed out".into(),
                },
            })
            .unwrap();
        assert_eq!(p.state().phase, Phase::Blocked);
        assert!(matches!(
            effects.as_slice(),
            [Effect::WaitForOperator { reason }]
                if reason.contains("inbox curator Intake: contained process timed out")
        ));
    }

    #[test]
    fn dependency_graph_failure_is_journaled_but_does_not_block_independent_admission() {
        let mut p = processor();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120000Z".into(),
            base: "base".into(),
            now_secs: 1,
        })
        .unwrap();
        assert_eq!(
            p.apply(ProcessorCommand::DependencyGraphRefreshed {
                boundary: RefreshBoundary::CohortOpen,
                outcome: LeafOutcome::Escalated {
                    reason: "registry is unavailable".into(),
                },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::WriteJournalAndStatus,
                Effect::ReconcileInbox { free_slots: 2 },
            ]
        );
        assert_eq!(p.state().phase, Phase::Rolling);
        assert!(p.state().integration.dependency_graph_refreshed_open);
        assert!(p.state().integration.degradations[0].contains("registry is unavailable"));
    }

    #[test]
    fn post_archive_inbox_finalization_is_checkpointed_and_cannot_reopen_admission() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Cleaning;
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Published;
        p.state.tasks.insert(task.id.clone(), task);
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.published_head = Some("published-head".into());
        p.state.integration.publication_pushed = Some(false);
        p.state.integration.ci_disposition = Some(CiDisposition::Disabled);

        let cleanup = p
            .apply(ProcessorCommand::KnowledgeCurated {
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap();
        assert_eq!(cleanup.last(), Some(&Effect::PrepareArchival));
        let cleanup = acknowledge_journal_and_skip_archive_ci(&mut p);
        assert_eq!(cleanup.last(), Some(&Effect::CleanupCohortControlPlane));
        assert_eq!(
            p.apply(ProcessorCommand::CleanupComplete).unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchDependencyCurator {
                    boundary: RefreshBoundary::PostArchive,
                }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::DependencyGraphRefreshed {
                boundary: RefreshBoundary::PostArchive,
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::ReconcileInboxFinalization
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::InboxFinalizationReconciled {
                curation_required: true,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchInboxCurator {
                    free_slots: 0,
                    mode: InboxCurationMode::Finalize,
                }
            ]
        );
        assert!(
            p.apply(ProcessorCommand::InboxCurated {
                free_slots: 0,
                mode: InboxCurationMode::Finalize,
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap()
            .is_empty()
        );
        assert_eq!(p.state().phase, Phase::Cleaning);
        p.apply(ProcessorCommand::CleanupComplete).unwrap();
        assert_eq!(p.state().phase, Phase::Idle);
    }

    #[test]
    fn post_archive_dependency_failure_still_reaches_final_reply_reconciliation_once() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Cleaning;
        assert_eq!(
            p.apply(ProcessorCommand::DependencyGraphRefreshed {
                boundary: RefreshBoundary::PostArchive,
                outcome: LeafOutcome::RetryableFailure {
                    reason: "registry is busy".into(),
                },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::WriteJournalAndStatus,
                Effect::ReconcileInboxFinalization,
            ]
        );
        assert!(
            p.state()
                .integration
                .dependency_graph_refreshed_post_archive
        );
        assert!(matches!(
            p.apply(ProcessorCommand::DependencyGraphRefreshed {
                boundary: RefreshBoundary::PostArchive,
                outcome: LeafOutcome::Completed { author: None },
            }),
            Err(ProcessorError::InvalidCommand(message))
                if message.contains("already acknowledged")
        ));
    }

    #[test]
    fn post_archive_inbox_finalization_rejects_a_stale_planner_capacity() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Cleaning;
        assert!(matches!(
            p.apply(ProcessorCommand::InboxCurated {
                free_slots: 1,
                mode: InboxCurationMode::Finalize,
                outcome: LeafOutcome::Completed { author: None },
            }),
            Err(ProcessorError::InvalidCommand(message))
                if message.contains("free_slots=1 must be zero")
        ));
    }

    #[test]
    fn inbox_curator_is_token_gated_before_its_processkit_dispatch() {
        let mut p = Processor::new(ProcessorConfig {
            cohort_token_budget: Some(100),
            ..ProcessorConfig::default()
        })
        .unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120000Z".into(),
            base: "base".into(),
            now_secs: 1,
        })
        .unwrap();
        finish_open_dependency_refresh(&mut p);
        let gated = p
            .apply(ProcessorCommand::InboxReconciled {
                free_slots: 3,
                curation_required: true,
            })
            .unwrap();
        assert!(matches!(
            gated.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::CheckTokenBudget {
                    next: ModelCall::InboxCurator {
                        free_slots: 3,
                        mode: InboxCurationMode::Intake,
                    }
                }
            ]
        ));
        assert_eq!(
            p.apply(ProcessorCommand::TokenBudgetChecked {
                next: ModelCall::InboxCurator {
                    free_slots: 3,
                    mode: InboxCurationMode::Intake,
                },
                observation: TokenBudgetObservation::Actual { tokens: 42 },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchInboxCurator {
                    free_slots: 3,
                    mode: InboxCurationMode::Intake,
                }
            ]
        );
    }

    fn task_to_ready(p: &mut Processor, id: &str, commit: &str) {
        p.apply(ProcessorCommand::WorkspaceReady { task_id: id.into() })
            .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: id.into(),
            outcome: LeafOutcome::Completed {
                author: Some("coder".into()),
            },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: id.into(),
            commit: commit.into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: id.into(),
            outcome: ReviewOutcome::Clean {
                review_sha: commit.into(),
            },
        })
        .unwrap();
    }

    #[test]
    fn enabled_token_budget_wraps_every_model_continuation_and_records_actual_usage() {
        let mut p = Processor::new(ProcessorConfig {
            cohort_token_budget: Some(100),
            ..ProcessorConfig::default()
        })
        .unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        let opening = p
            .apply(ProcessorCommand::Open {
                batch_id: "B-20260725T120000Z".into(),
                base: "base".into(),
                now_secs: 1,
            })
            .unwrap();
        assert!(matches!(
            opening.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::CheckTokenBudget {
                    next: ModelCall::DependencyCurator {
                        boundary: RefreshBoundary::CohortOpen,
                    }
                }
            ]
        ));
        assert_eq!(
            p.state().batch.as_ref().unwrap().cohort_token_budget,
            Some(100)
        );

        assert_eq!(
            p.apply(ProcessorCommand::TokenBudgetChecked {
                next: ModelCall::DependencyCurator {
                    boundary: RefreshBoundary::CohortOpen,
                },
                observation: TokenBudgetObservation::Actual { tokens: 1 },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchDependencyCurator {
                    boundary: RefreshBoundary::CohortOpen,
                }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::DependencyGraphRefreshed {
                boundary: RefreshBoundary::CohortOpen,
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::ReconcileInbox { free_slots: 3 }
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::InboxReconciled {
                free_slots: 3,
                curation_required: false,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DrainQueueInbox { free_slots: 3 }
            ]
        );
        let budget_gate = p
            .apply(ProcessorCommand::InboxDrained { free_slots: 3 })
            .unwrap();
        assert!(matches!(
            budget_gate.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::CheckTokenBudget {
                    next: ModelCall::Planner { free_slots: 3 }
                }
            ]
        ));
        let allowed = p
            .apply(ProcessorCommand::TokenBudgetChecked {
                next: ModelCall::Planner { free_slots: 3 },
                observation: TokenBudgetObservation::Actual { tokens: 99 },
            })
            .unwrap();
        assert_eq!(
            allowed,
            vec![
                Effect::PersistCheckpoint,
                Effect::PlanNextWave { free_slots: 3 }
            ]
        );
        assert_eq!(
            p.state().batch.as_ref().unwrap().token_budget_actual_tokens,
            Some(99)
        );
    }

    #[test]
    fn cohort_budget_preflight_prevents_new_model_calls_and_escalates_active_tasks() {
        let mut p = Processor::new(ProcessorConfig {
            max_parallel: 1,
            cohort_size: 2,
            cohort_budget_secs: Some(10),
            ..ProcessorConfig::default()
        })
        .unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        let opened = p
            .apply(ProcessorCommand::Open {
                batch_id: "B-20260725T120000Z".into(),
                base: "base".into(),
                now_secs: 100,
            })
            .unwrap();
        assert!(matches!(
            opened.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::CheckCohortBudget {
                    next: ModelCall::DependencyCurator {
                        boundary: RefreshBoundary::CohortOpen,
                    }
                }
            ]
        ));
        assert_eq!(
            p.state().batch.as_ref().unwrap().cohort_budget_secs,
            Some(10)
        );
        assert_eq!(
            p.apply(ProcessorCommand::CohortBudgetChecked {
                next: ModelCall::DependencyCurator {
                    boundary: RefreshBoundary::CohortOpen,
                },
                now_secs: 101,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchDependencyCurator {
                    boundary: RefreshBoundary::CohortOpen,
                }
            ]
        );
        p.apply(ProcessorCommand::DependencyGraphRefreshed {
            boundary: RefreshBoundary::CohortOpen,
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::InboxReconciled {
            free_slots: 1,
            curation_required: false,
        })
        .unwrap();
        let gate = p
            .apply(ProcessorCommand::InboxDrained { free_slots: 1 })
            .unwrap();
        assert!(matches!(
            gate.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::CheckCohortBudget {
                    next: ModelCall::Planner { free_slots: 1 }
                }
            ]
        ));
        p.apply(ProcessorCommand::CohortBudgetChecked {
            next: ModelCall::Planner { free_slots: 1 },
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let prepared = p
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            })
            .unwrap();
        assert!(matches!(
            prepared.as_slice(),
            [Effect::PersistCheckpoint, Effect::CheckCohortBudget { next: ModelCall::TaskLeafPreparation { task_id, kind: LeafKind::Implement } }]
                if task_id == "T-1"
        ));

        let halted = p
            .apply(ProcessorCommand::CohortBudgetChecked {
                next: ModelCall::TaskLeafPreparation {
                    task_id: "T-1".into(),
                    kind: LeafKind::Implement,
                },
                now_secs: 110,
            })
            .unwrap();
        assert!(halted.iter().any(|effect| {
            matches!(effect, Effect::EscalateTask { task_id, reason }
                if task_id == "T-1" && reason == "COHORT_BUDGET_SEC elapsed=10 limit=10")
        }));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Escalated);
        assert_eq!(
            p.state().batch.as_ref().unwrap().admission_closed,
            Some(CloseReasonWire::CohortMaxAge)
        );
    }

    #[test]
    fn checkpoint_refuses_a_changed_cohort_wall_clock_budget() {
        let config = ProcessorConfig {
            cohort_budget_secs: Some(10),
            ..ProcessorConfig::default()
        };
        let mut p = Processor::new(config.clone()).unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120001Z".into(),
            base: "base".into(),
            now_secs: 100,
        })
        .unwrap();

        let mismatched = ProcessorConfig {
            cohort_budget_secs: Some(11),
            ..config
        };
        assert!(matches!(
            Processor::from_checkpoint(mismatched, p.state().clone()),
            Err(ProcessorError::CorruptCheckpoint(message))
                if message.contains("cohort safety differs")
        ));
    }

    #[test]
    fn checkpoint_refuses_a_changed_unmetered_usage_policy() {
        let config = ProcessorConfig {
            cohort_token_budget: Some(100),
            cohort_token_budget_strict: true,
            ..ProcessorConfig::default()
        };
        let mut p = Processor::new(config.clone()).unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120001Z".into(),
            base: "base".into(),
            now_secs: 100,
        })
        .unwrap();

        let mismatched = ProcessorConfig {
            cohort_token_budget_strict: false,
            ..config
        };
        assert!(matches!(
            Processor::from_checkpoint(mismatched, p.state().clone()),
            Err(ProcessorError::CorruptCheckpoint(message))
                if message.contains("cohort safety differs")
        ));
    }

    #[test]
    fn exhausted_token_budget_closes_admission_and_terminally_escalates_active_tasks() {
        let mut p = Processor::new(ProcessorConfig {
            cohort_token_budget: Some(10),
            max_parallel: 1,
            // Keep admission open after the single active task so the token gate itself must
            // supply the terminal close reason.
            cohort_size: 2,
            ..ProcessorConfig::default()
        })
        .unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120001Z".into(),
            base: "base".into(),
            now_secs: 1,
        })
        .unwrap();
        p.apply(ProcessorCommand::TokenBudgetChecked {
            next: ModelCall::Planner { free_slots: 1 },
            observation: TokenBudgetObservation::Actual { tokens: 0 },
        })
        .unwrap();
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 2,
        })
        .unwrap();
        let guarded_leaf = p
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            })
            .unwrap();
        assert!(matches!(
            guarded_leaf.last(),
            Some(Effect::CheckTokenBudget {
                next: ModelCall::TaskLeafPreparation { task_id, kind: LeafKind::Implement }
            }) if task_id == "T-1"
        ));

        let halted = p
            .apply(ProcessorCommand::TokenBudgetChecked {
                next: ModelCall::TaskLeafPreparation {
                    task_id: "T-1".into(),
                    kind: LeafKind::Implement,
                },
                observation: TokenBudgetObservation::Actual { tokens: 10 },
            })
            .unwrap();
        assert!(matches!(
            halted.as_slice(),
            [Effect::PersistCheckpoint, Effect::EscalateTask { task_id, reason }, Effect::WriteJournalAndStatus, Effect::PrepareArchival]
                if task_id == "T-1" && reason == "COHORT_TOKEN_BUDGET actual=10 limit=10"
        ));
        assert_eq!(p.state().phase, Phase::Cleaning);
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Escalated);
        assert_eq!(
            p.state().batch.as_ref().unwrap().admission_closed,
            Some(CloseReasonWire::CohortTokenBudget)
        );
        assert!(!halted.iter().any(|effect| matches!(
            effect,
            Effect::DispatchTask { .. } | Effect::ReturnTask { .. }
        )));
    }

    #[test]
    fn diversity_review_and_authoritative_review_have_separate_token_boundaries() {
        let mut p = Processor::new(ProcessorConfig {
            cohort_token_budget: Some(10),
            max_parallel: 1,
            cohort_size: 2,
            ..ProcessorConfig::default()
        })
        .unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120003Z".into(),
            base: "base".into(),
            now_secs: 1,
        })
        .unwrap();
        p.apply(ProcessorCommand::TokenBudgetChecked {
            next: ModelCall::Planner { free_slots: 1 },
            observation: TokenBudgetObservation::Actual { tokens: 0 },
        })
        .unwrap();
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 2,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TokenBudgetChecked {
            next: ModelCall::Task {
                task_id: "T-1".into(),
                kind: LeafKind::Implement,
            },
            observation: TokenBudgetObservation::Actual { tokens: 0 },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        let prepared = p
            .apply(ProcessorCommand::TaskCommitted {
                task_id: "T-1".into(),
                commit: "task-head".into(),
            })
            .unwrap();
        assert!(matches!(
            prepared.as_slice(),
            [Effect::PersistCheckpoint, Effect::CheckTokenBudget { next: ModelCall::TaskReviewPreparation { task_id } }]
                if task_id == "T-1"
        ));

        let after_augment_gate = p
            .apply(ProcessorCommand::TokenBudgetChecked {
                next: ModelCall::TaskReviewPreparation {
                    task_id: "T-1".into(),
                },
                observation: TokenBudgetObservation::Actual { tokens: 6 },
            })
            .unwrap();
        assert_eq!(
            after_augment_gate,
            vec![
                Effect::PersistCheckpoint,
                Effect::PrepareTaskReview {
                    task_id: "T-1".into(),
                },
            ]
        );
        let authoritative_gate = p
            .apply(ProcessorCommand::TaskReviewPrepared {
                task_id: "T-1".into(),
                outcome: TaskReviewPreparationOutcome::DispatchClaude,
            })
            .unwrap();
        assert!(matches!(
            authoritative_gate.as_slice(),
            [Effect::PersistCheckpoint, Effect::CheckTokenBudget { next: ModelCall::Task { task_id, kind: LeafKind::Review } }]
                if task_id == "T-1"
        ));
        assert_eq!(p.state().tasks["T-1"].leaf_attempts["review"], 1);

        let halted = p
            .apply(ProcessorCommand::TokenBudgetChecked {
                next: ModelCall::Task {
                    task_id: "T-1".into(),
                    kind: LeafKind::Review,
                },
                observation: TokenBudgetObservation::Actual { tokens: 10 },
            })
            .unwrap();
        assert!(halted.iter().any(
            |effect| matches!(effect, Effect::EscalateTask { task_id, .. } if task_id == "T-1")
        ));
        assert!(!halted.iter().any(|effect| {
            matches!(
                effect,
                Effect::DispatchTask {
                    kind: LeafKind::Review,
                    ..
                }
            )
        }));
    }

    #[test]
    fn unavailable_token_telemetry_is_a_safe_halt_before_the_planner() {
        let mut p = Processor::new(ProcessorConfig {
            cohort_token_budget: Some(10),
            ..ProcessorConfig::default()
        })
        .unwrap();
        p.apply(ProcessorCommand::Recover {
            workspaces_present: BTreeSet::new(),
        })
        .unwrap();
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120002Z".into(),
            base: "base".into(),
            now_secs: 1,
        })
        .unwrap();
        let halted = p
            .apply(ProcessorCommand::TokenBudgetChecked {
                next: ModelCall::Planner { free_slots: 3 },
                observation: TokenBudgetObservation::Unavailable,
            })
            .unwrap();
        assert!(matches!(
            halted.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::WriteJournalAndStatus,
                Effect::PrepareArchival
            ]
        ));
        assert_eq!(p.state().phase, Phase::Cleaning);
        assert_eq!(
            p.state().batch.as_ref().unwrap().admission_closed,
            Some(CloseReasonWire::CohortTokenBudget)
        );
        assert_eq!(
            p.state().batch.as_ref().unwrap().token_budget_actual_tokens,
            None
        );
    }

    #[test]
    fn recovery_requires_a_workspace_before_resuming_a_live_task() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let checkpoint = p.state().clone();
        let mut resumed = Processor::from_checkpoint(p.config.clone(), checkpoint).unwrap();
        resumed.state.phase = Phase::Recovery;
        let effects = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::WaitForOperator { .. }]
        ));
        assert_eq!(resumed.state().phase, Phase::Blocked);
    }

    #[test]
    fn recovery_preserves_an_operator_block_without_an_active_batch() {
        let mut p = processor();
        p.state.phase = Phase::Blocked;
        p.state.blocked_reason = Some("planner made no progress".into());
        let checkpoint = p.state().clone();

        let mut resumed = Processor::from_checkpoint(p.config.clone(), checkpoint).unwrap();
        let effects = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();

        assert_eq!(resumed.state().phase, Phase::Blocked);
        assert!(matches!(
            effects.as_slice(),
            [Effect::WaitForOperator { reason }] if reason == "planner made no progress"
        ));
    }

    #[test]
    fn imported_recovery_intent_becomes_a_normal_durable_workspace_effect() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.state
            .tasks
            .get_mut("T-1")
            .unwrap()
            .imported_recovery_intent = Some(ImportedRecoveryIntent::EnsureWorkspace);
        p.state.phase = Phase::Recovery;

        let effects = p
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistCheckpoint, Effect::EnsureTaskWorkspace { task_id, .. }] if task_id == "T-1"
        ));
        assert_eq!(p.state().phase, Phase::Rolling);
        assert_eq!(
            p.state().tasks["T-1"].imported_recovery_intent,
            Some(ImportedRecoveryIntent::EnsureWorkspace)
        );
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        assert!(p.state().tasks["T-1"].imported_recovery_intent.is_none());
    }

    #[test]
    fn imported_review_workspace_resumes_review_without_replaying_implementation() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let task = p.state.tasks.get_mut("T-1").unwrap();
        task.phase = TaskPhase::Reviewing;
        task.review_sha = Some("reviewed-tip".into());
        task.imported_recovery_intent = Some(ImportedRecoveryIntent::EnsureWorkspaceForReview);
        p.state.phase = Phase::Recovery;

        let effects = p
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistCheckpoint, Effect::EnsureTaskWorkspace { task_id, .. }] if task_id == "T-1"
        ));
        let effects = p
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            })
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistCheckpoint, Effect::PrepareTaskReview { task_id }] if task_id == "T-1"
        ));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Reviewing);
        assert!(p.state().tasks["T-1"].imported_recovery_intent.is_none());
    }

    #[test]
    fn imported_merge_quarantine_returns_queue_before_resuming_publishing() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let task = p.state.tasks.get_mut("T-1").unwrap();
        task.phase = TaskPhase::Conflict;
        task.reason = Some("merge conflict".into());
        task.imported_recovery_intent = Some(ImportedRecoveryIntent::ReturnConflictToQueue);
        p.state.phase = Phase::Publishing;
        p.state.integration.workspace_prepared = true;
        p.state.integration.integration_head = Some("integration-head".into());

        let checkpoint = p.state().clone();
        let mut resumed = Processor::from_checkpoint(p.config.clone(), checkpoint).unwrap();
        let effects = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();

        assert_eq!(resumed.state().phase, Phase::Publishing);
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::ReturnTask { task_id, reason },
                Effect::DispatchIntegration { kind: LeafKind::IntegrationReview },
            ] if task_id == "T-1" && reason == "merge conflict"
        ));
    }

    #[test]
    fn recovery_prepares_integration_before_merging_a_ready_task() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        task_to_ready(&mut p, "T-1", "a1");
        p.state.batch.as_mut().unwrap().admission_closed = Some(CloseReasonWire::CohortSize);
        p.state.phase = Phase::Joining;

        let checkpoint = p.state().clone();
        let mut resumed = Processor::from_checkpoint(p.config.clone(), checkpoint).unwrap();
        let effects = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();

        assert_eq!(resumed.state().phase, Phase::Joining);
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::PrepareIntegrationWorkspace { .. }
            ]
        ));
        assert!(!resumed.state().integration.workspace_prepared);
    }

    #[test]
    fn published_recovery_restores_a_missing_integration_workspace_before_full_review() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let task = p.state.tasks.get_mut("T-1").unwrap();
        task.phase = TaskPhase::Merged;
        task.review_sha = Some("reviewed-tip".into());
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.integration_head = Some("integration-tip".into());
        p.state.integration.workspace_prepared = false;
        p.state.integration.imported_workspace_restore_pending = true;
        p.state.phase = Phase::Publishing;

        let checkpoint = p.state().clone();
        let mut resumed = Processor::from_checkpoint(p.config.clone(), checkpoint).unwrap();
        let effects = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();

        assert_eq!(resumed.state().phase, Phase::Joining);
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::PrepareIntegrationWorkspace { branch }
            ] if branch == "integration/B-20260724T120000Z"
        ));

        let effects = resumed
            .apply(ProcessorCommand::IntegrationWorkspaceReady)
            .unwrap();
        assert_eq!(resumed.state().phase, Phase::Publishing);
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::DispatchIntegration {
                    kind: LeafKind::IntegrationReview
                }
            ]
        ));
    }

    #[test]
    fn rolling_admission_tops_up_only_free_non_overlapping_slots() {
        let mut p = processor();
        open(&mut p);
        let effects = p
            .apply(ProcessorCommand::Admit {
                candidates: vec![
                    candidate("T-1", "engine/**"),
                    candidate("T-2", "tui/**"),
                    candidate("T-3", "engine/src/**"),
                ],
                now_secs: 101,
            })
            .unwrap();
        assert_eq!(
            effects,
            vec![
                Effect::PersistCheckpoint,
                Effect::EnsureTaskWorkspace {
                    task_id: "T-1".into(),
                    branch: "task/T-1".into(),
                },
                Effect::EnsureTaskWorkspace {
                    task_id: "T-2".into(),
                    branch: "task/T-2".into(),
                },
            ]
        );
        assert_eq!(p.state().batch.as_ref().unwrap().admitted_total, 2);
        // T-1/T-2 are capturing: no free slot means the later top-up does not double-admit T-3.
        assert!(
            p.apply(ProcessorCommand::Admit {
                candidates: vec![candidate("T-3", "engine/src/**")],
                now_secs: 102,
            })
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn rolling_admission_rejects_invalid_conflict_domains_before_runtime_creation() {
        for (domain, expected_valid) in [
            ("", false),
            (" \t ", false),
            ("engine/**, tui/**", true),
            ("engine/**\ntui/**", true),
            ("engine/**\n../outside/**", false),
            ("engine/**\r\n../outside/**", false),
            ("../outside/**", false),
            ("/absolute/**", false),
        ] {
            let mut p = processor();
            open(&mut p);
            let result = p.apply(ProcessorCommand::Admit {
                candidates: vec![candidate("T-1", domain)],
                now_secs: 101,
            });

            if expected_valid {
                assert!(result.is_ok(), "valid domain {domain:?} was rejected");
                assert_eq!(p.state().tasks["T-1"].conflict_domain, domain);
            } else {
                assert!(matches!(
                    result,
                    Err(ProcessorError::InvalidCommand(message))
                        if message.contains("invalid conflict domain")
                ));
                assert!(p.state().tasks.is_empty());
                assert_eq!(p.state().batch.as_ref().unwrap().admitted_total, 0);
            }
        }
    }

    #[test]
    fn review_findings_repair_then_clean_without_replaying_implementation() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        assert_eq!(p.state().tasks["T-1"].previous_review_sha, None);
        assert_eq!(p.state().tasks["T-1"].review_sha.as_deref(), Some("a1"));
        let findings = p
            .apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Findings {
                    signature: signature("R-01 missing error path"),
                    open_findings: 1,
                    open_finding_ids: vec!["R-01".into()],
                },
            })
            .unwrap();
        assert!(matches!(
            findings.last(),
            Some(Effect::PrepareTaskLeaf {
                kind: LeafKind::Fix,
                ..
            })
        ));
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed {
                author: Some("coder".into()),
            },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a2".into(),
        })
        .unwrap();
        assert_eq!(
            p.state().tasks["T-1"].previous_review_sha.as_deref(),
            Some("a1")
        );
        assert_eq!(p.state().tasks["T-1"].review_sha.as_deref(), Some("a2"));
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Clean {
                review_sha: "a2".into(),
            },
        })
        .unwrap();
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Ready);
        assert_eq!(p.state().tasks["T-1"].review_cycles, 2);
    }

    #[test]
    fn coder_risk_elevation_is_strictly_monotonic_and_checkpointed() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();

        let elevated = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::RiskElevated {
                    author: Some("coder".into()),
                    risk: Risk::High,
                    wont_fixed: None,
                },
            })
            .unwrap();
        assert!(matches!(
            elevated.last(),
            Some(Effect::CommitTask { task_id }) if task_id == "T-1"
        ));
        assert_eq!(p.state().tasks["T-1"].risk, Some(Risk::High));
        assert_eq!(
            p.state().tasks["T-1"].implementation_author.as_deref(),
            Some("coder")
        );
        let restored: ProcessorState =
            serde_json::from_str(&serde_json::to_string(p.state()).expect("checkpoint serializes"))
                .expect("checkpoint deserializes");
        assert_eq!(restored.tasks["T-1"].risk, Some(Risk::High));

        let mut lowered = processor();
        open(&mut lowered);
        lowered
            .apply(ProcessorCommand::Admit {
                candidates: vec![candidate("T-2", "engine/**")],
                now_secs: 101,
            })
            .unwrap();
        lowered
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-2".into(),
            })
            .unwrap();
        let rejected = lowered
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-2".into(),
                outcome: LeafOutcome::RiskElevated {
                    author: Some("coder".into()),
                    risk: Risk::Low,
                    wont_fixed: None,
                },
            })
            .unwrap();
        assert_eq!(lowered.state().tasks["T-2"].phase, TaskPhase::Escalated);
        assert!(matches!(
            rejected.last(),
            Some(Effect::EscalateTask { reason, .. }) if reason.contains("only a strictly higher")
        ));

        let mut unknown = processor();
        open(&mut unknown);
        unknown
            .apply(ProcessorCommand::Admit {
                candidates: vec![candidate("T-3", "engine/**")],
                now_secs: 101,
            })
            .unwrap();
        unknown.state.tasks.get_mut("T-3").unwrap().risk = None;
        unknown
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-3".into(),
            })
            .unwrap();
        unknown
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-3".into(),
                outcome: LeafOutcome::RiskElevated {
                    author: Some("coder".into()),
                    risk: Risk::High,
                    wont_fixed: None,
                },
            })
            .unwrap();
        assert_eq!(unknown.state().tasks["T-3"].phase, TaskPhase::Escalated);
        assert!(
            unknown.state().tasks["T-3"]
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("cannot prove"))
        );
    }

    #[test]
    fn reviewer_risk_elevation_is_durable_but_does_not_create_a_human_gate() {
        let mut p = processor();
        open(&mut p);
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Reviewing;
        task.review_sha = Some("reviewed-tip".into());
        p.state.tasks.insert(task.id.clone(), task);

        let effects = p
            .apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::CleanRiskElevated {
                    review_sha: "reviewed-tip".into(),
                    risk: Risk::High,
                },
            })
            .unwrap();
        assert!(effects.is_empty());
        assert_eq!(p.state().tasks["T-1"].risk, Some(Risk::High));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Ready);
    }

    #[test]
    fn incomplete_task_review_is_bounded_by_review_loop_max() {
        let mut p = processor();
        open(&mut p);
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Reviewing;
        task.review_sha = Some("task-tip".into());
        p.state.tasks.insert(task.id.clone(), task);

        assert!(matches!(
            p.apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Incomplete,
            })
            .unwrap()
            .last(),
            Some(Effect::PrepareTaskReview { task_id }) if task_id == "T-1"
        ));
        assert_eq!(p.state.tasks["T-1"].review_cycles, 1);

        let effects = p
            .apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Incomplete,
            })
            .unwrap();
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Escalated);
        assert_eq!(p.state.tasks["T-1"].review_cycles, 2);
        assert!(matches!(effects.last(), Some(Effect::EscalateTask { .. })));
    }

    #[test]
    fn incomplete_rounds_spend_the_same_review_budget_as_productive_ones() {
        // T-026 routed every «reviewer interrupted» shape (a report cut short before its `ИТОГ:`
        // tail, a ready claim without a fresh `SUMMARY-R`) into this arm instead of a terminal
        // escalation, so the arm's accounting is what keeps the loop finite. `Циклов-ревью` is ONE
        // budget shared by all round kinds: an incomplete round spends a unit exactly like a round
        // with findings, and a productive round afterwards neither resets nor re-charges it.
        let mut p = Processor::new(ProcessorConfig {
            max_parallel: 2,
            cohort_size: 3,
            review_loop_max: 4,
            integration_loop_max: 2,
            ci_fix_max: 2,
            stagnation_limit: 2,
            leaf_max_attempts: 1,
            ..ProcessorConfig::default()
        })
        .unwrap();
        open(&mut p);
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Reviewing;
        task.review_sha = Some("task-tip".into());
        p.state.tasks.insert(task.id.clone(), task);

        for expected in 1..=2 {
            assert!(matches!(
                p.apply(ProcessorCommand::TaskReview {
                    task_id: "T-1".into(),
                    outcome: ReviewOutcome::Incomplete,
                })
                .unwrap()
                .last(),
                Some(Effect::PrepareTaskReview { task_id }) if task_id == "T-1"
            ));
            assert_eq!(p.state.tasks["T-1"].review_cycles, expected);
            assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Reviewing);
        }

        // A round that does conclude with findings continues on the SAME counter — the two
        // incomplete rounds before it are not forgiven.
        let effects = p
            .apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Findings {
                    signature: signature("R-01 missing error path"),
                    open_findings: 1,
                    open_finding_ids: vec!["R-01".into()],
                },
            })
            .unwrap();
        assert!(matches!(
            effects.last(),
            Some(Effect::PrepareTaskLeaf {
                kind: LeafKind::Fix,
                ..
            })
        ));
        assert_eq!(p.state.tasks["T-1"].review_cycles, 3);

        // The fourth pass is the last one `REVIEW_LOOP_MAX = 4` allows: an incomplete round here
        // escalates instead of dispatching a fifth reviewer, which is the same wall a findings
        // round would hit at the same count.
        p.state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Reviewing;
        let effects = p
            .apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Incomplete,
            })
            .unwrap();
        assert_eq!(p.state.tasks["T-1"].review_cycles, 4);
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Escalated);
        assert!(matches!(effects.last(), Some(Effect::EscalateTask { .. })));
        assert_eq!(
            p.state.tasks["T-1"].reason.as_deref(),
            Some("не сходится ревью после 4 циклов")
        );
    }

    #[test]
    fn final_task_fix_does_not_schedule_an_over_limit_review() {
        let mut p = processor();
        open(&mut p);
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Committing;
        task.review_cycles = 2;
        p.state.tasks.insert(task.id.clone(), task);

        let effects = p
            .apply(ProcessorCommand::TaskCommitted {
                task_id: "T-1".into(),
                commit: "task-final-fix".into(),
            })
            .unwrap();
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Escalated);
        assert!(matches!(effects.last(), Some(Effect::EscalateTask { .. })));
    }

    #[test]
    fn optional_codex_task_leaf_falls_back_to_a_separate_claude_effect() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let prepared = p
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            })
            .unwrap();
        assert_eq!(
            prepared,
            vec![
                Effect::PersistCheckpoint,
                Effect::PrepareTaskLeaf {
                    task_id: "T-1".into(),
                    kind: LeafKind::Implement,
                },
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::TaskLeafPrepared {
                task_id: "T-1".into(),
                outcome: TaskLeafPreparationOutcome::Fallback,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchTask {
                    task_id: "T-1".into(),
                    kind: LeafKind::Implement,
                },
            ]
        );
        assert_eq!(p.state().tasks["T-1"].leaf_attempts["implement"], 1);

        let committed = p
            .apply(ProcessorCommand::TaskLeafPrepared {
                task_id: "T-1".into(),
                outcome: TaskLeafPreparationOutcome::Completed,
            })
            .unwrap();
        assert_eq!(
            committed,
            vec![
                Effect::PersistCheckpoint,
                Effect::CommitTask {
                    task_id: "T-1".into(),
                },
            ]
        );
        assert_eq!(
            p.state().tasks["T-1"].implementation_author.as_deref(),
            Some("coder_codex")
        );
    }

    #[test]
    fn sandbox_preflight_downgrade_is_checkpointed_before_claude_dispatch() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();

        let effects = p
            .apply(ProcessorCommand::TaskLeafPrepared {
                task_id: "T-1".into(),
                outcome: TaskLeafPreparationOutcome::SandboxDowngraded {
                    scope: CodexSandboxDowngrade::Worktree,
                },
            })
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistCheckpoint, Effect::WriteJournalAndStatus, Effect::DispatchTask { task_id, kind: LeafKind::Implement }]
                if task_id == "T-1"
        ));
        assert_eq!(
            p.state().integration.degradations,
            vec![CodexSandboxDowngrade::Worktree.degradation()]
        );
        assert_eq!(p.state().tasks["T-1"].implementation_author, None);
    }

    #[test]
    fn full_codex_review_can_fall_back_or_complete_at_its_preparation_boundary() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed {
                author: Some("coder".into()),
            },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        assert_eq!(
            p.apply(ProcessorCommand::TaskReviewPrepared {
                task_id: "T-1".into(),
                outcome: TaskReviewPreparationOutcome::DispatchClaude,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchTask {
                    task_id: "T-1".into(),
                    kind: LeafKind::Review,
                },
            ]
        );

        let mut completed = processor();
        open(&mut completed);
        completed
            .apply(ProcessorCommand::Admit {
                candidates: vec![candidate("T-2", "tui/**")],
                now_secs: 101,
            })
            .unwrap();
        completed
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-2".into(),
            })
            .unwrap();
        completed
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-2".into(),
                outcome: LeafOutcome::Completed {
                    author: Some("coder".into()),
                },
            })
            .unwrap();
        completed
            .apply(ProcessorCommand::TaskCommitted {
                task_id: "T-2".into(),
                commit: "a2".into(),
            })
            .unwrap();
        assert_eq!(
            completed
                .apply(ProcessorCommand::TaskReviewPrepared {
                    task_id: "T-2".into(),
                    outcome: TaskReviewPreparationOutcome::Completed(ReviewOutcome::Clean {
                        review_sha: "a2".into(),
                    }),
                })
                .unwrap(),
            Vec::<Effect>::new()
        );
        assert_eq!(completed.state().tasks["T-2"].phase, TaskPhase::Ready);
    }

    #[test]
    fn repeated_finding_escalates_before_spending_full_review_limit() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        let sig = signature("R-01 identical");
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: sig.clone(),
                open_findings: 1,
                open_finding_ids: vec!["R-01".into()],
            },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a2".into(),
        })
        .unwrap();
        let effects = p
            .apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Findings {
                    signature: sig,
                    open_findings: 1,
                    open_finding_ids: vec!["R-01".into()],
                },
            })
            .unwrap();
        assert!(matches!(effects.last(), Some(Effect::EscalateTask { .. })));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Escalated);
    }

    // -- Empty-fixed-set early exit (T-014) -------------------------------------------------

    #[test]
    fn empty_fixed_set_escalates_after_a_single_fix_round_before_a_repeat_review() {
        // The whole point of task T-014: escalate from ONE fix round's own report, never reaching
        // a SECOND review pass — unlike `repeated_finding_escalates_before_spending_full_review_limit`
        // above, which needs two `Findings` rounds (a full extra review call) before
        // `stagnation_decision` fires. Different signatures each round (as the task description's
        // "даже если по сигнатурам находки формально отличаются" scenario) still catch it, because
        // this signal never looks at the signature at all.
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: signature("R-05 wording A"),
                open_findings: 2,
                open_finding_ids: vec!["R-05".into(), "R-06".into()],
            },
        })
        .unwrap();
        assert_eq!(
            p.state().tasks["T-1"].pending_fix_open_findings,
            Some(2),
            "the round's open-finding count is durably recorded for the fix leaf to correlate"
        );
        let effects = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::CompletedWithWontFix {
                    author: Some("coder".into()),
                    wont_fixed: 2,
                },
            })
            .unwrap();
        assert!(
            matches!(effects.last(), Some(Effect::EscalateTask { .. })),
            "an all-won't-fix round escalates immediately: {effects:?}"
        );
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Escalated);
        assert!(
            p.state().tasks["T-1"]
                .reason
                .as_deref()
                .unwrap()
                .starts_with("пустой fixed-набор:"),
            "escalation reason is the empty-fixed-set literal, not stagnation's: {:?}",
            p.state().tasks["T-1"].reason
        );
        assert_eq!(
            p.state().tasks["T-1"].pending_fix_open_findings,
            None,
            "the coordinate is consumed (cleared) once judged"
        );
    }

    #[test]
    fn codex_preparation_preserves_wont_fix_and_escalates_the_same_round() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeafPrepared {
            task_id: "T-1".into(),
            outcome: TaskLeafPreparationOutcome::Completed,
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: signature("R-01 and R-02"),
                open_findings: 2,
                open_finding_ids: vec!["R-01".into(), "R-02".into()],
            },
        })
        .unwrap();

        let effects = p
            .apply(ProcessorCommand::TaskLeafPrepared {
                task_id: "T-1".into(),
                outcome: TaskLeafPreparationOutcome::CompletedWithWontFix { wont_fixed: 2 },
            })
            .unwrap();

        assert!(matches!(effects.last(), Some(Effect::EscalateTask { .. })));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Escalated);
        assert_eq!(p.state().tasks["T-1"].pending_fix_open_findings, None);
    }

    #[test]
    fn risk_elevation_does_not_suppress_empty_fixed_set_escalation() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: signature("R-01"),
                open_findings: 1,
                open_finding_ids: vec!["R-01".into()],
            },
        })
        .unwrap();

        let effects = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::RiskElevated {
                    author: Some("coder".into()),
                    risk: Risk::High,
                    wont_fixed: Some(1),
                },
            })
            .unwrap();

        assert!(matches!(effects.last(), Some(Effect::EscalateTask { .. })));
        assert_eq!(p.state().tasks["T-1"].risk, Some(Risk::High));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Escalated);
    }

    #[test]
    fn empty_fixed_set_does_not_fire_on_a_partial_fix() {
        // Real progress (some findings genuinely fixed, wont_fixed < open_findings) is left
        // entirely to the ordinary path — never a spurious early escalation.
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: signature("R-05, R-06, R-07"),
                open_findings: 3,
                open_finding_ids: vec!["R-05".into(), "R-06".into(), "R-07".into()],
            },
        })
        .unwrap();
        let effects = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::CompletedWithWontFix {
                    author: Some("coder".into()),
                    wont_fixed: 1,
                },
            })
            .unwrap();
        assert!(matches!(effects.last(), Some(Effect::CommitTask { .. })));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Committing);
    }

    #[test]
    fn empty_fixed_set_absent_field_does_not_regress_the_ordinary_completion_path() {
        // A fixer that has never heard of `не исправлено` (task T-014 is purely additive) reports
        // an ordinary `Completed`, not `CompletedWithWontFix` — the fix round proceeds exactly as
        // it always has, with no new escalation path engaged at all.
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: signature("R-05 out of scope"),
                open_findings: 1,
                open_finding_ids: vec!["R-05".into()],
            },
        })
        .unwrap();
        let effects = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::Completed {
                    author: Some("coder".into()),
                },
            })
            .unwrap();
        assert!(matches!(effects.last(), Some(Effect::CommitTask { .. })));
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Committing);
    }

    // -- Durable round coordinates: recorded, then consumed exactly once (R-07/R-08) ----------

    /// One task driven into `Fixing` by a review round that opened `R-05` and `R-06`, i.e. with
    /// both durable won't-fix coordinates populated. The caller concludes that fix round its own
    /// way and asserts what survives.
    fn task_in_a_fix_round() -> Processor {
        drive_into_a_fix_round(processor())
    }

    fn drive_into_a_fix_round(mut p: Processor) -> Processor {
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: signature("R-05 and R-06"),
                open_findings: 2,
                open_finding_ids: vec!["R-05".into(), "R-06".into()],
            },
        })
        .unwrap();
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Fixing);
        assert_eq!(p.state().tasks["T-1"].pending_fix_open_findings, Some(2));
        assert_eq!(
            p.state().tasks["T-1"]
                .pending_fix_open_finding_ids
                .as_deref(),
            Some(["R-05".to_string(), "R-06".to_string()].as_slice()),
            "the round records the exact ids the fixer is dispatched to address (R-06)"
        );
        p
    }

    /// Both round coordinates must be gone. They are durable, and a value that outlived its own
    /// round would be correlated against a LATER, unrelated fixer report — the false terminal
    /// escalation R-07 was filed against.
    fn assert_round_coordinates_consumed(p: &Processor) {
        assert_eq!(
            p.state().tasks["T-1"].pending_fix_open_findings,
            None,
            "the round's open-finding count must not outlive the round that captured it"
        );
        assert_eq!(
            p.state().tasks["T-1"].pending_fix_open_finding_ids,
            None,
            "neither must its id set (R-07): a stale set would validate a later round's \
             `не исправлено` entries against findings that round never saw"
        );
    }

    #[test]
    fn an_ordinary_fix_round_completion_consumes_both_round_coordinates() {
        // R-07. The fixer reported no `не исправлено` at all, so nothing correlates the round —
        // but the coordinates still belong to a round that is now over.
        let mut p = task_in_a_fix_round();
        let effects = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::Completed {
                    author: Some("coder".into()),
                },
            })
            .unwrap();
        assert!(matches!(effects.last(), Some(Effect::CommitTask { .. })));
        assert_round_coordinates_consumed(&p);
    }

    #[test]
    fn a_committing_risk_elevation_consumes_both_round_coordinates() {
        // R-07, the arm where the clearing had to be lifted OUT of `if let Some(wont_fixed)`:
        // an ordinary risk elevation carries no won't-fix metadata at all, yet still concludes
        // the fix round with a commit.
        let mut p = task_in_a_fix_round();
        let effects = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::RiskElevated {
                    author: Some("coder".into()),
                    risk: Risk::High,
                    wont_fixed: None,
                },
            })
            .unwrap();
        assert!(matches!(effects.last(), Some(Effect::CommitTask { .. })));
        assert_eq!(p.state().tasks["T-1"].risk, Some(Risk::High));
        assert_round_coordinates_consumed(&p);
    }

    #[test]
    fn a_codex_prepared_fix_round_consumes_both_round_coordinates() {
        // R-07 for the Codex preparation path, whose four arms are the same reducer decision
        // reached through a different backend.
        let mut p = task_in_a_fix_round();
        let effects = p
            .apply(ProcessorCommand::TaskLeafPrepared {
                task_id: "T-1".into(),
                outcome: TaskLeafPreparationOutcome::Completed,
            })
            .unwrap();
        assert!(matches!(effects.last(), Some(Effect::CommitTask { .. })));
        assert_round_coordinates_consumed(&p);

        let mut p = task_in_a_fix_round();
        let effects = p
            .apply(ProcessorCommand::TaskLeafPrepared {
                task_id: "T-1".into(),
                outcome: TaskLeafPreparationOutcome::RiskElevated {
                    risk: Risk::High,
                    wont_fixed: None,
                },
            })
            .unwrap();
        assert!(matches!(effects.last(), Some(Effect::CommitTask { .. })));
        assert_eq!(p.state().tasks["T-1"].risk, Some(Risk::High));
        assert_round_coordinates_consumed(&p);
    }

    #[test]
    fn a_retryable_fix_failure_keeps_the_round_coordinates_for_the_repeat() {
        // The negative control for the three tests above: a retry is the SAME round continuing,
        // so clearing here would silently disable the empty-fixed-set signal for every fix leaf
        // that ever timed out or crashed once.
        // The shared `processor()` config, with room for the retry this test is about.
        let mut p = drive_into_a_fix_round(
            Processor::new(ProcessorConfig {
                max_parallel: 2,
                cohort_size: 3,
                review_loop_max: 2,
                integration_loop_max: 2,
                ci_fix_max: 2,
                stagnation_limit: 2,
                leaf_max_attempts: 2,
                ..ProcessorConfig::default()
            })
            .unwrap(),
        );

        let effects = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::RetryableFailure {
                    reason: "supervisor timeout".into(),
                },
            })
            .unwrap();

        assert!(matches!(
            effects.last(),
            Some(Effect::PrepareTaskLeaf {
                kind: LeafKind::Fix,
                ..
            })
        ));
        assert_eq!(p.state().tasks["T-1"].pending_fix_open_findings, Some(2));
        assert_eq!(
            p.state().tasks["T-1"]
                .pending_fix_open_finding_ids
                .as_deref(),
            Some(["R-05".to_string(), "R-06".to_string()].as_slice()),
            "the repeat of this same round must keep its coordinate"
        );
    }

    #[test]
    fn an_empty_open_finding_id_set_is_recorded_as_known_empty_not_as_unknown() {
        // R-08. `None` and `Some(vec![])` are NOT interchangeable downstream: `None` makes
        // `outcome_adapter::validated_wont_fix_count` skip membership validation entirely (the
        // pre-R-06 unbounded count), while a known-empty set filters every `не исправлено=` entry
        // out. So the reducer must record what the round reported and never re-read emptiness as
        // "unknown" — a round that reports an open COUNT with no ids behind it must be able to
        // say so without silently disabling the check that stops a stale id from escalating a
        // successful fix round.
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-1".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-1".into(),
            commit: "a1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-1".into(),
            outcome: ReviewOutcome::Findings {
                signature: signature("one finding whose id the round did not name"),
                open_findings: 1,
                open_finding_ids: Vec::new(),
            },
        })
        .unwrap();

        assert_eq!(p.state().tasks["T-1"].pending_fix_open_findings, Some(1));
        assert_eq!(
            p.state().tasks["T-1"].pending_fix_open_finding_ids,
            Some(Vec::new()),
            "an empty set is a KNOWN set: it must reach the adapter as `Some(&[])`, which admits \
             no won't-fix entry, not as `None`, which admits every one of them unvalidated"
        );

        // The same rule on the risk-elevated twin of that arm.
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-2", "tui/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-2".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskLeaf {
            task_id: "T-2".into(),
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskCommitted {
            task_id: "T-2".into(),
            commit: "b1".into(),
        })
        .unwrap();
        p.apply(ProcessorCommand::TaskReview {
            task_id: "T-2".into(),
            outcome: ReviewOutcome::FindingsRiskElevated {
                signature: signature("risk elevated, ids not named"),
                risk: Risk::High,
                open_findings: 1,
                open_finding_ids: Vec::new(),
            },
        })
        .unwrap();

        assert_eq!(
            p.state().tasks["T-2"].pending_fix_open_finding_ids,
            Some(Vec::new())
        );
    }

    #[test]
    fn call_max_attempts_counts_the_initial_leaf_launch() {
        let config = ProcessorConfig {
            max_parallel: 2,
            cohort_size: 1,
            leaf_max_attempts: 2,
            ..ProcessorConfig::default()
        };
        config.validate().unwrap();
        let mut p = Processor::new(config).unwrap();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.apply(ProcessorCommand::WorkspaceReady {
            task_id: "T-1".into(),
        })
        .unwrap();
        let retry = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::RetryableFailure {
                    reason: "timeout".into(),
                },
            })
            .unwrap();
        assert!(matches!(
            retry.last(),
            Some(Effect::PrepareTaskLeaf {
                kind: LeafKind::Implement,
                ..
            })
        ));
        let exhausted = p
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::RetryableFailure {
                    reason: "timeout".into(),
                },
            })
            .unwrap();
        assert!(matches!(
            exhausted.last(),
            Some(Effect::EscalateTask { .. })
        ));
        assert_eq!(p.state().tasks["T-1"].leaf_attempts["implement"], 2);
    }

    #[test]
    fn join_quarantines_one_branch_and_publishes_the_other_after_ci() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**"), candidate("T-2", "tui/**")],
            now_secs: 101,
        })
        .unwrap();
        task_to_ready(&mut p, "T-1", "a1");
        task_to_ready(&mut p, "T-2", "b1");
        p.state.batch.as_mut().unwrap().admission_closed = Some(CloseReasonWire::CohortSize);
        let effects = p
            .apply(ProcessorCommand::Advance { now_secs: 102 })
            .unwrap();
        assert!(matches!(
            effects.last(),
            Some(Effect::PrepareIntegrationWorkspace { .. })
        ));
        p.apply(ProcessorCommand::IntegrationWorkspaceReady)
            .unwrap();
        p.apply(ProcessorCommand::TaskMerged {
            task_id: "T-1".into(),
            outcome: MergeOutcome::Merged {
                integration_sha: "i1".into(),
            },
        })
        .unwrap();
        let after_quarantine = p
            .apply(ProcessorCommand::TaskMerged {
                task_id: "T-2".into(),
                outcome: MergeOutcome::Quarantined {
                    reason: "conflict".into(),
                },
            })
            .unwrap();
        assert!(matches!(
            after_quarantine.last(),
            Some(Effect::DispatchIntegration {
                kind: LeafKind::IntegrationReview
            })
        ));
        p.apply(ProcessorCommand::IntegrationReview {
            outcome: ReviewOutcome::Clean {
                review_sha: "i1".into(),
            },
        })
        .unwrap();
        p.apply(ProcessorCommand::IntegrationVerified {
            head: "i1".into(),
            outcome: VerificationOutcome::Exempt {
                reason: "fixture profile disabled".into(),
            },
        })
        .unwrap();
        p.apply(ProcessorCommand::Published {
            head: "i1".into(),
            pushed: false,
        })
        .unwrap();
        let cleaning = p
            .apply(ProcessorCommand::CiVerified {
                outcome: CiOutcome::LocalOnly,
            })
            .unwrap();
        assert!(matches!(
            cleaning.last(),
            Some(Effect::PrepareKnowledgeCuration)
        ));
        let curation = p
            .apply(ProcessorCommand::KnowledgeCurationPrepared {
                outcome: KnowledgeCurationPreparationOutcome::Required,
            })
            .unwrap();
        assert!(matches!(
            curation.last(),
            Some(Effect::DispatchIntegration {
                kind: LeafKind::KnowledgeCurator
            })
        ));
        p.apply(ProcessorCommand::KnowledgeCurated {
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        acknowledge_journal_and_skip_archive_ci(&mut p);
        p.apply(ProcessorCommand::CleanupComplete).unwrap();
        p.apply(ProcessorCommand::DependencyGraphRefreshed {
            boundary: RefreshBoundary::PostArchive,
            outcome: LeafOutcome::Completed { author: None },
        })
        .unwrap();
        p.apply(ProcessorCommand::InboxFinalizationReconciled {
            curation_required: false,
        })
        .unwrap();
        p.apply(ProcessorCommand::CleanupComplete).unwrap();
        assert_eq!(p.state().phase, Phase::Idle);
    }

    #[test]
    fn join_persists_typed_conflict_before_merger_and_finalizes_only_matching_task() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        task_to_ready(&mut p, "T-1", "a1");
        p.state.batch.as_mut().unwrap().admission_closed = Some(CloseReasonWire::CohortSize);
        p.apply(ProcessorCommand::Advance { now_secs: 102 })
            .unwrap();
        p.apply(ProcessorCommand::IntegrationWorkspaceReady)
            .unwrap();

        let merger = p
            .apply(ProcessorCommand::TaskMerged {
                task_id: "T-1".into(),
                outcome: MergeOutcome::NeedsResolution {
                    pre_merge_head: "abc123".into(),
                    merge_paths: vec!["engine/src/lib.rs".into()],
                    paths: vec!["engine/src/lib.rs".into()],
                    protected_paths: Vec::new(),
                },
            })
            .unwrap();
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::ResolvingMerge);
        assert_eq!(
            p.state.integration.pending_merge_resolution.as_ref(),
            Some(&MergeResolutionRuntime {
                task_id: "T-1".into(),
                pre_merge_head: "abc123".into(),
                merge_paths: vec!["engine/src/lib.rs".into()],
                paths: vec!["engine/src/lib.rs".into()],
                protected_paths: Vec::new(),
            })
        );
        assert_eq!(p.state.integration.leaf_attempts["merger"], 1);
        assert!(matches!(
            merger.last(),
            Some(Effect::DispatchIntegration {
                kind: LeafKind::Merger
            })
        ));
        assert!(matches!(
            p.apply(ProcessorCommand::MergeResolution {
                task_id: "T-2".into(),
                outcome: LeafOutcome::Completed { author: None },
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("does not match pending")
        ));
        let finalized = p
            .apply(ProcessorCommand::MergeResolution {
                task_id: "T-1".into(),
                outcome: LeafOutcome::Completed {
                    author: Some("merger".into()),
                },
            })
            .unwrap();
        assert!(matches!(
            finalized.last(),
            Some(Effect::FinalizeMergeResolution { task_id }) if task_id == "T-1"
        ));
        let next = p
            .apply(ProcessorCommand::MergeResolutionFinalized {
                task_id: "T-1".into(),
                outcome: MergeOutcome::Merged {
                    integration_sha: "i1".into(),
                },
            })
            .unwrap();
        assert!(p.state.integration.pending_merge_resolution.is_none());
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Merged);
        assert_eq!(p.state.integration.integration_head.as_deref(), Some("i1"));
        assert!(next.iter().any(|effect| {
            matches!(
                effect,
                Effect::DispatchIntegration {
                    kind: LeafKind::IntegrationReview
                }
            )
        }));
    }

    #[test]
    fn failed_merger_requires_typed_abort_before_conflict_quarantine() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        task_to_ready(&mut p, "T-1", "a1");
        p.state.batch.as_mut().unwrap().admission_closed = Some(CloseReasonWire::CohortSize);
        p.apply(ProcessorCommand::Advance { now_secs: 102 })
            .unwrap();
        p.apply(ProcessorCommand::IntegrationWorkspaceReady)
            .unwrap();
        p.apply(ProcessorCommand::TaskMerged {
            task_id: "T-1".into(),
            outcome: MergeOutcome::NeedsResolution {
                pre_merge_head: "abc123".into(),
                merge_paths: vec!["engine/src/lib.rs".into()],
                paths: vec!["engine/src/lib.rs".into()],
                protected_paths: Vec::new(),
            },
        })
        .unwrap();

        let abort = p
            .apply(ProcessorCommand::MergeResolution {
                task_id: "T-1".into(),
                outcome: LeafOutcome::Escalated {
                    reason: "semantic ambiguity".into(),
                },
            })
            .unwrap();
        assert!(matches!(
            abort.last(),
            Some(Effect::AbortMergeResolution { task_id, reason })
                if task_id == "T-1" && reason == "merger could not resolve conflict: semantic ambiguity"
        ));
        let effects = p
            .apply(ProcessorCommand::MergeResolutionAborted {
                task_id: "T-1".into(),
                reason: "merger could not resolve conflict: semantic ambiguity".into(),
            })
            .unwrap();
        assert!(p.state.integration.pending_merge_resolution.is_none());
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Conflict);
        assert!(effects.iter().any(|effect| {
            matches!(effect, Effect::ReturnTask { task_id, .. } if task_id == "T-1")
        }));
        assert!(matches!(
            effects.last(),
            Some(Effect::WriteJournalAndStatus)
        ));
    }

    #[test]
    fn merge_conflict_paths_reject_windows_prefixes_and_vcs_metadata_before_checkpointing() {
        assert!(validate_merge_conflict_paths(&["engine/src/lib.rs".into()]).is_ok());
        for invalid in ["C:escape.rs", ".git/config", ".jj/repo", "../outside.rs"] {
            assert!(
                validate_merge_conflict_paths(&[invalid.into()]).is_err(),
                "{invalid:?} must not be accepted as a merge-conflict path"
            );
        }
    }

    #[test]
    fn failed_merger_blocks_without_returning_or_reclassifying_the_task() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Joining;
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Ready;
        task.review_sha = Some("task-tip".into());
        p.state.tasks.insert(task.id.clone(), task);

        let effects = p
            .apply(ProcessorCommand::TaskMerged {
                task_id: "T-1".into(),
                outcome: MergeOutcome::Failed {
                    reason: "merger supervisor crashed".into(),
                },
            })
            .unwrap();

        assert_eq!(p.state().phase, Phase::Blocked);
        assert_eq!(p.state().tasks["T-1"].phase, TaskPhase::Ready);
        assert!(p.state().tasks["T-1"].reason.is_none());
        assert!(matches!(
            effects.get(1),
            Some(Effect::WriteJournalAndStatus)
        ));
        assert!(
            matches!(effects.last(), Some(Effect::WaitForOperator { reason }) if reason.contains("merger did not produce a reliable result for T-1"))
        );
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::ReturnTask { .. }))
        );
    }

    #[test]
    fn recovery_rebuilds_the_full_cleanup_ledger_before_accepting_cleanup_complete() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Published;
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.published_head = Some("published-head".into());
        p.state.integration.publication_pushed = Some(false);
        p.state.integration.ci_disposition = Some(CiDisposition::Disabled);
        p.state.phase = Phase::Cleaning;

        let checkpoint = p.state().clone();
        let mut resumed = Processor::from_checkpoint(p.config.clone(), checkpoint).unwrap();
        let effects = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();

        assert_eq!(resumed.state().phase, Phase::Cleaning);
        assert!(matches!(effects.first(), Some(Effect::PersistCheckpoint)));
        assert!(matches!(
            effects.get(1),
            Some(Effect::WriteJournalAndStatus)
        ));
        assert_eq!(effects.last(), Some(&Effect::PrepareArchival));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::ArchiveTask { .. }))
        );
    }

    #[test]
    fn recovered_cleanup_never_duplicates_an_acknowledged_journal_entry() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Published;
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.published_head = Some("published-head".into());
        p.state.integration.publication_pushed = Some(false);
        p.state.integration.ci_disposition = Some(CiDisposition::Disabled);
        p.state.phase = Phase::Cleaning;

        let checkpoint = p.state().clone();
        let mut first_resume = Processor::from_checkpoint(p.config.clone(), checkpoint).unwrap();
        let initial_effects = first_resume
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        let journal = initial_effects
            .iter()
            .find(|effect| matches!(effect, Effect::WriteJournalAndStatus))
            .cloned()
            .expect("cleanup must initially journal before deletion");
        first_resume
            .acknowledge_non_command_effect(&journal)
            .unwrap();
        assert!(first_resume.state().integration.cleanup_journaled);

        let mut second_resume =
            Processor::from_checkpoint(p.config.clone(), first_resume.state().clone()).unwrap();
        let replay_effects = second_resume
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        assert!(
            !replay_effects
                .iter()
                .any(|effect| matches!(effect, Effect::WriteJournalAndStatus))
        );
        assert_eq!(replay_effects.last(), Some(&Effect::PrepareArchival));
        let physical = second_resume
            .apply(ProcessorCommand::ArchivalPrepared {
                outcome: ArchivalPreparationOutcome::Skipped,
            })
            .unwrap();
        assert!(physical.iter().any(|effect| {
            matches!(effect, Effect::ArchiveTask { task_id } if task_id == "T-1")
        }));
    }

    #[test]
    fn failed_integration_verification_blocks_after_review_and_before_publication() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.integration_head = Some("integration-tip".into());
        let effects = p
            .apply(ProcessorCommand::IntegrationVerified {
                head: "integration-tip".into(),
                outcome: VerificationOutcome::Failed {
                    signature: signature("build failed"),
                    reason: "command #1 exited 1".into(),
                },
            })
            .unwrap();
        assert_eq!(p.state().phase, Phase::Blocked);
        assert!(matches!(
            effects.last(),
            Some(Effect::WaitForOperator { .. })
        ));
        assert!(!effects.iter().any(|effect| {
            matches!(
                effect,
                Effect::DispatchIntegration {
                    kind: LeafKind::IntegrationReview
                } | Effect::Publish { .. }
            )
        }));
    }

    #[test]
    fn pause_refuses_a_leaf_result_without_losing_the_exact_resume_phase() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Pause).unwrap();
        let effects = p
            .apply(ProcessorCommand::Admit {
                candidates: vec![candidate("T-1", "engine/**")],
                now_secs: 101,
            })
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::WaitForOperator { .. }]
        ));
        p.apply(ProcessorCommand::Resume).unwrap();
        assert_eq!(p.state().phase, Phase::Rolling);
    }

    #[test]
    fn checkpoint_round_trips_without_losing_a_review_coordinate() {
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        p.state.tasks.get_mut("T-1").unwrap().review_cycles = 2;
        let text = serde_json::to_string(p.state()).unwrap();
        let loaded: ProcessorState = serde_json::from_str(&text).unwrap();
        assert_eq!(loaded.tasks["T-1"].review_cycles, 2);
        assert_eq!(loaded.schema_version, PROCESSOR_STATE_VERSION);
    }

    #[test]
    fn checkpoint_written_before_per_dimension_findings_still_loads() {
        // The durable per-dimension "reported findings last round" set round-trips while present …
        let mut current = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        current.dimensions_with_findings_last_round = vec!["security".into(), "performance".into()];
        let json = serde_json::to_value(&current).unwrap();
        let round_tripped: TaskRuntime = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            round_tripped.dimensions_with_findings_last_round,
            vec!["security".to_string(), "performance".to_string()]
        );

        // … and a checkpoint written before the field (every pre-T-018 checkpoint) loads with an
        // empty, fail-open default rather than panicking or fabricating a dimension.
        let mut legacy = json;
        legacy
            .as_object_mut()
            .unwrap()
            .remove("dimensions_with_findings_last_round")
            .expect("the field is serialized for a current task");
        let task: TaskRuntime = serde_json::from_value(legacy).unwrap();
        assert!(task.dimensions_with_findings_last_round.is_empty());
    }

    #[test]
    fn checkpoint_written_before_durable_sessions_still_loads() {
        // Exactly the shape an older engine persisted: every field it knew, and no
        // `leaf_sessions`. It must deserialize into "no known conversation", which is the
        // full-context re-seed the engine did before durable sessions existed.
        let mut current = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        current.review_cycles = 2;
        current.review_sha = Some("head-1".into());
        current.leaf_sessions.insert(
            LeafSessionKey::new(
                crate::session::SessionProvider::Claude,
                crate::session::SessionLineage::Coder,
            )
            .as_durable_key(),
            "11111111-2222-3333-4444-555555555555".into(),
        );
        let mut legacy = serde_json::to_value(&current).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("leaf_sessions")
            .expect("the field is serialized for a current task");
        let task: TaskRuntime = serde_json::from_value(legacy).unwrap();
        assert!(task.leaf_sessions.is_empty());
        assert_eq!(task.review_sha.as_deref(), Some("head-1"));
        assert_eq!(task.review_cycles, 2);
        assert_eq!(
            task.leaf_session(LeafSessionKey::new(
                crate::session::SessionProvider::Claude,
                crate::session::SessionLineage::Coder
            )),
            None
        );

        // A whole checkpoint without the field is equally readable, and re-serializing it keeps
        // the new map additive rather than migrating anything.
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let mut document = serde_json::to_value(p.state()).unwrap();
        document["tasks"]["T-1"]
            .as_object_mut()
            .unwrap()
            .remove("leaf_sessions")
            .expect("the field is serialized for a current checkpoint");
        let loaded: ProcessorState = serde_json::from_value(document).unwrap();
        assert!(loaded.tasks["T-1"].leaf_sessions.is_empty());
        assert_eq!(loaded.schema_version, PROCESSOR_STATE_VERSION);
        Processor::from_checkpoint(p.config.clone(), loaded).expect("legacy checkpoint resumes");
    }

    #[test]
    fn a_recorded_session_is_orthogonal_to_every_reducer_decision() {
        use crate::session::{LeafSessionKey, LeafSessionUpdate, SessionLineage, SessionProvider};

        let coder = LeafSessionKey::new(SessionProvider::Claude, SessionLineage::Coder);
        let reviewer = LeafSessionKey::new(SessionProvider::Codex, SessionLineage::Reviewer);

        // Drive one identical implement/review round twice: once with sessions recorded at every
        // step, once without. The phases and the emitted effects must be indistinguishable.
        let run = |with_sessions: bool| {
            let mut p = processor();
            open(&mut p);
            p.apply(ProcessorCommand::Admit {
                candidates: vec![candidate("T-1", "engine/**")],
                now_secs: 101,
            })
            .unwrap();
            p.apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            })
            .unwrap();
            if with_sessions {
                p.record_leaf_session(
                    "T-1",
                    &LeafSessionUpdate::Observed {
                        key: coder,
                        id: "11111111-2222-3333-4444-555555555555".into(),
                    },
                )
                .unwrap();
            }
            let mut effects = p
                .apply(ProcessorCommand::TaskLeaf {
                    task_id: "T-1".into(),
                    outcome: LeafOutcome::Completed {
                        author: Some("coder".into()),
                    },
                })
                .unwrap();
            if with_sessions {
                p.record_leaf_session(
                    "T-1",
                    &LeafSessionUpdate::Observed {
                        key: reviewer,
                        id: "019f054f-5e70-7d42-8586-ee66e3ac1d1e".into(),
                    },
                )
                .unwrap();
                p.record_leaf_session("T-1", &LeafSessionUpdate::Invalidated { key: coder })
                    .unwrap();
            }
            effects.extend(
                p.apply(ProcessorCommand::TaskCommitted {
                    task_id: "T-1".into(),
                    commit: "head-1".into(),
                })
                .unwrap(),
            );
            (p.state().clone(), effects)
        };

        let (with_state, with_effects) = run(true);
        let (without_state, without_effects) = run(false);
        assert_eq!(with_effects, without_effects);
        assert_eq!(
            with_state.tasks["T-1"].phase,
            without_state.tasks["T-1"].phase
        );
        // The ONLY difference between the two runs is the orthogonal map.
        let mut normalized = with_state.clone();
        normalized.tasks.get_mut("T-1").unwrap().leaf_sessions = BTreeMap::new();
        assert_eq!(normalized, without_state);
        // An invalidated coordinate is forgotten; a live one is retained verbatim.
        assert_eq!(with_state.tasks["T-1"].leaf_session(coder), None);
        assert_eq!(
            with_state.tasks["T-1"].leaf_session(reviewer),
            Some("019f054f-5e70-7d42-8586-ee66e3ac1d1e")
        );
    }

    #[test]
    fn a_lineage_keeps_only_the_conversation_of_the_provider_that_last_ran_it() {
        use crate::session::{LeafSessionKey, LeafSessionUpdate, SessionLineage, SessionProvider};

        let claude_coder = LeafSessionKey::new(SessionProvider::Claude, SessionLineage::Coder);
        let codex_coder = LeafSessionKey::new(SessionProvider::Codex, SessionLineage::Coder);
        let claude_reviewer =
            LeafSessionKey::new(SessionProvider::Claude, SessionLineage::Reviewer);
        let codex_id = "019f054f-5e70-7d42-8586-ee66e3ac1d1e";
        let claude_id = "11111111-2222-3333-4444-555555555555";
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        let observe = |p: &mut Processor, key, id: &str| {
            p.record_leaf_session("T-1", &LeafSessionUpdate::Observed { key, id: id.into() })
                .unwrap();
        };

        // Round 1 is routed to Codex, round 2 to Claude. Both are ordinary `route_coder`
        // outcomes for one task, and only the second one saw the tree it left behind.
        observe(&mut p, codex_coder, codex_id);
        observe(&mut p, claude_reviewer, claude_id);
        observe(&mut p, claude_coder, claude_id);
        assert_eq!(
            p.state().tasks["T-1"].leaf_session(codex_coder),
            None,
            "round 3 must not resume the Codex conversation that predates Claude's round"
        );
        assert_eq!(
            p.state().tasks["T-1"].leaf_session(claude_coder),
            Some(claude_id)
        );
        // Only the peer of the SAME lineage is dropped: an independent reviewer's conversation is
        // untouched by whoever authored the code.
        assert_eq!(
            p.state().tasks["T-1"].leaf_session(claude_reviewer),
            Some(claude_id)
        );

        // Routing back to Codex re-seeds it, and now Claude's coder conversation is the stale one.
        observe(&mut p, codex_coder, codex_id);
        assert_eq!(p.state().tasks["T-1"].leaf_session(claude_coder), None);
        assert_eq!(
            p.state().tasks["T-1"].leaf_session(codex_coder),
            Some(codex_id)
        );

        // A call that ran and failed may still have edited the tree, so it leaves the lineage with
        // no resumable conversation at all rather than handing one back to its peer.
        observe(&mut p, claude_coder, claude_id);
        p.record_leaf_session("T-1", &LeafSessionUpdate::Invalidated { key: codex_coder })
            .unwrap();
        assert_eq!(p.state().tasks["T-1"].leaf_session(claude_coder), None);
        assert_eq!(p.state().tasks["T-1"].leaf_session(codex_coder), None);
        assert_eq!(
            p.state().tasks["T-1"].leaf_session(claude_reviewer),
            Some(claude_id)
        );

        // A rejected id changes nothing at all, peer included.
        observe(&mut p, codex_coder, codex_id);
        assert!(matches!(
            p.record_leaf_session(
                "T-1",
                &LeafSessionUpdate::Observed {
                    key: claude_coder,
                    id: "../../escape".into()
                }
            ),
            Err(ProcessorError::InvalidCommand(_))
        ));
        assert_eq!(
            p.state().tasks["T-1"].leaf_session(codex_coder),
            Some(codex_id)
        );
    }

    #[test]
    fn a_malformed_or_unknown_session_coordinate_is_refused() {
        use crate::session::{LeafSessionKey, LeafSessionUpdate, SessionLineage, SessionProvider};

        let key = LeafSessionKey::new(SessionProvider::Claude, SessionLineage::Coder);
        let mut p = processor();
        open(&mut p);
        p.apply(ProcessorCommand::Admit {
            candidates: vec![candidate("T-1", "engine/**")],
            now_secs: 101,
        })
        .unwrap();
        assert!(matches!(
            p.record_leaf_session(
                "T-2",
                &LeafSessionUpdate::Observed {
                    key,
                    id: "abc".into()
                }
            ),
            Err(ProcessorError::MissingTask(_))
        ));
        // The id becomes a path component in the probe and an argv element in the call, so a
        // traversal attempt must never reach the checkpoint.
        assert!(matches!(
            p.record_leaf_session(
                "T-1",
                &LeafSessionUpdate::Observed {
                    key,
                    id: "../../escape".into()
                }
            ),
            Err(ProcessorError::InvalidCommand(_))
        ));
        assert!(p.state().tasks["T-1"].leaf_sessions.is_empty());
        // Forgetting an unrecorded coordinate is a no-op, not an error: the fix path may run
        // before any session was ever observed.
        p.record_leaf_session("T-1", &LeafSessionUpdate::Invalidated { key })
            .unwrap();
        assert!(p.state().tasks["T-1"].leaf_sessions.is_empty());
    }

    #[test]
    fn checkpoint_rejects_invalid_or_cross_wired_durable_coordinates() {
        let p = processor();
        let mut invalid_batch = p.state().clone();
        invalid_batch.batch = Some(CohortRuntime {
            id: "../batch".into(),
            base: "main".into(),
            started_at_secs: 1,
            wave: 1,
            admitted_total: 0,
            admission_closed: None,
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            token_budget_actual_tokens: None,
            events_outbox_enabled: true,
        });
        assert!(matches!(
            Processor::from_checkpoint(p.config.clone(), invalid_batch),
            Err(ProcessorError::CorruptCheckpoint(message)) if message.contains("invalid active batch id")
        ));

        let mut mismatched_task = p.state().clone();
        mismatched_task.tasks.insert(
            "T-1".into(),
            TaskRuntime {
                id: "T-2".into(),
                conflict_domain: "engine/**".into(),
                level: Some(Level::Coder),
                risk: Some(crate::resolvers::Risk::Medium),
                wave: 1,
                phase: TaskPhase::Capturing,
                leaf_attempts: BTreeMap::new(),
                review_cycles: 0,
                review_signatures: Vec::new(),
                pending_fix_open_findings: None,
                pending_fix_open_finding_ids: None,
                dimensions_with_findings_last_round: Vec::new(),
                implementation_author: None,
                previous_review_sha: None,
                review_sha: None,
                reason: None,
                imported_recovery_intent: None,
                leaf_sessions: BTreeMap::new(),
            },
        );
        assert!(matches!(
            Processor::from_checkpoint(p.config.clone(), mismatched_task),
            Err(ProcessorError::CorruptCheckpoint(message)) if message.contains("does not match embedded id")
        ));
    }

    #[test]
    fn integration_fix_must_commit_before_a_new_full_review() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        let effects = p
            .apply(ProcessorCommand::IntegrationReview {
                outcome: ReviewOutcome::Findings {
                    signature: signature("F-01 missing integration guard"),
                    open_findings: 1,
                    open_finding_ids: vec!["F-01".into()],
                },
            })
            .unwrap();
        assert!(matches!(
            effects.last(),
            Some(Effect::DispatchIntegration {
                kind: LeafKind::IntegrationFix
            })
        ));
        assert_eq!(
            p.apply(ProcessorCommand::IntegrationFix {
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap(),
            vec![Effect::PersistCheckpoint, Effect::CommitIntegrationFix]
        );
        assert_eq!(
            p.apply(ProcessorCommand::IntegrationFixCommitted {
                head: "integration-fix-1".into(),
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchIntegration {
                    kind: LeafKind::IntegrationReview,
                },
            ]
        );
        assert_eq!(
            p.state.integration.integration_head.as_deref(),
            Some("integration-fix-1")
        );
        assert_eq!(
            p.apply(ProcessorCommand::IntegrationReview {
                outcome: ReviewOutcome::Clean {
                    review_sha: "integration-fix-1".into(),
                },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::VerifyIntegration {
                    head: "integration-fix-1".into(),
                },
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::IntegrationVerified {
                head: "integration-fix-1".into(),
                outcome: VerificationOutcome::Passed,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::Publish {
                    batch_id: "B-20260724T120000Z".into(),
                },
            ]
        );
    }

    #[test]
    fn publication_refuses_a_reviewed_but_unverified_or_stale_integration_tip() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.merged_tasks.insert("T-1".into());
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Merged;
        p.state.tasks.insert("T-1".into(), task);
        p.state.integration.integration_head = Some("integration-current".into());
        p.state.integration.verification_head = Some("integration-old".into());

        assert!(matches!(
            p.apply(ProcessorCommand::Published {
                head: "integration-current".into(),
                pushed: false,
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("matching final verification")
        ));
    }

    #[test]
    fn pending_publication_approval_retries_only_after_a_fresh_phase_zero_recovery() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.merged_tasks.insert("T-1".into());
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Merged;
        p.state.tasks.insert("T-1".into(), task);
        p.state.integration.integration_head = Some("integration-current".into());
        p.state.integration.verification_head = Some("integration-current".into());

        assert!(matches!(
            p.apply(ProcessorCommand::PublicationAwaitingApproval {
                reason: "policy approval apr-00000000000000000000000000000000 is pending".into(),
            })
            .unwrap()
            .as_slice(),
            [Effect::WaitForOperator { .. }]
        ));
        assert_eq!(p.state.phase, Phase::Publishing);
        assert!(p.state.integration.published_head.is_none());

        let mut resumed = Processor::from_checkpoint(p.config.clone(), p.state.clone()).unwrap();
        assert_eq!(
            resumed.state.integration.publication_reanchor_cycles, 0,
            "a pending policy approval must not consume the separate remote-divergence budget"
        );
        assert_eq!(
            resumed
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::Publish {
                    batch_id: "B-20260724T120000Z".into(),
                },
            ],
            "only a new Phase-0 invocation may re-check the external operator decision"
        );
    }

    #[test]
    fn remote_push_divergence_reanchors_then_replays_the_full_integration_boundary() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.workspace_prepared = true;
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.integration_head = Some("integration-current".into());
        p.state.integration.review_sha = Some("integration-current".into());
        p.state.integration.verification_head = Some("integration-current".into());
        p.state.integration.f_cycles = 2;
        p.state
            .integration
            .leaf_attempts
            .insert(LeafKind::IntegrationReview.as_str().into(), 3);
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Merged;
        task.review_sha = Some("task-reviewed-tip".into());
        p.state.tasks.insert("T-1".into(), task);

        assert_eq!(
            p.apply(ProcessorCommand::PublicationReanchorRequired {
                reason: "origin/main advanced during typed push".into(),
                target: PublicationReanchorTarget::RemotePublication,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::ReanchorPublication {
                    batch_id: "B-20260724T120000Z".into(),
                },
            ]
        );
        assert_eq!(
            p.state.integration.publication_reanchor_reason.as_deref(),
            Some("origin/main advanced during typed push")
        );
        assert_eq!(
            p.state.integration.publication_reanchor_target,
            Some(PublicationReanchorTarget::RemotePublication)
        );
        assert_eq!(p.state.integration.publish_attempts, 1);

        let mut resumed = Processor::from_checkpoint(p.config.clone(), p.state.clone()).unwrap();
        assert_eq!(
            resumed.state.integration.publication_reanchor_cycles, 1,
            "the remote-divergence convergence budget must survive a Phase-0 restart"
        );
        assert_eq!(
            resumed
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::ReanchorPublication {
                    batch_id: "B-20260724T120000Z".into(),
                },
            ],
            "Phase 0 must retain the explicit re-anchor instead of retrying the rejected push"
        );

        assert_eq!(
            resumed
                .apply(ProcessorCommand::PublicationReanchored)
                .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::PrepareIntegrationWorkspace {
                    branch: "integration/B-20260724T120000Z".into(),
                },
            ]
        );
        assert_eq!(resumed.state.phase, Phase::Joining);
        assert_eq!(resumed.state.tasks["T-1"].phase, TaskPhase::Ready);
        assert_eq!(
            resumed.state.tasks["T-1"].review_sha.as_deref(),
            Some("task-reviewed-tip"),
            "the reviewed task candidate survives; only the integration must be replayed"
        );
        assert!(!resumed.state.integration.workspace_prepared);
        assert!(resumed.state.integration.merged_tasks.is_empty());
        assert!(resumed.state.integration.integration_head.is_none());
        assert!(resumed.state.integration.verification_head.is_none());
        assert_eq!(
            resumed
                .state
                .integration
                .leaf_attempts
                .get(LeafKind::IntegrationReview.as_str()),
            Some(&3),
            "provider call coordinates remain monotonic across a re-anchor"
        );
    }

    #[test]
    fn repeated_primary_divergence_stops_before_an_unbounded_reanchor() {
        let mut p = processor();
        p.config.integration_loop_max = 1;
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.workspace_prepared = true;
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.integration_head = Some("integration-tip".into());
        p.state.integration.verification_head = Some("integration-tip".into());
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Merged;
        p.state.tasks.insert(task.id.clone(), task);

        assert!(matches!(
            p.apply(ProcessorCommand::PublicationReanchorRequired {
                reason: "main advanced locally once".into(),
                target: PublicationReanchorTarget::LocalPrimary,
            })
            .unwrap()
            .as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::ReanchorPublication { .. }
            ]
        ));
        assert_eq!(p.state.integration.publication_reanchor_cycles, 1);

        // Model the completed first re-anchor and a new clean integration that loses another
        // push race. The second rejection must hold the still-inspectable integration surface
        // instead of looping through another destructive reset.
        p.apply(ProcessorCommand::PublicationReanchored).unwrap();
        p.state.phase = Phase::Publishing;
        p.state.integration.workspace_prepared = true;
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.integration_head = Some("integration-tip-2".into());
        p.state.integration.verification_head = Some("integration-tip-2".into());
        p.state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Merged;

        let effects = p
            .apply(ProcessorCommand::PublicationReanchorRequired {
                reason: "main advanced locally again".into(),
                target: PublicationReanchorTarget::LocalPrimary,
            })
            .unwrap();
        assert!(matches!(effects.first(), Some(Effect::PersistCheckpoint)));
        assert!(matches!(
            effects.get(1),
            Some(Effect::WriteJournalAndStatus)
        ));
        assert!(matches!(
            effects.get(2),
            Some(Effect::WaitForOperator { .. })
        ));
        assert_eq!(p.state.phase, Phase::Blocked);
        assert_eq!(p.state.integration.publication_reanchor_cycles, 1);
        assert_eq!(p.state.integration.publish_attempts, 2);
        assert!(p.state.integration.workspace_prepared);
        assert!(p.state.integration.merged_tasks.contains("T-1"));
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Merged);
    }

    #[test]
    fn rejected_publication_escalates_the_unpublished_merged_batch_then_cleans_it() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.merged_tasks.insert("T-1".into());
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Merged;
        p.state.tasks.insert("T-1".into(), task);
        p.state.integration.integration_head = Some("integration-current".into());
        p.state.integration.verification_head = Some("integration-current".into());

        let effects = p
            .apply(ProcessorCommand::PublicationRejected {
                reason: "policy push approval apr-00000000000000000000000000000000 was rejected"
                    .into(),
            })
            .unwrap();
        assert!(matches!(effects.first(), Some(Effect::PersistCheckpoint)));
        assert!(matches!(
            effects.get(1),
            Some(Effect::WriteJournalAndStatus)
        ));
        assert!(matches!(
            effects.get(2),
            Some(Effect::EscalateTask { task_id, .. }) if task_id == "T-1"
        ));
        assert_eq!(p.state.phase, Phase::Cleaning);
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Escalated);
        assert!(p.state.integration.merged_tasks.is_empty());
        assert!(p.state.integration.published_head.is_none());
    }

    #[test]
    fn clean_reviews_must_name_the_current_durable_tip() {
        let mut p = processor();
        open(&mut p);
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Reviewing;
        task.review_sha = Some("task-tip".into());
        p.state.tasks.insert(task.id.clone(), task);
        assert!(matches!(
            p.apply(ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Clean {
                    review_sha: "other-task-tip".into(),
                },
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("not committed tip")
        ));

        p.state.phase = Phase::Publishing;
        p.state.integration.integration_head = Some("integration-tip".into());
        assert!(matches!(
            p.apply(ProcessorCommand::IntegrationReview {
                outcome: ReviewOutcome::Clean {
                    review_sha: "other-integration-tip".into(),
                },
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("not current tip")
        ));
    }

    #[test]
    fn ci_fix_must_publish_a_new_head_before_ci_is_rechecked() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.published_head = Some("main-before-fix".into());
        assert_eq!(
            p.apply(ProcessorCommand::CiFix {
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap(),
            vec![Effect::PersistCheckpoint, Effect::CommitCiFix]
        );
        assert_eq!(
            p.apply(ProcessorCommand::CiFixCommitted {
                head: "main-after-fix".into(),
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::VerifyCi {
                    head: "main-after-fix".into(),
                },
            ]
        );
        assert_eq!(
            p.state.integration.published_head.as_deref(),
            Some("main-after-fix")
        );
        assert!(p.state.integration.ci_disposition.is_none());
    }

    #[test]
    fn optional_ci_degradation_continues_but_required_timeout_never_dispatches_a_fixer() {
        let mut optional = processor();
        open(&mut optional);
        optional.state.phase = Phase::Publishing;
        optional.state.integration.published_head = Some("published-head".into());
        optional.state.integration.publication_pushed = Some(true);
        let effects = optional
            .apply(ProcessorCommand::CiVerified {
                outcome: CiOutcome::BestEffortDegraded {
                    reason: "no optional workflow run became observable".into(),
                },
            })
            .unwrap();
        assert_eq!(optional.state.phase, Phase::Cleaning);
        assert_eq!(
            optional.state.integration.ci_disposition,
            Some(CiDisposition::UnconfirmedDegraded)
        );
        assert!(optional.state.integration.degradations[0].contains("not confirmed"));
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistCheckpoint, Effect::PrepareKnowledgeCuration]
        ));

        let mut required = processor();
        open(&mut required);
        required.state.phase = Phase::Publishing;
        required.state.integration.published_head = Some("published-head".into());
        required.state.integration.publication_pushed = Some(true);
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Published;
        required.state.tasks.insert(task.id.clone(), task);
        let effects = required
            .apply(ProcessorCommand::CiVerified {
                outcome: CiOutcome::RequiredUnconfirmed {
                    reason: "required checks did not report before the deadline".into(),
                },
            })
            .unwrap();
        assert_eq!(required.state.phase, Phase::Publishing);
        assert_eq!(required.state.tasks["T-1"].phase, TaskPhase::Published);
        assert_eq!(
            required.state.integration.ci_disposition,
            Some(CiDisposition::UnconfirmedDegraded)
        );
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::WriteJournalAndStatus,
                Effect::WaitForOperator { .. }
            ]
        ));
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::PrepareCiFix
                | Effect::DispatchIntegration {
                    kind: LeafKind::CiFix
                }
        )));
        let checkpoint = required.state().clone();
        let mut resumed = Processor::from_checkpoint(required.config.clone(), checkpoint).unwrap();
        let retry = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        assert!(matches!(
            retry.as_slice(),
            [Effect::PersistCheckpoint, Effect::VerifyCi { head }] if head == "published-head"
        ));
    }

    #[test]
    fn skipped_knowledge_preflight_goes_directly_to_cleanup_without_a_model_dispatch() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Cleaning;
        p.state.integration.published_head = Some("published-head".into());
        let effects = p
            .apply(ProcessorCommand::KnowledgeCurationPrepared {
                outcome: KnowledgeCurationPreparationOutcome::Skipped,
            })
            .unwrap();

        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::WriteJournalAndStatus))
        );
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::DispatchIntegration {
                kind: LeafKind::KnowledgeCurator
            }
        )));
        assert_eq!(effects.last(), Some(&Effect::PrepareArchival));
    }

    fn published_cleaner_for_archive_gate() -> Processor {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Cleaning;
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Published;
        p.state.tasks.insert(task.id.clone(), task);
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.published_head = Some("published-head".into());
        p.state.integration.publication_pushed = Some(true);
        p.state.integration.ci_disposition = Some(CiDisposition::Confirmed);
        p
    }

    #[test]
    fn required_archive_ci_is_journaled_then_reconfirmed_before_any_deletion() {
        let mut p = published_cleaner_for_archive_gate();
        let before = p.state.clone();
        assert!(matches!(
            p.apply(ProcessorCommand::ArchivalPrepared {
                outcome: ArchivalPreparationOutcome::ReconfirmRequired {
                    required_checks: vec!["validate".into()],
                },
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("journal")
        ));
        assert_eq!(p.state, before);

        p.acknowledge_non_command_effect(&Effect::WriteJournalAndStatus)
            .unwrap();
        let prepared = p
            .apply(ProcessorCommand::ArchivalPrepared {
                outcome: ArchivalPreparationOutcome::ReconfirmRequired {
                    required_checks: vec!["validate".into()],
                },
            })
            .unwrap();
        assert!(matches!(
            prepared.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::ReconfirmCiBeforeArchive { head, required_checks }
            ] if head == "published-head"
                && required_checks == &["validate".to_string()]
        ));
        assert!(
            !prepared
                .iter()
                .any(|effect| matches!(effect, Effect::ArchiveTask { .. }))
        );

        let before_bad_checks = p.state.clone();
        assert!(
            p.apply(ProcessorCommand::ArchivalPrepared {
                outcome: ArchivalPreparationOutcome::ReconfirmRequired {
                    required_checks: vec!["validate".into(), "validate".into()],
                },
            })
            .is_err()
        );
        assert_eq!(p.state, before_bad_checks);

        let before_stale = p.state.clone();
        assert!(
            p.apply(ProcessorCommand::ArchiveCiReconfirmed {
                head: "stale-head".into(),
                outcome: CiOutcome::Passed,
            })
            .is_err()
        );
        assert_eq!(p.state, before_stale);

        let physical = p
            .apply(ProcessorCommand::ArchiveCiReconfirmed {
                head: "published-head".into(),
                outcome: CiOutcome::Passed,
            })
            .unwrap();
        assert_eq!(
            p.state.integration.archive_ci_gate,
            Some(ArchiveCiGate::Confirmed)
        );
        assert!(
            physical.iter().any(
                |effect| matches!(effect, Effect::ArchiveTask { task_id } if task_id == "T-1")
            )
        );
    }

    #[test]
    fn failed_archive_reconfirmation_returns_to_phase_five_without_archiving() {
        let mut p = published_cleaner_for_archive_gate();
        p.state.integration.cleanup_journaled = true;
        p.apply(ProcessorCommand::ArchivalPrepared {
            outcome: ArchivalPreparationOutcome::ReconfirmRequired {
                required_checks: vec!["validate".into()],
            },
        })
        .unwrap();

        let effects = p
            .apply(ProcessorCommand::ArchiveCiReconfirmed {
                head: "published-head".into(),
                outcome: CiOutcome::Failed {
                    signature: signature("required archive check failed"),
                    reason: "required archive check failed".into(),
                },
            })
            .unwrap();
        assert_eq!(p.state.phase, Phase::Publishing);
        assert!(p.state.integration.archive_ci_gate.is_none());
        assert!(p.state.integration.ci_disposition.is_none());
        assert!(!p.state.integration.cleanup_journaled);
        assert_eq!(p.state.tasks["T-1"].phase, TaskPhase::Published);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::PrepareCiFix))
        );
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::ArchiveTask { .. }))
        );
    }

    #[test]
    fn local_archive_preflight_records_skip_before_physical_cleanup() {
        let mut p = published_cleaner_for_archive_gate();
        p.state.integration.publication_pushed = Some(false);
        p.state.integration.ci_disposition = Some(CiDisposition::Disabled);
        p.state.integration.cleanup_journaled = true;
        let effects = p
            .apply(ProcessorCommand::ArchivalPrepared {
                outcome: ArchivalPreparationOutcome::Skipped,
            })
            .unwrap();
        assert_eq!(
            p.state.integration.archive_ci_gate,
            Some(ArchiveCiGate::Skipped)
        );
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::ArchiveTask { .. }))
        );
    }

    #[test]
    fn archive_preflight_and_restored_gate_reject_incoherent_published_tasks_atomically() {
        let mut p = published_cleaner_for_archive_gate();
        p.state.integration.cleanup_journaled = true;
        p.state.integration.merged_tasks.clear();
        let before = p.state.clone();
        assert!(matches!(
            p.apply(ProcessorCommand::ArchivalPrepared {
                outcome: ArchivalPreparationOutcome::Skipped,
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("absent from the archive cohort")
        ));
        assert_eq!(p.state, before);

        p.state.integration.archive_ci_gate = Some(ArchiveCiGate::Skipped);
        assert!(matches!(
            Processor::from_checkpoint(p.config.clone(), p.state.clone()),
            Err(ProcessorError::CorruptCheckpoint(message)) if message.contains("omits published task")
        ));
    }

    #[test]
    fn disabled_ci_is_a_distinct_durable_success_disposition() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.published_head = Some("published-head".into());
        p.state.integration.publication_pushed = Some(false);
        assert!(matches!(
            p.apply(ProcessorCommand::CiVerified {
                outcome: CiOutcome::Passed,
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("contradicts")
        ));
        p.apply(ProcessorCommand::CiVerified {
            outcome: CiOutcome::LocalOnly,
        })
        .unwrap();
        assert_eq!(p.state.phase, Phase::Cleaning);
        assert_eq!(
            p.state.integration.ci_disposition,
            Some(CiDisposition::Disabled)
        );

        let mut legacy = processor();
        open(&mut legacy);
        legacy.state.phase = Phase::Publishing;
        legacy.state.integration.published_head = Some("legacy-published-head".into());
        let before_invalid = legacy.state().clone();
        assert!(
            legacy
                .apply(ProcessorCommand::CiVerified {
                    outcome: CiOutcome::BestEffortDegraded { reason: " ".into() },
                })
                .is_err()
        );
        assert_eq!(legacy.state(), &before_invalid);
        legacy
            .apply(ProcessorCommand::CiVerified {
                outcome: CiOutcome::Disabled,
            })
            .unwrap();
        assert_eq!(legacy.state.integration.publication_pushed, Some(true));
    }

    #[test]
    fn optional_codex_ci_fix_falls_back_to_a_separate_claude_effect() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.published_head = Some("main-before-fix".into());
        p.state.integration.publication_pushed = Some(true);
        let start = p
            .apply(ProcessorCommand::CiVerified {
                outcome: CiOutcome::Failed {
                    signature: signature("required check failed"),
                    reason: "required check failed".into(),
                },
            })
            .unwrap();
        assert_eq!(
            start,
            vec![
                Effect::PersistCheckpoint,
                Effect::Notify {
                    event: NotificationEvent::PublishCiFailed,
                    subject: "main-before-fix".into(),
                },
                Effect::PrepareCiFix,
            ]
        );
        assert_eq!(
            p.apply(ProcessorCommand::CiFixPrepared {
                outcome: CiFixPreparationOutcome::Fallback,
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::DispatchIntegration {
                    kind: LeafKind::CiFix,
                },
            ]
        );
        assert_eq!(p.state.integration.leaf_attempts["ci-fix"], 1);
        assert!(p.state.integration.ci_fix_provider_fallback);
        assert_eq!(
            p.apply(ProcessorCommand::CiFix {
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap(),
            vec![Effect::PersistCheckpoint, Effect::CommitCiFix]
        );
        assert!(!p.state.integration.ci_fix_provider_fallback);
    }

    #[test]
    fn codex_ci_fix_sandbox_downgrade_is_visible_before_claude_dispatch() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.published_head = Some("main-before-fix".into());
        p.state.integration.publication_pushed = Some(true);
        p.apply(ProcessorCommand::CiVerified {
            outcome: CiOutcome::Failed {
                signature: signature("required check failed"),
                reason: "required check failed".into(),
            },
        })
        .unwrap();

        assert_eq!(
            p.apply(ProcessorCommand::CiFixPrepared {
                outcome: CiFixPreparationOutcome::SandboxDowngraded {
                    scope: CodexSandboxDowngrade::Host,
                },
            })
            .unwrap(),
            vec![
                Effect::PersistCheckpoint,
                Effect::WriteJournalAndStatus,
                Effect::DispatchIntegration {
                    kind: LeafKind::CiFix,
                },
            ]
        );
        assert_eq!(
            p.state.integration.degradations,
            vec![CodexSandboxDowngrade::Host.degradation()]
        );
    }

    #[test]
    fn exhausted_or_failed_ci_repair_records_unconfirmed_manual_intervention() {
        let mut exhausted = processor();
        open(&mut exhausted);
        exhausted.state.phase = Phase::Publishing;
        exhausted.state.integration.published_head = Some("published-head".into());
        exhausted.state.integration.publication_pushed = Some(true);
        for signature_text in [
            "first required failure",
            "second required failure",
            "third required failure",
        ] {
            let effects = exhausted
                .apply(ProcessorCommand::CiVerified {
                    outcome: CiOutcome::Failed {
                        signature: signature(signature_text),
                        reason: signature_text.into(),
                    },
                })
                .unwrap();
            if signature_text.starts_with("first") {
                assert!(
                    effects
                        .iter()
                        .any(|effect| matches!(effect, Effect::PrepareCiFix))
                );
            }
        }
        assert_eq!(exhausted.state.phase, Phase::Blocked);
        assert_eq!(
            exhausted.state.integration.ci_disposition,
            Some(CiDisposition::UnconfirmedDegraded)
        );
        assert!(
            exhausted
                .state
                .integration
                .degradations
                .iter()
                .any(|reason| reason.contains("repair limit"))
        );

        let mut failed_fixer = processor();
        open(&mut failed_fixer);
        failed_fixer.state.phase = Phase::Publishing;
        failed_fixer.state.integration.published_head = Some("published-head".into());
        failed_fixer.state.integration.publication_pushed = Some(true);
        let effects = failed_fixer
            .apply(ProcessorCommand::CiFix {
                outcome: LeafOutcome::Escalated {
                    reason: "repair could not be completed safely".into(),
                },
            })
            .unwrap();
        assert_eq!(failed_fixer.state.phase, Phase::Blocked);
        assert_eq!(
            failed_fixer.state.integration.ci_disposition,
            Some(CiDisposition::UnconfirmedDegraded)
        );
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistCheckpoint,
                Effect::WriteJournalAndStatus,
                Effect::WaitForOperator { .. }
            ]
        ));
    }

    #[test]
    fn knowledge_failure_is_recorded_but_does_not_block_published_cleanup() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Cleaning;
        let mut task = TaskRuntime::new(&candidate("T-1", "engine/**"), 1);
        task.phase = TaskPhase::Published;
        p.state.tasks.insert(task.id.clone(), task);
        p.state.integration.merged_tasks.insert("T-1".into());
        p.state.integration.published_head = Some("published-head".into());
        p.state.integration.publication_pushed = Some(false);
        p.state.integration.ci_disposition = Some(CiDisposition::Disabled);
        let effects = p
            .apply(ProcessorCommand::KnowledgeCurated {
                outcome: LeafOutcome::Escalated {
                    reason: "knowledge store unavailable".into(),
                },
            })
            .unwrap();
        assert_eq!(p.state.phase, Phase::Cleaning);
        assert_eq!(p.state.integration.degradations.len(), 1);
        assert!(
            p.state
                .integration
                .pending_knowledge_curations
                .contains_key("B-20260724T120000Z")
        );
        assert_eq!(effects.last(), Some(&Effect::PrepareArchival));
        let physical = acknowledge_journal_and_skip_archive_ci(&mut p);
        assert!(physical.contains(&Effect::ArchiveTask {
            task_id: "T-1".into(),
        }));
        p.state.integration.dependency_graph_refreshed_post_archive = true;
        p.apply(ProcessorCommand::CleanupComplete).unwrap();
        assert!(
            p.state
                .integration
                .pending_knowledge_curations
                .contains_key("B-20260724T120000Z")
        );
        p.apply(ProcessorCommand::Open {
            batch_id: "B-20260725T120000Z".into(),
            base: "published-head".into(),
            now_secs: 2,
        })
        .unwrap();
        assert!(
            p.state
                .integration
                .pending_knowledge_curations
                .contains_key("B-20260724T120000Z")
        );
    }

    #[test]
    fn malformed_knowledge_result_cannot_partially_mutate_cleaning_state() {
        let mut p = processor();
        open(&mut p);
        p.state.phase = Phase::Cleaning;
        let before = p.state.clone();

        assert!(matches!(
            p.apply(ProcessorCommand::KnowledgeCurated {
                outcome: LeafOutcome::Escalated {
                    reason: "stale result".into(),
                },
            }),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("published head")
        ));
        assert_eq!(p.state, before);
        assert!(
            p.apply(ProcessorCommand::KnowledgeCurationPrepared {
                outcome: KnowledgeCurationPreparationOutcome::AlreadyCompleted,
            })
            .is_err()
        );
        assert_eq!(p.state, before);
        let mut legacy = serde_json::to_value(&before).unwrap();
        legacy["integration"]
            .as_object_mut()
            .unwrap()
            .remove("pending_knowledge_curations");
        legacy["integration"]
            .as_object_mut()
            .unwrap()
            .remove("archive_ci_gate");
        let decoded: ProcessorState = serde_json::from_value(legacy).unwrap();
        assert!(decoded.integration.pending_knowledge_curations.is_empty());
        assert!(decoded.integration.archive_ci_gate.is_none());

        p.state.integration.pending_knowledge_curations.insert(
            "../escape".into(),
            PendingKnowledgeCuration {
                base: "abc123".into(),
                published_head: "def456".into(),
                merged_tasks: BTreeSet::new(),
                fixed_task_findings: 0,
                integration_or_ci_signatures: 0,
                ci_failure_cycles: 0,
                quarantined_tasks: BTreeSet::new(),
                escalated_tasks: BTreeSet::new(),
                degradations: 1,
            },
        );
        assert!(matches!(
            Processor::from_checkpoint(p.config.clone(), p.state.clone()),
            Err(ProcessorError::CorruptCheckpoint(message))
                if message.contains("invalid deferred knowledge batch")
        ));
    }

    #[test]
    fn integration_failure_blocks_and_cleanup_cannot_forget_merged_work() {
        let config = ProcessorConfig {
            max_parallel: 2,
            integration_loop_max: 1,
            ..ProcessorConfig::default()
        };
        let mut p = Processor::new(config).unwrap();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.f_cycles = 1;
        let effects = p
            .apply(ProcessorCommand::IntegrationReview {
                outcome: ReviewOutcome::Findings {
                    signature: signature("F-01 irreparable"),
                    open_findings: 1,
                    open_finding_ids: vec!["F-01".into()],
                },
            })
            .unwrap();
        assert_eq!(p.state.phase, Phase::Blocked);
        assert!(matches!(
            effects.last(),
            Some(Effect::WaitForOperator { .. })
        ));

        let mut cleaner = processor();
        open(&mut cleaner);
        cleaner.state.phase = Phase::Cleaning;
        let mut merged = TaskRuntime::new(&candidate("T-9", "engine/**"), 1);
        merged.phase = TaskPhase::Merged;
        cleaner.state.tasks.insert(merged.id.clone(), merged);
        assert!(matches!(
            cleaner.apply(ProcessorCommand::CleanupComplete),
            Err(ProcessorError::InvalidCommand(message)) if message.contains("merged but not published")
        ));
    }

    #[test]
    fn incomplete_full_review_is_bounded_by_the_integration_cycle_limit() {
        let config = ProcessorConfig {
            max_parallel: 2,
            cohort_size: 3,
            review_loop_max: 2,
            integration_loop_max: 2,
            ci_fix_max: 2,
            stagnation_limit: 2,
            leaf_max_attempts: 1,
            ..ProcessorConfig::default()
        };
        let mut p = Processor::new(config).unwrap();
        open(&mut p);
        p.state.phase = Phase::Publishing;

        assert!(matches!(
            p.apply(ProcessorCommand::IntegrationReview {
                outcome: ReviewOutcome::Incomplete,
            })
            .unwrap()
            .last(),
            Some(Effect::DispatchIntegration {
                kind: LeafKind::IntegrationReview
            })
        ));
        assert_eq!(p.state.integration.f_cycles, 1);

        let effects = p
            .apply(ProcessorCommand::IntegrationReview {
                outcome: ReviewOutcome::Incomplete,
            })
            .unwrap();
        assert_eq!(p.state.phase, Phase::Blocked);
        assert!(matches!(
            effects.last(),
            Some(Effect::WaitForOperator { .. })
        ));
        assert_eq!(p.state.integration.f_cycles, 2);
    }

    #[test]
    fn final_integration_fix_does_not_schedule_an_over_limit_review() {
        let config = ProcessorConfig {
            max_parallel: 2,
            cohort_size: 3,
            review_loop_max: 2,
            integration_loop_max: 2,
            ci_fix_max: 2,
            stagnation_limit: 2,
            leaf_max_attempts: 1,
            ..ProcessorConfig::default()
        };
        let mut p = Processor::new(config).unwrap();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.f_cycles = 2;

        let effects = p
            .apply(ProcessorCommand::IntegrationFixCommitted {
                head: "integration-final-fix".into(),
            })
            .unwrap();
        assert_eq!(p.state.phase, Phase::Blocked);
        assert!(matches!(
            effects.last(),
            Some(Effect::WaitForOperator { .. })
        ));
    }

    #[test]
    fn publishing_recovery_does_not_dispatch_a_full_review_after_the_cycle_cap() {
        let config = ProcessorConfig {
            max_parallel: 2,
            cohort_size: 3,
            review_loop_max: 2,
            integration_loop_max: 2,
            ci_fix_max: 2,
            stagnation_limit: 2,
            leaf_max_attempts: 1,
            ..ProcessorConfig::default()
        };
        let mut p = Processor::new(config).unwrap();
        open(&mut p);
        p.state.phase = Phase::Publishing;
        p.state.integration.f_cycles = 2;

        let effect = p.publishing_resume_effect().unwrap();
        assert_eq!(p.state.phase, Phase::Blocked);
        assert!(matches!(effect, Effect::WaitForOperator { .. }));
    }
}
