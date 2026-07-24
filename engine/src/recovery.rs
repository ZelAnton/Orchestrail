//! Deterministic Phase-0 reconciliation planning.
//!
//! A process restart is not allowed to infer progress from a directory merely existing.  This
//! module combines the durable control-plane [`Snapshot`] with observations collected by the VCS
//! boundary and emits a *plan*, never mutations.  The runtime executes each action through its
//! guarded queue/VCS ports and persists its result before moving to the next action.
//!
//! The separation is deliberate: parsing Markdown is a read-only concern of [`crate::state`],
//! VCS probing belongs to [`crate::vcs`], and recovery policy must remain unit-testable without a
//! repository or an agent process.

use std::collections::{BTreeMap, BTreeSet};

use crate::processor::{
    CiDisposition, CloseReasonWire, CohortRuntime, ImportedRecoveryIntent, IntegrationRuntime,
    Phase, ProcessorConfig, ProcessorState, TaskPhase, TaskRuntime,
};
use crate::resolvers::{AdmissionGate, CohortCounters, CohortThresholds, admission_gate};
use crate::state::{
    CohortAdmission, CohortState, Descriptor, IntegrationState, Snapshot, TaskState,
};
use crate::time::{epoch_to_iso, iso_to_epoch};

/// VCS evidence for one task branch collected during Phase 0.  A missing map entry is **not**
/// evidence that the branch is missing; it is an incomplete observation and therefore blocks a
/// live task rather than inviting a destructive guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRepositoryObservation {
    pub branch_exists: bool,
    pub workspace_present: bool,
    /// Present only when the managed workspace was observed. A dirty or conflicted workspace is
    /// evidence of an interrupted mutation and must not authorize a recovered leaf dispatch.
    pub workspace_clean: Option<bool>,
    /// Exact durable branch/bookmark target when it exists. This is distinct from a JJ working
    /// copy's empty successor and is the coordinate a resumed review must verify.
    pub branch_head: Option<String>,
    /// True only when the observed branch has a commit/change past the batch base.  This is the
    /// Phase-0 discriminator between a freshly-created task branch and durable work.
    pub commits_after_base: bool,
    /// When an integration branch exists, whether this task branch/bookmark is a proven ancestor
    /// of it. `None` means there was no integration branch to compare against.
    pub integrated_into_active: Option<bool>,
}

/// Publication evidence for an existing integration branch.  `Unknown` is intentionally not
/// coerced to `NotPublished`: for a push-enabled batch local `main` is not an irreversible
/// publication boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationObservation {
    Published,
    NotPublished,
    Unknown,
}

/// VCS and durable-artifact observations for the integration branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationRepositoryObservation {
    pub branch_exists: bool,
    pub workspace_present: bool,
    /// Exact durable integration branch/bookmark tip when it exists. This is required before a
    /// legacy integration state may be adopted into a native publishing checkpoint.
    pub branch_head: Option<String>,
    /// True only when the integration branch contains a commit/change after the recorded cohort
    /// base. It is a range query, not a comparison of a branch name to a commit id.
    pub commits_after_base: bool,
    /// Present only for a registered integration workspace. A dirty or conflicted workspace is
    /// interrupted mutation evidence and must stay operator-held.
    pub workspace_clean: Option<bool>,
    pub merge_report_present: bool,
    /// Structured per-task records read from the legacy merger artifact. The importer still
    /// independently proves each claimed merged task against VCS ancestry.
    pub merge_report_lines: Option<Vec<crate::contract::MergeLine>>,
    pub publication: PublicationObservation,
}

/// Complete evidence supplied by the impure recovery adapter.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryInventory {
    pub tasks: BTreeMap<String, TaskRepositoryObservation>,
    pub integration: Option<IntegrationRepositoryObservation>,
}

/// Exact safe point to which an active task may return after reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResumePoint {
    /// The task branch is empty (or was recreated from the recorded base); invoke its maker.
    Implementation,
    /// A branch has durable commits, but the descriptor still says `в работе`; resume the maker
    /// rather than pretending the implementation is complete.
    ImplementationWithCommittedWork,
    /// A completed task commit exists and the descriptor is at `на ревью`; choose the reviewer
    /// from the persisted implementation-author history and review SHA, never from process memory.
    Review {
        review_sha: Option<String>,
        last_implementation_author: Option<String>,
    },
}

/// Which later phase owns continuation after task-level reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationResumePoint {
    Rolling,
    Merge,
    Publish,
    Accounting,
}

/// One mutation or resume action that the runtime must perform through a typed/guarded port.
/// Actions are in stable task-id order; callers must stop on the first failed action and retain
/// the evidence used to produce the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Queue says work is active but no descriptor survived.  Preserve a quarantine attempt
    /// counter; only the status is returned to `not-started`.
    ReturnOrphanedQueue {
        task_id: String,
        attempt: Option<u32>,
    },
    /// Descriptor is authoritative for a live task but the queue capture label was lost.
    RestoreQueueCapture {
        task_id: String,
        batch_id: String,
        branch: String,
        worktree: String,
    },
    /// A descriptor belongs to no batch (or not to this batch) and is safe to remove only through
    /// the managed-workspace guard.  The queue remains eligible for a later normal admission.
    RemoveUncapturedDescriptor { task_id: String, reason: String },
    /// Recreate/verify the exact conventional worktree.  `create_branch` is true only after a
    /// missing task branch was observed, so recovery never resets an existing branch with `-b`.
    EnsureTaskWorkspace {
        task_id: String,
        branch: String,
        create_branch: bool,
    },
    ResumeTask {
        task_id: String,
        point: TaskResumePoint,
    },
    /// A quarantined descriptor is returned through the bounded quarantine transaction, not
    /// silently re-labelled as ordinary work.
    RequeueConflict {
        task_id: String,
        previous_attempt: Option<u32>,
    },
    /// A terminally published descriptor still needs Phase 6 accounting/archival.
    AccountPublished { task_id: String },
    /// A `done` descriptor survived a Phase-6 crash before its immutable archive projection or
    /// workspace cleanup. Queue removal and the terminal event are replay-safe at this boundary.
    AccountDone { task_id: String },
    ContinueIntegration {
        batch_id: String,
        point: IntegrationResumePoint,
    },
}

/// The final disposition after every planned action succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDisposition {
    Idle,
    Rolling,
    Joining,
    Publishing,
    Cleaning,
    /// No leaf or VCS action is permitted until the operator resolves every blocker.
    Blocked,
}

/// A complete, auditable Phase-0 decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub actions: Vec<RecoveryAction>,
    pub disposition: RecoveryDisposition,
    pub blockers: Vec<String>,
}

impl RecoveryPlan {
    pub fn is_blocked(&self) -> bool {
        !self.blockers.is_empty()
    }
}

/// Why a legacy Markdown control plane cannot be made into a native checkpoint.  This is a
/// diagnostic, not a best-effort converter: callers keep the legacy state untouched and hold for
/// an operator whenever one coordinate cannot be proved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyImportError {
    message: String,
}

impl LegacyImportError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for LegacyImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for LegacyImportError {}

/// A complete, already-validated token-telemetry observation supplied by the native boundary
/// while adopting an open legacy cohort.  Recovery policy remains independent of the outbox
/// reader: the port owns file I/O and collapses every unreadable or incomplete signal into the
/// fail-closed `Unavailable` case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyTokenTelemetry {
    Actual { tokens: u64 },
    Unavailable,
}

/// Prepare an in-memory Phase-0 projection for the legacy 0.3b case where `batch.md` exists
/// but `cohort_state.md` was never written (or predates that artifact). The legacy processor
/// treats admission as closed in that shape: it may finish the proven batch tasks, but it must
/// never top up the cohort from the queue.
///
/// The caller supplies the cutover clock only because a native [`CohortRuntime`] requires a
/// durable timestamp for status/journal coordinates. It is not used to reopen or re-evaluate
/// admission: the synthesized state is closed before the reducer is constructed. The Markdown
/// control plane itself remains untouched; the first native runtime envelope becomes the
/// authoritative durable record.
pub fn synthesize_missing_legacy_cohort_state(
    snapshot: &Snapshot,
    imported_at_secs: u64,
) -> Result<Snapshot, LegacyImportError> {
    if snapshot.batch.is_none() || snapshot.cohort.is_some() {
        return Ok(snapshot.clone());
    }
    let batch = snapshot
        .batch
        .as_ref()
        .expect("checked batch is present before synthesizing cohort state");
    let batch_id = batch
        .batch_id
        .as_deref()
        .filter(|value| valid_batch_id(value))
        .ok_or_else(|| LegacyImportError::new("batch.md has no valid batch id"))?;
    let admitted_total = u32::try_from(batch.tasks.len()).map_err(|_| {
        LegacyImportError::new("batch.md has too many tasks for native admitted-total coordinate")
    })?;
    let wave = batch
        .tasks
        .iter()
        .filter_map(|task| task.wave)
        .filter(|wave| *wave > 0)
        .max()
        .unwrap_or(1);

    let mut projected = snapshot.clone();
    projected.cohort = Some(CohortState {
        batch_id: Some(batch_id.to_string()),
        admission: Some(CohortAdmission::Closed),
        admission_literal: Some("закрыт".into()),
        admission_reason: None,
        started_at: Some(epoch_to_iso(imported_at_secs)),
        wave: Some(wave),
        admitted_total: Some(admitted_total),
    });
    Ok(projected)
}

/// Bind the configuration that will govern a legacy cohort after cutover.
///
/// Legacy Markdown predates the native runtime's immutable safety snapshot, so its absence is
/// not evidence that a configured budget or event-outbox policy was disabled.  This function is
/// intentionally limited to the explicit legacy-import boundary; ordinary runtime checkpoints
/// must continue to reject a changed safety configuration in
/// [`crate::processor::Processor::from_checkpoint`].
pub fn bind_legacy_safety_snapshot(state: &mut ProcessorState, config: &ProcessorConfig) {
    if let Some(batch) = state.batch.as_mut() {
        batch.cohort_budget_secs = config.cohort_budget_secs;
        batch.cohort_token_budget = config.cohort_token_budget;
        batch.cohort_token_budget_strict = config.cohort_token_budget_strict;
        batch.events_outbox_enabled = config.events_outbox_enabled;
    }
}

/// Repeat the Phase-0.3b admission check for an imported *open* legacy cohort before it can
/// enter the native runtime.  A persisted `открыт` is not authoritative at a crash boundary:
/// current size/age/budget counters and, when configured, a complete token telemetry snapshot
/// decide whether native admission may remain open.  Existing active tasks are deliberately not
/// escalated here; this is an admission closure, matching the legacy Phase-0 contract.
pub fn recheck_legacy_open_admission(
    state: &mut ProcessorState,
    config: &ProcessorConfig,
    now_secs: u64,
    token_telemetry: Option<LegacyTokenTelemetry>,
) -> Result<bool, LegacyImportError> {
    if !matches!(state.phase, Phase::Rolling) {
        return Ok(false);
    }
    let Some(batch) = state.batch.as_mut() else {
        return Err(LegacyImportError::new(
            "legacy admission recheck requires an active cohort",
        ));
    };
    if batch.admission_closed.is_some() {
        return Ok(false);
    }

    let elapsed = now_secs.saturating_sub(batch.started_at_secs);
    let counters = CohortCounters {
        admitted_total: batch.admitted_total,
        age_minutes: elapsed / 60,
        elapsed_sec: elapsed,
    };
    let thresholds = CohortThresholds {
        size: config.cohort_size,
        max_age_minutes: config.cohort_max_age_minutes,
        budget_sec: config.cohort_budget_secs,
    };
    if let AdmissionGate::Close(reason) = admission_gate(counters, thresholds) {
        batch.admission_closed = Some(reason.into());
        return Ok(true);
    }

    let Some(limit) = config.cohort_token_budget else {
        return Ok(false);
    };
    let observation = token_telemetry.ok_or_else(|| {
        LegacyImportError::new(
            "legacy admission recheck requires token telemetry when COHORT_TOKEN_BUDGET is enabled",
        )
    })?;
    match observation {
        LegacyTokenTelemetry::Actual { tokens } if tokens < limit => {
            batch.token_budget_actual_tokens = Some(tokens);
            Ok(false)
        }
        LegacyTokenTelemetry::Actual { tokens } => {
            batch.token_budget_actual_tokens = Some(tokens);
            batch.admission_closed = Some(CloseReasonWire::CohortTokenBudget);
            Ok(true)
        }
        LegacyTokenTelemetry::Unavailable => {
            batch.token_budget_actual_tokens = None;
            batch.admission_closed = Some(CloseReasonWire::CohortTokenBudget);
            Ok(true)
        }
    }
}

/// Convert the one legacy state that has no unrecorded agent operation into a native reducer
/// checkpoint: a closed batch whose only non-merged tasks are already marked `ready` or
/// terminally `escalated`, before Phase 4 has created an integration branch/workspace.
///
/// A `working` or `in-review` descriptor cannot be imported this way because Markdown does not
/// prove whether its external leaf was interrupted before or after it changed the worktree.
/// Likewise, a started integration has VCS progress that needs an exact integration tip.  Those
/// shapes intentionally remain Phase-0 holds until their dedicated importers exist.
pub fn import_closed_ready_cohort(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    plan: &RecoveryPlan,
) -> Result<ProcessorState, LegacyImportError> {
    if !matches!(snapshot.integration.state, IntegrationState::None) {
        return Err(LegacyImportError::new(
            "legacy import accepts only a batch before integration_state.md exists",
        ));
    }
    if plan.is_blocked()
        || !plan.actions.is_empty()
        || !matches!(plan.disposition, RecoveryDisposition::Joining)
    {
        return Err(LegacyImportError::new(format!(
            "legacy import requires a clean joining plan (disposition={:?}, actions={}, blockers={})",
            plan.disposition,
            plan.actions.len(),
            plan.blockers.len()
        )));
    }
    let Some(integration) = inventory.integration.as_ref() else {
        return Err(LegacyImportError::new(
            "legacy import has no integration VCS observation",
        ));
    };
    if integration.branch_exists
        || integration.workspace_present
        || integration.merge_report_present
    {
        return Err(LegacyImportError::new(
            "legacy import accepts only a batch before integration VCS state exists",
        ));
    }

    let batch = snapshot
        .batch
        .as_ref()
        .ok_or_else(|| LegacyImportError::new("legacy import requires batch.md"))?;
    let batch_id = batch
        .batch_id
        .as_deref()
        .filter(|value| valid_batch_id(value))
        .ok_or_else(|| LegacyImportError::new("batch.md has no valid batch id"))?;
    let base = batch
        .base
        .as_deref()
        .filter(|value| valid_ref(value))
        .ok_or_else(|| LegacyImportError::new("batch.md has no valid immutable base"))?;
    if batch.integration_branch.as_deref() != Some(&format!("integration/{batch_id}")) {
        return Err(LegacyImportError::new(format!(
            "batch {batch_id} does not record the conventional integration branch"
        )));
    }
    if batch.tasks.is_empty() {
        return Err(LegacyImportError::new(
            "legacy import does not treat an empty batch as an active native cohort",
        ));
    }

    let cohort = snapshot
        .cohort
        .as_ref()
        .ok_or_else(|| LegacyImportError::new("legacy import requires cohort_state.md"))?;
    if cohort.batch_id.as_deref() != Some(batch_id) {
        return Err(LegacyImportError::new(format!(
            "cohort batch id {:?} does not match batch {batch_id}",
            cohort.batch_id
        )));
    }
    if cohort.admission != Some(CohortAdmission::Closed) {
        return Err(LegacyImportError::new(
            "legacy import requires an explicitly closed cohort admission",
        ));
    }
    let admission_closed = cohort
        .admission_reason
        .as_deref()
        .and_then(close_reason_from_legacy)
        // A pre-artifact legacy batch has no cohort document to carry a reason. The synthetic
        // Phase-0.3b projection deliberately renders that as a closed admission without one;
        // preserve the conservative closure in the native checkpoint rather than fabricating a
        // size/queue-empty cause. The same treatment is safe for an old manually closed file
        // that lacked a reason.
        .unwrap_or(CloseReasonWire::LegacyCohortStateAbsent);
    let started_at_secs = cohort
        .started_at
        .as_deref()
        .and_then(iso_to_epoch)
        .ok_or_else(|| LegacyImportError::new("cohort has no valid UTC start time"))?;
    let wave = cohort
        .wave
        .filter(|wave| *wave > 0)
        .ok_or_else(|| LegacyImportError::new("cohort has no positive wave"))?;
    let admitted_total = cohort
        .admitted_total
        .ok_or_else(|| LegacyImportError::new("cohort has no admitted-total coordinate"))?;
    if admitted_total as usize != batch.tasks.len() {
        return Err(LegacyImportError::new(format!(
            "cohort admitted total {admitted_total} does not equal {} batch tasks",
            batch.tasks.len()
        )));
    }

    let queue = unique_by_id(
        snapshot
            .queue
            .iter()
            .map(|entry| (entry.id.as_str(), entry)),
    );
    let descriptors = unique_by_id(
        snapshot
            .descriptors
            .iter()
            .map(|descriptor| (descriptor.id.as_str(), descriptor)),
    );
    let batch_tasks = unique_by_id(batch.tasks.iter().map(|task| (task.id.as_str(), task)));
    if descriptors.len() != batch_tasks.len()
        || descriptors
            .keys()
            .any(|task_id| !batch_tasks.contains_key(task_id))
    {
        return Err(LegacyImportError::new(
            "legacy import requires descriptors to match the active batch exactly",
        ));
    }

    let mut tasks = BTreeMap::new();
    for (task_id, batch_task) in batch_tasks {
        let descriptor = descriptors.get(task_id).copied().ok_or_else(|| {
            LegacyImportError::new(format!("batch task {task_id} has no descriptor"))
        })?;
        let queue_entry = queue.get(task_id).copied().ok_or_else(|| {
            LegacyImportError::new(format!("batch task {task_id} has no queue entry"))
        })?;
        let terminal_escalation = matches!(
            (descriptor.state, queue_entry.state),
            (Some(TaskState::Escalated), Some(TaskState::Escalated))
        );
        if !terminal_escalation
            && (descriptor.state != Some(TaskState::Ready)
                || !matches!(
                    queue_entry.state,
                    Some(TaskState::Working | TaskState::Ready)
                ))
        {
            return Err(LegacyImportError::new(format!(
                "legacy import requires {task_id} to be ready with a captured or legacy-ready queue row, or terminally escalated in both"
            )));
        }
        // A successful re-capture keeps its prior `попытка=N` through review and join. The
        // counter remains in the queue so a later merge quarantine continues the same bounded
        // budget; it is not evidence that an otherwise ready task is inconsistent.
        if descriptor.batch_id.as_deref() != Some(batch_id) {
            return Err(LegacyImportError::new(format!(
                "ready descriptor {task_id} does not belong to batch {batch_id}"
            )));
        }
        let branch = format!("task/{task_id}");
        let worktree = format!(".work/worktrees/{task_id}");
        if descriptor.branch.as_deref() != Some(branch.as_str())
            || descriptor.worktree.as_deref().map(normalize_path) != Some(worktree.clone())
            || batch_task.branch.as_deref() != Some(branch.as_str())
            || batch_task
                .worktree
                .as_deref()
                .is_some_and(|value| normalize_path(value) != worktree)
        {
            return Err(LegacyImportError::new(format!(
                "ready task {task_id} does not record conventional branch/worktree coordinates"
            )));
        }
        let level = descriptor.level.ok_or_else(|| {
            LegacyImportError::new(format!("ready task {task_id} has no executor level"))
        })?;
        if batch_task.level.as_deref() != Some(level.as_str()) {
            return Err(LegacyImportError::new(format!(
                "ready task {task_id} has inconsistent descriptor/batch executor levels"
            )));
        }
        let domain = descriptor.conflict_domain.as_ref().ok_or_else(|| {
            LegacyImportError::new(format!("ready task {task_id} has no conflict domain"))
        })?;
        let conflict_domain = domain.join(",");
        if batch_task.domain.as_deref() != Some(conflict_domain.as_str()) {
            return Err(LegacyImportError::new(format!(
                "ready task {task_id} has inconsistent descriptor/batch conflict domains"
            )));
        }
        let task_wave = batch_task.wave.filter(|wave| *wave > 0).ok_or_else(|| {
            LegacyImportError::new(format!("ready task {task_id} has no positive batch wave"))
        })?;
        let (phase, review_sha, review_cycles, reason) = if terminal_escalation {
            let reason = queue_entry
                .escalation_reason
                .as_deref()
                .filter(|reason| !reason.trim().is_empty())
                .ok_or_else(|| {
                    LegacyImportError::new(format!(
                        "escalated task {task_id} has no terminal escalation reason"
                    ))
                })?;
            // Escalation can occur before any review begins. Its stale/missing review fields
            // therefore cannot be made a precondition for the Phase-6-only import.
            (TaskPhase::Escalated, None, 0, Some(reason.to_string()))
        } else {
            let review_sha = descriptor
                .review_sha
                .clone()
                .filter(|value| valid_ref(value))
                .ok_or_else(|| {
                    LegacyImportError::new(format!(
                        "ready task {task_id} has no valid reviewed tip"
                    ))
                })?;
            let review_cycles = descriptor.review_cycles.ok_or_else(|| {
                LegacyImportError::new(format!(
                    "ready task {task_id} has no review-cycle coordinate"
                ))
            })?;
            (TaskPhase::Ready, Some(review_sha), review_cycles, None)
        };
        tasks.insert(
            task_id.to_string(),
            TaskRuntime {
                id: task_id.to_string(),
                conflict_domain,
                level: Some(level),
                risk: descriptor.risk,
                wave: task_wave,
                phase,
                leaf_attempts: BTreeMap::new(),
                review_cycles,
                review_signatures: Vec::new(),
                implementation_author: descriptor.implementation_authors.last().cloned(),
                previous_review_sha: None,
                review_sha,
                reason,
                imported_recovery_intent: None,
            },
        );
    }

    // Legacy skips Phase 4 entirely when no captured task remains ready to merge.  Do not
    // reconstruct a join merely to discover that fact again: terminal escalations have no
    // remaining planner or merger action and must enter the ordinary Phase-6 ledger directly.
    let phase = if tasks
        .values()
        .any(|task| matches!(task.phase, TaskPhase::Ready))
    {
        Phase::Joining
    } else {
        Phase::Cleaning
    };

    Ok(ProcessorState {
        schema_version: crate::processor::PROCESSOR_STATE_VERSION,
        phase,
        paused_from: None,
        batch: Some(CohortRuntime {
            id: batch_id.to_string(),
            base: base.to_string(),
            started_at_secs,
            wave,
            admitted_total,
            admission_closed: Some(admission_closed),
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            token_budget_actual_tokens: None,
            events_outbox_enabled: true,
        }),
        tasks,
        integration: IntegrationRuntime::default(),
        blocked_reason: None,
    })
}

/// Convert a strictly proved pre-join cohort that still contains active legacy tasks.
///
/// Markdown has no durable record of an interrupted agent process, so this importer never claims
/// an agent result.  It only accepts clean, exact VCS coordinates and represents the next action
/// as an [`ImportedRecoveryIntent`].  The normal Phase-0 reducer then records the corresponding
/// workspace or leaf effect in its durable ledger before the port is allowed to touch VCS or an
/// external agent.  Integration state is intentionally excluded: its merge/report/publication
/// coordinates need a separate importer.
pub fn import_active_cohort(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    plan: &RecoveryPlan,
) -> Result<ProcessorState, LegacyImportError> {
    if !matches!(snapshot.integration.state, IntegrationState::None) {
        return Err(LegacyImportError::new(
            "active legacy import accepts only a batch before integration_state.md exists",
        ));
    }
    if plan.is_blocked() || !matches!(plan.disposition, RecoveryDisposition::Rolling) {
        return Err(LegacyImportError::new(format!(
            "active legacy import requires an unblocked rolling plan (disposition={:?}, blockers={})",
            plan.disposition,
            plan.blockers.len()
        )));
    }
    let Some(integration) = inventory.integration.as_ref() else {
        return Err(LegacyImportError::new(
            "active legacy import has no integration VCS observation",
        ));
    };
    if integration.branch_exists
        || integration.workspace_present
        || integration.merge_report_present
    {
        return Err(LegacyImportError::new(
            "active legacy import accepts only a batch before integration VCS state exists",
        ));
    }

    let batch = snapshot
        .batch
        .as_ref()
        .ok_or_else(|| LegacyImportError::new("active legacy import requires batch.md"))?;
    let batch_id = batch
        .batch_id
        .as_deref()
        .filter(|value| valid_batch_id(value))
        .ok_or_else(|| LegacyImportError::new("batch.md has no valid batch id"))?;
    let base = batch
        .base
        .as_deref()
        .filter(|value| valid_ref(value))
        .ok_or_else(|| LegacyImportError::new("batch.md has no valid immutable base"))?;
    if batch.integration_branch.as_deref() != Some(&format!("integration/{batch_id}")) {
        return Err(LegacyImportError::new(format!(
            "batch {batch_id} does not record the conventional integration branch"
        )));
    }
    if batch.tasks.is_empty() {
        return Err(LegacyImportError::new(
            "active legacy import does not treat an empty batch as an active native cohort",
        ));
    }

    let cohort = snapshot
        .cohort
        .as_ref()
        .ok_or_else(|| LegacyImportError::new("active legacy import requires cohort_state.md"))?;
    if cohort.batch_id.as_deref() != Some(batch_id) {
        return Err(LegacyImportError::new(format!(
            "cohort batch id {:?} does not match batch {batch_id}",
            cohort.batch_id
        )));
    }
    let admission_closed = match cohort.admission {
        Some(CohortAdmission::Closed) => Some(
            cohort
                .admission_reason
                .as_deref()
                .and_then(close_reason_from_legacy)
                .unwrap_or(CloseReasonWire::LegacyCohortStateAbsent),
        ),
        Some(CohortAdmission::Open) => {
            if cohort.admission_reason.is_some() {
                return Err(LegacyImportError::new(
                    "open cohort unexpectedly records a close reason",
                ));
            }
            None
        }
        None => {
            return Err(LegacyImportError::new(
                "active legacy import requires an explicit cohort admission state",
            ));
        }
    };
    let started_at_secs = cohort
        .started_at
        .as_deref()
        .and_then(iso_to_epoch)
        .ok_or_else(|| LegacyImportError::new("cohort has no valid UTC start time"))?;
    let wave = cohort
        .wave
        .filter(|wave| *wave > 0)
        .ok_or_else(|| LegacyImportError::new("cohort has no positive wave"))?;
    let admitted_total = cohort
        .admitted_total
        .ok_or_else(|| LegacyImportError::new("cohort has no admitted-total coordinate"))?;
    if admitted_total as usize != batch.tasks.len() {
        return Err(LegacyImportError::new(format!(
            "cohort admitted total {admitted_total} does not equal {} batch tasks",
            batch.tasks.len()
        )));
    }

    let queue = unique_by_id(
        snapshot
            .queue
            .iter()
            .map(|entry| (entry.id.as_str(), entry)),
    );
    let descriptors = unique_by_id(
        snapshot
            .descriptors
            .iter()
            .map(|descriptor| (descriptor.id.as_str(), descriptor)),
    );
    let batch_tasks = unique_by_id(batch.tasks.iter().map(|task| (task.id.as_str(), task)));
    if descriptors.len() != batch_tasks.len()
        || descriptors
            .keys()
            .any(|task_id| !batch_tasks.contains_key(task_id))
    {
        return Err(LegacyImportError::new(
            "active legacy import requires descriptors to match the active batch exactly",
        ));
    }

    let mut expected_action_count = 0usize;
    let mut saw_active_task = false;
    let mut tasks = BTreeMap::new();
    for (task_id, batch_task) in batch_tasks {
        let descriptor = descriptors.get(task_id).copied().ok_or_else(|| {
            LegacyImportError::new(format!("batch task {task_id} has no descriptor"))
        })?;
        let queue_entry = queue.get(task_id).copied().ok_or_else(|| {
            LegacyImportError::new(format!("batch task {task_id} has no queue entry"))
        })?;
        // A task admitted after a prior quarantine remains a normal active capture, but the
        // queue keeps its attempt counter so a later Phase-6 return continues the bounded
        // budget. The native return boundary re-reads that counter from Markdown; it must not
        // make an otherwise proved Phase-0 import ineligible.
        if descriptor.batch_id.as_deref() != Some(batch_id) {
            return Err(LegacyImportError::new(format!(
                "descriptor {task_id} does not belong to batch {batch_id}"
            )));
        }
        let branch = format!("task/{task_id}");
        let worktree = format!(".work/worktrees/{task_id}");
        if descriptor.branch.as_deref() != Some(branch.as_str())
            || descriptor.worktree.as_deref().map(normalize_path) != Some(worktree.clone())
            || batch_task.branch.as_deref() != Some(branch.as_str())
            || batch_task.worktree.as_deref().map(normalize_path) != Some(worktree.clone())
        {
            return Err(LegacyImportError::new(format!(
                "task {task_id} does not record conventional branch/worktree coordinates"
            )));
        }
        let level = descriptor.level.ok_or_else(|| {
            LegacyImportError::new(format!("task {task_id} has no executor level"))
        })?;
        if batch_task.level.as_deref() != Some(level.as_str()) {
            return Err(LegacyImportError::new(format!(
                "task {task_id} has inconsistent descriptor/batch executor levels"
            )));
        }
        let domain = descriptor.conflict_domain.as_ref().ok_or_else(|| {
            LegacyImportError::new(format!("task {task_id} has no conflict domain"))
        })?;
        let conflict_domain = domain.join(",");
        if batch_task.domain.as_deref() != Some(conflict_domain.as_str()) {
            return Err(LegacyImportError::new(format!(
                "task {task_id} has inconsistent descriptor/batch conflict domains"
            )));
        }
        let task_wave = batch_task.wave.filter(|wave| *wave > 0).ok_or_else(|| {
            LegacyImportError::new(format!("task {task_id} has no positive batch wave"))
        })?;
        let previous_review_sha = match descriptor.review_sha.as_deref() {
            Some(review_sha) if valid_ref(review_sha) => Some(review_sha.to_string()),
            Some(_) => {
                return Err(LegacyImportError::new(format!(
                    "task {task_id} has an invalid persisted review SHA"
                )));
            }
            None => None,
        };
        let review_cycles = descriptor.review_cycles.unwrap_or(0);
        let implementation_author = descriptor.implementation_authors.last().cloned();

        let (phase, review_sha, imported_recovery_intent) = match descriptor.state {
            Some(TaskState::Ready) => {
                if !matches!(
                    queue_entry.state,
                    Some(TaskState::Working | TaskState::Ready)
                ) {
                    return Err(LegacyImportError::new(format!(
                        "ready task {task_id} has no captured or legacy-ready queue row"
                    )));
                }
                let review_sha = previous_review_sha.clone().ok_or_else(|| {
                    LegacyImportError::new(format!(
                        "ready task {task_id} has no valid reviewed tip"
                    ))
                })?;
                (TaskPhase::Ready, Some(review_sha), None)
            }
            Some(TaskState::Working) => {
                saw_active_task = true;
                if !matches!(
                    queue_entry.state,
                    Some(TaskState::Working | TaskState::InReview)
                ) {
                    return Err(LegacyImportError::new(format!(
                        "working task {task_id} has no active queue capture"
                    )));
                }
                let observation = inventory.tasks.get(task_id).ok_or_else(|| {
                    LegacyImportError::new(format!(
                        "working task {task_id} has no VCS recovery observation"
                    ))
                })?;
                if observation.workspace_present
                    && (!observation.branch_exists || observation.workspace_clean != Some(true))
                {
                    return Err(LegacyImportError::new(format!(
                        "working task {task_id} has an unclean or branchless managed workspace"
                    )));
                }
                let point = if observation.commits_after_base {
                    TaskResumePoint::ImplementationWithCommittedWork
                } else {
                    TaskResumePoint::Implementation
                };
                require_exact_resume_action(plan, task_id, &point)?;
                expected_action_count += 1;
                if !observation.branch_exists || !observation.workspace_present {
                    if observation.workspace_present {
                        return Err(LegacyImportError::new(format!(
                            "working task {task_id} has a workspace but no durable branch"
                        )));
                    }
                    require_exact_workspace_action(
                        plan,
                        task_id,
                        &branch,
                        !observation.branch_exists,
                    )?;
                    expected_action_count += 1;
                    (
                        TaskPhase::Capturing,
                        previous_review_sha.clone(),
                        Some(ImportedRecoveryIntent::EnsureWorkspace),
                    )
                } else {
                    (
                        TaskPhase::Implementing,
                        previous_review_sha.clone(),
                        Some(ImportedRecoveryIntent::DispatchImplementation),
                    )
                }
            }
            Some(TaskState::InReview) => {
                saw_active_task = true;
                if !matches!(
                    queue_entry.state,
                    Some(TaskState::Working | TaskState::InReview)
                ) {
                    return Err(LegacyImportError::new(format!(
                        "review task {task_id} has no active queue capture"
                    )));
                }
                let observation = inventory.tasks.get(task_id).ok_or_else(|| {
                    LegacyImportError::new(format!(
                        "review task {task_id} has no VCS recovery observation"
                    ))
                })?;
                let branch_head = observation
                    .branch_head
                    .as_deref()
                    .filter(|tip| valid_ref(tip))
                    .ok_or_else(|| {
                        LegacyImportError::new(format!(
                            "review task {task_id} has no valid durable branch tip"
                        ))
                    })?;
                if !observation.branch_exists || !observation.commits_after_base {
                    return Err(LegacyImportError::new(format!(
                        "review task {task_id} has no commit beyond the batch base"
                    )));
                }
                let point = TaskResumePoint::Review {
                    review_sha: descriptor.review_sha.clone(),
                    last_implementation_author: implementation_author.clone(),
                };
                require_exact_resume_action(plan, task_id, &point)?;
                expected_action_count += 1;
                if observation.workspace_present {
                    if observation.workspace_clean != Some(true) {
                        return Err(LegacyImportError::new(format!(
                            "review task {task_id} has an unclean managed workspace"
                        )));
                    }
                    (
                        TaskPhase::Reviewing,
                        Some(branch_head.to_string()),
                        Some(ImportedRecoveryIntent::DispatchReview),
                    )
                } else {
                    require_exact_workspace_action(plan, task_id, &branch, false)?;
                    expected_action_count += 1;
                    (
                        TaskPhase::Reviewing,
                        Some(branch_head.to_string()),
                        Some(ImportedRecoveryIntent::EnsureWorkspaceForReview),
                    )
                }
            }
            state => {
                return Err(LegacyImportError::new(format!(
                    "active legacy import supports only working, in-review, or ready tasks; {task_id} is {state:?}"
                )));
            }
        };
        tasks.insert(
            task_id.to_string(),
            TaskRuntime {
                id: task_id.to_string(),
                conflict_domain,
                level: Some(level),
                risk: descriptor.risk,
                wave: task_wave,
                phase,
                leaf_attempts: BTreeMap::new(),
                review_cycles,
                review_signatures: Vec::new(),
                implementation_author,
                previous_review_sha,
                review_sha,
                reason: None,
                imported_recovery_intent,
            },
        );
    }
    if !saw_active_task {
        return Err(LegacyImportError::new(
            "active legacy import requires at least one working or in-review task",
        ));
    }
    if plan.actions.len() != expected_action_count {
        return Err(LegacyImportError::new(format!(
            "active legacy import found {} unexpected recovery action(s)",
            plan.actions.len().saturating_sub(expected_action_count)
        )));
    }

    Ok(ProcessorState {
        schema_version: crate::processor::PROCESSOR_STATE_VERSION,
        phase: Phase::Rolling,
        paused_from: None,
        batch: Some(CohortRuntime {
            id: batch_id.to_string(),
            base: base.to_string(),
            started_at_secs,
            wave,
            admitted_total,
            admission_closed,
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            token_budget_actual_tokens: None,
            events_outbox_enabled: true,
        }),
        tasks,
        integration: IntegrationRuntime::default(),
        blocked_reason: None,
    })
}

/// Backwards-compatible strict subset for callers that deliberately require the old closed-only
/// import boundary. The general [`import_active_cohort`] additionally preserves an explicitly
/// open rolling admission so the native reducer can perform the next deterministic top-up.
pub fn import_closed_active_cohort(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    plan: &RecoveryPlan,
) -> Result<ProcessorState, LegacyImportError> {
    if snapshot.cohort.as_ref().and_then(|cohort| cohort.admission) != Some(CohortAdmission::Closed)
    {
        return Err(LegacyImportError::new(
            "closed active legacy import requires an explicitly closed cohort admission",
        ));
    }
    import_active_cohort(snapshot, inventory, plan)
}

/// Adopt an unreported Phase-4 integration boundary and replay the native per-task merger.
///
/// The integration branch may still be empty, or it may already contain one or more exact
/// reviewed task tips after the legacy merger crashed before writing `merge_report.md`.  We do
/// not reconstruct historic per-task merge results from ancestry: every task remains `Ready` and
/// the normal typed merger acknowledges it again.  Its VCS boundary treats an already-integrated
/// reviewed tip as an idempotent result and still runs the current verification policy before the
/// reducer records it.  Material integration history with no batch task ancestor is unexplained
/// and remains held.
pub fn import_unreported_integration_cohort(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    plan: &RecoveryPlan,
) -> Result<ProcessorState, LegacyImportError> {
    if !matches!(
        snapshot.integration.state,
        IntegrationState::None | IntegrationState::InProgress
    ) {
        return Err(LegacyImportError::new(
            "unreported integration import requires the pre-review Phase-4 boundary",
        ));
    }
    if snapshot.integration.review_sha.is_some()
        || snapshot
            .integration
            .f_cycles
            .is_some_and(|cycles| cycles != 0)
    {
        return Err(LegacyImportError::new(
            "unreported integration import refuses persisted integration review coordinates",
        ));
    }
    let batch_id = snapshot
        .batch
        .as_ref()
        .and_then(|batch| batch.batch_id.as_deref())
        .ok_or_else(|| {
            LegacyImportError::new("unreported integration import requires batch.md id")
        })?;
    let integration = inventory.integration.as_ref().ok_or_else(|| {
        LegacyImportError::new("unreported integration import has no integration VCS observation")
    })?;
    if !integration.branch_exists
        || integration.merge_report_present
        || !matches!(
            integration.publication,
            PublicationObservation::NotPublished
        )
        || integration
            .branch_head
            .as_deref()
            .is_none_or(|head| !valid_ref(head))
        || integration.workspace_present && integration.workspace_clean != Some(true)
    {
        return Err(LegacyImportError::new(
            "unreported integration import requires a clean-or-absent, report-free, unpublished integration branch",
        ));
    }
    if !matches!(plan.disposition, RecoveryDisposition::Joining)
        || plan.is_blocked()
        || plan.actions.len() != 1
        || !plan.actions.iter().all(|action| {
            matches!(
                action,
                RecoveryAction::ContinueIntegration {
                    batch_id: planned_batch_id,
                    point: IntegrationResumePoint::Merge,
                } if planned_batch_id == batch_id
            )
        })
    {
        return Err(LegacyImportError::new(
            "unreported integration import requires only the expected merge-continuation recovery action",
        ));
    }

    // Reuse the stricter closed-ready validation for every task, but make explicit that it sees
    // the pre-integration projection. This is not a guessed VCS state: the checks above prove the
    // real integration branch has no post-base content, and therefore the projection is exactly
    // the state before its creation.
    let mut pre_integration_snapshot = snapshot.clone();
    pre_integration_snapshot.integration.state = IntegrationState::None;
    pre_integration_snapshot.integration.review_sha = None;
    pre_integration_snapshot.integration.f_cycles = None;
    let mut pre_integration_inventory = inventory.clone();
    pre_integration_inventory.integration = Some(IntegrationRepositoryObservation {
        branch_exists: false,
        workspace_present: false,
        branch_head: None,
        commits_after_base: false,
        workspace_clean: None,
        merge_report_present: false,
        merge_report_lines: None,
        publication: PublicationObservation::NotPublished,
    });
    let pre_integration_plan = plan_recovery(&pre_integration_snapshot, &pre_integration_inventory);
    let mut state = import_closed_ready_cohort(
        &pre_integration_snapshot,
        &pre_integration_inventory,
        &pre_integration_plan,
    )?;
    let mut integrated_tasks = 0usize;
    for task in state
        .tasks
        .values()
        .filter(|task| matches!(task.phase, TaskPhase::Ready))
    {
        let observation = inventory.tasks.get(&task.id).ok_or_else(|| {
            LegacyImportError::new(format!(
                "unreported integration import has no VCS observation for ready task {}",
                task.id
            ))
        })?;
        if !observation.branch_exists
            || !observation.commits_after_base
            || observation.branch_head.as_deref() != task.review_sha.as_deref()
        {
            return Err(LegacyImportError::new(format!(
                "unreported integration import cannot prove ready task {} at its reviewed tip",
                task.id
            )));
        }
        match observation.integrated_into_active {
            Some(true) => integrated_tasks += 1,
            Some(false) => {}
            None => {
                return Err(LegacyImportError::new(format!(
                    "unreported integration import has no ancestry result for ready task {}",
                    task.id
                )));
            }
        }
    }
    if integration.commits_after_base && integrated_tasks == 0 {
        return Err(LegacyImportError::new(
            "unreported integration branch has post-base history but no reviewed batch task ancestor",
        ));
    }
    if !integration.commits_after_base && integrated_tasks != 0 {
        return Err(LegacyImportError::new(
            "unreported integration ancestry contradicts its empty base range",
        ));
    }
    state.integration.workspace_prepared = integration.workspace_present;
    state.integration.integration_head = integration
        .commits_after_base
        .then(|| integration.branch_head.clone())
        .flatten();
    Ok(state)
}

/// Adopt a complete legacy Phase-4 merger report after every task result has been recorded but
/// before the legacy processor started the integration-review loop.  The report is not trusted as
/// an execution log: every `merged` line must be backed by a typed VCS ancestry observation, and
/// every `quarantined` line must be absent from that same integration history.  We intentionally
/// re-run the native full-review and verification gates rather than treating the legacy report's
/// build marker as publication authority.
pub fn import_reported_integration_cohort(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    plan: &RecoveryPlan,
) -> Result<ProcessorState, LegacyImportError> {
    if !matches!(snapshot.integration.state, IntegrationState::None) {
        return Err(LegacyImportError::new(
            "reported integration import requires Phase 4 before integration_state.md exists",
        ));
    }
    let batch = snapshot
        .batch
        .as_ref()
        .ok_or_else(|| LegacyImportError::new("reported integration import requires batch.md"))?;
    let batch_id = batch
        .batch_id
        .as_deref()
        .filter(|value| valid_batch_id(value))
        .ok_or_else(|| LegacyImportError::new("batch.md has no valid batch id"))?;
    let integration = inventory.integration.as_ref().ok_or_else(|| {
        LegacyImportError::new("reported integration import has no integration VCS observation")
    })?;
    let integration_head = integration
        .branch_head
        .as_deref()
        .filter(|head| valid_ref(head))
        .ok_or_else(|| {
            LegacyImportError::new("reported integration import has no durable integration tip")
        })?;
    let report_lines = integration.merge_report_lines.as_ref().ok_or_else(|| {
        LegacyImportError::new("reported integration import requires merge_report.md contents")
    })?;
    if !integration.branch_exists
        || (integration.workspace_present && integration.workspace_clean != Some(true))
        || !integration.commits_after_base
        || !integration.merge_report_present
        || !matches!(
            integration.publication,
            PublicationObservation::NotPublished
        )
    {
        return Err(LegacyImportError::new(
            "reported integration import requires a clean-or-absent, unpublished integration workspace with material history",
        ));
    }

    let batch_ids: BTreeSet<&str> = batch.tasks.iter().map(|task| task.id.as_str()).collect();
    if batch_ids.is_empty() {
        return Err(LegacyImportError::new(
            "reported integration import requires at least one batch task",
        ));
    }
    let mut reported = BTreeMap::new();
    let mut quarantined = BTreeSet::new();
    for line in report_lines {
        if !batch_ids.contains(line.id.as_str()) || reported.contains_key(&line.id) {
            return Err(LegacyImportError::new(
                "merge_report.md has an unknown or duplicate batch task result",
            ));
        }
        match &line.outcome {
            crate::contract::MergeOutcome::Merged { sha, .. } if valid_ref(sha) => {}
            crate::contract::MergeOutcome::Merged { .. } => {
                return Err(LegacyImportError::new(
                    "merge_report.md has a merged result without a valid integration revision",
                ));
            }
            crate::contract::MergeOutcome::Quarantined { reason } if !reason.trim().is_empty() => {
                quarantined.insert(line.id.clone());
            }
            crate::contract::MergeOutcome::Quarantined { .. } => {
                return Err(LegacyImportError::new(
                    "merge_report.md has a quarantined result without a reason",
                ));
            }
        }
        reported.insert(line.id.clone(), line.outcome.clone());
    }
    let queue = unique_by_id(
        snapshot
            .queue
            .iter()
            .map(|entry| (entry.id.as_str(), entry)),
    );
    let descriptors = unique_by_id(
        snapshot
            .descriptors
            .iter()
            .map(|descriptor| (descriptor.id.as_str(), descriptor)),
    );
    if descriptors.len() != batch_ids.len()
        || descriptors
            .keys()
            .any(|task_id| !batch_ids.contains(task_id))
    {
        return Err(LegacyImportError::new(
            "reported integration import requires descriptors to match the active batch exactly",
        ));
    }

    // The merger only receives `ready` tasks. A task already escalated in Phase 2 has no merger
    // report line and must stay terminal through Phase 6. Likewise, a conflict descriptor with
    // an already-escalated queue row is a completed bounded-return transaction, not another
    // requeue candidate.
    let mut terminalized_quarantines = BTreeSet::new();
    for descriptor in descriptors.values() {
        let queue_entry = queue.get(descriptor.id.as_str()).copied().ok_or_else(|| {
            LegacyImportError::new(format!("task {} has no queue entry", descriptor.id))
        })?;
        let Some(outcome) = reported.get(&descriptor.id) else {
            if matches!(
                (descriptor.state, queue_entry.state),
                (Some(TaskState::Escalated), Some(TaskState::Escalated))
            ) {
                continue;
            }
            return Err(LegacyImportError::new(format!(
                "task {} is absent from merge_report.md but is not terminally escalated",
                descriptor.id
            )));
        };
        match outcome {
            crate::contract::MergeOutcome::Merged { .. }
                if matches!(
                    (descriptor.state, queue_entry.state),
                    (Some(TaskState::Ready), Some(TaskState::Ready))
                        | (Some(TaskState::Ready), Some(TaskState::Working))
                        | (Some(TaskState::Merged), Some(TaskState::Merged))
                        | (Some(TaskState::Merged), Some(TaskState::Working))
                ) => {}
            crate::contract::MergeOutcome::Quarantined { .. }
                if matches!(
                    (descriptor.state, queue_entry.state),
                    (Some(TaskState::Ready), Some(TaskState::Ready))
                        | (Some(TaskState::Ready), Some(TaskState::Working))
                        // Phase 4 can persist the descriptor conflict before the Phase-6 queue
                        // return. Both pre-return coordinates are legitimate.
                        | (Some(TaskState::Conflict), Some(TaskState::Ready))
                        | (Some(TaskState::Conflict), Some(TaskState::Working))
                        | (Some(TaskState::Conflict), Some(TaskState::Conflict))
                ) => {}
            crate::contract::MergeOutcome::Quarantined { .. }
                if matches!(
                    (descriptor.state, queue_entry.state),
                    (
                        Some(TaskState::Conflict | TaskState::Escalated),
                        Some(TaskState::Escalated)
                    )
                ) =>
            {
                terminalized_quarantines.insert(descriptor.id.clone());
            }
            _ => {
                return Err(LegacyImportError::new(format!(
                    "task {} status does not agree with its merge_report.md result",
                    descriptor.id
                )));
            }
        }
    }
    let requeues: BTreeSet<String> = quarantined
        .difference(&terminalized_quarantines)
        .cloned()
        .collect();
    let expected_action_count = requeues.len() + 1;
    let planned_requeues: BTreeSet<&str> = plan
        .actions
        .iter()
        .filter_map(|action| match action {
            RecoveryAction::RequeueConflict { task_id, .. } => Some(task_id.as_str()),
            _ => None,
        })
        .collect();
    let has_publish_continuation = plan.actions.iter().any(|action| {
        matches!(
            action,
            RecoveryAction::ContinueIntegration {
                batch_id: planned_batch_id,
                point: IntegrationResumePoint::Publish,
            } if planned_batch_id == batch_id
        )
    });
    if plan.is_blocked()
        || !matches!(plan.disposition, RecoveryDisposition::Publishing)
        || plan.actions.len() != expected_action_count
        || !has_publish_continuation
        || planned_requeues.len() != requeues.len()
        || planned_requeues != requeues.iter().map(String::as_str).collect()
    {
        return Err(LegacyImportError::new(
            "reported integration import requires only the exact publish/requeue recovery plan",
        ));
    }

    let mut pre_integration_snapshot = snapshot.clone();
    pre_integration_snapshot.integration.state = IntegrationState::None;
    pre_integration_snapshot.integration.review_sha = None;
    pre_integration_snapshot.integration.f_cycles = None;
    for descriptor in &mut pre_integration_snapshot.descriptors {
        if terminalized_quarantines.contains(&descriptor.id) {
            descriptor.state = Some(TaskState::Escalated);
            descriptor.status_literal = Some(TaskState::Escalated.as_str().into());
        } else if reported.contains_key(&descriptor.id) {
            descriptor.state = Some(TaskState::Ready);
            descriptor.status_literal = Some(TaskState::Ready.as_str().into());
        }
    }
    for entry in &mut pre_integration_snapshot.queue {
        if !terminalized_quarantines.contains(&entry.id) && reported.contains_key(&entry.id) {
            // The normal legacy queue state remains `working` after capture; older native
            // checkpoints may instead carry `ready`. Both are accepted by the shared pre-join
            // validation. A historical native `merged`/`published` row is projected back to
            // `ready` solely to reuse that pre-join validator; the real Markdown remains
            // untouched.
            if entry.state != Some(TaskState::Working) {
                entry.state = Some(TaskState::Ready);
                entry.status_literal = TaskState::Ready.as_str().into();
            }
            // This projection exists only to reuse strict pre-join identity validation. A
            // quarantined legacy task may have been captured after an earlier attempt, so its
            // queue row legitimately retains `попытка=N` while Phase 4 records the next
            // descriptor conflict. The native state carries a one-shot return intent; the real
            // control plane remains untouched and its port reads that counter before incrementing
            // it. Keep the coordinate in the synthetic view too: it is valid ready-task state,
            // not a reason to weaken or special-case the importer.
        }
    }
    let mut pre_integration_inventory = inventory.clone();
    pre_integration_inventory.integration = Some(IntegrationRepositoryObservation {
        branch_exists: false,
        workspace_present: false,
        branch_head: None,
        commits_after_base: false,
        workspace_clean: None,
        merge_report_present: false,
        merge_report_lines: None,
        publication: PublicationObservation::NotPublished,
    });
    let pre_integration_plan = plan_recovery(&pre_integration_snapshot, &pre_integration_inventory);
    let mut state = import_closed_ready_cohort(
        &pre_integration_snapshot,
        &pre_integration_inventory,
        &pre_integration_plan,
    )?;

    for (task_id, outcome) in reported {
        if terminalized_quarantines.contains(&task_id) {
            continue;
        }
        let observation = inventory.tasks.get(&task_id).ok_or_else(|| {
            LegacyImportError::new(format!("reported task {task_id} has no VCS observation"))
        })?;
        let task = state.tasks.get_mut(&task_id).ok_or_else(|| {
            LegacyImportError::new(format!("imported state has no task {task_id}"))
        })?;
        match outcome {
            crate::contract::MergeOutcome::Merged { sha, .. } => {
                if observation.integrated_into_active != Some(true) {
                    return Err(LegacyImportError::new(format!(
                        "merge_report.md claims {task_id} was merged but VCS does not prove its ancestry"
                    )));
                }
                task.phase = TaskPhase::Merged;
                task.review_sha = Some(sha);
                state.integration.merged_tasks.insert(task_id);
            }
            crate::contract::MergeOutcome::Quarantined { reason } => {
                if observation.integrated_into_active != Some(false) {
                    return Err(LegacyImportError::new(format!(
                        "merge_report.md claims {task_id} was quarantined but VCS ancestry is ambiguous"
                    )));
                }
                task.phase = TaskPhase::Conflict;
                task.reason = Some(reason);
                task.imported_recovery_intent = Some(ImportedRecoveryIntent::ReturnConflictToQueue);
            }
        }
    }
    state.phase = if state.integration.merged_tasks.is_empty() {
        // Legacy Phase 5 is skipped when every ready task was quarantined. Keep the existing
        // report as audit evidence, then let Phase 0 perform its normal journal/return/cleanup
        // ledger instead of trying to publish an empty integration.
        Phase::Cleaning
    } else {
        Phase::Publishing
    };
    // Phase 0.4 restores a missing registered `_integration` worktree from its durable branch
    // before it resumes the full-review/publication boundary.  Keep that restoration as the
    // native runtime's idempotent `PrepareIntegrationWorkspace` effect instead of pretending an
    // absent path is already usable or importing a material branch only when its checkout
    // survived the crash.
    state.integration.workspace_prepared = integration.workspace_present;
    state.integration.imported_workspace_restore_pending = !integration.workspace_present;
    state.integration.integration_head = Some(integration_head.to_string());
    Ok(state)
}

/// Adopt the legacy Phase-5 entry boundary after a complete merger report and the initial
/// `integration_state.md` have both been persisted, but before native runtime ownership existed.
/// The legacy `Ревью-SHA` identifies a previous review coordinate, not a publication proof: the
/// native runtime deliberately discards it and runs a fresh full integration review against the
/// actual durable integration tip.  The persisted F-cycle counter is retained so the legacy
/// batch cannot silently receive additional repair attempts beyond its configured limit.
pub fn import_reviewing_integration_cohort(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    plan: &RecoveryPlan,
) -> Result<ProcessorState, LegacyImportError> {
    if !matches!(snapshot.integration.state, IntegrationState::InProgress) {
        return Err(LegacyImportError::new(
            "reviewing integration import requires integration_state.md",
        ));
    }
    let review_sha = snapshot
        .integration
        .review_sha
        .as_deref()
        .filter(|value| valid_ref(value))
        .ok_or_else(|| {
            LegacyImportError::new(
                "reviewing integration import requires a valid persisted review SHA",
            )
        })?;
    let f_cycles = snapshot
        .integration
        .f_cycles
        .filter(|cycles| *cycles > 0)
        .ok_or_else(|| {
            LegacyImportError::new(
                "reviewing integration import requires a positive persisted F-cycle coordinate",
            )
        })?;

    // `plan_recovery` uses the integration VCS/reporter observations for its publication action;
    // changing the Markdown marker to the pre-review projection must not change its exact action
    // set. Prove that before delegating the shared, much stricter report/ancestry validation.
    let mut pre_review_snapshot = snapshot.clone();
    pre_review_snapshot.integration.state = IntegrationState::None;
    pre_review_snapshot.integration.review_sha = None;
    pre_review_snapshot.integration.f_cycles = None;
    let pre_review_plan = plan_recovery(&pre_review_snapshot, inventory);
    if &pre_review_plan != plan {
        return Err(LegacyImportError::new(
            "integration review marker changes the expected legacy recovery plan",
        ));
    }
    let mut state =
        import_reported_integration_cohort(&pre_review_snapshot, inventory, &pre_review_plan)?;
    state.integration.f_cycles = f_cycles;
    // `review_sha` is intentionally validated but not copied: a stale/malformed legacy review
    // artifact can never authorize publication and the next native effect is a full review.
    let _ = review_sha;
    Ok(state)
}

/// Adopt the narrow post-publication boundary before legacy Phase 6 has archived any task.
///
/// A local publication observation is enough only for `PUSH:false`: the VCS adapter proves that
/// the integration branch is already an ancestor of the configured main/bookmark.  The importer
/// still treats the merger report and every task ancestry observation as authority for which
/// tasks were published; publication itself must never be inferred from Markdown labels alone.
/// Once imported, Phase 0 rebuilds the native cleanup ledger (journal, archive/requeue, and
/// physical cleanup) before it can transition the cohort to idle.
pub fn import_published_accounting_cohort(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    plan: &RecoveryPlan,
    publication_pushed: bool,
    ci_disposition: CiDisposition,
) -> Result<ProcessorState, LegacyImportError> {
    if !publication_pushed && ci_disposition != CiDisposition::Disabled {
        return Err(LegacyImportError::new(
            "local published accounting import requires disabled CI disposition",
        ));
    }
    if !matches!(snapshot.integration.state, IntegrationState::InProgress) {
        return Err(LegacyImportError::new(
            "published accounting import requires integration_state.md",
        ));
    }
    let batch = snapshot
        .batch
        .as_ref()
        .ok_or_else(|| LegacyImportError::new("published accounting import requires batch.md"))?;
    let batch_id = batch
        .batch_id
        .as_deref()
        .filter(|value| valid_batch_id(value))
        .ok_or_else(|| LegacyImportError::new("batch.md has no valid batch id"))?;
    let integration = inventory.integration.as_ref().ok_or_else(|| {
        LegacyImportError::new("published accounting import has no integration VCS observation")
    })?;
    let published_head = integration
        .branch_head
        .as_deref()
        .filter(|head| valid_ref(head))
        .ok_or_else(|| {
            LegacyImportError::new("published accounting import has no durable integration tip")
        })?;
    if !integration.branch_exists
        || (integration.workspace_present && integration.workspace_clean != Some(true))
        || !integration.merge_report_present
        || !matches!(integration.publication, PublicationObservation::Published)
    {
        return Err(LegacyImportError::new(
            "published accounting import requires a clean-or-absent, report-backed, proven-published integration workspace",
        ));
    }

    let batch_ids: BTreeSet<&str> = batch.tasks.iter().map(|task| task.id.as_str()).collect();
    if batch_ids.is_empty() {
        return Err(LegacyImportError::new(
            "published accounting import requires at least one batch task",
        ));
    }
    let descriptors = unique_by_id(
        snapshot
            .descriptors
            .iter()
            .map(|descriptor| (descriptor.id.as_str(), descriptor)),
    );
    let queue = unique_by_id(
        snapshot
            .queue
            .iter()
            .map(|entry| (entry.id.as_str(), entry)),
    );
    if descriptors.len() != batch_ids.len()
        || descriptors
            .keys()
            .any(|task_id| !batch_ids.contains(task_id))
    {
        return Err(LegacyImportError::new(
            "published accounting import requires descriptors to match the active batch exactly",
        ));
    }

    let mut published = BTreeSet::new();
    let mut already_done = BTreeSet::new();
    let mut quarantined = BTreeSet::new();
    let mut terminalized_quarantines = BTreeSet::new();
    for (task_id, descriptor) in &descriptors {
        match descriptor.state {
            Some(TaskState::Published)
                if matches!(
                    queue.get(*task_id).and_then(|entry| entry.state),
                    Some(TaskState::Working | TaskState::Published)
                ) =>
            {
                published.insert(*task_id);
            }
            Some(TaskState::Published) => {
                return Err(LegacyImportError::new(format!(
                    "published accounting import requires published task {task_id} to agree in its queue row"
                )));
            }
            Some(TaskState::Done)
                if matches!(
                    queue.get(*task_id).and_then(|entry| entry.state),
                    None | Some(TaskState::Working | TaskState::Published | TaskState::Done)
                ) =>
            {
                already_done.insert(*task_id);
            }
            Some(TaskState::Done) => {
                return Err(LegacyImportError::new(format!(
                    "published accounting import requires done task {task_id} to have no queue row or a replay-safe terminal queue coordinate"
                )));
            }
            Some(TaskState::Conflict)
                if matches!(
                    queue.get(*task_id).and_then(|entry| entry.state),
                    Some(TaskState::Working | TaskState::Ready | TaskState::Conflict)
                ) =>
            {
                quarantined.insert(*task_id);
            }
            Some(TaskState::Conflict)
                if queue.get(*task_id).and_then(|entry| entry.state)
                    == Some(TaskState::Escalated) =>
            {
                quarantined.insert(*task_id);
                terminalized_quarantines.insert(*task_id);
            }
            Some(TaskState::Conflict) => {
                return Err(LegacyImportError::new(format!(
                    "published accounting import requires conflict task {task_id} to be captured, ready, conflict, or terminally escalated in its queue row"
                )));
            }
            Some(TaskState::Escalated)
                if queue.get(*task_id).and_then(|entry| entry.state)
                    == Some(TaskState::Escalated) => {}
            Some(TaskState::Escalated) => {
                return Err(LegacyImportError::new(format!(
                    "published accounting import requires terminally escalated task {task_id} to agree in its queue row"
                )));
            }
            other => {
                return Err(LegacyImportError::new(format!(
                    "published accounting import requires every batch descriptor to be published, done, conflict, or escalated; {task_id} is {other:?}"
                )));
            }
        }
    }
    if published.is_empty() && already_done.is_empty() {
        return Err(LegacyImportError::new(
            "published accounting import requires at least one published or done task",
        ));
    }

    let report_lines = integration.merge_report_lines.as_ref().ok_or_else(|| {
        LegacyImportError::new("published accounting import requires merge_report.md contents")
    })?;
    if report_lines.len() != published.len() + already_done.len() + quarantined.len() {
        return Err(LegacyImportError::new(
            "published accounting import requires exactly one merger result per terminal task",
        ));
    }
    let mut seen = BTreeSet::new();
    for line in report_lines {
        if !batch_ids.contains(line.id.as_str()) || !seen.insert(line.id.as_str()) {
            return Err(LegacyImportError::new(
                "published accounting import has an unknown or duplicate merger result",
            ));
        }
        match (
            &line.outcome,
            published.contains(line.id.as_str()) || already_done.contains(line.id.as_str()),
            quarantined.contains(line.id.as_str()),
        ) {
            (crate::contract::MergeOutcome::Merged { sha, .. }, true, false) if valid_ref(sha) => {}
            (crate::contract::MergeOutcome::Quarantined { reason }, false, true)
                if !reason.trim().is_empty() => {}
            _ => {
                return Err(LegacyImportError::new(format!(
                    "published accounting import result for {} disagrees with its terminal descriptor",
                    line.id
                )));
            }
        }
    }

    let planned_accounts: BTreeSet<&str> = plan
        .actions
        .iter()
        .filter_map(|action| match action {
            RecoveryAction::AccountPublished { task_id }
            | RecoveryAction::AccountDone { task_id } => Some(task_id.as_str()),
            _ => None,
        })
        .collect();
    let planned_requeues: BTreeSet<&str> = plan
        .actions
        .iter()
        .filter_map(|action| match action {
            RecoveryAction::RequeueConflict { task_id, .. } => Some(task_id.as_str()),
            _ => None,
        })
        .collect();
    let has_accounting_continuation = plan.actions.iter().any(|action| {
        matches!(
            action,
            RecoveryAction::ContinueIntegration {
                batch_id: planned_batch_id,
                point: IntegrationResumePoint::Accounting,
            } if planned_batch_id == batch_id
        )
    });
    let requeues: BTreeSet<&str> = quarantined
        .difference(&terminalized_quarantines)
        .copied()
        .collect();
    if plan.is_blocked()
        || !matches!(plan.disposition, RecoveryDisposition::Cleaning)
        || plan.actions.len() != published.len() + already_done.len() + requeues.len() + 1
        || planned_accounts
            != published
                .union(&already_done)
                .copied()
                .collect::<BTreeSet<_>>()
        || planned_requeues != requeues
        || !has_accounting_continuation
    {
        return Err(LegacyImportError::new(
            "published accounting import requires only exact account/requeue recovery actions",
        ));
    }

    // Reuse the stricter merger-report importer after projecting the durable publication labels
    // back to their immediate pre-publication state. The projection is in-memory only; it proves
    // that every native `Published` task descends from a report-backed `Merged` task instead of
    // trusting a label or the local main ancestry by itself.
    let mut pre_publication_snapshot = snapshot.clone();
    pre_publication_snapshot.integration.state = IntegrationState::None;
    pre_publication_snapshot.integration.review_sha = None;
    pre_publication_snapshot.integration.f_cycles = None;
    for descriptor in &mut pre_publication_snapshot.descriptors {
        if published.contains(descriptor.id.as_str())
            || already_done.contains(descriptor.id.as_str())
        {
            descriptor.state = Some(TaskState::Merged);
            descriptor.status_literal = Some(TaskState::Merged.as_str().into());
        } else if terminalized_quarantines.contains(descriptor.id.as_str()) {
            descriptor.state = Some(TaskState::Escalated);
            descriptor.status_literal = Some(TaskState::Escalated.as_str().into());
        }
    }
    for entry in &mut pre_publication_snapshot.queue {
        if published.contains(entry.id.as_str()) || already_done.contains(entry.id.as_str()) {
            entry.state = Some(TaskState::Merged);
            entry.status_literal = TaskState::Merged.as_str().into();
        }
    }
    for task_id in &already_done {
        if !pre_publication_snapshot
            .queue
            .iter()
            .any(|entry| entry.id == *task_id)
        {
            pre_publication_snapshot
                .queue
                .push(crate::state::QueueEntry {
                    id: (*task_id).to_string(),
                    title: (*task_id).to_string(),
                    state: Some(TaskState::Merged),
                    status_literal: TaskState::Merged.as_str().into(),
                    attempt: None,
                    quarantine: None,
                    escalation_reason: None,
                    prerequisites: Vec::new(),
                    delivery_target: crate::state::DeliveryTarget::Current,
                });
        }
    }
    let mut pre_publication_inventory = inventory.clone();
    let projected_integration = pre_publication_inventory
        .integration
        .as_mut()
        .expect("checked integration observation");
    projected_integration.publication = PublicationObservation::NotPublished;
    // After a local fast-forward the configured publication ref has advanced to the same tip as
    // integration, so the ordinary `base..integration` range is empty. At this boundary that
    // range no longer says anything about whether the cohort was material: the exact report and
    // typed per-task ancestry checks below do. Mark only the in-memory pre-publication projection
    // as material so the shared importer can apply those stronger proofs without confusing a
    // moving publication ref for the immutable batch base.
    projected_integration.commits_after_base = true;
    let pre_publication_plan = plan_recovery(&pre_publication_snapshot, &pre_publication_inventory);
    let mut state = import_reported_integration_cohort(
        &pre_publication_snapshot,
        &pre_publication_inventory,
        &pre_publication_plan,
    )?;
    for task_id in published.union(&already_done) {
        let task = state.tasks.get_mut(*task_id).ok_or_else(|| {
            LegacyImportError::new(format!("imported state has no published task {task_id}"))
        })?;
        if !matches!(task.phase, TaskPhase::Merged) {
            return Err(LegacyImportError::new(format!(
                "published accounting projection did not reconstruct merged task {task_id}"
            )));
        }
        task.phase = if already_done.contains(task_id) {
            TaskPhase::Done
        } else {
            TaskPhase::Published
        };
    }
    state.phase = Phase::Cleaning;
    // Accounting never dispatches an integration leaf, so a missing old workspace is simply
    // cleaned as absent rather than recreated.
    state.integration.imported_workspace_restore_pending = false;
    state.integration.published_head = Some(published_head.to_string());
    // Reaching the legacy `published` accounting boundary proves that the publication route and
    // its publication route was completed before Phase 6 began. Preserve the caller's
    // policy-derived CI classification instead of importing an internally impossible native
    // cleanup state. Required remote CI is marked confirmed only as migration evidence: the
    // native archival preflight still re-reads policy and re-confirms the exact published head.
    state.integration.publication_pushed = Some(publication_pushed);
    state.integration.ci_disposition = Some(ci_disposition);
    if published.is_empty() && !already_done.is_empty() {
        // Every surviving task already crossed the guarded publication/CI boundary. Do not
        // re-run remote CI merely to repair its archive projection and remove stale artifacts.
        state.integration.archive_ci_gate = Some(crate::processor::ArchiveCiGate::Skipped);
    }
    Ok(state)
}

fn require_exact_workspace_action(
    plan: &RecoveryPlan,
    task_id: &str,
    branch: &str,
    create_branch: bool,
) -> Result<(), LegacyImportError> {
    let matches = plan
        .actions
        .iter()
        .filter(|action| match action {
            RecoveryAction::EnsureTaskWorkspace {
                task_id: planned_task_id,
                branch: planned_branch,
                create_branch: planned_create_branch,
            } => {
                planned_task_id == task_id
                    && planned_branch == branch
                    && *planned_create_branch == create_branch
            }
            _ => false,
        })
        .count();
    if matches == 1 {
        Ok(())
    } else {
        Err(LegacyImportError::new(format!(
            "active legacy import requires exactly one matching workspace recovery action for {task_id}"
        )))
    }
}

fn require_exact_resume_action(
    plan: &RecoveryPlan,
    task_id: &str,
    point: &TaskResumePoint,
) -> Result<(), LegacyImportError> {
    let matches = plan
        .actions
        .iter()
        .filter(|action| match action {
            RecoveryAction::ResumeTask {
                task_id: planned_task_id,
                point: planned_point,
            } => planned_task_id == task_id && planned_point == point,
            _ => false,
        })
        .count();
    if matches == 1 {
        Ok(())
    } else {
        Err(LegacyImportError::new(format!(
            "active legacy import requires exactly one matching resume action for {task_id}"
        )))
    }
}

fn close_reason_from_legacy(value: &str) -> Option<CloseReasonWire> {
    match value {
        "COHORT_SIZE" => Some(CloseReasonWire::CohortSize),
        "COHORT_MAX_AGE" => Some(CloseReasonWire::CohortMaxAge),
        "COHORT_TOKEN_BUDGET" => Some(CloseReasonWire::CohortTokenBudget),
        "очередь-пуста" => Some(CloseReasonWire::QueueEmpty),
        "только-конфликты-с-готовыми" => {
            Some(CloseReasonWire::OnlyConflictsWithReady)
        }
        _ => None,
    }
}

fn normalize_path(value: &str) -> String {
    value.replace('\\', "/")
}

fn valid_batch_id(value: &str) -> bool {
    value
        .strip_prefix("B-")
        .is_some_and(|suffix| !suffix.is_empty() && !suffix.chars().any(char::is_whitespace))
        && !value.starts_with('-')
        && !value.contains('\0')
}

fn valid_ref(value: &str) -> bool {
    !value.trim().is_empty() && !value.starts_with('-') && !value.contains('\0')
}

/// Produce a fail-closed recovery plan from one stable control-plane snapshot and typed VCS
/// observations.  The function performs no filesystem/VCS/process I/O.
pub fn plan_recovery(snapshot: &Snapshot, inventory: &RecoveryInventory) -> RecoveryPlan {
    let mut actions = Vec::new();
    let mut blockers = Vec::new();
    validate_snapshot_identity(snapshot, &mut blockers);

    let queue_by_id = unique_by_id(
        snapshot
            .queue
            .iter()
            .map(|entry| (entry.id.as_str(), entry)),
    );
    let descriptors = unique_by_id(
        snapshot
            .descriptors
            .iter()
            .map(|descriptor| (descriptor.id.as_str(), descriptor)),
    );
    let batch_tasks = snapshot
        .batch
        .as_ref()
        .map(|batch| unique_by_id(batch.tasks.iter().map(|task| (task.id.as_str(), task))));

    for entry in &snapshot.queue {
        let Some(state) = entry.state else {
            blockers.push(format!("queue task {} has an unknown status", entry.id));
            continue;
        };
        if descriptors.contains_key(entry.id.as_str()) {
            continue;
        }
        if matches!(
            state,
            TaskState::Working | TaskState::InReview | TaskState::Ready | TaskState::Merged
        ) {
            actions.push(RecoveryAction::ReturnOrphanedQueue {
                task_id: entry.id.clone(),
                attempt: entry.attempt,
            });
        }
    }

    for descriptor in &snapshot.descriptors {
        reconcile_descriptor(
            descriptor,
            queue_by_id.get(descriptor.id.as_str()).copied(),
            snapshot,
            batch_tasks.as_ref(),
            inventory,
            &mut actions,
            &mut blockers,
        );
    }

    let disposition = if blockers.is_empty() {
        reconcile_integration(snapshot, inventory, &mut actions, &mut blockers)
    } else {
        RecoveryDisposition::Blocked
    };

    RecoveryPlan {
        actions,
        disposition: if blockers.is_empty() {
            disposition
        } else {
            RecoveryDisposition::Blocked
        },
        blockers,
    }
}

fn reconcile_descriptor(
    descriptor: &Descriptor,
    queue: Option<&crate::state::QueueEntry>,
    snapshot: &Snapshot,
    batch_tasks: Option<&BTreeMap<&str, &crate::state::BatchTask>>,
    inventory: &RecoveryInventory,
    actions: &mut Vec<RecoveryAction>,
    blockers: &mut Vec<String>,
) {
    let Some(state) = descriptor.state else {
        blockers.push(format!(
            "descriptor {} has no recognized status",
            descriptor.id
        ));
        return;
    };
    let queue_state = queue.and_then(|entry| entry.state);

    match state {
        TaskState::Working | TaskState::InReview => {
            let Some(batch) = snapshot.batch.as_ref() else {
                if matches!(queue_state, Some(TaskState::NotStarted) | None) {
                    actions.push(RecoveryAction::RemoveUncapturedDescriptor {
                        task_id: descriptor.id.clone(),
                        reason: "live descriptor exists without batch manifest".into(),
                    });
                } else {
                    blockers.push(format!(
                        "descriptor {} is live without batch manifest but queue is {:?}",
                        descriptor.id, queue_state
                    ));
                }
                return;
            };
            let Some(batch_id) = batch.batch_id.as_deref() else {
                blockers.push("batch manifest has no batch id".into());
                return;
            };
            let Some(batch_base) = batch.base.as_deref() else {
                blockers.push(format!("batch {batch_id} has no immutable base"));
                return;
            };
            if descriptor
                .batch_id
                .as_deref()
                .is_some_and(|id| id != batch_id)
            {
                blockers.push(format!(
                    "descriptor {} belongs to {:?}, not active batch {batch_id}",
                    descriptor.id, descriptor.batch_id
                ));
                return;
            }
            let Some(task) = batch_tasks.and_then(|tasks| tasks.get(descriptor.id.as_str())) else {
                actions.push(RecoveryAction::RemoveUncapturedDescriptor {
                    task_id: descriptor.id.clone(),
                    reason: "descriptor is absent from active batch manifest".into(),
                });
                return;
            };
            let (branch, worktree) = task_coordinates(descriptor, task, &descriptor.id, blockers);
            let Some((branch, worktree)) = branch.zip(worktree) else {
                return;
            };
            restore_queue_capture_if_lost(
                queue, descriptor, batch_id, &branch, &worktree, actions, blockers,
            );
            let Some(observation) = inventory.tasks.get(&descriptor.id) else {
                blockers.push(format!(
                    "no VCS observation for active task {} at base {batch_base}",
                    descriptor.id
                ));
                return;
            };
            if !observation.branch_exists || !observation.workspace_present {
                actions.push(RecoveryAction::EnsureTaskWorkspace {
                    task_id: descriptor.id.clone(),
                    branch,
                    create_branch: !observation.branch_exists,
                });
            }
            let point = match state {
                TaskState::Working if observation.commits_after_base => {
                    TaskResumePoint::ImplementationWithCommittedWork
                }
                TaskState::Working => TaskResumePoint::Implementation,
                TaskState::InReview if observation.commits_after_base => TaskResumePoint::Review {
                    review_sha: descriptor.review_sha.clone(),
                    last_implementation_author: descriptor.implementation_authors.last().cloned(),
                },
                // An `in-review` label without a durable task commit cannot be made true by
                // replaying a reviewer.  Stop for inspection instead of reviewing BASE.
                TaskState::InReview => {
                    blockers.push(format!(
                        "task {} is in review but its branch has no commit after the batch base",
                        descriptor.id
                    ));
                    return;
                }
                _ => unreachable!("outer match restricts task state"),
            };
            actions.push(RecoveryAction::ResumeTask {
                task_id: descriptor.id.clone(),
                point,
            });
        }
        TaskState::Ready => {
            if !belongs_to_active_batch(descriptor, snapshot, batch_tasks, blockers) {
                return;
            }
            if matches!(queue_state, Some(TaskState::NotStarted) | None) {
                let Some(batch_id) = snapshot
                    .batch
                    .as_ref()
                    .and_then(|batch| batch.batch_id.as_deref())
                else {
                    return;
                };
                let branch = descriptor
                    .branch
                    .clone()
                    .unwrap_or_else(|| format!("task/{}", descriptor.id));
                let worktree = descriptor
                    .worktree
                    .clone()
                    .unwrap_or_else(|| format!(".work/worktrees/{}", descriptor.id));
                restore_queue_capture_if_lost(
                    queue, descriptor, batch_id, &branch, &worktree, actions, blockers,
                );
            }
        }
        TaskState::Conflict if queue_state == Some(TaskState::Escalated) => {
            // Phase 6 may have already run the bounded quarantine transaction and exhausted its
            // budget before the crash.  The descriptor is still `conflict` until its physical
            // cleanup, but the queue's terminal escalation is authoritative: a second return
            // would incorrectly revive the task and increment its counter again.
        }
        TaskState::Conflict => actions.push(RecoveryAction::RequeueConflict {
            task_id: descriptor.id.clone(),
            previous_attempt: queue.and_then(|entry| entry.attempt),
        }),
        TaskState::Published => {
            if belongs_to_active_batch(descriptor, snapshot, batch_tasks, blockers) {
                actions.push(RecoveryAction::AccountPublished {
                    task_id: descriptor.id.clone(),
                });
            }
        }
        TaskState::Done => {
            if !belongs_to_active_batch(descriptor, snapshot, batch_tasks, blockers) {
                return;
            }
            if matches!(
                queue_state,
                None | Some(TaskState::Working | TaskState::Published | TaskState::Done)
            ) {
                actions.push(RecoveryAction::AccountDone {
                    task_id: descriptor.id.clone(),
                });
            } else {
                blockers.push(format!(
                    "done descriptor {} has contradictory queue state {:?}",
                    descriptor.id, queue_state
                ));
            }
        }
        TaskState::Merged => {
            belongs_to_active_batch(descriptor, snapshot, batch_tasks, blockers);
        }
        TaskState::NotStarted => actions.push(RecoveryAction::RemoveUncapturedDescriptor {
            task_id: descriptor.id.clone(),
            reason: "descriptor has not-started state and must not reserve a task".into(),
        }),
        TaskState::Escalated => {}
    }
}

fn reconcile_integration(
    snapshot: &Snapshot,
    inventory: &RecoveryInventory,
    actions: &mut Vec<RecoveryAction>,
    blockers: &mut Vec<String>,
) -> RecoveryDisposition {
    let Some(batch) = snapshot.batch.as_ref() else {
        return if snapshot.descriptors.iter().any(|descriptor| {
            matches!(
                descriptor.state,
                Some(TaskState::Working | TaskState::InReview)
            )
        }) {
            blockers.push("live descriptors exist without an active batch manifest".into());
            RecoveryDisposition::Blocked
        } else {
            RecoveryDisposition::Idle
        };
    };
    let Some(batch_id) = batch.batch_id.as_deref() else {
        blockers.push("batch manifest has no batch id".into());
        return RecoveryDisposition::Blocked;
    };
    let has_merged = snapshot
        .descriptors
        .iter()
        .any(|descriptor| matches!(descriptor.state, Some(TaskState::Merged)));
    let has_live = snapshot.descriptors.iter().any(|descriptor| {
        matches!(
            descriptor.state,
            Some(TaskState::Working | TaskState::InReview)
        )
    });

    let Some(integration) = inventory.integration.as_ref() else {
        return if has_merged {
            blockers.push(format!(
                "batch {batch_id} has merged tasks but no integration VCS observation"
            ));
            RecoveryDisposition::Blocked
        } else if has_live {
            RecoveryDisposition::Rolling
        } else {
            RecoveryDisposition::Joining
        };
    };
    if !integration.branch_exists {
        return if has_live {
            RecoveryDisposition::Rolling
        } else if has_merged {
            blockers.push(format!(
                "batch {batch_id} has merged descriptors but integration branch is absent"
            ));
            RecoveryDisposition::Blocked
        } else {
            RecoveryDisposition::Joining
        };
    };
    if integration.workspace_present && integration.workspace_clean != Some(true) {
        blockers.push(format!(
            "integration workspace for batch {batch_id} is dirty or conflicted"
        ));
        return RecoveryDisposition::Blocked;
    }
    match integration.publication {
        PublicationObservation::Published => {
            actions.push(RecoveryAction::ContinueIntegration {
                batch_id: batch_id.to_string(),
                point: IntegrationResumePoint::Accounting,
            });
            RecoveryDisposition::Cleaning
        }
        PublicationObservation::NotPublished if integration.merge_report_present => {
            actions.push(RecoveryAction::ContinueIntegration {
                batch_id: batch_id.to_string(),
                point: IntegrationResumePoint::Publish,
            });
            RecoveryDisposition::Publishing
        }
        PublicationObservation::NotPublished => {
            actions.push(RecoveryAction::ContinueIntegration {
                batch_id: batch_id.to_string(),
                point: IntegrationResumePoint::Merge,
            });
            RecoveryDisposition::Joining
        }
        PublicationObservation::Unknown => {
            blockers.push(format!(
                "publication boundary for integration/{batch_id} is unknown"
            ));
            RecoveryDisposition::Blocked
        }
    }
}

fn belongs_to_active_batch(
    descriptor: &Descriptor,
    snapshot: &Snapshot,
    batch_tasks: Option<&BTreeMap<&str, &crate::state::BatchTask>>,
    blockers: &mut Vec<String>,
) -> bool {
    let Some(batch) = snapshot.batch.as_ref() else {
        blockers.push(format!(
            "descriptor {} requires an active batch manifest",
            descriptor.id
        ));
        return false;
    };
    let Some(batch_id) = batch.batch_id.as_deref() else {
        blockers.push("batch manifest has no batch id".into());
        return false;
    };
    if descriptor
        .batch_id
        .as_deref()
        .is_some_and(|id| id != batch_id)
    {
        blockers.push(format!(
            "descriptor {} belongs to {:?}, not active batch {batch_id}",
            descriptor.id, descriptor.batch_id
        ));
        return false;
    }
    if !batch_tasks.is_some_and(|tasks| tasks.contains_key(descriptor.id.as_str())) {
        blockers.push(format!(
            "descriptor {} is absent from active batch {batch_id}",
            descriptor.id
        ));
        return false;
    }
    true
}

fn restore_queue_capture_if_lost(
    queue: Option<&crate::state::QueueEntry>,
    descriptor: &Descriptor,
    batch_id: &str,
    branch: &str,
    worktree: &str,
    actions: &mut Vec<RecoveryAction>,
    blockers: &mut Vec<String>,
) {
    // A legacy re-capture intentionally retains its attempt counter. If the capture label was
    // lost while that counter remains, this may instead be the stale descriptor left after a
    // completed quarantine return. Do not overwrite the queue or consume an extra retry; the
    // operator must resolve the contradictory pair.
    if queue.is_some_and(|entry| {
        entry.attempt.is_some() && matches!(entry.state, Some(TaskState::NotStarted) | None)
    }) {
        blockers.push(format!(
            "descriptor {} is live while its queue capture is absent and carries a quarantine attempt",
            descriptor.id
        ));
        return;
    }
    match queue.and_then(|entry| entry.state) {
        Some(TaskState::NotStarted) | None => actions.push(RecoveryAction::RestoreQueueCapture {
            task_id: descriptor.id.clone(),
            batch_id: batch_id.to_string(),
            branch: branch.to_string(),
            worktree: worktree.to_string(),
        }),
        Some(TaskState::Conflict | TaskState::Escalated) => {}
        Some(_) => {}
    }
}

fn task_coordinates(
    descriptor: &Descriptor,
    batch_task: &crate::state::BatchTask,
    id: &str,
    blockers: &mut Vec<String>,
) -> (Option<String>, Option<String>) {
    let conventional_branch = format!("task/{id}");
    let branch = descriptor
        .branch
        .clone()
        .or_else(|| batch_task.branch.clone())
        .unwrap_or_else(|| conventional_branch.clone());
    if branch != conventional_branch {
        blockers.push(format!(
            "task {id} records branch {branch:?}, expected {conventional_branch:?}"
        ));
        return (None, None);
    }
    let conventional_worktree = format!(".work/worktrees/{id}");
    let worktree = descriptor
        .worktree
        .clone()
        .or_else(|| batch_task.worktree.clone())
        .unwrap_or_else(|| conventional_worktree.clone());
    if worktree.replace('\\', "/") != conventional_worktree {
        blockers.push(format!(
            "task {id} records worktree {worktree:?}, expected {conventional_worktree:?}"
        ));
        return (None, None);
    }
    (Some(branch), Some(worktree))
}

fn validate_snapshot_identity(snapshot: &Snapshot, blockers: &mut Vec<String>) {
    validate_unique(
        "queue",
        snapshot.queue.iter().map(|entry| entry.id.as_str()),
        blockers,
    );
    validate_unique(
        "descriptor",
        snapshot
            .descriptors
            .iter()
            .map(|descriptor| descriptor.id.as_str()),
        blockers,
    );
    if let Some(batch) = &snapshot.batch {
        validate_unique(
            "batch",
            batch.tasks.iter().map(|task| task.id.as_str()),
            blockers,
        );
    }
}

fn validate_unique<'a>(
    kind: &str,
    ids: impl IntoIterator<Item = &'a str>,
    blockers: &mut Vec<String>,
) {
    let mut seen = BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            blockers.push(format!("duplicate {kind} record for {id}"));
        }
    }
}

fn unique_by_id<'a, T>(
    entries: impl IntoIterator<Item = (&'a str, &'a T)>,
) -> BTreeMap<&'a str, &'a T> {
    let mut out = BTreeMap::new();
    for (id, value) in entries {
        out.entry(id).or_insert(value);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::state::{
        BatchState, BatchTask, CohortAdmission, CohortState, DeliveryTarget, IntegrationSnapshot,
        QueueEntry,
    };

    fn queue(id: &str, state: TaskState, attempt: Option<u32>) -> QueueEntry {
        QueueEntry {
            id: id.into(),
            title: id.into(),
            state: Some(state),
            status_literal: state.as_str().into(),
            attempt,
            quarantine: None,
            escalation_reason: None,
            prerequisites: Vec::new(),
            delivery_target: DeliveryTarget::Current,
        }
    }

    fn descriptor(id: &str, state: TaskState) -> Descriptor {
        Descriptor {
            id: id.into(),
            state: Some(state),
            status_literal: Some(state.as_str().into()),
            prerequisites: Vec::new(),
            conflict_domain: Some(vec!["engine/**".into()]),
            level: Some(crate::resolvers::Level::Coder),
            risk: None,
            network: None,
            batch_id: Some("B-1".into()),
            branch: Some(format!("task/{id}")),
            worktree: Some(format!(".work/worktrees/{id}")),
            implementation_authors: vec!["coder".into()],
            review_sha: Some("abc".into()),
            review_cycles: Some(1),
        }
    }

    fn snapshot(queue: Vec<QueueEntry>, descriptors: Vec<Descriptor>) -> Snapshot {
        Snapshot {
            work_dir: PathBuf::from(".work"),
            queue,
            descriptors,
            cohort: Some(CohortState {
                batch_id: Some("B-1".into()),
                admission: None,
                admission_literal: None,
                admission_reason: None,
                started_at: None,
                wave: Some(1),
                admitted_total: Some(1),
            }),
            integration: IntegrationSnapshot {
                state: crate::state::IntegrationState::None,
                review_sha: None,
                f_cycles: None,
            },
            batch: Some(BatchState {
                batch_id: Some("B-1".into()),
                base: Some("base".into()),
                integration_branch: Some("integration/B-1".into()),
                tasks: vec![BatchTask {
                    id: "T-1".into(),
                    level: None,
                    branch: Some("task/T-1".into()),
                    worktree: Some(".work/worktrees/T-1".into()),
                    domain: None,
                    wave: Some(1),
                }],
            }),
        }
    }

    fn task_observation(commits_after_base: bool) -> RecoveryInventory {
        RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("task-head".into()),
                    commits_after_base,
                    integrated_into_active: None,
                },
            )]),
            integration: None,
        }
    }

    #[test]
    fn orphaned_active_queue_label_is_returned_without_losing_attempt() {
        let plan = plan_recovery(
            &snapshot(vec![queue("T-1", TaskState::Working, Some(2))], Vec::new()),
            &RecoveryInventory::default(),
        );
        assert_eq!(
            plan.actions,
            vec![RecoveryAction::ReturnOrphanedQueue {
                task_id: "T-1".into(),
                attempt: Some(2),
            }]
        );
        assert_eq!(plan.disposition, RecoveryDisposition::Joining);
    }

    #[test]
    fn committed_in_review_task_reuses_persisted_review_coordinates() {
        let plan = plan_recovery(
            &snapshot(
                vec![queue("T-1", TaskState::InReview, None)],
                vec![descriptor("T-1", TaskState::InReview)],
            ),
            &task_observation(true),
        );
        assert_eq!(
            plan.actions,
            vec![RecoveryAction::ResumeTask {
                task_id: "T-1".into(),
                point: TaskResumePoint::Review {
                    review_sha: Some("abc".into()),
                    last_implementation_author: Some("coder".into()),
                },
            }]
        );
        assert_eq!(plan.disposition, RecoveryDisposition::Rolling);
    }

    #[test]
    fn in_review_without_a_post_base_commit_fails_closed() {
        let plan = plan_recovery(
            &snapshot(
                vec![queue("T-1", TaskState::InReview, None)],
                vec![descriptor("T-1", TaskState::InReview)],
            ),
            &task_observation(false),
        );
        assert!(plan.is_blocked());
        assert_eq!(plan.disposition, RecoveryDisposition::Blocked);
        assert!(plan.blockers[0].contains("no commit after the batch base"));
    }

    #[test]
    fn lost_queue_capture_is_restored_from_the_descriptor_not_guessed() {
        let plan = plan_recovery(
            &snapshot(
                vec![queue("T-1", TaskState::NotStarted, None)],
                vec![descriptor("T-1", TaskState::Working)],
            ),
            &task_observation(false),
        );
        assert!(plan.actions.contains(&RecoveryAction::RestoreQueueCapture {
            task_id: "T-1".into(),
            batch_id: "B-1".into(),
            branch: "task/T-1".into(),
            worktree: ".work/worktrees/T-1".into(),
        }));
        assert!(plan.actions.contains(&RecoveryAction::ResumeTask {
            task_id: "T-1".into(),
            point: TaskResumePoint::Implementation,
        }));
    }

    #[test]
    fn lost_capture_with_a_quarantine_attempt_is_held_without_overwriting_the_queue() {
        let plan = plan_recovery(
            &snapshot(
                vec![queue("T-1", TaskState::NotStarted, Some(2))],
                vec![descriptor("T-1", TaskState::Working)],
            ),
            &task_observation(false),
        );

        assert!(plan.is_blocked());
        assert!(plan.actions.iter().all(|action| !matches!(
            action,
            RecoveryAction::RestoreQueueCapture { task_id, .. } if task_id == "T-1"
        )));
        assert!(plan.blockers.iter().any(|blocker| {
            blocker.contains("queue capture is absent and carries a quarantine attempt")
        }));
    }

    #[test]
    fn published_integration_takes_accounting_path_not_another_review() {
        let inventory = RecoveryInventory {
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("integration-head".into()),
                commits_after_base: true,
                workspace_clean: Some(true),
                merge_report_present: true,
                merge_report_lines: None,
                publication: PublicationObservation::Published,
            }),
            ..RecoveryInventory::default()
        };
        let plan = plan_recovery(
            &snapshot(
                vec![queue("T-1", TaskState::Merged, None)],
                vec![descriptor("T-1", TaskState::Merged)],
            ),
            &inventory,
        );
        assert_eq!(plan.disposition, RecoveryDisposition::Cleaning);
        assert!(plan.actions.contains(&RecoveryAction::ContinueIntegration {
            batch_id: "B-1".into(),
            point: IntegrationResumePoint::Accounting,
        }));
    }

    #[test]
    fn dirty_integration_workspace_blocks_recovery_before_any_continuation() {
        let inventory = RecoveryInventory {
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("integration-head".into()),
                commits_after_base: true,
                workspace_clean: Some(false),
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
            ..RecoveryInventory::default()
        };
        let plan = plan_recovery(
            &snapshot(
                vec![queue("T-1", TaskState::Ready, None)],
                vec![descriptor("T-1", TaskState::Ready)],
            ),
            &inventory,
        );
        assert!(plan.is_blocked());
        assert_eq!(plan.disposition, RecoveryDisposition::Blocked);
        assert!(
            plan.blockers
                .iter()
                .any(|blocker| blocker.contains("integration workspace"))
        );
        assert!(plan.actions.is_empty());
    }

    #[test]
    fn closed_fully_ready_batch_imports_without_replaying_a_leaf() {
        let mut snapshot = snapshot(
            // The descriptor is the lifecycle authority; a normal legacy queue row remains
            // captured after review has promoted the task to ready.
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::Ready)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
            ..RecoveryInventory::default()
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert!(plan.actions.is_empty());
        assert_eq!(plan.disposition, RecoveryDisposition::Joining);

        let imported = import_closed_ready_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Joining);
        assert_eq!(imported.batch.as_ref().unwrap().id, "B-1");
        assert_eq!(
            imported.batch.as_ref().unwrap().admission_closed,
            Some(CloseReasonWire::CohortSize)
        );
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Ready);
        assert_eq!(imported.tasks["T-1"].review_sha.as_deref(), Some("abc"));
        assert!(imported.integration.merged_tasks.is_empty());
    }

    #[test]
    fn closed_ready_retry_imports_with_its_quarantine_attempt_intact() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Ready, Some(2))],
            vec![descriptor("T-1", TaskState::Ready)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
            ..RecoveryInventory::default()
        };

        let plan = plan_recovery(&snapshot, &inventory);
        assert!(plan.actions.is_empty());
        assert_eq!(plan.disposition, RecoveryDisposition::Joining);
        let imported = import_closed_ready_cohort(&snapshot, &inventory, &plan)
            .expect("a ready retry remains a valid pre-join recovery shape");
        assert_eq!(imported.phase, Phase::Joining);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Ready);
        assert_eq!(snapshot.queue[0].attempt, Some(2));
    }

    #[test]
    fn closed_all_escalated_batch_imports_directly_to_cleanup() {
        let mut terminal_queue = queue("T-1", TaskState::Escalated, None);
        terminal_queue.status_literal = "эскалирована · причина=review retry limit".into();
        terminal_queue.escalation_reason = Some("review retry limit".into());
        let mut snapshot = snapshot(
            vec![terminal_queue],
            vec![descriptor("T-1", TaskState::Escalated)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(1),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
            ..RecoveryInventory::default()
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert!(plan.actions.is_empty());
        assert_eq!(plan.disposition, RecoveryDisposition::Joining);

        let imported = import_closed_ready_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Cleaning);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Escalated);
        assert_eq!(
            imported.tasks["T-1"].reason.as_deref(),
            Some("review retry limit")
        );
    }

    #[test]
    fn closed_working_batch_imports_a_missing_workspace_as_a_durable_effect() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::Working)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: false,
                    workspace_present: false,
                    workspace_clean: None,
                    branch_head: None,
                    commits_after_base: false,
                    integrated_into_active: None,
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Rolling);
        assert!(plan.actions.iter().any(|action| matches!(
            action,
            RecoveryAction::EnsureTaskWorkspace { task_id, create_branch: true, .. } if task_id == "T-1"
        )));
        assert!(plan.actions.iter().any(|action| matches!(
            action,
            RecoveryAction::ResumeTask { task_id, point: TaskResumePoint::Implementation } if task_id == "T-1"
        )));

        let imported = import_closed_active_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Rolling);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Capturing);
        assert_eq!(
            imported.tasks["T-1"].imported_recovery_intent,
            Some(ImportedRecoveryIntent::EnsureWorkspace)
        );
    }

    #[test]
    fn active_legacy_capture_with_a_prior_quarantine_attempt_imports_normally() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, Some(2))],
            vec![descriptor("T-1", TaskState::Working)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("task-head".into()),
                    commits_after_base: false,
                    integrated_into_active: None,
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };

        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Rolling);
        let imported = import_closed_active_cohort(&snapshot, &inventory, &plan)
            .expect("a valid active retry keeps its prior queue attempt");
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Implementing);
        assert_eq!(
            imported.tasks["T-1"].imported_recovery_intent,
            Some(ImportedRecoveryIntent::DispatchImplementation)
        );
        assert_eq!(
            snapshot.queue[0].attempt,
            Some(2),
            "the persisted queue coordinate remains the authority for the next return"
        );
    }

    #[test]
    fn missing_legacy_cohort_state_becomes_a_closed_active_cohort_without_top_up() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::Working)],
        );
        // This is the Phase-0.3b crash window: batch.md was published, while its first
        // cohort_state.md write has not occurred yet.
        snapshot.cohort = None;
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("base".into()),
                    commits_after_base: false,
                    integrated_into_active: None,
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Rolling);
        assert!(matches!(
            plan.actions.as_slice(),
            [RecoveryAction::ResumeTask {
                task_id,
                point: TaskResumePoint::Implementation,
            }] if task_id == "T-1"
        ));

        let imported_at = iso_to_epoch("2026-07-26T00:00:00Z").unwrap();
        let projected = synthesize_missing_legacy_cohort_state(&snapshot, imported_at).unwrap();
        assert!(
            snapshot.cohort.is_none(),
            "projection must not alter legacy evidence"
        );
        let cohort = projected.cohort.as_ref().unwrap();
        assert_eq!(cohort.admission, Some(CohortAdmission::Closed));
        assert_eq!(cohort.admission_reason, None);
        assert_eq!(cohort.started_at.as_deref(), Some("2026-07-26T00:00:00Z"));
        assert_eq!(cohort.wave, Some(1));
        assert_eq!(cohort.admitted_total, Some(1));

        let imported = import_active_cohort(&projected, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Rolling);
        assert_eq!(
            imported.batch.as_ref().unwrap().admission_closed,
            Some(CloseReasonWire::LegacyCohortStateAbsent)
        );
        assert_eq!(
            imported.tasks["T-1"].imported_recovery_intent,
            Some(ImportedRecoveryIntent::DispatchImplementation)
        );
    }

    #[test]
    fn open_working_batch_preserves_rolling_admission_for_native_top_up() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::Working)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Open),
            admission_literal: Some("открыт".into()),
            admission_reason: None,
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("base".into()),
                    commits_after_base: false,
                    integrated_into_active: None,
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        let imported = import_active_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Rolling);
        assert_eq!(imported.batch.as_ref().unwrap().admission_closed, None);
        assert_eq!(
            imported.tasks["T-1"].imported_recovery_intent,
            Some(ImportedRecoveryIntent::DispatchImplementation)
        );
    }

    #[test]
    fn phase_zero_recheck_binds_legacy_policy_and_closes_an_open_cohort_at_size() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::Working)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Open),
            admission_literal: Some("открыт".into()),
            admission_reason: None,
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("base".into()),
                    commits_after_base: false,
                    integrated_into_active: None,
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        let mut imported = import_active_cohort(&snapshot, &inventory, &plan).unwrap();
        let config = ProcessorConfig {
            cohort_size: 1,
            cohort_budget_secs: Some(600),
            cohort_token_budget: Some(100),
            events_outbox_enabled: false,
            ..ProcessorConfig::default()
        };

        bind_legacy_safety_snapshot(&mut imported, &config);
        assert!(
            recheck_legacy_open_admission(
                &mut imported,
                &config,
                iso_to_epoch("2026-07-25T12:00:01Z").unwrap(),
                None,
            )
            .unwrap()
        );

        let batch = imported.batch.as_ref().unwrap();
        assert_eq!(batch.admission_closed, Some(CloseReasonWire::CohortSize));
        assert_eq!(batch.cohort_budget_secs, Some(600));
        assert_eq!(batch.cohort_token_budget, Some(100));
        assert!(!batch.events_outbox_enabled);
    }

    #[test]
    fn phase_zero_recheck_closes_open_legacy_admission_when_token_telemetry_is_unavailable() {
        let mut state = ProcessorState {
            schema_version: crate::processor::PROCESSOR_STATE_VERSION,
            phase: Phase::Rolling,
            paused_from: None,
            batch: Some(CohortRuntime {
                id: "B-1".into(),
                base: "base".into(),
                started_at_secs: 100,
                wave: 1,
                admitted_total: 1,
                admission_closed: None,
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: Some(1),
                events_outbox_enabled: true,
            }),
            tasks: BTreeMap::new(),
            integration: IntegrationRuntime::default(),
            blocked_reason: None,
        };
        let config = ProcessorConfig {
            cohort_size: 2,
            cohort_token_budget: Some(100),
            ..ProcessorConfig::default()
        };

        bind_legacy_safety_snapshot(&mut state, &config);
        assert!(
            recheck_legacy_open_admission(
                &mut state,
                &config,
                101,
                Some(LegacyTokenTelemetry::Unavailable),
            )
            .unwrap()
        );
        let batch = state.batch.as_ref().unwrap();
        assert_eq!(
            batch.admission_closed,
            Some(CloseReasonWire::CohortTokenBudget)
        );
        assert_eq!(batch.token_budget_actual_tokens, None);
    }

    #[test]
    fn closed_review_batch_recovers_a_missing_workspace_without_replaying_implementation() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::InReview)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: false,
                    workspace_clean: None,
                    branch_head: Some("task-head".into()),
                    commits_after_base: true,
                    integrated_into_active: None,
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: false,
                workspace_present: false,
                branch_head: None,
                commits_after_base: false,
                workspace_clean: None,
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        let imported = import_closed_active_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Reviewing);
        assert_eq!(
            imported.tasks["T-1"].review_sha.as_deref(),
            Some("task-head")
        );
        assert_eq!(
            imported.tasks["T-1"].imported_recovery_intent,
            Some(ImportedRecoveryIntent::EnsureWorkspaceForReview)
        );
    }

    #[test]
    fn empty_legacy_integration_imports_at_the_native_join_boundary() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Ready, None)],
            vec![descriptor("T-1", TaskState::Ready)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        snapshot.integration.state = IntegrationState::InProgress;
        snapshot.integration.f_cycles = Some(0);
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("abc".into()),
                    commits_after_base: true,
                    integrated_into_active: Some(false),
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("base".into()),
                commits_after_base: false,
                workspace_clean: Some(true),
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Joining);
        assert_eq!(
            plan.actions,
            vec![RecoveryAction::ContinueIntegration {
                batch_id: "B-1".into(),
                point: IntegrationResumePoint::Merge,
            }]
        );

        let imported = import_unreported_integration_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Joining);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Ready);
        assert!(imported.integration.workspace_prepared);
        assert!(imported.integration.integration_head.is_none());
        assert!(imported.integration.merged_tasks.is_empty());
    }

    #[test]
    fn reported_legacy_merge_imports_only_with_proven_task_ancestry() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::Merged)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        // This is the normal legacy crash boundary: Phase 4 wrote the descriptor quarantine,
        // while Phase 6 has not yet returned the still-captured queue row.
        snapshot
            .queue
            .push(queue("T-2", TaskState::Working, Some(2)));
        snapshot
            .descriptors
            .push(descriptor("T-2", TaskState::Conflict));
        let mut escalated_queue = queue("T-3", TaskState::Escalated, None);
        escalated_queue.status_literal = "эскалирована · причина=review retry limit".into();
        escalated_queue.escalation_reason = Some("review retry limit".into());
        snapshot.queue.push(escalated_queue);
        snapshot
            .descriptors
            .push(descriptor("T-3", TaskState::Escalated));
        snapshot.batch.as_mut().unwrap().tasks.push(BatchTask {
            id: "T-2".into(),
            level: Some("coder".into()),
            branch: Some("task/T-2".into()),
            worktree: Some(".work/worktrees/T-2".into()),
            domain: Some("engine/**".into()),
            wave: Some(1),
        });
        snapshot.batch.as_mut().unwrap().tasks.push(BatchTask {
            id: "T-3".into(),
            level: Some("coder".into()),
            branch: Some("task/T-3".into()),
            worktree: Some(".work/worktrees/T-3".into()),
            domain: Some("engine/**".into()),
            wave: Some(1),
        });
        snapshot.cohort.as_mut().unwrap().admitted_total = Some(3);
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([
                (
                    "T-1".into(),
                    TaskRepositoryObservation {
                        branch_exists: true,
                        workspace_present: true,
                        workspace_clean: Some(true),
                        branch_head: Some("task-head".into()),
                        commits_after_base: true,
                        integrated_into_active: Some(true),
                    },
                ),
                (
                    "T-2".into(),
                    TaskRepositoryObservation {
                        branch_exists: true,
                        workspace_present: true,
                        workspace_clean: Some(true),
                        branch_head: Some("task-two-head".into()),
                        commits_after_base: true,
                        integrated_into_active: Some(false),
                    },
                ),
            ]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("integration-head".into()),
                commits_after_base: true,
                workspace_clean: Some(true),
                merge_report_present: true,
                merge_report_lines: Some(vec![
                    crate::contract::MergeLine {
                        id: "T-1".into(),
                        outcome: crate::contract::MergeOutcome::Merged {
                            sha: "merge-head".into(),
                            conflict_resolved: false,
                        },
                    },
                    crate::contract::MergeLine {
                        id: "T-2".into(),
                        outcome: crate::contract::MergeOutcome::Quarantined {
                            reason: "merge conflict".into(),
                        },
                    },
                ]),
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Publishing);
        assert!(plan.actions.contains(&RecoveryAction::ContinueIntegration {
            batch_id: "B-1".into(),
            point: IntegrationResumePoint::Publish,
        }));
        assert!(plan.actions.contains(&RecoveryAction::RequeueConflict {
            task_id: "T-2".into(),
            previous_attempt: Some(2),
        }));

        let imported = import_reported_integration_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Publishing);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Merged);
        assert_eq!(
            imported.integration.integration_head.as_deref(),
            Some("integration-head")
        );
        assert!(imported.integration.merged_tasks.contains("T-1"));
        assert_eq!(imported.tasks["T-2"].phase, TaskPhase::Conflict);
        assert_eq!(
            imported.tasks["T-2"].reason.as_deref(),
            Some("merge conflict")
        );
        assert_eq!(
            imported.tasks["T-2"].imported_recovery_intent,
            Some(ImportedRecoveryIntent::ReturnConflictToQueue)
        );
        assert_eq!(imported.tasks["T-3"].phase, TaskPhase::Escalated);
        assert_eq!(
            imported.tasks["T-3"].reason.as_deref(),
            Some("review retry limit")
        );

        let mut missing_workspace = inventory.clone();
        let integration = missing_workspace.integration.as_mut().unwrap();
        integration.workspace_present = false;
        integration.workspace_clean = None;
        let missing_workspace_plan = plan_recovery(&snapshot, &missing_workspace);
        let imported = import_reported_integration_cohort(
            &snapshot,
            &missing_workspace,
            &missing_workspace_plan,
        )
        .expect("a durable report/branch may recover through a missing checkout");
        assert_eq!(imported.phase, Phase::Publishing);
        assert!(
            !imported.integration.workspace_prepared,
            "the runtime must durably recreate the missing integration workspace before review"
        );
        assert!(
            imported.integration.imported_workspace_restore_pending,
            "only an imported missing workspace may route Phase 0 through reconstruction"
        );

        let mut ambiguous = inventory.clone();
        ambiguous
            .tasks
            .get_mut("T-1")
            .unwrap()
            .integrated_into_active = Some(false);
        assert!(
            import_reported_integration_cohort(
                &snapshot,
                &ambiguous,
                &plan_recovery(&snapshot, &ambiguous),
            )
            .unwrap_err()
            .to_string()
            .contains("does not prove its ancestry")
        );

        let mut reviewing = snapshot.clone();
        reviewing.integration.state = IntegrationState::InProgress;
        reviewing.integration.review_sha = Some("integration-head".into());
        reviewing.integration.f_cycles = Some(3);
        let reviewing_plan = plan_recovery(&reviewing, &inventory);
        let imported =
            import_reviewing_integration_cohort(&reviewing, &inventory, &reviewing_plan).unwrap();
        assert_eq!(imported.phase, Phase::Publishing);
        assert_eq!(imported.integration.f_cycles, 3);
        assert!(
            imported.integration.review_sha.is_none(),
            "the legacy review artifact must not authorize the new publication path"
        );
    }

    #[test]
    fn exhausted_quarantine_is_cleaned_without_a_second_queue_return() {
        let mut exhausted_queue = queue("T-1", TaskState::Escalated, Some(3));
        exhausted_queue.status_literal =
            "эскалирована · причина=карантин повторился 3 раз: merge conflict".into();
        exhausted_queue.escalation_reason =
            Some("карантин повторился 3 раз: merge conflict".into());
        let mut snapshot = snapshot(
            vec![exhausted_queue],
            vec![descriptor("T-1", TaskState::Conflict)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(1),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());

        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("task-head".into()),
                    commits_after_base: true,
                    integrated_into_active: Some(false),
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("integration-head".into()),
                commits_after_base: true,
                workspace_clean: Some(true),
                merge_report_present: true,
                merge_report_lines: Some(vec![crate::contract::MergeLine {
                    id: "T-1".into(),
                    outcome: crate::contract::MergeOutcome::Quarantined {
                        reason: "merge conflict".into(),
                    },
                }]),
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Publishing);
        assert_eq!(
            plan.actions,
            vec![RecoveryAction::ContinueIntegration {
                batch_id: "B-1".into(),
                point: IntegrationResumePoint::Publish,
            }]
        );

        let imported = import_reported_integration_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Cleaning);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Escalated);
        assert_eq!(
            imported.tasks["T-1"].reason.as_deref(),
            Some("карантин повторился 3 раз: merge conflict")
        );
        assert!(imported.integration.merged_tasks.is_empty());
    }

    #[test]
    fn published_legacy_batch_imports_into_native_accounting_with_proven_ancestry() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Working, None)],
            vec![descriptor("T-1", TaskState::Published)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        let mut escalated_queue = queue("T-2", TaskState::Escalated, None);
        escalated_queue.status_literal = "эскалирована · причина=review retry limit".into();
        escalated_queue.escalation_reason = Some("review retry limit".into());
        snapshot.queue.push(escalated_queue);
        snapshot
            .descriptors
            .push(descriptor("T-2", TaskState::Escalated));
        snapshot.batch.as_mut().unwrap().tasks.push(BatchTask {
            id: "T-2".into(),
            level: Some("coder".into()),
            branch: Some("task/T-2".into()),
            worktree: Some(".work/worktrees/T-2".into()),
            domain: Some("engine/**".into()),
            wave: Some(1),
        });
        snapshot.cohort.as_mut().unwrap().admitted_total = Some(2);
        snapshot.integration.state = IntegrationState::InProgress;

        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("task-head".into()),
                    commits_after_base: true,
                    integrated_into_active: Some(true),
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("published-head".into()),
                commits_after_base: true,
                workspace_clean: Some(true),
                merge_report_present: true,
                merge_report_lines: Some(vec![crate::contract::MergeLine {
                    id: "T-1".into(),
                    outcome: crate::contract::MergeOutcome::Merged {
                        sha: "merge-head".into(),
                        conflict_resolved: false,
                    },
                }]),
                publication: PublicationObservation::Published,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Cleaning);
        assert_eq!(
            plan.actions,
            vec![
                RecoveryAction::AccountPublished {
                    task_id: "T-1".into(),
                },
                RecoveryAction::ContinueIntegration {
                    batch_id: "B-1".into(),
                    point: IntegrationResumePoint::Accounting,
                },
            ]
        );

        let imported = import_published_accounting_cohort(
            &snapshot,
            &inventory,
            &plan,
            true,
            CiDisposition::Confirmed,
        )
        .unwrap();
        assert_eq!(imported.phase, Phase::Cleaning);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Published);
        assert!(imported.integration.merged_tasks.contains("T-1"));
        assert_eq!(
            imported.integration.published_head.as_deref(),
            Some("published-head")
        );
        assert_eq!(imported.integration.publication_pushed, Some(true));
        assert_eq!(
            imported.integration.ci_disposition,
            Some(CiDisposition::Confirmed)
        );
        assert_eq!(imported.tasks["T-2"].phase, TaskPhase::Escalated);
        assert_eq!(
            imported.tasks["T-2"].reason.as_deref(),
            Some("review retry limit")
        );

        assert!(
            import_published_accounting_cohort(
                &snapshot,
                &inventory,
                &plan,
                false,
                CiDisposition::Confirmed,
            )
            .unwrap_err()
            .to_string()
            .contains("local published accounting import requires disabled CI")
        );

        let mut missing_workspace = inventory.clone();
        let integration = missing_workspace.integration.as_mut().unwrap();
        integration.workspace_present = false;
        integration.workspace_clean = None;
        let missing_workspace_plan = plan_recovery(&snapshot, &missing_workspace);
        let imported = import_published_accounting_cohort(
            &snapshot,
            &missing_workspace,
            &missing_workspace_plan,
            false,
            CiDisposition::Disabled,
        )
        .expect("publication accounting may clean an already-removed integration checkout");
        assert_eq!(imported.phase, Phase::Cleaning);
        assert!(
            !imported.integration.workspace_prepared,
            "cleanup must not claim a missing integration workspace was restored"
        );
        assert!(
            !imported.integration.imported_workspace_restore_pending,
            "cleanup must not recreate an integration workspace after publication"
        );
        assert_eq!(imported.integration.publication_pushed, Some(false));
        assert_eq!(
            imported.integration.ci_disposition,
            Some(CiDisposition::Disabled)
        );

        let mut stale_queue = snapshot.clone();
        stale_queue.queue[0].state = Some(TaskState::Merged);
        stale_queue.queue[0].status_literal = TaskState::Merged.as_str().into();
        let stale_plan = plan_recovery(&stale_queue, &inventory);
        assert!(
            import_published_accounting_cohort(
                &stale_queue,
                &inventory,
                &stale_plan,
                true,
                CiDisposition::Confirmed,
            )
            .unwrap_err()
            .to_string()
            .contains("published task T-1 to agree")
        );
    }

    #[test]
    fn done_descriptor_with_no_queue_row_resumes_terminal_archive_repair_without_ci_recheck() {
        let mut snapshot = snapshot(Vec::new(), vec![descriptor("T-1", TaskState::Done)]);
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(1),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        snapshot.integration.state = IntegrationState::InProgress;
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("task-head".into()),
                    commits_after_base: true,
                    integrated_into_active: Some(true),
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("published-head".into()),
                commits_after_base: true,
                workspace_clean: Some(true),
                merge_report_present: true,
                merge_report_lines: Some(vec![crate::contract::MergeLine {
                    id: "T-1".into(),
                    outcome: crate::contract::MergeOutcome::Merged {
                        sha: "merge-head".into(),
                        conflict_resolved: false,
                    },
                }]),
                publication: PublicationObservation::Published,
            }),
        };

        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Cleaning);
        assert_eq!(
            plan.actions,
            vec![
                RecoveryAction::AccountDone {
                    task_id: "T-1".into(),
                },
                RecoveryAction::ContinueIntegration {
                    batch_id: "B-1".into(),
                    point: IntegrationResumePoint::Accounting,
                },
            ]
        );
        let imported = import_published_accounting_cohort(
            &snapshot,
            &inventory,
            &plan,
            true,
            CiDisposition::Confirmed,
        )
        .unwrap();

        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Done);
        assert_eq!(
            imported.integration.archive_ci_gate,
            Some(crate::processor::ArchiveCiGate::Skipped),
            "terminal recovery already crossed the CI gate"
        );
        assert!(imported.integration.merged_tasks.contains("T-1"));
    }

    #[test]
    fn unreported_partial_integration_replays_only_proven_batch_ancestry() {
        let mut snapshot = snapshot(
            vec![queue("T-1", TaskState::Ready, None)],
            vec![descriptor("T-1", TaskState::Ready)],
        );
        snapshot.cohort = Some(CohortState {
            batch_id: Some("B-1".into()),
            admission: Some(CohortAdmission::Closed),
            admission_literal: Some("закрыт · причина=COHORT_SIZE".into()),
            admission_reason: Some("COHORT_SIZE".into()),
            started_at: Some("2026-07-25T12:00:00Z".into()),
            wave: Some(2),
            admitted_total: Some(1),
        });
        let batch_task = snapshot.batch.as_mut().unwrap().tasks.first_mut().unwrap();
        batch_task.level = Some("coder".into());
        batch_task.domain = Some("engine/**".into());
        snapshot.integration.state = IntegrationState::None;
        let inventory = RecoveryInventory {
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRepositoryObservation {
                    branch_exists: true,
                    workspace_present: true,
                    workspace_clean: Some(true),
                    branch_head: Some("abc".into()),
                    commits_after_base: true,
                    integrated_into_active: Some(true),
                },
            )]),
            integration: Some(IntegrationRepositoryObservation {
                branch_exists: true,
                workspace_present: true,
                branch_head: Some("integration-head".into()),
                commits_after_base: true,
                workspace_clean: Some(true),
                merge_report_present: false,
                merge_report_lines: None,
                publication: PublicationObservation::NotPublished,
            }),
        };
        let plan = plan_recovery(&snapshot, &inventory);
        assert_eq!(plan.disposition, RecoveryDisposition::Joining);
        assert!(import_closed_ready_cohort(&snapshot, &inventory, &plan).is_err());
        let imported = import_unreported_integration_cohort(&snapshot, &inventory, &plan).unwrap();
        assert_eq!(imported.phase, Phase::Joining);
        assert_eq!(imported.tasks["T-1"].phase, TaskPhase::Ready);
        assert_eq!(
            imported.integration.integration_head.as_deref(),
            Some("integration-head")
        );
        assert!(imported.integration.merged_tasks.is_empty());

        let mut unexplained = inventory.clone();
        unexplained
            .tasks
            .get_mut("T-1")
            .unwrap()
            .integrated_into_active = Some(false);
        let error = import_unreported_integration_cohort(&snapshot, &unexplained, &plan)
            .unwrap_err()
            .to_string();
        assert!(error.contains("no reviewed batch task ancestor"));
    }
}
