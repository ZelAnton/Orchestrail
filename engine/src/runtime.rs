//! Crash-safe execution ledger around the pure [`crate::processor::Processor`] reducer.
//!
//! A reducer checkpoint alone records the state *after* it requests an effect.  If the process
//! dies after a leaf process starts but before its result returns, blindly re-running the effect
//! can duplicate an implementation, merge, push, or CI repair.  `ProcessorRuntime` persists the
//! state and a keyed set of outstanding effects in one atomic document before exposing any effect
//! to an executor.  Restart therefore produces explicit recovery obligations instead of a replay
//! guess.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointError, CheckpointStore};
use crate::events::{Event, Outbox, OutboxError, project_processor_transition};
use crate::processor::{
    CiFixPreparationOutcome, CiOutcome, Effect, LeafKind, LeafOutcome, MergeOutcome, Processor,
    ProcessorCommand, ProcessorConfig, ProcessorError, ProcessorState, ReviewOutcome, TaskPhase,
    VerificationOutcome,
};
use crate::recovery::bind_legacy_safety_snapshot;
use crate::session::LeafSessionUpdate;
use crate::telemetry::{
    OperationCompleted, OperationExecutorKind, OperationOutcome, OperationScope,
};
use crate::time::is_iso_utc;

/// Independently versioned durable runtime-envelope file under `.work/`.
pub const RUNTIME_CHECKPOINT_FILE: &str = "processor-runtime.json";
const RUNTIME_STATE_VERSION: u32 = 1;

/// A persisted processor state plus every effect the executor has not yet acknowledged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RuntimeCheckpoint {
    schema_version: u32,
    processor: ProcessorState,
    pending: BTreeMap<String, Effect>,
    /// Start/duration of an optional Codex CI adapter which explicitly yielded to Claude.
    /// Older checkpoints simply have no in-flight provider span.
    #[serde(default)]
    ci_fix_provider_span: Option<OperationTiming>,
}

/// How restart code must treat an outstanding effect. Only guarded, explicitly idempotent
/// workspace/reconciliation and Phase-6 accounting repairs may be repeated without a separate
/// operator decision. Every model leaf, commit, merge, publish, CI, and non-idempotent queue
/// mutation remains inspect-first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryRequirement {
    RetryIdempotently { key: String, effect: Effect },
    InspectBeforeContinuing { key: String, effect: Effect },
}

/// Runtime coordination failure.  These errors are intentionally separate from reducer errors:
/// a stale/missing acknowledgement is an executor protocol violation, not an invalid state
/// transition the reducer should try to repair.
#[derive(Debug)]
pub enum RuntimeError {
    Checkpoint(CheckpointError),
    Outbox(OutboxError),
    Processor(ProcessorError),
    CorruptCheckpoint(String),
    InvalidEventTime(String),
    PendingEffect(String),
    ExistingCheckpoint(String),
    UnexpectedAcknowledgement { expected: String, actual: String },
    UnknownEffect(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Checkpoint(error) => write!(f, "runtime checkpoint error: {error}"),
            Self::Outbox(error) => write!(f, "runtime event-outbox error: {error}"),
            Self::Processor(error) => write!(f, "processor transition error: {error}"),
            Self::CorruptCheckpoint(message)
            | Self::PendingEffect(message)
            | Self::ExistingCheckpoint(message)
            | Self::UnknownEffect(message) => f.write_str(message),
            Self::InvalidEventTime(value) => write!(
                f,
                "event time {value:?} is not an ISO-8601 UTC instant accepted by the event contract"
            ),
            Self::UnexpectedAcknowledgement { expected, actual } => write!(
                f,
                "effect acknowledgement {actual:?} does not match outstanding {expected:?}"
            ),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Checkpoint(error) => Some(error),
            Self::Outbox(error) => Some(error),
            Self::Processor(error) => Some(error),
            Self::CorruptCheckpoint(_)
            | Self::InvalidEventTime(_)
            | Self::PendingEffect(_)
            | Self::ExistingCheckpoint(_)
            | Self::UnexpectedAcknowledgement { .. }
            | Self::UnknownEffect(_) => None,
        }
    }
}

impl From<CheckpointError> for RuntimeError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<OutboxError> for RuntimeError {
    fn from(error: OutboxError) -> Self {
        Self::Outbox(error)
    }
}

impl From<ProcessorError> for RuntimeError {
    fn from(error: ProcessorError) -> Self {
        Self::Processor(error)
    }
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

/// The one writer for a processor state machine and its effect ledger.  The surrounding engine
/// owns the lease; this type is deliberately agnostic about lease acquisition and process I/O.
#[derive(Debug, Clone)]
pub struct ProcessorRuntime {
    processor: Processor,
    pending: BTreeMap<String, Effect>,
    ci_fix_provider_span: Option<OperationTiming>,
    store: CheckpointStore,
    outbox: Outbox,
}

impl ProcessorRuntime {
    /// Start a fresh deterministic processor. The first accepted command must be Phase-0
    /// [`ProcessorCommand::Recover`], exactly as for a restored runtime.
    pub fn new(config: ProcessorConfig, work: impl Into<PathBuf>) -> Result<Self> {
        let work = work.into();
        Ok(Self {
            processor: Processor::new(config)?,
            pending: BTreeMap::new(),
            ci_fix_provider_span: None,
            store: CheckpointStore::for_file(&work, RUNTIME_CHECKPOINT_FILE)?,
            outbox: Outbox::new(work),
        })
    }

    /// Restore a runtime. Missing file starts a new Phase-0 processor; an old/malformed envelope
    /// is a hard error. The reducer itself is reconstructed through `from_checkpoint`, so it
    /// cannot dispatch a leaf before recovery has run again.
    pub fn resume(config: ProcessorConfig, work: impl Into<PathBuf>) -> Result<Self> {
        let store = CheckpointStore::for_file(work, RUNTIME_CHECKPOINT_FILE)?;
        let Some(checkpoint) = store.load_json::<RuntimeCheckpoint>()? else {
            return Self::new(config, store_work(&store));
        };
        if checkpoint.schema_version != RUNTIME_STATE_VERSION {
            return Err(RuntimeError::CorruptCheckpoint(format!(
                "unsupported runtime checkpoint version {}",
                checkpoint.schema_version
            )));
        }
        for (stored_key, effect) in &checkpoint.pending {
            let Some(expected_key) = effect_key(effect) else {
                return Err(RuntimeError::CorruptCheckpoint(format!(
                    "runtime ledger contains unkeyed informational effect {effect:?}"
                )));
            };
            if stored_key != &expected_key {
                return Err(RuntimeError::CorruptCheckpoint(format!(
                    "runtime ledger key {stored_key:?} does not match effect key {expected_key:?} for {effect:?}"
                )));
            }
        }
        Ok(Self {
            processor: Processor::from_checkpoint(config, checkpoint.processor)?,
            pending: checkpoint.pending,
            ci_fix_provider_span: checkpoint.ci_fix_provider_span,
            outbox: Outbox::new(store_work(&store)),
            store,
        })
    }

    /// Durably adopt a strictly validated legacy processor state before any native effect has
    /// been issued.  The supplied state is intentionally passed through `Processor::from_checkpoint`,
    /// so the first native turn is still Phase 0 rather than an unchecked dispatch.  An existing
    /// runtime envelope is never overwritten: its pending-effect ledger is the authority once it
    /// exists.
    pub fn import_legacy(
        config: ProcessorConfig,
        work: impl Into<PathBuf>,
        mut legacy_state: ProcessorState,
    ) -> Result<Self> {
        let work = work.into();
        let store = CheckpointStore::for_file(&work, RUNTIME_CHECKPOINT_FILE)?;
        if store.load_json::<RuntimeCheckpoint>()?.is_some() {
            return Err(RuntimeError::ExistingCheckpoint(format!(
                "refusing to overwrite existing native runtime checkpoint {}",
                store.path().display()
            )));
        }
        // Legacy Markdown has no native safety snapshot.  Bind the currently selected native
        // policy exactly once at this explicit migration boundary before `from_checkpoint`
        // applies its ordinary anti-drift validation on every later resume.
        bind_legacy_safety_snapshot(&mut legacy_state, &config);
        let processor = Processor::from_checkpoint(config, legacy_state)?;
        let runtime = Self {
            processor,
            pending: BTreeMap::new(),
            ci_fix_provider_span: None,
            outbox: Outbox::new(work),
            store,
        };
        runtime.persist(
            &runtime.processor,
            &runtime.pending,
            &runtime.ci_fix_provider_span,
        )?;
        Ok(runtime)
    }

    pub fn state(&self) -> &ProcessorState {
        self.processor.state()
    }

    pub fn pending_effects(&self) -> &BTreeMap<String, Effect> {
        &self.pending
    }

    /// Bind the operator's non-semantic storage policy after configuration has been decoded.
    /// Rotation is deliberately not part of the reducer checkpoint: archives preserve the same
    /// logical event stream and may be enabled or disabled between otherwise identical runs.
    pub fn set_events_rotation_enabled(&mut self, enabled: bool) {
        self.outbox.set_rotation_enabled(enabled);
    }

    /// Return the durable ledger key associated with an effect. Effects without a key are
    /// informational scheduling markers and must not be acknowledged as completed work.
    pub fn effect_key(effect: &Effect) -> Option<String> {
        effect_key(effect)
    }

    /// Return the ledger key that `command` would acknowledge in the current state. Executors
    /// use this before submitting a result to prove that their observation belongs to the exact
    /// pending effect they ran, rather than to another task that happened to be in flight.
    pub fn command_acknowledgement_key(
        &self,
        command: &ProcessorCommand,
    ) -> Result<Option<String>> {
        acknowledgement_key(&self.processor, command)
    }

    /// Submit one structured external observation at an explicit UTC instant. The explicit clock
    /// is part of the control-plane input rather than an ambient wall-clock read: replay can
    /// reproduce the same transition identities, while the event outbox may safely retain the
    /// original presentation timestamp. The runtime writes projected events before the state
    /// checkpoint; a crash in between is repaired by the event idempotency key on replay, whereas
    /// the reverse order could permanently lose an event for an already-advanced state.
    pub fn apply_at(
        &mut self,
        command: ProcessorCommand,
        occurred_at: &str,
    ) -> Result<Vec<Effect>> {
        self.apply_internal(command, occurred_at, None)
    }

    /// Apply the result of one exact pending effect and best-effort materialize its operation
    /// timing before the successor checkpoint. The human/telemetry projection is intentionally
    /// non-authoritative: an outbox failure never converts completed product work into a retry.
    /// Production driver entry point with independently sampled wall-clock bounds and a
    /// monotonic elapsed duration. The clocks deliberately need not arithmetically agree.
    pub(crate) fn apply_effect_at_with_timing(
        &mut self,
        effect: &Effect,
        command: ProcessorCommand,
        occurred_at: &str,
        timing: OperationTiming,
    ) -> Result<Vec<Effect>> {
        self.apply_internal(command, occurred_at, Some((effect, timing)))
    }

    fn apply_internal(
        &mut self,
        command: ProcessorCommand,
        occurred_at: &str,
        operation: Option<(&Effect, OperationTiming)>,
    ) -> Result<Vec<Effect>> {
        if !is_iso_utc(occurred_at) {
            return Err(RuntimeError::InvalidEventTime(occurred_at.into()));
        }
        let acknowledgement = acknowledgement_key(&self.processor, &command)?;
        if let Some(actual) = acknowledgement.as_deref() {
            if !self.pending.contains_key(actual) {
                let expected = self
                    .pending
                    .keys()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| "no outstanding effect".into());
                return Err(RuntimeError::UnexpectedAcknowledgement {
                    expected,
                    actual: actual.to_string(),
                });
            }
        } else if !self.pending.is_empty() && !matches!(command, ProcessorCommand::Recover { .. }) {
            return Err(RuntimeError::PendingEffect(format!(
                "cannot apply command while {} effect(s) remain outstanding: {}",
                self.pending.len(),
                self.pending.keys().cloned().collect::<Vec<_>>().join(", ")
            )));
        }

        let before = self.processor.state().clone();
        let mut processor = self.processor.clone();
        let effects = processor.apply(command.clone())?;
        let mut pending = self.pending.clone();
        let mut ci_fix_provider_span = self.ci_fix_provider_span.clone();
        if let Some(key) = acknowledgement {
            pending.remove(&key);
        }
        for effect in &effects {
            if let Some(key) = effect_key(effect)
                && let Some(existing) = pending.insert(key.clone(), effect.clone())
                && existing != *effect
            {
                return Err(RuntimeError::CorruptCheckpoint(format!(
                    "effect key {key:?} aliases incompatible effects {existing:?} and {effect:?}"
                )));
            }
        }
        // The append-only outbox is intentionally ahead of the checkpoint. If a checkpoint
        // write then fails or the process dies, replaying this command produces the same stable
        // event ids and `append_idempotent` leaves the already-durable prefix untouched.
        for event in project_processor_transition(&before, processor.state(), &command, occurred_at)
        {
            self.outbox.append_idempotent(&event)?;
        }
        if let Some((effect, timing)) = operation {
            let timing = match (effect, &command) {
                (
                    Effect::PrepareCiFix,
                    ProcessorCommand::CiFixPrepared {
                        outcome: CiFixPreparationOutcome::Fallback,
                    },
                ) => {
                    ci_fix_provider_span = Some(timing.clone());
                    timing
                }
                (
                    Effect::PrepareCiFix,
                    ProcessorCommand::CiFixPrepared {
                        outcome: CiFixPreparationOutcome::Completed,
                    },
                ) => {
                    ci_fix_provider_span = None;
                    timing
                }
                (
                    Effect::DispatchIntegration {
                        kind: LeafKind::CiFix,
                    },
                    ProcessorCommand::CiFix { .. },
                ) if before.integration.ci_fix_provider_fallback => {
                    let combined = ci_fix_provider_span.as_ref().map_or_else(
                        || timing.clone(),
                        |provider| OperationTiming {
                            started_at: provider.started_at.clone(),
                            ended_at: timing.ended_at.clone(),
                            duration_ms: provider.duration_ms.saturating_add(timing.duration_ms),
                        },
                    );
                    ci_fix_provider_span = None;
                    combined
                }
                _ => timing,
            };
            for event in
                project_completed_operations(&before, processor.state(), effect, &command, &timing)
            {
                // Legacy operation telemetry is explicitly best-effort. A malformed/blocked
                // telemetry sink must show up as partial archive data, never replay a completed
                // merge, publish, or external call.
                let _ = self.outbox.append_idempotent(&event);
            }
        }
        self.persist(&processor, &pending, &ci_fix_provider_span)?;
        self.processor = processor;
        self.pending = pending;
        self.ci_fix_provider_span = ci_fix_provider_span;
        Ok(effects)
    }

    // Keep the unit-test fixtures concise without allowing production code to accidentally
    // manufacture event time from an ambient clock. Public runtime callers must use `apply_at`.
    #[cfg(test)]
    fn apply(&mut self, command: ProcessorCommand) -> Result<Vec<Effect>> {
        self.apply_at(command, "2026-07-24T12:00:00Z")
    }

    /// Acknowledge an effect with no dedicated reducer result (e.g. an idempotent queue return or
    /// descriptor archive). The runtime writes the reduced ledger before permitting a later phase
    /// command. A failed executor must leave the key present and therefore blocks progression.
    pub fn complete_effect(&mut self, key: &str) -> Result<()> {
        let effect = self.pending.get(key).cloned().ok_or_else(|| {
            RuntimeError::UnknownEffect(format!("no outstanding effect named {key:?}"))
        })?;
        let mut pending = self.pending.clone();
        pending.remove(key);
        let mut processor = self.processor.clone();
        processor.acknowledge_non_command_effect(&effect)?;
        self.persist(&processor, &pending, &self.ci_fix_provider_span)?;
        self.processor = processor;
        self.pending = pending;
        Ok(())
    }

    /// Acknowledge a durable non-command effect (for example archival, journal writing, or lease
    /// release) after its executor has verified completion. Command-producing effects must use
    /// [`Self::apply_at`] instead so the reducer receives the typed result.
    pub fn acknowledge_effect(&mut self, effect: &Effect) -> Result<()> {
        let key = effect_key(effect).ok_or_else(|| {
            RuntimeError::UnknownEffect(format!(
                "effect {effect:?} has no durable ledger key and cannot be acknowledged"
            ))
        })?;
        self.complete_effect(&key)
    }

    /// Persist one task's orthogonal provider conversation coordinate.
    ///
    /// It intentionally leaves the pending-effect ledger untouched, so it may be written while the
    /// leaf effect that observed it is still outstanding: the coordinate acknowledges nothing and
    /// no reducer decision reads it. Recording it *before* the acknowledging command also keeps it
    /// out of that command's projected transition, which compares the state around one exact
    /// decision. A crash between the two writes costs at most one re-seeded call.
    pub fn record_leaf_session(&mut self, task_id: &str, update: &LeafSessionUpdate) -> Result<()> {
        // Mutate a copy and adopt it only after the checkpoint is durable, exactly like the
        // command path: an in-memory coordinate must never outlive a failed write.
        let mut processor = self.processor.clone();
        processor.record_leaf_session(task_id, update)?;
        self.persist(&processor, &self.pending, &self.ci_fix_provider_span)?;
        self.processor = processor;
        Ok(())
    }

    /// Replace the temporary Phase-0 workspace reconciliation for one task with the exact
    /// retry-safe task effect already present in the restored ledger. This covers an idempotent
    /// workspace ensure and, only for a concrete port with durable Codex proof, task preparation.
    /// Both keys must already be durable: this method never manufactures or acknowledges the
    /// restored task effect itself.
    pub(crate) fn supersede_reconcile_with_pending_task_effect(
        &mut self,
        task_effect: &Effect,
    ) -> Result<()> {
        let task_id = match task_effect {
            Effect::EnsureTaskWorkspace { task_id, .. }
            | Effect::PrepareTaskLeaf { task_id, .. }
            | Effect::PrepareTaskReview { task_id } => task_id,
            _ => {
                return Err(RuntimeError::CorruptCheckpoint(
                    "only a retry-safe task effect may supersede its recovery reconciliation"
                        .into(),
                ));
            }
        };
        let task_effect_key = effect_key(task_effect).expect("retry-safe task effect is keyed");
        if self.pending.get(&task_effect_key) != Some(task_effect) {
            return Err(RuntimeError::CorruptCheckpoint(format!(
                "recovery task effect {task_effect_key:?} is absent or changed"
            )));
        }
        let reconcile = Effect::Reconcile {
            task_id: task_id.clone(),
        };
        let reconcile_key = effect_key(&reconcile).expect("task reconciliation is keyed");
        if self.pending.get(&reconcile_key) != Some(&reconcile) {
            return Err(RuntimeError::CorruptCheckpoint(format!(
                "recovery reconciliation {reconcile_key:?} is absent or changed"
            )));
        }
        self.supersede_recovery_effects_with_pending(task_effect, &[reconcile])
    }

    /// Remove only freshly reconstructed Phase-0 scheduling effects when an older exact pending
    /// effect is the stronger crash-replay authority. Neither the authoritative effect nor the
    /// reducer state is acknowledged here; the caller must execute that original ledger entry.
    pub(crate) fn supersede_recovery_effects_with_pending(
        &mut self,
        pending_effect: &Effect,
        reconstructed: &[Effect],
    ) -> Result<()> {
        let pending_key = effect_key(pending_effect).ok_or_else(|| {
            RuntimeError::CorruptCheckpoint(
                "recovery supersession requires a keyed pending effect".into(),
            )
        })?;
        if self.pending.get(&pending_key) != Some(pending_effect) {
            return Err(RuntimeError::CorruptCheckpoint(format!(
                "recovery authority {pending_key:?} is absent or changed"
            )));
        }
        let mut pending = self.pending.clone();
        for effect in reconstructed {
            let key = effect_key(effect).ok_or_else(|| {
                RuntimeError::CorruptCheckpoint(format!(
                    "reconstructed recovery effect is not keyed: {effect:?}"
                ))
            })?;
            if key == pending_key || pending.get(&key) != Some(effect) {
                return Err(RuntimeError::CorruptCheckpoint(format!(
                    "reconstructed recovery effect {key:?} is absent, changed, or aliases its authority"
                )));
            }
            pending.remove(&key);
        }
        self.persist(&self.processor, &pending, &self.ci_fix_provider_span)?;
        self.pending = pending;
        Ok(())
    }

    /// Enumerate recovery obligations after a restart. A caller may retry only entries explicitly
    /// marked idempotent; all other entries require VCS/control-plane/agent-artifact evidence and
    /// an appropriate structured command or `complete_effect` acknowledgement. Phase-6 cleanup
    /// effects are included in the retry-safe set because each concrete adapter re-checks its
    /// owned control-plane or VCS coordinate before making an idempotent repair.  Holding an
    /// interrupted archive/worktree cleanup forever would otherwise strand a completed cohort
    /// before the next queue boundary, despite those guards.
    pub fn recovery_requirements(&self) -> Vec<RecoveryRequirement> {
        self.pending
            .iter()
            .map(|(key, effect)| {
                if matches!(
                    effect,
                    Effect::EnsureTaskWorkspace { .. }
                        | Effect::PrepareIntegrationWorkspace { .. }
                        | Effect::ReanchorPublication { .. }
                        | Effect::Reconcile { .. }
                        | Effect::ReconcileInbox { .. }
                        | Effect::ReconcileInboxFinalization
                        | Effect::DrainQueueInbox { .. }
                        | Effect::CheckTokenBudget { .. }
                        | Effect::CheckCohortBudget { .. }
                        | Effect::VerifyCi { .. }
                        | Effect::PrepareKnowledgeCuration
                        | Effect::PrepareArchival
                        | Effect::ReconfirmCiBeforeArchive { .. }
                        | Effect::ArchiveTask { .. }
                        | Effect::CleanupTaskWorkspace { .. }
                        | Effect::CleanupIntegrationWorkspace
                        | Effect::CleanupCohortControlPlane
                        | Effect::WriteJournalAndStatus
                        | Effect::ReleaseLease
                ) {
                    RecoveryRequirement::RetryIdempotently {
                        key: key.clone(),
                        effect: effect.clone(),
                    }
                } else {
                    RecoveryRequirement::InspectBeforeContinuing {
                        key: key.clone(),
                        effect: effect.clone(),
                    }
                }
            })
            .collect()
    }

    fn persist(
        &self,
        processor: &Processor,
        pending: &BTreeMap<String, Effect>,
        ci_fix_provider_span: &Option<OperationTiming>,
    ) -> Result<()> {
        self.store.save_json(&RuntimeCheckpoint {
            schema_version: RUNTIME_STATE_VERSION,
            processor: processor.state().clone(),
            pending: pending.clone(),
            ci_fix_provider_span: ci_fix_provider_span.clone(),
        })?;
        Ok(())
    }
}

fn project_completed_operations(
    before: &ProcessorState,
    after: &ProcessorState,
    effect: &Effect,
    command: &ProcessorCommand,
    timing: &OperationTiming,
) -> Vec<Event> {
    let Some(batch) = after.batch.as_ref().or(before.batch.as_ref()) else {
        return Vec::new();
    };
    if !batch.events_outbox_enabled {
        return Vec::new();
    }
    match (effect, command) {
        (Effect::PlanNextWave { .. }, ProcessorCommand::Admit { .. }) => {
            let task_ids = after
                .tasks
                .keys()
                .filter(|task_id| !before.tasks.contains_key(*task_id))
                .cloned()
                .collect::<Vec<_>>();
            materialize_operation(
                &batch.id,
                &task_ids,
                OperationSpec {
                    operation: "planning",
                    role: "planner",
                    mode: "full",
                    attempt: before
                        .batch
                        .as_ref()
                        .map_or(1, |batch| u64::from(batch.wave.max(1))),
                    scope: OperationScope::Cohort,
                    executor_kind: OperationExecutorKind::Model,
                    outcome: OperationOutcome::Success,
                },
                timing,
            )
        }
        (
            Effect::MergeTask { task_id },
            ProcessorCommand::TaskMerged {
                task_id: completed,
                outcome,
            },
        ) if task_id == completed => {
            // The native typed VCS attempt is real tool work even though a clean merge skips the
            // legacy merger model. Record it under an additive name so the strict legacy
            // `merge=model` invariant remains intact without dropping elapsed tool time.
            let mut events = materialize_operation(
                &batch.id,
                std::slice::from_ref(task_id),
                OperationSpec {
                    operation: "vcs_merge",
                    role: "vcs",
                    mode: "full",
                    attempt: u64::from(before.integration.publication_reanchor_cycles)
                        .saturating_add(1),
                    scope: OperationScope::Task,
                    executor_kind: OperationExecutorKind::Tool,
                    outcome: if matches!(outcome, MergeOutcome::Merged { .. }) {
                        OperationOutcome::Success
                    } else {
                        OperationOutcome::Failed
                    },
                },
                timing,
            );
            // A clean native merge is handled by the typed VCS layer and deliberately skips the
            // legacy merger model call. Preserve the core operation coordinate as an explicit
            // zero-duration model gate rather than falsely labelling tool time as model time.
            if matches!(outcome, MergeOutcome::Merged { .. }) {
                let skipped = OperationTiming {
                    started_at: timing.ended_at.clone(),
                    ended_at: timing.ended_at.clone(),
                    duration_ms: 0,
                };
                events.extend(materialize_operation(
                    &batch.id,
                    std::slice::from_ref(task_id),
                    OperationSpec {
                        operation: "merge",
                        role: "merger",
                        mode: "full",
                        attempt: u64::from(before.integration.publication_reanchor_cycles)
                            .saturating_add(1),
                        scope: OperationScope::Task,
                        executor_kind: OperationExecutorKind::Model,
                        outcome: OperationOutcome::Skipped,
                    },
                    &skipped,
                ));
            }
            events
        }
        (
            Effect::DispatchIntegration {
                kind: LeafKind::Merger,
            },
            ProcessorCommand::MergeResolution { task_id, outcome },
        ) => materialize_operation(
            &batch.id,
            std::slice::from_ref(task_id),
            OperationSpec {
                operation: "merge",
                role: "merger",
                mode: "full",
                attempt: integration_attempt(before, LeafKind::Merger),
                // Conflict resolution is an integration model call and its usage coordinate is
                // `_integration`; keeping task scope here made the immutable join miss its
                // matching usage event. Only this task is affected, so the shared divisor is 1.
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::Model,
                outcome: leaf_operation_outcome(outcome),
            },
            timing,
        ),
        (
            Effect::DispatchIntegration {
                kind: LeafKind::IntegrationReview,
            },
            ProcessorCommand::IntegrationReview { outcome },
        ) => materialize_shared_operation(
            before,
            after,
            &batch.id,
            OperationSpec {
                operation: "integration_review",
                role: "full_reviewer",
                mode: "full",
                attempt: integration_attempt(before, LeafKind::IntegrationReview),
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::Model,
                outcome: review_operation_outcome(outcome),
            },
            timing,
        ),
        (
            Effect::DispatchIntegration {
                kind: LeafKind::IntegrationFix,
            },
            ProcessorCommand::IntegrationFix { outcome },
        ) => materialize_shared_operation(
            before,
            after,
            &batch.id,
            OperationSpec {
                operation: "integration_fix",
                role: "merger",
                mode: "fix",
                attempt: integration_attempt(before, LeafKind::IntegrationFix),
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::Model,
                outcome: leaf_operation_outcome(outcome),
            },
            timing,
        ),
        (
            Effect::DispatchIntegration {
                kind: LeafKind::CiFix,
            },
            ProcessorCommand::CiFix { outcome },
        ) => materialize_shared_operation(
            before,
            after,
            &batch.id,
            OperationSpec {
                operation: "ci_fix",
                role: "coder",
                mode: "fix",
                attempt: integration_attempt(before, LeafKind::CiFix),
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::Model,
                outcome: if before.integration.ci_fix_provider_fallback
                    && matches!(
                        outcome,
                        LeafOutcome::Completed { .. } | LeafOutcome::RiskElevated { .. }
                    ) {
                    OperationOutcome::Fallback
                } else {
                    leaf_operation_outcome(outcome)
                },
            },
            timing,
        ),
        (
            Effect::PrepareCiFix,
            ProcessorCommand::CiFixPrepared {
                outcome: CiFixPreparationOutcome::Completed,
            },
        ) => materialize_shared_operation(
            before,
            after,
            &batch.id,
            OperationSpec {
                operation: "ci_fix",
                role: "coder",
                mode: "fix",
                attempt: before.integration.ci_cycles.max(1) as u64,
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::Model,
                outcome: OperationOutcome::Success,
            },
            timing,
        ),
        (
            Effect::DispatchIntegration {
                kind: LeafKind::KnowledgeCurator,
            },
            ProcessorCommand::KnowledgeCurated { outcome },
        ) => materialize_shared_operation(
            before,
            after,
            &batch.id,
            OperationSpec {
                operation: "knowledge_curate",
                role: "knowledge_curator",
                mode: "full",
                attempt: integration_attempt(before, LeafKind::KnowledgeCurator),
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::Model,
                outcome: leaf_operation_outcome(outcome),
            },
            timing,
        ),
        (
            Effect::VerifyIntegration { .. },
            ProcessorCommand::IntegrationVerified { outcome, .. },
        ) => materialize_shared_operation(
            before,
            after,
            &batch.id,
            OperationSpec {
                operation: "verification",
                role: "verification",
                mode: "full",
                attempt: u64::from(after.integration.verification_attempts.max(1)),
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::Tool,
                outcome: verification_operation_outcome(outcome),
            },
            timing,
        ),
        (Effect::Publish { .. }, ProcessorCommand::Published { .. }) => {
            materialize_shared_operation(
                before,
                after,
                &batch.id,
                OperationSpec {
                    operation: "publish",
                    role: "publisher",
                    mode: "full",
                    attempt: u64::from(after.integration.publish_attempts.max(1)),
                    scope: OperationScope::Integration,
                    executor_kind: OperationExecutorKind::External,
                    outcome: OperationOutcome::Success,
                },
                timing,
            )
        }
        (Effect::Publish { .. }, ProcessorCommand::PublicationReanchorRequired { .. }) => {
            materialize_shared_operation(
                before,
                after,
                &batch.id,
                OperationSpec {
                    operation: "publish",
                    role: "publisher",
                    mode: "full",
                    attempt: u64::from(after.integration.publish_attempts.max(1)),
                    scope: OperationScope::Integration,
                    executor_kind: OperationExecutorKind::External,
                    outcome: OperationOutcome::Failed,
                },
                timing,
            )
        }
        (Effect::VerifyCi { .. }, ProcessorCommand::CiVerified { outcome }) => {
            materialize_shared_operation(
                before,
                after,
                &batch.id,
                OperationSpec {
                    operation: "ci_wait",
                    role: "ci",
                    mode: "full",
                    attempt: u64::from(after.integration.ci_wait_attempts.max(1)),
                    scope: OperationScope::Integration,
                    executor_kind: OperationExecutorKind::External,
                    outcome: ci_operation_outcome(outcome),
                },
                timing,
            )
        }
        (
            Effect::ReconfirmCiBeforeArchive { .. },
            ProcessorCommand::ArchiveCiReconfirmed { outcome, .. },
        ) => materialize_shared_operation(
            before,
            after,
            &batch.id,
            OperationSpec {
                operation: "ci_wait",
                role: "ci",
                mode: "archive",
                attempt: u64::from(after.integration.archive_ci_wait_attempts.max(1)),
                scope: OperationScope::Integration,
                executor_kind: OperationExecutorKind::External,
                outcome: ci_operation_outcome(outcome),
            },
            timing,
        ),
        _ => Vec::new(),
    }
}

#[derive(Clone, Copy)]
struct OperationSpec<'a> {
    operation: &'a str,
    role: &'a str,
    mode: &'a str,
    attempt: u64,
    scope: OperationScope,
    executor_kind: OperationExecutorKind,
    outcome: OperationOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OperationTiming {
    pub(crate) started_at: String,
    pub(crate) ended_at: String,
    pub(crate) duration_ms: u64,
}

fn materialize_shared_operation(
    before: &ProcessorState,
    after: &ProcessorState,
    batch_id: &str,
    spec: OperationSpec<'_>,
    timing: &OperationTiming,
) -> Vec<Event> {
    let task_ids = if after.integration.merged_tasks.is_empty() {
        &before.integration.merged_tasks
    } else {
        &after.integration.merged_tasks
    }
    .iter()
    .cloned()
    .collect::<Vec<_>>();
    materialize_operation(batch_id, &task_ids, spec, timing)
}

fn materialize_operation(
    batch_id: &str,
    task_ids: &[String],
    spec: OperationSpec<'_>,
    timing: &OperationTiming,
) -> Vec<Event> {
    let Ok(shared_task_count) = u64::try_from(task_ids.len()) else {
        return Vec::new();
    };
    if shared_task_count == 0 {
        return Vec::new();
    }
    task_ids
        .iter()
        .filter_map(|task_id| {
            OperationCompleted {
                operation: spec.operation.into(),
                role: spec.role.into(),
                mode: spec.mode.into(),
                attempt_number: spec.attempt.max(1),
                scope: spec.scope,
                executor_kind: spec.executor_kind,
                started_at: timing.started_at.clone(),
                ended_at: timing.ended_at.clone(),
                duration_ms: timing.duration_ms,
                outcome: spec.outcome,
                shared_task_count: if spec.scope == OperationScope::Task {
                    1
                } else {
                    shared_task_count
                },
            }
            .to_event(batch_id, task_id, &timing.ended_at)
            .ok()
        })
        .collect()
}

fn integration_attempt(state: &ProcessorState, kind: LeafKind) -> u64 {
    state
        .integration
        .leaf_attempts
        .get(kind.as_str())
        .copied()
        .map_or(1, u64::from)
        .max(1)
}

fn leaf_operation_outcome(outcome: &LeafOutcome) -> OperationOutcome {
    match outcome {
        LeafOutcome::Completed { .. }
        | LeafOutcome::RiskElevated { .. }
        | LeafOutcome::CompletedWithWontFix { .. } => OperationOutcome::Success,
        LeafOutcome::RetryableFailure { reason } | LeafOutcome::Escalated { reason } => {
            classified_model_failure(reason)
        }
    }
}

fn review_operation_outcome(outcome: &ReviewOutcome) -> OperationOutcome {
    match outcome {
        ReviewOutcome::Clean { .. }
        | ReviewOutcome::CleanRiskElevated { .. }
        | ReviewOutcome::Findings { .. }
        | ReviewOutcome::FindingsRiskElevated { .. } => OperationOutcome::Success,
        ReviewOutcome::Incomplete => OperationOutcome::Failed,
        ReviewOutcome::Escalated { reason } => classified_model_failure(reason),
    }
}

fn classified_model_failure(reason: &str) -> OperationOutcome {
    match reason {
        "supervisor timeout" => OperationOutcome::Timeout,
        "supervisor cancelled" => OperationOutcome::Cancelled,
        _ => OperationOutcome::Failed,
    }
}

fn verification_operation_outcome(outcome: &VerificationOutcome) -> OperationOutcome {
    match outcome {
        VerificationOutcome::Passed => OperationOutcome::Success,
        VerificationOutcome::Exempt { .. } => OperationOutcome::Skipped,
        VerificationOutcome::Failed { .. } | VerificationOutcome::Blocked { .. } => {
            OperationOutcome::Failed
        }
    }
}

fn ci_operation_outcome(outcome: &CiOutcome) -> OperationOutcome {
    match outcome {
        CiOutcome::Passed => OperationOutcome::Success,
        CiOutcome::LocalOnly | CiOutcome::Disabled => OperationOutcome::Skipped,
        CiOutcome::BestEffortDegraded { .. } | CiOutcome::RequiredUnconfirmed { .. } => {
            OperationOutcome::Timeout
        }
        CiOutcome::Failed { .. } => OperationOutcome::Failed,
    }
}

fn store_work(store: &CheckpointStore) -> PathBuf {
    store
        .path()
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".work"))
}

fn acknowledgement_key(
    processor: &Processor,
    command: &ProcessorCommand,
) -> Result<Option<String>> {
    let key = match command {
        ProcessorCommand::WorkspaceReady { task_id }
        | ProcessorCommand::WorkspaceFailed { task_id, .. } => {
            Some(format!("ensure-workspace:{task_id}"))
        }
        ProcessorCommand::TaskLeafPrepared { task_id, .. } => {
            let task = processor
                .state()
                .tasks
                .get(task_id)
                .ok_or_else(|| ProcessorError::MissingTask(task_id.clone()))?;
            let kind = match task.phase {
                TaskPhase::Implementing => LeafKind::Implement,
                TaskPhase::Fixing => LeafKind::Fix,
                _ => return Ok(None),
            };
            Some(format!("prepare-task-leaf:{task_id}:{}", leaf_key(kind)))
        }
        ProcessorCommand::TaskLeaf { task_id, .. } => {
            let task = processor
                .state()
                .tasks
                .get(task_id)
                .ok_or_else(|| ProcessorError::MissingTask(task_id.clone()))?;
            let kind = match task.phase {
                TaskPhase::Implementing => LeafKind::Implement,
                TaskPhase::Fixing => LeafKind::Fix,
                _ => return Ok(None),
            };
            Some(format!("dispatch-task:{task_id}:{}", leaf_key(kind)))
        }
        ProcessorCommand::TaskCommitted { task_id, .. } => Some(format!("commit-task:{task_id}")),
        ProcessorCommand::TaskReviewPrepared { task_id, .. } => {
            Some(format!("prepare-task-review:{task_id}"))
        }
        ProcessorCommand::TaskReview { task_id, .. } => {
            Some(format!("dispatch-task:{task_id}:review"))
        }
        ProcessorCommand::IntegrationWorkspaceReady => Some("prepare-integration".into()),
        ProcessorCommand::TokenBudgetChecked { next, .. } => Some(next.ledger_key()),
        ProcessorCommand::CohortBudgetChecked { next, .. } => Some(next.cohort_budget_ledger_key()),
        ProcessorCommand::TaskMerged { task_id, .. } => Some(format!("merge-task:{task_id}")),
        ProcessorCommand::MergeResolution { .. } => Some("dispatch-integration:merger".into()),
        ProcessorCommand::MergeResolutionFinalized { task_id, .. } => {
            Some(format!("finalize-merge-resolution:{task_id}"))
        }
        ProcessorCommand::MergeResolutionAborted { task_id, .. } => {
            Some(format!("abort-merge-resolution:{task_id}"))
        }
        ProcessorCommand::IntegrationVerified { .. } => Some("verify-integration".into()),
        ProcessorCommand::IntegrationReview { .. } => {
            Some("dispatch-integration:integration-review".into())
        }
        ProcessorCommand::IntegrationFix { .. } => {
            Some("dispatch-integration:integration-fix".into())
        }
        ProcessorCommand::IntegrationFixCommitted { .. } => Some("commit-integration-fix".into()),
        ProcessorCommand::Published { .. } => Some("publish".into()),
        ProcessorCommand::PublicationRejected { .. } => Some("publish".into()),
        ProcessorCommand::PublicationAwaitingApproval { .. } => Some("publish".into()),
        ProcessorCommand::PublicationReanchorRequired { .. } => Some("publish".into()),
        ProcessorCommand::PublicationReanchored => Some("reanchor-publication".into()),
        ProcessorCommand::CiVerified { .. } => Some("verify-ci".into()),
        ProcessorCommand::CiFixPrepared { .. } => Some("prepare-ci-fix".into()),
        ProcessorCommand::CiFix { .. } => Some("dispatch-integration:ci-fix".into()),
        ProcessorCommand::CiFixCommitted { .. } => Some("commit-ci-fix".into()),
        ProcessorCommand::KnowledgeCurationPrepared { .. } => {
            Some("prepare-knowledge-curation".into())
        }
        ProcessorCommand::KnowledgeCurated { .. } => {
            Some("dispatch-integration:knowledge-curator".into())
        }
        ProcessorCommand::ArchivalPrepared { .. } => Some("prepare-archival".into()),
        ProcessorCommand::ArchiveCiReconfirmed { .. } => Some("reconfirm-ci-before-archive".into()),
        ProcessorCommand::Admit { .. } => Some("plan-next-wave".into()),
        ProcessorCommand::InboxReconciled { .. } => Some("reconcile-inbox".into()),
        ProcessorCommand::InboxFinalizationReconciled { .. } => {
            Some("reconcile-inbox-finalization".into())
        }
        ProcessorCommand::DependencyGraphRefreshed { boundary, .. } => {
            Some(format!("dispatch-dependency-curator:{}", boundary.as_str()))
        }
        ProcessorCommand::InboxCurated { mode, .. } => {
            Some(format!("dispatch-inbox-curator:{mode:?}").to_ascii_lowercase())
        }
        ProcessorCommand::InboxDrained { .. } => Some("drain-queue-inbox".into()),
        ProcessorCommand::Open { .. }
        | ProcessorCommand::Recover { .. }
        | ProcessorCommand::Advance { .. }
        | ProcessorCommand::CleanupComplete
        | ProcessorCommand::Pause
        | ProcessorCommand::Resume
        | ProcessorCommand::Block { .. } => None,
    };
    Ok(key)
}

fn effect_key(effect: &Effect) -> Option<String> {
    match effect {
        Effect::PersistCheckpoint | Effect::WaitForOperator { .. } => None,
        Effect::ReconcileInbox { .. } => Some("reconcile-inbox".into()),
        Effect::ReconcileInboxFinalization => Some("reconcile-inbox-finalization".into()),
        Effect::DispatchDependencyCurator { boundary } => {
            Some(format!("dispatch-dependency-curator:{}", boundary.as_str()))
        }
        Effect::DispatchInboxCurator { mode, .. } => {
            Some(format!("dispatch-inbox-curator:{mode:?}").to_ascii_lowercase())
        }
        Effect::DrainQueueInbox { .. } => Some("drain-queue-inbox".into()),
        Effect::PlanNextWave { .. } => Some("plan-next-wave".into()),
        Effect::CheckTokenBudget { next } => Some(next.ledger_key()),
        Effect::CheckCohortBudget { next } => Some(next.cohort_budget_ledger_key()),
        Effect::Reconcile { task_id } => Some(format!("reconcile:{task_id}")),
        Effect::EnsureTaskWorkspace { task_id, .. } => Some(format!("ensure-workspace:{task_id}")),
        Effect::PrepareTaskReview { task_id } => Some(format!("prepare-task-review:{task_id}")),
        Effect::PrepareTaskLeaf { task_id, kind } => {
            Some(format!("prepare-task-leaf:{task_id}:{}", leaf_key(*kind)))
        }
        Effect::DispatchTask { task_id, kind } => {
            Some(format!("dispatch-task:{task_id}:{}", leaf_key(*kind)))
        }
        Effect::CommitTask { task_id } => Some(format!("commit-task:{task_id}")),
        Effect::PrepareIntegrationWorkspace { .. } => Some("prepare-integration".into()),
        Effect::MergeTask { task_id } => Some(format!("merge-task:{task_id}")),
        Effect::FinalizeMergeResolution { task_id } => {
            Some(format!("finalize-merge-resolution:{task_id}"))
        }
        Effect::AbortMergeResolution { task_id, .. } => {
            Some(format!("abort-merge-resolution:{task_id}"))
        }
        Effect::VerifyIntegration { .. } => Some("verify-integration".into()),
        Effect::DispatchIntegration { kind } => {
            Some(format!("dispatch-integration:{}", leaf_key(*kind)))
        }
        Effect::CommitIntegrationFix => Some("commit-integration-fix".into()),
        Effect::Publish { .. } => Some("publish".into()),
        Effect::ReanchorPublication { .. } => Some("reanchor-publication".into()),
        Effect::VerifyCi { .. } => Some("verify-ci".into()),
        Effect::Notify { event, subject } => Some(format!("notify:{}:{subject}", event.as_str())),
        Effect::PrepareCiFix => Some("prepare-ci-fix".into()),
        Effect::CommitCiFix => Some("commit-ci-fix".into()),
        Effect::PrepareKnowledgeCuration => Some("prepare-knowledge-curation".into()),
        Effect::PrepareArchival => Some("prepare-archival".into()),
        Effect::ReconfirmCiBeforeArchive { .. } => Some("reconfirm-ci-before-archive".into()),
        Effect::ReturnTask { task_id, .. } => Some(format!("return-task:{task_id}")),
        Effect::EscalateTask { task_id, .. } => Some(format!("escalate-task:{task_id}")),
        Effect::ArchiveTask { task_id } => Some(format!("archive-task:{task_id}")),
        Effect::CleanupTaskWorkspace { task_id } => Some(format!("cleanup-workspace:{task_id}")),
        Effect::CleanupIntegrationWorkspace => Some("cleanup-integration-workspace".into()),
        Effect::CleanupCohortControlPlane => Some("cleanup-cohort-control-plane".into()),
        Effect::WriteJournalAndStatus => Some("write-journal-and-status".into()),
        Effect::ReleaseLease => Some("release-lease".into()),
    }
}

fn leaf_key(kind: LeafKind) -> &'static str {
    match kind {
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::processor::{
        AdmissionCandidate, ArchiveCiGate, CloseReasonWire, CohortRuntime, ImportedRecoveryIntent,
        IntegrationRuntime, LeafOutcome, MergeResolutionRuntime, Phase, TaskPhase, TaskRuntime,
    };
    use crate::resolvers::Level;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temp_work(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-runtime-{label}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn config() -> ProcessorConfig {
        ProcessorConfig {
            max_parallel: 1,
            cohort_size: 1,
            ..ProcessorConfig::default()
        }
    }

    fn open(runtime: &mut ProcessorRuntime) {
        runtime
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        runtime.complete_effect("write-journal-and-status").unwrap();
        runtime
            .apply(ProcessorCommand::Open {
                batch_id: "B-1".into(),
                base: "base".into(),
                now_secs: 1,
            })
            .unwrap();
        runtime
            .apply(ProcessorCommand::DependencyGraphRefreshed {
                boundary: crate::dependency_graph::RefreshBoundary::CohortOpen,
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap();
        runtime
            .apply(ProcessorCommand::InboxReconciled {
                free_slots: 1,
                curation_required: false,
            })
            .unwrap();
        runtime
            .apply(ProcessorCommand::InboxDrained { free_slots: 1 })
            .unwrap();
    }

    fn imported_ready_state() -> ProcessorState {
        ProcessorState {
            schema_version: crate::processor::PROCESSOR_STATE_VERSION,
            phase: Phase::Joining,
            paused_from: None,
            batch: Some(CohortRuntime {
                id: "B-1".into(),
                base: "base".into(),
                started_at_secs: 1,
                wave: 2,
                admitted_total: 1,
                admission_closed: Some(CloseReasonWire::CohortSize),
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: None,
                events_outbox_enabled: true,
            }),
            tasks: BTreeMap::from([(
                "T-1".into(),
                TaskRuntime {
                    id: "T-1".into(),
                    conflict_domain: "engine/**".into(),
                    level: Some(Level::Coder),
                    risk: Some(crate::resolvers::Risk::Medium),
                    wave: 1,
                    phase: TaskPhase::Ready,
                    leaf_attempts: BTreeMap::new(),
                    review_cycles: 1,
                    review_signatures: Vec::new(),
                    pending_fix_open_findings: None,
                    pending_fix_open_finding_ids: None,
                    dimensions_with_findings_last_round: Vec::new(),
                    implementation_author: Some("coder".into()),
                    previous_review_sha: None,
                    review_sha: Some("reviewed".into()),
                    reason: None,
                    imported_recovery_intent: None,
                    leaf_sessions: BTreeMap::new(),
                },
            )]),
            integration: IntegrationRuntime::default(),
            blocked_reason: None,
        }
    }

    fn imported_publishing_conflict_state() -> ProcessorState {
        let mut state = imported_ready_state();
        state.phase = Phase::Publishing;
        state.integration.workspace_prepared = true;
        state.integration.integration_head = Some("integration-head".into());
        state.integration.merged_tasks.insert("T-1".into());
        state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Merged;
        state.tasks.insert(
            "T-2".into(),
            TaskRuntime {
                id: "T-2".into(),
                conflict_domain: "engine/**".into(),
                level: Some(Level::Coder),
                risk: Some(crate::resolvers::Risk::Medium),
                wave: 1,
                phase: TaskPhase::Conflict,
                leaf_attempts: BTreeMap::new(),
                review_cycles: 1,
                review_signatures: Vec::new(),
                pending_fix_open_findings: None,
                pending_fix_open_finding_ids: None,
                dimensions_with_findings_last_round: Vec::new(),
                implementation_author: Some("coder".into()),
                previous_review_sha: None,
                review_sha: Some("reviewed".into()),
                reason: Some("merge conflict".into()),
                imported_recovery_intent: Some(ImportedRecoveryIntent::ReturnConflictToQueue),
                leaf_sessions: BTreeMap::new(),
            },
        );
        state
    }

    fn post_archive_cleaning_state() -> ProcessorState {
        let mut state = imported_ready_state();
        state.phase = Phase::Cleaning;
        state.tasks.clear();
        state.integration.cleanup_journaled = true;
        state.integration.archive_ci_gate = Some(ArchiveCiGate::Skipped);
        state
    }

    #[test]
    fn planning_operation_is_materialized_only_for_the_tasks_the_reducer_admitted() {
        let mut after = imported_ready_state();
        after.phase = Phase::Rolling;
        after.batch.as_mut().unwrap().wave = 2;
        let mut before = after.clone();
        before.tasks.clear();
        before.batch.as_mut().unwrap().wave = 1;
        before.batch.as_mut().unwrap().admitted_total = 0;
        let command = ProcessorCommand::Admit {
            candidates: vec![AdmissionCandidate {
                id: "T-1".into(),
                conflict_domain: "engine/**".into(),
                level: Level::Coder,
                risk: crate::resolvers::Risk::Medium,
                ready: true,
                current_delivery_lane: true,
            }],
            now_secs: 1,
        };

        let events = project_completed_operations(
            &before,
            &after,
            &Effect::PlanNextWave { free_slots: 1 },
            &command,
            &OperationTiming {
                started_at: "2026-07-27T12:00:00.000Z".into(),
                ended_at: "2026-07-27T12:00:01.500Z".into(),
                duration_ms: 1_500,
            },
        );

        assert_eq!(events.len(), 1);
        let operation = OperationCompleted::from_event(&events[0]).unwrap();
        assert_eq!(events[0].task_id.as_deref(), Some("T-1"));
        assert_eq!(operation.operation, "planning");
        assert_eq!(operation.scope, OperationScope::Cohort);
        assert_eq!(operation.shared_task_count, 1);
        assert_eq!(operation.started_at, "2026-07-27T12:00:00.000Z");
        assert_eq!(operation.ended_at, "2026-07-27T12:00:01.500Z");
        assert_eq!(operation.duration_ms, 1_500);
    }

    #[test]
    fn conflict_merger_operation_joins_the_integration_usage_coordinate() {
        let before = imported_ready_state();
        let after = before.clone();
        let events = project_completed_operations(
            &before,
            &after,
            &Effect::DispatchIntegration {
                kind: LeafKind::Merger,
            },
            &ProcessorCommand::MergeResolution {
                task_id: "T-1".into(),
                outcome: LeafOutcome::Completed { author: None },
            },
            &OperationTiming {
                started_at: "2026-07-27T12:00:00.000Z".into(),
                ended_at: "2026-07-27T12:00:02.000Z".into(),
                duration_ms: 2_000,
            },
        );

        assert_eq!(events.len(), 1);
        let operation = OperationCompleted::from_event(&events[0]).unwrap();
        assert_eq!(operation.operation, "merge");
        assert_eq!(operation.scope, OperationScope::Integration);
        assert_eq!(operation.shared_task_count, 1);
    }

    #[test]
    fn clean_native_merge_records_tool_time_and_a_skipped_model_gate() {
        let before = imported_ready_state();
        let mut after = before.clone();
        after.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Merged;
        let events = project_completed_operations(
            &before,
            &after,
            &Effect::MergeTask {
                task_id: "T-1".into(),
            },
            &ProcessorCommand::TaskMerged {
                task_id: "T-1".into(),
                outcome: MergeOutcome::Merged {
                    integration_sha: "merged".into(),
                },
            },
            &OperationTiming {
                started_at: "2026-07-27T12:00:00.000Z".into(),
                ended_at: "2026-07-27T12:00:02.000Z".into(),
                duration_ms: 2_000,
            },
        );

        assert_eq!(events.len(), 2);
        let tool = OperationCompleted::from_event(&events[0]).unwrap();
        assert_eq!(tool.operation, "vcs_merge");
        assert_eq!(tool.executor_kind, OperationExecutorKind::Tool);
        assert_eq!(tool.outcome, OperationOutcome::Success);
        assert_eq!(tool.duration_ms, 2_000);
        let model_gate = OperationCompleted::from_event(&events[1]).unwrap();
        assert_eq!(model_gate.operation, "merge");
        assert_eq!(model_gate.executor_kind, OperationExecutorKind::Model);
        assert_eq!(model_gate.outcome, OperationOutcome::Skipped);
        assert_eq!(model_gate.duration_ms, 0);
    }

    #[test]
    fn model_operation_failure_reasons_preserve_timeout_and_cancellation() {
        assert_eq!(
            leaf_operation_outcome(&LeafOutcome::Escalated {
                reason: "supervisor timeout".into(),
            }),
            OperationOutcome::Timeout
        );
        assert_eq!(
            review_operation_outcome(&ReviewOutcome::Escalated {
                reason: "supervisor cancelled".into(),
            }),
            OperationOutcome::Cancelled
        );
        assert_eq!(
            leaf_operation_outcome(&LeafOutcome::RetryableFailure {
                reason: "invalid report".into(),
            }),
            OperationOutcome::Failed
        );
    }

    #[test]
    fn publication_reanchor_and_success_use_distinct_batch_attempts() {
        let mut before = imported_ready_state();
        before.phase = Phase::Publishing;
        before.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Merged;
        before.integration.merged_tasks.insert("T-1".into());
        before.integration.integration_head = Some("integration-tip".into());
        before.integration.verification_head = Some("integration-tip".into());
        let mut failed = before.clone();
        failed.integration.publish_attempts = 1;
        failed.integration.publication_reanchor_cycles = 1;
        let timing = OperationTiming {
            started_at: "2026-07-27T12:00:00.000Z".into(),
            ended_at: "2026-07-27T12:00:01.000Z".into(),
            duration_ms: 1_000,
        };
        let failed_events = project_completed_operations(
            &before,
            &failed,
            &Effect::Publish {
                batch_id: "B-1".into(),
            },
            &ProcessorCommand::PublicationReanchorRequired {
                reason: "remote advanced".into(),
                target: crate::processor::PublicationReanchorTarget::RemotePublication,
            },
            &timing,
        );
        let first = OperationCompleted::from_event(&failed_events[0]).unwrap();
        assert_eq!(first.attempt_number, 1);
        assert_eq!(first.outcome, OperationOutcome::Failed);

        let mut succeeded = failed.clone();
        succeeded.integration.publish_attempts = 2;
        let success_events = project_completed_operations(
            &failed,
            &succeeded,
            &Effect::Publish {
                batch_id: "B-1".into(),
            },
            &ProcessorCommand::Published {
                head: "integration-tip-2".into(),
                pushed: true,
            },
            &timing,
        );
        let second = OperationCompleted::from_event(&success_events[0]).unwrap();
        assert_eq!(second.attempt_number, 2);
        assert_eq!(second.outcome, OperationOutcome::Success);
        assert_ne!(failed_events[0].event_id, success_events[0].event_id);
    }

    #[test]
    fn claude_ci_fix_after_codex_yield_is_one_fallback_operation() {
        let mut before = imported_publishing_conflict_state();
        before.integration.ci_fix_provider_fallback = true;
        before.integration.leaf_attempts.insert("ci-fix".into(), 1);
        let mut after = before.clone();
        after.integration.ci_fix_provider_fallback = false;
        let events = project_completed_operations(
            &before,
            &after,
            &Effect::DispatchIntegration {
                kind: LeafKind::CiFix,
            },
            &ProcessorCommand::CiFix {
                outcome: LeafOutcome::Completed { author: None },
            },
            &OperationTiming {
                started_at: "2026-07-27T12:00:00.000Z".into(),
                ended_at: "2026-07-27T12:00:02.000Z".into(),
                duration_ms: 2_000,
            },
        );

        assert_eq!(events.len(), 1);
        let operation = OperationCompleted::from_event(&events[0]).unwrap();
        assert_eq!(operation.operation, "ci_fix");
        assert_eq!(operation.role, "coder");
        assert_eq!(operation.mode, "fix");
        assert_eq!(operation.outcome, OperationOutcome::Fallback);
    }

    #[test]
    fn ci_fix_fallback_span_survives_restart_and_covers_both_providers() {
        let work = temp_work("ci-fix-provider-span");
        let state = imported_publishing_conflict_state();
        let mut runtime = ProcessorRuntime::import_legacy(config(), &work, state).unwrap();
        runtime
            .processor
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        let prepare = Effect::PrepareCiFix;
        runtime
            .pending
            .insert(effect_key(&prepare).unwrap(), prepare.clone());
        runtime
            .persist(
                &runtime.processor,
                &runtime.pending,
                &runtime.ci_fix_provider_span,
            )
            .unwrap();
        runtime
            .apply_effect_at_with_timing(
                &prepare,
                ProcessorCommand::CiFixPrepared {
                    outcome: CiFixPreparationOutcome::Fallback,
                },
                "2026-07-27T12:00:01Z",
                OperationTiming {
                    started_at: "2026-07-27T12:00:00.000Z".into(),
                    ended_at: "2026-07-27T12:00:00.300Z".into(),
                    duration_ms: 300,
                },
            )
            .unwrap();
        drop(runtime);

        let mut resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        resumed
            .processor
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        assert_eq!(
            resumed
                .ci_fix_provider_span
                .as_ref()
                .map(|span| span.duration_ms),
            Some(300)
        );
        let effect = Effect::DispatchIntegration {
            kind: LeafKind::CiFix,
        };
        assert_eq!(
            resumed.pending.get(&effect_key(&effect).unwrap()),
            Some(&effect)
        );
        resumed
            .apply_effect_at_with_timing(
                &effect,
                ProcessorCommand::CiFix {
                    outcome: LeafOutcome::Completed { author: None },
                },
                "2026-07-27T12:00:02Z",
                OperationTiming {
                    started_at: "2026-07-27T12:00:01.000Z".into(),
                    ended_at: "2026-07-27T12:00:01.700Z".into(),
                    duration_ms: 700,
                },
            )
            .unwrap();

        assert!(resumed.ci_fix_provider_span.is_none());
        let mut reader = crate::events::TailReader::new(work.join(crate::events::OUTBOX_FILE));
        let event = reader
            .poll_all()
            .unwrap()
            .into_iter()
            .find(|event| {
                event.event_type == crate::events::EventType::OperationCompleted
                    && event
                        .payload
                        .get("operation")
                        .and_then(serde_json::Value::as_str)
                        == Some("ci_fix")
            })
            .unwrap();
        let operation = OperationCompleted::from_event(&event).unwrap();
        assert_eq!(operation.outcome, OperationOutcome::Fallback);
        assert_eq!(operation.started_at, "2026-07-27T12:00:00.000Z");
        assert_eq!(operation.ended_at, "2026-07-27T12:00:01.700Z");
        assert_eq!(operation.duration_ms, 1_000);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn archive_ci_preflight_and_reconfirmation_are_distinct_retry_safe_ledger_boundaries() {
        let work = temp_work("archive-ci-reconfirmation-ledger");
        let mut state = imported_ready_state();
        state.phase = Phase::Cleaning;
        state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Published;
        state.integration.merged_tasks.insert("T-1".into());
        state.integration.published_head = Some("published-head".into());
        state.integration.publication_pushed = Some(true);
        state.integration.ci_disposition = Some(crate::processor::CiDisposition::Confirmed);
        state.integration.cleanup_journaled = true;

        {
            let mut runtime = ProcessorRuntime::import_legacy(config(), &work, state).unwrap();
            let effects = runtime
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .unwrap();
            assert_eq!(
                effects,
                vec![Effect::PersistCheckpoint, Effect::PrepareArchival]
            );
            assert_eq!(
                runtime.recovery_requirements(),
                vec![RecoveryRequirement::RetryIdempotently {
                    key: "prepare-archival".into(),
                    effect: Effect::PrepareArchival,
                }]
            );
            runtime
                .apply(ProcessorCommand::ArchivalPrepared {
                    outcome: crate::processor::ArchivalPreparationOutcome::ReconfirmRequired {
                        required_checks: vec!["validate".into()],
                    },
                })
                .unwrap();
            assert!(
                runtime
                    .pending_effects()
                    .contains_key("reconfirm-ci-before-archive")
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::RetryIdempotently {
                key: "reconfirm-ci-before-archive".into(),
                effect: Effect::ReconfirmCiBeforeArchive {
                    head: "published-head".into(),
                    required_checks: vec!["validate".into()],
                },
            }]
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn outstanding_effect_blocks_an_unrelated_transition() {
        let work = temp_work("gate");
        let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
        open(&mut runtime);
        runtime
            .apply(ProcessorCommand::Admit {
                candidates: vec![AdmissionCandidate {
                    id: "T-1".into(),
                    conflict_domain: "engine/**".into(),
                    level: Level::Coder,
                    risk: crate::resolvers::Risk::Medium,
                    ready: true,
                    current_delivery_lane: true,
                }],
                now_secs: 2,
            })
            .unwrap();
        assert!(
            runtime
                .pending_effects()
                .contains_key("ensure-workspace:T-1")
        );
        assert!(matches!(
            runtime.apply(ProcessorCommand::Advance { now_secs: 3 }),
            Err(RuntimeError::PendingEffect(_))
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn planner_dispatch_is_durable_and_requires_inspection_after_a_crash() {
        let work = temp_work("planner-ledger");
        {
            let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
            open(&mut runtime);
            assert_eq!(
                runtime.pending_effects().get("plan-next-wave"),
                Some(&Effect::PlanNextWave { free_slots: 1 })
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "plan-next-wave".into(),
                effect: Effect::PlanNextWave { free_slots: 1 },
            }],
            "a restart cannot launch a second planner while the prior result is unknown"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn checkpointed_publication_reanchor_is_explicitly_retry_safe_after_a_crash() {
        let work = temp_work("publication-reanchor-ledger");
        let mut state = imported_ready_state();
        state.phase = Phase::Publishing;
        state.integration.workspace_prepared = true;
        state.integration.merged_tasks.insert("T-1".into());
        state.integration.integration_head = Some("integration-head".into());
        state.integration.verification_head = Some("integration-head".into());
        state.integration.publication_reanchor_reason =
            Some("origin/main advanced during typed push".into());
        state.integration.publication_reanchor_target =
            Some(crate::processor::PublicationReanchorTarget::RemotePublication);
        state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Merged;

        {
            let mut runtime = ProcessorRuntime::import_legacy(config(), &work, state).unwrap();
            assert_eq!(
                runtime
                    .apply(ProcessorCommand::Recover {
                        workspaces_present: BTreeSet::new(),
                    })
                    .unwrap(),
                vec![
                    Effect::PersistCheckpoint,
                    Effect::ReanchorPublication {
                        batch_id: "B-1".into(),
                    },
                ]
            );
            assert_eq!(
                runtime.pending_effects().get("reanchor-publication"),
                Some(&Effect::ReanchorPublication {
                    batch_id: "B-1".into(),
                })
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::RetryIdempotently {
                key: "reanchor-publication".into(),
                effect: Effect::ReanchorPublication {
                    batch_id: "B-1".into(),
                },
            }],
            "the VCS/control-plane re-anchor has an exact idempotent postcondition"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn checkpointed_merger_conflict_requires_inspection_and_keeps_distinct_ledger_keys() {
        let work = temp_work("merge-resolution-ledger");
        let mut state = imported_ready_state();
        state.integration.workspace_prepared = true;
        state.integration.integration_head = Some("integration-head".into());
        state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::ResolvingMerge;
        state.integration.pending_merge_resolution = Some(MergeResolutionRuntime {
            task_id: "T-1".into(),
            pre_merge_head: "integration-head".into(),
            merge_paths: vec!["engine/src/lib.rs".into()],
            paths: vec!["engine/src/lib.rs".into()],
            protected_paths: Vec::new(),
        });

        {
            let mut runtime = ProcessorRuntime::import_legacy(config(), &work, state)
                .expect("write pending merge checkpoint");
            let effects = runtime
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .expect("recovery keeps the checkpointed merger dispatch visible");
            assert!(matches!(
                effects.as_slice(),
                [
                    Effect::PersistCheckpoint,
                    Effect::DispatchIntegration {
                        kind: LeafKind::Merger
                    }
                ]
            ));
            assert!(
                runtime
                    .pending_effects()
                    .contains_key("dispatch-integration:merger")
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).expect("reload pending merger");
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "dispatch-integration:merger".into(),
                effect: Effect::DispatchIntegration {
                    kind: LeafKind::Merger,
                },
            }],
            "a restart must not run a second merger against an intentionally dirty integration workspace"
        );
        assert_eq!(
            resumed
                .command_acknowledgement_key(&ProcessorCommand::MergeResolution {
                    task_id: "T-1".into(),
                    outcome: LeafOutcome::Completed { author: None },
                })
                .expect("merger acknowledgement key"),
            Some("dispatch-integration:merger".into())
        );
        assert_eq!(
            ProcessorRuntime::effect_key(&Effect::FinalizeMergeResolution {
                task_id: "T-1".into(),
            }),
            Some("finalize-merge-resolution:T-1".into())
        );
        assert_eq!(
            ProcessorRuntime::effect_key(&Effect::AbortMergeResolution {
                task_id: "T-1".into(),
                reason: "unresolved".into(),
            }),
            Some("abort-merge-resolution:T-1".into())
        );
        assert_eq!(
            ProcessorRuntime::effect_key(&Effect::Notify {
                event: crate::notification::NotificationEvent::PublishCiFailed,
                subject: "published-head".into(),
            }),
            Some("notify:publish.ci_failed:published-head".into())
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn inbox_curator_dispatch_is_durable_and_requires_inspection_after_a_crash() {
        let work = temp_work("inbox-curator-ledger");
        {
            let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
            runtime
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .unwrap();
            runtime.complete_effect("write-journal-and-status").unwrap();
            runtime
                .apply(ProcessorCommand::Open {
                    batch_id: "B-1".into(),
                    base: "base".into(),
                    now_secs: 1,
                })
                .unwrap();
            runtime
                .apply(ProcessorCommand::DependencyGraphRefreshed {
                    boundary: crate::dependency_graph::RefreshBoundary::CohortOpen,
                    outcome: LeafOutcome::Completed { author: None },
                })
                .unwrap();
            runtime
                .apply(ProcessorCommand::InboxReconciled {
                    free_slots: 1,
                    curation_required: true,
                })
                .unwrap();
            assert!(
                runtime
                    .pending_effects()
                    .contains_key("dispatch-inbox-curator:intake")
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "dispatch-inbox-curator:intake".into(),
                effect: Effect::DispatchInboxCurator {
                    free_slots: 1,
                    mode: crate::processor::InboxCurationMode::Intake,
                },
            }],
            "a restart cannot invoke a second curator while the prior cross-project result is unknown"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn dependency_curator_dispatch_is_durable_and_requires_inspection_after_a_crash() {
        let work = temp_work("dependency-curator-ledger");
        {
            let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
            runtime
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .unwrap();
            runtime.complete_effect("write-journal-and-status").unwrap();
            runtime
                .apply(ProcessorCommand::Open {
                    batch_id: "B-1".into(),
                    base: "base".into(),
                    now_secs: 1,
                })
                .unwrap();
            assert!(
                runtime
                    .pending_effects()
                    .contains_key("dispatch-dependency-curator:cohort-open")
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "dispatch-dependency-curator:cohort-open".into(),
                effect: Effect::DispatchDependencyCurator {
                    boundary: crate::dependency_graph::RefreshBoundary::CohortOpen,
                },
            }],
            "a restart cannot run a second graph curator while the prior registry mutation is unknown"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn post_archive_dependency_curator_is_inspect_first_after_a_durable_crash() {
        let work = temp_work("post-archive-dependency-curator-ledger");
        {
            let mut runtime =
                ProcessorRuntime::import_legacy(config(), &work, post_archive_cleaning_state())
                    .expect("write post-archive cleanup checkpoint");
            runtime
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .expect("resume only guarded physical cleanup");
            runtime
                .complete_effect("cleanup-integration-workspace")
                .expect("acknowledge guarded integration workspace cleanup");
            runtime
                .complete_effect("cleanup-cohort-control-plane")
                .expect("acknowledge guarded cohort-control cleanup");
            let effects = runtime
                .apply(ProcessorCommand::CleanupComplete)
                .expect("schedule post-archive dependency curator only after physical cleanup");
            assert!(matches!(
                effects.as_slice(),
                [
                    Effect::PersistCheckpoint,
                    Effect::DispatchDependencyCurator {
                        boundary: crate::dependency_graph::RefreshBoundary::PostArchive,
                    }
                ]
            ));
            assert!(
                runtime
                    .pending_effects()
                    .contains_key("dispatch-dependency-curator:post-archive")
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "dispatch-dependency-curator:post-archive".into(),
                effect: Effect::DispatchDependencyCurator {
                    boundary: crate::dependency_graph::RefreshBoundary::PostArchive,
                },
            }],
            "the post-archive graph curator must never be replayed after a crash without native inspection"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn post_archive_final_inbox_curator_is_inspect_first_after_a_durable_crash() {
        let work = temp_work("post-archive-final-inbox-curator-ledger");
        {
            let mut runtime =
                ProcessorRuntime::import_legacy(config(), &work, post_archive_cleaning_state())
                    .expect("write post-archive cleanup checkpoint");
            runtime
                .apply(ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                })
                .expect("resume only guarded physical cleanup");
            runtime
                .complete_effect("cleanup-integration-workspace")
                .unwrap();
            runtime
                .complete_effect("cleanup-cohort-control-plane")
                .unwrap();
            runtime.apply(ProcessorCommand::CleanupComplete).unwrap();
            runtime
                .apply(ProcessorCommand::DependencyGraphRefreshed {
                    boundary: crate::dependency_graph::RefreshBoundary::PostArchive,
                    outcome: LeafOutcome::Completed { author: None },
                })
                .expect("acknowledge the completed post-archive graph curator");
            assert!(
                runtime
                    .pending_effects()
                    .contains_key("reconcile-inbox-finalization")
            );
            runtime
                .apply(ProcessorCommand::InboxFinalizationReconciled {
                    curation_required: true,
                })
                .expect("schedule the final inbox curator");
            assert!(
                runtime
                    .pending_effects()
                    .contains_key("dispatch-inbox-curator:finalize")
            );
        }

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "dispatch-inbox-curator:finalize".into(),
                effect: Effect::DispatchInboxCurator {
                    free_slots: 0,
                    mode: crate::processor::InboxCurationMode::Finalize,
                },
            }],
            "the final post-archive inbox curator must not be replayed after a crash without native inspection"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn legacy_import_persists_phase_zero_and_never_overwrites_a_runtime_ledger() {
        let work = temp_work("legacy-import");
        let imported = ProcessorRuntime::import_legacy(config(), &work, imported_ready_state())
            .expect("write imported checkpoint");
        assert_eq!(imported.state().phase, Phase::Recovery);
        assert!(imported.pending_effects().is_empty());
        assert!(matches!(
            ProcessorRuntime::import_legacy(config(), &work, imported_ready_state()),
            Err(RuntimeError::ExistingCheckpoint(_))
        ));
        drop(imported);

        let mut resumed = ProcessorRuntime::resume(config(), &work).unwrap();
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
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn legacy_import_binds_current_safety_snapshot_before_checkpoint_validation() {
        let work = temp_work("legacy-import-safety-snapshot");
        let import_config = ProcessorConfig {
            cohort_budget_secs: Some(600),
            cohort_token_budget: Some(100),
            events_outbox_enabled: false,
            ..config()
        };

        let imported =
            ProcessorRuntime::import_legacy(import_config, &work, imported_ready_state())
                .expect("legacy state has no native snapshot but must adopt the selected policy");
        let batch = imported.state().batch.as_ref().unwrap();
        assert_eq!(batch.cohort_budget_secs, Some(600));
        assert_eq!(batch.cohort_token_budget, Some(100));
        assert!(!batch.events_outbox_enabled);

        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn imported_conflict_return_clears_its_marker_with_the_effect_acknowledgement() {
        let work = temp_work("legacy-conflict-return");
        let mut runtime =
            ProcessorRuntime::import_legacy(config(), &work, imported_publishing_conflict_state())
                .expect("write imported conflict checkpoint");
        let effects = runtime
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .expect("schedule imported conflict return");
        let return_effect = effects
            .iter()
            .find(|effect| matches!(effect, Effect::ReturnTask { task_id, .. } if task_id == "T-2"))
            .cloned()
            .expect("recovery schedules the exact queue return");
        assert!(runtime.pending_effects().contains_key("return-task:T-2"));
        assert!(
            runtime
                .pending_effects()
                .contains_key("dispatch-integration:integration-review")
        );

        runtime
            .acknowledge_effect(&return_effect)
            .expect("persist completed queue return");
        assert!(!runtime.pending_effects().contains_key("return-task:T-2"));
        assert_eq!(runtime.state().tasks["T-2"].imported_recovery_intent, None);

        drop(runtime);
        let resumed = ProcessorRuntime::resume(config(), &work).expect("reload reduced ledger");
        assert_eq!(
            resumed.state().tasks["T-2"].imported_recovery_intent,
            None,
            "a restart must not increment the persisted quarantine counter a second time"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn acknowledgement_replaces_a_leaf_with_its_next_durable_effect() {
        let work = temp_work("ack");
        let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
        open(&mut runtime);
        runtime
            .apply(ProcessorCommand::Admit {
                candidates: vec![AdmissionCandidate {
                    id: "T-1".into(),
                    conflict_domain: "engine/**".into(),
                    level: Level::Coder,
                    risk: crate::resolvers::Risk::Medium,
                    ready: true,
                    current_delivery_lane: true,
                }],
                now_secs: 2,
            })
            .unwrap();
        runtime
            .apply(ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            })
            .unwrap();
        assert!(
            !runtime
                .pending_effects()
                .contains_key("ensure-workspace:T-1")
        );
        assert!(
            runtime
                .pending_effects()
                .contains_key("prepare-task-leaf:T-1:implement")
        );
        runtime
            .apply(ProcessorCommand::TaskLeafPrepared {
                task_id: "T-1".into(),
                outcome: crate::processor::TaskLeafPreparationOutcome::Skipped,
            })
            .unwrap();
        assert!(
            runtime
                .pending_effects()
                .contains_key("dispatch-task:T-1:implement")
        );
        runtime
            .apply(ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: LeafOutcome::Completed { author: None },
            })
            .unwrap();
        assert!(runtime.pending_effects().contains_key("commit-task:T-1"));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn restart_preserves_unknown_leaf_as_an_inspection_requirement() {
        let work = temp_work("resume");
        {
            let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
            open(&mut runtime);
            runtime
                .apply(ProcessorCommand::Admit {
                    candidates: vec![AdmissionCandidate {
                        id: "T-1".into(),
                        conflict_domain: "engine/**".into(),
                        level: Level::Coder,
                        risk: crate::resolvers::Risk::Medium,
                        ready: true,
                        current_delivery_lane: true,
                    }],
                    now_secs: 2,
                })
                .unwrap();
            runtime
                .apply(ProcessorCommand::WorkspaceReady {
                    task_id: "T-1".into(),
                })
                .unwrap();
        }
        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(resumed.state().phase, Phase::Recovery);
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "prepare-task-leaf:T-1:implement".into(),
                effect: Effect::PrepareTaskLeaf {
                    task_id: "T-1".into(),
                    kind: LeafKind::Implement,
                },
            }]
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn transition_events_are_durable_before_the_checkpointed_state() {
        let work = temp_work("events");
        let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                },
                "2026-07-24T12:00:00Z",
            )
            .unwrap();
        // Idle recovery writes observable status but keeps the engine eligible to open the next
        // cohort under the same outer lease; releasing here would leave an unrelated durable
        // `release-lease` acknowledgement blocking `Open`.
        runtime.complete_effect("write-journal-and-status").unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Open {
                    batch_id: "B-1".into(),
                    base: "base".into(),
                    now_secs: 1,
                },
                "2026-07-24T12:00:01Z",
            )
            .unwrap();

        let lines = fs::read_to_string(work.join(crate::events::OUTBOX_FILE)).unwrap();
        let events = lines
            .lines()
            .map(crate::events::parse_line)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            crate::events::fingerprint_identities(&events),
            vec!["cohort.opened|B-1||".to_string()]
        );
        assert_eq!(events[0].occurred_at, "2026-07-24T12:00:01Z");
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn interrupted_logical_lease_release_is_retry_safe() {
        let work = temp_work("release-recovery");
        let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
        runtime
            .pending
            .insert("release-lease".into(), Effect::ReleaseLease);
        runtime
            .persist(
                &runtime.processor,
                &runtime.pending,
                &runtime.ci_fix_provider_span,
            )
            .unwrap();
        drop(runtime);

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::RetryIdempotently {
                key: "release-lease".into(),
                effect: Effect::ReleaseLease,
            }]
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn read_only_budget_ci_and_knowledge_boundaries_are_retry_safe() {
        let work = temp_work("read-only-recovery");
        let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
        let budget = Effect::CheckCohortBudget {
            next: crate::processor::ModelCall::Planner { free_slots: 1 },
        };
        let ci = Effect::VerifyCi {
            head: "published-head".into(),
        };
        let knowledge = Effect::PrepareKnowledgeCuration;
        for effect in [&budget, &ci, &knowledge] {
            runtime
                .pending
                .insert(effect_key(effect).unwrap(), effect.clone());
        }
        runtime
            .persist(
                &runtime.processor,
                &runtime.pending,
                &runtime.ci_fix_provider_span,
            )
            .unwrap();
        drop(runtime);

        let resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        for effect in [budget, ci, knowledge] {
            assert!(resumed.recovery_requirements().iter().any(|requirement| {
                matches!(
                    requirement,
                    RecoveryRequirement::RetryIdempotently {
                        effect: candidate,
                        ..
                    } if candidate == &effect
                )
            }));
        }
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn pending_knowledge_preflight_supersedes_reconstructed_cleanup_preflights() {
        let work = temp_work("knowledge-preflight-recovery");
        let mut state = imported_ready_state();
        state.phase = Phase::Cleaning;
        state.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Published;
        state.integration.merged_tasks.insert("T-1".into());
        state.integration.published_head = Some("published-head".into());
        state.integration.publication_pushed = Some(false);
        state.integration.ci_disposition = Some(crate::processor::CiDisposition::Disabled);
        bind_legacy_safety_snapshot(&mut state, &config());
        let store = CheckpointStore::for_file(&work, RUNTIME_CHECKPOINT_FILE).unwrap();
        store
            .save_json(&RuntimeCheckpoint {
                schema_version: RUNTIME_STATE_VERSION,
                processor: state,
                pending: BTreeMap::from([(
                    "prepare-knowledge-curation".into(),
                    Effect::PrepareKnowledgeCuration,
                )]),
                ci_fix_provider_span: None,
            })
            .unwrap();

        let mut resumed = ProcessorRuntime::resume(config(), &work).unwrap();
        let effects = resumed
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        assert!(effects.contains(&Effect::WriteJournalAndStatus));
        assert!(effects.contains(&Effect::PrepareArchival));
        resumed
            .supersede_recovery_effects_with_pending(
                &Effect::PrepareKnowledgeCuration,
                &[Effect::WriteJournalAndStatus, Effect::PrepareArchival],
            )
            .unwrap();
        assert_eq!(
            resumed.pending_effects(),
            &BTreeMap::from([(
                "prepare-knowledge-curation".into(),
                Effect::PrepareKnowledgeCuration,
            )])
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn runtime_refuses_an_unvalidated_event_time_before_mutating_state() {
        let work = temp_work("bad-event-time");
        let mut runtime = ProcessorRuntime::new(config(), &work).unwrap();
        assert!(matches!(
            runtime.apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                },
                "not-a-timestamp",
            ),
            Err(RuntimeError::InvalidEventTime(_))
        ));
        assert_eq!(runtime.state().phase, Phase::Recovery);
        assert!(!work.join(crate::events::OUTBOX_FILE).exists());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn resume_rejects_a_ledger_key_that_does_not_match_its_effect() {
        let work = temp_work("bad-ledger-key");
        let store = CheckpointStore::for_file(&work, RUNTIME_CHECKPOINT_FILE).unwrap();
        store
            .save_json(&RuntimeCheckpoint {
                schema_version: RUNTIME_STATE_VERSION,
                processor: ProcessorState::default(),
                pending: BTreeMap::from([(
                    "ensure-workspace:T-2".into(),
                    Effect::EnsureTaskWorkspace {
                        task_id: "T-1".into(),
                        branch: "task/T-1".into(),
                    },
                )]),
                ci_fix_provider_span: None,
            })
            .unwrap();
        assert!(matches!(
            ProcessorRuntime::resume(config(), &work),
            Err(RuntimeError::CorruptCheckpoint(message)) if message.contains("does not match effect key")
        ));
        let _ = fs::remove_dir_all(work);
    }
}
