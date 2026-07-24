//! Exhaustive native effect adapter for the deterministic processor.
//!
//! [`crate::processor::Processor`] owns every phase decision. This module owns the other half of
//! the contract: no effect is allowed to disappear into an ad-hoc run-loop branch. A concrete
//! port supplies VCS, headless-agent, forge, and control-plane evidence; this adapter turns it
//! into exactly the command (or durable acknowledgement) the reducer permits.

use std::collections::BTreeSet;
use std::fmt;

use crate::dependency_graph::RefreshBoundary;
use crate::execution::{EffectExecutor, EffectResolution};
use crate::notification::NotificationEvent;
use crate::processor::{
    AdmissionCandidate, ArchivalPreparationOutcome, CiFixPreparationOutcome, CiOutcome, Effect,
    InboxCurationMode, KnowledgeCurationPreparationOutcome, LeafKind, LeafOutcome, MergeOutcome,
    ProcessorCommand, ProcessorState, PublicationReanchorTarget, ReviewOutcome,
    TaskLeafPreparationOutcome, TaskReviewPreparationOutcome, TokenBudgetObservation,
    VerificationOutcome,
};
use crate::supervise::CancellationProbe;

/// Result of inspecting an interrupted operation during phase-0 recovery. A recovery adapter may
/// return an already-recorded reducer command only when it proves the original mutation's result;
/// it otherwise holds for an operator. It must never re-run an unknown leaf/merge/publish here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciliation {
    Command(ProcessorCommand),
    Hold { reason: String },
}

/// Read-only state of the normal delivery lane at a cohort boundary. The scheduler must make
/// this decision from the durable queue, not from a speculative planner call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueReadiness {
    /// At least one current-lane queue row is still eligible to enter a future cohort.
    Pending,
    /// No current-lane task remains `not-started`; terminal escalations are reported separately
    /// so the operator does not mistake a stopped queue for a cleanly completed one.
    Exhausted { escalated: usize },
}

/// One task-local model boundary eligible for Phase-2 concurrent fan-out.  These variants are
/// deliberately narrower than [`Effect`]: worktree creation, VCS commits, and control-plane
/// writes stay serial so a batch never races a shared authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskEffect {
    PrepareLeaf { task_id: String, kind: LeafKind },
    PrepareReview { task_id: String },
    DispatchLeaf { task_id: String, kind: LeafKind },
    DispatchReview { task_id: String },
}

impl TaskEffect {
    fn from_effect(effect: &Effect) -> Option<Self> {
        match effect {
            Effect::PrepareTaskLeaf { task_id, kind } => Some(Self::PrepareLeaf {
                task_id: task_id.clone(),
                kind: *kind,
            }),
            Effect::PrepareTaskReview { task_id } => Some(Self::PrepareReview {
                task_id: task_id.clone(),
            }),
            Effect::DispatchTask {
                task_id,
                kind: LeafKind::Review,
            } => Some(Self::DispatchReview {
                task_id: task_id.clone(),
            }),
            Effect::DispatchTask { task_id, kind } => Some(Self::DispatchLeaf {
                task_id: task_id.clone(),
                kind: *kind,
            }),
            _ => None,
        }
    }

    fn into_resolution(self, result: TaskEffectResult) -> Result<EffectResolution, String> {
        match (self, result) {
            (Self::PrepareLeaf { task_id, .. }, TaskEffectResult::LeafPrepared { outcome }) => Ok(
                EffectResolution::Command(ProcessorCommand::TaskLeafPrepared { task_id, outcome }),
            ),
            (Self::PrepareReview { task_id }, TaskEffectResult::ReviewPrepared { outcome }) => {
                Ok(EffectResolution::Command(
                    ProcessorCommand::TaskReviewPrepared { task_id, outcome },
                ))
            }
            (Self::DispatchLeaf { task_id, .. }, TaskEffectResult::Leaf { outcome }) => {
                Ok(EffectResolution::Command(ProcessorCommand::TaskLeaf {
                    task_id,
                    outcome,
                }))
            }
            (Self::DispatchReview { task_id }, TaskEffectResult::Review { outcome }) => {
                Ok(EffectResolution::Command(ProcessorCommand::TaskReview {
                    task_id,
                    outcome,
                }))
            }
            (effect, result) => Err(format!(
                "task batch returned {result:?} for incompatible request {effect:?}"
            )),
        }
    }
}

/// Structured result for a [`TaskEffect`].  Keeping the phase-specific outcome tagged prevents
/// a worker from acknowledging another task's durable ledger key with an accidentally compatible
/// string result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskEffectResult {
    LeafPrepared {
        outcome: TaskLeafPreparationOutcome,
    },
    ReviewPrepared {
        outcome: TaskReviewPreparationOutcome,
    },
    Leaf {
        outcome: LeafOutcome,
    },
    Review {
        outcome: ReviewOutcome,
    },
}

/// Result of the irreversible publication boundary. A pending policy hold leaves the `Publish`
/// ledger entry outstanding *before* the primary checkout changes. A terminal decision instead
/// acknowledges that effect through a reducer command which escalates only this cohort; neither
/// path can accidentally turn a missing human approval into a local fast-forward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationResult {
    Published {
        head: String,
        pushed: bool,
    },
    Hold {
        reason: String,
    },
    Rejected {
        reason: String,
    },
    ReanchorRequired {
        reason: String,
        target: PublicationReanchorTarget,
    },
}

/// Result of the explicitly checkpointed recovery path after a rejected remote push.  A
/// concurrent successful publication is still resolved as `Published` so its exact remote proof
/// can take the normal CI/accounting path; otherwise the reducer restores the pre-merge task
/// candidates and starts a fresh integration from the fetched primary base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationReanchorResult {
    Reanchored,
    Published { head: String },
}

/// Impure services required by every processor effect. Implementations must use ProcessKit for
/// child processes and `vcs-*` for VCS/forge calls; the adapter deliberately has no shell-string
/// escape hatch. Each method is named after the evidence it must return, not an agent's prose.
pub trait ProcessorPort {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Whether this concrete port can safely re-execute only `PrepareTaskLeaf` and
    /// `PrepareTaskReview` after Phase-0 restored their outstanding ledger keys. Actual Claude
    /// dispatches and every other model effect remain inspect-first.
    fn task_preparation_replay_safe(&self) -> bool {
        false
    }

    /// Whether notification effects are protected by a durable pre-launch at-most-once claim.
    /// The production file/VCS port owns that receipt; arbitrary embedders remain inspect-first.
    fn notification_replay_safe(&self) -> bool {
        false
    }

    /// Observe the operator-owned pause marker at a deterministic phase/round boundary.
    /// Implementations must only inspect the marker; they must not clear it or create a
    /// replacement checkpoint.  A pause intentionally lets an already-started leaf complete.
    fn pause_requested(&mut self) -> Result<bool, Self::Error> {
        Ok(false)
    }

    /// Read the normal (`current`) delivery lane at a scheduler boundary. This deliberately does
    /// not ask a planner to manufacture a cohort merely to discover that the queue is exhausted.
    /// A malformed, unreadable, or unexpectedly non-terminal idle queue row must be returned as
    /// an error rather than treated as completion.
    fn current_queue_readiness(&mut self) -> Result<QueueReadiness, Self::Error>;
    /// Return the explicit control-plane clock used for admission/budget commands. Implementors
    /// may source it from an injected clock or a persisted run input, but must not make reducer
    /// event identities depend on an implicit leaf completion timestamp.
    fn now_secs(&mut self) -> Result<u64, Self::Error>;
    /// Return the explicit timestamp for the next lifecycle projection. Production ports sample
    /// wall time here; deterministic tests and embedders may retain the supplied fallback.
    fn event_occurred_at(&mut self, fallback: &str) -> Result<String, Self::Error> {
        Ok(fallback.to_owned())
    }
    /// Physical workspaces proved during phase-0 inspection. A missing workspace remains a
    /// reducer-level blocker; this port must not recreate it before recovery has decided so.
    fn recovery_workspaces(
        &mut self,
        state: &ProcessorState,
    ) -> Result<BTreeSet<String>, Self::Error>;
    fn reconcile(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<Reconciliation, Self::Error>;
    /// Complete the mechanical `inbox reconcile` + `actionable` boundary. The boolean means a
    /// validated record requires the curator; it is never inferred from untrusted prose by the
    /// reducer.
    fn reconcile_inbox(&mut self, state: &ProcessorState) -> Result<bool, Self::Error>;
    /// Reconcile after Phase-6 archival and determine whether a terminal inbox conversation
    /// still needs its durable `implemented` transition or an idempotent final reply. This must
    /// not inspect new/unresolved intake records, because cleanup cannot admit more work.
    fn reconcile_inbox_finalization(&mut self, state: &ProcessorState)
    -> Result<bool, Self::Error>;
    /// Build and atomically synchronize the registered project's graph snapshot for exactly one
    /// durable Phase-1.3 or Phase-6.7 boundary.
    fn refresh_dependency_graph(
        &mut self,
        boundary: RefreshBoundary,
        state: &ProcessorState,
    ) -> Result<LeafOutcome, Self::Error>;
    /// Run the narrowly scoped cross-project inbox curator under the active owner lease.
    fn curate_inbox(
        &mut self,
        mode: InboxCurationMode,
        state: &ProcessorState,
    ) -> Result<LeafOutcome, Self::Error>;
    /// Consume the local queue inbox and recover `msg-*` provenance before planning. This is
    /// retry-safe only because the concrete port owns its journaled transaction.
    fn drain_queue_inbox(&mut self, state: &ProcessorState) -> Result<(), Self::Error>;
    fn plan_candidates(
        &mut self,
        state: &ProcessorState,
        free_slots: usize,
    ) -> Result<Vec<AdmissionCandidate>, Self::Error>;
    /// Return the post-charge actual-token observation for the active cohort. Implementations
    /// may return [`TokenBudgetObservation::Unavailable`] for any telemetry read/parse failure;
    /// the reducer turns that into a durable safe halt rather than starting a model call.
    fn token_budget_observation(
        &mut self,
        state: &ProcessorState,
    ) -> Result<TokenBudgetObservation, Self::Error>;
    fn ensure_task_workspace(
        &mut self,
        task_id: &str,
        branch: &str,
        state: &ProcessorState,
    ) -> Result<(), Self::Error>;
    fn task_leaf(
        &mut self,
        task_id: &str,
        kind: LeafKind,
        state: &ProcessorState,
    ) -> Result<LeafOutcome, Self::Error>;
    /// Run the optional Codex maker before the separately dispatched Claude fallback.
    fn prepare_task_leaf(
        &mut self,
        task_id: &str,
        kind: LeafKind,
        state: &ProcessorState,
    ) -> Result<TaskLeafPreparationOutcome, Self::Error>;
    /// Run the optional diversity-review leaf, if the persisted route requires one. The
    /// authoritative task review remains a separate effect so the token gate can sit between
    /// the two model calls.
    fn prepare_task_review(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<TaskReviewPreparationOutcome, Self::Error>;
    fn task_review(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<ReviewOutcome, Self::Error>;
    /// Run one wave of independent task-local model calls.  Ports without their own concurrent
    /// ProcessKit worker retain deterministic serial behaviour through this default.  An
    /// implementation that does fan out must return answers in request order and may only run
    /// the supplied task-local calls; the driver applies reducer commands after the collection.
    fn execute_task_batch(
        &mut self,
        effects: &[TaskEffect],
        state: &ProcessorState,
    ) -> Result<Vec<TaskEffectResult>, Self::Error> {
        effects
            .iter()
            .map(|effect| match effect {
                TaskEffect::PrepareLeaf { task_id, kind } => self
                    .prepare_task_leaf(task_id, *kind, state)
                    .map(|outcome| TaskEffectResult::LeafPrepared { outcome }),
                TaskEffect::PrepareReview { task_id } => self
                    .prepare_task_review(task_id, state)
                    .map(|outcome| TaskEffectResult::ReviewPrepared { outcome }),
                TaskEffect::DispatchLeaf { task_id, kind } => self
                    .task_leaf(task_id, *kind, state)
                    .map(|outcome| TaskEffectResult::Leaf { outcome }),
                TaskEffect::DispatchReview { task_id } => self
                    .task_review(task_id, state)
                    .map(|outcome| TaskEffectResult::Review { outcome }),
            })
            .collect()
    }
    fn commit_task(&mut self, task_id: &str, state: &ProcessorState)
    -> Result<String, Self::Error>;
    fn ensure_integration_workspace(
        &mut self,
        branch: &str,
        state: &ProcessorState,
    ) -> Result<(), Self::Error>;
    fn merge_task(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<MergeOutcome, Self::Error>;
    /// Run the checkpointed merger leaf against one already-started typed conflict merge.
    fn resolve_merge_conflict(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<LeafOutcome, Self::Error>;
    /// Complete the typed merge after a successful merger leaf, including the same per-merge
    /// verification and rollback boundary used for automatically clean merges.
    fn finalize_merge_resolution(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<MergeOutcome, Self::Error>;
    /// Abort the typed in-progress merge before turning the task into a queue quarantine.
    fn abort_merge_resolution(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<(), Self::Error>;
    /// Execute the Phase-4 profile against the exact integration tip before review/publication.
    fn verify_integration(
        &mut self,
        head: &str,
        state: &ProcessorState,
    ) -> Result<VerificationOutcome, Self::Error>;
    fn integration_review(&mut self, state: &ProcessorState) -> Result<ReviewOutcome, Self::Error>;
    fn integration_fix(&mut self, state: &ProcessorState) -> Result<LeafOutcome, Self::Error>;
    fn commit_integration_fix(&mut self, state: &ProcessorState) -> Result<String, Self::Error>;
    fn publish(
        &mut self,
        batch_id: &str,
        state: &ProcessorState,
    ) -> Result<PublicationResult, Self::Error>;
    /// Re-anchor a publication candidate only after [`PublicationResult::ReanchorRequired`] was
    /// durably acknowledged. This operation is retry-safe: a restart may repeat it after any
    /// VCS/control-plane sub-step, but it must never reset a remote branch that is merely behind
    /// or otherwise indeterminate.
    fn reanchor_publication(
        &mut self,
        batch_id: &str,
        state: &ProcessorState,
    ) -> Result<PublicationReanchorResult, Self::Error>;
    fn verify_ci(&mut self, head: &str, state: &ProcessorState) -> Result<CiOutcome, Self::Error>;
    fn reconfirm_ci_before_archive(
        &mut self,
        head: &str,
        _required_checks: &[String],
        state: &ProcessorState,
    ) -> Result<CiOutcome, Self::Error> {
        self.verify_ci(head, state)
    }
    /// Best-effort operator notice for a durable processor boundary. Notification delivery is
    /// deliberately non-gating: an unavailable notifier must not change reducer state, retry a
    /// side effect, or prevent the following ordinary effect from running.
    fn notify(&mut self, _event: NotificationEvent, _subject: &str) -> Result<(), Self::Error> {
        Ok(())
    }
    fn prepare_ci_fix(
        &mut self,
        state: &ProcessorState,
    ) -> Result<CiFixPreparationOutcome, Self::Error>;
    fn ci_fix(&mut self, state: &ProcessorState) -> Result<LeafOutcome, Self::Error>;
    fn commit_ci_fix(&mut self, state: &ProcessorState) -> Result<String, Self::Error>;
    fn prepare_knowledge_curation(
        &mut self,
        _state: &ProcessorState,
    ) -> Result<KnowledgeCurationPreparationOutcome, Self::Error> {
        Ok(KnowledgeCurationPreparationOutcome::Required)
    }
    fn prepare_archival(
        &mut self,
        _state: &ProcessorState,
    ) -> Result<ArchivalPreparationOutcome, Self::Error> {
        Ok(ArchivalPreparationOutcome::Skipped)
    }
    fn curate_knowledge(&mut self, state: &ProcessorState) -> Result<LeafOutcome, Self::Error>;
    fn return_task(
        &mut self,
        task_id: &str,
        reason: &str,
        state: &ProcessorState,
    ) -> Result<(), Self::Error>;
    /// Persist a terminal escalation without turning it into a retry/quarantine queue row.
    fn escalate_task(
        &mut self,
        task_id: &str,
        reason: &str,
        state: &ProcessorState,
    ) -> Result<(), Self::Error>;
    fn archive_task(&mut self, task_id: &str, state: &ProcessorState) -> Result<(), Self::Error>;
    fn cleanup_task_workspace(
        &mut self,
        task_id: &str,
        state: &ProcessorState,
    ) -> Result<(), Self::Error>;
    fn cleanup_integration_workspace(&mut self, state: &ProcessorState) -> Result<(), Self::Error>;
    fn cleanup_cohort_control_plane(&mut self, state: &ProcessorState) -> Result<(), Self::Error>;
    fn write_journal_and_status(&mut self, state: &ProcessorState) -> Result<(), Self::Error>;
    /// Materialize the operator-facing pause boundary without changing the reducer state.
    /// Adapters that do not have a richer status projection retain the ordinary derived status.
    fn write_pause_status(&mut self, state: &ProcessorState) -> Result<(), Self::Error> {
        self.write_journal_and_status(state)
    }
    fn release_lease(&mut self) -> Result<(), Self::Error>;
}

/// Adapter error identifies a port failure separately from an unsupported/impossible leaf kind.
#[derive(Debug)]
pub enum NativeError<E> {
    Port(E),
    /// The owner-checked heartbeat observed that this process no longer owns the native lease.
    /// The current durable effect remains pending for Phase-0 inspection; it must not be
    /// acknowledged with a result gathered by a former owner.
    LeaseLost,
    InvalidEffect(String),
}

impl<E: fmt::Display> fmt::Display for NativeError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Port(error) => write!(f, "native processor port failed: {error}"),
            Self::LeaseLost => f.write_str(
                "native lease ownership was lost; refusing to dispatch or acknowledge an effect",
            ),
            Self::InvalidEffect(message) => f.write_str(message),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NativeError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Port(error) => Some(error),
            Self::LeaseLost | Self::InvalidEffect(_) => None,
        }
    }
}

/// One concrete, exhaustive [`EffectExecutor`] over a native port.
pub struct NativeExecutor<P> {
    port: P,
    cancellation_probe: Option<CancellationProbe>,
}

impl<P> NativeExecutor<P> {
    pub fn new(port: P) -> Self {
        Self {
            port,
            cancellation_probe: None,
        }
    }

    /// Bind the executor to the owner lease held by its caller. The probe is checked before
    /// dispatch and again before a result can acknowledge the durable effect ledger.
    pub fn with_cancellation_probe(mut self, cancellation_probe: CancellationProbe) -> Self {
        self.cancellation_probe = Some(cancellation_probe);
        self
    }

    pub fn port(&self) -> &P {
        &self.port
    }

    pub fn port_mut(&mut self) -> &mut P {
        &mut self.port
    }

    pub fn into_port(self) -> P {
        self.port
    }
}

impl<P: ProcessorPort> NativeExecutor<P> {
    /// Port-qualified exception to the runtime's conservative inspect-first classification. The
    /// generic runtime cannot assume an arbitrary embedding has durable Codex receipts; only the
    /// production headless port opts in, and only for the two preparation effects.
    pub fn interrupted_effect_retry_safe(&self, effect: &Effect) -> bool {
        (self.port.task_preparation_replay_safe()
            && matches!(
                effect,
                Effect::PrepareTaskLeaf { .. } | Effect::PrepareTaskReview { .. }
            ))
            || (self.port.notification_replay_safe() && matches!(effect, Effect::Notify { .. }))
    }
}

impl<P: ProcessorPort> EffectExecutor for NativeExecutor<P> {
    type Error = NativeError<P::Error>;

    fn event_occurred_at(&mut self, fallback: &str) -> Result<String, Self::Error> {
        NativeExecutor::event_occurred_at(self, fallback)
    }

    fn execute(
        &mut self,
        effect: &Effect,
        state: &ProcessorState,
    ) -> Result<EffectResolution, Self::Error> {
        self.ensure_lease_active()?;
        let port = &mut self.port;
        let resolution = match effect {
            Effect::Reconcile { task_id } => {
                match port.reconcile(task_id, state).map_err(NativeError::Port)? {
                    Reconciliation::Command(command) => EffectResolution::Reconciled(command),
                    Reconciliation::Hold { reason } => EffectResolution::Hold { reason },
                }
            }
            Effect::ReconcileInbox { free_slots } => {
                EffectResolution::Command(ProcessorCommand::InboxReconciled {
                    free_slots: *free_slots,
                    curation_required: port.reconcile_inbox(state).map_err(NativeError::Port)?,
                })
            }
            Effect::ReconcileInboxFinalization => {
                EffectResolution::Command(ProcessorCommand::InboxFinalizationReconciled {
                    curation_required: port
                        .reconcile_inbox_finalization(state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::DispatchDependencyCurator { boundary } => {
                EffectResolution::Command(ProcessorCommand::DependencyGraphRefreshed {
                    boundary: *boundary,
                    outcome: port
                        .refresh_dependency_graph(*boundary, state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::DispatchInboxCurator { free_slots, mode } => {
                EffectResolution::Command(ProcessorCommand::InboxCurated {
                    free_slots: *free_slots,
                    mode: *mode,
                    outcome: port.curate_inbox(*mode, state).map_err(NativeError::Port)?,
                })
            }
            Effect::DrainQueueInbox { free_slots } => {
                port.drain_queue_inbox(state).map_err(NativeError::Port)?;
                EffectResolution::Command(ProcessorCommand::InboxDrained {
                    free_slots: *free_slots,
                })
            }
            Effect::PlanNextWave { free_slots } => {
                EffectResolution::Command(ProcessorCommand::Admit {
                    candidates: port
                        .plan_candidates(state, *free_slots)
                        .map_err(NativeError::Port)?,
                    now_secs: port.now_secs().map_err(NativeError::Port)?,
                })
            }
            Effect::CheckTokenBudget { next } => {
                // An unavailable telemetry source is not a transport failure: legacy semantics
                // require this exact preflight to safe-halt the cohort before any model process
                // starts. Keep arbitrary adapter diagnostics out of the durable reason.
                let observation = port
                    .token_budget_observation(state)
                    .unwrap_or(TokenBudgetObservation::Unavailable);
                EffectResolution::Command(ProcessorCommand::TokenBudgetChecked {
                    next: next.clone(),
                    observation,
                })
            }
            Effect::CheckCohortBudget { next } => {
                EffectResolution::Command(ProcessorCommand::CohortBudgetChecked {
                    next: next.clone(),
                    now_secs: port.now_secs().map_err(NativeError::Port)?,
                })
            }
            Effect::EnsureTaskWorkspace { task_id, branch } => {
                match port.ensure_task_workspace(task_id, branch, state) {
                    Ok(()) => EffectResolution::Command(ProcessorCommand::WorkspaceReady {
                        task_id: task_id.clone(),
                    }),
                    Err(error) => EffectResolution::Command(ProcessorCommand::WorkspaceFailed {
                        task_id: task_id.clone(),
                        reason: error.to_string(),
                    }),
                }
            }
            Effect::PrepareTaskReview { task_id } => {
                EffectResolution::Command(ProcessorCommand::TaskReviewPrepared {
                    task_id: task_id.clone(),
                    outcome: port
                        .prepare_task_review(task_id, state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::PrepareTaskLeaf { task_id, kind } => {
                EffectResolution::Command(ProcessorCommand::TaskLeafPrepared {
                    task_id: task_id.clone(),
                    outcome: port
                        .prepare_task_leaf(task_id, *kind, state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::DispatchTask { task_id, kind } => match kind {
                LeafKind::Review => EffectResolution::Command(ProcessorCommand::TaskReview {
                    task_id: task_id.clone(),
                    outcome: port
                        .task_review(task_id, state)
                        .map_err(NativeError::Port)?,
                }),
                LeafKind::Implement | LeafKind::Fix => {
                    EffectResolution::Command(ProcessorCommand::TaskLeaf {
                        task_id: task_id.clone(),
                        outcome: port
                            .task_leaf(task_id, *kind, state)
                            .map_err(NativeError::Port)?,
                    })
                }
                other => {
                    return Err(NativeError::InvalidEffect(format!(
                        "task dispatch cannot use leaf kind {other:?}"
                    )));
                }
            },
            Effect::CommitTask { task_id } => {
                EffectResolution::Command(ProcessorCommand::TaskCommitted {
                    task_id: task_id.clone(),
                    commit: port
                        .commit_task(task_id, state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::PrepareIntegrationWorkspace { branch } => {
                port.ensure_integration_workspace(branch, state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Command(ProcessorCommand::IntegrationWorkspaceReady)
            }
            Effect::MergeTask { task_id } => {
                EffectResolution::Command(ProcessorCommand::TaskMerged {
                    task_id: task_id.clone(),
                    outcome: port.merge_task(task_id, state).map_err(NativeError::Port)?,
                })
            }
            Effect::FinalizeMergeResolution { task_id } => {
                EffectResolution::Command(ProcessorCommand::MergeResolutionFinalized {
                    task_id: task_id.clone(),
                    outcome: port
                        .finalize_merge_resolution(task_id, state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::AbortMergeResolution { task_id, reason } => {
                port.abort_merge_resolution(task_id, state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Command(ProcessorCommand::MergeResolutionAborted {
                    task_id: task_id.clone(),
                    reason: reason.clone(),
                })
            }
            Effect::VerifyIntegration { head } => {
                EffectResolution::Command(ProcessorCommand::IntegrationVerified {
                    head: head.clone(),
                    outcome: port
                        .verify_integration(head, state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::DispatchIntegration { kind } => match kind {
                LeafKind::Merger => {
                    let task_id = state
                        .integration
                        .pending_merge_resolution
                        .as_ref()
                        .map(|pending| pending.task_id.clone())
                        .ok_or_else(|| {
                            NativeError::InvalidEffect(
                                "merger dispatch has no pending merge resolution".into(),
                            )
                        })?;
                    EffectResolution::Command(ProcessorCommand::MergeResolution {
                        outcome: port
                            .resolve_merge_conflict(&task_id, state)
                            .map_err(NativeError::Port)?,
                        task_id,
                    })
                }
                LeafKind::IntegrationReview => {
                    EffectResolution::Command(ProcessorCommand::IntegrationReview {
                        outcome: port.integration_review(state).map_err(NativeError::Port)?,
                    })
                }
                LeafKind::IntegrationFix => {
                    EffectResolution::Command(ProcessorCommand::IntegrationFix {
                        outcome: port.integration_fix(state).map_err(NativeError::Port)?,
                    })
                }
                LeafKind::CiFix => EffectResolution::Command(ProcessorCommand::CiFix {
                    outcome: port.ci_fix(state).map_err(NativeError::Port)?,
                }),
                LeafKind::KnowledgeCurator => {
                    EffectResolution::Command(ProcessorCommand::KnowledgeCurated {
                        outcome: port.curate_knowledge(state).map_err(NativeError::Port)?,
                    })
                }
                other => {
                    return Err(NativeError::InvalidEffect(format!(
                        "integration dispatch cannot use leaf kind {other:?}"
                    )));
                }
            },
            Effect::CommitIntegrationFix => {
                EffectResolution::Command(ProcessorCommand::IntegrationFixCommitted {
                    head: port
                        .commit_integration_fix(state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::Publish { batch_id } => {
                match port.publish(batch_id, state).map_err(NativeError::Port)? {
                    PublicationResult::Published { head, pushed } => {
                        EffectResolution::Command(ProcessorCommand::Published { head, pushed })
                    }
                    PublicationResult::Hold { reason } => {
                        EffectResolution::Command(ProcessorCommand::PublicationAwaitingApproval {
                            reason,
                        })
                    }
                    PublicationResult::Rejected { reason } => {
                        EffectResolution::Command(ProcessorCommand::PublicationRejected { reason })
                    }
                    PublicationResult::ReanchorRequired { reason, target } => {
                        EffectResolution::Command(ProcessorCommand::PublicationReanchorRequired {
                            reason,
                            target,
                        })
                    }
                }
            }
            Effect::ReanchorPublication { batch_id } => {
                match port
                    .reanchor_publication(batch_id, state)
                    .map_err(NativeError::Port)?
                {
                    PublicationReanchorResult::Reanchored => {
                        EffectResolution::Command(ProcessorCommand::PublicationReanchored)
                    }
                    PublicationReanchorResult::Published { head } => {
                        EffectResolution::Command(ProcessorCommand::Published {
                            head,
                            pushed: true,
                        })
                    }
                }
            }
            Effect::VerifyCi { head } => EffectResolution::Command(ProcessorCommand::CiVerified {
                outcome: port.verify_ci(head, state).map_err(NativeError::Port)?,
            }),
            Effect::Notify { event, subject } => {
                port.notify(*event, subject).map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::PrepareCiFix => EffectResolution::Command(ProcessorCommand::CiFixPrepared {
                outcome: port.prepare_ci_fix(state).map_err(NativeError::Port)?,
            }),
            Effect::CommitCiFix => EffectResolution::Command(ProcessorCommand::CiFixCommitted {
                head: port.commit_ci_fix(state).map_err(NativeError::Port)?,
            }),
            Effect::PrepareKnowledgeCuration => {
                EffectResolution::Command(ProcessorCommand::KnowledgeCurationPrepared {
                    outcome: port
                        .prepare_knowledge_curation(state)
                        .map_err(NativeError::Port)?,
                })
            }
            Effect::PrepareArchival => {
                EffectResolution::Command(ProcessorCommand::ArchivalPrepared {
                    outcome: port.prepare_archival(state).map_err(NativeError::Port)?,
                })
            }
            Effect::ReconfirmCiBeforeArchive {
                head,
                required_checks,
            } => EffectResolution::Command(ProcessorCommand::ArchiveCiReconfirmed {
                head: head.clone(),
                outcome: port
                    .reconfirm_ci_before_archive(head, required_checks, state)
                    .map_err(NativeError::Port)?,
            }),
            Effect::ReturnTask { task_id, reason } => {
                port.return_task(task_id, reason, state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::EscalateTask { task_id, reason } => {
                port.escalate_task(task_id, reason, state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::ArchiveTask { task_id } => {
                port.archive_task(task_id, state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::CleanupTaskWorkspace { task_id } => {
                port.cleanup_task_workspace(task_id, state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::CleanupIntegrationWorkspace => {
                port.cleanup_integration_workspace(state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::CleanupCohortControlPlane => {
                port.cleanup_cohort_control_plane(state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::WriteJournalAndStatus => {
                port.write_journal_and_status(state)
                    .map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::ReleaseLease => {
                port.release_lease().map_err(NativeError::Port)?;
                EffectResolution::Acknowledge
            }
            Effect::PersistCheckpoint | Effect::WaitForOperator { .. } => {
                return Err(NativeError::InvalidEffect(
                    "driver-owned marker leaked to native executor".into(),
                ));
            }
        };
        // A contained leaf or a VCS/forge operation may finish just after the renewal worker
        // loses ownership. Do not turn that untrusted observation into a reducer command: the
        // original effect is intentionally retained for recovery inspection.
        self.ensure_lease_active()?;
        Ok(resolution)
    }

    fn execute_batch(
        &mut self,
        effects: &[Effect],
        state: &ProcessorState,
    ) -> Result<Vec<EffectResolution>, Self::Error> {
        self.ensure_lease_active()?;
        let task_effects = effects
            .iter()
            .map(|effect| {
                TaskEffect::from_effect(effect).ok_or_else(|| {
                    NativeError::InvalidEffect(format!(
                        "concurrent task batch contains non-task effect {effect:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let results = self
            .port
            .execute_task_batch(&task_effects, state)
            .map_err(NativeError::Port)?;
        if results.len() != task_effects.len() {
            return Err(NativeError::InvalidEffect(format!(
                "task port returned {} results for {} requests",
                results.len(),
                task_effects.len()
            )));
        }
        self.ensure_lease_active()?;
        task_effects
            .into_iter()
            .zip(results)
            .map(|(effect, result)| {
                effect
                    .into_resolution(result)
                    .map_err(NativeError::InvalidEffect)
            })
            .collect()
    }
}

impl<P: ProcessorPort> NativeExecutor<P> {
    /// Fail closed when the engine has lost its owner lease. This is public because the native
    /// scheduler has two explicit clock/recovery probes that are intentionally outside the
    /// effect adapter but must observe the same boundary.
    pub fn ensure_lease_active(&self) -> Result<(), NativeError<P::Error>> {
        if self
            .cancellation_probe
            .as_ref()
            .is_some_and(CancellationProbe::is_cancelled)
        {
            return Err(NativeError::LeaseLost);
        }
        Ok(())
    }

    /// Run the Phase-0 physical-workspace observation under the same owner boundary as normal
    /// effects. The scheduler must not turn an observation made by a former owner into a durable
    /// `Recover` command.
    pub fn recovery_workspaces(
        &mut self,
        state: &ProcessorState,
    ) -> Result<BTreeSet<String>, NativeError<P::Error>> {
        self.ensure_lease_active()?;
        let workspaces = self
            .port
            .recovery_workspaces(state)
            .map_err(NativeError::Port)?;
        self.ensure_lease_active()?;
        Ok(workspaces)
    }

    /// Observe the normal delivery lane under the same owner boundary as effects. A lease lost
    /// while the control plane is read must not be turned into a new `Open` command by the former
    /// owner.
    pub fn current_queue_readiness(&mut self) -> Result<QueueReadiness, NativeError<P::Error>> {
        self.ensure_lease_active()?;
        let readiness = self
            .port
            .current_queue_readiness()
            .map_err(NativeError::Port)?;
        self.ensure_lease_active()?;
        Ok(readiness)
    }

    /// Read the explicit scheduler clock without allowing a lease loss during the port call to
    /// advance the reducer with a stale owner's input.
    pub fn now_secs(&mut self) -> Result<u64, NativeError<P::Error>> {
        self.ensure_lease_active()?;
        let now_secs = self.port.now_secs().map_err(NativeError::Port)?;
        self.ensure_lease_active()?;
        Ok(now_secs)
    }

    /// Sample the lifecycle-event clock under the same owner check as every other observation.
    pub fn event_occurred_at(&mut self, fallback: &str) -> Result<String, NativeError<P::Error>> {
        self.ensure_lease_active()?;
        let occurred_at = self
            .port
            .event_occurred_at(fallback)
            .map_err(NativeError::Port)?;
        self.ensure_lease_active()?;
        Ok(occurred_at)
    }

    /// Read the PAUSE kill-switch under the same owner boundary as all other native
    /// observations.  The scheduler calls this only before beginning its next phase/round;
    /// an in-flight effect is deliberately never cancelled by an operator pause.
    pub fn pause_requested(&mut self) -> Result<bool, NativeError<P::Error>> {
        self.ensure_lease_active()?;
        let paused = self.port.pause_requested().map_err(NativeError::Port)?;
        self.ensure_lease_active()?;
        Ok(paused)
    }

    /// Write the derived pause status before the caller owner-check releases its lease.  This
    /// does not acknowledge or alter any pending reducer effect.
    pub fn write_pause_status(
        &mut self,
        state: &ProcessorState,
    ) -> Result<(), NativeError<P::Error>> {
        self.ensure_lease_active()?;
        self.port
            .write_pause_status(state)
            .map_err(NativeError::Port)?;
        self.ensure_lease_active()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::convert::Infallible;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::processor::{
        IntegrationRuntime, MergeResolutionRuntime, ProcessorCommand, ProcessorConfig,
        ProcessorState,
    };
    use crate::runtime::{ProcessorRuntime, RecoveryRequirement};

    #[derive(Default)]
    struct Port {
        plan_calls: Option<Arc<AtomicUsize>>,
        cancel_after_plan: Option<Arc<AtomicBool>>,
        paused: bool,
        pause_status_writes: usize,
        publish_hold: Option<String>,
        publish_reanchor: Option<PublicationReanchorTarget>,
    }

    impl ProcessorPort for Port {
        type Error = Infallible;

        fn pause_requested(&mut self) -> Result<bool, Self::Error> {
            Ok(self.paused)
        }

        fn current_queue_readiness(&mut self) -> Result<QueueReadiness, Self::Error> {
            Ok(QueueReadiness::Exhausted { escalated: 0 })
        }

        fn now_secs(&mut self) -> Result<u64, Self::Error> {
            Ok(1)
        }
        fn recovery_workspaces(
            &mut self,
            _: &ProcessorState,
        ) -> Result<BTreeSet<String>, Self::Error> {
            Ok(BTreeSet::new())
        }
        fn reconcile(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<Reconciliation, Self::Error> {
            Ok(Reconciliation::Hold {
                reason: "inspect".into(),
            })
        }
        fn reconcile_inbox(&mut self, _: &ProcessorState) -> Result<bool, Self::Error> {
            Ok(false)
        }
        fn reconcile_inbox_finalization(
            &mut self,
            _: &ProcessorState,
        ) -> Result<bool, Self::Error> {
            Ok(false)
        }
        fn refresh_dependency_graph(
            &mut self,
            _: RefreshBoundary,
            _: &ProcessorState,
        ) -> Result<LeafOutcome, Self::Error> {
            Ok(LeafOutcome::Completed { author: None })
        }
        fn curate_inbox(
            &mut self,
            _: InboxCurationMode,
            _: &ProcessorState,
        ) -> Result<LeafOutcome, Self::Error> {
            Ok(LeafOutcome::Completed { author: None })
        }
        fn drain_queue_inbox(&mut self, _: &ProcessorState) -> Result<(), Self::Error> {
            Ok(())
        }
        fn plan_candidates(
            &mut self,
            _: &ProcessorState,
            _: usize,
        ) -> Result<Vec<AdmissionCandidate>, Self::Error> {
            if let Some(calls) = &self.plan_calls {
                calls.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(cancelled) = &self.cancel_after_plan {
                cancelled.store(true, Ordering::SeqCst);
            }
            Ok(vec![])
        }
        fn token_budget_observation(
            &mut self,
            _: &ProcessorState,
        ) -> Result<TokenBudgetObservation, Self::Error> {
            Ok(TokenBudgetObservation::Actual { tokens: 0 })
        }
        fn ensure_task_workspace(
            &mut self,
            _: &str,
            _: &str,
            _: &ProcessorState,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
        fn task_leaf(
            &mut self,
            _: &str,
            _: LeafKind,
            _: &ProcessorState,
        ) -> Result<LeafOutcome, Self::Error> {
            Ok(LeafOutcome::Completed { author: None })
        }
        fn prepare_task_leaf(
            &mut self,
            _: &str,
            _: LeafKind,
            _: &ProcessorState,
        ) -> Result<TaskLeafPreparationOutcome, Self::Error> {
            Ok(TaskLeafPreparationOutcome::Skipped)
        }
        fn prepare_task_review(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<TaskReviewPreparationOutcome, Self::Error> {
            Ok(TaskReviewPreparationOutcome::DispatchClaude)
        }
        fn task_review(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<ReviewOutcome, Self::Error> {
            Ok(ReviewOutcome::Incomplete)
        }
        fn commit_task(&mut self, _: &str, _: &ProcessorState) -> Result<String, Self::Error> {
            Ok("task-head".into())
        }
        fn ensure_integration_workspace(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
        fn merge_task(&mut self, _: &str, _: &ProcessorState) -> Result<MergeOutcome, Self::Error> {
            Ok(MergeOutcome::Quarantined { reason: "q".into() })
        }
        fn resolve_merge_conflict(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<LeafOutcome, Self::Error> {
            Ok(LeafOutcome::Escalated {
                reason: "test".into(),
            })
        }
        fn finalize_merge_resolution(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<MergeOutcome, Self::Error> {
            Ok(MergeOutcome::Failed {
                reason: "test".into(),
            })
        }
        fn abort_merge_resolution(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
        fn verify_integration(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<VerificationOutcome, Self::Error> {
            Ok(VerificationOutcome::Exempt {
                reason: "test".into(),
            })
        }
        fn integration_review(&mut self, _: &ProcessorState) -> Result<ReviewOutcome, Self::Error> {
            Ok(ReviewOutcome::Incomplete)
        }
        fn integration_fix(&mut self, _: &ProcessorState) -> Result<LeafOutcome, Self::Error> {
            Ok(LeafOutcome::Completed { author: None })
        }
        fn commit_integration_fix(&mut self, _: &ProcessorState) -> Result<String, Self::Error> {
            Ok("integration-head".into())
        }
        fn publish(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<PublicationResult, Self::Error> {
            if let Some(reason) = &self.publish_hold {
                return Ok(PublicationResult::Hold {
                    reason: reason.clone(),
                });
            }
            if let Some(target) = self.publish_reanchor {
                return Ok(PublicationResult::ReanchorRequired {
                    reason: "test publication divergence".into(),
                    target,
                });
            }
            Ok(PublicationResult::Published {
                head: "published-head".into(),
                pushed: true,
            })
        }
        fn reanchor_publication(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<PublicationReanchorResult, Self::Error> {
            Ok(PublicationReanchorResult::Reanchored)
        }
        fn verify_ci(&mut self, _: &str, _: &ProcessorState) -> Result<CiOutcome, Self::Error> {
            Ok(CiOutcome::Passed)
        }
        fn prepare_ci_fix(
            &mut self,
            _: &ProcessorState,
        ) -> Result<CiFixPreparationOutcome, Self::Error> {
            Ok(CiFixPreparationOutcome::Skipped)
        }
        fn ci_fix(&mut self, _: &ProcessorState) -> Result<LeafOutcome, Self::Error> {
            Ok(LeafOutcome::Completed { author: None })
        }
        fn commit_ci_fix(&mut self, _: &ProcessorState) -> Result<String, Self::Error> {
            Ok("ci-head".into())
        }
        fn curate_knowledge(&mut self, _: &ProcessorState) -> Result<LeafOutcome, Self::Error> {
            Ok(LeafOutcome::Completed { author: None })
        }
        fn return_task(&mut self, _: &str, _: &str, _: &ProcessorState) -> Result<(), Self::Error> {
            Ok(())
        }
        fn escalate_task(
            &mut self,
            _: &str,
            _: &str,
            _: &ProcessorState,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
        fn archive_task(&mut self, _: &str, _: &ProcessorState) -> Result<(), Self::Error> {
            Ok(())
        }
        fn cleanup_task_workspace(
            &mut self,
            _: &str,
            _: &ProcessorState,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
        fn cleanup_integration_workspace(&mut self, _: &ProcessorState) -> Result<(), Self::Error> {
            Ok(())
        }
        fn cleanup_cohort_control_plane(&mut self, _: &ProcessorState) -> Result<(), Self::Error> {
            Ok(())
        }
        fn write_journal_and_status(&mut self, _: &ProcessorState) -> Result<(), Self::Error> {
            Ok(())
        }
        fn write_pause_status(&mut self, _: &ProcessorState) -> Result<(), Self::Error> {
            self.pause_status_writes = self.pause_status_writes.saturating_add(1);
            Ok(())
        }
        fn release_lease(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn every_reducer_effect_has_one_native_mapping_or_is_driver_owned() {
        let state = ProcessorState::default();
        let mut executor = NativeExecutor::new(Port::default());
        for effect in [
            Effect::Reconcile {
                task_id: "T-1".into(),
            },
            Effect::ReconcileInbox { free_slots: 1 },
            Effect::ReconcileInboxFinalization,
            Effect::DispatchDependencyCurator {
                boundary: RefreshBoundary::CohortOpen,
            },
            Effect::DispatchInboxCurator {
                free_slots: 1,
                mode: InboxCurationMode::Intake,
            },
            Effect::DrainQueueInbox { free_slots: 1 },
            Effect::PlanNextWave { free_slots: 1 },
            Effect::CheckTokenBudget {
                next: crate::processor::ModelCall::Planner { free_slots: 1 },
            },
            Effect::CheckCohortBudget {
                next: crate::processor::ModelCall::Planner { free_slots: 1 },
            },
            Effect::EnsureTaskWorkspace {
                task_id: "T-1".into(),
                branch: "task/T-1".into(),
            },
            Effect::PrepareTaskReview {
                task_id: "T-1".into(),
            },
            Effect::PrepareTaskLeaf {
                task_id: "T-1".into(),
                kind: LeafKind::Implement,
            },
            Effect::DispatchTask {
                task_id: "T-1".into(),
                kind: LeafKind::Implement,
            },
            Effect::DispatchTask {
                task_id: "T-1".into(),
                kind: LeafKind::Review,
            },
            Effect::CommitTask {
                task_id: "T-1".into(),
            },
            Effect::PrepareIntegrationWorkspace {
                branch: "integration/B-1".into(),
            },
            Effect::MergeTask {
                task_id: "T-1".into(),
            },
            Effect::FinalizeMergeResolution {
                task_id: "T-1".into(),
            },
            Effect::AbortMergeResolution {
                task_id: "T-1".into(),
                reason: "r".into(),
            },
            Effect::VerifyIntegration {
                head: "integration-head".into(),
            },
            Effect::DispatchIntegration {
                kind: LeafKind::IntegrationReview,
            },
            Effect::DispatchIntegration {
                kind: LeafKind::IntegrationFix,
            },
            Effect::DispatchIntegration {
                kind: LeafKind::CiFix,
            },
            Effect::DispatchIntegration {
                kind: LeafKind::KnowledgeCurator,
            },
            Effect::CommitIntegrationFix,
            Effect::Publish {
                batch_id: "B-1".into(),
            },
            Effect::VerifyCi {
                head: "head".into(),
            },
            Effect::Notify {
                event: crate::notification::NotificationEvent::PublishCiFailed,
                subject: "head".into(),
            },
            Effect::PrepareCiFix,
            Effect::CommitCiFix,
            Effect::PrepareKnowledgeCuration,
            Effect::ReturnTask {
                task_id: "T-1".into(),
                reason: "r".into(),
            },
            Effect::EscalateTask {
                task_id: "T-1".into(),
                reason: "r".into(),
            },
            Effect::ArchiveTask {
                task_id: "T-1".into(),
            },
            Effect::CleanupTaskWorkspace {
                task_id: "T-1".into(),
            },
            Effect::CleanupIntegrationWorkspace,
            Effect::CleanupCohortControlPlane,
            Effect::WriteJournalAndStatus,
            Effect::ReleaseLease,
        ] {
            assert!(executor.execute(&effect, &state).is_ok(), "{effect:?}");
        }

        let merger_state = ProcessorState {
            integration: IntegrationRuntime {
                pending_merge_resolution: Some(MergeResolutionRuntime {
                    task_id: "T-1".into(),
                    pre_merge_head: "integration-head".into(),
                    merge_paths: vec!["engine/src/lib.rs".into()],
                    paths: vec!["engine/src/lib.rs".into()],
                    protected_paths: Vec::new(),
                }),
                ..IntegrationRuntime::default()
            },
            ..ProcessorState::default()
        };
        assert!(matches!(
            executor
                .execute(
                    &Effect::DispatchIntegration {
                        kind: LeafKind::Merger,
                    },
                    &merger_state,
                )
                .expect("native merger mapping"),
            EffectResolution::Command(ProcessorCommand::MergeResolution { task_id, .. })
                if task_id == "T-1"
        ));
        assert!(
            executor
                .execute(&Effect::PersistCheckpoint, &state)
                .is_err()
        );
        assert!(
            executor
                .execute(&Effect::WaitForOperator { reason: "x".into() }, &state)
                .is_err()
        );
    }

    #[test]
    fn lost_lease_neither_starts_nor_acknowledges_a_native_effect() {
        let state = ProcessorState::default();
        let cancelled = Arc::new(AtomicBool::new(true));
        let calls = Arc::new(AtomicUsize::new(0));
        let probe = CancellationProbe::new({
            let cancelled = Arc::clone(&cancelled);
            move || cancelled.load(Ordering::SeqCst)
        });
        let mut executor = NativeExecutor::new(Port {
            plan_calls: Some(Arc::clone(&calls)),
            cancel_after_plan: None,
            ..Default::default()
        })
        .with_cancellation_probe(probe.clone());
        let effect = Effect::PlanNextWave { free_slots: 1 };
        assert!(matches!(
            executor.execute(&effect, &state),
            Err(NativeError::LeaseLost)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        cancelled.store(false, Ordering::SeqCst);
        let mut executor = NativeExecutor::new(Port {
            plan_calls: Some(Arc::clone(&calls)),
            cancel_after_plan: Some(Arc::clone(&cancelled)),
            ..Default::default()
        })
        .with_cancellation_probe(probe);
        assert!(matches!(
            executor.execute(&effect, &state),
            Err(NativeError::LeaseLost)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn planner_started_before_a_crash_is_never_replayed_without_inspection() {
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let work = std::env::temp_dir().join(format!(
            "orchestrail-native-planner-crash-{}-{sequence}",
            std::process::id()
        ));
        let config = ProcessorConfig {
            max_parallel: 1,
            cohort_size: 1,
            ..ProcessorConfig::default()
        };
        let calls = Arc::new(AtomicUsize::new(0));
        {
            let mut runtime = ProcessorRuntime::new(config.clone(), &work).unwrap();
            runtime
                .apply_at(
                    ProcessorCommand::Recover {
                        workspaces_present: BTreeSet::new(),
                    },
                    "2026-07-25T12:00:00Z",
                )
                .unwrap();
            runtime.complete_effect("write-journal-and-status").unwrap();
            runtime
                .apply_at(
                    ProcessorCommand::Open {
                        batch_id: "B-1".into(),
                        base: "main".into(),
                        now_secs: 1,
                    },
                    "2026-07-25T12:00:01Z",
                )
                .unwrap();
            runtime
                .apply_at(
                    ProcessorCommand::DependencyGraphRefreshed {
                        boundary: RefreshBoundary::CohortOpen,
                        outcome: LeafOutcome::Completed { author: None },
                    },
                    "2026-07-25T12:00:02Z",
                )
                .unwrap();
            runtime
                .apply_at(
                    ProcessorCommand::InboxReconciled {
                        free_slots: 1,
                        curation_required: false,
                    },
                    "2026-07-25T12:00:03Z",
                )
                .unwrap();
            runtime
                .apply_at(
                    ProcessorCommand::InboxDrained { free_slots: 1 },
                    "2026-07-25T12:00:04Z",
                )
                .unwrap();
            let effect = runtime
                .pending_effects()
                .get("plan-next-wave")
                .cloned()
                .expect("planner effect is durably pending before dispatch");
            let mut executor = NativeExecutor::new(Port {
                plan_calls: Some(Arc::clone(&calls)),
                ..Default::default()
            });
            assert!(matches!(
                executor.execute(&effect, runtime.state()).unwrap(),
                EffectResolution::Command(ProcessorCommand::Admit { .. })
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            // Simulate a process crash precisely after the contained planner returned but before
            // the driver could write its `Admit` acknowledgement to the durable checkpoint.
        }

        let resumed = ProcessorRuntime::resume(config, &work).unwrap();
        assert_eq!(
            resumed.recovery_requirements(),
            vec![RecoveryRequirement::InspectBeforeContinuing {
                key: "plan-next-wave".into(),
                effect: Effect::PlanNextWave { free_slots: 1 },
            }]
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "resume only exposes an inspection obligation; it cannot replay the planner"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn pause_observation_and_status_keep_the_reducer_untouched() {
        let state = ProcessorState::default();
        let mut executor = NativeExecutor::new(Port {
            paused: true,
            ..Default::default()
        });

        assert!(executor.pause_requested().unwrap());
        executor.write_pause_status(&state).unwrap();
        assert_eq!(executor.port().pause_status_writes, 1);
    }

    #[test]
    fn publication_hold_acknowledges_the_safe_probe_and_waits_for_phase_zero_recovery() {
        let mut executor = NativeExecutor::new(Port {
            publish_hold: Some("operator approval pending".into()),
            ..Default::default()
        });
        assert_eq!(
            executor
                .execute(
                    &Effect::Publish {
                        batch_id: "B-1".into(),
                    },
                    &ProcessorState::default(),
                )
                .unwrap(),
            EffectResolution::Command(ProcessorCommand::PublicationAwaitingApproval {
                reason: "operator approval pending".into(),
            })
        );
    }

    #[test]
    fn publication_reanchor_uses_a_distinct_durable_completion_command() {
        let mut executor = NativeExecutor::new(Port::default());
        assert_eq!(
            executor
                .execute(
                    &Effect::ReanchorPublication {
                        batch_id: "B-1".into(),
                    },
                    &ProcessorState::default(),
                )
                .unwrap(),
            EffectResolution::Command(ProcessorCommand::PublicationReanchored)
        );
    }

    #[test]
    fn local_primary_reanchor_is_preserved_in_the_publish_command() {
        let mut executor = NativeExecutor::new(Port {
            publish_reanchor: Some(PublicationReanchorTarget::LocalPrimary),
            ..Default::default()
        });
        assert_eq!(
            executor
                .execute(
                    &Effect::Publish {
                        batch_id: "B-1".into(),
                    },
                    &ProcessorState::default(),
                )
                .unwrap(),
            EffectResolution::Command(ProcessorCommand::PublicationReanchorRequired {
                reason: "test publication divergence".into(),
                target: PublicationReanchorTarget::LocalPrimary,
            })
        );
    }
}
