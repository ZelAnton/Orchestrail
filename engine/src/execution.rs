//! Typed effect execution contract for the deterministic processor.
//!
//! The reducer emits an [`Effect`] only after the runtime has durably recorded it. This module
//! is the narrow bridge from that ledger to impure implementations: a VCS/queue/agent adapter
//! must answer an effect with the exact [`ProcessorCommand`] that acknowledges it, explicitly
//! acknowledge a non-command effect, or leave it pending for a later recovery inspection.
//!
//! There is intentionally no stringly-typed `"success"` response here. Production adapters run
//! agent leaves through ProcessKit and VCS/forge operations through `vcs-*`; their parsers convert
//! evidence into structured commands before it reaches this driver.

use std::collections::VecDeque;
use std::fmt;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::processor::{Effect, ProcessorCommand, ProcessorState};
use crate::runtime::{OperationTiming, ProcessorRuntime, RuntimeError};
use crate::session::LeafSessionUpdate;

/// The outcome of executing one reducer effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectResolution {
    /// Submit a command that acknowledges this exact effect. The driver verifies the pending
    /// ledger key before modifying the reducer.
    Command(ProcessorCommand),
    /// Submit a command for a recovery inspection and then acknowledge the inspected
    /// `Reconcile` effect itself. Recovery is the only valid use: its normal command
    /// acknowledges the original interrupted mutation, while the inspection effect has its own
    /// key.
    Reconciled(ProcessorCommand),
    /// The effect has no reducer result (archive, queue return, journal write, workspace cleanup,
    /// lease release) and the executor verified it completed idempotently.
    Acknowledge,
    /// Stop without changing the runtime. The durable effect remains pending, so a restart will
    /// require fresh inspection rather than replaying an unproven mutation.
    Hold { reason: String },
}

/// Impure port implemented by the native control-plane layer. It owns any ProcessKit invocation,
/// VCS/forge call, durable artifact mutation, and its evidence collection.
pub trait EffectExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Explicit lifecycle timestamp paired with the next reducer acknowledgement. Production
    /// native ports sample wall time; deterministic fixtures keep the caller's fixed value.
    fn event_occurred_at(&mut self, fallback: &str) -> Result<String, Self::Error> {
        Ok(fallback.to_owned())
    }

    /// Resolve one effect against the checkpoint state captured immediately before invocation.
    /// Implementations must never modify the processor checkpoint directly.
    fn execute(
        &mut self,
        effect: &Effect,
        state: &ProcessorState,
    ) -> Result<EffectResolution, Self::Error>;

    /// Hand over the provider conversation coordinate the just-executed leaf observed for
    /// `task_id`, if any. This is explicitly NOT a result: it acknowledges no effect and carries
    /// no decision, so the driver records it beside — never instead of — the typed command. An
    /// adapter that does not resume conversations keeps the default and is unaffected.
    fn take_leaf_session(&mut self, _task_id: &str) -> Option<LeafSessionUpdate> {
        None
    }

    /// Resolve independent task leaves from one rolling round.  The default deliberately keeps
    /// the old serial behaviour so lightweight adapters do not need a worker implementation;
    /// the native adapter overrides this boundary for ProcessKit-backed fan-out.  Results must
    /// be returned in the same stable order as `effects`, never completion order.
    fn execute_batch(
        &mut self,
        effects: &[Effect],
        state: &ProcessorState,
    ) -> Result<Vec<EffectResolution>, Self::Error> {
        effects
            .iter()
            .map(|effect| self.execute(effect, state))
            .collect()
    }
}

/// Bounded result of one execution turn. The caller provides the next time-driven command (for
/// example `Advance`) after a quiescent turn; this avoids an ambient timer silently changing the
/// deterministic state machine.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DriveReport {
    pub completed_steps: usize,
    pub held: Option<String>,
}

/// A driver error names whether the durable runtime or an impure adapter rejected the action.
#[derive(Debug)]
pub enum DriveError<E> {
    Runtime(RuntimeError),
    Executor(E),
    WrongAcknowledgement {
        effect: Effect,
        expected: Option<String>,
        actual: Option<String>,
    },
    InvalidResolution {
        effect: Effect,
        reason: String,
    },
    BatchResolutionCount {
        expected: usize,
        actual: usize,
    },
    StepLimit {
        limit: usize,
    },
}

impl<E: fmt::Display> fmt::Display for DriveError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "runtime error: {error}"),
            Self::Executor(error) => write!(f, "effect executor error: {error}"),
            Self::WrongAcknowledgement {
                effect,
                expected,
                actual,
            } => write!(
                f,
                "command for effect {effect:?} acknowledges {actual:?}, expected {expected:?}"
            ),
            Self::InvalidResolution { effect, reason } => {
                write!(f, "invalid resolution for effect {effect:?}: {reason}")
            }
            Self::BatchResolutionCount { expected, actual } => write!(
                f,
                "task batch returned {actual} resolutions for {expected} requested effects"
            ),
            Self::StepLimit { limit } => write!(
                f,
                "effect turn exceeded its explicit {limit}-step limit; pending work was retained"
            ),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for DriveError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Executor(error) => Some(error),
            Self::WrongAcknowledgement { .. }
            | Self::InvalidResolution { .. }
            | Self::BatchResolutionCount { .. }
            | Self::StepLimit { .. } => None,
        }
    }
}

impl<E> From<RuntimeError> for DriveError<E> {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

/// Execute effects returned from one previously accepted command until the queue is quiescent,
/// an adapter deliberately holds work, or `max_steps` is reached. `occurred_at` is passed through
/// to every successor command and must be an ISO-8601 UTC instant accepted by the runtime.
///
/// Non-persistent markers (`PersistCheckpoint`, `WaitForOperator`) are not handed to the
/// executor. The checkpoint is already persisted by [`ProcessorRuntime::apply_at`]; the latter
/// terminates the turn with its operator reason intact.
pub fn drive<E: EffectExecutor>(
    runtime: &mut ProcessorRuntime,
    initial_effects: impl IntoIterator<Item = Effect>,
    occurred_at: &str,
    executor: &mut E,
    max_steps: usize,
) -> Result<DriveReport, DriveError<E::Error>> {
    let mut queue: VecDeque<Effect> = initial_effects.into_iter().collect();
    let mut report = DriveReport::default();

    while let Some(mut effect) = queue.pop_front() {
        if report.completed_steps >= max_steps {
            return Err(DriveError::StepLimit { limit: max_steps });
        }

        // Phase 2 advances every active task by one step per rolling round.  The reducer has
        // already persisted each effect in its ledger; dispatching a fan-out therefore cannot
        // make an unfinished child invisible.  Keep all reducer acknowledgements until the
        // contained calls have completed, then apply them by stable queue order rather than
        // wall-clock completion order.  `PersistCheckpoint` is only a driver marker (the state
        // was written by `apply_at`) and may sit between two independent task effects.
        if is_task_leaf_effect(&effect) {
            queue.push_front(effect);
            if let Some((effects, consumed, marker_count)) = task_batch_at_front(&queue) {
                if report.completed_steps.saturating_add(consumed) > max_steps {
                    return Err(DriveError::StepLimit { limit: max_steps });
                }
                for _ in 0..consumed {
                    queue.pop_front();
                }
                let operation_started_at = wall_epoch_millis();
                let started = Instant::now();
                let resolutions = executor
                    .execute_batch(&effects, runtime.state())
                    .map_err(DriveError::Executor)?;
                let batch_duration_ms = elapsed_millis(started);
                let operation_ended_at = wall_epoch_millis();
                if resolutions.len() != effects.len() {
                    return Err(DriveError::BatchResolutionCount {
                        expected: effects.len(),
                        actual: resolutions.len(),
                    });
                }
                report.completed_steps += marker_count;
                let event_occurred_at = executor
                    .event_occurred_at(occurred_at)
                    .map_err(DriveError::Executor)?;
                for (effect, resolution) in effects.into_iter().zip(resolutions) {
                    record_leaf_session(runtime, &effect, executor)?;
                    let held = apply_resolution(
                        runtime,
                        &mut queue,
                        effect,
                        resolution,
                        &event_occurred_at,
                        OperationTiming {
                            started_at: crate::time::epoch_millis_to_iso(operation_started_at),
                            ended_at: crate::time::epoch_millis_to_iso(operation_ended_at),
                            duration_ms: batch_duration_ms,
                        },
                    )?;
                    report.completed_steps += 1;
                    if let Some(reason) = held {
                        report.held = Some(reason);
                        return Ok(report);
                    }
                }
                continue;
            }
            // No independent sibling is ready.  Restore the normal single-effect path below.
            effect = queue.pop_front().expect("task effect was just restored");
        }
        match &effect {
            Effect::PersistCheckpoint => {
                // `apply_at` already made the state and event outbox durable before returning.
                report.completed_steps += 1;
            }
            Effect::WaitForOperator { reason } => {
                report.held = Some(reason.clone());
                return Ok(report);
            }
            _ => {
                let operation_started_at = wall_epoch_millis();
                let started = Instant::now();
                let resolution = executor
                    .execute(&effect, runtime.state())
                    .map_err(DriveError::Executor)?;
                let duration_ms = elapsed_millis(started);
                let operation_ended_at = wall_epoch_millis();
                let event_occurred_at = executor
                    .event_occurred_at(occurred_at)
                    .map_err(DriveError::Executor)?;
                record_leaf_session(runtime, &effect, executor)?;
                match apply_resolution(
                    runtime,
                    &mut queue,
                    effect,
                    resolution,
                    &event_occurred_at,
                    OperationTiming {
                        started_at: crate::time::epoch_millis_to_iso(operation_started_at),
                        ended_at: crate::time::epoch_millis_to_iso(operation_ended_at),
                        duration_ms,
                    },
                )? {
                    Some(reason) => {
                        report.held = Some(reason);
                        return Ok(report);
                    }
                    None => report.completed_steps += 1,
                }
            }
        }
    }
    Ok(report)
}

/// Persist the conversation coordinate a task leaf observed, before its typed result is applied.
///
/// Only task-scoped leaf effects have a lineage to remember. The write is orthogonal: it does not
/// touch the pending ledger, so it can never mask, satisfy, or reorder the acknowledgement that
/// follows it. A reducer rejection (unknown task, malformed provider id) is a real adapter
/// protocol violation and is surfaced rather than silently dropped.
fn record_leaf_session<E: EffectExecutor>(
    runtime: &mut ProcessorRuntime,
    effect: &Effect,
    executor: &mut E,
) -> Result<(), DriveError<E::Error>> {
    let task_id = match effect {
        Effect::PrepareTaskLeaf { task_id, .. }
        | Effect::PrepareTaskReview { task_id }
        | Effect::DispatchTask { task_id, .. } => task_id.clone(),
        _ => return Ok(()),
    };
    let Some(update) = executor.take_leaf_session(&task_id) else {
        return Ok(());
    };
    runtime
        .record_leaf_session(&task_id, &update)
        .map_err(DriveError::Runtime)
}

/// Apply a single result only after its originating effect is known.  A batch reuses the exact
/// same acknowledgement checks as the serial path; it merely postpones this mutation until all
/// siblings have returned.
fn apply_resolution<E>(
    runtime: &mut ProcessorRuntime,
    queue: &mut VecDeque<Effect>,
    effect: Effect,
    resolution: EffectResolution,
    occurred_at: &str,
    timing: OperationTiming,
) -> Result<Option<String>, DriveError<E>> {
    match resolution {
        EffectResolution::Command(command) => {
            ensure_matching_command(runtime, &effect, &command)?;
            queue.extend(runtime.apply_effect_at_with_timing(
                &effect,
                command,
                occurred_at,
                timing,
            )?);
            Ok(None)
        }
        EffectResolution::Reconciled(command) => {
            if !matches!(effect, Effect::Reconcile { .. }) {
                return Err(DriveError::InvalidResolution {
                    effect,
                    reason: "only a recovery inspection may use Reconciled".into(),
                });
            }
            if runtime.command_acknowledgement_key(&command)?.is_none() {
                return Err(DriveError::InvalidResolution {
                    effect,
                    reason: "recovery may only reconcile a recorded command that acknowledges an outstanding mutation".into(),
                });
            }
            if !reconciliation_matches_task(&effect, &command) {
                return Err(DriveError::InvalidResolution {
                    effect,
                    reason: "recovery command does not belong to the task named by its reconciliation effect".into(),
                });
            }
            queue.extend(runtime.apply_at(command, occurred_at)?);
            runtime.acknowledge_effect(&effect)?;
            Ok(None)
        }
        EffectResolution::Acknowledge => {
            if ProcessorRuntime::effect_key(&effect).is_none() {
                return Err(DriveError::InvalidResolution {
                    effect,
                    reason: "an informational effect cannot be acknowledged".into(),
                });
            }
            if !acknowledgement_only_effect(&effect) {
                return Err(DriveError::InvalidResolution {
                    effect,
                    reason: "this effect requires a typed reducer command and cannot be acknowledged without its result".into(),
                });
            }
            runtime.acknowledge_effect(&effect)?;
            Ok(None)
        }
        EffectResolution::Hold { reason } => Ok(Some(reason)),
    }
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn wall_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn reconciliation_matches_task(effect: &Effect, command: &ProcessorCommand) -> bool {
    let Effect::Reconcile { task_id } = effect else {
        return false;
    };
    match command {
        ProcessorCommand::WorkspaceReady { task_id: candidate }
        | ProcessorCommand::WorkspaceFailed {
            task_id: candidate, ..
        }
        | ProcessorCommand::TaskLeafPrepared {
            task_id: candidate, ..
        }
        | ProcessorCommand::TaskLeaf {
            task_id: candidate, ..
        }
        | ProcessorCommand::TaskCommitted {
            task_id: candidate, ..
        }
        | ProcessorCommand::TaskReviewPrepared {
            task_id: candidate, ..
        }
        | ProcessorCommand::TaskReview {
            task_id: candidate, ..
        } => candidate == task_id,
        _ => false,
    }
}

fn acknowledgement_only_effect(effect: &Effect) -> bool {
    matches!(
        effect,
        Effect::Notify { .. }
            | Effect::ReturnTask { .. }
            | Effect::EscalateTask { .. }
            | Effect::ArchiveTask { .. }
            | Effect::CleanupTaskWorkspace { .. }
            | Effect::CleanupIntegrationWorkspace
            | Effect::CleanupCohortControlPlane
            | Effect::WriteJournalAndStatus
            | Effect::ReleaseLease
    )
}

fn is_task_leaf_effect(effect: &Effect) -> bool {
    matches!(
        effect,
        Effect::PrepareTaskLeaf { .. }
            | Effect::PrepareTaskReview { .. }
            | Effect::DispatchTask { .. }
    )
}

/// Return a maximal stable fan-out of unique task IDs at the front of the queue.  Persist
/// markers are intentionally absorbed: they are already durable and do not order two leaves.
/// Any real control-plane effect ends the round, so no result can observe an unrelated mutation
/// that should have happened first.
fn task_batch_at_front(queue: &VecDeque<Effect>) -> Option<(Vec<Effect>, usize, usize)> {
    use std::collections::BTreeSet;

    let mut effects = Vec::new();
    let mut ids = BTreeSet::new();
    let mut consumed = 0;
    let mut markers = 0;
    for effect in queue {
        match effect {
            Effect::PersistCheckpoint => {
                consumed += 1;
                markers += 1;
            }
            Effect::PrepareTaskLeaf { task_id, .. }
            | Effect::PrepareTaskReview { task_id }
            | Effect::DispatchTask { task_id, .. } => {
                if !ids.insert(task_id.clone()) {
                    break;
                }
                effects.push(effect.clone());
                consumed += 1;
            }
            _ => break,
        }
    }
    // Planner adapters normally return sorted candidates, but collection must not inherit an
    // accidental provider/listing order.  The durable task ID is the Phase-2 tie-breaker.
    effects.sort_by(|left, right| task_effect_id(left).cmp(task_effect_id(right)));
    (effects.len() >= 2).then_some((effects, consumed, markers))
}

fn task_effect_id(effect: &Effect) -> &str {
    match effect {
        Effect::PrepareTaskLeaf { task_id, .. }
        | Effect::PrepareTaskReview { task_id }
        | Effect::DispatchTask { task_id, .. } => task_id,
        _ => unreachable!("only task effects enter a task batch"),
    }
}

fn ensure_matching_command<E>(
    runtime: &ProcessorRuntime,
    effect: &Effect,
    command: &ProcessorCommand,
) -> Result<(), DriveError<E>> {
    let expected = ProcessorRuntime::effect_key(effect);
    let actual = runtime.command_acknowledgement_key(command)?;
    if expected != actual {
        return Err(DriveError::WrongAcknowledgement {
            effect: effect.clone(),
            expected,
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use std::collections::BTreeMap;

    use super::*;
    use crate::processor::{
        AdmissionCandidate, CiOutcome, LeafKind, LeafOutcome, MergeOutcome, Phase, ProcessorConfig,
        ReviewOutcome, TaskPhase,
    };
    use crate::resolvers::Level;
    use crate::session::{LeafSessionKey, SessionLineage, SessionProvider};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[derive(Default)]
    struct ScriptedExecutor {
        task_batches: Vec<Vec<String>>,
        event_time: Option<String>,
        /// Conversation coordinates this fake adapter claims to have observed, drained per task
        /// exactly as a real provider adapter does.
        sessions: BTreeMap<String, LeafSessionUpdate>,
    }

    impl EffectExecutor for ScriptedExecutor {
        type Error = Infallible;

        fn event_occurred_at(&mut self, fallback: &str) -> Result<String, Self::Error> {
            Ok(self
                .event_time
                .clone()
                .unwrap_or_else(|| fallback.to_owned()))
        }

        fn take_leaf_session(&mut self, task_id: &str) -> Option<LeafSessionUpdate> {
            self.sessions.remove(task_id)
        }

        fn execute(
            &mut self,
            effect: &Effect,
            state: &ProcessorState,
        ) -> Result<EffectResolution, Self::Error> {
            let resolution = match effect {
                Effect::ReconcileInbox { free_slots } => {
                    EffectResolution::Command(ProcessorCommand::InboxReconciled {
                        free_slots: *free_slots,
                        curation_required: false,
                    })
                }
                Effect::ReconcileInboxFinalization => {
                    EffectResolution::Command(ProcessorCommand::InboxFinalizationReconciled {
                        curation_required: false,
                    })
                }
                Effect::DispatchDependencyCurator { boundary } => {
                    EffectResolution::Command(ProcessorCommand::DependencyGraphRefreshed {
                        boundary: *boundary,
                        outcome: LeafOutcome::Completed { author: None },
                    })
                }
                Effect::DispatchInboxCurator { free_slots, mode } => {
                    EffectResolution::Command(ProcessorCommand::InboxCurated {
                        free_slots: *free_slots,
                        mode: *mode,
                        outcome: LeafOutcome::Completed { author: None },
                    })
                }
                Effect::DrainQueueInbox { free_slots } => {
                    EffectResolution::Command(ProcessorCommand::InboxDrained {
                        free_slots: *free_slots,
                    })
                }
                Effect::PlanNextWave { .. } if state.tasks.is_empty() => {
                    EffectResolution::Command(ProcessorCommand::Admit {
                        candidates: vec![AdmissionCandidate {
                            id: "T-1".into(),
                            conflict_domain: "engine/**".into(),
                            level: Level::Coder,
                            risk: crate::resolvers::Risk::Medium,
                            ready: true,
                            current_delivery_lane: true,
                        }],
                        now_secs: 1,
                    })
                }
                Effect::PlanNextWave { .. } => EffectResolution::Command(ProcessorCommand::Admit {
                    candidates: Vec::new(),
                    now_secs: 2,
                }),
                Effect::CheckTokenBudget { next } => {
                    EffectResolution::Command(ProcessorCommand::TokenBudgetChecked {
                        next: next.clone(),
                        observation: crate::processor::TokenBudgetObservation::Actual { tokens: 0 },
                    })
                }
                Effect::CheckCohortBudget { next } => {
                    EffectResolution::Command(ProcessorCommand::CohortBudgetChecked {
                        next: next.clone(),
                        now_secs: 1,
                    })
                }
                Effect::EnsureTaskWorkspace { task_id, .. } => {
                    EffectResolution::Command(ProcessorCommand::WorkspaceReady {
                        task_id: task_id.clone(),
                    })
                }
                Effect::PrepareTaskReview { task_id } => {
                    EffectResolution::Command(ProcessorCommand::TaskReviewPrepared {
                        task_id: task_id.clone(),
                        outcome: crate::processor::TaskReviewPreparationOutcome::DispatchClaude,
                    })
                }
                Effect::PrepareTaskLeaf { task_id, .. } => {
                    EffectResolution::Command(ProcessorCommand::TaskLeafPrepared {
                        task_id: task_id.clone(),
                        outcome: crate::processor::TaskLeafPreparationOutcome::Skipped,
                    })
                }
                Effect::DispatchTask { task_id, kind } => match kind {
                    LeafKind::Implement | LeafKind::Fix => {
                        EffectResolution::Command(ProcessorCommand::TaskLeaf {
                            task_id: task_id.clone(),
                            outcome: LeafOutcome::Completed {
                                author: Some("coder".into()),
                            },
                        })
                    }
                    LeafKind::Review => EffectResolution::Command(ProcessorCommand::TaskReview {
                        task_id: task_id.clone(),
                        outcome: ReviewOutcome::Clean {
                            review_sha: "task-head".into(),
                        },
                    }),
                    other => panic!("unexpected task leaf {other:?}"),
                },
                Effect::CommitTask { task_id } => {
                    EffectResolution::Command(ProcessorCommand::TaskCommitted {
                        task_id: task_id.clone(),
                        commit: "task-head".into(),
                    })
                }
                Effect::PrepareIntegrationWorkspace { .. } => {
                    EffectResolution::Command(ProcessorCommand::IntegrationWorkspaceReady)
                }
                Effect::MergeTask { task_id } => {
                    EffectResolution::Command(ProcessorCommand::TaskMerged {
                        task_id: task_id.clone(),
                        outcome: MergeOutcome::Merged {
                            integration_sha: "integration-head".into(),
                        },
                    })
                }
                Effect::FinalizeMergeResolution { task_id } => {
                    EffectResolution::Command(ProcessorCommand::MergeResolutionFinalized {
                        task_id: task_id.clone(),
                        outcome: MergeOutcome::Merged {
                            integration_sha: "integration-head".into(),
                        },
                    })
                }
                Effect::AbortMergeResolution { task_id, reason } => {
                    EffectResolution::Command(ProcessorCommand::MergeResolutionAborted {
                        task_id: task_id.clone(),
                        reason: reason.clone(),
                    })
                }
                Effect::VerifyIntegration { head } => {
                    EffectResolution::Command(ProcessorCommand::IntegrationVerified {
                        head: head.clone(),
                        outcome: crate::processor::VerificationOutcome::Exempt {
                            reason: "fixture profile disabled".into(),
                        },
                    })
                }
                Effect::DispatchIntegration { kind } => match kind {
                    LeafKind::Merger => {
                        let task_id = state
                            .integration
                            .pending_merge_resolution
                            .as_ref()
                            .expect("fixture merger has pending state")
                            .task_id
                            .clone();
                        EffectResolution::Command(ProcessorCommand::MergeResolution {
                            task_id,
                            outcome: LeafOutcome::Escalated {
                                reason: "fixture".into(),
                            },
                        })
                    }
                    LeafKind::IntegrationReview => {
                        EffectResolution::Command(ProcessorCommand::IntegrationReview {
                            outcome: ReviewOutcome::Clean {
                                review_sha: "integration-head".into(),
                            },
                        })
                    }
                    LeafKind::KnowledgeCurator => {
                        EffectResolution::Command(ProcessorCommand::KnowledgeCurated {
                            outcome: LeafOutcome::Completed { author: None },
                        })
                    }
                    other => panic!("unexpected integration leaf {other:?}"),
                },
                Effect::Publish { .. } => EffectResolution::Command(ProcessorCommand::Published {
                    head: "integration-head".into(),
                    pushed: false,
                }),
                Effect::ReanchorPublication { .. } => {
                    EffectResolution::Command(ProcessorCommand::PublicationReanchored)
                }
                Effect::VerifyCi { .. } => {
                    EffectResolution::Command(ProcessorCommand::CiVerified {
                        outcome: CiOutcome::LocalOnly,
                    })
                }
                Effect::PrepareCiFix => {
                    EffectResolution::Command(ProcessorCommand::CiFixPrepared {
                        outcome: crate::processor::CiFixPreparationOutcome::Skipped,
                    })
                }
                Effect::PrepareKnowledgeCuration => {
                    EffectResolution::Command(ProcessorCommand::KnowledgeCurationPrepared {
                        outcome: crate::processor::KnowledgeCurationPreparationOutcome::Required,
                    })
                }
                Effect::PrepareArchival => {
                    EffectResolution::Command(ProcessorCommand::ArchivalPrepared {
                        outcome: crate::processor::ArchivalPreparationOutcome::Skipped,
                    })
                }
                Effect::ReconfirmCiBeforeArchive { head, .. } => {
                    EffectResolution::Command(ProcessorCommand::ArchiveCiReconfirmed {
                        head: head.clone(),
                        outcome: CiOutcome::Passed,
                    })
                }
                Effect::ReturnTask { .. }
                | Effect::EscalateTask { .. }
                | Effect::ArchiveTask { .. }
                | Effect::CleanupTaskWorkspace { .. }
                | Effect::CleanupIntegrationWorkspace
                | Effect::CleanupCohortControlPlane
                | Effect::WriteJournalAndStatus
                | Effect::ReleaseLease => EffectResolution::Acknowledge,
                other => panic!("unexpected effect {other:?}"),
            };
            Ok(resolution)
        }

        fn execute_batch(
            &mut self,
            effects: &[Effect],
            state: &ProcessorState,
        ) -> Result<Vec<EffectResolution>, Self::Error> {
            self.task_batches.push(
                effects
                    .iter()
                    .map(|effect| match effect {
                        Effect::PrepareTaskLeaf { task_id, .. }
                        | Effect::PrepareTaskReview { task_id }
                        | Effect::DispatchTask { task_id, .. } => task_id.clone(),
                        other => panic!("unexpected non-task effect in batch {other:?}"),
                    })
                    .collect(),
            );
            effects
                .iter()
                .map(|effect| self.execute(effect, state))
                .collect()
        }
    }

    fn work() -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-execution-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn scripted_port_drives_a_full_publish_without_bypassing_the_durable_ledger() {
        let work = work();
        let mut runtime = ProcessorRuntime::new(
            ProcessorConfig {
                max_parallel: 1,
                cohort_size: 1,
                ..ProcessorConfig::default()
            },
            &work,
        )
        .unwrap();
        let mut executor = ScriptedExecutor::default();
        let at = "2026-07-24T12:00:00Z";

        let recovered = runtime
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: Default::default(),
                },
                at,
            )
            .unwrap();
        drive(&mut runtime, recovered, at, &mut executor, 100).unwrap();

        let opened = runtime
            .apply_at(
                ProcessorCommand::Open {
                    batch_id: "B-test".into(),
                    base: "base".into(),
                    now_secs: 1,
                },
                at,
            )
            .unwrap();
        drive(&mut runtime, opened, at, &mut executor, 100).unwrap();
        assert_eq!(runtime.state().tasks["T-1"].phase, TaskPhase::Ready);
        assert!(runtime.pending_effects().is_empty());

        let advanced = runtime
            .apply_at(ProcessorCommand::Advance { now_secs: 2 }, at)
            .unwrap();
        drive(&mut runtime, advanced, at, &mut executor, 100).unwrap();
        assert_eq!(runtime.state().phase, Phase::Cleaning);
        assert_eq!(runtime.state().tasks["T-1"].phase, TaskPhase::Published);
        assert!(runtime.pending_effects().is_empty());

        let post_archive = runtime
            .apply_at(ProcessorCommand::CleanupComplete, at)
            .unwrap();
        drive(&mut runtime, post_archive, at, &mut executor, 100).unwrap();
        assert_eq!(runtime.state().phase, Phase::Cleaning);
        assert!(
            runtime
                .state()
                .integration
                .dependency_graph_refreshed_post_archive
        );

        let cleaned = runtime
            .apply_at(ProcessorCommand::CleanupComplete, at)
            .unwrap();
        drive(&mut runtime, cleaned, at, &mut executor, 100).unwrap();
        assert_eq!(runtime.state().phase, Phase::Idle);
        assert_eq!(runtime.state().tasks["T-1"].phase, TaskPhase::Done);
        assert!(runtime.pending_effects().is_empty());

        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn an_observed_conversation_becomes_durable_without_entering_the_effect_ledger() {
        let work = work();
        let mut runtime = ProcessorRuntime::new(
            ProcessorConfig {
                max_parallel: 1,
                cohort_size: 1,
                ..ProcessorConfig::default()
            },
            &work,
        )
        .unwrap();
        let key = LeafSessionKey::new(SessionProvider::Claude, SessionLineage::Coder);
        let mut executor = ScriptedExecutor {
            sessions: BTreeMap::from([(
                "T-1".to_owned(),
                LeafSessionUpdate::Observed {
                    key,
                    id: "11111111-2222-3333-4444-555555555555".into(),
                },
            )]),
            ..ScriptedExecutor::default()
        };
        let at = "2026-07-24T12:00:00Z";

        let recovered = runtime
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: Default::default(),
                },
                at,
            )
            .unwrap();
        drive(&mut runtime, recovered, at, &mut executor, 100).unwrap();
        let opened = runtime
            .apply_at(
                ProcessorCommand::Open {
                    batch_id: "B-test".into(),
                    base: "base".into(),
                    now_secs: 1,
                },
                at,
            )
            .unwrap();
        drive(&mut runtime, opened, at, &mut executor, 100).unwrap();

        // The coordinate is durable in the checkpoint...
        assert_eq!(
            runtime.state().tasks["T-1"].leaf_session(key),
            Some("11111111-2222-3333-4444-555555555555")
        );
        // ...it was drained exactly once, so it cannot be replayed onto a later effect...
        assert!(executor.sessions.is_empty());
        // ...and it neither acknowledged nor left anything in the effect ledger: the run reached
        // the very same phase as the identical run without any session at all.
        assert!(runtime.pending_effects().is_empty());
        assert_eq!(runtime.state().tasks["T-1"].phase, TaskPhase::Ready);

        // It also survives a restart from the persisted checkpoint.
        let reloaded = ProcessorRuntime::resume(
            ProcessorConfig {
                max_parallel: 1,
                cohort_size: 1,
                ..ProcessorConfig::default()
            },
            &work,
        )
        .unwrap();
        assert_eq!(
            reloaded.state().tasks["T-1"].leaf_session(key),
            Some("11111111-2222-3333-4444-555555555555")
        );

        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn driver_uses_the_port_event_clock_for_effect_completion_transitions() {
        let work = work();
        let mut runtime = ProcessorRuntime::new(
            ProcessorConfig {
                max_parallel: 1,
                cohort_size: 1,
                ..ProcessorConfig::default()
            },
            &work,
        )
        .unwrap();
        let fallback = "2026-07-24T12:00:00Z";
        let completed_at = "2026-07-24T12:00:05.250Z";
        let mut executor = ScriptedExecutor {
            event_time: Some(completed_at.into()),
            ..ScriptedExecutor::default()
        };
        let recovered = runtime
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: Default::default(),
                },
                fallback,
            )
            .unwrap();
        drive(&mut runtime, recovered, fallback, &mut executor, 100).unwrap();
        let opened = runtime
            .apply_at(
                ProcessorCommand::Open {
                    batch_id: "B-clock".into(),
                    base: "base".into(),
                    now_secs: 1,
                },
                fallback,
            )
            .unwrap();
        drive(&mut runtime, opened, fallback, &mut executor, 100).unwrap();

        let mut reader = crate::events::TailReader::new(work.join(crate::events::OUTBOX_FILE));
        let captured = reader
            .poll_all()
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == crate::events::EventType::TaskCaptured)
            .expect("planner admission projects task.captured");
        assert_eq!(captured.occurred_at, completed_at);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_command_cannot_acknowledge_another_task_effect() {
        let work = work();
        let runtime = ProcessorRuntime::new(ProcessorConfig::default(), &work).unwrap();
        let effect = Effect::EnsureTaskWorkspace {
            task_id: "T-1".into(),
            branch: "task/T-1".into(),
        };
        let error = ensure_matching_command::<Infallible>(
            &runtime,
            &effect,
            &ProcessorCommand::WorkspaceReady {
                task_id: "T-2".into(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, DriveError::WrongAcknowledgement { .. }));
        // The state has no T-2; a real executor reaches the same fail-closed error before a
        // different in-flight task can be advanced.
        assert!(runtime.pending_effects().is_empty());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn acknowledgement_cannot_discard_an_effect_that_requires_a_typed_result() {
        assert!(!acknowledgement_only_effect(&Effect::EnsureTaskWorkspace {
            task_id: "T-1".into(),
            branch: "task/T-1".into(),
        }));
        assert!(!acknowledgement_only_effect(&Effect::CommitTask {
            task_id: "T-1".into(),
        }));
        assert!(acknowledgement_only_effect(&Effect::WriteJournalAndStatus));
        assert!(acknowledgement_only_effect(&Effect::ReleaseLease));
    }

    #[test]
    fn reconciliation_command_must_name_the_same_task() {
        let reconcile = Effect::Reconcile {
            task_id: "T-1".into(),
        };
        assert!(reconciliation_matches_task(
            &reconcile,
            &ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            }
        ));
        assert!(!reconciliation_matches_task(
            &reconcile,
            &ProcessorCommand::WorkspaceReady {
                task_id: "T-2".into(),
            }
        ));
        assert!(!reconciliation_matches_task(
            &reconcile,
            &ProcessorCommand::IntegrationWorkspaceReady,
        ));
    }

    #[test]
    fn independent_task_leaves_are_collected_in_stable_batches() {
        let work = work();
        let at = "2026-07-25T12:00:00Z";
        let mut runtime = ProcessorRuntime::new(
            ProcessorConfig {
                max_parallel: 2,
                cohort_size: 2,
                ..ProcessorConfig::default()
            },
            &work,
        )
        .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: Default::default(),
                },
                at,
            )
            .unwrap();
        runtime.complete_effect("write-journal-and-status").unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Open {
                    batch_id: "B-batch".into(),
                    base: "base".into(),
                    now_secs: 1,
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::DependencyGraphRefreshed {
                    boundary: crate::dependency_graph::RefreshBoundary::CohortOpen,
                    outcome: LeafOutcome::Completed { author: None },
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::InboxReconciled {
                    free_slots: 2,
                    curation_required: false,
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(ProcessorCommand::InboxDrained { free_slots: 2 }, at)
            .unwrap();
        let admitted = runtime
            .apply_at(
                ProcessorCommand::Admit {
                    candidates: vec![
                        AdmissionCandidate {
                            id: "T-2".into(),
                            conflict_domain: "t2/**".into(),
                            level: Level::Coder,
                            risk: crate::resolvers::Risk::Medium,
                            ready: true,
                            current_delivery_lane: true,
                        },
                        AdmissionCandidate {
                            id: "T-1".into(),
                            conflict_domain: "t1/**".into(),
                            level: Level::Coder,
                            risk: crate::resolvers::Risk::Medium,
                            ready: true,
                            current_delivery_lane: true,
                        },
                    ],
                    now_secs: 2,
                },
                at,
            )
            .unwrap();
        let mut executor = ScriptedExecutor::default();
        drive(&mut runtime, admitted, at, &mut executor, 100).unwrap();

        // Candidate resolution and collection are ordered by durable T-ID, not which worker
        // happened to finish first.  Every model-bearing phase of the two independent tasks
        // therefore reached one two-item fan-out.
        assert_eq!(
            executor.task_batches,
            vec![
                vec!["T-1".to_string(), "T-2".to_string()],
                vec!["T-1".to_string(), "T-2".to_string()],
                vec!["T-1".to_string(), "T-2".to_string()],
                vec!["T-1".to_string(), "T-2".to_string()],
            ]
        );
        assert_eq!(runtime.state().tasks["T-1"].phase, TaskPhase::Ready);
        assert_eq!(runtime.state().tasks["T-2"].phase, TaskPhase::Ready);
        assert!(runtime.pending_effects().is_empty());
        let _ = fs::remove_dir_all(work);
    }
}
