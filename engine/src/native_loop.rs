//! Deterministic turn scheduler for the native processor.
//!
//! The reducer deliberately does not read clocks or poll the queue by itself. This loop supplies
//! only the three operator-independent turn boundaries it owns: phase-0 recovery inspection,
//! rolling `Advance`, and cleanup completion after every durable cleanup effect was acknowledged.
//! All leaf/VCS/forge work remains in [`crate::native::NativeExecutor`].

use std::collections::BTreeSet;
use std::fmt;

use crate::execution::{DriveError, DriveReport, drive};
use crate::native::{NativeError, NativeExecutor, ProcessorPort, QueueReadiness};
use crate::processor::{Effect, Phase, ProcessorCommand};
use crate::runtime::{ProcessorRuntime, RecoveryRequirement, RuntimeError};

#[derive(Debug, Clone)]
pub struct NativeLoopConfig {
    pub batch_id: String,
    pub base: String,
    pub occurred_at: String,
    pub max_turns: usize,
    pub max_effects_per_turn: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeLoopOutcome {
    /// The requested cohort completed cleanup and the reducer returned to idle.
    Completed,
    /// A reducer/adapter safety gate requires a human decision. The runtime checkpoint and
    /// outstanding-effect ledger remain durable for an explicit future continuation.
    Held { reason: String },
    /// The processor was already idle with an existing inactive state and no requested work.
    Idle,
    /// The normal delivery lane is exhausted, but terminal escalations require a human decision
    /// before the overall queue can be reported as cleanly completed.
    Escalated { count: usize },
}

#[derive(Debug)]
pub enum NativeLoopError<E> {
    Runtime(RuntimeError),
    Port(NativeError<E>),
    Drive(DriveError<NativeError<E>>),
    TurnLimit { limit: usize },
}

impl<E: fmt::Display> fmt::Display for NativeLoopError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "native runtime failure: {error}"),
            Self::Port(error) => write!(f, "native processor port failure: {error}"),
            Self::Drive(error) => write!(f, "native effect drive failed: {error}"),
            Self::TurnLimit { limit } => write!(f, "native loop exceeded turn limit {limit}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NativeLoopError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Port(error) => Some(error),
            Self::Drive(error) => Some(error),
            Self::TurnLimit { .. } => None,
        }
    }
}

/// Run one requested cohort through deterministic turn boundaries. A caller owns the native
/// lease around this call and is responsible for final OS lease release after the reducer emits
/// its `ReleaseLease` effect.
pub fn run_until_idle<P: ProcessorPort>(
    runtime: &mut ProcessorRuntime,
    executor: &mut NativeExecutor<P>,
    config: &NativeLoopConfig,
) -> Result<NativeLoopOutcome, NativeLoopError<P::Error>> {
    let mut opened = runtime.state().batch.is_some();
    for _ in 0..config.max_turns {
        let mut replayable_recovery_effects = Vec::new();
        // Recovery workspace inspection and deterministic clock reads are the two scheduler
        // probes outside `NativeExecutor::execute`. They still precede durable reducer commands,
        // so an owner that has lost its lease must not use either result to advance the runtime.
        executor
            .ensure_lease_active()
            .map_err(NativeLoopError::Port)?;
        // Orchestra's PAUSE is a phase/round boundary, not a signal to kill an already
        // executing agent or VCS mutation.  At this point the prior drive has fully settled and
        // the durable runtime ledger is the exact recovery authority, so materialize the derived
        // operator status and return for the caller's owner-checked lease release.
        if executor.pause_requested().map_err(NativeLoopError::Port)? {
            executor
                .write_pause_status(runtime.state())
                .map_err(NativeLoopError::Port)?;
            return Ok(NativeLoopOutcome::Held {
                reason: format!(
                    "paused by .work/PAUSE at {:?} phase/round boundary; remove it and rerun to resume through phase-0 recovery",
                    runtime.state().phase
                ),
            });
        }
        let command = match runtime.state().phase {
            Phase::Recovery => {
                if let Some(reason) = inspection_recovery_hold(runtime, |effect| {
                    executor.interrupted_effect_retry_safe(effect)
                }) {
                    return Ok(NativeLoopOutcome::Held { reason });
                }
                replayable_recovery_effects = replayable_recovery_effects_for(runtime, |effect| {
                    executor.interrupted_effect_retry_safe(effect)
                });
                let mut workspaces = executor
                    .recovery_workspaces(runtime.state())
                    .map_err(NativeLoopError::Port)?;
                for effect in &replayable_recovery_effects {
                    if let Effect::EnsureTaskWorkspace { task_id, .. } = effect {
                        workspaces.insert(task_id.clone());
                    }
                }
                ProcessorCommand::Recover {
                    workspaces_present: workspaces,
                }
            }
            Phase::Idle if !opened => {
                // An exhausted queue is a stable, read-only scheduler fact while this owner
                // lease is held. Do not create a throwaway batch or invoke the model planner
                // merely to discover that there is nothing in the normal delivery lane.
                match executor
                    .current_queue_readiness()
                    .map_err(NativeLoopError::Port)?
                {
                    QueueReadiness::Pending => {}
                    QueueReadiness::Exhausted { escalated: 0 } => {
                        return Ok(NativeLoopOutcome::Idle);
                    }
                    QueueReadiness::Exhausted { escalated } => {
                        return Ok(NativeLoopOutcome::Escalated { count: escalated });
                    }
                }
                opened = true;
                ProcessorCommand::Open {
                    batch_id: config.batch_id.clone(),
                    base: config.base.clone(),
                    now_secs: executor.now_secs().map_err(NativeLoopError::Port)?,
                }
            }
            Phase::Idle => return Ok(NativeLoopOutcome::Completed),
            Phase::Rolling if runtime.pending_effects().is_empty() => ProcessorCommand::Advance {
                now_secs: executor.now_secs().map_err(NativeLoopError::Port)?,
            },
            Phase::Cleaning if runtime.pending_effects().is_empty() => {
                ProcessorCommand::CleanupComplete
            }
            Phase::Paused | Phase::Blocked => {
                return Ok(NativeLoopOutcome::Held {
                    reason: runtime
                        .state()
                        .blocked_reason
                        .clone()
                        .unwrap_or_else(|| format!("processor is {:?}", runtime.state().phase)),
                });
            }
            phase => {
                return Ok(NativeLoopOutcome::Held {
                    reason: format!(
                        "processor is {phase:?} with no durable effect to execute; explicit reconciliation required"
                    ),
                });
            }
        };
        let event_occurred_at = executor
            .event_occurred_at(&config.occurred_at)
            .map_err(NativeLoopError::Port)?;
        let mut effects = runtime
            .apply_at(command, &event_occurred_at)
            .map_err(NativeLoopError::Runtime)?;
        activate_recovery_effects(runtime, &mut effects, replayable_recovery_effects)
            .map_err(NativeLoopError::Runtime)?;
        let report = drive(
            runtime,
            effects,
            &config.occurred_at,
            executor,
            config.max_effects_per_turn,
        )
        .map_err(NativeLoopError::Drive)?;
        if let Some(reason) = report.held {
            return Ok(NativeLoopOutcome::Held { reason });
        }
    }
    Err(NativeLoopError::TurnLimit {
        limit: config.max_turns,
    })
}

/// Continue through successive cohorts while the owner lease remains held, stopping only when
/// the normal delivery lane is exhausted or a durable safety gate requires an operator. This is
/// the native counterpart of the legacy processor's final "return to phase 0" boundary: it
/// never reacquires the lease between cohorts and it never opens an empty batch just to poll the
/// planner.
///
/// `--batch` identifies the first cohort. Subsequent cohorts derive a deterministic, valid
/// descendant (`<first>-2`, `<first>-3`, …), so a fast series of cohorts cannot collide merely
/// because the wall clock has the same second-resolution value.
pub fn run_until_queue_exhausted<P: ProcessorPort>(
    runtime: &mut ProcessorRuntime,
    executor: &mut NativeExecutor<P>,
    config: &NativeLoopConfig,
) -> Result<NativeLoopOutcome, NativeLoopError<P::Error>> {
    let mut config = config.clone();
    let first_batch_id = config.batch_id.clone();
    let mut completed_cohort_count = 0_u32;

    loop {
        let resumed_existing_cohort = runtime.state().batch.is_some();
        match run_until_idle(runtime, executor, &config)? {
            NativeLoopOutcome::Idle => {
                return Ok(if completed_cohort_count == 0 {
                    NativeLoopOutcome::Idle
                } else {
                    NativeLoopOutcome::Completed
                });
            }
            NativeLoopOutcome::Held { reason } => return Ok(NativeLoopOutcome::Held { reason }),
            NativeLoopOutcome::Escalated { count } => {
                return Ok(NativeLoopOutcome::Escalated { count });
            }
            NativeLoopOutcome::Completed => {
                completed_cohort_count = completed_cohort_count.saturating_add(1);
                match executor
                    .current_queue_readiness()
                    .map_err(NativeLoopError::Port)?
                {
                    QueueReadiness::Pending => {}
                    QueueReadiness::Exhausted { escalated: 0 } => {
                        return Ok(NativeLoopOutcome::Completed);
                    }
                    QueueReadiness::Exhausted { escalated } => {
                        return Ok(NativeLoopOutcome::Escalated { count: escalated });
                    }
                }

                // A fresh cohort that admitted nothing cannot make queue state advance. Retrying
                // it would repeatedly invoke the planner while holding the lease; persist an
                // explicit block instead so the next invocation cannot silently restart paid
                // work before an operator resolves the planner/policy/prerequisite condition.
                if !resumed_existing_cohort && runtime.state().tasks.is_empty() {
                    let reason = "planner completed a fresh cohort without admitting a current-lane task while queue entries remain not-started".to_string();
                    let report = run_command(
                        runtime,
                        executor,
                        ProcessorCommand::Block {
                            reason: reason.clone(),
                        },
                        &config.occurred_at,
                        config.max_effects_per_turn,
                    )?;
                    return Ok(NativeLoopOutcome::Held {
                        reason: report.held.unwrap_or(reason),
                    });
                }

                config.batch_id = format!(
                    "{first_batch_id}-{}",
                    completed_cohort_count.saturating_add(1)
                );
            }
        }
    }
}

/// An unresolved mutating effect is evidence of an interrupted side effect, not permission to
/// reopen the reducer and queue a second, unrelated operation. The runtime allows only its
/// closed set of guarded VCS/control-plane repair effects to retry. A concrete port may also prove
/// that only the two task *preparation* effects have a pre-spawn reservation and exact finalized
/// receipt; actual model dispatches, commits, publication, CI repair, and cross-project curation
/// remain visible for explicit Phase-0 inspection first.
fn inspection_recovery_hold(
    runtime: &ProcessorRuntime,
    retry_safe: impl Fn(&Effect) -> bool,
) -> Option<String> {
    runtime
        .recovery_requirements()
        .into_iter()
        .find_map(|requirement| match requirement {
            RecoveryRequirement::InspectBeforeContinuing { key, effect }
                if !retry_safe(&effect) =>
            {
                Some(format!(
                    "phase-0 inspection required for outstanding effect {key}: {effect:?}"
                ))
            }
            RecoveryRequirement::InspectBeforeContinuing { .. } => None,
            RecoveryRequirement::RetryIdempotently { .. } => None,
        })
}

fn replayable_recovery_effects_for(
    runtime: &ProcessorRuntime,
    retry_safe: impl Fn(&Effect) -> bool,
) -> Vec<Effect> {
    let mut effects = runtime
        .recovery_requirements()
        .into_iter()
        .filter_map(|requirement| match requirement {
            RecoveryRequirement::RetryIdempotently { effect, .. } => Some(effect),
            RecoveryRequirement::InspectBeforeContinuing { effect, .. } if retry_safe(&effect) => {
                Some(effect)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    // A crash after `Recover` persisted its temporary Reconcile but before the scheduler
    // superseded it can leave both keys in the next checkpoint. The exact task effect remains the
    // authority; do not demand that the transient Reconcile survive its own replacement again.
    let superseding_tasks = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::EnsureTaskWorkspace { task_id, .. }
            | Effect::PrepareTaskLeaf { task_id, .. }
            | Effect::PrepareTaskReview { task_id } => Some(task_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    effects.retain(|effect| {
        !matches!(effect, Effect::Reconcile { task_id } if superseding_tasks.contains(task_id))
    });
    effects
}

fn activate_recovery_effects(
    runtime: &mut ProcessorRuntime,
    effects: &mut Vec<Effect>,
    replayable: Vec<Effect>,
) -> Result<(), RuntimeError> {
    for replay in replayable {
        if effects.contains(&replay) {
            continue;
        }
        // These exact pending effects carry their own replay proof and do not need a temporary
        // reducer reconciliation. `ReleaseLease` is only the native port's idempotent logical
        // marker; the outer owner guard performs the actual checked OS lease release. A
        // production notification has a durable pre-launch claim, so repeating it either loads
        // the terminal receipt or observes the unfinished claim without sending again.
        if matches!(replay, Effect::ReleaseLease | Effect::Notify { .. }) {
            effects.push(replay);
            continue;
        }
        if matches!(replay, Effect::PrepareKnowledgeCuration) {
            let reconstructed = effects
                .iter()
                .filter(|effect| {
                    matches!(
                        effect,
                        Effect::WriteJournalAndStatus | Effect::PrepareArchival
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            if reconstructed.len() != 2
                || effects.iter().any(|effect| {
                    !matches!(
                        effect,
                        Effect::PersistCheckpoint
                            | Effect::WriteJournalAndStatus
                            | Effect::PrepareArchival
                    )
                })
            {
                return Err(RuntimeError::CorruptCheckpoint(format!(
                    "knowledge preflight replay encountered an unexpected Phase-0 cleanup projection: {effects:?}"
                )));
            }
            runtime.supersede_recovery_effects_with_pending(&replay, &reconstructed)?;
            effects.retain(|effect| !reconstructed.contains(effect));
            effects.push(replay);
            continue;
        }
        let task_id = match &replay {
            Effect::EnsureTaskWorkspace { task_id, .. }
            | Effect::PrepareTaskLeaf { task_id, .. }
            | Effect::PrepareTaskReview { task_id } => task_id,
            _ => {
                return Err(RuntimeError::CorruptCheckpoint(format!(
                    "retry-idempotent effect was not reproduced by Phase 0: {replay:?}"
                )));
            }
        };
        let reconcile = effects
            .iter_mut()
            .find(|effect| {
                matches!(effect, Effect::Reconcile { task_id: candidate } if candidate == task_id)
            })
            .ok_or_else(|| {
                RuntimeError::CorruptCheckpoint(format!(
                    "replayable task effect for {task_id} has no Phase-0 reconciliation"
                ))
            })?;
        runtime.supersede_reconcile_with_pending_task_effect(&replay)?;
        *reconcile = replay;
    }
    Ok(())
}

/// Keep the full drive report visible to callers that schedule one explicit reducer command
/// (TUI actions, resume buttons, etc.) without duplicating its checkpoint/effect discipline.
pub fn run_command<P: ProcessorPort>(
    runtime: &mut ProcessorRuntime,
    executor: &mut NativeExecutor<P>,
    mut command: ProcessorCommand,
    occurred_at: &str,
    max_effects: usize,
) -> Result<DriveReport, NativeLoopError<P::Error>> {
    executor
        .ensure_lease_active()
        .map_err(NativeLoopError::Port)?;
    // TUI/manual commands share the same PAUSE boundary as the autonomous scheduler.  Without
    // this gate an explicit button could begin a new durable mutation after the operator had
    // requested a clean stop.
    if executor.pause_requested().map_err(NativeLoopError::Port)? {
        executor
            .write_pause_status(runtime.state())
            .map_err(NativeLoopError::Port)?;
        return Ok(DriveReport {
            completed_steps: 0,
            held: Some(format!(
                "paused by .work/PAUSE at {:?} phase/round boundary; remove it and rerun to resume through phase-0 recovery",
                runtime.state().phase
            )),
        });
    }
    let mut replayable = Vec::new();
    if let ProcessorCommand::Recover { workspaces_present } = &mut command {
        if let Some(reason) = inspection_recovery_hold(runtime, |effect| {
            executor.interrupted_effect_retry_safe(effect)
        }) {
            return Ok(DriveReport {
                completed_steps: 0,
                held: Some(reason),
            });
        }
        replayable = replayable_recovery_effects_for(runtime, |effect| {
            executor.interrupted_effect_retry_safe(effect)
        });
        for effect in &replayable {
            if let Effect::EnsureTaskWorkspace { task_id, .. } = effect {
                workspaces_present.insert(task_id.clone());
            }
        }
    }
    let event_occurred_at = executor
        .event_occurred_at(occurred_at)
        .map_err(NativeLoopError::Port)?;
    let mut effects = runtime
        .apply_at(command, &event_occurred_at)
        .map_err(NativeLoopError::Runtime)?;
    activate_recovery_effects(runtime, &mut effects, replayable)
        .map_err(NativeLoopError::Runtime)?;
    drive(runtime, effects, occurred_at, executor, max_effects).map_err(NativeLoopError::Drive)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;

    use crate::processor::{
        AdmissionCandidate, Effect, LeafKind, ProcessorCommand, ProcessorConfig,
    };
    use crate::resolvers::Level;
    use crate::runtime::ProcessorRuntime;

    use super::{
        activate_recovery_effects, inspection_recovery_hold, replayable_recovery_effects_for,
    };

    fn temp_work(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-native-loop-{name}-{}",
            std::process::id()
        ))
    }

    #[test]
    fn recovery_refuses_to_schedule_over_an_uninspected_leaf_effect() {
        let work = temp_work("inspection-hold");
        let mut runtime = ProcessorRuntime::new(ProcessorConfig::default(), &work).unwrap();
        let at = "2026-07-25T12:00:00Z";
        runtime
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                },
                at,
            )
            .unwrap();
        runtime.complete_effect("write-journal-and-status").unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Open {
                    batch_id: "B-20260725T120000Z".into(),
                    base: "main".into(),
                    now_secs: 1,
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::DependencyGraphRefreshed {
                    boundary: crate::dependency_graph::RefreshBoundary::CohortOpen,
                    outcome: crate::processor::LeafOutcome::Completed { author: None },
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::InboxReconciled {
                    free_slots: 3,
                    curation_required: false,
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(ProcessorCommand::InboxDrained { free_slots: 3 }, at)
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Admit {
                    candidates: vec![AdmissionCandidate {
                        id: "T-1".into(),
                        conflict_domain: "engine/**".into(),
                        level: Level::Coder,
                        risk: crate::resolvers::Risk::Medium,
                        ready: true,
                        current_delivery_lane: true,
                    }],
                    now_secs: 2,
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::WorkspaceReady {
                    task_id: "T-1".into(),
                },
                at,
            )
            .unwrap();
        assert_eq!(
            runtime.pending_effects().values().next(),
            Some(&Effect::PrepareTaskLeaf {
                task_id: "T-1".into(),
                kind: LeafKind::Implement,
            })
        );

        let mut resumed = ProcessorRuntime::resume(ProcessorConfig::default(), &work).unwrap();
        let hold =
            inspection_recovery_hold(&resumed, |_| false).expect("leaf must require inspection");
        assert!(hold.contains("prepare-task-leaf:T-1:implement"));
        assert!(
            inspection_recovery_hold(&resumed, |effect| matches!(
                effect,
                Effect::PrepareTaskLeaf { .. } | Effect::PrepareTaskReview { .. }
            ))
            .is_none(),
            "a reservation-protected production adapter may cross only the preparation boundary"
        );
        let first_protected = replayable_recovery_effects_for(&resumed, |effect| {
            matches!(effect, Effect::PrepareTaskLeaf { .. })
        });
        assert_eq!(first_protected.len(), 1);
        let first_effects = resumed
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::from(["T-1".into()]),
                },
                at,
            )
            .unwrap();
        assert!(
            first_effects
                .iter()
                .any(|effect| matches!(effect, Effect::Reconcile { task_id } if task_id == "T-1"))
        );
        // Crash after the reducer persisted its temporary reconciliation but before the native
        // scheduler could replace it. The next Phase 0 must collapse that stale helper key and
        // still execute only the original preparation.
        drop(resumed);
        let mut resumed = ProcessorRuntime::resume(ProcessorConfig::default(), &work).unwrap();
        let protected = replayable_recovery_effects_for(&resumed, |effect| {
            matches!(effect, Effect::PrepareTaskLeaf { .. })
        });
        assert!(matches!(
            protected.as_slice(),
            [Effect::PrepareTaskLeaf {
                task_id,
                kind: LeafKind::Implement
            }] if task_id == "T-1"
        ));
        let mut effects = resumed
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::from(["T-1".into()]),
                },
                at,
            )
            .unwrap();
        activate_recovery_effects(&mut resumed, &mut effects, protected).unwrap();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::PrepareTaskLeaf {
                task_id,
                kind: LeafKind::Implement
            } if task_id == "T-1"
        )));
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::Reconcile { task_id } if task_id == "T-1"
        )));
        assert!(!resumed.pending_effects().contains_key("reconcile:T-1"));
        assert!(
            resumed
                .pending_effects()
                .contains_key("prepare-task-leaf:T-1:implement")
        );
        resumed
            .apply_at(
                ProcessorCommand::TaskLeafPrepared {
                    task_id: "T-1".into(),
                    outcome: crate::processor::TaskLeafPreparationOutcome::Fallback,
                },
                at,
            )
            .expect("the protected preparation result must acknowledge the restored ledger key");
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn recovery_replays_a_pending_workspace_ensure_even_when_the_checkout_is_missing() {
        let work = temp_work("workspace-ensure-replay");
        let at = "2026-07-25T12:00:00Z";
        let mut runtime = ProcessorRuntime::new(ProcessorConfig::default(), &work).unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: BTreeSet::new(),
                },
                at,
            )
            .unwrap();
        runtime.complete_effect("write-journal-and-status").unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Open {
                    batch_id: "B-20260725T120000Z".into(),
                    base: "main".into(),
                    now_secs: 1,
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::DependencyGraphRefreshed {
                    boundary: crate::dependency_graph::RefreshBoundary::CohortOpen,
                    outcome: crate::processor::LeafOutcome::Completed { author: None },
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::InboxReconciled {
                    free_slots: 3,
                    curation_required: false,
                },
                at,
            )
            .unwrap();
        runtime
            .apply_at(ProcessorCommand::InboxDrained { free_slots: 3 }, at)
            .unwrap();
        runtime
            .apply_at(
                ProcessorCommand::Admit {
                    candidates: vec![AdmissionCandidate {
                        id: "T-1".into(),
                        conflict_domain: "engine/**".into(),
                        level: Level::Coder,
                        risk: crate::resolvers::Risk::Medium,
                        ready: true,
                        current_delivery_lane: true,
                    }],
                    now_secs: 2,
                },
                at,
            )
            .unwrap();
        drop(runtime);

        let mut resumed = ProcessorRuntime::resume(ProcessorConfig::default(), &work).unwrap();
        let replayable = replayable_recovery_effects_for(&resumed, |_| false);
        assert!(matches!(
            replayable.as_slice(),
            [Effect::EnsureTaskWorkspace { task_id, .. }] if task_id == "T-1"
        ));
        let mut observed = BTreeSet::new();
        for effect in &replayable {
            if let Effect::EnsureTaskWorkspace { task_id, .. } = effect {
                observed.insert(task_id.clone());
            }
        }
        let mut effects = resumed
            .apply_at(
                ProcessorCommand::Recover {
                    workspaces_present: observed,
                },
                at,
            )
            .unwrap();
        activate_recovery_effects(&mut resumed, &mut effects, replayable).unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistCheckpoint, Effect::EnsureTaskWorkspace { task_id, .. }]
                if task_id == "T-1"
        ));
        resumed
            .apply_at(
                ProcessorCommand::WorkspaceReady {
                    task_id: "T-1".into(),
                },
                at,
            )
            .expect("the replayed ensure must acknowledge its original durable key");
        let _ = fs::remove_dir_all(work);
    }
}
