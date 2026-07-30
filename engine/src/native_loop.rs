//! Deterministic turn scheduler for the native processor.
//!
//! The reducer deliberately does not read clocks or poll the queue by itself. This loop supplies
//! only the three operator-independent turn boundaries it owns: phase-0 recovery inspection,
//! rolling `Advance`, and cleanup completion after every durable cleanup effect was acknowledged.
//! All leaf/VCS/forge work remains in [`crate::native::NativeExecutor`].

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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

/// Floor for one `--watch` poll interval. A misconfigured zero interval must degrade into a slow
/// poll, never into a busy loop that reads the control plane as fast as the CPU allows.
const MIN_WATCH_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// The operator's clean-stop fact for `--watch`, observed at poll boundaries only.
///
/// Deliberately *not* a [`crate::supervise::CancellationProbe`]: this probe is fallible, and the
/// distinction is load-bearing. An embedder backs the fact with an observation that can fail (the
/// CLI reads a control-plane marker), and a failed observation is not evidence that an operator
/// asked for a stop. Collapsing the two would let a long-lived service exit successfully on a
/// transient `.work` I/O failure while naming a marker file nobody created. `Err` therefore still
/// ends waiting fail-closed — a control plane this process cannot read is no basis for opening
/// another cohort — but as [`NativeLoopError::WatchStopUnobservable`], with the cause preserved.
#[derive(Clone)]
pub struct WatchStopProbe {
    check: Arc<dyn Fn() -> Result<bool, String> + Send + Sync + 'static>,
}

impl WatchStopProbe {
    pub fn new(check: impl Fn() -> Result<bool, String> + Send + Sync + 'static) -> Self {
        Self {
            check: Arc::new(check),
        }
    }

    /// Observe the stop fact. `Ok(false)` means "keep watching"; `Err` means the fact could not be
    /// established at all and must never be reported as an operator-requested stop.
    pub fn stop_requested(&self) -> Result<bool, String> {
        (self.check)()
    }
}

impl fmt::Debug for WatchStopProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WatchStopProbe(..)")
    }
}

/// Bounded waiting policy for the continuous `--watch` scheduler.
///
/// The wait is a plain backoff poll of the same read-only queue fact the cohort boundary already
/// uses: the first interval after a drained lane is short so a populator's new task starts almost
/// immediately, and every consecutive empty poll doubles the interval up to
/// [`Self::max_poll_interval`], so an idle service costs one directory read per ceiling instead of
/// a spin. The interval resets after each drained lane.
///
/// `stop` is the operator's clean-shutdown fact. It is injected rather than read from a fixed file
/// because "stop watching" is an operating-mode decision of the embedder: the CLI backs it with an
/// operator-owned marker file, and an interactive embedder can back it with an in-process flag.
#[derive(Debug, Clone)]
pub struct WatchConfig {
    pub initial_poll_interval: Duration,
    pub max_poll_interval: Duration,
    pub stop: Option<WatchStopProbe>,
}

impl WatchConfig {
    pub fn new(initial_poll_interval: Duration, max_poll_interval: Duration) -> Self {
        Self {
            initial_poll_interval,
            max_poll_interval,
            stop: None,
        }
    }

    /// Bind the operator-owned clean-stop fact observed at every poll boundary.
    pub fn with_stop_probe(mut self, stop: WatchStopProbe) -> Self {
        self.stop = Some(stop);
        self
    }
}

/// Why one `--watch` wait ended. Only [`Self::CurrentLaneWork`] permits another delivery wave.
enum WatchWakeup {
    CurrentLaneWork,
    Stopped,
    Held { reason: String },
    Escalated { count: usize },
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
    /// Continuous `--watch` waiting ended because the operator requested a clean stop. `waves`
    /// counts the delivery lanes fully drained during this invocation, so a stopped service can
    /// still be distinguished from one that never had work. Only [`run_watching`] returns this.
    WatchStopped { waves: usize },
}

#[derive(Debug)]
pub enum NativeLoopError<E> {
    Runtime(RuntimeError),
    Port(NativeError<E>),
    Drive(DriveError<NativeError<E>>),
    TurnLimit {
        limit: usize,
    },
    /// A [`WatchStopProbe`] could not observe the operator's stop fact at a poll boundary. Waiting
    /// ends fail-closed, but as a failure: an unobservable control plane must never be reported as
    /// [`NativeLoopOutcome::WatchStopped`], which would tell a service supervisor that an operator
    /// ended this process on purpose.
    WatchStopUnobservable(String),
}

impl<E: fmt::Display> fmt::Display for NativeLoopError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "native runtime failure: {error}"),
            Self::Port(error) => write!(f, "native processor port failure: {error}"),
            Self::Drive(error) => write!(f, "native effect drive failed: {error}"),
            Self::TurnLimit { limit } => write!(f, "native loop exceeded turn limit {limit}"),
            Self::WatchStopUnobservable(message) => {
                write!(f, "--watch stop request could not be observed: {message}")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NativeLoopError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Port(error) => Some(error),
            Self::Drive(error) => Some(error),
            Self::TurnLimit { .. } | Self::WatchStopUnobservable(_) => None,
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
            // A single cohort has no waiting phase, so it cannot report a watch stop. Forward it
            // unchanged rather than reinterpreting an outcome this function did not produce.
            NativeLoopOutcome::WatchStopped { waves } => {
                return Ok(NativeLoopOutcome::WatchStopped { waves });
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

/// Keep one owner lease across successive delivery waves: drain the current lane exactly as
/// [`run_until_queue_exhausted`] does, then — instead of exiting — wait at a settled boundary for
/// new current-lane work and drain it again. This is the continuous `--watch` mode of the CLI.
///
/// The wait is deliberately the weakest possible operation: a bounded backoff poll of the same
/// read-only queue fact the cohort boundary already trusts (see [`WatchConfig`]). Every poll
/// boundary is a full stop boundary, in this order:
///
/// 1. the owner lease must still be held (a renewal worker that lost ownership ends the loop with
///    an error instead of letting a former owner open another cohort);
/// 2. an operator stop request ([`WatchConfig::stop`]) ends waiting cleanly with
///    [`NativeLoopOutcome::WatchStopped`], while a stop fact that could not be *observed* ends it
///    with [`NativeLoopError::WatchStopUnobservable`] — both stop before the next wave, but only
///    the first one is a successful, operator-attributed exit;
/// 3. `.work/PAUSE` ends waiting as a hold, exactly like the per-turn boundary of
///    [`run_until_idle`] — the durable ledger is already settled here, so no in-flight mutation
///    can be interrupted;
/// 4. the interval is spent, and only then is the queue re-read; terminal escalations end watching
///    with [`NativeLoopOutcome::Escalated`] rather than being hidden behind an endless wait.
///
/// Each new wave receives a deterministic, distinct descendant batch id (`<first>-w2`, `<first>-w3`,
/// …) so a second wave can never reuse the first wave's cohort identity, and a fresh lifecycle
/// timestamp sampled through the port clock.
///
/// A hold, an escalation, or a scheduler error inside a wave is returned unchanged: `--watch` adds
/// waiting, never a second opinion about a safety gate.
pub fn run_watching<P: ProcessorPort>(
    runtime: &mut ProcessorRuntime,
    executor: &mut NativeExecutor<P>,
    config: &NativeLoopConfig,
    watch: &WatchConfig,
) -> Result<NativeLoopOutcome, NativeLoopError<P::Error>> {
    let initial_interval = watch.initial_poll_interval.max(MIN_WATCH_POLL_INTERVAL);
    let max_interval = watch.max_poll_interval.max(initial_interval);
    let mut wave_config = config.clone();
    let mut waves = 0_usize;
    let mut wave_number = 1_u32;

    loop {
        match run_until_queue_exhausted(runtime, executor, &wave_config)? {
            NativeLoopOutcome::Completed => waves = waves.saturating_add(1),
            NativeLoopOutcome::Idle => {}
            outcome @ (NativeLoopOutcome::Held { .. }
            | NativeLoopOutcome::Escalated { .. }
            | NativeLoopOutcome::WatchStopped { .. }) => return Ok(outcome),
        }
        match wait_for_current_lane_work(
            runtime,
            executor,
            watch.stop.as_ref(),
            initial_interval,
            max_interval,
        )? {
            WatchWakeup::CurrentLaneWork => {}
            WatchWakeup::Stopped => return Ok(NativeLoopOutcome::WatchStopped { waves }),
            WatchWakeup::Held { reason } => return Ok(NativeLoopOutcome::Held { reason }),
            WatchWakeup::Escalated { count } => {
                return Ok(NativeLoopOutcome::Escalated { count });
            }
        }
        wave_number = wave_number.saturating_add(1);
        wave_config.batch_id = format!("{}-w{wave_number}", config.batch_id);
        wave_config.occurred_at = executor
            .event_occurred_at(&config.occurred_at)
            .map_err(NativeLoopError::Port)?;
    }
}

/// Poll for new current-lane work at settled boundaries only. The caller has just observed a
/// drained lane, so the durable checkpoint and effect ledger are consistent and every exit from
/// this function is a safe stopping point for the process.
///
/// Each iteration spends its interval *before* re-reading the queue. The caller's own exhaustion
/// check already read the lane microseconds ago, so an immediate re-read would only be a redundant
/// control-plane read — and, were two consecutive reads ever to disagree, a poll-free spin between
/// this function and a wave that finds nothing to open. Operator markers are still observed first,
/// so a stop or pause never has to wait out an interval it did not cause.
fn wait_for_current_lane_work<P: ProcessorPort>(
    runtime: &ProcessorRuntime,
    executor: &mut NativeExecutor<P>,
    stop: Option<&WatchStopProbe>,
    initial_interval: Duration,
    max_interval: Duration,
) -> Result<WatchWakeup, NativeLoopError<P::Error>> {
    let mut interval = initial_interval;
    loop {
        // Ownership first: neither the operator markers nor the queue may be turned into a
        // scheduling decision by a process whose lease renewal has already failed.
        executor
            .ensure_lease_active()
            .map_err(NativeLoopError::Port)?;
        // A stop fact that cannot be observed stops waiting too, but as an error: only a *proven*
        // request may be reported as the operator's clean stop.
        if let Some(stop) = stop
            && stop
                .stop_requested()
                .map_err(NativeLoopError::WatchStopUnobservable)?
        {
            return Ok(WatchWakeup::Stopped);
        }
        if executor.pause_requested().map_err(NativeLoopError::Port)? {
            executor
                .write_pause_status(runtime.state())
                .map_err(NativeLoopError::Port)?;
            return Ok(WatchWakeup::Held {
                reason: format!(
                    "paused by .work/PAUSE while --watch waited for new current-lane work at the {:?} boundary; remove it and rerun to resume through phase-0 recovery",
                    runtime.state().phase
                ),
            });
        }
        executor
            .wait_for_watch_interval(interval)
            .map_err(NativeLoopError::Port)?;
        match executor
            .current_queue_readiness()
            .map_err(NativeLoopError::Port)?
        {
            QueueReadiness::Pending => return Ok(WatchWakeup::CurrentLaneWork),
            QueueReadiness::Exhausted { escalated: 0 } => {}
            QueueReadiness::Exhausted { escalated } => {
                return Ok(WatchWakeup::Escalated { count: escalated });
            }
        }
        interval = interval.saturating_mul(2).min(max_interval);
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
    use std::collections::{BTreeSet, VecDeque};
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::native::test_support::Port;
    use crate::native::{NativeError, NativeExecutor, QueueReadiness};
    use crate::ownership::{LeaseHeartbeat, LeaseStatus, LeaseStore};
    use crate::processor::{
        AdmissionCandidate, Effect, LeafKind, ProcessorCommand, ProcessorConfig,
    };
    use crate::resolvers::Level;
    use crate::runtime::ProcessorRuntime;
    use crate::state::now_epoch_secs;
    use crate::supervise::CancellationProbe;

    use super::{
        NativeLoopConfig, NativeLoopError, NativeLoopOutcome, WatchConfig, WatchStopProbe,
        activate_recovery_effects, inspection_recovery_hold, replayable_recovery_effects_for,
        run_watching,
    };

    fn temp_work(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-native-loop-{name}-{}",
            std::process::id()
        ))
    }

    fn watch_config(batch_id: &str) -> NativeLoopConfig {
        NativeLoopConfig {
            batch_id: batch_id.into(),
            base: "main".into(),
            occurred_at: "2026-07-30T10:00:00Z".into(),
            max_turns: 64,
            max_effects_per_turn: 512,
        }
    }

    fn flag_probe(flag: &Arc<AtomicBool>) -> CancellationProbe {
        let flag = Arc::clone(flag);
        CancellationProbe::new(move || flag.load(Ordering::SeqCst))
    }

    /// A stop fact that is always *observable*: the flag itself is the operator's answer, so every
    /// read succeeds. Fixtures for an unobservable fact return `Err` instead.
    fn flag_stop_probe(flag: &Arc<AtomicBool>) -> WatchStopProbe {
        let flag = Arc::clone(flag);
        WatchStopProbe::new(move || Ok(flag.load(Ordering::SeqCst)))
    }

    fn fresh_runtime(work: &std::path::Path) -> ProcessorRuntime {
        let _ = fs::remove_dir_all(work);
        ProcessorRuntime::new(ProcessorConfig::default(), work).expect("fresh native runtime")
    }

    #[test]
    fn watch_backs_off_while_the_lane_is_empty_and_opens_a_distinct_wave_on_new_work() {
        let work = temp_work("watch-backoff");
        let mut runtime = fresh_runtime(&work);
        let stop = Arc::new(AtomicBool::new(false));
        let mut executor = NativeExecutor::new(Port {
            // The first answer drains the initial lane; the next two keep the watch loop waiting
            // (each answer follows one spent interval); the last two admit exactly one further
            // delivery wave.
            readiness: VecDeque::from([
                QueueReadiness::Exhausted { escalated: 0 },
                QueueReadiness::Exhausted { escalated: 0 },
                QueueReadiness::Exhausted { escalated: 0 },
                QueueReadiness::Pending,
                QueueReadiness::Pending,
            ]),
            flip_after_waits: Some((4, Arc::clone(&stop))),
            ..Default::default()
        });

        let outcome = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch"),
            &WatchConfig::new(Duration::from_millis(50), Duration::from_millis(100))
                .with_stop_probe(flag_stop_probe(&stop)),
        )
        .expect("watch scheduler");

        assert_eq!(outcome, NativeLoopOutcome::WatchStopped { waves: 1 });
        assert_eq!(
            executor.port().waits,
            vec![
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(100),
                Duration::from_millis(50),
            ],
            "an empty lane doubles the poll interval up to the ceiling and resets it after a drained lane"
        );
        assert_eq!(
            executor.port().planned_batches,
            vec!["B-watch-w2".to_string()],
            "the woken wave must open a distinct descendant cohort id, never reuse the first one"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn watch_stops_at_a_pause_boundary_without_opening_another_cohort() {
        let work = temp_work("watch-pause");
        let mut runtime = fresh_runtime(&work);
        let mut executor = NativeExecutor::new(Port {
            pause_after_waits: Some(2),
            ..Default::default()
        });

        let outcome = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch-pause"),
            &WatchConfig::new(Duration::from_millis(50), Duration::from_millis(60)),
        )
        .expect("watch scheduler");

        assert!(
            matches!(outcome, NativeLoopOutcome::Held { ref reason }
                if reason.contains(".work/PAUSE") && reason.contains("--watch")),
            "an operator pause during waiting must hold with an explicit reason: {outcome:?}"
        );
        assert_eq!(
            executor.port().waits,
            vec![Duration::from_millis(50), Duration::from_millis(60)],
            "PAUSE is observed at the poll boundary that follows the wait, not inside it"
        );
        assert_eq!(
            executor.port().pause_status_writes,
            1,
            "the derived pause status is materialized exactly once before the caller releases the lease"
        );
        assert!(
            executor.port().planned_batches.is_empty(),
            "a paused watch loop must never open another cohort or call the planner"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn an_operator_stop_request_ends_watch_waiting_cleanly() {
        let work = temp_work("watch-stop");
        let mut runtime = fresh_runtime(&work);
        let stop = Arc::new(AtomicBool::new(false));
        let mut executor = NativeExecutor::new(Port {
            flip_after_waits: Some((2, Arc::clone(&stop))),
            ..Default::default()
        });

        let outcome = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch-stop"),
            &WatchConfig::new(Duration::from_millis(50), Duration::from_millis(50))
                .with_stop_probe(flag_stop_probe(&stop)),
        )
        .expect("watch scheduler");

        assert_eq!(outcome, NativeLoopOutcome::WatchStopped { waves: 0 });
        assert_eq!(executor.port().waits.len(), 2);
        assert_eq!(
            executor.port().pause_status_writes,
            0,
            "a requested clean stop is not an operator pause and must not rewrite pause status"
        );
        assert!(executor.port().planned_batches.is_empty());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_stop_request_present_before_the_first_wait_degrades_to_a_single_run() {
        let work = temp_work("watch-stop-cold");
        let mut runtime = fresh_runtime(&work);
        let stop = Arc::new(AtomicBool::new(true));
        let mut executor = NativeExecutor::new(Port::default());

        let outcome = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch-cold-stop"),
            &WatchConfig::new(Duration::from_millis(50), Duration::from_millis(50))
                .with_stop_probe(flag_stop_probe(&stop)),
        )
        .expect("watch scheduler");

        assert_eq!(outcome, NativeLoopOutcome::WatchStopped { waves: 0 });
        assert!(
            executor.port().waits.is_empty(),
            "an already requested stop must be observed before the first wait is spent"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn watch_reports_an_escalation_observed_while_waiting_instead_of_hiding_it() {
        let work = temp_work("watch-escalated");
        let mut runtime = fresh_runtime(&work);
        let mut executor = NativeExecutor::new(Port {
            readiness: VecDeque::from([
                QueueReadiness::Exhausted { escalated: 0 },
                QueueReadiness::Exhausted { escalated: 3 },
            ]),
            ..Default::default()
        });

        let outcome = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch-escalated"),
            &WatchConfig::new(Duration::from_millis(50), Duration::from_millis(50)),
        )
        .expect("watch scheduler");

        assert_eq!(outcome, NativeLoopOutcome::Escalated { count: 3 });
        assert_eq!(
            executor.port().waits.len(),
            1,
            "a queue that only holds escalations is reported at the first poll, not waited on for ever"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_lease_lost_while_watch_waits_stops_before_the_next_wave() {
        let work = temp_work("watch-lease-lost");
        let mut runtime = fresh_runtime(&work);
        let lost = Arc::new(AtomicBool::new(false));
        let mut executor = NativeExecutor::new(Port {
            // The lane is drained once, and every later answer offers new work: the lost lease
            // must win over that work rather than letting a former owner drain it.
            readiness: VecDeque::from([
                QueueReadiness::Exhausted { escalated: 0 },
                QueueReadiness::Pending,
                QueueReadiness::Pending,
            ]),
            flip_after_waits: Some((1, Arc::clone(&lost))),
            ..Default::default()
        })
        .with_cancellation_probe(flag_probe(&lost));

        let error = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch-lease"),
            &WatchConfig::new(Duration::from_millis(50), Duration::from_millis(50)),
        )
        .expect_err("a former owner must not continue watching");

        assert!(
            matches!(error, NativeLoopError::Port(NativeError::LeaseLost)),
            "losing ownership during a wait is a fail-closed error, not a clean stop: {error}"
        );
        assert_eq!(executor.port().waits.len(), 1);
        assert!(
            executor.port().planned_batches.is_empty(),
            "the woken wave must not open a cohort after ownership was lost"
        );
        let _ = fs::remove_dir_all(work);
    }

    /// A stop fact that could not be read is not evidence of an operator decision. The difference
    /// is what a service supervisor sees: a clean stop is a successful exit attributed to the
    /// operator's marker, an unobservable fact must be an error that names its own cause.
    #[test]
    fn an_unobservable_stop_request_fails_watch_instead_of_reporting_a_clean_stop() {
        let work = temp_work("watch-stop-unobservable");
        let mut runtime = fresh_runtime(&work);
        let stop_reads = Arc::new(AtomicUsize::new(0));
        let mut executor = NativeExecutor::new(Port {
            // The lane is drained twice, and every later answer offers new work: an unobservable
            // stop fact must win over that work rather than opening another wave.
            readiness: VecDeque::from([
                QueueReadiness::Exhausted { escalated: 0 },
                QueueReadiness::Exhausted { escalated: 0 },
                QueueReadiness::Pending,
                QueueReadiness::Pending,
            ]),
            ..Default::default()
        });
        let reads = Arc::clone(&stop_reads);
        let stop = WatchStopProbe::new(move || {
            // The first boundary reads a readable, unset marker; the next one fails the way a real
            // `.work` read fails (denied access, a replaced path component, a failing disk).
            if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(false)
            } else {
                Err("control-plane path is not a plain directory".to_string())
            }
        });

        let error = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch-unobservable"),
            &WatchConfig::new(Duration::from_millis(50), Duration::from_millis(50))
                .with_stop_probe(stop),
        )
        .expect_err("an unobservable stop fact must not be reported as a clean operator stop");

        assert!(
            matches!(&error, NativeLoopError::WatchStopUnobservable(message)
                if message.contains("not a plain directory")),
            "the failure must carry the observation's own cause: {error}"
        );
        assert_eq!(
            stop_reads.load(Ordering::SeqCst),
            2,
            "an observable, unset stop fact keeps watching; the failing read ends it"
        );
        assert_eq!(
            executor.port().waits.len(),
            1,
            "the loop stops at the boundary whose stop fact could not be observed"
        );
        assert_eq!(
            executor.port().pause_status_writes,
            0,
            "an unreadable stop fact is not an operator pause and must not rewrite pause status"
        );
        assert!(
            executor.port().planned_batches.is_empty(),
            "no wave may be opened once the stop fact cannot be observed"
        );
        let _ = fs::remove_dir_all(work);
    }

    /// The renewal worker is a background thread owned by the CLI, so the guarantee under test is
    /// that `--watch` waiting does not starve it: a wait longer than the whole lease TTL must leave
    /// the durable `lease.json` record renewed and live rather than stale.
    #[test]
    fn watch_waiting_longer_than_the_lease_ttl_keeps_the_owner_lease_live() {
        let work = temp_work("watch-heartbeat");
        let mut runtime = fresh_runtime(&work);
        let store = LeaseStore::new(&work);
        let acquired = store
            .acquire("engine-watch", &work, 2, now_epoch_secs())
            .expect("acquire the watch fixture lease");
        let heartbeat = LeaseHeartbeat::start(store.clone(), &acquired);
        let stop = Arc::new(AtomicBool::new(false));
        let mut executor = NativeExecutor::new(Port {
            sleep_during_wait: true,
            flip_after_waits: Some((1, Arc::clone(&stop))),
            ..Default::default()
        })
        .with_cancellation_probe(heartbeat.cancellation_probe());

        let outcome = run_watching(
            &mut runtime,
            &mut executor,
            &watch_config("B-watch-heartbeat"),
            &WatchConfig::new(Duration::from_millis(2_500), Duration::from_millis(2_500))
                .with_stop_probe(flag_stop_probe(&stop)),
        )
        .expect("watch scheduler");

        assert_eq!(outcome, NativeLoopOutcome::WatchStopped { waves: 0 });
        heartbeat
            .stop()
            .expect("the renewal worker must not have lost ownership while watch waited");
        let status = store
            .status(now_epoch_secs())
            .expect("read the lease record");
        assert!(
            matches!(&status, LeaseStatus::Live { record, liveness }
                if record.owner_id == "engine-watch"
                    && record.generation > acquired.generation
                    && liveness.heartbeat_age_secs < record.ttl_seconds),
            "the heartbeat must keep renewing the lease across a wait longer than its TTL: {status:?}"
        );
        assert!(
            store
                .release("engine-watch", now_epoch_secs())
                .expect("release the fixture lease")
        );
        let _ = fs::remove_dir_all(work);
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
