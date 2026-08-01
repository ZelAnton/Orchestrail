//! Pure projection from deterministic processor transitions to the legacy-compatible event outbox.
//!
//! The projection deliberately emits the shared cutover surface defined by
//! `tests/test-engine-processor-parity.ps1`: cohort open/join/publish/close, task capture, and
//! the observable review/publication/cleanup status transitions.  It does not infer events from
//! Markdown or timestamps; a runtime supplies the before/after reducer checkpoints and a single
//! clock value, then writes the returned typed events idempotently through [`super::Outbox`].

use serde_json::{Map, Value};

use crate::processor::{
    CiDisposition, Phase, ProcessorCommand, ProcessorState, ReviewOutcome, TaskPhase, TaskRuntime,
};

use super::{Actor, ActorKind, Event, EventType, SCHEMA_VERSION, deterministic_event_id};

/// Project one accepted reducer command into ordered, durable outbox events.  `occurred_at` is
/// presentation metadata only: identity comes from durable transition coordinates and is supplied
/// by [`deterministic_event_id`], so replay with a later clock remains idempotent.
pub fn project_processor_transition(
    before: &ProcessorState,
    after: &ProcessorState,
    command: &ProcessorCommand,
    occurred_at: &str,
) -> Vec<Event> {
    let mut events = Vec::new();
    let batch_id = after
        .batch
        .as_ref()
        .or(before.batch.as_ref())
        .map(|batch| batch.id.as_str());

    if before.batch.is_none()
        && after.batch.is_some()
        && let Some(batch_id) = batch_id
    {
        events.push(event(
            EventType::CohortOpened,
            Some(batch_id),
            None,
            None,
            None,
            "open",
            occurred_at,
        ));
    }
    if matches!(command, ProcessorCommand::Admit { .. })
        && before.phase == Phase::Rolling
        && let Some(batch) = before.batch.as_ref()
    {
        events.push(cohort_round_event(
            EventType::CohortRoundStarted,
            &batch.id,
            batch.wave,
            before
                .batch
                .as_ref()
                .is_some_and(|batch| batch.admission_closed.is_some()),
            occurred_at,
        ));
    }
    if before
        .batch
        .as_ref()
        .is_some_and(|batch| batch.admission_closed.is_none())
        && after
            .batch
            .as_ref()
            .is_some_and(|batch| batch.admission_closed.is_some())
        && let Some(batch_id) = batch_id
    {
        let reason = after
            .batch
            .as_ref()
            .and_then(|batch| batch.admission_closed)
            .map(|reason| reason.as_legacy_literal())
            .unwrap_or_default();
        let mut payload = Map::new();
        payload.insert("reason".into(), Value::from(reason));
        events.push(event_with_payload(
            EventType::CohortAdmissionClosed,
            Some(batch_id),
            None,
            payload,
            format!("admission-closed:{reason}"),
            occurred_at,
        ));
    }
    if before.phase != Phase::Joining
        && after.phase == Phase::Joining
        && let Some(batch_id) = batch_id
    {
        events.push(event(
            EventType::CohortJoinStarted,
            Some(batch_id),
            None,
            None,
            None,
            "join",
            occurred_at,
        ));
    }
    // Any command that durably establishes a new terminal publication disposition owns the
    // cohort event. This includes CI-repair cap/stagnation and every terminal repair failure, not
    // only the ordinary verification outcomes. A required-but-unconfirmed observation is still
    // an operator hold rather than terminal accounting, while archive reconfirmation may move an
    // already-accounted head back to the same hold. Neither may claim the stable cohort identity.
    let publication_ci_terminal = !matches!(
        command,
        ProcessorCommand::ArchiveCiReconfirmed { .. }
            | ProcessorCommand::CiVerified {
                outcome: crate::processor::CiOutcome::RequiredUnconfirmed { .. }
            }
    );
    if publication_ci_terminal
        && before.integration.ci_disposition != after.integration.ci_disposition
        && after.integration.ci_disposition.is_some()
        && let Some(batch_id) = batch_id
        && let Some(head) = after.integration.published_head.as_deref()
        && let Some(pushed) = after.integration.publication_pushed
    {
        let mut payload = Map::new();
        payload.insert("main_sha".into(), Value::from(head));
        payload.insert("pushed".into(), Value::from(pushed));
        payload.insert(
            "tasks".into(),
            Value::Array(
                after
                    .integration
                    .merged_tasks
                    .iter()
                    .cloned()
                    .map(Value::from)
                    .collect(),
            ),
        );
        payload.insert(
            "ci".into(),
            Value::from(match after.integration.ci_disposition {
                Some(CiDisposition::Confirmed) => "confirmed",
                Some(CiDisposition::Disabled) => "disabled",
                Some(CiDisposition::UnconfirmedDegraded) => "unconfirmed-degraded",
                None => unreachable!("guarded above"),
            }),
        );
        events.push(event_with_payload(
            EventType::CohortPublished,
            Some(batch_id),
            None,
            payload,
            head.into(),
            occurred_at,
        ));
    }
    if matches!(command, ProcessorCommand::Advance { .. })
        && let Some(batch) = after.batch.as_ref().or(before.batch.as_ref())
    {
        // `CohortRuntime::wave` is the next admission wave after an accepted round, so the
        // completed round is one less. Keep wave 1 for an empty/no-admission close instead of
        // ever emitting an invalid round zero.
        let completed_wave = batch.wave.saturating_sub(1).max(1);
        events.push(cohort_round_event(
            EventType::CohortRoundClosed,
            &batch.id,
            completed_wave,
            batch.admission_closed.is_some(),
            occurred_at,
        ));
    }

    for (id, current) in &after.tasks {
        let previous = before.tasks.get(id);
        if previous.is_some_and(|task| matches!(task.phase, TaskPhase::Capturing))
            && matches!(current.phase, TaskPhase::Implementing)
            && let Some(batch_id) = batch_id
        {
            events.push(event(
                EventType::TaskCaptured,
                Some(batch_id),
                Some(id),
                None,
                None,
                "capture",
                occurred_at,
            ));
        }
        if let Some(previous) = previous {
            project_task_status(previous, current, command, occurred_at, &mut events);
        }
    }

    if before.batch.is_some()
        && after.batch.is_none()
        && after.phase == Phase::Idle
        && let Some(batch_id) = batch_id
    {
        events.push(event(
            EventType::CohortClosed,
            Some(batch_id),
            None,
            None,
            None,
            "close",
            occurred_at,
        ));
    }
    events
}

/// Reconstruct the one deterministic `published -> done` fact needed by Phase-6 archive
/// recovery. The normal reducer emits the identical event after `CleanupComplete`; this helper
/// lets the effect boundary append it earlier, while the live descriptor still exists for the
/// immutable metrics projection. Replaying it after a crash is safe because the semantic
/// coordinate (and therefore UUID) is identical.
pub fn project_task_done_transition(task: &TaskRuntime, occurred_at: &str) -> Option<Event> {
    if !matches!(task.phase, TaskPhase::Published | TaskPhase::Done) {
        return None;
    }
    let mut previous = task.clone();
    previous.phase = TaskPhase::Published;
    let mut current = task.clone();
    current.phase = TaskPhase::Done;
    let mut events = Vec::with_capacity(1);
    project_task_status(
        &previous,
        &current,
        &ProcessorCommand::CleanupComplete,
        occurred_at,
        &mut events,
    );
    events.pop()
}

fn project_task_status(
    previous: &TaskRuntime,
    current: &TaskRuntime,
    command: &ProcessorCommand,
    occurred_at: &str,
    events: &mut Vec<Event>,
) {
    // A fixer commits while its descriptor is already `на ревью`. When that commit exhausts
    // REVIEW_LOOP_MAX, the reducer moves directly `Committing -> Escalated` without exposing an
    // additional reviewer effect. Preserve the control-plane transition rather than emitting the
    // implementation-phase label that the transient runtime phase happens to carry.
    let from = if matches!(
        command,
        ProcessorCommand::TaskCommitted { task_id, .. }
            if task_id == &current.id
    ) && previous.phase == TaskPhase::Committing
        && current.phase == TaskPhase::Escalated
        && previous.review_cycles > 0
    {
        "на ревью"
    } else {
        task_status(previous.phase)
    };
    let to = task_status(current.phase);
    let review_loop = matches!(
        command,
        ProcessorCommand::TaskReview {
            task_id,
            outcome: ReviewOutcome::Findings { .. } | ReviewOutcome::Incomplete,
        } if task_id == &current.id
    ) && from == "на ревью"
        && to == "на ревью";
    if from != to || review_loop {
        // Use stable semantic coordinates, never an enum discriminant: adding a future task
        // phase must not silently change the id of an already-defined durable transition.
        let coordinate = format!("{}:{}:{}>{}", current.id, current.review_cycles, from, to);
        events.push(event(
            EventType::TaskStatusChanged,
            None,
            Some(&current.id),
            Some(from),
            Some(to),
            &coordinate,
            occurred_at,
        ));
    }
}

/// The full processor contract distinguishes merger quarantine/progress from publication.
/// Differential cutover intentionally filters the intermediate `ready -> merged -> published`
/// vocabulary because the old harness compresses it, but the native outbox must retain the real
/// transitions for the TUI, recovery and future full-fidelity comparison.
fn task_status(phase: TaskPhase) -> &'static str {
    match phase {
        TaskPhase::Capturing | TaskPhase::Implementing | TaskPhase::Committing => "в работе",
        TaskPhase::Reviewing | TaskPhase::Fixing => "на ревью",
        TaskPhase::Ready => "готова к слиянию",
        TaskPhase::ResolvingMerge => "разрешение конфликта",
        TaskPhase::Merged => "слита",
        TaskPhase::Published => "опубликована",
        TaskPhase::Done => "выполнена",
        TaskPhase::Conflict => "конфликт",
        TaskPhase::Returned | TaskPhase::Escalated => "эскалирована",
    }
}

fn event(
    event_type: EventType,
    batch_id: Option<&str>,
    task_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    coordinate: &str,
    occurred_at: &str,
) -> Event {
    let mut payload = Map::new();
    if let Some(from) = from {
        payload.insert("from".into(), Value::from(from));
    }
    if let Some(to) = to {
        payload.insert("to".into(), Value::from(to));
    }
    event_with_payload(
        event_type,
        batch_id,
        task_id,
        payload,
        coordinate.into(),
        occurred_at,
    )
}

fn cohort_round_event(
    event_type: EventType,
    batch_id: &str,
    wave: u32,
    admission_closed: bool,
    occurred_at: &str,
) -> Event {
    let mut payload = Map::new();
    payload.insert("wave".into(), Value::from(wave));
    payload.insert(
        "admission".into(),
        Value::from(if admission_closed { "closed" } else { "open" }),
    );
    event_with_payload(
        event_type,
        Some(batch_id),
        None,
        payload,
        format!("round:{wave}"),
        occurred_at,
    )
}

fn event_with_payload(
    event_type: EventType,
    batch_id: Option<&str>,
    task_id: Option<&str>,
    payload: Map<String, Value>,
    coordinate: String,
    occurred_at: &str,
) -> Event {
    let identity = format!(
        "{}|{}|{}|{}",
        event_type.as_str(),
        batch_id.unwrap_or_default(),
        task_id.unwrap_or_default(),
        coordinate
    );
    Event {
        schema_version: SCHEMA_VERSION,
        event_id: deterministic_event_id(&identity),
        occurred_at: occurred_at.into(),
        event_type,
        actor: Actor {
            kind: ActorKind::Agent,
            name: "engine".into(),
        },
        batch_id: batch_id.map(str::to_string),
        task_id: task_id.map(str::to_string),
        payload_version: 1,
        payload,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::events::fingerprint::identities;
    use crate::processor::{
        AdmissionCandidate, CiOutcome, CohortRuntime, IntegrationRuntime, Processor,
        ProcessorConfig,
    };
    use crate::resolvers::Level;

    fn processor() -> Processor {
        Processor::new(ProcessorConfig {
            max_parallel: 1,
            cohort_size: 1,
            ..ProcessorConfig::default()
        })
        .unwrap()
    }

    fn command_events(processor: &mut Processor, command: ProcessorCommand) -> Vec<Event> {
        let before = processor.state().clone();
        processor.apply(command.clone()).unwrap();
        project_processor_transition(&before, processor.state(), &command, "2026-07-24T12:00:00Z")
    }

    fn open(processor: &mut Processor) -> Vec<Event> {
        command_events(
            processor,
            ProcessorCommand::Open {
                batch_id: "B-1".into(),
                base: "base".into(),
                now_secs: 1,
            },
        )
    }

    fn apply_and_collect(
        processor: &mut Processor,
        events: &mut Vec<Event>,
        command: ProcessorCommand,
    ) {
        events.extend(command_events(processor, command));
    }

    fn compared_identities(events: &[Event]) -> Vec<String> {
        identities(events)
            .into_iter()
            .filter(|identity| {
                matches!(
                    identity.as_str(),
                    "cohort.opened|B-20260101T000000Z||"
                        | "cohort.join_started|B-20260101T000000Z||"
                        | "cohort.published|B-20260101T000000Z||"
                        | "cohort.closed|B-20260101T000000Z||"
                        | "task.captured|B-20260101T000000Z|T-101|"
                        | "task.captured|B-20260101T000000Z|T-102|"
                        | "task.status_changed||T-101|в работе>на ревью"
                        | "task.status_changed||T-102|в работе>на ревью"
                        | "task.status_changed||T-101|на ревью>готова к слиянию"
                        | "task.status_changed||T-102|на ревью>готова к слиянию"
                        | "task.status_changed||T-101|на ревью>на ревью"
                        | "task.status_changed||T-101|на ревью>эскалирована"
                        | "task.status_changed||T-101|опубликована>выполнена"
                        | "task.status_changed||T-102|опубликована>выполнена"
                )
            })
            .collect()
    }

    #[test]
    fn required_ci_hold_does_not_collide_with_the_later_terminal_publication_event() {
        let mut before = ProcessorState {
            phase: Phase::Publishing,
            batch: Some(CohortRuntime {
                id: "B-1".into(),
                base: "main".into(),
                started_at_secs: 1,
                wave: 1,
                admitted_total: 1,
                admission_closed: None,
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: None,
                events_outbox_enabled: true,
            }),
            ..ProcessorState::default()
        };
        before.integration.published_head = Some("published-head".into());
        before.integration.publication_pushed = Some(true);
        let mut held = before.clone();
        held.integration.ci_disposition = Some(CiDisposition::UnconfirmedDegraded);
        assert!(
            project_processor_transition(
                &before,
                &held,
                &ProcessorCommand::CiVerified {
                    outcome: CiOutcome::RequiredUnconfirmed {
                        reason: "check is still pending".into(),
                    },
                },
                "2026-07-24T12:00:00Z",
            )
            .is_empty()
        );

        let mut confirmed = held.clone();
        confirmed.integration.ci_disposition = Some(CiDisposition::Confirmed);
        let events = project_processor_transition(
            &held,
            &confirmed,
            &ProcessorCommand::CiVerified {
                outcome: CiOutcome::Passed,
            },
            "2026-07-24T12:01:00Z",
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::CohortPublished);

        let mut archive_failed = confirmed.clone();
        archive_failed.integration.ci_disposition = Some(CiDisposition::UnconfirmedDegraded);
        assert!(
            project_processor_transition(
                &confirmed,
                &archive_failed,
                &ProcessorCommand::ArchiveCiReconfirmed {
                    head: "published-head".into(),
                    outcome: CiOutcome::RequiredUnconfirmed {
                        reason: "check disappeared before archive".into(),
                    },
                },
                "2026-07-24T12:02:00Z",
            )
            .is_empty()
        );
    }

    #[test]
    fn projection_matches_the_legacy_two_scenario_cutover_oracle() {
        const BATCH: &str = "B-20260101T000000Z";
        let clean_config = ProcessorConfig {
            max_parallel: 2,
            cohort_size: 2,
            ..ProcessorConfig::default()
        };
        let mut clean = Processor::new(clean_config).unwrap();
        let mut events = Vec::new();
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::Open {
                batch_id: BATCH.into(),
                base: "base".into(),
                now_secs: 1,
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::Admit {
                candidates: vec![
                    AdmissionCandidate {
                        id: "T-101".into(),
                        conflict_domain: "alpha/**".into(),
                        level: Level::Coder,
                        risk: crate::resolvers::Risk::Medium,
                        ready: true,
                        current_delivery_lane: true,
                    },
                    AdmissionCandidate {
                        id: "T-102".into(),
                        conflict_domain: "beta/**".into(),
                        level: Level::Coder,
                        risk: crate::resolvers::Risk::Medium,
                        ready: true,
                        current_delivery_lane: true,
                    },
                ],
                now_secs: 2,
            },
        );
        for (task_id, commit) in [("T-101", "a101"), ("T-102", "a102")] {
            apply_and_collect(
                &mut clean,
                &mut events,
                ProcessorCommand::WorkspaceReady {
                    task_id: task_id.into(),
                },
            );
            apply_and_collect(
                &mut clean,
                &mut events,
                ProcessorCommand::TaskLeaf {
                    task_id: task_id.into(),
                    outcome: crate::processor::LeafOutcome::Completed { author: None },
                },
            );
            apply_and_collect(
                &mut clean,
                &mut events,
                ProcessorCommand::TaskCommitted {
                    task_id: task_id.into(),
                    commit: commit.into(),
                },
            );
            apply_and_collect(
                &mut clean,
                &mut events,
                ProcessorCommand::TaskReview {
                    task_id: task_id.into(),
                    outcome: ReviewOutcome::Clean {
                        review_sha: commit.into(),
                    },
                },
            );
        }
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::Advance { now_secs: 3 },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::IntegrationWorkspaceReady,
        );
        for (task_id, head) in [("T-101", "i101"), ("T-102", "i102")] {
            apply_and_collect(
                &mut clean,
                &mut events,
                ProcessorCommand::TaskMerged {
                    task_id: task_id.into(),
                    outcome: crate::processor::MergeOutcome::Merged {
                        integration_sha: head.into(),
                    },
                },
            );
        }
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::IntegrationReview {
                outcome: ReviewOutcome::Clean {
                    review_sha: "i102".into(),
                },
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::IntegrationVerified {
                head: "i102".into(),
                outcome: crate::processor::VerificationOutcome::Exempt {
                    reason: "fixture profile disabled".into(),
                },
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::Published {
                head: "i102".into(),
                pushed: true,
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::CiVerified {
                outcome: crate::processor::CiOutcome::Passed,
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::KnowledgeCurated {
                outcome: crate::processor::LeafOutcome::Completed { author: None },
            },
        );
        clean
            .acknowledge_non_command_effect(&crate::processor::Effect::WriteJournalAndStatus)
            .unwrap();
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::ArchivalPrepared {
                outcome: crate::processor::ArchivalPreparationOutcome::ReconfirmRequired {
                    required_checks: vec!["validate".into()],
                },
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::ArchiveCiReconfirmed {
                head: "i102".into(),
                outcome: crate::processor::CiOutcome::Passed,
            },
        );
        apply_and_collect(&mut clean, &mut events, ProcessorCommand::CleanupComplete);
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::DependencyGraphRefreshed {
                boundary: crate::dependency_graph::RefreshBoundary::PostArchive,
                outcome: crate::processor::LeafOutcome::Completed { author: None },
            },
        );
        apply_and_collect(
            &mut clean,
            &mut events,
            ProcessorCommand::InboxFinalizationReconciled {
                curation_required: false,
            },
        );
        apply_and_collect(&mut clean, &mut events, ProcessorCommand::CleanupComplete);

        // The legacy parity check's second, independent fixture reuses T-101 and the fixed
        // batch id. It takes one finding/fix cycle, then escalates after committing that final
        // permitted fix, before a second review under REVIEW_LOOP_MAX=1; the event identity
        // reducer must deduplicate the repeated opening and capture coordinates exactly as the
        // PowerShell harness does.
        let review_config = ProcessorConfig {
            max_parallel: 1,
            cohort_size: 1,
            review_loop_max: 1,
            stagnation_limit: 3,
            ..ProcessorConfig::default()
        };
        let mut review_cycle = Processor::new(review_config).unwrap();
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::Open {
                batch_id: BATCH.into(),
                base: "base".into(),
                now_secs: 1,
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::Admit {
                candidates: vec![AdmissionCandidate {
                    id: "T-101".into(),
                    conflict_domain: "zeta/**".into(),
                    level: Level::Coder,
                    risk: crate::resolvers::Risk::Medium,
                    ready: true,
                    current_delivery_lane: true,
                }],
                now_secs: 2,
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::WorkspaceReady {
                task_id: "T-101".into(),
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::TaskLeaf {
                task_id: "T-101".into(),
                outcome: crate::processor::LeafOutcome::Completed { author: None },
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::TaskCommitted {
                task_id: "T-101".into(),
                commit: "r101".into(),
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::TaskReview {
                task_id: "T-101".into(),
                outcome: ReviewOutcome::Findings {
                    signature: "0123456789abcdef".into(),
                    open_findings: 1,
                },
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::TaskLeaf {
                task_id: "T-101".into(),
                outcome: crate::processor::LeafOutcome::Completed { author: None },
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::TaskCommitted {
                task_id: "T-101".into(),
                commit: "r102".into(),
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::Advance { now_secs: 3 },
        );
        review_cycle
            .acknowledge_non_command_effect(&crate::processor::Effect::WriteJournalAndStatus)
            .unwrap();
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::ArchivalPrepared {
                outcome: crate::processor::ArchivalPreparationOutcome::Skipped,
            },
        );
        apply_and_collect(
            &mut review_cycle,
            &mut events,
            ProcessorCommand::CleanupComplete,
        );

        assert_eq!(
            compared_identities(&events),
            vec![
                "cohort.closed|B-20260101T000000Z||".to_string(),
                "cohort.join_started|B-20260101T000000Z||".to_string(),
                "cohort.opened|B-20260101T000000Z||".to_string(),
                "cohort.published|B-20260101T000000Z||".to_string(),
                "task.captured|B-20260101T000000Z|T-101|".to_string(),
                "task.captured|B-20260101T000000Z|T-102|".to_string(),
                "task.status_changed||T-101|в работе>на ревью".to_string(),
                "task.status_changed||T-101|на ревью>готова к слиянию".to_string(),
                "task.status_changed||T-101|на ревью>на ревью".to_string(),
                "task.status_changed||T-101|на ревью>эскалирована".to_string(),
                "task.status_changed||T-101|опубликована>выполнена".to_string(),
                "task.status_changed||T-102|в работе>на ревью".to_string(),
                "task.status_changed||T-102|на ревью>готова к слиянию".to_string(),
                "task.status_changed||T-102|опубликована>выполнена".to_string(),
            ]
        );
    }

    #[test]
    fn projection_matches_shared_capture_and_review_fingerprint_surface() {
        let mut processor = processor();
        processor
            .apply(ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            })
            .unwrap();
        let mut events = open(&mut processor);
        command_events(
            &mut processor,
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
        );
        events.extend(command_events(
            &mut processor,
            ProcessorCommand::WorkspaceReady {
                task_id: "T-1".into(),
            },
        ));
        command_events(
            &mut processor,
            ProcessorCommand::TaskLeaf {
                task_id: "T-1".into(),
                outcome: crate::processor::LeafOutcome::Completed { author: None },
            },
        );
        events.extend(command_events(
            &mut processor,
            ProcessorCommand::TaskCommitted {
                task_id: "T-1".into(),
                commit: "a1".into(),
            },
        ));
        events.extend(command_events(
            &mut processor,
            ProcessorCommand::TaskReview {
                task_id: "T-1".into(),
                outcome: ReviewOutcome::Findings {
                    signature: "0123456789abcdef".into(),
                    open_findings: 1,
                },
            },
        ));
        assert_eq!(
            identities(&events),
            vec![
                "cohort.opened|B-1||".to_string(),
                "task.captured|B-1|T-1|".to_string(),
                "task.status_changed||T-1|в работе>на ревью".to_string(),
                "task.status_changed||T-1|на ревью>на ревью".to_string(),
            ]
        );
    }

    #[test]
    fn publication_and_cleanup_preserve_merged_and_published_statuses() {
        let mut before = ProcessorState {
            phase: Phase::Publishing,
            batch: Some(CohortRuntime {
                id: "B-1".into(),
                base: "base".into(),
                started_at_secs: 1,
                wave: 1,
                admitted_total: 1,
                admission_closed: None,
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: None,
                events_outbox_enabled: true,
            }),
            tasks: std::collections::BTreeMap::new(),
            integration: IntegrationRuntime::default(),
            ..ProcessorState::default()
        };
        let mut merged = crate::processor::TaskRuntime {
            id: "T-1".into(),
            conflict_domain: "engine/**".into(),
            level: Some(Level::Coder),
            risk: Some(crate::resolvers::Risk::Medium),
            wave: 1,
            phase: TaskPhase::Merged,
            leaf_attempts: Default::default(),
            review_cycles: 0,
            review_signatures: Vec::new(),
            pending_fix_open_findings: None,
            implementation_author: None,
            previous_review_sha: None,
            review_sha: None,
            reason: None,
            imported_recovery_intent: None,
            leaf_sessions: std::collections::BTreeMap::new(),
        };
        before.tasks.insert(merged.id.clone(), merged.clone());
        let mut after = before.clone();
        merged.phase = TaskPhase::Published;
        after.tasks.insert(merged.id.clone(), merged.clone());
        after.integration.merged_tasks.insert("T-1".into());
        after.integration.published_head = Some("main1".into());
        after.integration.publication_pushed = Some(false);
        let publish = project_processor_transition(
            &before,
            &after,
            &ProcessorCommand::Published {
                head: "main1".into(),
                pushed: false,
            },
            "2026-07-24T12:00:00Z",
        );
        assert_eq!(
            identities(&publish),
            vec!["task.status_changed||T-1|слита>опубликована".to_string()]
        );

        let mut after_ci = after.clone();
        after_ci.integration.ci_disposition = Some(CiDisposition::Disabled);
        let ci = project_processor_transition(
            &after,
            &after_ci,
            &ProcessorCommand::CiVerified {
                outcome: crate::processor::CiOutcome::LocalOnly,
            },
            "2026-07-24T12:00:01Z",
        );
        assert_eq!(identities(&ci), vec!["cohort.published|B-1||".to_string()]);
        assert_eq!(ci[0].payload["main_sha"], "main1");
        assert_eq!(ci[0].payload["pushed"], false);
        assert_eq!(ci[0].payload["tasks"], serde_json::json!(["T-1"]));
        assert_eq!(ci[0].payload["ci"], "disabled");

        let mut unconfirmed_state = after.clone();
        unconfirmed_state.integration.ci_disposition = Some(CiDisposition::UnconfirmedDegraded);
        let unconfirmed = project_processor_transition(
            &after,
            &unconfirmed_state,
            &ProcessorCommand::CiFix {
                outcome: crate::processor::LeafOutcome::Escalated {
                    reason: "manual intervention required".into(),
                },
            },
            "2026-07-24T12:00:02Z",
        );
        assert_eq!(unconfirmed[0].payload["ci"], "unconfirmed-degraded");

        for terminal_command in [
            ProcessorCommand::CiVerified {
                outcome: CiOutcome::Failed {
                    signature: "ci-red".into(),
                    reason: "repair cap reached".into(),
                },
            },
            ProcessorCommand::CiFix {
                outcome: crate::processor::LeafOutcome::RetryableFailure {
                    reason: "runner failed".into(),
                },
            },
            ProcessorCommand::CiFixPrepared {
                outcome: crate::processor::CiFixPreparationOutcome::Escalated {
                    reason: "protocol failure".into(),
                },
            },
        ] {
            let terminal = project_processor_transition(
                &after,
                &unconfirmed_state,
                &terminal_command,
                "2026-07-24T12:00:03Z",
            );
            assert_eq!(
                identities(&terminal),
                vec!["cohort.published|B-1||".to_string()]
            );
        }

        let mut closed = after_ci.clone();
        closed.tasks.get_mut("T-1").unwrap().phase = TaskPhase::Done;
        closed.batch = None;
        closed.phase = Phase::Idle;
        let cleanup = project_processor_transition(
            &after_ci,
            &closed,
            &ProcessorCommand::CleanupComplete,
            "2026-07-24T12:00:01Z",
        );
        assert_eq!(
            identities(&cleanup),
            vec![
                "cohort.closed|B-1||".to_string(),
                "task.status_changed||T-1|опубликована>выполнена".to_string(),
            ]
        );
    }

    #[test]
    fn merger_quarantine_projects_a_conflict_not_an_escalation() {
        let mut before = ProcessorState {
            phase: Phase::Joining,
            tasks: std::collections::BTreeMap::new(),
            ..ProcessorState::default()
        };
        let mut task = crate::processor::TaskRuntime {
            id: "T-1".into(),
            conflict_domain: "engine/**".into(),
            level: Some(Level::Coder),
            risk: Some(crate::resolvers::Risk::Medium),
            wave: 1,
            phase: TaskPhase::Ready,
            leaf_attempts: Default::default(),
            review_cycles: 0,
            review_signatures: Vec::new(),
            pending_fix_open_findings: None,
            implementation_author: None,
            previous_review_sha: None,
            review_sha: Some("reviewed1".into()),
            reason: None,
            imported_recovery_intent: None,
            leaf_sessions: std::collections::BTreeMap::new(),
        };
        before.tasks.insert(task.id.clone(), task.clone());
        let mut after = before.clone();
        task.phase = TaskPhase::Conflict;
        task.reason = Some("merge conflict".into());
        after.tasks.insert(task.id.clone(), task);
        let events = project_processor_transition(
            &before,
            &after,
            &ProcessorCommand::TaskMerged {
                task_id: "T-1".into(),
                outcome: crate::processor::MergeOutcome::Quarantined {
                    reason: "merge conflict".into(),
                },
            },
            "2026-07-24T12:00:00Z",
        );
        assert_eq!(
            identities(&events),
            vec!["task.status_changed||T-1|готова к слиянию>конфликт".to_string()]
        );
    }

    #[test]
    fn admission_and_advance_project_round_boundaries_with_stable_wave_coordinates() {
        let before = ProcessorState {
            phase: Phase::Rolling,
            batch: Some(CohortRuntime {
                id: "B-1".into(),
                base: "base".into(),
                started_at_secs: 1,
                wave: 1,
                admitted_total: 0,
                admission_closed: None,
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: None,
                events_outbox_enabled: true,
            }),
            ..ProcessorState::default()
        };
        let mut admitted = before.clone();
        admitted.batch.as_mut().unwrap().wave = 2;
        let start = project_processor_transition(
            &before,
            &admitted,
            &ProcessorCommand::Admit {
                candidates: Vec::new(),
                now_secs: 2,
            },
            "2026-07-24T12:00:00Z",
        );
        assert_eq!(identities(&start), vec!["cohort.round_started|B-1||"]);
        assert_eq!(start[0].payload["wave"], 1);
        assert_eq!(start[0].payload["admission"], "open");

        let mut advanced = admitted.clone();
        advanced.phase = Phase::Joining;
        advanced.batch.as_mut().unwrap().admission_closed =
            Some(crate::resolvers::CloseReason::CohortSize.into());
        let close = project_processor_transition(
            &admitted,
            &advanced,
            &ProcessorCommand::Advance { now_secs: 3 },
            "2026-07-24T12:00:01Z",
        );
        assert_eq!(
            identities(&close),
            vec![
                "cohort.admission_closed|B-1||".to_string(),
                "cohort.join_started|B-1||".to_string(),
                "cohort.round_closed|B-1||".to_string(),
            ]
        );
        let round_closed = close
            .iter()
            .find(|event| event.event_type == EventType::CohortRoundClosed)
            .unwrap();
        assert_eq!(round_closed.payload["wave"], 1);
        assert_eq!(round_closed.payload["admission"], "closed");
    }
}
