//! Real ProcessKit-backed proof that a task reviewer consumes immutable range evidence.

use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use orchestrail_engine::events::{EventType, OUTBOX_FILE, TailReader};
use orchestrail_engine::headless::{HeadlessConfig, HeadlessExternalPort};
use orchestrail_engine::native::{TaskEffect, TaskEffectResult};
use orchestrail_engine::native_port::{ExternalPort, ExternalTaskEffect};
use orchestrail_engine::processor::{CohortRuntime, ProcessorState, TaskPhase, TaskRuntime};
use orchestrail_engine::resolvers::{CodexReviewer, Level, Risk};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn task_reviewer_reads_the_immutable_range_before_emitting_a_clean_gate() {
    let root = std::env::temp_dir().join(format!(
        "orchestrail-headless-reviewer-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let work = root.join(".work");
    let workspace = root.join(".work/worktrees/T-1");
    fs::create_dir_all(work.join("native-evidence")).unwrap();
    fs::create_dir_all(&workspace).unwrap();
    fs::write(
        work.join("native-evidence/review-range-T-1-1.json"),
        r#"{"schema":"orchestrail/task-review-range@1","base":"base","head":"review-head","files":[{"path":"src/lib.rs","old_path":null,"diff_sha256":"fixture-review-sentinel:T-1","raw":"fixture-review-sentinel:T-1"}]}"#,
    )
    .unwrap();
    let mut config = HeadlessConfig::new(
        &work,
        &root,
        orchestrail_engine::config::EngineConfig::default().codex,
    );
    config.claude_command = env!("CARGO_BIN_EXE_orchestrail-fixture-claude").into();
    config.call_deadline = Duration::from_secs(10);
    let mut port = HeadlessExternalPort::new(config).unwrap();
    let state = ProcessorState {
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
        tasks: BTreeMap::from([(
            "T-1".into(),
            TaskRuntime {
                id: "T-1".into(),
                conflict_domain: "engine/**".into(),
                level: Some(Level::Coder),
                risk: Some(Risk::Medium),
                wave: 1,
                phase: TaskPhase::Reviewing,
                leaf_attempts: BTreeMap::from([("review".into(), 1)]),
                review_cycles: 0,
                review_signatures: Vec::new(),
                pending_fix_open_findings: None,
                pending_fix_open_finding_ids: None,
                dimensions_with_findings_last_round: Vec::new(),
                implementation_author: Some("coder".into()),
                previous_review_sha: None,
                review_sha: Some("review-head".into()),
                reason: None,
                imported_recovery_intent: None,
                leaf_sessions: std::collections::BTreeMap::new(),
            },
        )]),
        ..ProcessorState::default()
    };

    let outcome = port.task_review("T-1", &workspace, &state).unwrap();
    assert!(
        matches!(outcome, orchestrail_engine::processor::ReviewOutcome::Clean { review_sha } if review_sha == "review-head")
    );
    let review = fs::read_to_string(work.join("tasks/T-1/review.md")).unwrap();
    assert!(review.contains("Evidence sentinel: fixture-review-sentinel:T-1"));
    assert!(work.join("native-evidence/T-1-reviewer.md").is_file());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn concurrent_reviewers_consume_their_own_immutable_ranges_in_request_order() {
    let root = std::env::temp_dir().join(format!(
        "orchestrail-headless-reviewer-batch-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let work = root.join(".work");
    let workspace_one = work.join("worktrees/T-1");
    let workspace_two = work.join("worktrees/T-2");
    fs::create_dir_all(work.join("native-evidence")).unwrap();
    fs::create_dir_all(&workspace_one).unwrap();
    fs::create_dir_all(&workspace_two).unwrap();
    fs::write(
        work.join("native-evidence/fixture-review-batch.barrier"),
        "T-1\nT-2\n",
    )
    .unwrap();
    for (task_id, head) in [("T-1", "review-head-T-1"), ("T-2", "review-head-T-2")] {
        let sentinel = format!("fixture-review-sentinel:{task_id}");
        fs::write(
            work.join("native-evidence")
                .join(format!("review-range-{task_id}-1.json")),
            format!(
                r#"{{"schema":"orchestrail/task-review-range@1","base":"base","head":"{head}","files":[{{"path":"src/{task_id}.rs","old_path":null,"diff_sha256":"{sentinel}","raw":"{sentinel}"}}]}}"#
            ),
        )
        .unwrap();
    }
    let mut config = HeadlessConfig::new(
        &work,
        &root,
        orchestrail_engine::config::EngineConfig::default().codex,
    );
    config.claude_command = env!("CARGO_BIN_EXE_orchestrail-fixture-claude").into();
    config.call_deadline = Duration::from_secs(10);
    let mut port = HeadlessExternalPort::new(config).unwrap();
    let task = |id: &str, head: &str| TaskRuntime {
        id: id.into(),
        conflict_domain: format!("engine/{id}/**"),
        level: Some(Level::Coder),
        risk: Some(Risk::Medium),
        wave: 1,
        phase: TaskPhase::Reviewing,
        leaf_attempts: BTreeMap::from([("review".into(), 1)]),
        review_cycles: 0,
        review_signatures: Vec::new(),
        pending_fix_open_findings: None,
        pending_fix_open_finding_ids: None,
        dimensions_with_findings_last_round: Vec::new(),
        implementation_author: Some("coder".into()),
        previous_review_sha: None,
        review_sha: Some(head.into()),
        reason: None,
        imported_recovery_intent: None,
        leaf_sessions: std::collections::BTreeMap::new(),
    };
    let state = ProcessorState {
        batch: Some(CohortRuntime {
            id: "B-1".into(),
            base: "base".into(),
            started_at_secs: 1,
            wave: 1,
            admitted_total: 2,
            admission_closed: None,
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            token_budget_actual_tokens: None,
            events_outbox_enabled: true,
        }),
        tasks: BTreeMap::from([
            ("T-1".into(), task("T-1", "review-head-T-1")),
            ("T-2".into(), task("T-2", "review-head-T-2")),
        ]),
        ..ProcessorState::default()
    };

    let results = port
        .execute_task_batch(
            &[
                ExternalTaskEffect {
                    effect: TaskEffect::DispatchReview {
                        task_id: "T-2".into(),
                    },
                    workspace: workspace_two,
                },
                ExternalTaskEffect {
                    effect: TaskEffect::DispatchReview {
                        task_id: "T-1".into(),
                    },
                    workspace: workspace_one,
                },
            ],
            &state,
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(matches!(
        &results[0],
        TaskEffectResult::Review {
            outcome: orchestrail_engine::processor::ReviewOutcome::Clean { review_sha }
        } if review_sha == "review-head-T-2"
    ));
    assert!(matches!(
        &results[1],
        TaskEffectResult::Review {
            outcome: orchestrail_engine::processor::ReviewOutcome::Clean { review_sha }
        } if review_sha == "review-head-T-1"
    ));
    let first = fs::read_to_string(work.join("tasks/T-1/review.md")).unwrap();
    let second = fs::read_to_string(work.join("tasks/T-2/review.md")).unwrap();
    assert!(first.contains("fixture-review-sentinel:T-1"));
    assert!(!first.contains("fixture-review-sentinel:T-2"));
    assert!(second.contains("fixture-review-sentinel:T-2"));
    assert!(!second.contains("fixture-review-sentinel:T-1"));
    assert!(work.join("native-evidence/T-1-reviewer.md").is_file());
    assert!(work.join("native-evidence/T-2-reviewer.md").is_file());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn reviewer_deadline_during_parallel_collection_never_becomes_a_clean_result() {
    let root = std::env::temp_dir().join(format!(
        "orchestrail-headless-reviewer-deadline-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let work = root.join(".work");
    let workspace_one = work.join("worktrees/T-1");
    let workspace_two = work.join("worktrees/T-2");
    fs::create_dir_all(work.join("native-evidence")).unwrap();
    fs::create_dir_all(&workspace_one).unwrap();
    fs::create_dir_all(&workspace_two).unwrap();
    fs::write(
        work.join("native-evidence/fixture-review-batch.barrier"),
        "T-1\nT-2\n",
    )
    .unwrap();
    fs::write(
        work.join("native-evidence/fixture-review-delay-T-1"),
        "delay only T-1 after rendezvous\n",
    )
    .unwrap();
    for (task_id, head) in [("T-1", "review-head-T-1"), ("T-2", "review-head-T-2")] {
        let sentinel = format!("fixture-review-sentinel:{task_id}");
        fs::write(
            work.join("native-evidence")
                .join(format!("review-range-{task_id}-1.json")),
            format!(
                r#"{{"schema":"orchestrail/task-review-range@1","base":"base","head":"{head}","files":[{{"path":"src/{task_id}.rs","old_path":null,"diff_sha256":"{sentinel}","raw":"{sentinel}"}}]}}"#
            ),
        )
        .unwrap();
    }
    let mut config = HeadlessConfig::new(
        &work,
        &root,
        orchestrail_engine::config::EngineConfig::default().codex,
    );
    config.claude_command = env!("CARGO_BIN_EXE_orchestrail-fixture-claude").into();
    config.call_deadline = Duration::from_millis(750);
    let mut port = HeadlessExternalPort::new(config).unwrap();
    let task = |id: &str, head: &str| TaskRuntime {
        id: id.into(),
        conflict_domain: format!("engine/{id}/**"),
        level: Some(Level::Coder),
        risk: Some(Risk::Medium),
        wave: 1,
        phase: TaskPhase::Reviewing,
        leaf_attempts: BTreeMap::from([("review".into(), 1)]),
        review_cycles: 0,
        review_signatures: Vec::new(),
        pending_fix_open_findings: None,
        pending_fix_open_finding_ids: None,
        dimensions_with_findings_last_round: Vec::new(),
        implementation_author: Some("coder".into()),
        previous_review_sha: None,
        review_sha: Some(head.into()),
        reason: None,
        imported_recovery_intent: None,
        leaf_sessions: std::collections::BTreeMap::new(),
    };
    let state = ProcessorState {
        batch: Some(CohortRuntime {
            id: "B-1".into(),
            base: "base".into(),
            started_at_secs: 1,
            wave: 1,
            admitted_total: 2,
            admission_closed: None,
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            token_budget_actual_tokens: None,
            events_outbox_enabled: true,
        }),
        tasks: BTreeMap::from([
            ("T-1".into(), task("T-1", "review-head-T-1")),
            ("T-2".into(), task("T-2", "review-head-T-2")),
        ]),
        ..ProcessorState::default()
    };

    let results = port
        .execute_task_batch(
            &[
                ExternalTaskEffect {
                    effect: TaskEffect::DispatchReview {
                        task_id: "T-1".into(),
                    },
                    workspace: workspace_one,
                },
                ExternalTaskEffect {
                    effect: TaskEffect::DispatchReview {
                        task_id: "T-2".into(),
                    },
                    workspace: workspace_two,
                },
            ],
            &state,
        )
        .unwrap();

    assert_eq!(results.len(), 2, "collection retains both request slots");
    assert!(matches!(
        &results[0],
        TaskEffectResult::Review {
            outcome: orchestrail_engine::processor::ReviewOutcome::Escalated { reason }
        } if reason.contains("supervisor timeout")
    ));
    assert!(matches!(
        &results[1],
        TaskEffectResult::Review {
            outcome: orchestrail_engine::processor::ReviewOutcome::Clean { review_sha }
        } if review_sha == "review-head-T-2"
    ));
    assert!(
        !work.join("tasks/T-1/review.md").exists(),
        "the timed-out child must not leave a clean review artifact"
    );
    assert!(work.join("tasks/T-2/review.md").is_file());
    assert!(
        work.join("native-evidence/fixture-review-started-T-1")
            .is_file()
    );
    assert!(
        work.join("native-evidence/fixture-review-started-T-2")
            .is_file()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn mixed_claude_and_codex_reviewers_consume_their_own_ranges_in_request_order() {
    let root = std::env::temp_dir().join(format!(
        "orchestrail-headless-mixed-reviewer-batch-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let work = root.join(".work");
    let claude_workspace = work.join("worktrees/T-1");
    let codex_workspace = work.join("worktrees/T-2");
    fs::create_dir_all(work.join("native-evidence")).unwrap();
    fs::create_dir_all(&claude_workspace).unwrap();
    fs::create_dir_all(&codex_workspace).unwrap();
    fs::write(
        work.join("native-evidence/fixture-review-batch.barrier"),
        "T-1\nT-2\n",
    )
    .unwrap();
    for (task_id, head) in [("T-1", "review-head-T-1"), ("T-2", "review-head-T-2")] {
        let sentinel = format!("fixture-review-sentinel:{task_id}");
        fs::write(
            work.join("native-evidence")
                .join(format!("review-range-{task_id}-1.json")),
            format!(
                r#"{{"schema":"orchestrail/task-review-range@1","base":"base","head":"{head}","files":[{{"path":"src/{task_id}.rs","old_path":null,"diff_sha256":"{sentinel}","raw":"{sentinel}"}}]}}"#
            ),
        )
        .unwrap();
    }
    let mut codex = orchestrail_engine::config::EngineConfig::default().codex;
    codex.reviewer = CodexReviewer::FastStd;
    codex.command = env!("CARGO_BIN_EXE_orchestrail-fixture-codex").into();
    let mut config = HeadlessConfig::new(&work, &root, codex);
    config.claude_command = env!("CARGO_BIN_EXE_orchestrail-fixture-claude").into();
    config.call_deadline = Duration::from_secs(10);
    let mut port = HeadlessExternalPort::new(config).unwrap();
    let task = |id: &str, head: &str, author: &str| TaskRuntime {
        id: id.into(),
        conflict_domain: format!("engine/{id}/**"),
        level: Some(Level::Coder),
        risk: Some(Risk::Medium),
        wave: 1,
        phase: TaskPhase::Reviewing,
        leaf_attempts: BTreeMap::from([("review".into(), 1)]),
        review_cycles: 0,
        review_signatures: Vec::new(),
        pending_fix_open_findings: None,
        pending_fix_open_finding_ids: None,
        dimensions_with_findings_last_round: Vec::new(),
        implementation_author: Some(author.into()),
        previous_review_sha: None,
        review_sha: Some(head.into()),
        reason: None,
        imported_recovery_intent: None,
        leaf_sessions: std::collections::BTreeMap::new(),
    };
    let state = ProcessorState {
        batch: Some(CohortRuntime {
            id: "B-1".into(),
            base: "base".into(),
            started_at_secs: 1,
            wave: 1,
            admitted_total: 2,
            admission_closed: None,
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            token_budget_actual_tokens: None,
            events_outbox_enabled: true,
        }),
        tasks: BTreeMap::from([
            // A Codex-authored range must use the independent Claude reviewer.
            ("T-1".into(), task("T-1", "review-head-T-1", "coder_codex")),
            // A Claude-authored standard range is eligible for a full Codex review.
            ("T-2".into(), task("T-2", "review-head-T-2", "coder")),
        ]),
        ..ProcessorState::default()
    };

    let results = port
        .execute_task_batch(
            &[
                ExternalTaskEffect {
                    effect: TaskEffect::PrepareReview {
                        task_id: "T-2".into(),
                    },
                    workspace: codex_workspace,
                },
                ExternalTaskEffect {
                    effect: TaskEffect::DispatchReview {
                        task_id: "T-1".into(),
                    },
                    workspace: claude_workspace,
                },
            ],
            &state,
        )
        .unwrap();

    assert_eq!(results.len(), 2);
    assert!(matches!(
        &results[0],
        TaskEffectResult::ReviewPrepared {
            outcome: orchestrail_engine::processor::TaskReviewPreparationOutcome::Completed(
                orchestrail_engine::processor::ReviewOutcome::Clean { review_sha }
            )
        } if review_sha == "review-head-T-2"
    ));
    assert!(matches!(
        &results[1],
        TaskEffectResult::Review {
            outcome: orchestrail_engine::processor::ReviewOutcome::Clean { review_sha }
        } if review_sha == "review-head-T-1"
    ));
    let claude_review = fs::read_to_string(work.join("tasks/T-1/review.md")).unwrap();
    let codex_review = fs::read_to_string(work.join("tasks/T-2/review.md")).unwrap();
    assert!(claude_review.contains("fixture-review-sentinel:T-1"));
    assert!(!claude_review.contains("fixture-review-sentinel:T-2"));
    assert!(codex_review.contains("fixture-review-sentinel:T-2"));
    assert!(!codex_review.contains("fixture-review-sentinel:T-1"));
    assert!(work.join("native-evidence/T-1-reviewer.md").is_file());
    assert!(work.join("native-evidence/T-2-reviewer_codex.md").is_file());
    let events = TailReader::new(work.join(OUTBOX_FILE)).poll_all().unwrap();
    let attempt = events
        .iter()
        .find(|event| event.event_type == EventType::CodexAttempt)
        .expect("the contained Codex reviewer must finalize its durable attempt");
    assert_eq!(attempt.task_id.as_deref(), Some("T-2"));
    assert_eq!(
        attempt
            .payload
            .get("role")
            .and_then(serde_json::Value::as_str),
        Some("reviewer")
    );
    assert_eq!(
        attempt
            .payload
            .get("mode")
            .and_then(serde_json::Value::as_str),
        Some("full")
    );
    assert_eq!(
        attempt
            .payload
            .get("effective_sandbox")
            .and_then(serde_json::Value::as_str),
        Some("read-only")
    );
    assert_eq!(
        attempt
            .payload
            .get("outcome")
            .and_then(serde_json::Value::as_str),
        Some("success")
    );
    let _ = fs::remove_dir_all(root);
}
