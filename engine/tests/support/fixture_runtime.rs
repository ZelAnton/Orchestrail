//! Test-only companion process for durable-runtime crash fixtures.
//!
//! It deliberately contains no subprocess or VCS calls.  The parent integration test launches
//! this binary through its ProcessKit-only adapter, then opens the checkpoint from a separate
//! process to prove the on-disk recovery contract rather than merely dropping and recreating a
//! runtime within one address space.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use orchestrail_engine::headless::{HeadlessConfig, HeadlessExternalPort};
use orchestrail_engine::native_port::ExternalPort;
use orchestrail_engine::processor::{
    CloseReasonWire, CohortRuntime, Effect, ImportedRecoveryIntent, IntegrationRuntime, LeafKind,
    MergeResolutionRuntime, PROCESSOR_STATE_VERSION, Phase, ProcessorCommand, ProcessorConfig,
    ProcessorState, ReviewOutcome, TaskPhase, TaskReviewPreparationOutcome, TaskRuntime,
};
use orchestrail_engine::resolvers::{Level, Risk};
use orchestrail_engine::runtime::ProcessorRuntime;

fn main() {
    let mut args = std::env::args_os();
    let _binary = args.next();
    let Some(mode) = args.next() else {
        fail("expected `write-pending-merger <work>`");
    };
    let Some(work) = args.next() else {
        fail("expected work directory");
    };
    let work = PathBuf::from(work);
    match mode.to_str() {
        Some("write-pending-merger") if args.next().is_none() => write_pending_merger(work),
        Some("write-reviewer-result-without-ack") => {
            let Some(claude_command) = args.next() else {
                fail("expected Claude fixture command");
            };
            if args.next().is_some() {
                fail("unexpected fixture argument");
            }
            write_reviewer_result_without_ack(work, PathBuf::from(claude_command));
        }
        _ => fail("unsupported fixture mode or arguments"),
    }
}

fn write_pending_merger(work: PathBuf) {
    let mut runtime = ProcessorRuntime::import_legacy(config(), work, pending_merger_state())
        .unwrap_or_else(|error| fail(&format!("write imported checkpoint: {error}")));
    let effects = runtime
        .apply_at(
            ProcessorCommand::Recover {
                workspaces_present: BTreeSet::new(),
            },
            "2026-07-26T00:00:00Z",
        )
        .unwrap_or_else(|error| fail(&format!("create pending merger dispatch: {error}")));
    if !runtime
        .pending_effects()
        .contains_key("dispatch-integration:merger")
    {
        fail("merger dispatch was not durably pending");
    }
    if effects
        != [
            Effect::PersistCheckpoint,
            Effect::DispatchIntegration {
                kind: LeafKind::Merger,
            },
        ]
    {
        fail("recovery did not persist exactly the pending merger dispatch");
    }
}

/// Model the exact crash window after a contained reviewer returns cleanly but before the native
/// driver can acknowledge `DispatchTask(review)` in the runtime ledger.
fn write_reviewer_result_without_ack(work: PathBuf, claude_command: PathBuf) {
    let workspace = work.join("worktrees/T-1");
    let evidence = work.join("native-evidence");
    std::fs::create_dir_all(&workspace)
        .unwrap_or_else(|error| fail(&format!("create reviewer workspace: {error}")));
    std::fs::create_dir_all(&evidence)
        .unwrap_or_else(|error| fail(&format!("create native evidence directory: {error}")));
    std::fs::write(
        evidence.join("review-range-T-1-1.json"),
        r#"{"schema":"orchestrail/task-review-range@1","base":"base","head":"reviewed","files":[{"path":"src/lib.rs","old_path":null,"diff_sha256":"fixture-review-sentinel:T-1","raw":"fixture-review-sentinel:T-1"}]}"#,
    )
    .unwrap_or_else(|error| fail(&format!("write immutable review range: {error}")));

    let mut runtime = ProcessorRuntime::import_legacy(config(), &work, pending_reviewer_state())
        .unwrap_or_else(|error| fail(&format!("write imported reviewer checkpoint: {error}")));
    let recovery = runtime
        .apply_at(
            ProcessorCommand::Recover {
                workspaces_present: BTreeSet::from(["T-1".to_string()]),
            },
            "2026-07-26T00:00:00Z",
        )
        .unwrap_or_else(|error| fail(&format!("schedule review preparation: {error}")));
    if recovery
        != [
            Effect::PersistCheckpoint,
            Effect::PrepareTaskReview {
                task_id: "T-1".into(),
            },
        ]
    {
        fail("recovery did not schedule exactly the task-review preparation");
    }
    let dispatch = runtime
        .apply_at(
            ProcessorCommand::TaskReviewPrepared {
                task_id: "T-1".into(),
                outcome: TaskReviewPreparationOutcome::DispatchClaude,
            },
            "2026-07-26T00:00:01Z",
        )
        .unwrap_or_else(|error| fail(&format!("schedule review dispatch: {error}")));
    if dispatch
        != [
            Effect::PersistCheckpoint,
            Effect::DispatchTask {
                task_id: "T-1".into(),
                kind: LeafKind::Review,
            },
        ]
        || !runtime
            .pending_effects()
            .contains_key("dispatch-task:T-1:review")
    {
        fail("review dispatch was not durably pending");
    }

    let root = work.parent().unwrap_or(&work);
    let mut headless = HeadlessConfig::new(
        &work,
        root,
        orchestrail_engine::config::EngineConfig::default().codex,
    );
    headless.claude_command = claude_command.to_string_lossy().into_owned();
    let mut port = HeadlessExternalPort::new(headless)
        .unwrap_or_else(|error| fail(&format!("create contained reviewer port: {error}")));
    let outcome = port
        .task_review("T-1", &workspace, runtime.state())
        .unwrap_or_else(|error| fail(&format!("run contained reviewer: {error}")));
    if !matches!(outcome, ReviewOutcome::Clean { ref review_sha } if review_sha == "reviewed") {
        fail("contained reviewer did not produce the expected clean result");
    }
    // Intentionally exit without `runtime.apply_at(TaskReview { .. })`. The parent fixture
    // therefore observes the real post-leaf/pre-acknowledgement crash state.
}

fn config() -> ProcessorConfig {
    ProcessorConfig {
        max_parallel: 1,
        cohort_size: 1,
        ..ProcessorConfig::default()
    }
}

fn pending_merger_state() -> ProcessorState {
    let task = TaskRuntime {
        id: "T-1".into(),
        conflict_domain: "engine/**".into(),
        level: Some(Level::Coder),
        risk: Some(Risk::Medium),
        wave: 1,
        phase: TaskPhase::ResolvingMerge,
        leaf_attempts: BTreeMap::new(),
        review_cycles: 1,
        review_signatures: Vec::new(),
        pending_fix_open_findings: None,
        implementation_author: Some("coder".into()),
        previous_review_sha: None,
        review_sha: Some("reviewed".into()),
        reason: None,
        imported_recovery_intent: None,
        leaf_sessions: std::collections::BTreeMap::new(),
    };
    let mut integration = IntegrationRuntime {
        workspace_prepared: true,
        integration_head: Some("integration-head".into()),
        ..IntegrationRuntime::default()
    };
    integration.pending_merge_resolution = Some(MergeResolutionRuntime {
        task_id: "T-1".into(),
        pre_merge_head: "integration-head".into(),
        merge_paths: vec!["engine/src/lib.rs".into()],
        paths: vec!["engine/src/lib.rs".into()],
        protected_paths: Vec::new(),
    });

    ProcessorState {
        schema_version: PROCESSOR_STATE_VERSION,
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
        tasks: BTreeMap::from([("T-1".into(), task)]),
        integration,
        blocked_reason: None,
    }
}

fn pending_reviewer_state() -> ProcessorState {
    let task = TaskRuntime {
        id: "T-1".into(),
        conflict_domain: "engine/**".into(),
        level: Some(Level::Coder),
        risk: Some(Risk::Medium),
        wave: 1,
        phase: TaskPhase::Reviewing,
        leaf_attempts: BTreeMap::new(),
        review_cycles: 0,
        review_signatures: Vec::new(),
        pending_fix_open_findings: None,
        implementation_author: Some("coder".into()),
        previous_review_sha: None,
        review_sha: Some("reviewed".into()),
        reason: None,
        imported_recovery_intent: Some(ImportedRecoveryIntent::DispatchReview),
        leaf_sessions: std::collections::BTreeMap::new(),
    };
    ProcessorState {
        schema_version: PROCESSOR_STATE_VERSION,
        phase: Phase::Rolling,
        paused_from: None,
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
        tasks: BTreeMap::from([("T-1".into(), task)]),
        integration: IntegrationRuntime::default(),
        blocked_reason: None,
    }
}

fn fail(message: &str) -> ! {
    eprintln!("fixture_runtime: {message}");
    std::process::exit(2)
}
