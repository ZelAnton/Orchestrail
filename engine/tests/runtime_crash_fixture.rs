//! Cross-process crash/recovery fixtures for the durable processor runtime.
//!
//! The writer is a separate test-only executable.  It leaves an intentionally unresolved merge
//! in the checkpoint with the merger effect pending; the test process must restore that record
//! inspect-first instead of launching an unknowable second merger.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::Command;
use orchestrail_engine::processor::{Effect, LeafKind, ProcessorConfig};
use orchestrail_engine::runtime::{ProcessorRuntime, RUNTIME_CHECKPOINT_FILE, RecoveryRequirement};

const FIXTURE_BIN: &str = env!("CARGO_BIN_EXE_orchestrail-fixture-runtime");
const CLAUDE_FIXTURE_BIN: &str = env!("CARGO_BIN_EXE_orchestrail-fixture-claude");

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempWork(PathBuf);

impl TempWork {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "orchestrail-runtime-crash-fixture-{}-{nanos}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temporary work directory");
        Self(path)
    }
}

impl Drop for TempWork {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn config() -> ProcessorConfig {
    ProcessorConfig {
        max_parallel: 1,
        cohort_size: 1,
        ..ProcessorConfig::default()
    }
}

#[test]
fn pending_merger_written_by_another_process_is_inspect_first_on_resume() {
    let work = TempWork::new();
    let child = Command::new(FIXTURE_BIN)
        .args(["write-pending-merger"])
        .arg(&work.0)
        .output()
        .expect("launch isolated checkpoint writer through ProcessKit");
    assert!(
        child.status.success(),
        "isolated checkpoint writer failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        work.0.join(RUNTIME_CHECKPOINT_FILE).is_file(),
        "the child must atomically leave the native runtime checkpoint"
    );

    let resumed = ProcessorRuntime::resume(config(), &work.0)
        .expect("a separate process must decode the writer's durable checkpoint");
    assert_eq!(
        resumed.recovery_requirements(),
        vec![RecoveryRequirement::InspectBeforeContinuing {
            key: "dispatch-integration:merger".into(),
            effect: Effect::DispatchIntegration {
                kind: LeafKind::Merger,
            },
        }],
        "the merger's external result is unknown after a process boundary; recovery may not replay it"
    );
}

#[test]
fn reviewer_result_from_another_process_is_inspect_first_before_ledger_acknowledgement() {
    let work = TempWork::new();
    let child = Command::new(FIXTURE_BIN)
        .args(["write-reviewer-result-without-ack"])
        .arg(&work.0)
        .arg(CLAUDE_FIXTURE_BIN)
        .output()
        .expect("launch isolated reviewer checkpoint writer through ProcessKit");
    assert!(
        child.status.success(),
        "isolated reviewer checkpoint writer failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        work.0.join("tasks/T-1/review.md").is_file(),
        "the crashed child reached a real clean reviewer artifact before it lost the acknowledgement"
    );
    assert!(
        work.0.join("native-evidence/T-1-reviewer.md").is_file(),
        "the reviewer transcript is durable evidence, not a reason to replay the unknown child"
    );

    let resumed = ProcessorRuntime::resume(config(), &work.0)
        .expect("a separate process must decode the post-review pre-acknowledgement checkpoint");
    assert_eq!(
        resumed.recovery_requirements(),
        vec![RecoveryRequirement::InspectBeforeContinuing {
            key: "dispatch-task:T-1:review".into(),
            effect: Effect::DispatchTask {
                task_id: "T-1".into(),
                kind: LeafKind::Review,
            },
        }],
        "a clean artifact alone cannot authorize a second reviewer launch or a forged acknowledgement"
    );
}
