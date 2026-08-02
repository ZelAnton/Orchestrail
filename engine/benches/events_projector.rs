use std::collections::BTreeMap;

use criterion::{Criterion, criterion_group, criterion_main};
use orchestrail_engine::events::project_processor_transition;
use orchestrail_engine::processor::{ProcessorCommand, ProcessorState, TaskPhase, TaskRuntime};

const STREAM_TASK_COUNT: u64 = 10_000;

fn published_task(index: u64) -> TaskRuntime {
    TaskRuntime {
        id: format!("T-{index:05}"),
        conflict_domain: "engine/events/**".into(),
        level: None,
        risk: None,
        wave: 1,
        phase: TaskPhase::Published,
        leaf_attempts: BTreeMap::new(),
        review_cycles: 0,
        review_signatures: Vec::new(),
        pending_fix_open_findings: None,
        pending_fix_open_finding_ids: None,
        implementation_author: None,
        previous_review_sha: None,
        review_sha: None,
        reason: None,
        imported_recovery_intent: None,
        leaf_sessions: BTreeMap::new(),
        dimensions_with_findings_last_round: Vec::new(),
    }
}

fn projector_fold(c: &mut Criterion) {
    let before = ProcessorState {
        tasks: (0..STREAM_TASK_COUNT)
            .map(|index| {
                let task = published_task(index);
                (task.id.clone(), task)
            })
            .collect(),
        ..Default::default()
    };
    let mut after = before.clone();
    for task in after.tasks.values_mut() {
        task.phase = TaskPhase::Done;
    }
    let command = ProcessorCommand::CleanupComplete;

    c.bench_function("projector/fold_full_synthetic_stream", |b| {
        b.iter(|| {
            std::hint::black_box(project_processor_transition(
                &before,
                &after,
                &command,
                "2026-08-02T12:00:00Z",
            ))
        });
    });
}

criterion_group!(benches, projector_fold);
criterion_main!(benches);
