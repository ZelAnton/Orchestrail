#![allow(dead_code)]

//! Synthetic, filesystem-backed fixtures shared by the Criterion benches.
//!
//! Bench targets cannot import `engine/tests/support` as a crate, so the small generators live
//! here. They deliberately use the same public event and snapshot input contracts as the tests.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use orchestrail_engine::events::{
    Actor, ActorKind, Event, EventType, SCHEMA_VERSION, deterministic_event_id,
};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Events in the large-journal fixtures. This is deliberately above the 10^4 minimum.
pub const LARGE_EVENT_COUNT: u64 = 10_000;

/// A unique temporary directory deleted when its owning benchmark fixture is dropped.
pub struct FixtureDir {
    path: PathBuf,
}

impl FixtureDir {
    pub fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "orchestrail-bench-{label}-{}-{nanos}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create synthetic benchmark fixture directory");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for FixtureDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub fn synthetic_event(index: u64) -> Event {
    Event {
        schema_version: SCHEMA_VERSION,
        event_id: deterministic_event_id(&format!("criterion-event-{index}")),
        occurred_at: "2026-08-02T12:00:00Z".into(),
        event_type: EventType::TaskStatusChanged,
        actor: Actor {
            kind: ActorKind::Agent,
            name: "criterion".into(),
        },
        batch_id: Some("B-BENCH".into()),
        task_id: Some(format!("T-{index:05}")),
        payload_version: 1,
        payload: [
            ("from".into(), serde_json::Value::from("в работе")),
            ("to".into(), serde_json::Value::from("на ревью")),
        ]
        .into_iter()
        .collect(),
    }
}

pub fn event_lines(count: u64) -> String {
    let mut lines = String::with_capacity(count as usize * 300);
    for index in 0..count {
        lines.push_str(&synthetic_event(index).to_json_line());
        lines.push('\n');
    }
    lines
}

pub fn write_events(path: &Path, count: u64) {
    fs::write(path, event_lines(count)).expect("write synthetic events.jsonl");
}

pub fn append_event(path: &Path, index: u64) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open synthetic events.jsonl for append");
    writeln!(file, "{}", synthetic_event(index).to_json_line())
        .expect("append synthetic event line");
}

/// Make a queue and archive plus several dozen task descriptors for `Snapshot::load`.
pub fn write_snapshot_fixture(work: &Path, descriptor_count: u64) {
    let mut queue = String::new();
    let mut archive = String::from("# Completed tasks\n\n");
    for index in 0..descriptor_count {
        let id = format!("T-{:05}", index + 1);
        queue.push_str(&format!(
            "### [{id}] Synthetic queue task {index} — статус: не начата\n"
        ));
        queue.push_str("Предпосылки:\nDelivery target: current\n\n");

        let task_dir = work.join("tasks").join(&id);
        fs::create_dir_all(&task_dir).expect("create synthetic task descriptor directory");
        let descriptor = format!(
            "# {id}\n\nСтатус: на ревью\nПредпосылки:\nКонфликт-домен: engine/**\nРекомендуемый исполнитель: coder\nРиск: medium\n"
        );
        fs::write(task_dir.join("task.md"), descriptor).expect("write synthetic task descriptor");
        archive.push_str(&format!("- [{id}] synthetic archived task\n"));
    }
    fs::write(work.join("Tasks_Queue.md"), queue).expect("write synthetic queue");
    let archive_dir = work.join("archive");
    fs::create_dir_all(&archive_dir).expect("create synthetic archive directory");
    fs::write(archive_dir.join("Tasks_Archive.md"), archive).expect("write synthetic archive");

    File::create(work.join("cohort_state.md")).expect("create empty cohort state");
    File::create(work.join("integration_state.md")).expect("create empty integration state");
    File::create(work.join("batch.md")).expect("create empty batch state");
}
