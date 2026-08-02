//! Hermetic, offline proof of the `events tail` subcommand end to end: drive the REAL built
//! binary against a local `.work/events.jsonl`-shaped fixture (lines copied from the real
//! journal's format) and assert it prints only new, unique, fully-committed events — never the
//! torn tail. No network, no live journal: the fixture is written to a temp file.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::Command;

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

// Real-format lines (shape copied from `.work/events.jsonl`).
const A: &str = r#"{"schema_version":1,"event_id":"evt-a","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","batch_id":"B-1","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1,"tasks":["T-1"]}}"#;
const B: &str = r#"{"schema_version":1,"event_id":"evt-b","occurred_at":"2026-07-08T12:24:11Z","type":"task.captured","batch_id":"B-1","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"level":"coder","wave":1}}"#;
// A half-written final record with no trailing newline (crash mid-append) — must never print.
const TORN: &str = r#"{"schema_version":1,"event_id":"evt-c","occurred_at":"2026-07-08T12:24:"#;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_fixture(contents: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "orchestra-events-fixture-{}-{nanos}-{n}.jsonl",
        std::process::id()
    ));
    fs::write(&path, contents).unwrap();
    path
}

#[test]
fn events_tail_prints_deduped_committed_events_only() {
    // A, B, a duplicate of A (dedup), then a torn tail (never delivered).
    let fixture = temp_fixture(&format!("{A}\n{B}\n{A}\n{TORN}"));
    let out = Command::new(BIN)
        .arg("events")
        .arg("tail")
        .arg(&fixture)
        .output()
        .expect("spawn events tail");
    let _ = fs::remove_file(&fixture);

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "events tail exited nonzero: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        lines.len(),
        2,
        "expected exactly A and B (dedup drops the second A; torn tail excluded), got: {stdout}"
    );
    assert!(
        lines[0].contains("\"event_id\":\"evt-a\""),
        "line0: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("\"event_id\":\"evt-b\""),
        "line1: {}",
        lines[1]
    );
    // The torn tail's id must never appear.
    assert!(
        !stdout.contains("evt-c"),
        "torn tail must not be delivered: {stdout}"
    );
    // Output is normalized decoded JSON: envelope fields present, payload_version defaulted.
    assert!(lines[0].contains("\"type\":\"cohort.opened\""));
    assert!(lines[0].contains("\"payload_version\":1"));
}

#[test]
fn events_tail_without_follow_reads_to_current_eof() {
    const EVENT_COUNT: usize = 512;
    let padding = "x".repeat(2_048);
    let mut journal = String::with_capacity(EVENT_COUNT * (padding.len() + 192));
    for event in 0..EVENT_COUNT {
        journal.push_str(&format!(
            r#"{{"schema_version":1,"event_id":"evt-large-{event}","occurred_at":"2026-07-08T12:24:10Z","type":"task.captured","batch_id":"B-1","task_id":"T-1","actor":{{"kind":"agent","name":"processor"}},"payload":{{"padding":"{padding}"}}}}"#
        ));
        journal.push('\n');
    }
    let fixture = temp_fixture(&journal);
    let out = Command::new(BIN)
        .arg("events")
        .arg("tail")
        .arg(&fixture)
        .output()
        .expect("spawn events tail");
    let _ = fs::remove_file(&fixture);

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "events tail exited nonzero: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        stdout.lines().count(),
        EVENT_COUNT,
        "non-follow mode must read past one poll window to current EOF"
    );
    assert!(
        stdout.contains("\"event_id\":\"evt-large-511\""),
        "the final event past the one-megabyte poll bound must be printed"
    );
}

#[test]
fn events_tail_missing_file_errors() {
    let missing = std::env::temp_dir().join("orchestra-events-absent-xyz.jsonl");
    let out = Command::new(BIN)
        .arg("events")
        .arg("tail")
        .arg(&missing)
        .output()
        .expect("spawn events tail");
    assert!(
        !out.status.success(),
        "missing file should be a nonzero exit"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("file not found"),
        "stderr should explain the missing file"
    );
}

#[test]
fn events_usage_on_bad_subcommand() {
    let out = Command::new(BIN)
        .arg("events")
        .arg("bogus")
        .output()
        .expect("spawn events");
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage: events tail"));
}
