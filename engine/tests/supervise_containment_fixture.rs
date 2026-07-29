//! Platform-specific live containment proof for the production ProcessKit supervisor.

#![cfg(windows)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use orchestrail_engine::supervise::{Reason, SpawnSpec, run};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_marker() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "orchestrail-processkit-descendant-{}-{}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

fn ready_marker(marker: &std::path::Path) -> std::path::PathBuf {
    let mut ready = marker.as_os_str().to_os_string();
    ready.push(".ready");
    std::path::PathBuf::from(ready)
}

fn release_marker(marker: &std::path::Path) -> std::path::PathBuf {
    let mut release = marker.as_os_str().to_os_string();
    release.push(".release");
    std::path::PathBuf::from(release)
}

#[test]
fn deadline_reaps_nested_processkit_descendant_without_orphaning_it() {
    let marker = unique_marker();
    let ready = ready_marker(&marker);
    let release = release_marker(&marker);
    let fixture = env!("CARGO_BIN_EXE_orchestrail-fixture-process-tree");
    let marker_for_worker = marker.clone();
    let runner = std::thread::spawn(move || {
        run(&SpawnSpec::new(
            fixture,
            vec![
                "spawn".into(),
                marker_for_worker.to_string_lossy().into_owned(),
            ],
        )
        // Native Windows startup can be slow under a parallel workspace test load. The release
        // barrier below keeps the proof fast after this generous startup allowance.
        .deadline(Some(Duration::from_secs(10))))
    });

    let readiness_deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < readiness_deadline && !ready.is_file() {
        std::thread::sleep(Duration::from_millis(10));
    }
    let nested_child_started = ready.is_file();
    let verdict = runner.join().expect("join supervised fixture runner");
    assert!(
        nested_child_started,
        "the nested ProcessKit child never reached its readiness boundary before the deadline"
    );
    assert_eq!(
        verdict.reason,
        Reason::Timeout,
        "{}",
        verdict.outcome_reason
    );

    // Release the nested child only after `run` reports that containment teardown completed. If
    // it survived outside the Windows Job, it will now prove that by writing the marker.
    std::fs::write(&release, "release-descendant")
        .expect("write nested-child release marker after supervisor teardown");
    let marker_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < marker_deadline && !marker.exists() {
        std::thread::sleep(Duration::from_millis(10));
    }
    let descendant_ran = marker.exists();
    let _ = std::fs::remove_file(&marker);
    let _ = std::fs::remove_file(&ready);
    let _ = std::fs::remove_file(&release);
    assert!(
        !descendant_ran,
        "nested ProcessKit descendant kept running after outer containment teardown"
    );
}
