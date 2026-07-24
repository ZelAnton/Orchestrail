//! ProcessKit-backed CLI proof for durable owner hand-off between separate engine processes.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temporary_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "orchestrail-lease-cross-process-{}-{nanos}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&root).expect("create temporary root");
    root
}

fn lease(root: &std::path::Path, work: &std::path::Path, args: &[&str]) -> Output {
    let mut command = Command::new(BIN);
    command.args(["lease"]);
    command.args(args);
    command.arg("--work");
    command.arg(work);
    command.arg("--root");
    command.arg(root);
    command
        .output()
        .expect("launch contained engine lease command")
}

#[test]
fn stale_lease_takeover_between_engine_processes_preserves_owner_cas() {
    let root = temporary_root();
    let work = root.join(".work");
    let lease_file = work.join("orchestrator.lock/lease.json");

    let acquired = lease(
        &root,
        &work,
        &["acquire", "--owner", "owner-a", "--ttl", "60"],
    );
    assert!(
        acquired.status.success(),
        "owner A should acquire: {}",
        String::from_utf8_lossy(&acquired.stderr)
    );
    let live_before = fs::read(&lease_file).expect("read owner A lease");

    // A second, separately launched engine process must not replace a live owner, even when it
    // asks for the explicit takeover verb.
    let live_takeover = lease(
        &root,
        &work,
        &["takeover", "--owner", "owner-b", "--ttl", "60"],
    );
    assert_eq!(live_takeover.status.code(), Some(3));
    assert_eq!(
        fs::read(&lease_file).expect("read lease after refused live takeover"),
        live_before,
        "a refused takeover must not rewrite a live owner record"
    );

    // Model the durable record left behind by the now-exited owner-A process. Test setup changes
    // only its heartbeat; the `takeover` itself remains a separately launched engine process.
    // This avoids a flaky wall-clock wait around a one-second TTL.
    let mut stale_record: serde_json::Value =
        serde_json::from_slice(&live_before).expect("parse owner A lease JSON");
    stale_record["heartbeat"] = serde_json::Value::String("1970-01-01T00:00:00Z".into());
    fs::write(
        &lease_file,
        serde_json::to_vec(&stale_record).expect("serialize stale owner A lease"),
    )
    .expect("write stale owner A lease");
    let taken_over = lease(
        &root,
        &work,
        &["takeover", "--owner", "owner-b", "--ttl", "60", "--json"],
    );
    assert!(
        taken_over.status.success(),
        "stale takeover should succeed: {}",
        String::from_utf8_lossy(&taken_over.stderr)
    );
    let takeover_json: serde_json::Value =
        serde_json::from_slice(&taken_over.stdout).expect("parse takeover JSON");
    assert_eq!(takeover_json["owner"], "owner-b");
    assert_eq!(takeover_json["generation"], 2);
    assert_eq!(takeover_json["adopted_stale"], true);
    assert_eq!(takeover_json["taken_over_from"], "owner-a");

    let owner_b_record = fs::read(&lease_file).expect("read owner B lease");
    let late_old_release = lease(&root, &work, &["release", "--owner", "owner-a"]);
    assert_eq!(
        late_old_release.status.code(),
        Some(4),
        "a late old owner must fail the owner check"
    );
    assert_eq!(
        fs::read(&lease_file).expect("read lease after late old release"),
        owner_b_record,
        "a late old owner must not remove or rewrite the new lease"
    );

    let released = lease(&root, &work, &["release", "--owner", "owner-b"]);
    assert!(
        released.status.success(),
        "new owner should release: {}",
        String::from_utf8_lossy(&released.stderr)
    );
    assert!(
        !work.join("orchestrator.lock").exists(),
        "normal owner release removes only its empty lease directory"
    );

    // Legacy-compatible `takeover` also acquires a vacant lease, but must not claim that it
    // adopted a stale owner when there was no prior record.
    let vacant_takeover = lease(
        &root,
        &work,
        &["takeover", "--owner", "owner-c", "--ttl", "60", "--json"],
    );
    assert!(vacant_takeover.status.success());
    let vacant_json: serde_json::Value =
        serde_json::from_slice(&vacant_takeover.stdout).expect("parse vacant takeover JSON");
    assert_eq!(vacant_json["owner"], "owner-c");
    assert_eq!(vacant_json["adopted_stale"], false);
    assert!(vacant_json["taken_over_from"].is_null());
    let vacant_release = lease(&root, &work, &["release", "--owner", "owner-c"]);
    assert!(
        vacant_release.status.success(),
        "vacant-takeover owner should release: {}",
        String::from_utf8_lossy(&vacant_release.stderr)
    );
    let _ = fs::remove_dir_all(root);
}
