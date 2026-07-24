//! Hermetic proof that the native `engine lease` boundary never executes a legacy
//! `state-tx.ps1`, regardless of whether a caller places one in an arbitrary project or a
//! historically-proven Orchestra checkout. Native ownership uses the interoperable lease JSON
//! format directly and status is a successful read of an absent lease, not a failed script lookup.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::Command;

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A fresh, unique temp directory (removed by the caller at the end of each test).
fn tmp(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "orchestra-lease-default-{tag}-{}-{nanos}-{n}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn native_lease_status_never_executes_untrusted_target_local_tools() {
    // A non-checkout project carrying a target-local `state-tx.ps1`. If the engine ever executed
    // it, the script drops a sentinel; native status must neither discover nor invoke it.
    let root = tmp("foreign-root");
    let work = root.join(".work");
    fs::create_dir_all(&work).unwrap();
    let tools = root.join("tools");
    fs::create_dir_all(&tools).unwrap();

    let sentinel = root.join("EXECUTED.marker");
    let stub = format!(
        "Set-Content -LiteralPath '{}' -Value ran\nexit 0\n",
        sentinel.display()
    );
    fs::write(tools.join("state-tx.ps1"), stub).unwrap();

    let out = Command::new(BIN)
        .args(["lease", "status", "--work"])
        .arg(&work)
        .arg("--root")
        .arg(&root)
        .output()
        .expect("spawn engine lease");

    assert!(
        out.status.success(),
        "native absent-lease status succeeds: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("none (free"),
        "native status reports an absent lease"
    );
    assert!(
        !sentinel.exists(),
        "the untrusted target-local tools/state-tx.ps1 must never be executed"
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn native_lease_status_never_executes_tools_even_under_legacy_identity_markers() {
    let root = tmp("checkout-root");
    let work = root.join(".work");
    fs::create_dir_all(&work).unwrap();
    fs::create_dir_all(root.join("agents")).unwrap();
    fs::write(root.join("agents").join("processor.md"), "x").unwrap();
    fs::write(root.join("generate-codex-agents.ps1"), "x").unwrap();
    fs::create_dir_all(root.join("tools")).unwrap();
    fs::write(root.join("tools").join("sync-runtime.ps1"), "x").unwrap();

    let sentinel = root.join("EXECUTED.marker");
    let stub = format!(
        "Set-Content -LiteralPath '{}' -Value ran\nWrite-Output 'none (free)'\nexit 0\n",
        sentinel.display()
    );
    fs::write(root.join("tools").join("state-tx.ps1"), stub).unwrap();

    let out = Command::new(BIN)
        .args(["lease", "status", "--work"])
        .arg(&work)
        .arg("--root")
        .arg(&root)
        .output()
        .expect("spawn engine lease");

    assert!(out.status.success());
    assert!(
        !sentinel.exists(),
        "native lease must never execute a legacy script, even in a marker-shaped checkout (stdout: {}, stderr: {})",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let _ = fs::remove_dir_all(&root);
}
