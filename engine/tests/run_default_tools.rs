//! Hermetic, offline proof that `engine run --once`'s **DEFAULT** `--tools` resolution (no explicit
//! `--tools`) honours the SAME checkout-vs-mirror identity rule `cmd_lease` already uses for its
//! `--script` (task T-287, `engine::toolscript::resolve_tool_script`, `docs/queue_contract.md` §9,
//! K-052). Without `--tools`, `run` may default to `<root>/tools` **only** if `--root` is a proven
//! Orchestra checkout (all three identity markers `agents/processor.md`,
//! `generate-codex-agents.ps1`, `tools/sync-runtime.ps1`); otherwise it must resolve to the cc-sync
//! mirror or, absent that, refuse cleanly — never silently trust a foreign/stale target-local
//! `<root>/tools` directory just because it happens to exist.
//!
//! Every case bails out before any lease/spawn work (either `run` refuses up front when the
//! resolver finds nothing anywhere, or `run_once`'s own tool-existence check reports the exact
//! resolved path for a tool deliberately left absent from that directory) — so these tests need
//! no PowerShell host and never touch this repository's own `.work`. Every test controls the
//! child's `HOME`/`USERPROFILE` so the real machine's cc-sync mirror can never influence the
//! outcome. The full end-to-end round (real `--tools`, real `.ps1` tools) stays covered by
//! `run_fixture.rs`, which this change leaves untouched.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::{Command, Output};

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
        "orchestra-run-default-tools-{tag}-{}-{nanos}-{n}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Seed all three checkout identity markers under `root` (a proven Orchestra checkout).
fn seed_checkout_markers(root: &std::path::Path) {
    fs::create_dir_all(root.join("agents")).unwrap();
    fs::write(root.join("agents").join("processor.md"), "x").unwrap();
    fs::write(root.join("generate-codex-agents.ps1"), "x").unwrap();
    fs::create_dir_all(root.join("tools")).unwrap();
    fs::write(root.join("tools").join("sync-runtime.ps1"), "x").unwrap();
}

fn run_engine(work: &std::path::Path, root: &std::path::Path, home: &std::path::Path) -> Output {
    Command::new(BIN)
        .args(["run", "--once", "--work"])
        .arg(work)
        .arg("--root")
        .arg(root)
        .args(["--batch", "B-default-tools-test", "--json"])
        .env("HOME", home)
        .env("USERPROFILE", home)
        .output()
        .expect("spawn engine run --once")
}

fn stderr_of(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[test]
fn default_tools_refuses_a_foreign_root_local_tools_dir_without_a_mirror() {
    // A non-checkout project (NO identity markers) carrying its own/stale `tools/state-tx.ps1`.
    // If `run` ever trusted a bare `root.join("tools")` default, it would happily pick this up;
    // the correct behaviour is a clean up-front refusal, never a silent select of the foreign dir.
    let root = tmp("foreign-root");
    let work = root.join(".work");
    fs::create_dir_all(&work).unwrap();
    fs::create_dir_all(root.join("tools")).unwrap();
    fs::write(root.join("tools").join("state-tx.ps1"), "x").unwrap();

    // Empty HOME/USERPROFILE so no real cc-sync mirror can satisfy resolution either.
    let fake_home = tmp("foreign-home");

    let out = run_engine(&work, &root, &fake_home);
    let stderr = stderr_of(&out);
    assert_eq!(
        out.status.code(),
        Some(2),
        "no trusted tools dir anywhere -> a clean usage refusal: {stderr}"
    );
    assert!(
        stderr.contains("tools directory not found"),
        "prints the clean not-found diagnostic instead of selecting the foreign root/tools: {stderr}"
    );
    assert!(
        !stderr.contains(root.join("tools").display().to_string().as_str()),
        "never even names the untrusted target-local tools dir as a candidate: {stderr}"
    );

    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&fake_home);
}

#[test]
fn default_tools_falls_back_to_the_cc_sync_mirror_without_identity_markers() {
    // Non-checkout root (no markers), also carrying a decoy `root/tools/queue-tx.ps1` that must
    // NEVER be selected. The mirror has `state-tx.ps1` (so the resolver's marker script exists
    // there) but deliberately lacks `queue-tx.ps1`, so `run_once`'s own tool-existence check
    // reports the exact resolved directory — proving the mirror, not `root/tools`, was chosen.
    let root = tmp("mirrorcase-root");
    let work = root.join(".work");
    fs::create_dir_all(&work).unwrap();
    fs::create_dir_all(root.join("tools")).unwrap();
    fs::write(root.join("tools").join("queue-tx.ps1"), "decoy").unwrap();

    let fake_home = tmp("mirrorcase-home");
    let mirror = fake_home.join(".claude").join("scripts");
    fs::create_dir_all(&mirror).unwrap();
    fs::write(mirror.join("state-tx.ps1"), "x").unwrap();

    let out = run_engine(&work, &root, &fake_home);
    let stderr = stderr_of(&out);
    assert_eq!(
        out.status.code(),
        Some(2),
        "missing queue-tx.ps1 in the resolved tools dir is a clean usage error: {stderr}"
    );
    assert!(
        stderr.contains("queue-tx.ps1 not found at"),
        "run_once's own existence check names the missing tool: {stderr}"
    );
    assert!(
        stderr.contains(mirror.display().to_string().as_str()),
        "the resolved tools dir is the cc-sync mirror: {stderr}"
    );
    assert!(
        !stderr.contains(root.join("tools").display().to_string().as_str()),
        "never resolves to the non-checkout root's own tools dir: {stderr}"
    );

    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&fake_home);
}

#[test]
fn default_tools_selects_root_tools_in_a_proven_checkout() {
    // With all three identity markers under --root, the default DOES resolve to the checkout-local
    // <root>/tools. `queue-tx.ps1` is deliberately absent from it so `run_once`'s existence check
    // names that exact path — proving the checkout dir, not the (empty) mirror, was selected.
    let root = tmp("checkout-root");
    let work = root.join(".work");
    fs::create_dir_all(&work).unwrap();
    seed_checkout_markers(&root);
    fs::write(root.join("tools").join("state-tx.ps1"), "x").unwrap();

    // Empty mirror so this can ONLY resolve via the checkout, never the machine's real mirror.
    let fake_home = tmp("checkout-home");

    let out = run_engine(&work, &root, &fake_home);
    let stderr = stderr_of(&out);
    assert_eq!(
        out.status.code(),
        Some(2),
        "missing queue-tx.ps1 in the resolved tools dir is a clean usage error: {stderr}"
    );
    assert!(
        stderr.contains("queue-tx.ps1 not found at"),
        "run_once's own existence check names the missing tool: {stderr}"
    );
    assert!(
        stderr.contains(root.join("tools").display().to_string().as_str()),
        "the resolved tools dir is the proven checkout's own tools dir: {stderr}"
    );

    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&fake_home);
}

#[test]
fn explicit_tools_override_bypasses_the_resolver() {
    // An explicit `--tools <dir>` (the harness/tests/fixtures contract) is used as-is, never routed
    // through the checkout-vs-mirror resolver — even over a non-checkout root with no mirror.
    let root = tmp("override-root");
    let work = root.join(".work");
    fs::create_dir_all(&work).unwrap();
    let explicit_tools = tmp("override-tools");
    fs::write(explicit_tools.join("state-tx.ps1"), "x").unwrap();
    // queue-tx.ps1 deliberately absent so the failure names the EXPLICIT dir, proving it was used.

    let fake_home = tmp("override-home");

    let out = Command::new(BIN)
        .args(["run", "--once", "--work"])
        .arg(&work)
        .arg("--root")
        .arg(&root)
        .arg("--tools")
        .arg(&explicit_tools)
        .args(["--batch", "B-explicit-tools-test", "--json"])
        .env("HOME", &fake_home)
        .env("USERPROFILE", &fake_home)
        .output()
        .expect("spawn engine run --once --tools");
    let stderr = stderr_of(&out);
    assert_eq!(
        out.status.code(),
        Some(2),
        "missing queue-tx.ps1 in the explicit tools dir is a clean usage error: {stderr}"
    );
    assert!(
        stderr.contains(explicit_tools.display().to_string().as_str()),
        "the explicit --tools override is honoured as-is: {stderr}"
    );

    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&explicit_tools);
    let _ = fs::remove_dir_all(&fake_home);
}
