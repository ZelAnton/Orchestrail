//! ProcessKit-backed CLI proof that PAUSE holds before Phase-0 and resumes without a leaf spawn.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use vcs_git::{Git, GitApi};

mod common;
use common::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temporary_work() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "orchestrail-processor-pause-resume-{}-{nanos}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let work = root.join(".work");
    fs::create_dir_all(&work).expect("create temporary .work directory");
    work
}

fn root_for(work: &Path) -> &Path {
    work.parent().expect("temporary work has a project root")
}

fn initialize_repository(root: &Path) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create typed Git test runtime");
    let git = Git::hardened();
    runtime
        .block_on(git.init(root))
        .expect("initialize Git repository");
    runtime
        .block_on(git.config_set(root, "user.name", "Orchestrail Test"))
        .expect("configure Git user name");
    runtime
        .block_on(git.config_set(root, "user.email", "orchestrail-test@example.invalid"))
        .expect("configure Git user email");
    fs::write(root.join("base.txt"), "base\n").expect("write initial repository file");
    runtime
        .block_on(git.add(root, &[PathBuf::from("base.txt")]))
        .expect("stage initial repository file");
    runtime
        .block_on(git.commit(root, "Initial base"))
        .expect("commit initial repository file");
}

fn processor(
    work: &Path,
    root: &Path,
    owner: &str,
    include_base: bool,
    continue_requested: bool,
) -> Output {
    let mut command = Command::new(BIN);
    command.args(["processor", "--once", "--live", "--work"]);
    command.arg(work);
    command.args(["--root"]);
    command.arg(root);
    if include_base {
        // The bootstrap checkout has no primary branch. Supplying one for the post-resume empty
        // path avoids an unrelated branch-discovery failure; that path never resolves, mutates,
        // or publishes the ref.
        command.args(["--base", "main"]);
    }
    if continue_requested {
        command.arg("--continue");
    }
    command.args(["--batch", "B-pause-fixture", "--owner", owner, "--json"]);
    command
        .output()
        .expect("launch contained processor command")
}

fn lease(root: &Path, work: &Path, args: &[&str]) -> Output {
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

fn release_sync(work: &Path, root: &Path, owner: &str, resume: bool) -> Output {
    let mut command = Command::new(BIN);
    command.args(["release-sync", "--live", "--work"]);
    command.arg(work);
    command.arg("--root");
    command.arg(root);
    command.args(["--version", "1.2.3", "--owner", owner, "--json"]);
    if resume {
        command.arg("--resume");
    }
    command.output().expect("launch release-sync command")
}

#[test]
fn pause_then_resume_cli_never_starts_phase_zero_or_a_leaf_while_held() {
    let work = temporary_work();
    let pause = work.join("PAUSE");
    fs::write(&pause, "operator hold\n").expect("write PAUSE marker");

    // This path is deliberately not a repository. A successful hold proves PAUSE is checked
    // before typed VCS discovery, phase-0 inspection, or any agent boundary exists.
    let held = processor(&work, root_for(&work), "pause-owner-a", false, false);
    assert!(
        held.status.success(),
        "paused processor should return a held result: {}",
        String::from_utf8_lossy(&held.stderr)
    );
    let held_json: serde_json::Value =
        serde_json::from_slice(&held.stdout).expect("parse held processor JSON");
    assert_eq!(held_json["processor"], "held");
    assert!(
        held_json["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("before phase-0 recovery"))
    );
    assert!(work.join("status.md").is_file());
    assert!(work.join("journal.md").is_file());
    assert!(
        !work.join("processor_runtime.json").exists(),
        "cold-start PAUSE must not create a native reducer checkpoint"
    );
    assert!(
        !work.join("orchestrator.lock").exists(),
        "held process must owner-check and release its lease before returning"
    );

    fs::remove_file(&pause).expect("remove operator PAUSE marker");
    initialize_repository(root_for(&work));
    let resumed = processor(&work, root_for(&work), "pause-owner-b", true, false);
    assert!(
        resumed.status.success(),
        "resumed empty processor should reach idle without a leaf: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let resumed_json: serde_json::Value =
        serde_json::from_slice(&resumed.stdout).expect("parse resumed processor JSON");
    assert_eq!(resumed_json["processor"], "idle");
    assert!(
        !work.join("processor_runtime.json").exists(),
        "empty resume must not invent a cohort or a leaf checkpoint"
    );
    assert!(
        !work.join("orchestrator.lock").exists(),
        "resumed process must release its own lease"
    );

    // The deferred fallback preserves the prior CLI classification: without PAUSE, a missing
    // base on an invalid repository is a usage/configuration refusal (2), not a runtime result.
    let invalid_work = temporary_work();
    let invalid_base = processor(
        &invalid_work,
        root_for(&invalid_work),
        "pause-owner-c",
        false,
        false,
    );
    assert_eq!(invalid_base.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&invalid_base.stderr)
            .contains("cannot determine publication branch through typed VCS"),
        "unexpected invalid-base refusal: {}",
        String::from_utf8_lossy(&invalid_base.stderr)
    );
    assert!(
        !work.join("orchestrator.lock").exists(),
        "usage refusal after the delayed base lookup must still release its lease"
    );
    let _ = fs::remove_dir_all(root_for(&invalid_work));
    let _ = fs::remove_dir_all(root_for(&work));
}

#[test]
fn continue_reuses_an_addressed_live_lease_before_the_pause_gate() {
    let work = temporary_work();
    let root = root_for(&work).to_path_buf();
    let equivalent_root_spelling = root.join("nested").join("..").join(".");
    fs::write(work.join("PAUSE"), "operator hold\n").expect("write PAUSE marker");
    let acquired = lease(
        &root,
        &work,
        &["acquire", "--owner", "interrupted-owner", "--ttl", "60"],
    );
    assert!(
        acquired.status.success(),
        "seed addressed lease: {}",
        String::from_utf8_lossy(&acquired.stderr)
    );

    let continued = processor(
        &work,
        &equivalent_root_spelling,
        "new-process-owner",
        false,
        true,
    );
    assert!(
        continued.status.success(),
        "continue should normalize the addressed root, renew the owner, then hold: {}",
        String::from_utf8_lossy(&continued.stderr)
    );
    let continued_json: serde_json::Value =
        serde_json::from_slice(&continued.stdout).expect("parse continued processor JSON");
    assert_eq!(continued_json["processor"], "held");
    assert!(
        !work.join("orchestrator.lock").exists(),
        "continued process must release the renewed existing owner lease on PAUSE"
    );
    let _ = fs::remove_dir_all(root_for(&work));
}

#[test]
fn paused_release_sync_touches_no_vcs_and_releases_its_owner_lease() {
    let work = temporary_work();
    let root = root_for(&work).to_path_buf();
    fs::write(work.join("PAUSE"), "operator hold\n").expect("write PAUSE marker");
    let output = release_sync(&work, &root, "release-pause-owner", true);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("paused by .work/PAUSE"),
        "unexpected release-sync pause result: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !work.join("orchestrator.lock").exists(),
        "release-sync must owner-check and release its lease on PAUSE"
    );
    let _ = fs::remove_dir_all(root_for(&work));
}

#[test]
fn live_commands_reject_an_alternate_control_plane_before_lease_or_vcs() {
    let canonical_work = temporary_work();
    let root = root_for(&canonical_work).to_path_buf();
    let alternate_work = root.join("alternate-work");
    fs::create_dir(&alternate_work).expect("create alternate control directory");

    let processor_output = processor(
        &alternate_work,
        &root,
        "alternate-processor-owner",
        false,
        false,
    );
    assert_eq!(processor_output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&processor_output.stderr)
            .contains("must address the selected project root's .work directory")
    );

    let release_output = release_sync(&alternate_work, &root, "alternate-release-owner", false);
    assert_eq!(release_output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&release_output.stderr)
            .contains("must address the selected project root's .work directory")
    );
    assert!(!alternate_work.join("orchestrator.lock").exists());
    let _ = fs::remove_dir_all(root);
}
