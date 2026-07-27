//! Real Git boundary proof for the typed `vcs-*` service.
//!
//! The setup subprocesses are test-only and run through the shared ProcessKit fixture adapter.
//! Once the fixture exists, the engine performs discovery, worktree creation, commit and cleanup
//! solely through `VcsService` / `vcs-core`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::Command;
use orchestrail_engine::recovery::PublicationObservation;
use orchestrail_engine::vcs::VcsService;
use vcs_core::{BackendKind, MergeProbe};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct GitFixture {
    root: PathBuf,
}

impl GitFixture {
    fn new() -> Option<Self> {
        let root = std::env::temp_dir().join(format!(
            "orchestrail-vcs-fixture-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).ok()?;
        if !git(&root, ["init", "--initial-branch=main"]) {
            let _ = fs::remove_dir_all(&root);
            eprintln!("SKIP: git is not available for VcsService boundary fixture");
            return None;
        }
        if !git(&root, ["config", "user.name", "Orchestrail Test"])
            || !git(&root, ["config", "user.email", "test@example.invalid"])
        {
            let _ = fs::remove_dir_all(&root);
            panic!("git fixture identity configuration failed");
        }
        fs::write(root.join("seed.txt"), "seed\n").expect("seed fixture file");
        if !git(&root, ["add", "seed.txt"]) || !git(&root, ["commit", "-m", "seed"]) {
            let _ = fs::remove_dir_all(&root);
            panic!("git fixture initial commit failed");
        }
        Some(Self { root })
    }
}

impl Drop for GitFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn git<const N: usize>(cwd: &Path, args: [&str; N]) -> bool {
    let output = Command::new("git").args(args).current_dir(cwd).output();
    match output {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}

#[test]
fn typed_vcs_service_creates_commits_and_cleans_a_guarded_task_worktree() {
    let Some(fixture) = GitFixture::new() else {
        return;
    };
    let vcs = VcsService::discover(&fixture.root).expect("discover test Git repository");
    assert_eq!(vcs.backend(), BackendKind::Git);
    assert_eq!(vcs.snapshot().unwrap().branch.as_deref(), Some("main"));
    let base = vcs
        .snapshot()
        .expect("read immutable test base")
        .head
        .expect("seed commit head");

    let work = fixture.root.join(".work");
    let workspace = vcs
        .ensure_task_workspace(&work, "T-1", "main")
        .expect("create typed task worktree");
    assert_eq!(workspace.branch, "task/T-1");
    let physical_workspace = fs::canonicalize(&workspace.path).expect("resolve task worktree");
    let physical_worktrees =
        fs::canonicalize(work.join("worktrees")).expect("resolve guarded worktree root");
    assert!(physical_workspace.starts_with(physical_worktrees));
    let empty_task = vcs
        .task_recovery_observation(&work, "T-1", &base)
        .expect("observe empty task branch through vcs-core");
    assert!(empty_task.branch_exists);
    assert!(empty_task.workspace_present);
    assert!(!empty_task.commits_after_base);
    let empty_task_from_ref = vcs
        .task_recovery_observation(&work, "T-1", "main")
        .expect("observe empty task branch from a symbolic base ref");
    assert!(
        !empty_task_from_ref.commits_after_base,
        "a task branch created directly from main has no post-base commit"
    );
    fs::write(workspace.path.join("implementation.txt"), "done\n").expect("task edit");
    // Simulate an unrelated edit arriving after the leaf's structured report. The reported path
    // API must leave it uncommitted instead of treating every worktree change as task output.
    fs::write(workspace.path.join("seed.txt"), "operator edit\n").expect("unrelated edit");
    let commit = vcs
        .commit_workspace_paths(
            &workspace,
            &[PathBuf::from("implementation.txt")],
            "Implement T-1",
        )
        .expect("commit exact changed task paths");
    assert!(!commit.is_empty());
    assert!(
        !vcs.workspace_diff(&workspace).unwrap().is_empty(),
        "the unrelated tracked edit must remain outside the task commit"
    );
    let committed_task = vcs
        .task_recovery_observation(&work, "T-1", &base)
        .expect("observe post-base task commit through vcs-core");
    assert!(committed_task.commits_after_base);

    let integration = vcs
        .ensure_integration_workspace(&work, "B-20260724T120000Z", "main")
        .expect("create the guarded singleton integration workspace");
    assert!(matches!(
        vcs.preflight_task_merge(&integration, "T-1")
            .expect("typed merge probe must roll back its test merge"),
        MergeProbe::Clean
    ));

    vcs.remove_task_workspace(&work, "T-1")
        .expect("guarded cleanup is allowed for its exact task path");
    assert!(!workspace.path.exists());
    // A terminal cleanup is idempotent; it does not infer or delete unrelated directories.
    vcs.remove_task_workspace(&work, "T-1")
        .expect("second cleanup is a no-op");

    let integration_before_commit = vcs
        .integration_recovery_observation(
            &work,
            "B-20260724T120000Z",
            "main",
            PublicationObservation::NotPublished,
        )
        .expect("observe integration workspace through vcs-core");
    assert!(integration_before_commit.branch_exists);
    assert!(integration_before_commit.workspace_present);
    assert_eq!(integration_before_commit.workspace_clean, Some(true));
    assert!(!integration_before_commit.commits_after_base);
    assert!(!integration_before_commit.merge_report_present);
    fs::write(integration.path.join("integration-fix.txt"), "fixed\n")
        .expect("integration fix edit");
    assert!(
        !vcs.commit_integration_workspace_paths(
            &integration,
            &[PathBuf::from("integration-fix.txt")],
            "Fix integration",
        )
        .expect("commit a precise integration fix")
        .is_empty()
    );
    vcs.remove_integration_workspace(&work, "B-20260724T120000Z")
        .expect("clean the exact integration workspace and branch");
}
