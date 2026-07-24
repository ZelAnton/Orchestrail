//! Hermetic, offline proof of the `engine plan --dry-run` subcommand end to end: build a temporary
//! `.work/`-shaped directory from Markdown fixtures (queue + descriptor + cohort + batch + config +
//! Tasks_Done), drive the REAL built binary against it, and assert it prints the cohort
//! budget/circuit-breaker gate, the admission plan, and the per-active-task reviewer tier — and,
//! crucially, that it writes NOTHING (no lock, no new files). Everything is a temp directory; no
//! network, no live `.work/`.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TmpWork {
    dir: PathBuf,
}

impl TmpWork {
    fn new() -> TmpWork {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "orchestra-plan-fixture-{}-{nanos}-{n}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        TmpWork { dir }
    }
    fn write(&self, rel: &str, contents: &str) {
        let path = self.dir.join(rel);
        if let Some(p) = path.parent() {
            fs::create_dir_all(p).unwrap();
        }
        fs::write(path, contents).unwrap();
    }
    /// The relative paths of every file under the work dir (to prove the dry-run writes nothing).
    fn file_set(&self) -> BTreeSet<PathBuf> {
        let mut out = BTreeSet::new();
        collect(&self.dir, &self.dir, &mut out);
        out
    }
}
impl Drop for TmpWork {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn collect(root: &PathBuf, dir: &PathBuf, out: &mut BTreeSet<PathBuf>) {
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                collect(root, &p, out);
            } else {
                out.insert(p.strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }
}

fn run_plan(work: &PathBuf, extra: &[&str]) -> Output {
    let mut cmd = Command::new(BIN);
    cmd.arg("plan").arg("--dry-run").arg("--work").arg(work);
    for a in extra {
        cmd.arg(a);
    }
    cmd.output().expect("spawn engine plan")
}

/// A `.work/` with an open cohort, one working task, a batch manifest, config, done history, and a
/// queue mixing a ready candidate with a prerequisite-blocked one.
fn seed(w: &TmpWork) {
    w.write(
        "config.md",
        "MAX_PARALLEL: 5\nREVIEWER_TIERING: true\n# COHORT_SIZE and COHORT_MAX_AGE left as defaults\n",
    );
    w.write(
        "Tasks_Done.md",
        "### [T-101] Done predecessor — статус: выполнена\n### [T-105] Another done — статус: выполнена\n",
    );
    w.write(
        "Tasks_Queue.md",
        "### [T-106] Active task — статус: в работе · батч=B-1\nПредпосылки: T-105\n\n\
         ### [T-107] Ready candidate — статус: не начата\nПредпосылки: T-105\n\n\
         ### [T-108] Blocked candidate — статус: не начата\nПредпосылки: T-107\n",
    );
    w.write(
        "tasks/T-106/task.md",
        "# T-106\nСтатус: в работе\nПредпосылки: T-105\n",
    );
    w.write(
        "cohort_state.md",
        "# Cohort state — Batch B-1\nНачало когорты: 2026-07-11T11:39:48Z\nПриём: открыт\nВолна: 1\nAdmitted всего: 1\n",
    );
    w.write(
        "batch.md",
        "# Batch B-1\nБаза: abc123\n## Задачи\n- [T-106] уровень=coder_deep ветка=task/T-106 домен=engine/src/state/** волна=1\n",
    );
}

#[test]
fn dry_run_prints_gate_admission_plan_and_reviewer_tier() {
    let w = TmpWork::new();
    seed(&w);

    let out = run_plan(&w.dir, &[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "plan exited nonzero: {stdout}");

    // Cohort + budget/circuit-breaker gate. The specific decision (keep/close) depends on the
    // fixture's cohort age vs the real wall clock, so assert the gate STRUCTURE here (the
    // Continue/Close paths themselves are unit-tested in `resolvers::budget`); the age is echoed.
    assert!(stdout.contains("admission=open"), "{stdout}");
    assert!(stdout.contains("admitted=1"), "{stdout}");
    assert!(stdout.contains("Budget/circuit-breaker gate:"), "{stdout}");
    assert!(stdout.contains("COHORT_SIZE=15"), "{stdout}");
    assert!(stdout.contains("COHORT_MAX_AGE=90m"), "{stdout}");

    // Active task T-106: really-active class, coder_deep -> reviewer tier (T-105 base_reviewer).
    assert!(
        stdout.contains("T-106 · status=working · class=active"),
        "{stdout}"
    );
    assert!(stdout.contains("reviewer=reviewer"), "{stdout}");

    // Admission plan: T-107 is ready (prereq T-105 done); T-108 is blocked on T-107.
    assert!(stdout.contains("would admit: T-107"), "{stdout}");
    assert!(stdout.contains("T-107 · ready"), "{stdout}");
    assert!(
        stdout.contains("T-108 · blocked (unmet prereqs: T-107)"),
        "{stdout}"
    );
}

#[test]
fn dry_run_writes_nothing() {
    let w = TmpWork::new();
    seed(&w);
    let before = w.file_set();

    let out = run_plan(&w.dir, &[]);
    assert!(out.status.success());

    let after = w.file_set();
    assert_eq!(
        before, after,
        "dry-run must not create/remove any file (e.g. orchestrator.lock)"
    );
    // Belt and suspenders: the lock must never appear.
    assert!(!w.dir.join("orchestrator.lock").exists());
}

#[test]
fn dry_run_requires_the_flag() {
    let w = TmpWork::new();
    seed(&w);
    // Without --dry-run the subcommand refuses (exit 2) rather than doing anything.
    let out = Command::new(BIN)
        .arg("plan")
        .arg("--work")
        .arg(&w.dir)
        .output()
        .expect("spawn engine plan");
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--dry-run"));
}

#[test]
fn idle_work_dir_has_no_cohort_gate() {
    let w = TmpWork::new();
    // Only a queue, no cohort/batch — the idle state.
    w.write("Tasks_Queue.md", "### [T-200] Solo — статус: не начата\n");
    let out = run_plan(&w.dir, &[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{stdout}");
    assert!(stdout.contains("Cohort: none"), "{stdout}");
    // A candidate with no prerequisites is ready.
    assert!(stdout.contains("T-200 · ready"), "{stdout}");
}
