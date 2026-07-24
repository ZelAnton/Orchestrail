//! Hermetic, offline end-to-end proof of `engine run --once --review --join` (task T-243): after
//! the review round promotes the ready tasks, drive the REAL built binary through the **join
//! barrier** (phases 4–6) over a throwaway **sandbox** `.work` and the REAL transaction tools
//! (`queue-tx.ps1` / `state-tx.ps1` / `outbox.ps1`), and assert every branch of the segment wires
//! correctly:
//!
//!   * **clean** — both ready tasks merge (merger stand-in `merge_report.md`), the integration
//!     review passes clean at cycle 1, the batch publishes (ff-merge simulated), each task advances
//!     `готова к слиянию -> слита -> опубликована -> выполнена` through the REAL §13.1 state machine,
//!     the integration state runs `none -> in-progress -> reviewed -> published -> cleaned`, and the
//!     §19 `cohort.join_started` / `cohort.published` / `cohort.closed` events are emitted.
//!   * **merge quarantine** — the merger stand-in quarantines one branch; that task is rolled back
//!     `готова к слиянию -> конфликт` and re-queued transactionally (`queue-tx return`), the rest
//!     publishes.
//!   * **integration escalation** — the integration review never converges, so the bounded cycle
//!     exhausts `INTEGRATION_LOOP_MAX` and STOPS without publishing: the merged tasks stay `слита`,
//!     the integration state is `failed`, and only `cohort.closed` (not `cohort.published`) is
//!     emitted.
//!   * **integration convergence** — a with-findings integration review that converges at cycle N
//!     dispatches a deterministic integration fix each cycle and then publishes.
//!
//! Every merger / full_reviewer / fix round runs a deterministic `__fake-agent` stand-in (never a
//! live model call, never a real VCS mutation — the sandbox `.work` has no repository); the merge/
//! quarantine decision comes from the real `contract::parse_merge_report`, the integration gate from
//! the real `contract`/`gate`, and the cycle limit from the real `resolvers::cycles`. Like the other
//! fixtures, the PowerShell tools need a `pwsh`/`powershell` host; when neither is available the
//! tests self-skip (never fail). Every sandbox lives in a per-test temp directory removed on drop.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tools_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine crate has a parent (repo root)")
        .join("tools")
}

fn pwsh_host() -> Option<String> {
    for host in ["pwsh", "powershell"] {
        let ok = Command::new(host)
            .args(["-NoProfile", "-Command", "exit 0"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            return Some(host.to_string());
        }
    }
    None
}

macro_rules! host_or_skip {
    () => {
        match pwsh_host() {
            Some(h) => h,
            None => {
                eprintln!(
                    "SKIP: no PowerShell host (pwsh/powershell) available for the .ps1 tools"
                );
                return;
            }
        }
    };
}

struct Sandbox {
    work: PathBuf,
}

impl Sandbox {
    fn new() -> Sandbox {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let work = std::env::temp_dir().join(format!(
            "orchestra-join-fixture-{}-{nanos}-{n}",
            std::process::id()
        ));
        fs::create_dir_all(&work).unwrap();
        Sandbox { work }
    }
    fn read(&self, rel: &str) -> String {
        fs::read_to_string(self.work.join(rel)).unwrap_or_default()
    }
    fn descriptor_status(&self, id: &str) -> String {
        let md = self.read(&format!("tasks/{id}/task.md"));
        for line in md.lines() {
            if let Some(rest) = line.trim().strip_prefix("Статус:") {
                return rest.trim().to_string();
            }
        }
        String::new()
    }
    fn descriptor_exists(&self, id: &str) -> bool {
        self.work.join("tasks").join(id).join("task.md").exists()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.work);
    }
}

fn queue_propose(host: &str, work: &Path, id: &str, title: &str) -> Output {
    let script = tools_dir().join("queue-tx.ps1");
    let mut cmd = Command::new(host);
    cmd.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(&script)
        .args(["propose", "--work"])
        .arg(work)
        .args(["--id", id, "--title", title]);
    cmd.output().expect("spawn queue-tx propose")
}

fn write_planned_descriptor(work: &Path, id: &str, domain: &str) {
    let dir = work.join("tasks").join(id);
    fs::create_dir_all(&dir).expect("create planned descriptor directory");
    let text = format!("# {id}\nСтатус: не начата\nКонфликт-домен: {domain}\n");
    fs::write(dir.join("task.md"), text).expect("write planned descriptor");
}

fn engine_run(work: &Path, extra: &[&str]) -> Output {
    let mut cmd = Command::new(BIN);
    cmd.args(["run", "--once", "--work"])
        .arg(work)
        .arg("--tools")
        .arg(tools_dir())
        .args(["--base", "sandbox-base"]);
    for a in extra {
        cmd.arg(a);
    }
    cmd.output().expect("spawn engine run")
}

fn stdout_of(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}
fn stderr_of(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// Seed two independent (non-overlapping) tasks so both are admitted into one cohort.
fn seed_two(host: &str, sb: &Sandbox) {
    for (id, title) in [("T-101", "join one"), ("T-102", "join two")] {
        let out = queue_propose(host, &sb.work, id, title);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&sb.work, "T-101", "alpha/**");
    write_planned_descriptor(&sb.work, "T-102", "beta/**");
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn clean_join_publishes_and_archives_both_tasks() {
    let host = host_or_skip!();
    let sb = Sandbox::new();
    seed_two(&host, &sb);

    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-20260716T000000Z",
            "--cohort-size",
            "2",
            "--review",
            "--join",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run --join exits 0: {out} / {}",
        stderr_of(&run)
    );

    // The join report: both merged, both published, integration fully cleaned in one review cycle.
    assert!(
        out.contains("\"integration\":\"cleaned\""),
        "the integration state reaches cleaned: {out}"
    );
    assert!(
        out.contains("\"merged\":[\"T-101\",\"T-102\"]")
            && out.contains("\"published\":[\"T-101\",\"T-102\"]"),
        "both tasks merged and published: {out}"
    );
    assert!(
        out.contains("\"integration_cycles\":1"),
        "the clean integration review converged at cycle 1: {out}"
    );

    // The batch was archived: both descriptors are gone and both ids are in Tasks_Done.md.
    assert!(
        !sb.descriptor_exists("T-101") && !sb.descriptor_exists("T-102"),
        "archived tasks have their descriptor dir removed"
    );
    let done = sb.read("Tasks_Done.md");
    assert!(
        done.contains("[T-101]") && done.contains("[T-102]"),
        "both tasks archived to Tasks_Done.md: {done}"
    );

    // The §19 join-barrier events were emitted, plus the per-task archival transition.
    let events = sb.read("events.jsonl");
    for ty in [
        "\"type\":\"cohort.join_started\"",
        "\"type\":\"cohort.published\"",
        "\"type\":\"cohort.closed\"",
    ] {
        assert!(events.contains(ty), "events.jsonl missing {ty}:\n{events}");
    }
    assert!(
        events.contains("\"from\":\"опубликована\",\"to\":\"выполнена\""),
        "the published->done archival transition is emitted: {events}"
    );

    // No dangling owner lease.
    assert!(
        out.contains("\"lease_released\":true"),
        "the lease is released after the join barrier: {out}"
    );
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "no dangling lease"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn merge_quarantine_requeues_the_conflicting_branch_and_publishes_the_rest() {
    let host = host_or_skip!();
    let sb = Sandbox::new();
    seed_two(&host, &sb);

    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-20260716T111111Z",
            "--cohort-size",
            "2",
            "--review",
            "--join",
            "--inject-merge-conflict",
            "T-102",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run --join (quarantine) exits 0: {out} / {}",
        stderr_of(&run)
    );

    // T-101 merged + published; T-102 quarantined (not merged). The batch still cleans up.
    assert!(
        out.contains("\"merged\":[\"T-101\"]")
            && out.contains("\"quarantined\":[\"T-102\"]")
            && out.contains("\"published\":[\"T-101\"]"),
        "T-101 published, T-102 quarantined: {out}"
    );
    assert!(
        out.contains("\"integration\":\"cleaned\""),
        "the surviving task still integrates and publishes cleanly: {out}"
    );

    // T-101 archived; T-102 re-queued transactionally with an incremented attempt counter and its
    // descriptor dropped (Phase 6.2 bounded requeue).
    let done = sb.read("Tasks_Done.md");
    assert!(done.contains("[T-101]"), "T-101 archived: {done}");
    assert!(!done.contains("[T-102]"), "T-102 NOT archived: {done}");
    assert!(
        !sb.descriptor_exists("T-102"),
        "the quarantined task's descriptor dir is removed"
    );
    let queue = sb.read("Tasks_Queue.md");
    let t102_line = queue
        .lines()
        .find(|l| l.contains("[T-102]"))
        .unwrap_or_default();
    assert!(
        t102_line.contains("не начата") && t102_line.contains("попытка=2"),
        "the quarantined task is re-queued with an incremented attempt: {t102_line}"
    );

    // The join started and the survivor published; both events present.
    let events = sb.read("events.jsonl");
    assert!(
        events.contains("\"type\":\"cohort.join_started\"")
            && events.contains("\"type\":\"cohort.published\""),
        "join_started + published emitted for the surviving task: {events}"
    );
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "no dangling lease"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn integration_review_escalation_leaves_the_batch_unpublished() {
    let host = host_or_skip!();
    let sb = Sandbox::new();
    seed_two(&host, &sb);

    // The integration review NEVER converges (no `--integration-converge-after`), so a tight
    // `--integration-loop-max 1` exhausts the cycle without publishing.
    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-20260716T222222Z",
            "--cohort-size",
            "2",
            "--review",
            "--join",
            "--inject-f-findings",
            "--integration-loop-max",
            "1",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run --join exits 0 even when the integration review does not converge: {out} / {}",
        stderr_of(&run)
    );

    // The batch merged but did NOT publish: integration state `failed`, nothing published.
    assert!(
        out.contains("\"integration\":\"failed\""),
        "the integration state is failed (batch unpublished): {out}"
    );
    assert!(
        out.contains("\"merged\":[\"T-101\",\"T-102\"]") && out.contains("\"published\":[]"),
        "both tasks merged but neither published: {out}"
    );

    // The merged tasks stay `слита` (not published, not archived); nothing is in Tasks_Done.md.
    assert_eq!(
        sb.descriptor_status("T-101"),
        "слита",
        "an unpublished merged task stays слита"
    );
    assert_eq!(sb.descriptor_status("T-102"), "слита");
    assert!(
        !sb.read("Tasks_Done.md").contains("[T-101]"),
        "no task is archived when the batch is unpublished"
    );

    // The join started and the cohort closed, but there is NO cohort.published (nothing shipped).
    let events = sb.read("events.jsonl");
    assert!(
        events.contains("\"type\":\"cohort.join_started\"")
            && events.contains("\"type\":\"cohort.closed\""),
        "join_started + closed emitted: {events}"
    );
    assert!(
        !events.contains("\"type\":\"cohort.published\""),
        "cohort.published must NOT be emitted for an unpublished batch: {events}"
    );
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "no dangling lease even when the integration review does not converge"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn integration_review_converges_after_a_fix_cycle_then_publishes() {
    let host = host_or_skip!();
    let sb = Sandbox::new();
    seed_two(&host, &sb);

    // Findings on cycle 1 dispatch a deterministic integration fix; the re-review at cycle 2 is a
    // fresh clean SUMMARY-F, so the batch publishes after TWO integration cycles.
    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-20260716T333333Z",
            "--cohort-size",
            "2",
            "--review",
            "--join",
            "--inject-f-findings",
            "--integration-converge-after",
            "2",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run --join (converging integration review) exits 0: {out} / {}",
        stderr_of(&run)
    );

    assert!(
        out.contains("\"integration\":\"cleaned\""),
        "a converging integration review publishes and cleans up: {out}"
    );
    assert!(
        out.contains("\"integration_cycles\":2"),
        "the integration review took two cycles to converge: {out}"
    );
    assert!(
        out.contains("\"published\":[\"T-101\",\"T-102\"]"),
        "both tasks publish after the integration fix converges: {out}"
    );
    assert!(
        sb.read("events.jsonl")
            .contains("\"type\":\"cohort.published\""),
        "cohort.published is emitted once the integration review converges"
    );
}
