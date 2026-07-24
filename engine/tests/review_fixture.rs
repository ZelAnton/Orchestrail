//! Hermetic, offline end-to-end proof of `engine run --once --review` (tasks T-127 / T-128): after
//! the execution round, drive the REAL built binary through the per-task **review fix cycle** over a
//! throwaway **sandbox** `.work` and the REAL transaction tools (`queue-tx.ps1` / `state-tx.ps1` /
//! `outbox.ps1`), and assert BOTH terminal outcomes of the loop wire correctly:
//!
//!   * **converging** — an open `R-` (findings) dispatches a supervised deterministic coder fix
//!     round and a re-review; once the fix converges the re-review is a fresh clean `SUMMARY-R` with
//!     zero open `R-`, so the descriptor is promoted `на ревью -> готова к слиянию`. The per-cycle
//!     `Циклов-ревью: N` counter and each cycle's `state-tx check-transition`-validated transition
//!     are recorded on the descriptor and emitted through the outbox (keyed by cycle → distinct
//!     fingerprints).
//!   * **diverging** — persistent findings never converge, so the loop escalates the task
//!     `на ревью -> эскалирована` on exhausting `REVIEW_LOOP_MAX` (`не сходится ревью после N
//!     циклов`) through the SAME `state-tx check-transition` + `queue-tx escalate` path the
//!     execution round uses — a clean terminal escalation, no re-interpretation.
//!
//! The review + fix rounds run the deterministic `__fake-agent` stand-ins (never a live model call);
//! the tier + concrete reviewer come from the real tiering/reviewer resolvers, the branch from the
//! real `contract`/`gate`, and the loop limit from the real `resolvers::cycles`. Like `run_fixture`,
//! the PowerShell-driven tools need a `pwsh`/`powershell` host; when neither is available the tests
//! self-skip (never fail). Every sandbox lives in a per-test temp directory removed on drop; the run
//! is offline/token-free.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// The real `tools/` directory, resolved from this crate's manifest dir (`.../engine`) → repo root.
fn tools_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine crate has a parent (repo root)")
        .join("tools")
}

/// The first PowerShell host that can actually launch, or `None` (then the caller self-skips).
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

/// A throwaway sandbox `.work` directory, removed on drop.
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
            "orchestra-review-fixture-{}-{nanos}-{n}",
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
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.work);
    }
}

/// Seed one not-started task into the sandbox queue through the REAL `queue-tx.ps1 propose`.
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

/// Seed the planner-owned descriptor that supplies a task's typed conflict-domain to admission.
fn write_planned_descriptor(work: &Path, id: &str, state: &str, domain: &str) {
    let dir = work.join("tasks").join(id);
    fs::create_dir_all(&dir).expect("create planned descriptor directory");
    let text = format!("# {id}\nСтатус: {state}\nКонфликт-домен: {domain}\n");
    fs::write(dir.join("task.md"), text).expect("write planned descriptor");
}

/// Run `engine run --once` over the sandbox with the real tools.
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

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn review_fix_cycle_converges_findings_to_ready() {
    let host = host_or_skip!();
    let sb = Sandbox::new();

    // Two independent (non-overlapping) tasks so both are admitted in one cohort.
    for (id, title) in [("T-501", "clean pass"), ("T-502", "converging findings")] {
        let out = queue_propose(&host, &sb.work, id, title);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&sb.work, "T-501", "не начата", "engine/src/**");
    write_planned_descriptor(&sb.work, "T-502", "не начата", "tui/**");

    // T-502's reviewer stand-in emits an open `R-` (findings) on cycle 1; the fix converges, so the
    // re-review at cycle 2 is a fresh clean `SUMMARY-R` (`--converge-after 2`). T-501 is clean at
    // cycle 1. The default `REVIEW_LOOP_MAX` (8) is never approached.
    let batch = "B-20260712T140000Z";
    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            batch,
            "--cohort-size",
            "2",
            "--review",
            "--inject-findings",
            "T-502",
            "--converge-after",
            "2",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run --review exits 0: {out} / {}",
        stderr_of(&run)
    );

    // Both were admitted and executed to review first (the execution round is unchanged).
    assert!(
        out.contains("\"admitted\":[\"T-501\",\"T-502\"]"),
        "both independent tasks admitted: {out}"
    );

    // The review phase resolved the tier + reviewer through the real resolvers (coder → opus
    // `reviewer`) and both gate branches were exercised (T-502 cycle 1 findings, then clean).
    assert!(
        out.contains("\"gate\":\"clean\""),
        "the clean branch is exercised: {out}"
    );
    assert!(
        out.contains("\"gate\":\"findings\"")
            || sb.read("events.jsonl").contains("\"gate\":\"findings\""),
        "the with-findings branch drove a fix cycle: {out}"
    );
    assert!(
        out.contains("\"reviewer\":\"reviewer\""),
        "the tiering/reviewer resolvers elected the opus reviewer: {out}"
    );
    // Neither task escalated; the fix cycle converged both to ready.
    assert!(
        !out.contains("\"to\":\"escalated\""),
        "a converging review fix cycle escalates nothing: {out}"
    );
    // T-502 took a two-cycle fix; T-501 landed clean on the first cycle.
    assert!(
        out.contains("\"cycles\":2"),
        "the converging task ran two review cycles: {out}"
    );
    assert!(
        out.contains("\"cycles\":1"),
        "the immediately-clean task ran a single review cycle: {out}"
    );

    // The DESCRIPTORS are the strongest assertion: the fix cycle promotes BOTH to готова к слиянию,
    // and the converging task's descriptor carries the persistent `Циклов-ревью: 2` counter.
    assert_eq!(
        sb.descriptor_status("T-501"),
        "готова к слиянию",
        "the clean pass promotes the descriptor to ready"
    );
    assert_eq!(
        sb.descriptor_status("T-502"),
        "готова к слиянию",
        "the converging fix cycle promotes the descriptor to ready"
    );
    let t502_desc = sb.read("tasks/T-502/task.md");
    assert!(
        t502_desc.contains("Циклов-ревью: 2"),
        "the review-cycle counter is recorded on the descriptor: {t502_desc}"
    );

    // The fix cycle emitted its transitions through outbox, distinguishable by the ASCII `cycle`
    // coordinate (host-agnostic — Cyrillic payload words are \u-escaped under WinPS). Cycle 1 is a
    // findings self-transition; cycle 2 is the clean promotion.
    let events = sb.read("events.jsonl");
    assert!(
        events.contains("\"gate\":\"clean\""),
        "the clean promotion event was emitted through outbox: {events}"
    );
    assert!(
        events.contains("\"cycle\":1") && events.contains("\"cycle\":2"),
        "each review cycle is a distinct observable event keyed by cycle: {events}"
    );

    // The queue was never escalated by the fix cycle (both tasks converged).
    let queue = sb.read("Tasks_Queue.md");
    assert!(
        !queue.contains("эскалирована"),
        "no task was escalated by the converging fix cycle: {queue}"
    );

    // No dangling owner lease.
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "the engine released its lease after the review round"
    );
    assert!(
        out.contains("\"lease_released\":true"),
        "the lease is released: {out}"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn review_fix_cycle_escalates_on_review_loop_max() {
    let host = host_or_skip!();
    let sb = Sandbox::new();

    for (id, title) in [
        ("T-511", "clean companion"),
        ("T-512", "diverging findings"),
    ] {
        let out = queue_propose(&host, &sb.work, id, title);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&sb.work, "T-511", "не начата", "engine/src/**");
    write_planned_descriptor(&sb.work, "T-512", "не начата", "tui/**");

    // T-512's findings NEVER converge (no `--converge-after`), so the fix cycle loops until it
    // exhausts `REVIEW_LOOP_MAX`. A tight limit of 2 makes it escalate fast: cycles 1 and 2 run
    // (review + fix each), the 3rd cycle is over budget → clean terminal escalation.
    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-diverge",
            "--cohort-size",
            "2",
            "--review",
            "--inject-findings",
            "T-512",
            "--review-loop-max",
            "2",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run --review exits 0 even when a task escalates: {out} / {}",
        stderr_of(&run)
    );

    // T-511 converged clean → готова к слиянию; T-512 exhausted the budget → эскалирована.
    assert_eq!(
        sb.descriptor_status("T-511"),
        "готова к слиянию",
        "the clean companion is still promoted while its cohort-mate escalates"
    );
    assert_eq!(
        sb.descriptor_status("T-512"),
        "эскалирована",
        "the diverging task escalates on exhausting REVIEW_LOOP_MAX"
    );
    // The terminal descriptor records the completed cycle count (REVIEW_LOOP_MAX = 2).
    let t512_desc = sb.read("tasks/T-512/task.md");
    assert!(
        t512_desc.contains("Циклов-ревью: 2"),
        "the escalated descriptor records the completed cycle count: {t512_desc}"
    );
    assert!(
        out.contains("\"to\":\"escalated\"") && out.contains("\"cycles\":2"),
        "the JSON report surfaces the budget escalation after two cycles: {out}"
    );

    // The escalation went through the SAME transactional queue path (queue-tx escalate): the queue
    // entry now reads `эскалирована`, and the reason names the loop-limit cause.
    let queue = sb.read("Tasks_Queue.md");
    let t512_line = queue
        .lines()
        .find(|l| l.contains("[T-512]"))
        .unwrap_or_default();
    assert!(
        t512_line.contains("эскалирована"),
        "the queue records the escalation transactionally: {t512_line}"
    );
    assert!(
        t512_line.contains("не сходится ревью после 2 циклов"),
        "the queue escalation reason names the loop-limit cause: {t512_line}"
    );

    // The budget-exhaustion event is emitted through outbox with the host-agnostic ASCII markers
    // (Cyrillic reason/status words \u-escape under WinPS, so assert on the ASCII cap/limit).
    let events = sb.read("events.jsonl");
    assert!(
        events.contains("\"cap\":\"REVIEW_LOOP_MAX\"") && events.contains("\"limit\":2"),
        "the escalation event carries the REVIEW_LOOP_MAX cap marker: {events}"
    );

    // No dangling owner lease even though a task escalated.
    assert!(
        out.contains("\"lease_released\":true"),
        "the lease is released after the escalating fix cycle: {out}"
    );
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "no dangling lease"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn both_clean_tasks_are_both_promoted() {
    let host = host_or_skip!();
    let sb = Sandbox::new();

    for (id, title) in [("T-511", "clean one"), ("T-512", "clean two")] {
        let out = queue_propose(&host, &sb.work, id, title);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&sb.work, "T-511", "не начата", "engine/src/**");
    write_planned_descriptor(&sb.work, "T-512", "не начата", "tui/**");

    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-allclean",
            "--cohort-size",
            "2",
            "--review",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run --review exits 0: {out} / {}",
        stderr_of(&run)
    );
    // Both clean passes promote to ready; neither shows a findings branch.
    assert!(
        !out.contains("\"gate\":\"findings\""),
        "no task should have findings: {out}"
    );
    assert_eq!(sb.descriptor_status("T-511"), "готова к слиянию");
    assert_eq!(sb.descriptor_status("T-512"), "готова к слиянию");
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn an_escalated_task_is_not_reviewed() {
    let host = host_or_skip!();
    let sb = Sandbox::new();

    for (id, title) in [("T-521", "reviewed"), ("T-522", "escalates in execution")] {
        let out = queue_propose(&host, &sb.work, id, title);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&sb.work, "T-521", "не начата", "engine/src/**");
    write_planned_descriptor(&sb.work, "T-522", "не начата", "tui/**");

    // T-522 escalates in the EXECUTION round, so the review round must skip it (only `in-review`
    // tasks are reviewed); T-521 is executed then cleanly reviewed to `ready`.
    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-escrev",
            "--cohort-size",
            "2",
            "--review",
            "--inject-escalate",
            "T-522",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run exits 0: {out} / {}",
        stderr_of(&run)
    );

    // T-521: executed → reviewed clean → ready. T-522: escalated in execution, never reviewed.
    assert_eq!(sb.descriptor_status("T-521"), "готова к слиянию");
    assert_eq!(sb.descriptor_status("T-522"), "эскалирована");
    assert!(
        out.contains("\"gate\":\"clean\""),
        "the surviving task was reviewed clean: {out}"
    );
    // The escalated task carries no review object (review is skipped for a non-`in-review` task).
    assert!(
        out.contains("\"id\":\"T-522\""),
        "the escalated task is still reported: {out}"
    );
}
