//! Hermetic, offline end-to-end proof of `engine run --once` (task T-109): drive the REAL built
//! binary as the headless engine over a throwaway **sandbox** `.work` and the REAL transaction
//! tools (`queue-tx.ps1` / `state-tx.ps1` / `outbox.ps1`), and assert the engine drives ONE
//! cohort/phase end to end — take its owner lease, admit a cohort by readiness, capture each task,
//! run ONE supervised leaf round (the deterministic `__fake-agent` stand-in), validate every
//! descriptor/cohort transition through `state-tx`, emit the §19 events, and release the lease —
//! with nothing ever touching this repository's own `.work`.
//!
//! The opt-in `--live` path (task T-244) is exercised too, but WITHOUT any model/network: the
//! live-mode test only runs where a real `claude` binary is absent, so the supervised leaf fails to
//! spawn and the task fail-closes — proving the live spawn target was taken with no billable call.
//!
//! Like `lease_fixture`, the PowerShell-driven scenarios need a `pwsh`/`powershell` host to run the
//! `.ps1` tools; when neither is available they self-skip with a note (never fail). Every sandbox
//! lives in a per-test temp directory removed on drop; the default round is offline and token-free.

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
            "orchestra-run-fixture-{}-{nanos}-{n}",
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
fn queue_propose(
    host: &str,
    work: &Path,
    id: &str,
    title: &str,
    predecessors: Option<&str>,
) -> Output {
    let script = tools_dir().join("queue-tx.ps1");
    let mut cmd = Command::new(host);
    cmd.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(&script)
        .args(["propose", "--work"])
        .arg(work)
        .args(["--id", id, "--title", title]);
    if let Some(p) = predecessors {
        cmd.args(["--predecessors", p]);
    }
    cmd.output().expect("spawn queue-tx propose")
}

/// Seed the planner-owned descriptor that supplies a task's typed conflict-domain to admission.
fn write_planned_descriptor(work: &Path, id: &str, state: &str, domain: Option<&str>) {
    let dir = work.join("tasks").join(id);
    fs::create_dir_all(&dir).expect("create planned descriptor directory");
    let mut text = format!("# {id}\nСтатус: {state}\n");
    if let Some(domain) = domain {
        text.push_str(&format!("Конфликт-домен: {domain}\n"));
    }
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
fn opens_cohort_and_runs_one_round_end_to_end() {
    let host = host_or_skip!();
    let sb = Sandbox::new();

    // Seed three tasks: T-201 and T-202 are independent (ready); T-203 depends on the still-open
    // T-202, so it is NOT ready and must be filtered out of admission (readiness gate).
    for (id, title, pred) in [
        ("T-201", "sandbox one", None),
        ("T-202", "sandbox two", None),
        ("T-203", "sandbox three", Some("T-202")),
    ] {
        let out = queue_propose(&host, &sb.work, id, title, pred);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }

    write_planned_descriptor(&sb.work, "T-201", "не начата", Some("engine/src/**"));
    write_planned_descriptor(&sb.work, "T-202", "не начата", Some("tui/**"));
    // T-203 is not ready, so its domain is irrelevant to this admission pass.
    write_planned_descriptor(&sb.work, "T-203", "не начата", Some("docs/**"));

    let batch = "B-20260712T000000Z";
    let run = engine_run(
        &sb.work,
        &["--batch", batch, "--cohort-size", "3", "--json"],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "engine run exits 0: {out} / {}",
        stderr_of(&run)
    );

    // The JSON report: exactly the two ready tasks were admitted, both advanced to review.
    assert!(
        out.contains("\"admitted\":[\"T-201\",\"T-202\"]"),
        "admitted the two ready tasks only (T-203 blocked by its prerequisite): {out}"
    );
    assert!(
        out.contains("\"to\":\"in-review\""),
        "tasks advanced to review: {out}"
    );
    assert!(
        !out.contains("\"to\":\"escalated\""),
        "the happy path escalates nothing: {out}"
    );
    assert!(
        out.contains("\"lease_released\":true"),
        "the lease is released: {out}"
    );
    assert!(
        out.contains("\"reviewer\":\"reviewer\""),
        "T-105 reviewer tier recorded: {out}"
    );

    // The descriptors the engine wrote reflect the round's terminal state.
    assert_eq!(sb.descriptor_status("T-201"), "на ревью");
    assert_eq!(sb.descriptor_status("T-202"), "на ревью");
    // T-203 was never admitted (its prerequisite is still open), so its planner-seeded
    // descriptor (written above, before the run, to supply its domain) is left untouched.
    assert_eq!(
        sb.descriptor_status("T-203"),
        "не начата",
        "the blocked task's descriptor is not advanced"
    );

    // The queue was mutated only through queue-tx capture: the two admitted tasks read `в работе`,
    // the blocked one is still `не начата`.
    let queue = sb.read("Tasks_Queue.md");
    assert!(
        queue.contains("T-201") && queue.contains("в работе"),
        "T-201 captured: {queue}"
    );
    let t203_line = queue
        .lines()
        .find(|l| l.contains("[T-203]"))
        .unwrap_or_default();
    assert!(
        t203_line.contains("не начата"),
        "the blocked task stays not-started: {t203_line}"
    );

    // The cohort state closed through the validated cohort transition.
    let cohort = sb.read("cohort_state.md");
    assert!(
        cohort.contains("Приём: закрыт"),
        "admission closed: {cohort}"
    );

    // The events outbox carries the full round in §19 format.
    let events = sb.read("events.jsonl");
    for ty in [
        "\"type\":\"cohort.opened\"",
        "\"type\":\"task.captured\"",
        "\"type\":\"cohort.round_started\"",
        "\"type\":\"task.status_changed\"",
        "\"type\":\"cohort.round_closed\"",
        "\"type\":\"cohort.admission_closed\"",
    ] {
        assert!(events.contains(ty), "events.jsonl missing {ty}:\n{events}");
    }

    // No dangling owner lease: release removed the whole lock directory.
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "the engine released its lease (orchestrator.lock is gone)"
    );

    // Determinism / hermeticity: no real model call could have run — the round used the offline
    // __fake-agent stand-in (verdict `готово`), which the report surfaces verbatim.
    assert!(
        out.contains("\"verdict\":\"готово\""),
        "round used the offline leaf stand-in: {out}"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn descriptor_domains_control_cohort_packing() {
    let host = host_or_skip!();

    let overlapping = Sandbox::new();
    for (id, title) in [("T-401", "engine root"), ("T-402", "engine state")] {
        let out = queue_propose(&host, &overlapping.work, id, title, None);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(
        &overlapping.work,
        "T-401",
        "не начата",
        Some("engine/src/**"),
    );
    write_planned_descriptor(
        &overlapping.work,
        "T-402",
        "не начата",
        Some("engine/src/state/**"),
    );
    let overlap_run = engine_run(
        &overlapping.work,
        &["--batch", "B-overlap", "--cohort-size", "2", "--json"],
    );
    let overlap_out = stdout_of(&overlap_run);
    assert!(
        overlap_run.status.success(),
        "overlap run exits 0: {overlap_out} / {}",
        stderr_of(&overlap_run)
    );
    assert!(
        overlap_out.contains("\"admitted\":[\"T-401\"]"),
        "overlapping domains must not share a cohort: {overlap_out}"
    );

    let disjoint = Sandbox::new();
    for (id, title) in [("T-411", "engine"), ("T-412", "tui")] {
        let out = queue_propose(&host, &disjoint.work, id, title, None);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&disjoint.work, "T-411", "не начата", Some("engine/src/**"));
    write_planned_descriptor(&disjoint.work, "T-412", "не начата", Some("tui/**"));
    let disjoint_run = engine_run(
        &disjoint.work,
        &["--batch", "B-disjoint", "--cohort-size", "2", "--json"],
    );
    let disjoint_out = stdout_of(&disjoint_run);
    assert!(
        disjoint_run.status.success(),
        "disjoint run exits 0: {disjoint_out} / {}",
        stderr_of(&disjoint_run)
    );
    assert!(
        disjoint_out.contains("\"admitted\":[\"T-411\",\"T-412\"]"),
        "non-overlapping domains should share a cohort: {disjoint_out}"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn active_descriptor_domain_blocks_an_overlapping_candidate() {
    let host = host_or_skip!();
    let sb = Sandbox::new();
    for (id, title) in [("T-421", "blocked by active"), ("T-422", "independent")] {
        let out = queue_propose(&host, &sb.work, id, title, None);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&sb.work, "T-420", "в работе", Some("engine/src/**"));
    write_planned_descriptor(&sb.work, "T-421", "не начата", Some("engine/src/state/**"));
    write_planned_descriptor(&sb.work, "T-422", "не начата", Some("tui/**"));

    let run = engine_run(
        &sb.work,
        &["--batch", "B-active", "--cohort-size", "2", "--json"],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "active-domain run exits 0: {out} / {}",
        stderr_of(&run)
    );
    assert!(
        out.contains("\"admitted\":[\"T-422\"]"),
        "the active descriptor blocks only its overlapping candidate: {out}"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn injected_escalation_takes_one_task_to_escalated() {
    let host = host_or_skip!();
    let sb = Sandbox::new();

    for (id, title) in [("T-301", "one"), ("T-302", "two")] {
        let out = queue_propose(&host, &sb.work, id, title, None);
        assert!(out.status.success(), "propose {id}: {}", stderr_of(&out));
    }
    write_planned_descriptor(&sb.work, "T-301", "не начата", Some("engine/src/**"));
    write_planned_descriptor(&sb.work, "T-302", "не начата", Some("tui/**"));

    // Deterministic fault injection: T-302's leaf emits an `эскалация` verdict.
    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-esc",
            "--cohort-size",
            "3",
            "--inject-escalate",
            "T-302",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "run exits 0: {out} / {}",
        stderr_of(&run)
    );

    // T-301 advanced to review; T-302 fail-closed to escalated.
    assert_eq!(sb.descriptor_status("T-301"), "на ревью");
    assert_eq!(sb.descriptor_status("T-302"), "эскалирована");
    assert!(
        out.contains("\"to\":\"escalated\""),
        "the injected task escalated: {out}"
    );

    // The escalation was reflected transactionally in the queue (queue-tx escalate).
    let queue = sb.read("Tasks_Queue.md");
    let t302_line = queue
        .lines()
        .find(|l| l.contains("[T-302]"))
        .unwrap_or_default();
    assert!(
        t302_line.contains("эскалирована"),
        "the queue records the escalation: {t302_line}"
    );

    // The lease is still cleanly released even though a task escalated.
    assert!(
        out.contains("\"lease_released\":true"),
        "lease released after escalation: {out}"
    );
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "no dangling lease"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn run_requires_a_work_dir_and_accepts_live() {
    // `--work` is required and has NO default, so `run` can never silently touch the live `.work`.
    let missing_work = Command::new(BIN)
        .args(["run", "--once"])
        .output()
        .expect("spawn engine run without --work");
    assert_eq!(
        missing_work.status.code(),
        Some(2),
        "missing --work is a usage error"
    );
    assert!(
        stderr_of(&missing_work).contains("--work"),
        "explains --work is required: {}",
        stderr_of(&missing_work)
    );

    // `--once` is the only mode.
    let no_once = Command::new(BIN)
        .args(["run", "--work", "/tmp/whatever"])
        .output()
        .expect("spawn engine run without --once");
    assert_eq!(
        no_once.status.code(),
        Some(2),
        "missing --once is a usage error"
    );

    // `--live` (task T-244) is now ACCEPTED, not refused: passing it without `--work` falls through
    // to the SAME `--work`-required usage error, and never prints an out-of-scope "refusing --live"
    // message. This proves the flag is parsed as a valid opt-in, not rejected up front. (No leaf is
    // spawned here — the run bails on the missing `--work` before any model call, so this stays
    // hermetic regardless of whether a real `claude` binary is installed.)
    let live = Command::new(BIN)
        .args(["run", "--once", "--live"])
        .output()
        .expect("spawn engine run --live");
    assert_eq!(
        live.status.code(),
        Some(2),
        "--live without --work is a --work usage error, not a --live refusal"
    );
    let live_err = stderr_of(&live);
    assert!(
        live_err.contains("--work"),
        "the failure is the missing --work, proving --live was accepted: {live_err}"
    );
    assert!(
        !live_err.contains("refusing"),
        "--live is no longer refused: {live_err}"
    );
}

/// Whether a real `claude` binary is launchable on this host. Used to keep the live-mode e2e test
/// TOKEN-FREE: it only runs where `claude` is ABSENT (e.g. CI), so the supervised leaf can never
/// reach a real model — it fails to spawn and the task fail-closes. On a dev box with `claude`
/// installed the test self-skips rather than risk a real (billable) call.
fn claude_present() -> bool {
    Command::new("claude")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn live_mode_spawns_a_real_leaf_and_fails_closed_without_claude() {
    let host = host_or_skip!();
    if claude_present() {
        eprintln!(
            "SKIP: a real `claude` binary is installed — refusing to risk a billable model call"
        );
        return;
    }
    let sb = Sandbox::new();

    let out = queue_propose(&host, &sb.work, "T-501", "live wiring", None);
    assert!(out.status.success(), "propose T-501: {}", stderr_of(&out));
    write_planned_descriptor(&sb.work, "T-501", "не начата", Some("engine/src/**"));

    // Under `--live` the coder leaf spawns the REAL `claude` child instead of the offline stand-in.
    // With no `claude` on PATH the supervised spawn fails (a crash), so the task fail-closes to
    // `escalated` — the OFFLINE stand-in would instead have driven a clean `готово`. That divergence
    // is the proof the live spawn target was actually taken, with no model/network involved.
    let run = engine_run(
        &sb.work,
        &[
            "--batch",
            "B-live",
            "--cohort-size",
            "1",
            "--live",
            "--json",
        ],
    );
    let out = stdout_of(&run);
    assert!(
        run.status.success(),
        "live run still exits 0 (escalation is a clean outcome): {out} / {}",
        stderr_of(&run)
    );
    assert!(
        out.contains("\"to\":\"escalated\""),
        "the live leaf could not spawn a real claude, so the task fail-closed: {out}"
    );
    assert!(
        !out.contains("\"verdict\":\"готово\""),
        "no offline stand-in ran — the round did not fabricate a clean verdict: {out}"
    );
    assert_eq!(
        sb.descriptor_status("T-501"),
        "эскалирована",
        "the descriptor records the fail-closed escalation"
    );
    // The lease is still cleanly released even though the live leaf failed to spawn.
    assert!(
        out.contains("\"lease_released\":true"),
        "lease released after a live-spawn failure: {out}"
    );
    assert!(
        !sb.work.join("orchestrator.lock").exists(),
        "no dangling lease after a live run"
    );
}
