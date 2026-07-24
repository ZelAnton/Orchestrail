//! Hermetic end-to-end proof of the spike, offline and token-free: drive the REAL built
//! binary supervising a REAL child process (its own `__fake-agent` stand-in) and assert
//! the supervision + stream-json parse behave, and that the deadline path fires. This is
//! the Stage 1 evidence that the "spawn + supervise a leaf-agent call outside Claude Code"
//! environment is tractable — the exact question the intent doc (§8.1, risk R1) gates on.

mod common;
use common::Command;
use std::time::Instant;

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

#[test]
fn selfcheck_reports_pass() {
    let out = Command::new(BIN)
        .arg("selfcheck")
        .output()
        .expect("spawn selfcheck");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "selfcheck exited nonzero: {stdout}");
    assert!(
        stdout.contains("\"selfcheck\":\"pass\""),
        "unexpected selfcheck output: {stdout}"
    );
    // The three classified cases must all show their expected reason.
    assert!(
        stdout.contains("\"success_case\":{\"reason\":\"ok\""),
        "{stdout}"
    );
    assert!(
        stdout.contains("\"timeout_case\":{\"reason\":\"timeout\""),
        "{stdout}"
    );
    assert!(
        stdout.contains("\"error_case\":{\"reason\":\"error\""),
        "{stdout}"
    );
}

#[test]
fn argv_claude_is_headless_and_offline() {
    let out = Command::new(BIN)
        .args(["argv", "claude"])
        .output()
        .expect("spawn argv claude");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(stdout.contains("--output-format stream-json"), "{stdout}");
    assert!(stdout.contains("--permission-mode"), "{stdout}");
    assert!(stdout.contains("--max-turns 40"), "{stdout}");
}

#[test]
fn argv_codex_pins_fail_closed_policy() {
    let out = Command::new(BIN)
        .args(["argv", "codex"])
        .output()
        .expect("spawn argv codex");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(stdout.contains("approval_policy=never"), "{stdout}");
    assert!(stdout.contains("--sandbox workspace-write"), "{stdout}");
}

#[test]
fn live_flag_is_required_for_real_calls() {
    // Without --live the binary must REFUSE to spawn a real model call (exit 2), so an
    // automated run can never accidentally burn tokens / need auth.
    let out = Command::new(BIN)
        .args(["claude", "hello"])
        .output()
        .expect("spawn claude without --live");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--live"), "{stderr}");
}

#[test]
fn deadline_actually_bounds_wallclock() {
    // The `hang` fake-agent sleeps 30s; a 400ms deadline must cut it far sooner. This is
    // the real timeout+tree-kill path, exercised through the built binary's selfcheck.
    let started = Instant::now();
    let out = Command::new(BIN)
        .arg("selfcheck")
        .output()
        .expect("spawn selfcheck");
    let elapsed = started.elapsed();
    assert!(out.status.success());
    // selfcheck's hang case has a 400ms deadline; the whole selfcheck must finish in a
    // few seconds, proving the deadline fired rather than waiting out the 30s sleep.
    assert!(
        elapsed.as_secs() < 15,
        "selfcheck took too long ({elapsed:?}) — the deadline path did not bound the hang"
    );
}
