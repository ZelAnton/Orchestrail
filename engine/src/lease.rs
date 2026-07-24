//! `engine lease {acquire|heartbeat|release|status}` — the engine's **owner-lease
//! interlock** with the running `processor` (task T-107).
//!
//! Exactly one control-plane owner may mutate a run's state at a time. That mutual
//! exclusion is enforced by the owner **lease** on `.work/orchestrator.lock`
//! (`docs/queue_contract.md` §14–§17), whose single transactional interface is
//! `tools/state-tx.ps1` (owner-checked, liveness-checked by pid/heartbeat/TTL). This
//! module lets the engine take / renew / release / inspect that same lease **strictly**
//! through `state-tx.ps1` — it does **not** re-implement any owner or liveness check,
//! and it never removes a foreign lease with a bare `rm -rf`.
//!
//! The engine holds the lease under its **own role** (`engine`), distinct from
//! `processor`, so it never masquerades as the processor in `lease.json`. Because the
//! engine and the processor share the one lock file, they mutually exclude each other by
//! construction: the engine's `acquire` succeeds only when the lease is free or provably
//! stale, and cleanly refuses when a **live** processor holds it (and vice-versa).
//!
//! **Boundaries this module keeps.**
//! * All mutation goes through `state-tx.ps1`'s standard verbs (`acquire` / `takeover` /
//!   `heartbeat` / `release` / `status`). No parallel lease mechanism, no direct file
//!   surgery on `lease.json`, no `rm -rf` of the lock directory.
//! * The engine never passes `--force`. Adopting a provably-**stale** lease uses the
//!   liveness-gated `takeover` verb (which refuses a *live* lease with exit 10); forcing
//!   over a live/legacy/corrupt lease is an **operator**-only path (operator runs
//!   `state-tx.ps1 … --force` directly), never something the engine does on its own.
//! * `release` presents the engine's own `owner_id`; `state-tx` refuses to release a
//!   lease owned by someone else (exit 13, the late-cleanup-race guard, §15), so the
//!   engine physically cannot tear down a live processor lease.
//!
//! This module is used only by the `engine lease` subcommand; it is **not** wired into
//! `agents/processor.md`, `launchers/cc-processor`, or any live `.work/` path — the
//! engine's lease path is developed in parallel with the running orchestrator.

use std::time::Duration;

use crate::supervise::{self, Reason, SpawnSpec, Verdict};

/// The lease role the engine owns its lease under — deliberately distinct from
/// `processor` so the engine never impersonates the processor in `lease.json`.
pub const ENGINE_ROLE: &str = "engine";

/// The PowerShell hosts to try, in order: PowerShell 7 (`pwsh`) first, then Windows
/// PowerShell 5.1 (`powershell`). `state-tx.ps1` is pure ASCII and runs under both.
pub const HOSTS: &[&str] = &["pwsh", "powershell"];

/// A generous wall-clock bound for one `state-tx.ps1` call: it must cover pwsh cold
/// start plus `state-tx`'s own short serialization lock (whose internal timeout is 30s),
/// yet still fail closed rather than hang forever.
pub const STATE_TX_DEADLINE: Duration = Duration::from_secs(120);

/// Engine-side exit codes for `engine lease` (a small, documented vocabulary translated
/// from `state-tx.ps1`'s richer set, so the outcome is legible without parsing prose).
pub mod exit {
    /// Success: lease taken / adopted / renewed / released (or not-held), or a status
    /// query that ran to completion.
    pub const OK: i32 = 0;
    /// Any other failure (spawn failure, timeout, `state-tx` lock busy, generation
    /// mismatch, unknown code) — the underlying detail is printed.
    pub const FAILED: i32 = 1;
    /// Usage / argument error (also `state-tx` exit 2).
    pub const USAGE: i32 = 2;
    /// Refused: a **live** lease is held by another owner (`state-tx` exit 10). Not a
    /// crash — the intended clean refusal when a live processor holds the lock.
    pub const HELD_LIVE: i32 = 3;
    /// Refused: not the lease owner (`state-tx` exit 13) — the engine will not renew or
    /// tear down a lease it does not own.
    pub const NOT_OWNER: i32 = 4;
    /// Refused: a non-structured (legacy/degraded `mkdir`) lock holds the directory
    /// (`state-tx` exit 19); resolving it is an operator decision.
    pub const LEGACY_LOCK: i32 = 5;
    /// Refused: the existing lease record is corrupt (`state-tx` exit 18); resolving it
    /// is an operator decision.
    pub const CORRUPT: i32 = 6;
}

/// The five lease operations the engine exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseOp {
    Acquire,
    Takeover,
    Heartbeat,
    Release,
    Status,
}

impl LeaseOp {
    /// Parse the subcommand token (`engine lease <op>`).
    pub fn from_arg(s: &str) -> Option<LeaseOp> {
        match s {
            "acquire" => Some(LeaseOp::Acquire),
            "takeover" => Some(LeaseOp::Takeover),
            "heartbeat" => Some(LeaseOp::Heartbeat),
            "release" => Some(LeaseOp::Release),
            "status" => Some(LeaseOp::Status),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            LeaseOp::Acquire => "acquire",
            LeaseOp::Takeover => "takeover",
            LeaseOp::Heartbeat => "heartbeat",
            LeaseOp::Release => "release",
            LeaseOp::Status => "status",
        }
    }
}

// ---------------------------------------------------------------------------------------
// argv builders — pure. They assemble the arguments passed to `state-tx.ps1` AFTER the
// script path (i.e. `<op> --work … --role engine …`). The engine role is pinned here and
// `--force` is never emitted by any builder.
// ---------------------------------------------------------------------------------------

/// `acquire --work <w> --root <r> --role engine [--ttl …] [--session …] [--pid …] [--owner …]`
pub fn acquire_argv(
    work: &str,
    root: &str,
    ttl: Option<&str>,
    session: Option<&str>,
    pid: Option<&str>,
    owner: Option<&str>,
) -> Vec<String> {
    let mut a = base_owner_argv("acquire", work, root, ttl, pid);
    if let Some(s) = session {
        a.push("--session".into());
        a.push(s.into());
    }
    if let Some(o) = owner {
        a.push("--owner".into());
        a.push(o.into());
    }
    a
}

/// `takeover --work <w> --root <r> --role engine [--ttl …] [--pid …]` — the liveness-gated
/// adoption of a provably-stale lease. No `--force` and no `--require-role`: idle
/// acquisition installs a fresh `engine` lease (recording `taken_over_from`), and
/// `state-tx takeover` still refuses a **live** lease (exit 10), which is the safety that
/// matters. Addressed `--require-role`/`--require-root` guards are for processor *resume*
/// (§16), not for taking a free/dead lock.
pub fn takeover_argv(work: &str, root: &str, ttl: Option<&str>, pid: Option<&str>) -> Vec<String> {
    base_owner_argv("takeover", work, root, ttl, pid)
}

fn base_owner_argv(
    verb: &str,
    work: &str,
    root: &str,
    ttl: Option<&str>,
    pid: Option<&str>,
) -> Vec<String> {
    let mut a = vec![
        verb.into(),
        "--work".into(),
        work.into(),
        "--root".into(),
        root.into(),
        "--role".into(),
        ENGINE_ROLE.into(),
    ];
    if let Some(t) = ttl {
        a.push("--ttl".into());
        a.push(t.into());
    }
    if let Some(p) = pid {
        a.push("--pid".into());
        a.push(p.into());
    }
    a
}

/// `heartbeat --work <w> --owner <id>` — only the owner may renew (`state-tx` exit 13
/// otherwise).
pub fn heartbeat_argv(work: &str, owner: &str) -> Vec<String> {
    vec![
        "heartbeat".into(),
        "--work".into(),
        work.into(),
        "--owner".into(),
        owner.into(),
    ]
}

/// `release --work <w> --owner <id>` — owner-checked release. The engine always presents
/// its own `owner_id` and never `--force`, so it can only ever remove **its own** lease.
pub fn release_argv(work: &str, owner: &str) -> Vec<String> {
    vec![
        "release".into(),
        "--work".into(),
        work.into(),
        "--owner".into(),
        owner.into(),
    ]
}

/// `status --work <w> [--json]` — read-only lease snapshot (live/stale, owner, role, TTL).
pub fn status_argv(work: &str, json: bool) -> Vec<String> {
    let mut a = vec!["status".into(), "--work".into(), work.into()];
    if json {
        a.push("--json".into());
    }
    a
}

// ---------------------------------------------------------------------------------------
// Exit-code classification — pure. Translate a `state-tx.ps1` exit code into the engine's
// vocabulary (see `exit`). `state-tx` codes: 0 ok · 2 usage · 3 gen-mismatch · 7 lock
// busy · 10 held-live · 11 stale-present · 12 no-lease-to-renew · 13 not-owner · 14
// no-lease(status) · 18 corrupt · 19 legacy-lock.
// ---------------------------------------------------------------------------------------

/// The classification of an `acquire` (or `takeover`) attempt. `Stale` only arises from a
/// bare `acquire` (`state-tx` exit 11) and drives escalation to the liveness-gated
/// `takeover`; it never appears as a final outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireVerdict {
    Acquired,
    Stale,
    HeldLive,
    Legacy,
    Corrupt,
    Usage,
    Failed,
}

/// Classify a bare `acquire` result. Exit 11 (a stale lease is present) maps to `Stale`,
/// signalling the caller to escalate to `takeover`.
pub fn acquire_verdict(state_tx_code: i32) -> AcquireVerdict {
    match state_tx_code {
        0 => AcquireVerdict::Acquired,
        11 => AcquireVerdict::Stale,
        10 => AcquireVerdict::HeldLive,
        19 => AcquireVerdict::Legacy,
        18 => AcquireVerdict::Corrupt,
        2 => AcquireVerdict::Usage,
        _ => AcquireVerdict::Failed,
    }
}

/// Classify a `takeover` result (used after an `acquire` reported a stale lease). A stale
/// lease that raced live between the two calls surfaces here as `HeldLive` (exit 10);
/// `takeover` never returns 11, so we treat that defensively as `Failed`.
pub fn takeover_verdict(state_tx_code: i32) -> AcquireVerdict {
    match acquire_verdict(state_tx_code) {
        AcquireVerdict::Stale => AcquireVerdict::Failed,
        other => other,
    }
}

/// The engine exit code for an acquire/takeover verdict.
pub fn acquire_exit(v: AcquireVerdict) -> i32 {
    match v {
        AcquireVerdict::Acquired => exit::OK,
        AcquireVerdict::HeldLive => exit::HELD_LIVE,
        AcquireVerdict::Legacy => exit::LEGACY_LOCK,
        AcquireVerdict::Corrupt => exit::CORRUPT,
        AcquireVerdict::Usage => exit::USAGE,
        AcquireVerdict::Stale | AcquireVerdict::Failed => exit::FAILED,
    }
}

/// Engine exit code for a `heartbeat` result. Exit 12 (no lease to renew) and 3
/// (generation mismatch) fold into the generic `FAILED` with the printed detail.
pub fn heartbeat_exit(state_tx_code: i32) -> i32 {
    match state_tx_code {
        0 => exit::OK,
        13 => exit::NOT_OWNER,
        2 => exit::USAGE,
        18 => exit::CORRUPT,
        19 => exit::LEGACY_LOCK,
        _ => exit::FAILED,
    }
}

/// Engine exit code for a `release` result. Exit 0 covers both `released` and the
/// idempotent `not-held`; exit 13 (not the owner — the late-cleanup-race guard, §15) maps
/// to `NOT_OWNER`, i.e. the engine will not tear down a lease it does not own.
pub fn release_exit(state_tx_code: i32) -> i32 {
    match state_tx_code {
        0 => exit::OK,
        13 => exit::NOT_OWNER,
        2 => exit::USAGE,
        18 => exit::CORRUPT,
        19 => exit::LEGACY_LOCK,
        _ => exit::FAILED,
    }
}

/// Extract the `owner_id` from a `state-tx` acquire/takeover line
/// (`"<verb> owner=<id> generation=<n> role=engine ttl=<t>"`). Returns the token after
/// the first `owner=`, up to the next whitespace. `None` if absent.
pub fn extract_owner(output: &str) -> Option<String> {
    let idx = output.find("owner=")? + "owner=".len();
    let rest = &output[idx..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    let id = &rest[..end];
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

// ---------------------------------------------------------------------------------------
// Spawn helper — the only impure piece here. It runs `state-tx.ps1` under the crate's
// supervisor (deadline + tree-kill + captured output) and returns the structured verdict.
// It tries the PowerShell hosts in order, skipping one that cannot even be spawned.
// ---------------------------------------------------------------------------------------

/// Run `state-tx.ps1` with `op_args` (the args after the script path) under supervision.
/// Returns the supervised [`Verdict`] of the first PowerShell host that spawned, or an
/// `Err` describing why no host could be launched.
pub fn run_state_tx(
    script: &str,
    op_args: &[String],
    deadline: Duration,
) -> Result<Verdict, String> {
    let mut last_err = String::new();
    for host in HOSTS {
        let mut args = vec![
            "-NoProfile".to_string(),
            "-ExecutionPolicy".to_string(),
            "Bypass".to_string(),
            "-File".to_string(),
            script.to_string(),
        ];
        args.extend(op_args.iter().cloned());
        let spec = SpawnSpec::new(*host, args).deadline(Some(deadline));
        let v = supervise::run(&spec);
        if v.reason == Reason::Crash && v.outcome_reason.starts_with("spawn failed") {
            last_err = format!("{host}: {}", v.outcome_reason);
            continue;
        }
        return Ok(v);
    }
    Err(format!(
        "no PowerShell host available (tried {}): {last_err}",
        HOSTS.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_parsing_round_trips() {
        for (s, op) in [
            ("acquire", LeaseOp::Acquire),
            ("takeover", LeaseOp::Takeover),
            ("heartbeat", LeaseOp::Heartbeat),
            ("release", LeaseOp::Release),
            ("status", LeaseOp::Status),
        ] {
            assert_eq!(LeaseOp::from_arg(s), Some(op));
            assert_eq!(op.as_str(), s);
        }
        assert_eq!(LeaseOp::from_arg(""), None);
    }

    #[test]
    fn acquire_argv_pins_engine_role_and_never_forces() {
        let a = acquire_argv("/w", "/r", Some("600"), Some("sess"), Some("42"), None);
        assert_eq!(a[0], "acquire");
        assert!(a.windows(2).any(|w| w[0] == "--work" && w[1] == "/w"));
        assert!(a.windows(2).any(|w| w[0] == "--root" && w[1] == "/r"));
        // The role is pinned to `engine`, never `processor`.
        assert!(a.windows(2).any(|w| w[0] == "--role" && w[1] == "engine"));
        assert!(!a.iter().any(|s| s == "processor"));
        assert!(a.windows(2).any(|w| w[0] == "--ttl" && w[1] == "600"));
        assert!(a.windows(2).any(|w| w[0] == "--session" && w[1] == "sess"));
        assert!(a.windows(2).any(|w| w[0] == "--pid" && w[1] == "42"));
        // The engine never forces its way past a live/legacy/corrupt lease.
        assert!(!a.iter().any(|s| s == "--force"));
    }

    #[test]
    fn acquire_argv_omits_absent_options() {
        let a = acquire_argv("/w", "/r", None, None, None, Some("own-1"));
        assert!(!a.iter().any(|s| s == "--ttl"));
        assert!(!a.iter().any(|s| s == "--session"));
        assert!(!a.iter().any(|s| s == "--pid"));
        assert!(a.windows(2).any(|w| w[0] == "--owner" && w[1] == "own-1"));
    }

    #[test]
    fn takeover_argv_is_role_engine_no_force_no_require() {
        let a = takeover_argv("/w", "/r", Some("900"), Some("7"));
        assert_eq!(a[0], "takeover");
        assert!(a.windows(2).any(|w| w[0] == "--role" && w[1] == "engine"));
        assert!(!a.iter().any(|s| s == "--force"));
        // Idle acquisition takes a free/dead lock; no addressed resume guards.
        assert!(!a.iter().any(|s| s == "--require-role"));
        assert!(!a.iter().any(|s| s == "--require-root"));
    }

    #[test]
    fn heartbeat_and_release_argv_are_owner_checked() {
        let h = heartbeat_argv("/w", "own-9");
        assert_eq!(h[0], "heartbeat");
        assert!(h.windows(2).any(|w| w[0] == "--owner" && w[1] == "own-9"));
        let r = release_argv("/w", "own-9");
        assert_eq!(r[0], "release");
        assert!(r.windows(2).any(|w| w[0] == "--owner" && w[1] == "own-9"));
        // The engine never releases by force; only the owner-checked path.
        assert!(!r.iter().any(|s| s == "--force"));
    }

    #[test]
    fn status_argv_toggles_json() {
        assert!(!status_argv("/w", false).iter().any(|s| s == "--json"));
        assert!(status_argv("/w", true).iter().any(|s| s == "--json"));
    }

    #[test]
    fn acquire_verdicts_map_state_tx_codes() {
        assert_eq!(acquire_verdict(0), AcquireVerdict::Acquired);
        assert_eq!(acquire_verdict(11), AcquireVerdict::Stale);
        assert_eq!(acquire_verdict(10), AcquireVerdict::HeldLive);
        assert_eq!(acquire_verdict(19), AcquireVerdict::Legacy);
        assert_eq!(acquire_verdict(18), AcquireVerdict::Corrupt);
        assert_eq!(acquire_verdict(2), AcquireVerdict::Usage);
        assert_eq!(acquire_verdict(7), AcquireVerdict::Failed);
        // takeover never re-reports "stale"; it becomes a defensive Failed.
        assert_eq!(takeover_verdict(11), AcquireVerdict::Failed);
        assert_eq!(takeover_verdict(10), AcquireVerdict::HeldLive);
        assert_eq!(takeover_verdict(0), AcquireVerdict::Acquired);
    }

    #[test]
    fn engine_exit_codes_are_stable() {
        assert_eq!(acquire_exit(AcquireVerdict::Acquired), exit::OK);
        assert_eq!(acquire_exit(AcquireVerdict::HeldLive), exit::HELD_LIVE);
        assert_eq!(acquire_exit(AcquireVerdict::Legacy), exit::LEGACY_LOCK);
        assert_eq!(acquire_exit(AcquireVerdict::Corrupt), exit::CORRUPT);
        assert_eq!(acquire_exit(AcquireVerdict::Usage), exit::USAGE);
        assert_eq!(acquire_exit(AcquireVerdict::Failed), exit::FAILED);

        assert_eq!(heartbeat_exit(0), exit::OK);
        assert_eq!(heartbeat_exit(13), exit::NOT_OWNER);
        assert_eq!(heartbeat_exit(12), exit::FAILED); // no lease to renew
        assert_eq!(heartbeat_exit(19), exit::LEGACY_LOCK);

        assert_eq!(release_exit(0), exit::OK); // released OR not-held
        assert_eq!(release_exit(13), exit::NOT_OWNER); // late-cleanup guard
        assert_eq!(release_exit(18), exit::CORRUPT);
    }

    #[test]
    fn owner_is_extracted_from_acquire_output() {
        let line = "acquire owner=ab12cd34 generation=1 role=engine ttl=900";
        assert_eq!(extract_owner(line).as_deref(), Some("ab12cd34"));
        let taken = "takeover owner=ff99 generation=4 role=engine ttl=900 taken_over_from=old";
        assert_eq!(extract_owner(taken).as_deref(), Some("ff99"));
        assert_eq!(
            extract_owner("heartbeat owner=zz1 generation=2").as_deref(),
            Some("zz1")
        );
        assert_eq!(extract_owner("no owner token here at all"), None);
        assert_eq!(extract_owner("owner="), None);
    }
}
