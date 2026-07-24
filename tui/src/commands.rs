//! The TUI's small, safe **command channel "downward"** to a running orchestrator (intent doc
//! §5 / §6.2). Everything else in this crate only *observes* `.work/`; this module is the sole
//! mutation boundary. Pause/force-lock mirror their established control files; approval decisions
//! delegate to `tools/policy.ps1`, never to direct Rust writes of approval artifacts.
//!
//! Six commands, each a faithful mirror of a mechanism that already exists:
//!
//! * **pause** — create `.work/PAUSE` (the kill switch checked at a phase/round boundary), the
//!   same file `launchers/cc-pause.sh` writes. A running processor only checks that the file
//!   *exists*; its body is purely informational (see `agents/processor.md`, "Пауза — kill switch
//!   `.work/PAUSE`").
//! * **resume** — remove `.work/PAUSE`, mirroring `launchers/cc-unpause.sh` (tolerant of an
//!   already-absent file — that is *not* an error).
//! * **lease-status** — read who owns `.work/orchestrator.lock` and whether the lease is live,
//!   strictly through the **existing owner-checked read path**: the engine crate's `lease` module
//!   (`orchestrail_engine::lease`, task T-107) which delegates to `tools/state-tx.ps1 status
//!   --json` and *nothing else* (KB K-003). We reuse its argv builder + supervised spawn rather
//!   than shelling out to a separate `engine` binary or re-implementing any `lease.json` reading.
//! * **force-lock** — the operator-only "processor is definitely dead, take the lock by hand"
//!   action, routed through the **single transactional path** `state-tx.ps1 release --force`
//!   (uniform legacy-lock / corrupt-lease / foreign-owner diagnostics, `docs/queue_contract.md`
//!   §14–§17) via the crate's existing supervised spawn (`lease::run_state_tx` + the
//!   checkout-vs-mirror `resolve_tool_script`, the same mechanism lease-status uses) — **not** a
//!   raw `fs::remove_dir_all`. `launchers/cc-processor.sh --force-lock` now takes the very same
//!   `state-tx release --force` path, so both operator front-ends share one audited mechanism
//!   instead of two independent raw removals. This is the one destructive command; the caller (see
//!   `main.rs` / `app::Modal`) guards it behind an explicit confirmation. We reuse the crate's
//!   supervised state-tx spawn but with the `release --force` verb the engine's own owner-checked
//!   `lease` helpers deliberately never emit — forcing over a live/legacy/corrupt lease is an
//!   operator decision, not something the engine does on its own (K-003).
//! * **approval-approve / approval-reject** — consume a pending human-gate request strictly via
//!   `tools/policy.ps1`, resolved through the same checkout-vs-cc-sync rule and launched through
//!   the same engine supervisor as `state-tx status`. Rust never writes `.work/approvals/`
//!   directly. The structured result distinguishes an applied approval/rejection from a request
//!   that another operator already consumed, an expired request, and an execution failure.
//!
//! **Testable core.** As with [`crate::app`], the decision/format/parse logic here is pure and
//! unit-tested without a terminal; the only impure pieces are the two thin filesystem writes
//! (`pause` / `resume`) and the supervised PowerShell spawns in [`query_lease_status`] /
//! [`force_lock`] / [`decide_approval`]. Callers never issue any command implicitly — a keystroke
//! in `main.rs` is what triggers one, and every destructive decision additionally requires the
//! confirmation gate.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use orchestrail_engine::{lease, time::epoch_to_iso};

/// The pause kill switch, relative to the `.work/` directory.
const PAUSE_FILE: &str = "PAUSE";
/// A policy decision is a tiny local PowerShell transaction; keep the same bounded supervision
/// window as the state-tx command channel.
const POLICY_DEADLINE: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------------------------
// pause / resume — direct `.work/PAUSE` writes mirroring cc-pause.sh / cc-unpause.sh
// ---------------------------------------------------------------------------------------------

/// The body written into `.work/PAUSE`, mirroring `launchers/cc-pause.sh`: a `paused_at=<ISO-8601
/// UTC>` line, plus the launcher's optional `reason=` line (here recording the TUI as the source,
/// so an operator inspecting the file later can tell a TUI-raised pause from a `cc-pause` one).
/// Pure so the format is unit-tested against a fixed timestamp; only the file *existence* matters
/// to the processor, so the exact body is informational.
pub fn pause_body(now_iso8601: &str) -> String {
    format!("paused_at={now_iso8601}\nreason=paused from orchestrail-tui\n")
}

/// **pause**: create (or overwrite) `<work>/PAUSE` with [`pause_body`]. Mirrors `cc-pause.sh`'s
/// direct file write — no external process is spawned. Returns the path written.
pub fn pause(work_dir: &Path, now_iso8601: &str) -> io::Result<PathBuf> {
    let path = work_dir.join(PAUSE_FILE);
    fs::write(&path, pause_body(now_iso8601))?;
    Ok(path)
}

/// **resume**: remove `<work>/PAUSE`, mirroring `cc-unpause.sh`. Tolerant of an already-absent
/// file (that is *not* an error — nothing to clear). Returns `true` if a file was actually
/// removed, `false` if there was nothing to clear.
pub fn resume(work_dir: &Path) -> io::Result<bool> {
    let path = work_dir.join(PAUSE_FILE);
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------------------------
// force-lock — the operator force-takeover, via the single transactional `state-tx release
// --force` path (the same supervised spawn as lease-status), not a raw directory removal.
// ---------------------------------------------------------------------------------------------

/// The outcome of the operator-only force-lock, run through the single transactional path
/// `state-tx.ps1 release --force`. It mirrors the shapes `state-tx release --force` emits so the
/// diagnostics (a removed lock, an already-free lock, or a failure) are identical whether
/// force-lock is invoked from the TUI or from `launchers/cc-processor.sh --force-lock`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForceLockOutcome {
    /// The lock was force-released (`state-tx` printed `released`): the directory — a live/stale
    /// lease, a legacy `mkdir` lock, or a corrupt record — is now gone.
    Removed,
    /// There was no lock to remove (`state-tx` printed `not-held`) — idempotent absence.
    Absent,
    /// `state-tx.ps1` could not be run at all (no PowerShell host / script not found / spawn or
    /// timeout failure). The lock was **not** touched.
    Unavailable(String),
    /// `state-tx` ran but reported a failure (e.g. its serialization lock was busy). Detail verbatim.
    Failed(String),
}

impl ForceLockOutcome {
    /// A one-line human summary for the footer.
    pub fn summary(&self) -> String {
        match self {
            ForceLockOutcome::Removed => {
                "force-lock: .work/orchestrator.lock снят (state-tx release --force)".to_string()
            }
            ForceLockOutcome::Absent => {
                "force-lock: замка не было — .work/orchestrator.lock отсутствует (state-tx: not-held)"
                    .to_string()
            }
            ForceLockOutcome::Unavailable(why) => format!("force-lock недоступен: {why}"),
            ForceLockOutcome::Failed(detail) => format!("force-lock не удался: {detail}"),
        }
    }
}

/// Build the arguments after `state-tx.ps1` for the operator force-release: `release --work <dir>
/// --force`. `--force` is what lets this single transactional path tear down a live/stale/legacy/
/// corrupt or foreign lease (an operator decision); with `--force`, `state-tx release` needs no
/// `--owner` (see `tools/state-tx.ps1` `Cmd-Release`).
pub fn force_release_argv(work_dir: &Path) -> Vec<String> {
    vec![
        "release".to_string(),
        "--work".to_string(),
        work_dir.to_string_lossy().into_owned(),
        "--force".to_string(),
    ]
}

/// Pure classification of a `state-tx.ps1 release --force` invocation into a [`ForceLockOutcome`].
/// On success (`exit 0`) `state-tx` prints `not-held` when there was nothing to remove and
/// `released` otherwise; a missing exit code (supervision could not complete) or a non-zero exit is
/// surfaced with its detail rather than silently reported as a removal.
pub fn classify_force_lock(
    exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
    supervision_reason: &str,
) -> ForceLockOutcome {
    if exit_code.is_none() {
        return ForceLockOutcome::Unavailable(format!(
            "state-tx не завершился: {supervision_reason}"
        ));
    }
    if exit_code == Some(0) {
        if stdout.lines().any(|l| l.trim() == "not-held") {
            return ForceLockOutcome::Absent;
        }
        // `released` — the only other exit-0 output of `release --force`.
        return ForceLockOutcome::Removed;
    }
    let detail = stderr
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .or_else(|| stdout.lines().map(str::trim).find(|l| !l.is_empty()))
        .unwrap_or(supervision_reason)
        .to_string();
    ForceLockOutcome::Failed(if detail.is_empty() {
        format!("state-tx завершился с кодом {exit_code:?}")
    } else {
        detail
    })
}

/// **force-lock** (impure): the operator's force-takeover of `<work>/orchestrator.lock`, run
/// through the single transactional path `state-tx.ps1 release --force` via the crate's existing
/// supervised spawn ([`lease::run_state_tx`]) and the checkout-vs-mirror script resolver — the same
/// mechanism as [`query_lease_status`], not a raw `fs::remove_dir_all`. So the TUI and
/// `launchers/cc-processor.sh --force-lock` share one audited, diagnosable path. **Destructive**:
/// the caller must have obtained explicit operator confirmation first (see `app::Modal` /
/// `main.rs`); this function does not itself gate on that. Any inability to run the transaction is
/// returned as [`ForceLockOutcome::Unavailable`] (the lock is left untouched), never a panic.
pub fn force_lock(work_dir: &Path) -> ForceLockOutcome {
    let script = match resolve_state_tx_script(work_dir) {
        Some(s) => s,
        None => {
            return ForceLockOutcome::Unavailable(
                "state-tx.ps1 не найден (ни в <root>/tools, ни в зеркале ~/.claude/scripts)"
                    .to_string(),
            );
        }
    };
    let argv = force_release_argv(work_dir);
    match lease::run_state_tx(&script.to_string_lossy(), &argv, lease::STATE_TX_DEADLINE) {
        Err(e) => ForceLockOutcome::Unavailable(e),
        Ok(v) => classify_force_lock(v.exit_code, &v.stdout, &v.stderr, &v.outcome_reason),
    }
}

// ---------------------------------------------------------------------------------------------
// lease-status — via the engine crate's owner-checked `tools/state-tx.ps1 status --json` path
// ---------------------------------------------------------------------------------------------

/// A structured, valid lease read from `.work/orchestrator.lock/lease.json` (via `state-tx.ps1
/// status --json`). Fields are `None` when the underlying JSON omits them, never invented.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeasePresent {
    pub role: Option<String>,
    pub owner_id: Option<String>,
    pub host: Option<String>,
    pub pid: Option<i64>,
    /// Whether the holder is provably live (pid / heartbeat-vs-TTL, decided by `state-tx.ps1`).
    pub live: bool,
    pub heartbeat_age_secs: Option<i64>,
    pub ttl_seconds: Option<i64>,
    pub generation: Option<i64>,
    /// The human liveness reason string from `state-tx.ps1` (e.g. "pid 1234 alive …").
    pub reason: Option<String>,
}

/// The outcome of a lease-status query — one of the `state-tx.ps1 status` shapes, or an
/// unavailable query (no PowerShell host / script not found / spawn or timeout failure). Read-only
/// throughout: nothing here mutates the lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    /// The query itself could not run (e.g. `state-tx.ps1`/pwsh missing, spawn failure, timeout).
    Unavailable(String),
    /// No lease at all — `.work/orchestrator.lock` is free (`{present:false}`, state-tx exit 14).
    Absent,
    /// A lock directory exists but is not a structured lease we can read: a legacy `mkdir` lock or
    /// a corrupt `lease.json` (state-tx exit 19 / 18). The string carries the detail.
    Degraded(String),
    /// A structured, valid lease was read.
    Present(LeasePresent),
}

impl LeaseStatus {
    /// A one-line human summary for the overlay/footer.
    pub fn summary(&self) -> String {
        match self {
            LeaseStatus::Unavailable(why) => format!("аренда: запрос недоступен ({why})"),
            LeaseStatus::Absent => {
                "аренда: свободна — .work/orchestrator.lock отсутствует".to_string()
            }
            LeaseStatus::Degraded(detail) => format!("аренда: нечитаемый замок ({detail})"),
            LeaseStatus::Present(l) => {
                let who = l.owner_id.as_deref().unwrap_or("?");
                let role = l.role.as_deref().unwrap_or("?");
                let liveness = if l.live {
                    "жива"
                } else {
                    "устарела"
                };
                format!("аренда: {liveness} · владелец {who} · роль {role}")
            }
        }
    }
}

/// Pure parse of the `state-tx.ps1 status --json` stdout into a [`LeaseStatus`]. The JSON is
/// self-describing (`present` / `valid` / `legacy`), so the classification needs no exit code.
/// Empty or unparseable output degrades to [`LeaseStatus::Unavailable`] rather than panicking —
/// the same "total loading" leniency as the rest of this observer.
pub fn parse_lease_status(stdout: &str) -> LeaseStatus {
    let text = stdout.trim();
    if text.is_empty() {
        return LeaseStatus::Unavailable("пустой ответ state-tx".to_string());
    }
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => return LeaseStatus::Unavailable(format!("нераспознанный JSON: {e}")),
    };
    let present = v.get("present").and_then(|x| x.as_bool()).unwrap_or(false);
    if !present {
        return LeaseStatus::Absent;
    }
    let valid = v.get("valid").and_then(|x| x.as_bool()).unwrap_or(false);
    if !valid {
        let err = v
            .get("error")
            .and_then(|x| x.as_str())
            .unwrap_or("нет деталей");
        let legacy = v.get("legacy").and_then(|x| x.as_bool()).unwrap_or(false);
        let kind = if legacy {
            "legacy-lock"
        } else {
            "corrupt-lease"
        };
        return LeaseStatus::Degraded(format!("{kind}: {err}"));
    }
    LeaseStatus::Present(LeasePresent {
        role: json_str(&v, "role"),
        owner_id: json_str(&v, "owner_id"),
        host: json_str(&v, "host"),
        pid: json_i64(&v, "pid"),
        live: v.get("live").and_then(|x| x.as_bool()).unwrap_or(false),
        heartbeat_age_secs: json_i64(&v, "heartbeat_age_secs"),
        ttl_seconds: json_i64(&v, "ttl_seconds"),
        generation: json_i64(&v, "generation"),
        reason: json_str(&v, "reason"),
    })
}

fn json_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(str::to_string)
}

fn json_i64(v: &serde_json::Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64())
}

/// Resolve the `state-tx.ps1` runner for the observed project, following the single "checkout vs
/// mirror" rule (see `resolve_tool_script` / `knowledge.md`, "Резолвинг раннеров `tools/*.ps1`"):
/// the project's own `<root>/tools/state-tx.ps1` (root = the `.work/` parent) is trusted ONLY when
/// `root` carries all three checkout identity markers; otherwise the resolver uses the cc-sync
/// mirror at `~/.claude/scripts/state-tx.ps1`. `None` if neither a trusted checkout copy nor a
/// mirror copy exists — the caller surfaces that as [`LeaseStatus::Unavailable`] instead of
/// guessing (or executing a foreign target-local file).
pub fn resolve_state_tx_script(work_dir: &Path) -> Option<PathBuf> {
    resolve_tool_script(work_dir, "state-tx.ps1")
}

/// **lease-status** (impure): run `state-tx.ps1 status --work <dir> --json` through the engine
/// crate's owner-checked lease helpers ([`lease::status_argv`] + [`lease::run_state_tx`], the same
/// supervised spawn `engine lease status` uses) and classify the result. Read-only: `status` never
/// mutates the lock. Any inability to run the query is returned as [`LeaseStatus::Unavailable`],
/// never a panic.
pub fn query_lease_status(work_dir: &Path) -> LeaseStatus {
    let script = match resolve_state_tx_script(work_dir) {
        Some(s) => s,
        None => {
            return LeaseStatus::Unavailable(
                "state-tx.ps1 не найден (ни в <root>/tools, ни в зеркале ~/.claude/scripts)"
                    .to_string(),
            );
        }
    };
    let work_s = work_dir.to_string_lossy().into_owned();
    let script_s = script.to_string_lossy().into_owned();
    let argv = lease::status_argv(&work_s, true);
    match lease::run_state_tx(&script_s, &argv, lease::STATE_TX_DEADLINE) {
        Err(e) => LeaseStatus::Unavailable(e),
        Ok(v) => {
            if v.exit_code.is_none() {
                return LeaseStatus::Unavailable(format!(
                    "state-tx не завершился: {} — {}",
                    v.reason.as_str(),
                    v.outcome_reason
                ));
            }
            let out = v.stdout.trim();
            if out.is_empty() {
                let why = v
                    .stderr
                    .lines()
                    .map(str::trim)
                    .find(|l| !l.is_empty())
                    .unwrap_or("нет вывода");
                return LeaseStatus::Unavailable(why.to_string());
            }
            parse_lease_status(out)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// approval decisions — supervised tools/policy.ps1, never direct approval-artifact writes
// ---------------------------------------------------------------------------------------------

/// The irreversible operator decision requested by a pending approval card.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    Approve,
    Reject,
}

impl ApprovalDecision {
    fn command(self) -> &'static str {
        match self {
            ApprovalDecision::Approve => "approval-approve",
            ApprovalDecision::Reject => "approval-reject",
        }
    }

    fn policy_value(self) -> &'static str {
        match self {
            ApprovalDecision::Approve => "approve",
            ApprovalDecision::Reject => "reject",
        }
    }
}

/// Structured command outcome. `Rejected` is a successfully applied operator rejection even
/// though `policy.ps1` deliberately exits 11 after persisting it to keep downstream gates closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalOutcome {
    Approved,
    Rejected,
    AlreadyConsumed,
    Expired,
    Failed,
}

/// Result shown by the Decision Inbox immediately after a supervised policy transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalCommandResult {
    pub id: String,
    pub outcome: ApprovalOutcome,
    pub reason: String,
}

impl ApprovalCommandResult {
    pub fn summary(&self) -> String {
        match self.outcome {
            ApprovalOutcome::Approved => format!("approval {} одобрен и потреблён", self.id),
            ApprovalOutcome::Rejected => format!("approval {} отклонён и потреблён", self.id),
            ApprovalOutcome::AlreadyConsumed => {
                format!("approval {} уже потреблён: {}", self.id, self.reason)
            }
            ApprovalOutcome::Expired => {
                format!("approval {} просрочен: {}", self.id, self.reason)
            }
            ApprovalOutcome::Failed => {
                format!(
                    "решение по approval {} не применено: {}",
                    self.id, self.reason
                )
            }
        }
    }
}

/// Resolve `policy.ps1` through the same checkout-vs-cc-sync mirror rule as `state-tx.ps1`.
pub fn resolve_policy_script(work_dir: &Path) -> Option<PathBuf> {
    resolve_tool_script(work_dir, "policy.ps1")
}

/// Resolve a `tools/<name>.ps1` runner for the observed project, delegating to the SINGLE shared
/// checkout-vs-mirror resolver ([`orchestrail_engine::toolscript::resolve_tool_script`]) so the
/// identity-marker rule (`docs/queue_contract.md` §9, `knowledge.md` "Резолвинг раннеров
/// `tools/*.ps1`") is enforced in one place across the TUI and the engine's `lease` command. The
/// project root is the `.work/` parent: a checkout-local `<root>/tools/<name>` is trusted ONLY when
/// `root` carries all three checkout identity markers; otherwise the resolver goes straight to the
/// cc-sync mirror `~/.claude/scripts/<name>`, never executing a foreign/stale target-local file.
fn resolve_tool_script(work_dir: &Path, name: &str) -> Option<PathBuf> {
    // `.work/` always has a parent in practice; if it somehow doesn't, pass the dir itself — it
    // won't satisfy the checkout markers, so resolution falls to the mirror exactly as before.
    let root = work_dir.parent().unwrap_or(work_dir);
    orchestrail_engine::toolscript::resolve_tool_script(root, name)
}

/// Build the arguments after `policy.ps1`. Values stay distinct argv elements, including an
/// arbitrary rejection explanation; no shell command string is ever concatenated.
pub fn approval_decision_argv(
    work_dir: &Path,
    id: &str,
    decision: ApprovalDecision,
    rejection_reason: Option<&str>,
) -> Vec<String> {
    let mut argv = vec![
        decision.command().to_string(),
        "--work".to_string(),
        work_dir.to_string_lossy().into_owned(),
        "--id".to_string(),
        id.to_string(),
        "--by".to_string(),
        "orchestrail-tui".to_string(),
    ];
    // policy.ps1 stores the operator's reject explanation in the approval artifact's `note`
    // field. The UI calls it a reason; mapping it here keeps the persisted policy schema intact.
    if decision == ApprovalDecision::Reject
        && let Some(reason) = rejection_reason.filter(|s| !s.trim().is_empty())
    {
        argv.push("--note".to_string());
        argv.push(reason.trim().to_string());
    }
    argv.push("--json".to_string());
    argv
}

/// Parse one supervised policy invocation. A freshly persisted rejection is identified from the
/// JSON output before inspecting the non-zero exit code; all other non-zero outcomes are refusals.
pub fn parse_approval_result(
    id: &str,
    requested: ApprovalDecision,
    exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
    supervision_reason: &str,
) -> ApprovalCommandResult {
    let json = serde_json::from_str::<serde_json::Value>(stdout.trim()).ok();
    let applied = json.as_ref().map(|v| {
        v.get("id").and_then(|x| x.as_str()) == Some(id)
            && v.get("decision").and_then(|x| x.as_str()) == Some(requested.policy_value())
            && v.get("state")
                .and_then(|x| x.as_str())
                .map(|s| s == format!("decided-{}", requested.policy_value()))
                .unwrap_or(false)
    });
    if applied == Some(true) && (requested == ApprovalDecision::Reject || exit_code == Some(0)) {
        return ApprovalCommandResult {
            id: id.to_string(),
            outcome: match requested {
                ApprovalDecision::Approve => ApprovalOutcome::Approved,
                ApprovalDecision::Reject => ApprovalOutcome::Rejected,
            },
            reason: String::new(),
        };
    }

    let detail = stderr
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .or_else(|| stdout.lines().map(str::trim).find(|line| !line.is_empty()))
        .unwrap_or(supervision_reason)
        .to_string();
    let lower = detail.to_ascii_lowercase();
    let outcome = if lower.contains("already") && lower.contains("one-time") {
        ApprovalOutcome::AlreadyConsumed
    } else if lower.contains("approval id")
        && (lower.contains("expired at") || lower.contains("by the deadline"))
    {
        ApprovalOutcome::Expired
    } else {
        ApprovalOutcome::Failed
    };
    ApprovalCommandResult {
        id: id.to_string(),
        outcome,
        reason: if detail.is_empty() {
            format!("policy.ps1 завершился с кодом {:?}", exit_code)
        } else {
            detail
        },
    }
}

/// Apply approve/reject through the supervised PowerShell command channel. This function never
/// opens or writes an approval JSON file itself.
pub fn decide_approval(
    work_dir: &Path,
    id: &str,
    decision: ApprovalDecision,
    rejection_reason: Option<&str>,
) -> ApprovalCommandResult {
    let script = match resolve_policy_script(work_dir) {
        Some(path) => path,
        None => {
            return ApprovalCommandResult {
                id: id.to_string(),
                outcome: ApprovalOutcome::Failed,
                reason: "policy.ps1 не найден (ни в <root>/tools, ни в зеркале ~/.claude/scripts)"
                    .to_string(),
            };
        }
    };
    let argv = approval_decision_argv(work_dir, id, decision, rejection_reason);
    match lease::run_state_tx(&script.to_string_lossy(), &argv, POLICY_DEADLINE) {
        Ok(verdict) => parse_approval_result(
            id,
            decision,
            verdict.exit_code,
            &verdict.stdout,
            &verdict.stderr,
            &verdict.outcome_reason,
        ),
        Err(error) => ApprovalCommandResult {
            id: id.to_string(),
            outcome: ApprovalOutcome::Failed,
            reason: error,
        },
    }
}

// ---------------------------------------------------------------------------------------------// ISO-8601 UTC timestamp — reuse the engine's dependency-free civil-from-days converter.
// ---------------------------------------------------------------------------------------------

/// The current instant as `YYYY-MM-DDTHH:MM:SSZ` (the same shape `cc-pause.sh`'s `date -u` emits).
/// Falls back to the Unix epoch string if the clock is somehow before 1970 (it never is).
pub fn now_iso8601() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    epoch_to_iso(secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TmpWork {
        dir: PathBuf,
    }
    impl TmpWork {
        fn new() -> TmpWork {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "orchestrail-tui-cmd-{}-{nanos}-{n}",
                std::process::id()
            ));
            fs::create_dir_all(&dir).unwrap();
            TmpWork { dir }
        }
    }
    impl Drop for TmpWork {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn pause_body_mirrors_cc_pause_format() {
        let body = pause_body("2026-07-12T01:09:08Z");
        // cc-pause.sh writes `paused_at=<date -u>` first…
        assert!(body.starts_with("paused_at=2026-07-12T01:09:08Z\n"));
        // …plus the optional `reason=` line the launcher supports.
        assert!(body.contains("reason=paused from orchestrail-tui"));
        // trailing newline, like the shell heredoc.
        assert!(body.ends_with('\n'));
    }

    #[test]
    fn pause_creates_the_kill_switch() {
        let w = TmpWork::new();
        assert!(!w.dir.join("PAUSE").exists());
        let p = pause(&w.dir, "2026-07-12T01:09:08Z").unwrap();
        assert_eq!(p, w.dir.join("PAUSE"));
        let body = fs::read_to_string(&p).unwrap();
        assert!(body.contains("paused_at=2026-07-12T01:09:08Z"));
    }

    #[test]
    fn resume_removes_pause_and_tolerates_absence() {
        let w = TmpWork::new();
        // Nothing to clear yet -> Ok(false), not an error (mirrors cc-unpause.sh).
        assert!(!resume(&w.dir).unwrap(), "nothing to clear yet");
        pause(&w.dir, "2026-07-12T01:09:08Z").unwrap();
        assert!(w.dir.join("PAUSE").exists());
        // Now there is something to clear -> Ok(true).
        assert!(resume(&w.dir).unwrap(), "PAUSE was present and removed");
        assert!(!w.dir.join("PAUSE").exists());
        // Second resume is again a harmless no-op.
        assert!(!resume(&w.dir).unwrap(), "already cleared");
    }

    #[test]
    fn force_release_argv_is_release_force_no_owner() {
        // force-lock now routes through `state-tx.ps1 release --force`, not a raw removal.
        let argv = force_release_argv(Path::new("/repo/.work"));
        assert_eq!(argv[0], "release");
        assert!(argv.windows(2).any(|w| w == ["--work", "/repo/.work"]));
        // `--force` is what makes this the operator-only single transactional path.
        assert!(argv.iter().any(|s| s == "--force"));
        // With `--force`, `state-tx release` needs no `--owner` (it can tear down a foreign lease).
        assert!(!argv.iter().any(|s| s == "--owner"));
    }

    #[test]
    fn classify_force_lock_maps_released_absent_and_failure() {
        // exit 0 + "released" -> the lock (live/legacy/corrupt/foreign) was removed.
        assert_eq!(
            classify_force_lock(Some(0), "released\n", "", "ok"),
            ForceLockOutcome::Removed
        );
        // exit 0 + "not-held" -> nothing to remove (idempotent absence).
        assert_eq!(
            classify_force_lock(Some(0), "not-held\n", "", "ok"),
            ForceLockOutcome::Absent
        );
        // A non-zero exit (e.g. state-tx.lock busy) surfaces the detail, not a false "removed".
        match classify_force_lock(Some(7), "", "state-tx: lock busy", "error") {
            ForceLockOutcome::Failed(d) => assert!(d.contains("lock busy")),
            other => panic!("expected Failed, got {other:?}"),
        }
        // No exit code (supervision could not complete) -> Unavailable, lock left untouched.
        match classify_force_lock(None, "", "", "supervisor deadline exceeded") {
            ForceLockOutcome::Unavailable(d) => assert!(d.contains("deadline")),
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    #[test]
    fn force_lock_summaries_are_human_readable() {
        assert!(ForceLockOutcome::Removed.summary().contains("снят"));
        assert!(ForceLockOutcome::Absent.summary().contains("отсутствует"));
        assert!(
            ForceLockOutcome::Unavailable("pwsh?".into())
                .summary()
                .contains("недоступен")
        );
        assert!(
            ForceLockOutcome::Failed("busy".into())
                .summary()
                .contains("не удался")
        );
    }

    #[test]
    fn parse_lease_status_reads_a_live_lease() {
        // The exact shape state-tx.ps1 `status --json` emits for a valid lease.
        let json = r#"{"present":true,"valid":true,"role":"processor","owner_id":"ab12cd34","session_id":"s1","root":"/p","host":"HOSTA","pid":4321,"acquired":"2026-07-12T00:00:00Z","heartbeat":"2026-07-12T01:00:00Z","ttl_seconds":900,"generation":3,"live":true,"liveness_basis":"pid","heartbeat_age_secs":12,"reason":"pid 4321 alive (start-time matches)"}"#;
        match parse_lease_status(json) {
            LeaseStatus::Present(l) => {
                assert_eq!(l.owner_id.as_deref(), Some("ab12cd34"));
                assert_eq!(l.role.as_deref(), Some("processor"));
                assert_eq!(l.host.as_deref(), Some("HOSTA"));
                assert_eq!(l.pid, Some(4321));
                assert!(l.live);
                assert_eq!(l.heartbeat_age_secs, Some(12));
                assert_eq!(l.ttl_seconds, Some(900));
                assert_eq!(l.generation, Some(3));
                assert!(l.reason.as_deref().unwrap().contains("alive"));
            }
            other => panic!("expected Present, got {other:?}"),
        }
    }

    #[test]
    fn parse_lease_status_reads_a_stale_lease() {
        let json = r#"{"present":true,"valid":true,"role":"engine","owner_id":"ff99","host":"HOSTB","pid":7,"ttl_seconds":900,"generation":1,"live":false,"heartbeat_age_secs":4000,"reason":"heartbeat 4000s old >= ttl 900s (expired)"}"#;
        match parse_lease_status(json) {
            LeaseStatus::Present(l) => {
                assert!(!l.live, "stale lease must report live=false");
                assert_eq!(l.owner_id.as_deref(), Some("ff99"));
                assert!(l.reason.as_deref().unwrap().contains("expired"));
            }
            other => panic!("expected Present, got {other:?}"),
        }
    }

    #[test]
    fn parse_lease_status_maps_absent_legacy_and_corrupt() {
        // No lease at all (state-tx exit 14).
        assert_eq!(
            parse_lease_status(r#"{"present":false}"#),
            LeaseStatus::Absent
        );
        // Legacy mkdir lock (exit 19).
        match parse_lease_status(
            r#"{"present":true,"valid":false,"legacy":true,"error":"no lease.json in lock dir"}"#,
        ) {
            LeaseStatus::Degraded(d) => {
                assert!(d.starts_with("legacy-lock"));
                assert!(d.contains("no lease.json"));
            }
            other => panic!("expected Degraded(legacy), got {other:?}"),
        }
        // Corrupt lease.json (exit 18).
        match parse_lease_status(
            r#"{"present":true,"valid":false,"error":"unparseable heartbeat timestamp"}"#,
        ) {
            LeaseStatus::Degraded(d) => assert!(d.starts_with("corrupt-lease")),
            other => panic!("expected Degraded(corrupt), got {other:?}"),
        }
    }

    #[test]
    fn parse_lease_status_degrades_on_garbage() {
        assert!(matches!(
            parse_lease_status(""),
            LeaseStatus::Unavailable(_)
        ));
        assert!(matches!(
            parse_lease_status("not json at all"),
            LeaseStatus::Unavailable(_)
        ));
    }

    #[test]
    fn lease_summary_is_human_readable() {
        assert!(LeaseStatus::Absent.summary().contains("свободна"));
        assert!(
            LeaseStatus::Unavailable("pwsh?".into())
                .summary()
                .contains("недоступен")
        );
    }

    #[test]
    fn approval_argv_keeps_reject_reason_as_one_argument() {
        let argv = approval_decision_argv(
            Path::new("/repo/.work"),
            "apr-123",
            ApprovalDecision::Reject,
            Some("неверный scope; $(do-not-run)"),
        );
        assert_eq!(argv[0], "approval-reject");
        assert!(argv.windows(2).any(|w| w == ["--id", "apr-123"]));
        assert!(
            argv.windows(2)
                .any(|w| w == ["--note", "неверный scope; $(do-not-run)"])
        );
        assert_eq!(argv.last().map(String::as_str), Some("--json"));
    }

    #[test]
    fn parses_approve_reject_consumed_and_expired_results() {
        let approved = parse_approval_result(
            "apr-a",
            ApprovalDecision::Approve,
            Some(0),
            r#"{"state":"decided-approve","id":"apr-a","decision":"approve"}"#,
            "",
            "ok",
        );
        assert_eq!(approved.outcome, ApprovalOutcome::Approved);

        // policy.ps1 persists rejection, emits JSON, then exits 11 intentionally.
        let rejected = parse_approval_result(
            "apr-r",
            ApprovalDecision::Reject,
            Some(11),
            r#"{"state":"decided-reject","id":"apr-r","decision":"reject"}"#,
            "policy: approval id 'apr-r' rejected by 'orchestrail-tui'",
            "error",
        );
        assert_eq!(rejected.outcome, ApprovalOutcome::Rejected);

        let consumed = parse_approval_result(
            "apr-a",
            ApprovalDecision::Reject,
            Some(11),
            "",
            "policy: approval id 'apr-a' is already approve; a one-time id cannot be decided twice",
            "error",
        );
        assert_eq!(consumed.outcome, ApprovalOutcome::AlreadyConsumed);

        let expired = parse_approval_result(
            "apr-x",
            ApprovalDecision::Approve,
            Some(11),
            "",
            "policy: approval id 'apr-x' expired at 2026-07-16T00:00:00Z",
            "error",
        );
        assert_eq!(expired.outcome, ApprovalOutcome::Expired);

        let supervisor_timeout = parse_approval_result(
            "apr-x",
            ApprovalDecision::Approve,
            None,
            "",
            "",
            "supervisor deadline exceeded",
        );
        assert_eq!(supervisor_timeout.outcome, ApprovalOutcome::Failed);
    }
    #[test]
    fn now_iso8601_has_the_expected_shape() {
        let s = now_iso8601();
        assert_eq!(s.len(), 20, "YYYY-MM-DDTHH:MM:SSZ is 20 chars: {s}");
        assert!(s.ends_with('Z'));
        assert_eq!(&s[4..5], "-");
        assert_eq!(&s[10..11], "T");
    }

    #[test]
    fn resolve_tool_script_honours_checkout_identity_markers() {
        // Build <root>/.work and a target-local <root>/tools/state-tx.ps1. Without the three
        // checkout identity markers, the resolver must NOT hand back that target-local path — it
        // may fall to the cc-sync mirror or None, but never the untrusted local file (the security
        // property from `docs/queue_contract.md` §9). Once all three markers are present, it
        // returns the target-local path as before (no regression for a real orchestra checkout).
        // The shared decision itself lives in and is exhaustively unit-tested by
        // `orchestrail_engine::toolscript`; this test locks the TUI's use of it end to end.
        let root = TmpWork::new();
        let work = root.dir.join(".work");
        fs::create_dir_all(&work).unwrap();
        let tools = root.dir.join("tools");
        fs::create_dir_all(&tools).unwrap();
        let target_local = tools.join("state-tx.ps1");
        fs::write(&target_local, "exit 0\n").unwrap();

        // No identity markers -> the target-local script is untrusted and never returned.
        assert_ne!(
            resolve_state_tx_script(&work),
            Some(target_local.clone()),
            "target-local tools/state-tx.ps1 must not be trusted without checkout markers"
        );

        // Add all three checkout identity markers -> proven checkout, target-local path returned.
        fs::create_dir_all(root.dir.join("agents")).unwrap();
        fs::write(root.dir.join("agents").join("processor.md"), "x").unwrap();
        fs::write(root.dir.join("generate-codex-agents.ps1"), "x").unwrap();
        fs::write(tools.join("sync-runtime.ps1"), "x").unwrap();

        assert_eq!(
            resolve_state_tx_script(&work),
            Some(target_local),
            "a proven orchestra checkout still resolves its own tools/state-tx.ps1"
        );
    }
}
