//! Aggregate the Decision Inbox (plan §6.2) — the main human-in-the-loop screen — from a
//! read-only `engine::state::Snapshot` (T-103) plus the out-of-band `.work/PAUSE` marker.
//!
//! Like [`crate::app`], this module carries no terminal / ratatui dependency and never writes a
//! file, takes a lock, or emits an event. `build` projects the decoded `Snapshot`; `load_approvals`
//! adds a best-effort read-only projection of persistent human-gate JSON artifacts.
//!
//! Four categories are surfaced:
//!
//! * **Approvals** — undecided one-time requests from `.work/approvals/*.json`; unexpired requests
//!   are actionable, expired requests and malformed artifacts stay visible but are not actionable.
//!
//! * **Escalated** — queue entries whose status is the terminal `эскалирована · причина=…`
//!   (§13.1): these can never proceed without an operator decision.
//! * **Quarantined** — queue entries re-queued after quarantine (`не начата · попытка=N ·
//!   карантин=…`, §13.1 §3): still `not-started` and will be picked up automatically, but the
//!   repeated failure is worth an operator's eyes.
//! * **Blocked** — plain `не начата` queue entries whose `Предпосылки:` list contains a T-ID that
//!   is not yet resolved, resolved the same way `tools/queue-tx.ps1 ready` does: a predecessor is
//!   "done" if it is `выполнена`/archived, "infeasible" if it is itself `эскалирована`, else still
//!   in flight. A predecessor absent from both `snapshot.queue` and `snapshot.descriptors` is
//!   cross-checked against the caller-supplied `done_ids` (task ids already archived to
//!   `Tasks_Done.md`, decoded by `main.rs::done_task_ids`): confirmed there → resolved; still
//!   absent → surfaced explicitly as `blocking_unknown` rather than silently assumed done, so a
//!   typo'd/stale `Предпосылки:` entry that slipped past `queue-tx.ps1 validate-deps` does not
//!   hide a genuinely blocked task from the operator (R-2).
//!
//! Nothing here invents facts absent from the source Markdown: card fields are `None`/empty when
//! the corresponding suffix (`причина=`, `попытка=`, …) is itself absent from the artifact.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use orchestrail_engine::state::{Snapshot, TaskState};
use orchestrail_engine::time::{is_iso_utc, iso_to_epoch};

/// One escalated task — terminal, requires an explicit operator decision (§6.2 Q1/Q2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalatedCard {
    pub id: String,
    pub title: String,
    /// The `причина=` suffix, when the escalation carries one.
    pub reason: Option<String>,
    /// Other queue tasks that list this id as a predecessor (§6.2 Q4 "what will unblock").
    pub blocks: Vec<String>,
}

/// One task returned to the queue after quarantine (merge conflict / review rejection / CI …).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineCard {
    pub id: String,
    pub title: String,
    /// The `попытка=` counter, when present.
    pub attempt: Option<u32>,
    /// The `карантин=` reason, when present.
    pub reason: Option<String>,
    /// Other queue tasks that list this id as a predecessor.
    pub blocks: Vec<String>,
}

/// One not-yet-started task whose predecessor is not resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedCard {
    pub id: String,
    pub title: String,
    /// The concrete unresolved predecessor T-ID (the first one found, in `Предпосылки:` order).
    pub blocking_on: String,
    /// Whether the blocking predecessor is itself escalated — never completes without an
    /// operator decision on THAT task — versus merely still in flight.
    pub blocking_infeasible: bool,
    /// The predecessor id appears in neither `snapshot.queue`, `snapshot.descriptors`, nor
    /// `Tasks_Done.md` — a data-integrity signal (typo'd/stale `Предпосылки:` entry, or a race
    /// with archival) worth the operator's attention, distinct from ordinary in-flight blocking.
    pub blocking_unknown: bool,
}

/// One persistent human-gate request decoded from `.work/approvals/<id>.json`. Only undecided
/// records are loaded: decided records are one-time consumed and disappear from the pending list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalCard {
    pub id: String,
    pub subject: String,
    pub task: Option<String>,
    pub batch: Option<String>,
    pub reason: String,
    pub created_at: Option<String>,
    pub deadline: Option<String>,
    pub fingerprint: Option<String>,
    pub policy_hash: Option<String>,
}

/// Read-only projection of approval artifacts. Expired undecided requests remain visible as
/// expired outcomes but are not actionable; malformed artifacts are surfaced instead of silently
/// disappearing from the operator's inbox.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApprovalProjection {
    pub pending: Vec<ApprovalCard>,
    pub expired: Vec<ApprovalCard>,
    pub errors: Vec<String>,
}
/// The full Decision Inbox projection (§6.2), built from one `Snapshot` plus the pause marker.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecisionInbox {
    /// Whether `.work/PAUSE` currently exists.
    pub paused: bool,
    /// The pause marker's own content, if any (informational only — see `agents/processor.md`
    /// "Пауза — kill switch `.work/PAUSE`": only the file's *existence* gates the processor).
    pub pause_note: Option<String>,
    /// Undecided, unexpired one-time human-gate requests, newest deadline first.
    pub approvals: Vec<ApprovalCard>,
    /// Undecided requests whose deadline has passed. Visible but never actionable.
    pub expired_approvals: Vec<ApprovalCard>,
    /// Approval-directory / JSON errors surfaced to the operator.
    pub approval_errors: Vec<String>,
    pub escalated: Vec<EscalatedCard>,
    pub quarantined: Vec<QuarantineCard>,
    pub blocked: Vec<BlockedCard>,
}

impl DecisionInbox {
    /// Nothing at all currently needs the operator.
    pub fn is_empty(&self) -> bool {
        !self.paused
            && self.approvals.is_empty()
            && self.expired_approvals.is_empty()
            && self.approval_errors.is_empty()
            && self.escalated.is_empty()
            && self.quarantined.is_empty()
            && self.blocked.is_empty()
    }

    /// Total number of cards (excludes the pause banner, which is a state, not a card).
    pub fn card_count(&self) -> usize {
        self.approvals.len()
            + self.expired_approvals.len()
            + self.approval_errors.len()
            + self.escalated.len()
            + self.quarantined.len()
            + self.blocked.len()
    }
}

/// Load persistent approval requests read-only. Decided artifacts are consumed and intentionally
/// omitted; an undecided request moves from `pending` to `expired` as soon as its canonical UTC
/// deadline is no later than `now_iso8601`.
pub fn load_approvals(work_dir: &Path, now_iso8601: &str) -> ApprovalProjection {
    let dir = work_dir.join("approvals");
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ApprovalProjection::default();
        }
        Err(error) => {
            return ApprovalProjection {
                errors: vec![format!("approvals/: {error}")],
                ..ApprovalProjection::default()
            };
        }
    };

    let mut projection = ApprovalProjection::default();
    let mut paths = Vec::new();
    for entry in entries {
        match entry {
            Ok(entry) if entry.path().extension().and_then(|x| x.to_str()) == Some("json") => {
                paths.push(entry.path())
            }
            Ok(_) => {}
            Err(error) => projection.errors.push(format!("approvals/: {error}")),
        }
    }
    paths.sort();

    for path in paths {
        let name = path
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("<неизвестный файл>")
            .to_string();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) => {
                projection.errors.push(format!("{name}: {error}"));
                continue;
            }
        };
        let value: serde_json::Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(error) => {
                projection
                    .errors
                    .push(format!("{name}: нераспознанный JSON: {error}"));
                continue;
            }
        };
        if value.get("schema").and_then(|x| x.as_str()) != Some("orchestra/approval@1") {
            projection
                .errors
                .push(format!("{name}: неподдерживаемая schema approval"));
            continue;
        }
        let id = match value.get("id").and_then(|x| x.as_str()) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => {
                projection.errors.push(format!("{name}: отсутствует id"));
                continue;
            }
        };
        if path.file_stem().and_then(|x| x.to_str()) != Some(id.as_str()) {
            projection
                .errors
                .push(format!("{name}: id '{id}' не совпадает с именем файла"));
            continue;
        }
        // A non-empty decision means policy.ps1 already consumed this one-time id.
        if value
            .get("decision")
            .and_then(|x| x.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .is_some()
        {
            continue;
        }

        let card = ApprovalCard {
            id,
            subject: approval_string(&value, "subject").unwrap_or_else(|| "не указан".into()),
            task: approval_string(&value, "task"),
            batch: approval_string(&value, "batch"),
            reason: approval_string(&value, "reason").unwrap_or_else(|| "не указана".into()),
            created_at: approval_string(&value, "created_at"),
            deadline: approval_string(&value, "deadline"),
            fingerprint: approval_string(&value, "fingerprint"),
            policy_hash: approval_string(&value, "policy_hash"),
        };
        match approval_deadline_reached(card.deadline.as_deref(), now_iso8601) {
            Some(true) => projection.expired.push(card),
            Some(false) => projection.pending.push(card),
            None => projection.errors.push(format!(
                "{name}: отсутствует или некорректен deadline; approval не доступен для решения"
            )),
        }
    }

    projection.pending.sort_by(|a, b| {
        a.deadline
            .as_deref()
            .unwrap_or("")
            .cmp(b.deadline.as_deref().unwrap_or(""))
            .then_with(|| a.id.cmp(&b.id))
    });
    projection.expired.sort_by(|a, b| a.id.cmp(&b.id));
    projection
}

/// Return whether an approval deadline has been reached (`deadline <= now`). Both values must be
/// the engine's canonical UTC format; a malformed or absent deadline is not actionable. Seconds
/// alone are insufficient because the shared timestamp format permits fractional seconds, which
/// do not sort correctly against an otherwise identical whole-second timestamp lexically.
fn approval_deadline_reached(deadline: Option<&str>, now_iso8601: &str) -> Option<bool> {
    let deadline = deadline?;
    if !is_iso_utc(deadline) || !is_iso_utc(now_iso8601) {
        return None;
    }
    let deadline_secs = iso_to_epoch(deadline)?;
    let now_secs = iso_to_epoch(now_iso8601)?;
    match deadline_secs.cmp(&now_secs) {
        std::cmp::Ordering::Less => Some(true),
        std::cmp::Ordering::Greater => Some(false),
        std::cmp::Ordering::Equal => {
            Some(fractional_millis(deadline)? <= fractional_millis(now_iso8601)?)
        }
    }
}

/// Milliseconds in a timestamp that [`is_iso_utc`] has already accepted. No fractional part means
/// zero; one or two digits are right-padded so `.5Z` and `.500Z` compare as the same instant.
fn fractional_millis(iso: &str) -> Option<u16> {
    let Some((_, fraction)) = iso.split_once('.') else {
        return Some(0);
    };
    let fraction = fraction.strip_suffix('Z')?;
    let digits = fraction.parse::<u16>().ok()?;
    let scale = match fraction.len() {
        1 => 100,
        2 => 10,
        3 => 1,
        _ => return None,
    };
    Some(digits * scale)
}

fn approval_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}
/// Build the Decision Inbox from a snapshot plus whether `.work/PAUSE` currently exists (and its
/// optional note), plus `done_ids` — task ids already archived to `Tasks_Done.md` (decoded by the
/// caller, see `main.rs::done_task_ids`), used only to positively confirm a predecessor absent
/// from the live snapshot is truly done rather than silently assuming it (R-2). Read-only: only
/// reads the already-decoded `snapshot.queue` / `snapshot.descriptors` fields plus the passed-in
/// `done_ids` — no filesystem access happens inside this function.
pub fn build(
    snapshot: &Snapshot,
    paused: bool,
    pause_note: Option<String>,
    done_ids: &BTreeSet<String>,
) -> DecisionInbox {
    // Authoritative state-by-id for predecessor resolution: the descriptor's `Статус:` is
    // authoritative once a task is captured (`engine::state::descriptor`), so it overrides the
    // queue header, which can lag until the processor resyncs it at a phase boundary.
    let mut state_by_id: BTreeMap<&str, TaskState> = BTreeMap::new();
    for q in &snapshot.queue {
        if let Some(st) = q.state {
            state_by_id.insert(q.id.as_str(), st);
        }
    }
    for d in &snapshot.descriptors {
        if let Some(st) = d.state {
            state_by_id.insert(d.id.as_str(), st);
        }
    }

    let mut escalated = Vec::new();
    let mut quarantined = Vec::new();
    let mut blocked = Vec::new();

    for q in &snapshot.queue {
        match q.state {
            Some(TaskState::Escalated) => escalated.push(EscalatedCard {
                id: q.id.clone(),
                title: q.title.clone(),
                reason: q.escalation_reason.clone(),
                blocks: dependents_of(&q.id, snapshot),
            }),
            Some(TaskState::NotStarted) if q.quarantine.is_some() => {
                quarantined.push(QuarantineCard {
                    id: q.id.clone(),
                    title: q.title.clone(),
                    attempt: q.attempt,
                    reason: q.quarantine.clone(),
                    blocks: dependents_of(&q.id, snapshot),
                })
            }
            Some(TaskState::NotStarted) => {
                if let Some((blocking_on, blocking_infeasible, blocking_unknown)) =
                    first_unresolved_prerequisite(&q.prerequisites, &state_by_id, done_ids)
                {
                    blocked.push(BlockedCard {
                        id: q.id.clone(),
                        title: q.title.clone(),
                        blocking_on,
                        blocking_infeasible,
                        blocking_unknown,
                    });
                }
            }
            _ => {}
        }
    }

    DecisionInbox {
        paused,
        pause_note,
        approvals: Vec::new(),
        expired_approvals: Vec::new(),
        approval_errors: Vec::new(),
        escalated,
        quarantined,
        blocked,
    }
}

/// The first predecessor T-ID that is not yet resolved, whether it is escalated (infeasible
/// without an operator decision on it), and whether it is unresolvable-unknown (absent from the
/// queue, descriptors, AND `done_ids`). `None` if every predecessor is resolved.
fn first_unresolved_prerequisite(
    prerequisites: &[String],
    state_by_id: &BTreeMap<&str, TaskState>,
    done_ids: &BTreeSet<String>,
) -> Option<(String, bool, bool)> {
    for p in prerequisites {
        match state_by_id.get(p.as_str()) {
            Some(TaskState::Done) => continue,
            Some(TaskState::Escalated) => return Some((p.clone(), true, false)),
            Some(_) => return Some((p.clone(), false, false)),
            None => {
                if done_ids.contains(p.as_str()) {
                    // Confirmed archived to Tasks_Done.md -> resolved.
                    continue;
                }
                // Not in queue, descriptors, NOR Tasks_Done.md: surface explicitly (R-2) instead
                // of silently assuming "already done".
                return Some((p.clone(), false, true));
            }
        }
    }
    None
}

/// Other queue tasks that list `id` in their own `Предпосылки:` — i.e. what completing/resolving
/// `id` would help unblock.
fn dependents_of(id: &str, snapshot: &Snapshot) -> Vec<String> {
    snapshot
        .queue
        .iter()
        .filter(|q| q.prerequisites.iter().any(|p| p == id))
        .map(|q| q.id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrail_engine::state::{
        DeliveryTarget, Descriptor, IntegrationSnapshot, IntegrationState, QueueEntry,
    };
    use std::path::PathBuf;

    fn queue_entry(
        id: &str,
        title: &str,
        state: Option<TaskState>,
        attempt: Option<u32>,
        quarantine: Option<&str>,
        escalation_reason: Option<&str>,
        prerequisites: &[&str],
    ) -> QueueEntry {
        QueueEntry {
            id: id.to_string(),
            title: title.to_string(),
            state,
            status_literal: String::new(),
            attempt,
            quarantine: quarantine.map(str::to_string),
            escalation_reason: escalation_reason.map(str::to_string),
            prerequisites: prerequisites.iter().map(|s| s.to_string()).collect(),
            delivery_target: DeliveryTarget::Current,
        }
    }

    fn snapshot(queue: Vec<QueueEntry>, descriptors: Vec<Descriptor>) -> Snapshot {
        Snapshot {
            work_dir: PathBuf::from("/tmp/does-not-matter"),
            queue,
            descriptors,
            cohort: None,
            integration: IntegrationSnapshot {
                state: IntegrationState::None,
                review_sha: None,
                f_cycles: None,
            },
            batch: None,
        }
    }

    #[test]
    fn empty_snapshot_yields_empty_inbox() {
        let snap = snapshot(vec![], vec![]);
        let inbox = build(&snap, false, None, &BTreeSet::new());
        assert!(inbox.is_empty());
        assert_eq!(inbox.card_count(), 0);
    }

    #[test]
    fn escalated_entry_becomes_a_card_with_reason_and_dependents() {
        let q = vec![
            queue_entry(
                "T-050",
                "Старая задача",
                Some(TaskState::Escalated),
                None,
                None,
                Some("INTEGRATION_LOOP_MAX"),
                &[],
            ),
            queue_entry(
                "T-060",
                "Ждёт T-050",
                Some(TaskState::NotStarted),
                None,
                None,
                None,
                &["T-050"],
            ),
        ];
        let snap = snapshot(q, vec![]);
        let inbox = build(&snap, false, None, &BTreeSet::new());
        assert!(!inbox.is_empty());
        assert_eq!(inbox.escalated.len(), 1);
        let card = &inbox.escalated[0];
        assert_eq!(card.id, "T-050");
        assert_eq!(card.reason.as_deref(), Some("INTEGRATION_LOOP_MAX"));
        assert_eq!(card.blocks, vec!["T-060".to_string()]);
        // T-060 is blocked on the escalated (infeasible) T-050.
        assert_eq!(inbox.blocked.len(), 1);
        assert_eq!(inbox.blocked[0].blocking_on, "T-050");
        assert!(inbox.blocked[0].blocking_infeasible);
        assert!(!inbox.blocked[0].blocking_unknown);
    }

    #[test]
    fn quarantined_entry_reads_attempt_and_reason() {
        let q = vec![queue_entry(
            "T-104",
            "Экран Decision Inbox",
            Some(TaskState::NotStarted),
            Some(2),
            Some("merge-conflict"),
            None,
            &["T-102", "T-103"],
        )];
        let snap = snapshot(q, vec![]);
        let inbox = build(&snap, false, None, &BTreeSet::new());
        assert_eq!(inbox.quarantined.len(), 1);
        let card = &inbox.quarantined[0];
        assert_eq!(card.attempt, Some(2));
        assert_eq!(card.reason.as_deref(), Some("merge-conflict"));
        // a quarantined entry is not ALSO counted as merely blocked.
        assert!(inbox.blocked.is_empty());
    }

    #[test]
    fn blocked_entry_waits_on_in_flight_predecessor() {
        let q = vec![
            queue_entry(
                "T-102",
                "TUI экран",
                Some(TaskState::Working),
                None,
                None,
                None,
                &[],
            ),
            queue_entry(
                "T-104",
                "Экран Decision Inbox",
                Some(TaskState::NotStarted),
                None,
                None,
                None,
                &["T-102"],
            ),
        ];
        let snap = snapshot(q, vec![]);
        let inbox = build(&snap, false, None, &BTreeSet::new());
        assert_eq!(inbox.blocked.len(), 1);
        assert_eq!(inbox.blocked[0].blocking_on, "T-102");
        assert!(!inbox.blocked[0].blocking_infeasible);
        assert!(!inbox.blocked[0].blocking_unknown);
    }

    #[test]
    fn predecessor_resolved_via_done_status_unblocks() {
        let q = vec![
            queue_entry(
                "T-101",
                "Основа",
                Some(TaskState::Done),
                None,
                None,
                None,
                &[],
            ),
            queue_entry(
                "T-104",
                "Экран Decision Inbox",
                Some(TaskState::NotStarted),
                None,
                None,
                None,
                &["T-101"],
            ),
        ];
        let snap = snapshot(q, vec![]);
        let inbox = build(&snap, false, None, &BTreeSet::new());
        assert!(inbox.blocked.is_empty());
    }

    #[test]
    fn predecessor_confirmed_done_via_tasks_done_ids_unblocks() {
        // T-050 does not appear in the live snapshot's queue/descriptors, but IS present in the
        // caller-supplied `done_ids` (decoded from Tasks_Done.md) -> confirmed done, so T-104 is
        // NOT reported as blocked on it.
        let q = vec![queue_entry(
            "T-104",
            "Экран Decision Inbox",
            Some(TaskState::NotStarted),
            None,
            None,
            None,
            &["T-050"],
        )];
        let snap = snapshot(q, vec![]);
        let done: BTreeSet<String> = ["T-050".to_string()].into_iter().collect();
        let inbox = build(&snap, false, None, &done);
        assert!(inbox.blocked.is_empty());
    }

    #[test]
    fn predecessor_missing_everywhere_is_surfaced_as_unknown_not_silently_resolved() {
        // T-050 is absent from the queue, descriptors, AND `done_ids` (R-2): rather than
        // silently assuming "already done", it must be surfaced so a typo'd/stale
        // `Предпосылки:` entry doesn't hide a genuinely blocked task from the operator.
        let q = vec![queue_entry(
            "T-104",
            "Экран Decision Inbox",
            Some(TaskState::NotStarted),
            None,
            None,
            None,
            &["T-050"],
        )];
        let snap = snapshot(q, vec![]);
        let inbox = build(&snap, false, None, &BTreeSet::new());
        assert_eq!(inbox.blocked.len(), 1);
        assert_eq!(inbox.blocked[0].blocking_on, "T-050");
        assert!(inbox.blocked[0].blocking_unknown);
        assert!(!inbox.blocked[0].blocking_infeasible);
    }

    #[test]
    fn descriptor_state_overrides_lagging_queue_header_for_predecessor_resolution() {
        // The queue header still says "в работе" (not yet resynced) but the descriptor already
        // reads "выполнена" -- the descriptor wins, so the dependent is not reported as blocked.
        let q = vec![
            queue_entry(
                "T-101",
                "Основа",
                Some(TaskState::Working),
                None,
                None,
                None,
                &[],
            ),
            queue_entry(
                "T-104",
                "Экран Decision Inbox",
                Some(TaskState::NotStarted),
                None,
                None,
                None,
                &["T-101"],
            ),
        ];
        let d = vec![Descriptor {
            id: "T-101".to_string(),
            state: Some(TaskState::Done),
            status_literal: Some("выполнена".to_string()),
            prerequisites: vec![],
            conflict_domain: None,
            level: None,
            risk: None,
            network: None,
            batch_id: None,
            branch: None,
            worktree: None,
            implementation_authors: vec![],
            review_sha: None,
            review_cycles: None,
        }];
        let snap = snapshot(q, d);
        let inbox = build(&snap, false, None, &BTreeSet::new());
        assert!(inbox.blocked.is_empty());
    }

    #[test]
    fn approval_loader_lists_pending_separates_expired_and_hides_consumed() {
        let root = std::env::temp_dir().join(format!(
            "orchestrail-tui-inbox-approvals-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let approvals = root.join("approvals");
        fs::create_dir_all(&approvals).unwrap();
        let artifact = |id: &str, deadline: &str, decision: &str| {
            format!(
                r#"{{"schema":"orchestra/approval@1","id":"{id}","subject":"task:T-250|batch:","task":"T-250","batch":"","reason":"human-review","fingerprint":"aa","policy_hash":"bb","created_at":"2026-07-16T00:00:00Z","deadline":"{deadline}","decision":"{decision}"}}"#
            )
        };
        fs::write(
            approvals.join("apr-pending.json"),
            artifact("apr-pending", "2026-07-17T00:00:00Z", ""),
        )
        .unwrap();
        fs::write(
            approvals.join("apr-expired.json"),
            artifact("apr-expired", "2026-07-15T00:00:00Z", ""),
        )
        .unwrap();
        fs::write(
            approvals.join("apr-consumed.json"),
            artifact("apr-consumed", "2026-07-17T00:00:00Z", "approve"),
        )
        .unwrap();
        fs::write(approvals.join("broken.json"), "not-json").unwrap();

        let loaded = load_approvals(&root, "2026-07-16T12:00:00Z");
        assert_eq!(loaded.pending.len(), 1);
        assert_eq!(loaded.pending[0].id, "apr-pending");
        assert_eq!(loaded.pending[0].task.as_deref(), Some("T-250"));
        assert_eq!(loaded.expired.len(), 1);
        assert_eq!(loaded.expired[0].id, "apr-expired");
        assert_eq!(loaded.errors.len(), 1);
        assert!(loaded.errors[0].contains("broken.json"));
        assert!(
            !loaded
                .pending
                .iter()
                .chain(loaded.expired.iter())
                .any(|a| a.id == "apr-consumed")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn approval_deadline_reaches_the_exact_second_and_handles_fractional_seconds() {
        let now = "2026-07-16T12:00:00Z";
        // "No later than" includes the exact second; `<` incorrectly left this actionable.
        assert_eq!(
            approval_deadline_reached(Some("2026-07-16T12:00:00Z"), now),
            Some(true)
        );
        // A fractional deadline in the same second is still in the future. Raw lexical comparison
        // gets this wrong because `.` sorts before `Z`.
        assert_eq!(
            approval_deadline_reached(Some("2026-07-16T12:00:00.500Z"), now),
            Some(false)
        );
        assert_eq!(
            approval_deadline_reached(Some("2026-07-16T11:59:59.999Z"), now),
            Some(true)
        );
    }

    #[test]
    fn malformed_or_missing_approval_deadline_is_not_actionable() {
        let now = "2026-07-16T12:00:00Z";
        assert_eq!(approval_deadline_reached(None, now), None);
        assert_eq!(approval_deadline_reached(Some("not-a-time"), now), None);
        assert_eq!(
            approval_deadline_reached(Some("2026-07-16T12:00:00Z"), "not-a-time"),
            None
        );
    }

    #[test]
    fn pause_marker_is_surfaced_verbatim() {
        let inbox = build(
            &snapshot(vec![], vec![]),
            true,
            Some("2026-07-11T12:00:00Z".into()),
            &BTreeSet::new(),
        );
        assert!(!inbox.is_empty());
        assert!(inbox.paused);
        assert_eq!(inbox.pause_note.as_deref(), Some("2026-07-11T12:00:00Z"));
    }
}
