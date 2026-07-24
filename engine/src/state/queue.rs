//! Parse `.work/Tasks_Queue.md` — the backlog (contract §1–§12) — into typed entries.
//!
//! Each task is one block whose header is `### [T-NNN] <title> — статус: <literal>`. The status
//! is read by the §13.1 rule: only the **base word** of the literal decides the state, so a
//! re-queued `не начата · попытка=N · карантин=…` still counts as `not-started` and the
//! `попытка`/`карантин`/`причина` suffixes are captured separately. The body's `Предпосылки:`
//! line yields the prerequisite T-ids. Read-only: this only decodes the file's text.

use super::canonical::{TaskState, suffix_field};
use super::util::{is_task_id, line_field, parse_task_id_list};

/// Delivery lane of a task (`docs/queue_contract.md` §11.1), decoded from the body's
/// `Delivery target:` field. `current` is the normal delivery line the ordinary
/// processor/engine admits; `next_major` parks intentional breaking work out of the ordinary
/// execution capacity. A **missing field, or any value other than an explicit `next_major`,
/// resolves to `current`** — backward-compatible with every existing fieldless queue/archive
/// record, and fail-safe (only an explicit `next_major` parks a task).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryTarget {
    #[default]
    Current,
    NextMajor,
}

impl DeliveryTarget {
    /// The canonical field literal (`current` / `next_major`).
    pub fn as_str(&self) -> &'static str {
        match self {
            DeliveryTarget::Current => "current",
            DeliveryTarget::NextMajor => "next_major",
        }
    }

    /// Map a `Delivery target:` field value to a lane. Only an explicit `next_major` (any case)
    /// parks the task; every other value — including an empty/garbage one — is `current`.
    pub fn from_field(value: &str) -> DeliveryTarget {
        if value.trim().eq_ignore_ascii_case("next_major") {
            DeliveryTarget::NextMajor
        } else {
            DeliveryTarget::Current
        }
    }
}

/// One decoded queue block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEntry {
    pub id: String,
    pub title: String,
    /// Canonical state, or `None` if the literal is unrecognized (a hand-edit typo).
    pub state: Option<TaskState>,
    /// The raw Markdown status literal after `статус:` (for diagnostics / round-trip).
    pub status_literal: String,
    /// `попытка=N` (quarantine attempt counter), when present.
    pub attempt: Option<u32>,
    /// `карантин=<reason>`, when present on a re-queued entry.
    pub quarantine: Option<String>,
    /// `причина=<reason>`, when present on an escalated entry.
    pub escalation_reason: Option<String>,
    /// T-ids from the block's `Предпосылки:` line.
    pub prerequisites: Vec<String>,
    /// Delivery lane from the block's `Delivery target:` line (§11.1); `current` when absent.
    pub delivery_target: DeliveryTarget,
}

/// Parse the full queue text into entries, in file order.
pub fn parse_queue(text: &str) -> Vec<QueueEntry> {
    let lines: Vec<&str> = text.lines().collect();
    // Header indices (lines that open a task block).
    let heads: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| parse_header(l).is_some())
        .map(|(i, _)| i)
        .collect();

    let mut out = Vec::new();
    for (n, &start) in heads.iter().enumerate() {
        let end = heads.get(n + 1).copied().unwrap_or(lines.len());
        let (id, title, status_literal) = match parse_header(lines[start]) {
            Some(h) => h,
            None => continue,
        };
        let body = lines[start + 1..end].join("\n");
        let prerequisites = line_field(&body, "Предпосылки:")
            .map(parse_task_id_list)
            .unwrap_or_default();
        let delivery_target = line_field(&body, "Delivery target:")
            .map(DeliveryTarget::from_field)
            .unwrap_or_default();
        out.push(QueueEntry {
            id,
            title,
            state: TaskState::from_markdown(&status_literal),
            attempt: suffix_field(&status_literal, "попытка=").and_then(|v| v.parse().ok()),
            quarantine: suffix_field(&status_literal, "карантин="),
            escalation_reason: suffix_field(&status_literal, "причина="),
            status_literal,
            prerequisites,
            delivery_target,
        });
    }
    out
}

/// Decode a header line `### [T-NNN] <title> — статус: <literal>` into `(id, title, literal)`.
/// Returns `None` for any non-header line.
fn parse_header(line: &str) -> Option<(String, String, String)> {
    let rest = line.strip_prefix("###")?.trim_start();
    let rest = rest.strip_prefix('[')?;
    let close = rest.find(']')?;
    let id = rest[..close].trim().to_string();
    if !is_task_id(&id) {
        return None;
    }
    let after = &rest[close + 1..];
    let (marker_pos, marker_len) = find_status_marker(after)?;
    let title = after[..marker_pos]
        .trim()
        // strip the trailing ` — ` (any dash variant) that separates title from status.
        .trim_end_matches(['—', '–', '-', ' '])
        .trim()
        .to_string();
    let status_literal = after[marker_pos + marker_len..].trim().to_string();
    Some((id, title, status_literal))
}

/// Locate the `статус:` marker in a header tail. Queue headers use lowercase `статус:`; the
/// capitalized `Статус:` is tolerated for robustness. Returns `(byte_pos, byte_len)`.
fn find_status_marker(s: &str) -> Option<(usize, usize)> {
    for marker in ["статус:", "Статус:"] {
        if let Some(p) = s.find(marker) {
            return Some((p, marker.len()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE: &str = "Формат записей описан где-то.\n------\n\n\
### [T-102] Превратить tui/ в живой обзорный экран — статус: в работе · батч=B-20260711T113948Z · worktree=.work/worktrees/T-102 · ветка=task/T-102\n\
Тело задачи.\n\
Предпосылки: T-101\n\n\
### [T-103] Дать движку read-only снимок control-plane — статус: не начата\n\
Описание.\n\
Предпосылки: T-101\n\n\
### [T-104] Экран Decision Inbox — статус: не начата · попытка=2 · карантин=merge-conflict\n\
Тело.\n\
Предпосылки: T-102, T-103\n\n\
### [T-050] Старая задача — статус: эскалирована · причина=INTEGRATION_LOOP_MAX\n\
Тело без предпосылок.\n";

    #[test]
    fn parses_all_entries_in_order() {
        let q = parse_queue(QUEUE);
        let ids: Vec<&str> = q.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["T-102", "T-103", "T-104", "T-050"]);
    }

    #[test]
    fn captured_entry_reads_working_and_keeps_title() {
        let q = parse_queue(QUEUE);
        let e = &q[0];
        assert_eq!(e.id, "T-102");
        assert_eq!(e.title, "Превратить tui/ в живой обзорный экран");
        assert_eq!(e.state, Some(TaskState::Working));
        assert_eq!(e.prerequisites, vec!["T-101"]);
        assert!(e.attempt.is_none());
    }

    #[test]
    fn plain_not_started_entry() {
        let e = &parse_queue(QUEUE)[1];
        assert_eq!(e.state, Some(TaskState::NotStarted));
        assert!(e.attempt.is_none());
        assert!(e.quarantine.is_none());
    }

    #[test]
    fn requeued_entry_keeps_not_started_and_captures_suffixes() {
        let e = &parse_queue(QUEUE)[2];
        assert_eq!(e.id, "T-104");
        assert_eq!(e.state, Some(TaskState::NotStarted));
        assert_eq!(e.attempt, Some(2));
        assert_eq!(e.quarantine.as_deref(), Some("merge-conflict"));
        assert_eq!(e.prerequisites, vec!["T-102", "T-103"]);
    }

    #[test]
    fn escalated_entry_reads_escalated_not_not_started() {
        let e = &parse_queue(QUEUE)[3];
        assert_eq!(e.id, "T-050");
        assert_eq!(e.state, Some(TaskState::Escalated));
        assert_eq!(e.escalation_reason.as_deref(), Some("INTEGRATION_LOOP_MAX"));
        assert!(e.prerequisites.is_empty());
    }

    #[test]
    fn ignores_prose_and_separators() {
        // The leading prose / `------` lines must not become entries.
        assert_eq!(parse_queue(QUEUE).len(), 4);
        assert!(parse_queue("just some text\nno headers here").is_empty());
    }

    #[test]
    fn fieldless_entries_default_to_current_lane() {
        // Every entry in the legacy fixture (none carries a `Delivery target:` line) is `current`.
        for e in parse_queue(QUEUE) {
            assert_eq!(
                e.delivery_target,
                DeliveryTarget::Current,
                "{} must default to current",
                e.id
            );
        }
    }

    #[test]
    fn parses_delivery_target_field_and_defaults() {
        let q = "\
### [T-201] Explicit current — статус: не начата\n\
Тело.\n\
Delivery target: current\n\n\
### [T-202] Parked breaking work — статус: не начата\n\
Тело.\n\
Delivery target: next_major\n\n\
### [T-203] No field at all — статус: не начата\n\
Тело без поля.\n\n\
### [T-204] Garbage value — статус: не начата\n\
Delivery target: whatever\n";
        let entries = parse_queue(q);
        assert_eq!(entries[0].delivery_target, DeliveryTarget::Current);
        assert_eq!(entries[1].delivery_target, DeliveryTarget::NextMajor);
        // Missing field and an unrecognized value both fall back to the current lane.
        assert_eq!(entries[2].delivery_target, DeliveryTarget::Current);
        assert_eq!(entries[3].delivery_target, DeliveryTarget::Current);
    }

    #[test]
    fn delivery_target_from_field_is_case_insensitive_on_next_major_only() {
        assert_eq!(
            DeliveryTarget::from_field("next_major"),
            DeliveryTarget::NextMajor
        );
        assert_eq!(
            DeliveryTarget::from_field("  NEXT_MAJOR  "),
            DeliveryTarget::NextMajor
        );
        assert_eq!(
            DeliveryTarget::from_field("current"),
            DeliveryTarget::Current
        );
        assert_eq!(DeliveryTarget::from_field(""), DeliveryTarget::Current);
        assert_eq!(
            DeliveryTarget::from_field("nonsense"),
            DeliveryTarget::Current
        );
        assert_eq!(DeliveryTarget::Current.as_str(), "current");
        assert_eq!(DeliveryTarget::NextMajor.as_str(), "next_major");
    }
}
