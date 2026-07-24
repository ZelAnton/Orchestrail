//! Canonical control-plane state names (contract `docs/queue_contract.md` §13).
//!
//! §13 fixes the **canonical ASCII names** of the task / cohort-admission / integration
//! lifecycles and declares the human-readable Cyrillic Markdown literals a *compatible
//! representation* of them. This module is the single place that maps those Cyrillic literals
//! onto the canonical names, byte-for-byte with the §13.1–§13.3 tables — and with
//! `tools/state-tx.ps1`, whose transition validator works over the very same ASCII names.
//!
//! Read-only by construction: nothing here mutates state or checks transitions. The snapshot
//! only *observes* the current state; transition validation stays with `state-tx.ps1` (§17).

/// The middle-dot (`·`, U+00B7) that separates a status word from its ` · key=value` suffixes
/// (`не начата · попытка=2 · карантин=…`, `закрыт · причина=…`). See §13.1/§13.2.
const SEP: char = '\u{00B7}';

/// Canonical task state (§13.1). The Cyrillic literal from a descriptor `Статус:` line or a
/// queue header label maps here via [`TaskState::from_markdown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    NotStarted,
    Working,
    InReview,
    Ready,
    Merged,
    Published,
    Done,
    Escalated,
    Conflict,
}

impl TaskState {
    /// The canonical ASCII name (exactly as in the §13.1 table / `state-tx.ps1`).
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::NotStarted => "not-started",
            TaskState::Working => "working",
            TaskState::InReview => "in-review",
            TaskState::Ready => "ready",
            TaskState::Merged => "merged",
            TaskState::Published => "published",
            TaskState::Done => "done",
            TaskState::Escalated => "escalated",
            TaskState::Conflict => "conflict",
        }
    }

    /// The §13.1 terminal states (`done`, `escalated`) — no outgoing transitions.
    pub fn is_terminal(&self) -> bool {
        matches!(self, TaskState::Done | TaskState::Escalated)
    }

    /// Map a Markdown status *literal* to its canonical name (§13.1).
    ///
    /// Only the **base word** (everything before the first ` · ` suffix) decides the state, so a
    /// re-queued `не начата · попытка=2 · карантин=…` still reads as `not-started` and an
    /// `эскалирована · причина=…` label reads as `escalated` (no false match on `не начата`).
    /// The suffixes themselves are parsed separately by the source parsers via [`suffix_field`].
    pub fn from_markdown(literal: &str) -> Option<TaskState> {
        Some(match base_word(literal) {
            "не начата" => TaskState::NotStarted,
            "в работе" => TaskState::Working,
            "на ревью" => TaskState::InReview,
            "готова к слиянию" => TaskState::Ready,
            "слита" => TaskState::Merged,
            "опубликована" => TaskState::Published,
            "выполнена" => TaskState::Done,
            "эскалирована" => TaskState::Escalated,
            "конфликт" => TaskState::Conflict,
            _ => return None,
        })
    }

    /// Parse the canonical ASCII name back to a variant (round-trip / robustness).
    pub fn from_canonical(s: &str) -> Option<TaskState> {
        Some(match s {
            "not-started" => TaskState::NotStarted,
            "working" => TaskState::Working,
            "in-review" => TaskState::InReview,
            "ready" => TaskState::Ready,
            "merged" => TaskState::Merged,
            "published" => TaskState::Published,
            "done" => TaskState::Done,
            "escalated" => TaskState::Escalated,
            "conflict" => TaskState::Conflict,
            _ => return None,
        })
    }
}

/// Canonical cohort-admission state (§13.2). Maps the `Приём:` literal in `cohort_state.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CohortAdmission {
    Open,
    Closed,
}

impl CohortAdmission {
    pub fn as_str(&self) -> &'static str {
        match self {
            CohortAdmission::Open => "open",
            CohortAdmission::Closed => "closed",
        }
    }

    /// Map a `Приём:` literal (`открыт` / `закрыт · причина=…`) to its canonical name.
    pub fn from_markdown(literal: &str) -> Option<CohortAdmission> {
        Some(match base_word(literal) {
            "открыт" => CohortAdmission::Open,
            "закрыт" => CohortAdmission::Closed,
            _ => return None,
        })
    }

    pub fn from_canonical(s: &str) -> Option<CohortAdmission> {
        Some(match s {
            "open" => CohortAdmission::Open,
            "closed" => CohortAdmission::Closed,
            _ => return None,
        })
    }
}

/// Canonical integration/publication/cleanup state (§13.3).
///
/// Unlike §13.1/§13.2 this lifecycle has **no Cyrillic Markdown literal**: it is a *derived*
/// phase the processor advances via `state-tx.ps1 check-transition --kind integration` and the
/// event journal, over `integration_state.md` + `merge_report.md`. From the read-only files in
/// this snapshot's scope only two points are honestly determinable — `none` (join not started,
/// `integration_state.md` absent) and `in-progress` (the file exists, so the join is underway).
/// The enum still models every §13.3 name so consumers (resolvers, TUI) share one vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationState {
    None,
    InProgress,
    Reviewed,
    Published,
    Failed,
    Cleaned,
}

impl IntegrationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            IntegrationState::None => "none",
            IntegrationState::InProgress => "in-progress",
            IntegrationState::Reviewed => "reviewed",
            IntegrationState::Published => "published",
            IntegrationState::Failed => "failed",
            IntegrationState::Cleaned => "cleaned",
        }
    }

    pub fn from_canonical(s: &str) -> Option<IntegrationState> {
        Some(match s {
            "none" => IntegrationState::None,
            "in-progress" => IntegrationState::InProgress,
            "reviewed" => IntegrationState::Reviewed,
            "published" => IntegrationState::Published,
            "failed" => IntegrationState::Failed,
            "cleaned" => IntegrationState::Cleaned,
            _ => return None,
        })
    }
}

/// The base status word: everything before the first ` · ` suffix separator, trimmed.
/// `готова к слиянию` (spaces, no separator) is returned whole; `в работе · батч=…` yields
/// `в работе`.
fn base_word(literal: &str) -> &str {
    let cut = literal.find(SEP).unwrap_or(literal.len());
    literal[..cut].trim()
}

/// The value of a ` · key=value` suffix segment inside a status literal (segments split on the
/// middle dot). `key` must include the trailing `=` (e.g. `"попытка="`). Returns the trimmed
/// value of the first matching segment, or `None`.
pub fn suffix_field(literal: &str, key: &str) -> Option<String> {
    literal
        .split(SEP)
        .map(str::trim)
        .find_map(|seg| seg.strip_prefix(key).map(|v| v.trim().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_task_cyrillic_literal_maps_to_its_canonical_name() {
        // §13.1: one Cyrillic literal per canonical state.
        for (literal, canon) in [
            ("не начата", "not-started"),
            ("в работе", "working"),
            ("на ревью", "in-review"),
            ("готова к слиянию", "ready"),
            ("слита", "merged"),
            ("опубликована", "published"),
            ("выполнена", "done"),
            ("эскалирована", "escalated"),
            ("конфликт", "conflict"),
        ] {
            let st =
                TaskState::from_markdown(literal).unwrap_or_else(|| panic!("{literal} must map"));
            assert_eq!(st.as_str(), canon, "{literal} -> {canon}");
            assert_eq!(TaskState::from_canonical(canon), Some(st));
        }
    }

    #[test]
    fn suffixed_labels_map_by_base_word_without_false_positives() {
        // Re-queued after quarantine: base word is still `не начата` (§13.1 rule).
        assert_eq!(
            TaskState::from_markdown("не начата · попытка=2 · карантин=merge-conflict"),
            Some(TaskState::NotStarted)
        );
        // A captured label must read as `working`, NOT stumble onto `не начата`.
        assert_eq!(
            TaskState::from_markdown(
                "в работе · батч=B-1 · worktree=.work/worktrees/T-1 · ветка=task/T-1"
            ),
            Some(TaskState::Working)
        );
        // An escalated label must read as `escalated`, NOT `not-started`.
        assert_eq!(
            TaskState::from_markdown("эскалирована · причина=INTEGRATION_LOOP_MAX"),
            Some(TaskState::Escalated)
        );
    }

    #[test]
    fn terminal_states_are_done_and_escalated_only() {
        assert!(TaskState::Done.is_terminal());
        assert!(TaskState::Escalated.is_terminal());
        for st in [
            TaskState::NotStarted,
            TaskState::Working,
            TaskState::InReview,
            TaskState::Ready,
            TaskState::Merged,
            TaskState::Published,
            TaskState::Conflict,
        ] {
            assert!(!st.is_terminal(), "{} must not be terminal", st.as_str());
        }
    }

    #[test]
    fn unknown_task_literal_is_none() {
        assert_eq!(TaskState::from_markdown("совершенно неизвестно"), None);
        assert_eq!(TaskState::from_canonical("bogus"), None);
    }

    #[test]
    fn cohort_admission_literals_map() {
        // §13.2: both Cyrillic literals.
        assert_eq!(
            CohortAdmission::from_markdown("открыт"),
            Some(CohortAdmission::Open)
        );
        assert_eq!(
            CohortAdmission::from_markdown("закрыт · причина=COHORT_SIZE"),
            Some(CohortAdmission::Closed)
        );
        assert_eq!(CohortAdmission::Open.as_str(), "open");
        assert_eq!(CohortAdmission::Closed.as_str(), "closed");
        assert_eq!(CohortAdmission::from_markdown("что-то"), None);
    }

    #[test]
    fn integration_canonical_names_round_trip() {
        // §13.3 has no Cyrillic literal — exercise every canonical name via round-trip.
        for name in [
            "none",
            "in-progress",
            "reviewed",
            "published",
            "failed",
            "cleaned",
        ] {
            let st = IntegrationState::from_canonical(name)
                .unwrap_or_else(|| panic!("{name} must parse"));
            assert_eq!(st.as_str(), name);
        }
        assert_eq!(IntegrationState::from_canonical("bogus"), None);
    }

    #[test]
    fn suffix_field_extracts_segments() {
        let label = "не начата · попытка=3 · карантин=only-conflicts";
        assert_eq!(suffix_field(label, "попытка="), Some("3".to_string()));
        assert_eq!(
            suffix_field(label, "карантин="),
            Some("only-conflicts".to_string())
        );
        assert_eq!(suffix_field(label, "причина="), None);
    }
}
