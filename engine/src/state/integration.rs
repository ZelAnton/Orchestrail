//! Parse `.work/integration_state.md` — the batch join-barrier state (§13.3).
//!
//! The file is the processor-owned join bookkeeping: it holds only `Ревью-SHA:` (the integration
//! tip the reviewer already saw) and `F-циклов:` (the integration review-cycle counter). It is
//! created in Phase 5 and removed in 6.4 (`agents/processor.md`).
//!
//! §13.3 is a **derived** lifecycle — the processor advances `none → in-progress → reviewed →
//! published → cleaned` (or `failed`) via `state-tx.ps1 check-transition --kind integration` and
//! the event journal, over `integration_state.md` **and** `merge_report.md`. From this file
//! alone only two points are honestly determinable, so the snapshot reports:
//!
//! * `none` — the file is absent (join not started, the normal idle state);
//! * `in-progress` — the file exists (the join is underway).
//!
//! Finer phases (`reviewed`/`published`/`failed`/`cleaned`) need `merge_report.md` / events
//! (module `events`, T-101), out of this snapshot's scope; the raw `Ревью-SHA`/`F-циклов` fields
//! are surfaced so a consumer can reason further. Read-only.

use std::io;
use std::path::Path;

use crate::work_fs::{self, MAX_CONTROL_BYTES};

use super::canonical::IntegrationState;
use super::util::line_field;

/// Decoded integration/join-barrier snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationSnapshot {
    pub state: IntegrationState,
    /// `Ревью-SHA:` — the integration tip the reviewer already fully reviewed.
    pub review_sha: Option<String>,
    /// `F-циклов:` — the integration review-cycle counter (`INTEGRATION_LOOP_MAX` guard).
    pub f_cycles: Option<u32>,
}

impl IntegrationSnapshot {
    /// The idle snapshot: no join underway (`integration_state.md` absent).
    fn none() -> IntegrationSnapshot {
        IntegrationSnapshot {
            state: IntegrationState::None,
            review_sha: None,
            f_cycles: None,
        }
    }
}

/// Decode `integration_state.md` text. A present file means the join is underway, hence
/// `in-progress`; the `Ревью-SHA`/`F-циклов` fields are captured when present.
pub fn parse_integration(text: &str) -> IntegrationSnapshot {
    IntegrationSnapshot {
        state: IntegrationState::InProgress,
        review_sha: line_field(text, "Ревью-SHA:").map(str::to_string),
        f_cycles: line_field(text, "F-циклов:").and_then(|v| v.parse().ok()),
    }
}

/// Load `<work_dir>/integration_state.md`. Absent file → `none` (no active integration);
/// an existing but unreadable file is returned as an error.
pub fn try_load_integration(work_dir: &Path) -> io::Result<IntegrationSnapshot> {
    let path = work_dir.join("integration_state.md");
    Ok(
        work_fs::read_optional_text(work_dir, &path, MAX_CONTROL_BYTES)?
            .map(|text| parse_integration(&text))
            .unwrap_or_else(IntegrationSnapshot::none),
    )
}

/// Best-effort compatibility loader for passive observers; control-plane decisions use
/// [`try_load_integration`].
pub fn load_integration(work_dir: &Path) -> IntegrationSnapshot {
    try_load_integration(work_dir).unwrap_or_else(|_| IntegrationSnapshot::none())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn present_file_reads_in_progress_with_fields() {
        let text = "# Integration state — B-1\nРевью-SHA: 4851279ec958\nF-циклов: 2\n";
        let i = parse_integration(text);
        assert_eq!(i.state, IntegrationState::InProgress);
        assert_eq!(i.review_sha.as_deref(), Some("4851279ec958"));
        assert_eq!(i.f_cycles, Some(2));
    }

    #[test]
    fn present_file_without_fields_still_in_progress() {
        let i = parse_integration("# Integration state\n");
        assert_eq!(i.state, IntegrationState::InProgress);
        assert!(i.review_sha.is_none());
        assert!(i.f_cycles.is_none());
    }

    #[test]
    fn absent_file_is_none() {
        let dir = std::env::temp_dir().join("orchestra-state-no-integration-xyz");
        let i = load_integration(&dir);
        assert_eq!(i.state, IntegrationState::None);
        assert!(i.review_sha.is_none());
        assert!(i.f_cycles.is_none());
    }
}
