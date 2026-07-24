//! Parse `.work/cohort_state.md` — the rolling cohort-admission state (§13.2).
//!
//! The file (created in Phase 1, removed in 6.4) carries `Приём:` (admission), plus the
//! bookkeeping fields `Начало когорты:`, `Волна:`, `Admitted всего:`, and a `B-<stamp>` batch id
//! in its heading. Its **absence is the normal idle state** — no active cohort — and reads as
//! `None`, not an error. Optional fields are tolerated when missing. Read-only.

use std::io;
use std::path::Path;

use crate::work_fs::{self, MAX_CONTROL_BYTES};

use super::canonical::{CohortAdmission, suffix_field};
use super::util::{find_batch_id, line_field};

/// Decoded cohort-admission state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CohortState {
    pub batch_id: Option<String>,
    /// Canonical admission, or `None` if the `Приём:` literal is absent/unrecognized.
    pub admission: Option<CohortAdmission>,
    /// The raw `Приём:` literal, when present.
    pub admission_literal: Option<String>,
    /// `причина=<COHORT_SIZE|COHORT_MAX_AGE|очередь-пуста|только-конфликты-с-готовыми>` on close.
    pub admission_reason: Option<String>,
    pub started_at: Option<String>,
    pub wave: Option<u32>,
    pub admitted_total: Option<u32>,
}

/// Decode `cohort_state.md` text.
pub fn parse_cohort(text: &str) -> CohortState {
    let admission_literal = line_field(text, "Приём:").map(str::to_string);
    let admission = admission_literal
        .as_deref()
        .and_then(CohortAdmission::from_markdown);
    let admission_reason = admission_literal
        .as_deref()
        .and_then(|l| suffix_field(l, "причина="));
    CohortState {
        batch_id: find_batch_id(text),
        admission,
        admission_literal,
        admission_reason,
        started_at: line_field(text, "Начало когорты:").map(str::to_string),
        wave: line_field(text, "Волна:").and_then(|v| v.parse().ok()),
        admitted_total: line_field(text, "Admitted всего:").and_then(|v| v.parse().ok()),
    }
}

/// Load `<work_dir>/cohort_state.md`. `None` = file absent = no active cohort (§13.2 idle);
/// an existing but unreadable file is an error, never a fabricated idle state.
pub fn try_load_cohort(work_dir: &Path) -> io::Result<Option<CohortState>> {
    let path = work_dir.join("cohort_state.md");
    Ok(
        work_fs::read_optional_text(work_dir, &path, MAX_CONTROL_BYTES)?
            .map(|text| parse_cohort(&text)),
    )
}

/// Best-effort compatibility loader for passive observers; control-plane decisions use
/// [`try_load_cohort`].
pub fn load_cohort(work_dir: &Path) -> Option<CohortState> {
    try_load_cohort(work_dir).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_open_cohort_with_all_fields() {
        let text = "# Cohort state — Batch B-20260711T113948Z\n\
Начало когорты: 2026-07-11T11:39:48Z\n\
Приём: открыт\n\
Волна: 1\n\
Admitted всего: 2\n";
        let c = parse_cohort(text);
        assert_eq!(c.batch_id.as_deref(), Some("B-20260711T113948Z"));
        assert_eq!(c.admission, Some(CohortAdmission::Open));
        assert_eq!(c.started_at.as_deref(), Some("2026-07-11T11:39:48Z"));
        assert_eq!(c.wave, Some(1));
        assert_eq!(c.admitted_total, Some(2));
        assert!(c.admission_reason.is_none());
    }

    #[test]
    fn parses_closed_cohort_with_reason() {
        let text = "# Cohort state — Batch B-1\nПриём: закрыт · причина=COHORT_SIZE\nВолна: 3\n";
        let c = parse_cohort(text);
        assert_eq!(c.admission, Some(CohortAdmission::Closed));
        assert_eq!(c.admission_reason.as_deref(), Some("COHORT_SIZE"));
        assert_eq!(c.wave, Some(3));
    }

    #[test]
    fn tolerates_minimal_file_missing_optional_fields() {
        // The shape the harness fixture writes: no reason, no start, no admitted total.
        let text = "# Когорта\nПриём: закрыт\nВолна: 1\n";
        let c = parse_cohort(text);
        assert_eq!(c.admission, Some(CohortAdmission::Closed));
        assert!(c.batch_id.is_none());
        assert!(c.started_at.is_none());
        assert!(c.admitted_total.is_none());
    }

    #[test]
    fn absent_file_is_no_active_cohort() {
        let dir = std::env::temp_dir().join("orchestra-state-no-cohort-xyz");
        assert!(load_cohort(&dir).is_none());
    }
}
