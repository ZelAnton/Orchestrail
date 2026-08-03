//! Resolver 3 — **clean / with-findings review gate** (`agents/processor.md`, phases 2.6 / 2.7 /
//! 2.8).
//!
//! After a review pass the processor branches three ways on the `review.md` content and the
//! `date -u` mark it took just before the call. This resolver names that branch as a pure
//! function over the already-parsed [`ReviewParse`], **reusing** `ReviewParse::is_clean_pass`
//! for the clean determination (the freshness + zero-open-`R-` gate proved in `contract.rs`):
//!
//! * **Findings** (2.8) — open `R-` present → address them, then re-review.
//! * **Clean** (2.6) — a `SUMMARY-R` inside the invocation window AND no open `R-` → the task goes
//!   `на ревью → готова к слиянию`.
//! * **Incomplete** (2.7) — no open `R-` but no fresh `SUMMARY-R` either (the reviewer was cut
//!   short, e.g. by `maxTurns`) → re-run the SAME reviewer; never hand the coder an empty list.
//!
//! # Relationship to the production adapter (T-026)
//!
//! This tree is the whole contract, and the production path
//! ([`crate::outcome_adapter::task_review_outcome`] + `headless::finish_task_review`) implements
//! the same three branches in the same order over the same parsed artifact. Two documented
//! differences follow from the extra inputs the production path has, and neither redefines a
//! branch:
//!
//! * **The `Clean` branch is strictly tighter there.** The engine's own reviewer prompt requires
//!   `review.md` to END with an exact `ИТОГ: готово к слиянию · открытых=0`, so the adapter needs
//!   that decodable completion tail on top of the freshness gate this resolver checks. An artifact
//!   this resolver calls `Clean` but that lacks the tail is **not** promoted there — it falls into
//!   `Incomplete` (a bounded re-run inside `REVIEW_LOOP_MAX`), never into a terminal escalation.
//!   `Incomplete` is therefore WIDER on the production path than here, never narrower, and the
//!   difference can only cost a repeat pass, never a merge authorization.
//! * **Escalation has no analogue here at all**, because its inputs are not in [`ReviewParse`]:
//!   a supervision failure (timeout/crash/cancel), an explicit `ИТОГ: эскалация codex`, or an
//!   undecodable positive claim by a reviewer that did finish (unknown `ИТОГ:` verdict word,
//!   malformed `Риск-повышен:` marker). A pure function over the artifact cannot observe any of
//!   them, so nothing in this file is expected to produce them.
//!
//! Before T-026 the production adapter instead escalated the task terminally for three artifact
//! shapes this resolver calls `Incomplete` (no tail at all, a `готово к слиянию` tail over a
//! stale/absent `SUMMARY-R`, an `открытые находки` tail with no open `R-`), which made
//! `ReviewOutcome::Incomplete` practically unreachable in production and burned a whole task on a
//! transient reviewer truncation. The branches are now one semantics with the one tightening
//! stated above.

use crate::contract::ReviewParse;

/// The three-way phase-2.6/2.7/2.8 review-gate branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewGate {
    /// 2.6 — fresh `SUMMARY-R` and no open `R-`: clean pass, promote to `готова к слиянию`.
    Clean,
    /// 2.8 — open `R-` findings: dispatch a fix, then re-review.
    Findings,
    /// 2.7 — no open `R-` and no fresh `SUMMARY-R`: reviewer interrupted, re-run it unchanged.
    /// The production twin ([`crate::processor::ReviewOutcome::Incomplete`]) means exactly this
    /// and is spent against `REVIEW_LOOP_MAX`; see the module docs for the one shape it covers in
    /// addition (a clean-looking artifact without the required `ИТОГ:` completion tail).
    Incomplete,
}

/// Resolve the review-gate branch. `since` and `until` bound the exact UTC invocation window for
/// `SUMMARY-R`. Open `R-` findings take precedence (2.8); else a clean in-window pass (2.6, via
/// `is_clean_pass`); else the pass is incomplete (2.7).
///
/// The `ИТОГ:` tail is deliberately NOT an input: this is a function of the durable artifact, and
/// the reviewer's report protocol is decoded by [`crate::outcome_adapter::task_review_outcome`],
/// which layers it on top of this exact branch order (see the module docs, T-026).
pub fn review_gate(parse: &ReviewParse, since: &str, until: &str) -> ReviewGate {
    if !parse.open_review_findings().is_empty() {
        ReviewGate::Findings
    } else if parse.is_clean_pass(since, until) {
        ReviewGate::Clean
    } else {
        ReviewGate::Incomplete
    }
}

/// The **integration-review** gate (`agents/processor.md` phase 5.2): the same three-way branch as
/// [`review_gate`], but over the batch-level integration findings (`F-` / `SUMMARY-F`) instead of
/// the per-task ones (`R-` / `SUMMARY-R`). Open `F-` findings take precedence; else a fresh clean
/// pass (a `SUMMARY-F` newer than `since` and no open `F-`); else the pass is incomplete (the
/// `full_reviewer` was cut short — re-run it unchanged, never fabricate an empty fix list).
pub fn integration_gate(parse: &ReviewParse, since: &str, until: &str) -> ReviewGate {
    if !parse.open_integration_findings().is_empty() {
        ReviewGate::Findings
    } else if parse.is_clean_integration_pass(since, until) {
        ReviewGate::Clean
    } else {
        ReviewGate::Incomplete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::parse_review;

    const SINCE: &str = "2026-07-10T17:00:00Z";
    const UNTIL: &str = "2026-07-10T19:00:00Z";

    #[test]
    fn open_findings_take_precedence() {
        // Open `R-` → Findings, even alongside a fresh SUMMARY-R.
        let dirty = "### [R-02] still broken — статус: новая\n";
        assert_eq!(
            review_gate(&parse_review(dirty), SINCE, UNTIL),
            ReviewGate::Findings
        );
        let with_summary = "\
### [R-02] still broken — статус: новая\n\
### [SUMMARY-R-2026-07-10T18:00:00Z] Итог — статус: готово к слиянию\n";
        assert_eq!(
            review_gate(&parse_review(with_summary), SINCE, UNTIL),
            ReviewGate::Findings
        );
    }

    #[test]
    fn fresh_summary_no_open_is_clean() {
        let clean = "\
### [R-01] fixed — статус: исправлено\n\
### [SUMMARY-R-2026-07-10T18:00:00Z] Итог — статус: готово к слиянию\n";
        assert_eq!(
            review_gate(&parse_review(clean), SINCE, UNTIL),
            ReviewGate::Clean
        );
    }

    #[test]
    fn stale_summary_is_incomplete_not_clean() {
        // A SUMMARY-R older than `since` is not a fresh clean pass (phase 2.6 freshness rule) and
        // there are no open findings → the pass is incomplete (2.7), re-run the reviewer.
        let stale = "### [SUMMARY-R-2026-07-10T16:00:00Z] Итог — статус: готово к слиянию\n";
        assert_eq!(
            review_gate(&parse_review(stale), SINCE, UNTIL),
            ReviewGate::Incomplete
        );
    }

    #[test]
    fn no_summary_no_findings_is_incomplete() {
        // Reviewer interrupted before writing anything actionable (e.g. maxTurns) → 2.7.
        let empty = "# review\n(no markers yet)\n";
        assert_eq!(
            review_gate(&parse_review(empty), SINCE, UNTIL),
            ReviewGate::Incomplete
        );
        // A resolved (`исправлено`) finding is NOT open, so with no fresh summary it is 2.7 too.
        let resolved = "### [R-01] done — статус: исправлено\n";
        assert_eq!(
            review_gate(&parse_review(resolved), SINCE, UNTIL),
            ReviewGate::Incomplete
        );
    }

    #[test]
    fn integration_gate_mirrors_review_gate_over_f_findings() {
        // Open F- → Findings, even with a fresh SUMMARY-F.
        let dirty = "\
### [F-02] build break — статус: новая\n\
### [SUMMARY-F-2026-07-10T18:00:00Z] Итог — статус: готово к слиянию\n";
        assert_eq!(
            integration_gate(&parse_review(dirty), SINCE, UNTIL),
            ReviewGate::Findings
        );
        // Fresh SUMMARY-F, no open F- → Clean.
        let clean = "\
### [F-01] fixed — статус: исправлено\n\
### [SUMMARY-F-2026-07-10T18:00:00Z] Итог — статус: готово к слиянию\n";
        assert_eq!(
            integration_gate(&parse_review(clean), SINCE, UNTIL),
            ReviewGate::Clean
        );
        // No fresh SUMMARY-F, no open F- (interrupted full_reviewer) → Incomplete.
        let stale = "### [SUMMARY-F-2026-07-10T16:00:00Z] Итог — статус: готово к слиянию\n";
        assert_eq!(
            integration_gate(&parse_review(stale), SINCE, UNTIL),
            ReviewGate::Incomplete
        );
        // A per-task SUMMARY-R must NOT satisfy the integration gate.
        let cross = "### [SUMMARY-R-2026-07-10T18:00:00Z] Итог — статус: готово к слиянию\n";
        assert_eq!(
            integration_gate(&parse_review(cross), SINCE, UNTIL),
            ReviewGate::Incomplete
        );
    }
}
