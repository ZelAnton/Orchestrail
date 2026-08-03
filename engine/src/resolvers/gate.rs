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
//! the same three branches in the same order over the same parsed artifact. Three documented
//! differences separate them: two follow from inputs the production path has and a pure function
//! over the artifact cannot, the third is a deliberate policy choice over evidence BOTH can see.
//! All three make production STRICTER, none reorders a branch, and none promotes anything this
//! resolver would not:
//!
//! * **The `Clean` branch is strictly tighter there.** The engine's own reviewer prompt requires
//!   `review.md` to END with an exact `ИТОГ: готово к слиянию · открытых=0`, so the adapter needs
//!   that decodable completion tail on top of the freshness gate this resolver checks. An artifact
//!   this resolver calls `Clean` but that lacks the tail is **not** promoted there — it falls into
//!   `Incomplete` (a bounded re-run inside `REVIEW_LOOP_MAX`). That direction only ever costs a
//!   repeat pass, never a merge authorization.
//! * **The `Incomplete` branch is tighter there in exactly one shape** (T-026, finding R-14): an
//!   artifact whose freshest `SUMMARY-R` is dated ABOVE the invocation window
//!   ([`ReviewParse::summary_after_window`]) escalates terminally in production instead of
//!   repeating. This resolver still calls it `Incomplete`, and correctly so for what it can see —
//!   the deviation is not about the artifact's CURRENT state, which really is "no proved clean
//!   pass", but about its FUTURE: `SUMMARY-R` entries are append-only by protocol, so a
//!   future-dated mark stays [`ReviewParse::latest_summary`]'s chronological maximum forever and
//!   no later pass can re-prove `is_clean_pass` — not even one whose reviewer honestly stamps a
//!   fresh, in-window summary. A repeat there is provably useless, and spending `REVIEW_LOOP_MAX`
//!   reviewer calls to discover that replaces a precise diagnosis with an anonymous «не сходится
//!   ревью». So `Incomplete` is WIDER in production for the tail-less shape above and NARROWER for
//!   this one; a caller of this resolver must not read `Incomplete` as "production will retry".
//! * **Escalation has no analogue here at all**, because its inputs are not in [`ReviewParse`]:
//!   a supervision failure (timeout/crash/cancel), an explicit `ИТОГ: эскалация codex`, or an
//!   undecodable positive claim by a reviewer that did finish (unknown `ИТОГ:` verdict word,
//!   malformed `Риск-повышен:` marker). A pure function over the artifact cannot observe any of
//!   them, so nothing in this file is expected to produce them. The future-dated-summary
//!   escalation above is the one case whose evidence IS visible here — it is a deliberate policy
//!   choice of the adapter, not a fact this resolver lacks, which is why it is named as a
//!   divergence rather than folded into this bullet.
//!
//! Before T-026 the production adapter escalated the task terminally for all FOUR of the artifact
//! shapes this resolver calls `Incomplete` (no tail at all, a `готово к слиянию` tail over an
//! absent/stale/future-dated `SUMMARY-R`, an `открытые находки` tail with no open `R-`), which made
//! `ReviewOutcome::Incomplete` practically unreachable in production and burned a whole task on a
//! transient reviewer truncation. The three CONVERGING shapes now repeat as this tree says; only
//! the non-convergent fourth kept its terminal treatment, with a reason that names it.

use crate::contract::ReviewParse;

/// The three-way phase-2.6/2.7/2.8 review-gate branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewGate {
    /// 2.6 — fresh `SUMMARY-R` and no open `R-`: clean pass, promote to `готова к слиянию`.
    Clean,
    /// 2.8 — open `R-` findings: dispatch a fix, then re-review.
    Findings,
    /// 2.7 — no open `R-` and no fresh `SUMMARY-R`: reviewer interrupted, re-run it unchanged.
    /// The production twin ([`crate::processor::ReviewOutcome::Incomplete`]) means the same and is
    /// spent against `REVIEW_LOOP_MAX`, but the two sets are not identical in either direction —
    /// see the module docs for the one shape it covers in ADDITION (a clean-looking artifact
    /// without the required `ИТОГ:` completion tail) and the one it EXCLUDES as terminal (a
    /// `SUMMARY-R` dated above the window, which no later pass can supersede).
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
