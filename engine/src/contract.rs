//! Deterministic, machine-readable parse of the STRUCTURED markers leaf agents already
//! emit. The intent doc calls this the real prerequisite (§7/§8.2): the one "soft" place
//! where `processor.md` interprets free text today is a leaf agent's report, and a
//! deterministic engine needs every such return to be parseable WITHOUT free-text
//! guessing. This module shows that is tractable for the markers already in use:
//!
//!   * review findings   `### [R-NN] title — статус: новая|исправлено|отклонено`
//!   * integration finds  `### [F-NN] title — статус: ...`
//!   * clean-pass summary `### [SUMMARY-R-<UTC ISO-8601>] Итог ревью задачи — статус: готово к слиянию`
//!   * Codex sentinels    `CODEX_UNAVAILABLE`, `CODEX_FAILED`, `ЭСКАЛАЦИЯ codex: ...`
//!   * coder Mode-3 tail  `Изменённые файлы: <list>`
//!   * terminal outcome   `ИТОГ: <verdict> · key=value · ...` (the LAST line of every leaf
//!     report; task T-111 adds it ADDITIVELY so the decision tree reads a deterministic
//!     verdict token instead of interpreting the prose that still sits above it — every
//!     marker above is parsed unchanged).
//!
//! The clean-pass GATE the processor applies (phase 2.6) is: a FRESH `SUMMARY-R` exists
//! AND there is no open (`статус: новая`) `R-` finding. Both halves are computed here as
//! pure functions over the review text — no model judgment involved.

use std::cmp::Ordering;

use crate::time::iso_chrono_cmp;
use crate::task_id::is_task_id;

/// A finding's lifecycle status (the four words the contract uses, plus a catch-all).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    New,      // новая
    Fixed,    // исправлено
    Rejected, // отклонено
    Ready,    // готово к слиянию (SUMMARY-R only)
    Other(String),
}

impl Status {
    fn parse(s: &str) -> Status {
        match s.trim() {
            "новая" => Status::New,
            "исправлено" => Status::Fixed,
            "отклонено" => Status::Rejected,
            "готово к слиянию" => Status::Ready,
            other => Status::Other(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub id: String, // e.g. "R-02", "F-01", "SUMMARY-R-2026-07-10T18:00:00Z"
    /// Stable heading text between the marker id and `статус:`. It is retained so a changed
    /// finding heading is not mistaken for the exact same stagnating finding merely because its
    /// monotonic `R-`/`F-` id was reused across a reviewer update.
    pub title: String,
    pub status: Status,
}

impl Finding {
    pub fn is_summary(&self) -> bool {
        self.id.starts_with("SUMMARY-R")
    }
    pub fn is_review(&self) -> bool {
        self.id.starts_with("R-")
    }
    pub fn is_integration(&self) -> bool {
        self.id.starts_with("F-")
    }
    /// The UTC ISO-8601 timestamp embedded in a SUMMARY-R id (after the `SUMMARY-R-` prefix).
    pub fn summary_timestamp(&self) -> Option<&str> {
        self.id.strip_prefix("SUMMARY-R-")
    }
    /// The integration-review clean-pass summary (`SUMMARY-F-<UTC>`), the join-barrier analogue
    /// of `SUMMARY-R` (`agents/processor.md` phase 5.2). Distinct prefix from `SUMMARY-R`, so a
    /// per-task summary never satisfies the integration gate and vice versa.
    pub fn is_integration_summary(&self) -> bool {
        self.id.starts_with("SUMMARY-F")
    }
    /// The UTC ISO-8601 timestamp embedded in a `SUMMARY-F` id (after the `SUMMARY-F-` prefix).
    pub fn integration_summary_timestamp(&self) -> Option<&str> {
        self.id.strip_prefix("SUMMARY-F-")
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReviewParse {
    pub findings: Vec<Finding>,
}

impl ReviewParse {
    /// The freshest SUMMARY-R finding (the latest by true chronological order, via
    /// `iso_chrono_cmp`). NOT a raw string compare: the id's timestamp carries an OPTIONAL
    /// fractional-seconds tail (`is_utc_timestamp` accepts `.d{1,3}`), for which "ISO-8601 sorts
    /// lexically" is FALSE — `…:00.5Z` would sort before the same second's `…:00Z` (`.` < `Z`)
    /// though it is 500 ms later, so a lexical `max_by` could pick the earlier of two same-second
    /// summaries.
    pub fn latest_summary(&self) -> Option<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.is_summary())
            .max_by(|a, b| {
                iso_chrono_cmp(
                    a.summary_timestamp().unwrap_or_default(),
                    b.summary_timestamp().unwrap_or_default(),
                )
            })
    }
    /// Open review findings = `R-` entries with status `новая`.
    pub fn open_review_findings(&self) -> Vec<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.is_review() && f.status == Status::New)
            .collect()
    }
    /// The processor's phase-2.6 clean gate, made deterministic: a `SUMMARY-R` that was emitted
    /// during this exact review invocation (strictly after `since` and not after `until`) AND no
    /// open `R-`. The upper bound is as important as the lower one: without it, a stale artifact
    /// containing an arbitrary far-future summary could authorize a later call that never wrote a
    /// new clean report.
    pub fn is_clean_pass(&self, since: &str, until: &str) -> bool {
        let summary_in_call_window = self
            .latest_summary()
            .and_then(|f| f.summary_timestamp())
            .map(|ts| {
                iso_chrono_cmp(ts, since) == Ordering::Greater
                    && iso_chrono_cmp(ts, until) != Ordering::Greater
            })
            .unwrap_or(false);
        summary_in_call_window && self.open_review_findings().is_empty()
    }

    /// The freshest `SUMMARY-F` (integration-review clean-pass summary), the latest by true
    /// chronological order via `iso_chrono_cmp` — the batch-level twin of
    /// [`Self::latest_summary`]
    /// (`Self::latest_summary`) and, like it, NOT a raw lexical compare: the optional
    /// fractional-seconds tail makes "ISO-8601 sorts lexically" false for same-second marks.
    pub fn latest_integration_summary(&self) -> Option<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.is_integration_summary())
            .max_by(|a, b| {
                iso_chrono_cmp(
                    a.integration_summary_timestamp().unwrap_or_default(),
                    b.integration_summary_timestamp().unwrap_or_default(),
                )
            })
    }
    /// Open integration findings = `F-` entries with status `новая`.
    pub fn open_integration_findings(&self) -> Vec<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.is_integration() && f.status == Status::New)
            .collect()
    }
    /// The processor's phase-5.2 integration clean gate. Its `SUMMARY-F` must likewise lie inside
    /// the exact `since..=until` invocation window and have no open `F-` findings. The
    /// batch-level twin of [`is_clean_pass`](Self::is_clean_pass).
    pub fn is_clean_integration_pass(&self, since: &str, until: &str) -> bool {
        let summary_in_call_window = self
            .latest_integration_summary()
            .and_then(|f| f.integration_summary_timestamp())
            .map(|ts| {
                iso_chrono_cmp(ts, since) == Ordering::Greater
                    && iso_chrono_cmp(ts, until) != Ordering::Greater
            })
            .unwrap_or(false);
        summary_in_call_window && self.open_integration_findings().is_empty()
    }
}

/// One task's line in `merge_report.md` (`agents/merger.md`, "Формат `merge_report.md`"): a task
/// is either `merged=<SHA>` (optionally `conflict-resolved`) or `quarantined=<причина>`. Parsed
/// deterministically so the engine's Phase 4.3 merge/quarantine decision reads the merger's own
/// report, never free text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    /// The branch merged into the integration branch; `sha` is its merge-commit SHA / change id.
    /// `conflict_resolved` marks a hand-stitched seam (the `conflict-resolved` suffix).
    Merged {
        sha: String,
        conflict_resolved: bool,
    },
    /// The branch was NOT merged (its code is absent from the integration branch).
    Quarantined { reason: String },
}

/// One `- [T-ID] merged=…|quarantined=…` line decoded from `merge_report.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeLine {
    pub id: String,
    pub outcome: MergeOutcome,
}

/// Parse `merge_report.md` into its per-task result lines (`agents/merger.md` format). Each result
/// is a bullet `- [T-ID] merged=<SHA>[ conflict-resolved]` or `- [T-ID] quarantined=<причина>`;
/// any other line (heading, `База:`, `Итоговая сборка …`) is ignored. Order preserved.
pub fn parse_merge_report(text: &str) -> Vec<MergeLine> {
    let mut out = Vec::new();
    for line in text.lines() {
        let l = line.trim();
        let Some(rest) = l.strip_prefix("- [") else {
            continue;
        };
        let Some(close) = rest.find(']') else {
            continue;
        };
        let id = rest[..close].trim().to_string();
        if !is_task_id(&id) {
            continue;
        }
        let after = rest[close + 1..].trim();
        if let Some(v) = after.strip_prefix("merged=") {
            let mut parts = v.split_whitespace();
            let sha = parts.next().unwrap_or("").to_string();
            if sha.is_empty() {
                continue;
            }
            let conflict_resolved = parts.any(|p| p == "conflict-resolved");
            out.push(MergeLine {
                id,
                outcome: MergeOutcome::Merged {
                    sha,
                    conflict_resolved,
                },
            });
        } else if let Some(v) = after.strip_prefix("quarantined=") {
            let reason = v.trim().to_string();
            out.push(MergeLine {
                id,
                outcome: MergeOutcome::Quarantined { reason },
            });
        }
    }
    out
}

/// Parse a review.md / review_integration.md body into its findings.
pub fn parse_review(text: &str) -> ReviewParse {
    let mut out = ReviewParse::default();
    for line in text.lines() {
        if let Some(f) = parse_heading(line) {
            out.findings.push(f);
        }
    }
    out
}

/// Parse one `### [ID] ... — статус: X` heading. Returns None for non-heading lines.
fn parse_heading(line: &str) -> Option<Finding> {
    let l = line.trim_start();
    if !l.starts_with("###") {
        return None;
    }
    let lb = l.find('[')?;
    let rb = l[lb + 1..].find(']')? + lb + 1;
    let id = l[lb + 1..rb].trim().to_string();
    if !is_marker_id(&id) {
        return None;
    }
    // status after the last "статус:"
    let p = l.rfind("статус:")?;
    let status_literal = l[p + "статус:".len()..].trim();
    if status_literal.is_empty() {
        return None;
    }
    let status = Status::parse(status_literal);
    let title = l[rb + 1..p]
        .trim()
        .trim_matches(['—', '-'])
        .trim()
        .to_string();
    Some(Finding { id, title, status })
}

fn is_marker_id(id: &str) -> bool {
    // `R-`/`F-` findings carry a monotonic, never-reused counter (agents/reviewer.template.md),
    // so over a long review cycle a legitimate id can be one digit (`R-9`) or three-plus
    // (`R-100`), not only the two-digit `R-NN` form. Accept one-or-more ASCII digits. This is a
    // deliberately narrow loosening — «≥1 digit», NOT «any suffix after the dash»: an empty
    // suffix (`R-`) or a non-digit one (`R-a1`) is still rejected, as is any id that does not
    // start with a known marker prefix. `SUMMARY-R-`/`SUMMARY-F-` still require a full UTC
    // timestamp and are unchanged.
    let digits = |rest: &str| !rest.is_empty() && rest.as_bytes().iter().all(u8::is_ascii_digit);
    id.strip_prefix("R-").is_some_and(digits)
        || id.strip_prefix("F-").is_some_and(digits)
        || id.strip_prefix("SUMMARY-R-").is_some_and(is_utc_timestamp)
        || id.strip_prefix("SUMMARY-F-").is_some_and(is_utc_timestamp)
}

/// Whether `timestamp` is the strict `YYYY-MM-DDTHH:MM:SS(.d{1,3})?Z` UTC shape a `SUMMARY-*`
/// marker id carries. Single-sourced with the event-outbox parser (`events/parse.rs`) on
/// [`crate::time::is_iso_utc`]; kept as a local alias so the gate's own vocabulary stays stable.
/// Fractional-second comparison is delegated to [`crate::time::iso_chrono_cmp`] from the same
/// shared module, keeping validation, normalization and ordering in one contract.
fn is_utc_timestamp(timestamp: &str) -> bool {
    crate::time::is_iso_utc(timestamp)
}

/// A Codex adapter sentinel found in a leaf-agent report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sentinel {
    Unavailable, // CODEX_UNAVAILABLE — fall back to Claude
    Failed,      // CODEX_FAILED — substantive failure
    Escalation,  // "ЭСКАЛАЦИЯ codex: ..."
}

/// Detect the FIRST Codex sentinel present (order: escalation, unavailable, failed).
/// These are whole-token contracts, not free text.
pub fn detect_sentinel(text: &str) -> Option<Sentinel> {
    if contains_phrase(text, "ЭСКАЛАЦИЯ codex:") {
        return Some(Sentinel::Escalation);
    }
    if contains_token(text, "CODEX_UNAVAILABLE") {
        return Some(Sentinel::Unavailable);
    }
    if contains_token(text, "CODEX_FAILED") {
        return Some(Sentinel::Failed);
    }
    None
}

fn contains_phrase(text: &str, phrase: &str) -> bool {
    text.match_indices(phrase)
        .any(|(start, _)| token_boundary_before(text, start))
}

fn contains_token(text: &str, token: &str) -> bool {
    text.match_indices(token).any(|(start, _)| {
        token_boundary_before(text, start) && token_boundary_after(text, start + token.len())
    })
}

fn token_boundary_before(text: &str, byte_index: usize) -> bool {
    text[..byte_index]
        .chars()
        .next_back()
        .is_none_or(|c| !c.is_alphanumeric() && c != '_')
}

fn token_boundary_after(text: &str, byte_index: usize) -> bool {
    text[byte_index..]
        .chars()
        .next()
        .is_none_or(|c| !c.is_alphanumeric() && c != '_')
}

/// Parse the coder Mode-3 tail `Изменённые файлы: a, b, c` into a file list. The mode-3
/// contract REQUIRES this line, so its absence is a distinct, detectable condition (None).
pub fn parse_changed_files(text: &str) -> Option<Vec<String>> {
    for line in text.lines() {
        let l = line.trim();
        if let Some(rest) = l.strip_prefix("Изменённые файлы:") {
            let files: Vec<String> = rest
                .split([',', ';'])
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            return (!files.is_empty()).then_some(files);
        }
    }
    None
}

/// A leaf agent's terminal outcome line `ИТОГ: <verdict>[ · key=value]...`.
///
/// Task T-111 adds this line to the END of every leaf-agent report (coder / reviewer /
/// full_reviewer / merger, plus the Codex variants). It is STRICTLY ADDITIVE: the prose
/// bullets and every other marker (`SUMMARY-R`/`R-NN`/`F-NN`, `merge_report.md` lines,
/// `Изменённые файлы:`, the Codex sentinels) are untouched and still parsed as before — this
/// line merely hands the processor's decision tree a deterministic verdict + fields so the
/// one soft place (interpreting the free-text report) no longer needs model judgment.
///
/// Grammar: the verdict phrase (may contain spaces) followed by zero or more ` · `-separated
/// `key=value` fields. The separator is ` · ` (space, U+00B7 MIDDLE DOT, space) — the same one
/// the queue-status line already uses. Field values never contain the separator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The verdict phrase, role-specific and open-ended on purpose (e.g. `готово`,
    /// `эскалация`, `готово к слиянию`, `открытые находки`, `слито всё`, `есть карантин`).
    /// Kept as a string (not an enum) so a new verdict never breaks the parse — the consumer
    /// interprets it per role, exactly as `Status::Other` catches unknown statuses.
    pub verdict: String,
    /// The ordered `key=value` fields after the verdict (e.g. `режим=1`, `открытых=0`,
    /// `сборка=ok`, `риск=high`). Order preserved; a segment without `=` yields an empty value.
    pub fields: Vec<(String, String)>,
}

impl Outcome {
    /// First value for `key`, if present (fields keep insertion order; first wins on dupes).
    pub fn field(&self, key: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// The ` · ` separator shared by the terminal outcome line and the queue-status line.
const OUTCOME_SEP: &str = " \u{00B7} ";

/// Parse the terminal `ИТОГ: ...` line out of a leaf-agent report. The contract puts it LAST,
/// so the LAST matching line wins — a quoted example higher up (docs, a cited prior report)
/// never shadows the real one. Returns `None` when no such line is present (a detectable
/// condition — a report that forgot its machine-readable tail, not a silent guess).
pub fn parse_outcome(text: &str) -> Option<Outcome> {
    let mut found: Option<Outcome> = None;
    for line in text.lines() {
        let l = line.trim();
        if let Some(rest) = l.strip_prefix("ИТОГ:") {
            let mut parts = rest.split(OUTCOME_SEP);
            let verdict = parts.next().unwrap_or("").trim().to_string();
            if verdict.is_empty() {
                continue;
            }
            let fields = parts
                .map(|seg| match seg.split_once('=') {
                    Some((k, v)) => (k.trim().to_string(), v.trim().to_string()),
                    None => (seg.trim().to_string(), String::new()),
                })
                .collect();
            found = Some(Outcome { verdict, fields });
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    const REVIEW_UNTIL: &str = "2026-07-11T00:00:00Z";
    const INTEGRATION_UNTIL: &str = "2026-07-13T00:00:00Z";

    const REVIEW_DIRTY: &str = "\
# review\n\
### [R-01] Race in cohort open — статус: исправлено\n\
- Файл: x\n\
### [R-02] Missing error handling — статус: новая\n\
- Файл: y\n";

    const REVIEW_CLEAN: &str = "\
# review\n\
### [R-01] Race in cohort open — статус: исправлено\n\
### [SUMMARY-R-2026-07-10T18:00:00Z] Итог ревью задачи — статус: готово к слиянию\n\
- Открытых проблем: 0\n";

    #[test]
    fn parses_findings_and_statuses() {
        let p = parse_review(REVIEW_DIRTY);
        assert_eq!(p.findings.len(), 2);
        assert_eq!(p.findings[0].id, "R-01");
        assert_eq!(p.findings[0].title, "Race in cohort open");
        assert_eq!(p.findings[0].status, Status::Fixed);
        assert_eq!(p.findings[1].status, Status::New);
        assert_eq!(p.open_review_findings().len(), 1);
    }

    #[test]
    fn dirty_pass_is_not_clean() {
        let p = parse_review(REVIEW_DIRTY);
        // No summary at all => never clean.
        assert!(!p.is_clean_pass("2026-07-10T17:00:00Z", REVIEW_UNTIL));
        assert!(p.latest_summary().is_none());
    }

    #[test]
    fn clean_pass_requires_fresh_summary_and_zero_open() {
        let p = parse_review(REVIEW_CLEAN);
        assert!(p.latest_summary().is_some());
        assert_eq!(p.open_review_findings().len(), 0);
        // Summary at 18:00 is newer than a review started at 17:00 => clean.
        assert!(p.is_clean_pass("2026-07-10T17:00:00Z", REVIEW_UNTIL));
        // But a STALE summary (review started at 19:00, after the summary) is NOT clean:
        // this is exactly the "fresh, not any historical SUMMARY-R" rule (phase 2.6).
        assert!(!p.is_clean_pass("2026-07-10T19:00:00Z", REVIEW_UNTIL));
    }

    #[test]
    fn open_finding_blocks_clean_even_with_summary() {
        let text = format!("{REVIEW_CLEAN}### [R-09] still broken — статус: новая\n");
        let p = parse_review(&text);
        assert_eq!(p.open_review_findings().len(), 1);
        assert!(!p.is_clean_pass("2026-07-10T17:00:00Z", REVIEW_UNTIL));
    }

    #[test]
    fn marker_ids_accept_one_or_more_digits() {
        // The counter behind R-/F- can legitimately be one digit (`R-9`) or three-plus
        // (`R-100`) over a long review cycle, not only the historical two-digit `R-NN`.
        for id in ["R-9", "R-42", "R-100", "F-9", "F-100"] {
            let text = format!("### [{id}] finding — статус: новая\n");
            let p = parse_review(&text);
            assert_eq!(p.findings.len(), 1, "{id} must parse as a valid marker id");
            assert_eq!(p.findings[0].id, id);
            assert_eq!(p.findings[0].status, Status::New);
        }
    }

    #[test]
    fn open_non_two_digit_finding_blocks_clean_pass() {
        // The fail-open bug this task closes: a three-digit open `R-100` was silently dropped by
        // the parser, so a fresh SUMMARY-R falsely cleared the gate. It must now block the pass.
        let text = format!("{REVIEW_CLEAN}### [R-100] regression — статус: новая\n");
        let p = parse_review(&text);
        assert_eq!(p.open_review_findings().len(), 1);
        assert_eq!(p.open_review_findings()[0].id, "R-100");
        assert!(!p.is_clean_pass("2026-07-10T17:00:00Z", REVIEW_UNTIL));

        // Same for the batch-level integration gate over a single-digit open `F-9`.
        let integ = "\
### [SUMMARY-F-2026-07-12T18:00:00Z] Итог интеграционного ревью — статус: готово к слиянию\n\
### [F-9] build break — статус: новая\n";
        let p = parse_review(integ);
        assert_eq!(p.open_integration_findings().len(), 1);
        assert_eq!(p.open_integration_findings()[0].id, "F-9");
        assert!(!p.is_clean_integration_pass("2026-07-12T17:00:00Z", INTEGRATION_UNTIL));
    }

    #[test]
    fn summary_timestamps_compare_chronologically_not_lexically() {
        // `is_utc_timestamp` accepts an optional `.d{1,3}` fractional tail, so a SUMMARY id can be
        // `…:00.5Z`. Lexically `.` (0x2E) < `Z` (0x5A), so a raw string compare misranks a
        // fractional mark as EARLIER than the same second's whole-second mark — though it is
        // 500 ms later. This test pins the true chronological order; on the old lexical `ts > since`
        // / `max_by(cmp)` it fails.
        const SINCE: &str = "2026-07-10T18:00:00Z"; // second precision, as epoch_to_iso writes

        // (1) Freshness: a fresh SUMMARY-R with fractional seconds in the SAME second as `since`
        // must clear the gate — `…:00.5Z` is 500 ms after `…:00Z`. (Lexically it read as stale.)
        let frac = "### [SUMMARY-R-2026-07-10T18:00:00.5Z] Итог — статус: готово к слиянию\n";
        let p = parse_review(frac);
        assert!(
            p.latest_summary().is_some(),
            "fractional SUMMARY-R still a valid marker"
        );
        assert!(
            p.is_clean_pass(SINCE, "2026-07-10T18:00:01Z"),
            "a .5s summary in the `since` second is fresher than `since`, not stale"
        );

        // (2) Max selection: between two same-second summaries, the fractional one is the later.
        // Order them so a lexical `max_by` would wrongly pick the whole-second `…:00Z`.
        let two = "\
### [SUMMARY-R-2026-07-10T18:00:00.5Z] Итог — статус: готово к слиянию\n\
### [SUMMARY-R-2026-07-10T18:00:00Z] Итог — статус: готово к слиянию\n";
        let p = parse_review(two);
        assert_eq!(
            p.latest_summary().unwrap().summary_timestamp(),
            Some("2026-07-10T18:00:00.5Z"),
            "the .5s mark is chronologically the latest of the two"
        );

        // (3) The batch-level SUMMARY-F twin behaves identically (both gate pairs share the fix).
        const SINCE_F: &str = "2026-07-12T18:00:00Z";
        let frac_f = "### [SUMMARY-F-2026-07-12T18:00:00.5Z] Итог — статус: готово к слиянию\n";
        let pf = parse_review(frac_f);
        assert!(pf.latest_integration_summary().is_some());
        assert!(
            pf.is_clean_integration_pass(SINCE_F, "2026-07-12T18:00:01Z"),
            "a .5s SUMMARY-F in the `since` second is fresh, not stale"
        );
        let two_f = "\
### [SUMMARY-F-2026-07-12T18:00:00.5Z] Итог — статус: готово к слиянию\n\
### [SUMMARY-F-2026-07-12T18:00:00Z] Итог — статус: готово к слиянию\n";
        let pf = parse_review(two_f);
        assert_eq!(
            pf.latest_integration_summary()
                .unwrap()
                .integration_summary_timestamp(),
            Some("2026-07-12T18:00:00.5Z"),
            "the .5s SUMMARY-F is chronologically the latest of the two"
        );

        // Mixed sub-second widths normalize by decimal position, not digit count: `.05Z` (50 ms)
        // is EARLIER than `.5Z` (500 ms) even though it is lexically longer.
        let widths = "\
### [SUMMARY-R-2026-07-10T18:00:00.5Z] Итог — статус: готово к слиянию\n\
### [SUMMARY-R-2026-07-10T18:00:00.05Z] Итог — статус: готово к слиянию\n";
        let p = parse_review(widths);
        assert_eq!(
            p.latest_summary().unwrap().summary_timestamp(),
            Some("2026-07-10T18:00:00.5Z"),
            ".5s (500 ms) is later than .05s (50 ms) in the same second"
        );
    }

    #[test]
    fn marker_ids_reject_empty_or_non_digit_suffixes() {
        // The loosening is «≥1 digit», not «anything after the dash»: a bare/empty suffix and a
        // non-digit one stay invalid, as do ids without a known marker prefix and a SUMMARY-R/F
        // whose tail is not a UTC timestamp.
        for id in [
            "R-",
            "F-",
            "R-a1",
            "F-1a",
            "R-1.2",
            "X-01",
            "SUMMARY-R-nope",
            "note",
        ] {
            let text = format!("### [{id}] not a marker — статус: новая\n");
            let p = parse_review(&text);
            assert!(
                p.findings.is_empty(),
                "{id} must be rejected as a marker id"
            );
        }
    }

    #[test]
    fn sentinels_are_whole_token() {
        assert_eq!(detect_sentinel("all good"), None);
        assert_eq!(
            detect_sentinel("... CODEX_UNAVAILABLE, falling back"),
            Some(Sentinel::Unavailable)
        );
        assert_eq!(
            detect_sentinel("CODEX_FAILED — ENV_LIMIT"),
            Some(Sentinel::Failed)
        );
        // Escalation takes precedence when both appear.
        assert_eq!(
            detect_sentinel("ЭСКАЛАЦИЯ codex: sandbox-init; CODEX_FAILED"),
            Some(Sentinel::Escalation)
        );
    }

    #[test]
    fn changed_files_tail_parsed_and_absence_detected() {
        let report = "did the fix.\nИзменённые файлы: src/a.rs, src/b.rs ; tools/x.ps1\n";
        let files = parse_changed_files(report).unwrap();
        assert_eq!(files, vec!["src/a.rs", "src/b.rs", "tools/x.ps1"]);
        // A report missing the mandatory line is a detectable condition, not a guess.
        assert!(parse_changed_files("no tail here").is_none());
    }

    #[test]
    fn integration_findings_recognized() {
        let text = "### [F-01] build break after merge — статус: новая\n";
        let p = parse_review(text);
        assert!(p.findings[0].is_integration());
        assert_eq!(p.findings[0].status, Status::New);
    }

    #[test]
    fn integration_clean_pass_requires_fresh_summary_f_and_zero_open_f() {
        // A fresh SUMMARY-F with no open F- is a clean integration pass (phase 5.2 gate).
        let clean = "\
### [F-01] build break — статус: исправлено\n\
### [SUMMARY-F-2026-07-12T18:00:00Z] Итог интеграционного ревью — статус: готово к слиянию\n";
        let p = parse_review(clean);
        assert!(p.latest_integration_summary().is_some());
        assert_eq!(p.open_integration_findings().len(), 0);
        assert!(p.is_clean_integration_pass("2026-07-12T17:00:00Z", INTEGRATION_UNTIL));
        // A stale SUMMARY-F (review started AFTER it) is not a fresh clean pass.
        assert!(!p.is_clean_integration_pass("2026-07-12T19:00:00Z", INTEGRATION_UNTIL));
        // An open F- blocks the clean pass even with a fresh summary.
        let dirty = format!("{clean}### [F-09] still broken — статус: новая\n");
        let d = parse_review(&dirty);
        assert_eq!(d.open_integration_findings().len(), 1);
        assert!(!d.is_clean_integration_pass("2026-07-12T17:00:00Z", INTEGRATION_UNTIL));
    }

    #[test]
    fn summary_r_and_summary_f_do_not_cross_satisfy() {
        // A per-task SUMMARY-R never satisfies the integration gate, and a SUMMARY-F never the
        // per-task gate — the two summaries have distinct prefixes.
        let r =
            parse_review("### [SUMMARY-R-2026-07-12T18:00:00Z] Итог — статус: готово к слиянию\n");
        assert!(r.latest_summary().is_some());
        assert!(r.latest_integration_summary().is_none());
        assert!(!r.is_clean_integration_pass("2026-07-12T17:00:00Z", INTEGRATION_UNTIL));
        let f =
            parse_review("### [SUMMARY-F-2026-07-12T18:00:00Z] Итог — статус: готово к слиянию\n");
        assert!(f.latest_integration_summary().is_some());
        assert!(f.latest_summary().is_none());
        assert!(!f.is_clean_pass("2026-07-12T17:00:00Z", INTEGRATION_UNTIL));
    }

    #[test]
    fn merge_report_parses_merged_and_quarantined_lines() {
        let text = "\
# Merge Report — Batch B-1\n\
Интеграционная ветка: integration/B-1\n\
База: base-sha\n\
\n\
## Результаты\n\
- [T-101] merged=abc123\n\
- [T-102] merged=def456 conflict-resolved\n\
- [T-103] quarantined=сломала сборку интеграции\n\
\n\
Итоговая сборка интеграционной ветки: ok\n";
        let lines = parse_merge_report(text);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].id, "T-101");
        assert_eq!(
            lines[0].outcome,
            MergeOutcome::Merged {
                sha: "abc123".to_string(),
                conflict_resolved: false
            }
        );
        assert_eq!(
            lines[1].outcome,
            MergeOutcome::Merged {
                sha: "def456".to_string(),
                conflict_resolved: true
            }
        );
        assert_eq!(
            lines[2].outcome,
            MergeOutcome::Quarantined {
                reason: "сломала сборку интеграции".to_string()
            }
        );
    }

    #[test]
    fn merge_report_ignores_noise_and_malformed_lines() {
        let text = "\
# heading\n\
- [not-a-task] merged=x\n\
- [T-1abc] merged=x\n\
- [T-200] merged=\n\
- [T-201] неизвестно=y\n\
- [T-202] merged=ok\n";
        let lines = parse_merge_report(text);
        // Only the well-formed T-202 line survives (bad id, empty sha, unknown verb dropped).
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].id, "T-202");
    }

    #[test]
    fn coder_outcome_verdict_and_fields() {
        // A coder Mode-1 success with a risk elevation, exactly as coder.template.md prescribes.
        let report = "Реализовал контракт.\nИзменённые файлы: a.rs\nИТОГ: готово \u{00B7} режим=1 \u{00B7} риск=high\n";
        let o = parse_outcome(report).unwrap();
        assert_eq!(o.verdict, "готово");
        assert_eq!(o.field("режим"), Some("1"));
        assert_eq!(o.field("риск"), Some("high"));
        // Absent field is a detectable None, not a guess.
        assert_eq!(o.field("причина"), None);
    }

    #[test]
    fn escalation_outcome_carries_reason() {
        let report = "не сошлось.\nИТОГ: эскалация \u{00B7} режим=2 \u{00B7} причина=denylist\n";
        let o = parse_outcome(report).unwrap();
        assert_eq!(o.verdict, "эскалация");
        assert_eq!(o.field("причина"), Some("denylist"));
    }

    #[test]
    fn multiword_verdict_is_preserved() {
        // Reviewer / merger verdicts contain spaces; only ` · ` delimits the fields.
        let review =
            "прогон чист.\nИТОГ: готово к слиянию \u{00B7} прогонов=2 \u{00B7} открытых=0\n";
        let o = parse_outcome(review).unwrap();
        assert_eq!(o.verdict, "готово к слиянию");
        assert_eq!(o.field("прогонов"), Some("2"));
        assert_eq!(o.field("открытых"), Some("0"));

        let merge = "ИТОГ: слито всё \u{00B7} слито=3 \u{00B7} карантин=0 \u{00B7} сборка=ok\n";
        let m = parse_outcome(merge).unwrap();
        assert_eq!(m.verdict, "слито всё");
        assert_eq!(m.field("сборка"), Some("ok"));
    }

    #[test]
    fn last_outcome_line_wins() {
        // The contract puts the marker LAST; if two matching lines appear (e.g. an agent
        // restates a draft before the final one), the terminal line is authoritative.
        let report = "\
ИТОГ: эскалация \u{00B7} режим=1 \u{00B7} причина=draft\n\
Передумал, всё сошлось.\n\
ИТОГ: готово \u{00B7} режим=1\n";
        let o = parse_outcome(report).unwrap();
        assert_eq!(o.verdict, "готово");
        assert_eq!(o.field("причина"), None);
    }

    #[test]
    fn outcome_verdict_only_and_absence() {
        // Bare verdict, no fields.
        let o = parse_outcome("ИТОГ: открытые находки\n").unwrap();
        assert_eq!(o.verdict, "открытые находки");
        assert!(o.fields.is_empty());
        // A report with no terminal marker is a distinct, detectable condition.
        assert!(parse_outcome("just prose, no tail").is_none());
        // An empty verdict is not a valid outcome.
        assert!(parse_outcome("ИТОГ:").is_none());
    }
}
