//! Resolver 4 — **review-cycle limit** (`agents/processor.md`, phases 2.5 / 2.8; `REVIEW_LOOP_MAX`).
//!
//! The processor caps how many review cycles a single task may run by the persistent
//! `Циклов-ревью: N` field (set to `1` at the first review in 2.5, incremented on entry to each
//! subsequent 2.8 — the field survives resume, so the cap is read off it, not off context). When
//! the count of the cycle about to run exceeds `REVIEW_LOOP_MAX`, the task escalates
//! `не сходится ревью после N циклов` instead of looping forever.
//!
//! This resolver is the pure comparison at that decision point; the same shape governs the
//! integration loop (`INTEGRATION_LOOP_MAX`) and the CI-fix loop (`CI_FIX_MAX`).
//!
//! # Stagnation detector (`STAGNATION_LIMIT`)
//!
//! A plain cycle counter only bounds how MANY attempts run — the same finding or CI error can
//! repeat verbatim until the budget is spent, burning expensive model runs with zero progress.
//! [`stagnation_decision`] is the early-exit sibling of [`review_cycle_decision`]: it fires on the
//! *lack of change* between consecutive attempts rather than on their count. Each R-/F-/CI attempt
//! is fingerprinted with an [`AttemptSignature`] (a compact, normalized hash of the finding/error
//! text and its evidence, via [`normalize_signature_text`]); when the same signature repeats,
//! unchanged, for `STAGNATION_LIMIT` attempts in a row the loop is declared stuck and escalates
//! EARLY — with a reason distinct from plain cycle exhaustion. A loop that keeps producing
//! *different* findings (real, if slow, progress) is never cut short. Both pieces are pure and
//! deterministic; the processor (`agents/processor.md`, phases 2.4–2.9, 5.2, 5.4) persists the
//! last signature and its consecutive-repeat count so the judgment survives resume, exactly as it
//! does for `Циклов-ревью`.

/// The review-cycle-limit decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleDecision {
    /// Under budget — run the cycle.
    Proceed,
    /// Over budget — escalate. `after_cycles` is the number of cycles already completed (the
    /// `N` in `не сходится ревью после N циклов`).
    Escalate { after_cycles: u32 },
}

impl CycleDecision {
    /// The canonical escalation reason literal, or `None` when proceeding.
    pub fn escalation_reason(&self) -> Option<String> {
        match self {
            CycleDecision::Escalate { after_cycles } => {
                Some(format!("не сходится ревью после {after_cycles} циклов"))
            }
            CycleDecision::Proceed => None,
        }
    }
}

/// Decide whether the review cycle numbered `cycle` (the current `Циклов-ревью` value for the
/// cycle about to run) may proceed under `limit` (`REVIEW_LOOP_MAX`). Escalate once the count
/// exceeds the limit — at that point `limit` cycles have already run without converging.
pub fn review_cycle_decision(cycle: u32, limit: u32) -> CycleDecision {
    if cycle > limit {
        CycleDecision::Escalate {
            after_cycles: cycle.saturating_sub(1),
        }
    } else {
        CycleDecision::Proceed
    }
}

// ============================================================================
// Attempt signature — the normalized fingerprint of one R/F/CI attempt.
// ============================================================================

/// Normalize a review-finding / CI-error text down to its **significant core**, so that two
/// reports of the *same* problem normalize identically despite cosmetic noise. Deterministic and
/// conservative — it drops only insignificant differences:
///
/// * whitespace is canonicalized — every run of Unicode whitespace (including newlines and tabs)
///   becomes a single ASCII space, and leading/trailing whitespace is trimmed;
/// * case is folded (`to_lowercase`), so a re-capitalized message is not a new problem;
/// * volatile timestamps are masked to the fixed token `<ts>`, since the *same* error logged at
///   two different moments must not read as two different errors. Two shapes are recognized:
///     - ISO-8601 date-times (`2026-07-08T09:31:07Z`, optional fractional seconds and `Z`/±offset,
///       date/time joined by `T` or a space), and
///     - bare wall-clock times (`09:31:07`, optional fractional seconds).
///
/// Nothing else is stripped: line/column numbers, identifiers, paths and message wording all
/// survive, so genuinely different findings keep distinct normalized forms (no false "same").
pub fn normalize_signature_text(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut masked = String::with_capacity(text.len());
    let mut idx = 0usize;
    let mut prev_is_digit = false;
    while idx < text.len() {
        // Timestamp components are pure ASCII, so a timestamp can only begin at an ASCII digit;
        // `prev_is_digit` keeps us from matching in the middle of a longer numeric run.
        if bytes[idx].is_ascii_digit()
            && !prev_is_digit
            && let Some(len) = timestamp_len(bytes, idx)
        {
            // Reject a partial match that is immediately followed by another digit (i.e. the
            // timestamp shape is really the head of a longer number).
            let after = idx + len;
            if after >= bytes.len() || !bytes[after].is_ascii_digit() {
                masked.push_str("<ts>");
                idx = after;
                prev_is_digit = false;
                continue;
            }
        }
        // Copy one whole UTF-8 char (idx is always on a char boundary here).
        let ch = text[idx..].chars().next().expect("idx on char boundary");
        masked.push(ch);
        prev_is_digit = ch.is_ascii_digit();
        idx += ch.len_utf8();
    }
    // Case-fold, then collapse all whitespace to single spaces (also trims the ends).
    masked
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// If a timestamp pattern begins at `bytes[start]`, return its byte length; else `None`.
fn timestamp_len(bytes: &[u8], start: usize) -> Option<usize> {
    iso_datetime_len(bytes, start).or_else(|| clock_len(bytes, start))
}

/// Consume exactly `n` ASCII digits from `bytes[i..]`, returning the index past them.
fn take_digits(bytes: &[u8], mut i: usize, n: usize) -> Option<usize> {
    for _ in 0..n {
        match bytes.get(i) {
            Some(b) if b.is_ascii_digit() => i += 1,
            _ => return None,
        }
    }
    Some(i)
}

/// `hh:mm:ss` with an optional `.fraction`, returning the index past it (or `None`).
fn clock_core(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = take_digits(bytes, start, 2)?;
    for _ in 0..2 {
        if bytes.get(i) != Some(&b':') {
            return None;
        }
        i = take_digits(bytes, i + 1, 2)?;
    }
    if bytes.get(i) == Some(&b'.') {
        let mut j = i + 1;
        while bytes.get(j).is_some_and(u8::is_ascii_digit) {
            j += 1;
        }
        if j > i + 1 {
            i = j; // only consume the dot when at least one fractional digit follows
        }
    }
    Some(i)
}

/// A trailing `Z`/`z` or `±hh:mm` / `±hhmm` zone, returning the index past it (or `start`).
fn zone_end(bytes: &[u8], start: usize) -> usize {
    match bytes.get(start) {
        Some(b'Z' | b'z') => start + 1,
        Some(b'+' | b'-') => {
            if let Some(i) = take_digits(bytes, start + 1, 2) {
                if bytes.get(i) == Some(&b':') {
                    if let Some(j) = take_digits(bytes, i + 1, 2) {
                        return j;
                    }
                } else if let Some(j) = take_digits(bytes, i, 2) {
                    return j;
                }
            }
            start
        }
        _ => start,
    }
}

/// `YYYY-MM-DD`(`T`|` `)`hh:mm:ss`(`.frac`)?(zone)? — full ISO-8601 date-time length, or `None`.
fn iso_datetime_len(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = take_digits(bytes, start, 4)?; // year
    for _ in 0..2 {
        if bytes.get(i) != Some(&b'-') {
            return None;
        }
        i = take_digits(bytes, i + 1, 2)?; // month, then day
    }
    match bytes.get(i) {
        Some(b'T' | b't' | b' ') => i += 1,
        _ => return None,
    }
    i = clock_core(bytes, i)?;
    Some(zone_end(bytes, i) - start)
}

/// A bare `hh:mm:ss`(`.frac`)? clock length, or `None`.
fn clock_len(bytes: &[u8], start: usize) -> Option<usize> {
    Some(clock_core(bytes, start)? - start)
}

/// 64-bit FNV-1a hash — a tiny, dependency-free, deterministic fingerprint primitive (stable
/// across platforms and runs, which is all the signature needs; not a cryptographic hash).
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// A compact fingerprint of one R/F/CI attempt: its finding/error text and evidence, each
/// normalized (see [`normalize_signature_text`]) and folded into one deterministic hash. It is the
/// unit the stagnation detector compares across consecutive attempts — equal signatures mean
/// "same problem, same evidence, no progress"; a change in either the wording OR the evidence
/// flips it, so slow-but-real progress is never mistaken for a stall.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttemptSignature(String);

impl AttemptSignature {
    /// Fingerprint an attempt from its message `text` alone (no separate evidence locus).
    pub fn of(text: &str) -> Self {
        Self::of_finding(text, "")
    }

    /// Fingerprint an attempt from its message `text` and its `evidence` — the concrete locus the
    /// finding/error points at (e.g. `file:line`, a snippet, the failing check name). Both are
    /// normalized independently and folded with a NUL separator (which cannot occur in the
    /// normalized text), so a changed message OR changed evidence yields a different signature.
    pub fn of_finding(text: &str, evidence: &str) -> Self {
        let folded = format!(
            "{}\u{0}{}",
            normalize_signature_text(text),
            normalize_signature_text(evidence)
        );
        Self(format!("{:016x}", fnv1a_64(folded.as_bytes())))
    }

    /// The compact fingerprint (16 lowercase hex chars) — what the processor persists as
    /// `Сигнатура-попытки` in the descriptor / integration state.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ============================================================================
// Stagnation detector — the early-exit sibling of the cycle-limit resolver.
// ============================================================================

/// The stagnation-detector decision. Where [`CycleDecision`] bounds how MANY attempts may run,
/// this bounds how many times the *same* attempt may repeat with no progress before the loop is
/// declared stuck.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagnationDecision {
    /// The latest attempt differs from the run so far, or there is not yet enough repetition —
    /// keep going (the plain cycle limit still applies).
    Progressing,
    /// The latest signature has repeated `repeats` times in a row (`>= STAGNATION_LIMIT`) with no
    /// change in finding or evidence — escalate EARLY instead of spending the remaining attempts
    /// on the identical problem.
    Stagnated { repeats: u32 },
}

impl StagnationDecision {
    /// The canonical early-escalation reason literal, or `None` while progressing. Distinct by
    /// construction from [`CycleDecision::escalation_reason`] (`не сходится ревью после N циклов`),
    /// so the two escalation causes stay observably separate in the descriptor, journal and outbox.
    pub fn escalation_reason(&self) -> Option<String> {
        match self {
            StagnationDecision::Stagnated { repeats } => Some(format!(
                "стагнация: та же находка без прогресса (повторов подряд: {repeats})"
            )),
            StagnationDecision::Progressing => None,
        }
    }

    /// Whether this decision is a stagnation (a convenience for the caller's early-exit branch).
    pub fn is_stagnated(&self) -> bool {
        matches!(self, StagnationDecision::Stagnated { .. })
    }
}

/// Decide whether an R/F/CI fix loop has **stagnated**: the latest attempt's signature
/// (`signatures.last()`) repeated, unchanged, for at least `limit` (`STAGNATION_LIMIT`) consecutive
/// attempts. Symmetric in shape to [`review_cycle_decision`] but orthogonal in meaning — it fires
/// on *lack of change* between attempts, not on their count, so an identical finding is caught
/// early (before `REVIEW_LOOP_MAX` / `INTEGRATION_LOOP_MAX` / `CI_FIX_MAX` would run out), while a
/// loop that keeps producing *different* findings is never cut short.
///
/// `signatures` is the ordered history of attempt fingerprints for one loop (oldest first); only
/// the trailing run of equal signatures is measured against `limit`. An empty history — or a
/// `limit` of 0, which is never a valid `STAGNATION_LIMIT` (the schema floors it at 2) — never
/// stagnates.
pub fn stagnation_decision(signatures: &[AttemptSignature], limit: u32) -> StagnationDecision {
    if limit == 0 {
        return StagnationDecision::Progressing;
    }
    let Some(last) = signatures.last() else {
        return StagnationDecision::Progressing;
    };
    let run = signatures.iter().rev().take_while(|&s| s == last).count();
    let repeats = u32::try_from(run).unwrap_or(u32::MAX);
    if repeats >= limit {
        StagnationDecision::Stagnated { repeats }
    } else {
        StagnationDecision::Progressing
    }
}

// ============================================================================
// Empty-fixed-set detector — the cheap, EARLY-EXIT sibling of the stagnation detector (T-014).
// ============================================================================

/// The empty-fixed-set decision (task T-014, `agents/processor.md` phases 2.5/2.8). Where
/// [`stagnation_decision`] needs the SAME finding to repeat across `STAGNATION_LIMIT` consecutive
/// REVIEW passes before it fires — and can be defeated by a reviewer that rewords the same finding
/// enough each round to change its [`AttemptSignature`] — this decision is evaluated once, right
/// after a SINGLE fix round, from the fixer's OWN report: task T-014's additive `не исправлено`
/// outcome field ([`crate::contract::Outcome::wont_fix`]) lets a Mode-2 fixer declare exactly which
/// of the round's open findings it declined to fix. When that declared count accounts for every
/// finding the round was dispatched to address, the round's fixed set was PROVABLY empty — no
/// need to spend another full review pass to rediscover the same stall via repetition. It is
/// additive and independent: it never replaces [`stagnation_decision`], which still runs
/// unconditionally on the review-signature history exactly as before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyFixedSetDecision {
    /// Either the fixer reported no won't-fix entries this round, or it accounted for fewer than
    /// all of the round's open findings — real progress (or at least not PROVABLY zero progress).
    Progressing,
    /// The fixer's won't-fix count accounts for every finding this round was dispatched to
    /// address: the fixed set was empty. The caller (`crate::outcome_adapter::task_leaf_outcome`,
    /// R-06) already validates `wont_fixed` before it reaches this decision — deduplicated, and,
    /// whenever the round's open-finding ids are known, bounded to ids that are actually members
    /// of THIS round's open-finding set — so in the common case `wont_fixed` can never exceed
    /// `open_findings` here. The one remaining exception is the additive/legacy degradation where
    /// that id set is unknown (a checkpoint predating R-06, or a leaf role that never runs a task
    /// fix cycle): there `wont_fixed` is deduplicated but unvalidated for membership, and MAY still
    /// exceed `open_findings` — that over-report is still treated as proof of zero progress, not a
    /// reason to keep going.
    Empty { wont_fixed: u32, open_findings: u32 },
}

impl EmptyFixedSetDecision {
    /// The canonical early-escalation reason literal, or `None` while progressing. Distinct by
    /// construction from both [`CycleDecision::escalation_reason`] and
    /// [`StagnationDecision::escalation_reason`], so all three escalation causes stay observably
    /// separate in the descriptor, journal and outbox.
    pub fn escalation_reason(&self) -> Option<String> {
        match self {
            EmptyFixedSetDecision::Empty {
                wont_fixed,
                open_findings,
            } => Some(format!(
                "пустой fixed-набор: фиксер не исправил ни одной находки в раунде (не исправлено: {wont_fixed} из {open_findings})"
            )),
            EmptyFixedSetDecision::Progressing => None,
        }
    }

    /// Whether this decision is an empty-fixed-set escalation (a convenience for the caller's
    /// early-exit branch, symmetric with [`StagnationDecision::is_stagnated`]).
    pub fn is_empty(&self) -> bool {
        matches!(self, EmptyFixedSetDecision::Empty { .. })
    }
}

/// Decide whether a single fix round left an EMPTY fixed set. `wont_fixed` is the count of
/// DISTINCT findings the fixer's `не исправлено` field named THIS round — validated by the caller
/// (R-06) against the round's actual open-finding ids whenever that set is known, so a stale,
/// duplicate, or otherwise unrelated id the fixer merely mentioned cannot inflate it; `open_findings`
/// is the count of findings (`ReviewOutcome::Findings`'s own count) the round was dispatched to
/// address. `open_findings == 0` never escalates (there was nothing to fix in the first place, a
/// structurally impossible but defensively-handled case) — nor does `wont_fixed == 0` (no
/// won't-fix field reported at all, the ordinary/legacy case this stays additive over).
pub fn empty_fixed_set_decision(wont_fixed: u32, open_findings: u32) -> EmptyFixedSetDecision {
    if open_findings > 0 && wont_fixed >= open_findings {
        EmptyFixedSetDecision::Empty {
            wont_fixed,
            open_findings,
        }
    } else {
        EmptyFixedSetDecision::Progressing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proceeds_up_to_and_including_the_limit() {
        // Default REVIEW_LOOP_MAX = 8: cycles 1..=8 run, the 9th escalates.
        for cycle in 1..=8 {
            assert_eq!(
                review_cycle_decision(cycle, 8),
                CycleDecision::Proceed,
                "cycle {cycle}"
            );
        }
        assert_eq!(
            review_cycle_decision(9, 8),
            CycleDecision::Escalate { after_cycles: 8 }
        );
    }

    #[test]
    fn escalation_reason_names_completed_cycle_count() {
        let d = review_cycle_decision(9, 8);
        assert_eq!(
            d.escalation_reason().as_deref(),
            Some("не сходится ревью после 8 циклов")
        );
        assert_eq!(review_cycle_decision(3, 8).escalation_reason(), None);
    }

    #[test]
    fn boundary_and_tight_limits() {
        // Exactly at the limit proceeds; one past escalates naming the limit as N.
        assert_eq!(review_cycle_decision(8, 8), CycleDecision::Proceed);
        // A tight limit of 1 allows only the first cycle.
        assert_eq!(review_cycle_decision(1, 1), CycleDecision::Proceed);
        assert_eq!(
            review_cycle_decision(2, 1),
            CycleDecision::Escalate { after_cycles: 1 }
        );
    }

    // -- Signature normalization (Этап 1: tested independently of the stagnation decision) -----

    #[test]
    fn normalize_collapses_whitespace_and_trims() {
        assert_eq!(
            normalize_signature_text("  R-01   missing\tnull\n  check "),
            "r-01 missing null check"
        );
        // Whitespace-only differences alone must not change the normalized core.
        assert_eq!(
            normalize_signature_text("a  b\tc"),
            normalize_signature_text("a b\nc")
        );
    }

    #[test]
    fn normalize_is_case_insensitive() {
        assert_eq!(
            normalize_signature_text("Unhandled Error In Parser"),
            normalize_signature_text("unhandled error in parser")
        );
    }

    #[test]
    fn normalize_masks_iso_timestamps() {
        // The same message logged at two different ISO-8601 moments normalizes identically.
        let a = normalize_signature_text("2026-07-08T09:31:07Z build failed: E0433");
        let b = normalize_signature_text("2026-07-09T22:04:55Z build failed: E0433");
        assert_eq!(a, b);
        assert_eq!(a, "<ts> build failed: e0433");
        // Fractional seconds and a numeric offset are absorbed too.
        assert_eq!(
            normalize_signature_text("at 2026-07-08 09:31:07.123456+02:00 boom"),
            normalize_signature_text("at 2026-01-01 00:00:00-05:00 boom")
        );
    }

    #[test]
    fn normalize_masks_bare_clock_times() {
        assert_eq!(
            normalize_signature_text("test hung at 09:31:07 in worker"),
            normalize_signature_text("test hung at 23:59:59 in worker")
        );
    }

    #[test]
    fn normalize_keeps_genuinely_different_findings_distinct() {
        // Different line numbers / identifiers are significant — not masked away.
        assert_ne!(
            normalize_signature_text("error at file.rs:12"),
            normalize_signature_text("error at file.rs:13")
        );
        assert_ne!(
            normalize_signature_text("missing null check"),
            normalize_signature_text("missing bounds check")
        );
        // A short "n:n:n" that is not a real hh:mm:ss clock stays untouched (single digits).
        assert_eq!(normalize_signature_text("ratio 1:2:3"), "ratio 1:2:3");
    }

    #[test]
    fn signature_is_stable_and_compact() {
        let s = AttemptSignature::of("some finding");
        assert_eq!(s.as_str().len(), 16);
        assert!(s.as_str().chars().all(|c| c.is_ascii_hexdigit()));
        // Deterministic: same input -> same fingerprint.
        assert_eq!(s, AttemptSignature::of("some finding"));
    }

    #[test]
    fn signature_ignores_cosmetic_noise_but_not_content() {
        assert_eq!(
            AttemptSignature::of("2026-07-08T09:31:07Z  Missing NULL check"),
            AttemptSignature::of("2026-07-09T10:10:10Z missing null check")
        );
        assert_ne!(
            AttemptSignature::of("missing null check"),
            AttemptSignature::of("missing bounds check")
        );
    }

    #[test]
    fn signature_folds_in_evidence() {
        // Same finding text, different evidence locus -> different signature.
        assert_ne!(
            AttemptSignature::of_finding("missing null check", "file.rs:12"),
            AttemptSignature::of_finding("missing null check", "file.rs:40")
        );
        // Same text and same evidence -> equal.
        assert_eq!(
            AttemptSignature::of_finding("missing null check", "file.rs:12"),
            AttemptSignature::of_finding("missing null check", "file.rs:12")
        );
        // Evidence and text are not interchangeable across the fold boundary.
        assert_ne!(
            AttemptSignature::of_finding("ab", "c"),
            AttemptSignature::of_finding("a", "bc")
        );
    }

    // -- Stagnation decision (Этап 3: repeat / change / exact-threshold transitions) -----------

    fn sig(text: &str) -> AttemptSignature {
        AttemptSignature::of(text)
    }

    #[test]
    fn stagnation_different_signatures_in_a_row_is_progress() {
        // Default STAGNATION_LIMIT = 2: three DIFFERENT findings back to back is progress.
        let hist = [sig("finding a"), sig("finding b"), sig("finding c")];
        assert_eq!(
            stagnation_decision(&hist, 2),
            StagnationDecision::Progressing
        );
    }

    #[test]
    fn stagnation_repeat_below_limit_is_progress() {
        // One occurrence (the first time a finding appears) is below the default limit of 2.
        let hist = [sig("finding a")];
        assert_eq!(
            stagnation_decision(&hist, 2),
            StagnationDecision::Progressing
        );
        // A prior different finding then the current one once = trailing run of 1 < 2.
        let hist = [sig("finding a"), sig("finding b")];
        assert_eq!(
            stagnation_decision(&hist, 2),
            StagnationDecision::Progressing
        );
    }

    #[test]
    fn stagnation_repeat_at_limit_stagnates() {
        // The same finding twice in a row hits the default limit of 2 -> stagnation.
        let hist = [sig("finding a"), sig("finding a")];
        assert_eq!(
            stagnation_decision(&hist, 2),
            StagnationDecision::Stagnated { repeats: 2 }
        );
        // A longer identical run reports the full trailing run length.
        let hist = [sig("finding a"), sig("finding a"), sig("finding a")];
        assert_eq!(
            stagnation_decision(&hist, 2),
            StagnationDecision::Stagnated { repeats: 3 }
        );
        // Only the TRAILING run counts: an earlier repeat that was then broken does not.
        let hist = [sig("finding a"), sig("finding a"), sig("finding b")];
        assert_eq!(
            stagnation_decision(&hist, 2),
            StagnationDecision::Progressing
        );
    }

    #[test]
    fn stagnation_changed_evidence_resets_the_run() {
        // Same finding wording, but the evidence locus moved between attempts (the coder relocated
        // the code) — that is progress, so the repeat run resets and does NOT stagnate.
        let hist = [
            AttemptSignature::of_finding("missing null check", "file.rs:12"),
            AttemptSignature::of_finding("missing null check", "file.rs:40"),
        ];
        assert_eq!(
            stagnation_decision(&hist, 2),
            StagnationDecision::Progressing
        );
        // Same wording AND same evidence twice in a row is a genuine stall.
        let hist = [
            AttemptSignature::of_finding("missing null check", "file.rs:12"),
            AttemptSignature::of_finding("missing null check", "file.rs:12"),
        ];
        assert!(stagnation_decision(&hist, 2).is_stagnated());
    }

    #[test]
    fn stagnation_reason_is_distinct_from_cycle_exhaustion() {
        let stall = StagnationDecision::Stagnated { repeats: 2 };
        let reason = stall.escalation_reason().expect("stagnation has a reason");
        assert!(reason.starts_with("стагнация:"));
        // It must not collide with the plain cycle-limit escalation reason.
        assert_ne!(
            Some(reason),
            review_cycle_decision(9, 8).escalation_reason()
        );
        assert_eq!(StagnationDecision::Progressing.escalation_reason(), None);
    }

    #[test]
    fn stagnation_edge_cases() {
        // Empty history never stagnates.
        assert_eq!(stagnation_decision(&[], 2), StagnationDecision::Progressing);
        // limit 0 (never a valid STAGNATION_LIMIT) is inert even on an identical run.
        let hist = [sig("a"), sig("a"), sig("a")];
        assert_eq!(
            stagnation_decision(&hist, 0),
            StagnationDecision::Progressing
        );
        // A tight limit of 1 stagnates on the first occurrence (degenerate but consistent).
        assert_eq!(
            stagnation_decision(&[sig("a")], 1),
            StagnationDecision::Stagnated { repeats: 1 }
        );
    }

    // -- Empty-fixed-set detector (T-014) -------------------------------------------------

    #[test]
    fn empty_fixed_set_fires_on_a_single_round_no_repeat_needed() {
        // The whole point: it fires from ONE round's own report, unlike stagnation which needs
        // STAGNATION_LIMIT consecutive repeats across separate review passes.
        assert_eq!(
            empty_fixed_set_decision(3, 3),
            EmptyFixedSetDecision::Empty {
                wont_fixed: 3,
                open_findings: 3,
            }
        );
        assert!(empty_fixed_set_decision(3, 3).is_empty());
    }

    #[test]
    fn empty_fixed_set_over_reporting_still_proves_zero_progress() {
        // A fixer that names MORE findings than were open (duplicate/stale ids) still accounts
        // for the whole open set — still proof of zero progress, not a reason to keep going.
        assert_eq!(
            empty_fixed_set_decision(5, 3),
            EmptyFixedSetDecision::Empty {
                wont_fixed: 5,
                open_findings: 3,
            }
        );
    }

    #[test]
    fn empty_fixed_set_partial_wont_fix_is_progress() {
        // Some findings were genuinely fixed (wont_fixed < open_findings): real progress, left
        // entirely to the ordinary review/stagnation path.
        assert_eq!(
            empty_fixed_set_decision(1, 3),
            EmptyFixedSetDecision::Progressing
        );
        assert!(!empty_fixed_set_decision(1, 3).is_empty());
    }

    #[test]
    fn empty_fixed_set_no_wont_fix_field_is_progress() {
        // The ordinary/legacy case this stays additive over: no `не исправлено` field at all.
        assert_eq!(
            empty_fixed_set_decision(0, 3),
            EmptyFixedSetDecision::Progressing
        );
    }

    #[test]
    fn empty_fixed_set_zero_open_findings_never_escalates() {
        // Structurally impossible (a fix round is only dispatched when there ARE open findings)
        // but defensively never escalates on a round with nothing to fix.
        assert_eq!(
            empty_fixed_set_decision(0, 0),
            EmptyFixedSetDecision::Progressing
        );
        assert_eq!(
            empty_fixed_set_decision(4, 0),
            EmptyFixedSetDecision::Progressing
        );
    }

    #[test]
    fn empty_fixed_set_reason_is_distinct_from_other_escalations() {
        let empty = EmptyFixedSetDecision::Empty {
            wont_fixed: 2,
            open_findings: 2,
        };
        let reason = empty.escalation_reason().expect("empty set has a reason");
        assert!(reason.starts_with("пустой fixed-набор:"));
        assert_ne!(
            Some(reason.clone()),
            review_cycle_decision(9, 8).escalation_reason()
        );
        assert_ne!(
            Some(reason),
            StagnationDecision::Stagnated { repeats: 2 }.escalation_reason()
        );
        assert_eq!(EmptyFixedSetDecision::Progressing.escalation_reason(), None);
    }
}
