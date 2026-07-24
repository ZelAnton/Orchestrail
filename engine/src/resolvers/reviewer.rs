//! Resolver 2b — **Codex reviewer (checker) routing**, independence invariant, and
//! **re-election after a fix** (`agents/processor.md`, "Codex-ревьюер (reviewer_codex) и
//! маршрутизация"; phases 2.4 / 2.5 / 2.8).
//!
//! `reviewer_codex` is an independent, read-only Codex review that lifts the review off the
//! expensive opus `reviewer` and adds model diversity. Its single load-bearing rule is
//! **independence**: the reviewer must be a *different mind* than the author of the range under
//! review. So the resolver keys on `implBy` — the **last** element of the ordered `Реализовано:`
//! history, i.e. the author of the last-committed range that a re-review actually covers:
//!
//! * a Codex-authored range → the base **Claude** reviewer (never `reviewer_codex`), whatever
//!   the flag says;
//! * a Claude-authored range → `reviewer_codex` (mode `full`) replaces the base for
//!   `coder_fast`/`coder`; for `coder_deep` under `deep`, a `reviewer_codex` **`augment`** pass
//!   runs BEFORE the base `reviewer`, which still owns the `SUMMARY-R` gate.
//!
//! Because `implBy` is the *last* history element, re-running the resolver after each fix
//! (`reelect_reviewer`) re-elects the reviewer when the range's author flips between cycles —
//! closing the self-review bypass where a `coder_codex` R-fix on a Claude-implemented task would
//! otherwise stay on `reviewer_codex` (Codex reviewing Codex). The decision is deterministic
//! from the persistent `Реализовано:` history, so it survives resume without chat/branch guesses.

use super::vocab::{BaseReviewer, ImplBy, Level};

/// The `CODEX_REVIEWER` routing flag (`config.example.md`: `off` / `fast` / `fast+std` / `deep`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexReviewer {
    Off,
    Fast,
    FastStd,
    Deep,
}

impl CodexReviewer {
    /// Parse a config value; unrecognized/empty → `None` (caller applies the `off` default).
    pub fn parse(value: &str) -> Option<CodexReviewer> {
        Some(match value.trim() {
            "off" => CodexReviewer::Off,
            "fast" => CodexReviewer::Fast,
            "fast+std" => CodexReviewer::FastStd,
            "deep" => CodexReviewer::Deep,
            _ => return None,
        })
    }
}

/// The typed inputs of the reviewer-routing decision for one entry into review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewerRouteInput {
    /// `CODEX_REVIEWER`.
    pub codex_reviewer: CodexReviewer,
    /// The base Claude tier from `tiering::base_reviewer` (keys `full`/`augment`, never rewritten).
    pub base: BaseReviewer,
    /// `Рекомендуемый исполнитель`.
    pub level: Level,
    /// The author of the range under review — the LAST element of the `Реализовано:` history.
    pub impl_by: ImplBy,
}

/// The reviewer-routing outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewerRoute {
    /// A single Claude reviewer (the base tier) owns the pass — either `CODEX_REVIEWER=off`,
    /// independence (a Codex-authored range), or a level outside the flag's set.
    Claude(BaseReviewer),
    /// `reviewer_codex` in mode `full` REPLACES the base Claude reviewer (`coder_fast`/`coder`,
    /// Claude-authored range). Its `ЭСКАЛАЦИЯ codex:` sentinel falls back to the same `base`.
    CodexFull,
    /// A diversity pass: `reviewer_codex` in mode `augment` runs BEFORE the base Claude reviewer,
    /// which still owns the `SUMMARY-R` gate (`coder_deep` under `CODEX_REVIEWER=deep`). Its
    /// sentinel is non-blocking — just skip the diversity pass.
    Augment(BaseReviewer),
}

/// Resolve `<ревьюер задачи>` for one entry into review, from the flag, the base tier, the level
/// and the author of the range under review. Pure over [`ReviewerRouteInput`].
pub fn route_reviewer(inp: &ReviewerRouteInput) -> ReviewerRoute {
    // `off` → the base Claude reviewer (current behavior).
    if inp.codex_reviewer == CodexReviewer::Off {
        return ReviewerRoute::Claude(inp.base);
    }
    // Independence: a Codex-authored range is reviewed by Claude, whatever the flag.
    if inp.impl_by == ImplBy::Codex {
        return ReviewerRoute::Claude(inp.base);
    }
    // A Claude-authored range: route by flag × level.
    match inp.codex_reviewer {
        CodexReviewer::Off => ReviewerRoute::Claude(inp.base), // unreachable (handled above)
        CodexReviewer::Fast => match inp.level {
            Level::CoderFast => ReviewerRoute::CodexFull,
            Level::Coder | Level::CoderDeep => ReviewerRoute::Claude(inp.base),
        },
        CodexReviewer::FastStd => match inp.level {
            Level::CoderFast | Level::Coder => ReviewerRoute::CodexFull,
            Level::CoderDeep => ReviewerRoute::Claude(inp.base),
        },
        CodexReviewer::Deep => match inp.level {
            // Same as fast+std for the shallower levels …
            Level::CoderFast | Level::Coder => ReviewerRoute::CodexFull,
            // … plus a diversity pass for coder_deep (Opus is augmented, not replaced).
            Level::CoderDeep => ReviewerRoute::Augment(inp.base),
        },
    }
}

/// Re-elect the reviewer from the *current* `Реализовано:` history (phase 2.8). `implBy` is the
/// **last** element — the author of the range the imminent re-review covers — so a flip in the
/// range's author between cycles re-elects the reviewer. An empty/unrecognized history cannot
/// confirm independence, so it routes to the base Claude reviewer (independence-preserving).
pub fn reelect_reviewer(
    codex_reviewer: CodexReviewer,
    base: BaseReviewer,
    level: Level,
    impl_history: &[ImplBy],
) -> ReviewerRoute {
    match impl_history.last().copied() {
        Some(impl_by) => route_reviewer(&ReviewerRouteInput {
            codex_reviewer,
            base,
            level,
            impl_by,
        }),
        None => ReviewerRoute::Claude(base),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parse() {
        assert_eq!(CodexReviewer::parse("off"), Some(CodexReviewer::Off));
        assert_eq!(
            CodexReviewer::parse("fast+std"),
            Some(CodexReviewer::FastStd)
        );
        assert_eq!(CodexReviewer::parse(" deep "), Some(CodexReviewer::Deep));
        assert_eq!(CodexReviewer::parse("on"), None);
    }

    fn inp(
        codex_reviewer: CodexReviewer,
        base: BaseReviewer,
        level: Level,
        impl_by: ImplBy,
    ) -> ReviewerRouteInput {
        ReviewerRouteInput {
            codex_reviewer,
            base,
            level,
            impl_by,
        }
    }

    /// `off` and independence both pin to the base tier, at every level and (for independence)
    /// under every non-off flag.
    #[test]
    fn off_and_independence_pin_to_base() {
        use BaseReviewer::*;
        use CodexReviewer::*;
        use ImplBy::*;
        use Level::*;
        // off → base, regardless of level / author.
        for level in [CoderFast, Coder, CoderDeep] {
            let base = if level == CoderFast {
                ReviewerStd
            } else {
                Reviewer
            };
            for author in [Claude, Codex] {
                assert_eq!(
                    route_reviewer(&inp(Off, base, level, author)),
                    ReviewerRoute::Claude(base)
                );
            }
        }
        // A Codex-authored range → base Claude reviewer, under every non-off flag/level.
        for flag in [Fast, FastStd, Deep] {
            for level in [CoderFast, Coder, CoderDeep] {
                let base = if level == CoderFast {
                    ReviewerStd
                } else {
                    Reviewer
                };
                assert_eq!(
                    route_reviewer(&inp(flag, base, level, Codex)),
                    ReviewerRoute::Claude(base),
                    "{flag:?} {} codex-authored",
                    level.as_str()
                );
            }
        }
    }

    /// Claude-authored range: flag × level routing table.
    #[test]
    fn claude_authored_routing_table() {
        use BaseReviewer::*;
        use CodexReviewer::*;
        use Level::*;
        let base_of = |l: Level| {
            if l == CoderFast {
                ReviewerStd
            } else {
                Reviewer
            }
        };
        let cases = [
            // fast → only coder_fast to reviewer_codex(full).
            (Fast, CoderFast, ReviewerRoute::CodexFull),
            (Fast, Coder, ReviewerRoute::Claude(Reviewer)),
            (Fast, CoderDeep, ReviewerRoute::Claude(Reviewer)),
            // fast+std → coder_fast and coder to reviewer_codex(full); coder_deep stays base.
            (FastStd, CoderFast, ReviewerRoute::CodexFull),
            (FastStd, Coder, ReviewerRoute::CodexFull),
            (FastStd, CoderDeep, ReviewerRoute::Claude(Reviewer)),
            // deep → as fast+std for fast/coder, plus an augment pass for coder_deep.
            (Deep, CoderFast, ReviewerRoute::CodexFull),
            (Deep, Coder, ReviewerRoute::CodexFull),
            (Deep, CoderDeep, ReviewerRoute::Augment(Reviewer)),
        ];
        for (flag, level, want) in cases {
            assert_eq!(
                route_reviewer(&inp(flag, base_of(level), level, ImplBy::Claude)),
                want,
                "{flag:?} × {}",
                level.as_str()
            );
        }
    }

    /// The phase-2.8 re-election matrix (`agents/processor.md`, "Матрица маршрутизации по
    /// диапазонам"): `CODEX_REVIEWER=fast+std`, `L=coder` (base `reviewer`). The reviewer flips
    /// with the author of the LAST committed range across fix cycles.
    #[test]
    fn reelection_matrix_last_author_wins() {
        use ImplBy::*;
        let base = BaseReviewer::Reviewer;
        let level = Level::Coder;
        let flag = CodexReviewer::FastStd;
        // Row 1 — primary success by coder_codex: range author = codex → base Claude reviewer.
        assert_eq!(
            reelect_reviewer(flag, base, level, &[Codex]),
            ReviewerRoute::Claude(base)
        );
        // Row 2 — primary fallback to Claude: author = Claude → reviewer_codex(full).
        assert_eq!(
            reelect_reviewer(flag, base, level, &[Claude]),
            ReviewerRoute::CodexFull
        );
        // Row 3 — successful R-fix by codex after a fallback: last author = codex → reviewer
        // (re-elected off reviewer_codex — the bypass this closes).
        assert_eq!(
            reelect_reviewer(flag, base, level, &[Claude, Codex]),
            ReviewerRoute::Claude(base)
        );
        // Row 4 — another fallback: last author = Claude → reviewer_codex(full), re-elected.
        assert_eq!(
            reelect_reviewer(flag, base, level, &[Claude, Codex, Claude]),
            ReviewerRoute::CodexFull
        );
        // Tiering is preserved: the same matrix at L=coder_fast keys on base reviewer_std.
        let stdbase = BaseReviewer::ReviewerStd;
        assert_eq!(
            reelect_reviewer(flag, stdbase, Level::CoderFast, &[Claude, Codex]),
            ReviewerRoute::Claude(stdbase)
        );
    }

    #[test]
    fn reelection_empty_history_is_independence_safe() {
        // No recorded author cannot confirm independence → base Claude reviewer.
        assert_eq!(
            reelect_reviewer(
                CodexReviewer::FastStd,
                BaseReviewer::Reviewer,
                Level::Coder,
                &[]
            ),
            ReviewerRoute::Claude(BaseReviewer::Reviewer)
        );
    }

    #[test]
    fn coder_deep_ranges_are_always_claude_authored() {
        // coder_deep is coded/fixed only by Claude, so its augment pass is never suppressed and
        // the base reviewer (Opus) always owns the gate.
        assert_eq!(
            reelect_reviewer(
                CodexReviewer::Deep,
                BaseReviewer::Reviewer,
                Level::CoderDeep,
                &[ImplBy::Claude]
            ),
            ReviewerRoute::Augment(BaseReviewer::Reviewer)
        );
    }
}
