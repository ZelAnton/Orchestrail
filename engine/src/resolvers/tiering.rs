//! Resolver 1 — **review tier** (`agents/processor.md`, phase 2.4 / "Тиринг ревью и экономика
//! циклов").
//!
//! `REVIEWER_TIERING` (default on) spends the planner's difficulty/responsibility signal that
//! already lives in `Рекомендуемый исполнитель`: a `coder_fast` task is reviewed by the cheaper
//! sonnet/high **`reviewer_std`**, everything else by the opus/high **`reviewer`**. Turning the
//! tiering off pins every task to `reviewer`. This is the BASE Claude tier; the Codex reviewer
//! routing (`reviewer.rs`) keys its `full`/`augment` decisions on this base but never changes
//! the tier itself.

use super::vocab::{BaseReviewer, Level};

/// Resolve the base Claude reviewer tier from the tiering flag and the task's level.
///
/// * `REVIEWER_TIERING: false` → always `reviewer` (opus/high), regardless of level.
/// * `REVIEWER_TIERING: true` (default) → `reviewer_std` for `coder_fast`, `reviewer` for
///   `coder` / `coder_deep`.
pub fn base_reviewer(tiering_enabled: bool, level: Level) -> BaseReviewer {
    if !tiering_enabled {
        return BaseReviewer::Reviewer;
    }
    match level {
        Level::CoderFast => BaseReviewer::ReviewerStd,
        Level::Coder | Level::CoderDeep => BaseReviewer::Reviewer,
    }
}

/// Resolve the base Claude reviewer tier for each named review dimension of a task at `level`,
/// preserving name and dispatch order. It is the symmetric per-dimension extension of
/// [`base_reviewer`] the roster path (`resolvers::reviewer::ReviewerRoster`) needs: until an
/// upstream per-dimension tier roster exists, every dimension shares the task's single base tier,
/// so each returned tier equals `base_reviewer(tiering_enabled, level)` — a regression-parity
/// identity for the current single-dimension (`whole-diff`) path. It is the seam where a future
/// roster attaches a cheaper/specialist tier to one dimension WITHOUT changing the single-dimension
/// contract or [`base_reviewer`]'s own signature and behavior.
pub fn base_reviewers_for_dimensions<'a>(
    tiering_enabled: bool,
    level: Level,
    dimensions: impl IntoIterator<Item = &'a str>,
) -> Vec<(&'a str, BaseReviewer)> {
    let base = base_reviewer(tiering_enabled, level);
    dimensions
        .into_iter()
        .map(|dimension| (dimension, base))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every (tiering, level) cell of the phase-2.4 base-tier table.
    #[test]
    fn base_tier_table() {
        use BaseReviewer::*;
        use Level::*;
        let cases = [
            // tiering on: only coder_fast drops to reviewer_std.
            (true, CoderFast, ReviewerStd),
            (true, Coder, Reviewer),
            (true, CoderDeep, Reviewer),
            // tiering off: everything pins to reviewer.
            (false, CoderFast, Reviewer),
            (false, Coder, Reviewer),
            (false, CoderDeep, Reviewer),
        ];
        for (tiering, level, want) in cases {
            assert_eq!(
                base_reviewer(tiering, level),
                want,
                "tiering={tiering} level={}",
                level.as_str()
            );
        }
    }

    /// The per-dimension tier resolver is a regression-parity extension: every dimension gets the
    /// same tier `base_reviewer` returns for the task level, with names and order preserved.
    #[test]
    fn per_dimension_tiers_equal_the_single_base_tier() {
        use BaseReviewer::*;
        use Level::*;
        let dimensions = ["functionality", "security", "performance"];
        let cases = [
            (true, CoderFast, ReviewerStd),
            (true, Coder, Reviewer),
            (true, CoderDeep, Reviewer),
            (false, CoderFast, Reviewer),
            (false, Coder, Reviewer),
            (false, CoderDeep, Reviewer),
        ];
        for (tiering, level, want) in cases {
            let resolved = base_reviewers_for_dimensions(tiering, level, dimensions);
            let expected: Vec<(&str, BaseReviewer)> =
                dimensions.iter().map(|name| (*name, want)).collect();
            assert_eq!(
                resolved,
                expected,
                "tiering={tiering} level={}",
                level.as_str()
            );
            // And it never diverges from the single-dimension resolver it extends.
            assert_eq!(base_reviewer(tiering, level), want);
        }
    }
}
