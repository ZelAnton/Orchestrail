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
}
