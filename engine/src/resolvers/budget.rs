//! Resolver — the cohort **budget / circuit-breaker gate** (`agents/processor.md` "Роллинг-приём
//! когорты", Фаза 2.0/2.9, and the `COHORT_BUDGET_SEC` circuit-breaker of "Supervisor вызова
//! исполнителя").
//!
//! After a top-up round the processor decides whether to keep the cohort's admission open. Three
//! parsed config thresholds gate it — `COHORT_SIZE` (capacity), `COHORT_MAX_AGE` (admission
//! window), `COHORT_BUDGET_SEC` (a total wall-clock budget circuit-breaker) — weighed against the
//! already-read counters `Admitted всего`, the cohort's age, and its elapsed wall-clock. This
//! resolver ports that gate into a pure function.
//!
//! Its close reasons reuse the SAME `Приём: закрыт · причина=<…>` vocabulary as
//! [`super::admission::CloseReason`] (§13.2) — it never invents a new word. The wall-clock budget
//! is a *time* circuit-breaker, so on exhaustion it closes with the existing time reason
//! `COHORT_MAX_AGE` (the §13.2 vocabulary has no separate budget literal).

use super::admission::CloseReason;

/// The parsed size / age / wall-clock-budget thresholds a cohort's admission is gated on — the
/// config keys `COHORT_SIZE`, `COHORT_MAX_AGE`, `COHORT_BUDGET_SEC`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CohortThresholds {
    /// `COHORT_SIZE` — max tasks admitted into one cohort before admission closes (default
    /// `3 × MAX_PARALLEL`).
    pub size: u32,
    /// `COHORT_MAX_AGE` — minutes to keep admission open from the cohort's start (default 90).
    pub max_age_minutes: u64,
    /// `COHORT_BUDGET_SEC` — total wall-clock budget for the cohort in seconds; `None` disables
    /// the budget circuit-breaker. The config parser normalizes its user-facing `0` (no limit) to
    /// `None`, so a raw `Some(0)` here remains a fail-closed degenerate threshold.
    pub budget_sec: Option<u64>,
}

/// The already-read cohort counters the gate weighs against [`CohortThresholds`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CohortCounters {
    /// `Admitted всего` — how many tasks the cohort has admitted so far (across all waves).
    pub admitted_total: u32,
    /// Minutes elapsed since `Начало когорты`.
    pub age_minutes: u64,
    /// Seconds elapsed since the cohort started (for the `COHORT_BUDGET_SEC` circuit-breaker).
    pub elapsed_sec: u64,
}

/// The gate decision: keep admitting, or latch admission closed with a canonical reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionGate {
    /// Keep the cohort's admission open — no size/age/budget circuit-breaker has tripped.
    Continue,
    /// Latch `Приём: закрыт · причина=<reason>`.
    Close(CloseReason),
}

/// Resolve the cohort budget / circuit-breaker gate. Pure: a deterministic function of already-read
/// `counters` and parsed `thresholds`.
///
/// Priority follows the processor's textual order (size before the time limits): the cohort being
/// full (`COHORT_SIZE`) is reported first; otherwise either the admission window (`COHORT_MAX_AGE`)
/// or the wall-clock budget (`COHORT_BUDGET_SEC`) elapsing closes admission with the time-based
/// `COHORT_MAX_AGE` reason. Only a `budget_sec` of `None` disables the budget circuit-breaker;
/// `Some(0)` is an already-exhausted, fail-closed boundary.
pub fn admission_gate(counters: CohortCounters, thresholds: CohortThresholds) -> AdmissionGate {
    // Size circuit-breaker: the cohort is full.
    if counters.admitted_total >= thresholds.size {
        return AdmissionGate::Close(CloseReason::CohortSize);
    }
    // Age circuit-breaker: the admission window has elapsed.
    if counters.age_minutes >= thresholds.max_age_minutes {
        return AdmissionGate::Close(CloseReason::CohortMaxAge);
    }
    // Wall-clock budget circuit-breaker: the cohort has spent its whole time budget -> stop
    // admitting, reported with the existing time-based reason. The config parser normalizes the
    // user-facing unlimited value `0` to `None`; an unnormalized `Some(0)` fails closed here.
    if let Some(budget) = thresholds.budget_sec
        && counters.elapsed_sec >= budget
    {
        return AdmissionGate::Close(CloseReason::CohortMaxAge);
    }
    AdmissionGate::Continue
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thresholds(size: u32, max_age_minutes: u64, budget_sec: Option<u64>) -> CohortThresholds {
        CohortThresholds {
            size,
            max_age_minutes,
            budget_sec,
        }
    }

    fn counters(admitted_total: u32, age_minutes: u64, elapsed_sec: u64) -> CohortCounters {
        CohortCounters {
            admitted_total,
            age_minutes,
            elapsed_sec,
        }
    }

    #[test]
    fn continues_while_under_every_threshold() {
        let t = thresholds(15, 90, Some(3600));
        assert_eq!(
            admission_gate(counters(4, 30, 1800), t),
            AdmissionGate::Continue
        );
        // Only None disables the budget and therefore never trips even at a huge elapsed.
        assert_eq!(
            admission_gate(counters(4, 30, 999_999), thresholds(15, 90, None)),
            AdmissionGate::Continue
        );
    }

    #[test]
    fn zero_wall_clock_budget_fails_closed() {
        assert_eq!(
            admission_gate(counters(4, 30, 0), thresholds(15, 90, Some(0))),
            AdmissionGate::Close(CloseReason::CohortMaxAge)
        );
        assert_eq!(
            admission_gate(counters(4, 30, u64::MAX), thresholds(15, 90, Some(0))),
            AdmissionGate::Close(CloseReason::CohortMaxAge)
        );
    }

    #[test]
    fn closes_on_cohort_size_at_capacity() {
        let t = thresholds(15, 90, None);
        assert_eq!(
            admission_gate(counters(15, 10, 600), t),
            AdmissionGate::Close(CloseReason::CohortSize)
        );
        assert_eq!(
            admission_gate(counters(16, 10, 600), t),
            AdmissionGate::Close(CloseReason::CohortSize)
        );
    }

    #[test]
    fn closes_on_cohort_max_age_when_window_elapsed() {
        let t = thresholds(15, 90, None);
        assert_eq!(
            admission_gate(counters(4, 90, 5400), t),
            AdmissionGate::Close(CloseReason::CohortMaxAge)
        );
        assert_eq!(
            admission_gate(counters(4, 120, 7200), t),
            AdmissionGate::Close(CloseReason::CohortMaxAge)
        );
    }

    #[test]
    fn closes_on_wall_clock_budget_exhaustion_with_time_reason() {
        // COHORT_BUDGET_SEC circuit-breaker: budget spent, still under size and age -> close with
        // the time-based COHORT_MAX_AGE reason (no separate budget literal in the §13.2 vocabulary).
        let t = thresholds(15, 90, Some(3600));
        assert_eq!(
            admission_gate(counters(4, 30, 3600), t),
            AdmissionGate::Close(CloseReason::CohortMaxAge)
        );
        assert_eq!(
            admission_gate(counters(4, 30, 4000), t),
            AdmissionGate::Close(CloseReason::CohortMaxAge)
        );
    }

    #[test]
    fn size_takes_precedence_over_time_limits_when_both_trip() {
        // Both full AND past the age window / budget: report COHORT_SIZE (processor's textual order).
        let t = thresholds(15, 90, Some(3600));
        assert_eq!(
            admission_gate(counters(15, 120, 4000), t),
            AdmissionGate::Close(CloseReason::CohortSize)
        );
    }
}
