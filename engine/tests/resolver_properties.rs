use orchestrail_engine::resolvers::{
    ActiveClass, ActiveTask, AdmissionGate, AdmissionOutcome, Candidate, CohortCounters,
    CohortThresholds, CycleDecision, Domain, Level, admission_gate, base_reviewer, plan_admission,
    review_cycle_decision,
};
use orchestrail_engine::state::DeliveryTarget;
use proptest::prelude::*;

fn domain() -> BoxedStrategy<Domain> {
    let glob = prop::sample::select(vec![
        "engine/src/**",
        "engine/tests/**",
        "tui/src/**",
        "docs/**",
        "Cargo.toml",
        "README.md",
        "engine/src/lib.rs",
        "engine/src/state/**",
    ])
    .prop_map(str::to_owned);

    prop_oneof![
        Just(Domain::unknown()),
        prop::collection::vec(glob, 0..4).prop_map(|globs| Domain::from_globs(&globs)),
    ]
    .boxed()
}

fn delivery_target() -> impl Strategy<Value = DeliveryTarget> {
    prop::sample::select(vec![DeliveryTarget::Current, DeliveryTarget::NextMajor])
}

fn active_class() -> impl Strategy<Value = ActiveClass> {
    prop::sample::select(vec![ActiveClass::Active, ActiveClass::Terminal])
}

fn admission_snapshot() -> BoxedStrategy<(Vec<Candidate>, Vec<ActiveTask>, usize)> {
    let candidates = prop::collection::vec((any::<bool>(), domain(), delivery_target()), 0..16)
        .prop_map(|raw| {
            raw.into_iter()
                .enumerate()
                .map(|(index, (ready, domain, delivery))| Candidate {
                    // Indexed ids make every generated descriptor uniquely addressable when the
                    // admitted ids are checked against the source snapshot below.
                    id: format!("T-{index}"),
                    ready,
                    domain,
                    delivery,
                })
                .collect()
        });
    let active = prop::collection::vec((domain(), active_class()), 0..8).prop_map(|raw| {
        raw.into_iter()
            .map(|(domain, class)| ActiveTask { domain, class })
            .collect()
    });

    (candidates, active, 0usize..9).boxed()
}

fn level() -> impl Strategy<Value = Level> {
    prop::sample::select(vec![Level::CoderFast, Level::Coder, Level::CoderDeep])
}

fn cycle_pair() -> BoxedStrategy<(u32, u32, u32)> {
    (0u32..u32::MAX, any::<u32>())
        .prop_flat_map(|(lower, limit)| {
            (Just(lower), Just(limit), 1u32..=u32::MAX - lower)
                .prop_map(|(lower, limit, increment)| (lower, lower + increment, limit))
        })
        .boxed()
}

fn budget_closure_case() -> BoxedStrategy<(CohortCounters, CohortThresholds)> {
    prop_oneof![
        // Size boundary / degenerate cases: zero is fail-closed, as are an exact limit and the
        // largest representable exact limit. Age and time cannot preempt this assertion.
        prop::sample::select(vec![(0, 0), (1, 1), (1, 2), (u32::MAX, u32::MAX)]).prop_map(
            |(size, admitted_total)| (
                CohortCounters {
                    admitted_total,
                    age_minutes: 0,
                    elapsed_sec: 0,
                },
                CohortThresholds {
                    size,
                    max_age_minutes: u64::MAX,
                    budget_sec: None,
                },
            )
        ),
        // Age boundary / degenerate cases, including zero and the largest representable value.
        prop::sample::select(vec![(0, 0), (1, 1), (1, 2), (u64::MAX, u64::MAX)]).prop_map(
            |(max_age_minutes, age_minutes)| (
                CohortCounters {
                    admitted_total: 0,
                    age_minutes,
                    elapsed_sec: 0,
                },
                CohortThresholds {
                    size: u32::MAX,
                    max_age_minutes,
                    budget_sec: None,
                },
            )
        ),
        // Wall-clock budget boundaries: zero is fail-closed regardless of elapsed time, as are an
        // exact enabled threshold, a value above it, and the largest representable exact limit.
        prop::sample::select(vec![
            (Some(0), 0),
            (Some(0), u64::MAX),
            (Some(1), 1),
            (Some(1), 2),
            (Some(u64::MAX), u64::MAX),
        ])
        .prop_map(|(budget_sec, elapsed_sec)| (
            CohortCounters {
                admitted_total: 0,
                age_minutes: 0,
                elapsed_sec,
            },
            CohortThresholds {
                size: u32::MAX,
                max_age_minutes: u64::MAX,
                budget_sec,
            },
        )),
    ]
    .boxed()
}

fn disabled_budget_case() -> impl Strategy<Value = (CohortCounters, CohortThresholds)> {
    Just(None).prop_map(|budget_sec| {
        (
            CohortCounters {
                admitted_total: 0,
                age_minutes: 0,
                elapsed_sec: u64::MAX,
            },
            CohortThresholds {
                size: u32::MAX,
                max_age_minutes: u64::MAX,
                budget_sec,
            },
        )
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    fn admission_never_overlaps_domains_or_exceeds_capacity(
        (candidates, active, capacity) in admission_snapshot(),
    ) {
        let outcome = plan_admission(&candidates, &active, capacity);
        if let AdmissionOutcome::Admitted(ids) = outcome {
            prop_assert!(ids.len() <= capacity);
            for (index, id) in ids.iter().enumerate() {
                let candidate = candidates
                    .iter()
                    .find(|candidate| candidate.id == *id)
                    .expect("admission result must name a generated candidate");
                for other_id in &ids[index + 1..] {
                    let other = candidates
                        .iter()
                        .find(|candidate| candidate.id == *other_id)
                        .expect("admission result must name a generated candidate");
                    prop_assert!(!candidate.domain.intersects(&other.domain));
                }
            }
        }
    }

    #[test]
    fn admission_is_deterministic(
        (candidates, active, capacity) in admission_snapshot(),
    ) {
        let first = plan_admission(&candidates, &active, capacity);
        let second = plan_admission(&candidates, &active, capacity);
        prop_assert_eq!(first, second);
    }

    #[test]
    fn tiering_is_total_and_stable(tiering_enabled in any::<bool>(), level in level()) {
        let first = base_reviewer(tiering_enabled, level);
        let second = base_reviewer(tiering_enabled, level);
        prop_assert_eq!(first, second);
    }

    #[test]
    fn review_cycle_closure_is_monotonic((lower, higher, limit) in cycle_pair()) {
        let lower_decision = review_cycle_decision(lower, limit);
        let higher_decision = review_cycle_decision(higher, limit);

        if matches!(lower_decision, CycleDecision::Escalate { .. }) {
            let higher_is_escalated = matches!(higher_decision, CycleDecision::Escalate { .. });
            prop_assert!(higher_is_escalated);
        }
    }

    #[test]
    fn budget_boundaries_fail_closed((counters, thresholds) in budget_closure_case()) {
        prop_assert!(matches!(
            admission_gate(counters, thresholds),
            AdmissionGate::Close(_)
        ));
    }

    #[test]
    fn disabled_budget_never_trips_from_elapsed_time(
        (counters, thresholds) in disabled_budget_case(),
    ) {
        prop_assert_eq!(admission_gate(counters, thresholds), AdmissionGate::Continue);
    }
}
