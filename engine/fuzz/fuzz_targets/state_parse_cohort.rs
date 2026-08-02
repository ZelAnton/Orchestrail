//! Fuzz `orchestrail_engine::state::parse_cohort`: `.work/cohort_state.md` decode (§13.2
//! `Приём:` admission).
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::state::parse_cohort(data);
});
