//! Fuzz `orchestrail_engine::state::parse_integration`: `.work/integration_state.md` decode
//! (§13.3 join-barrier state).
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::state::parse_integration(data);
});
