//! Fuzz `orchestrail_engine::state::parse_batch`: `.work/batch.md` manifest decode (base,
//! integration branch, admitted tasks).
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::state::parse_batch(data);
});
