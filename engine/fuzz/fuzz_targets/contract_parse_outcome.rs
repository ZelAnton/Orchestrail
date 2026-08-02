//! Fuzz `orchestrail_engine::contract::parse_outcome`: the terminal `ИТОГ: ...` line of a
//! leaf-agent report (task T-111). Untrusted model-authored free text; a missing/malformed
//! marker is a detectable `None`, never a panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::contract::parse_outcome(data);
});
