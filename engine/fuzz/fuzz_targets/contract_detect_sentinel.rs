//! Fuzz `orchestrail_engine::contract::detect_sentinel`: whole-token detection of the Codex
//! adapter sentinels (`CODEX_UNAVAILABLE`, `CODEX_FAILED`, `ЭСКАЛАЦИЯ codex: ...`) inside
//! untrusted leaf-agent report text.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::contract::detect_sentinel(data);
});
