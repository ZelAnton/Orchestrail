//! Fuzz `orchestrail_engine::config::parse`: `.work/config.md` `KEY: value` decode. A
//! human-editable file; malformed/duplicate/out-of-range values must fail closed with a
//! `ConfigError`, never panic or hang.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::config::parse(data);
});
