//! Fuzz `orchestrail_engine::events::parse_line`: one `.work/events.jsonl` line, including an
//! arbitrarily torn tail (a crash mid-`Outbox::append` can leave a partially-written final
//! line). Envelope validation must reject anything malformed with a `ParseError`, never panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::events::parse_line(data);
});
