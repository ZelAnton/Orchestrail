//! Fuzz `orchestrail_engine::state::parse_queue`: `.work/Tasks_Queue.md` entries (§13.1
//! control-plane Markdown, human/agent-editable).
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::state::parse_queue(data);
});
