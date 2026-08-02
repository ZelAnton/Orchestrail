//! Fuzz `orchestrail_engine::contract::parse_changed_files`: the coder Mode-3 tail
//! `Изменённые файлы: a, b, c` inside untrusted leaf-agent report text. Absence is `None`, a
//! detectable condition, never a panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::contract::parse_changed_files(data);
});
