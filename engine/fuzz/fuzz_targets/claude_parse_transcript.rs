//! Fuzz `orchestrail_engine::claude::parse_transcript`: the stream-json transcript a spawned
//! leaf-agent `claude` child prints on stdout. Fully untrusted — a hostile or truncated child
//! process output must never panic, hang, or allocate without bound; a structurally broken line
//! is simply skipped (see `parse_transcript`'s per-line `let Ok(...) else { continue }`).
//!
//! `&str` (not `&[u8]`) matches the function's own signature: `libfuzzer-sys`'s `Arbitrary` impl
//! for `&str` already discards a chunk of input that is not valid UTF-8 before the target body
//! runs, satisfying the "invalid UTF-8 is dropped before the call" branch of the task contract.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::claude::parse_transcript(data);
});
