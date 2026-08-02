//! Fuzz `orchestrail_engine::contract::parse_review`: `review.md` / `review_integration.md`
//! body decode into `### [R-NN]`/`### [F-NN]`/`### [SUMMARY-R-...]` findings. Untrusted
//! reviewer-authored free text; a non-heading or malformed marker line is simply not collected,
//! never a panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = orchestrail_engine::contract::parse_review(data);
});
