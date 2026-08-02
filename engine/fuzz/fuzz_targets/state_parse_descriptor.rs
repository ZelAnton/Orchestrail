//! Fuzz `orchestrail_engine::state::parse_descriptor`: `.work/tasks/<T-ID>/task.md` decode
//! (§13.1). `id` is not itself untrusted-input parsing surface here — the caller always derives
//! it from the already-validated (`is_task_id`) directory name and `parse_descriptor` only ever
//! `.to_string()`s it verbatim into the returned `Descriptor` — so it is held fixed and the
//! fuzzer explores the Markdown `text` body, which is where every field (`Статус:`,
//! `Предпосылки:`, `Конфликт-домен:`, `Риск:`, `Сеть:`/`Экосистема:`, `Реализовано:`, …) is
//! actually decoded.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let _ = orchestrail_engine::state::parse_descriptor("T-1", text);
});
