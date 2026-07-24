//! The processor's **per-task decision trees**, compiled into deterministic pure functions
//! (intent doc §4/§7; `agents/processor.md` phases 2.x).
//!
//! Today the one "soft" place in `agents/processor.md` is a leaf agent's free-text report —
//! `contract.rs` already makes that parseable. The *branching* the processor does on those
//! parsed facts (which reviewer, which executor, is the pass clean, has the loop run out) is
//! still prose. This module ports that judgment into small, total functions over typed inputs,
//! one family per resolver, each mirrored to the section of `processor.md` it compiles:
//!
//! * [`tiering`] — the base Claude reviewer tier (`REVIEWER_TIERING` × `Рекомендуемый
//!   исполнитель`; phase 2.4).
//! * [`coder`] — Codex coder (maker) routing: `CODEX_CODER` × level, the `Сеть:` network gate,
//!   and the KB `ENV_LIMIT` pitfall check (phases 2.2 / 2.8).
//! * [`reviewer`] — Codex reviewer (checker) routing, the maker/checker independence invariant,
//!   and re-election after a fix off the `Реализовано:` history (phases 2.4 / 2.5 / 2.8).
//! * [`gate`] — the clean / with-findings / incomplete review gate, reusing
//!   [`crate::contract::ReviewParse::is_clean_pass`] (phases 2.6 / 2.7 / 2.8).
//! * [`cycles`] — the `REVIEW_LOOP_MAX` review-cycle limit off the `Циклов-ревью` counter
//!   (phases 2.5 / 2.8).
//!
//! Beyond the per-task trees it also carries the batch/cohort-level judgment (`agents/planner.md`
//! "Выбор батча"; `agents/processor.md` "Роллинг-приём когорты"; intent doc §4/§7):
//!
//! * [`admission`] — cohort admission planning: readiness by prerequisite completion, packing of
//!   non-overlapping conflict-domains, and the three empty-result reasons.
//! * [`budget`] — the cohort budget / circuit-breaker gate (`COHORT_SIZE` / `COHORT_MAX_AGE` /
//!   `COHORT_BUDGET_SEC`), sharing [`admission::CloseReason`]'s §13.2 close vocabulary.
//!
//! **Pure by construction.** Every function is a deterministic transformation of already-parsed
//! inputs — no mutation, no I/O, no `claude`/`codex`/`state-tx`/`queue-tx` calls. Reading the
//! descriptor, scanning `.work/knowledge/`, and taking the `date -u` mark stay in the caller;
//! the `vocab` bridges (`Level::from_field`, `parse_impl_history`, …) turn the raw `task.md`
//! literals into the typed values these functions consume. The module is NOT wired into the
//! running orchestrator; it is the judgment layer a future engine and the equivalence difftest
//! (T-110) build on.

pub mod admission;
pub mod budget;
pub mod coder;
pub mod cycles;
pub mod gate;
pub mod reviewer;
pub mod tiering;
pub mod vocab;

pub use admission::{
    ActiveClass, ActiveTask, AdmissionOutcome, Candidate, CloseReason, Domain, EmptyReason,
    is_ready, plan_admission, unmet_prerequisites,
};
pub use budget::{AdmissionGate, CohortCounters, CohortThresholds, admission_gate};
pub use coder::{
    CoderRoute, CoderRouteInput, CodexCoder, Ecosystem, EnvLimitClass, NetworkNeed, StayClaude,
    network_need, route_coder,
};
pub use cycles::{
    AttemptSignature, CycleDecision, StagnationDecision, normalize_signature_text,
    review_cycle_decision, stagnation_decision,
};
pub use gate::{ReviewGate, integration_gate, review_gate};
pub use reviewer::{
    CodexReviewer, ReviewerRoute, ReviewerRouteInput, reelect_reviewer, route_reviewer,
};
pub use tiering::base_reviewer;
pub use vocab::{BaseReviewer, ImplBy, Level, Risk, last_impl, parse_impl_history};
