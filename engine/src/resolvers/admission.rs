//! Resolver family — **cohort admission planning** (`agents/planner.md` "Выбор батча";
//! intent doc §4/§7).
//!
//! `agents/planner.md` compiles, from the head of the queue, a cohort of not-started tasks that
//! are **ready** (all prerequisites done) and whose conflict-domains overlap NEITHER each other
//! NOR any already-active task's domain. This module ports that judgment into pure functions over
//! already-parsed inputs:
//!
//! * [`unmet_prerequisites`] / [`is_ready`] — readiness by prerequisite completion (the same
//!   semantics `tools/queue-tx.ps1 ready` gives textually: a candidate is ready iff every declared
//!   prerequisite is in the completed set; the unmet ones name the concrete blocker).
//! * [`Domain`] — a conflict-domain (a set of path globs) with a conservative overlap test.
//! * [`plan_admission`] — pack up to `capacity` ready, non-overlapping candidates; on an empty
//!   result, name one of the exactly-three planner reasons ([`EmptyReason`]).
//! * [`CloseReason`] — the shared `Приём: закрыт · причина=<…>` vocabulary (§13.2) both this
//!   planner (via [`EmptyReason::to_close_reason`]) and the budget gate ([`super::budget`]) speak.
//!
//! **Pure by construction.** Every function is a deterministic transformation of already-parsed
//! inputs — no I/O, no mutation, no `queue-tx`/`state-tx`/`claude`/`codex` calls. Reading the
//! queue/descriptors/`Tasks_Done.md`, deriving a fresh candidate's conflict-domain from task text,
//! and taking the wall-clock stay in the caller; these functions consume the typed result.

use std::collections::BTreeSet;

use crate::state::{DeliveryTarget, TaskState};

/// The declared prerequisites of `prerequisites` that are NOT yet in `completed` — the concrete
/// blockers, in declared order, deduplicated. Empty ⇒ ready. Mirrors `queue-tx ready` naming the
/// specific unfinished predecessor rather than a bare yes/no.
pub fn unmet_prerequisites(prerequisites: &[String], completed: &BTreeSet<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    prerequisites
        .iter()
        .filter(|p| !completed.contains(p.as_str()))
        .filter(|p| seen.insert((*p).clone()))
        .cloned()
        .collect()
}

/// A candidate is **ready** iff all its prerequisites are complete (in `completed`). The empty
/// prerequisite list is trivially ready.
pub fn is_ready(prerequisites: &[String], completed: &BTreeSet<String>) -> bool {
    prerequisites.iter().all(|p| completed.contains(p.as_str()))
}

/// Split and normalize a conflict-domain specification into its non-empty glob tokens.
/// Descriptor admission uses this same tokenizer so validation cannot disagree with the
/// resolver about newline-separated domains.
pub(crate) fn domain_tokens(spec: &str) -> impl Iterator<Item = &str> {
    spec.split([',', ' ', '\t', '\n', '\r'])
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

/// A single glob reduced to a conservative path matcher for overlap tests.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Matcher {
    /// The whole pattern is a literal path (no wildcard): matches exactly this path.
    Exact(String),
    /// The pattern carried a wildcard: matches any path with this literal prefix (the characters
    /// before the first wildcard). A raw string prefix — conservative, it never *under*-matches.
    Prefix(String),
}

/// Reduce one glob to its matcher: everything before the first `*`/`?`/`[` metacharacter is the
/// literal prefix; a pattern with no metacharacter is an exact path.
fn matcher(pattern: &str) -> Matcher {
    let p = pattern.trim();
    match p.find(['*', '?', '[']) {
        Some(i) => Matcher::Prefix(p[..i].to_string()),
        None => Matcher::Exact(p.to_string()),
    }
}

/// Whether `ancestor` names `path` itself or a path component ancestor of it.
///
/// A bare conflict-domain can name either a file or a directory. Treating it as a potential
/// directory makes the matcher conservative without confusing similarly named paths such as
/// `engine/src` and `engine/src-old`.
fn is_path_ancestor_or_same(ancestor: &str, path: &str) -> bool {
    path == ancestor
        || path
            .strip_prefix(ancestor)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

/// Do two matchers possibly cover a common path? Conservative (prefers a false *overlap* over a
/// false *disjoint*, matching planner's "при сомнении делай домен шире"):
/// * a bare path can be a file or a directory, so two exacts overlap when either is a
///   component-boundary ancestor of the other;
/// * an exact overlaps a prefix when the exact could be inside the prefix or the prefix could be
///   inside the exact's potential directory;
/// * two prefixes overlap iff one is a string-prefix of the other (both describe "all paths under
///   X", so they share paths iff one prefix contains the other).
fn matchers_overlap(a: &Matcher, b: &Matcher) -> bool {
    match (a, b) {
        (Matcher::Exact(x), Matcher::Exact(y)) => {
            is_path_ancestor_or_same(x, y) || is_path_ancestor_or_same(y, x)
        }
        (Matcher::Exact(x), Matcher::Prefix(p)) | (Matcher::Prefix(p), Matcher::Exact(x)) => {
            x.starts_with(p.as_str()) || is_path_ancestor_or_same(x, p)
        }
        (Matcher::Prefix(x), Matcher::Prefix(y)) => {
            x.starts_with(y.as_str()) || y.starts_with(x.as_str())
        }
    }
}

/// A conflict-domain: the set of path globs a task is expected to touch (`Конфликт-домен`).
/// `unknown` is distinct from a known empty set: it intersects every domain so callers cannot
/// accidentally pack a task whose descriptor did not provide a usable domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Domain {
    matchers: Vec<Matcher>,
    unknown: bool,
}

impl Domain {
    /// Parse a known `Конфликт-домен` value — comma- and/or whitespace-separated globs
    /// (`engine/src/state/**, engine/src/lib.rs`) — into a domain. An empty string remains a
    /// known empty set for resolver-level callers; descriptor readers use [`Domain::unknown`] for
    /// missing or invalid fields.
    pub fn parse(spec: &str) -> Domain {
        let matchers = domain_tokens(spec).map(matcher).collect();
        Domain {
            matchers,
            unknown: false,
        }
    }

    /// Build a known domain from the typed glob list decoded by the state layer.
    pub fn from_globs(globs: &[String]) -> Domain {
        Domain {
            matchers: globs.iter().map(|glob| matcher(glob)).collect(),
            unknown: false,
        }
    }

    /// A descriptor did not carry a usable `Конфликт-домен`; fail closed by treating it as
    /// potentially intersecting every other domain.
    pub fn unknown() -> Domain {
        Domain {
            matchers: Vec::new(),
            unknown: true,
        }
    }

    /// True if this domain shares any path with `other` (any pattern-pair overlaps), or either
    /// side is unknown and therefore must be treated conservatively.
    pub fn intersects(&self, other: &Domain) -> bool {
        self.unknown
            || other.unknown
            || self
                .matchers
                .iter()
                .any(|a| other.matchers.iter().any(|b| matchers_overlap(a, b)))
    }

    /// True if the domain is a known empty set (and therefore conflicts with nothing).
    pub fn is_empty(&self) -> bool {
        !self.unknown && self.matchers.is_empty()
    }

    /// True when the descriptor did not provide a usable domain.
    pub fn is_unknown(&self) -> bool {
        self.unknown
    }
}

/// How an already-active task's domain blocks a candidate — the two classes `agents/planner.md`
/// distinguishes. Both block a domain-overlapping candidate **equally** (merge-conflict safety is
/// class-independent); they differ only in the *reason* an empty cohort reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveClass {
    /// `в работе` / `на ревью` — the domain frees up *within this cohort* once the task reaches a
    /// terminal status; a conflict with it is a temporary "wait for next round" block.
    Active,
    /// `готова к слиянию` / `эскалирована` / `конфликт` — Phase-2-terminal; the worktree/domain is
    /// only released in Phase 6, so the conflict does not clear within this cohort.
    Terminal,
}

impl ActiveClass {
    /// The class an active task's descriptor `Статус:` maps to, or `None` for a state that does
    /// not block admission (`не начата` before capture, `слита`/`опубликована`/`выполнена`).
    pub fn from_state(state: TaskState) -> Option<ActiveClass> {
        Some(match state {
            TaskState::Working | TaskState::InReview => ActiveClass::Active,
            TaskState::Ready | TaskState::Escalated | TaskState::Conflict => ActiveClass::Terminal,
            TaskState::NotStarted | TaskState::Merged | TaskState::Published | TaskState::Done => {
                return None;
            }
        })
    }
}

/// An already-active task the planner must not overlap: its conflict-domain and its class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTask {
    pub domain: Domain,
    pub class: ActiveClass,
}

/// One not-started queue candidate under consideration for admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub id: String,
    /// Whether all its prerequisites are complete (resolve with [`is_ready`]).
    pub ready: bool,
    /// Its conflict-domain. A known-empty [`Domain`] (`Domain::parse("")`, `is_empty()` true)
    /// conflicts with nothing. An unknown domain (`Domain::unknown()`, `is_unknown()` true —
    /// built by `run.rs::descriptor_domain` when a descriptor has no usable `Конфликт-домен:`)
    /// is a distinct, fail-closed state: it intersects every domain, so it conflicts with
    /// everything.
    pub domain: Domain,
    /// Its delivery lane (`docs/queue_contract.md` §11.1). Only `current` competes for the
    /// ordinary execution capacity; `next_major` is parked out of ordinary admission the same
    /// way an unready candidate is skipped. A fieldless queue entry decodes to `current`.
    pub delivery: DeliveryTarget,
}

impl Candidate {
    /// Build a candidate resolving its readiness from `prerequisites` against `completed`.
    pub fn new(
        id: impl Into<String>,
        prerequisites: &[String],
        completed: &BTreeSet<String>,
        domain: Domain,
        delivery: DeliveryTarget,
    ) -> Candidate {
        Candidate {
            id: id.into(),
            ready: is_ready(prerequisites, completed),
            domain,
            delivery,
        }
    }

    /// Whether this candidate competes for the ordinary current-lane execution capacity: ready
    /// **and** on the `current` delivery lane. A `next_major` candidate never is (§11.1).
    fn admissible_to_current_lane(&self) -> bool {
        self.ready && self.delivery == DeliveryTarget::Current
    }
}

/// Why an admission round admitted nothing — exactly the three reasons `agents/planner.md`
/// ("Выбор батча" п.4 / "Финальный отчёт") returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyReason {
    /// `очередь-пуста` — no admissible not-started candidate remains (a stable fact while the
    /// orchestrator lock is held: queue populators do not write).
    QueueEmpty,
    /// `только-конфликты-с-активными` — at least one candidate is blocked **exclusively** by
    /// really-active (`в работе`/`на ревью`) domains; retry next round, do NOT close admission.
    OnlyConflictsWithActive,
    /// `только-конфликты-с-готовыми` — every remaining candidate overlaps a Phase-2-terminal
    /// domain; the round is a dead end for this cohort, so admission closes.
    OnlyConflictsWithReady,
}

impl EmptyReason {
    /// The literal `agents/planner.md` writes for this reason.
    pub fn as_str(&self) -> &'static str {
        match self {
            EmptyReason::QueueEmpty => "очередь-пуста",
            EmptyReason::OnlyConflictsWithActive => "только-конфликты-с-активными",
            EmptyReason::OnlyConflictsWithReady => "только-конфликты-с-готовыми",
        }
    }

    /// The cohort-admission close reason this empty result implies, or `None` when admission stays
    /// **open** (`только-конфликты-с-активными` — a temporary block; `agents/processor.md` 2.0
    /// holds admission and retries next round rather than latching it closed).
    pub fn to_close_reason(&self) -> Option<CloseReason> {
        Some(match self {
            EmptyReason::QueueEmpty => CloseReason::QueueEmpty,
            EmptyReason::OnlyConflictsWithReady => CloseReason::OnlyConflictsWithReady,
            EmptyReason::OnlyConflictsWithActive => return None,
        })
    }
}

/// Every reason a cohort's admission can be latched **closed** — byte-for-byte the
/// `Приём: закрыт · причина=<COHORT_SIZE|COHORT_MAX_AGE|COHORT_TOKEN_BUDGET|очередь-пуста|только-конфликты-с-готовыми>`
/// vocabulary encoded in `state::cohort` / `state::canonical` (§13.2). Both the admission planner
/// (via [`EmptyReason::to_close_reason`]) and the budget/circuit-breaker gate ([`super::budget`])
/// speak this one vocabulary; neither invents a new word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// `COHORT_SIZE` — the cohort reached its admission capacity.
    CohortSize,
    /// `COHORT_MAX_AGE` — the cohort's admission window (or wall-clock budget) elapsed.
    CohortMaxAge,
    /// `COHORT_TOKEN_BUDGET` — a trusted post-charge actual-token snapshot reached its ceiling,
    /// or telemetry required for an enabled safety gate became unavailable.
    CohortTokenBudget,
    /// `очередь-пуста` — no admissible not-started candidate remains.
    QueueEmpty,
    /// `только-конфликты-с-готовыми` — every remaining candidate overlaps a Phase-2-terminal domain.
    OnlyConflictsWithReady,
}

impl CloseReason {
    /// The `причина=` literal (§13.2), byte-for-byte with `state::cohort`.
    pub fn as_str(&self) -> &'static str {
        match self {
            CloseReason::CohortSize => "COHORT_SIZE",
            CloseReason::CohortMaxAge => "COHORT_MAX_AGE",
            CloseReason::CohortTokenBudget => "COHORT_TOKEN_BUDGET",
            CloseReason::QueueEmpty => "очередь-пуста",
            CloseReason::OnlyConflictsWithReady => "только-конфликты-с-готовыми",
        }
    }
}

/// The result of one admission round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// The ordered ids the round would admit (always non-empty).
    Admitted(Vec<String>),
    /// Nothing admitted, with the reason.
    Empty(EmptyReason),
}

/// Plan one admission round (`agents/planner.md` "Выбор батча"): walking `candidates` from the
/// head, admit up to `capacity` **ready, current-lane** candidates whose conflict-domains overlap
/// NEITHER each other NOR any active task's domain (either class blocks equally). `next_major`
/// candidates are parked out of the ordinary capacity (§11.1). Pure: a deterministic function of
/// already-parsed inputs.
///
/// On an empty result it names one of the three planner reasons, with the documented priority
/// (`только-конфликты-с-активными` outranks `только-конфликты-с-готовыми`; п.4). `capacity` is
/// expected to be ≥ 1 (the planner is called with the free slot count); at `capacity == 0` nothing
/// is admitted and the reason degrades to the empty-round classification below.
pub fn plan_admission(
    candidates: &[Candidate],
    active: &[ActiveTask],
    capacity: usize,
) -> AdmissionOutcome {
    let mut admitted: Vec<String> = Vec::new();
    let mut admitted_domains: Vec<&Domain> = Vec::new();

    for c in candidates {
        if admitted.len() >= capacity {
            break;
        }
        if !c.ready {
            continue;
        }
        // Skip a `next_major` candidate: the ordinary admission path selects only the `current`
        // delivery lane (§11.1) — parked breaking work never competes for execution capacity,
        // filtered the same way an unready candidate is above.
        if c.delivery == DeliveryTarget::NextMajor {
            continue;
        }
        // Skip a candidate overlapping an already-admitted candidate's domain (candidates in one
        // cohort must be pairwise non-overlapping) ...
        if admitted_domains.iter().any(|d| d.intersects(&c.domain)) {
            continue;
        }
        // ... or any active task's domain (both classes block equally, for merge-conflict safety).
        if active.iter().any(|a| a.domain.intersects(&c.domain)) {
            continue;
        }
        admitted.push(c.id.clone());
        admitted_domains.push(&c.domain);
    }

    if !admitted.is_empty() {
        return AdmissionOutcome::Admitted(admitted);
    }
    AdmissionOutcome::Empty(empty_reason(candidates, active))
}

/// Classify why a round admitted nothing (`agents/planner.md` "Выбор батча" п.4).
fn empty_reason(candidates: &[Candidate], active: &[ActiveTask]) -> EmptyReason {
    // No not-started candidate at all -> the queue is empty (stable while the lock is held).
    if candidates.is_empty() {
        return EmptyReason::QueueEmpty;
    }

    // Split the active domains by class once.
    let mut active_domains: Vec<&Domain> = Vec::new();
    let mut terminal_domains: Vec<&Domain> = Vec::new();
    for at in active {
        match at.class {
            ActiveClass::Active => active_domains.push(&at.domain),
            ActiveClass::Terminal => terminal_domains.push(&at.domain),
        }
    }

    // Inspect the current-lane candidates that could not be admitted (all of them, since none
    // was). A `next_major` candidate is not admissible to the current lane at all (§11.1), so it
    // never counts as a domain-blocked candidate — a cohort left with only parked next_major work
    // reads as queue-empty for the current lane below.
    let mut blocked_exclusively_by_active = false;
    let mut blocked_by_terminal = false;
    for c in candidates.iter().filter(|c| c.admissible_to_current_lane()) {
        let hits_active = active_domains.iter().any(|d| d.intersects(&c.domain));
        let hits_terminal = terminal_domains.iter().any(|d| d.intersects(&c.domain));
        if hits_active && !hits_terminal {
            blocked_exclusively_by_active = true;
        }
        if hits_terminal {
            blocked_by_terminal = true;
        }
    }

    // Priority (п.4): a candidate that a finishing active task can unblock outranks one stuck on a
    // Phase-2-terminal domain.
    if blocked_exclusively_by_active {
        EmptyReason::OnlyConflictsWithActive
    } else if blocked_by_terminal {
        EmptyReason::OnlyConflictsWithReady
    } else {
        // Candidates exist but none is ready (all pending prerequisites): nothing is admissible
        // this round. Readiness itself is surfaced by `is_ready`/`unmet_prerequisites`; from the
        // admission gate's view there is no admissible candidate, so the round reads as queue-empty.
        EmptyReason::QueueEmpty
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completed(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn prereqs(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn cand(id: &str, ready: bool, domain: &str) -> Candidate {
        Candidate {
            id: id.to_string(),
            ready,
            domain: Domain::parse(domain),
            delivery: DeliveryTarget::Current,
        }
    }

    /// A `next_major`-lane candidate (otherwise like [`cand`]).
    fn cand_next_major(id: &str, ready: bool, domain: &str) -> Candidate {
        Candidate {
            delivery: DeliveryTarget::NextMajor,
            ..cand(id, ready, domain)
        }
    }

    fn active(domain: &str, class: ActiveClass) -> ActiveTask {
        ActiveTask {
            domain: Domain::parse(domain),
            class,
        }
    }

    // --- readiness ------------------------------------------------------------------------------

    #[test]
    fn readiness_by_prerequisite_completion() {
        let done = completed(&["T-101", "T-103"]);
        assert!(is_ready(&prereqs(&[]), &done));
        assert!(is_ready(&prereqs(&["T-101"]), &done));
        assert!(is_ready(&prereqs(&["T-101", "T-103"]), &done));
        assert!(!is_ready(&prereqs(&["T-101", "T-105"]), &done));
        // The unmet set names the concrete blocker (deduplicated, declared order).
        assert_eq!(
            unmet_prerequisites(&prereqs(&["T-101", "T-105", "T-105", "T-107"]), &done),
            vec!["T-105".to_string(), "T-107".to_string()]
        );
        assert!(unmet_prerequisites(&prereqs(&["T-101"]), &done).is_empty());
    }

    #[test]
    fn candidate_new_resolves_readiness() {
        let done = completed(&["T-105"]);
        let ready = Candidate::new(
            "T-106",
            &prereqs(&["T-105"]),
            &done,
            Domain::parse("engine/**"),
            DeliveryTarget::Current,
        );
        assert!(ready.ready);
        let blocked = Candidate::new(
            "T-109",
            &prereqs(&["T-106"]),
            &done,
            Domain::parse("x/**"),
            DeliveryTarget::Current,
        );
        assert!(!blocked.ready);
    }

    // --- domain overlap -------------------------------------------------------------------------

    #[test]
    fn domain_overlap_is_conservative_and_boundary_aware() {
        // Disjoint sibling subtrees do not overlap.
        assert!(
            !Domain::parse("engine/src/state/**")
                .intersects(&Domain::parse("engine/src/resolvers/**"))
        );
        assert!(!Domain::parse("tui/**").intersects(&Domain::parse("engine/src/state/**")));
        // An exact file inside a subtree overlaps that subtree; a sibling file does not.
        assert!(
            Domain::parse("engine/src/state/**")
                .intersects(&Domain::parse("engine/src/state/cohort.rs"))
        );
        assert!(
            !Domain::parse("engine/src/state/**").intersects(&Domain::parse("engine/src/lib.rs"))
        );
        // A bare path may name a directory, so it overlaps files and globs inside that subtree.
        assert!(Domain::parse("engine/src").intersects(&Domain::parse("engine/src/lib.rs")));
        assert!(Domain::parse("engine/src").intersects(&Domain::parse("engine/src/**")));
        // A superset subtree overlaps its subset subtree.
        assert!(Domain::parse("engine/**").intersects(&Domain::parse("engine/src/state/**")));
        // A multi-glob domain overlaps when ANY pair overlaps.
        let d = Domain::parse("engine/src/state/**, engine/src/lib.rs");
        assert!(d.intersects(&Domain::parse("engine/src/lib.rs")));
        assert!(!d.intersects(&Domain::parse("engine/tests/**")));
        // The empty domain intersects nothing.
        assert!(!Domain::parse("").intersects(&Domain::parse("engine/**")));
        assert!(Domain::parse("").is_empty());
    }

    // --- packing --------------------------------------------------------------------------------

    #[test]
    fn packs_several_non_overlapping_ready_candidates_in_order() {
        let cands = vec![
            cand("T-1", true, "tui/**"),
            cand("T-2", true, "engine/src/state/**"),
            cand("T-3", true, "docs/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &[], 5),
            AdmissionOutcome::Admitted(vec!["T-1".into(), "T-2".into(), "T-3".into()])
        );
        // Capacity caps the cohort at the head of the queue.
        assert_eq!(
            plan_admission(&cands, &[], 2),
            AdmissionOutcome::Admitted(vec!["T-1".into(), "T-2".into()])
        );
    }

    #[test]
    fn skips_candidate_overlapping_an_already_admitted_candidate() {
        // T-2's domain is a subset of T-1's -> once T-1 is admitted, T-2 is skipped; T-3 gets in.
        let cands = vec![
            cand("T-1", true, "engine/**"),
            cand("T-2", true, "engine/src/state/**"),
            cand("T-3", true, "tui/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &[], 5),
            AdmissionOutcome::Admitted(vec!["T-1".into(), "T-3".into()])
        );
    }

    #[test]
    fn skips_unready_candidate_but_admits_ready_one() {
        let cands = vec![cand("T-1", false, "a/**"), cand("T-2", true, "b/**")];
        assert_eq!(
            plan_admission(&cands, &[], 5),
            AdmissionOutcome::Admitted(vec!["T-2".into()])
        );
    }

    // --- delivery lane (§11.1) ------------------------------------------------------------------

    #[test]
    fn skips_next_major_candidate_but_admits_current_one() {
        // A ready `next_major` candidate at the head is parked; the `current` one behind it is
        // admitted — the ordinary admission path selects only the current lane.
        let cands = vec![
            cand_next_major("T-1", true, "a/**"),
            cand("T-2", true, "b/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &[], 5),
            AdmissionOutcome::Admitted(vec!["T-2".into()])
        );
    }

    #[test]
    fn admits_nothing_when_only_next_major_candidates_remain() {
        // A cohort left with only parked breaking work admits nothing and reads as queue-empty for
        // the current lane (which latches admission closed), NOT a domain-conflict reason.
        let cands = vec![
            cand_next_major("T-1", true, "a/**"),
            cand_next_major("T-2", true, "b/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &[], 5),
            AdmissionOutcome::Empty(EmptyReason::QueueEmpty)
        );
    }

    #[test]
    fn next_major_candidate_does_not_mask_a_real_active_conflict_reason() {
        // Only the `current` candidate is domain-blocked by a really-active task; the parked
        // `next_major` candidate must not be counted as its own blocked candidate.
        let active = vec![active("engine/src/state/**", ActiveClass::Active)];
        let cands = vec![
            cand("T-1", true, "engine/src/state/**"),
            cand_next_major("T-2", true, "engine/src/state/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &active, 5),
            AdmissionOutcome::Empty(EmptyReason::OnlyConflictsWithActive)
        );
    }

    #[test]
    fn skips_candidate_overlapping_a_really_active_task() {
        let active = vec![active("tui/**", ActiveClass::Active)];
        let cands = vec![
            cand("T-1", true, "tui/components/**"),
            cand("T-2", true, "engine/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &active, 5),
            AdmissionOutcome::Admitted(vec!["T-2".into()])
        );
    }

    #[test]
    fn skips_candidate_overlapping_a_phase2_terminal_task() {
        let active = vec![active("engine/src/state/**", ActiveClass::Terminal)];
        let cands = vec![
            cand("T-1", true, "engine/src/state/cohort.rs"),
            cand("T-2", true, "docs/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &active, 5),
            AdmissionOutcome::Admitted(vec!["T-2".into()])
        );
    }

    // --- the three empty reasons ----------------------------------------------------------------

    #[test]
    fn empty_reason_queue_empty_when_no_candidates() {
        assert_eq!(
            plan_admission(&[], &[], 5),
            AdmissionOutcome::Empty(EmptyReason::QueueEmpty)
        );
    }

    #[test]
    fn empty_reason_only_conflicts_with_active() {
        // The single ready candidate overlaps only a really-active domain -> temporary block.
        let active = vec![active("engine/src/state/**", ActiveClass::Active)];
        let cands = vec![cand("T-1", true, "engine/src/state/**")];
        assert_eq!(
            plan_admission(&cands, &active, 5),
            AdmissionOutcome::Empty(EmptyReason::OnlyConflictsWithActive)
        );
    }

    #[test]
    fn empty_reason_only_conflicts_with_ready() {
        // The single ready candidate overlaps a Phase-2-terminal domain -> dead end for the cohort.
        let active = vec![active("engine/src/state/**", ActiveClass::Terminal)];
        let cands = vec![cand("T-1", true, "engine/src/state/**")];
        assert_eq!(
            plan_admission(&cands, &active, 5),
            AdmissionOutcome::Empty(EmptyReason::OnlyConflictsWithReady)
        );
    }

    #[test]
    fn empty_reason_priority_active_outranks_ready() {
        // One candidate blocked only by active, another only by terminal: active wins (п.4).
        let active = vec![
            active("aaa/**", ActiveClass::Active),
            active("bbb/**", ActiveClass::Terminal),
        ];
        let cands = vec![
            cand("T-1", true, "aaa/sub/**"),
            cand("T-2", true, "bbb/sub/**"),
        ];
        assert_eq!(
            plan_admission(&cands, &active, 5),
            AdmissionOutcome::Empty(EmptyReason::OnlyConflictsWithActive)
        );
    }

    #[test]
    fn candidate_hitting_both_classes_is_not_exclusively_active() {
        // A candidate overlapping BOTH an active and a terminal domain is not unblockable by the
        // active task alone -> the reason is `только-конфликты-с-готовыми`.
        let active = vec![
            active("shared/**", ActiveClass::Active),
            active("shared/**", ActiveClass::Terminal),
        ];
        let cands = vec![cand("T-1", true, "shared/sub/**")];
        assert_eq!(
            plan_admission(&cands, &active, 5),
            AdmissionOutcome::Empty(EmptyReason::OnlyConflictsWithReady)
        );
    }

    #[test]
    fn all_unready_candidates_read_as_queue_empty() {
        let cands = vec![cand("T-1", false, "a/**"), cand("T-2", false, "b/**")];
        assert_eq!(
            plan_admission(&cands, &[], 5),
            AdmissionOutcome::Empty(EmptyReason::QueueEmpty)
        );
    }

    // --- vocabulary bridges ---------------------------------------------------------------------

    #[test]
    fn empty_reason_strings_and_close_bridge() {
        assert_eq!(EmptyReason::QueueEmpty.as_str(), "очередь-пуста");
        assert_eq!(
            EmptyReason::OnlyConflictsWithActive.as_str(),
            "только-конфликты-с-активными"
        );
        assert_eq!(
            EmptyReason::OnlyConflictsWithReady.as_str(),
            "только-конфликты-с-готовыми"
        );
        // Only очередь-пуста / только-конфликты-с-готовыми close admission; с-активными holds it open.
        assert_eq!(
            EmptyReason::QueueEmpty.to_close_reason(),
            Some(CloseReason::QueueEmpty)
        );
        assert_eq!(
            EmptyReason::OnlyConflictsWithReady.to_close_reason(),
            Some(CloseReason::OnlyConflictsWithReady)
        );
        assert_eq!(EmptyReason::OnlyConflictsWithActive.to_close_reason(), None);
    }

    #[test]
    fn close_reason_vocabulary_matches_section_13_2() {
        assert_eq!(CloseReason::CohortSize.as_str(), "COHORT_SIZE");
        assert_eq!(CloseReason::CohortMaxAge.as_str(), "COHORT_MAX_AGE");
        assert_eq!(CloseReason::QueueEmpty.as_str(), "очередь-пуста");
        assert_eq!(
            CloseReason::OnlyConflictsWithReady.as_str(),
            "только-конфликты-с-готовыми"
        );
    }

    #[test]
    fn active_class_from_state_maps_the_two_blocking_classes() {
        assert_eq!(
            ActiveClass::from_state(TaskState::Working),
            Some(ActiveClass::Active)
        );
        assert_eq!(
            ActiveClass::from_state(TaskState::InReview),
            Some(ActiveClass::Active)
        );
        assert_eq!(
            ActiveClass::from_state(TaskState::Ready),
            Some(ActiveClass::Terminal)
        );
        assert_eq!(
            ActiveClass::from_state(TaskState::Escalated),
            Some(ActiveClass::Terminal)
        );
        assert_eq!(
            ActiveClass::from_state(TaskState::Conflict),
            Some(ActiveClass::Terminal)
        );
        // Non-blocking states.
        assert_eq!(ActiveClass::from_state(TaskState::NotStarted), None);
        assert_eq!(ActiveClass::from_state(TaskState::Merged), None);
        assert_eq!(ActiveClass::from_state(TaskState::Published), None);
        assert_eq!(ActiveClass::from_state(TaskState::Done), None);
    }
}
