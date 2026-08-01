//! Hermetic, offline proof of the typed GitLab and Gitea publication CI adapters.
//!
//! Nothing here spawns `glab`/`tea`, opens a socket, or needs a repository: the adapters are
//! split so the transport (which needs the real typed client) stays in `headless`, while the
//! classification and both watch loops are pure functions over an already-decoded response plus
//! a caller-supplied poll. That split is exactly what lets these fixtures cover the cases a live
//! forge could otherwise only produce by accident — a truncated page, a response for the wrong
//! commit, an API outage, and an exhausted deadline.
//!
//! The shapes below are the real ones: GitLab's
//! `GET /projects/:id/repository/commits/:sha/statuses` array (`id`/`name`/`status`/`sha`/
//! `allow_failure`) and Gitea's `GET /repos/{owner}/{repo}/commits/{ref}/status` combined
//! document (`sha`/`total_count`/`statuses[]` with `id`/`context`/`status`).
//!
//! The invariant every case here defends: `CiOutcome::Passed` requires a positive confirmation
//! for the exact published commit; every unproven state is fail-closed. Gitea gets extra
//! attention because it is the forge whose body cannot show that a page is complete — its
//! `total_count` is the returned page's length and its page size is an instance setting — so
//! these fixtures pin the next-page completeness probe that stands in for that missing evidence.

use std::cell::Cell;
use std::time::Duration;

use orchestrail_engine::config::EngineConfig;
use orchestrail_engine::forge_ci::{
    self, CiPoll, Forge, GITEA_STATUS_PAGE_SIZE, GITLAB_STATUS_PAGE_SIZE, GitLabCommitStatus,
    GiteaCombinedStatus,
};
use orchestrail_engine::processor::CiOutcome;

/// A published commit id; the gate refuses anything that is not a full object id.
const HEAD: &str = "3f7a1c9e2b4d6f80a1c3e5079b2d4f6081a3c5e7";
/// A different published commit, used to prove the adapters are SHA-bound.
const OTHER_HEAD: &str = "0000111122223333444455556666777788889999";

fn gitlab_statuses(json: &str) -> Vec<GitLabCommitStatus> {
    serde_json::from_str(json).expect("GitLab commit-status list fixture must decode")
}

fn gitea_combined(json: &str) -> GiteaCombinedStatus {
    forge_ci::parse_gitea_combined_status(json).expect("Gitea combined-status fixture must decode")
}

fn required(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

/// The completeness probe failing — the endpoint went away between the page and its proof.
#[derive(Debug, PartialEq, Eq)]
struct ProbeOutage;

/// The empty page Gitea serves past the last one, and the only answer that lets a snapshot pass.
/// The body is the real one: `statuses` comes back as an explicit `null` rather than `[]`.
fn last_page() -> Result<GiteaCombinedStatus, ProbeOutage> {
    Ok(gitea_combined(
        r#"{"state": "pending", "sha": "", "total_count": 0, "statuses": null}"#,
    ))
}

/// A non-empty next page: the published commit has more status contexts than the first page
/// carried, whatever that page's length suggested. The hidden context is red, which is exactly
/// the result a truncated pass would have published over.
fn one_more_page() -> Result<GiteaCombinedStatus, ProbeOutage> {
    Ok(gitea_combined(
        r#"{"state": "failure", "sha": "", "total_count": 1, "statuses": [
          {"id": 999, "context": "hidden-by-truncation", "status": "failure"}
        ]}"#,
    ))
}

/// A probe request that fails.
fn probe_outage() -> Result<GiteaCombinedStatus, ProbeOutage> {
    Err(ProbeOutage)
}

/// A probe that must never be called: an already fail-closed snapshot has nothing to prove, and
/// spending a forge request on it would be a live-forge regression invisible in the verdict.
fn no_probe() -> Result<GiteaCombinedStatus, ProbeOutage> {
    panic!("a non-passing Gitea page must not spend a completeness probe");
}

// ---------------------------------------------------------------------------------------------
// GitLab adapter
// ---------------------------------------------------------------------------------------------

#[test]
fn gitlab_confirms_only_a_fully_green_required_set() {
    let statuses = gitlab_statuses(&format!(
        r#"[
          {{"id": 91, "name": "build", "status": "success", "sha": "{HEAD}", "allow_failure": false}},
          {{"id": 92, "name": "test",  "status": "success", "sha": "{HEAD}", "allow_failure": false}},
          {{"id": 93, "name": "docs",  "status": "running", "sha": "{HEAD}", "allow_failure": false}}
        ]"#
    ));
    // Strict: exactly the two required jobs are consulted, so an unrelated running job cannot
    // hold up a set the operator already declared sufficient.
    assert_eq!(
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["build", "test"])),
        CiPoll::Passing
    );
    // Best effort: every blocking job counts, so the running one keeps the snapshot pending.
    assert!(matches!(
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &[]),
        CiPoll::Pending { .. }
    ));
}

#[test]
fn gitlab_failed_job_is_red_with_a_stable_signature() {
    let statuses = gitlab_statuses(&format!(
        r#"[{{"id": 91, "name": "test", "status": "failed", "sha": "{HEAD}", "allow_failure": false}}]"#
    ));
    let CiPoll::Failing { signature, reason } =
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["test"]))
    else {
        panic!("a failed required GitLab job must classify as failing");
    };
    assert!(
        reason.contains("test") && reason.contains("failed"),
        "{reason}"
    );
    assert!(!signature.is_empty());
    // The signature is the stagnation detector's key: identical evidence must normalize to an
    // identical signature, and a different commit must not collide with it.
    let CiPoll::Failing {
        signature: repeated,
        ..
    } = forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["test"]))
    else {
        panic!("classification must be deterministic");
    };
    assert_eq!(signature, repeated);
    let elsewhere = gitlab_statuses(&format!(
        r#"[{{"id": 91, "name": "test", "status": "failed", "sha": "{OTHER_HEAD}", "allow_failure": false}}]"#
    ));
    let CiPoll::Failing {
        signature: other, ..
    } = forge_ci::classify_gitlab_statuses(OTHER_HEAD, &elsewhere, &required(&["test"]))
    else {
        panic!("classification must be deterministic");
    };
    assert_ne!(signature, other);
}

#[test]
fn gitlab_pending_states_never_confirm() {
    for state in [
        "created",
        "waiting_for_resource",
        "preparing",
        "pending",
        "running",
        "scheduled",
        "manual",
        // An unknown state added by a future GitLab release must degrade to pending, never to a
        // pass and never to a repair-dispatching failure.
        "quantum_superposition",
    ] {
        let statuses = gitlab_statuses(&format!(
            r#"[{{"id": 1, "name": "test", "status": "{state}", "sha": "{HEAD}", "allow_failure": false}}]"#
        ));
        assert!(
            matches!(
                forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["test"])),
                CiPoll::Pending { .. }
            ),
            "GitLab state {state:?} must not confirm or fail the publication gate"
        );
    }
}

#[test]
fn gitlab_missing_required_check_is_pending_not_a_pass() {
    let statuses = gitlab_statuses(&format!(
        r#"[{{"id": 91, "name": "build", "status": "success", "sha": "{HEAD}", "allow_failure": false}}]"#
    ));
    let CiPoll::Pending { reason } =
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["build", "test"]))
    else {
        panic!("an absent required check must never be treated as green");
    };
    assert!(reason.contains("test"), "{reason}");
}

#[test]
fn gitlab_latest_status_supersedes_an_earlier_red_run() {
    let statuses = gitlab_statuses(&format!(
        r#"[
          {{"id": 91, "name": "test", "status": "failed",  "sha": "{HEAD}", "allow_failure": false}},
          {{"id": 97, "name": "test", "status": "success", "sha": "{HEAD}", "allow_failure": false}}
        ]"#
    ));
    assert_eq!(
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["test"])),
        CiPoll::Passing
    );
}

#[test]
fn gitlab_refuses_a_status_reported_for_another_commit() {
    let statuses = gitlab_statuses(&format!(
        r#"[{{"id": 91, "name": "test", "status": "success", "sha": "{OTHER_HEAD}", "allow_failure": false}}]"#
    ));
    let CiPoll::Pending { reason } =
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["test"]))
    else {
        panic!("a status for a different commit must never confirm the published one");
    };
    assert!(reason.contains(OTHER_HEAD), "{reason}");
}

#[test]
fn gitlab_refuses_a_possibly_truncated_page() {
    let entries: Vec<String> = (0..GITLAB_STATUS_PAGE_SIZE)
        .map(|index| {
            format!(
                r#"{{"id": {index}, "name": "job{index}", "status": "success", "sha": "{HEAD}", "allow_failure": false}}"#
            )
        })
        .collect();
    let statuses = gitlab_statuses(&format!("[{}]", entries.join(",")));
    assert!(
        matches!(
            forge_ci::classify_gitlab_statuses(HEAD, &statuses, &[]),
            CiPoll::Pending { .. }
        ),
        "an exactly-full page cannot be proved complete, so it must not pass"
    );
}

#[test]
fn gitlab_empty_status_list_is_pending() {
    assert!(matches!(
        forge_ci::classify_gitlab_statuses(HEAD, &[], &[]),
        CiPoll::Pending { .. }
    ));
    assert!(matches!(
        forge_ci::classify_gitlab_statuses(HEAD, &[], &required(&["test"])),
        CiPoll::Pending { .. }
    ));
}

#[test]
fn gitlab_allow_failure_is_non_blocking_only_when_the_operator_required_nothing() {
    let statuses = gitlab_statuses(&format!(
        r#"[
          {{"id": 91, "name": "test",  "status": "success", "sha": "{HEAD}", "allow_failure": false}},
          {{"id": 92, "name": "flaky", "status": "failed",  "sha": "{HEAD}", "allow_failure": true}}
        ]"#
    ));
    // Best effort mirrors GitLab's own pipeline semantics: a non-blocking job cannot make the
    // commit red, because GitLab itself does not consider the pipeline failed.
    assert_eq!(
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &[]),
        CiPoll::Passing
    );
    // Naming it required is an explicit operator contract that outranks that hint.
    assert!(matches!(
        forge_ci::classify_gitlab_statuses(HEAD, &statuses, &required(&["flaky"])),
        CiPoll::Failing { .. }
    ));
}

#[test]
fn gitlab_only_non_blocking_statuses_cannot_confirm_best_effort() {
    let statuses = gitlab_statuses(&format!(
        r#"[{{"id": 91, "name": "flaky", "status": "success", "sha": "{HEAD}", "allow_failure": true}}]"#
    ));
    assert!(
        matches!(
            forge_ci::classify_gitlab_statuses(HEAD, &statuses, &[]),
            CiPoll::Pending { .. }
        ),
        "with every status non-blocking there is nothing that proves the commit was tested"
    );
}

// ---------------------------------------------------------------------------------------------
// Gitea adapter
// ---------------------------------------------------------------------------------------------

#[test]
fn gitea_confirms_only_a_fully_green_required_set() {
    let combined = gitea_combined(&format!(
        r#"{{"sha": "{HEAD}", "state": "success", "total_count": 3, "statuses": [
          {{"id": 1, "context": "build", "status": "success"}},
          {{"id": 2, "context": "test",  "status": "success"}},
          {{"id": 3, "context": "docs",  "status": "pending"}}
        ]}}"#
    ));
    assert_eq!(
        forge_ci::classify_gitea_statuses(
            HEAD,
            &combined,
            &required(&["build", "test"]),
            last_page
        ),
        Ok(CiPoll::Passing)
    );
    assert!(matches!(
        forge_ci::classify_gitea_statuses(HEAD, &combined, &[], no_probe),
        Ok(CiPoll::Pending { .. })
    ));
}

#[test]
fn gitea_red_states_are_failing_with_a_stable_signature() {
    for state in ["failure", "error", "warning"] {
        let combined = gitea_combined(&format!(
            r#"{{"sha": "{HEAD}", "state": "{state}", "total_count": 1, "statuses": [
              {{"id": 1, "context": "test", "status": "{state}"}}
            ]}}"#
        ));
        let Ok(CiPoll::Failing { signature, reason }) =
            forge_ci::classify_gitea_statuses(HEAD, &combined, &required(&["test"]), no_probe)
        else {
            panic!("Gitea state {state:?} must classify as failing");
        };
        assert!(
            reason.contains("test") && reason.contains(state),
            "{reason}"
        );
        assert!(!signature.is_empty());
    }
}

#[test]
fn gitea_pending_and_unknown_states_never_confirm() {
    for state in ["pending", "running", "brand_new_state"] {
        let combined = gitea_combined(&format!(
            r#"{{"sha": "{HEAD}", "state": "pending", "total_count": 1, "statuses": [
              {{"id": 1, "context": "test", "status": "{state}"}}
            ]}}"#
        ));
        assert!(
            matches!(
                forge_ci::classify_gitea_statuses(HEAD, &combined, &required(&["test"]), no_probe),
                Ok(CiPoll::Pending { .. })
            ),
            "Gitea state {state:?} must not confirm the publication gate"
        );
    }
}

#[test]
fn gitea_missing_required_check_is_pending_not_a_pass() {
    let combined = gitea_combined(&format!(
        r#"{{"sha": "{HEAD}", "state": "success", "total_count": 1, "statuses": [
          {{"id": 1, "context": "build", "status": "success"}}
        ]}}"#
    ));
    let Ok(CiPoll::Pending { reason }) =
        forge_ci::classify_gitea_statuses(HEAD, &combined, &required(&["build", "test"]), no_probe)
    else {
        panic!("an absent required check must never be treated as green");
    };
    assert!(reason.contains("test"), "{reason}");
}

#[test]
fn gitea_latest_status_supersedes_an_earlier_red_run() {
    let combined = gitea_combined(&format!(
        r#"{{"sha": "{HEAD}", "state": "success", "total_count": 2, "statuses": [
          {{"id": 1, "context": "test", "status": "failure"}},
          {{"id": 4, "context": "test", "status": "success"}}
        ]}}"#
    ));
    assert_eq!(
        forge_ci::classify_gitea_statuses(HEAD, &combined, &required(&["test"]), last_page),
        Ok(CiPoll::Passing)
    );
}

#[test]
fn gitea_refuses_a_combined_status_for_another_commit() {
    let combined = gitea_combined(&format!(
        r#"{{"sha": "{OTHER_HEAD}", "state": "success", "total_count": 1, "statuses": [
          {{"id": 1, "context": "test", "status": "success"}}
        ]}}"#
    ));
    let Ok(CiPoll::Pending { reason }) =
        forge_ci::classify_gitea_statuses(HEAD, &combined, &required(&["test"]), no_probe)
    else {
        panic!("a combined status for a different commit must never confirm the published one");
    };
    assert!(reason.contains(OTHER_HEAD), "{reason}");
}

/// A self-consistent page of `count` green contexts for the published commit — the shape a Gitea
/// server returns whether or not it clamped the page: `total_count` always equals the number of
/// entries actually sent.
fn gitea_green_page(count: usize) -> GiteaCombinedStatus {
    let entries: Vec<String> = (0..count)
        .map(|index| format!(r#"{{"id": {index}, "context": "job{index}", "status": "success"}}"#))
        .collect();
    gitea_combined(&format!(
        r#"{{"sha": "{HEAD}", "state": "success", "total_count": {count}, "statuses": [{}]}}"#,
        entries.join(",")
    ))
}

/// The regression this whole probe exists for: a page silently clamped by the *server's*
/// `[api] MAX_RESPONSE_ITEMS` is shorter than the requested limit and reports a `total_count`
/// equal to its own length, so nothing in the body says it was cut. Only the next page does.
#[test]
fn gitea_refuses_a_pass_when_a_clamped_page_hides_further_statuses() {
    // Five green contexts on an instance whose response limit is five: both of the body-only
    // signals a page can offer read "complete".
    let clamped = gitea_green_page(5);
    assert!(clamped.statuses.len() < GITEA_STATUS_PAGE_SIZE);
    assert_eq!(clamped.total_count, clamped.statuses.len());

    // Best effort is where truncation could fail open: every visible context is green, so a pass
    // would be published while an unseen red one exists.
    let Ok(CiPoll::Pending { reason }) =
        forge_ci::classify_gitea_statuses(HEAD, &clamped, &[], one_more_page)
    else {
        panic!("a page with a further page behind it must never confirm the publication gate");
    };
    assert!(reason.contains("page 2"), "{reason}");
    assert!(reason.contains("MAX_RESPONSE_ITEMS"), "{reason}");

    // Strict mode is held to the same proof, so the two modes cannot disagree about whether the
    // snapshot they classified was the whole snapshot.
    assert!(matches!(
        forge_ci::classify_gitea_statuses(HEAD, &clamped, &required(&["job1"]), one_more_page),
        Ok(CiPoll::Pending { .. })
    ));

    // And with nothing behind it, the very same page passes: the probe is what decides, not the
    // page's length.
    assert_eq!(
        forge_ci::classify_gitea_statuses(HEAD, &clamped, &[], last_page),
        Ok(CiPoll::Passing)
    );
}

#[test]
fn gitea_confirms_an_exactly_full_page_the_probe_proves_complete() {
    // A page exactly at the requested limit is not refused on its length alone: on a server that
    // honoured the limit it may well be the whole set, and the probe says which.
    let full = gitea_green_page(GITEA_STATUS_PAGE_SIZE);
    assert_eq!(
        forge_ci::classify_gitea_statuses(HEAD, &full, &[], last_page),
        Ok(CiPoll::Passing)
    );
    assert!(matches!(
        forge_ci::classify_gitea_statuses(HEAD, &full, &[], one_more_page),
        Ok(CiPoll::Pending { .. })
    ));
}

#[test]
fn gitea_refuses_a_page_smaller_than_its_own_reported_total() {
    // A deployment whose `total_count` really is the total makes truncation directly visible, and
    // that answer is reached without spending a probe.
    let truncated = gitea_combined(&format!(
        r#"{{"sha": "{HEAD}", "state": "success", "total_count": 7, "statuses": [
          {{"id": 1, "context": "test", "status": "success"}}
        ]}}"#
    ));
    let Ok(CiPoll::Pending { reason }) =
        forge_ci::classify_gitea_statuses(HEAD, &truncated, &[], no_probe)
    else {
        panic!("a page smaller than the reported total must not pass");
    };
    assert!(reason.contains('7'), "{reason}");
}

#[test]
fn gitea_completeness_probe_failure_never_confirms() {
    // The page itself was green, so the probe is the only thing left between it and a pass. If
    // the probe cannot be answered, the poll fails rather than passing: the watch loops read that
    // as an outage, which is fail-closed in both modes.
    assert_eq!(
        forge_ci::classify_gitea_statuses(HEAD, &gitea_green_page(3), &[], probe_outage),
        Err(ProbeOutage)
    );
}

#[test]
fn gitea_spends_a_probe_only_on_a_would_be_pass() {
    // `no_probe` panics if called, so every non-passing case above already proves this for its own
    // shape; here the counter proves the positive half — a pass costs exactly one extra request,
    // not one per poll.
    let probes = Cell::new(0usize);
    let counting = || {
        probes.set(probes.get() + 1);
        last_page()
    };
    assert_eq!(
        forge_ci::classify_gitea_statuses(HEAD, &gitea_green_page(2), &[], counting),
        Ok(CiPoll::Passing)
    );
    assert_eq!(probes.get(), 1, "a confirmed page is proved exactly once");

    let pending = gitea_combined(&format!(
        r#"{{"sha": "{HEAD}", "state": "pending", "total_count": 1, "statuses": [
          {{"id": 1, "context": "test", "status": "pending"}}
        ]}}"#
    ));
    assert!(matches!(
        forge_ci::classify_gitea_statuses(HEAD, &pending, &[], || {
            probes.set(probes.get() + 1);
            last_page()
        }),
        Ok(CiPoll::Pending { .. })
    ));
    assert_eq!(
        probes.get(),
        1,
        "a snapshot that is already fail-closed must not spend a forge request"
    );
}

#[test]
fn gitea_empty_status_list_is_pending() {
    // Both spellings of "no statuses yet": `[]` and the explicit `null` Gitea actually sends for
    // an empty page. Neither may read as an outage, and neither may pass.
    for body in [
        format!(r#"{{"sha": "{HEAD}", "state": "pending", "total_count": 0, "statuses": []}}"#),
        format!(r#"{{"sha": "{HEAD}", "state": "pending", "total_count": 0, "statuses": null}}"#),
        "null".to_string(),
    ] {
        let combined = gitea_combined(&body);
        assert!(matches!(
            forge_ci::classify_gitea_statuses(HEAD, &combined, &[], no_probe),
            Ok(CiPoll::Pending { .. })
        ));
        assert!(matches!(
            forge_ci::classify_gitea_statuses(HEAD, &combined, &required(&["test"]), no_probe),
            Ok(CiPoll::Pending { .. })
        ));
    }
}

// ---------------------------------------------------------------------------------------------
// Watch loops: outage and deadline, for both new forges
// ---------------------------------------------------------------------------------------------

/// A scripted poll: returns each scripted answer once, then repeats the last one, and counts
/// calls so a test can prove the loop actually re-polled rather than answering from one sample.
struct ScriptedPoll {
    answers: Vec<Result<CiPoll, ()>>,
    calls: Cell<usize>,
}

impl ScriptedPoll {
    fn new(answers: Vec<Result<CiPoll, ()>>) -> Self {
        Self {
            answers,
            calls: Cell::new(0),
        }
    }

    fn next(&self) -> Result<CiPoll, ()> {
        let index = self.calls.get();
        self.calls.set(index + 1);
        self.answers
            .get(index)
            .or_else(|| self.answers.last())
            .cloned()
            .expect("a scripted poll needs at least one answer")
    }
}

fn pending(reason: &str) -> Result<CiPoll, ()> {
    Ok(CiPoll::Pending {
        reason: reason.to_string(),
    })
}

#[test]
fn an_api_outage_never_confirms_required_checks_on_either_forge() {
    for forge in [Forge::GitLab, Forge::Gitea] {
        let script = ScriptedPoll::new(vec![Err(())]);
        let outcome = forge_ci::watch_required(
            forge,
            HEAD,
            Duration::from_millis(20),
            Duration::from_millis(1),
            || script.next(),
        );
        let CiOutcome::RequiredUnconfirmed { reason } = outcome else {
            panic!("{forge} outage must resolve to RequiredUnconfirmed, never Passed or Failed");
        };
        assert!(
            reason.contains(forge.display_name()) && reason.contains("unavailable"),
            "{reason}"
        );
        assert!(reason.contains(HEAD), "{reason}");
        assert!(script.calls.get() > 1, "the watcher must retry an outage");
    }
}

#[test]
fn an_api_outage_degrades_best_effort_instead_of_confirming_it() {
    let script = ScriptedPoll::new(vec![Err(())]);
    let outcome = forge_ci::watch_best_effort(
        HEAD,
        Duration::from_millis(20),
        Duration::from_millis(1),
        || script.next(),
    );
    let CiOutcome::BestEffortDegraded { reason } = outcome else {
        panic!("an unavailable endpoint must never read as a confirmed best-effort CI");
    };
    assert!(reason.contains("unavailable"), "{reason}");
    assert_eq!(
        script.calls.get(),
        1,
        "best effort does not block on an outage it cannot resolve"
    );
}

#[test]
fn an_expired_deadline_never_confirms_required_checks_on_either_forge() {
    for forge in [Forge::GitLab, Forge::Gitea] {
        let script = ScriptedPoll::new(vec![pending("the pipeline is still running")]);
        let outcome = forge_ci::watch_required(
            forge,
            HEAD,
            Duration::ZERO,
            Duration::from_millis(1),
            || script.next(),
        );
        let CiOutcome::RequiredUnconfirmed { reason } = outcome else {
            panic!("{forge} must fail closed when the deadline expires while work is pending");
        };
        assert!(
            reason.contains("were not confirmed before the deadline")
                && reason.contains("the pipeline is still running"),
            "{reason}"
        );
    }
}

#[test]
fn an_expired_deadline_degrades_best_effort() {
    let script = ScriptedPoll::new(vec![pending("still running")]);
    let outcome =
        forge_ci::watch_best_effort(HEAD, Duration::ZERO, Duration::from_millis(1), || {
            script.next()
        });
    assert!(matches!(outcome, CiOutcome::BestEffortDegraded { .. }));
}

#[test]
fn a_pending_snapshot_that_later_turns_green_is_confirmed() {
    let script = ScriptedPoll::new(vec![
        pending("the pipeline is still running"),
        pending("the pipeline is still running"),
        Ok(CiPoll::Passing),
    ]);
    let outcome = forge_ci::watch_required(
        Forge::GitLab,
        HEAD,
        Duration::from_secs(30),
        Duration::from_millis(1),
        || script.next(),
    );
    assert_eq!(outcome, CiOutcome::Passed);
    assert_eq!(script.calls.get(), 3);
}

#[test]
fn a_red_required_check_is_reported_before_the_deadline_with_its_signature() {
    let script = ScriptedPoll::new(vec![Ok(CiPoll::Failing {
        signature: "sig-1".into(),
        reason: "Gitea check \"test\" reported \"failure\"".into(),
    })]);
    let outcome = forge_ci::watch_required(
        Forge::Gitea,
        HEAD,
        Duration::from_secs(30),
        Duration::from_millis(1),
        || script.next(),
    );
    let CiOutcome::Failed { signature, reason } = outcome else {
        panic!("a proven red required check must be reported as Failed, not merely unconfirmed");
    };
    assert_eq!(signature, "sig-1");
    assert!(reason.contains("failure"), "{reason}");
    assert_eq!(
        script.calls.get(),
        1,
        "a terminal red result stops the loop"
    );
}

// ---------------------------------------------------------------------------------------------
// Forge selection
// ---------------------------------------------------------------------------------------------

#[test]
fn forge_defaults_to_github_and_rejects_an_unsupported_value() {
    assert_eq!(EngineConfig::default().forge, Forge::GitHub);
    for value in ["github", "gitlab", "gitea"] {
        assert_eq!(
            Forge::parse(value).map(Forge::as_config_value),
            Some(value),
            "{value} must be an accepted FORGE spelling"
        );
    }
    // No URL guessing and no case-insensitive fallback: an unrecognised forge is rejected by
    // the configuration parser rather than silently polled with the GitHub client.
    for value in ["bitbucket", "GitHub", "forgejo ", ""] {
        assert_eq!(
            Forge::parse(value),
            None,
            "{value:?} must not resolve to a supported forge"
        );
    }
}
