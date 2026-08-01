//! Forge-independent publication CI gate: which typed forge client to poll, how one
//! commit-SHA-bound snapshot of that forge's checks classifies, and the two watch loops
//! (`required_checks` strict, empty-list best effort) built on top of a single poll.
//!
//! The transport lives in [`crate::headless`] because it needs the real `vcs-github` /
//! `vcs-gitlab` / `vcs-gitea` clients and a tokio runtime; everything here is a pure
//! function of an already-decoded response plus a caller-supplied poll closure, so the
//! whole gate — including outage and deadline behaviour — is exercised hermetically from
//! `engine/tests/` without a forge, a network, or a CLI on `PATH`.
//!
//! The gate is fail-closed by construction: [`CiPoll::Passing`] is returned only on a
//! positive confirmation that every selected check reported a terminal success for the
//! exact published commit. Partial pages, absent required checks, unknown states, an
//! unavailable endpoint, and an exhausted deadline all resolve to a non-passing outcome.

use std::fmt;
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::processor::CiOutcome;
use crate::resolvers::AttemptSignature;

/// GitLab returns one page of commit statuses with no total in the body, so the request asks
/// for the documented maximum page size and a page that comes back exactly full is refused
/// rather than mistaken for the complete set.
pub const GITLAB_STATUS_PAGE_SIZE: usize = 100;

/// Gitea clamps a list request to its `MAX_RESPONSE_ITEMS` setting (default 50) and reports
/// `total_count` for the returned page rather than the true total, so the request asks for
/// exactly that documented default and an exactly-full page is refused for the same reason.
pub const GITEA_STATUS_PAGE_SIZE: usize = 50;

/// The typed forge client the publication CI gate polls, selected by the `FORGE` key in
/// `.work/config.md`.
///
/// Every supported value maps to a typed client of the same `vcs-*` family; there is
/// deliberately no "guess it from the remote URL" variant, because a mis-guessed forge would
/// silently poll the wrong API and time out into a degraded publication instead of failing
/// loudly at configuration time. An unrecognised value is rejected by the configuration
/// parser, so an unsupported forge can never reach this gate at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Forge {
    /// `gh api repos/{owner}/{repo}/commits/<sha>/check-runs` — the historical default, kept
    /// as the default so an existing `config.md` without a `FORGE` key keeps its behaviour.
    #[default]
    GitHub,
    /// `glab api projects/:fullpath/repository/commits/<sha>/statuses`.
    GitLab,
    /// `tea api /repos/{owner}/{repo}/commits/<sha>/status`.
    Gitea,
}

impl Forge {
    /// The lowercase `FORGE` spelling accepted by the configuration parser.
    pub fn as_config_value(self) -> &'static str {
        match self {
            Self::GitHub => "github",
            Self::GitLab => "gitlab",
            Self::Gitea => "gitea",
        }
    }

    /// The display name used in operator-facing CI reasons. It is deliberately the vendor
    /// spelling, so the historical GitHub messages are unchanged.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::GitHub => "GitHub",
            Self::GitLab => "GitLab",
            Self::Gitea => "Gitea",
        }
    }

    /// Decode a `FORGE` value. `None` is an unsupported forge; the caller rejects the whole
    /// configuration rather than defaulting to a forge whose API this engine cannot poll.
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "github" => Self::GitHub,
            "gitlab" => Self::GitLab,
            "gitea" => Self::Gitea,
            _ => return None,
        })
    }

    /// The exact spellings accepted by `FORGE`, for the configuration error message.
    pub fn supported_config_values() -> [&'static str; 3] {
        [
            Self::GitHub.as_config_value(),
            Self::GitLab.as_config_value(),
            Self::Gitea.as_config_value(),
        ]
    }
}

impl fmt::Display for Forge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_config_value())
    }
}

/// One commit-SHA-bound snapshot of a forge's checks, classified for the publication gate.
///
/// `Passing` is the only outcome that lets publication complete, and it requires a positive
/// terminal success for every selected check. Everything unproven — including "the response
/// may have been truncated" and "this state is not one we recognise" — is `Pending`, which the
/// watch loops fold into a fail-closed outcome once the deadline expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiPoll {
    Passing,
    Pending { reason: String },
    Failing { signature: String, reason: String },
}

/// Whether a full Git-compatible commit id was supplied. The gate refuses to poll a branch,
/// a tag, or an abbreviated id: only an exact object id proves the checks belong to the commit
/// that was actually published.
pub fn is_full_commit_id(head: &str) -> bool {
    matches!(head.len(), 40 | 64) && head.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `glab api` endpoint for the exact published commit's statuses.
///
/// `:fullpath` is expanded by `glab` from the repository the request runs in, so the project
/// is resolved from the bound repo's remote rather than from the process's current directory.
/// The commit id is interpolated only after [`is_full_commit_id`] accepted it, so the endpoint
/// can never be flag-like or carry a traversal.
///
/// `sort=desc` is not cosmetic. GitLab orders this collection ascending by default, so a page
/// that did hit the size cap would hold the *oldest* statuses — and a stale green could then
/// hide a newer red one. Newest-first means a capped page always carries the results that
/// decide the gate, and [`GITLAB_STATUS_PAGE_SIZE`] remains the backstop for the truncation
/// that ordering cannot rule out.
pub fn gitlab_statuses_endpoint(head: &str) -> String {
    format!(
        "projects/:fullpath/repository/commits/{head}/statuses\
         ?per_page={GITLAB_STATUS_PAGE_SIZE}&page=1&order_by=id&sort=desc"
    )
}

/// `tea api` endpoint for the exact published commit's combined status.
///
/// `{owner}`/`{repo}` are expanded by `tea` from the repository the request runs in. The
/// leading `/` keeps the argument from ever being read as a flag; `tea` prefixes `/api/v1`
/// itself.
pub fn gitea_status_endpoint(head: &str) -> String {
    format!("/repos/{{owner}}/{{repo}}/commits/{head}/status?page=1&limit={GITEA_STATUS_PAGE_SIZE}")
}

/// One entry of GitLab's `GET /projects/:id/repository/commits/:sha/statuses` array.
///
/// Unknown fields are tolerated on purpose: a presentation field added by a future GitLab
/// release must not turn an already-terminal CI result into a parse failure.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitLabCommitStatus {
    /// Monotonically increasing status id, so a re-run supersedes an earlier red result for
    /// the same job name exactly the way the GitHub adapter supersedes a check run.
    #[serde(default)]
    pub id: u64,
    /// Job name; the operator's required-check names are matched against it.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
    /// The commit the status belongs to. GitLab echoes it per entry, which lets the adapter
    /// prove SHA binding from the payload rather than trusting the request alone.
    #[serde(default)]
    pub sha: String,
    /// GitLab's own "this job failing does not fail the pipeline" marker. See
    /// [`classify_gitlab_statuses`] for exactly when it is honoured.
    #[serde(default)]
    pub allow_failure: bool,
}

/// Gitea's `GET /repos/{owner}/{repo}/commits/{ref}/status` document.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GiteaCombinedStatus {
    /// The commit the combined status was computed for; compared against the published head.
    #[serde(default)]
    pub sha: String,
    /// Gitea reports the size of the returned page here rather than the true total (the true
    /// total goes to the `X-Total-Count` header). It is still cross-checked: a Forgejo or
    /// future Gitea that reports a real total makes truncation directly detectable.
    #[serde(default)]
    pub total_count: usize,
    #[serde(default)]
    pub statuses: Vec<GiteaCommitStatus>,
}

/// One entry of Gitea's combined status. Gitea calls the check name `context` and puts the
/// state in `status`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GiteaCommitStatus {
    /// Per-commit status index; latest wins for a repeated context.
    #[serde(default)]
    pub id: u64,
    #[serde(default)]
    pub context: String,
    #[serde(default)]
    pub status: String,
}

/// How one forge state maps onto the gate's three-way decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckVerdict {
    /// Terminal and green (or explicitly not applicable, which cannot hide a red result).
    Success,
    /// Terminal and red.
    Failure,
    /// Not terminal, or terminal in a way this engine does not recognise. Fail-closed.
    Unsettled,
}

/// GitLab's `Ci::HasStatus` states.
///
/// Divergence from the GitHub adapter, stated explicitly because the two gates are otherwise
/// symmetric: GitHub's `action_required` conclusion is classified as a *failure*, while
/// GitLab's closest analogue `manual` (a job waiting for a human to start it) is classified as
/// *unsettled*. A blocking manual job is not a red check that a CI repair leaf could ever fix,
/// and `CiOutcome::Failed` is the only variant that dispatches such a repair — so reporting it
/// as pending, and letting the deadline turn it into `RequiredUnconfirmed`, is both fail-closed
/// and free of an unjustified repair loop.
fn gitlab_verdict(status: &str) -> CheckVerdict {
    match status {
        "success" | "skipped" => CheckVerdict::Success,
        "failed" | "canceled" | "cancelled" | "canceling" | "cancelling" => CheckVerdict::Failure,
        _ => CheckVerdict::Unsettled,
    }
}

/// Gitea's `CommitStatusState`. `warning` is red here because Gitea's own combined-status
/// calculation folds a warning into a failing commit state.
fn gitea_verdict(status: &str) -> CheckVerdict {
    match status {
        "success" | "skipped" => CheckVerdict::Success,
        "error" | "failure" | "warning" => CheckVerdict::Failure,
        _ => CheckVerdict::Unsettled,
    }
}

/// Classify one page of GitLab commit statuses for the published commit `head`.
///
/// Selection mirrors the GitHub adapter: a non-empty `required_checks` selects exactly those
/// job names (latest status id wins), and an absent required name is `Pending`, never a pass.
/// An empty `required_checks` is the best-effort mode and considers every blocking status.
///
/// `allow_failure` is honoured **only** in best-effort mode. In strict mode the operator has
/// named this exact check as required for publication, and that explicit contract outranks
/// GitLab's pipeline-level "this one may fail" hint; in best-effort mode there is no operator
/// contract to honour, so the gate mirrors GitLab's own pipeline semantics instead of
/// inventing a stricter rule than the forge applies to itself.
pub fn classify_gitlab_statuses(
    head: &str,
    statuses: &[GitLabCommitStatus],
    required_checks: &[String],
) -> CiPoll {
    if statuses.len() >= GITLAB_STATUS_PAGE_SIZE {
        return CiPoll::Pending {
            reason: format!(
                "GitLab returned a full page of {} commit statuses; refusing a possibly-truncated pass",
                statuses.len()
            ),
        };
    }
    if let Some(foreign) = statuses
        .iter()
        .find(|status| !status.sha.is_empty() && !status.sha.eq_ignore_ascii_case(head))
    {
        return CiPoll::Pending {
            reason: format!(
                "GitLab reported a status for commit {:?} while {head} was published",
                foreign.sha
            ),
        };
    }
    if statuses.is_empty() {
        return CiPoll::Pending {
            reason: "GitLab has not reported any commit statuses for the published commit".into(),
        };
    }
    let selected: Vec<&GitLabCommitStatus> = if required_checks.is_empty() {
        statuses
            .iter()
            .filter(|status| !status.allow_failure)
            .collect()
    } else {
        let mut selected = Vec::with_capacity(required_checks.len());
        for required in required_checks {
            let Some(status) = statuses
                .iter()
                .filter(|status| status.name.trim() == required)
                .max_by_key(|status| status.id)
            else {
                return CiPoll::Pending {
                    reason: format!(
                        "required GitLab check {required:?} has not reported for the published commit"
                    ),
                };
            };
            selected.push(status);
        }
        selected
    };
    if selected.is_empty() {
        return CiPoll::Pending {
            reason:
                "GitLab reported only non-blocking (allow_failure) statuses for the published commit"
                    .into(),
        };
    }
    classify_selected(
        Forge::GitLab,
        head,
        selected
            .into_iter()
            .map(|status| (status.name.as_str(), status.status.as_str())),
        gitlab_verdict,
    )
}

/// Classify Gitea's combined status document for the published commit `head`. Selection and
/// fail-closed rules are identical to [`classify_gitlab_statuses`]; Gitea has no
/// `allow_failure` concept, so every returned context is blocking.
pub fn classify_gitea_statuses(
    head: &str,
    combined: &GiteaCombinedStatus,
    required_checks: &[String],
) -> CiPoll {
    if combined.statuses.len() >= GITEA_STATUS_PAGE_SIZE {
        return CiPoll::Pending {
            reason: format!(
                "Gitea returned a full page of {} commit statuses; refusing a possibly-truncated pass",
                combined.statuses.len()
            ),
        };
    }
    if combined.total_count != combined.statuses.len() {
        return CiPoll::Pending {
            reason: format!(
                "Gitea reported {} statuses but returned {}; refusing a partial-page pass",
                combined.total_count,
                combined.statuses.len()
            ),
        };
    }
    if !combined.sha.is_empty() && !combined.sha.eq_ignore_ascii_case(head) {
        return CiPoll::Pending {
            reason: format!(
                "Gitea reported the combined status of commit {:?} while {head} was published",
                combined.sha
            ),
        };
    }
    if combined.statuses.is_empty() {
        return CiPoll::Pending {
            reason: "Gitea has not reported any commit statuses for the published commit".into(),
        };
    }
    let selected: Vec<&GiteaCommitStatus> = if required_checks.is_empty() {
        combined.statuses.iter().collect()
    } else {
        let mut selected = Vec::with_capacity(required_checks.len());
        for required in required_checks {
            let Some(status) = combined
                .statuses
                .iter()
                .filter(|status| status.context.trim() == required)
                .max_by_key(|status| status.id)
            else {
                return CiPoll::Pending {
                    reason: format!(
                        "required Gitea check {required:?} has not reported for the published commit"
                    ),
                };
            };
            selected.push(status);
        }
        selected
    };
    classify_selected(
        Forge::Gitea,
        head,
        selected
            .into_iter()
            .map(|status| (status.context.as_str(), status.status.as_str())),
        gitea_verdict,
    )
}

/// Fold an already-selected set of `(name, state)` pairs into one poll classification.
/// The first red check wins and carries a normalized signature; anything unsettled is
/// pending. Only an all-green set returns `Passing`.
fn classify_selected<'a, I>(
    forge: Forge,
    head: &str,
    selected: I,
    verdict: fn(&str) -> CheckVerdict,
) -> CiPoll
where
    I: Iterator<Item = (&'a str, &'a str)>,
{
    let forge_name = forge.display_name();
    for (raw_name, raw_state) in selected {
        let name = if raw_name.trim().is_empty() {
            "<unnamed check>"
        } else {
            raw_name.trim()
        };
        let state = raw_state.trim().to_ascii_lowercase();
        match verdict(state.as_str()) {
            CheckVerdict::Success => {}
            CheckVerdict::Failure => {
                return CiPoll::Failing {
                    signature: AttemptSignature::of_finding(
                        &format!("{} commit check failed", forge.as_config_value()),
                        &format!("{head}:{name}:{state}"),
                    )
                    .as_str()
                    .to_string(),
                    reason: format!("{forge_name} check {name:?} reported {state:?}"),
                };
            }
            CheckVerdict::Unsettled => {
                return CiPoll::Pending {
                    reason: format!("{forge_name} check {name:?} is {state:?}"),
                };
            }
        }
    }
    CiPoll::Passing
}

/// Strict watch: a non-empty set of required checks must be positively confirmed green for the
/// exact published commit before the deadline. A red required check returns `Failed` with a
/// signature; everything else — pending work, an unavailable endpoint, an unparsable response —
/// keeps polling and finally returns `RequiredUnconfirmed`, which never dispatches a CI repair.
///
/// `poll` returns `Err` for any transport-level problem; the reason is deliberately not
/// propagated into the outcome, because it can carry raw client output.
pub fn watch_required<P>(
    forge: Forge,
    head: &str,
    deadline_after: Duration,
    backoff: Duration,
    mut poll: P,
) -> CiOutcome
where
    P: FnMut() -> Result<CiPoll, ()>,
{
    debug_assert!(!head.is_empty());
    let deadline = Instant::now() + deadline_after;
    loop {
        let pending_reason = match poll() {
            Ok(CiPoll::Passing) => return CiOutcome::Passed,
            Ok(CiPoll::Failing { signature, reason }) => {
                return CiOutcome::Failed { signature, reason };
            }
            Ok(CiPoll::Pending { reason }) => reason,
            Err(()) => format!(
                "the typed {} checks endpoint is unavailable",
                forge.display_name()
            ),
        };
        let now = Instant::now();
        if now >= deadline {
            return CiOutcome::RequiredUnconfirmed {
                reason: format!(
                    "required checks for published commit {head} were not confirmed before the deadline while {pending_reason}"
                ),
            };
        }
        sleep_until_next_poll(deadline, now, backoff);
    }
}

/// Best-effort watch: no operator named any required check, so nothing here can block
/// publication. A green snapshot still reports `Passed`; every other terminal or non-terminal
/// state degrades to `BestEffortDegraded`, so an unobserved CI never reads as a confirmed one.
pub fn watch_best_effort<P>(
    head: &str,
    deadline_after: Duration,
    backoff: Duration,
    mut poll: P,
) -> CiOutcome
where
    P: FnMut() -> Result<CiPoll, ()>,
{
    let deadline = Instant::now() + deadline_after;
    loop {
        match poll() {
            Ok(CiPoll::Passing) => return CiOutcome::Passed,
            Ok(CiPoll::Failing { .. }) => {
                return CiOutcome::BestEffortDegraded {
                    reason: format!(
                        "best-effort checks for published commit {head} did not pass; manual confirmation is recommended"
                    ),
                };
            }
            Err(()) => {
                return CiOutcome::BestEffortDegraded {
                    reason: format!(
                        "best-effort checks for published commit {head} are unavailable; manual confirmation is recommended"
                    ),
                };
            }
            Ok(CiPoll::Pending { .. }) => {
                let now = Instant::now();
                if now >= deadline {
                    return CiOutcome::BestEffortDegraded {
                        reason: format!(
                            "best-effort checks for published commit {head} were not confirmed before the deadline; manual confirmation is recommended"
                        ),
                    };
                }
                sleep_until_next_poll(deadline, now, backoff);
            }
        }
    }
}

/// Wait for the configured backoff without ever sleeping past the deadline, and never for
/// zero time, so a misconfigured backoff cannot turn the watch into a busy loop.
fn sleep_until_next_poll(deadline: Instant, now: Instant, backoff: Duration) {
    let remaining = deadline.saturating_duration_since(now);
    thread::sleep(backoff.min(remaining).max(Duration::from_millis(1)));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gitlab(name: &str, status: &str, id: u64) -> GitLabCommitStatus {
        GitLabCommitStatus {
            id,
            name: name.into(),
            status: status.into(),
            sha: String::new(),
            allow_failure: false,
        }
    }

    #[test]
    fn forge_values_round_trip_and_reject_unknown() {
        for forge in [Forge::GitHub, Forge::GitLab, Forge::Gitea] {
            assert_eq!(Forge::parse(forge.as_config_value()), Some(forge));
        }
        assert_eq!(Forge::parse("bitbucket"), None);
        assert_eq!(Forge::parse("GitHub"), None);
        assert_eq!(Forge::default(), Forge::GitHub);
    }

    #[test]
    fn endpoints_are_sha_bound_and_never_flag_like() {
        let head = "a".repeat(40);
        let gitlab = gitlab_statuses_endpoint(&head);
        assert!(gitlab.contains(&head) && gitlab.starts_with("projects/:fullpath/"));
        // Newest first, so a page that hit the cap still carries the deciding results.
        assert!(gitlab.contains("order_by=id&sort=desc"), "{gitlab}");
        assert!(
            !gitlab.contains(' '),
            "a line-continued endpoint must not carry whitespace"
        );
        let gitea = gitea_status_endpoint(&head);
        assert!(gitea.contains(&head) && gitea.starts_with("/repos/{owner}/{repo}/"));
        assert!(!gitlab.starts_with('-') && !gitea.starts_with('-'));
    }

    #[test]
    fn commit_id_guard_refuses_refs_and_abbreviations() {
        assert!(is_full_commit_id(&"a".repeat(40)));
        assert!(is_full_commit_id(&"0".repeat(64)));
        assert!(!is_full_commit_id("main"));
        assert!(!is_full_commit_id(&"a".repeat(39)));
        assert!(!is_full_commit_id(&"z".repeat(40)));
    }

    #[test]
    fn gitlab_allow_failure_is_honoured_only_without_required_checks() {
        let head = "b".repeat(40);
        let mut red = gitlab("flaky", "failed", 1);
        red.allow_failure = true;
        let statuses = vec![gitlab("build", "success", 2), red];
        // Best effort: GitLab's own pipeline semantics apply, so the non-blocking job is not
        // allowed to turn a green pipeline red.
        assert_eq!(
            classify_gitlab_statuses(&head, &statuses, &[]),
            CiPoll::Passing
        );
        // Strict: the operator explicitly required that exact check, which outranks the hint.
        assert!(matches!(
            classify_gitlab_statuses(&head, &statuses, &["flaky".to_string()]),
            CiPoll::Failing { .. }
        ));
    }

    #[test]
    fn gitlab_manual_is_unsettled_not_failed() {
        let head = "c".repeat(40);
        let statuses = vec![gitlab("deploy", "manual", 1)];
        assert!(matches!(
            classify_gitlab_statuses(&head, &statuses, &["deploy".to_string()]),
            CiPoll::Pending { .. }
        ));
    }

    #[test]
    fn gitea_warning_is_red_like_gitea_itself_computes_it() {
        let head = "d".repeat(40);
        let combined = GiteaCombinedStatus {
            sha: head.clone(),
            total_count: 1,
            statuses: vec![GiteaCommitStatus {
                id: 1,
                context: "lint".into(),
                status: "warning".into(),
            }],
        };
        assert!(matches!(
            classify_gitea_statuses(&head, &combined, &[]),
            CiPoll::Failing { .. }
        ));
    }

    #[test]
    fn watch_required_folds_transport_failure_into_unconfirmed() {
        let head = "e".repeat(40);
        let outcome = watch_required(
            Forge::GitLab,
            &head,
            Duration::ZERO,
            Duration::from_millis(1),
            || Err(()),
        );
        let CiOutcome::RequiredUnconfirmed { reason } = outcome else {
            panic!("an unavailable endpoint must never confirm required checks");
        };
        assert!(reason.contains("the typed GitLab checks endpoint is unavailable"));
    }
}
