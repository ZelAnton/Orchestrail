//! Strict adapters from supervised agent evidence to processor commands.
//!
//! These functions deliberately accept only the legacy protocol's machine-readable tail and
//! structured review/merge artifacts. They never infer a successful operation from conversational
//! prose, a process exit alone, or an unverified status label. An executor supplies the selected
//! author and the immutable VCS review coordinate; neither is trusted from an agent report.

use crate::contract::{self, Status};
use crate::processor::{LeafOutcome, MergeOutcome, ReviewOutcome};
use crate::resolvers::{AttemptSignature, Risk};
use crate::supervise::{Reason, Verdict};

/// Convert a supervised coder or fixer report into the only outcomes accepted by the reducer.
/// A timeout/crash may use the reducer's bounded retry budget; all other supervision or protocol
/// failures are terminal escalations. `author` is selected by the engine's routing decision, not
/// copied from untrusted output.
pub fn task_leaf_outcome(verdict: &Verdict, report: &str, author: &str) -> LeafOutcome {
    match verdict.reason {
        Reason::Timeout | Reason::Crash => LeafOutcome::RetryableFailure {
            reason: supervisor_reason(verdict),
        },
        Reason::Cancelled | Reason::Error => LeafOutcome::Escalated {
            reason: supervisor_reason(verdict),
        },
        Reason::Ok => match strict_outcome(report) {
            Some(outcome) if outcome.verdict == "готово" && valid_mode(outcome.field("режим")) =>
            {
                match reported_risk(&outcome) {
                    Ok(Some(risk)) => LeafOutcome::RiskElevated {
                        author: Some(author.to_string()),
                        risk,
                    },
                    // A fix round (`режим=2`, `agents/coder.md` "Режим 2") may additionally
                    // declare `не исправлено` entries (task T-014). Risk elevation above takes
                    // priority and is left untouched: a round that ALSO strictly raised the
                    // task's blast-radius classification keeps the ordinary human-visible risk
                    // path rather than the robotic empty-fixed-set escalation.
                    Ok(None) if outcome.field("режим") == Some("2") => {
                        let wont_fix = outcome.wont_fix();
                        if wont_fix.is_empty() {
                            LeafOutcome::Completed {
                                author: Some(author.to_string()),
                            }
                        } else {
                            LeafOutcome::CompletedWithWontFix {
                                author: Some(author.to_string()),
                                wont_fixed: u32::try_from(wont_fix.len()).unwrap_or(u32::MAX),
                            }
                        }
                    }
                    Ok(None) => LeafOutcome::Completed {
                        author: Some(author.to_string()),
                    },
                    Err(reason) => LeafOutcome::Escalated { reason },
                }
            }
            Some(outcome) if outcome.verdict == "эскалация" => LeafOutcome::Escalated {
                reason: required_reason(&outcome).unwrap_or_else(|| {
                    "agent reported escalation without required причина field".into()
                }),
            },
            Some(outcome) => LeafOutcome::Escalated {
                reason: format!(
                    "invalid coder outcome {:?}; expected готово with режим=1|2|3 or эскалация",
                    outcome.verdict
                ),
            },
            None => LeafOutcome::Escalated {
                reason: "missing machine-readable ИТОГ line in coder report".into(),
            },
        },
    }
}

/// A coder risk is an optional, exact protocol token. Explanatory descriptor prose belongs to
/// the planner field; accepting it here would let an agent smuggle an unverified classification
/// through a machine-readable completion tail.
fn reported_risk(outcome: &contract::Outcome) -> Result<Option<Risk>, String> {
    let risks: Vec<_> = outcome
        .fields
        .iter()
        .filter_map(|(key, value)| (key == "риск").then_some(value.as_str()))
        .collect();
    match risks.as_slice() {
        [] => Ok(None),
        [literal] => Risk::from_token(literal).map(Some).ok_or_else(|| {
            format!(
                "invalid coder риск={literal:?}; expected one exact token: low, medium, or high"
            )
        }),
        _ => Err("coder outcome has duplicate риск fields".into()),
    }
}

/// Convert a supervised per-task review result. A clean transition requires both the declared
/// outcome and the `SUMMARY-R`/no-open-`R-` gate bounded to this invocation. A report with open
/// findings is signed from every marker id, heading, and status so a changed finding is not treated
/// as stagnation.
pub fn task_review_outcome(
    verdict: &Verdict,
    report: &str,
    since: &str,
    until: &str,
    review_sha: &str,
) -> ReviewOutcome {
    if verdict.reason != Reason::Ok {
        return ReviewOutcome::Escalated {
            reason: supervisor_reason(verdict),
        };
    }

    let risk = match review_risk_elevation(report) {
        Ok(risk) => risk,
        Err(reason) => return review_protocol_error(&reason),
    };
    let parsed = contract::parse_review(report);
    let Some(outcome) = strict_outcome(report) else {
        return review_protocol_error("missing machine-readable ИТОГ line");
    };
    let open = parsed.open_review_findings();
    match outcome.verdict.as_str() {
        "готово к слиянию" if parsed.is_clean_pass(since, until) => match risk {
            Some(risk) => ReviewOutcome::CleanRiskElevated {
                review_sha: review_sha.to_string(),
                risk,
            },
            None => ReviewOutcome::Clean {
                review_sha: review_sha.to_string(),
            },
        },
        "открытые находки" if !open.is_empty() => {
            let open_findings = u32::try_from(open.len()).unwrap_or(u32::MAX);
            let signature = finding_signature(open.into_iter());
            match risk {
                Some(risk) => ReviewOutcome::FindingsRiskElevated {
                    signature,
                    risk,
                    open_findings,
                },
                None => ReviewOutcome::Findings {
                    signature,
                    open_findings,
                },
            }
        }
        "эскалация codex" => ReviewOutcome::Escalated {
            reason: required_reason(&outcome)
                .unwrap_or_else(|| "Codex reviewer escalation without причина field".into()),
        },
        // A "ready" claim over an artifact that still carries an open `R-` is a disagreement about
        // findings, not a broken protocol: the reviewer may simply have ignored a finding it did
        // not author (the engine's own review-cycle gate writes one). The safe reading is the
        // conservative one — the round has open findings and owes a fix cycle. It can never
        // authorize a merge, so escalating the whole task here would only convert a recoverable
        // round into a terminal one. A missing fresh `SUMMARY-R` remains a protocol error below:
        // there the reviewer claims a clean gate it never proved during this invocation.
        "готово к слиянию" if !open.is_empty() => {
            let open_findings = u32::try_from(open.len()).unwrap_or(u32::MAX);
            let signature = finding_signature(open.into_iter());
            match risk {
                Some(risk) => ReviewOutcome::FindingsRiskElevated {
                    signature,
                    risk,
                    open_findings,
                },
                None => ReviewOutcome::Findings {
                    signature,
                    open_findings,
                },
            }
        }
        "готово к слиянию" => review_protocol_error(
            "reviewer declared ready but did not provide a fresh clean SUMMARY-R gate",
        ),
        "открытые находки" => {
            review_protocol_error("reviewer declared open findings but no open R-* finding exists")
        }
        other => review_protocol_error(&format!("unknown reviewer outcome {other:?}")),
    }
}

/// Review prose is intentionally free-form, so a reviewer risk elevation needs one narrow
/// marker before the reducer may treat it as descriptor metadata. It is additive to the legacy
/// R-finding: `Риск-повышен: high — public API now affected`.
fn review_risk_elevation(report: &str) -> Result<Option<Risk>, String> {
    let markers: Vec<_> = report
        .lines()
        .filter_map(|line| line.trim().strip_prefix("Риск-повышен:"))
        .map(str::trim)
        .collect();
    match markers.as_slice() {
        [] => Ok(None),
        [marker] => {
            let (level, reason) = marker.split_once(" — ").ok_or_else(|| {
                "reviewer risk marker must be `Риск-повышен: low|medium|high — <reason>`"
                    .to_string()
            })?;
            if reason.trim().is_empty() {
                return Err("reviewer risk marker must include a non-empty reason".into());
            }
            Risk::from_token(level)
                .map(Some)
                .ok_or_else(|| "reviewer risk marker must name exactly low, medium, or high".into())
        }
        _ => Err("review report has duplicate Риск-повышен markers".into()),
    }
}

/// Convert a supervised full-review report. Its gate is independent from task review: it requires
/// `SUMMARY-F` and `F-*` findings, and a clean result is the only route toward publication.
pub fn integration_review_outcome(
    verdict: &Verdict,
    report: &str,
    since: &str,
    until: &str,
    review_sha: &str,
) -> ReviewOutcome {
    if verdict.reason != Reason::Ok {
        return ReviewOutcome::Escalated {
            reason: supervisor_reason(verdict),
        };
    }

    let parsed = contract::parse_review(report);
    let Some(outcome) = strict_outcome(report) else {
        return review_protocol_error("missing machine-readable ИТОГ line");
    };
    let open = parsed.open_integration_findings();
    match outcome.verdict.as_str() {
        "готово к публикации" if parsed.is_clean_integration_pass(since, until) => {
            ReviewOutcome::Clean {
                review_sha: review_sha.to_string(),
            }
        }
        "открытые находки" if !open.is_empty() => ReviewOutcome::Findings {
            open_findings: u32::try_from(open.len()).unwrap_or(u32::MAX),
            signature: finding_signature(open.into_iter()),
        },
        "готово к публикации" => review_protocol_error(
            "full reviewer declared ready but did not provide a fresh clean SUMMARY-F gate",
        ),
        "открытые находки" => review_protocol_error(
            "full reviewer declared open findings but no open F-* finding exists",
        ),
        other => review_protocol_error(&format!("unknown full-review outcome {other:?}")),
    }
}

/// Decode the merger's explicit `merge_report.md` line for one expected task. A nonzero
/// supervised result, an `эскалация слияния` tail, missing line, duplicate line, or a mismatched
/// task outcome is never reinterpreted as a successful merge.
pub fn merge_task_outcome(verdict: &Verdict, report: &str, task_id: &str) -> MergeOutcome {
    if verdict.reason != Reason::Ok {
        return MergeOutcome::Failed {
            reason: supervisor_reason(verdict),
        };
    }
    let Some(outcome) = strict_outcome(report) else {
        return MergeOutcome::Failed {
            reason: "missing machine-readable ИТОГ line in merger report".into(),
        };
    };
    if outcome.verdict == "эскалация слияния" {
        return MergeOutcome::Failed {
            reason: required_reason(&outcome)
                .unwrap_or_else(|| "merger escalated without a причина field".into()),
        };
    }
    if !matches!(outcome.verdict.as_str(), "слито всё" | "есть карантин") {
        return MergeOutcome::Failed {
            reason: format!("unknown merger outcome {:?}", outcome.verdict),
        };
    }
    let lines: Vec<_> = contract::parse_merge_report(report)
        .into_iter()
        .filter(|line| line.id == task_id)
        .collect();
    match lines.as_slice() {
        [line] => match &line.outcome {
            contract::MergeOutcome::Merged { sha, .. } => MergeOutcome::Merged {
                integration_sha: sha.clone(),
            },
            contract::MergeOutcome::Quarantined { reason } => MergeOutcome::Quarantined {
                reason: reason.clone(),
            },
        },
        [] => MergeOutcome::Failed {
            reason: format!("merge report has no result line for {task_id}"),
        },
        _ => MergeOutcome::Failed {
            reason: format!("merge report has duplicate result lines for {task_id}"),
        },
    }
}

fn valid_mode(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "2" | "3"))
}

fn required_reason(outcome: &contract::Outcome) -> Option<String> {
    outcome
        .field("причина")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn review_protocol_error(message: &str) -> ReviewOutcome {
    ReviewOutcome::Escalated {
        reason: format!("invalid review protocol: {message}"),
    }
}

/// Sign a round from every open finding's marker id, heading, and status. Exposed inside the crate
/// so a native gate that must re-derive the same round signature (after it re-reads the artifact the
/// reviewer left behind) uses this exact definition instead of a second, drifting copy.
pub(crate) fn finding_signature<'a>(
    findings: impl Iterator<Item = &'a contract::Finding>,
) -> String {
    let mut fields: Vec<_> = findings
        .map(|finding| {
            let status = match &finding.status {
                Status::New => "new",
                Status::Fixed => "fixed",
                Status::Rejected => "rejected",
                Status::Ready => "ready",
                Status::Other(value) => value.as_str(),
            };
            (
                format!("{}|{}", finding.id, finding.title),
                status.to_string(),
            )
        })
        .collect();
    fields.sort();
    let (messages, evidence): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
    AttemptSignature::of_finding(&messages.join("\n"), &evidence.join("\n"))
        .as_str()
        .to_string()
}

/// A terminal protocol tail must be exactly one `ИТОГ:` line and it must be the final non-empty
/// report line. Accepting a quoted/copy-pasted earlier tail would let later unstructured prose
/// masquerade as a completed leaf.
fn strict_outcome(report: &str) -> Option<contract::Outcome> {
    let active: Vec<_> = report
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if active.last().is_none_or(|line| !line.starts_with("ИТОГ:"))
        || active
            .iter()
            .filter(|line| line.starts_with("ИТОГ:"))
            .count()
            != 1
    {
        return None;
    }
    contract::parse_outcome(report)
}

fn supervisor_reason(verdict: &Verdict) -> String {
    // `outcome_reason` may contain a ProcessKit/OS error that includes an executable path,
    // argv fragment, or other ambient diagnostic. This string is copied into durable task and
    // integration control-plane records, so it must be a closed vocabulary rather than a
    // convenient channel for child stderr or launch details. Raw model reports remain subject to
    // their dedicated evidence-artifact contracts; they are never smuggled through a reason.
    match verdict.reason {
        Reason::Ok => "supervisor ok".into(),
        Reason::Timeout => "supervisor timeout".into(),
        Reason::Cancelled => "supervisor cancelled".into(),
        Reason::Crash => "supervisor crash".into(),
        Reason::Error => "supervisor error".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REVIEW_UNTIL: &str = "2026-07-24T12:00:02Z";

    fn verdict(reason: Reason) -> Verdict {
        Verdict {
            reason,
            exit_code: Some(reason.exit_code()),
            timed_out: reason == Reason::Timeout,
            cancelled: reason == Reason::Cancelled,
            duration_ms: 1,
            stdout: String::new(),
            stderr: String::new(),
            outcome_reason: reason.as_str().into(),
        }
    }

    #[test]
    fn coder_outcome_requires_machine_tail_and_selected_author() {
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: x\nИТОГ: готово · режим=3\n",
                "coder_codex"
            ),
            LeafOutcome::Completed {
                author: Some("coder_codex".into())
            }
        );
        assert!(matches!(
            task_leaf_outcome(&verdict(Reason::Ok), "ИТОГ: готово\n", "coder"),
            LeafOutcome::Escalated { .. }
        ));
        assert!(matches!(
            task_leaf_outcome(&verdict(Reason::Timeout), "", "coder"),
            LeafOutcome::RetryableFailure { .. }
        ));
    }

    #[test]
    fn fix_round_wont_fix_field_yields_completed_with_wont_fix() {
        // A режим=2 fix round that additionally reports `не исправлено` entries (task T-014)
        // decodes to the distinct `CompletedWithWontFix` variant, carrying the count.
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: review.md\nИТОГ: готово \u{00B7} режим=2 \u{00B7} \
не исправлено=R-05=вне скоупа \u{00B7} не исправлено=R-07=false positive\n",
                "coder",
            ),
            LeafOutcome::CompletedWithWontFix {
                author: Some("coder".into()),
                wont_fixed: 2,
            }
        );
    }

    #[test]
    fn fix_round_without_wont_fix_field_stays_ordinary_completed() {
        // Additive and OPTIONAL: an ordinary режим=2 round without the field is unaffected.
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: x\nИТОГ: готово \u{00B7} режим=2\n",
                "coder",
            ),
            LeafOutcome::Completed {
                author: Some("coder".into())
            }
        );
    }

    #[test]
    fn implement_round_wont_fix_field_is_ignored_at_mode_1() {
        // The won't-fix decode is gated on режим=2 (a fix round); a Mode-1 report with the same
        // field text (implausible in practice, but not relied upon) stays ordinary Completed.
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: x\nИТОГ: готово \u{00B7} режим=1 \u{00B7} не исправлено=R-05=x\n",
                "coder",
            ),
            LeafOutcome::Completed {
                author: Some("coder".into())
            }
        );
    }

    #[test]
    fn supervisor_reason_redacts_processkit_diagnostics_from_durable_outcomes() {
        let verdict = Verdict {
            reason: Reason::Crash,
            exit_code: None,
            timed_out: false,
            cancelled: false,
            duration_ms: 1,
            stdout: "child output containing token=super-secret".into(),
            stderr: "stderr containing token=super-secret".into(),
            outcome_reason: "spawn failed: C:/private/bin --token=super-secret".into(),
        };
        assert_eq!(
            task_review_outcome(&verdict, "", "2026-07-24T12:00:00Z", REVIEW_UNTIL, "head"),
            ReviewOutcome::Escalated {
                reason: "supervisor crash".into(),
            }
        );
    }

    #[test]
    fn coder_risk_extension_is_exact_and_remains_separate_from_completion() {
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: x\nИТОГ: готово · режим=2 · риск=high\n",
                "coder"
            ),
            LeafOutcome::RiskElevated {
                author: Some("coder".into()),
                risk: Risk::High,
            }
        );
        assert!(matches!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "ИТОГ: готово · режим=2 · риск=high — public API\n",
                "coder"
            ),
            LeafOutcome::Escalated { reason } if reason.contains("invalid coder риск")
        ));
        assert!(matches!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "ИТОГ: готово · режим=2 · риск=high · риск=low\n",
                "coder"
            ),
            LeafOutcome::Escalated { reason } if reason.contains("duplicate")
        ));
    }

    #[test]
    fn task_review_requires_tail_and_fresh_artifact_gate() {
        let clean = "### [SUMMARY-R-2026-07-24T12:00:01Z] complete — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n";
        assert_eq!(
            task_review_outcome(
                &verdict(Reason::Ok),
                clean,
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head"
            ),
            ReviewOutcome::Clean {
                review_sha: "review-head".into()
            }
        );
        let open_one =
            "### [R-1] missing null check — статус: новая\nИТОГ: открытые находки · открытых=1\n";
        let open_changed =
            "### [R-1] missing bounds check — статус: новая\nИТОГ: открытые находки · открытых=1\n";
        let first = task_review_outcome(
            &verdict(Reason::Ok),
            open_one,
            "2026-07-24T12:00:00Z",
            REVIEW_UNTIL,
            "review-head",
        );
        let changed = task_review_outcome(
            &verdict(Reason::Ok),
            open_changed,
            "2026-07-24T12:00:00Z",
            REVIEW_UNTIL,
            "review-head",
        );
        assert!(matches!(first, ReviewOutcome::Findings { .. }));
        assert_ne!(
            first, changed,
            "changed finding heading must change signature"
        );

        let stale_future = "### [SUMMARY-R-9999-12-31T23:59:59Z] old artifact — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n";
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                stale_future,
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Escalated { reason }
                if reason.contains("fresh clean SUMMARY-R gate")
        ));
    }

    #[test]
    fn ready_claim_over_an_open_finding_owes_a_fix_cycle_rather_than_an_escalation() {
        // The engine's review-cycle gate may put an open `R-` into the artifact before the reviewer
        // runs. A reviewer whose own passes are clean then declares "готово к слиянию" over a file
        // that still has an open finding. That must stay a repeatable round, not a terminal task
        // escalation, and it must never satisfy the clean gate.
        let ready_over_open = "### [R-04] build is broken — статус: новая\n### [SUMMARY-R-2026-07-24T12:00:01Z] complete — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n";
        let outcome = task_review_outcome(
            &verdict(Reason::Ok),
            ready_over_open,
            "2026-07-24T12:00:00Z",
            REVIEW_UNTIL,
            "review-head",
        );
        let ReviewOutcome::Findings {
            signature,
            open_findings,
        } = outcome
        else {
            panic!("a ready claim over an open finding must reduce to Findings: {outcome:?}");
        };
        let declared_open = task_review_outcome(
            &verdict(Reason::Ok),
            "### [R-04] build is broken — статус: новая\nИТОГ: открытые находки · открытых=1\n",
            "2026-07-24T12:00:00Z",
            REVIEW_UNTIL,
            "review-head",
        );
        assert_eq!(
            declared_open,
            ReviewOutcome::Findings {
                signature,
                open_findings
            },
            "the same open findings sign the round the same way whichever tail the reviewer wrote"
        );

        // A risk elevation on the same artifact still reaches the reducer.
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "Риск-повышен: high — public API is now affected\n### [R-04] build is broken — статус: новая\n### [SUMMARY-R-2026-07-24T12:00:01Z] complete — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n",
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::FindingsRiskElevated {
                risk: Risk::High,
                ..
            }
        ));
    }

    #[test]
    fn review_risk_extension_requires_one_exact_reasoned_marker() {
        let elevated = task_review_outcome(
            &verdict(Reason::Ok),
            "Риск-повышен: high — public API is now affected\n### [R-1] missing API test — статус: новая\nИТОГ: открытые находки · открытых=1\n",
            "2026-07-24T12:00:00Z",
            REVIEW_UNTIL,
            "review-head",
        );
        assert!(matches!(
            elevated,
            ReviewOutcome::FindingsRiskElevated {
                risk: Risk::High,
                ..
            }
        ));
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "Риск-повышен: high\n### [R-1] missing API test — статус: новая\nИТОГ: открытые находки · открытых=1\n",
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Escalated { reason } if reason.contains("marker")
        ));
    }

    #[test]
    fn reviewer_supervision_failures_are_terminal_not_incomplete_retries() {
        for reason in [
            Reason::Timeout,
            Reason::Crash,
            Reason::Cancelled,
            Reason::Error,
        ] {
            assert!(matches!(
                task_review_outcome(
                    &verdict(reason),
                    "",
                    "2026-07-24T12:00:00Z",
                    REVIEW_UNTIL,
                    "review"
                ),
                ReviewOutcome::Escalated { .. }
            ));
            assert!(matches!(
                integration_review_outcome(
                    &verdict(reason),
                    "",
                    "2026-07-24T12:00:00Z",
                    REVIEW_UNTIL,
                    "integration"
                ),
                ReviewOutcome::Escalated { .. }
            ));
        }
    }

    #[test]
    fn integration_and_merger_protocols_are_independent_and_exact() {
        let integration = "### [SUMMARY-F-2026-07-24T12:00:01Z] complete — статус: готово к слиянию\nИТОГ: готово к публикации · открытых=0\n";
        assert!(matches!(
            integration_review_outcome(
                &verdict(Reason::Ok),
                integration,
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "integration-head"
            ),
            ReviewOutcome::Clean { .. }
        ));
        let stale_future = "### [SUMMARY-F-9999-12-31T23:59:59Z] old artifact — статус: готово к публикации\nИТОГ: готово к публикации · открытых=0\n";
        assert!(matches!(
            integration_review_outcome(
                &verdict(Reason::Ok),
                stale_future,
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "integration-head",
            ),
            ReviewOutcome::Escalated { reason }
                if reason.contains("fresh clean SUMMARY-F gate")
        ));
        let merged =
            "- [T-1] merged=integration-head\nИТОГ: слито всё · слито=1 · карантин=0 · сборка=ok\n";
        assert_eq!(
            merge_task_outcome(&verdict(Reason::Ok), merged, "T-1"),
            MergeOutcome::Merged {
                integration_sha: "integration-head".into()
            }
        );
        assert!(matches!(
            merge_task_outcome(&verdict(Reason::Ok), merged, "T-2"),
            MergeOutcome::Failed { .. }
        ));
    }
}
