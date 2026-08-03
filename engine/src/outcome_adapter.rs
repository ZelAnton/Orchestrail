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
///
/// `open_finding_ids` is the exact set of `R-`/`F-` ids the round being judged was DISPATCHED to
/// address (`TaskRuntime::pending_fix_open_finding_ids`, R-06) — `None` for every leaf role that
/// never runs a task fix cycle (merger, curators, CI fix, ...) or for a checkpoint predating R-06.
/// It bounds which `не исправлено=<id>` entries `Outcome::wont_fix` may count toward `wont_fixed`:
/// only a distinct id that is ALSO a member of this set counts, so a fixer that mentions a
/// duplicate, stale, or otherwise unrelated finding id it never worked this round cannot inflate
/// `wont_fixed` past the round's real open-finding count and trigger a false empty-fixed-set
/// escalation (R-06). `None` — the set is not merely empty but ABSENT (see the two variants of that
/// on `TaskRuntime::pending_fix_open_finding_ids`) — degrades to the pre-R-06 behaviour of
/// deduplicating only, never fabricated membership; `Some(&[])` is a known set like any other and
/// admits nothing (R-08).
pub fn task_leaf_outcome(
    verdict: &Verdict,
    report: &str,
    author: &str,
    open_finding_ids: Option<&[String]>,
) -> LeafOutcome {
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
                let risk = match reported_risk(&outcome) {
                    Ok(risk) => risk,
                    Err(reason) => return LeafOutcome::Escalated { reason },
                };
                // Risk elevation and Mode-2 won't-fix metadata describe independent facts about
                // one successful round. Decode both before selecting the structured outcome so
                // neither signal can suppress the other.
                let wont_fixed = (outcome.field("режим") == Some("2"))
                    .then(|| validated_wont_fix_count(&outcome, open_finding_ids))
                    .filter(|&count| count > 0);
                match (risk, wont_fixed) {
                    (Some(risk), wont_fixed) => LeafOutcome::RiskElevated {
                        author: Some(author.to_string()),
                        risk,
                        wont_fixed,
                    },
                    (None, Some(wont_fixed)) => LeafOutcome::CompletedWithWontFix {
                        author: Some(author.to_string()),
                        wont_fixed,
                    },
                    (None, None) => LeafOutcome::Completed {
                        author: Some(author.to_string()),
                    },
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

/// The count `empty_fixed_set_decision` (R-06) may trust: DISTINCT `не исправлено=<id>` entries,
/// further bounded to ids that are members of `open_finding_ids` when that set is known. Neither a
/// repeated id (task T-014's original count was a bare `entries.len()`) nor an id naming a
/// finding this round was never dispatched to address (a duplicate, stale, or otherwise
/// unrelated `R-`/`F-` the fixer merely mentioned) can inflate the count past the round's real
/// open-finding total. `open_finding_ids: None` — the set is simply unknown for this leaf role or
/// checkpoint — degrades to deduplication alone, never a fabricated membership proof. An empty but
/// KNOWN set (`Some(&[])`) is not that case and keeps filtering everything out (R-08): a round
/// whose open ids are all accounted for admits no won't-fix entry at all.
fn validated_wont_fix_count(
    outcome: &contract::Outcome,
    open_finding_ids: Option<&[String]>,
) -> u32 {
    let entries = outcome.wont_fix();
    let mut ids: Vec<&str> = entries
        .iter()
        .map(|entry| entry.finding_id.as_str())
        .filter(|id| open_finding_ids.is_none_or(|open| open.iter().any(|open_id| open_id == id)))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    u32::try_from(ids.len()).unwrap_or(u32::MAX)
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

/// Convert a supervised per-task review result into the phase-2.6/2.7/2.8 branch
/// (`agents/processor.md`) — the SAME three-way gate the pure resolver
/// [`crate::resolvers::review_gate`] compiles, plus the facts a pure function over the artifact
/// cannot see (how the call itself ended, and what the reviewer claimed about it).
/// A clean transition requires the declared outcome AND the `SUMMARY-R`/no-open-`R-` gate bounded
/// to this invocation. A report with open findings is signed from every marker id, heading, and
/// status so a changed finding is not treated as stagnation.
///
/// # One contract, two implementations — the deliberate divergence (T-026, in the style of K-006)
///
/// The branch order below is [`crate::resolvers::gate::review_gate`]'s decision tree, restated
/// over the same parsed artifact:
///
/// 1. open `R-` present → `Findings` (2.8),
/// 2. else a `SUMMARY-R` inside this invocation window → `Clean` (2.6),
/// 3. else the pass is **incomplete** (2.7) — re-run the SAME reviewer inside `REVIEW_LOOP_MAX`.
///
/// This adapter **deliberately tightens two of those steps, in the same direction — never the
/// branch order**. Both are stated here rather than left implicit, because both are real
/// divergences from the pure resolver:
///
/// 1. **Step 2 needs the completion tail.** The engine's reviewer prompt requires `review.md` to
///    END with an exact `ИТОГ: готово к слиянию · открытых=0`, so a clean promotion additionally
///    needs that decodable tail. An artifact `review_gate` would call `Clean` but that carries no
///    such tail is NOT promoted — it falls to step 3, costing one bounded repeat.
/// 2. **Step 3 excludes the one artifact that cannot converge.** A `SUMMARY-R` dated above the
///    call window makes the clean gate unprovable FOREVER (see below), so `review_gate`'s
///    `Incomplete` verdict for that shape becomes a terminal escalation here (R-14).
///
/// Both make production stricter than the resolver, never looser, and neither promotes anything
/// the resolver would not.
///
/// **What the tightening may cost is a bounded retry, never the task.** The three CONVERGING ways
/// an artifact can fail to prove the clean gate — a missing tail (the classic `maxTurns`
/// truncation), a `готово к слиянию` tail over an absent or too-OLD `SUMMARY-R`, an `открытые
/// находки` tail with no open `R-` — are exactly `agents/processor.md` 2.7 «ревьюер был прерван до
/// завершения»: they land in [`ReviewOutcome::Incomplete`], whose reducer arm
/// (`processor::task_review`) spends one `Циклов-ревью` unit and re-dispatches the same reviewer
/// until `REVIEW_LOOP_MAX` is exhausted, then escalates. Before T-026 those three shapes escalated
/// the whole task on the first occurrence, so a transient reviewer truncation burned a task that
/// the contract says to simply re-run — while `resolvers/gate.rs` and `run.rs` documented the
/// opposite semantics for the same phases. The `Incomplete` arm of the reducer was, in consequence,
/// practically unreachable from this adapter.
///
/// **The absence of a claim is not a claim.** Escalation stays reserved for facts, not for gaps:
///
/// * a supervision failure (timeout/crash/cancel/error) — a fact about the call, and the one input
///   `review_gate` structurally cannot have;
/// * an explicit `ИТОГ: эскалация codex` — the reviewer declaring itself unable to review;
/// * an UNDECODABLE positive claim by a reviewer that did finish its tail: an unknown `ИТОГ:`
///   verdict word, or a malformed `Риск-повышен:` marker. Repeating a reviewer that speaks a
///   foreign protocol has no reason to converge, so these stay terminal exactly as before;
/// * a `SUMMARY-R` dated ABOVE the call window ([`contract::ReviewParse::summary_after_window`]) —
///   a fact about the ARTIFACT rather than the claim, and the fourth shape's separation from the
///   three above (R-14). Summaries are append-only, so a future-dated mark stays the chronological
///   maximum forever and no later pass — not even one that honestly stamps a fresh summary — can
///   re-prove the gate. Here the uselessness of a repeat is provable, not merely likely, so the
///   round stays terminal and the reason names the mark instead of burning `REVIEW_LOOP_MAX` calls
///   to reach an anonymous «не сходится ревью».
///
/// A `готово к слиянию` tail over an artifact that still carries an open `R-` remains `Findings`
/// (it is a disagreement about findings, not a broken protocol — the engine's own review-cycle
/// gate writes findings the reviewer did not author), and step 1 now covers the tail-less form of
/// the same artifact for the same reason.
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
    // The completion tail is OPTIONAL evidence here, not a precondition of reading the artifact:
    // its absence is the 2.7 truncation signature, decided below with the rest of the gate.
    let tail = strict_outcome(report);
    if let Some(outcome) = &tail {
        match outcome.verdict.as_str() {
            "эскалация codex" => {
                return ReviewOutcome::Escalated {
                    reason: required_reason(outcome).unwrap_or_else(|| {
                        "Codex reviewer escalation without причина field".into()
                    }),
                };
            }
            "готово к слиянию" | "открытые находки" => {}
            other => return review_protocol_error(&format!("unknown reviewer outcome {other:?}")),
        }
    }
    let open = parsed.open_review_findings();
    if !open.is_empty() {
        let open_findings = u32::try_from(open.len()).unwrap_or(u32::MAX);
        let open_finding_ids = open.iter().map(|finding| finding.id.clone()).collect();
        let signature = finding_signature(open.into_iter());
        return match risk {
            Some(risk) => ReviewOutcome::FindingsRiskElevated {
                signature,
                risk,
                open_findings,
                open_finding_ids,
            },
            None => ReviewOutcome::Findings {
                signature,
                open_findings,
                open_finding_ids,
            },
        };
    }
    if tail
        .as_ref()
        .is_some_and(|outcome| outcome.verdict == "готово к слиянию")
        && parsed.is_clean_pass(since, until)
    {
        return match risk {
            Some(risk) => ReviewOutcome::CleanRiskElevated {
                review_sha: review_sha.to_string(),
                risk,
            },
            None => ReviewOutcome::Clean {
                review_sha: review_sha.to_string(),
            },
        };
    }
    // 2.7 has exactly one carve-out, and it is not about the reviewer's claim but about the
    // artifact's ability to ever prove the gate again. A `SUMMARY-R` dated ABOVE this call's window
    // is append-only poison: it stays `latest_summary()`'s chronological maximum forever (the
    // reviewer protocol never deletes summaries), so `is_clean_pass` is false for this round AND
    // for every later one — including a round whose reviewer honestly stamps a fresh, correct
    // summary. Repeating here would spend the whole `REVIEW_LOOP_MAX` budget on calls that cannot
    // converge and then escalate with the anonymous «не сходится ревью», hiding the one fact an
    // operator needs. So this shape stays terminal, exactly as it was before T-026, and the reason
    // names it. The offending mark is quoted because `is_marker_id` admits a `SUMMARY-R-` id only
    // with a strict `YYYY-MM-DDTHH:MM:SS(.d{1,3})?Z` timestamp behind it: a bounded protocol token
    // like the verdict words above, not agent free text (see `supervisor_reason`).
    if let Some(mark) = parsed.summary_after_window(until) {
        return review_protocol_error(&format!(
            "reviewer's latest SUMMARY-R {mark} is dated after this call's window end {until}; \
             summaries are never deleted, so no later pass can re-prove the clean gate"
        ));
    }
    // 2.7: no open `R-` and no proved clean pass. A risk elevation this round may have reported is
    // deliberately dropped rather than carried on a unit variant: the round is repeated, not
    // concluded, and the repeat re-derives the marker from the artifact it rewrites.
    ReviewOutcome::Incomplete
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
            open_finding_ids: open.iter().map(|finding| finding.id.clone()).collect(),
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
                "coder_codex",
                None,
            ),
            LeafOutcome::Completed {
                author: Some("coder_codex".into())
            }
        );
        assert!(matches!(
            task_leaf_outcome(&verdict(Reason::Ok), "ИТОГ: готово\n", "coder", None),
            LeafOutcome::Escalated { .. }
        ));
        assert!(matches!(
            task_leaf_outcome(&verdict(Reason::Timeout), "", "coder", None),
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
                None,
            ),
            LeafOutcome::CompletedWithWontFix {
                author: Some("coder".into()),
                wont_fixed: 2,
            }
        );
    }

    // -- Won't-fix id validation (R-06) ------------------------------------------------------

    #[test]
    fn wont_fix_entry_naming_a_stale_unrelated_finding_does_not_count_toward_wont_fixed() {
        // Exactly the R-06 scenario: the fixer genuinely fixed the round's ONLY open finding
        // (R-05, absent from не исправлено=) but the report ALSO cites a stale/unrelated id
        // (R-03, closed in an earlier cycle and not a member of this round's open-finding set).
        // Before R-06 this inflated `wont_fixed` to 1 — equal to `open_findings`, wrongly
        // triggering `empty_fixed_set_decision`'s terminal escalation over a round that actually
        // made full progress. With the round's real open-finding ids supplied, the stale id is
        // filtered out and the round decodes as an ordinary `Completed`, never `CompletedWithWontFix`.
        let open_finding_ids = vec!["R-05".to_string()];
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: review.md\nИТОГ: готово \u{00B7} режим=2 \u{00B7} \
не исправлено=R-03=устарело\n",
                "coder",
                Some(&open_finding_ids),
            ),
            LeafOutcome::Completed {
                author: Some("coder".into())
            },
            "a stale id absent from this round's open findings must not manufacture a wont-fix round"
        );
    }

    #[test]
    fn wont_fix_entry_naming_an_open_finding_still_counts_when_ids_are_known() {
        // The membership filter is not a blanket suppression: an entry that DOES name one of the
        // round's own open findings still counts, alongside a stale one that does not.
        let open_finding_ids = vec!["R-05".to_string(), "R-06".to_string()];
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: review.md\nИТОГ: готово \u{00B7} режим=2 \u{00B7} \
не исправлено=R-06=вне скоупа \u{00B7} не исправлено=R-03=устарело\n",
                "coder",
                Some(&open_finding_ids),
            ),
            LeafOutcome::CompletedWithWontFix {
                author: Some("coder".into()),
                wont_fixed: 1,
            },
            "R-06 is a real member of this round and must still count; the stale R-03 must not"
        );
    }

    #[test]
    fn an_empty_known_id_set_admits_nothing_while_an_absent_one_still_degrades() {
        // R-08: the two are deliberately NOT the same input.
        //   * `Some(&[])` — the round reported its open set and it is empty. Like any known set it
        //     is authoritative, so no `не исправлено=` entry is a member and none counts. The
        //     round decodes as an ordinary `Completed`, and `empty_fixed_set_decision` cannot
        //     escalate it on the strength of an id the round never opened.
        //   * `None` — there is no such set to check against (a pre-R-06 checkpoint, a leaf role
        //     with no fix cycle), so the count degrades to deduplication alone rather than
        //     fabricating membership.
        // Reading emptiness as absence would collapse the first case into the second and re-open
        // R-06's false terminal escalation on the one live route that produces an empty set.
        let report = "Изменённые файлы: review.md\nИТОГ: готово \u{00B7} режим=2 \u{00B7} \
не исправлено=R-03=устарело\n";
        assert_eq!(
            task_leaf_outcome(&verdict(Reason::Ok), report, "coder", Some(&[])),
            LeafOutcome::Completed {
                author: Some("coder".into())
            },
            "a known-empty open set admits no won't-fix entry at all"
        );
        assert_eq!(
            task_leaf_outcome(&verdict(Reason::Ok), report, "coder", None),
            LeafOutcome::CompletedWithWontFix {
                author: Some("coder".into()),
                wont_fixed: 1,
            },
            "an ABSENT set keeps the documented pre-R-06 degradation: deduplicated, unvalidated"
        );
    }

    #[test]
    fn wont_fix_duplicate_entries_are_deduplicated_before_counting() {
        // task T-014's original count was a bare `entries.len()`; a fixer that repeats the same
        // id (deliberately or not) must not inflate the count past the number of DISTINCT
        // findings actually declined, matching `empty_fixed_set_decision`'s documented contract.
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: review.md\nИТОГ: готово \u{00B7} режим=2 \u{00B7} \
не исправлено=R-05=вне скоупа \u{00B7} не исправлено=R-05=вне скоупа\n",
                "coder",
                None,
            ),
            LeafOutcome::CompletedWithWontFix {
                author: Some("coder".into()),
                wont_fixed: 1,
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
                None,
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
                None,
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
                "coder",
                None,
            ),
            LeafOutcome::RiskElevated {
                author: Some("coder".into()),
                risk: Risk::High,
                wont_fixed: None,
            }
        );
        assert!(matches!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "ИТОГ: готово · режим=2 · риск=high — public API\n",
                "coder",
                None,
            ),
            LeafOutcome::Escalated { reason } if reason.contains("invalid coder риск")
        ));
        assert!(matches!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "ИТОГ: готово · режим=2 · риск=high · риск=low\n",
                "coder",
                None,
            ),
            LeafOutcome::Escalated { reason } if reason.contains("duplicate")
        ));
    }

    #[test]
    fn fix_round_preserves_risk_elevation_and_wont_fix_count_together() {
        assert_eq!(
            task_leaf_outcome(
                &verdict(Reason::Ok),
                "Изменённые файлы: x\nИТОГ: готово · режим=2 · риск=high · не исправлено=R-01=out of scope\n",
                "coder",
                None,
            ),
            LeafOutcome::RiskElevated {
                author: Some("coder".into()),
                risk: Risk::High,
                wont_fixed: Some(1),
            }
        );
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

        // A `SUMMARY-R` outside the invocation window never authorizes the promotion — but WHICH
        // side of the window it fell out of decides what happens next (R-14). Below the window
        // (older than `since`) the gate is merely unproved for this round, so it is the phase-2.7
        // repeat (T-026): the next pass writes a newer summary and that one proves it.
        let stale_old = "### [SUMMARY-R-2026-07-24T11:00:00Z] previous round — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n";
        assert_eq!(
            task_review_outcome(
                &verdict(Reason::Ok),
                stale_old,
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Incomplete
        );
        // Above the window, no next pass can help: summaries are append-only, so this mark stays
        // the chronological maximum forever. Terminal, and the reason names the mark.
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
                if reason.contains("9999-12-31T23:59:59Z") && reason.contains(REVIEW_UNTIL)
        ));
    }

    // -- Phase 2.7 «reviewer interrupted» on the production path (T-026) ----------------------

    #[test]
    fn a_report_cut_short_before_its_итог_tail_is_a_bounded_repeat_not_a_terminal_escalation() {
        // The classic `maxTurns` truncation: the reviewer wrote real prose into `review.md` and
        // was cut before the mandatory completion tail. `resolvers::review_gate` has always called
        // this 2.7 (no open `R-`, no fresh `SUMMARY-R`); the production adapter used to escalate
        // the whole task on «missing machine-readable ИТОГ line», burning a task the contract says
        // to simply re-run inside `REVIEW_LOOP_MAX`.
        let truncated = "## Проход 1\nThe diff touches engine/src/processor.rs; checking the\n";
        assert_eq!(
            task_review_outcome(
                &verdict(Reason::Ok),
                truncated,
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Incomplete
        );
        // Truncated even earlier — nothing at all in the artifact — is the same round state, and
        // agrees with `finish_task_review`'s own absent/unchanged-artifact shortcut.
        assert_eq!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "",
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Incomplete
        );
        // A truncation that DID leave open findings behind is phase 2.8, exactly as `review_gate`
        // orders the branches: open `R-` take precedence over the missing tail, and the fixer is
        // handed a real (if partial) list rather than the reviewer being re-run.
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "### [R-1] missing null check — статус: новая\nЕщё раз проверяю",
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Findings {
                open_findings: 1,
                ..
            }
        ));
    }

    #[test]
    fn a_round_with_no_open_findings_and_no_fresh_summary_repeats_the_reviewer() {
        // The second historically divergent shape: the reviewer finished its tail but the artifact
        // proves neither branch. Whether it claims readiness over a `SUMMARY-R` it never wrote
        // this invocation, or claims findings that are not open, the round is 2.7 — repeat the
        // same reviewer. It can never authorize a merge, so a terminal escalation here would only
        // convert a recoverable round into a lost task.
        for artifact in [
            // Ready claim, no `SUMMARY-R` at all.
            "### [R-1] fixed earlier — статус: исправлено\nИТОГ: готово к слиянию · открытых=0\n",
            // Ready claim over a summary older than this invocation window.
            "### [SUMMARY-R-2026-07-24T11:00:00Z] previous round — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n",
            // Findings claim with no open `R-` behind it.
            "### [R-1] fixed earlier — статус: исправлено\nИТОГ: открытые находки · открытых=1\n",
        ] {
            assert_eq!(
                task_review_outcome(
                    &verdict(Reason::Ok),
                    artifact,
                    "2026-07-24T12:00:00Z",
                    REVIEW_UNTIL,
                    "review-head",
                ),
                ReviewOutcome::Incomplete,
                "an unproved, findings-free round must repeat the reviewer: {artifact:?}"
            );
        }
    }

    #[test]
    fn a_future_dated_summary_stays_terminal_because_no_later_pass_can_supersede_it() {
        // R-14: the one shape the 2.7 widening must NOT swallow. `latest_summary` is a chronological
        // `max` and the reviewer protocol never deletes a `SUMMARY-R`, so a mark dated above the
        // call window is permanent poison — `is_clean_pass` is false for this round and for every
        // round after it. Repeating would spend all of `REVIEW_LOOP_MAX` on calls that cannot
        // converge and end at the anonymous «не сходится ревью»; here the uselessness of a repeat
        // is PROVABLE, unlike the three shapes above, so the round escalates immediately with the
        // mark named.
        const SINCE: &str = "2026-07-24T12:00:00Z";
        let future = "### [SUMMARY-R-9999-12-31T23:59:59Z] broken clock — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n";
        assert!(matches!(
            task_review_outcome(&verdict(Reason::Ok), future, SINCE, REVIEW_UNTIL, "review-head"),
            ReviewOutcome::Escalated { reason }
                if reason.contains("9999-12-31T23:59:59Z") && reason.contains(REVIEW_UNTIL)
        ));

        // The non-convergence itself, made explicit: adding the honest, in-window summary a LATER
        // reviewer pass would write changes nothing, because the future mark is still the maximum.
        // This is what distinguishes the shape from a merely stale one and what makes a bounded
        // repeat pointless rather than merely expensive.
        let future_plus_honest_retry = "### [SUMMARY-R-9999-12-31T23:59:59Z] broken clock — статус: готово к слиянию\n### [SUMMARY-R-2026-07-24T12:00:01Z] honest retry — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n";
        assert!(
            matches!(
                task_review_outcome(
                    &verdict(Reason::Ok),
                    future_plus_honest_retry,
                    SINCE,
                    REVIEW_UNTIL,
                    "review-head",
                ),
                ReviewOutcome::Escalated { .. }
            ),
            "a repeat that writes a correct fresh summary still cannot prove the gate"
        );

        // The poison is a property of the ARTIFACT, not of the reviewer's claim, so a truncated
        // report carrying the same mark is terminal too — the tail is not what decides it.
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "### [SUMMARY-R-9999-12-31T23:59:59Z] broken clock — статус: готово к слиянию\nещё читаю диапазон\n",
                SINCE,
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Escalated { .. }
        ));

        // Branch ORDER is untouched: an open `R-` is still phase 2.8 and outranks the artifact's
        // freshness state, exactly as `review_gate` orders it. The fixer gets its real list; the
        // poisoned mark is only reached once the findings are gone.
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "### [R-1] missing null check — статус: новая\n### [SUMMARY-R-9999-12-31T23:59:59Z] broken clock — статус: готово к слиянию\nИТОГ: открытые находки · открытых=1\n",
                SINCE,
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Findings {
                open_findings: 1,
                ..
            }
        ));

        // The second deliberate divergence from `resolvers::review_gate`, pinned like the first:
        // the pure resolver decides on the artifact's CURRENT state and calls this `Incomplete`,
        // which is right for what it can see. Production additionally knows the mark can never be
        // superseded, so it is stricter here — the opposite direction from the tail divergence
        // below, and the reason `Incomplete` must not be read as "production will retry".
        assert_eq!(
            crate::resolvers::review_gate(&contract::parse_review(future), SINCE, REVIEW_UNTIL),
            crate::resolvers::ReviewGate::Incomplete,
            "the pure resolver sees only that no clean pass is proved"
        );
    }

    #[test]
    fn an_undecodable_or_explicit_reviewer_claim_stays_terminal() {
        // The 2.7 widening is about the ABSENCE of a claim. A reviewer that finished its tail and
        // made a claim the engine cannot decode — a foreign verdict word, a malformed risk marker
        // — has no reason to converge on a repeat, and an explicit Codex escalation is a decision,
        // not an interruption. All three stay terminal exactly as before T-026.
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "ИТОГ: готово · режим=1\n",
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Escalated { reason } if reason.contains("unknown reviewer outcome")
        ));
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "Риск-повышен: высокий — public API\nИТОГ: готово к слиянию · открытых=0\n",
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Escalated { reason } if reason.contains("marker")
        ));
        assert!(matches!(
            task_review_outcome(
                &verdict(Reason::Ok),
                "ИТОГ: эскалация codex · причина=sandbox denied the review range\n",
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Escalated { reason } if reason.contains("sandbox denied")
        ));
    }

    #[test]
    fn review_gate_and_the_production_adapter_name_the_same_branch() {
        // The anti-drift net for T-026: for every artifact that carries the completion tail the
        // engine's reviewer prompt mandates, the compiled resolver and the production adapter must
        // name the SAME phase-2.6/2.7/2.8 branch. TWO intentional exceptions are pinned separately
        // instead of being listed here, and in this one gate production is stricter than the
        // resolver both times, in opposite directions:
        //   * a tail-less clean-looking artifact — resolver `Clean`, production `Incomplete`
        //     (`the_clean_branch_stays_strictly_tighter_than_the_pure_gate`, right below);
        //   * a `SUMMARY-R` dated above the window — resolver `Incomplete`, production `Escalated`
        //     (`a_future_dated_summary_stays_terminal_because_no_later_pass_can_supersede_it`).
        // Neither may be added to the table below; adding either would assert the drift instead of
        // the agreement.
        use crate::resolvers::{ReviewGate, review_gate};
        const SINCE: &str = "2026-07-24T12:00:00Z";
        for (artifact, expected) in [
            (
                "### [R-1] missing null check — статус: новая\nИТОГ: открытые находки · открытых=1\n",
                ReviewGate::Findings,
            ),
            (
                "### [SUMMARY-R-2026-07-24T12:00:01Z] complete — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n",
                ReviewGate::Clean,
            ),
            // Interrupted, in each of the shapes the two implementations used to disagree on.
            (
                "### [SUMMARY-R-2026-07-24T11:00:00Z] previous round — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n",
                ReviewGate::Incomplete,
            ),
            (
                "### [R-1] done — статус: исправлено\nИТОГ: открытые находки · открытых=1\n",
                ReviewGate::Incomplete,
            ),
            ("", ReviewGate::Incomplete),
            ("## Проход 1\nещё читаю диапазон\n", ReviewGate::Incomplete),
        ] {
            let resolved = review_gate(&contract::parse_review(artifact), SINCE, REVIEW_UNTIL);
            assert_eq!(resolved, expected, "resolver branch for {artifact:?}");
            let produced =
                task_review_outcome(&verdict(Reason::Ok), artifact, SINCE, REVIEW_UNTIL, "head");
            let matched = matches!(
                (expected, &produced),
                (ReviewGate::Findings, ReviewOutcome::Findings { .. })
                    | (ReviewGate::Clean, ReviewOutcome::Clean { .. })
                    | (ReviewGate::Incomplete, ReviewOutcome::Incomplete)
            );
            assert!(
                matched,
                "production adapter named {produced:?} where the resolver named {expected:?} for {artifact:?}"
            );
        }
    }

    #[test]
    fn the_clean_branch_stays_strictly_tighter_than_the_pure_gate() {
        // The single deliberate divergence from `resolvers::review_gate`, pinned as a test so it
        // cannot drift back: an artifact whose freshness gate the resolver accepts is NOT promoted
        // without the mandated completion tail. The resolver would call it `Clean`; production
        // calls it `Incomplete` — tighter, and only ever at the cost of one bounded repeat.
        let untailed_clean =
            "### [SUMMARY-R-2026-07-24T12:00:01Z] complete — статус: готово к слиянию\n";
        let parsed = contract::parse_review(untailed_clean);
        assert_eq!(
            crate::resolvers::review_gate(&parsed, "2026-07-24T12:00:00Z", REVIEW_UNTIL),
            crate::resolvers::ReviewGate::Clean,
            "the pure resolver decides on the artifact alone"
        );
        assert_eq!(
            task_review_outcome(
                &verdict(Reason::Ok),
                untailed_clean,
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Incomplete,
            "production additionally requires the ИТОГ tail before promoting a clean pass"
        );
        // With the tail present the two agree again, which is what makes the divergence a
        // tightening of one branch rather than a second semantics.
        assert_eq!(
            task_review_outcome(
                &verdict(Reason::Ok),
                &format!("{untailed_clean}ИТОГ: готово к слиянию · открытых=0\n"),
                "2026-07-24T12:00:00Z",
                REVIEW_UNTIL,
                "review-head",
            ),
            ReviewOutcome::Clean {
                review_sha: "review-head".into()
            }
        );
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
            open_finding_ids,
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
                open_findings,
                open_finding_ids,
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
