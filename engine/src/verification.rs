//! ProcessKit-contained verification of an operator command profile.
//!
//! The legacy configuration exposes human-authored command strings, so compatibility requires a
//! platform shell.  The shell itself is still launched only through [`crate::supervise`], with a
//! verified integration worktree as its child directory, deadline, lease-loss cancellation,
//! bounded
//! capture, and tree containment.  This module never treats an absent profile as a successful
//! build gate.
//!
//! Two callers share that one execution body. [`verify_integration`] is the Phase-4 publication
//! gate: its result authorises (or refuses) a tip and is persisted as durable evidence.
//! [`verify_review_cycle`] is the optional per-round preview inside a task worktree (phases
//! 2.5/2.8): it authorises nothing and can only add a review finding, but it must not be allowed
//! to run under weaker containment than the gate it previews, which is why the two share
//! `run_command_sequence` rather than each owning a copy.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::command_line::parse_typed_argv;
use crate::config::VerificationMode;
use crate::processor::VerificationOutcome;
use crate::resolvers::AttemptSignature;
use crate::supervise::{self, CancellationProbe, Reason, SpawnSpec};
use crate::work_fs;

const MISSING_PROFILE_REASON: &str =
    "verification profile is required but VERIFICATION_COMMANDS and SMOKE_CMD are absent";
const MAX_EVIDENCE_BYTES: u64 = 1024 * 1024;

/// The durable, non-secret evidence returned with a Phase-4 verification decision.  The caller
/// persists this before asking an integration reviewer to proceed, so a crash cannot silently
/// lose the identity of the command profile which authorised the next phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationRun {
    pub outcome: VerificationOutcome,
    pub transcript: String,
    /// The immutable command profile that authorised (or explicitly exempted) this outcome.
    /// It is persisted in `verification.json` by the headless adapter and must never be inferred
    /// from a later mutable `config.md` read during recovery.
    pub profile: VerificationProfile,
    /// One result per command that actually started, in deterministic profile order.
    pub commands: Vec<VerificationCommandRun>,
}

/// Legacy-compatible, hash-bound verification-profile snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationProfile {
    pub mode: String,
    pub state: String,
    pub source: String,
    pub commands: Vec<String>,
    pub fingerprint: String,
}

/// Typed result of one ProcessKit-contained profile command.  Transcript capture remains in the
/// immutable native evidence file; this compact record is deliberately safe for the interoperable
/// verification evidence consumed by recovery/status tooling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationCommandRun {
    pub command: String,
    pub reason: String,
    pub exit_code: Option<i32>,
}

impl VerificationRun {
    /// Map the in-memory native run onto the stable interoperable verification document. The
    /// caller supplies only already-durable cohort coordinates, making a retry reproduce the
    /// authorising head/base/profile rather than consult a new ambient value.
    pub fn evidence(&self, head: &str, base: &str, updated_at: &str) -> VerificationEvidence {
        let (verdict, exemption) = match &self.outcome {
            VerificationOutcome::Passed => ("pass", ""),
            VerificationOutcome::Exempt { reason } => ("exempt", reason.as_str()),
            VerificationOutcome::Failed { .. } => ("failed", ""),
            VerificationOutcome::Blocked { reason } => (
                "blocked",
                if reason == MISSING_PROFILE_REASON {
                    "missing-profile"
                } else {
                    "invalid-profile"
                },
            ),
        };
        VerificationEvidence {
            schema: "orchestra/verification@1".into(),
            verdict: verdict.into(),
            verified_head: head.into(),
            base: base.into(),
            profile_fingerprint: self.profile.fingerprint.clone(),
            profile_state: self.profile.state.clone(),
            profile_source: self.profile.source.clone(),
            commands: self.commands.clone(),
            exemption: exemption.into(),
            updated_at: updated_at.into(),
        }
    }
}

/// Construct a durable evidence record for a native, non-process exemption. The caller still
/// owns the VCS proof and atomic persistence; keeping the wire construction here prevents a
/// docs-only boundary from drifting from the ordinary ProcessKit-run profile fingerprint.
pub fn exemption_evidence(
    profile: &VerificationProfile,
    head: &str,
    base: &str,
    reason: &str,
    updated_at: &str,
) -> VerificationEvidence {
    VerificationEvidence {
        schema: "orchestra/verification@1".into(),
        verdict: "exempt".into(),
        verified_head: head.into(),
        base: base.into(),
        profile_fingerprint: profile.fingerprint.clone(),
        profile_state: profile.state.clone(),
        profile_source: profile.source.clone(),
        commands: Vec::new(),
        exemption: reason.into(),
        updated_at: updated_at.into(),
    }
}

/// Fail-closed docs-only classifier over a typed VCS range. Documentation-like stems (`readme`,
/// `changelog`, `contributing`, `license`, `agents`, and `claude`) count as documentation only
/// without an extension or with `.md`, `.txt`, or `.rst`. This intentionally diverges from legacy
/// `verification.ps1`, which accepts those stems with any extension: the stricter rule prevents
/// code files such as `agents.rs`, `claude.rs`, or `license.py` from becoming docs-exempt solely
/// because of their stem. A range with no changed files is never a documentation change. Callers
/// must supply both sides of a rename/copy so moving source into a Markdown-looking path remains
/// executable rather than gaining an exemption.
pub fn is_docs_only(paths: &[PathBuf]) -> bool {
    !paths.is_empty() && paths.iter().all(|path| is_documentation_path(path))
}

fn is_documentation_path(path: &Path) -> bool {
    let normalized = path.to_string_lossy().replace('\\', "/");
    if normalized
        .split('/')
        .any(|component| component.eq_ignore_ascii_case("docs"))
    {
        return true;
    }
    let Some(file_name) = normalized.rsplit('/').next() else {
        return false;
    };
    let file_name = file_name.to_ascii_lowercase();
    if file_name.ends_with(".md") {
        return true;
    }

    let (stem, extension) = file_name
        .rsplit_once('.')
        .map_or((file_name.as_str(), None), |(stem, extension)| {
            (stem, Some(extension))
        });
    matches!(
        stem,
        "readme" | "changelog" | "contributing" | "license" | "agents" | "claude"
    ) && matches!(extension, None | Some("md" | "txt" | "rst"))
}

/// JSON shape intentionally shared with legacy `verification.ps1`. The Rust engine does not use
/// a shell supervisor, so command output paths are retained in the native transcript rather than
/// represented as executable/path-bearing fields in this cross-process evidence document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationEvidence {
    pub schema: String,
    pub verdict: String,
    pub verified_head: String,
    pub base: String,
    pub profile_fingerprint: String,
    pub profile_state: String,
    pub profile_source: String,
    pub commands: Vec<VerificationCommandRun>,
    pub exemption: String,
    pub updated_at: String,
}

/// Load a persisted legacy-compatible evidence file. The caller owns the path containment and
/// treats any unreadable/malformed document as a blocked verification boundary.
pub fn read_evidence(path: &Path) -> Result<VerificationEvidence, String> {
    let text = read_plain_evidence_text(path).map_err(|error| {
        format!(
            "cannot read verification evidence {}: {error}",
            path.display()
        )
    })?;
    serde_json::from_str(&text).map_err(|error| {
        format!(
            "cannot parse verification evidence {}: {error}",
            path.display()
        )
    })
}

/// Read a verification record through the shared checked-handle primitive. The evidence file is
/// a mutable control-plane input, so [`work_fs::read_plain_text`] verifies metadata before open,
/// on the opened handle, and after the bounded read.
fn read_plain_evidence_text(path: &Path) -> Result<String, std::io::Error> {
    work_fs::read_plain_text(path, MAX_EVIDENCE_BYTES)
}

/// Prove that an evidence record is authoritative for the exact reducer outcome and immutable
/// integration coordinate that is about to advance. A correctly shaped but stale or differently
/// classified file cannot bless a new tip.
pub fn validate_evidence(
    evidence: &VerificationEvidence,
    outcome: &VerificationOutcome,
    head: &str,
    base: &str,
) -> Result<(), String> {
    validate_evidence_for_profile(evidence, outcome, head, base, None)
}

/// Validate evidence against an immutable startup profile when the caller owns one.  The legacy
/// wire document has no `mode` field, but its fingerprint commits the mode, source, and complete
/// ordered command vector; state/source and the started-command prefix make that commitment
/// inspectable without trusting a mutable `config.md` during recovery.
pub fn validate_evidence_for_profile(
    evidence: &VerificationEvidence,
    outcome: &VerificationOutcome,
    head: &str,
    base: &str,
    expected_profile: Option<&VerificationProfile>,
) -> Result<(), String> {
    if evidence.schema != "orchestra/verification@1" {
        return Err(format!(
            "verification evidence has unsupported schema {:?}",
            evidence.schema
        ));
    }
    if evidence.verified_head != head || evidence.base != base {
        return Err(format!(
            "verification evidence is stale (head={:?}, base={:?}; expected head={head:?}, base={base:?})",
            evidence.verified_head, evidence.base
        ));
    }
    let expected = match outcome {
        VerificationOutcome::Passed => "pass",
        VerificationOutcome::Exempt { .. } => "exempt",
        VerificationOutcome::Failed { .. } => "failed",
        VerificationOutcome::Blocked { .. } => "blocked",
    };
    if evidence.verdict != expected {
        return Err(format!(
            "verification evidence verdict {:?} disagrees with native outcome {expected:?}",
            evidence.verdict
        ));
    }
    if let VerificationOutcome::Exempt { reason } = outcome
        && evidence.exemption != reason.as_str()
    {
        return Err(format!(
            "verification evidence exemption {:?} disagrees with native outcome {reason:?}",
            evidence.exemption
        ));
    }
    match outcome {
        VerificationOutcome::Passed if !evidence.exemption.is_empty() => {
            return Err("passing verification evidence must not contain an exemption".into());
        }
        VerificationOutcome::Failed { .. } if !evidence.exemption.is_empty() => {
            return Err("failed verification evidence must not contain an exemption".into());
        }
        VerificationOutcome::Blocked { .. }
            if !matches!(
                evidence.exemption.as_str(),
                "missing-profile" | "invalid-profile"
            ) =>
        {
            return Err(
                "blocked verification evidence must record missing-profile or invalid-profile"
                    .into(),
            );
        }
        _ => {}
    }
    if evidence.profile_fingerprint.len() != 64
        || !evidence
            .profile_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("verification evidence has an invalid profile fingerprint".into());
    }
    if let Some(profile) = expected_profile {
        if evidence.profile_fingerprint != profile.fingerprint
            || evidence.profile_state != profile.state
            || evidence.profile_source != profile.source
        {
            return Err(format!(
                "verification evidence profile does not match the immutable startup snapshot (fingerprint={:?}, state={:?}, source={:?})",
                evidence.profile_fingerprint, evidence.profile_state, evidence.profile_source
            ));
        }
        let commands_match_prefix = evidence
            .commands
            .iter()
            .zip(&profile.commands)
            .all(|(result, command)| result.command == *command);
        if evidence.commands.len() > profile.commands.len() || !commands_match_prefix {
            return Err(
                "verification evidence commands do not match the configured profile".into(),
            );
        }
        match outcome {
            VerificationOutcome::Passed if evidence.commands.len() != profile.commands.len() => {
                return Err(
                    "passing verification evidence did not record every configured command".into(),
                );
            }
            VerificationOutcome::Passed
                if evidence
                    .commands
                    .iter()
                    .any(|command| command.reason != "ok" || command.exit_code != Some(0)) =>
            {
                return Err(
                    "passing verification evidence contains a command that did not succeed".into(),
                );
            }
            VerificationOutcome::Exempt { .. } | VerificationOutcome::Blocked { .. }
                if !evidence.commands.is_empty() =>
            {
                return Err(
                    "exempt or blocked verification evidence must not report started commands"
                        .into(),
                );
            }
            VerificationOutcome::Failed { .. } if evidence.commands.is_empty() => {
                return Err(
                    "failed verification evidence did not record its failed command".into(),
                );
            }
            VerificationOutcome::Failed { .. }
                if evidence
                    .commands
                    .last()
                    .is_none_or(|command| command.reason == "ok") =>
            {
                return Err("failed verification evidence has no failed terminal command".into());
            }
            _ => {}
        }
    }
    Ok(())
}

/// Execute the configured integration profile sequentially in `workspace`.
///
/// `VERIFICATION_COMMANDS` always wins over legacy `SMOKE_CMD`. `auto` and `required` both fail
/// closed when neither profile exists; automatic language-specific profile discovery is not yet a
/// native feature and must not be invented at publication time.
pub fn verify_integration(
    mode: VerificationMode,
    verification_commands: &[String],
    smoke_cmd: Option<&str>,
    workspace: &Path,
    deadline: Duration,
    output_max_bytes: usize,
    cancellation_probe: Option<CancellationProbe>,
) -> VerificationRun {
    let profile = profile(mode, verification_commands, smoke_cmd);
    match disposition(&profile) {
        ProfileDisposition::Exempt { reason } => {
            return VerificationRun {
                outcome: VerificationOutcome::Exempt {
                    reason: reason.into(),
                },
                transcript: "verification=exempt; reason=operator-disabled\n".into(),
                profile,
                commands: Vec::new(),
            };
        }
        ProfileDisposition::Blocked { reason } => {
            return VerificationRun {
                outcome: VerificationOutcome::Blocked {
                    reason: reason.into(),
                },
                transcript: format!("verification=blocked; reason={reason}\n"),
                profile,
                commands: Vec::new(),
            };
        }
        ProfileDisposition::Configured => {}
    }
    let sequence = run_command_sequence(
        &profile.commands,
        INTEGRATION_FINDING_SUBJECT,
        workspace,
        deadline,
        output_max_bytes,
        cancellation_probe,
    );
    VerificationRun {
        outcome: sequence.outcome,
        transcript: sequence.transcript,
        profile,
        commands: sequence.commands,
    }
}

/// Run the operator's review/fix-cycle profile inside one task worktree (`agents/processor.md`
/// phases 2.5/2.8).
///
/// This is deliberately the *same* contained execution as the Phase-4 gate — one `supervise::run`
/// child per command, the same `call_output_max_bytes` capture ceiling, the same lease-loss
/// cancellation and the same typed-argv refusal of shell grammar — differing only in the worktree
/// it runs in and in the fact that it authorizes nothing: its result becomes review evidence for
/// this round rather than a publication decision, so it writes no `verification.json`.
pub fn verify_review_cycle(
    profile: &VerificationProfile,
    workspace: &Path,
    deadline: Duration,
    output_max_bytes: usize,
    cancellation_probe: Option<CancellationProbe>,
) -> VerificationRun {
    let sequence = run_command_sequence(
        &profile.commands,
        REVIEW_CYCLE_FINDING_SUBJECT,
        workspace,
        deadline,
        output_max_bytes,
        cancellation_probe,
    );
    VerificationRun {
        outcome: sequence.outcome,
        transcript: sequence.transcript,
        profile: profile.clone(),
        commands: sequence.commands,
    }
}

/// The stable subject halves of the two verification attempt signatures. They are distinct so a
/// review-cycle failure and a Phase-4 failure of the same command never collapse into one
/// stagnation fingerprint.
const INTEGRATION_FINDING_SUBJECT: &str = "integration verification failed";
const REVIEW_CYCLE_FINDING_SUBJECT: &str = "review-cycle verification failed";

/// The profile-independent half of a verification run: what the contained children did.
struct CommandSequenceRun {
    outcome: VerificationOutcome,
    transcript: String,
    commands: Vec<VerificationCommandRun>,
}

/// Execute `commands` sequentially under ProcessKit containment, stopping at the first command
/// that does not end `ok`.
///
/// Both the publication gate and the review/fix-cycle gate share this one body on purpose: a
/// second copy would let the cheaper, more frequently executed path drift away from the
/// containment, capture-ceiling, cancellation, and shell-grammar rules the publication gate
/// establishes.
fn run_command_sequence(
    commands: &[String],
    finding_subject: &str,
    workspace: &Path,
    deadline: Duration,
    output_max_bytes: usize,
    cancellation_probe: Option<CancellationProbe>,
) -> CommandSequenceRun {
    let invocations = commands
        .iter()
        .map(|command| {
            parse_typed_argv(command)
                .map(|argv| (command, argv))
                .map_err(|error| format!("invalid typed verification command {command:?}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>();
    let invocations = match invocations {
        Ok(invocations) => invocations,
        Err(reason) => {
            return CommandSequenceRun {
                transcript: format!("verification=blocked; reason={reason}\n"),
                outcome: VerificationOutcome::Blocked { reason },
                commands: Vec::new(),
            };
        }
    };

    let mut transcript = String::new();
    let mut results = Vec::with_capacity(invocations.len());
    for (index, (command, argv)) in invocations.iter().enumerate() {
        let (program, args) = argv
            .split_first()
            .expect("typed argv parser always returns one executable");
        let verdict = supervise::run(
            &SpawnSpec::new(program.clone(), args.to_vec())
                .current_dir(workspace)
                .deadline(Some(deadline))
                .output_max_bytes(output_max_bytes)
                .cancel_probe(cancellation_probe.clone()),
        );
        let number = index + 1;
        transcript.push_str(&format!(
            "command={number}; verdict={}; exit={:?}; duration-ms={}; reason={}\n",
            verdict.reason.as_str(),
            verdict.exit_code,
            verdict.duration_ms,
            one_line(&verdict.outcome_reason),
        ));
        results.push(VerificationCommandRun {
            command: (*command).clone(),
            reason: verdict.reason.as_str().into(),
            exit_code: verdict.exit_code,
        });
        if !verdict.stdout.is_empty() {
            transcript.push_str("stdout:\n");
            transcript.push_str(&verdict.stdout);
            if !transcript.ends_with('\n') {
                transcript.push('\n');
            }
        }
        if !verdict.stderr.is_empty() {
            transcript.push_str("stderr:\n");
            transcript.push_str(&verdict.stderr);
            if !transcript.ends_with('\n') {
                transcript.push('\n');
            }
        }
        if verdict.reason != Reason::Ok {
            let reason = format!(
                "verification command #{number} ended {} ({})",
                verdict.reason.as_str(),
                one_line(&verdict.outcome_reason),
            );
            let outcome = if verdict.reason == Reason::Cancelled {
                VerificationOutcome::Blocked { reason }
            } else {
                VerificationOutcome::Failed {
                    signature: AttemptSignature::of_finding(
                        finding_subject,
                        &format!("{number}:{command}:{}", verdict.reason.as_str()),
                    )
                    .as_str()
                    .to_string(),
                    reason,
                }
            };
            return CommandSequenceRun {
                outcome,
                transcript,
                commands: results,
            };
        }
    }
    CommandSequenceRun {
        outcome: VerificationOutcome::Passed,
        transcript,
        commands: results,
    }
}

/// How a resolved profile settles before any child is launched. `Configured` deliberately carries
/// no command payload: the commands are `profile.commands`, and duplicating them here would create
/// a second list that could disagree with the hashed snapshot the evidence is bound to.
enum ProfileDisposition {
    Exempt { reason: &'static str },
    Blocked { reason: &'static str },
    Configured,
}

fn disposition(profile: &VerificationProfile) -> ProfileDisposition {
    if profile.state == "disabled" {
        return ProfileDisposition::Exempt {
            reason: "operator-disabled",
        };
    }
    if profile.state == "configured" {
        return ProfileDisposition::Configured;
    }
    ProfileDisposition::Blocked {
        reason: MISSING_PROFILE_REASON,
    }
}

/// Exactly mirror legacy profile precedence: explicit command JSON wins over the smoke fallback;
/// a `disabled` mode remains an intentional exemption even when commands are present.
pub fn profile(
    mode: VerificationMode,
    verification_commands: &[String],
    smoke_cmd: Option<&str>,
) -> VerificationProfile {
    profile_with_policy_commands(mode, verification_commands, smoke_cmd, &[])
}

/// Construct a profile that distinguishes `constraints.md` required checks from the ordinary
/// operator profile while still running both in one deterministic ProcessKit sequence.
pub fn profile_with_policy_commands(
    mode: VerificationMode,
    verification_commands: &[String],
    smoke_cmd: Option<&str>,
    policy_verification_commands: &[String],
) -> VerificationProfile {
    let (source, commands) = if !verification_commands.is_empty() {
        ("VERIFICATION_COMMANDS", verification_commands.to_vec())
    } else if let Some(smoke) = smoke_cmd {
        ("SMOKE_CMD", vec![smoke.into()])
    } else {
        ("none", Vec::new())
    };
    let mut commands = commands;
    commands.extend(policy_verification_commands.iter().cloned());
    let source = if policy_verification_commands.is_empty() {
        source.into()
    } else {
        format!("policy-required+{source}")
    };
    let mode = match mode {
        VerificationMode::Disabled => "disabled",
        VerificationMode::Auto => "auto",
        VerificationMode::Required => "required",
    };
    fingerprinted_profile(mode, source, commands)
}

/// Snapshot the exact command profile a review/fix cycle would execute, or `None` when the
/// operator configured nothing runnable.
///
/// Precedence mirrors the Phase-4 rule and then extends it by one step: an explicit
/// `REVIEW_CYCLE_VERIFICATION_COMMANDS` subset wins over `VERIFICATION_COMMANDS`, which still wins
/// over the legacy `SMOKE_CMD`. The `source` field records which operator key actually supplied
/// the commands, so a cycle finding cannot misattribute a cheap lint subset to the full
/// publication profile. Policy-required `constraints.md` checks are intentionally NOT folded in:
/// they are a publication precondition, and running them every round would silently redefine
/// which checks the operator agreed to pay for on each cycle.
pub fn review_cycle_profile(
    review_cycle_commands: &[String],
    verification_commands: &[String],
    smoke_cmd: Option<&str>,
) -> Option<VerificationProfile> {
    let (source, commands) = if !review_cycle_commands.is_empty() {
        (
            "REVIEW_CYCLE_VERIFICATION_COMMANDS",
            review_cycle_commands.to_vec(),
        )
    } else if !verification_commands.is_empty() {
        ("VERIFICATION_COMMANDS", verification_commands.to_vec())
    } else {
        ("SMOKE_CMD", vec![smoke_cmd?.to_string()])
    };
    // The gate is explicit opt-in, so its snapshot is `required`: it never carries the implicit
    // `auto`/`disabled` precedence the publication profile inherits from legacy configuration.
    Some(fingerprinted_profile("required", source.into(), commands))
}

/// Bind one mode/source/command vector into the legacy-compatible hashed snapshot. Both the
/// publication profile and the review/fix-cycle profile derive their identity here so a command
/// list can never be presented under two different fingerprints.
fn fingerprinted_profile(mode: &str, source: String, commands: Vec<String>) -> VerificationProfile {
    let state = if mode == "disabled" {
        "disabled"
    } else if commands.is_empty() {
        "missing"
    } else {
        "configured"
    };
    let encoded_commands = serde_json::to_string(&commands)
        .expect("a vector of Rust strings is always a serializable verification profile");
    let canonical = format!(
        "{{\"mode\":{},\"source\":{},\"commands\":{encoded_commands}}}",
        serde_json::to_string(mode).expect("static mode is serializable"),
        serde_json::to_string(&source).expect("source is serializable"),
    );
    let fingerprint = hex(Sha256::digest(canonical.as_bytes()).as_slice());
    VerificationProfile {
        mode: mode.into(),
        state: state.into(),
        source,
        commands,
        fingerprint,
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn one_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn profile_selection_is_explicit_and_fail_closed() {
        let mut mode = VerificationMode::Disabled;
        let mut smoke = None;
        let mut configured = Vec::new();
        assert!(matches!(
            disposition(&profile(mode, &configured, smoke)),
            ProfileDisposition::Exempt { .. }
        ));

        mode = VerificationMode::Required;
        assert!(matches!(
            disposition(&profile(mode, &configured, smoke)),
            ProfileDisposition::Blocked { .. }
        ));

        // The executable command list is the profile's own hashed vector, so the disposition and
        // the commands are asserted against that single source of truth.
        smoke = Some("cargo test");
        let smoke_profile = profile(mode, &configured, smoke);
        assert!(matches!(
            disposition(&smoke_profile),
            ProfileDisposition::Configured
        ));
        assert_eq!(smoke_profile.commands, vec!["cargo test"]);

        configured = vec!["cargo fmt --check".into()];
        let configured_profile = profile(mode, &configured, smoke);
        assert!(matches!(
            disposition(&configured_profile),
            ProfileDisposition::Configured
        ));
        assert_eq!(configured_profile.commands, vec!["cargo fmt --check"]);
    }

    #[test]
    fn policy_required_commands_are_distinct_and_content_bound_in_the_profile() {
        let config = vec!["cargo test --lib".into()];
        let policy = vec!["cargo clippy -- -D warnings".into()];
        let policy_profile =
            profile_with_policy_commands(VerificationMode::Auto, &config, None, &policy);
        assert_eq!(
            policy_profile.source,
            "policy-required+VERIFICATION_COMMANDS"
        );
        assert_eq!(
            policy_profile.commands,
            vec!["cargo test --lib", "cargo clippy -- -D warnings"]
        );
        assert_ne!(
            policy_profile.fingerprint,
            profile(VerificationMode::Auto, &config, None).fingerprint
        );
    }

    #[test]
    fn docs_only_classification_requires_a_nonempty_fully_documentary_typed_range() {
        assert!(is_docs_only(&[
            PathBuf::from("docs/code.rs"),
            PathBuf::from("README.md"),
            PathBuf::from("Docs/README.MD"),
            PathBuf::from("nested/CHANGELOG"),
            PathBuf::from("LICENSE"),
            PathBuf::from("AGENTS"),
            PathBuf::from("readme.md"),
            PathBuf::from("changelog.txt"),
            PathBuf::from("contributing.rst"),
            PathBuf::from("engine/design-notes.md"),
        ]));
        assert!(!is_docs_only(&[]));
        assert!(!is_docs_only(&[
            PathBuf::from("docs/guide.md"),
            PathBuf::from("engine/src/lib.rs"),
        ]));
        assert!(!is_docs_only(&[PathBuf::from("claude.rs")]));
        assert!(!is_docs_only(&[PathBuf::from("agents.rs")]));
        assert!(!is_docs_only(&[PathBuf::from("license.py")]));
    }

    #[test]
    fn evidence_binds_the_head_base_profile_and_exact_started_commands() {
        let expected_profile = profile(
            VerificationMode::Auto,
            &["cargo fmt --check".into()],
            Some("cargo test"),
        );
        assert_eq!(expected_profile.source, "VERIFICATION_COMMANDS");
        assert_eq!(expected_profile.state, "configured");
        assert_eq!(
            expected_profile.fingerprint,
            "e8959b273a07f7c14f99d392abb1418ddecbee168ac02c15cf69c83719b45525"
        );
        assert_eq!(
            profile(
                VerificationMode::Auto,
                &["cargo fmt --check".into(), "тест".into()],
                None,
            )
            .fingerprint,
            "c637f819b0852500eb4be6cd0fc7e80a3714cf2866458119ffa35845c9beeee8",
            "must match legacy PowerShell ConvertTo-Json UTF-8 hashing for non-ASCII commands"
        );
        let run = VerificationRun {
            outcome: VerificationOutcome::Passed,
            transcript: String::new(),
            profile: expected_profile,
            commands: vec![VerificationCommandRun {
                command: "cargo fmt --check".into(),
                reason: "ok".into(),
                exit_code: Some(0),
            }],
        };
        let evidence = run.evidence("head", "base", "2026-07-25T12:00:00.000Z");
        assert_eq!(evidence.schema, "orchestra/verification@1");
        assert_eq!(evidence.verdict, "pass");
        assert_eq!(evidence.verified_head, "head");
        assert_eq!(evidence.base, "base");
        assert_eq!(evidence.commands[0].command, "cargo fmt --check");
        assert!(validate_evidence(&evidence, &VerificationOutcome::Passed, "head", "base").is_ok());
        assert!(
            validate_evidence_for_profile(
                &evidence,
                &VerificationOutcome::Passed,
                "head",
                "base",
                Some(&run.profile),
            )
            .is_ok()
        );
        assert!(
            validate_evidence(
                &evidence,
                &VerificationOutcome::Passed,
                "different-head",
                "base"
            )
            .is_err()
        );
        let mismatched = profile(VerificationMode::Required, &["cargo test".into()], None);
        assert!(
            validate_evidence_for_profile(
                &evidence,
                &VerificationOutcome::Passed,
                "head",
                "base",
                Some(&mismatched),
            )
            .is_err()
        );
    }

    #[test]
    fn configured_command_runs_contained_in_the_integration_worktree() {
        let workspace = std::env::temp_dir().join(format!(
            "orchestrail-verification-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&workspace).unwrap();
        fs::write(workspace.join("marker.txt"), "present\n").unwrap();
        #[cfg(windows)]
        let command = "findstr /M present marker.txt";
        #[cfg(not(windows))]
        let command = "test -f marker.txt";
        let result = verify_integration(
            VerificationMode::Required,
            &[command.into()],
            None,
            &workspace,
            Duration::from_secs(10),
            1024 * 1024,
            None,
        );
        let _ = fs::remove_dir_all(&workspace);
        assert_eq!(result.outcome, VerificationOutcome::Passed);
        assert!(result.transcript.contains("verdict=ok"));
    }

    #[test]
    fn review_cycle_profile_prefers_the_subset_and_stays_absent_without_a_profile() {
        assert_eq!(review_cycle_profile(&[], &[], None), None);

        let smoke = review_cycle_profile(&[], &[], Some("cargo test")).unwrap();
        assert_eq!(smoke.source, "SMOKE_CMD");
        assert_eq!(smoke.commands, vec!["cargo test"]);

        let full = review_cycle_profile(&[], &["cargo test".into()], Some("ignored")).unwrap();
        assert_eq!(full.source, "VERIFICATION_COMMANDS");
        assert_eq!(full.commands, vec!["cargo test"]);

        let subset = review_cycle_profile(
            &["cargo clippy".into()],
            &["cargo test".into()],
            Some("ignored"),
        )
        .unwrap();
        assert_eq!(subset.source, "REVIEW_CYCLE_VERIFICATION_COMMANDS");
        assert_eq!(subset.commands, vec!["cargo clippy"]);
        assert_eq!(subset.state, "configured");
        assert_eq!(subset.mode, "required");

        // The same command list under a different operator key is a different profile identity,
        // so a cycle finding can never be re-read as publication proof.
        assert_ne!(
            review_cycle_profile(&["cargo test".into()], &[], None)
                .unwrap()
                .fingerprint,
            full.fingerprint
        );
    }

    #[test]
    fn review_cycle_run_shares_phase_four_containment_and_refuses_shell_grammar() {
        let workspace = std::env::temp_dir().join(format!(
            "orchestrail-review-cycle-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&workspace).unwrap();
        fs::write(workspace.join("marker.txt"), "present\n").unwrap();
        #[cfg(windows)]
        let (passing, failing) = (
            "findstr /M present marker.txt",
            "findstr /M absent marker.txt",
        );
        #[cfg(not(windows))]
        let (passing, failing) = ("test -f marker.txt", "test -f absent.txt");

        let passed = verify_review_cycle(
            &review_cycle_profile(&[passing.into()], &[], None).unwrap(),
            &workspace,
            Duration::from_secs(10),
            1024 * 1024,
            None,
        );
        assert_eq!(passed.outcome, VerificationOutcome::Passed);
        assert!(passed.transcript.contains("verdict=ok"));
        assert_eq!(passed.commands.len(), 1);

        let failed = verify_review_cycle(
            &review_cycle_profile(&[failing.into()], &[], None).unwrap(),
            &workspace,
            Duration::from_secs(10),
            1024 * 1024,
            None,
        );
        let _ = fs::remove_dir_all(&workspace);
        let VerificationOutcome::Failed { signature, reason } = &failed.outcome else {
            panic!("a non-zero cycle command must fail: {:?}", failed.outcome);
        };
        assert!(reason.contains("verification command #1"));
        // A cycle failure and a publication failure of the identical command must not collapse
        // into the same stagnation fingerprint.
        let integration = verify_integration(
            VerificationMode::Required,
            &[failing.into()],
            None,
            Path::new("."),
            Duration::from_secs(10),
            1024 * 1024,
            None,
        );
        let VerificationOutcome::Failed {
            signature: integration_signature,
            ..
        } = &integration.outcome
        else {
            panic!("fixture command must fail at the publication gate too");
        };
        assert_ne!(signature, integration_signature);

        // Configuration validation is not the only defence: a bypassed profile is still refused
        // before any child exists, exactly as on the publication path.
        let bypassed = verify_review_cycle(
            &review_cycle_profile(&["cargo test && cargo fmt".into()], &[], None).unwrap(),
            Path::new("."),
            Duration::from_secs(1),
            1024,
            None,
        );
        assert!(matches!(
            bypassed.outcome,
            VerificationOutcome::Blocked { ref reason } if reason.contains("shell operator")
        ));
        assert!(bypassed.commands.is_empty());
    }

    #[test]
    fn direct_call_still_blocks_shell_grammar_if_configuration_validation_was_bypassed() {
        let result = verify_integration(
            VerificationMode::Required,
            &["cargo test && cargo fmt".into()],
            None,
            Path::new("."),
            Duration::from_secs(1),
            1024,
            None,
        );
        assert!(matches!(
            result.outcome,
            VerificationOutcome::Blocked { ref reason } if reason.contains("shell operator")
        ));
        assert!(result.commands.is_empty());
        assert_eq!(
            result
                .evidence("head", "base", "2026-07-25T12:00:00Z")
                .exemption,
            "invalid-profile"
        );
    }
}
