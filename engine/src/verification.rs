//! ProcessKit-contained Phase-4 integration verification.
//!
//! The legacy configuration exposes human-authored command strings, so compatibility requires a
//! platform shell.  The shell itself is still launched only through [`crate::supervise`], with a
//! verified integration worktree as its child directory, deadline, lease-loss cancellation,
//! bounded
//! capture, and tree containment.  This module never treats an absent profile as a successful
//! build gate.

use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::command_line::parse_typed_argv;
use crate::config::VerificationMode;
use crate::processor::VerificationOutcome;
use crate::resolvers::AttemptSignature;
use crate::supervise::{self, CancellationProbe, Reason, SpawnSpec};

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

/// Legacy-compatible docs-only classifier over a typed VCS range. A range with no changed files
/// is never a documentation change. Callers must supply both sides of a rename/copy so moving
/// source into a Markdown-looking path remains executable rather than gaining an exemption.
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
    let stem = file_name
        .split_once('.')
        .map_or(file_name.as_str(), |(before, _)| before);
    matches!(
        stem,
        "readme" | "changelog" | "contributing" | "license" | "agents" | "claude"
    ) || file_name.ends_with(".md")
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

/// Read a verification record through a checked handle.  The evidence file is a mutable
/// control-plane input, so a pre-read `symlink_metadata` check alone would leave a rename race
/// that could redirect the parser to an arbitrary file. This mirrors inbox control-plane reads.
fn read_plain_evidence_text(path: &Path) -> Result<String, std::io::Error> {
    let before = fs::symlink_metadata(path)?;
    assert_plain_evidence_file(path, &before)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        const O_NOFOLLOW: i32 = 0o400_000;
        options.custom_flags(O_NOFOLLOW);
    }
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        const O_NOFOLLOW: i32 = 0x0100;
        options.custom_flags(O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    let opened = file.metadata()?;
    assert_plain_evidence_file(path, &opened)?;
    if opened.len() > MAX_EVIDENCE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "verification evidence exceeds the {MAX_EVIDENCE_BYTES}-byte limit: {}",
                path.display()
            ),
        ));
    }
    let mut text = String::new();
    (&mut file)
        .take(MAX_EVIDENCE_BYTES + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > MAX_EVIDENCE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "verification evidence grew beyond the {MAX_EVIDENCE_BYTES}-byte limit: {}",
                path.display()
            ),
        ));
    }
    let after = fs::symlink_metadata(path)?;
    assert_plain_evidence_file(path, &after)?;
    Ok(text)
}

fn assert_plain_evidence_file(path: &Path, metadata: &fs::Metadata) -> Result<(), std::io::Error> {
    #[cfg(windows)]
    let redirected = {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    };
    #[cfg(not(windows))]
    let redirected = metadata.file_type().is_symlink();
    if !metadata.is_file() || redirected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "verification evidence is not a plain file: {}",
                path.display()
            ),
        ));
    }
    Ok(())
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
    let commands = match commands(&profile) {
        VerificationCommands::Exempt { reason } => {
            return VerificationRun {
                outcome: VerificationOutcome::Exempt {
                    reason: reason.into(),
                },
                transcript: "verification=exempt; reason=operator-disabled\n".into(),
                profile,
                commands: Vec::new(),
            };
        }
        VerificationCommands::Blocked { reason } => {
            return VerificationRun {
                outcome: VerificationOutcome::Blocked {
                    reason: reason.into(),
                },
                transcript: format!("verification=blocked; reason={reason}\n"),
                profile,
                commands: Vec::new(),
            };
        }
        VerificationCommands::Commands(commands) => commands,
    };

    let invocations = commands
        .into_iter()
        .map(|command| {
            parse_typed_argv(command)
                .map(|argv| (command, argv))
                .map_err(|error| format!("invalid typed verification command {command:?}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>();
    let invocations = match invocations {
        Ok(invocations) => invocations,
        Err(reason) => {
            return VerificationRun {
                outcome: VerificationOutcome::Blocked {
                    reason: reason.clone(),
                },
                transcript: format!("verification=blocked; reason={reason}\n"),
                profile,
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
            command: (*command).into(),
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
                        "integration verification failed",
                        &format!("{number}:{command}:{}", verdict.reason.as_str()),
                    )
                    .as_str()
                    .to_string(),
                    reason,
                }
            };
            return VerificationRun {
                outcome,
                transcript,
                profile,
                commands: results,
            };
        }
    }
    VerificationRun {
        outcome: VerificationOutcome::Passed,
        transcript,
        profile,
        commands: results,
    }
}

enum VerificationCommands<'a> {
    Exempt { reason: &'static str },
    Blocked { reason: &'static str },
    Commands(Vec<&'a str>),
}

fn commands(profile: &VerificationProfile) -> VerificationCommands<'_> {
    if profile.state == "disabled" {
        return VerificationCommands::Exempt {
            reason: "operator-disabled",
        };
    }
    if profile.state == "configured" {
        return VerificationCommands::Commands(
            profile.commands.iter().map(String::as_str).collect(),
        );
    }
    VerificationCommands::Blocked {
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
            commands(&profile(mode, &configured, smoke)),
            VerificationCommands::Exempt { .. }
        ));

        mode = VerificationMode::Required;
        assert!(matches!(
            commands(&profile(mode, &configured, smoke)),
            VerificationCommands::Blocked { .. }
        ));

        smoke = Some("cargo test");
        assert!(matches!(
            commands(&profile(mode, &configured, smoke)),
            VerificationCommands::Commands(commands) if commands == vec!["cargo test"]
        ));

        configured = vec!["cargo fmt --check".into()];
        assert!(matches!(
            commands(&profile(mode, &configured, smoke)),
            VerificationCommands::Commands(commands) if commands == vec!["cargo fmt --check"]
        ));
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
            PathBuf::from("docs/guide.rs"),
            PathBuf::from("README.md"),
            PathBuf::from("Docs/README.MD"),
            PathBuf::from("nested/CHANGELOG"),
        ]));
        assert!(!is_docs_only(&[]));
        assert!(!is_docs_only(&[
            PathBuf::from("docs/guide.md"),
            PathBuf::from("engine/src/lib.rs"),
        ]));
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
