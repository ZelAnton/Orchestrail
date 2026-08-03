//! Best-effort, redacted operator notifications.
//!
//! A notification must never become another orchestration gate.  The dispatcher records a
//! durable claim before launching its one contained ProcessKit child; a resumed effect therefore
//! observes the claim instead of sending a duplicate message. A fresh interrupted claim is
//! reported as in progress, while a stale one is finalized as unknown without a second launch.
//! It deliberately persists neither command output nor the underlying VCS/approval payload.

use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::supervise::{self, Reason, SpawnSpec};
use crate::task_id::is_task_id;
use crate::work_fs;

const NOTIFICATION_SCHEMA: &str = "orchestrail/notification@1";
const NOTIFICATION_DEADLINE: Duration = Duration::from_secs(30);
/// A claim older than this has no trustworthy delivery owner. Recovery finalizes it as
/// `unknown` without launching another child, preserving the at-most-once boundary.
const NOTIFICATION_CLAIM_STALE_AFTER: Duration = Duration::from_secs(5 * 60);
const NOTIFICATION_OUTPUT_MAX_BYTES: usize = 16 * 1024;
const NOTIFICATION_RECEIPT_MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug)]
enum NotificationError {
    Io(io::Error),
    Serialize(serde_json::Error),
    Deserialize(serde_json::Error),
    Invalid(String),
}

impl fmt::Display for NotificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "notification receipt I/O error: {error}"),
            Self::Serialize(error) => {
                write!(f, "notification receipt serialization error: {error}")
            }
            Self::Deserialize(error) => write!(f, "notification receipt JSON error: {error}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for NotificationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Serialize(error) | Self::Deserialize(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

impl From<io::Error> for NotificationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl NotificationError {
    fn class(&self) -> NotificationErrorClass {
        match self {
            Self::Io(error)
                if matches!(
                    work_fs::plain_read_violation(error),
                    Some(
                        work_fs::PlainReadViolation::NotPlain { .. }
                            | work_fs::PlainReadViolation::ParentNotPlain { .. }
                    )
                ) =>
            {
                NotificationErrorClass::Redirected
            }
            Self::Io(_) => NotificationErrorClass::Io,
            Self::Serialize(_) => NotificationErrorClass::Serialize,
            Self::Deserialize(_) | Self::Invalid(_) => NotificationErrorClass::SchemaMismatch,
        }
    }
}

type NotificationResult<T> = std::result::Result<T, NotificationError>;

fn ensure_receipt_location(work: &Path, path: &Path) -> NotificationResult<()> {
    let parent = path.parent().ok_or_else(|| {
        NotificationError::Invalid(format!(
            "notification receipt path has no parent: {}",
            path.display()
        ))
    })?;
    if parent != work.join("notifications") || path.parent().and_then(Path::parent) != Some(work) {
        return Err(NotificationError::Invalid(format!(
            "notification receipt path is outside the notifications directory: {}",
            path.display()
        )));
    }
    work_fs::ensure_plain_parent(work, path)?;
    Ok(())
}

/// The three processor boundaries that may request an operator notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationEvent {
    #[serde(rename = "task.escalated")]
    TaskEscalated,
    #[serde(rename = "approval.pending")]
    ApprovalPending,
    #[serde(rename = "publish.ci_failed")]
    PublishCiFailed,
}

impl NotificationEvent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskEscalated => "task.escalated",
            Self::ApprovalPending => "approval.pending",
            Self::PublishCiFailed => "publish.ci_failed",
        }
    }
}

/// The only information retained after a best-effort notification attempt.  The fields are all
/// controlled values or validated identifiers, so it is safe to project this into `journal.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationOutcome {
    id: String,
    event: NotificationEvent,
    status: NotificationStatus,
    reason: NotificationReason,
    duration_ms: u128,
    error_class: Option<NotificationErrorClass>,
    resolution: NotificationResolution,
}

impl NotificationOutcome {
    pub fn journal_entry(&self) -> String {
        let mut entry = format!(
            "- notify id={} event={} status={} reason={} duration_ms={}",
            self.id,
            self.event.as_str(),
            self.status.as_str(),
            self.reason.as_str(),
            self.duration_ms
        );
        if let Some(error_class) = self.error_class {
            entry.push_str(" error_class=");
            entry.push_str(error_class.as_str());
        }
        entry
    }

    /// Whether the outcome is durable enough for an approval's pending marker to be consumed.
    /// An unfinished claim, an unavailable receipt, or a failed journal append must leave that
    /// marker in place so a later processor turn can retry the diagnostic/recovery path.
    pub fn resolves_notification_pending(&self) -> bool {
        matches!(self.resolution, NotificationResolution::Resolved)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotificationResolution {
    Resolved,
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotificationErrorClass {
    Redirected,
    SchemaMismatch,
    Serialize,
    Io,
}

impl NotificationErrorClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Redirected => "redirected",
            Self::SchemaMismatch => "schema_mismatch",
            Self::Serialize => "serialize",
            Self::Io => "io",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NotificationStatus {
    Disabled,
    Sent,
    Timeout,
    Cancelled,
    Crash,
    Error,
    Failed,
    /// A live claim is intentionally not re-run; the approval marker remains pending until the
    /// claim becomes stale or the original child finalizes its receipt.
    InProgress,
    /// A stale claim was finalized without a second child launch because delivery is unknowable.
    Unknown,
}

impl NotificationStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Sent => "sent",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Crash => "crash",
            Self::Error => "error",
            Self::Failed => "failed",
            Self::InProgress => "in_progress",
            Self::Unknown => "unknown",
        }
    }

    const fn reason(self) -> NotificationReason {
        match self {
            Self::Disabled => NotificationReason::NotConfigured,
            Self::Sent => NotificationReason::ExitZero,
            Self::Timeout => NotificationReason::Deadline,
            Self::Cancelled => NotificationReason::Cancelled,
            Self::Crash => NotificationReason::ProcessKitCrash,
            Self::Error => NotificationReason::ProcessKitError,
            Self::Failed => NotificationReason::InvalidOrUnavailable,
            Self::InProgress => NotificationReason::ClaimInProgress,
            Self::Unknown => NotificationReason::StaleClaim,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NotificationReason {
    NotConfigured,
    ExitZero,
    Deadline,
    Cancelled,
    ProcessKitCrash,
    ProcessKitError,
    InvalidOrUnavailable,
    ClaimInProgress,
    StaleClaim,
}

impl NotificationReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::ExitZero => "exit_zero",
            Self::Deadline => "deadline",
            Self::Cancelled => "cancelled",
            Self::ProcessKitCrash => "processkit_crash",
            Self::ProcessKitError => "processkit_error",
            Self::InvalidOrUnavailable => "invalid_or_unavailable",
            Self::ClaimInProgress => "claim_in_progress",
            Self::StaleClaim => "stale_claim",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct NotificationReceipt {
    schema: String,
    id: String,
    event: NotificationEvent,
    subject: String,
    status: Option<NotificationStatus>,
    reason: Option<NotificationReason>,
    duration_ms: Option<u128>,
    /// Present only on an unfinished claim. Missing timestamps are legacy claims whose age
    /// cannot be proven; they are recovered fail-closed as `unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claimed_at_secs: Option<u64>,
}

/// Contained dispatcher for one configured, typed argv command.  `None` is a successful no-op
/// so projects that do not use notifications retain the exact ordinary processor behavior.
#[derive(Debug, Clone)]
pub struct NotificationDispatcher {
    work: PathBuf,
    command: Option<Vec<String>>,
}

impl NotificationDispatcher {
    pub fn new(work: impl AsRef<Path>, command: Option<Vec<String>>) -> Self {
        Self {
            work: work.as_ref().to_path_buf(),
            command,
        }
    }

    /// Send one notification at most once. An unfinished claim is never re-launched: while it is
    /// fresh, the dispatcher returns an `in_progress` diagnostic; once it is stale (or has no
    /// timestamp from the legacy format), it records an `unknown` recovery outcome. This keeps
    /// retrying finite and preserves the at-most-once boundary when child delivery is unknowable.
    pub fn dispatch(&self, event: NotificationEvent, subject: &str) -> Option<NotificationOutcome> {
        if !valid_subject(event, subject) {
            return Some(self.outcome(event, subject, NotificationStatus::Failed, 0));
        }
        let Some(command) = self.command.as_ref().filter(|command| !command.is_empty()) else {
            // The parser never produces an empty vector, but the dispatcher is also usable by
            // embedded ports. Treat a malformed programmatic configuration as disabled/failure
            // rather than indexing it and panicking during a terminal processor effect.
            let status = if self.command.is_none() {
                NotificationStatus::Disabled
            } else {
                NotificationStatus::Failed
            };
            return Some(if self.command.is_none() {
                self.outcome(event, subject, status, 0)
            } else {
                self.retryable_outcome(event, subject, status, 0)
            });
        };
        if command[0].is_empty() {
            return Some(self.retryable_outcome(event, subject, NotificationStatus::Failed, 0));
        }
        let receipt = self.receipt_path(event, subject);
        match self.claim_or_load(&receipt, event, subject) {
            Ok(Claim::Final(outcome)) => return Some(outcome),
            Ok(Claim::InProgress) => {
                return Some(self.retryable_outcome(
                    event,
                    subject,
                    NotificationStatus::InProgress,
                    0,
                ));
            }
            Ok(Claim::Stale) => {
                let outcome = self.outcome(event, subject, NotificationStatus::Unknown, 0);
                return Some(match self.save_final_receipt(&receipt, subject, &outcome) {
                    Ok(()) => outcome,
                    Err(error) => self.failure_outcome(event, subject, 0, &error),
                });
            }
            Ok(Claim::Claimed) => {}
            Err(error) => {
                return Some(self.failure_outcome(event, subject, 0, &error));
            }
        }

        let context = safe_context(event, subject);
        let verdict = supervise::run(
            &SpawnSpec::new(
                command[0].clone(),
                command[1..]
                    .iter()
                    .cloned()
                    .chain([event.as_str().to_string(), context])
                    .collect(),
            )
            .current_dir(&self.work)
            .deadline(Some(NOTIFICATION_DEADLINE))
            .output_max_bytes(NOTIFICATION_OUTPUT_MAX_BYTES),
        );
        let status = match verdict.reason {
            Reason::Ok => NotificationStatus::Sent,
            Reason::Timeout => NotificationStatus::Timeout,
            Reason::Cancelled => NotificationStatus::Cancelled,
            Reason::Crash => NotificationStatus::Crash,
            Reason::Error => NotificationStatus::Error,
        };
        let outcome = self.outcome(event, subject, status, verdict.duration_ms);
        // A failed finalization must remain an unfinished claim.  Retrying the effect then skips
        // rather than duplicating a notification whose child may already have delivered it.
        if let Err(error) = self.save_final_receipt(&receipt, subject, &outcome) {
            return Some(self.failure_outcome(event, subject, verdict.duration_ms, &error));
        }
        Some(outcome)
    }

    fn outcome(
        &self,
        event: NotificationEvent,
        subject: &str,
        status: NotificationStatus,
        duration_ms: u128,
    ) -> NotificationOutcome {
        NotificationOutcome {
            id: notification_id(event, subject),
            event,
            status,
            reason: status.reason(),
            duration_ms,
            error_class: None,
            resolution: NotificationResolution::Resolved,
        }
    }

    fn retryable_outcome(
        &self,
        event: NotificationEvent,
        subject: &str,
        status: NotificationStatus,
        duration_ms: u128,
    ) -> NotificationOutcome {
        let mut outcome = self.outcome(event, subject, status, duration_ms);
        outcome.resolution = NotificationResolution::Retry;
        outcome
    }

    fn failure_outcome(
        &self,
        event: NotificationEvent,
        subject: &str,
        duration_ms: u128,
        error: &NotificationError,
    ) -> NotificationOutcome {
        let mut outcome =
            self.retryable_outcome(event, subject, NotificationStatus::Failed, duration_ms);
        outcome.error_class = Some(error.class());
        outcome
    }

    fn receipt_path(&self, event: NotificationEvent, subject: &str) -> PathBuf {
        self.work
            .join("notifications")
            .join(format!("{}.json", notification_id(event, subject)))
    }

    fn claim_or_load(
        &self,
        path: &Path,
        event: NotificationEvent,
        subject: &str,
    ) -> NotificationResult<Claim> {
        let expected_id = notification_id(event, subject);
        let receipt = NotificationReceipt {
            schema: NOTIFICATION_SCHEMA.into(),
            id: expected_id.clone(),
            event,
            subject: subject.into(),
            status: None,
            reason: None,
            duration_ms: None,
            claimed_at_secs: Some(now_secs()),
        };
        let mut content =
            serde_json::to_vec_pretty(&receipt).map_err(NotificationError::Serialize)?;
        content.push(b'\n');
        ensure_receipt_location(&self.work, path)?;
        match work_fs::create_new_plain_file_rooted(&self.work, path) {
            Ok(mut file) => {
                file.write_all(&content)?;
                file.sync_all()?;
                Ok(Claim::Claimed)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.load_existing_receipt(path, event, subject, expected_id)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn load_existing_receipt(
        &self,
        path: &Path,
        event: NotificationEvent,
        subject: &str,
        expected_id: String,
    ) -> NotificationResult<Claim> {
        let text = work_fs::read_required_text(&self.work, path, NOTIFICATION_RECEIPT_MAX_BYTES)?;
        let existing: NotificationReceipt =
            serde_json::from_str(&text).map_err(NotificationError::Deserialize)?;
        if existing.schema != NOTIFICATION_SCHEMA
            || existing.id != expected_id
            || existing.event != event
            || existing.subject != subject
        {
            return Err(NotificationError::Invalid(format!(
                "notification receipt does not match request: {}",
                path.display()
            )));
        }
        match (existing.status, existing.duration_ms) {
            (Some(status), Some(duration_ms)) => Ok(Claim::Final(NotificationOutcome {
                id: expected_id,
                event,
                status,
                // Receipts written by the initial rollout did not retain a reason. Their status
                // was already controlled, so derive the same stable reason rather than re-running
                // the notifier during an upgrade.
                reason: existing.reason.unwrap_or_else(|| status.reason()),
                duration_ms,
                error_class: None,
                resolution: NotificationResolution::Resolved,
            })),
            (None, None) if claim_is_stale(existing.claimed_at_secs) => Ok(Claim::Stale),
            (None, None) => Ok(Claim::InProgress),
            _ => Err(NotificationError::Invalid(format!(
                "notification receipt has inconsistent completion fields: {}",
                path.display()
            ))),
        }
    }

    fn save_final_receipt(
        &self,
        path: &Path,
        subject: &str,
        outcome: &NotificationOutcome,
    ) -> NotificationResult<()> {
        let mut content = serde_json::to_vec_pretty(&NotificationReceipt {
            schema: NOTIFICATION_SCHEMA.into(),
            id: outcome.id.clone(),
            event: outcome.event,
            subject: subject.into(),
            status: Some(outcome.status),
            reason: Some(outcome.reason),
            duration_ms: Some(outcome.duration_ms),
            claimed_at_secs: None,
        })
        .map_err(NotificationError::Serialize)?;
        content.push(b'\n');
        ensure_receipt_location(&self.work, path)?;
        let existing = fs::symlink_metadata(path)?;
        work_fs::require_plain_file(path, &existing)?;
        work_fs::replace_file(&self.work, path, &content, NOTIFICATION_RECEIPT_MAX_BYTES)?;
        let final_metadata = fs::symlink_metadata(path)?;
        work_fs::require_plain_file(path, &final_metadata)?;
        Ok(())
    }
}

enum Claim {
    Claimed,
    InProgress,
    Stale,
    Final(NotificationOutcome),
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn claim_is_stale(claimed_at_secs: Option<u64>) -> bool {
    let Some(claimed_at_secs) = claimed_at_secs else {
        // A v1 receipt created before claim timestamps were persisted cannot establish that a
        // child is still live. Do not wait forever and do not launch a potentially duplicate one.
        return true;
    };
    let now = now_secs();
    now < claimed_at_secs
        || now.saturating_sub(claimed_at_secs) >= NOTIFICATION_CLAIM_STALE_AFTER.as_secs()
}

fn notification_id(event: NotificationEvent, subject: &str) -> String {
    let digest = Sha256::digest(format!("{}|{subject}", event.as_str()).as_bytes());
    format!("nfy-{}", hex(&digest[..16]))
}

fn valid_subject(event: NotificationEvent, subject: &str) -> bool {
    match event {
        NotificationEvent::TaskEscalated => is_task_id(subject),
        NotificationEvent::ApprovalPending => subject
            .strip_prefix("apr-")
            .is_some_and(|hex| hex.len() == 32 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())),
        // A native VCS service may express a commit with a shortened immutable object ID.  Keep
        // the notification context to that narrow, line-safe token rather than passing a forge
        // response, check name, or failure output to the operator command.
        NotificationEvent::PublishCiFailed => {
            !subject.is_empty()
                && subject.len() <= 128
                && subject
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        }
    }
}

fn safe_context(event: NotificationEvent, subject: &str) -> String {
    match event {
        NotificationEvent::TaskEscalated => format!("task={subject} reached_terminal_escalation"),
        NotificationEvent::ApprovalPending => {
            format!("approval={subject} awaits_operator_decision")
        }
        NotificationEvent::PublishCiFailed => {
            format!("published_head={subject} required_ci_failed")
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn work(label: &str) -> PathBuf {
        let work = std::env::current_dir()
            .unwrap()
            .join("target/test-temp")
            .join(format!(
                "orchestrail-notification-{label}-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&work).unwrap();
        work
    }

    #[cfg(windows)]
    fn symlink_file(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }

    #[cfg(unix)]
    fn symlink_file(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn symlink_directory(target: &Path, link: &Path) -> io::Result<()> {
        use std::process::{Command, Stdio};

        match std::os::windows::fs::symlink_dir(target, link) {
            Ok(()) => Ok(()),
            Err(error) if error.raw_os_error() == Some(1_314) => {
                let parent = target.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "test target has no parent")
                })?;
                let relative_link = link.strip_prefix(parent).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "test junction paths do not share a parent",
                    )
                })?;
                let target_name = target.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "test target has no name")
                })?;
                let command = format!(
                    "mklink /j {} {}",
                    relative_link.to_string_lossy().replace('/', "\\"),
                    target_name.to_string_lossy()
                );
                let output = Command::new("cmd.exe")
                    .args(["/d", "/c", &command])
                    .current_dir(parent)
                    .stdin(Stdio::null())
                    .output()?;
                if output.status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "failed to create test junction {} -> {}: stdout={} stderr={}",
                        link.display(),
                        target.display(),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    fn symlink_directory(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn remove_directory_link(path: &Path) -> io::Result<()> {
        fs::remove_dir(path)
    }

    #[cfg(unix)]
    fn remove_directory_link(path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    #[test]
    fn disabled_notification_is_a_successful_noop_without_a_receipt() {
        let work = work("disabled");
        let dispatcher = NotificationDispatcher::new(&work, None);
        let outcome = dispatcher
            .dispatch(NotificationEvent::TaskEscalated, "T-17")
            .unwrap();
        assert!(
            outcome
                .journal_entry()
                .contains("status=disabled reason=not_configured")
        );
        assert!(!work.join("notifications").exists());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn malformed_embedded_argv_fails_without_panicking_or_spawning() {
        let work = work("malformed-argv");
        let dispatcher = NotificationDispatcher::new(&work, Some(Vec::new()));
        let outcome = dispatcher
            .dispatch(NotificationEvent::TaskEscalated, "T-17")
            .unwrap();
        assert!(
            outcome
                .journal_entry()
                .contains("status=failed reason=invalid_or_unavailable")
        );
        assert!(!work.join("notifications").exists());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn fresh_claim_is_reported_and_keeps_the_pending_retry_marker() {
        let work = work("fresh-claim");
        let dispatcher =
            NotificationDispatcher::new(&work, Some(vec!["orchestrail-no-such-notifier".into()]));
        let event = NotificationEvent::TaskEscalated;
        let subject = "T-17";
        let receipt = dispatcher.receipt_path(event, subject);
        assert!(matches!(
            dispatcher.claim_or_load(&receipt, event, subject),
            Ok(Claim::Claimed)
        ));

        let outcome = dispatcher.dispatch(event, subject).unwrap();
        assert!(
            outcome
                .journal_entry()
                .contains("status=in_progress reason=claim_in_progress")
        );
        assert!(!outcome.resolves_notification_pending());
        let receipt_text = fs::read_to_string(receipt).unwrap();
        assert!(receipt_text.contains("\"status\": null"));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn stale_claim_is_finalized_as_unknown_without_relaunching_the_child() {
        let work = work("stale-claim");
        let dispatcher =
            NotificationDispatcher::new(&work, Some(vec!["orchestrail-no-such-notifier".into()]));
        let event = NotificationEvent::TaskEscalated;
        let subject = "T-17";
        let receipt = dispatcher.receipt_path(event, subject);
        fs::create_dir(receipt.parent().unwrap()).unwrap();
        let stale = NotificationReceipt {
            schema: NOTIFICATION_SCHEMA.into(),
            id: notification_id(event, subject),
            event,
            subject: subject.into(),
            status: None,
            reason: None,
            duration_ms: None,
            claimed_at_secs: Some(
                now_secs().saturating_sub(NOTIFICATION_CLAIM_STALE_AFTER.as_secs() + 1),
            ),
        };
        fs::write(&receipt, serde_json::to_vec_pretty(&stale).unwrap()).unwrap();

        let recovered = dispatcher.dispatch(event, subject).unwrap();
        assert!(
            recovered
                .journal_entry()
                .contains("status=unknown reason=stale_claim")
        );
        assert!(recovered.resolves_notification_pending());
        let persisted: NotificationReceipt =
            serde_json::from_str(&fs::read_to_string(&receipt).unwrap()).unwrap();
        assert_eq!(persisted.status, Some(NotificationStatus::Unknown));
        assert_eq!(persisted.claimed_at_secs, None);

        let resumed = dispatcher.dispatch(event, subject).unwrap();
        assert_eq!(resumed, recovered);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn legacy_incomplete_receipt_is_recovered_fail_closed() {
        let work = work("legacy-incomplete-claim");
        let dispatcher =
            NotificationDispatcher::new(&work, Some(vec!["orchestrail-no-such-notifier".into()]));
        let event = NotificationEvent::TaskEscalated;
        let subject = "T-17";
        let receipt = dispatcher.receipt_path(event, subject);
        fs::create_dir(receipt.parent().unwrap()).unwrap();
        fs::write(
            &receipt,
            serde_json::json!({
                "schema": NOTIFICATION_SCHEMA,
                "id": notification_id(event, subject),
                "event": event,
                "subject": subject,
                "status": null,
                "reason": null,
                "duration_ms": null,
            })
            .to_string(),
        )
        .unwrap();

        let outcome = dispatcher.dispatch(event, subject).unwrap();
        assert!(outcome.journal_entry().contains("status=unknown"));
        assert!(outcome.resolves_notification_pending());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn successful_notification_is_deduplicated_after_finalization() {
        let work = work("successful-dedup");
        let dispatcher = NotificationDispatcher::new(&work, Some(successful_command()));
        let first = dispatcher
            .dispatch(NotificationEvent::TaskEscalated, "T-17")
            .unwrap();
        assert!(
            first
                .journal_entry()
                .contains("status=sent reason=exit_zero")
        );
        let resumed = dispatcher
            .dispatch(NotificationEvent::TaskEscalated, "T-17")
            .unwrap();
        assert_eq!(resumed, first);
        assert_eq!(fs::read_dir(work.join("notifications")).unwrap().count(), 1);
        let _ = fs::remove_dir_all(work);
    }

    fn successful_command() -> Vec<String> {
        #[cfg(windows)]
        {
            vec!["cmd.exe".into(), "/d".into(), "/c".into(), "exit 0".into()]
        }
        #[cfg(unix)]
        {
            vec!["sh".into(), "-c".into(), "exit 0".into()]
        }
    }

    #[test]
    fn failed_spawn_is_claimed_and_is_not_relaunched_on_resume() {
        let work = work("once");
        let dispatcher = NotificationDispatcher::new(
            &work,
            Some(vec![
                "orchestrail-no-such-notifier".into(),
                "--channel".into(),
                "ops".into(),
            ]),
        );
        let first = dispatcher
            .dispatch(
                NotificationEvent::ApprovalPending,
                &format!("apr-{}", "a".repeat(32)),
            )
            .unwrap();
        assert!(
            first
                .journal_entry()
                .contains("status=crash reason=processkit_crash")
        );
        let resumed = dispatcher
            .dispatch(
                NotificationEvent::ApprovalPending,
                &format!("apr-{}", "a".repeat(32)),
            )
            .unwrap();
        assert_eq!(resumed, first);
        assert_eq!(fs::read_dir(work.join("notifications")).unwrap().count(), 1);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn redirected_receipt_or_parent_fails_closed_without_touching_the_target() {
        let receipt_work = work("redirected-receipt");
        let dispatcher = NotificationDispatcher::new(
            &receipt_work,
            Some(vec!["orchestrail-no-such-notifier".into()]),
        );
        let subject = "T-17";
        let receipt = dispatcher.receipt_path(NotificationEvent::TaskEscalated, subject);
        fs::create_dir(receipt.parent().unwrap()).unwrap();
        let external = receipt_work.with_extension("external.json");
        fs::write(&external, "external sentinel\n").unwrap();
        if symlink_file(&external, &receipt).is_ok() {
            let outcome = dispatcher
                .dispatch(NotificationEvent::TaskEscalated, subject)
                .unwrap();
            assert!(
                outcome
                    .journal_entry()
                    .contains("status=failed reason=invalid_or_unavailable")
            );
            assert!(outcome.journal_entry().contains("error_class=redirected"));
            assert_eq!(
                fs::read_to_string(&external).unwrap(),
                "external sentinel\n"
            );
            fs::remove_file(&receipt).unwrap();
        }
        let _ = fs::remove_file(&external);
        let _ = fs::remove_dir_all(receipt_work);

        let work = work("redirected-parent");
        let external = work.with_extension("external-dir");
        fs::create_dir(&external).unwrap();
        let notifications = work.join("notifications");
        symlink_directory(&external, &notifications).unwrap();
        let dispatcher =
            NotificationDispatcher::new(&work, Some(vec!["orchestrail-no-such-notifier".into()]));
        let outcome = dispatcher
            .dispatch(NotificationEvent::TaskEscalated, subject)
            .unwrap();
        assert!(outcome.journal_entry().contains("status=failed"));
        assert!(outcome.journal_entry().contains("error_class=redirected"));
        assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
        remove_directory_link(&notifications).unwrap();
        let _ = fs::remove_dir_all(work);
        let _ = fs::remove_dir_all(external);
    }

    #[test]
    fn receipt_reload_rechecks_a_parent_redirect_created_after_the_claim() {
        let work = work("post-claim-parent-redirect");
        let dispatcher =
            NotificationDispatcher::new(&work, Some(vec!["orchestrail-no-such-notifier".into()]));
        let event = NotificationEvent::TaskEscalated;
        let subject = "T-17";
        let receipt = dispatcher.receipt_path(event, subject);
        assert!(matches!(
            dispatcher.claim_or_load(&receipt, event, subject),
            Ok(Claim::Claimed)
        ));

        let original_notifications = work.join("notifications-original");
        fs::rename(work.join("notifications"), &original_notifications).unwrap();
        let external = work.with_extension("post-claim-external");
        fs::create_dir(&external).unwrap();
        fs::copy(
            original_notifications.join(receipt.file_name().unwrap()),
            external.join(receipt.file_name().unwrap()),
        )
        .unwrap();
        symlink_directory(&external, &work.join("notifications")).unwrap();

        let result = dispatcher.load_existing_receipt(
            &receipt,
            event,
            subject,
            notification_id(event, subject),
        );
        let Err(error) = result else {
            panic!("redirected parent must fail before the external receipt is read")
        };
        assert_eq!(error.class(), NotificationErrorClass::Redirected);

        remove_directory_link(&work.join("notifications")).unwrap();
        let _ = fs::remove_dir_all(work);
        let _ = fs::remove_dir_all(external);
    }

    #[test]
    fn invalid_receipt_reports_a_redacted_schema_classification() {
        let work = work("invalid-receipt-class");
        let dispatcher =
            NotificationDispatcher::new(&work, Some(vec!["orchestrail-no-such-notifier".into()]));
        let event = NotificationEvent::TaskEscalated;
        let subject = "T-17";
        let receipt = dispatcher.receipt_path(event, subject);
        fs::create_dir(receipt.parent().unwrap()).unwrap();
        fs::write(&receipt, "{}\n").unwrap();

        let journal = dispatcher.dispatch(event, subject).unwrap().journal_entry();
        assert!(journal.contains("status=failed"));
        assert!(journal.contains("error_class=schema_mismatch"));
        assert!(!journal.contains(&work.display().to_string()));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn subjects_and_contexts_are_strictly_redacted() {
        assert!(valid_subject(
            NotificationEvent::PublishCiFailed,
            "f00dbabe"
        ));
        assert!(!valid_subject(
            NotificationEvent::PublishCiFailed,
            "bad subject\ntext"
        ));
        assert_eq!(
            safe_context(NotificationEvent::TaskEscalated, "T-1"),
            "task=T-1 reached_terminal_escalation"
        );
    }
}
