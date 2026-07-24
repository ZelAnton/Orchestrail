//! Best-effort, redacted operator notifications.
//!
//! A notification must never become another orchestration gate.  The dispatcher records a
//! durable claim before launching its one contained ProcessKit child; a resumed effect therefore
//! observes the claim instead of sending a duplicate message.  It deliberately persists neither
//! command output nor the underlying VCS/approval payload.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::supervise::{self, Reason, SpawnSpec};
use crate::work_fs;

const NOTIFICATION_SCHEMA: &str = "orchestrail/notification@1";
const NOTIFICATION_DEADLINE: Duration = Duration::from_secs(30);
const NOTIFICATION_OUTPUT_MAX_BYTES: usize = 16 * 1024;
const NOTIFICATION_RECEIPT_MAX_BYTES: u64 = 64 * 1024;

fn redirected(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

fn require_plain_directory(path: &Path) -> Result<(), ()> {
    let metadata = fs::symlink_metadata(path).map_err(|_| ())?;
    if metadata.is_dir() && !redirected(&metadata) {
        Ok(())
    } else {
        Err(())
    }
}

fn ensure_receipt_parent(work: &Path, path: &Path) -> Result<(), ()> {
    require_plain_directory(work)?;
    let parent = path.parent().ok_or(())?;
    if parent != work.join("notifications") || path.parent().and_then(Path::parent) != Some(work) {
        return Err(());
    }
    match fs::symlink_metadata(parent) {
        Ok(_) => require_plain_directory(parent),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(parent).map_err(|_| ())?;
            require_plain_directory(parent)
        }
        Err(_) => Err(()),
    }
}

fn read_receipt(work: &Path, path: &Path) -> Result<String, ()> {
    ensure_receipt_parent(work, path)?;
    let before = fs::symlink_metadata(path).map_err(|_| ())?;
    if !before.is_file() || redirected(&before) {
        return Err(());
    }
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
    let mut file = options.open(path).map_err(|_| ())?;
    let opened = file.metadata().map_err(|_| ())?;
    if !opened.is_file() || redirected(&opened) || opened.len() > NOTIFICATION_RECEIPT_MAX_BYTES {
        return Err(());
    }
    let mut text = String::new();
    (&mut file)
        .take(NOTIFICATION_RECEIPT_MAX_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|_| ())?;
    if text.len() as u64 > NOTIFICATION_RECEIPT_MAX_BYTES {
        return Err(());
    }
    let after = fs::symlink_metadata(path).map_err(|_| ())?;
    if !after.is_file() || redirected(&after) {
        return Err(());
    }
    Ok(text)
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
}

impl NotificationOutcome {
    pub fn journal_entry(&self) -> String {
        format!(
            "- notify id={} event={} status={} reason={} duration_ms={}",
            self.id,
            self.event.as_str(),
            self.status.as_str(),
            self.reason.as_str(),
            self.duration_ms
        )
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

    /// Send one notification at most once. `None` means another process (or an interrupted
    /// predecessor) already holds an unfinished durable claim, so this caller must not infer a
    /// delivery result or launch a second child.
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
            return Some(self.outcome(event, subject, status, 0));
        };
        if command[0].is_empty() {
            return Some(self.outcome(event, subject, NotificationStatus::Failed, 0));
        }
        let receipt = self.receipt_path(event, subject);
        match self.claim_or_load(&receipt, event, subject) {
            Ok(Claim::Final(outcome)) => return Some(outcome),
            Ok(Claim::InProgress) => return None,
            Ok(Claim::Claimed) => {}
            Err(()) => return Some(self.outcome(event, subject, NotificationStatus::Failed, 0)),
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
        if self
            .save_final_receipt(&receipt, subject, &outcome)
            .is_err()
        {
            return None;
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
        }
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
    ) -> Result<Claim, ()> {
        let expected_id = notification_id(event, subject);
        let receipt = NotificationReceipt {
            schema: NOTIFICATION_SCHEMA.into(),
            id: expected_id.clone(),
            event,
            subject: subject.into(),
            status: None,
            reason: None,
            duration_ms: None,
        };
        let mut content = serde_json::to_vec_pretty(&receipt).map_err(|_| ())?;
        content.push(b'\n');
        ensure_receipt_parent(&self.work, path)?;
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                file.write_all(&content).map_err(|_| ())?;
                file.sync_all().map_err(|_| ())?;
                Ok(Claim::Claimed)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let text = read_receipt(&self.work, path)?;
                let existing: NotificationReceipt = serde_json::from_str(&text).map_err(|_| ())?;
                if existing.schema != NOTIFICATION_SCHEMA
                    || existing.id != expected_id
                    || existing.event != event
                    || existing.subject != subject
                {
                    return Err(());
                }
                match (existing.status, existing.duration_ms) {
                    (Some(status), Some(duration_ms)) => Ok(Claim::Final(NotificationOutcome {
                        id: expected_id,
                        event,
                        status,
                        // Receipts written by the initial rollout did not retain a reason. Their
                        // status was already controlled, so derive the same stable reason rather
                        // than re-running the notifier during an upgrade.
                        reason: existing.reason.unwrap_or_else(|| status.reason()),
                        duration_ms,
                    })),
                    (None, None) => Ok(Claim::InProgress),
                    _ => Err(()),
                }
            }
            Err(_) => Err(()),
        }
    }

    fn save_final_receipt(
        &self,
        path: &Path,
        subject: &str,
        outcome: &NotificationOutcome,
    ) -> Result<(), ()> {
        let mut content = serde_json::to_vec_pretty(&NotificationReceipt {
            schema: NOTIFICATION_SCHEMA.into(),
            id: outcome.id.clone(),
            event: outcome.event,
            subject: subject.into(),
            status: Some(outcome.status),
            reason: Some(outcome.reason),
            duration_ms: Some(outcome.duration_ms),
        })
        .map_err(|_| ())?;
        content.push(b'\n');
        ensure_receipt_parent(&self.work, path)?;
        let existing = fs::symlink_metadata(path).map_err(|_| ())?;
        if !existing.is_file() || redirected(&existing) {
            return Err(());
        }
        work_fs::replace_file(&self.work, path, &content, NOTIFICATION_RECEIPT_MAX_BYTES)
            .map_err(|_| ())?;
        let final_metadata = fs::symlink_metadata(path).map_err(|_| ())?;
        if !final_metadata.is_file() || redirected(&final_metadata) {
            return Err(());
        }
        Ok(())
    }
}

enum Claim {
    Claimed,
    InProgress,
    Final(NotificationOutcome),
}

fn notification_id(event: NotificationEvent, subject: &str) -> String {
    let digest = Sha256::digest(format!("{}|{subject}", event.as_str()).as_bytes());
    format!("nfy-{}", hex(&digest[..16]))
}

fn valid_subject(event: NotificationEvent, subject: &str) -> bool {
    match event {
        NotificationEvent::TaskEscalated => subject.strip_prefix("T-").is_some_and(|number| {
            !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
        }),
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
        let work = std::env::temp_dir().join(format!(
            "orchestrail-notification-{label}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&work).unwrap();
        work
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
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&external, &receipt).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&external, &receipt).is_ok();
        if linked {
            let outcome = dispatcher
                .dispatch(NotificationEvent::TaskEscalated, subject)
                .unwrap();
            assert!(outcome.journal_entry().contains("status=failed"));
            assert_eq!(
                fs::read_to_string(&external).unwrap(),
                "external sentinel\n"
            );
        }
        let _ = fs::remove_file(&external);
        let _ = fs::remove_dir_all(receipt_work);

        let work = work("redirected-parent");
        let external = work.with_extension("external-dir");
        fs::create_dir(&external).unwrap();
        let notifications = work.join("notifications");
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(&external, &notifications).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&external, &notifications).is_ok();
        if linked {
            let dispatcher = NotificationDispatcher::new(
                &work,
                Some(vec!["orchestrail-no-such-notifier".into()]),
            );
            let outcome = dispatcher
                .dispatch(NotificationEvent::TaskEscalated, subject)
                .unwrap();
            assert!(outcome.journal_entry().contains("status=failed"));
            assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
        }
        let _ = fs::remove_dir_all(work);
        let _ = fs::remove_dir_all(external);
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
