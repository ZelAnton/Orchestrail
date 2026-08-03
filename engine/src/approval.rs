//! Durable, one-time human approval records.
//!
//! The processor may never turn an ambiguous policy condition into an implicit grant.  This
//! module records the exact subject, reason, content-bound fingerprint, and policy snapshot that
//! an operator decided.  Its deterministic identifier makes a resumed request idempotent, while
//! a changed fingerprint or policy necessarily addresses a different record.
//!
//! # Crash recovery of the mutation lock
//!
//! Every mutation runs inside `.work/approvals/.approval-mutation.lock`, taken through the same
//! durable transaction-lock primitive as the owner lease's `state-tx.lock`
//! (the shared transaction-lock primitive). That shared primitive is what makes a killed
//! process recoverable: a lock left behind by a holder that died mid-mutation is waited on only
//! until the configured stale threshold, then broken by exactly one contender under a kernel
//! lifecycle sidecar, and the mutation proceeds. An approval mutation therefore cannot be wedged
//! forever by a crash, a power loss, or a killed supervisor child. A holder that is still live is
//! still refused — with [`ApprovalError::Busy`] naming the holder and its age, so an operator can
//! tell "someone else is deciding right now" from "this lock needs looking at".
//!
//! Recovery never softens ownership. The UUID holder token inside the lock is checked before the
//! critical section and again when it is released, and a lock that stopped recording this
//! acquisition is [`ApprovalError::Corrupt`] at both boundaries — including when the mutation
//! itself also failed, because a second writer inside an authorization mutation is the more
//! serious of the two facts and may not be suppressed by the less serious one.
//!
//! Adopting that primitive deliberately inherits its fail-closed storage requirement: the sidecar
//! is refused on network, stackable, and unclassifiable filesystems, so a `.work/approvals`
//! directory on such storage now reports an error instead of mutating. That is the correct
//! trade for an authorization artifact — a lock whose exclusion the kernel will not honor cannot
//! be allowed to admit two concurrent deciders — and `.work/approvals` sits under the same
//! `.work` root as the lease lock, which already imposes exactly this requirement.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::ownership::{
    TRANSACTION_LOCK_STALE_AFTER, TRANSACTION_LOCK_TIMEOUT, TransactionLockError,
    TransactionLockPolicy, TransactionReleaseError, acquire_transaction_lock,
};
use crate::work_fs;

pub const APPROVAL_SCHEMA: &str = "orchestrail/approval@1";
pub const APPROVAL_MANIFEST_SCHEMA: &str = "orchestrail/approval-manifest@1";
const MAX_APPROVAL_BYTES: u64 = 4 * 1024 * 1024;
const MUTATION_LOCK: &str = ".approval-mutation.lock";
/// The mutation lock's own kernel-locked lifecycle sidecar. It is persistent and never removed,
/// and its dot-prefixed, non-`.json` name keeps it out of every approval-artifact projection.
const MUTATION_LIFECYCLE_LOCK: &str = ".approval-mutation.lifecycle.lock";
/// Read ceiling for the holder token recorded inside the lock. The token itself is a v4 UUID
/// rather than the lease's `pid:sequence` form: an approval mutation can come from any of several
/// short-lived processes, and a recycled PID must never be able to look like a still-valid owner.
const MUTATION_LOCK_MAX_TOKEN_BYTES: u64 = 1_024;

/// A serializable artifact whose own content identity must match the approval record it is
/// stored beside. This makes the human-readable manifest a checked part of the authorization,
/// rather than unauthenticated explanatory metadata.
pub trait ContentBoundApprovalManifest: Serialize {
    fn fingerprint(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRequest {
    pub task_id: Option<String>,
    pub batch_id: Option<String>,
    pub reason: String,
    /// A caller-owned, content-bound identity for the exact change set being approved.
    pub fingerprint: String,
    /// SHA-256 (or another immutable identity) of the active policy source.
    pub policy_hash: String,
    pub now_secs: u64,
    pub deadline_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub schema: String,
    pub id: String,
    pub subject: String,
    pub task_id: Option<String>,
    pub batch_id: Option<String>,
    pub reason: String,
    pub fingerprint: String,
    pub policy_hash: String,
    pub created_at_secs: u64,
    pub deadline_at_secs: u64,
    /// A newly created, still-undecided record must receive one best-effort operator notice.
    /// Keeping this marker in the authority artifact closes the crash window between durable
    /// approval creation and the separately contained notification child.
    #[serde(default)]
    pub notification_pending: bool,
    pub decision: Option<ApprovalDecision>,
    pub decided_by: Option<String>,
    pub decided_at_secs: Option<u64>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalDecision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalStatus {
    Approved { id: String },
    Pending { id: String, deadline_at_secs: u64 },
    Rejected { id: String },
    ExpiredTimeout { id: String },
    ExpiredStale { id: String },
    Missing { id: String },
}

impl ApprovalStatus {
    pub fn allows_progress(&self) -> bool {
        matches!(self, Self::Approved { .. })
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Approved { id }
            | Self::Pending { id, .. }
            | Self::Rejected { id }
            | Self::ExpiredTimeout { id }
            | Self::ExpiredStale { id }
            | Self::Missing { id } => id,
        }
    }
}

#[derive(Debug)]
pub enum ApprovalError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Invalid(String),
    Corrupt(String),
    AlreadyDecided {
        id: String,
    },
    Expired {
        id: String,
    },
    Missing {
        id: String,
    },
    /// A live peer holds the mutation lock. The observed holder and its age travel with the
    /// refusal: a caller that retries, and an operator reading the message, both need to know
    /// whether this is momentary contention or a lock worth inspecting.
    Busy {
        age_ms: Option<u128>,
        kind: String,
    },
}

impl fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "approval I/O error: {error}"),
            Self::Json(error) => write!(f, "approval JSON error: {error}"),
            Self::Invalid(message) | Self::Corrupt(message) => f.write_str(message),
            Self::AlreadyDecided { id } => write!(f, "approval {id:?} was already decided"),
            Self::Expired { id } => write!(f, "approval {id:?} expired before a decision"),
            Self::Missing { id } => write!(f, "approval {id:?} does not exist"),
            Self::Busy { age_ms, kind } => match age_ms {
                Some(age_ms) => write!(
                    f,
                    "another approval mutation is already in progress ({kind}, age={age_ms}ms); \
                     a holder that stops responding is recovered automatically once it exceeds \
                     the {}ms stale threshold",
                    TRANSACTION_LOCK_STALE_AFTER.as_millis()
                ),
                None => write!(
                    f,
                    "another approval mutation is already in progress ({kind}, age unavailable); \
                     inspect .work/approvals/{MUTATION_LOCK} before retrying"
                ),
            },
        }
    }
}

impl std::error::Error for ApprovalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Invalid(_)
            | Self::Corrupt(_)
            | Self::AlreadyDecided { .. }
            | Self::Expired { .. }
            | Self::Missing { .. }
            | Self::Busy { .. } => None,
        }
    }
}

impl From<std::io::Error> for ApprovalError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<TransactionLockError> for ApprovalError {
    fn from(error: TransactionLockError) -> Self {
        match error {
            TransactionLockError::Io(error) => Self::Io(error),
            TransactionLockError::Busy { age_ms, kind } => Self::Busy { age_ms, kind },
        }
    }
}

impl From<serde_json::Error> for ApprovalError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub type Result<T> = std::result::Result<T, ApprovalError>;

#[derive(Debug, Clone)]
pub struct ApprovalStore {
    work: PathBuf,
    directory: PathBuf,
}

impl ApprovalStore {
    pub fn new(work: impl AsRef<Path>) -> Result<Self> {
        let work = work.as_ref();
        let metadata = fs::symlink_metadata(work).map_err(ApprovalError::Io)?;
        if !metadata.is_dir() || work_fs::redirected(&metadata) {
            return Err(ApprovalError::Invalid(format!(
                "approval work directory does not exist: {}",
                work.display()
            )));
        }
        let directory = work.join("approvals");
        Ok(Self {
            work: work.to_path_buf(),
            directory,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Create or retrieve the deterministic record for this exact request.
    pub fn request(&self, request: ApprovalRequest) -> Result<ApprovalRecord> {
        validate_request(&request)?;
        let subject = subject(&request)?;
        let id = approval_id(
            &subject,
            &request.reason,
            &request.fingerprint,
            &request.policy_hash,
        );
        self.with_mutation_lock(|| {
            if let Some(record) = self.load(&id)? {
                validate_record(&record, &id)?;
                return Ok(record);
            }
            let record = ApprovalRecord {
                schema: APPROVAL_SCHEMA.into(),
                id: id.clone(),
                subject,
                task_id: request.task_id,
                batch_id: request.batch_id,
                reason: request.reason,
                fingerprint: request.fingerprint,
                policy_hash: request.policy_hash,
                created_at_secs: request.now_secs,
                deadline_at_secs: request.now_secs.saturating_add(request.deadline_secs),
                notification_pending: true,
                decision: None,
                decided_by: None,
                decided_at_secs: None,
                note: None,
            };
            self.save(&record)?;
            Ok(record)
        })
    }

    /// Decide an open approval exactly once. Expired records cannot be consumed after the fact.
    pub fn decide(
        &self,
        id: &str,
        decision: ApprovalDecision,
        by: &str,
        note: Option<String>,
        now_secs: u64,
    ) -> Result<ApprovalRecord> {
        validate_id(id)?;
        validate_text(by, "decision maker")?;
        if let Some(note) = &note {
            validate_note(note)?;
        }
        self.with_mutation_lock(|| {
            let mut record = self
                .load(id)?
                .ok_or_else(|| ApprovalError::Missing { id: id.into() })?;
            validate_record(&record, id)?;
            if record.decision.is_some() {
                return Err(ApprovalError::AlreadyDecided { id: id.into() });
            }
            if now_secs > record.deadline_at_secs {
                return Err(ApprovalError::Expired { id: id.into() });
            }
            record.decision = Some(decision);
            record.decided_by = Some(by.into());
            record.decided_at_secs = Some(now_secs);
            record.note = note;
            self.save(&record)?;
            Ok(record)
        })
    }

    /// Mark the creation notification as consumed. This is idempotent and is deliberately
    /// separate from a decision. The dispatcher clears this marker only after it has journaled a
    /// durable delivery result or a fail-closed stale-claim `unknown` recovery; a fresh unfinished
    /// claim remains pending and is diagnosed on the next retry instead of being silently lost.
    pub fn clear_notification_pending(&self, id: &str) -> Result<()> {
        validate_id(id)?;
        self.with_mutation_lock(|| {
            let mut record = self
                .load(id)?
                .ok_or_else(|| ApprovalError::Missing { id: id.into() })?;
            validate_record(&record, id)?;
            if record.notification_pending {
                record.notification_pending = false;
                self.save(&record)?;
            }
            Ok(())
        })
    }

    /// Re-evaluate a record against the current content/policy fingerprint and current clock.
    /// An approval is usable only if both identities still exactly match.
    pub fn status(
        &self,
        id: &str,
        fingerprint: &str,
        policy_hash: &str,
        now_secs: u64,
    ) -> Result<ApprovalStatus> {
        validate_id(id)?;
        validate_identity(fingerprint, "fingerprint")?;
        validate_identity(policy_hash, "policy hash")?;
        let Some(record) = self.load(id)? else {
            return Ok(ApprovalStatus::Missing { id: id.into() });
        };
        validate_record(&record, id)?;
        if record.fingerprint != fingerprint || record.policy_hash != policy_hash {
            return Ok(ApprovalStatus::ExpiredStale { id: id.into() });
        }
        match record.decision {
            Some(ApprovalDecision::Approve) => Ok(ApprovalStatus::Approved { id: id.into() }),
            Some(ApprovalDecision::Reject) => Ok(ApprovalStatus::Rejected { id: id.into() }),
            None if now_secs > record.deadline_at_secs => {
                Ok(ApprovalStatus::ExpiredTimeout { id: id.into() })
            }
            None => Ok(ApprovalStatus::Pending {
                id: id.into(),
                deadline_at_secs: record.deadline_at_secs,
            }),
        }
    }

    pub fn load(&self, id: &str) -> Result<Option<ApprovalRecord>> {
        validate_id(id)?;
        let path = self.path(id);
        if !approval_directory_available(&self.work, &self.directory, false)? {
            return Ok(None);
        }
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        work_fs::require_plain_file(&path, &metadata).map_err(|_| {
            ApprovalError::Corrupt(format!(
                "approval artifact is redirected or non-regular: {}",
                path.display()
            ))
        })?;
        let text = read_approval_text(&path, metadata.len())?;
        let record: ApprovalRecord = serde_json::from_str(&text)?;
        validate_record(&record, id)?;
        Ok(Some(record))
    }

    /// Atomically persist the content manifest for an existing approval. A caller cannot attach
    /// a manifest that describes a different change set than the deterministic record.
    pub fn save_manifest<T: ContentBoundApprovalManifest>(
        &self,
        id: &str,
        manifest: &T,
    ) -> Result<()> {
        validate_id(id)?;
        self.with_mutation_lock(|| {
            let record = self
                .load(id)?
                .ok_or_else(|| ApprovalError::Missing { id: id.into() })?;
            validate_record(&record, id)?;
            let manifest_fingerprint = manifest.fingerprint();
            validate_identity(&manifest_fingerprint, "manifest fingerprint")?;
            if manifest_fingerprint != record.fingerprint {
                return Err(ApprovalError::Invalid(format!(
                    "approval manifest fingerprint does not match approval {id:?}"
                )));
            }
            let persisted = PersistedApprovalManifest {
                schema: APPROVAL_MANIFEST_SCHEMA,
                approval_id: id,
                fingerprint: &manifest_fingerprint,
                manifest,
            };
            let mut content = serde_json::to_vec_pretty(&persisted)?;
            content.push(b'\n');
            self.replace_plain(&self.manifest_path(id)?, &content)
        })
    }

    /// Return the checked, deterministic location of an approval's accompanying manifest.
    pub fn manifest_path(&self, id: &str) -> Result<PathBuf> {
        validate_id(id)?;
        Ok(self.directory.join(format!("{id}.manifest.json")))
    }

    fn save(&self, record: &ApprovalRecord) -> Result<()> {
        validate_record(record, &record.id)?;
        let bytes = serde_json::to_vec_pretty(record)?;
        let mut content = bytes;
        content.push(b'\n');
        self.replace_plain(&self.path(&record.id), &content)
    }

    fn replace_plain(&self, path: &Path, content: &[u8]) -> Result<()> {
        approval_directory_available(&self.work, &self.directory, true)?;
        if content.len() as u64 > MAX_APPROVAL_BYTES {
            return Err(ApprovalError::Corrupt(
                "approval artifact is oversized".into(),
            ));
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) => work_fs::require_plain_file(path, &metadata).map_err(|_| {
                ApprovalError::Corrupt(format!(
                    "approval artifact is redirected or non-regular: {}",
                    path.display()
                ))
            })?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        work_fs::replace_file(&self.work, path, content, MAX_APPROVAL_BYTES)?;
        let metadata = fs::symlink_metadata(path)?;
        work_fs::require_plain_file(path, &metadata).map_err(|_| {
            ApprovalError::Corrupt(format!(
                "approval replacement is not a plain file: {}",
                path.display()
            ))
        })?;
        Ok(())
    }

    fn with_mutation_lock<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        self.with_mutation_lock_policy(
            TRANSACTION_LOCK_TIMEOUT,
            TRANSACTION_LOCK_STALE_AFTER,
            operation,
        )
    }

    /// Run one mutation under the durable approval mutation lock.
    ///
    /// The lock itself is [`crate::ownership`]'s transaction-lock primitive rather than a private
    /// copy of it: contention waits up to `timeout`, and a lock orphaned by a crashed holder is
    /// broken after `stale_after` by exactly one contender. The holder token check below is kept
    /// on top of that primitive's own filesystem-identity proofs because it answers a different
    /// question — those proofs establish that the pathname still names the entry this process
    /// created, while the token establishes that the *contents* still record this acquisition. An
    /// external writer that overwrites the lock in place changes only the latter.
    ///
    /// Timing is a parameter purely so tests can drive both branches without waiting minutes;
    /// every caller uses the shared defaults through [`Self::with_mutation_lock`], deliberately
    /// the lease's own thresholds rather than an approval-specific pair. The wait is synchronous,
    /// including on the TUI's decide keystroke, which is a considered trade: an *orphaned* lock
    /// (the case that used to fail forever) is broken on the first observation without waiting at
    /// all, and only a genuinely live peer — which holds the lock for one small read-modify-write
    /// — can make a caller wait, bounded well under the 120s the legacy decide path on that same
    /// keystroke already allows.
    fn with_mutation_lock_policy<T>(
        &self,
        timeout: Duration,
        stale_after: Duration,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        approval_directory_available(&self.work, &self.directory, true)?;
        let lock = self.directory.join(MUTATION_LOCK);
        let token = Uuid::new_v4().to_string();
        let policy = TransactionLockPolicy::new(
            &self.work,
            &lock,
            MUTATION_LIFECYCLE_LOCK,
            timeout,
            stale_after,
        );
        let mut guard = acquire_transaction_lock(&policy, token.clone())?;
        let result = (|| {
            self.verify_mutation_lock_token(&lock, &token)?;
            operation()
        })();
        // The guard removes the lock only while it still records our own token, so a lock taken
        // over by anyone else is left untouched and reported. The two ways a release can fail are
        // not the same condition, and collapsing them into one would silently weaken the holder
        // token guarantee this store depends on:
        //
        // * A holder substitution — the lock stopped recording this acquisition — is exactly the
        //   condition [`Self::verify_mutation_lock_token`] refuses on entry, observed one moment
        //   later. It carries the same meaning there and here (another writer may have entered an
        //   authorization mutation), so it reports the same `Corrupt`, and it is reported even
        //   when the mutation itself failed: a second decider on an approval outranks whatever the
        //   operation was going to say, and suppressing it would leave that fact unrecorded.
        // * A plain I/O failure to remove our own lock is surfaced only when the mutation
        //   succeeded; otherwise the mutation's own error is the primary one worth reporting.
        match guard.release() {
            Ok(()) => result,
            Err(owner_changed @ TransactionReleaseError::OwnerChanged { .. }) => {
                Err(ApprovalError::Corrupt(format!(
                    "approval mutation lock ownership changed before release: {}",
                    owner_changed.describe()
                )))
            }
            Err(TransactionReleaseError::Io(error)) if result.is_ok() => {
                Err(ApprovalError::Io(error))
            }
            Err(TransactionReleaseError::Io(_)) => result,
        }
    }

    /// Prove the held lock still records this acquisition's token before entering the critical
    /// section. Anything else — a truncated, overwritten, or externally rewritten lock — is a
    /// corrupt control-plane state, never a mutation this process may proceed with.
    fn verify_mutation_lock_token(&self, lock: &Path, token: &str) -> Result<()> {
        let observed =
            work_fs::read_required_text(&self.work, lock, MUTATION_LOCK_MAX_TOKEN_BYTES)?;
        if observed != token {
            return Err(ApprovalError::Corrupt(
                "approval mutation lock ownership changed before use".into(),
            ));
        }
        Ok(())
    }

    fn path(&self, id: &str) -> PathBuf {
        self.directory.join(format!("{id}.json"))
    }
}

fn approval_directory_available(work: &Path, directory: &Path, create: bool) -> Result<bool> {
    work_fs::require_plain_directory(work).map_err(|error| {
        if error.kind() == io::ErrorKind::InvalidData {
            ApprovalError::Corrupt("approval .work is redirected".into())
        } else {
            ApprovalError::Io(error)
        }
    })?;
    match fs::symlink_metadata(directory) {
        Ok(_) => work_fs::require_plain_directory(directory)
            .map(|()| true)
            .map_err(|error| {
                if error.kind() == io::ErrorKind::InvalidData {
                    ApprovalError::Corrupt(
                        "approval directory is redirected or non-directory".into(),
                    )
                } else {
                    ApprovalError::Io(error)
                }
            }),
        Err(error) if error.kind() == io::ErrorKind::NotFound && !create => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            work_fs::ensure_plain_directory(directory).map_err(|error| {
                if error.kind() == io::ErrorKind::InvalidData {
                    ApprovalError::Corrupt("created approval directory is not plain".into())
                } else {
                    ApprovalError::Io(error)
                }
            })?;
            Ok(true)
        }
        Err(error) => Err(error.into()),
    }
}

fn read_approval_text(path: &Path, initial_len: u64) -> Result<String> {
    if initial_len > MAX_APPROVAL_BYTES {
        return Err(ApprovalError::Corrupt(
            "approval artifact is invalid or oversized".into(),
        ));
    }
    let bytes = work_fs::read_plain_bytes(path, MAX_APPROVAL_BYTES).map_err(|error| {
        match work_fs::plain_read_violation(&error) {
            Some(work_fs::PlainReadViolation::GrewWhileReading { .. }) => {
                ApprovalError::Corrupt("approval artifact grew oversized".into())
            }
            Some(work_fs::PlainReadViolation::Oversize { .. }) => {
                ApprovalError::Corrupt("approval artifact is invalid or oversized".into())
            }
            Some(
                work_fs::PlainReadViolation::NotPlain { .. }
                | work_fs::PlainReadViolation::ParentNotPlain { .. },
            ) => ApprovalError::Corrupt("approval artifact was replaced while reading".into()),
            None if error.kind() == io::ErrorKind::InvalidData => {
                ApprovalError::Corrupt("approval artifact was replaced while reading".into())
            }
            None => ApprovalError::Io(error),
        }
    })?;
    String::from_utf8(bytes).map_err(|_| {
        ApprovalError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        ))
    })
}

#[derive(Serialize)]
struct PersistedApprovalManifest<'a, T: ?Sized> {
    schema: &'static str,
    approval_id: &'a str,
    fingerprint: &'a str,
    manifest: &'a T,
}

pub fn approval_id(subject: &str, reason: &str, fingerprint: &str, policy_hash: &str) -> String {
    let digest =
        Sha256::digest(format!("{subject}|{reason}|{fingerprint}|{policy_hash}").as_bytes());
    format!("apr-{}", hex(&digest[..16]))
}

/// Read the operator-owned machine/user pre-consent flag. It is intentionally not a project
/// configuration key: a repository agent cannot grant itself the broader authority. Unknown
/// spellings are an error rather than an accidental approval.
pub fn system_auto_approve() -> Result<bool> {
    parse_system_auto_approve(std::env::var("ORCHESTRA_AUTO_APPROVE").ok().as_deref())
}

pub fn parse_system_auto_approve(value: Option<&str>) -> Result<bool> {
    match value.map(str::trim) {
        None | Some("") | Some("off") => Ok(false),
        Some("on") => Ok(true),
        Some(value) => Err(ApprovalError::Invalid(format!(
            "ORCHESTRA_AUTO_APPROVE must be on or off, got {value:?}"
        ))),
    }
}

fn subject(request: &ApprovalRequest) -> Result<String> {
    let task = request.task_id.as_deref().unwrap_or_default();
    let batch = request.batch_id.as_deref().unwrap_or_default();
    if task.is_empty() && batch.is_empty() {
        return Err(ApprovalError::Invalid(
            "approval needs a task ID and/or a batch ID".into(),
        ));
    }
    if !task.is_empty() {
        validate_subject_id(task, "task ID")?;
    }
    if !batch.is_empty() {
        validate_subject_id(batch, "batch ID")?;
    }
    Ok(format!("task:{task}|batch:{batch}"))
}

fn validate_request(request: &ApprovalRequest) -> Result<()> {
    if request.deadline_secs == 0 {
        return Err(ApprovalError::Invalid(
            "approval deadline must be at least one second".into(),
        ));
    }
    validate_text(&request.reason, "approval reason")?;
    validate_identity(&request.fingerprint, "fingerprint")?;
    validate_identity(&request.policy_hash, "policy hash")
}

fn validate_record(record: &ApprovalRecord, expected_id: &str) -> Result<()> {
    if record.schema != APPROVAL_SCHEMA {
        return Err(ApprovalError::Corrupt(format!(
            "approval {expected_id:?} has unsupported schema {:?}",
            record.schema
        )));
    }
    if record.id != expected_id {
        return Err(ApprovalError::Corrupt(format!(
            "approval file {expected_id:?} contains mismatched id {:?}",
            record.id
        )));
    }
    validate_id(&record.id)?;
    validate_text(&record.reason, "approval reason")?;
    validate_identity(&record.fingerprint, "fingerprint")?;
    validate_identity(&record.policy_hash, "policy hash")?;
    if record.deadline_at_secs < record.created_at_secs {
        return Err(ApprovalError::Corrupt(format!(
            "approval {expected_id:?} has a deadline before creation"
        )));
    }
    if record.decision.is_some()
        && (record.decided_by.as_deref().is_none_or(str::is_empty)
            || record.decided_at_secs.is_none())
    {
        return Err(ApprovalError::Corrupt(format!(
            "approval {expected_id:?} has an incomplete decision"
        )));
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<()> {
    let Some(hex) = id.strip_prefix("apr-") else {
        return Err(ApprovalError::Invalid(format!(
            "invalid approval id {id:?}"
        )));
    };
    if hex.len() != 32 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApprovalError::Invalid(format!(
            "invalid approval id {id:?}"
        )));
    }
    Ok(())
}

fn validate_subject_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.contains(['\0', '\n', '\r', '|'])
        || value.starts_with('-')
        || value.contains("..")
    {
        return Err(ApprovalError::Invalid(format!("invalid {label} {value:?}")));
    }
    Ok(())
}

fn validate_text(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() || value.contains(['\0', '\n', '\r', '|']) {
        return Err(ApprovalError::Invalid(format!("invalid {label} {value:?}")));
    }
    Ok(())
}

fn validate_note(value: &str) -> Result<()> {
    if value.contains('\0') || value.len() > 4_096 {
        return Err(ApprovalError::Invalid("invalid approval note".into()));
    }
    Ok(())
}

fn validate_identity(value: &str, label: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApprovalError::Invalid(format!("invalid {label} {value:?}")));
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::thread;
    use std::time::SystemTime;

    use super::*;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[derive(serde::Serialize)]
    struct TestManifest {
        fingerprint: String,
        summary: String,
    }

    impl ContentBoundApprovalManifest for TestManifest {
        fn fingerprint(&self) -> String {
            self.fingerprint.clone()
        }
    }

    fn work() -> PathBuf {
        let work = std::env::temp_dir().join(format!(
            "orchestrail-approval-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&work).unwrap();
        work
    }

    /// Age a lock file's last-write stamp into the past, exactly as a holder that crashed that
    /// long ago leaves it behind. Staleness is derived from this stamp and nothing else, so this
    /// reproduces an orphaned lock without waiting out the real threshold.
    fn freeze_lock_age(path: &Path, age: Duration) {
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(fs::FileTimes::new().set_modified(SystemTime::now() - age))
            .unwrap();
    }

    fn request(now_secs: u64) -> ApprovalRequest {
        ApprovalRequest {
            task_id: None,
            batch_id: Some("B-1".into()),
            reason: "policy-bypass".into(),
            fingerprint: "a".repeat(64),
            policy_hash: "b".repeat(64),
            now_secs,
            deadline_secs: 60,
        }
    }

    #[test]
    fn request_is_idempotent_and_binds_all_authority_inputs() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let first = store.request(request(10)).unwrap();
        let resumed = store.request(request(20)).unwrap();
        assert_eq!(first, resumed);
        assert_eq!(
            store
                .status(&first.id, &"a".repeat(64), &"b".repeat(64), 11)
                .unwrap(),
            ApprovalStatus::Pending {
                id: first.id.clone(),
                deadline_at_secs: 70
            }
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_new_approval_retains_its_notification_marker_across_a_resume() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let created = store.request(request(10)).unwrap();
        let resumed = store.request(request(20)).unwrap();
        assert_eq!(created, resumed);
        assert!(created.notification_pending);
        store.clear_notification_pending(&created.id).unwrap();
        assert!(
            !store
                .load(&created.id)
                .unwrap()
                .unwrap()
                .notification_pending
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn approval_is_one_time_and_never_survives_stale_content_or_policy() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let record = store.request(request(10)).unwrap();
        store
            .decide(&record.id, ApprovalDecision::Approve, "operator", None, 20)
            .unwrap();
        assert!(
            store
                .status(&record.id, &"a".repeat(64), &"b".repeat(64), 21)
                .unwrap()
                .allows_progress()
        );
        assert!(matches!(
            store.status(&record.id, &"c".repeat(64), &"b".repeat(64), 21),
            Ok(ApprovalStatus::ExpiredStale { .. })
        ));
        assert!(matches!(
            store.decide(&record.id, ApprovalDecision::Reject, "operator", None, 22),
            Err(ApprovalError::AlreadyDecided { .. })
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn unanswered_request_expires_fail_closed() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let record = store.request(request(10)).unwrap();
        assert!(matches!(
            store.status(&record.id, &"a".repeat(64), &"b".repeat(71), 71),
            Err(ApprovalError::Invalid(_))
        ));
        assert!(matches!(
            store.status(&record.id, &"a".repeat(64), &"b".repeat(64), 71),
            Ok(ApprovalStatus::ExpiredTimeout { .. })
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn status_does_not_create_an_approval_directory() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let status = store
            .status(
                "apr-00000000000000000000000000000000",
                &"a".repeat(64),
                &"b".repeat(64),
                1,
            )
            .unwrap();
        assert!(matches!(status, ApprovalStatus::Missing { .. }));
        assert!(!work.join("approvals").exists());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn manifest_is_durable_and_must_match_the_approval_fingerprint() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let record = store.request(request(10)).unwrap();
        let manifest = TestManifest {
            fingerprint: record.fingerprint.clone(),
            summary: "the exact typed diff".into(),
        };
        store.save_manifest(&record.id, &manifest).unwrap();

        let path = store.manifest_path(&record.id).unwrap();
        let persisted: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(persisted["schema"], APPROVAL_MANIFEST_SCHEMA);
        assert_eq!(persisted["approval_id"], record.id);
        assert_eq!(persisted["fingerprint"], record.fingerprint);
        assert_eq!(persisted["manifest"]["summary"], manifest.summary);

        let mismatched = TestManifest {
            fingerprint: "c".repeat(64),
            summary: "different change set".into(),
        };
        assert!(matches!(
            store.save_manifest(&record.id, &mismatched),
            Err(ApprovalError::Invalid(_))
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn public_load_rejects_a_record_whose_id_disagrees_with_its_filename() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let record = store.request(request(10)).unwrap();
        let path = store.path(&record.id);
        let mut value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        value["id"] = serde_json::Value::String(format!("apr-{}", "f".repeat(32)));
        fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

        assert!(matches!(
            store.load(&record.id),
            Err(ApprovalError::Corrupt(_))
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn approval_read_maps_the_typed_oversize_violation() {
        let work = work();
        let path = work.join("oversized.json");
        fs::write(&path, vec![b' '; MAX_APPROVAL_BYTES as usize + 1]).unwrap();

        assert!(matches!(
            read_approval_text(&path, MAX_APPROVAL_BYTES),
            Err(ApprovalError::Corrupt(diagnostic))
                if diagnostic == "approval artifact is invalid or oversized"
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn system_auto_approval_is_strict_and_disabled_by_default() {
        assert!(!parse_system_auto_approve(None).unwrap());
        assert!(!parse_system_auto_approve(Some(" off ")).unwrap());
        assert!(parse_system_auto_approve(Some("on")).unwrap());
        assert!(parse_system_auto_approve(Some("true")).is_err());
    }

    #[test]
    fn approval_mutations_are_serialized_and_redirects_fail_closed() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let record = store.request(request(10)).unwrap();
        fs::write(store.directory().join(MUTATION_LOCK), "held\n").unwrap();
        // A freshly written foreign lock is live, so the bounded wait expires and the mutation is
        // refused. The wait is shortened here only so the test does not sit out the real timeout.
        assert!(matches!(
            store.with_mutation_lock_policy(
                Duration::from_millis(5),
                TRANSACTION_LOCK_STALE_AFTER,
                || store.load(&record.id)
            ),
            Err(ApprovalError::Busy { .. })
        ));
        fs::remove_file(store.directory().join(MUTATION_LOCK)).unwrap();

        let path = store.path(&record.id);
        fs::remove_file(&path).unwrap();
        let external = work.with_extension("external.json");
        fs::write(&external, "external sentinel\n").unwrap();
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&external, &path).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&external, &path).is_ok();
        if linked {
            assert!(matches!(
                store.load(&record.id),
                Err(ApprovalError::Corrupt(_))
            ));
            assert_eq!(
                fs::read_to_string(&external).unwrap(),
                "external sentinel\n"
            );
        }
        let _ = fs::remove_file(&external);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn an_orphaned_mutation_lock_is_recovered_and_the_mutation_completes() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        let record = store.request(request(10)).unwrap();

        // Precisely what a process killed between creating and removing the lock leaves behind:
        // the file is there, its holder is gone, and nobody will ever come back to remove it.
        let lock = store.directory().join(MUTATION_LOCK);
        fs::write(&lock, "crashed-holder").unwrap();
        freeze_lock_age(
            &lock,
            TRANSACTION_LOCK_STALE_AFTER + Duration::from_secs(60),
        );

        // Deliberately the production policy rather than a test-shortened one: recovery has to
        // hold for the real callers (`native_port`'s publication gate, the TUI's decide command).
        let decided = store
            .decide(&record.id, ApprovalDecision::Approve, "operator", None, 11)
            .expect("an orphaned lock must not wedge approval mutations forever");
        assert_eq!(decided.decision, Some(ApprovalDecision::Approve));
        assert!(
            store
                .status(&record.id, &"a".repeat(64), &"b".repeat(64), 12)
                .unwrap()
                .allows_progress()
        );
        assert!(
            !lock.exists(),
            "the recovered lock is released by its new owner, not leaked again"
        );

        // The remaining mutation entry points recover through the same lock, not just `decide`.
        fs::write(&lock, "crashed-holder-again").unwrap();
        freeze_lock_age(
            &lock,
            TRANSACTION_LOCK_STALE_AFTER + Duration::from_secs(60),
        );
        store.clear_notification_pending(&record.id).unwrap();
        assert!(
            !store
                .load(&record.id)
                .unwrap()
                .unwrap()
                .notification_pending
        );
        assert!(!lock.exists());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_live_mutation_lock_is_never_broken_early_and_refuses_with_diagnostics() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        store.request(request(10)).unwrap();
        let lock = store.directory().join(MUTATION_LOCK);
        fs::write(&lock, "live-peer").unwrap();

        let entered = AtomicUsize::new(0);
        let error = store
            .with_mutation_lock_policy(
                Duration::from_millis(20),
                TRANSACTION_LOCK_STALE_AFTER,
                || {
                    entered.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .unwrap_err();

        assert!(
            matches!(&error, ApprovalError::Busy { age_ms: Some(_), kind } if kind.contains("live-peer")),
            "a refusal must name the holder it observed: {error:?}"
        );
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("age="), "{diagnostic}");
        assert!(
            diagnostic.contains(&format!(
                "{}ms stale threshold",
                TRANSACTION_LOCK_STALE_AFTER.as_millis()
            )),
            "{diagnostic}"
        );
        assert_eq!(
            entered.load(Ordering::Relaxed),
            0,
            "a refused acquisition must never run the mutation"
        );
        assert_eq!(
            fs::read_to_string(&lock).unwrap(),
            "live-peer",
            "a holder short of the stale threshold is waited out, never broken"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn the_holder_token_check_still_refuses_a_lock_rewritten_under_us() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        store.request(request(10)).unwrap();
        let lock = store.directory().join(MUTATION_LOCK);

        fs::write(&lock, "someone-elses-token").unwrap();
        assert!(matches!(
            store.verify_mutation_lock_token(&lock, "our-token"),
            Err(ApprovalError::Corrupt(message)) if message.contains("ownership changed before use")
        ));
        fs::write(&lock, "our-token").unwrap();
        store
            .verify_mutation_lock_token(&lock, "our-token")
            .expect("our own token is what lets the critical section proceed");

        // An emptied or truncated lock is ownership evidence we no longer have, not an absence of
        // contention: it must fail closed exactly like foreign content does.
        fs::write(&lock, "").unwrap();
        assert!(matches!(
            store.verify_mutation_lock_token(&lock, "our-token"),
            Err(ApprovalError::Corrupt(_))
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_hijacked_mutation_lock_is_never_removed_by_its_previous_holder() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        store.request(request(10)).unwrap();
        let lock = store.directory().join(MUTATION_LOCK);

        let error = store
            .with_mutation_lock_policy(
                Duration::from_millis(20),
                TRANSACTION_LOCK_STALE_AFTER,
                || {
                    // An external writer takes the lock over in place while we hold it.
                    fs::write(&lock, "hijacker").unwrap();
                    Ok(())
                },
            )
            .unwrap_err();

        // The token check on entry calls this `Corrupt`; observing the very same substitution one
        // moment later, at release, may not quietly downgrade it to an I/O error.
        assert!(
            matches!(&error, ApprovalError::Corrupt(message)
                if message.contains("ownership changed before release")
                    && message.contains("hijacker")),
            "{error:?}"
        );
        assert_eq!(
            fs::read_to_string(&lock).unwrap(),
            "hijacker",
            "owner-conditional cleanup must leave a lock this holder no longer owns"
        );
        let _ = fs::remove_dir_all(work);
    }

    /// A hijack is evidence that a second decider may have entered an authorization mutation, so
    /// it outranks the mutation's own failure. Reporting only the operation error here would hide
    /// the more serious fact behind the less serious one.
    #[test]
    fn a_hijack_is_reported_even_when_the_mutation_itself_failed() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        store.request(request(10)).unwrap();
        let lock = store.directory().join(MUTATION_LOCK);

        let error = store
            .with_mutation_lock_policy(
                Duration::from_millis(20),
                TRANSACTION_LOCK_STALE_AFTER,
                || {
                    fs::write(&lock, "hijacker").unwrap();
                    Err::<(), _>(ApprovalError::Missing {
                        id: "apr-does-not-matter".into(),
                    })
                },
            )
            .unwrap_err();

        assert!(
            matches!(&error, ApprovalError::Corrupt(message)
                if message.contains("ownership changed before release")),
            "a concurrent operation failure must not suppress the hijack: {error:?}"
        );
        let _ = fs::remove_dir_all(work);
    }

    // Two threads contend on one orphaned lock through the same kernel-backed sidecar the
    // production cross-process path uses, so the mutual exclusion asserted here is the same
    // property independent processes get; only the scheduling is cheaper to drive in-process.
    #[test]
    fn contenders_recovering_one_orphaned_lock_never_overlap() {
        let work = work();
        let store = ApprovalStore::new(&work).unwrap();
        store.request(request(10)).unwrap();
        let lock = store.directory().join(MUTATION_LOCK);
        fs::write(&lock, "crashed-holder").unwrap();
        freeze_lock_age(
            &lock,
            TRANSACTION_LOCK_STALE_AFTER + Duration::from_secs(60),
        );

        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let start = Barrier::new(2);
        thread::scope(|scope| {
            let handles = (0..2)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        store
                            .with_mutation_lock_policy(
                                Duration::from_secs(10),
                                TRANSACTION_LOCK_STALE_AFTER,
                                || {
                                    let entrants = active.fetch_add(1, Ordering::SeqCst) + 1;
                                    peak.fetch_max(entrants, Ordering::SeqCst);
                                    thread::sleep(Duration::from_millis(20));
                                    assert_eq!(
                                        active.fetch_sub(1, Ordering::SeqCst),
                                        1,
                                        "approval critical sections overlapped"
                                    );
                                    Ok(())
                                },
                            )
                            .expect("both contenders must eventually get the recovered lock");
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().unwrap();
            }
        });

        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "breaking one stale lock must admit one contender, never two"
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(!lock.exists(), "the last holder released the lock");
        let _ = fs::remove_dir_all(work);
    }
}
