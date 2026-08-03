//! Native, interoperable owner lease for `.work/orchestrator.lock`.
//!
//! Orchestra's historical control plane stores an `orchestra/lease@1` JSON record in that
//! directory. Orchestrail preserves that record shape and recognizes the historical
//! `state-tx.lock` forms so it can inspect them and take over a stale lease after Orchestra has
//! stopped. A running PowerShell control plane does not participate in Orchestrail's kernel-lock
//! protocol, so concurrent lease mutation by the two control planes is not supported. A
//! persistent, kernel-locked lifecycle sidecar makes identity-check + create/rename/remove
//! sequences indivisible across Orchestrail processes. The sidecar is accepted only when the host
//! can prove that its storage is local with reliable kernel locking; network, unrecognized, and
//! known stackable Unix filesystems fail closed because their backing stores cannot be recursively
//! validated. Regular-file mutations retain the identity-authorized file handle until the final
//! pathname syscall returns. That closes the reducible file-ID-reuse interval between validation
//! and mutation. Rust's standard library has no cross-platform unlink/rename-by-handle operation,
//! so a nanosecond-scale final check-to-syscall race remains: under the supported local,
//! cooperative deployment model and standard filesystem caching, exploiting it would require an
//! external process to time unlink/recreate operations within the corresponding microsecond-scale
//! filesystem operation. This proportionate residual risk is accepted. This module does not invoke
//! the PowerShell script and never recursively removes a lock directory: a foreign/corrupt lock
//! remains an operator-visible refusal.

use std::env;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::supervise::CancellationProbe;
use crate::time::{epoch_to_iso, iso_to_epoch};
use crate::work_fs;

pub const ENGINE_ROLE: &str = "engine";
const LEASE_SCHEMA: &str = "orchestra/lease@1";
const LOCK_DIRECTORY: &str = "orchestrator.lock";
const LEASE_FILE: &str = "lease.json";
const STAGING_FILE: &str = "lease.json.tmp";
const TRANSACTION_LOCK: &str = "state-tx.lock";
const TRANSACTION_LIFECYCLE_LOCK: &str = ".state-tx.lifecycle.lock";
const MAX_LEASE_BYTES: u64 = 64 * 1024;
const TRANSACTION_LOCK_TIMEOUT: Duration = Duration::from_secs(30);
const TRANSACTION_LOCK_STALE_AFTER: Duration = Duration::from_secs(5 * 60);
const TRANSACTION_LOCK_RETRY: Duration = Duration::from_millis(50);
static TRANSACTION_OWNER_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_STALE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// The interoperable lease document written by both control planes. Numeric time enters the
/// native API explicitly and is rendered in the established UTC ISO form at the file boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub schema: String,
    pub role: String,
    pub owner_id: String,
    pub session_id: String,
    pub root: String,
    pub host: String,
    pub pid: u32,
    pub pid_started: Option<String>,
    pub acquired: String,
    pub heartbeat: String,
    pub ttl_seconds: u64,
    pub generation: u64,
    pub taken_over_from: Option<String>,
}

/// A safe liveness assessment. This native port deliberately uses the portable heartbeat proof;
/// a future process-start proof may only make a stale result earlier, never turn a stale record
/// into an automatic live takeover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseLiveness {
    pub live: bool,
    pub heartbeat_age_secs: u64,
    pub basis: &'static str,
}

/// Read-only current lease state. Corrupt and legacy locks are distinct from a vacant lease so
/// callers never silently overwrite an ownership record they cannot validate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    Vacant,
    Live {
        record: LeaseRecord,
        liveness: LeaseLiveness,
    },
    Stale {
        record: LeaseRecord,
        liveness: LeaseLiveness,
    },
    LegacyLock {
        detail: String,
    },
    Corrupt {
        detail: String,
    },
}

#[derive(Debug)]
pub enum LeaseError {
    Io(io::Error),
    Json(serde_json::Error),
    InvalidInput(String),
    Busy {
        age_ms: Option<u128>,
        kind: String,
    },
    HeldLive {
        owner: String,
        role: String,
    },
    Stale {
        owner: String,
    },
    NotOwner {
        owner: String,
    },
    AddressMismatch {
        expected_role: String,
        actual_role: String,
        expected_root: String,
        actual_root: String,
    },
    LegacyLock {
        detail: String,
    },
    Corrupt {
        detail: String,
    },
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "lease I/O error: {error}"),
            Self::Json(error) => write!(f, "lease JSON error: {error}"),
            Self::InvalidInput(message)
            | Self::LegacyLock { detail: message }
            | Self::Corrupt { detail: message } => f.write_str(message),
            Self::Busy { age_ms, kind } => match age_ms {
                Some(age_ms) => write!(
                    f,
                    "lease transaction lock is held ({kind}, age={age_ms}ms); retry after inspection or wait for the 300000ms stale-recovery threshold"
                ),
                None => write!(
                    f,
                    "lease transaction lock is held ({kind}, age unavailable); inspect .work/state-tx.lock before retrying"
                ),
            },
            Self::HeldLive { owner, role } => {
                write!(f, "lease is held live by owner={owner} role={role}")
            }
            Self::Stale { owner } => write!(
                f,
                "a stale lease is present for owner={owner}; explicit takeover is required"
            ),
            Self::NotOwner { owner } => write!(f, "lease belongs to owner={owner}"),
            Self::AddressMismatch {
                expected_role,
                actual_role,
                expected_root,
                actual_root,
            } => write!(
                f,
                "lease address mismatch: expected role={expected_role} root={expected_root}, found role={actual_role} root={actual_root}"
            ),
        }
    }
}

impl std::error::Error for LeaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidInput(_)
            | Self::Busy { .. }
            | Self::HeldLive { .. }
            | Self::Stale { .. }
            | Self::NotOwner { .. }
            | Self::AddressMismatch { .. }
            | Self::LegacyLock { .. }
            | Self::Corrupt { .. } => None,
        }
    }
}

impl From<io::Error> for LeaseError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for LeaseError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub type Result<T> = std::result::Result<T, LeaseError>;

/// Native owner-lease store rooted at one explicit `.work` directory.
#[derive(Debug, Clone)]
pub struct LeaseStore {
    work: PathBuf,
}

/// A native renewal worker for a live processor lease.
///
/// The worker uses the owner id plus the generation returned by [`LeaseStore::acquire`] as an
/// optimistic ownership proof for every renewal.  It does not attempt to take over, repair, or
/// release a lease: if another owner wins or the control plane becomes unavailable, the failure
/// is retained and exposed as a cancellation probe for the enclosing contained call. `stop`
/// wakes the worker immediately, so normal CLI cleanup never waits for the renewal interval.
#[derive(Debug)]
pub struct LeaseHeartbeat {
    stop: Option<mpsc::Sender<()>>,
    failure: Arc<Mutex<Option<String>>>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseHeartbeatError {
    Failed(String),
    WorkerPanicked,
}

impl fmt::Display for LeaseHeartbeatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(message) => f.write_str(message),
            Self::WorkerPanicked => f.write_str("native lease-heartbeat worker panicked"),
        }
    }
}

impl std::error::Error for LeaseHeartbeatError {}

impl LeaseHeartbeat {
    /// Start renewing `record` at most sixty seconds apart and at least three times within its
    /// TTL.  The lower one-second clamp avoids a busy loop for intentionally short test leases.
    pub fn start(store: LeaseStore, record: &LeaseRecord) -> Self {
        let interval = Duration::from_secs((record.ttl_seconds / 3).clamp(1, 60));
        Self::start_with_interval(store, record.owner_id.clone(), record.generation, interval)
    }

    fn start_with_interval(
        store: LeaseStore,
        owner: String,
        generation: u64,
        interval: Duration,
    ) -> Self {
        let (stop_tx, stop_rx) = mpsc::channel();
        let failure = Arc::new(Mutex::new(None));
        let failure_for_worker = Arc::clone(&failure);
        let worker = thread::spawn(move || {
            let mut generation = generation;
            loop {
                match stop_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                match store.heartbeat(&owner, Some(generation), system_now_secs()) {
                    Ok(record) => generation = record.generation,
                    Err(error) => {
                        if let Ok(mut failure) = failure_for_worker.lock() {
                            *failure = Some(format!("native lease heartbeat failed: {error}"));
                        }
                        break;
                    }
                }
            }
        });
        Self {
            stop: Some(stop_tx),
            failure,
            worker: Some(worker),
        }
    }

    /// Produce a read-only cancellation fact for supervised children. A failed owner/generation
    /// renewal means the process may no longer authorize a leaf result, so the child must be
    /// contained and stopped rather than allowed to spend its remaining deadline.
    pub fn cancellation_probe(&self) -> CancellationProbe {
        let failure = Arc::clone(&self.failure);
        CancellationProbe::new(move || {
            failure
                .lock()
                .map(|failure| failure.is_some())
                .unwrap_or(true)
        })
    }

    /// Stop the worker, wait for it, and surface any lost-ownership or persistence failure.
    /// This consumes the monitor so a caller cannot accidentally leave a detached renewal thread
    /// alive after releasing its lease.
    pub fn stop(mut self) -> std::result::Result<(), LeaseHeartbeatError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            return Err(LeaseHeartbeatError::WorkerPanicked);
        }
        match self.failure.lock() {
            Ok(failure) => match failure.as_ref() {
                Some(message) => Err(LeaseHeartbeatError::Failed(message.clone())),
                None => Ok(()),
            },
            Err(_) => Err(LeaseHeartbeatError::WorkerPanicked),
        }
    }
}

fn system_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl LeaseStore {
    pub fn new(work: impl Into<PathBuf>) -> Self {
        Self { work: work.into() }
    }

    pub fn work(&self) -> &Path {
        &self.work
    }

    pub fn lock_directory(&self) -> PathBuf {
        self.work.join(LOCK_DIRECTORY)
    }

    /// Inspect without creating `.work`, the lease directory, or a serialization lock.
    pub fn status(&self, now_secs: u64) -> Result<LeaseStatus> {
        self.read_status(now_secs)
    }

    /// Acquire only a vacant lease. A stale record is intentionally not adopted here: callers
    /// must make the takeover decision explicit and auditable.
    pub fn acquire(
        &self,
        owner: &str,
        root: &Path,
        ttl_seconds: u64,
        now_secs: u64,
    ) -> Result<LeaseRecord> {
        self.with_transaction(|| match self.read_status(now_secs)? {
            LeaseStatus::Vacant => {
                self.write_new_record(owner, root, ttl_seconds, now_secs, 1, None)
            }
            LeaseStatus::Live { record, .. } => Err(LeaseError::HeldLive {
                owner: record.owner_id,
                role: record.role,
            }),
            LeaseStatus::Stale { record, .. } => Err(LeaseError::Stale {
                owner: record.owner_id,
            }),
            LeaseStatus::LegacyLock { detail } => Err(LeaseError::LegacyLock { detail }),
            LeaseStatus::Corrupt { detail } => Err(LeaseError::Corrupt { detail }),
        })
    }

    /// Replace a demonstrably stale structured lease under the shared transaction lock. A record
    /// that becomes live while waiting is refused; no `force` operation exists in this API.
    pub fn takeover(
        &self,
        owner: &str,
        root: &Path,
        ttl_seconds: u64,
        now_secs: u64,
    ) -> Result<LeaseRecord> {
        self.with_transaction(|| match self.read_status(now_secs)? {
            LeaseStatus::Stale { record, .. } => self.write_new_record(
                owner,
                root,
                ttl_seconds,
                now_secs,
                record.generation.saturating_add(1),
                Some(record.owner_id),
            ),
            LeaseStatus::Vacant => {
                self.write_new_record(owner, root, ttl_seconds, now_secs, 1, None)
            }
            LeaseStatus::Live { record, .. } => Err(LeaseError::HeldLive {
                owner: record.owner_id,
                role: record.role,
            }),
            LeaseStatus::LegacyLock { detail } => Err(LeaseError::LegacyLock { detail }),
            LeaseStatus::Corrupt { detail } => Err(LeaseError::Corrupt { detail }),
        })
    }

    /// Atomically adopt a stale predecessor only when it remains addressed to the expected
    /// project root and role.  `processor --continue` must use this rather than observing the
    /// address and then calling [`Self::takeover`] separately: another writer could otherwise
    /// replace the stale record in that gap.  A vacant record is still a normal fresh acquisition.
    pub fn takeover_addressed(
        &self,
        owner: &str,
        root: &Path,
        expected_role: &str,
        ttl_seconds: u64,
        now_secs: u64,
    ) -> Result<LeaseRecord> {
        self.with_transaction(|| match self.read_status(now_secs)? {
            LeaseStatus::Stale { record, .. } => {
                if record.role != expected_role || !roots_equivalent(Path::new(&record.root), root)
                {
                    return Err(LeaseError::AddressMismatch {
                        expected_role: expected_role.into(),
                        actual_role: record.role,
                        expected_root: lexical_absolute_path(root)
                            .map(|path| path.to_string_lossy().into_owned())
                            .unwrap_or_else(|| root.to_string_lossy().into_owned()),
                        actual_root: record.root,
                    });
                }
                if record.owner_id == owner {
                    return Err(LeaseError::InvalidInput(
                        "addressed stale takeover needs a new owner distinct from the stale record"
                            .into(),
                    ));
                }
                self.write_new_record(
                    owner,
                    root,
                    ttl_seconds,
                    now_secs,
                    record.generation.saturating_add(1),
                    Some(record.owner_id),
                )
            }
            LeaseStatus::Vacant => {
                self.write_new_record(owner, root, ttl_seconds, now_secs, 1, None)
            }
            LeaseStatus::Live { record, .. } => Err(LeaseError::HeldLive {
                owner: record.owner_id,
                role: record.role,
            }),
            LeaseStatus::LegacyLock { detail } => Err(LeaseError::LegacyLock { detail }),
            LeaseStatus::Corrupt { detail } => Err(LeaseError::Corrupt { detail }),
        })
    }

    /// Renew exactly the caller's ownership record. `expected_generation` gives a caller an
    /// optional optimistic-CAS boundary on top of the directory transaction lock.
    pub fn heartbeat(
        &self,
        owner: &str,
        expected_generation: Option<u64>,
        now_secs: u64,
    ) -> Result<LeaseRecord> {
        validate_owner(owner)?;
        self.with_transaction(|| {
            let (LeaseStatus::Live { mut record, .. } | LeaseStatus::Stale { mut record, .. }) =
                self.read_status(now_secs)?
            else {
                return self.status_error_for_mutation(now_secs);
            };
            if record.owner_id != owner {
                return Err(LeaseError::NotOwner {
                    owner: record.owner_id,
                });
            }
            if expected_generation.is_some_and(|value| value != record.generation) {
                return Err(LeaseError::InvalidInput(format!(
                    "lease generation mismatch: expected {:?}, current {}",
                    expected_generation, record.generation
                )));
            }
            record.heartbeat = epoch_to_iso(now_secs);
            record.generation = record.generation.saturating_add(1);
            self.write_record(&record)?;
            Ok(record)
        })
    }

    /// Owner-checked release. The lock directory is removed only when it is empty after deleting
    /// the known lease file; foreign entries are never recursively deleted.
    pub fn release(&self, owner: &str, now_secs: u64) -> Result<bool> {
        validate_owner(owner)?;
        self.with_transaction(|| match self.read_status(now_secs)? {
            LeaseStatus::Vacant => Ok(false),
            LeaseStatus::Live { record, .. } | LeaseStatus::Stale { record, .. } => {
                if record.owner_id != owner {
                    return Err(LeaseError::NotOwner {
                        owner: record.owner_id,
                    });
                }
                fs::remove_file(self.lease_path())?;
                self.remove_empty_lock_directory()?;
                Ok(true)
            }
            LeaseStatus::LegacyLock { detail } => Err(LeaseError::LegacyLock { detail }),
            LeaseStatus::Corrupt { detail } => Err(LeaseError::Corrupt { detail }),
        })
    }

    fn write_new_record(
        &self,
        owner: &str,
        root: &Path,
        ttl_seconds: u64,
        now_secs: u64,
        generation: u64,
        taken_over_from: Option<String>,
    ) -> Result<LeaseRecord> {
        validate_owner(owner)?;
        if ttl_seconds == 0 {
            return Err(LeaseError::InvalidInput(
                "lease TTL must be at least one second".into(),
            ));
        }
        let root = absolute_root(root)?;
        let now = epoch_to_iso(now_secs);
        let record = LeaseRecord {
            schema: LEASE_SCHEMA.into(),
            role: ENGINE_ROLE.into(),
            owner_id: owner.into(),
            session_id: owner.into(),
            root,
            host: host_name(),
            // A portable heartbeat is the safe liveness proof. Writing a PID without an equally
            // portable creation-time proof would make a reused PID look live forever to legacy.
            pid: 0,
            pid_started: None,
            acquired: now.clone(),
            heartbeat: now,
            ttl_seconds,
            generation,
            taken_over_from,
        };
        self.write_record(&record)?;
        Ok(record)
    }

    fn status_error_for_mutation<T>(&self, now_secs: u64) -> Result<T> {
        match self.read_status(now_secs)? {
            LeaseStatus::Vacant => Err(LeaseError::InvalidInput(
                "no structured lease is present".into(),
            )),
            LeaseStatus::LegacyLock { detail } => Err(LeaseError::LegacyLock { detail }),
            LeaseStatus::Corrupt { detail } => Err(LeaseError::Corrupt { detail }),
            LeaseStatus::Live { record, .. } | LeaseStatus::Stale { record, .. } => {
                Err(LeaseError::NotOwner {
                    owner: record.owner_id,
                })
            }
        }
    }

    fn read_status(&self, now_secs: u64) -> Result<LeaseStatus> {
        match fs::symlink_metadata(&self.work) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(LeaseStatus::Vacant);
            }
            Err(error) => return Err(error.into()),
        }
        work_fs::require_plain_directory(&self.work)?;
        let lock = self.lock_directory();
        match fs::symlink_metadata(&lock) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(LeaseStatus::Vacant);
            }
            Err(error) => return Err(error.into()),
        }
        if work_fs::require_plain_directory(&lock).is_err() {
            return Ok(LeaseStatus::LegacyLock {
                detail: format!("lease path {} is not a plain directory", lock.display()),
            });
        }
        let lease = self.lease_path();
        match fs::symlink_metadata(&lease) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let entries = fs::read_dir(&lock)?
                    .filter_map(std::result::Result::ok)
                    .filter_map(|entry| {
                        let name = entry.file_name();
                        (name != STAGING_FILE && !work_fs::is_replace_temp_for(&lease, &name))
                            .then(|| name.to_string_lossy().into_owned())
                    })
                    .collect::<Vec<_>>();
                return if entries.is_empty() {
                    Ok(LeaseStatus::Vacant)
                } else {
                    Ok(LeaseStatus::LegacyLock {
                        detail: format!(
                            "lock directory contains non-lease entries without {LEASE_FILE}: {}",
                            entries.join(", ")
                        ),
                    })
                };
            }
            Err(error) => return Err(error.into()),
            Ok(metadata) => work_fs::require_plain_file(&lease, &metadata)?,
        }
        let raw = work_fs::read_plain_text(&lease, MAX_LEASE_BYTES)?;
        let record: LeaseRecord = match serde_json::from_str(&raw) {
            Ok(record) => record,
            Err(error) => {
                return Ok(LeaseStatus::Corrupt {
                    detail: format!("cannot parse {}: {error}", lease.display()),
                });
            }
        };
        if let Err(detail) = validate_record(&record) {
            return Ok(LeaseStatus::Corrupt { detail });
        }
        let heartbeat = iso_to_epoch(&record.heartbeat).ok_or_else(|| {
            LeaseError::InvalidInput(format!(
                "validated heartbeat no longer parses: {:?}",
                record.heartbeat
            ))
        })?;
        let age = now_secs.saturating_sub(heartbeat);
        // A future heartbeat (clock skew) is never considered stale; fail closed until a later
        // observation catches up rather than permitting a second writer on a skewed clock.
        let live = now_secs < heartbeat || age < record.ttl_seconds;
        let liveness = LeaseLiveness {
            live,
            heartbeat_age_secs: age,
            basis: "heartbeat",
        };
        Ok(if live {
            LeaseStatus::Live { record, liveness }
        } else {
            LeaseStatus::Stale { record, liveness }
        })
    }

    /// Replace the lease as one complete document. The shared storage primitive flushes the file
    /// before rename and the parent directory afterwards on Unix, so an acknowledged heartbeat is
    /// not lost solely because the directory entry was never made durable.
    fn write_record(&self, record: &LeaseRecord) -> Result<()> {
        let lock = self.lock_directory();
        work_fs::ensure_plain_parent(&self.work, &self.lease_path())?;
        work_fs::ensure_plain_directory(&lock)?;
        let lease = self.lease_path();
        let mut payload = serde_json::to_vec(record)?;
        payload.push(b'\n');
        work_fs::replace_file(&self.work, &lease, &payload, MAX_LEASE_BYTES)?;
        Ok(())
    }

    fn with_transaction<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        self.with_transaction_policy(
            TRANSACTION_LOCK_TIMEOUT,
            TRANSACTION_LOCK_STALE_AFTER,
            operation,
        )
    }

    fn with_transaction_policy<T>(
        &self,
        timeout: Duration,
        stale_after: Duration,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        match fs::symlink_metadata(&self.work) {
            Ok(_) => work_fs::require_plain_directory(&self.work)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                work_fs::ensure_plain_directory(&self.work)?;
            }
            Err(error) => return Err(error.into()),
        }
        let tx = self.work.join(TRANSACTION_LOCK);
        let mut guard = acquire_transaction_lock(&tx, timeout, stale_after)?;
        let result = operation();
        // A failure to remove our owner-checked CreateNew file is surfaced only when the operation
        // itself succeeded; otherwise preserve the primary error. Drop is a best-effort panic path.
        match guard.release() {
            Ok(()) => result,
            Err(error) if result.is_ok() => Err(error.into()),
            Err(_) => result,
        }
    }

    fn remove_empty_lock_directory(&self) -> Result<()> {
        let lock = self.lock_directory();
        match fs::symlink_metadata(&lock) {
            Ok(_) => work_fs::require_plain_directory(&lock)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        match fs::remove_dir(&lock) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn lease_path(&self) -> PathBuf {
        self.lock_directory().join(LEASE_FILE)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactionLockKind {
    File { owner: String },
    EmptyDirectory,
    NonEmptyDirectory,
    Redirected,
    Other,
}

impl TransactionLockKind {
    fn describe(&self) -> String {
        match self {
            Self::File { owner } => format!("CreateNew file owner={owner:?}"),
            Self::EmptyDirectory => "legacy native empty directory".into(),
            Self::NonEmptyDirectory => "non-empty directory (automatic removal refused)".into(),
            Self::Redirected => "symlink/reparse point (automatic removal refused)".into(),
            Self::Other => "unsupported filesystem entry (automatic removal refused)".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransactionLockSnapshot {
    kind: TransactionLockKind,
    stamp: Option<SystemTime>,
    age_ms: Option<u128>,
    identity: TransactionFilesystemIdentity,
}

impl TransactionLockSnapshot {
    fn same_identity_as(&self, other: &Self) -> bool {
        self.identity == other.identity && self.kind == other.kind && self.stamp == other.stamp
    }
}

/// Cross-process serialization for the complete `state-tx.lock` lifecycle.
///
/// The sidecar is persistent: Orchestrail never renames or removes it, so acquiring its operating
/// system file lock cannot recurse into another stale-lock protocol. Every CreateNew, final stale
/// identity check plus rename, and owner check plus removal requires this guard. `flock` and
/// `LockFileEx` are released by the kernel when a process exits, which gives the whole lifecycle
/// one filesystem-backed critical section shared by threads and independent processes. The open
/// sidecar identity is revalidated before every mutation. The actual `state-tx.lock` identity is
/// separately revalidated at each pathname syscall boundary because the lifecycle guard only
/// serializes cooperating Orchestrail processes and cannot prevent an external unlink/recreate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransactionFilesystemIdentity {
    filesystem: u64,
    file: u64,
}

struct TransactionLifecycleGuard {
    file: fs::File,
    path: PathBuf,
    identity: TransactionFilesystemIdentity,
}

impl TransactionLifecycleGuard {
    fn validate(&self) -> io::Result<()> {
        validate_transaction_lifecycle_identity(&self.file, &self.path, self.identity)
    }
}

fn try_acquire_transaction_lifecycle(
    transaction_path: &Path,
) -> io::Result<Option<TransactionLifecycleGuard>> {
    let parent = transaction_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "state transaction lock has no parent: {}",
                transaction_path.display()
            ),
        )
    })?;
    let lifecycle_path = parent.join(TRANSACTION_LIFECYCLE_LOCK);
    let file = work_fs::open_plain_file_read_write(&lifecycle_path, true)?;
    validate_filesystem_supports_kernel_locking(&lifecycle_path)?;
    let Some(identity) = try_lock_transaction_lifecycle_file(&file, &lifecycle_path)? else {
        return Ok(None);
    };
    Ok(Some(TransactionLifecycleGuard {
        file,
        path: lifecycle_path,
        identity,
    }))
}

fn try_lock_transaction_lifecycle_file(
    file: &fs::File,
    path: &Path,
) -> io::Result<Option<TransactionFilesystemIdentity>> {
    if !try_lock_transaction_lifecycle_file_os(file)? {
        return Ok(None);
    }

    let identity = transaction_lifecycle_identity(file)?;
    validate_transaction_lifecycle_identity(file, path, identity)?;
    Ok(Some(identity))
}

#[cfg(unix)]
fn try_lock_transaction_lifecycle_file_os(file: &fs::File) -> io::Result<bool> {
    use std::os::fd::AsRawFd;

    // SAFETY: `file` owns a live descriptor for the duration of this call. `flock` neither takes
    // ownership of it nor dereferences any Rust memory.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error
        .raw_os_error()
        .is_some_and(|code| code == libc::EAGAIN || code == libc::EWOULDBLOCK)
    {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(windows)]
fn try_lock_transaction_lifecycle_file_os(file: &fs::File) -> io::Result<bool> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    let mut overlapped = OVERLAPPED::default();
    // SAFETY: the `File` keeps its handle live, `overlapped` is a valid writable structure for the
    // synchronous call, and LockFileEx borrows rather than owns both values.
    let locked = unsafe {
        LockFileEx(
            file.as_raw_handle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    if locked != 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(not(any(unix, windows)))]
fn try_lock_transaction_lifecycle_file_os(_file: &fs::File) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cross-process transaction lifecycle locks are unavailable on this platform",
    ))
}

fn lifecycle_identity_error(path: &Path, reason: impl fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!(
            "state transaction lifecycle lock identity is no longer valid at {}: {reason}; \
             refusing control-plane mutation",
            path.display()
        ),
    )
}

#[cfg(unix)]
fn transaction_lifecycle_identity(file: &fs::File) -> io::Result<TransactionFilesystemIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(TransactionFilesystemIdentity {
        filesystem: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(unix)]
fn validate_transaction_lifecycle_identity(
    file: &fs::File,
    path: &Path,
    expected: TransactionFilesystemIdentity,
) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let opened = file.metadata()?;
    if opened.nlink() == 0 {
        return Err(lifecycle_identity_error(
            path,
            "the locked file descriptor has no directory link",
        ));
    }
    let current =
        fs::symlink_metadata(path).map_err(|error| lifecycle_identity_error(path, error))?;
    work_fs::require_plain_file(path, &current)
        .map_err(|error| lifecycle_identity_error(path, error))?;

    let opened_identity = TransactionFilesystemIdentity {
        filesystem: opened.dev(),
        file: opened.ino(),
    };
    let current_identity = TransactionFilesystemIdentity {
        filesystem: current.dev(),
        file: current.ino(),
    };
    if opened_identity != expected {
        return Err(lifecycle_identity_error(
            path,
            "the open file descriptor changed identity",
        ));
    }
    if current.nlink() == 0 || current_identity != opened_identity {
        return Err(lifecycle_identity_error(
            path,
            "the current path names a different file",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn windows_transaction_file_identity(
    file: &fs::File,
) -> io::Result<(TransactionFilesystemIdentity, u64)> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut information = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: `file` owns a live handle and `information` points to writable storage for the
    // complete BY_HANDLE_FILE_INFORMATION result.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), information.as_mut_ptr()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful GetFileInformationByHandle call initialized the result structure.
    let information = unsafe { information.assume_init() };
    Ok((
        TransactionFilesystemIdentity {
            filesystem: u64::from(information.dwVolumeSerialNumber),
            file: (u64::from(information.nFileIndexHigh) << 32)
                | u64::from(information.nFileIndexLow),
        },
        u64::from(information.nNumberOfLinks),
    ))
}

#[cfg(windows)]
fn transaction_lifecycle_identity(file: &fs::File) -> io::Result<TransactionFilesystemIdentity> {
    windows_transaction_file_identity(file).map(|(identity, _)| identity)
}

#[cfg(windows)]
fn validate_transaction_lifecycle_identity(
    file: &fs::File,
    path: &Path,
    expected: TransactionFilesystemIdentity,
) -> io::Result<()> {
    let (opened_identity, opened_links) = windows_transaction_file_identity(file)
        .map_err(|error| lifecycle_identity_error(path, error))?;
    if opened_links == 0 {
        return Err(lifecycle_identity_error(
            path,
            "the locked file handle has no directory link",
        ));
    }
    let current = work_fs::open_existing_plain_file(path)
        .map_err(|error| lifecycle_identity_error(path, error))?;
    let (current_identity, current_links) = windows_transaction_file_identity(&current)
        .map_err(|error| lifecycle_identity_error(path, error))?;
    if opened_identity != expected {
        return Err(lifecycle_identity_error(
            path,
            "the open file handle changed identity",
        ));
    }
    if current_links == 0 || current_identity != opened_identity {
        return Err(lifecycle_identity_error(
            path,
            "the current path names a different file",
        ));
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn transaction_lifecycle_identity(_file: &fs::File) -> io::Result<TransactionFilesystemIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "transaction lifecycle file identity is unavailable on this platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn validate_transaction_lifecycle_identity(
    _file: &fs::File,
    _path: &Path,
    _expected: TransactionFilesystemIdentity,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "transaction lifecycle file identity is unavailable on this platform",
    ))
}

fn transaction_target_identity_error(path: &Path, reason: impl fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!(
            "state transaction lock identity is no longer valid at {}: {reason}; refusing mutation",
            path.display()
        ),
    )
}

#[cfg(unix)]
fn transaction_target_identity_from_metadata(
    _path: &Path,
    metadata: &fs::Metadata,
) -> io::Result<TransactionFilesystemIdentity> {
    use std::os::unix::fs::MetadataExt;

    Ok(TransactionFilesystemIdentity {
        filesystem: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(windows)]
fn transaction_target_identity_from_metadata(
    path: &Path,
    _metadata: &fs::Metadata,
) -> io::Result<TransactionFilesystemIdentity> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;

    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    windows_transaction_file_identity(&file).map(|(identity, _)| identity)
}

#[cfg(not(any(unix, windows)))]
fn transaction_target_identity_from_metadata(
    path: &Path,
    _metadata: &fs::Metadata,
) -> io::Result<TransactionFilesystemIdentity> {
    Err(transaction_target_identity_error(
        path,
        "filesystem identity is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn opened_transaction_target_identity(
    file: &fs::File,
) -> io::Result<(TransactionFilesystemIdentity, u64)> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok((
        TransactionFilesystemIdentity {
            filesystem: metadata.dev(),
            file: metadata.ino(),
        },
        metadata.nlink(),
    ))
}

#[cfg(windows)]
fn opened_transaction_target_identity(
    file: &fs::File,
) -> io::Result<(TransactionFilesystemIdentity, u64)> {
    windows_transaction_file_identity(file)
}

#[cfg(not(any(unix, windows)))]
fn opened_transaction_target_identity(
    _file: &fs::File,
) -> io::Result<(TransactionFilesystemIdentity, u64)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem identity is unavailable on this platform",
    ))
}

fn validate_transaction_target_identity(
    path: &Path,
    expected: TransactionFilesystemIdentity,
) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if work_fs::redirected(&metadata) {
        return Err(transaction_target_identity_error(
            path,
            "the current path is redirected",
        ));
    }
    let current = transaction_target_identity_from_metadata(path, &metadata)?;
    if current != expected {
        return Err(transaction_target_identity_error(
            path,
            "the current path names a different filesystem entry",
        ));
    }
    Ok(())
}

fn validate_open_transaction_target_identity(
    file: &fs::File,
    path: &Path,
    expected: TransactionFilesystemIdentity,
) -> io::Result<()> {
    let (opened, links) = opened_transaction_target_identity(file)
        .map_err(|error| transaction_target_identity_error(path, error))?;
    if links == 0 {
        return Err(transaction_target_identity_error(
            path,
            "the open file has no directory link",
        ));
    }
    if opened != expected {
        return Err(transaction_target_identity_error(
            path,
            "the open file changed identity",
        ));
    }
    validate_transaction_target_identity(path, expected)
}

fn unsupported_locking_filesystem(path: &Path, reason: impl fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "lease lock requires local filesystem with reliable kernel locking; NFS and SMB are \
             not supported ({}: {reason})",
            path.display()
        ),
    )
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn validate_linux_filesystem_type(path: &Path, filesystem_type: u64) -> io::Result<()> {
    const LOCAL_FILESYSTEMS: &[u64] = &[
        0x0000_3434, // NILFS
        0x0000_4D44, // FAT/MS-DOS
        0x0000_72B6, // JFFS2
        0x0000_EF53, // ext2/ext3/ext4
        0x0102_1994, // tmpfs
        0x1501_3346, // UDF
        0x2011_BAB0, // exFAT
        0x2405_1905, // UBIFS
        0x2FC1_2FC1, // ZFS
        0x3153_464A, // JFS
        0x5265_4973, // ReiserFS
        0x5265_4974, // ReiserFS v2
        0x5265_4975, // ReiserFS v3
        0x5346_544E, // NTFS
        0x5846_5342, // XFS
        0x6175_6673, // AUFS
        0x794C_7630, // overlayfs
        0x8584_58F6, // ramfs
        0x9123_683E, // Btrfs
        0xCA45_1A4E, // bcachefs
        0xF2F5_2010, // F2FS
    ];
    const NETWORK_OR_UNVERIFIABLE_FILESYSTEMS: &[u64] = &[
        0x0000_517B, // SMB
        0x0000_564C, // NCP
        0x0000_6969, // NFS
        0x00C3_6400, // Ceph
        0x0102_1997, // 9P, including network-backed virtio mounts
        0x0116_1970, // GFS2
        0x0BD0_0BD0, // Lustre
        0x4750_4653, // GPFS
        0x5346_414F, // AFS
        0x6573_5546, // FUSE (backing store cannot be established)
        0x7375_7245, // Coda
        0x7461_636F, // OCFS2
        0xAAD7_AAEA, // PanFS
        0xFF53_4D42, // CIFS
    ];

    let filesystem_type = filesystem_type & u64::from(u32::MAX);
    if LOCAL_FILESYSTEMS.contains(&filesystem_type) {
        return Ok(());
    }
    let classification = if NETWORK_OR_UNVERIFIABLE_FILESYSTEMS.contains(&filesystem_type) {
        "detected a network or distributed filesystem"
    } else {
        "filesystem type is not in the local-filesystem allowlist"
    };
    Err(unsupported_locking_filesystem(
        path,
        format_args!("{classification}: magic 0x{filesystem_type:08x}"),
    ))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn validate_filesystem_supports_kernel_locking(path: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let encoded = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| unsupported_locking_filesystem(path, "path contains an embedded NUL byte"))?;
    let mut statistics = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `encoded` is NUL-terminated and `statistics` points to writable storage for statfs.
    if unsafe { libc::statfs(encoded.as_ptr(), statistics.as_mut_ptr()) } != 0 {
        let error = io::Error::last_os_error();
        return Err(unsupported_locking_filesystem(
            path,
            format_args!("filesystem type lookup failed: {error}"),
        ));
    }
    // SAFETY: a successful statfs call initialized the entire output structure.
    let statistics = unsafe { statistics.assume_init() };
    validate_linux_filesystem_type(path, statistics.f_type as u64)
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn validate_filesystem_supports_kernel_locking(path: &Path) -> io::Result<()> {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;

    let encoded = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| unsupported_locking_filesystem(path, "path contains an embedded NUL byte"))?;
    let mut statistics = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `encoded` is NUL-terminated and `statistics` points to writable storage for statfs.
    if unsafe { libc::statfs(encoded.as_ptr(), statistics.as_mut_ptr()) } != 0 {
        let error = io::Error::last_os_error();
        return Err(unsupported_locking_filesystem(
            path,
            format_args!("filesystem type lookup failed: {error}"),
        ));
    }
    // SAFETY: statfs succeeded and its fixed-size filesystem-name field is NUL-terminated.
    let statistics = unsafe { statistics.assume_init() };
    let filesystem = unsafe { CStr::from_ptr(statistics.f_fstypename.as_ptr()) }
        .to_string_lossy()
        .to_ascii_lowercase();
    validate_named_unix_filesystem_type(path, &filesystem)
}

#[cfg(any(
    test,
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn validate_named_unix_filesystem_type(path: &Path, filesystem: &str) -> io::Result<()> {
    match filesystem {
        "apfs" | "exfat" | "ext2fs" | "ffs" | "hammer" | "hammer2" | "hfs" | "lfs" | "msdos"
        | "ntfs" | "tmpfs" | "ufs" | "zfs" => Ok(()),
        // Fail closed: nullfs and unionfs are stackable, and this code cannot recursively prove
        // that their backing stores provide local, reliable kernel-lock semantics.
        "nullfs" | "unionfs" => Err(unsupported_locking_filesystem(
            path,
            format_args!(
                "filesystem type {filesystem:?} is not supported because its stackable backing \
                 store cannot be validated"
            ),
        )),
        _ => Err(unsupported_locking_filesystem(
            path,
            format_args!("filesystem type {filesystem:?} is not in the local-filesystem allowlist"),
        )),
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))
))]
fn validate_filesystem_supports_kernel_locking(path: &Path) -> io::Result<()> {
    Err(unsupported_locking_filesystem(
        path,
        "filesystem type detection is unavailable on this Unix target",
    ))
}

#[cfg(windows)]
fn windows_path_is_unc(path: &Path) -> bool {
    use std::path::Prefix;

    matches!(
        path.components().next(),
        Some(Component::Prefix(prefix))
            if matches!(prefix.kind(), Prefix::UNC(..) | Prefix::VerbatimUNC(..))
    )
}

#[cfg(windows)]
fn validate_filesystem_supports_kernel_locking(path: &Path) -> io::Result<()> {
    use std::iter;
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, GetVolumePathNameW};

    if windows_path_is_unc(path) {
        return Err(unsupported_locking_filesystem(path, "UNC path detected"));
    }
    let absolute = fs::canonicalize(path).map_err(|error| {
        unsupported_locking_filesystem(path, format_args!("canonical path lookup failed: {error}"))
    })?;
    if windows_path_is_unc(&absolute) {
        return Err(unsupported_locking_filesystem(
            path,
            "canonical path resolves to a UNC share",
        ));
    }
    let wide_path: Vec<u16> = absolute
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();
    let mut volume_path = vec![0_u16; 32_768];
    // SAFETY: both vectors are live, NUL-terminated/writable as required, and the output length
    // describes the complete allocated buffer.
    if unsafe {
        GetVolumePathNameW(
            wide_path.as_ptr(),
            volume_path.as_mut_ptr(),
            volume_path.len() as u32,
        )
    } == 0
    {
        let error = io::Error::last_os_error();
        return Err(unsupported_locking_filesystem(
            path,
            format_args!("Windows volume lookup failed: {error}"),
        ));
    }
    // SAFETY: GetVolumePathNameW succeeded and wrote a NUL-terminated root path to the buffer.
    let drive_type = unsafe { GetDriveTypeW(volume_path.as_ptr()) };
    validate_windows_drive_type(path, drive_type)
}

#[cfg(windows)]
fn validate_windows_drive_type(path: &Path, drive_type: u32) -> io::Result<()> {
    const DRIVE_UNKNOWN: u32 = 0;
    const DRIVE_NO_ROOT_DIR: u32 = 1;
    const DRIVE_REMOTE: u32 = 4;

    match drive_type {
        DRIVE_REMOTE => Err(unsupported_locking_filesystem(
            path,
            "Windows reports a remote or mapped network drive",
        )),
        DRIVE_UNKNOWN | DRIVE_NO_ROOT_DIR => Err(unsupported_locking_filesystem(
            path,
            "Windows could not classify the containing volume",
        )),
        _ => Ok(()),
    }
}

#[cfg(not(any(unix, windows)))]
fn validate_filesystem_supports_kernel_locking(path: &Path) -> io::Result<()> {
    Err(unsupported_locking_filesystem(
        path,
        "filesystem type detection is unavailable on this platform",
    ))
}

struct TransactionLockGuard {
    path: PathBuf,
    owner: String,
    armed: bool,
    lifecycle: Option<TransactionLifecycleGuard>,
}

impl TransactionLockGuard {
    fn release(&mut self) -> io::Result<()> {
        if !self.armed {
            return Ok(());
        }
        let lifecycle = self.lifecycle.as_ref().ok_or_else(|| {
            io::Error::other("armed state transaction lock has no lifecycle guard")
        })?;
        lifecycle.validate()?;
        let snapshot = transaction_lock_snapshot(&self.path)?;
        let expected_identity = snapshot.identity;
        match snapshot.kind {
            TransactionLockKind::File { owner } if owner == self.owner => {
                remove_owned_transaction_lock(&self.path, expected_identity, lifecycle)?;
                self.armed = false;
                self.lifecycle.take();
                Ok(())
            }
            kind => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "refusing to release replaced state transaction lock: {}",
                    kind.describe()
                ),
            )),
        }
    }
}

impl Drop for TransactionLockGuard {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

fn acquire_transaction_lock(
    path: &Path,
    timeout: Duration,
    stale_after: Duration,
) -> Result<TransactionLockGuard> {
    let owner = format!(
        "{}:{}",
        process::id(),
        TRANSACTION_OWNER_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let deadline = Instant::now() + timeout;
    loop {
        let lifecycle = match try_acquire_transaction_lifecycle(path)? {
            Some(lifecycle) => lifecycle,
            None => {
                if Instant::now() >= deadline {
                    match transaction_lock_snapshot(path) {
                        Ok(snapshot) => {
                            return Err(LeaseError::Busy {
                                age_ms: snapshot.age_ms,
                                kind: snapshot.kind.describe(),
                            });
                        }
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {
                            // The releasing owner removes the canonical entry before dropping the
                            // lifecycle guard. Report the still-held cross-process boundary rather
                            // than misclassifying that bounded hand-off as an I/O failure.
                            return Err(LeaseError::Busy {
                                age_ms: None,
                                kind: "cross-process lifecycle transition".into(),
                            });
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                thread::sleep(
                    TRANSACTION_LOCK_RETRY.min(deadline.saturating_duration_since(Instant::now())),
                );
                continue;
            }
        };
        if create_transaction_lock(path, &owner, &lifecycle)? {
            return Ok(TransactionLockGuard {
                path: path.to_path_buf(),
                owner,
                armed: true,
                lifecycle: Some(lifecycle),
            });
        }
        let snapshot = match transaction_lock_snapshot(path) {
            Ok(snapshot) => snapshot,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if create_transaction_lock(path, &owner, &lifecycle)? {
                    return Ok(TransactionLockGuard {
                        path: path.to_path_buf(),
                        owner,
                        armed: true,
                        lifecycle: Some(lifecycle),
                    });
                }
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        if break_stale_transaction_lock(path, &snapshot, stale_after, &lifecycle)? {
            continue;
        }
        if Instant::now() >= deadline {
            return Err(LeaseError::Busy {
                age_ms: snapshot.age_ms,
                kind: snapshot.kind.describe(),
            });
        }
        thread::sleep(
            TRANSACTION_LOCK_RETRY.min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn create_transaction_lock(
    path: &Path,
    owner: &str,
    lifecycle: &TransactionLifecycleGuard,
) -> io::Result<bool> {
    lifecycle.validate()?;
    let mut file = match work_fs::create_new_plain_file(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(false),
        Err(_error) if fs::symlink_metadata(path).is_ok() => {
            // Windows reports AccessDenied rather than AlreadyExists when CreateNew targets the
            // directory lock left by an older native build. An entry proven to exist is still
            // contention; any absent-path/open failure retains its original diagnostic.
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    let (created_identity, created_links) = opened_transaction_target_identity(&file)
        .map_err(|error| transaction_target_identity_error(path, error))?;
    if created_links == 0 {
        return Err(transaction_target_identity_error(
            path,
            "the newly created file has no directory link",
        ));
    }
    // CreateNew returns a handle to the entry it created. Keep that handle and prove the pathname
    // still names the same volume/file-index before trusting the lifecycle validation or contents.
    validate_open_transaction_target_identity(&file, path, created_identity)?;
    lifecycle.validate()?;
    if let Err(error) = (|| -> io::Result<()> {
        file.write_all(owner.as_bytes())?;
        file.sync_all()
    })() {
        lifecycle.validate()?;
        validate_open_transaction_target_identity(&file, path, created_identity)?;
        let _ = fs::remove_file(path);
        return Err(error);
    }
    // An external process is not bound by the lifecycle sidecar and may unlink/recreate the
    // pathname. Revalidate the open file against that path immediately before the final guard
    // validation so a replacement can never be accepted as the lock we just initialized.
    validate_open_transaction_target_identity(&file, path, created_identity)?;
    lifecycle.validate()?;
    // `file` intentionally stays in scope until this return, retaining the created filesystem
    // object throughout initialization even if an external process removes its pathname.
    Ok(true)
}

fn remove_owned_transaction_lock(
    path: &Path,
    expected: TransactionFilesystemIdentity,
    lifecycle: &TransactionLifecycleGuard,
) -> io::Result<()> {
    // The ownership read in `release` and this removal are one logical filesystem transaction:
    // every code path capable of renaming, creating, or removing the canonical name needs the
    // same cross-process lifecycle guard. The mutation helper borrows the authorized handle into
    // the remove callback and drops it only after the syscall returns. This closes the reducible
    // unlink/recreate + file-ID-reuse gap that would exist if the handle were closed first.
    //
    // Rust std has no cross-platform conditional unlink-by-identity operation, so a non-cooperating
    // external process could still replace the pathname in the nanosecond-scale interval between
    // the helper's final same-handle check and this path syscall. Requiring microsecond-level timing
    // on a local filesystem makes that residual race proportionate for cooperative deployments.
    mutate_transaction_file_if_unchanged(path, expected, lifecycle, |_authorized| {
        fs::remove_file(path)
    })
}

fn mutate_transaction_file_if_unchanged<T>(
    path: &Path,
    expected: TransactionFilesystemIdentity,
    lifecycle: &TransactionLifecycleGuard,
    mutation: impl FnOnce(&fs::File) -> io::Result<T>,
) -> io::Result<T> {
    lifecycle.validate()?;
    let authorized = work_fs::open_existing_plain_file(path)?;
    validate_open_transaction_target_identity(&authorized, path, expected)?;

    // Recheck the lifecycle after opening, then make the same authorized handle the final source
    // of truth immediately before mutation. Passing it into the callback keeps the OS reference
    // alive throughout the path-based syscall, preventing its file ID from being recycled first.
    lifecycle.validate()?;
    validate_open_transaction_target_identity(&authorized, path, expected)?;
    let result = mutation(&authorized);
    drop(authorized);
    result
}

fn transaction_lock_snapshot(path: &Path) -> io::Result<TransactionLockSnapshot> {
    let metadata = fs::symlink_metadata(path)?;
    let identity = transaction_target_identity_from_metadata(path, &metadata)?;
    // NTFS file-name tunneling may copy the previous occupant's creation time to a newly created
    // lock at the same path. Last-write time belongs to the new contents and is the only safe age
    // source for stale-lock recovery.
    let stamp = metadata.modified().ok();
    let age_ms = stamp
        .and_then(|stamp| SystemTime::now().duration_since(stamp).ok())
        .map(|age| age.as_millis());
    let kind = if work_fs::redirected(&metadata) {
        TransactionLockKind::Redirected
    } else if metadata.is_file() {
        let owner = work_fs::read_plain_text(path, MAX_LEASE_BYTES)?;
        TransactionLockKind::File {
            owner: owner.trim().chars().take(128).collect(),
        }
    } else if metadata.is_dir() {
        work_fs::require_plain_directory(path)?;
        if fs::read_dir(path)?.next().transpose()?.is_none() {
            TransactionLockKind::EmptyDirectory
        } else {
            TransactionLockKind::NonEmptyDirectory
        }
    } else {
        TransactionLockKind::Other
    };
    Ok(TransactionLockSnapshot {
        kind,
        stamp,
        age_ms,
        identity,
    })
}

fn rename_transaction_lock_if_unchanged(
    path: &Path,
    stale_path: &Path,
    expected: TransactionFilesystemIdentity,
    lifecycle: &TransactionLifecycleGuard,
) -> io::Result<()> {
    lifecycle.validate()?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_file() && !work_fs::redirected(&metadata) {
        // Keep the regular-file source handle alive through rename for the same reason as remove:
        // otherwise closing it after authorization would permit file-ID reuse before path lookup.
        return mutate_transaction_file_if_unchanged(path, expected, lifecycle, |_authorized| {
            fs::rename(path, stale_path)
        });
    }

    // The only supported non-file source is the legacy empty-directory lock. The cross-platform
    // plain-file opener cannot hold a directory handle, so do its final pathname identity check as
    // close as possible to rename. The lifecycle sidecar excludes cooperating Orchestrail peers;
    // the same proportionate nanosecond-scale race against an external process remains here.
    validate_transaction_target_identity(path, expected)?;
    fs::rename(path, stale_path)
}

fn break_stale_transaction_lock(
    path: &Path,
    decided: &TransactionLockSnapshot,
    stale_after: Duration,
    lifecycle: &TransactionLifecycleGuard,
) -> io::Result<bool> {
    if decided
        .age_ms
        .is_none_or(|age_ms| age_ms <= stale_after.as_millis())
    {
        return Ok(false);
    }
    lifecycle.validate()?;
    // The lifecycle guard serializes this final content/metadata check with every Orchestrail
    // create and release. An unrelated process does not honor that boundary, so the confirmed
    // filesystem identity is carried to the final source-path check immediately before rename.
    let confirmed = match transaction_lock_snapshot(path) {
        Ok(snapshot) => snapshot,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    if !decided.same_identity_as(&confirmed)
        || confirmed
            .age_ms
            .is_none_or(|age_ms| age_ms <= stale_after.as_millis())
    {
        return Ok(false);
    }

    match confirmed.kind {
        TransactionLockKind::File { .. } | TransactionLockKind::EmptyDirectory => {}
        TransactionLockKind::NonEmptyDirectory
        | TransactionLockKind::Redirected
        | TransactionLockKind::Other => return Ok(false),
    }

    let mut stale_path = path.as_os_str().to_os_string();
    stale_path.push(format!(
        ".{}.{}.stale",
        process::id(),
        TRANSACTION_STALE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let stale_path = PathBuf::from(stale_path);
    match rename_transaction_lock_if_unchanged(path, &stale_path, confirmed.identity, lifecycle) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::NotFound
            ) =>
        {
            return Ok(true);
        }
        Err(error) => return Err(error),
    }

    validate_transaction_target_identity(&stale_path, confirmed.identity)?;
    let quarantined = transaction_lock_snapshot(&stale_path)?;
    if !confirmed.same_identity_as(&quarantined) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "state transaction lock identity changed while quarantining {}",
                path.display()
            ),
        ));
    }

    // The canonical lock name is now free. Revalidate the quarantine identity at the cleanup
    // syscall too, because an unrelated process could replace even this process-unique pathname.
    let removal = match confirmed.kind {
        TransactionLockKind::File { .. } => {
            remove_owned_transaction_lock(&stale_path, quarantined.identity, lifecycle)
        }
        TransactionLockKind::EmptyDirectory => {
            lifecycle.validate()?;
            validate_transaction_target_identity(&stale_path, quarantined.identity)?;
            fs::remove_dir(&stale_path)
        }
        TransactionLockKind::NonEmptyDirectory
        | TransactionLockKind::Redirected
        | TransactionLockKind::Other => return Ok(false),
    };
    match removal {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

fn validate_owner(owner: &str) -> Result<()> {
    if owner.trim().is_empty() || owner.contains(['\0', '\n', '\r']) {
        Err(LeaseError::InvalidInput(format!(
            "invalid lease owner {owner:?}"
        )))
    } else {
        Ok(())
    }
}

fn validate_record(record: &LeaseRecord) -> std::result::Result<(), String> {
    if record.schema != LEASE_SCHEMA {
        return Err(format!("unsupported lease schema {:?}", record.schema));
    }
    if record.role.trim().is_empty()
        || record.owner_id.trim().is_empty()
        || record.root.trim().is_empty()
        || record.host.trim().is_empty()
        || record.ttl_seconds == 0
        || record.generation == 0
    {
        return Err("lease record has a required empty/zero field".into());
    }
    if iso_to_epoch(&record.heartbeat).is_none() || iso_to_epoch(&record.acquired).is_none() {
        return Err("lease record has an invalid UTC timestamp".into());
    }
    Ok(())
}

fn absolute_root(root: &Path) -> Result<String> {
    let root = if root.is_absolute() {
        root.to_path_buf()
    } else {
        env::current_dir()?.join(root)
    };
    Ok(root.to_string_lossy().into_owned())
}

/// Compare project-root addresses with the same lexical rules as Orchestra's lease verifier.
///
/// This deliberately does not resolve symlinks: a lease address is the user-visible project
/// root, and resolving a junction/symlink would both require the path to exist and can turn a
/// harmless spelling difference into a different extended Windows path.  It does, however,
/// collapse `.`/`..`, ignore a trailing separator, and follows the host platform's ordinary path
/// case rule.  Invalid/unresolvable relative input fails closed as a non-match.
pub fn roots_equivalent(left: &Path, right: &Path) -> bool {
    let Some(left) = lexical_absolute_path(left) else {
        return false;
    };
    let Some(right) = lexical_absolute_path(right) else {
        return false;
    };

    let left = left.to_string_lossy();
    let right = right.to_string_lossy();
    if cfg!(windows) {
        left.eq_ignore_ascii_case(&right)
    } else {
        left == right
    }
}

fn lexical_absolute_path(path: &Path) -> Option<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir().ok()?.join(path)
    };

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                // `Path.GetFullPath` (the interoperable legacy comparator) retains the root
                // rather than escaping it when a path starts with more `..` components.
                let _ = normalized.pop();
            }
            Component::Normal(segment) => normalized.push(segment),
        }
    }
    Some(normalized)
}

fn host_name() -> String {
    env::var("COMPUTERNAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown-host".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn work(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-native-lease-{label}-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn acquire_heartbeat_and_owner_checked_release_are_durable() {
        let work = work("lifecycle");
        let store = LeaseStore::new(&work);
        let record = store.acquire("engine-a", Path::new("."), 10, 100).unwrap();
        assert_eq!(record.role, ENGINE_ROLE);
        assert_eq!(record.generation, 1);
        assert!(matches!(
            store.status(101).unwrap(),
            LeaseStatus::Live { .. }
        ));

        let renewed = store.heartbeat("engine-a", Some(1), 102).unwrap();
        assert_eq!(renewed.generation, 2);
        assert!(matches!(
            store.release("engine-b", 103),
            Err(LeaseError::NotOwner { .. })
        ));
        assert!(store.release("engine-a", 103).unwrap());
        assert_eq!(store.status(103).unwrap(), LeaseStatus::Vacant);
        assert!(!store.lock_directory().exists());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn fresh_transaction_lock_times_out_with_owner_and_age_diagnostics() {
        let work = work("fresh-transaction-lock");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        fs::write(&tx, "foreign-pid").unwrap();
        let store = LeaseStore::new(&work);
        let error =
            store
                .with_transaction_policy(Duration::from_millis(5), Duration::from_secs(300), || {
                    Ok(())
                })
                .unwrap_err();
        assert!(matches!(
            &error,
            LeaseError::Busy {
                age_ms: Some(_),
                kind
            } if kind.contains("foreign-pid")
        ));
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("age="));
        assert!(diagnostic.contains("300000ms stale-recovery threshold"));
        assert_eq!(fs::read_to_string(&tx).unwrap(), "foreign-pid");
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn stale_file_and_legacy_directory_transaction_locks_are_recovered() {
        for (label, directory) in [("file", false), ("directory", true)] {
            let work = work(&format!("stale-transaction-{label}"));
            fs::create_dir_all(&work).unwrap();
            let tx = work.join(TRANSACTION_LOCK);
            if directory {
                fs::create_dir(&tx).unwrap();
            } else {
                fs::write(&tx, "crashed-pid").unwrap();
            }
            thread::sleep(Duration::from_millis(2));
            let store = LeaseStore::new(&work);
            store
                .with_transaction_policy(Duration::from_millis(20), Duration::ZERO, || {
                    let metadata = fs::symlink_metadata(&tx).unwrap();
                    assert!(metadata.is_file(), "native acquisition uses CreateNew file");
                    assert!(
                        fs::read_to_string(&tx)
                            .unwrap()
                            .starts_with(&format!("{}:", process::id())),
                        "native lock identity contains the process id and a unique sequence"
                    );
                    Ok(())
                })
                .unwrap();
            assert!(
                !tx.exists(),
                "owner-checked release removes the native lock"
            );
            let _ = fs::remove_dir_all(work);
        }
    }

    #[test]
    fn unsupported_stale_lock_kind_is_not_renamed_or_panicked() {
        let work = work("unsupported-stale-transaction-lock");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        fs::create_dir(&tx).unwrap();
        fs::write(tx.join("foreign-entry"), "do not move").unwrap();
        thread::sleep(Duration::from_millis(2));

        let lifecycle = try_acquire_transaction_lifecycle(&tx)
            .unwrap()
            .expect("test owns lifecycle lock");
        let decided = transaction_lock_snapshot(&tx).unwrap();
        assert!(matches!(
            decided.kind,
            TransactionLockKind::NonEmptyDirectory
        ));
        assert!(!break_stale_transaction_lock(&tx, &decided, Duration::ZERO, &lifecycle).unwrap());
        assert_eq!(
            fs::read_to_string(tx.join("foreign-entry")).unwrap(),
            "do not move"
        );
        assert!(
            fs::read_dir(&work).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".stale")
            }),
            "unsupported lock must not be displaced into quarantine"
        );
        drop(lifecycle);
        let _ = fs::remove_dir_all(work);
    }

    #[cfg(unix)]
    #[test]
    fn lifecycle_guard_rejects_an_unlinked_and_recreated_sidecar() {
        let work = work("recreated-lifecycle-sidecar");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        let original = try_acquire_transaction_lifecycle(&tx)
            .unwrap()
            .expect("test owns original lifecycle lock");
        let lifecycle_path = original.path.clone();

        fs::remove_file(&lifecycle_path).unwrap();
        fs::write(&lifecycle_path, "replacement").unwrap();
        let replacement = try_acquire_transaction_lifecycle(&tx)
            .unwrap()
            .expect("replacement inode has an independent kernel lock");

        let error = create_transaction_lock(&tx, "must-not-be-created", &original).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("identity is no longer valid"));
        assert!(
            !tx.exists(),
            "an obsolete lifecycle guard must deny further mutations"
        );

        drop(replacement);
        drop(original);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn target_identity_checks_reject_unlink_and_recreate_before_rename_or_remove() {
        let work = work("recreated-transaction-target");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        fs::write(&tx, "same-owner").unwrap();
        let original_file = work_fs::open_existing_plain_file(&tx).unwrap();
        let (original_identity, _) = opened_transaction_target_identity(&original_file).unwrap();
        let lifecycle = try_acquire_transaction_lifecycle(&tx)
            .unwrap()
            .expect("test owns lifecycle lock");

        // Holding the deleted file open prevents its filesystem identity from being recycled,
        // making the unlink/recreate race deterministic on both Unix and Windows.
        fs::remove_file(&tx).unwrap();
        fs::write(&tx, "same-owner").unwrap();
        let stale_path = work.join("state-tx.lock.test.stale");

        let rename_error =
            rename_transaction_lock_if_unchanged(&tx, &stale_path, original_identity, &lifecycle)
                .unwrap_err();
        assert_eq!(rename_error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            rename_error
                .to_string()
                .contains("identity is no longer valid")
        );
        assert!(!stale_path.exists());

        let remove_error =
            remove_owned_transaction_lock(&tx, original_identity, &lifecycle).unwrap_err();
        assert_eq!(remove_error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            remove_error
                .to_string()
                .contains("identity is no longer valid")
        );
        assert_eq!(fs::read_to_string(&tx).unwrap(), "same-owner");

        drop(original_file);
        drop(lifecycle);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn authorized_transaction_file_handle_stays_live_through_remove_mutation() {
        let work = work("handle-backed-transaction-remove");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        fs::write(&tx, "same-owner").unwrap();
        let expected = transaction_lock_snapshot(&tx).unwrap().identity;
        let lifecycle = try_acquire_transaction_lifecycle(&tx)
            .unwrap()
            .expect("test owns lifecycle lock");

        let mut mutation_observed = false;
        mutate_transaction_file_if_unchanged(&tx, expected, &lifecycle, |authorized| {
            let (before, links) = opened_transaction_target_identity(authorized)?;
            assert_eq!(before, expected);
            assert!(links > 0, "authorized file starts linked at its pathname");

            fs::remove_file(&tx)?;

            // The same handle remains usable after unlink while the mutation callback is
            // active. This proves the OS reference keeps the original file object alive for
            // the complete critical section, so its file ID cannot be recycled before remove.
            let (after, _) = opened_transaction_target_identity(authorized)?;
            assert_eq!(after, expected);
            mutation_observed = true;
            Ok(())
        })
        .unwrap();

        assert!(mutation_observed);
        assert!(!tx.exists());
        drop(lifecycle);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn stackable_named_filesystems_fail_closed() {
        let path = Path::new("/test/.state-tx.lifecycle.lock");
        for filesystem in ["nullfs", "unionfs"] {
            let error = validate_named_unix_filesystem_type(path, filesystem).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(
                error
                    .to_string()
                    .contains(&format!("filesystem type {filesystem:?} is not supported"))
            );
            assert!(error.to_string().contains("stackable backing store"));
        }
        validate_named_unix_filesystem_type(path, "ufs").unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn network_and_unknown_linux_filesystems_fail_closed() {
        let path = Path::new("/test/.state-tx.lifecycle.lock");
        for filesystem_type in [
            0x0000_6969, // NFS
            0xFF53_4D42, // CIFS
            0x0000_517B, // SMB
            0x5346_414F, // AFS
            0x6573_5546, // FUSE
            0xDEAD_BEEF, // unknown
        ] {
            let error = validate_linux_filesystem_type(path, filesystem_type).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(error.to_string().contains("NFS and SMB are not supported"));
        }
        validate_linux_filesystem_type(path, 0x0000_EF53).unwrap();
        validate_linux_filesystem_type(path, 0x794C_7630).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_unc_paths_are_classified_as_remote() {
        assert!(windows_path_is_unc(Path::new(
            r"\\server\share\.state-tx.lifecycle.lock"
        )));
        assert!(windows_path_is_unc(Path::new(
            r"\\?\UNC\server\share\.state-tx.lifecycle.lock"
        )));
        assert!(!windows_path_is_unc(Path::new(
            r"C:\project\.work\.state-tx.lifecycle.lock"
        )));
        let path = Path::new(r"Z:\project\.work\.state-tx.lifecycle.lock");
        let remote = validate_windows_drive_type(path, 4).unwrap_err();
        assert_eq!(remote.kind(), io::ErrorKind::Unsupported);
        assert!(
            remote
                .to_string()
                .contains("remote or mapped network drive")
        );
        assert!(validate_windows_drive_type(path, 3).is_ok());
    }

    #[cfg(windows)]
    #[test]
    fn old_creation_time_does_not_make_a_fresh_transaction_lock_stale() {
        use std::os::windows::fs::FileTimesExt;

        let work = work("ntfs-tunneled-transaction-lock");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        fs::write(&tx, "fresh-owner").unwrap();
        let file = fs::OpenOptions::new().write(true).open(&tx).unwrap();
        let now = SystemTime::now();
        let old_creation = now - Duration::from_secs(10 * 60);
        file.set_times(
            fs::FileTimes::new()
                .set_created(old_creation)
                .set_modified(now),
        )
        .unwrap();

        let snapshot = transaction_lock_snapshot(&tx).unwrap();
        assert!(
            snapshot.age_ms.is_some_and(|age| age < 60_000),
            "staleness must use the fresh last-write time, not the tunneled creation time"
        );
        let lifecycle = try_acquire_transaction_lifecycle(&tx)
            .unwrap()
            .expect("test owns lifecycle lock");
        assert!(
            !break_stale_transaction_lock(&tx, &snapshot, TRANSACTION_LOCK_STALE_AFTER, &lifecycle)
                .unwrap()
        );
        assert_eq!(fs::read_to_string(&tx).unwrap(), "fresh-owner");
        drop(lifecycle);
        let _ = fs::remove_dir_all(work);
    }

    // Deterministically scheduling the vulnerable rename/recreate window across two child
    // processes is not a hermetic unit-test boundary. The production guard is nevertheless
    // cross-process by construction: both this thread test and independent processes contend on
    // the same kernel `flock`/`LockFileEx`, never on Rust process memory.
    #[test]
    fn concurrent_stale_lock_break_only_one_succeeds() {
        let work = work("concurrent-stale-transaction-lock");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        fs::write(&tx, "crashed-pid").unwrap();
        let stale_after = Duration::from_millis(50);
        thread::sleep(Duration::from_millis(100));

        let start = Arc::new(Barrier::new(3));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let owners = thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..2 {
                let tx = tx.clone();
                let start = Arc::clone(&start);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                handles.push(scope.spawn(move || {
                    start.wait();
                    let mut guard =
                        acquire_transaction_lock(&tx, Duration::from_secs(2), stale_after).unwrap();
                    let owner = guard.owner.clone();
                    let entrants = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(entrants, Ordering::SeqCst);
                    assert_eq!(entrants, 1, "transaction critical sections overlapped");
                    assert_eq!(fs::read_to_string(&tx).unwrap(), owner);
                    thread::sleep(Duration::from_millis(20));
                    assert_eq!(
                        fs::read_to_string(&tx).unwrap(),
                        owner,
                        "a concurrent stale breaker replaced the live lock"
                    );
                    assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                    guard.release().unwrap();
                    owner
                }));
            }
            start.wait();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert_eq!(owners.len(), 2);
        assert_ne!(owners[0], owners[1]);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(!tx.exists());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn obsolete_same_process_guard_cannot_release_a_recreated_transaction_lock() {
        let work = work("recreated-transaction-lock");
        fs::create_dir_all(&work).unwrap();
        let tx = work.join(TRANSACTION_LOCK);
        fs::write(&tx, "123:old").unwrap();
        let mut obsolete = TransactionLockGuard {
            path: tx.clone(),
            owner: "123:old".into(),
            armed: true,
            lifecycle: Some(
                try_acquire_transaction_lifecycle(&tx)
                    .unwrap()
                    .expect("test owns lifecycle lock"),
            ),
        };
        fs::remove_file(&tx).unwrap();
        fs::write(&tx, "123:new").unwrap();

        let error = obsolete.release().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(fs::read_to_string(&tx).unwrap(), "123:new");
        obsolete.armed = false;
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn stale_lease_needs_explicit_takeover_and_old_owner_loses_access() {
        let work = work("takeover");
        let store = LeaseStore::new(&work);
        store.acquire("engine-a", Path::new("."), 10, 100).unwrap();
        assert!(matches!(
            store.status(110).unwrap(),
            LeaseStatus::Stale { .. }
        ));
        assert!(matches!(
            store.acquire("engine-b", Path::new("."), 10, 110),
            Err(LeaseError::Stale { .. })
        ));
        let adopted = store.takeover("engine-b", Path::new("."), 10, 110).unwrap();
        assert_eq!(adopted.generation, 2);
        assert_eq!(adopted.taken_over_from.as_deref(), Some("engine-a"));
        assert!(matches!(
            store.heartbeat("engine-a", None, 111),
            Err(LeaseError::NotOwner { .. })
        ));
        assert!(store.release("engine-b", 111).unwrap());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn legacy_and_corrupt_lock_states_are_never_overwritten() {
        let work = work("foreign");
        let store = LeaseStore::new(&work);
        fs::create_dir_all(store.lock_directory()).unwrap();
        fs::write(store.lock_directory().join("info"), "legacy").unwrap();
        assert!(matches!(
            store.status(100).unwrap(),
            LeaseStatus::LegacyLock { .. }
        ));
        assert!(matches!(
            store.acquire("engine", Path::new("."), 10, 100),
            Err(LeaseError::LegacyLock { .. })
        ));
        fs::remove_file(store.lock_directory().join("info")).unwrap();
        fs::write(store.lease_path(), "not-json").unwrap();
        assert!(matches!(
            store.status(100).unwrap(),
            LeaseStatus::Corrupt { .. }
        ));
        assert!(matches!(
            store.takeover("engine", Path::new("."), 10, 100),
            Err(LeaseError::Corrupt { .. })
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn unique_replace_temp_is_recoverable_but_arbitrary_lock_content_is_not() {
        let work = work("replace-temp-recovery");
        let store = LeaseStore::new(&work);
        fs::create_dir_all(store.lock_directory()).unwrap();
        let recoverable = store
            .lock_directory()
            .join(format!(".{LEASE_FILE}.{}.17.tmp", process::id()));
        fs::write(&recoverable, "partial lease").unwrap();
        assert_eq!(store.status(100).unwrap(), LeaseStatus::Vacant);

        fs::write(
            store.lock_directory().join(".lease.json.owner.tmp"),
            "foreign",
        )
        .unwrap();
        assert!(matches!(
            store.status(100).unwrap(),
            LeaseStatus::LegacyLock { .. }
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn stale_legacy_shape_is_read_and_taken_over_without_running_powershell() {
        let work = work("interop");
        let store = LeaseStore::new(&work);
        fs::create_dir_all(store.lock_directory()).unwrap();
        let legacy = serde_json::json!({
            "schema": "orchestra/lease@1",
            "role": "processor",
            "owner_id": "legacy-owner",
            "session_id": "legacy-session",
            "root": "D:/legacy",
            "host": "other-host",
            "pid": 0,
            "pid_started": null,
            "acquired": "1970-01-01T00:01:40Z",
            "heartbeat": "1970-01-01T00:01:40Z",
            "ttl_seconds": 10,
            "generation": 7,
            "taken_over_from": null
        });
        fs::write(store.lease_path(), serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert!(matches!(
            store.status(110).unwrap(),
            LeaseStatus::Stale { .. }
        ));
        let new = store.takeover("engine", Path::new("."), 10, 110).unwrap();
        assert_eq!(new.generation, 8);
        assert_eq!(new.taken_over_from.as_deref(), Some("legacy-owner"));
        assert!(store.release("engine", 111).unwrap());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn addressed_takeover_refuses_a_stale_record_with_a_foreign_root_or_role() {
        let work = work("addressed-takeover");
        let store = LeaseStore::new(&work);
        let root = std::env::current_dir().expect("current directory");
        let other_root = root.join("other-project");
        let first = store
            .acquire("foreign-root", &other_root, 10, 100)
            .expect("seed foreign-root stale lease");

        assert!(matches!(
            store.takeover_addressed("resumer", &root, ENGINE_ROLE, 10, 111),
            Err(LeaseError::AddressMismatch { .. })
        ));
        assert!(matches!(
            store.status(111),
            Ok(LeaseStatus::Stale { ref record, .. })
                if record.owner_id == first.owner_id && record.root == first.root
        ));

        let mut foreign_role = first;
        foreign_role.root = root.to_string_lossy().into_owned();
        foreign_role.role = "merger".into();
        fs::write(
            store.lease_path(),
            serde_json::to_vec(&foreign_role).expect("serialize stale foreign-role lease"),
        )
        .expect("replace fixture lease record");
        assert!(matches!(
            store.takeover_addressed("resumer", &root, ENGINE_ROLE, 10, 111),
            Err(LeaseError::AddressMismatch { .. })
        ));
        assert!(matches!(
            store.status(111),
            Ok(LeaseStatus::Stale { ref record, .. }) if record.role == "merger"
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn addressed_takeover_requires_a_new_owner_inside_the_transaction() {
        let work = work("addressed-takeover-owner");
        let store = LeaseStore::new(&work);
        let root = std::env::current_dir().expect("current directory");
        let seeded = store
            .acquire("interrupted-owner", &root, 10, 100)
            .expect("seed addressed stale lease");

        assert!(matches!(
            store.takeover_addressed("interrupted-owner", &root, ENGINE_ROLE, 10, 111),
            Err(LeaseError::InvalidInput(_))
        ));
        assert!(matches!(
            store.status(111),
            Ok(LeaseStatus::Stale { ref record, .. })
                if record.owner_id == seeded.owner_id && record.generation == seeded.generation
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn renewal_worker_keeps_the_owner_lease_live_and_stops_promptly() {
        let work = work("renewal-worker");
        let store = LeaseStore::new(&work);
        let initial = store
            .acquire("engine-a", Path::new("."), 60, 100)
            .expect("acquire test lease");
        let worker = LeaseHeartbeat::start_with_interval(
            store.clone(),
            initial.owner_id.clone(),
            initial.generation,
            Duration::from_millis(5),
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let renewed = loop {
            let status = store.status(system_now_secs()).expect("read lease");
            if let LeaseStatus::Live { record, .. } = status
                && record.generation > initial.generation
            {
                break record;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker did not renew lease"
            );
            thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(renewed.owner_id, "engine-a");
        worker.stop().expect("stop clean renewal worker");
        assert!(store.release("engine-a", system_now_secs()).unwrap());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn heartbeat_cancellation_probe_latches_lost_ownership() {
        let work = work("renewal-lost-owner");
        let store = LeaseStore::new(&work);
        let initial = store
            .acquire("engine-a", Path::new("."), 60, system_now_secs())
            .expect("acquire test lease");
        let worker = LeaseHeartbeat::start_with_interval(
            store.clone(),
            initial.owner_id.clone(),
            initial.generation,
            Duration::from_millis(5),
        );
        let probe = worker.cancellation_probe();
        store
            .takeover(
                "engine-b",
                Path::new("."),
                60,
                system_now_secs().saturating_add(61),
            )
            .expect("explicitly replace stale owner in fixture");

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !probe.is_cancelled() {
            assert!(
                std::time::Instant::now() < deadline,
                "lost ownership did not reach the containment cancellation probe"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            worker.stop().is_err(),
            "the monitoring owner must surface its CAS failure"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn redirected_work_or_lease_file_is_never_followed() {
        let real_work = work("redirect-real");
        let redirected_work = work("redirect-link");
        fs::create_dir(&real_work).unwrap();
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(&real_work, &redirected_work).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&real_work, &redirected_work).is_ok();
        if linked {
            let store = LeaseStore::new(&redirected_work);
            assert!(matches!(
                store.acquire("engine-a", Path::new("."), 60, 100),
                Err(LeaseError::Io(_))
            ));
            assert_eq!(fs::read_dir(&real_work).unwrap().count(), 0);
        }
        let _ = fs::remove_dir_all(redirected_work);
        let _ = fs::remove_dir_all(real_work);

        let work = work("redirect-file");
        let store = LeaseStore::new(&work);
        fs::create_dir_all(store.lock_directory()).unwrap();
        let external = work.with_extension("external.json");
        fs::write(&external, "external sentinel\n").unwrap();
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&external, store.lease_path()).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&external, store.lease_path()).is_ok();
        if linked {
            assert!(matches!(store.status(100), Err(LeaseError::Io(_))));
            assert_eq!(
                fs::read_to_string(&external).unwrap(),
                "external sentinel\n"
            );
        }
        let _ = fs::remove_file(&external);
        let _ = fs::remove_dir_all(work);
    }
}
