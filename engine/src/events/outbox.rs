//! Append-only, idempotent writer for the typed `.work/events.jsonl` outbox.
//!
//! The engine lease is the single-writer interlock. This module validates the committed prefix
//! once into a process-local semantic index, incrementally validates any newly observed range,
//! and refuses an event-id collision with different content, so a bad caller cannot turn replay
//! into a silently divergent event stream.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex, MutexGuard};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Event, ParseError, parse_line};
use crate::work_fs;

static OUTBOX_ACCESS: Mutex<()> = Mutex::new(());

/// A committed event record may not force an unbounded allocation while the one-time index scan
/// is catching up. This matches the tail reader's per-record ceiling.
const MAX_EVENT_LINE_BYTES: u64 = 1024 * 1024;
const MAX_CACHED_OUTBOXES: usize = 16;

#[derive(Debug, Default)]
struct CachedIndex {
    committed_len: u64,
    committed_lines: usize,
    semantic_by_id: HashMap<String, [u8; 32]>,
    scanned_bytes: u64,
}

/// The process owns one orchestration lease in production, hence normally one cache entry. A
/// small bound prevents hermetic tests or repeated repository probes from retaining indexes for
/// every temporary work directory for the lifetime of the process.
static OUTBOX_INDEXES: LazyLock<Mutex<HashMap<PathBuf, CachedIndex>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Serialize native readers which derive an identity from committed history with native appends.
/// The owner lease excludes another orchestrator process; this guard additionally excludes the
/// engine's own parallel leaf threads.
pub(crate) fn lock_outbox() -> io::Result<MutexGuard<'static, ()>> {
    OUTBOX_ACCESS
        .lock()
        .map_err(|_| io::Error::other("native outbox interlock is poisoned"))
}

/// Durable filename relative to the selected `.work` directory.
pub const OUTBOX_FILE: &str = "events.jsonl";

#[derive(Debug)]
pub enum OutboxError {
    Io(io::Error),
    InvalidExisting { line: usize, error: ParseError },
    EventIdCollision { event_id: String },
}

impl std::fmt::Display for OutboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "outbox I/O error: {error}"),
            Self::InvalidExisting { line, error } => {
                write!(f, "outbox contains invalid committed line {line}: {error}")
            }
            Self::EventIdCollision { event_id } => {
                write!(
                    f,
                    "outbox event id {event_id} already names different content"
                )
            }
        }
    }
}

impl std::error::Error for OutboxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidExisting { error, .. } => Some(error),
            Self::EventIdCollision { .. } => None,
        }
    }
}

impl From<io::Error> for OutboxError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, OutboxError>;

/// Outcome of an idempotent append request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended,
    AlreadyPresent,
}

/// Owns the selected `.work/events.jsonl` location; it does not own the orchestration lease.
#[derive(Debug, Clone)]
pub struct Outbox {
    work: PathBuf,
}

impl Outbox {
    pub fn new(work: impl Into<PathBuf>) -> Self {
        Self { work: work.into() }
    }

    pub fn path(&self) -> PathBuf {
        self.work.join(OUTBOX_FILE)
    }

    /// Append `event` exactly once by its event id. The event must already carry a deterministic
    /// UUID from [`deterministic_event_id`]; this method never substitutes a random identity on a
    /// replay path.
    pub fn append_idempotent(&self, event: &Event) -> Result<AppendOutcome> {
        let _guard = lock_outbox()?;
        let path = self.path();
        let desired = event.to_json_line();
        let mut file = open_plain_outbox(&path)?;
        let file_len = file.metadata()?.len();
        let committed_len = committed_prefix_len(&mut file, file_len)?;
        let cache_key = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        let mut indexes = OUTBOX_INDEXES
            .lock()
            .map_err(|_| io::Error::other("native outbox index cache is poisoned"))?;
        if !indexes.contains_key(&cache_key) && indexes.len() >= MAX_CACHED_OUTBOXES {
            indexes.clear();
        }
        let index = indexes.entry(cache_key.clone()).or_default();
        if index.committed_len > committed_len {
            *index = CachedIndex::default();
        }
        if let Err(error) = scan_committed_range(&mut file, index, committed_len) {
            indexes.remove(&cache_key);
            return Err(error);
        }
        let desired_fingerprint = semantic_fingerprint(event);
        let already_present = match index.semantic_by_id.get(&event.event_id) {
            Some(existing) if existing == &desired_fingerprint => true,
            Some(_) => {
                return Err(OutboxError::EventIdCollision {
                    event_id: event.event_id.clone(),
                });
            }
            None => false,
        };
        if committed_len != file_len {
            file.set_len(committed_len)?;
            file.sync_all()?;
        }
        if already_present {
            return Ok(AppendOutcome::AlreadyPresent);
        }
        file.seek(SeekFrom::End(0))?;
        let line = format!("{desired}\n");
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        index.committed_len = index
            .committed_len
            .checked_add(u64::try_from(line.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "outbox append length overflowed",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "outbox length overflowed")
            })?;
        index.committed_lines = index.committed_lines.saturating_add(1);
        index
            .semantic_by_id
            .insert(event.event_id.clone(), desired_fingerprint);
        Ok(AppendOutcome::Appended)
    }
}

fn committed_prefix_len(file: &mut File, file_len: u64) -> io::Result<u64> {
    if file_len == 0 {
        return Ok(0);
    }
    file.seek(SeekFrom::Start(file_len - 1))?;
    let mut last = [0_u8; 1];
    file.read_exact(&mut last)?;
    if last[0] == b'\n' {
        return Ok(file_len);
    }

    let inspected = file_len.min(MAX_EVENT_LINE_BYTES + 1);
    file.seek(SeekFrom::Start(file_len - inspected))?;
    let mut tail = vec![
        0_u8;
        usize::try_from(inspected).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "outbox tail length overflowed")
        })?
    ];
    file.read_exact(&mut tail)?;
    if let Some(index) = tail.iter().rposition(|byte| *byte == b'\n') {
        return Ok(file_len - inspected + index as u64 + 1);
    }
    if file_len <= MAX_EVENT_LINE_BYTES {
        return Ok(0);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unterminated events.jsonl tail exceeds {MAX_EVENT_LINE_BYTES} byte limit"),
    ))
}

fn scan_committed_range(
    file: &mut File,
    index: &mut CachedIndex,
    committed_len: u64,
) -> Result<()> {
    if index.committed_len == committed_len {
        return Ok(());
    }
    file.seek(SeekFrom::Start(index.committed_len))?;
    let mut reader = BufReader::new(file);
    let mut position = index.committed_len;
    while position < committed_len {
        let remaining = committed_len - position;
        let read_limit = remaining.min(MAX_EVENT_LINE_BYTES + 2);
        let mut bounded = (&mut reader).take(read_limit);
        let mut raw = Vec::new();
        let read = bounded.read_until(b'\n', &mut raw)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "outbox ended before its committed prefix",
            )
            .into());
        }
        let read = u64::try_from(read).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "outbox record length overflowed",
            )
        })?;
        position = position.checked_add(read).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "outbox scan offset overflowed")
        })?;
        index.scanned_bytes = index.scanned_bytes.saturating_add(read);
        index.committed_lines = index.committed_lines.saturating_add(1);
        if raw.last() != Some(&b'\n') || raw.len() - 1 > MAX_EVENT_LINE_BYTES as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("events.jsonl record exceeds {MAX_EVENT_LINE_BYTES} byte limit"),
            )
            .into());
        }
        raw.pop();
        let line = std::str::from_utf8(&raw).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("outbox contains non-UTF-8 committed line: {error}"),
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let existing = parse_line(line).map_err(|error| OutboxError::InvalidExisting {
            line: index.committed_lines,
            error,
        })?;
        let fingerprint = semantic_fingerprint(&existing);
        if let Some(previous) = index
            .semantic_by_id
            .insert(existing.event_id.clone(), fingerprint)
            && previous != fingerprint
        {
            return Err(OutboxError::EventIdCollision {
                event_id: existing.event_id,
            });
        }
    }
    index.committed_len = committed_len;
    Ok(())
}

fn semantic_fingerprint(event: &Event) -> [u8; 32] {
    let mut semantic = event.clone();
    semantic.occurred_at.clear();
    Sha256::digest(semantic.to_json_line().as_bytes()).into()
}

fn open_plain_outbox(path: &std::path::Path) -> io::Result<File> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event outbox has no parent"))?;
    work_fs::ensure_plain_directory(parent)?;
    work_fs::open_plain_file_read_write(path, true)
}

/// Open an existing outbox for readers without following a symlink/reparse-point replacement.
/// Missing files remain `NotFound`; readers must decide whether that means empty or unavailable.
pub(crate) fn open_existing_plain_outbox(path: &std::path::Path) -> io::Result<File> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event outbox has no parent"))?;
    work_fs::require_plain_directory(parent)?;
    work_fs::open_existing_plain_file(path)
}

/// Convert a stable semantic key into the legacy contract's standard URL-namespace UUIDv5. The
/// key must be built from durable coordinates (event type, batch/task id, transition/cycle),
/// never a clock or randomly generated run id.
pub fn deterministic_event_id(key: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use serde_json::Map;

    use super::*;
    use crate::events::{Actor, ActorKind, EventType, SCHEMA_VERSION};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temp_work(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-outbox-{label}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn event(key: &str) -> Event {
        Event {
            schema_version: SCHEMA_VERSION,
            event_id: deterministic_event_id(key),
            occurred_at: "2026-07-24T12:00:00Z".into(),
            event_type: EventType::CohortOpened,
            actor: Actor {
                kind: ActorKind::Agent,
                name: "engine".into(),
            },
            batch_id: Some("B-1".into()),
            task_id: None,
            payload_version: 1,
            payload: Map::new(),
        }
    }

    #[test]
    fn deterministic_uuid_is_stable_and_envelope_valid() {
        let first = deterministic_event_id("cohort.opened|B-1");
        assert_eq!(first, "3511e4d4-81ca-5434-8916-48671f482067");
        assert_eq!(first, deterministic_event_id("cohort.opened|B-1"));
        assert_ne!(first, deterministic_event_id("cohort.opened|B-2"));
        assert_eq!(first.as_bytes()[14], b'5');
        assert!(matches!(first.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
    }

    #[test]
    fn append_is_idempotent_and_rejects_a_collision() {
        let work = temp_work("append");
        let outbox = Outbox::new(&work);
        let first = event("cohort.opened|B-1");
        assert_eq!(
            outbox.append_idempotent(&first).unwrap(),
            AppendOutcome::Appended
        );
        assert_eq!(
            outbox.append_idempotent(&first).unwrap(),
            AppendOutcome::AlreadyPresent
        );
        let mut replay = first.clone();
        replay.occurred_at = "2026-07-25T12:00:00Z".into();
        assert_eq!(
            outbox.append_idempotent(&replay).unwrap(),
            AppendOutcome::AlreadyPresent
        );
        let mut collision = first.clone();
        collision.payload.insert("different".into(), true.into());
        assert!(matches!(
            outbox.append_idempotent(&collision),
            Err(OutboxError::EventIdCollision { .. })
        ));
        assert_eq!(
            fs::read_to_string(outbox.path()).unwrap().lines().count(),
            1
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn committed_invalid_line_fails_closed_before_append() {
        let work = temp_work("invalid");
        fs::create_dir_all(&work).unwrap();
        fs::write(work.join(OUTBOX_FILE), "{bad json}\n").unwrap();
        assert!(matches!(
            Outbox::new(&work).append_idempotent(&event("cohort.opened|B-1")),
            Err(OutboxError::InvalidExisting { line: 1, .. })
        ));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn torn_tail_is_removed_before_an_idempotent_or_new_append() {
        let work = temp_work("torn");
        let outbox = Outbox::new(&work);
        let first = event("first");
        let replay = event("replay");
        fs::create_dir_all(&work).unwrap();
        fs::write(
            outbox.path(),
            format!("{}\n{}", first.to_json_line(), replay.to_json_line()),
        )
        .unwrap();

        assert_eq!(
            outbox.append_idempotent(&replay).unwrap(),
            AppendOutcome::Appended,
            "a complete-looking record without its commit newline is still an uncommitted tail"
        );
        let text = fs::read_to_string(outbox.path()).unwrap();
        assert!(text.ends_with('\n'));
        assert_eq!(text.lines().count(), 2);
        assert_eq!(parse_line(text.lines().nth(1).unwrap()).unwrap(), replay);

        fs::write(
            outbox.path(),
            format!("{}\n{{\"schema_version\":", first.to_json_line()),
        )
        .unwrap();
        let third = event("third");
        assert_eq!(
            outbox.append_idempotent(&third).unwrap(),
            AppendOutcome::Appended
        );
        let text = fs::read_to_string(outbox.path()).unwrap();
        assert_eq!(text.lines().count(), 2);
        assert_eq!(parse_line(text.lines().nth(1).unwrap()).unwrap(), third);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn parallel_native_appenders_never_interleave_or_lose_events() {
        let work = temp_work("parallel");
        let barrier = Arc::new(Barrier::new(8));
        let mut threads = Vec::new();
        for index in 0..8 {
            let outbox = Outbox::new(&work);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                outbox
                    .append_idempotent(&event(&format!("parallel-event-{index}")))
                    .unwrap();
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let text = fs::read_to_string(work.join(OUTBOX_FILE)).unwrap();
        assert!(text.ends_with('\n'));
        let events = text
            .lines()
            .map(|line| parse_line(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 8);
        let ids = events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), 8);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn large_existing_journal_is_indexed_once_and_followup_appends_scan_only_new_bytes() {
        let work = temp_work("large-index");
        fs::create_dir_all(&work).unwrap();
        let mut journal = String::new();
        for index in 0..4096 {
            journal.push_str(&event(&format!("existing-{index}")).to_json_line());
            journal.push('\n');
        }
        fs::write(work.join(OUTBOX_FILE), &journal).unwrap();
        let outbox = Outbox::new(&work);
        assert_eq!(
            outbox.append_idempotent(&event("new-after-index")).unwrap(),
            AppendOutcome::Appended
        );
        let key = fs::canonicalize(outbox.path()).unwrap();
        let scanned_after_index = OUTBOX_INDEXES
            .lock()
            .unwrap()
            .get(&key)
            .unwrap()
            .scanned_bytes;
        assert_eq!(scanned_after_index, journal.len() as u64);

        assert_eq!(
            Outbox::new(&work)
                .append_idempotent(&event("second-after-index"))
                .unwrap(),
            AppendOutcome::Appended
        );
        assert_eq!(
            OUTBOX_INDEXES
                .lock()
                .unwrap()
                .get(&key)
                .unwrap()
                .scanned_bytes,
            scanned_after_index,
            "a fresh Outbox handle must reuse the committed index instead of rescanning history"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn oversized_committed_record_fails_at_the_explicit_line_ceiling() {
        let work = temp_work("oversized-record");
        fs::create_dir_all(&work).unwrap();
        let mut record = vec![b'x'; MAX_EVENT_LINE_BYTES as usize + 1];
        record.push(b'\n');
        fs::write(work.join(OUTBOX_FILE), record).unwrap();
        let error = Outbox::new(&work)
            .append_idempotent(&event("after-oversized"))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("record exceeds 1048576 byte limit"),
            "unexpected bounded-read error: {error}"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn redirected_outbox_is_rejected_without_touching_its_target() {
        let work = temp_work("redirected");
        fs::create_dir_all(&work).unwrap();
        let target = work.with_extension("target");
        fs::write(&target, "operator-owned\n").unwrap();
        let link = work.join(OUTBOX_FILE);
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&target, &link).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&target, &link).is_ok();
        #[cfg(not(any(windows, unix)))]
        let linked = false;
        if !linked {
            let _ = fs::remove_file(&target);
            let _ = fs::remove_dir_all(&work);
            return;
        }

        assert!(matches!(
            Outbox::new(&work).append_idempotent(&event("redirected")),
            Err(OutboxError::Io(_))
        ));
        assert_eq!(fs::read_to_string(&target).unwrap(), "operator-owned\n");
        let _ = fs::remove_file(&link);
        let _ = fs::remove_file(&target);
        let _ = fs::remove_dir_all(work);

        let target = temp_work("redirected-parent-target");
        fs::create_dir(&target).unwrap();
        let work = temp_work("redirected-parent-link");
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(&target, &work).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&target, &work).is_ok();
        #[cfg(not(any(windows, unix)))]
        let linked = false;
        if linked {
            assert!(matches!(
                Outbox::new(&work).append_idempotent(&event("redirected-parent")),
                Err(OutboxError::Io(_))
            ));
            assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        }
        let _ = fs::remove_dir_all(work);
        let _ = fs::remove_dir_all(target);
    }
}
