//! Append-only, idempotent writer for the typed `.work/events.jsonl` outbox.
//!
//! The engine lease is the single-writer interlock. This module validates the committed prefix
//! once into a process-local semantic index, incrementally validates any newly observed range,
//! and refuses an event-id collision with different content, so a bad caller cannot turn replay
//! into a silently divergent event stream.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Event, EventType, ParseError, parse_line};
use crate::work_fs;

static OUTBOX_ACCESS: Mutex<()> = Mutex::new(());

/// A committed event record may not force an unbounded allocation while the one-time index scan
/// is catching up. This matches the tail reader's per-record ceiling.
const MAX_EVENT_LINE_BYTES: u64 = 1024 * 1024;
const MAX_CACHED_OUTBOXES: usize = 16;
/// Schema version 1 is the first-ever released shape for event rotation metadata.
/// Event rotation is a greenfield feature in this release; no deployed version has ever written
/// this metadata. Earlier shapes existed only in unreleased drafts and are not deployed schemas.
/// Any genuine future format change, including adding or renaming fields or changing
/// serialization, must assign a new schema version and implement a migration from this baseline.
const ROTATION_SCHEMA_VERSION: u32 = 1;
const SEGMENT_DIGITS: usize = 20;
/// How many times resolving one layout re-reads a control plane that keeps moving underneath it.
/// Each retry needs a rotation to have committed, and rotation happens at most once per published
/// cohort, so exhausting this is a stream no consumer can resolve rather than plain contention.
const MAX_LAYOUT_SNAPSHOT_ATTEMPTS: usize = 8;
/// Longest `occurred_at` retained as the informational `last_rotation_at`. The rotation index is
/// a fixed-size control artifact; one caller-supplied string must not be the field that decides
/// whether it still fits, so an unusually long timestamp is simply not recorded.
const MAX_ROTATION_TIMESTAMP_BYTES: usize = 64;

/// Immutable segment directory relative to the selected `.work` directory.
pub const EVENTS_ARCHIVE_DIR: &str = "events_archive";
/// Atomic commit pointer for the archive, relative to the selected `.work` directory.
pub const EVENTS_ROTATION_FILE: &str = "events_rotation.json";

static ROTATION_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Capacity the rotation index must satisfy *before* an archive segment becomes visible.
///
/// The index is deliberately O(1) in the number of rotations, so neither bound can be reached by
/// rotating; they exist because a bound that is never validated is a bound that fails exactly
/// once, at the worst possible moment — after the segment has already been renamed away and the
/// active file can no longer be recovered by writing a smaller index. Tests lower them to prove
/// the deferral path, production always uses [`RotationLimits::DEFAULT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RotationLimits {
    /// Largest sequence number that may be published. Well below the confined directory-entry
    /// ceiling, so the archive listing cannot become unreadable and wedge the writer.
    pub(crate) max_segments: u64,
    /// Largest serialized `events_rotation.json` payload that may be committed.
    pub(crate) max_metadata_bytes: u64,
}

impl RotationLimits {
    pub(crate) const DEFAULT: Self = Self {
        max_segments: 65_536,
        max_metadata_bytes: 4 * 1024,
    };
}

#[derive(Debug, Default)]
struct CachedIndex {
    archived_len: u64,
    committed_len: u64,
    committed_lines: usize,
    semantic_by_id: HashMap<String, [u8; 32]>,
    active_ids: HashSet<String>,
    published_batches: HashSet<String>,
    scanned_bytes: u64,
}

/// The rotation index: a **fixed-size** commit pointer over the archive directory.
///
/// Each immutable segment carries its own sequence number and logical byte range in its file
/// name, so this artifact never grows with the number of rotations — it only records how much of
/// the archive is committed plus whatever single transfer is in flight. That is what keeps a
/// long-lived project from reaching a size at which the index can no longer be republished after
/// its segment has already been renamed into place, which would wedge every future append.
///
/// This struct defines schema version 1, the first-ever released shape of this greenfield feature.
/// No deployed version has ever used event rotation or written rotation metadata before this
/// release; different shapes from earlier unreleased drafts are not prior deployed formats. Any
/// genuine future format change, including adding or renaming fields or changing serialization,
/// must bump the schema version and implement a migration from this baseline so that deployed data
/// cannot be confused with the no-prior-deployment state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RotationMetadata {
    schema_version: u32,
    /// Strictly increases with every published index state. Concurrent readers use it to prove
    /// that the layout they resolved still describes the bytes they just read.
    #[serde(default)]
    generation: u64,
    /// How many leading segments of `events_archive/` (ordered by sequence) are committed. A
    /// segment renamed into place but not yet counted here is invisible: its bytes are still
    /// part of the active file, so counting it early would publish them twice.
    #[serde(default)]
    segment_count: u64,
    /// Logical end offset of the last committed segment; zero when none is committed.
    #[serde(default)]
    archived_len: u64,
    /// Non-zero only between committing the archive segment and atomically replacing the active
    /// file. Readers skip this many duplicated bytes while the transfer is in that state.
    #[serde(default)]
    active_prefix_bytes: u64,
    /// Digest of those duplicated bytes, proving the prefix about to be dropped is exactly the
    /// prefix that was archived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_prefix_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_rotation_at: Option<String>,
}

/// One immutable archive segment as described by its own file name and on-disk size.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ArchivedSegment {
    name: String,
    sequence: u64,
    start_offset: u64,
    end_offset: u64,
    len: u64,
}

/// The validated committed archive plus whatever is visible beyond it.
#[derive(Debug, Clone)]
struct RotationState {
    metadata: RotationMetadata,
    committed: Vec<ArchivedSegment>,
    /// Segments present in the directory beyond the commit pointer. Normal operation exposes at
    /// most one: a segment renamed into place by a rotation that has not committed yet.
    uncommitted: Vec<ArchivedSegment>,
}

#[derive(Debug, Clone)]
pub(crate) struct EventStreamSource {
    pub(crate) path: PathBuf,
    pub(crate) start_offset: u64,
    pub(crate) end_offset: u64,
    pub(crate) physical_start: u64,
    pub(crate) archived: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct EventStreamLayout {
    pub(crate) sources: Vec<EventStreamSource>,
    pub(crate) archived_len: u64,
    pub(crate) logical_len: u64,
}

/// A layout plus the index generation it was resolved from.
///
/// Resolving a layout and then reading bytes are separate filesystem operations, and rotation
/// physically replaces the active file between them if it lands in that window. Consumers must
/// therefore treat a layout as a *hypothesis* until [`EventStreamSnapshot::is_still_current`]
/// confirms that no rotation was committed while they were reading; otherwise newly appended
/// bytes would be interpreted at the logical offsets of the range that was just archived.
#[derive(Debug, Clone)]
pub(crate) struct EventStreamSnapshot {
    pub(crate) layout: EventStreamLayout,
    path: PathBuf,
    generation: u64,
}

impl EventStreamSnapshot {
    /// Whether the rotation index is still exactly the one this layout was derived from.
    ///
    /// Appends do not invalidate a snapshot: they only extend the active file past the range the
    /// layout already fixed, and every logical offset below it keeps its meaning. Only rotation
    /// changes what a logical offset means, and every rotation state transition publishes a new
    /// generation, so an unchanged generation proves the layout still holds.
    pub(crate) fn is_still_current(&self) -> io::Result<bool> {
        Ok(rotation_generation(&self.path)? == self.generation)
    }
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

/// Operator-owned, non-semantic storage policy for the guarded transfer.
///
/// Rotation is not only an on/off decision: a cohort boundary is a *safe* place to rotate, not a
/// *worthwhile* one. Without a size threshold every eligible cohort close would archive whatever
/// the active file happens to hold — including a few hundred bytes — so a low-volume project
/// would accumulate one archive segment (and one directory entry) per cohort while gaining
/// nothing. `min_segment_bytes` is therefore part of the policy, not a hidden constant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotationPolicy {
    /// Whether a safe cohort boundary may transfer the active segment at all.
    pub enabled: bool,
    /// Smallest active segment, in bytes, that a safe boundary is allowed to archive. A boundary
    /// reached below this size leaves the active file untouched and retries at the next one.
    pub min_segment_bytes: u64,
}

impl RotationPolicy {
    /// Default threshold for `EVENTS_ROTATION_MIN_BYTES`. Large enough that an ordinary cohort
    /// (kilobytes of events) never rotates on its own, small enough that a long-lived project
    /// still bounds the active file well below the point where a full scan becomes expensive.
    pub const DEFAULT_MIN_SEGMENT_BYTES: u64 = 8 * 1024 * 1024;

    /// The historical single-file behaviour: nothing is ever transferred.
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            min_segment_bytes: Self::DEFAULT_MIN_SEGMENT_BYTES,
        }
    }

    /// Rotate at a safe boundary once the active segment has reached `min_segment_bytes`.
    pub const fn enabled_above(min_segment_bytes: u64) -> Self {
        Self {
            enabled: true,
            min_segment_bytes,
        }
    }

    /// An empty active segment is never archived, so a zero threshold still cannot produce an
    /// empty segment; it only means "every safe boundary with at least one committed byte".
    fn effective_min_segment_bytes(self) -> u64 {
        self.min_segment_bytes.max(1)
    }
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Owns the selected `.work/events.jsonl` location; it does not own the orchestration lease.
#[derive(Debug, Clone)]
pub struct Outbox {
    work: PathBuf,
    rotation: RotationPolicy,
}

impl Outbox {
    pub fn new(work: impl Into<PathBuf>) -> Self {
        Self {
            work: work.into(),
            rotation: RotationPolicy::disabled(),
        }
    }

    /// Construct an outbox whose completed published cohorts are transferred to immutable
    /// archive segments. The default constructor deliberately keeps this policy disabled.
    pub fn with_rotation_policy(work: impl Into<PathBuf>, rotation: RotationPolicy) -> Self {
        Self {
            work: work.into(),
            rotation,
        }
    }

    pub fn set_rotation_policy(&mut self, rotation: RotationPolicy) {
        self.rotation = rotation;
    }

    pub fn path(&self) -> PathBuf {
        self.work.join(OUTBOX_FILE)
    }

    /// Append `event` exactly once by its event id. The event must already carry a deterministic
    /// UUID from [`deterministic_event_id`]; this method never substitutes a random identity on a
    /// replay path.
    pub fn append_idempotent(&self, event: &Event) -> Result<AppendOutcome> {
        let _guard = lock_outbox()?;
        self.append_idempotent_locked(event)
    }

    /// Append while the caller holds [`lock_outbox`]. Keeping the lock boundary separate lets
    /// tests prove cache reuse without unrelated parallel outboxes evicting the bounded index.
    fn append_idempotent_locked(&self, event: &Event) -> Result<AppendOutcome> {
        recover_rotation(&self.work)?;
        let path = self.path();
        let desired = event.to_json_line();
        let mut file = open_plain_outbox(&path)?;
        let file_len = file.metadata()?.len();
        let committed_len = committed_prefix_len(&mut file, file_len)?;
        let layout = event_stream_layout(&path)?;
        let cache_key = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        let mut indexes = OUTBOX_INDEXES
            .lock()
            .map_err(|_| io::Error::other("native outbox index cache is poisoned"))?;
        if !indexes.contains_key(&cache_key) && indexes.len() >= MAX_CACHED_OUTBOXES {
            indexes.clear();
        }
        let index = indexes.entry(cache_key.clone()).or_default();
        if index.archived_len != layout.archived_len || index.committed_len > committed_len {
            *index = CachedIndex::default();
        }
        if let Err(error) = scan_committed_history(&layout, &mut file, index, committed_len) {
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
            self.rotate_after_published_close(event, index)?;
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
        index.active_ids.insert(event.event_id.clone());
        if event.event_type == EventType::CohortPublished
            && let Some(batch_id) = &event.batch_id
        {
            index.published_batches.insert(batch_id.clone());
        }
        self.rotate_after_published_close(event, index)?;
        Ok(AppendOutcome::Appended)
    }

    fn rotate_after_published_close(&self, event: &Event, index: &mut CachedIndex) -> Result<()> {
        if !self.rotation.enabled
            || event.event_type != EventType::CohortClosed
            || !index.active_ids.contains(&event.event_id)
        {
            return Ok(());
        }
        let Some(batch_id) = event.batch_id.as_deref() else {
            return Ok(());
        };
        if !index.published_batches.contains(batch_id) {
            return Ok(());
        }
        let archived = rotate_active_segment(
            &self.work,
            &event.occurred_at,
            self.rotation,
            RotationLimits::DEFAULT,
        )?;
        if archived > 0 {
            index.archived_len = index.archived_len.checked_add(archived).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "archive offset overflowed")
            })?;
            index.committed_len = 0;
            index.active_ids.clear();
        }
        Ok(())
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

fn scan_committed_history(
    layout: &EventStreamLayout,
    file: &mut File,
    index: &mut CachedIndex,
    committed_len: u64,
) -> Result<()> {
    if index.archived_len == 0 && layout.archived_len > 0 {
        for source in layout.sources.iter().filter(|source| source.archived) {
            let mut archived = open_existing_plain_outbox(&source.path)?;
            scan_committed_range(
                &mut archived,
                0,
                source.end_offset - source.start_offset,
                index,
                false,
            )?;
        }
        index.archived_len = layout.archived_len;
    }
    if index.committed_len == committed_len {
        return Ok(());
    }
    let start = index.committed_len;
    scan_committed_range(file, start, committed_len - start, index, true)?;
    index.committed_len = committed_len;
    Ok(())
}

fn scan_committed_range(
    file: &mut File,
    file_offset: u64,
    byte_len: u64,
    index: &mut CachedIndex,
    active: bool,
) -> Result<()> {
    file.seek(SeekFrom::Start(file_offset))?;
    let mut reader = BufReader::new(file);
    let mut position = 0_u64;
    while position < byte_len {
        let remaining = byte_len - position;
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
        if active {
            index.active_ids.insert(existing.event_id.clone());
        }
        if existing.event_type == EventType::CohortPublished
            && let Some(batch_id) = existing.batch_id
        {
            index.published_batches.insert(batch_id);
        }
    }
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

/// Resolve immutable archives plus the current active file into the original monolithic byte
/// address space, together with the index generation that resolution assumed.
///
/// Every consumer that reads bytes through the returned layout must re-confirm the snapshot
/// before it acts on them (see [`EventStreamSnapshot::is_still_current`]).
pub(crate) fn event_stream_snapshot(path: &Path) -> io::Result<EventStreamSnapshot> {
    if path.file_name().and_then(|name| name.to_str()) != Some(OUTBOX_FILE) {
        return Ok(EventStreamSnapshot {
            layout: standalone_layout(path)?,
            path: path.to_path_buf(),
            generation: 0,
        });
    }
    let work = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event outbox has no parent"))?;
    // Resolving a layout means reading the index, then the archive directory, then the active
    // file — three observations of a control plane a rotation may be moving. The index is read
    // first, so a rotation racing this resolution can only add segments the commit pointer does
    // not count yet, and those stay invisible. Everything else is reconciled by requiring the
    // generation to be unchanged across the whole resolution: only then did those three
    // observations belong to one rotation state.
    for _ in 0..MAX_LAYOUT_SNAPSHOT_ATTEMPTS {
        let metadata = load_rotation_metadata(work)?;
        let generation = metadata.generation;
        let resolved = rotation_state_from(work, metadata)
            .and_then(|state| layout_from_state(path, &state))
            .map(|layout| EventStreamSnapshot {
                layout,
                path: path.to_path_buf(),
                generation,
            });
        // A disagreement observed while the index was moving describes a state that never
        // existed; only a stable generation makes it a real integrity failure worth reporting.
        if load_rotation_metadata(work)?.generation != generation {
            continue;
        }
        return resolved;
    }
    Err(io::Error::other(
        "events rotation state kept changing while resolving the stream layout",
    ))
}

/// Resolve the layout alone, for callers that hold the writer interlock and therefore cannot
/// observe a concurrent rotation.
pub(crate) fn event_stream_layout(path: &Path) -> io::Result<EventStreamLayout> {
    Ok(event_stream_snapshot(path)?.layout)
}

fn layout_from_state(path: &Path, state: &RotationState) -> io::Result<EventStreamLayout> {
    let work = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event outbox has no parent"))?;
    let metadata = &state.metadata;
    let mut sources = Vec::with_capacity(state.committed.len() + 1);
    for segment in &state.committed {
        sources.push(EventStreamSource {
            path: work.join(EVENTS_ARCHIVE_DIR).join(&segment.name),
            start_offset: segment.start_offset,
            end_offset: segment.end_offset,
            physical_start: 0,
            archived: true,
        });
    }
    let archived_len = metadata.archived_len;
    // Length and prefix identity both come from one handle. Reading them through two separate
    // opens would let the completion step replace the active file in between, and the digest of
    // the replacement would then be compared against the archived prefix — a spurious integrity
    // failure for a concurrent reader that observed nothing wrong.
    let mut active = match open_existing_plain_outbox(path) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let active_len = match active.as_ref() {
        Some(file) => file.metadata()?.len(),
        None => 0,
    };
    let physical_start = if metadata.active_prefix_bytes == 0 || active_len == 0 {
        0
    } else if active_len >= metadata.active_prefix_bytes {
        let file = active.as_mut().expect("a non-empty active file is open");
        verify_active_prefix(
            file,
            metadata.active_prefix_sha256.as_deref(),
            metadata.active_prefix_bytes,
        )?;
        metadata.active_prefix_bytes
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "events rotation metadata points past the active segment",
        ));
    };
    let active_bytes = active_len.checked_sub(physical_start).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "active event range underflowed")
    })?;
    let logical_len = archived_len.checked_add(active_bytes).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "event stream length overflowed")
    })?;
    if active_bytes > 0 {
        sources.push(EventStreamSource {
            path: path.to_path_buf(),
            start_offset: archived_len,
            end_offset: logical_len,
            physical_start,
            archived: false,
        });
    }
    Ok(EventStreamLayout {
        sources,
        archived_len,
        logical_len,
    })
}

fn standalone_layout(path: &Path) -> io::Result<EventStreamLayout> {
    let len = plain_file_len(path)?.unwrap_or(0);
    let sources = (len > 0)
        .then(|| EventStreamSource {
            path: path.to_path_buf(),
            start_offset: 0,
            end_offset: len,
            physical_start: 0,
            archived: false,
        })
        .into_iter()
        .collect();
    Ok(EventStreamLayout {
        sources,
        archived_len: 0,
        logical_len: len,
    })
}

fn empty_rotation_metadata() -> RotationMetadata {
    RotationMetadata {
        schema_version: ROTATION_SCHEMA_VERSION,
        ..RotationMetadata::default()
    }
}

fn load_rotation_metadata(work: &Path) -> io::Result<RotationMetadata> {
    let path = work.join(EVENTS_ROTATION_FILE);
    // The index has a fixed maximum size by construction, so reading it under that same bound
    // makes a foreign or tampered artifact fail loudly instead of being parsed.
    let text = match work_fs::read_optional_text(
        work,
        &path,
        RotationLimits::DEFAULT.max_metadata_bytes,
    ) {
        Ok(text) => text,
        // A control plane that does not exist yet is the pre-creation state a follow-mode
        // consumer is documented to wait through, not a failure.
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let Some(text) = text else {
        return Ok(empty_rotation_metadata());
    };
    let metadata: RotationMetadata = serde_json::from_str(&text).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("events rotation metadata is invalid: {error}"),
        )
    })?;
    if metadata.schema_version != ROTATION_SCHEMA_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported events rotation metadata version {}",
                metadata.schema_version
            ),
        ));
    }
    Ok(metadata)
}

/// The published generation of `path`'s rotation index, or zero for a stream that cannot rotate.
fn rotation_generation(path: &Path) -> io::Result<u64> {
    if path.file_name().and_then(|name| name.to_str()) != Some(OUTBOX_FILE) {
        return Ok(0);
    }
    let work = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event outbox has no parent"))?;
    Ok(load_rotation_metadata(work)?.generation)
}

fn rotation_metadata_payload(metadata: &RotationMetadata) -> io::Result<Vec<u8>> {
    let mut payload = serde_json::to_vec_pretty(metadata).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("cannot serialize events rotation metadata: {error}"),
        )
    })?;
    payload.push(b'\n');
    Ok(payload)
}

fn write_rotation_metadata(
    work: &Path,
    metadata: &RotationMetadata,
    max_bytes: u64,
) -> io::Result<()> {
    let payload = rotation_metadata_payload(metadata)?;
    work_fs::replace_file(work, &work.join(EVENTS_ROTATION_FILE), &payload, max_bytes)
}

fn next_sequence(current: u64) -> io::Result<u64> {
    current.checked_add(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "events archive segment sequence overflowed",
        )
    })
}

fn next_generation(current: u64) -> io::Result<u64> {
    current.checked_add(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "events rotation generation overflowed",
        )
    })
}

/// A timestamp is informational; it must never be the field that decides whether the fixed-size
/// index still fits, so an unusually long one is dropped instead of being recorded.
fn bounded_rotation_timestamp(occurred_at: &str) -> Option<String> {
    (!occurred_at.is_empty() && occurred_at.len() <= MAX_ROTATION_TIMESTAMP_BYTES)
        .then(|| occurred_at.to_string())
}

fn is_sha256_hex(value: Option<&str>) -> bool {
    value.is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

/// Resolve the index together with the archive directory it points into.
///
/// Everything per-segment comes from the immutable file names, so this is the only place that
/// has to reconcile the two, and it does so strictly: the committed prefix must be exactly
/// sequences `1..=segment_count`, contiguous in logical space, each file exactly as long as its
/// own declared range, ending precisely at the recorded `archived_len`.
fn rotation_state(work: &Path) -> io::Result<RotationState> {
    rotation_state_from(work, load_rotation_metadata(work)?)
}

/// Reconcile an already-read index with the archive directory. Callers that must prove the two
/// belong to the same generation read the index themselves and pass it in.
fn rotation_state_from(work: &Path, metadata: RotationMetadata) -> io::Result<RotationState> {
    let listed = listed_archive_segments(work)?;
    let count = usize::try_from(metadata.segment_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "events rotation commit pointer is out of range",
        )
    })?;
    if listed.len() < count {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "events archive is missing a committed segment",
        ));
    }
    let mut committed = listed;
    let uncommitted = committed.split_off(count);
    let mut expected_sequence = 1_u64;
    let mut expected_start = 0_u64;
    for segment in &committed {
        if segment.sequence != expected_sequence
            || segment.start_offset != expected_start
            || segment.end_offset <= segment.start_offset
            || segment.len != segment.end_offset - segment.start_offset
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid events archive segment {:?}", segment.name),
            ));
        }
        expected_sequence += 1;
        expected_start = segment.end_offset;
    }
    if expected_start != metadata.archived_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "events rotation commit pointer disagrees with its archive segments",
        ));
    }
    if metadata.active_prefix_bytes > 0 {
        let Some(last) = committed.last() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "events rotation has a pending prefix without an archive segment",
            ));
        };
        if metadata.active_prefix_bytes != last.len
            || !is_sha256_hex(metadata.active_prefix_sha256.as_deref())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "events rotation pending prefix disagrees with its archive segment",
            ));
        }
    } else if metadata.active_prefix_sha256.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "events rotation records a prefix digest without a pending prefix",
        ));
    }
    Ok(RotationState {
        metadata,
        committed,
        uncommitted,
    })
}

/// Every archive segment currently visible, ordered by sequence.
///
/// An entry whose name is not exactly a segment name is skipped rather than rejected: in-flight
/// temporaries live in this directory too, and an unrelated file dropped here must not make the
/// whole event stream unreadable. Skipping is safe because a *missing* or renamed committed
/// segment still fails loudly through the contiguity and commit-pointer checks.
fn listed_archive_segments(work: &Path) -> io::Result<Vec<ArchivedSegment>> {
    let archive = work.join(EVENTS_ARCHIVE_DIR);
    let entries = match work_fs::plain_directory_entries(work, &archive) {
        Ok(Some(entries)) => entries,
        // Absent archive directory, or an absent control plane altogether: no archived range.
        Ok(None) => return Ok(Vec::new()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut segments = Vec::new();
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some((sequence, start_offset, end_offset)) = parse_segment_name(&name) else {
            continue;
        };
        // `DirEntry::metadata` does not follow a final symlink, so a redirected segment is
        // rejected here instead of silently sourcing bytes from outside the archive.
        let metadata = entry.metadata()?;
        work_fs::require_plain_file(&archive.join(&name), &metadata)?;
        segments.push(ArchivedSegment {
            name,
            sequence,
            start_offset,
            end_offset,
            len: metadata.len(),
        });
    }
    segments.sort_by_key(|segment| segment.sequence);
    Ok(segments)
}

fn plain_file_len(path: &Path) -> io::Result<Option<u64>> {
    match open_existing_plain_outbox(path) {
        Ok(file) => Ok(Some(file.metadata()?.len())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// An archive segment describes itself: its sequence and its logical byte range are part of its
/// immutable name, which is what lets the index stay a fixed-size commit pointer.
fn segment_name(sequence: u64, start_offset: u64, end_offset: u64) -> String {
    format!(
        "segment_{sequence:0SEGMENT_DIGITS$}_{start_offset:0SEGMENT_DIGITS$}_{end_offset:0SEGMENT_DIGITS$}.jsonl"
    )
}

fn parse_segment_name(name: &str) -> Option<(u64, u64, u64)> {
    let digits = name.strip_prefix("segment_")?.strip_suffix(".jsonl")?;
    let mut fields = digits.split('_');
    let sequence = parse_segment_field(fields.next()?)?;
    let start_offset = parse_segment_field(fields.next()?)?;
    let end_offset = parse_segment_field(fields.next()?)?;
    if fields.next().is_some() {
        return None;
    }
    Some((sequence, start_offset, end_offset))
}

fn parse_segment_field(text: &str) -> Option<u64> {
    if text.len() != SEGMENT_DIGITS || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn recover_rotation(work: &Path) -> io::Result<()> {
    work_fs::ensure_plain_directory(work)?;
    let RotationState {
        mut metadata,
        uncommitted,
        ..
    } = rotation_state(work)?;
    complete_pending_rotation(work, &mut metadata)?;
    if uncommitted.is_empty() {
        return Ok(());
    }

    // A segment is renamed into place immediately before the commit that counts it, so a crash
    // can leave exactly one uncounted segment. Anything else is an archive this writer cannot
    // explain, and guessing would risk publishing or dropping committed bytes.
    let [orphan] = uncommitted.as_slice() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "events archive contains an unresolvable partial rotation",
        ));
    };
    if orphan.sequence != next_sequence(metadata.segment_count)?
        || orphan.start_offset != metadata.archived_len
        || orphan.end_offset <= orphan.start_offset
        || orphan.len != orphan.end_offset - orphan.start_offset
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "events archive contains an unresolvable partial rotation",
        ));
    }

    // The active file is still authoritative in that state; adopt the orphan only after proving
    // byte identity, then run exactly the sequence the interrupted rotation would have run.
    let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(&orphan.name);
    let active_path = work.join(OUTBOX_FILE);
    let active_len = plain_file_len(&active_path)?.unwrap_or(0);
    if active_len < orphan.len || !files_share_prefix(&active_path, &archive_path, orphan.len)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partial archive segment does not match the active event prefix",
        ));
    }
    metadata.generation = next_generation(metadata.generation)?;
    metadata.segment_count = orphan.sequence;
    metadata.archived_len = orphan.end_offset;
    metadata.active_prefix_bytes = orphan.len;
    metadata.active_prefix_sha256 = Some(sha256_file(&archive_path, orphan.len)?);
    write_rotation_metadata(work, &metadata, RotationLimits::DEFAULT.max_metadata_bytes)?;
    complete_pending_rotation(work, &mut metadata)
}

fn complete_pending_rotation(work: &Path, metadata: &mut RotationMetadata) -> io::Result<()> {
    let prefix = metadata.active_prefix_bytes;
    if prefix == 0 {
        return Ok(());
    }
    let active_path = work.join(OUTBOX_FILE);
    let active_len = plain_file_len(&active_path)?.unwrap_or(0);
    // A crash after the replacement leaves an active file that no longer carries the prefix;
    // only the marker has to be cleared then.
    if active_len > 0 {
        if active_len < prefix {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "active event segment is shorter than its pending archived prefix",
            ));
        }
        let mut active = open_existing_plain_outbox(&active_path)?;
        verify_active_prefix(
            &mut active,
            metadata.active_prefix_sha256.as_deref(),
            prefix,
        )?;
        drop(active);
        replace_active_without_prefix(work, &active_path, prefix)?;
    }
    metadata.active_prefix_bytes = 0;
    metadata.active_prefix_sha256 = None;
    metadata.generation = next_generation(metadata.generation)?;
    write_rotation_metadata(work, metadata, RotationLimits::DEFAULT.max_metadata_bytes)
}

/// Placeholder of the exact width of a hexadecimal SHA-256, so the capacity decision below is
/// made on the byte-for-byte payload that will be committed rather than on an estimate.
const PLACEHOLDER_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Transfer the whole committed active segment into the archive, or decline to.
///
/// Returns the number of archived bytes, or zero when the boundary was declined: the segment is
/// too small for the configured policy, or the index could not accept another segment. Declining
/// is always a no-op for the outbox — the active file keeps every byte and the next safe
/// boundary tries again.
fn rotate_active_segment(
    work: &Path,
    occurred_at: &str,
    policy: RotationPolicy,
    limits: RotationLimits,
) -> io::Result<u64> {
    recover_rotation(work)?;
    let active_path = work.join(OUTBOX_FILE);
    let Some(active_len) = plain_file_len(&active_path)? else {
        return Ok(0);
    };
    // A safe boundary is not automatically a worthwhile one: archiving a few hundred bytes would
    // spend an immutable segment and a directory entry per cohort while bounding nothing.
    if active_len < policy.effective_min_segment_bytes() {
        return Ok(0);
    }
    let mut active = open_existing_plain_outbox(&active_path)?;
    if committed_prefix_len(&mut active, active_len)? != active_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "refusing to rotate an unterminated active event segment",
        ));
    }
    let state = rotation_state(work)?;
    if !state.uncommitted.is_empty() || state.metadata.active_prefix_bytes != 0 {
        // Recovery above finishes every in-flight transfer, so anything left here is an archive
        // state this writer must not extend.
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "events archive still has an unfinished rotation",
        ));
    }
    let sequence = next_sequence(state.metadata.segment_count)?;
    let start_offset = state.metadata.archived_len;
    let end_offset = start_offset
        .checked_add(active_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "archive offset overflowed"))?;
    let name = segment_name(sequence, start_offset, end_offset);
    let mut next = state.metadata.clone();
    next.generation = next_generation(next.generation)?;
    next.segment_count = sequence;
    next.archived_len = end_offset;
    next.active_prefix_bytes = active_len;
    next.active_prefix_sha256 = Some(PLACEHOLDER_SHA256.to_string());
    next.last_rotation_at = bounded_rotation_timestamp(occurred_at);

    // Capacity is proved before the segment becomes visible, never after. Renaming first and
    // only then discovering that the index cannot be written would strand the segment: the
    // archived bytes could no longer be described, recovery would keep failing on the same
    // oversized index, and every future append would fail with it. Deferring instead costs
    // nothing but a larger active file.
    let payload = rotation_metadata_payload(&next)?;
    if sequence > limits.max_segments || payload.len() as u64 > limits.max_metadata_bytes {
        return Ok(0);
    }

    let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(&name);
    let digest = copy_segment_atomically(work, &mut active, active_len, &archive_path)?;
    next.active_prefix_sha256 = Some(digest);
    write_rotation_metadata(work, &next, limits.max_metadata_bytes)?;
    complete_pending_rotation(work, &mut next)?;
    Ok(active_len)
}

fn copy_segment_atomically(
    work: &Path,
    active: &mut File,
    len: u64,
    target: &Path,
) -> io::Result<String> {
    work_fs::ensure_plain_parent(work, target)?;
    if fs::symlink_metadata(target).is_ok() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "events archive segment already exists: {}",
                target.display()
            ),
        ));
    }
    let sequence = ROTATION_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = target
        .parent()
        .expect("archive target has parent")
        .join(format!(
            ".segment.{}.{sequence}.tmp.jsonl",
            std::process::id()
        ));
    // Rooted creation, not the bare primitive: the parent proof above is a separate syscall, so
    // only capturing the archive directory's identity around the creation itself closes the window
    // where `.work/events_archive` is swapped for a redirect between the check and the create.
    let mut output = work_fs::create_new_plain_file_rooted(work, &temp)?;
    active.seek(SeekFrom::Start(0))?;
    let result = (|| {
        let mut remaining = len;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        while remaining > 0 {
            let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
            let read = active.read(&mut buffer[..wanted])?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "active event segment ended during rotation",
                ));
            }
            output.write_all(&buffer[..read])?;
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        output.sync_all()?;
        Ok(format!("{:x}", hasher.finalize()))
    })();
    drop(output);
    let digest = match result {
        Ok(digest) => digest,
        Err(error) => {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
    };
    if let Err(error) = fs::rename(&temp, target) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    work_fs::require_plain_file(target, &fs::symlink_metadata(target)?)?;
    sync_directory(target.parent().expect("archive target has parent"))?;
    Ok(digest)
}

fn replace_active_without_prefix(work: &Path, path: &Path, prefix: u64) -> io::Result<()> {
    let mut input = open_existing_plain_outbox(path)?;
    let len = input.metadata()?.len();
    input.seek(SeekFrom::Start(prefix))?;
    let sequence = ROTATION_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = work.join(format!(
        ".{OUTBOX_FILE}.rotation.{}.{sequence}.tmp",
        std::process::id()
    ));
    // The replacement temp lives directly in the work root, so rooted creation is the only proof
    // that the root itself was not redirected while this rotation was writing through it.
    let mut output = work_fs::create_new_plain_file_rooted(work, &temp)?;
    let result = (|| {
        io::copy(&mut input.take(len - prefix), &mut output)?;
        output.sync_all()
    })();
    drop(output);
    if let Err(error) = result {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    work_fs::require_plain_file(path, &fs::symlink_metadata(path)?)?;
    sync_directory(work)
}

fn verify_active_prefix(
    active: &mut File,
    expected_sha256: Option<&str>,
    prefix: u64,
) -> io::Result<()> {
    let expected = expected_sha256.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "pending rotation has no archived prefix digest",
        )
    })?;
    active.seek(SeekFrom::Start(0))?;
    if sha256_stream(active, prefix)? != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "active event prefix differs from its archived segment",
        ));
    }
    Ok(())
}

fn files_share_prefix(left: &Path, right: &Path, len: u64) -> io::Result<bool> {
    Ok(sha256_file(left, len)? == sha256_file(right, len)?)
}

fn sha256_file(path: &Path, len: u64) -> io::Result<String> {
    let mut file = open_existing_plain_outbox(path)?;
    sha256_stream(&mut file, len)
}

fn sha256_stream(file: &mut File, len: u64) -> io::Result<String> {
    let mut remaining = len;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        let read = file.read(&mut buffer[..wanted])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "event segment ended while hashing its declared range",
            ));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(path: &Path) -> io::Result<()> {
    work_fs::require_plain_directory(path)
}

/// Convert a stable semantic key into the legacy contract's standard URL-namespace UUIDv5. The
/// key must be built from durable coordinates (event type, batch/task id, transition/cycle),
/// never a clock or randomly generated run id.
pub fn deterministic_event_id(key: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use serde_json::Map;

    use super::*;
    use crate::events::reader::install_rotation_probe;
    use crate::events::{Actor, ActorKind, Cursor, EventType, SCHEMA_VERSION, TailReader};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temp_work(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-outbox-{label}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn event(key: &str) -> Event {
        event_of_type(key, EventType::CohortOpened)
    }

    fn event_of_type(key: &str, event_type: EventType) -> Event {
        Event {
            schema_version: SCHEMA_VERSION,
            event_id: deterministic_event_id(key),
            occurred_at: "2026-07-24T12:00:00Z".into(),
            event_type,
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

    fn published() -> Event {
        event_of_type("cohort.published|B-1", EventType::CohortPublished)
    }

    fn closed() -> Event {
        event_of_type("cohort.closed|B-1", EventType::CohortClosed)
    }

    fn lifecycle_for_batch(key: &str, event_type: EventType, batch_id: &str) -> Event {
        let mut event = event_of_type(key, event_type);
        event.batch_id = Some(batch_id.into());
        event
    }

    /// Rotation policy for tests that care about the boundary rather than the threshold: any
    /// committed byte is enough. Threshold behaviour has its own dedicated tests.
    fn rotate_every_boundary() -> RotationPolicy {
        RotationPolicy::enabled_above(1)
    }

    fn archived_segments(work: &Path) -> Vec<ArchivedSegment> {
        listed_archive_segments(work).expect("archive directory is readable")
    }

    fn archived_segment_path(work: &Path, sequence: u64) -> PathBuf {
        let segment = archived_segments(work)
            .into_iter()
            .find(|segment| segment.sequence == sequence)
            .unwrap_or_else(|| panic!("archive segment {sequence} exists"));
        work.join(EVENTS_ARCHIVE_DIR).join(segment.name)
    }

    fn padded_event(key: &str, payload_bytes: usize) -> Event {
        let mut event = event(key);
        event
            .payload
            .insert("note".into(), "x".repeat(payload_bytes).into());
        event
    }

    fn cohort_pair(batch_id: &str) -> (Event, Event) {
        (
            lifecycle_for_batch(
                &format!("published|{batch_id}"),
                EventType::CohortPublished,
                batch_id,
            ),
            lifecycle_for_batch(
                &format!("closed|{batch_id}"),
                EventType::CohortClosed,
                batch_id,
            ),
        )
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
    fn rotation_is_opt_in_and_requires_a_published_cohort_close() {
        let disabled_work = temp_work("rotation-disabled");
        let disabled = Outbox::new(&disabled_work);
        disabled.append_idempotent(&published()).unwrap();
        disabled.append_idempotent(&closed()).unwrap();
        assert!(!disabled_work.join(EVENTS_ROTATION_FILE).exists());
        assert_eq!(
            fs::read_to_string(disabled.path()).unwrap().lines().count(),
            2
        );

        let unpublished_work = temp_work("rotation-unpublished");
        let unpublished = Outbox::with_rotation_policy(&unpublished_work, rotate_every_boundary());
        unpublished.append_idempotent(&closed()).unwrap();
        assert!(!unpublished_work.join(EVENTS_ROTATION_FILE).exists());
        assert_eq!(
            fs::read_to_string(unpublished.path())
                .unwrap()
                .lines()
                .count(),
            1
        );

        let _ = fs::remove_dir_all(disabled_work);
        let _ = fs::remove_dir_all(unpublished_work);
    }

    #[test]
    fn an_absent_control_plane_reads_as_an_empty_rotated_stream() {
        // A follow-mode consumer may be started before the writer creates `.work` at all; the
        // archive-aware layout must wait for creation exactly like the single-file reader did.
        let work = temp_work("absent-control-plane");
        let path = work.join(OUTBOX_FILE);
        let mut reader = TailReader::new(&path);
        assert!(reader.poll_all().unwrap().is_empty());
        assert_eq!(reader.cursor().byte_offset, 0);

        let first = event("appears-later");
        Outbox::new(&work).append_idempotent(&first).unwrap();
        assert_eq!(reader.poll_all().unwrap(), vec![first]);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_safe_boundary_below_the_configured_threshold_does_not_rotate() {
        let work = temp_work("rotation-threshold");
        let threshold = 4096_u64;
        let outbox = Outbox::with_rotation_policy(&work, RotationPolicy::enabled_above(threshold));
        let small = event("threshold-small");
        let (published_one, closed_one) = cohort_pair("B-1");
        for event in [&small, &published_one, &closed_one] {
            outbox.append_idempotent(event).unwrap();
        }

        // The boundary was safe; the segment was simply not worth an immutable archive entry.
        let active = fs::read(outbox.path()).unwrap();
        assert!(!active.is_empty() && (active.len() as u64) < threshold);
        assert!(!work.join(EVENTS_ROTATION_FILE).exists());
        assert!(archived_segments(&work).is_empty());

        // Crossing the threshold makes the next safe boundary rotate exactly once.
        let bulk = padded_event("threshold-bulk", threshold as usize);
        let (published_two, closed_two) = cohort_pair("B-2");
        for event in [&bulk, &published_two, &closed_two] {
            outbox.append_idempotent(event).unwrap();
        }
        assert_eq!(archived_segments(&work).len(), 1);
        assert!(fs::read(outbox.path()).unwrap().is_empty());
        assert_eq!(
            TailReader::new(outbox.path()).poll_all().unwrap(),
            vec![
                small,
                published_one,
                closed_one,
                bulk,
                published_two,
                closed_two
            ]
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn the_rotation_index_stays_a_fixed_size_across_many_rotations() {
        let work = temp_work("rotation-index-bounded");
        let outbox = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        let rotations = 20;
        let mut expected = Vec::new();
        let mut first_index_len = None;
        for cohort in 0..rotations {
            let (published, closed) = cohort_pair(&format!("B-{cohort}"));
            for event in [published, closed] {
                outbox.append_idempotent(&event).unwrap();
                expected.push(event);
            }
            let index_len = fs::metadata(work.join(EVENTS_ROTATION_FILE)).unwrap().len();
            assert!(
                index_len <= RotationLimits::DEFAULT.max_metadata_bytes,
                "rotation {cohort} produced a {index_len}-byte index"
            );
            let first = *first_index_len.get_or_insert(index_len);
            assert!(
                index_len <= first + 64,
                "the index must not grow with the number of rotations: {first} -> {index_len}"
            );
        }

        // Bounded metadata is only worth anything if the archive it points at is still complete.
        assert_eq!(archived_segments(&work).len(), rotations as usize);
        assert_eq!(
            load_rotation_metadata(&work).unwrap().segment_count,
            rotations
        );
        assert_eq!(TailReader::new(outbox.path()).poll_all().unwrap(), expected);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn the_worst_case_rotation_index_fits_the_capacity_it_is_checked_against() {
        // Every field is fixed-width by construction, so the capacity check in the rotation path
        // can never be the thing that fails after a segment has been published.
        let payload = rotation_metadata_payload(&RotationMetadata {
            schema_version: u32::MAX,
            generation: u64::MAX,
            segment_count: u64::MAX,
            archived_len: u64::MAX,
            active_prefix_bytes: u64::MAX,
            active_prefix_sha256: Some(PLACEHOLDER_SHA256.into()),
            last_rotation_at: Some("z".repeat(MAX_ROTATION_TIMESTAMP_BYTES)),
        })
        .unwrap();
        assert!(
            (payload.len() as u64) < RotationLimits::DEFAULT.max_metadata_bytes,
            "worst-case index is {} bytes",
            payload.len()
        );
        assert_eq!(
            bounded_rotation_timestamp(&"z".repeat(MAX_ROTATION_TIMESTAMP_BYTES + 1)),
            None,
            "one caller-supplied string must not decide whether the index fits"
        );
    }

    #[test]
    fn rotation_defers_instead_of_publishing_a_segment_it_cannot_index() {
        let work = temp_work("rotation-capacity-defer");
        let outbox = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        let first = event("capacity-first");
        let (published_one, closed_one) = cohort_pair("B-1");
        for event in [&first, &published_one, &closed_one] {
            outbox.append_idempotent(event).unwrap();
        }
        assert_eq!(archived_segments(&work).len(), 1);

        let after = event("capacity-after");
        outbox.append_idempotent(&after).unwrap();
        let index_before = fs::read(work.join(EVENTS_ROTATION_FILE)).unwrap();
        let active_before = fs::read(outbox.path()).unwrap();

        // A boundary that would need a second segment while the archive may hold only one, and a
        // boundary whose index could not be written at all. Both must decline *before* the
        // segment becomes visible, because a published segment whose index cannot follow it can
        // never be described again — and every later append would fail on that same index.
        for limits in [
            RotationLimits {
                max_segments: 1,
                ..RotationLimits::DEFAULT
            },
            RotationLimits {
                max_metadata_bytes: 8,
                ..RotationLimits::DEFAULT
            },
        ] {
            assert_eq!(
                rotate_active_segment(
                    &work,
                    "2026-07-24T12:00:00Z",
                    rotate_every_boundary(),
                    limits
                )
                .unwrap(),
                0,
                "a boundary the index cannot absorb must be declined, not attempted"
            );
        }

        assert_eq!(archived_segments(&work).len(), 1);
        assert_eq!(
            fs::read_dir(work.join(EVENTS_ARCHIVE_DIR)).unwrap().count(),
            1,
            "a declined boundary must not leave a published segment or a stray temporary"
        );
        assert_eq!(
            fs::read(work.join(EVENTS_ROTATION_FILE)).unwrap(),
            index_before
        );
        assert_eq!(fs::read(outbox.path()).unwrap(), active_before);

        // Degrading means "a larger active file", never "a stalled outbox".
        let later = event("capacity-later");
        assert_eq!(
            outbox.append_idempotent(&later).unwrap(),
            AppendOutcome::Appended
        );
        assert_eq!(
            TailReader::new(outbox.path()).poll_all().unwrap(),
            vec![first, published_one, closed_one, after, later]
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn an_oversized_rotation_index_is_rejected_instead_of_parsed() {
        let work = temp_work("rotation-index-oversized");
        fs::create_dir_all(&work).unwrap();
        let mut payload = String::from("{\"schema_version\":1,\"padding\":\"");
        payload.push_str(&"x".repeat(RotationLimits::DEFAULT.max_metadata_bytes as usize));
        payload.push_str("\"}\n");
        fs::write(work.join(EVENTS_ROTATION_FILE), payload).unwrap();
        let error = Outbox::new(&work)
            .append_idempotent(&event("after-oversized-index"))
            .unwrap_err();
        assert!(
            error.to_string().contains("exceeds"),
            "an index beyond its own bound must fail closed: {error}"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn cursor_resumes_across_rotation_and_archive_to_active_boundary() {
        let work = temp_work("cursor-rotation");
        let first = event("first-before-rotation");
        Outbox::new(&work).append_idempotent(&first).unwrap();
        let mut initial = TailReader::new(work.join(OUTBOX_FILE));
        assert_eq!(initial.poll_all().unwrap(), vec![first]);
        let before_rotation = initial.cursor();

        let rotating = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        rotating.append_idempotent(&published()).unwrap();
        rotating.append_idempotent(&closed()).unwrap();
        assert_eq!(fs::read(work.join(OUTBOX_FILE)).unwrap(), b"");
        assert!(archived_segment_path(&work, 1).is_file());

        let mut resumed = TailReader::with_cursor(work.join(OUTBOX_FILE), &before_rotation);
        assert_eq!(resumed.poll_all().unwrap(), vec![published(), closed()]);
        let after_rotation = resumed.cursor();

        let active = event("first-after-rotation");
        rotating.append_idempotent(&active).unwrap();
        let mut resumed_again = TailReader::with_cursor(work.join(OUTBOX_FILE), &after_rotation);
        assert_eq!(resumed_again.poll_all().unwrap(), vec![active]);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn repeated_rotations_preserve_sorted_contiguous_segment_order() {
        let work = temp_work("multiple-rotations");
        let outbox = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        let published_one = published();
        let closed_one = closed();
        let opened_two = lifecycle_for_batch("open-B-2", EventType::CohortOpened, "B-2");
        let published_two = lifecycle_for_batch("published-B-2", EventType::CohortPublished, "B-2");
        let closed_two = lifecycle_for_batch("closed-B-2", EventType::CohortClosed, "B-2");
        for event in [
            &published_one,
            &closed_one,
            &opened_two,
            &published_two,
            &closed_two,
        ] {
            outbox.append_idempotent(event).unwrap();
        }

        let metadata = load_rotation_metadata(&work).unwrap();
        let segments = archived_segments(&work);
        assert_eq!(metadata.segment_count, 2);
        assert_eq!(
            segments
                .iter()
                .map(|segment| segment.name.as_str())
                .collect::<Vec<_>>(),
            [
                segment_name(1, 0, segments[0].end_offset),
                segment_name(2, segments[0].end_offset, segments[1].end_offset)
            ]
        );
        assert_eq!(segments[0].start_offset, 0);
        assert_eq!(segments[0].end_offset, segments[1].start_offset);
        assert_eq!(metadata.archived_len, segments[1].end_offset);
        assert_eq!(
            TailReader::new(outbox.path()).poll_all().unwrap(),
            vec![
                published_one,
                closed_one,
                opened_two,
                published_two,
                closed_two
            ]
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn deduplication_and_collision_detection_span_archived_history() {
        let work = temp_work("archive-dedup");
        let rotating = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        let first = event("archived-id");
        rotating.append_idempotent(&first).unwrap();
        rotating.append_idempotent(&published()).unwrap();
        rotating.append_idempotent(&closed()).unwrap();

        // Model a process restart: correctness must come from archived bytes, not the warm
        // process-local semantic index populated before rotation.
        {
            let _guard = lock_outbox().unwrap();
            let key = fs::canonicalize(rotating.path()).unwrap();
            OUTBOX_INDEXES.lock().unwrap().remove(&key);
        }

        assert_eq!(
            rotating.append_idempotent(&first).unwrap(),
            AppendOutcome::AlreadyPresent
        );
        let mut collision = first;
        collision.payload.insert("different".into(), true.into());
        assert!(matches!(
            rotating.append_idempotent(&collision),
            Err(OutboxError::EventIdCollision { .. })
        ));
        assert!(fs::read(work.join(OUTBOX_FILE)).unwrap().is_empty());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn recovery_adopts_archive_published_before_metadata_without_duplication() {
        let work = temp_work("orphan-recovery");
        let outbox = Outbox::new(&work);
        let first = event("orphan-first");
        outbox.append_idempotent(&first).unwrap();
        outbox.append_idempotent(&published()).unwrap();
        let active_path = outbox.path();
        let mut active = open_existing_plain_outbox(&active_path).unwrap();
        let len = active.metadata().unwrap().len();
        let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(segment_name(1, 0, len));
        copy_segment_atomically(&work, &mut active, len, &archive_path).unwrap();
        drop(active);
        assert!(!work.join(EVENTS_ROTATION_FILE).exists());

        let after = event("after-orphan-recovery");
        assert_eq!(
            outbox.append_idempotent(&after).unwrap(),
            AppendOutcome::Appended
        );
        let ids = TailReader::new(&active_path)
            .poll_all()
            .unwrap()
            .into_iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![first.event_id, published().event_id, after.event_id]
        );
        let metadata = load_rotation_metadata(&work).unwrap();
        assert_eq!(metadata.active_prefix_bytes, 0);
        assert_eq!(metadata.segment_count, 1);
        assert_eq!(archived_segments(&work).len(), 1);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn reader_sees_one_copy_while_metadata_precedes_active_replacement() {
        let work = temp_work("pending-reader");
        let outbox = Outbox::new(&work);
        let first = event("pending-first");
        outbox.append_idempotent(&first).unwrap();
        let active_path = outbox.path();
        let mut active = open_existing_plain_outbox(&active_path).unwrap();
        let len = active.metadata().unwrap().len();
        let name = segment_name(1, 0, len);
        let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(&name);
        let digest = copy_segment_atomically(&work, &mut active, len, &archive_path).unwrap();
        drop(active);
        write_rotation_metadata(
            &work,
            &RotationMetadata {
                schema_version: ROTATION_SCHEMA_VERSION,
                generation: 1,
                segment_count: 1,
                archived_len: len,
                active_prefix_bytes: len,
                active_prefix_sha256: Some(digest),
                last_rotation_at: None,
            },
            RotationLimits::DEFAULT.max_metadata_bytes,
        )
        .unwrap();

        assert_eq!(
            TailReader::new(&active_path).poll_all().unwrap(),
            vec![first.clone()]
        );
        let after = event("after-pending-recovery");
        outbox.append_idempotent(&after).unwrap();
        assert_eq!(
            TailReader::new(&active_path).poll_all().unwrap(),
            vec![first, after]
        );
        let _ = fs::remove_dir_all(work);
    }

    /// The rotation write path creates its segment through the same rooted primitive as
    /// `work_fs::replace_file`, so a `.work/events_archive` swapped for a redirect *after* the
    /// parent proof and *before* the creation must be refused. Without the rooted creation the
    /// archive directory identity is only checked once, and the immutable segment is published
    /// into whatever the redirect points at.
    #[test]
    fn an_archive_parent_swapped_under_the_segment_temp_publishes_no_segment() {
        let work = temp_work("rotation-archive-parent-swap");
        let external = work.join("external");
        let archive = work.join(EVENTS_ARCHIVE_DIR);
        let displaced = work.join("events_archive-original");
        let outbox = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        outbox.append_idempotent(&published()).unwrap();
        fs::create_dir_all(&external).unwrap();

        let hook_archive = archive.clone();
        let hook_displaced = displaced.clone();
        let hook_external = external.clone();
        work_fs::set_create_new_plain_file_hook(move || {
            fs::rename(&hook_archive, &hook_displaced)?;
            work_fs::symlink_directory_for_test(&hook_external, &hook_archive)
        });

        let error = match outbox.append_idempotent(&closed()) {
            Err(OutboxError::Io(error)) => error,
            other => panic!("a redirected archive parent must fail the rotation: {other:?}"),
        };
        assert!(
            matches!(
                work_fs::plain_read_violation(&error),
                Some(work_fs::PlainReadViolation::ParentNotPlain { path }) if path == &archive
            ),
            "unexpected rotation error: {error}"
        );
        let redirected_entries = fs::read_dir(&external)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            redirected_entries
                .iter()
                .all(|name| name.starts_with(".segment.")),
            "no immutable segment may be published through the redirect: {redirected_entries:?}"
        );
        assert!(
            !redirected_entries.is_empty(),
            "the fixture must exercise creation through the redirected parent"
        );
        assert!(!work.join(EVENTS_ROTATION_FILE).exists());

        // Restore a plain archive directory before reading: the stream reader walks it too, and
        // this assertion is about the events the refused rotation preserved, not the redirect.
        work_fs::remove_directory_link_for_test(&archive).unwrap();
        fs::rename(&displaced, &archive).unwrap();
        assert_eq!(
            TailReader::new(outbox.path()).poll_all().unwrap(),
            vec![published(), closed()],
            "a refused rotation keeps every byte in the active segment"
        );
        assert!(archived_segments(&work).is_empty());
        let _ = fs::remove_dir_all(work);
    }

    /// The active-file replacement writes its temporary directly into the work root, so the root
    /// itself is the parent chain that has to survive the creation. A root swapped inside that
    /// window must be refused before the replacement can be renamed through the redirect.
    ///
    /// Unix-only fixture, not a Unix-only property: this replacement holds the active segment open
    /// while it writes, and Windows refuses to rename a directory that owns an open descendant
    /// handle, so the redirect cannot be installed there at all. The archive-parent proof above
    /// exercises the same rooted primitive on every platform.
    #[cfg(unix)]
    #[test]
    fn a_work_root_swapped_under_the_active_replacement_is_rejected() {
        let work = temp_work("rotation-active-root-swap");
        let displaced = temp_work("rotation-active-root-displaced");
        let external = temp_work("rotation-active-root-external");
        fs::create_dir_all(&external).unwrap();
        let outbox = Outbox::new(&work);
        let first = event("pending-prefix");
        outbox.append_idempotent(&first).unwrap();
        let active_path = outbox.path();
        let mut active = open_existing_plain_outbox(&active_path).unwrap();
        let len = active.metadata().unwrap().len();
        let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(segment_name(1, 0, len));
        let digest = copy_segment_atomically(&work, &mut active, len, &archive_path).unwrap();
        drop(active);
        let mut metadata = RotationMetadata {
            schema_version: ROTATION_SCHEMA_VERSION,
            generation: 1,
            segment_count: 1,
            archived_len: len,
            active_prefix_bytes: len,
            active_prefix_sha256: Some(digest),
            last_rotation_at: None,
        };
        write_rotation_metadata(&work, &metadata, RotationLimits::DEFAULT.max_metadata_bytes)
            .unwrap();

        let hook_work = work.clone();
        let hook_displaced = displaced.clone();
        let hook_external = external.clone();
        work_fs::set_create_new_plain_file_hook(move || {
            fs::rename(&hook_work, &hook_displaced)?;
            work_fs::symlink_directory_for_test(&hook_external, &hook_work)
        });

        let error = complete_pending_rotation(&work, &mut metadata).unwrap_err();
        assert!(
            matches!(
                work_fs::plain_read_violation(&error),
                Some(work_fs::PlainReadViolation::ParentNotPlain { path }) if path == &work
            ),
            "unexpected pending-rotation error: {error}"
        );
        let redirected_entries = fs::read_dir(&external)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            !redirected_entries.iter().any(|name| name == OUTBOX_FILE),
            "no active segment may be renamed into place through the redirect: {redirected_entries:?}"
        );
        assert!(
            !redirected_entries.is_empty(),
            "the fixture must exercise creation through the redirected root"
        );
        assert_eq!(
            fs::read_to_string(displaced.join(OUTBOX_FILE)).unwrap(),
            format!("{}\n", first.to_json_line()),
            "the real active segment keeps its archived prefix after a refused replacement"
        );

        work_fs::remove_directory_link_for_test(&work).unwrap();
        let _ = fs::remove_dir_all(displaced);
        let _ = fs::remove_dir_all(external);
    }

    #[test]
    fn torn_tail_repair_remains_confined_to_the_active_segment_after_rotation() {
        let work = temp_work("archive-torn-active");
        let rotating = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        rotating.append_idempotent(&published()).unwrap();
        rotating.append_idempotent(&closed()).unwrap();
        let archive_path = archived_segment_path(&work, 1);
        let archived = fs::read(&archive_path).unwrap();

        let committed = event("active-committed");
        fs::write(
            rotating.path(),
            format!("{}\n{{\"schema_version\":", committed.to_json_line()),
        )
        .unwrap();
        let appended = event("active-after-torn");
        rotating.append_idempotent(&appended).unwrap();
        assert_eq!(
            fs::read(&archive_path).unwrap(),
            archived,
            "immutable archive bytes must not participate in torn-tail repair"
        );
        let events = TailReader::new(rotating.path()).poll_all().unwrap();
        assert_eq!(events, vec![published(), closed(), committed, appended]);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_rotation_between_a_polls_snapshot_and_its_read_never_loses_or_reorders_events() {
        let work = temp_work("poll-rotation-race");
        let outbox = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        let first = event("race-first");
        let second = event("race-second");
        outbox.append_idempotent(&first).unwrap();
        outbox.append_idempotent(&second).unwrap();

        let (published_one, closed_one) = cohort_pair("B-1");
        // Uniform-length records, so the bytes the reader is about to read at the pre-rotation
        // offsets are exactly two *complete* post-rotation records: a reader that trusted its
        // stale layout would deliver them first and leave its cursor inside the archived range,
        // silently dropping everything that was archived.
        let after_one = event("race-after-one");
        let after_two = event("race-after-two");
        let after_three = event("race-after-three");
        let path = work.join(OUTBOX_FILE);
        {
            let rotating = outbox.clone();
            let interleaved = [
                published_one.clone(),
                closed_one.clone(),
                after_one.clone(),
                after_two.clone(),
                after_three.clone(),
            ];
            install_rotation_probe(&path, move || {
                for event in &interleaved {
                    rotating.append_idempotent(event).unwrap();
                }
            });
        }

        let mut reader = TailReader::new(&path);
        let delivered = reader.poll_all().unwrap();
        assert!(
            !archived_segments(&work).is_empty(),
            "the probe must really have rotated inside the poll"
        );
        assert_eq!(
            delivered,
            vec![
                first,
                second,
                published_one,
                closed_one,
                after_one,
                after_two,
                after_three
            ]
        );
        assert_eq!(reader.stats().skipped_dup, 0);
        assert_eq!(reader.stats().skipped_invalid, 0);
        assert_eq!(
            reader.cursor().byte_offset,
            event_stream_layout(&path).unwrap().logical_len,
            "the cursor must end at the logical end of the rotated stream"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_resumed_cursor_survives_a_rotation_racing_its_poll() {
        let work = temp_work("cursor-rotation-race");
        let outbox = Outbox::with_rotation_policy(&work, rotate_every_boundary());
        let delivered_before = event("resume-delivered");
        outbox.append_idempotent(&delivered_before).unwrap();
        let path = work.join(OUTBOX_FILE);
        let mut initial = TailReader::new(&path);
        assert_eq!(initial.poll_all().unwrap(), vec![delivered_before.clone()]);
        let cursor = Cursor::from_json(&initial.cursor().to_json()).unwrap();

        let pending = event("resume-pending");
        outbox.append_idempotent(&pending).unwrap();
        let (published_one, closed_one) = cohort_pair("B-1");
        let after = event("resume-after-rotation");
        {
            let rotating = outbox.clone();
            let interleaved = [published_one.clone(), closed_one.clone(), after.clone()];
            install_rotation_probe(&path, move || {
                for event in &interleaved {
                    rotating.append_idempotent(event).unwrap();
                }
            });
        }

        let mut resumed = TailReader::with_cursor(&path, &cursor);
        assert_eq!(
            resumed.poll_all().unwrap(),
            vec![pending, published_one, closed_one, after]
        );
        assert_eq!(
            resumed.stats().skipped_dup,
            0,
            "a cursor resolved through the archive must not re-read what it already delivered"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn a_live_reader_keeps_exact_order_while_the_writer_rotates_concurrently() {
        let work = temp_work("concurrent-rotation");
        let path = work.join(OUTBOX_FILE);
        let cohorts = 12;
        let expected = (0..cohorts)
            .flat_map(|cohort| {
                let batch = format!("B-{cohort}");
                let opened = lifecycle_for_batch(
                    &format!("opened|{batch}"),
                    EventType::CohortOpened,
                    &batch,
                );
                let (published, closed) = cohort_pair(&batch);
                [opened, published, closed]
            })
            .collect::<Vec<_>>();

        let done = Arc::new(AtomicBool::new(false));
        let writer = {
            let outbox = Outbox::with_rotation_policy(&work, rotate_every_boundary());
            let events = expected.clone();
            let done = Arc::clone(&done);
            thread::spawn(move || {
                for event in &events {
                    outbox.append_idempotent(event).unwrap();
                }
                done.store(true, Ordering::Release);
            })
        };

        let mut reader = TailReader::new(&path);
        let mut delivered = Vec::new();
        while !done.load(Ordering::Acquire) {
            delivered.extend(reader.poll_all().unwrap());
        }
        writer.join().unwrap();
        delivered.extend(reader.poll_all().unwrap());

        assert_eq!(delivered, expected);
        assert_eq!(reader.stats().skipped_dup, 0);
        assert_eq!(reader.stats().skipped_invalid, 0);
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
        let observations = (|| -> Result<_> {
            let _outbox_guard = lock_outbox()?;
            let first_outcome = outbox.append_idempotent_locked(&event("new-after-index"))?;
            let key = fs::canonicalize(outbox.path())?;
            let scanned_bytes = || -> io::Result<u64> {
                OUTBOX_INDEXES
                    .lock()
                    .map_err(|_| io::Error::other("native outbox index cache is poisoned"))?
                    .get(&key)
                    .map(|index| index.scanned_bytes)
                    .ok_or_else(|| io::Error::other("native outbox index cache entry is missing"))
            };
            let scanned_after_index = scanned_bytes()?;
            let second_outcome =
                Outbox::new(&work).append_idempotent_locked(&event("second-after-index"))?;
            Ok((
                first_outcome,
                scanned_after_index,
                second_outcome,
                scanned_bytes()?,
            ))
        })()
        .unwrap();
        assert_eq!(observations.0, AppendOutcome::Appended);
        assert_eq!(observations.1, journal.len() as u64);
        assert_eq!(observations.2, AppendOutcome::Appended);
        assert_eq!(
            observations.3, observations.1,
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
