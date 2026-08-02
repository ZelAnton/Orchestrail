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
use crate::work_fs::{self, MAX_CONTROL_BYTES};

static OUTBOX_ACCESS: Mutex<()> = Mutex::new(());

/// A committed event record may not force an unbounded allocation while the one-time index scan
/// is catching up. This matches the tail reader's per-record ceiling.
const MAX_EVENT_LINE_BYTES: u64 = 1024 * 1024;
const MAX_CACHED_OUTBOXES: usize = 16;
const ROTATION_SCHEMA_VERSION: u32 = 1;
const SEGMENT_DIGITS: usize = 20;

/// Immutable segment directory relative to the selected `.work` directory.
pub const EVENTS_ARCHIVE_DIR: &str = "events_archive";
/// Atomic logical-range map relative to the selected `.work` directory.
pub const EVENTS_ROTATION_FILE: &str = "events_rotation.json";

static ROTATION_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RotationMetadata {
    schema_version: u32,
    segments: Vec<ArchivedSegment>,
    /// Non-zero only between publishing the archive map and atomically replacing the active
    /// file. Readers skip this many duplicated bytes while the transfer is in that state.
    #[serde(default)]
    active_prefix_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_rotation_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ArchivedSegment {
    name: String,
    start_offset: u64,
    end_offset: u64,
    sha256: String,
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
    rotation_enabled: bool,
}

impl Outbox {
    pub fn new(work: impl Into<PathBuf>) -> Self {
        Self {
            work: work.into(),
            rotation_enabled: false,
        }
    }

    /// Construct an outbox whose completed published cohorts are transferred to immutable
    /// archive segments. The default constructor deliberately keeps this policy disabled.
    pub fn with_rotation_enabled(work: impl Into<PathBuf>, rotation_enabled: bool) -> Self {
        Self {
            work: work.into(),
            rotation_enabled,
        }
    }

    pub fn set_rotation_enabled(&mut self, enabled: bool) {
        self.rotation_enabled = enabled;
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
        if !self.rotation_enabled
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
        let archived = rotate_active_segment(&self.work, &event.occurred_at)?;
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
/// address space. A path not named `events.jsonl` retains the historical single-file behaviour,
/// which keeps embedders and file-local tests independent of `.work` layout conventions.
pub(crate) fn event_stream_layout(path: &Path) -> io::Result<EventStreamLayout> {
    if path.file_name().and_then(|name| name.to_str()) != Some(OUTBOX_FILE) {
        return standalone_layout(path);
    }
    let work = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event outbox has no parent"))?;
    let metadata = load_rotation_metadata(work)?;
    validate_rotation_metadata(work, &metadata)?;

    let mut sources = Vec::with_capacity(metadata.segments.len() + 1);
    for segment in &metadata.segments {
        sources.push(EventStreamSource {
            path: work.join(EVENTS_ARCHIVE_DIR).join(&segment.name),
            start_offset: segment.start_offset,
            end_offset: segment.end_offset,
            physical_start: 0,
            archived: true,
        });
    }
    let archived_len = metadata
        .segments
        .last()
        .map_or(0, |segment| segment.end_offset);
    let active_len = plain_file_len(path)?.unwrap_or(0);
    let physical_start = if metadata.active_prefix_bytes == 0 || active_len == 0 {
        0
    } else if active_len >= metadata.active_prefix_bytes {
        verify_active_prefix(path, metadata.segments.last(), metadata.active_prefix_bytes)?;
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
    let Some(text) = work_fs::read_optional_text(work, &path, MAX_CONTROL_BYTES)? else {
        return Ok(empty_rotation_metadata());
    };
    serde_json::from_str(&text).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("events rotation metadata is invalid: {error}"),
        )
    })
}

fn write_rotation_metadata(work: &Path, metadata: &RotationMetadata) -> io::Result<()> {
    let mut payload = serde_json::to_vec_pretty(metadata).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("cannot serialize events rotation metadata: {error}"),
        )
    })?;
    payload.push(b'\n');
    work_fs::replace_file(
        work,
        &work.join(EVENTS_ROTATION_FILE),
        &payload,
        MAX_CONTROL_BYTES,
    )
}

fn validate_rotation_metadata(work: &Path, metadata: &RotationMetadata) -> io::Result<()> {
    if metadata.schema_version != ROTATION_SCHEMA_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported events rotation metadata version {}",
                metadata.schema_version
            ),
        ));
    }
    let mut expected_start = 0_u64;
    for (position, segment) in metadata.segments.iter().enumerate() {
        let expected_name = segment_name(position + 1);
        if segment.name != expected_name
            || segment.start_offset != expected_start
            || segment.end_offset <= segment.start_offset
            || segment.sha256.len() != 64
            || !segment.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid events archive segment metadata for {:?}",
                    segment.name
                ),
            ));
        }
        let path = work.join(EVENTS_ARCHIVE_DIR).join(&segment.name);
        let len = plain_file_len(&path)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("events archive segment is missing: {}", path.display()),
            )
        })?;
        if len != segment.end_offset - segment.start_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("events archive segment length changed: {}", path.display()),
            ));
        }
        expected_start = segment.end_offset;
    }
    if metadata.active_prefix_bytes > 0 {
        let Some(last) = metadata.segments.last() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "events rotation has a pending prefix without an archive segment",
            ));
        };
        if metadata.active_prefix_bytes != last.end_offset - last.start_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "events rotation pending prefix disagrees with its archive segment",
            ));
        }
    }
    Ok(())
}

fn plain_file_len(path: &Path) -> io::Result<Option<u64>> {
    match open_existing_plain_outbox(path) {
        Ok(file) => Ok(Some(file.metadata()?.len())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn segment_name(sequence: usize) -> String {
    format!("segment_{sequence:0SEGMENT_DIGITS$}.jsonl")
}

fn recover_rotation(work: &Path) -> io::Result<()> {
    work_fs::ensure_plain_directory(work)?;
    let mut metadata = load_rotation_metadata(work)?;
    validate_rotation_metadata(work, &metadata)?;
    complete_pending_rotation(work, &mut metadata)?;

    metadata = load_rotation_metadata(work)?;
    validate_rotation_metadata(work, &metadata)?;
    let finals = archive_final_names(work)?;
    let referenced = metadata
        .segments
        .iter()
        .map(|segment| segment.name.as_str())
        .collect::<HashSet<_>>();
    let extras = finals
        .iter()
        .filter(|name| !referenced.contains(name.as_str()))
        .collect::<Vec<_>>();
    if extras.is_empty() {
        return Ok(());
    }
    let expected = segment_name(metadata.segments.len() + 1);
    if extras.len() != 1 || extras[0].as_str() != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "events archive contains an unresolvable partial rotation",
        ));
    }

    // A final segment can become visible just before the metadata replacement. The active file
    // is still authoritative in that state; adopt the orphan only after proving byte identity.
    let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(&expected);
    let archive_len = plain_file_len(&archive_path)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "partial archive segment disappeared",
        )
    })?;
    if archive_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partial archive segment is empty",
        ));
    }
    let active_path = work.join(OUTBOX_FILE);
    let active_len = plain_file_len(&active_path)?.unwrap_or(0);
    if active_len < archive_len || !files_share_prefix(&active_path, &archive_path, archive_len)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partial archive segment does not match the active event prefix",
        ));
    }
    let start_offset = metadata
        .segments
        .last()
        .map_or(0, |segment| segment.end_offset);
    let end_offset = start_offset
        .checked_add(archive_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "archive offset overflowed"))?;
    metadata.segments.push(ArchivedSegment {
        name: expected,
        start_offset,
        end_offset,
        sha256: sha256_file(&archive_path, archive_len)?,
    });
    metadata.active_prefix_bytes = archive_len;
    write_rotation_metadata(work, &metadata)?;
    complete_pending_rotation(work, &mut metadata)
}

fn complete_pending_rotation(work: &Path, metadata: &mut RotationMetadata) -> io::Result<()> {
    let prefix = metadata.active_prefix_bytes;
    if prefix == 0 {
        return Ok(());
    }
    let active_path = work.join(OUTBOX_FILE);
    let active_len = plain_file_len(&active_path)?.unwrap_or(0);
    if active_len > 0 {
        if active_len < prefix {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "active event segment is shorter than its pending archived prefix",
            ));
        }
        verify_active_prefix(&active_path, metadata.segments.last(), prefix)?;
        replace_active_without_prefix(work, &active_path, prefix)?;
    }
    metadata.active_prefix_bytes = 0;
    write_rotation_metadata(work, metadata)
}

fn rotate_active_segment(work: &Path, occurred_at: &str) -> io::Result<u64> {
    recover_rotation(work)?;
    let active_path = work.join(OUTBOX_FILE);
    let Some(active_len) = plain_file_len(&active_path)? else {
        return Ok(0);
    };
    if active_len == 0 {
        return Ok(0);
    }
    let mut active = open_existing_plain_outbox(&active_path)?;
    if committed_prefix_len(&mut active, active_len)? != active_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "refusing to rotate an unterminated active event segment",
        ));
    }
    let mut metadata = load_rotation_metadata(work)?;
    validate_rotation_metadata(work, &metadata)?;
    let name = segment_name(metadata.segments.len() + 1);
    let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(&name);
    let digest = copy_segment_atomically(work, &mut active, active_len, &archive_path)?;
    let start_offset = metadata
        .segments
        .last()
        .map_or(0, |segment| segment.end_offset);
    let end_offset = start_offset
        .checked_add(active_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "archive offset overflowed"))?;
    metadata.segments.push(ArchivedSegment {
        name,
        start_offset,
        end_offset,
        sha256: digest,
    });
    metadata.active_prefix_bytes = active_len;
    metadata.last_rotation_at = Some(occurred_at.to_string());
    write_rotation_metadata(work, &metadata)?;
    complete_pending_rotation(work, &mut metadata)?;
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
    let mut output = work_fs::create_new_plain_file(&temp)?;
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
    let mut output = work_fs::create_new_plain_file(&temp)?;
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

fn archive_final_names(work: &Path) -> io::Result<Vec<String>> {
    let archive = work.join(EVENTS_ARCHIVE_DIR);
    let Some(entries) = work_fs::plain_directory_entries(work, &archive)? else {
        return Ok(Vec::new());
    };
    let mut names = Vec::new();
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".segment.") && name.ends_with(".tmp.jsonl") {
            continue;
        }
        let valid = name
            .strip_prefix("segment_")
            .and_then(|value| value.strip_suffix(".jsonl"))
            .is_some_and(|digits| {
                digits.len() == SEGMENT_DIGITS && digits.bytes().all(|byte| byte.is_ascii_digit())
            });
        if !valid || !entry.file_type()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected entry in events archive: {name}"),
            ));
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}

fn verify_active_prefix(
    active_path: &Path,
    segment: Option<&ArchivedSegment>,
    prefix: u64,
) -> io::Result<()> {
    let segment = segment.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "pending rotation has no archive segment",
        )
    })?;
    if sha256_file(active_path, prefix)? != segment.sha256 {
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use serde_json::Map;

    use super::*;
    use crate::events::{Actor, ActorKind, EventType, SCHEMA_VERSION, TailReader};

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
        let unpublished = Outbox::with_rotation_enabled(&unpublished_work, true);
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
    fn cursor_resumes_across_rotation_and_archive_to_active_boundary() {
        let work = temp_work("cursor-rotation");
        let first = event("first-before-rotation");
        Outbox::new(&work).append_idempotent(&first).unwrap();
        let mut initial = TailReader::new(work.join(OUTBOX_FILE));
        assert_eq!(initial.poll_all().unwrap(), vec![first]);
        let before_rotation = initial.cursor();

        let rotating = Outbox::with_rotation_enabled(&work, true);
        rotating.append_idempotent(&published()).unwrap();
        rotating.append_idempotent(&closed()).unwrap();
        assert_eq!(fs::read(work.join(OUTBOX_FILE)).unwrap(), b"");
        assert!(
            work.join(EVENTS_ARCHIVE_DIR)
                .join(segment_name(1))
                .is_file()
        );

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
        let outbox = Outbox::with_rotation_enabled(&work, true);
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
        assert_eq!(
            metadata
                .segments
                .iter()
                .map(|segment| segment.name.as_str())
                .collect::<Vec<_>>(),
            [segment_name(1), segment_name(2)]
        );
        assert_eq!(metadata.segments[0].start_offset, 0);
        assert_eq!(
            metadata.segments[0].end_offset,
            metadata.segments[1].start_offset
        );
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
        let rotating = Outbox::with_rotation_enabled(&work, true);
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
        let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(segment_name(1));
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
        assert_eq!(metadata.segments.len(), 1);
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
        let name = segment_name(1);
        let archive_path = work.join(EVENTS_ARCHIVE_DIR).join(&name);
        let digest = copy_segment_atomically(&work, &mut active, len, &archive_path).unwrap();
        drop(active);
        write_rotation_metadata(
            &work,
            &RotationMetadata {
                schema_version: ROTATION_SCHEMA_VERSION,
                segments: vec![ArchivedSegment {
                    name,
                    start_offset: 0,
                    end_offset: len,
                    sha256: digest,
                }],
                active_prefix_bytes: len,
                last_rotation_at: None,
            },
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

    #[test]
    fn torn_tail_repair_remains_confined_to_the_active_segment_after_rotation() {
        let work = temp_work("archive-torn-active");
        let rotating = Outbox::with_rotation_enabled(&work, true);
        rotating.append_idempotent(&published()).unwrap();
        rotating.append_idempotent(&closed()).unwrap();
        let archived = fs::read(work.join(EVENTS_ARCHIVE_DIR).join(segment_name(1))).unwrap();

        let committed = event("active-committed");
        fs::write(
            rotating.path(),
            format!("{}\n{{\"schema_version\":", committed.to_json_line()),
        )
        .unwrap();
        let appended = event("active-after-torn");
        rotating.append_idempotent(&appended).unwrap();
        assert_eq!(
            fs::read(work.join(EVENTS_ARCHIVE_DIR).join(segment_name(1))).unwrap(),
            archived,
            "immutable archive bytes must not participate in torn-tail repair"
        );
        let events = TailReader::new(rotating.path()).poll_all().unwrap();
        assert_eq!(events, vec![published(), closed(), committed, appended]);
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
