//! Append-only, idempotent writer for the typed `.work/events.jsonl` outbox.
//!
//! The engine lease is the single-writer interlock. This module nevertheless validates the full
//! committed prefix before appending and refuses an event-id collision with different content, so
//! a bad caller cannot turn replay into a silently divergent event stream.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use super::{Event, ParseError, parse_line};

static OUTBOX_ACCESS: Mutex<()> = Mutex::new(());

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
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let committed_len = if contents.is_empty() || contents.ends_with('\n') {
            contents.len()
        } else {
            contents.rfind('\n').map_or(0, |index| index + 1)
        };
        let committed = &contents[..committed_len];
        let mut already_present = false;
        for (index, line) in committed.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let existing = parse_line(line).map_err(|error| OutboxError::InvalidExisting {
                line: index + 1,
                error,
            })?;
            if existing.event_id == event.event_id {
                if same_semantic_event(&existing, event) {
                    already_present = true;
                } else {
                    return Err(OutboxError::EventIdCollision {
                        event_id: event.event_id.clone(),
                    });
                }
            }
        }
        if committed_len != contents.len() {
            file.set_len(u64::try_from(committed_len).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "outbox prefix length overflowed",
                )
            })?)?;
            file.sync_all()?;
        }
        if already_present {
            return Ok(AppendOutcome::AlreadyPresent);
        }
        file.seek(SeekFrom::End(0))?;
        let line = format!("{desired}\n");
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        Ok(AppendOutcome::Appended)
    }
}

fn open_plain_outbox(path: &std::path::Path) -> io::Result<File> {
    open_checked_outbox(path, true)
}

/// Open an existing outbox for readers without following a symlink/reparse-point replacement.
/// Missing files remain `NotFound`; readers must decide whether that means empty or unavailable.
pub(crate) fn open_existing_plain_outbox(path: &std::path::Path) -> io::Result<File> {
    open_checked_outbox(path, false)
}

fn open_checked_outbox(path: &std::path::Path, create: bool) -> io::Result<File> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event outbox has no parent"))?;
    match fs::symlink_metadata(parent) {
        Ok(metadata) => assert_plain_outbox_directory(parent, &metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            fs::create_dir(parent)?;
            assert_plain_outbox_directory(parent, &fs::symlink_metadata(parent)?)?;
        }
        Err(error) => return Err(error),
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) => assert_plain_outbox(path, &metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(create).create(create);
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
    let file = options.open(path)?;
    assert_plain_outbox(path, &file.metadata()?)?;
    assert_plain_outbox(path, &fs::symlink_metadata(path)?)?;
    Ok(file)
}

fn assert_plain_outbox(path: &std::path::Path, metadata: &fs::Metadata) -> io::Result<()> {
    if !metadata.is_file() || redirected(metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("event outbox is not a plain file: {}", path.display()),
        ));
    }
    Ok(())
}

fn assert_plain_outbox_directory(
    path: &std::path::Path,
    metadata: &fs::Metadata,
) -> io::Result<()> {
    if !metadata.is_dir() || redirected(metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "event outbox parent is not a plain directory: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

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

/// Replaying a durable transition may observe a new wall-clock timestamp. The deterministic id
/// is the authority for that transition, so compare every semantic envelope field except
/// `occurred_at`; a different payload/type/actor still surfaces as a collision.
fn same_semantic_event(existing: &Event, desired: &Event) -> bool {
    existing.schema_version == desired.schema_version
        && existing.event_id == desired.event_id
        && existing.event_type == desired.event_type
        && existing.actor == desired.actor
        && existing.batch_id == desired.batch_id
        && existing.task_id == desired.task_id
        && existing.payload_version == desired.payload_version
        && existing.payload == desired.payload
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
