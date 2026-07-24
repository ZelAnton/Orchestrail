//! Atomic persistence for the deterministic processor reducer.
//!
//! This is deliberately a narrow storage boundary: the state machine owns the schema, while this
//! module guarantees a checkpoint is either the complete previous JSON document or the complete
//! next one.  It never creates a second source of truth from temporary files.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::processor::ProcessorState;

/// File name under `.work/` used by the native deterministic engine.
pub const CHECKPOINT_FILE: &str = "processor-state.json";

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;

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

fn require_plain_directory(path: &Path, metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.is_dir() && !redirected(metadata) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "checkpoint root is not a plain directory: {}",
                path.display()
            ),
        ))
    }
}

fn require_plain_file(path: &Path, metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.is_file() && !redirected(metadata) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("checkpoint is not a plain regular file: {}", path.display()),
        ))
    }
}

#[derive(Debug)]
pub enum CheckpointError {
    Io(io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "checkpoint I/O error: {error}"),
            Self::Json(error) => write!(f, "invalid processor checkpoint JSON: {error}"),
        }
    }
}

impl std::error::Error for CheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
        }
    }
}

impl From<io::Error> for CheckpointError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for CheckpointError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub type Result<T> = std::result::Result<T, CheckpointError>;

/// Store a native checkpoint immediately under a chosen `.work/` directory.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    work: PathBuf,
    file_name: String,
}

impl CheckpointStore {
    pub fn new(work: impl Into<PathBuf>) -> Self {
        Self {
            work: work.into(),
            file_name: CHECKPOINT_FILE.into(),
        }
    }

    /// Open an adjacent, independently versioned checkpoint document.  The filename is confined
    /// to a single path component so callers cannot escape the selected `.work` directory.
    pub fn for_file(work: impl Into<PathBuf>, file_name: impl Into<String>) -> Result<Self> {
        let file_name = file_name.into();
        let path = Path::new(&file_name);
        if path.components().count() != 1 || path.file_name().is_none() {
            return Err(CheckpointError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("checkpoint filename must be one path component: {file_name:?}"),
            )));
        }
        Ok(Self {
            work: work.into(),
            file_name,
        })
    }

    pub fn path(&self) -> PathBuf {
        self.work.join(&self.file_name)
    }

    /// Return `None` only when the checkpoint does not exist. A directory, permissions error, or
    /// malformed document is a hard failure; recovery must not treat damaged state as idle.
    pub fn load(&self) -> Result<Option<ProcessorState>> {
        self.load_json()
    }

    /// Load an independently versioned JSON checkpoint from this guarded store.
    pub fn load_json<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        let work_metadata = match fs::symlink_metadata(&self.work) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        require_plain_directory(&self.work, &work_metadata)?;
        let path = self.path();
        let before = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        require_plain_file(&path, &before)?;
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
        let mut file = options.open(&path)?;
        let opened = file.metadata()?;
        require_plain_file(&path, &opened)?;
        if opened.len() > MAX_CHECKPOINT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "checkpoint exceeds the {MAX_CHECKPOINT_BYTES}-byte limit: {}",
                    path.display()
                ),
            )
            .into());
        }
        let mut text = String::new();
        (&mut file)
            .take(MAX_CHECKPOINT_BYTES + 1)
            .read_to_string(&mut text)?;
        if text.len() as u64 > MAX_CHECKPOINT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "checkpoint grew beyond the {MAX_CHECKPOINT_BYTES}-byte limit: {}",
                    path.display()
                ),
            )
            .into());
        }
        require_plain_file(&path, &fs::symlink_metadata(&path)?)?;
        Ok(Some(serde_json::from_str(&text)?))
    }

    /// Write JSON to a same-directory uniquely named file, flush it, then replace the target.
    /// A same-directory rename prevents a cross-device partial copy. The processor lease is the
    /// single-writer guard; failure leaves the old complete checkpoint intact whenever the platform
    /// supports atomic replacement, and never deliberately truncates the target first.
    pub fn save(&self, state: &ProcessorState) -> Result<()> {
        self.save_json(state)
    }

    /// Atomically save any typed JSON checkpoint.  The caller owns the schema; this storage layer
    /// owns only same-directory durable replacement.
    pub fn save_json<T: Serialize>(&self, state: &T) -> Result<()> {
        match fs::symlink_metadata(&self.work) {
            Ok(metadata) => require_plain_directory(&self.work, &metadata)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&self.work)?;
                require_plain_directory(&self.work, &fs::symlink_metadata(&self.work)?)?;
            }
            Err(error) => return Err(error.into()),
        }
        let payload = serde_json::to_vec_pretty(state)?;
        if payload.len().saturating_add(1) as u64 > MAX_CHECKPOINT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("checkpoint exceeds the {MAX_CHECKPOINT_BYTES}-byte limit"),
            )
            .into());
        }
        let target = self.path();
        match fs::symlink_metadata(&target) {
            Ok(metadata) => require_plain_file(&target, &metadata)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let temp = self.temp_path();
        let mut file = create_new(&temp)?;
        let write_result = (|| -> io::Result<()> {
            file.write_all(&payload)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            Ok(())
        })();
        drop(file);
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp);
            return Err(error.into());
        }
        if let Err(error) = fs::rename(&temp, &target) {
            let _ = fs::remove_file(&temp);
            return Err(error.into());
        }
        require_plain_file(&target, &fs::symlink_metadata(&target)?)?;
        Ok(())
    }

    fn temp_path(&self) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        self.work.join(format!(
            ".{}.{}.{sequence}.tmp",
            self.file_name,
            std::process::id()
        ))
    }
}

fn create_new(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::{PROCESSOR_STATE_VERSION, Phase};

    fn temp_work(label: &str) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "orchestrail-checkpoint-{label}-{}-{sequence}",
            std::process::id()
        ))
    }

    #[test]
    fn missing_checkpoint_is_not_an_error_but_invalid_json_is() {
        let work = temp_work("load");
        let store = CheckpointStore::new(&work);
        assert!(store.load().unwrap().is_none());
        fs::create_dir_all(&work).unwrap();
        fs::write(store.path(), "{not json").unwrap();
        assert!(matches!(store.load(), Err(CheckpointError::Json(_))));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn save_replaces_a_complete_checkpoint_and_round_trips() {
        let work = temp_work("roundtrip");
        let store = CheckpointStore::new(&work);
        let first = ProcessorState {
            phase: Phase::Idle,
            ..ProcessorState::default()
        };
        store.save(&first).unwrap();
        let mut second = first.clone();
        second.phase = Phase::Rolling;
        store.save(&second).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.phase, Phase::Rolling);
        assert_eq!(loaded.schema_version, PROCESSOR_STATE_VERSION);
        let leftovers = fs::read_dir(&work)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp"))
            .count();
        assert_eq!(leftovers, 0);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn checkpoint_store_rejects_redirected_file_and_work_root() {
        let work = temp_work("redirected-file");
        fs::create_dir(&work).unwrap();
        let store = CheckpointStore::new(&work);
        let external = work.with_extension("external.json");
        fs::write(&external, "{}\n").unwrap();
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&external, store.path()).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&external, store.path()).is_ok();
        if linked {
            assert!(matches!(store.load(), Err(CheckpointError::Io(_))));
            assert!(matches!(
                store.save(&ProcessorState::default()),
                Err(CheckpointError::Io(_))
            ));
            assert_eq!(fs::read_to_string(&external).unwrap(), "{}\n");
        }
        let _ = fs::remove_file(&external);
        let _ = fs::remove_dir_all(work);

        let real_work = temp_work("real-work");
        fs::create_dir(&real_work).unwrap();
        let redirected_work = temp_work("redirected-root");
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(&real_work, &redirected_work).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&real_work, &redirected_work).is_ok();
        if linked {
            let redirected = CheckpointStore::new(&redirected_work);
            assert!(matches!(
                redirected.save(&ProcessorState::default()),
                Err(CheckpointError::Io(_))
            ));
            assert_eq!(fs::read_dir(&real_work).unwrap().count(), 0);
        }
        let _ = fs::remove_dir_all(redirected_work);
        let _ = fs::remove_dir_all(real_work);
    }

    #[test]
    fn existing_directory_at_checkpoint_path_fails_loudly() {
        let work = temp_work("directory");
        let store = CheckpointStore::new(&work);
        fs::create_dir_all(store.path()).unwrap();
        assert!(matches!(store.load(), Err(CheckpointError::Io(_))));
        let _ = fs::remove_dir_all(work);
    }
}
