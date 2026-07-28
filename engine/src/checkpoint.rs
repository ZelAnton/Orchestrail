//! Atomic persistence for the deterministic processor reducer.
//!
//! This is deliberately a narrow storage boundary: the state machine owns the schema, while this
//! module guarantees a checkpoint is either the complete previous JSON document or the complete
//! next one.  It never creates a second source of truth from temporary files.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::processor::ProcessorState;
use crate::work_fs;

/// File name under `.work/` used by the native deterministic engine.
pub const CHECKPOINT_FILE: &str = "processor-state.json";

#[cfg(test)]
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;

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
        match fs::symlink_metadata(&self.work) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        work_fs::require_plain_directory(&self.work)?;
        let path = self.path();
        work_fs::read_optional_text(&self.work, &path, MAX_CHECKPOINT_BYTES)?
            .map(|text| serde_json::from_str(&text).map_err(CheckpointError::from))
            .transpose()
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
        work_fs::ensure_plain_directory(&self.work)?;
        let mut payload = serde_json::to_vec_pretty(state)?;
        if payload.len().saturating_add(1) as u64 > MAX_CHECKPOINT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("checkpoint exceeds the {MAX_CHECKPOINT_BYTES}-byte limit"),
            )
            .into());
        }
        payload.push(b'\n');
        let target = self.path();
        work_fs::replace_file(&self.work, &target, &payload, MAX_CHECKPOINT_BYTES)?;
        Ok(())
    }
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
