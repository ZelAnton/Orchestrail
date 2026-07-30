//! Small metadata-invalidated caches for parsed, periodically polled control-plane files.

use std::io;
use std::path::{Path, PathBuf};

use orchestrail_engine::work_fs::{self, PlainFileStamp};

/// Cache one parsed optional plain file while its confined `(mtime, len)` stamp is unchanged.
#[derive(Debug)]
pub(crate) struct PlainFileCache<T> {
    key: Option<(PathBuf, Option<PlainFileStamp>)>,
    value: Option<T>,
}

impl<T> Default for PlainFileCache<T> {
    fn default() -> Self {
        Self {
            key: None,
            value: None,
        }
    }
}

impl<T> PlainFileCache<T> {
    /// Read and parse on a metadata miss, otherwise return the previously parsed value.
    ///
    /// The stamp is checked again after the bounded read. If a concurrent writer changed the
    /// file, that result may still be rendered for this observer tick but is deliberately not
    /// cached, so the next tick retries instead of pinning a racy parse.
    pub(crate) fn load_with(
        &mut self,
        work: &Path,
        path: &Path,
        max_bytes: u64,
        parse: impl FnOnce(Option<String>) -> T,
    ) -> io::Result<&T> {
        let before = match work_fs::optional_plain_file_stamp(work, path, max_bytes) {
            Ok(stamp) => stamp,
            Err(error) => {
                self.invalidate();
                return Err(error);
            }
        };
        if self
            .key
            .as_ref()
            .is_some_and(|(cached_path, stamp)| cached_path == path && stamp == &before)
        {
            return Ok(self
                .value
                .as_ref()
                .expect("an initialized file stamp always has a parsed value"));
        }

        let text = match work_fs::read_optional_text(work, path, max_bytes) {
            Ok(text) => text,
            Err(error) => {
                self.invalidate();
                return Err(error);
            }
        };
        let value = parse(text);
        let after = match work_fs::optional_plain_file_stamp(work, path, max_bytes) {
            Ok(stamp) => stamp,
            Err(error) => {
                self.invalidate();
                return Err(error);
            }
        };
        self.value = Some(value);
        self.key = (after == before).then(|| (path.to_path_buf(), after));
        Ok(self
            .value
            .as_ref()
            .expect("the freshly parsed cache value was just stored"))
    }

    pub(crate) fn invalidate(&mut self) {
        self.key = None;
        self.value = None;
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temp_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-tui-cache-{label}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn unchanged_file_reuses_parse_and_changed_file_reparses() {
        let work = temp_root("metadata");
        fs::create_dir(&work).unwrap();
        let path = work.join("state.md");
        fs::write(&path, "one\n").unwrap();
        let parses = Cell::new(0);
        let mut cache = PlainFileCache::default();

        let mut load = || {
            cache
                .load_with(&work, &path, 64, |text| {
                    parses.set(parses.get() + 1);
                    text.unwrap_or_default()
                })
                .unwrap()
                .clone()
        };
        assert_eq!(load(), "one\n");
        assert_eq!(load(), "one\n");
        assert_eq!(
            parses.get(),
            1,
            "an unchanged file must not be parsed twice"
        );

        fs::write(&path, "two, changed\n").unwrap();
        assert_eq!(load(), "two, changed\n");
        assert_eq!(
            parses.get(),
            2,
            "changed metadata must invalidate the parse"
        );

        let _ = fs::remove_dir_all(work);
    }
}
