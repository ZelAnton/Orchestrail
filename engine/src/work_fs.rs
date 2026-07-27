//! Confined filesystem operations for authority-bearing artifacts below one `.work` root.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
pub(crate) const MAX_CONTROL_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) fn redirected(metadata: &fs::Metadata) -> bool {
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

pub(crate) fn require_plain_directory(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() && !redirected(&metadata) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "control-plane path is not a plain directory: {}",
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
            format!(
                "control-plane path is not a plain regular file: {}",
                path.display()
            ),
        ))
    }
}

fn relative_below<'a>(work: &Path, path: &'a Path) -> io::Result<&'a Path> {
    let relative = path.strip_prefix(work).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "artifact escapes work root {}: {}",
                work.display(),
                path.display()
            ),
        )
    })?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "artifact is not a confined relative path: {}",
                path.display()
            ),
        ));
    }
    Ok(relative)
}

/// Prove the work root and every existing directory below it without following redirects.
/// Missing directories are created one component at a time and then re-proved.
pub(crate) fn ensure_plain_parent(work: &Path, path: &Path) -> io::Result<()> {
    let relative = relative_below(work, path)?;
    require_plain_directory(work)?;
    let mut current = PathBuf::from(work);
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    for component in parent.components() {
        let Component::Normal(name) = component else {
            unreachable!("relative_below rejected non-normal components")
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(_) => require_plain_directory(&current)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                require_plain_directory(&current)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn plain_parent_exists(work: &Path, path: &Path) -> io::Result<bool> {
    let relative = relative_below(work, path)?;
    require_plain_directory(work)?;
    let mut current = PathBuf::from(work);
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    for component in parent.components() {
        let Component::Normal(name) = component else {
            unreachable!("relative_below rejected non-normal components")
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(_) => require_plain_directory(&current)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

/// Test whether a confined control-plane entry exists without following its final component.
/// This is intentionally weaker than a typed file/directory proof: callers use it only to make
/// presence itself fail closed (for example PAUSE markers and checkpoint routing). A dangling
/// symlink or Windows reparse point therefore counts as present and is rejected later by the
/// typed reader instead of being mistaken for absence.
pub(crate) fn entry_exists(work: &Path, path: &Path) -> io::Result<bool> {
    if !plain_parent_exists(work, path)? {
        return Ok(false);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn assert_plain_parent(work: &Path, path: &Path) -> io::Result<()> {
    if plain_parent_exists(work, path)? {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("control-plane parent is absent: {}", path.display()),
        ))
    }
}

pub(crate) fn read_optional_bytes(
    work: &Path,
    path: &Path,
    max_bytes: u64,
) -> io::Result<Option<Vec<u8>>> {
    if !plain_parent_exists(work, path)? {
        return Ok(None);
    }
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    require_plain_file(path, &before)?;
    if before.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "control-plane artifact exceeds {max_bytes} bytes: {}",
                path.display()
            ),
        ));
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
    let mut file = options.open(path)?;
    require_plain_file(path, &file.metadata()?)?;
    let mut bytes = Vec::new();
    (&mut file).take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "control-plane artifact grew beyond {max_bytes} bytes: {}",
                path.display()
            ),
        ));
    }
    require_plain_file(path, &fs::symlink_metadata(path)?)?;
    assert_plain_parent(work, path)?;
    Ok(Some(bytes))
}

pub(crate) fn read_optional_text(
    work: &Path,
    path: &Path,
    max_bytes: u64,
) -> io::Result<Option<String>> {
    read_optional_bytes(work, path, max_bytes)?
        .map(|bytes| {
            String::from_utf8(bytes).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "control-plane artifact is not UTF-8: {} ({error})",
                        path.display()
                    ),
                )
            })
        })
        .transpose()
}

pub(crate) fn read_required_bytes(work: &Path, path: &Path, max_bytes: u64) -> io::Result<Vec<u8>> {
    read_optional_bytes(work, path, max_bytes)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "required control-plane artifact is absent: {}",
                path.display()
            ),
        )
    })
}

pub(crate) fn read_required_text(work: &Path, path: &Path, max_bytes: u64) -> io::Result<String> {
    read_optional_text(work, path, max_bytes)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "required control-plane artifact is absent: {}",
                path.display()
            ),
        )
    })
}

pub(crate) fn replace_file(
    work: &Path,
    path: &Path,
    payload: &[u8],
    max_bytes: u64,
) -> io::Result<()> {
    if payload.len() as u64 > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "control-plane artifact exceeds {max_bytes} bytes: {}",
                path.display()
            ),
        ));
    }
    ensure_plain_parent(work, path)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) => require_plain_file(path, &metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "artifact path has no filename")
    })?;
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = path.parent().expect("filename has parent").join(format!(
        ".{}.{}.{sequence}.tmp",
        name.to_string_lossy(),
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let result = (|| {
        file.write_all(payload)?;
        file.sync_all()
    })();
    drop(file);
    if let Err(error) = result {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    ensure_plain_parent(work, path)?;
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    require_plain_file(path, &fs::symlink_metadata(path)?)?;
    ensure_plain_parent(work, path)
}

pub(crate) fn plain_directory_entries(
    work: &Path,
    path: &Path,
) -> io::Result<Option<Vec<fs::DirEntry>>> {
    if !plain_parent_exists(work, path)? {
        return Ok(None);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => require_plain_directory(path)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let entries = fs::read_dir(path)?.collect::<io::Result<Vec<_>>>()?;
    require_plain_directory(path)?;
    assert_plain_parent(work, path)?;
    Ok(Some(entries))
}

pub(crate) fn remove_plain_file(work: &Path, path: &Path) -> io::Result<bool> {
    if !plain_parent_exists(work, path)? {
        return Ok(false);
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) => require_plain_file(path, &metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    fs::remove_file(path)?;
    assert_plain_parent(work, path)?;
    Ok(true)
}

pub(crate) fn remove_plain_directory_all(work: &Path, path: &Path) -> io::Result<bool> {
    if !plain_parent_exists(work, path)? {
        return Ok(false);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => require_plain_directory(path)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    fs::remove_dir_all(path)?;
    assert_plain_parent(work, path)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-work-fs-{label}-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn bounded_plain_text_round_trips_and_refuses_oversize() {
        let work = temp_root("bounded");
        fs::create_dir(&work).unwrap();
        let path = work.join("nested/state.md");
        replace_file(&work, &path, b"state\n", 32).unwrap();
        assert_eq!(
            read_optional_text(&work, &path, 32).unwrap().as_deref(),
            Some("state\n")
        );
        assert!(read_optional_text(&work, &path, 4).is_err());
        assert!(replace_file(&work, &path, b"too large", 4).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "state\n");
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn redirected_file_or_parent_is_never_followed() {
        let work = temp_root("redirect");
        let external = temp_root("external");
        fs::create_dir(&work).unwrap();
        fs::create_dir(&external).unwrap();
        let external_file = external.join("outside.md");
        fs::write(&external_file, "outside\n").unwrap();

        let linked_file = work.join("linked.md");
        #[cfg(windows)]
        let file_linked = std::os::windows::fs::symlink_file(&external_file, &linked_file).is_ok();
        #[cfg(unix)]
        let file_linked = std::os::unix::fs::symlink(&external_file, &linked_file).is_ok();
        if file_linked {
            assert!(read_optional_text(&work, &linked_file, 32).is_err());
            assert!(replace_file(&work, &linked_file, b"changed", 32).is_err());
            assert_eq!(fs::read_to_string(&external_file).unwrap(), "outside\n");
            fs::remove_file(&linked_file).unwrap();
        }

        let dangling_file = work.join("dangling.md");
        let missing_target = external.join("missing.md");
        #[cfg(windows)]
        let dangling_linked =
            std::os::windows::fs::symlink_file(&missing_target, &dangling_file).is_ok();
        #[cfg(unix)]
        let dangling_linked = std::os::unix::fs::symlink(&missing_target, &dangling_file).is_ok();
        if dangling_linked {
            assert!(!dangling_file.try_exists().unwrap());
            assert!(entry_exists(&work, &dangling_file).unwrap());
            fs::remove_file(&dangling_file).unwrap();
        }

        let linked_dir = work.join("linked-dir");
        #[cfg(windows)]
        let directory_linked = std::os::windows::fs::symlink_dir(&external, &linked_dir).is_ok();
        #[cfg(unix)]
        let directory_linked = std::os::unix::fs::symlink(&external, &linked_dir).is_ok();
        if directory_linked {
            let nested = linked_dir.join("outside.md");
            assert!(read_optional_text(&work, &nested, 32).is_err());
            assert!(replace_file(&work, &nested, b"changed", 32).is_err());
            assert_eq!(fs::read_to_string(&external_file).unwrap(), "outside\n");
            #[cfg(windows)]
            fs::remove_dir(&linked_dir).unwrap();
            #[cfg(unix)]
            fs::remove_file(&linked_dir).unwrap();
        }

        let _ = fs::remove_dir_all(work);
        let _ = fs::remove_dir_all(external);
    }
}
