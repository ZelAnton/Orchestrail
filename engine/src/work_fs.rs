//! Confined filesystem operations for authority-bearing artifacts below one `.work` root.
//!
//! Atomic replacement flushes the temporary file before rename and, on Unix, flushes the plain
//! parent directory afterwards so the new directory entry is durable across a power loss. Rust's
//! standard library exposes no equivalent portable directory flush on Windows; that platform
//! therefore revalidates confinement after rename but otherwise relies on its rename semantics.

#[cfg(test)]
use std::cell::RefCell;
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
type CreateNewPlainFileHook = Box<dyn FnOnce() -> io::Result<()>>;

#[cfg(test)]
thread_local! {
    static CREATE_NEW_PLAIN_FILE_HOOK: RefCell<Option<CreateNewPlainFileHook>> =
        const { RefCell::new(None) };
    static CREATE_NEW_PLAIN_FILE_POST_HOOK: RefCell<Option<CreateNewPlainFileHook>> =
        const { RefCell::new(None) };
}

/// Default ceiling for one authority-bearing control-plane artifact.
///
/// The operator TUI reuses this exact ceiling for the same `.work` files instead of maintaining
/// an independent, weaker read policy.
pub const MAX_CONTROL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CONTROL_DIRECTORY_ENTRIES: usize = 100_000;

/// Stable classifications for confinement and size failures raised by plain-file readers.
///
/// Callers may downcast an [`io::Error`] with [`plain_read_violation`] instead of coupling their
/// domain diagnostics to this module's human-readable messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlainReadViolation {
    Oversize { path: PathBuf, max_bytes: u64 },
    GrewWhileReading { path: PathBuf, max_bytes: u64 },
    NotPlain { path: PathBuf },
    ParentNotPlain { path: PathBuf },
}

impl fmt::Display for PlainReadViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Oversize { path, max_bytes } => write!(
                formatter,
                "control-plane artifact exceeds {max_bytes} bytes: {}",
                path.display()
            ),
            Self::GrewWhileReading { path, max_bytes } => write!(
                formatter,
                "control-plane artifact grew beyond {max_bytes} bytes: {}",
                path.display()
            ),
            Self::NotPlain { path } => write!(
                formatter,
                "control-plane path is not a plain regular file: {}",
                path.display()
            ),
            Self::ParentNotPlain { path } => write!(
                formatter,
                "control-plane path is not a plain directory: {}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for PlainReadViolation {}

fn plain_read_error(violation: PlainReadViolation) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, violation)
}

pub(crate) fn plain_read_violation(error: &io::Error) -> Option<&PlainReadViolation> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<PlainReadViolation>())
}

/// Metadata used to invalidate a parsed control-plane artifact without reading it again.
///
/// Values are produced only after the work root, parent chain, and final entry have passed the
/// same anti-symlink/reparse checks as [`read_optional_text`]. The byte ceiling is also enforced
/// before a stamp is returned, so a cache probe cannot turn an oversized artifact into success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlainFileStamp {
    len: u64,
    modified: Option<SystemTime>,
}

impl PlainFileStamp {
    /// Current file length in bytes.
    pub fn len(self) -> u64 {
        self.len
    }

    /// Whether the file was empty when stamped.
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    /// Last-modified time when the filesystem exposes it.
    pub fn modified(self) -> Option<SystemTime> {
        self.modified
    }
}

/// Recognize only the unique same-directory temporary name emitted by [`replace_file`] for one
/// exact target. Recovery code may ignore such a crash residue without treating arbitrary hidden
/// files as owned scratch state.
pub(crate) fn is_replace_temp_for(target: &Path, candidate: &OsStr) -> bool {
    let (Some(target_name), Some(candidate)) = (target.file_name(), candidate.to_str()) else {
        return false;
    };
    let prefix = format!(".{}.", target_name.to_string_lossy());
    let Some(coordinates) = candidate
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(".tmp"))
    else {
        return false;
    };
    let mut parts = coordinates.split('.');
    let pid = parts.next().unwrap_or_default();
    let sequence = parts.next().unwrap_or_default();
    parts.next().is_none()
        && !pid.is_empty()
        && pid.bytes().all(|byte| byte.is_ascii_digit())
        && !sequence.is_empty()
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
}

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
        Err(plain_read_error(PlainReadViolation::ParentNotPlain {
            path: path.to_path_buf(),
        }))
    }
}

pub(crate) fn require_plain_file(path: &Path, metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.is_file() && !redirected(metadata) {
        Ok(())
    } else {
        Err(plain_read_error(PlainReadViolation::NotPlain {
            path: path.to_path_buf(),
        }))
    }
}

/// Create one directory when absent and prove that the resulting entry is neither a symlink nor
/// a Windows reparse point. Callers use this for the selected `.work` root before confined walks.
pub(crate) fn ensure_plain_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => require_plain_directory(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            require_plain_directory(path)
        }
        Err(error) => Err(error),
    }
}

fn add_no_follow(options: &mut OpenOptions) {
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
}

fn open_plain_file(
    path: &Path,
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
) -> io::Result<File> {
    if !create_new {
        match fs::symlink_metadata(path) {
            Ok(metadata) => require_plain_file(path, &metadata)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound && create => {}
            Err(error) => return Err(error),
        }
    }
    let mut options = OpenOptions::new();
    options
        .read(read)
        .write(write)
        .create(create)
        .create_new(create_new);
    add_no_follow(&mut options);
    let file = options.open(path)?;
    require_plain_file(path, &file.metadata()?)?;
    require_plain_file(path, &fs::symlink_metadata(path)?)?;
    Ok(file)
}

pub(crate) fn open_existing_plain_file(path: &Path) -> io::Result<File> {
    open_plain_file(path, true, false, false, false)
}

pub(crate) fn open_plain_file_read_write(path: &Path, create: bool) -> io::Result<File> {
    open_plain_file(path, true, true, create, false)
}

pub(crate) fn create_new_plain_file(path: &Path) -> io::Result<File> {
    open_plain_file(path, false, true, false, true)
}

/// Create a new confined control-plane file and reject a parent redirect installed around the
/// creation attempt.
pub(crate) fn create_new_plain_file_rooted(work: &Path, path: &Path) -> io::Result<File> {
    let parent_identity = ensure_plain_parent_identity(work, path)?;
    #[cfg(test)]
    run_create_new_plain_file_hook()?;
    let file = create_new_plain_file(path);
    #[cfg(test)]
    run_create_new_plain_file_post_hook()?;
    let file = file?;
    assert_plain_parent(work, path, Some(&parent_identity))?;
    assert_plain_file_identity(path, &file)?;
    Ok(file)
}

#[cfg(test)]
fn set_create_new_plain_file_hook(hook: impl FnOnce() -> io::Result<()> + 'static) {
    CREATE_NEW_PLAIN_FILE_HOOK.with(|slot| {
        assert!(
            slot.replace(Some(Box::new(hook))).is_none(),
            "create-new test hook is already installed"
        );
    });
}

#[cfg(test)]
fn set_create_new_plain_file_post_hook(hook: impl FnOnce() -> io::Result<()> + 'static) {
    CREATE_NEW_PLAIN_FILE_POST_HOOK.with(|slot| {
        assert!(
            slot.replace(Some(Box::new(hook))).is_none(),
            "create-new post test hook is already installed"
        );
    });
}

#[cfg(test)]
fn run_create_new_plain_file_hook() -> io::Result<()> {
    CREATE_NEW_PLAIN_FILE_HOOK.with(|slot| match slot.borrow_mut().take() {
        Some(hook) => hook(),
        None => Ok(()),
    })
}

#[cfg(test)]
fn run_create_new_plain_file_post_hook() -> io::Result<()> {
    CREATE_NEW_PLAIN_FILE_POST_HOOK.with(|slot| match slot.borrow_mut().take() {
        Some(hook) => hook(),
        None => Ok(()),
    })
}

pub(crate) fn read_plain_bytes(path: &Path, max_bytes: u64) -> io::Result<Vec<u8>> {
    let mut file = open_existing_plain_file(path)?;
    if file.metadata()?.len() > max_bytes {
        return Err(plain_read_error(PlainReadViolation::Oversize {
            path: path.to_path_buf(),
            max_bytes,
        }));
    }
    let mut bytes = Vec::new();
    (&mut file).take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(plain_read_error(PlainReadViolation::GrewWhileReading {
            path: path.to_path_buf(),
            max_bytes,
        }));
    }
    require_plain_file(path, &fs::symlink_metadata(path)?)?;
    Ok(bytes)
}

pub(crate) fn read_plain_text(path: &Path, max_bytes: u64) -> io::Result<String> {
    String::from_utf8(read_plain_bytes(path, max_bytes)?).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "control-plane artifact is not UTF-8: {} ({error})",
                path.display()
            ),
        )
    })
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
    ensure_plain_parent_identity(work, path).map(|_| ())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlainDirectoryIdentity(u64, u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlainParentIdentity(Vec<(PathBuf, PlainDirectoryIdentity)>);

#[cfg(unix)]
fn plain_file_identity(file: &File) -> io::Result<PlainDirectoryIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(PlainDirectoryIdentity(metadata.dev(), metadata.ino()))
}

#[cfg(unix)]
fn plain_directory_identity(path: &Path) -> io::Result<PlainDirectoryIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() && !redirected(&metadata) {
        Ok(PlainDirectoryIdentity(metadata.dev(), metadata.ino()))
    } else {
        Err(plain_read_error(PlainReadViolation::ParentNotPlain {
            path: path.to_path_buf(),
        }))
    }
}

#[cfg(windows)]
fn plain_file_identity(file: &File) -> io::Result<PlainDirectoryIdentity> {
    use std::ffi::c_void;
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    #[repr(C)]
    struct ByHandleFileInformation {
        file_attributes: u32,
        creation_time: FileTime,
        last_access_time: FileTime,
        last_write_time: FileTime,
        volume_serial_number: u32,
        file_size_high: u32,
        file_size_low: u32,
        number_of_links: u32,
        file_index_high: u32,
        file_index_low: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandle(
            file: *mut c_void,
            information: *mut ByHandleFileInformation,
        ) -> i32;
    }

    let mut information = MaybeUninit::<ByHandleFileInformation>::uninit();
    // SAFETY: `file` owns a live Windows handle and `information` points to writable storage
    // with the exact layout required by `GetFileInformationByHandle`.
    let succeeded =
        unsafe { GetFileInformationByHandle(file.as_raw_handle(), information.as_mut_ptr()) };
    if succeeded == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a nonzero return guarantees that Windows initialized the output structure.
    let information = unsafe { information.assume_init() };
    let file_index =
        (u64::from(information.file_index_high) << 32) | u64::from(information.file_index_low);
    Ok(PlainDirectoryIdentity(
        u64::from(information.volume_serial_number),
        file_index,
    ))
}

#[cfg(windows)]
fn plain_directory_identity(path: &Path) -> io::Result<PlainDirectoryIdentity> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;

    let mut options = OpenOptions::new();
    options
        .access_mode(0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let directory = options.open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || redirected(&metadata) {
        return Err(plain_read_error(PlainReadViolation::ParentNotPlain {
            path: path.to_path_buf(),
        }));
    }
    plain_file_identity(&directory)
}

#[cfg(not(any(unix, windows)))]
fn plain_directory_identity(path: &Path) -> io::Result<PlainDirectoryIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "control-plane parent identity is unsupported on this platform: {}",
            path.display()
        ),
    ))
}

#[cfg(not(any(unix, windows)))]
fn plain_file_identity(_file: &File) -> io::Result<PlainDirectoryIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "control-plane file identity is unsupported on this platform",
    ))
}

fn assert_plain_file_identity(path: &Path, expected: &File) -> io::Result<()> {
    let observed = open_existing_plain_file(path)?;
    if plain_file_identity(&observed)? == plain_file_identity(expected)? {
        Ok(())
    } else {
        Err(plain_read_error(PlainReadViolation::NotPlain {
            path: path.to_path_buf(),
        }))
    }
}

fn ensure_plain_parent_identity(work: &Path, path: &Path) -> io::Result<PlainParentIdentity> {
    let relative = relative_below(work, path)?;
    let mut current = PathBuf::from(work);
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let mut identities = vec![(current.clone(), plain_directory_identity(&current)?)];
    for component in parent.components() {
        let Component::Normal(name) = component else {
            unreachable!("relative_below rejected non-normal components")
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(&current)?,
            Err(error) => return Err(error),
        }
        identities.push((current.clone(), plain_directory_identity(&current)?));
    }
    Ok(PlainParentIdentity(identities))
}

fn plain_parent_identity(work: &Path, path: &Path) -> io::Result<PlainParentIdentity> {
    let relative = relative_below(work, path)?;
    let mut current = PathBuf::from(work);
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let mut identities = vec![(current.clone(), plain_directory_identity(&current)?)];
    for component in parent.components() {
        let Component::Normal(name) = component else {
            unreachable!("relative_below rejected non-normal components")
        };
        current.push(name);
        identities.push((current.clone(), plain_directory_identity(&current)?));
    }
    Ok(PlainParentIdentity(identities))
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
pub fn entry_exists(work: &Path, path: &Path) -> io::Result<bool> {
    if !plain_parent_exists(work, path)? {
        return Ok(false);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn assert_plain_parent(
    work: &Path,
    path: &Path,
    expected_identity: Option<&PlainParentIdentity>,
) -> io::Result<()> {
    if !plain_parent_exists(work, path)? {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("control-plane parent is absent: {}", path.display()),
        ));
    }
    if let Some(expected) = expected_identity {
        let observed = plain_parent_identity(work, path)?;
        if &observed != expected {
            let changed = expected
                .0
                .iter()
                .zip(&observed.0)
                .find_map(|((expected_path, expected_id), (_, observed_id))| {
                    (expected_id != observed_id).then_some(expected_path.as_path())
                })
                .unwrap_or(path);
            return Err(plain_read_error(PlainReadViolation::ParentNotPlain {
                path: changed.to_path_buf(),
            }));
        }
    }
    Ok(())
}

pub(crate) fn read_optional_bytes(
    work: &Path,
    path: &Path,
    max_bytes: u64,
) -> io::Result<Option<Vec<u8>>> {
    if !plain_parent_exists(work, path)? {
        return Ok(None);
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) => require_plain_file(path, &metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let bytes = read_plain_bytes(path, max_bytes)?;
    assert_plain_parent(work, path, None)?;
    Ok(Some(bytes))
}

pub fn read_optional_text(work: &Path, path: &Path, max_bytes: u64) -> io::Result<Option<String>> {
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

/// Return confined metadata for an optional plain file without reading its contents.
///
/// This is the cache-invalidation companion to [`read_optional_text`]: absence is `Ok(None)`,
/// while an oversized artifact, redirected parent/final entry, or metadata error fails loudly.
pub fn optional_plain_file_stamp(
    work: &Path,
    path: &Path,
    max_bytes: u64,
) -> io::Result<Option<PlainFileStamp>> {
    if !plain_parent_exists(work, path)? {
        return Ok(None);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    require_plain_file(path, &metadata)?;
    if metadata.len() > max_bytes {
        return Err(plain_read_error(PlainReadViolation::Oversize {
            path: path.to_path_buf(),
            max_bytes,
        }));
    }
    let stamp = PlainFileStamp {
        len: metadata.len(),
        modified: metadata.modified().ok(),
    };
    assert_plain_parent(work, path, None)?;
    require_plain_file(path, &fs::symlink_metadata(path)?)?;
    Ok(Some(stamp))
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

#[cfg(unix)]
fn sync_parent_directory(work: &Path, path: &Path) -> io::Result<()> {
    ensure_plain_parent(work, path)?;
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "artifact path has no parent")
    })?;
    let mut options = OpenOptions::new();
    options.read(true);
    add_no_follow(&mut options);
    let directory = options.open(parent)?;
    if !directory.metadata()?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "control-plane parent is not a directory: {}",
                parent.display()
            ),
        ));
    }
    require_plain_directory(parent)?;
    directory.sync_all()?;
    ensure_plain_parent(work, path)
}

#[cfg(not(unix))]
fn sync_parent_directory(work: &Path, path: &Path) -> io::Result<()> {
    // `std` has no portable directory fsync on Windows. Keep the no-op explicit and retain both
    // post-rename confinement checks so callers do not mistake a redirected parent for success.
    ensure_plain_parent(work, path)
}

/// Replace one control-plane artifact without exposing a partially written target.
///
/// The temporary file is synced before the same-directory rename. After rename, the parent
/// directory is synced on Unix; if that final sync fails, the target may already contain the new
/// payload but its crash durability is unproven, so the error is deliberately returned.
pub fn replace_file(work: &Path, path: &Path, payload: &[u8], max_bytes: u64) -> io::Result<()> {
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
    ensure_plain_parent(work, &temp)?;
    let mut file = create_new_plain_file(&temp)?;
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
    sync_parent_directory(work, path)
}

pub fn plain_directory_entries(work: &Path, path: &Path) -> io::Result<Option<Vec<fs::DirEntry>>> {
    plain_directory_entries_bounded(work, path, MAX_CONTROL_DIRECTORY_ENTRIES)
}

/// Enumerate a confined plain directory without allowing a hostile control plane to grow one
/// observer allocation without bound. The ordinary engine-facing wrapper retains a generous
/// global ceiling; small consumers such as the TUI approval inbox can select a tighter limit.
pub fn plain_directory_entries_bounded(
    work: &Path,
    path: &Path,
    max_entries: usize,
) -> io::Result<Option<Vec<fs::DirEntry>>> {
    if !plain_parent_exists(work, path)? {
        return Ok(None);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => require_plain_directory(path)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(path)? {
        if entries.len() >= max_entries {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "control-plane directory exceeds {max_entries} entries: {}",
                    path.display()
                ),
            ));
        }
        entries.push(entry?);
    }
    require_plain_directory(path)?;
    assert_plain_parent(work, path, None)?;
    Ok(Some(entries))
}

pub fn remove_plain_file(work: &Path, path: &Path) -> io::Result<bool> {
    if !plain_parent_exists(work, path)? {
        return Ok(false);
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) => require_plain_file(path, &metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    fs::remove_file(path)?;
    assert_plain_parent(work, path, None)?;
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
    assert_plain_parent(work, path, None)?;
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

    #[cfg(windows)]
    fn symlink_directory(target: &Path, link: &Path) -> io::Result<()> {
        use std::process::{Command, Stdio};

        match std::os::windows::fs::symlink_dir(target, link) {
            Ok(()) => Ok(()),
            Err(error) if error.raw_os_error() == Some(1_314) => {
                let parent = target.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "test target has no parent")
                })?;
                let relative_link = link.strip_prefix(parent).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "test junction paths do not share a parent",
                    )
                })?;
                let target_name = target.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "test target has no name")
                })?;
                let command = format!(
                    "mklink /j {} {}",
                    relative_link.to_string_lossy().replace('/', "\\"),
                    target_name.to_string_lossy()
                );
                let output = Command::new("cmd.exe")
                    .args(["/d", "/c", &command])
                    .current_dir(parent)
                    .stdin(Stdio::null())
                    .output()?;
                if output.status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "failed to create test junction {} -> {}: stdout={} stderr={}",
                        link.display(),
                        target.display(),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    fn symlink_directory(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn remove_directory_link(path: &Path) -> io::Result<()> {
        fs::remove_dir(path)
    }

    #[cfg(unix)]
    fn remove_directory_link(path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    #[test]
    fn rooted_creation_rejects_a_parent_swapped_after_the_initial_check() {
        let work = temp_root("create-parent-swap");
        let parent = work.join("claims");
        let original_parent = work.join("claims-original");
        let external = work.join("external");
        let path = parent.join("claim.lock");
        fs::create_dir_all(&parent).unwrap();
        fs::create_dir(&external).unwrap();

        let hook_parent = parent.clone();
        let hook_original_parent = original_parent.clone();
        let hook_external = external.clone();
        set_create_new_plain_file_hook(move || {
            fs::rename(&hook_parent, &hook_original_parent)?;
            symlink_directory(&hook_external, &hook_parent)
        });
        let error = create_new_plain_file_rooted(&work, &path).unwrap_err();

        assert!(matches!(
            plain_read_violation(&error),
            Some(PlainReadViolation::ParentNotPlain { path: violation_path })
                if violation_path == &parent
        ));
        assert!(
            external.join("claim.lock").is_file(),
            "the post-create parent proof must reject the redirected creation"
        );

        remove_directory_link(&parent).unwrap();
        fs::remove_dir_all(work).unwrap();
    }

    #[test]
    fn rooted_creation_rejects_a_transient_parent_redirect_restored_as_plain() {
        let work = temp_root("create-transient-parent-swap");
        let external = temp_root("create-transient-parent-external");
        let parent = work.join("claims");
        let original_parent = work.join("claims-original");
        let path = parent.join("claim.lock");
        fs::create_dir_all(&parent).unwrap();
        fs::create_dir(&external).unwrap();

        let hook_parent = parent.clone();
        let hook_original_parent = original_parent.clone();
        let hook_external = external.clone();
        set_create_new_plain_file_hook(move || {
            fs::rename(&hook_parent, &hook_original_parent)?;
            symlink_directory(&hook_external, &hook_parent)
        });
        let post_hook_parent = parent.clone();
        set_create_new_plain_file_post_hook(move || {
            remove_directory_link(&post_hook_parent)?;
            fs::create_dir(&post_hook_parent)
        });

        let error = create_new_plain_file_rooted(&work, &path).unwrap_err();

        assert!(matches!(
            plain_read_violation(&error),
            Some(PlainReadViolation::ParentNotPlain { path: violation_path })
                if violation_path == &parent
        ));
        assert!(
            require_plain_directory(&parent).is_ok(),
            "the post-create check must reject even after the path becomes plain again"
        );
        assert!(
            external.join("claim.lock").is_file(),
            "the fixture must exercise creation through the transient redirect"
        );

        fs::remove_dir_all(work).unwrap();
        fs::remove_dir_all(external).unwrap();
    }

    #[test]
    fn rooted_creation_rejects_a_transient_redirect_restored_to_the_same_parent() {
        let work = temp_root("create-restored-parent-swap");
        let external = temp_root("create-restored-parent-external");
        let parent = work.join("claims");
        let original_parent = work.join("claims-original");
        let path = parent.join("claim.lock");
        fs::create_dir_all(&parent).unwrap();
        fs::create_dir(&external).unwrap();

        let hook_parent = parent.clone();
        let hook_original_parent = original_parent.clone();
        let hook_external = external.clone();
        set_create_new_plain_file_hook(move || {
            fs::rename(&hook_parent, &hook_original_parent)?;
            symlink_directory(&hook_external, &hook_parent)
        });
        let post_hook_parent = parent.clone();
        let post_hook_original_parent = original_parent.clone();
        let post_hook_path = path.clone();
        set_create_new_plain_file_post_hook(move || {
            remove_directory_link(&post_hook_parent)?;
            fs::rename(&post_hook_original_parent, &post_hook_parent)?;
            fs::write(&post_hook_path, "decoy\n")
        });

        let error = create_new_plain_file_rooted(&work, &path).unwrap_err();

        assert!(matches!(
            plain_read_violation(&error),
            Some(PlainReadViolation::NotPlain { path: violation_path })
                if violation_path == &path
        ));
        assert_eq!(fs::read_to_string(&path).unwrap(), "decoy\n");
        assert!(
            external.join("claim.lock").is_file(),
            "the fixture must exercise creation through the transient redirect"
        );

        fs::remove_dir_all(work).unwrap();
        fs::remove_dir_all(external).unwrap();
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
        let stamp = optional_plain_file_stamp(&work, &path, 32)
            .unwrap()
            .expect("plain file has a cache stamp");
        assert_eq!(stamp.len(), 6);
        assert!(!stamp.is_empty());
        let error = read_optional_text(&work, &path, 4).unwrap_err();
        assert!(matches!(
            plain_read_violation(&error),
            Some(PlainReadViolation::Oversize {
                path: violation_path,
                max_bytes: 4
            }) if violation_path == &path
        ));
        assert!(optional_plain_file_stamp(&work, &path, 4).is_err());
        assert!(replace_file(&work, &path, b"too large", 4).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "state\n");
        let _ = fs::remove_dir_all(work);
    }

    /// The confined optional reader is the primitive that `headless`, `vcs`, and `native_port`
    /// delegate their former private copies to. Absence of the artifact — or of its parent chain
    /// — must stay a recoverable `None`, while every confinement, limit, and encoding violation
    /// must fail loudly: those two halves are exactly what those modules map onto their own error
    /// types, so a silent change here would weaken three trust boundaries at once.
    #[test]
    fn confined_optional_reader_separates_absence_from_violation() {
        let work = temp_root("optional-reader");
        let outside = temp_root("optional-reader-outside");
        fs::create_dir(&work).unwrap();
        fs::create_dir(&outside).unwrap();

        assert_eq!(
            read_optional_text(&work, &work.join("review.md"), 32).unwrap(),
            None
        );
        assert_eq!(
            read_optional_text(&work, &work.join("absent/review.md"), 32).unwrap(),
            None
        );
        assert!(
            plain_directory_entries(&work, &work.join("absent"))
                .unwrap()
                .is_none()
        );

        let directory = work.join("nested");
        fs::create_dir(&directory).unwrap();
        let error = read_optional_text(&work, &directory, 32).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("plain regular file"));
        assert!(matches!(
            plain_read_violation(&error),
            Some(PlainReadViolation::NotPlain { path }) if path == &directory
        ));

        let binary = directory.join("binary.md");
        fs::write(&binary, [0xff_u8, 0xfe]).unwrap();
        let error = read_optional_text(&work, &binary, 32).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not UTF-8"));

        // A parent component that exists but is not a plain directory must fail closed instead of
        // degrading into "artifact absent". On a host without symlink privileges this is the only
        // locally executable proof that the parent chain is *proven* rather than merely walked.
        let error = read_optional_text(&work, &binary.join("leaf.md"), 32).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("plain directory"));
        assert!(matches!(
            plain_read_violation(&error),
            Some(PlainReadViolation::ParentNotPlain { path }) if path == &binary
        ));

        let external = outside.join("outside.md");
        fs::write(&external, "outside\n").unwrap();
        assert_eq!(
            read_optional_text(&work, &external, 32).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            read_optional_text(&work, &work, 32).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        let _ = fs::remove_dir_all(work);
        let _ = fs::remove_dir_all(outside);
    }

    #[test]
    fn bounded_directory_enumeration_fails_loudly_at_its_ceiling() {
        let work = temp_root("bounded-directory");
        let directory = work.join("approvals");
        fs::create_dir(&work).unwrap();
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("one.json"), "{}\n").unwrap();
        fs::write(directory.join("two.json"), "{}\n").unwrap();

        let error = plain_directory_entries_bounded(&work, &directory, 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds 1 entries"));
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
            assert!(optional_plain_file_stamp(&work, &linked_file, 32).is_err());
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
            assert!(optional_plain_file_stamp(&work, &nested, 32).is_err());
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
