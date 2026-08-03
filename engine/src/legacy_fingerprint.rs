//! Byte-compatible final-state fingerprint components for Orchestra cutover scenarios.
//!
//! Orchestra's disposable lifecycle harness projects a run into four timestamp-independent
//! strings: a typed VCS tree digest supplied by the caller, normalized queue rows, archived task
//! IDs plus live descriptor statuses, and an event-id-deduplicated outbox identity multiset.  It
//! then hashes their fixed newline-separated envelope.  This module owns the non-VCS projection
//! in Rust and the typed Git/JJ tree inventory ([`committed_tree_inventory`]) that feeds it, so
//! the native engine can be compared against the legacy harness over the same committed surface
//! without accepting timestamps, UUIDs, or Markdown layout as false drift.  The end-to-end
//! differential comparison against the legacy `tools/harness.ps1` oracle lives in the opt-in,
//! `#[ignore]`d `engine/tests/legacy_fingerprint_diff_fixture.rs`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use crate::events::outbox::{EventStreamLayout, event_stream_snapshot, open_existing_plain_outbox};
use crate::task_id::is_task_id;
use sha2::{Digest, Sha256};
use vcs_core::{BackendKind, Repo};
use vcs_diff::DiffSpec;
use vcs_git::{GitApi, RevSpec};
use vcs_jj::{JjApi, RevsetExpr};

/// How many times the outbox projection is rebuilt when guarded rotation keeps moving the stream
/// underneath it. Rotation happens at most once per published cohort, so this is generous.
const MAX_OUTBOX_SNAPSHOT_ATTEMPTS: usize = 8;

/// A committed tree entry supplied by a typed VCS inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub path: PathBuf,
    /// Text decoded by the legacy harness's UTF-8 stdout reader.  The harness then feeds this
    /// exact string back into UTF-8 SHA-256, so this deliberately is not a raw blob API.
    pub content: String,
}

impl TreeEntry {
    pub fn new(path: impl Into<PathBuf>, content: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            content: content.into(),
        }
    }
}

/// The four legacy components and their exact combined SHA-256 envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalStateFingerprint {
    pub tree: String,
    pub queue: String,
    pub archive: String,
    pub outbox: String,
    pub combined: String,
}

#[derive(Debug)]
pub enum FingerprintError {
    Io(io::Error),
    InvalidTreePath(PathBuf),
    DuplicateTreePath(String),
    /// A typed Git/JJ inventory operation failed, or the caller supplied an invalid revision.
    /// The message preserves the backend/ProcessKit diagnostic without leaking argv.
    Vcs(String),
}

impl fmt::Display for FingerprintError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "legacy fingerprint I/O error: {error}"),
            Self::InvalidTreePath(path) => write!(
                formatter,
                "legacy fingerprint tree entry must be a normalized relative path: {}",
                path.display()
            ),
            Self::DuplicateTreePath(path) => {
                write!(
                    formatter,
                    "legacy fingerprint tree inventory duplicates {path:?}"
                )
            }
            Self::Vcs(message) => {
                write!(
                    formatter,
                    "legacy fingerprint tree inventory error: {message}"
                )
            }
        }
    }
}

impl std::error::Error for FingerprintError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidTreePath(_) | Self::DuplicateTreePath(_) | Self::Vcs(_) => None,
        }
    }
}

impl From<io::Error> for FingerprintError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, FingerprintError>;

/// Hash a complete committed tree with Orchestra's `path=sha256(UTF-8-text)` contract.
pub fn tree_digest(entries: impl IntoIterator<Item = TreeEntry>) -> Result<String> {
    let mut paths = BTreeMap::new();
    for entry in entries {
        let path = normalized_tree_path(&entry.path)?;
        let digest = sha256(entry.content.as_bytes());
        if paths.insert(path.clone(), digest).is_some() {
            return Err(FingerprintError::DuplicateTreePath(path));
        }
    }
    let body = paths
        .into_iter()
        .map(|(path, digest)| format!("{path}={digest}"))
        .collect::<Vec<_>>()
        .join("\n");
    Ok(sha256(body.as_bytes()))
}

/// Project one `.work` directory and a typed committed tree into the exact harness envelope.
pub fn final_state_fingerprint(
    work: &Path,
    tree: impl IntoIterator<Item = TreeEntry>,
) -> Result<FinalStateFingerprint> {
    let tree = tree_digest(tree)?;
    let queue = queue_digest(work)?;
    let archive = archive_digest(work)?;
    let outbox = outbox_digest(work)?;
    let combined = sha256(
        format!("tree={tree}\nqueue={queue}\narchive={archive}\noutbox={outbox}").as_bytes(),
    );
    Ok(FinalStateFingerprint {
        tree,
        queue,
        archive,
        outbox,
        combined,
    })
}

/// Collect the complete committed tree of `revision` as `TreeEntry` rows, symmetric to the legacy
/// harness's `git ls-tree -r` / `jj file list` oracle: every committed path plus its UTF-8 text.
/// Feed the result straight into [`tree_digest`] or [`final_state_fingerprint`] so the native
/// engine and the legacy harness are compared over the *same* committed surface rather than a
/// weaker changed-paths-only view.
///
/// The whole inventory goes through the typed `vcs-git` / `vcs-jj` boundary — never a raw
/// `git`/`jj` command line: the file set is the empty-tree→`revision` diff (Git) or the
/// `root()..revision` summary (JJ), where every path is an addition, and each file's content is
/// the backend's typed blob read (`git show <rev>:<path>` / `jj file show -r <rev> <path>`). Git
/// decodes blobs lossily, matching the harness's UTF-8 stdout reader. Path normalization (JJ
/// backslash handling, ambiguous-path rejection) is deferred to [`tree_digest`], so this function
/// carries backend paths through unchanged.
///
/// `revision` is any single-commit ref/revset the backend accepts (`HEAD`, a branch, a SHA, `@-`).
/// It is rejected when empty or flag-like by the typed `RevSpec`/`RevsetExpr` guards.
pub fn committed_tree_inventory(repo_root: &Path, revision: &str) -> Result<Vec<TreeEntry>> {
    if revision.trim().is_empty() {
        return Err(FingerprintError::Vcs(
            "committed tree revision must be non-empty".into(),
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| FingerprintError::Vcs(format!("create VCS runtime: {error}")))?;
    runtime.block_on(collect_committed_tree(repo_root, revision))
}

/// Map any typed backend / ProcessKit diagnostic into a `FingerprintError` without leaking the
/// concrete error type (or argv) into this module's public surface.
fn vcs_error(error: impl fmt::Display) -> FingerprintError {
    FingerprintError::Vcs(error.to_string())
}

async fn collect_committed_tree(repo_root: &Path, revision: &str) -> Result<Vec<TreeEntry>> {
    let repo = Repo::discover(repo_root).map_err(|error| {
        FingerprintError::Vcs(format!(
            "discover repository at {}: {error}",
            repo_root.display()
        ))
    })?;
    let root = repo.root().to_path_buf();
    match repo.kind() {
        BackendKind::Git => git_committed_tree(&repo, &root, revision).await,
        BackendKind::Jj => jj_committed_tree(&repo, &root, revision).await,
        other => Err(FingerprintError::Vcs(format!(
            "unsupported VCS backend for committed-tree inventory: {other:?}"
        ))),
    }
}

async fn git_committed_tree(repo: &Repo, dir: &Path, revision: &str) -> Result<Vec<TreeEntry>> {
    let git = repo.git().ok_or_else(|| {
        FingerprintError::Vcs("Git repository exposed no typed Git client".into())
    })?;
    // Diffing the well-known empty tree against `revision` lists the full committed tree, since
    // every path is a pure addition — this is the typed stand-in for a recursive `ls-tree`.
    let empty = git.empty_tree_oid(dir).await.map_err(vcs_error)?;
    let rev = RevSpec::new(revision.to_owned()).map_err(vcs_error)?;
    let files = git
        .diff(dir, DiffSpec::Rev(format!("{empty}..{revision}")))
        .await
        .map_err(vcs_error)?;
    let mut entries = Vec::with_capacity(files.len());
    for file in files {
        let content = git
            .show_file(dir, &rev, &to_forward_slash(&file.path))
            .await
            .map_err(vcs_error)?;
        entries.push(TreeEntry::new(file.path, content));
    }
    Ok(entries)
}

async fn jj_committed_tree(repo: &Repo, dir: &Path, revision: &str) -> Result<Vec<TreeEntry>> {
    let jj = repo
        .jj()
        .ok_or_else(|| FingerprintError::Vcs("JJ repository exposed no typed JJ client".into()))?;
    // `root()..revision` lists every path present at `revision` as an addition — the typed
    // stand-in for `jj file list -r revision`.
    let from = RevsetExpr::new("root()").map_err(vcs_error)?;
    let to = RevsetExpr::new(revision.to_owned()).map_err(vcs_error)?;
    let files = jj.diff_summary(dir, &from, &to).await.map_err(vcs_error)?;
    let mut entries = Vec::with_capacity(files.len());
    for file in files {
        let content = jj
            .file_show(dir, &to, &to_forward_slash(&file.path))
            .await
            .map_err(vcs_error)?;
        entries.push(TreeEntry::new(file.path, content));
    }
    Ok(entries)
}

/// Present a repository-relative path to the typed blob readers with `/` separators. Both backends
/// accept and re-normalize this, and it keeps a Windows-produced backslash out of the argument.
fn to_forward_slash(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn queue_digest(work: &Path) -> Result<String> {
    let text = read_optional(&work.join("Tasks_Queue.md"))?;
    let mut rows = Vec::new();
    for line in text.lines() {
        let Some((task_id, status)) = queue_row(line) else {
            continue;
        };
        let attempt = decimal_suffix(line, "попытка=")
            .map(|value| format!("/попытка={value}"))
            .unwrap_or_default();
        rows.push(format!("{task_id}={status}{attempt}"));
    }
    rows.sort();
    Ok(rows.join(";"))
}

fn archive_digest(work: &Path) -> Result<String> {
    let done = read_optional(&work.join("Tasks_Done.md"))?;
    let done_ids = task_ids_in_text(&done).into_iter().collect::<BTreeSet<_>>();

    let tasks = work.join("tasks");
    let mut descriptors = Vec::new();
    match fs::read_dir(tasks) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let task_id = entry.file_name().to_string_lossy().into_owned();
                let descriptor = read_optional(&entry.path().join("task.md"))?;
                let status = descriptor
                    .lines()
                    .find_map(|line| line.strip_prefix("Статус:").map(str::trim))
                    .unwrap_or_default();
                descriptors.push((task_id, status.to_string()));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    // The harness orders directory objects by `Name`, not by a rendered `name=status` row.
    // Keep that ordering rule explicit before we serialize the component.
    descriptors.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(format!(
        "done:{}|descr:{}",
        done_ids.into_iter().collect::<Vec<_>>().join(","),
        descriptors
            .into_iter()
            .map(|(task_id, status)| format!("{task_id}={status}"))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

/// Project the outbox component, retrying if guarded rotation moves the stream mid-projection.
///
/// The projection reads one source after another through a layout resolved beforehand, and
/// rotation replaces the active file underneath exactly that sequence. A digest built from a
/// half-rotated view would report drift the run never had, so a projection that observed a
/// rotation is discarded and rebuilt.
fn outbox_digest(work: &Path) -> Result<String> {
    let path = work.join("events.jsonl");
    for _ in 0..MAX_OUTBOX_SNAPSHOT_ATTEMPTS {
        let snapshot = event_stream_snapshot(&path)?;
        let digest = outbox_digest_of(&snapshot.layout)?;
        if snapshot.is_still_current()? {
            return Ok(digest);
        }
    }
    Err(FingerprintError::Io(io::Error::other(
        "events stream was rotated during every attempted outbox projection",
    )))
}

fn outbox_digest_of(layout: &EventStreamLayout) -> Result<String> {
    let mut event_ids = BTreeSet::new();
    let mut identities = Vec::new();
    for source in &layout.sources {
        let mut file = open_existing_plain_outbox(&source.path)?;
        file.seek(SeekFrom::Start(source.physical_start))?;
        let byte_len = source.end_offset - source.start_offset;
        let mut text = String::new();
        (&mut file).take(byte_len).read_to_string(&mut text)?;
        let text = if source.start_offset == 0 {
            text.strip_prefix('\u{feff}').unwrap_or(&text)
        } else {
            &text
        };
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let Some(event_id) = value.get("event_id").map(json_scalar) else {
                continue;
            };
            if !event_ids.insert(event_id) {
                continue;
            }
            let event_type = value.get("type").map(json_scalar).unwrap_or_default();
            let batch = value.get("batch_id").map(json_scalar).unwrap_or_default();
            let task = value.get("task_id").map(json_scalar).unwrap_or_default();
            let (from, to) = value
                .get("payload")
                .and_then(serde_json::Value::as_object)
                .map(|payload| {
                    (
                        payload.get("from").map(json_scalar).unwrap_or_default(),
                        payload.get("to").map(json_scalar).unwrap_or_default(),
                    )
                })
                .unwrap_or_default();
            let transition = if from.is_empty() && to.is_empty() {
                String::new()
            } else {
                format!("{from}>{to}")
            };
            identities.push(format!("{event_type}|{batch}|{task}|{transition}"));
        }
    }
    identities.sort();
    Ok(identities.join(";"))
}

fn json_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Null => String::new(),
        value => value.to_string(),
    }
}

fn read_optional(path: &Path) -> Result<String> {
    match fs::read_to_string(path) {
        // .NET's UTF-8 reader used by `Read-Text` consumes a leading BOM. Rust's
        // `read_to_string` intentionally does not, so make that compatibility behavior explicit.
        Ok(text) => Ok(text.strip_prefix('\u{feff}').unwrap_or(&text).to_string()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error.into()),
    }
}

fn normalized_tree_path(path: &Path) -> Result<String> {
    let mut normalized = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                // `jj file list` on Windows can yield backslashes, which the legacy harness
                // explicitly converts to `/` before sorting. On a non-Windows host `Path`
                // would otherwise retain them as a literal filename character.
                for segment in part.to_string_lossy().split('\\') {
                    if segment.is_empty() || segment == "." || segment == ".." {
                        return Err(FingerprintError::InvalidTreePath(path.to_path_buf()));
                    }
                    normalized.push(segment.to_string());
                }
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(FingerprintError::InvalidTreePath(path.to_path_buf()));
            }
        }
    }
    if normalized.is_empty() {
        return Err(FingerprintError::InvalidTreePath(path.to_path_buf()));
    }
    Ok(normalized.join("/"))
}

fn queue_row(line: &str) -> Option<(&str, &str)> {
    let after_marker = line.strip_prefix("###")?;
    let rest = after_marker.trim_start();
    if rest.len() == after_marker.len() {
        return None;
    }
    let rest = rest.strip_prefix('[')?;
    let (task_id, _) = rest.split_once(']')?;
    if !is_task_id(task_id) {
        return None;
    }
    // The legacy regex's non-greedy title match backtracks over title dashes until one is
    // followed by `статус:`. A title itself may legitimately contain an em dash.
    let status = line
        .split('—')
        .skip(1)
        .map(str::trim_start)
        .find_map(|after_dash| after_dash.strip_prefix("статус:").map(str::trim_start))?;
    let status = status.split('·').next().unwrap_or_default().trim();
    (!status.is_empty()).then_some((task_id, status))
}

fn task_ids_in_text(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut ids = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'[' {
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end] != b']' {
            end += 1;
        }
        if end < bytes.len()
            && std::str::from_utf8(&bytes[start..end])
                .ok()
                .is_some_and(is_task_id)
        {
            ids.push(String::from_utf8_lossy(&bytes[start..end]).into_owned());
        }
        index = end.saturating_add(1);
    }
    ids
}

fn decimal_suffix<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    let (_, value) = line.split_once(marker)?;
    let digits = value
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (digits > 0).then_some(&value[..digits])
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::events::{
        Actor, ActorKind, Event, EventType, Outbox, RotationPolicy, SCHEMA_VERSION,
        deterministic_event_id,
    };

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn work() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "orchestrail-legacy-fingerprint-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).expect("create fingerprint fixture");
        root
    }

    fn lifecycle_event(key: &str, event_type: EventType) -> Event {
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
            payload: serde_json::Map::new(),
        }
    }

    #[test]
    fn projects_legacy_queue_archive_and_outbox_components_without_timestamp_noise() {
        let work = work();
        fs::create_dir_all(work.join("tasks/T-9")).unwrap();
        fs::write(
            work.join("Tasks_Queue.md"),
            "### [T-2] Later — статус: не начата · попытка=3\n### [T-1] First — статус: эскалирована\n",
        )
        .unwrap();
        fs::write(
            work.join("Tasks_Done.md"),
            "### [T-7] Done\n### [T-7] Duplicate mention\n",
        )
        .unwrap();
        fs::write(work.join("tasks/T-9/task.md"), "# T-9\nСтатус: на ревью\n").unwrap();
        fs::write(
            work.join("events.jsonl"),
            concat!(
                "{\"event_id\":\"evt-1\",\"occurred_at\":\"2026-01-01T00:00:00Z\",\"type\":\"task.status_changed\",\"task_id\":\"T-2\",\"payload\":{\"from\":\"не начата\",\"to\":\"в работе\"}}\n",
                "not-json\n",
                "{\"event_id\":\"evt-1\",\"occurred_at\":\"2027-01-01T00:00:00Z\",\"type\":\"ignored-replay\"}\n",
                "{\"event_id\":\"evt-2\",\"type\":\"task.status_changed\",\"task_id\":\"T-2\",\"payload\":{\"from\":\"не начата\",\"to\":\"в работе\"}}\n"
            ),
        )
        .unwrap();

        let fingerprint = final_state_fingerprint(
            &work,
            [
                TreeEntry::new("z.txt", "z\n"),
                TreeEntry::new("src/main.rs", "fn main() {}\n"),
            ],
        )
        .expect("project legacy fixture");
        assert_eq!(
            fingerprint.queue,
            "T-1=эскалирована;T-2=не начата/попытка=3"
        );
        assert_eq!(fingerprint.archive, "done:T-7|descr:T-9=на ревью");
        assert_eq!(
            fingerprint.outbox,
            "task.status_changed||T-2|не начата>в работе;task.status_changed||T-2|не начата>в работе"
        );
        assert_eq!(
            fingerprint.tree,
            tree_digest([
                TreeEntry::new("src/main.rs", "fn main() {}\n"),
                TreeEntry::new("z.txt", "z\n"),
            ])
            .unwrap(),
            "tree inventory order is ignored"
        );
        assert_eq!(
            fingerprint.tree,
            "98801d61ce21434f49727c0cd25053f966cd502979e54046030bebd108a31318"
        );
        assert_eq!(
            fingerprint.combined,
            "7b400d7c0af8af649db695c3c1cbd3f5d2d1ad475ffa51aafccb1bfdb40ab1dd"
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn rotated_and_monolithic_outboxes_have_identical_legacy_fingerprints() {
        let monolithic = work();
        let rotated = work();
        let events = [
            lifecycle_event("open", EventType::CohortOpened),
            lifecycle_event("published", EventType::CohortPublished),
            lifecycle_event("closed", EventType::CohortClosed),
        ];
        let plain_outbox = Outbox::new(&monolithic);
        let rotating_outbox =
            Outbox::with_rotation_policy(&rotated, RotationPolicy::enabled_above(1));
        for event in &events {
            plain_outbox.append_idempotent(event).unwrap();
            rotating_outbox.append_idempotent(event).unwrap();
        }

        let tree = [TreeEntry::new("app.txt", "same\n")];
        let plain = final_state_fingerprint(&monolithic, tree.clone()).unwrap();
        let archived = final_state_fingerprint(&rotated, tree).unwrap();
        assert_eq!(archived.outbox, plain.outbox);
        assert_eq!(archived.combined, plain.combined);
        let _ = fs::remove_dir_all(monolithic);
        let _ = fs::remove_dir_all(rotated);
    }

    #[test]
    fn tree_digest_rejects_ambiguous_paths() {
        assert!(matches!(
            tree_digest([
                TreeEntry::new("src/../main.rs", "first"),
                TreeEntry::new("main.rs", "second"),
            ]),
            Err(FingerprintError::InvalidTreePath(_))
        ));
    }

    #[test]
    fn tree_digest_uses_the_legacy_jj_backslash_normalization_on_every_host() {
        assert_eq!(
            tree_digest([TreeEntry::new(r"src\main.rs", "fn main() {}\n")]).unwrap(),
            tree_digest([TreeEntry::new("src/main.rs", "fn main() {}\n")]).unwrap()
        );
    }

    #[test]
    fn queue_projection_matches_the_harness_header_grammar_and_utf8_bom_behavior() {
        let work = work();
        fs::write(
            work.join("Tasks_Queue.md"),
            concat!(
                "\u{feff}### [T-2] valid — статус: не начата · попытка=2\n",
                "###[T-3] missing heading space — статус: в работе\n",
                "### [T-4] wrong post-dash token — ignored статус: на ревью\n",
                "### [T-5] title — still title — статус: готова к слиянию\n"
            ),
        )
        .unwrap();

        let fingerprint = final_state_fingerprint(&work, [TreeEntry::new("app.txt", "base\n")])
            .expect("project queue grammar fixture");
        assert_eq!(
            fingerprint.queue,
            "T-2=не начата/попытка=2;T-5=готова к слиянию"
        );
        let _ = fs::remove_dir_all(work);
    }

    /// Run a scratch-repository setup subprocess. Returns `false` when the tool is missing or the
    /// command failed, so a test can self-skip on a host without `git`/`jj` (like the `vcs_fixture`
    /// boundary proof) instead of turning an unavailable oracle into a false failure. This is only
    /// disposable test scaffolding; the product inventory itself never shells out.
    fn setup_ok(program: &str, args: &[&str], cwd: &Path, envs: &[(&str, &str)]) -> bool {
        let mut command = std::process::Command::new(program);
        command.args(args).current_dir(cwd);
        for (key, value) in envs {
            command.env(key, value);
        }
        command
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    #[test]
    fn committed_tree_inventory_reads_a_real_git_tree_like_the_harness_oracle() {
        let repo = work();
        let no_env: &[(&str, &str)] = &[];
        if !setup_ok("git", &["init", "--initial-branch=main"], &repo, no_env) {
            eprintln!("SKIP: git is unavailable for the committed-tree inventory test");
            let _ = fs::remove_dir_all(&repo);
            return;
        }
        assert!(setup_ok(
            "git",
            &["config", "user.name", "Orchestrail Test"],
            &repo,
            no_env
        ));
        assert!(setup_ok(
            "git",
            &["config", "user.email", "test@example.invalid"],
            &repo,
            no_env
        ));
        // Compare committed blobs verbatim: keep autocrlf from rewriting LF into CRLF on Windows.
        assert!(setup_ok(
            "git",
            &["config", "core.autocrlf", "false"],
            &repo,
            no_env
        ));

        fs::write(repo.join("alpha.txt"), "alpha\n").unwrap();
        fs::create_dir_all(repo.join("nested")).unwrap();
        fs::write(repo.join("nested/beta.txt"), "beta\n").unwrap();
        assert!(setup_ok("git", &["add", "-A"], &repo, no_env));
        assert!(setup_ok("git", &["commit", "-m", "seed"], &repo, no_env));

        let inventory =
            committed_tree_inventory(&repo, "HEAD").expect("git committed-tree inventory");
        assert_committed_tree_matches(inventory);
        let _ = fs::remove_dir_all(&repo);
    }

    #[test]
    fn committed_tree_inventory_reads_a_real_jj_tree_like_the_harness_oracle() {
        let repo = work();
        // The jj identity config must live OUTSIDE the working copy, or jj would snapshot it into
        // the committed tree and pollute the inventory.
        let config = repo.with_extension("jjconfig.toml");
        fs::write(
            &config,
            "[user]\nname = \"Orchestrail Test\"\nemail = \"test@example.invalid\"\n",
        )
        .unwrap();
        let envs: &[(&str, &str)] = &[("JJ_CONFIG", config.to_str().unwrap())];

        if !setup_ok("jj", &["git", "init"], &repo, envs) {
            eprintln!("SKIP: jj is unavailable for the committed-tree inventory test");
            let _ = fs::remove_dir_all(&repo);
            let _ = fs::remove_file(&config);
            return;
        }

        fs::write(repo.join("alpha.txt"), "alpha\n").unwrap();
        fs::create_dir_all(repo.join("nested")).unwrap();
        fs::write(repo.join("nested/beta.txt"), "beta\n").unwrap();
        // Describe finalizes the working-copy commit `@`; `jj new` leaves it as the immutable
        // parent `@-`, the stable committed tree the inventory reads.
        assert!(setup_ok("jj", &["describe", "-m", "seed"], &repo, envs));
        assert!(setup_ok("jj", &["new"], &repo, envs));

        let inventory = committed_tree_inventory(&repo, "@-").expect("jj committed-tree inventory");
        assert_committed_tree_matches(inventory);
        let _ = fs::remove_dir_all(&repo);
        let _ = fs::remove_file(&config);
    }

    /// Both backends must enumerate the same two committed paths with byte-identical UTF-8 content
    /// and reproduce the harness `path=sha256(content)` tree digest.
    fn assert_committed_tree_matches(inventory: Vec<TreeEntry>) {
        assert_eq!(inventory.len(), 2, "the full committed tree is enumerated");
        let by_path: BTreeMap<String, String> = inventory
            .iter()
            .map(|entry| (to_forward_slash(&entry.path), entry.content.clone()))
            .collect();
        assert_eq!(
            by_path.get("alpha.txt").map(String::as_str),
            Some("alpha\n"),
            "root file content round-trips"
        );
        assert_eq!(
            by_path.get("nested/beta.txt").map(String::as_str),
            Some("beta\n"),
            "nested file is enumerated with a forward-slash path"
        );
        let expected = [
            TreeEntry::new("alpha.txt", "alpha\n"),
            TreeEntry::new("nested/beta.txt", "beta\n"),
        ];
        assert_eq!(
            tree_digest(inventory).unwrap(),
            tree_digest(expected).unwrap(),
            "typed inventory reproduces the harness tree digest"
        );
    }

    #[test]
    fn committed_tree_inventory_rejects_an_empty_revision_without_touching_the_backend() {
        let repo = work();
        assert!(matches!(
            committed_tree_inventory(&repo, "   "),
            Err(FingerprintError::Vcs(_))
        ));
        let _ = fs::remove_dir_all(&repo);
    }
}
