//! Typed Git/JJ boundary for the deterministic processor.
//!
//! The legacy processor manipulates branches and worktrees in many phases.  This module is the
//! only Rust-engine route for those operations: it builds on `vcs-core`'s common Git/JJ facade,
//! keeps VCS-like user input out of argv, and verifies a managed worktree after every creation.
//! It deliberately contains no shell strings or direct process spawning.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vcs_cli_support::reject_flag_like;
use vcs_core::{BackendKind, BranchDelete, MergeProbe, Repo, WorktreeCreate, WorktreeRemove};
use vcs_diff::DiffSpec;
use vcs_git::{
    CloneSpec, Git, GitApi, GitPush, MergeCheck, MergeCommit, MergeNoCommit, RefName, RevSpec,
    WorktreeAdd,
};
use vcs_jj::{BookmarkMove, BookmarkName, JjApi, RevsetExpr, WorkspaceAdd};

use crate::approval::ContentBoundApprovalManifest;
use crate::recovery::{
    IntegrationRepositoryObservation, PublicationObservation, RecoveryInventory,
    TaskRepositoryObservation,
};
use crate::state::Snapshot;
use crate::task_id::is_task_id;
use crate::work_fs;

const MAX_IGNORE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_FINGERPRINT_BYTES: u64 = 64 * 1024 * 1024;

/// A backend-neutral snapshot needed by processor recovery and publication decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositorySnapshot {
    pub backend: BackendKind,
    pub root: PathBuf,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub dirty: bool,
    pub conflicted: bool,
}

/// A verified task worktree (Git) or workspace (JJ).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskWorkspace {
    pub task_id: String,
    pub branch: String,
    /// Canonical `.work` root which owns this task's managed `worktrees` directory.
    pub work: PathBuf,
    pub path: PathBuf,
    pub backend: BackendKind,
}

/// A verified singleton integration worktree/workspace for one cohort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationWorkspace {
    pub batch_id: String,
    pub branch: String,
    /// Canonical `.work` root which owns this singleton workspace.
    pub work: PathBuf,
    pub path: PathBuf,
    pub backend: BackendKind,
}

/// Result of the destructive-but-retry-safe recovery after a failed remote publication.  The
/// remote can win a race after the initial failed-push observation; in that case the adapter must
/// record the normal publication boundary rather than resetting a now-published primary branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationReanchorOutcome {
    Reanchored,
    Published { head: String },
}

/// Exact local trunk transition produced by the separate release-sync mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseSyncHead {
    pub previous: String,
    pub current: String,
}

/// A tag resolved through the typed Git backend and proved reachable from synchronized trunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseTagEvidence {
    pub tag: String,
    pub revision: String,
}

/// Exact typed range handed to the semantic release-notes leaf. Keeping the complete diff in an
/// engine-owned artifact prevents that leaf from constructing Git/JJ command lines itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseNotesRangeEvidence {
    pub schema: String,
    pub base: String,
    pub head: String,
    pub files: Vec<TaskReviewRangeFile>,
}

/// A typed, abortable integration merge which is waiting for the checkpointed merger leaf.
/// `pre_merge_head` remains the durable integration branch tip until `finalize` succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConflictSession {
    pub task_id: String,
    pub pre_merge_head: String,
    /// Git records all changed paths in the typed merge result, including cleanly auto-merged
    /// files. JJ records its exact conflict set because a multi-parent conflict has no gap-free
    /// range-diff surface. `paths` below is always the subset presented to the merger leaf.
    pub merge_paths: Vec<PathBuf>,
    pub paths: Vec<PathBuf>,
    /// Content snapshots of every non-conflicting changed path. They are taken after the typed
    /// conflict merge begins and rechecked before finalization, so the merger leaf cannot smuggle
    /// an edit to a clean auto-merged file into the eventual merge commit.
    pub protected_paths: Vec<MergePathFingerprint>,
}

static REMOTE_PROOF_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// An engine-owned temporary parent for a remote-publication proof. The clone lives below a
/// directory created by this process, never in the managed repository or its `.work` control
/// plane. Dropping it therefore cannot remove an operator-owned path.
struct RemoteProofClone {
    parent: PathBuf,
    repository: PathBuf,
}

impl RemoteProofClone {
    fn create() -> Result<Self> {
        for _ in 0..64 {
            let sequence = REMOTE_PROOF_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let parent = std::env::temp_dir().join(format!(
                "orchestrail-jj-publication-proof-{}-{sequence}",
                std::process::id()
            ));
            match fs::create_dir(&parent) {
                Ok(()) => {
                    let repository = parent.join("remote.git");
                    return Ok(Self { parent, repository });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(VcsError::Io(error)),
            }
        }
        Err(VcsError::Runtime(
            "could not allocate an isolated temporary directory for JJ remote publication proof"
                .into(),
        ))
    }
}

impl Drop for RemoteProofClone {
    fn drop(&mut self) {
        // The parent was created above with a literal child path, before any remote input was
        // consulted. Best-effort cleanup must not hide an otherwise valid remote observation.
        let _ = fs::remove_dir_all(&self.parent);
    }
}

/// A byte-for-byte content snapshot of a path in an intentionally paused merge. `None` records
/// an expected absence such as the source side of a rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePathFingerprint {
    pub path: PathBuf,
    pub sha256: Option<String>,
}

/// Immutable coordinates that the finalization boundary must prove still describe the paused
/// merge. Grouping them prevents a caller from accidentally mixing paths or refs from different
/// conflict sessions.
#[derive(Debug, Clone, Copy)]
pub struct MergeResolutionFinalization<'a> {
    pub task_head: &'a str,
    pub pre_merge_head: &'a str,
    pub merge_paths: &'a [PathBuf],
    pub conflict_paths: &'a [PathBuf],
    pub protected_paths: &'a [MergePathFingerprint],
}

/// Typed, human-inspectable content surface of the exact integration range an operator is asked
/// to approve. The digest belongs to the VCS-produced file-diff section, which includes both
/// path identity and changed content (or the backend's binary object identity), rather than to a
/// model report or a mutable working-tree observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApprovalChangeManifest {
    pub schema: &'static str,
    pub base: String,
    pub head: String,
    pub changes: Vec<ApprovalChangeEntry>,
}

/// One renamed-or-ordinary path record in [`ApprovalChangeManifest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApprovalChangeEntry {
    pub path: String,
    pub old_path: Option<String>,
    pub diff_sha256: String,
}

impl ApprovalChangeManifest {
    /// Stable approval identity for this exact VCS range and typed diff content. Length-prefixing
    /// prevents path/value concatenation ambiguity without relying on a display separator.
    pub fn fingerprint(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"orchestrail/approval-change-manifest/v1\0");
        update_manifest_field(&mut digest, &self.base);
        update_manifest_field(&mut digest, &self.head);
        for change in &self.changes {
            update_manifest_field(&mut digest, &change.path);
            update_manifest_field(&mut digest, change.old_path.as_deref().unwrap_or(""));
            update_manifest_field(&mut digest, &change.diff_sha256);
        }
        format!("{:x}", digest.finalize())
    }
}

impl ContentBoundApprovalManifest for ApprovalChangeManifest {
    fn fingerprint(&self) -> String {
        ApprovalChangeManifest::fingerprint(self)
    }
}

/// Immutable, content-bound review scope for one task-review attempt.  It is produced from the
/// typed VCS range rather than agent prose, so a reviewer and a later operator can inspect the
/// exact base-to-head surface that the reducer authorised.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskReviewRangeEvidence {
    pub schema: String,
    pub base: String,
    pub head: String,
    pub files: Vec<TaskReviewRangeFile>,
}

/// One typed file-diff section retained in [`TaskReviewRangeEvidence`].  `raw` is the
/// backend-produced git-format section, including the complete hunk content (or binary object
/// identity), while the digest gives the evidence a stable compact identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskReviewRangeFile {
    pub path: String,
    pub old_path: Option<String>,
    pub diff_sha256: String,
    pub raw: String,
}

impl TaskReviewRangeEvidence {
    /// Stable identity over exact coordinates and content.  The explicit fields make a range
    /// rewrite or a content-only change detectable without trusting the rendered Markdown review.
    pub fn fingerprint(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"orchestrail/task-review-range@1\0");
        update_manifest_field(&mut digest, &self.base);
        update_manifest_field(&mut digest, &self.head);
        for file in &self.files {
            update_manifest_field(&mut digest, &file.path);
            update_manifest_field(&mut digest, file.old_path.as_deref().unwrap_or(""));
            update_manifest_field(&mut digest, &file.diff_sha256);
        }
        format!("{:x}", digest.finalize())
    }
}

/// A focused VCS error that preserves the typed backend diagnostic but makes a processor-side
/// managed-path violation explicit.
#[derive(Debug)]
pub enum VcsError {
    InvalidInput(String),
    ManagedPath(String),
    Runtime(String),
    /// The local fast-forward completed, then the separately typed remote publication command
    /// failed. Callers may now inspect the remote boundary without confusing this with an
    /// earlier local fast-forward/ref/worktree failure.
    PublicationPushFailed(String),
    /// The primary ref moved outside the processor before the local fast-forward. Replaying this
    /// condition must retain that local primary rather than replacing it with a remote ref.
    PublicationLocalDivergence(String),
    /// The rollback-clean typed probe found paths that cannot be merged automatically. The caller
    /// may quarantine this task, but may not pretend a partial merge occurred.
    MergeConflict {
        task_id: String,
        paths: Vec<PathBuf>,
    },
    Io(std::io::Error),
    Backend(vcs_core::Error),
    ProcessKit(processkit::Error),
}

impl fmt::Display for VcsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VcsError::InvalidInput(message)
            | VcsError::ManagedPath(message)
            | VcsError::Runtime(message)
            | VcsError::PublicationPushFailed(message)
            | VcsError::PublicationLocalDivergence(message) => f.write_str(message),
            VcsError::MergeConflict { task_id, paths } => write!(
                f,
                "task {task_id} conflicts in: {}",
                paths
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            VcsError::Io(error) => write!(f, "filesystem error: {error}"),
            VcsError::Backend(error) => write!(f, "VCS operation failed: {error}"),
            VcsError::ProcessKit(error) => write!(f, "typed Git operation failed: {error}"),
        }
    }
}

impl std::error::Error for VcsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VcsError::Io(error) => Some(error),
            VcsError::Backend(error) => Some(error),
            VcsError::ProcessKit(error) => Some(error),
            VcsError::InvalidInput(_)
            | VcsError::ManagedPath(_)
            | VcsError::Runtime(_)
            | VcsError::PublicationPushFailed(_)
            | VcsError::PublicationLocalDivergence(_)
            | VcsError::MergeConflict { .. } => None,
        }
    }
}

impl From<std::io::Error> for VcsError {
    fn from(error: std::io::Error) -> Self {
        VcsError::Io(error)
    }
}

impl From<vcs_core::Error> for VcsError {
    fn from(error: vcs_core::Error) -> Self {
        VcsError::Backend(error)
    }
}

pub type Result<T> = std::result::Result<T, VcsError>;

/// A repository discovered through `vcs-core`, anchored to its physical root.
#[derive(Debug, Clone)]
pub struct VcsService {
    root: PathBuf,
    backend: BackendKind,
}

impl VcsService {
    /// Validate operator-supplied release identity text before lease acquisition or any fetch.
    pub fn validate_release_identity(version: &str, explicit_tag: Option<&str>) -> Result<()> {
        validate_release_text("release version", version, 120)?;
        if let Some(tag) = explicit_tag {
            validate_release_text("release tag", tag, 240)?;
        }
        Ok(())
    }

    /// Validate an explicitly configured release trunk before lease acquisition. Auto-detected
    /// branch names are still validated at the typed VCS boundary after discovery.
    pub fn validate_release_trunk(base: &str) -> Result<()> {
        validate_release_text("release trunk", base, 240)
    }

    /// Discover the Git or JJ repository at or above `root`.  `vcs-core` prefers a valid JJ
    /// repository in colocated checkouts, matching Orchestra's VCS-selection rule.
    pub fn discover(root: impl AsRef<Path>) -> Result<VcsService> {
        let repo = Repo::discover(root)?;
        Ok(VcsService {
            root: canonical_path(repo.root())?,
            backend: repo.kind(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn backend(&self) -> BackendKind {
        self.backend
    }

    /// Require the operator-selected project root to be the physical root discovered by the
    /// typed VCS backend. Without this proof a nested `<subdir>/.work` could masquerade as an
    /// empty live control plane while Git/JJ operations still target the parent repository.
    pub fn ensure_selected_repository_root(&self, selected_root: &Path) -> Result<()> {
        let selected = canonical_path(selected_root)?;
        if selected != self.root {
            return Err(VcsError::ManagedPath(format!(
                "selected project root {} is not the discovered repository root {}",
                selected.display(),
                self.root.display()
            )));
        }
        Ok(())
    }

    /// Refuse release-only operation while any managed task/integration workspace remains,
    /// including a VCS registration whose physical path disappeared. Such residue belongs to
    /// Phase-0 recovery and is evidence of an unfinished cohort even when Markdown was lost.
    pub fn ensure_no_managed_workspaces(&self, work: &Path) -> Result<()> {
        let root = canonical_path(&self.root)?;
        let work = canonical_path(work)?;
        if !work.starts_with(&root) {
            return Err(VcsError::ManagedPath(format!(
                "release control plane {} escapes repository root {}",
                work.display(),
                root.display()
            )));
        }
        let managed = work.join("worktrees");
        if let Some(entries) = work_fs::plain_directory_entries(&work, &managed)?
            && !entries.is_empty()
        {
            return Err(VcsError::ManagedPath(format!(
                "release synchronization found unfinished managed workspace entries under {}",
                managed.display()
            )));
        }
        let repo = self.repo()?;
        if let Some(entry) = self
            .block_on(repo.list_worktrees())?
            .into_iter()
            .find(|entry| {
                entry.path != root
                    && (entry.path.starts_with(&managed)
                        || entry
                            .path
                            .parent()
                            .is_some_and(|parent| same_path(parent, &managed)))
            })
        {
            return Err(VcsError::ManagedPath(format!(
                "release synchronization found unfinished VCS workspace registration {}",
                entry.path.display()
            )));
        }
        Ok(())
    }

    /// Whether the fixed publication remote used by the typed publisher is configured.
    ///
    /// `vcs-core::Repo::push` publishes to `origin` for both Git and JJ, so this probe reads the
    /// matching typed Git configuration key instead of speculatively pushing or interpreting a
    /// failed network operation. Colocated JJ uses the repository root; pure JJ keeps the same
    /// Git configuration in its private bare store.
    pub fn publication_remote_configured(&self) -> Result<bool> {
        let config_directory = self.git_backend_directory()?;
        let git = Git::hardened();
        let fetch_url =
            self.block_on_processkit(git.config_get(&config_directory, "remote.origin.url"))?;
        if fetch_url.is_some() {
            return Ok(true);
        }
        Ok(self
            .block_on_processkit(git.config_get(&config_directory, "remote.origin.pushurl"))?
            .is_some())
    }

    /// Fetch and fast-forward the exact checked-out trunk for release observation. A local-ahead
    /// or diverged trunk is rejected before mutation, so synchronization can never hide an
    /// unpublished local commit. JJ moves only the named bookmark and reanchors an empty child.
    pub fn sync_release_trunk(&self, base: &str) -> Result<ReleaseSyncHead> {
        self.sync_release_trunk_with_cancellation(base, || false)
    }

    /// Cancellation-aware release synchronization. Remote observation may take long enough for
    /// an owner lease to be lost, so authority is re-proved after each network operation and
    /// immediately before the local branch/bookmark mutation.
    pub fn sync_release_trunk_with_cancellation(
        &self,
        base: &str,
        cancelled: impl Fn() -> bool,
    ) -> Result<ReleaseSyncHead> {
        validate_ref("release trunk", base)?;
        if cancelled() {
            return Err(VcsError::Runtime(
                "release sync cancelled before VCS observation".into(),
            ));
        }
        let previous = self.require_clean_local_primary(base)?;
        let repo = self.repo()?;
        // `Repo::fetch_branch` intentionally narrows the branch refspec and JJ does not
        // necessarily import Git tags through that operation. Refresh the configured remote
        // through the published typed Git client first so release evidence is actually visible,
        // then fetch/import the exact trunk once more as the authoritative movement target.
        let git_directory = self.git_backend_directory()?;
        self.block_on_processkit(Git::hardened().fetch_from(&git_directory, "origin"))?;
        if cancelled() {
            return Err(VcsError::Runtime(
                "release sync lost owner authority after remote refresh".into(),
            ));
        }
        self.block_on(repo.fetch_branch(base))?;
        if cancelled() {
            return Err(VcsError::Runtime(
                "release sync lost owner authority after trunk fetch".into(),
            ));
        }
        let remote = self.fetched_remote_publication_target(&repo, base)?;
        if !self.revision_is_ancestor_of(&previous, &remote)? {
            return Err(VcsError::PublicationLocalDivergence(format!(
                "release sync refuses local-ahead or diverged trunk: local {base}={previous}, fetched origin/{base}={remote}"
            )));
        }
        if previous != remote {
            if cancelled() {
                return Err(VcsError::Runtime(
                    "release sync lost owner authority before local fast-forward".into(),
                ));
            }
            match self.backend {
                BackendKind::Git => {
                    let git = repo.git().ok_or_else(|| {
                        VcsError::Runtime("Git repository has no typed Git client".into())
                    })?;
                    let remote_revision = RevSpec::new(remote.clone()).map_err(|error| {
                        VcsError::Runtime(format!("invalid fetched release revision: {error}"))
                    })?;
                    self.block_on_processkit(
                        git.merge_commit(&self.root, MergeCommit::branch(remote_revision)),
                    )
                    .map_err(|error| {
                        VcsError::Runtime(format!("typed release fast-forward failed: {error}"))
                    })?;
                }
                BackendKind::Jj => {
                    let jj = repo.jj().ok_or_else(|| {
                        VcsError::Runtime("JJ repository has no typed JJ client".into())
                    })?;
                    let bookmark = BookmarkName::new(base.to_string()).map_err(|error| {
                        VcsError::Runtime(format!("invalid release bookmark: {error}"))
                    })?;
                    let revision = RevsetExpr::new(remote.clone()).map_err(|error| {
                        VcsError::Runtime(format!("invalid fetched release revision: {error}"))
                    })?;
                    self.block_on_processkit(
                        jj.bookmark_move(&self.root, BookmarkMove::new(bookmark, revision)),
                    )?;
                    let moved = RevsetExpr::new(base.to_string()).map_err(|error| {
                        VcsError::Runtime(format!("invalid moved release bookmark: {error}"))
                    })?;
                    self.block_on_processkit(jj.new_child(&self.root, &moved))?;
                }
                _ => {
                    return Err(VcsError::Runtime(
                        "release sync does not support this VCS backend".into(),
                    ));
                }
            }
        }
        if cancelled() {
            return Err(VcsError::Runtime(
                "release sync lost owner authority after local fast-forward".into(),
            ));
        }
        self.require_primary_reanchored_to(base, &remote)?;
        Ok(ReleaseSyncHead {
            previous,
            current: remote,
        })
    }

    /// Resolve an explicit tag, or the unambiguous `<version>` / `v<version>` convention, and
    /// prove that the tagged commit is visible in synchronized trunk history.
    pub fn verify_release_tag(
        &self,
        version: &str,
        explicit_tag: Option<&str>,
        synced_head: &str,
        base: &str,
    ) -> Result<ReleaseTagEvidence> {
        validate_ref("release version", version)?;
        validate_ref("synchronized release head", synced_head)?;
        validate_ref("release trunk", base)?;
        if let Some(tag) = explicit_tag {
            validate_ref("release tag", tag)?;
        }
        let git = Git::hardened();
        let git_directory = self.git_backend_directory()?;
        let remote = self.block_on_processkit(git.remote_url(&git_directory, "origin"))?;
        let clone = RemoteProofClone::create()?;
        self.block_on_processkit(git.clone_repo(
            &remote,
            &clone.repository,
            CloneSpec::new().branch(base).bare(),
        ))?;
        let synced_revision = RevSpec::new(synced_head.to_string()).map_err(|error| {
            VcsError::Runtime(format!("invalid synchronized release revision: {error}"))
        })?;
        self.block_on_processkit(git.resolve_commit(&clone.repository, &synced_revision))?;
        let tags = self.block_on_processkit(git.tag_list(&clone.repository))?;
        let candidates = match explicit_tag {
            Some(tag) => vec![tag.to_string()],
            None => {
                let mut candidates = Vec::new();
                for candidate in [version.to_string(), format!("v{version}")] {
                    if tags.iter().any(|tag| tag == &candidate) {
                        candidates.push(candidate);
                    }
                }
                candidates
            }
        };
        if candidates.is_empty() {
            return Err(VcsError::Runtime(format!(
                "release tag is not visible after sync (looked for {version:?} and {:?})",
                format!("v{version}")
            )));
        }
        let mut resolved = Vec::new();
        for tag in candidates {
            if !tags.iter().any(|known| known == &tag) {
                return Err(VcsError::Runtime(format!(
                    "explicit release tag {tag:?} is not visible after sync"
                )));
            }
            let revision = RevSpec::new(format!("refs/tags/{tag}")).map_err(|error| {
                VcsError::Runtime(format!("invalid release tag revision: {error}"))
            })?;
            let revision =
                self.block_on_processkit(git.resolve_commit(&clone.repository, &revision))?;
            resolved.push((tag, revision));
        }
        let first_revision = &resolved[0].1;
        if resolved
            .iter()
            .any(|(_, revision)| revision != first_revision)
        {
            return Err(VcsError::Runtime(format!(
                "release version {version:?} is ambiguous: conventional tags resolve to different commits; pass an explicit tag"
            )));
        }
        let (tag, revision) = resolved.remove(0);
        let range = RevSpec::new(format!("{synced_head}..{revision}")).map_err(|error| {
            VcsError::Runtime(format!(
                "invalid remote release-tag ancestry range: {error}"
            ))
        })?;
        if !self
            .block_on_processkit(git.log(&clone.repository, &range, 1))?
            .is_empty()
        {
            return Err(VcsError::Runtime(format!(
                "release tag {tag:?} resolves to {revision}, which is not reachable from synchronized trunk {synced_head}"
            )));
        }
        Ok(ReleaseTagEvidence { tag, revision })
    }

    /// Keep a pre-sync head as a release-notes diff base only when it is actually an ancestor of
    /// the proved release revision. An already-newer/divergent local observation collapses to an
    /// empty tag-bound range rather than exposing unrelated post-release commits to the notes
    /// leaf.
    pub fn release_notes_range_base(
        &self,
        pre_sync_head: &str,
        release_revision: &str,
    ) -> Result<String> {
        validate_ref("pre-sync release head", pre_sync_head)?;
        validate_ref("release revision", release_revision)?;
        if self.revision_is_ancestor_of(pre_sync_head, release_revision)? {
            Ok(pre_sync_head.to_string())
        } else {
            Ok(release_revision.to_string())
        }
    }

    /// Materialize the exact release range through the published typed VCS adapters. Both
    /// coordinates are already remote-proved revisions; the returned sections are sorted and
    /// content-hashed so the confined semantic leaf needs no direct VCS access.
    pub fn release_notes_range_evidence(
        &self,
        base: &str,
        head: &str,
    ) -> Result<ReleaseNotesRangeEvidence> {
        validate_ref("release notes base", base)?;
        validate_ref("release notes head", head)?;
        let repo = self.repo()?;
        let range = DiffSpec::Rev(format!("{base}..{head}"));
        let files = match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                self.block_on_processkit(git.diff(&self.root, range))?
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                self.block_on_processkit(jj.diff(&self.root, range))?
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot produce release-notes evidence for an unsupported VCS backend".into(),
                ));
            }
        };
        let mut files = files
            .into_iter()
            .map(|file| {
                let raw = file.raw;
                Ok(TaskReviewRangeFile {
                    path: manifest_path(&file.path)?,
                    old_path: file.old_path.as_deref().map(manifest_path).transpose()?,
                    diff_sha256: format!("{:x}", Sha256::digest(raw.as_bytes())),
                    raw,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        files.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.old_path.cmp(&right.old_path))
        });
        Ok(ReleaseNotesRangeEvidence {
            schema: "orchestrail/release-notes-range@1".into(),
            base: base.into(),
            head: head.into(),
            files,
        })
    }

    /// Re-prove the exact clean primary after release-only leaves have written their confined
    /// `.work` artifacts. A model result cannot authorize delivery if it dirtied product files,
    /// moved the trunk, or changed the JJ parent underneath the synchronized bookmark.
    pub fn verify_release_primary(&self, base: &str, expected_head: &str) -> Result<()> {
        validate_ref("release trunk", base)?;
        validate_ref("expected synchronized release head", expected_head)?;
        let actual = self.require_clean_local_primary(base)?;
        if actual != expected_head {
            return Err(VcsError::Runtime(format!(
                "release primary changed after synchronization: expected {base}={expected_head}, observed {actual}"
            )));
        }
        Ok(())
    }

    fn git_backend_directory(&self) -> Result<PathBuf> {
        let git_metadata = self.root.join(".git");
        match fs::symlink_metadata(&git_metadata) {
            Ok(metadata)
                if (metadata.is_dir() || metadata.is_file()) && !work_fs::redirected(&metadata) =>
            {
                Ok(self.root.clone())
            }
            Ok(_) => Err(VcsError::ManagedPath(format!(
                "refusing redirected or non-regular Git metadata path {}",
                git_metadata.display()
            ))),
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && self.backend == BackendKind::Jj =>
            {
                let store = self.root.join(".jj/repo/store/git");
                let metadata = fs::symlink_metadata(&store).map_err(|error| {
                    VcsError::Runtime(format!(
                        "cannot inspect pure-JJ Git store at {}: {error}",
                        store.display()
                    ))
                })?;
                if !metadata.is_dir() || work_fs::redirected(&metadata) {
                    return Err(VcsError::ManagedPath(format!(
                        "refusing redirected or non-directory pure-JJ Git store {}",
                        store.display()
                    )));
                }
                Ok(store)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(VcsError::Runtime(format!(
                    "Git repository root has no metadata path at {}",
                    git_metadata.display()
                )))
            }
            Err(error) => Err(VcsError::Io(error)),
        }
    }

    /// Ensure the native control plane can never be captured by either backend's working-copy
    /// snapshot.  This is the first mutating Phase-0 VCS preflight, after the owner lease and
    /// PAUSE gate but before recovery reads or repairs legacy state.
    ///
    /// A control plane outside the repository cannot enter a VCS snapshot and is a no-op. A
    /// colocated repository uses Git's private `info/exclude`, which both Git and JJ honor. A
    /// pure JJ repository uses its private Git store's `info/exclude`. Neither path dirties the
    /// product working copy, and the rule is idempotent.
    pub fn ensure_control_plane_ignored(&self, work: impl AsRef<Path>) -> Result<bool> {
        let actual_work = canonical_path(work)?;
        let Ok(relative_work) = actual_work.strip_prefix(&self.root) else {
            return Ok(false);
        };
        if relative_work.as_os_str().is_empty() {
            return Err(VcsError::ManagedPath(format!(
                "processor control plane cannot be the repository root {}",
                self.root.display()
            )));
        }
        if relative_work
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(VcsError::ManagedPath(format!(
                "processor control plane has a non-normal repository-relative path {}",
                relative_work.display()
            )));
        }
        let relative_work = relative_work.to_str().ok_or_else(|| {
            VcsError::ManagedPath("processor control-plane path is not valid UTF-8".into())
        })?;
        let ignore_rule = if relative_work == ".work" {
            ".work/".to_string()
        } else {
            format!("/{}/", relative_work.replace('\\', "/"))
        };

        let git_metadata = self.root.join(".git");
        let ignore_path = match fs::symlink_metadata(&git_metadata) {
            Ok(metadata) if metadata.is_dir() && !work_fs::redirected(&metadata) => {
                let info = git_metadata.join("info");
                match fs::symlink_metadata(&info) {
                    Ok(metadata) if metadata.is_dir() && !work_fs::redirected(&metadata) => {}
                    Ok(_) => {
                        return Err(VcsError::ManagedPath(format!(
                            "refusing redirected or non-directory Git info path {}",
                            info.display()
                        )));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        fs::create_dir(&info)?;
                    }
                    Err(error) => return Err(VcsError::Io(error)),
                }
                info.join("exclude")
            }
            Ok(_) => {
                return Err(VcsError::ManagedPath(format!(
                    "phase-0 ignore preflight requires a plain primary .git directory, got {}",
                    git_metadata.display()
                )));
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && self.backend == BackendKind::Jj =>
            {
                let mut directory = self.root.clone();
                for component in [".jj", "repo", "store", "git", "info"] {
                    directory.push(component);
                    match fs::symlink_metadata(&directory) {
                        Ok(metadata) if metadata.is_dir() && !work_fs::redirected(&metadata) => {}
                        Ok(_) => {
                            return Err(VcsError::ManagedPath(format!(
                                "refusing redirected or non-directory pure-JJ metadata path {}",
                                directory.display()
                            )));
                        }
                        Err(error) => return Err(VcsError::Io(error)),
                    }
                }
                directory.join("exclude")
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(VcsError::ManagedPath(format!(
                    "Git repository root has no private metadata directory at {}",
                    git_metadata.display()
                )));
            }
            Err(error) => return Err(VcsError::Io(error)),
        };

        let ignore_parent = ignore_path.parent().expect("ignore path has info parent");
        match fs::symlink_metadata(&ignore_path) {
            Ok(metadata) if metadata.is_file() && !work_fs::redirected(&metadata) => {}
            Ok(_) => {
                return Err(VcsError::ManagedPath(format!(
                    "refusing redirected or non-file ignore path {}",
                    ignore_path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(VcsError::Io(error)),
        }
        let mut content =
            work_fs::read_optional_bytes(ignore_parent, &ignore_path, MAX_IGNORE_BYTES)?
                .unwrap_or_default();
        if content
            .split(|byte| *byte == b'\n')
            .any(|line| line.strip_suffix(b"\r").unwrap_or(line) == ignore_rule.as_bytes())
        {
            return Ok(false);
        }
        if !content.is_empty() && !content.ends_with(b"\n") {
            content.push(b'\n');
        }
        content.extend_from_slice(ignore_rule.as_bytes());
        content.push(b'\n');
        work_fs::replace_file(ignore_parent, &ignore_path, &content, MAX_IGNORE_BYTES)?;
        Ok(true)
    }

    /// Get a read-only repository snapshot.  In particular this does not cause JJ to snapshot a
    /// working copy merely to refresh operator state.
    pub fn snapshot(&self) -> Result<RepositorySnapshot> {
        let repo = self.repo()?;
        let snapshot = self.block_on(repo.snapshot_readonly())?;
        Ok(RepositorySnapshot {
            backend: self.backend,
            root: self.root.clone(),
            head: snapshot.head,
            branch: snapshot.branch,
            dirty: snapshot.dirty,
            conflicted: snapshot.conflicted,
        })
    }

    /// Prove that the primary checkout is clean, on `base`, and names the exact already-published
    /// revision before a CI-repair leaf is allowed to edit it. This intentionally observes the
    /// live working copy: an unrecorded operator edit is a hard stop, not something a Mode-3
    /// repair may inherit or accidentally commit.
    pub fn published_primary_workspace(&self, base: &str, published_head: &str) -> Result<PathBuf> {
        validate_ref("publication branch", base)?;
        validate_ref("published head", published_head)?;
        let repo = self.repo()?;
        let snapshot = self.block_on(repo.snapshot())?;
        let on_primary_branch = match self.backend {
            BackendKind::Git => snapshot.branch.as_deref() == Some(base),
            // A clean JJ child of a commit carrying both `main` and the retained integration
            // bookmark may be reported under either nearest bookmark. Parent/target proof below
            // is the unambiguous publication coordinate.
            BackendKind::Jj => true,
            _ => false,
        };
        let exact_published = match self.backend {
            BackendKind::Git => snapshot.head.as_deref() == Some(published_head),
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                let parent_revision =
                    RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                        VcsError::Runtime(format!("invalid JJ primary parent revision: {error}"))
                    })?;
                let parent = self
                    .block_on_processkit(jj.template_query(
                        &self.root,
                        &parent_revision,
                        "commit_id",
                        Some(1),
                    ))?
                    .trim()
                    .to_string();
                jj_bookmark_target(&bookmarks, base)? == published_head && parent == published_head
            }
            _ => false,
        };
        if !on_primary_branch || !exact_published || snapshot.dirty || snapshot.conflicted {
            return Err(VcsError::Runtime(format!(
                "primary checkout is not a clean exact published {base}={published_head} (branch={:?}, head={:?}, dirty={}, conflicted={})",
                snapshot.branch, snapshot.head, snapshot.dirty, snapshot.conflicted
            )));
        }
        Ok(self.root.clone())
    }

    /// Create (or verify and reuse) the task's only permitted worktree/workspace:
    /// `<work>/worktrees/T-NNN` on `task/T-NNN`, forked from the recorded `base`.
    ///
    /// A pre-existing directory which the VCS does not recognise is refused rather than removed:
    /// recovery must never guess that an arbitrary path is disposable.
    pub fn ensure_task_workspace(
        &self,
        work: impl AsRef<Path>,
        task_id: &str,
        base: &str,
    ) -> Result<TaskWorkspace> {
        validate_task_id(task_id)?;
        validate_ref("base", base)?;
        let branch = format!("task/{task_id}");
        let path = managed_task_path(&self.root, work.as_ref(), task_id)?;
        let managed_work = managed_work_root(&path)?;
        let repo = self.repo()?;

        let existing = self.block_on(repo.list_worktrees())?;
        if let Some(found) = existing.iter().find(|entry| same_path(&entry.path, &path)) {
            if !managed_directory_present(&path)? {
                self.remove_missing_workspace_registration(
                    &repo,
                    &path,
                    &branch,
                    found.branch.as_deref(),
                )?;
            } else {
                let belongs_to_branch =
                    self.workspace_registration_matches(found.branch.as_deref(), &path, &branch)?;
                if !belongs_to_branch {
                    return Err(VcsError::ManagedPath(format!(
                        "managed worktree {} belongs to {:?}, expected {branch}",
                        path.display(),
                        found.branch
                    )));
                }
                return Ok(TaskWorkspace {
                    task_id: task_id.to_string(),
                    branch,
                    work: managed_work,
                    path,
                    backend: self.backend,
                });
            }
        }

        if managed_directory_present(&path)? {
            return Err(VcsError::ManagedPath(format!(
                "refusing to reuse unregistered managed worktree path {}",
                path.display()
            )));
        }

        self.create_or_restore_workspace(&repo, &path, &branch, base)?;

        let verified = self.block_on(repo.list_worktrees())?;
        let Some(found) = verified.iter().find(|entry| same_path(&entry.path, &path)) else {
            return Err(VcsError::ManagedPath(format!(
                "VCS created no registered worktree at {}",
                path.display()
            )));
        };
        let belongs_to_branch =
            self.workspace_registration_matches(found.branch.as_deref(), &path, &branch)?;
        if !belongs_to_branch {
            return Err(VcsError::ManagedPath(format!(
                "VCS registered {} on {:?}, expected {branch}",
                path.display(),
                found.branch
            )));
        }

        Ok(TaskWorkspace {
            task_id: task_id.to_string(),
            branch,
            work: managed_work,
            path,
            backend: self.backend,
        })
    }

    /// Create (or verify and reuse) the only integration workspace at
    /// `<work>/worktrees/_integration`, on `integration/<batch_id>` from `base`. A prior task
    /// workspace can never be mistaken for it because both the VCS registration and exact branch
    /// are checked before reuse.
    pub fn ensure_integration_workspace(
        &self,
        work: impl AsRef<Path>,
        batch_id: &str,
        base: &str,
    ) -> Result<IntegrationWorkspace> {
        validate_batch_id(batch_id)?;
        validate_ref("base", base)?;
        let branch = format!("integration/{batch_id}");
        let path = managed_integration_path(&self.root, work.as_ref())?;
        let managed_work = managed_work_root(&path)?;
        let repo = self.repo()?;
        let existing = self.block_on(repo.list_worktrees())?;
        if let Some(found) = existing.iter().find(|entry| same_path(&entry.path, &path)) {
            if !managed_directory_present(&path)? {
                self.remove_missing_workspace_registration(
                    &repo,
                    &path,
                    &branch,
                    found.branch.as_deref(),
                )?;
            } else {
                let belongs_to_branch =
                    self.workspace_registration_matches(found.branch.as_deref(), &path, &branch)?;
                if !belongs_to_branch {
                    return Err(VcsError::ManagedPath(format!(
                        "integration workspace {} belongs to {:?}, expected {branch}",
                        path.display(),
                        found.branch
                    )));
                }
                return Ok(IntegrationWorkspace {
                    batch_id: batch_id.into(),
                    branch,
                    work: managed_work,
                    path,
                    backend: self.backend,
                });
            }
        }
        if managed_directory_present(&path)? {
            return Err(VcsError::ManagedPath(format!(
                "refusing to reuse unregistered integration workspace path {}",
                path.display()
            )));
        }
        self.create_or_restore_workspace(&repo, &path, &branch, base)?;
        let verified = self.block_on(repo.list_worktrees())?;
        let Some(found) = verified.iter().find(|entry| same_path(&entry.path, &path)) else {
            return Err(VcsError::ManagedPath(format!(
                "VCS created no registered integration workspace at {}",
                path.display()
            )));
        };
        let belongs_to_branch =
            self.workspace_registration_matches(found.branch.as_deref(), &path, &branch)?;
        if !belongs_to_branch {
            return Err(VcsError::ManagedPath(format!(
                "VCS registered integration workspace {} on {:?}, expected {branch}",
                path.display(),
                found.branch
            )));
        }
        Ok(IntegrationWorkspace {
            batch_id: batch_id.into(),
            branch,
            work: managed_work,
            path,
            backend: self.backend,
        })
    }

    /// Materialize a managed worktree without moving an already durable task/integration ref.
    ///
    /// Phase-0 recovery frequently has the branch/bookmark but not its checkout: Git worktree
    /// registration can be pruned after a directory loss, and a JJ workspace can have been
    /// removed while its bookmark survives. `vcs-core::WorktreeCreate` deliberately creates a
    /// *new* branch/bookmark, so using it unconditionally would either fail on Git or, worse,
    /// reset the intended recovery coordinate if a backend changed that behavior. Reattach the
    /// known existing ref through the published backend-specific typed API and prove its tip did
    /// not move; only an absent ref may use the common creation facade.
    fn remove_missing_workspace_registration(
        &self,
        repo: &Repo,
        path: &Path,
        branch: &str,
        observed_branch: Option<&str>,
    ) -> Result<()> {
        // A missing directory is not enough authority to remove a VCS registration: first tie
        // the exact registered path to the expected durable task/integration ref. JJ can report
        // an empty child without a nearest bookmark, which is deliberately held rather than
        // forgetting a workspace whose identity cannot be proved from the remaining metadata.
        self.require_missing_workspace_registration(repo, path, branch, observed_branch)?;
        self.block_on(repo.remove_worktree(WorktreeRemove::new(path).force()))?;
        if self
            .block_on(repo.list_worktrees())?
            .iter()
            .any(|entry| same_path(&entry.path, path))
        {
            return Err(VcsError::ManagedPath(format!(
                "VCS retained missing managed workspace registration at {}",
                path.display()
            )));
        }
        Ok(())
    }

    fn require_missing_workspace_registration(
        &self,
        repo: &Repo,
        path: &Path,
        branch: &str,
        observed_branch: Option<&str>,
    ) -> Result<()> {
        if observed_branch == Some(branch) {
            return Ok(());
        }
        if self.backend == BackendKind::Jj
            && observed_branch.is_none()
            && self.jj_missing_workspace_is_direct_child_of(repo, path, branch)?
        {
            return Ok(());
        }
        Err(VcsError::ManagedPath(format!(
            "missing managed workspace {} is registered to {:?}, not {branch}",
            path.display(),
            observed_branch
        )))
    }

    /// Prove a deleted JJ workspace still belongs to the expected bookmark without opening its
    /// missing path. A normal committed workspace is a clean direct child of the durable
    /// bookmark, so `workspace list` plus a root-level parent query preserves the same ownership
    /// proof that [`Self::jj_workspace_matches_bookmark`] obtains from a live checkout.
    fn jj_missing_workspace_is_direct_child_of(
        &self,
        repo: &Repo,
        path: &Path,
        branch: &str,
    ) -> Result<bool> {
        let jj = repo
            .jj()
            .ok_or_else(|| VcsError::Runtime("JJ repository has no typed JJ client".into()))?;
        let workspaces = self.block_on_processkit(jj.workspace_list(&self.root))?;
        let names: Vec<String> = workspaces
            .iter()
            .map(|workspace| workspace.name.clone())
            .collect();
        let roots = self.block_on_value(jj.workspace_roots(&self.root, &names))?;
        let mut matching = workspaces
            .into_iter()
            .zip(roots)
            .filter_map(|(workspace, root)| {
                root.ok()
                    .filter(|root| same_path(root, path))
                    .map(|_| workspace)
            });
        let Some(workspace) = matching.next() else {
            return Ok(false);
        };
        if matching.next().is_some() || workspace.commit.trim().is_empty() {
            return Ok(false);
        }
        let target = jj_bookmark_target(
            &self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?,
            branch,
        )?;
        if workspace.commit == target {
            return Ok(true);
        }
        let parent =
            RevsetExpr::new(format!("parents({})", workspace.commit)).map_err(|error| {
                VcsError::Runtime(format!(
                    "invalid missing-JJ-workspace parent query for {}: {error}",
                    path.display()
                ))
            })?;
        let actual_parent = self
            .block_on_processkit(jj.template_query_ignoring_working_copy(
                &self.root,
                &parent,
                "commit_id",
                Some(1),
            ))?
            .trim()
            .to_string();
        Ok(actual_parent == target)
    }

    fn create_or_restore_workspace(
        &self,
        repo: &Repo,
        path: &Path,
        branch: &str,
        base: &str,
    ) -> Result<()> {
        let branch_existed = self.block_on(repo.branch_exists(branch))?;
        let expected_tip = if branch_existed {
            Some(self.branch_tip(repo, branch)?)
        } else {
            None
        };

        match self.backend {
            BackendKind::Git if branch_existed => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let branch_ref = RevSpec::new(branch.to_owned()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid existing Git workspace branch {branch:?}: {error}"
                    ))
                })?;
                self.block_on_processkit(
                    git.worktree_add(&self.root, WorktreeAdd::checkout(path, branch_ref)),
                )?;
            }
            BackendKind::Jj if branch_existed => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let revision = RevsetExpr::new(branch.to_owned()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid existing JJ workspace bookmark {branch:?}: {error}"
                    ))
                })?;
                self.block_on_processkit(jj.workspace_add(
                    &self.root,
                    WorkspaceAdd::new(workspace_name_for_branch(branch), revision, path),
                ))?;
            }
            BackendKind::Git | BackendKind::Jj => {
                self.block_on(repo.create_worktree(WorktreeCreate::new(path, branch).base(base)))?;
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot create a workspace for an unsupported VCS backend".into(),
                ));
            }
        }

        if let Some(expected_tip) = expected_tip {
            let actual_tip = self.branch_tip(repo, branch)?;
            if actual_tip != expected_tip {
                return Err(VcsError::Runtime(format!(
                    "refusing recovered workspace: {branch} moved from {expected_tip:?} to {actual_tip:?}"
                )));
            }
        }
        Ok(())
    }

    /// Observe one task's exact recovery coordinates without mutating the repository.  In
    /// particular, `commits_after_base` is based on the task branch tip, not the existence of a
    /// branch: Git creates that branch before the first implementation commit.
    pub fn task_recovery_observation(
        &self,
        work: impl AsRef<Path>,
        task_id: &str,
        base: &str,
    ) -> Result<TaskRepositoryObservation> {
        validate_task_id(task_id)?;
        validate_ref("base", base)?;
        let branch = format!("task/{task_id}");
        let path = observed_managed_workspace_path(&self.root, work.as_ref(), task_id)?;
        let repo = self.repo()?;
        let branch_exists = self.block_on(repo.branch_exists(&branch))?;
        let worktrees = self.block_on(repo.list_worktrees())?;
        let workspace_present = match worktrees.iter().find(|entry| same_path(&entry.path, &path)) {
            Some(entry) => {
                if managed_directory_present(&path)? {
                    self.workspace_registration_matches(entry.branch.as_deref(), &path, &branch)?
                } else {
                    self.require_missing_workspace_registration(
                        &repo,
                        &path,
                        &branch,
                        entry.branch.as_deref(),
                    )?;
                    false
                }
            }
            None => false,
        };
        if managed_directory_present(&path)? && !workspace_present {
            return Err(VcsError::ManagedPath(format!(
                "recovery path {} exists but is not registered to {branch}",
                path.display()
            )));
        }
        // `Repo::log` is presentation-oriented and JJ may render an abbreviated change id.
        // Recovery persists this coordinate and later compares it with exact bookmark targets,
        // so resolve the durable branch/bookmark through the typed ref-specific helper instead.
        let branch_head = branch_exists
            .then(|| self.branch_tip(&repo, &branch))
            .transpose()?;
        let commits_after_base = if branch_exists {
            self.has_nonempty_commit_after_base(&repo, base, &branch)?
        } else {
            false
        };
        let workspace_clean = if workspace_present {
            let snapshot = self.block_on(self.repo()?.at(&path).snapshot())?;
            Some(!snapshot.dirty && !snapshot.conflicted)
        } else {
            None
        };
        Ok(TaskRepositoryObservation {
            branch_exists,
            workspace_present,
            workspace_clean,
            branch_head,
            commits_after_base,
            integrated_into_active: None,
        })
    }

    /// Observe the singleton integration branch/workspace.  Publication itself is supplied by a
    /// forge-aware adapter because for push-enabled runs it is proven against the remote default
    /// branch, not against the local checkout.
    pub fn integration_recovery_observation(
        &self,
        work: impl AsRef<Path>,
        batch_id: &str,
        base: &str,
        publication: PublicationObservation,
    ) -> Result<IntegrationRepositoryObservation> {
        validate_batch_id(batch_id)?;
        validate_ref("base", base)?;
        let branch = format!("integration/{batch_id}");
        let path = observed_managed_workspace_path(&self.root, work.as_ref(), "_integration")?;
        let repo = self.repo()?;
        let branch_exists = self.block_on(repo.branch_exists(&branch))?;
        let worktrees = self.block_on(repo.list_worktrees())?;
        let workspace_present = match worktrees.iter().find(|entry| same_path(&entry.path, &path)) {
            Some(entry) => {
                if managed_directory_present(&path)? {
                    self.workspace_registration_matches(entry.branch.as_deref(), &path, &branch)?
                } else {
                    self.require_missing_workspace_registration(
                        &repo,
                        &path,
                        &branch,
                        entry.branch.as_deref(),
                    )?;
                    false
                }
            }
            None => false,
        };
        if managed_directory_present(&path)? && !workspace_present {
            return Err(VcsError::ManagedPath(format!(
                "recovery path {} exists but is not registered to {branch}",
                path.display()
            )));
        }
        // See task recovery above: this is a durable checkpoint coordinate, never a display
        // value, and must therefore be the exact Git ref/JJ bookmark target.
        let branch_head = branch_exists
            .then(|| self.branch_tip(&repo, &branch))
            .transpose()?;
        let commits_after_base = if branch_exists {
            self.has_nonempty_commit_after_base(&repo, base, &branch)?
        } else {
            false
        };
        let workspace_clean = if workspace_present {
            let snapshot = self.block_on(self.repo()?.at(&path).snapshot())?;
            Some(!snapshot.dirty && !snapshot.conflicted)
        } else {
            None
        };
        let merge_report_lines =
            read_plain_work_artifact(work.as_ref(), &work.as_ref().join("merge_report.md"))?
                .map(|report| crate::contract::parse_merge_report(&report));
        Ok(IntegrationRepositoryObservation {
            branch_exists,
            workspace_present,
            branch_head,
            commits_after_base,
            workspace_clean,
            merge_report_present: merge_report_lines.is_some(),
            merge_report_lines,
            publication,
        })
    }

    /// Prove that a branch/bookmark contains a material change after `base`.  Git has no
    /// mutable empty successor, so its ordinary revision range is exact.  JJ deliberately keeps
    /// a new empty working-copy change after a bookmark; its revset therefore excludes `empty()`
    /// before applying the one-result bound.  Counting that successor as implementation or a
    /// merge would turn a clean recovery boundary into a false "partially completed" state.
    fn has_nonempty_commit_after_base(
        &self,
        repo: &Repo,
        base: &str,
        branch: &str,
    ) -> Result<bool> {
        match self.backend {
            BackendKind::Git => Ok(!self
                .block_on(repo.log(&format!("{base}..{branch}"), 1))?
                .is_empty()),
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let revset = RevsetExpr::new(format!("({branch} ~ {base}) ~ empty()"))
                    .map_err(|error| {
                        VcsError::Runtime(format!(
                            "cannot construct JJ non-empty range for {branch:?} from {base:?}: {error}"
                        ))
                    })?;
                Ok(!self
                    .block_on_processkit(jj.log(&self.root, &revset, 1))?
                    .is_empty())
            }
            _ => Err(VcsError::Runtime(
                "cannot prove recovery history for an unsupported VCS backend".into(),
            )),
        }
    }

    /// Collect exactly the VCS evidence consumed by [`crate::recovery::plan_recovery`] from a
    /// stable control-plane snapshot.  Descriptors that are not part of the active batch are left
    /// to the reconciliation planner — probing their claimed paths would trust precisely the
    /// orphaned data Phase 0 is meant to clean up.
    pub fn recovery_inventory(
        &self,
        snapshot: &Snapshot,
        publication: PublicationObservation,
    ) -> Result<RecoveryInventory> {
        let Some(batch) = snapshot.batch.as_ref() else {
            return Ok(RecoveryInventory::default());
        };
        let Some(base) = batch.base.as_deref() else {
            return Err(VcsError::InvalidInput(
                "active batch manifest has no immutable base".into(),
            ));
        };
        let batch_id = batch.batch_id.as_deref().ok_or_else(|| {
            VcsError::InvalidInput("active batch manifest has no batch id".into())
        })?;
        let integration =
            self.integration_recovery_observation(&snapshot.work_dir, batch_id, base, publication)?;
        let mut inventory = RecoveryInventory::default();
        // Once integration exists, Phase 4 recovery must prove a merger report against the
        // actual task-branch ancestry. Observing every declared batch task also keeps that proof
        // independent from whether a legacy crash had already rewritten its Markdown status.
        // Before integration, the same observations are harmless and retain the existing active
        // task recovery evidence.
        for task in &batch.tasks {
            let mut observation =
                self.task_recovery_observation(&snapshot.work_dir, &task.id, base)?;
            if integration.branch_exists && observation.branch_exists {
                observation.integrated_into_active =
                    Some(self.task_is_merged_into_integration(&task.id, batch_id)?);
            }
            inventory.tasks.insert(task.id.clone(), observation);
        }
        inventory.integration = Some(integration);
        Ok(inventory)
    }

    /// Determine the irreversible publication boundary for a no-push run. In that mode the local
    /// publication branch is the authority: treating it as merely "not published" after a
    /// fast-forward would replay Phase 5 over an already released tree.
    pub fn local_integration_publication_observation(
        &self,
        batch_id: &str,
        base: &str,
    ) -> Result<PublicationObservation> {
        validate_batch_id(batch_id)?;
        validate_ref("publication branch", base)?;
        let integration_branch = format!("integration/{batch_id}");
        let repo = self.repo()?;
        if !self.block_on(repo.branch_exists(&integration_branch))? {
            return Ok(PublicationObservation::NotPublished);
        }
        Ok(if self.branch_is_ancestor_of(&integration_branch, base)? {
            PublicationObservation::Published
        } else {
            PublicationObservation::NotPublished
        })
    }

    /// Determine the irreversible publication boundary for a push-enabled run. The engine
    /// fetches the exact configured publication branch from the same `origin` remote used by
    /// [`Repo::push`] and proves the retained integration branch is contained in that freshly
    /// fetched remote-tracking ref. A local `main` is deliberately not evidence here: it may
    /// have advanced before a failed push. Git has a typed fetch that changes only its remote
    /// tracking ref.
    ///
    /// JJ's typed fetch can reconcile a tracked local bookmark, so it cannot be used merely to
    /// observe the candidate under proof. Instead the JJ path uses the colocated Git remote URL
    /// to make a typed, temporary bare clone of only the publication branch. The source checkout,
    /// its Git refs, and JJ operation log remain untouched; the clone proves whether the durable
    /// integration commit is an ancestor of the freshly obtained remote branch.
    ///
    /// On Git, missing `origin`, an unavailable remote, or a missing remote publication branch
    /// are errors rather than an optimistic `NotPublished` answer. Recovery must not infer
    /// remote state from stale local tracking data or silently fall back to the primary checkout.
    pub fn remote_integration_publication_observation(
        &self,
        batch_id: &str,
        base: &str,
    ) -> Result<PublicationObservation> {
        validate_batch_id(batch_id)?;
        validate_ref("publication branch", base)?;
        let integration_branch = format!("integration/{batch_id}");
        let repo = self.repo()?;
        if !self.block_on(repo.branch_exists(&integration_branch))? {
            return Ok(PublicationObservation::NotPublished);
        }

        match self.backend {
            // `vcs-core` owns the network invocation and writes only the dedicated
            // remote-tracking ref for this branch. It retries transient failures and refuses
            // interactive prompts.
            BackendKind::Git => {
                self.block_on(repo.fetch_branch(base))?;
                Ok(
                    if self.branch_is_ancestor_of(&integration_branch, &format!("origin/{base}"))? {
                        PublicationObservation::Published
                    } else {
                        PublicationObservation::NotPublished
                    },
                )
            }
            BackendKind::Jj => {
                self.jj_remote_integration_publication_observation(&repo, &integration_branch, base)
            }
            _ => Err(VcsError::Runtime(
                "cannot prove remote publication for an unsupported VCS backend".into(),
            )),
        }
    }

    /// Prove a JJ publication through an isolated Git clone rather than `jj git fetch`: a fetch
    /// may reconcile the tracked local bookmark, which is the state this recovery decision must
    /// only observe. The targeted clone is a complete (non-shallow) view of remote `base`, so
    /// `base..integration` is empty exactly when the remote publication branch contains the
    /// retained integration target.
    fn jj_remote_integration_publication_observation(
        &self,
        repo: &Repo,
        integration_branch: &str,
        base: &str,
    ) -> Result<PublicationObservation> {
        let jj = repo
            .jj()
            .ok_or_else(|| VcsError::Runtime("JJ repository has no typed JJ client".into()))?;
        let bookmarks = self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
        let integration_target = jj_bookmark_target(&bookmarks, integration_branch)?;

        // `vcs-core` deliberately exposes only its selected JJ facade in a colocated checkout.
        // The published `vcs-git` client is nevertheless the typed authority for the underlying
        // Git remote URL and isolated clone; no engine-owned argv or shell syntax is involved.
        let git = Git::hardened();
        let git_directory = self.git_backend_directory()?;
        let remote = self.block_on_processkit(git.remote_url(&git_directory, "origin"))?;
        let clone = RemoteProofClone::create()?;
        self.block_on_processkit(git.clone_repo(
            &remote,
            &clone.repository,
            CloneSpec::new().branch(base).bare(),
        ))?;

        // First prove the requested remote publication ref actually exists in the clone. This
        // preserves the Git path's fail-closed distinction between a missing/unreachable remote
        // publication branch and a confirmed remote branch that simply lacks the integration.
        let publication = RevSpec::new(base.to_owned()).map_err(|error| {
            VcsError::Runtime(format!("invalid remote publication revision: {error}"))
        })?;
        self.block_on_processkit(git.resolve_commit(&clone.repository, &publication))?;

        let integration = RevSpec::new(integration_target.clone()).map_err(|error| {
            VcsError::Runtime(format!("invalid durable JJ integration target: {error}"))
        })?;
        // Distinguish the normal "remote has not received this integration commit" state from
        // a failed ancestry query. Only the exact-object lookup below may become NotPublished;
        // once both endpoints are known to this complete clone, every later ProcessKit failure
        // is an indeterminate transport/tool failure and must hold recovery.
        match self.block_on_processkit(git.resolve_commit(&clone.repository, &integration)) {
            Ok(_) => {}
            Err(VcsError::ProcessKit(processkit::Error::Exit { .. })) => {
                return Ok(PublicationObservation::NotPublished);
            }
            Err(error) => return Err(error),
        }
        let range = RevSpec::new(format!("{base}..{}", integration.as_str())).map_err(|error| {
            VcsError::Runtime(format!(
                "invalid isolated remote publication range: {error}"
            ))
        })?;
        match self.block_on_processkit(git.log(&clone.repository, &range, 1))? {
            commits if commits.is_empty() => Ok(PublicationObservation::Published),
            _ => Ok(PublicationObservation::NotPublished),
        }
    }

    /// Prove task ancestry in the active integration ref without relying on merger prose. Git
    /// asks its typed `branch --merged` adapter; JJ evaluates the equivalent ancestor revset.
    /// Prove whether the exact durable task branch/bookmark is already an ancestor of the
    /// managed integration branch.  Phase-0 uses this before replaying an interrupted legacy
    /// merger; callers must still verify the task tip against its reviewed coordinate.
    pub fn task_is_merged_into_integration(&self, task_id: &str, batch_id: &str) -> Result<bool> {
        validate_task_id(task_id)?;
        validate_batch_id(batch_id)?;
        let task_branch = format!("task/{task_id}");
        let integration_branch = format!("integration/{batch_id}");
        self.branch_is_ancestor_of(&task_branch, &integration_branch)
    }

    /// Prove the first branch/bookmark is contained in the history named by the second one. The
    /// operation is read-only but backend-specific because Git and JJ expose the proof through
    /// different typed adapters.
    fn branch_is_ancestor_of(&self, branch: &str, target: &str) -> Result<bool> {
        validate_ref("ancestor branch", branch)?;
        validate_ref("target branch", target)?;
        let repo = self.repo()?;
        match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let branch = RefName::new(branch.to_string()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid ancestor branch for ancestry check: {error}"
                    ))
                })?;
                let target = RevSpec::new(target.to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid target branch for ancestry check: {error}"))
                })?;
                self.block_on_processkit(
                    git.is_merged(&self.root, MergeCheck::branch(branch).into_base(target)),
                )
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let revset =
                    RevsetExpr::new(format!("{branch} & ::{target}")).map_err(|error| {
                        VcsError::Runtime(format!("cannot construct JJ ancestry query: {error}"))
                    })?;
                Ok(!self
                    .block_on_processkit(jj.log(&self.root, &revset, 1))?
                    .is_empty())
            }
            _ => Err(VcsError::Runtime(
                "cannot prove task integration for an unsupported VCS backend".into(),
            )),
        }
    }

    /// The revision counterpart of [`Self::branch_is_ancestor_of`], used only after a prior
    /// re-anchor has removed the managed integration ref. Git's `branch --merged` typed API
    /// intentionally accepts a branch name rather than a raw SHA, so an exact typed log range
    /// provides the same ancestry proof without reintroducing a CLI-string escape hatch.
    fn revision_is_ancestor_of(&self, ancestor: &str, target: &str) -> Result<bool> {
        validate_ref("ancestor revision", ancestor)?;
        validate_ref("target revision", target)?;
        let repo = self.repo()?;
        match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                // `git log target..ancestor` is empty exactly when every ancestor commit is
                // already reachable from target.
                let range = RevSpec::new(format!("{target}..{ancestor}")).map_err(|error| {
                    VcsError::Runtime(format!("invalid Git re-anchor ancestry range: {error}"))
                })?;
                Ok(self
                    .block_on_processkit(git.log(&self.root, &range, 1))?
                    .is_empty())
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let revset =
                    RevsetExpr::new(format!("{ancestor} & ::{target}")).map_err(|error| {
                        VcsError::Runtime(format!(
                            "cannot construct JJ re-anchor ancestry query: {error}"
                        ))
                    })?;
                Ok(!self
                    .block_on_processkit(jj.log(&self.root, &revset, 1))?
                    .is_empty())
            }
            _ => Err(VcsError::Runtime(
                "cannot prove revision ancestry for an unsupported VCS backend".into(),
            )),
        }
    }

    /// Commit the explicit relative paths reported by a task leaf in one verified workspace.
    /// Every path must be a currently changed, ordinary relative path; an empty report, a path
    /// outside the workspace, and a report that names an unchanged file all fail closed. This is
    /// deliberately not an "all changed files" convenience: a leaf must not accidentally absorb
    /// an operator's or another tool's unrelated edit into its task commit.
    pub fn commit_workspace_paths(
        &self,
        workspace: &TaskWorkspace,
        paths: &[PathBuf],
        message: &str,
    ) -> Result<String> {
        self.require_workspace(workspace)?;
        self.commit_paths_at(&workspace.path, &workspace.branch, paths, message)
    }

    /// Resolve the exact durable tip of a verified task branch. The boundary observes the live
    /// working copy first, so a reviewer cannot hide an uncommitted edit behind a stale durable
    /// ref. JJ then uses the task bookmark because `jj commit` deliberately leaves an empty
    /// successor at `@` after finalising the bookmark's change.
    pub fn task_workspace_tip(&self, workspace: &TaskWorkspace) -> Result<String> {
        self.require_workspace(workspace)?;
        self.workspace_branch_tip(&workspace.path, &workspace.branch)
    }

    /// Read the typed Git-format diff of a task workspace for a guard or reviewer boundary.
    pub fn workspace_diff(&self, workspace: &TaskWorkspace) -> Result<Vec<vcs_diff::FileDiff>> {
        self.require_workspace(workspace)?;
        let repo = self.repo()?.at(&workspace.path);
        self.block_on(repo.diff())
    }

    /// Produce the immutable review surface for the exact committed task range.  This is separate
    /// from [`Self::workspace_diff`]: reviewers must not be scoped from a mutable worktree diff,
    /// and the workspace must still prove that its durable task bookmark/branch names `head`.
    pub fn task_review_range_evidence(
        &self,
        workspace: &TaskWorkspace,
        base: &str,
        head: &str,
    ) -> Result<TaskReviewRangeEvidence> {
        self.require_workspace(workspace)?;
        validate_ref("task review base", base)?;
        validate_ref("task review head", head)?;
        let actual = self.task_workspace_tip(workspace)?;
        if actual != head {
            return Err(VcsError::Runtime(format!(
                "refusing task review evidence: workspace {} tip {actual:?} differs from expected review head {head:?}",
                workspace.task_id
            )));
        }
        let repo = self.repo()?.at(&workspace.path);
        let (resolved_base, files) = match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let base = RevSpec::new(base.to_owned()).map_err(|error| {
                    VcsError::Runtime(format!("invalid task review base revision: {error}"))
                })?;
                let resolved_base =
                    self.block_on_processkit(git.resolve_commit(&workspace.path, &base))?;
                let range = DiffSpec::Rev(format!("{resolved_base}..{actual}"));
                let files = self.block_on_processkit(git.diff(&workspace.path, range))?;
                (resolved_base, files)
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let base = RevsetExpr::new(base.to_owned()).map_err(|error| {
                    VcsError::Runtime(format!("invalid JJ task review base revision: {error}"))
                })?;
                let resolved_base = self
                    .block_on_processkit(jj.template_query_ignoring_working_copy(
                        &workspace.path,
                        &base,
                        "commit_id",
                        Some(1),
                    ))?
                    .trim()
                    .to_string();
                if resolved_base.is_empty() {
                    return Err(VcsError::Runtime(
                        "JJ task review base did not resolve to one durable commit".into(),
                    ));
                }
                let range = DiffSpec::Rev(format!("{resolved_base}..{actual}"));
                let files = self.block_on_processkit(jj.diff(&workspace.path, range))?;
                (resolved_base, files)
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot produce task review evidence for an unsupported VCS backend".into(),
                ));
            }
        };
        let mut files = files
            .into_iter()
            .map(|file| {
                let raw = file.raw;
                Ok(TaskReviewRangeFile {
                    path: manifest_path(&file.path)?,
                    old_path: file.old_path.as_deref().map(manifest_path).transpose()?,
                    diff_sha256: format!("{:x}", Sha256::digest(raw.as_bytes())),
                    raw,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        files.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.old_path.cmp(&right.old_path))
        });
        Ok(TaskReviewRangeEvidence {
            schema: "orchestrail/task-review-range@1".into(),
            base: resolved_base,
            head: actual,
            files,
        })
    }

    /// Return the complete, repository-relative changed-path surface between two durable
    /// revisions.  This is deliberately range-based rather than a live-worktree diff: Phase-4
    /// verification must classify exactly the committed integration tip it is about to bless.
    /// Renames/copies retain both their old and new paths, so a code-to-document rename cannot
    /// receive a narrower docs-only classification than its actual change surface.
    pub fn changed_paths_between(&self, base: &str, head: &str) -> Result<Vec<PathBuf>> {
        validate_ref("verification base", base)?;
        validate_ref("verification head", head)?;
        let repo = self.repo()?;
        let mut paths = BTreeSet::new();
        match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let range = DiffSpec::Rev(format!("{base}..{head}"));
                for file in self.block_on_processkit(git.diff(&self.root, range))? {
                    paths.insert(file.path);
                    if let Some(old_path) = file.old_path {
                        paths.insert(old_path);
                    }
                }
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let base = RevsetExpr::new(base.to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid verification base revision: {error}"))
                })?;
                let head = RevsetExpr::new(head.to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid verification head revision: {error}"))
                })?;
                for file in self.block_on_processkit(jj.diff_summary(&self.root, &base, &head))? {
                    paths.insert(file.path);
                    if let Some(old_path) = file.old_path {
                        paths.insert(old_path);
                    }
                }
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot inspect verification range for an unsupported VCS backend".into(),
                ));
            }
        }
        Ok(paths.into_iter().collect())
    }

    /// Produce the exact typed diff/content manifest for a managed integration tip immediately
    /// before a human policy approval. Unlike a commit SHA alone, this gives the operator an
    /// inspectable, content-bound path surface and prevents a prior approval from authorizing a
    /// different base-to-head range.
    pub fn integration_approval_manifest(
        &self,
        integration: &IntegrationWorkspace,
        base: &str,
        head: &str,
    ) -> Result<ApprovalChangeManifest> {
        self.require_integration_workspace(integration)?;
        validate_ref("approval base", base)?;
        validate_ref("approval head", head)?;
        let actual = self.integration_workspace_tip(integration)?;
        if actual != head {
            return Err(VcsError::Runtime(format!(
                "refusing approval manifest: integration tip {actual:?} differs from requested head {head:?}"
            )));
        }
        self.approval_change_manifest_between(base, head)
    }

    fn approval_change_manifest_between(
        &self,
        base: &str,
        head: &str,
    ) -> Result<ApprovalChangeManifest> {
        let repo = self.repo()?;
        let range = format!("{base}..{head}");
        let files = match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                self.block_on_processkit(git.diff(&self.root, DiffSpec::Rev(range)))?
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                self.block_on_processkit(jj.diff(&self.root, DiffSpec::Rev(range)))?
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot produce an approval manifest for an unsupported VCS backend".into(),
                ));
            }
        };
        let mut changes = files
            .into_iter()
            .map(|file| {
                let path = manifest_path(&file.path)?;
                let old_path = file.old_path.as_deref().map(manifest_path).transpose()?;
                Ok(ApprovalChangeEntry {
                    path,
                    old_path,
                    diff_sha256: format!("{:x}", Sha256::digest(file.raw.as_bytes())),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        changes.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.old_path.cmp(&right.old_path))
                .then_with(|| left.diff_sha256.cmp(&right.diff_sha256))
        });
        Ok(ApprovalChangeManifest {
            schema: "orchestrail/approval-change-manifest@1",
            base: base.into(),
            head: head.into(),
            changes,
        })
    }

    /// Run a fully rolled-back typed merge probe of one verified task branch into the guarded
    /// integration workspace. A clean probe is evidence for the merger leaf, not a replacement
    /// for it: the leaf still owns the real merge, its report, verification and any deliberate
    /// conflict resolution. A conflict result is likewise evidence to present to that leaf, never
    /// an automatic quarantine decision.
    pub fn preflight_task_merge(
        &self,
        workspace: &IntegrationWorkspace,
        task_id: &str,
    ) -> Result<MergeProbe> {
        self.require_integration_workspace(workspace)?;
        validate_task_id(task_id)?;
        let repo = self.repo()?.at(&workspace.path);
        self.block_on(repo.try_merge(&format!("task/{task_id}")))
    }

    /// Start one *known-conflicting* merge and leave it in the backend's typed, recoverable
    /// conflict state for the checkpointed merger leaf.  A clean probe never enters this method;
    /// a probe/working-copy race that turns clean is aborted and reported as a contradiction.
    /// The integration bookmark/branch continues to name `pre_merge_head` until finalization.
    pub fn begin_merge_conflict_resolution(
        &self,
        integration: &IntegrationWorkspace,
        task: &TaskWorkspace,
        expected_task_head: &str,
        expected_integration_head: &str,
    ) -> Result<MergeConflictSession> {
        self.require_integration_workspace(integration)?;
        self.require_workspace(task)?;
        validate_ref("expected task head", expected_task_head)?;
        validate_ref("expected integration head", expected_integration_head)?;
        if integration.backend != task.backend || integration.backend != self.backend {
            return Err(VcsError::Runtime(
                "task and integration workspaces belong to different VCS backends".into(),
            ));
        }
        let actual_task_head = self.task_workspace_tip(task)?;
        if actual_task_head != expected_task_head {
            return Err(VcsError::Runtime(format!(
                "refusing conflict resolution: task {} tip {actual_task_head:?} differs from reviewed tip {expected_task_head:?}",
                task.task_id
            )));
        }
        let pre_merge_head = self.integration_workspace_tip(integration)?;
        if pre_merge_head != expected_integration_head {
            return Err(VcsError::Runtime(format!(
                "refusing conflict resolution: integration tip {pre_merge_head:?} differs from durable tip {expected_integration_head:?}"
            )));
        }
        let paths = match self.preflight_task_merge(integration, &task.task_id)? {
            MergeProbe::Conflicts(paths) if !paths.is_empty() => paths,
            MergeProbe::Clean => {
                return Err(VcsError::Runtime(format!(
                    "refusing conflict resolution for {}: typed preflight is clean",
                    task.task_id
                )));
            }
            _ => {
                return Err(VcsError::Runtime(format!(
                    "refusing conflict resolution for {}: VCS returned no conflict paths",
                    task.task_id
                )));
            }
        };

        let repo = self.repo()?.at(&integration.path);
        let mut merge_paths = match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let branch = RevSpec::new(task.branch.clone())
                    .map_err(|error| VcsError::Runtime(format!("invalid task branch: {error}")))?;
                let start = self.block_on_processkit(
                    git.merge_no_commit(&integration.path, MergeNoCommit::branch(branch).no_ff()),
                );
                let actual_paths =
                    self.block_on_processkit(git.conflicted_files(&integration.path))?;
                if actual_paths.is_empty() {
                    let abort = self.block_on_processkit(git.merge_abort(&integration.path));
                    return Err(match (start, abort) {
                        (Ok(_), Ok(_)) => VcsError::Runtime(format!(
                            "merge {} unexpectedly completed without conflicts after conflicting preflight",
                            task.task_id
                        )),
                        (Err(error), Ok(_)) => VcsError::Runtime(format!(
                            "merge {} failed without a typed conflict set: {error}",
                            task.task_id
                        )),
                        (_, Err(abort_error)) => VcsError::Runtime(format!(
                            "merge {} produced no typed conflict set and typed abort failed: {abort_error}",
                            task.task_id
                        )),
                    });
                }
                if actual_paths.iter().collect::<BTreeSet<_>>() != paths.iter().collect() {
                    let abort = self.block_on_processkit(git.merge_abort(&integration.path));
                    return Err(match abort {
                        Ok(_) => VcsError::Runtime(format!(
                            "merge {} conflict paths changed between preflight and start",
                            task.task_id
                        )),
                        Err(error) => VcsError::Runtime(format!(
                            "merge {} conflict paths changed and typed abort failed: {error}",
                            task.task_id
                        )),
                    });
                }
                let merge_paths: Vec<PathBuf> = self
                    .block_on_processkit(git.status(&integration.path))?
                    .into_iter()
                    .flat_map(|entry| {
                        entry
                            .old_path
                            .into_iter()
                            .chain(std::iter::once(entry.path))
                    })
                    .collect();
                // A non-zero Git result is expected for an unresolved textual conflict. The
                // independently queried conflict set is the authoritative proof that it is safe
                // to hand the worktree to the merger leaf.
                merge_paths
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                // The managed workspace keeps `@` as a clean empty child of the durable
                // integration bookmark. Using that child as a merge parent would promote the
                // throwaway (and usually undescribed) change into publishable history.
                let current = RevsetExpr::new(integration.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid integration bookmark revision for conflict merge: {error}"
                    ))
                })?;
                let task_branch = RevsetExpr::new(task.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!("invalid task bookmark revision: {error}"))
                })?;
                self.block_on_processkit(jj.new_merge(
                    &integration.path,
                    &format!("resolve merge {} into {}", task.task_id, integration.branch),
                    vec![current.clone(), task_branch],
                ))?;
                let has_conflict =
                    self.block_on_processkit(jj.has_workingcopy_conflict(&integration.path))?;
                let conflicted = RevsetExpr::new("@").map_err(|error| {
                    VcsError::Runtime(format!("cannot construct JJ conflicted revision: {error}"))
                })?;
                let actual_paths =
                    self.block_on_processkit(jj.resolve_list(&integration.path, &conflicted))?;
                if !has_conflict || actual_paths.is_empty() {
                    let restore = RevsetExpr::new(pre_merge_head.clone()).map_err(|error| {
                        VcsError::Runtime(format!("invalid JJ conflict rollback target: {error}"))
                    })?;
                    let cleanup =
                        self.block_on_processkit(jj.new_child(&integration.path, &restore));
                    return Err(match cleanup {
                        Ok(_) => VcsError::Runtime(format!(
                            "JJ merge {} unexpectedly completed without a typed conflict set",
                            task.task_id
                        )),
                        Err(error) => VcsError::Runtime(format!(
                            "JJ merge {} produced no typed conflict set and restore failed: {error}",
                            task.task_id
                        )),
                    });
                }
                if actual_paths.iter().collect::<BTreeSet<_>>() != paths.iter().collect() {
                    let restore = RevsetExpr::new(pre_merge_head.clone()).map_err(|error| {
                        VcsError::Runtime(format!("invalid JJ conflict rollback target: {error}"))
                    })?;
                    let cleanup =
                        self.block_on_processkit(jj.new_child(&integration.path, &restore));
                    return Err(match cleanup {
                        Ok(_) => VcsError::Runtime(format!(
                            "JJ merge {} conflict paths changed between preflight and start",
                            task.task_id
                        )),
                        Err(error) => VcsError::Runtime(format!(
                            "JJ merge {} conflict paths changed and restore failed: {error}",
                            task.task_id
                        )),
                    });
                }
                // JJ represents the in-progress result as a multi-parent conflicted change.
                // Its typed range-diff adapter correctly rejects a graph with a missing parent,
                // so unlike Git porcelain it cannot provide a complete staged path surface here.
                // The conflict path set is still typed and exact; finalization separately proves
                // that the working copy has no unresolved conflict before moving the bookmark.
                paths.clone()
            }
            _ => return Err(VcsError::Runtime("unsupported VCS backend".into())),
        };

        merge_paths.sort();
        merge_paths.dedup();
        if merge_paths.is_empty()
            || !paths
                .iter()
                .all(|path| merge_paths.binary_search(path).is_ok())
        {
            return Err(VcsError::Runtime(format!(
                "typed merge {} conflict paths are not contained in its changed-path surface",
                task.task_id
            )));
        }
        let protected_paths = merge_paths
            .iter()
            .filter(|path| !paths.contains(path))
            .map(|path| {
                Ok(MergePathFingerprint {
                    path: path.clone(),
                    sha256: fingerprint_merge_path(&integration.path, path)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MergeConflictSession {
            task_id: task.task_id.clone(),
            pre_merge_head,
            merge_paths,
            paths,
            protected_paths,
        })
    }

    /// Commit/record a previously started conflict merge only after the merger leaf has returned
    /// success.  The typed backend re-reads both the durable branch tip and unresolved conflict
    /// state; the model's changed-file report is evidence for audit, never authority to finish.
    pub fn finalize_merge_conflict_resolution(
        &self,
        integration: &IntegrationWorkspace,
        task: &TaskWorkspace,
        expected: MergeResolutionFinalization<'_>,
    ) -> Result<String> {
        let MergeResolutionFinalization {
            task_head: expected_task_head,
            pre_merge_head: expected_pre_merge_head,
            merge_paths: expected_merge_paths,
            conflict_paths: expected_paths,
            protected_paths: expected_protected_paths,
        } = expected;
        self.require_integration_workspace(integration)?;
        self.require_workspace(task)?;
        validate_ref("expected task head", expected_task_head)?;
        validate_ref(
            "expected pre-merge integration head",
            expected_pre_merge_head,
        )?;
        if expected_merge_paths.is_empty() || expected_paths.is_empty() {
            return Err(VcsError::InvalidInput(
                "resolved merge has no recorded typed path surface to stage".into(),
            ));
        }
        for path in expected_merge_paths {
            validate_workspace_relative_path(path)?;
        }
        for path in expected_paths {
            validate_workspace_relative_path(path)?;
        }
        if !expected_paths
            .iter()
            .all(|path| expected_merge_paths.contains(path))
        {
            return Err(VcsError::InvalidInput(
                "resolved conflict paths are outside the recorded merge surface".into(),
            ));
        }
        verify_merge_path_fingerprints(
            &integration.path,
            expected_merge_paths,
            expected_paths,
            expected_protected_paths,
        )?;
        let actual_task_head = self.task_workspace_tip(task)?;
        if actual_task_head != expected_task_head {
            return Err(VcsError::Runtime(format!(
                "refusing resolved merge: task {} tip {actual_task_head:?} differs from reviewed tip {expected_task_head:?}",
                task.task_id
            )));
        }
        let actual_pre_merge_head =
            self.integration_workspace_tip_during_merge_resolution(integration)?;
        if actual_pre_merge_head != expected_pre_merge_head {
            return Err(VcsError::Runtime(format!(
                "refusing resolved merge: integration tip {actual_pre_merge_head:?} differs from expected pre-merge tip {expected_pre_merge_head:?}"
            )));
        }
        let repo = self.repo()?.at(&integration.path);
        match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                // The merger leaf has no VCS authority. Stage only the exact paths that the
                // typed conflict start recorded, then re-query instead of trusting a report that
                // says it resolved them.
                let observed_merge_paths = self
                    .block_on_processkit(git.status(&integration.path))?
                    .into_iter()
                    .flat_map(|entry| {
                        entry
                            .old_path
                            .into_iter()
                            .chain(std::iter::once(entry.path))
                    })
                    .collect::<BTreeSet<_>>();
                let expected_merge_set = expected_merge_paths
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if observed_merge_paths != expected_merge_set {
                    return Err(VcsError::Runtime(
                        "resolved Git merge changed a path outside its recorded typed merge surface"
                            .into(),
                    ));
                }
                self.block_on_processkit(git.add(&integration.path, expected_paths))?;
                let unresolved =
                    self.block_on_processkit(git.conflicted_files(&integration.path))?;
                if !unresolved.is_empty() {
                    return Err(VcsError::Runtime(format!(
                        "refusing resolved merge for {}: unresolved paths remain: {}",
                        task.task_id,
                        unresolved
                            .iter()
                            .map(|path| path.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                self.block_on_processkit(git.merge_continue(&integration.path))?;
                let snapshot = self.block_on(repo.snapshot_readonly())?;
                snapshot.head.ok_or_else(|| {
                    VcsError::Runtime(format!(
                        "resolved merge {} completed but integration workspace has no durable head",
                        task.task_id
                    ))
                })
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                // `status` deliberately snapshots the merger leaf's filesystem edits before
                // any bookmark can move. A conflicted JJ change itself has no gap-free
                // multi-parent diff surface, but this post-leaf working-copy delta must name
                // exactly the paths that were originally conflicted.
                let observed_paths = self
                    .block_on_processkit(jj.status(&integration.path))?
                    .into_iter()
                    .flat_map(|entry| {
                        entry
                            .old_path
                            .into_iter()
                            .chain(std::iter::once(entry.path))
                    })
                    .collect::<BTreeSet<_>>();
                let expected_paths = expected_paths.iter().cloned().collect::<BTreeSet<_>>();
                if observed_paths != expected_paths {
                    return Err(VcsError::Runtime(
                        "resolved JJ merge changed a path outside its recorded typed conflict set"
                            .into(),
                    ));
                }
                if self.block_on_processkit(jj.has_workingcopy_conflict(&integration.path))? {
                    return Err(VcsError::Runtime(format!(
                        "refusing resolved JJ merge for {}: working copy is still conflicted",
                        task.task_id
                    )));
                }
                let current = RevsetExpr::new("@").map_err(|error| {
                    VcsError::Runtime(format!("cannot construct JJ current revision: {error}"))
                })?;
                let bookmark = BookmarkName::new(integration.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!("invalid integration bookmark: {error}"))
                })?;
                self.block_on_processkit(jj.bookmark_set(&integration.path, &bookmark, &current))?;
                let merged = RevsetExpr::new(integration.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!("invalid resolved integration bookmark: {error}"))
                })?;
                self.block_on_processkit(jj.new_child(&integration.path, &merged))?;
                let bookmarks = self
                    .block_on_processkit(jj.bookmarks_ignoring_working_copy(&integration.path))?;
                jj_bookmark_target(&bookmarks, &integration.branch)
            }
            _ => Err(VcsError::Runtime("unsupported VCS backend".into())),
        }
    }

    /// Abort a previously started typed conflict merge and prove that the integration workspace
    /// is clean at its exact recorded pre-merge tip before the task is returned to the queue.
    pub fn abort_merge_conflict_resolution(
        &self,
        integration: &IntegrationWorkspace,
        expected_pre_merge_head: &str,
    ) -> Result<()> {
        self.require_integration_workspace(integration)?;
        validate_ref(
            "expected pre-merge integration head",
            expected_pre_merge_head,
        )?;
        let actual = self.integration_workspace_tip_during_merge_resolution(integration)?;
        if actual != expected_pre_merge_head {
            return Err(VcsError::Runtime(format!(
                "refusing conflict abort: integration tip {actual:?} differs from expected pre-merge tip {expected_pre_merge_head:?}"
            )));
        }
        let repo = self.repo()?.at(&integration.path);
        let before = self.block_on(repo.snapshot_readonly())?;
        if !before.dirty && !before.conflicted {
            // The VCS half may have completed before a later control-plane write failed. The
            // durable reducer still owns the abort effect, so recognizing this exact clean
            // pre-merge state makes that retry idempotent without accepting any other state.
            return Ok(());
        }
        match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                self.block_on_processkit(git.merge_abort(&integration.path))?;
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let restore =
                    RevsetExpr::new(expected_pre_merge_head.to_owned()).map_err(|error| {
                        VcsError::Runtime(format!("invalid JJ conflict rollback target: {error}"))
                    })?;
                self.block_on_processkit(jj.new_child(&integration.path, &restore))?;
            }
            _ => return Err(VcsError::Runtime("unsupported VCS backend".into())),
        }
        let restored = self.integration_workspace_tip(integration)?;
        if restored != expected_pre_merge_head {
            return Err(VcsError::Runtime(format!(
                "conflict abort completed at {restored:?}, not requested {expected_pre_merge_head:?}"
            )));
        }
        let snapshot = self.block_on(repo.snapshot_readonly())?;
        if snapshot.dirty || snapshot.conflicted {
            return Err(VcsError::Runtime(
                "conflict abort left a dirty or conflicted integration workspace".into(),
            ));
        }
        Ok(())
    }

    /// Merge one verified task branch into the integration workspace using the typed backend
    /// implementation. The clean probe is rollback-safe; a later merge failure is followed by a
    /// typed abort before the error is returned, so no caller can advance from an unknown index.
    pub fn merge_task_into_integration(
        &self,
        integration: &IntegrationWorkspace,
        task: &TaskWorkspace,
        expected_task_head: &str,
        expected_integration_head: Option<&str>,
    ) -> Result<String> {
        self.require_integration_workspace(integration)?;
        self.require_workspace(task)?;
        validate_ref("expected task head", expected_task_head)?;
        if let Some(expected) = expected_integration_head {
            validate_ref("expected integration head", expected)?;
        }
        if integration.backend != task.backend || integration.backend != self.backend {
            return Err(VcsError::Runtime(
                "task and integration workspaces belong to different VCS backends".into(),
            ));
        }
        let actual_task_head = self.task_workspace_tip(task)?;
        if actual_task_head != expected_task_head {
            return Err(VcsError::Runtime(format!(
                "refusing merge: task {} tip {actual_task_head:?} differs from reviewed tip {expected_task_head:?}",
                task.task_id
            )));
        }
        let actual_integration_head = self.integration_workspace_tip(integration)?;
        if let Some(expected) = expected_integration_head
            && actual_integration_head != expected
        {
            return Err(VcsError::Runtime(format!(
                "refusing merge: integration tip {actual_integration_head:?} differs from durable tip {expected:?}"
            )));
        }
        // A legacy merger may have committed this exact reviewed task before crashing without a
        // report. Replaying that Phase-0.4 boundary must be idempotent on both backends: Git's
        // already-up-to-date behavior is not a new merge commit, while JJ would otherwise create
        // a redundant change. The native port still runs per-merge verification before it
        // acknowledges this existing ancestry in the reducer.
        if self.task_is_merged_into_integration(&task.task_id, &integration.batch_id)? {
            return Ok(actual_integration_head);
        }
        match self.preflight_task_merge(integration, &task.task_id)? {
            MergeProbe::Clean => {}
            MergeProbe::Conflicts(paths) => {
                return Err(VcsError::MergeConflict {
                    task_id: task.task_id.clone(),
                    paths,
                });
            }
            _ => {
                return Err(VcsError::Runtime(
                    "VCS returned an unsupported merge-probe outcome".into(),
                ));
            }
        }

        let repo = self.repo()?.at(&integration.path);
        let merged: Result<()> = match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let branch = RevSpec::new(task.branch.clone())
                    .map_err(|error| VcsError::Runtime(format!("invalid task branch: {error}")))?;
                self.block_on_processkit(git.merge_commit(
                    &integration.path,
                    MergeCommit::branch(branch).no_ff().message(format!(
                        "merge {} into {}",
                        task.task_id, integration.branch
                    )),
                ))
                .map_err(|error| VcsError::Runtime(format!("typed Git merge failed: {error}")))
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                // `@` is the deliberately unbookmarked clean workspace child. It must never be
                // made a merge parent: otherwise the empty successor becomes part of durable
                // integration history and JJ refuses to publish its empty description.
                let current = RevsetExpr::new(integration.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid durable JJ integration bookmark revision: {error}"
                    ))
                })?;
                let task_branch = RevsetExpr::new(task.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!("invalid task bookmark revision: {error}"))
                })?;
                self.block_on_processkit(jj.new_merge(
                    &integration.path,
                    &format!("merge {} into {}", task.task_id, integration.branch),
                    vec![current, task_branch],
                ))?;
                let bookmark = BookmarkName::new(integration.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!("invalid integration bookmark: {error}"))
                })?;
                let current = RevsetExpr::new("@").map_err(|error| {
                    VcsError::Runtime(format!("cannot construct JJ current revision: {error}"))
                })?;
                self.block_on_processkit(jj.bookmark_set(&integration.path, &bookmark, &current))?;
                // A JJ merge creates a non-empty current change. Advance the durable bookmark
                // to it, then work from an empty child. Review/verification boundaries can
                // therefore reject any subsequent mutable working-copy write.
                let merged_revision =
                    RevsetExpr::new(integration.branch.clone()).map_err(|error| {
                        VcsError::Runtime(format!(
                            "invalid integration bookmark revision after merge: {error}"
                        ))
                    })?;
                self.block_on_processkit(jj.new_child(&integration.path, &merged_revision))
            }
            _ => Err(VcsError::Runtime("unsupported VCS backend".into())),
        };
        if let Err(error) = merged {
            return match self.block_on(repo.abort_in_progress()) {
                Ok(_) => Err(error),
                Err(cleanup_error) => Err(VcsError::Runtime(format!(
                    "merge {} failed ({error}); typed abort also failed ({cleanup_error})",
                    task.task_id
                ))),
            };
        }
        if self.backend == BackendKind::Jj {
            let jj = repo
                .jj()
                .ok_or_else(|| VcsError::Runtime("JJ repository has no typed JJ client".into()))?;
            let bookmarks =
                self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&integration.path))?;
            return jj_bookmark_target(&bookmarks, &integration.branch);
        }
        let snapshot = self.block_on(repo.snapshot_readonly())?;
        snapshot.head.ok_or_else(|| {
            VcsError::Runtime(format!(
                "merge {} completed but integration workspace has no durable head",
                task.task_id
            ))
        })
    }

    /// Remove one just-merged task from the managed integration branch after a deterministic
    /// per-merge validation failure. Both coordinates are mandatory: the branch is rewound only
    /// when it still names the exact candidate merge, and only to the exact durable pre-merge
    /// tip. A crash between the JJ workspace transition and its backwards bookmark move leaves a
    /// disagreeing durable bookmark, which the next merge precondition refuses rather than
    /// accidentally re-merging an unacknowledged candidate.
    pub fn rollback_integration_merge(
        &self,
        integration: &IntegrationWorkspace,
        expected_merged_head: &str,
        restore_head: &str,
    ) -> Result<()> {
        self.require_integration_workspace(integration)?;
        validate_ref("expected merged integration head", expected_merged_head)?;
        validate_ref("integration rollback target", restore_head)?;
        let actual = self.integration_workspace_tip(integration)?;
        if actual != expected_merged_head {
            return Err(VcsError::Runtime(format!(
                "refusing integration rollback: current tip {actual:?} differs from candidate {expected_merged_head:?}"
            )));
        }
        let repo = self.repo()?.at(&integration.path);
        match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let target = RevSpec::new(restore_head.to_owned()).map_err(|error| {
                    VcsError::Runtime(format!("invalid integration rollback target: {error}"))
                })?;
                self.block_on_processkit(git.reset_hard(&integration.path, &target))?;
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let target = RevsetExpr::new(restore_head.to_owned()).map_err(|error| {
                    VcsError::Runtime(format!("invalid JJ integration rollback target: {error}"))
                })?;
                // Advance the workspace first. If a crash happens before the bookmark move, the
                // old bookmark still contradicts the checkpointed pre-merge tip and recovery
                // holds instead of treating the candidate as safely absent.
                self.block_on_processkit(jj.new_child(&integration.path, &target))?;
                let bookmark = BookmarkName::new(integration.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!("invalid integration bookmark: {error}"))
                })?;
                self.block_on_processkit(jj.bookmark_move(
                    &integration.path,
                    BookmarkMove::new(bookmark, target).allow_backwards(),
                ))?;
            }
            _ => return Err(VcsError::Runtime("unsupported VCS backend".into())),
        }
        let restored = self.integration_workspace_tip(integration)?;
        if restored != restore_head {
            return Err(VcsError::Runtime(format!(
                "integration rollback completed at {restored:?}, not requested {restore_head:?}"
            )));
        }
        let snapshot = self.block_on(repo.snapshot_readonly())?;
        if snapshot.dirty || snapshot.conflicted {
            return Err(VcsError::Runtime(
                "integration rollback left a dirty or conflicted workspace".into(),
            ));
        }
        Ok(())
    }

    /// Commit the exact relative paths reported by a merger, integration-fix, or CI-fix leaf in
    /// the guarded integration workspace. It shares the task path validation and Git staging
    /// rules, so a failed leaf cannot silently commit every incidental change in `_integration`.
    pub fn commit_integration_workspace_paths(
        &self,
        workspace: &IntegrationWorkspace,
        paths: &[PathBuf],
        message: &str,
    ) -> Result<String> {
        self.require_integration_workspace(workspace)?;
        self.commit_paths_at(&workspace.path, &workspace.branch, paths, message)
    }

    /// Resolve the exact durable integration branch tip without trusting a model report or a
    /// transient JJ working-copy successor.
    pub fn integration_workspace_tip(&self, workspace: &IntegrationWorkspace) -> Result<String> {
        self.require_integration_workspace(workspace)?;
        self.workspace_branch_tip(&workspace.path, &workspace.branch)
    }

    /// Read the durable integration ref while a previously checkpointed merge is intentionally
    /// dirty/conflicted. This is deliberately narrower than [`integration_workspace_tip`]: the
    /// caller must already possess the reducer's pending-conflict coordinate, and this method
    /// still verifies the registered Git branch/JJ bookmark rather than trusting `HEAD`/`@`.
    pub fn integration_workspace_tip_during_merge_resolution(
        &self,
        workspace: &IntegrationWorkspace,
    ) -> Result<String> {
        self.require_integration_workspace(workspace)?;
        self.workspace_branch_tip_allowing_in_progress_merge(&workspace.path, &workspace.branch)
    }

    /// Fast-forward the checked-out publication branch to a verified integration workspace and,
    /// when requested, push that exact branch through the typed `vcs-core` backend.  Publication
    /// is intentionally owned here rather than by a model/forge adapter: a leaf may review or
    /// report readiness, but it may never choose an arbitrary ref or claim that a local branch
    /// became published.
    ///
    /// Git first proves that the publication branch is already contained in the integration
    /// branch, then uses `merge` without `--no-ff`; that is a true fast-forward rather than an
    /// accidental merge commit.
    /// JJ publication uses its typed `bookmark move` operation without `--allow-backwards`: jj
    /// itself proves the destination is a descendant of the current bookmark target before it
    /// permits the move, and this boundary independently verifies the exact target afterwards.
    pub fn publish_integration(
        &self,
        workspace: &IntegrationWorkspace,
        base: &str,
        expected_integration_head: &str,
        push: bool,
    ) -> Result<String> {
        self.require_integration_workspace(workspace)?;
        validate_ref("publication branch", base)?;
        validate_ref("expected integration head", expected_integration_head)?;
        let actual_integration_head = self.integration_workspace_tip(workspace)?;
        if actual_integration_head != expected_integration_head {
            return Err(VcsError::Runtime(format!(
                "refusing publication: integration workspace tip {actual_integration_head:?} differs from expected durable tip {expected_integration_head:?}"
            )));
        }
        let repo = self.repo()?;
        let root_snapshot = self.block_on(repo.snapshot_readonly())?;
        if root_snapshot.dirty || root_snapshot.conflicted {
            return Err(VcsError::Runtime(
                "refusing publication from a dirty or conflicted primary checkout".into(),
            ));
        }
        // Git has one checked-out branch, so its textual branch name is an exact guard. JJ may
        // render any nearest bookmark that shares the current revision (for example the retained
        // `integration/<batch>` after `main` has already moved), so its exact primary-parent
        // proof lives in the JJ branch below instead of trusting that ambiguous presentation.
        if self.backend == BackendKind::Git && root_snapshot.branch.as_deref() != Some(base) {
            return Err(VcsError::Runtime(format!(
                "primary checkout is on {:?}, expected publication branch {base}",
                root_snapshot.branch
            )));
        }

        let jj_published_target = match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let integration = RevSpec::new(workspace.branch.clone()).map_err(|error| {
                    VcsError::Runtime(format!("invalid integration branch: {error}"))
                })?;
                let publication = RefName::new(base.to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid publication branch: {error}"))
                })?;
                let can_fast_forward = self.block_on_processkit(git.is_merged(
                    &self.root,
                    MergeCheck::branch(publication).into_base(integration.clone()),
                ))?;
                if !can_fast_forward {
                    return Err(VcsError::PublicationLocalDivergence(format!(
                        "local publication cannot fast-forward: {base} is not an ancestor of {}",
                        workspace.branch
                    )));
                }
                self.block_on_processkit(
                    git.merge_commit(&self.root, MergeCommit::branch(integration)),
                )
                .map_err(|error| {
                    VcsError::Runtime(format!("typed fast-forward publication failed: {error}"))
                })?;
                None
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let before =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                let current_target = jj_bookmark_target(&before, base)?;
                let integration_target = jj_bookmark_target(&before, &workspace.branch)?;
                let parent_revision =
                    RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                        VcsError::Runtime(format!("invalid JJ primary parent revision: {error}"))
                    })?;
                let primary_parent = self
                    .block_on_processkit(jj.template_query_ignoring_working_copy(
                        &self.root,
                        &parent_revision,
                        "commit_id",
                        Some(1),
                    ))?
                    .trim()
                    .to_string();
                if primary_parent != current_target {
                    return Err(VcsError::Runtime(format!(
                        "JJ primary workspace parent {primary_parent:?} is not the current {base} target {current_target:?}"
                    )));
                }
                if !self.branch_is_ancestor_of(base, &workspace.branch)? {
                    return Err(VcsError::PublicationLocalDivergence(format!(
                        "local JJ publication cannot fast-forward: {base} is not an ancestor of {}",
                        workspace.branch
                    )));
                }
                let moved = if current_target == integration_target {
                    // The durable bookmark already names the verified integration target. This
                    // is the normal retry shape after a crash or a failed first push: preserve
                    // the existing clean child and only re-run the remote publication below.
                    false
                } else {
                    let publication = BookmarkName::new(base.to_string()).map_err(|error| {
                        VcsError::Runtime(format!("invalid publication bookmark: {error}"))
                    })?;
                    let integration =
                        RevsetExpr::new(workspace.branch.clone()).map_err(|error| {
                            VcsError::Runtime(format!(
                                "invalid integration bookmark revision: {error}"
                            ))
                        })?;
                    // `BookmarkMove::new` deliberately omits `--allow-backwards`; jj refuses a
                    // non-descendant target, which is the native fast-forward proof.
                    self.block_on_processkit(
                        jj.bookmark_move(&self.root, BookmarkMove::new(publication, integration)),
                    )?;
                    true
                };
                let after =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                let published_target = jj_bookmark_target(&after, base)?;
                let verified_integration_target = jj_bookmark_target(&after, &workspace.branch)?;
                if published_target != verified_integration_target {
                    return Err(VcsError::Runtime(format!(
                        "JJ publication moved {base} to {published_target}, not exact integration target {verified_integration_target}"
                    )));
                }
                if moved {
                    // Moving a bookmark does not move a JJ working copy. Start a fresh, *empty*
                    // child on the just-published bookmark so a later direct CI repair has a
                    // clean edit surface. `jj edit <bookmark>` is wrong here: it makes `@` the
                    // non-empty published change itself, which JJ correctly reports as changed.
                    let published_revision =
                        RevsetExpr::new(base.to_string()).map_err(|error| {
                            VcsError::Runtime(format!(
                                "invalid published JJ bookmark revision: {error}"
                            ))
                        })?;
                    self.block_on_processkit(jj.new_child(&self.root, &published_revision))?;
                    let root_after_edit = self.block_on(repo.snapshot_readonly())?;
                    let parent_revision =
                        RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                            VcsError::Runtime(format!(
                                "invalid JJ primary parent revision: {error}"
                            ))
                        })?;
                    let parent = self
                        .block_on_processkit(jj.template_query_ignoring_working_copy(
                            &self.root,
                            &parent_revision,
                            "commit_id",
                            Some(1),
                        ))?
                        .trim()
                        .to_string();
                    if parent != published_target
                        || root_after_edit.dirty
                        || root_after_edit.conflicted
                    {
                        return Err(VcsError::Runtime(format!(
                            "JJ primary workspace did not create a clean child of published {base} target {published_target}: observed parent={parent:?}, branch={:?}, head={:?}, dirty={}, conflicted={}",
                            root_after_edit.branch,
                            root_after_edit.head,
                            root_after_edit.dirty,
                            root_after_edit.conflicted
                        )));
                    }
                }
                Some(published_target)
            }
            _ => return Err(VcsError::Runtime("unsupported VCS backend".into())),
        };
        if push {
            let push_result = if self.backend == BackendKind::Jj {
                let target = jj_published_target.as_deref().ok_or_else(|| {
                    VcsError::Runtime(
                        "JJ publication did not retain its exact bookmark target for push".into(),
                    )
                })?;
                let store = self.git_backend_directory()?;
                let source = RefName::new(target.to_string()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid exact JJ publication target {target:?}: {error}"
                    ))
                })?;
                let destination = RefName::new(base.to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid publication branch {base:?}: {error}"))
                })?;
                self.block_on_processkit(
                    Git::hardened().push(&store, GitPush::refspec(&source, &destination)),
                )
            } else {
                self.block_on(repo.push(base))
            };
            push_result.map_err(|error| {
                VcsError::PublicationPushFailed(format!(
                    "typed push of exact publication target for branch {base} failed: {error}"
                ))
            })?;
        }
        if let Some(target) = jj_published_target {
            return Ok(target);
        }
        let published = self.block_on(repo.snapshot_readonly())?;
        published.head.ok_or_else(|| {
            VcsError::Runtime("publication completed but primary checkout has no head".into())
        })
    }

    /// Recover a local primary ref which was fast-forwarded to an integration candidate before
    /// its typed push was rejected because `origin/<base>` advanced independently.  This method
    /// runs only after the reducer has durably recorded that requirement, so every mutating step
    /// can be repeated after a crash:
    ///
    /// 1. fetch the exact origin/base ref and prove the candidate is neither published nor a
    ///    descendant of that ref;
    /// 2. reset/move the local primary ref to that fetched remote target;
    /// 3. remove only the managed integration worktree/workspace and its branch/bookmark.
    ///
    /// Task branches and workspaces deliberately survive.  They are replayed by the normal
    /// merger/review/verification sequence from the new remote base.
    pub fn reanchor_after_remote_rejection(
        &self,
        work: impl AsRef<Path>,
        batch_id: &str,
        base: &str,
        expected_integration_head: &str,
    ) -> Result<PublicationReanchorOutcome> {
        validate_batch_id(batch_id)?;
        validate_ref("publication branch", base)?;
        validate_ref("expected integration head", expected_integration_head)?;
        let integration_branch = format!("integration/{batch_id}");
        let repo = self.repo()?;

        // Fetching is deliberately inside this already checkpointed recovery effect.  JJ's
        // fetch may reconcile remote-tracking bookmarks, so it must not be used by the initial
        // read-only failed-push classifier.
        self.block_on(repo.fetch_branch(base))?;
        let remote_target = self.fetched_remote_publication_target(&repo, base)?;
        let branch_exists = self.block_on(repo.branch_exists(&integration_branch))?;
        if !branch_exists {
            self.require_primary_reanchored_to(base, &remote_target)?;
            // A retry can arrive after the prior process deleted the integration ref but before
            // its reducer acknowledgement.  Do not silently re-merge if a concurrent writer
            // published that exact retained integration object in the meantime.
            if self.revision_is_ancestor_of(expected_integration_head, &remote_target)? {
                return Ok(PublicationReanchorOutcome::Published {
                    head: expected_integration_head.to_string(),
                });
            }
            return Ok(PublicationReanchorOutcome::Reanchored);
        }

        let actual_integration = self.branch_tip(&repo, &integration_branch)?;
        if actual_integration != expected_integration_head {
            return Err(VcsError::Runtime(format!(
                "refusing publication re-anchor: {integration_branch} tip {actual_integration:?} differs from checkpointed integration tip {expected_integration_head:?}"
            )));
        }
        if self.branch_is_ancestor_of(&integration_branch, &remote_target)? {
            return Ok(PublicationReanchorOutcome::Published {
                head: expected_integration_head.to_string(),
            });
        }
        if self.branch_is_ancestor_of(&remote_target, &integration_branch)? {
            return Err(VcsError::Runtime(format!(
                "refusing publication re-anchor: fetched origin/{base} is an ancestor of {integration_branch}; the rejected push is not a proven remote divergence"
            )));
        }

        match self.backend {
            BackendKind::Git => {
                let git = repo.git().ok_or_else(|| {
                    VcsError::Runtime("Git repository has no typed Git client".into())
                })?;
                let target = RevSpec::new(remote_target.clone()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid fetched Git publication target for re-anchor: {error}"
                    ))
                })?;
                self.block_on_processkit(git.reset_hard(&self.root, &target))?;
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmark = BookmarkName::new(base.to_string()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid JJ publication bookmark for re-anchor: {error}"
                    ))
                })?;
                let target = RevsetExpr::new(remote_target.clone()).map_err(|error| {
                    VcsError::Runtime(format!(
                        "invalid fetched JJ publication target for re-anchor: {error}"
                    ))
                })?;
                self.block_on_processkit(jj.bookmark_move(
                    &self.root,
                    BookmarkMove::new(bookmark, target.clone()).allow_backwards(),
                ))?;
                // The primary workspace must remain a clean child of the bookmark. Editing the
                // bookmark itself would make a subsequent task/integration operation rewrite a
                // published remote revision.
                self.block_on_processkit(jj.new_child(&self.root, &target))?;
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot re-anchor publication for an unsupported VCS backend".into(),
                ));
            }
        }
        self.require_primary_reanchored_to(base, &remote_target)?;
        self.remove_integration_workspace(work, batch_id)?;
        Ok(PublicationReanchorOutcome::Reanchored)
    }

    /// Recreate an unpublished integration above a primary branch that moved locally before the
    /// processor could fast-forward it. Unlike [`Self::reanchor_after_remote_rejection`], this
    /// path must never fetch-and-reset to `origin`: the locally observed primary target is the
    /// authority and may be an intentional operator/CI commit that has not been pushed yet.
    pub fn reanchor_after_local_divergence(
        &self,
        work: impl AsRef<Path>,
        batch_id: &str,
        base: &str,
        expected_integration_head: &str,
    ) -> Result<PublicationReanchorOutcome> {
        validate_batch_id(batch_id)?;
        validate_ref("publication branch", base)?;
        validate_ref("expected integration head", expected_integration_head)?;
        let integration_branch = format!("integration/{batch_id}");
        let repo = self.repo()?;
        let branch_exists = self.block_on(repo.branch_exists(&integration_branch))?;
        if !branch_exists {
            // A retry after the prior process removed the integration surface. The local primary
            // remains authoritative, but it must still be a clean, exact base checkout/bookmark
            // before the ordinary merger starts over it.
            self.require_clean_local_primary(base)?;
            return Ok(PublicationReanchorOutcome::Reanchored);
        }

        let actual_integration = self.branch_tip(&repo, &integration_branch)?;
        if actual_integration != expected_integration_head {
            return Err(VcsError::Runtime(format!(
                "refusing local publication re-anchor: {integration_branch} tip {actual_integration:?} differs from checkpointed integration tip {expected_integration_head:?}"
            )));
        }
        let primary_target = self.require_clean_local_primary(base)?;
        if self.branch_is_ancestor_of(base, &integration_branch)?
            || self.branch_is_ancestor_of(&integration_branch, base)?
        {
            return Err(VcsError::Runtime(format!(
                "refusing local publication re-anchor: {base} and {integration_branch} are not a proven divergent pair"
            )));
        }
        self.remove_integration_workspace(work, batch_id)?;
        // A writer may race this operation. Never silently start over a different primary tip;
        // another Phase-0 observation can make that later decision explicitly.
        self.require_primary_reanchored_to(base, &primary_target)?;
        Ok(PublicationReanchorOutcome::Reanchored)
    }

    /// Commit an exact CI repair directly on the already-published primary branch and, when the
    /// run is push-enabled, push that same branch before returning its SHA. CI repair is the
    /// deliberate post-publication exception to integration-worktree-only writes: the required
    /// checks must observe a real published successor, never an unpushed integration tip.
    pub fn commit_published_ci_fix(
        &self,
        base: &str,
        published_head: &str,
        paths: &[PathBuf],
        message: &str,
        push: bool,
    ) -> Result<String> {
        validate_ref("publication branch", base)?;
        validate_ref("published head", published_head)?;
        let repo = self.repo()?;
        let before = self.block_on(repo.snapshot())?;
        let on_expected_published_tip = match self.backend {
            BackendKind::Git => {
                before.branch.as_deref() == Some(base)
                    && before.head.as_deref() == Some(published_head)
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                let parent_revision =
                    RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                        VcsError::Runtime(format!("invalid JJ primary parent revision: {error}"))
                    })?;
                let parent = self
                    .block_on_processkit(jj.template_query_ignoring_working_copy(
                        &self.root,
                        &parent_revision,
                        "commit_id",
                        Some(1),
                    ))?
                    .trim()
                    .to_string();
                jj_bookmark_target(&bookmarks, base)? == published_head && parent == published_head
            }
            _ => false,
        };
        if !on_expected_published_tip {
            return Err(VcsError::Runtime(format!(
                "primary checkout is not rooted at expected published {base}={published_head} for CI repair (branch={:?}, head={:?})",
                before.branch, before.head
            )));
        }
        if before.conflicted {
            return Err(VcsError::Runtime(
                "refusing CI repair from a conflicted primary checkout".into(),
            ));
        }
        let head = match self.backend {
            BackendKind::Git => self.commit_paths_at(&self.root, base, paths, message)?,
            BackendKind::Jj => {
                // `jj commit` leaves a fresh empty `@` child and does not advance a bookmark
                // that names its parent. Commit the exact validated paths first, then advance
                // the publication bookmark to that committed parent with JJ's no-backwards
                // proof. The returned SHA is the bookmark target, not the empty successor.
                self.commit_paths_at(&self.root, base, paths, message)?;
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmark = BookmarkName::new(base.to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid publication bookmark: {error}"))
                })?;
                let committed_parent = RevsetExpr::new("@-".to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid JJ committed CI repair revision: {error}"))
                })?;
                self.block_on_processkit(
                    jj.bookmark_move(&self.root, BookmarkMove::new(bookmark, committed_parent)),
                )?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                jj_bookmark_target(&bookmarks, base)?
            }
            _ => return Err(VcsError::Runtime("unsupported VCS backend".into())),
        };
        if push {
            self.block_on(repo.push(base))?;
        }
        let after = self.block_on(repo.snapshot_readonly())?;
        let clean_after = match self.backend {
            BackendKind::Git => after.head.as_deref() == Some(head.as_str()),
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                let parent_revision =
                    RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                        VcsError::Runtime(format!("invalid JJ primary parent revision: {error}"))
                    })?;
                let parent = self
                    .block_on_processkit(jj.template_query_ignoring_working_copy(
                        &self.root,
                        &parent_revision,
                        "commit_id",
                        Some(1),
                    ))?
                    .trim()
                    .to_string();
                jj_bookmark_target(&bookmarks, base)? == head && parent == head
            }
            _ => false,
        };
        if !clean_after || after.dirty || after.conflicted {
            return Err(VcsError::Runtime(format!(
                "primary checkout does not cleanly name committed CI repair {head}"
            )));
        }
        Ok(head)
    }

    /// Remove a terminal task workspace and its task branch/bookmark.  The function refuses an
    /// unexpected VCS registration, but is idempotent after a completed cleanup.
    pub fn remove_task_workspace(&self, work: impl AsRef<Path>, task_id: &str) -> Result<()> {
        validate_task_id(task_id)?;
        let path = managed_task_path(&self.root, work.as_ref(), task_id)?;
        let branch = format!("task/{task_id}");
        let repo = self.repo()?;
        let existing = self.block_on(repo.list_worktrees())?;
        if let Some(found) = existing.iter().find(|entry| same_path(&entry.path, &path)) {
            if !self.workspace_registration_matches(found.branch.as_deref(), &path, &branch)? {
                return Err(VcsError::ManagedPath(format!(
                    "refusing to remove {} registered to {:?}, not {branch}",
                    path.display(),
                    found.branch
                )));
            }
            self.block_on(repo.remove_worktree(WorktreeRemove::new(&path).force()))?;
        } else if managed_directory_present(&path)? {
            return Err(VcsError::ManagedPath(format!(
                "refusing to remove unregistered managed worktree path {}",
                path.display()
            )));
        }

        if self.block_on(repo.branch_exists(&branch))? {
            self.block_on(repo.delete_branch(BranchDelete::new(&branch).force()))?;
        }
        Ok(())
    }

    /// Remove the exact singleton integration workspace and its integration branch/bookmark.
    /// Like task cleanup it refuses an arbitrary directory or mismatched registration.
    pub fn remove_integration_workspace(
        &self,
        work: impl AsRef<Path>,
        batch_id: &str,
    ) -> Result<()> {
        validate_batch_id(batch_id)?;
        let path = managed_integration_path(&self.root, work.as_ref())?;
        let branch = format!("integration/{batch_id}");
        let repo = self.repo()?;
        let existing = self.block_on(repo.list_worktrees())?;
        if let Some(found) = existing.iter().find(|entry| same_path(&entry.path, &path)) {
            if !self.workspace_registration_matches(found.branch.as_deref(), &path, &branch)? {
                return Err(VcsError::ManagedPath(format!(
                    "refusing to remove integration workspace {} registered to {:?}, not {branch}",
                    path.display(),
                    found.branch
                )));
            }
            self.block_on(repo.remove_worktree(WorktreeRemove::new(&path).force()))?;
        } else if managed_directory_present(&path)? {
            return Err(VcsError::ManagedPath(format!(
                "refusing to remove unregistered integration workspace path {}",
                path.display()
            )));
        }
        if self.block_on(repo.branch_exists(&branch))? {
            self.block_on(repo.delete_branch(BranchDelete::new(&branch).force()))?;
        }
        Ok(())
    }

    /// Resolve the exact freshly fetched `origin/<base>` target.  Git's typed fetch creates the
    /// normal remote-tracking ref; JJ exposes the corresponding remote bookmark explicitly.
    fn fetched_remote_publication_target(&self, repo: &Repo, base: &str) -> Result<String> {
        match self.backend {
            BackendKind::Git => self.branch_tip(repo, &format!("origin/{base}")),
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks = self.block_on_processkit(jj.bookmarks_all(&self.root))?;
                jj_remote_bookmark_target(&bookmarks, base, "origin")
            }
            _ => Err(VcsError::Runtime(
                "cannot resolve a remote publication target for an unsupported VCS backend".into(),
            )),
        }
    }

    /// Read the current local primary only when the checked-out workspace is clean and names the
    /// exact primary branch/bookmark shape the merger will later use. The returned revision is
    /// intentionally a local authority: callers use it to prove that a local-divergence
    /// re-anchor never discarded an external primary advance.
    fn require_clean_local_primary(&self, base: &str) -> Result<String> {
        let repo = self.repo()?;
        let target = self.branch_tip(&repo, base)?;
        let snapshot = self.block_on(repo.snapshot_readonly())?;
        if snapshot.dirty || snapshot.conflicted {
            return Err(VcsError::Runtime(
                "local publication re-anchor requires a clean primary checkout".into(),
            ));
        }
        match self.backend {
            BackendKind::Git => {
                if snapshot.branch.as_deref() != Some(base)
                    || snapshot.head.as_deref() != Some(&target)
                {
                    return Err(VcsError::Runtime(format!(
                        "local publication re-anchor primary is not exact {base}={target} (branch={:?}, head={:?})",
                        snapshot.branch, snapshot.head
                    )));
                }
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let parent_revision =
                    RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                        VcsError::Runtime(format!(
                            "invalid JJ primary parent revision for local re-anchor: {error}"
                        ))
                    })?;
                let parent = self
                    .block_on_processkit(jj.template_query_ignoring_working_copy(
                        &self.root,
                        &parent_revision,
                        "commit_id",
                        Some(1),
                    ))?
                    .trim()
                    .to_string();
                if parent != target {
                    return Err(VcsError::Runtime(format!(
                        "local publication re-anchor primary is not a clean child of {base}={target} (parent={parent:?})"
                    )));
                }
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot prove local primary for an unsupported VCS backend".into(),
                ));
            }
        }
        Ok(target)
    }

    /// Prove that the primary checkout names the fetched remote target after re-anchor.  The
    /// proof is intentionally stronger than an ancestry check: a later retry must not accept a
    /// locally advanced primary ref as though the destructive reset had completed.
    fn require_primary_reanchored_to(&self, base: &str, remote_target: &str) -> Result<()> {
        let repo = self.repo()?;
        let snapshot = self.block_on(repo.snapshot_readonly())?;
        if snapshot.dirty || snapshot.conflicted {
            return Err(VcsError::Runtime(
                "publication re-anchor left the primary checkout dirty or conflicted".into(),
            ));
        }
        match self.backend {
            BackendKind::Git => {
                if snapshot.branch.as_deref() != Some(base)
                    || snapshot.head.as_deref() != Some(remote_target)
                {
                    return Err(VcsError::Runtime(format!(
                        "publication re-anchor did not leave Git primary at {base}={remote_target} (branch={:?}, head={:?})",
                        snapshot.branch, snapshot.head
                    )));
                }
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                let local_target = jj_bookmark_target(&bookmarks, base)?;
                let parent_revision =
                    RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                        VcsError::Runtime(format!(
                            "invalid JJ primary parent revision for re-anchor: {error}"
                        ))
                    })?;
                let parent = self
                    .block_on_processkit(jj.template_query_ignoring_working_copy(
                        &self.root,
                        &parent_revision,
                        "commit_id",
                        Some(1),
                    ))?
                    .trim()
                    .to_string();
                if local_target != remote_target || parent != remote_target {
                    return Err(VcsError::Runtime(format!(
                        "publication re-anchor did not leave JJ primary as a clean child of {base}={remote_target} (bookmark={local_target:?}, parent={parent:?})"
                    )));
                }
            }
            _ => {
                return Err(VcsError::Runtime(
                    "cannot verify primary re-anchor for an unsupported VCS backend".into(),
                ));
            }
        }
        Ok(())
    }

    fn branch_tip(&self, repo: &Repo, reference: &str) -> Result<String> {
        validate_ref("branch reference", reference)?;
        match self.backend {
            BackendKind::Git => {
                let commits = self.block_on(repo.log(reference, 1))?;
                commits
                    .first()
                    .map(|commit| commit.id.clone())
                    .filter(|tip| !tip.trim().is_empty())
                    .ok_or_else(|| {
                        VcsError::Runtime(format!("reference {reference:?} has no readable tip"))
                    })
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(&self.root))?;
                jj_bookmark_target(&bookmarks, reference)
            }
            _ => Err(VcsError::Runtime(
                "cannot resolve a branch tip for an unsupported VCS backend".into(),
            )),
        }
    }

    fn repo(&self) -> Result<Repo> {
        Ok(Repo::discover(&self.root)?)
    }

    fn block_on<T>(
        &self,
        future: impl std::future::Future<Output = vcs_core::Result<T>>,
    ) -> Result<T> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| VcsError::Runtime(format!("create VCS runtime: {error}")))?;
        Ok(runtime.block_on(future)?)
    }

    fn block_on_processkit<T>(
        &self,
        future: impl std::future::Future<Output = std::result::Result<T, processkit::Error>>,
    ) -> Result<T> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| VcsError::Runtime(format!("create VCS runtime: {error}")))?;
        runtime.block_on(future).map_err(VcsError::ProcessKit)
    }

    /// Await a bounded fan-out already owned by a published VCS client. Unlike
    /// [`Self::block_on_processkit`], the client reports one typed result per requested
    /// workspace rather than one aggregate `Result`; callers inspect every element explicitly.
    fn block_on_value<T>(&self, future: impl std::future::Future<Output = T>) -> Result<T> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| VcsError::Runtime(format!("create VCS runtime: {error}")))?;
        Ok(runtime.block_on(future))
    }

    fn require_workspace(&self, workspace: &TaskWorkspace) -> Result<()> {
        validate_task_id(&workspace.task_id)?;
        if workspace.backend != self.backend
            || workspace.branch != format!("task/{}", workspace.task_id)
        {
            return Err(VcsError::ManagedPath(format!(
                "workspace metadata for {} does not match this repository",
                workspace.path.display()
            )));
        }
        let expected = managed_task_path(&self.root, &workspace.work, &workspace.task_id)?;
        if !same_path(&expected, &workspace.path) {
            return Err(VcsError::ManagedPath(format!(
                "workspace {} is outside the managed task path {}",
                workspace.path.display(),
                expected.display()
            )));
        }
        Ok(())
    }

    fn require_integration_workspace(&self, workspace: &IntegrationWorkspace) -> Result<()> {
        validate_batch_id(&workspace.batch_id)?;
        if workspace.backend != self.backend
            || workspace.branch != format!("integration/{}", workspace.batch_id)
        {
            return Err(VcsError::ManagedPath(format!(
                "integration workspace metadata for {} does not match this repository",
                workspace.path.display()
            )));
        }
        let expected = managed_integration_path(&self.root, &workspace.work)?;
        if !same_path(&expected, &workspace.path) {
            return Err(VcsError::ManagedPath(format!(
                "integration workspace {} is outside the managed path {}",
                workspace.path.display(),
                expected.display()
            )));
        }
        Ok(())
    }

    fn commit_paths_at(
        &self,
        path: &Path,
        branch: &str,
        paths: &[PathBuf],
        message: &str,
    ) -> Result<String> {
        validate_commit_message(message)?;
        validate_ref("workspace branch", branch)?;
        let repo = self.repo()?.at(path);
        if paths.is_empty() {
            return Err(VcsError::InvalidInput(
                "workspace has no reported changed paths to commit".into(),
            ));
        }
        // `Repo::changed_files()` is a diff surface and therefore cannot include a brand-new
        // Git file until it has already been staged. Exact evidence must be checked *before*
        // this boundary stages it, so Git uses its typed porcelain status (which includes `??`)
        // while JJ retains the common facade's all-working-copy change model.
        let (changed, untracked_roots): (BTreeSet<PathBuf>, Vec<PathBuf>) =
            if self.backend == BackendKind::Git {
                let git = repo.git_at().ok_or_else(|| {
                    VcsError::Runtime("Git backend did not expose its typed Git client".into())
                })?;
                let status = self.block_on_processkit(git.status())?;
                let untracked_roots = status
                    .iter()
                    .filter(|change| change.code == "??")
                    .map(|change| change.path.clone())
                    .collect();
                (
                    status.into_iter().map(|change| change.path).collect(),
                    untracked_roots,
                )
            } else {
                (
                    self.block_on(repo.changed_files())?
                        .into_iter()
                        .map(|change| change.path)
                        .collect(),
                    Vec::new(),
                )
            };
        let mut vetted = std::collections::BTreeSet::new();
        for report_path in paths {
            validate_workspace_relative_path(report_path)?;
            let is_untracked_descendant = untracked_roots.iter().any(|root| {
                report_path.starts_with(root)
                    && fs::symlink_metadata(path.join(report_path))
                        .is_ok_and(|metadata| !metadata.file_type().is_dir())
            });
            if !changed.contains(report_path) && !is_untracked_descendant {
                return Err(VcsError::InvalidInput(format!(
                    "reported commit path {} is not a current changed path",
                    report_path.display()
                )));
            }
            if !vetted.insert(report_path.clone()) {
                return Err(VcsError::InvalidInput(format!(
                    "reported commit path {} appears more than once",
                    report_path.display()
                )));
            }
        }
        let vetted: Vec<PathBuf> = vetted.into_iter().collect();
        if self.backend == BackendKind::Git {
            let git = repo.git_at().ok_or_else(|| {
                VcsError::Runtime("Git backend did not expose its typed Git client".into())
            })?;
            self.block_on_processkit(git.add(&vetted))?;
        }
        self.block_on(repo.commit_paths(&vetted, message))?;
        if self.backend == BackendKind::Jj {
            let jj = repo.jj().ok_or_else(|| {
                VcsError::Runtime("JJ backend did not expose its typed JJ client".into())
            })?;
            // `jj commit` finalises the current change and starts a new, empty working-copy
            // change. Its implicit bookmark-following differs between a task's ordinary child
            // and an integration child following an explicit merge, so never infer that the
            // managed bookmark advanced. Prove the committed parent of the successor and move
            // exactly this bookmark to it when necessary.
            let committed = RevsetExpr::new("parents(@)").map_err(|error| {
                VcsError::Runtime(format!(
                    "cannot construct committed JJ parent revision: {error}"
                ))
            })?;
            let committed_target = self
                .block_on_processkit(jj.template_query_ignoring_working_copy(
                    path,
                    &committed,
                    "commit_id",
                    Some(1),
                ))?
                .trim()
                .to_string();
            if committed_target.is_empty() {
                return Err(VcsError::Runtime(
                    "JJ commit left no durable parent for the managed bookmark".into(),
                ));
            }
            let bookmarks = self.block_on_processkit(jj.bookmarks_ignoring_working_copy(path))?;
            if jj_bookmark_target(&bookmarks, branch)? != committed_target {
                let bookmark = BookmarkName::new(branch.to_owned()).map_err(|error| {
                    VcsError::Runtime(format!("invalid managed JJ bookmark for commit: {error}"))
                })?;
                self.block_on_processkit(
                    jj.bookmark_move(path, BookmarkMove::new(bookmark, committed.clone())),
                )?;
            }
            let after = self.block_on_processkit(jj.bookmarks_ignoring_working_copy(path))?;
            return jj_bookmark_target(&after, branch);
        }
        self.block_on(repo.snapshot())?
            .head
            .ok_or_else(|| VcsError::Runtime("VCS did not report a post-commit head".into()))
    }

    fn workspace_branch_tip(&self, path: &Path, branch: &str) -> Result<String> {
        validate_ref("workspace branch", branch)?;
        let repo = self.repo()?.at(path);
        // This is an execution boundary, not a passive UI refresh. JJ's read-only snapshot
        // deliberately ignores a bare working-tree write, so use the live snapshot here and
        // reject it before trusting a durable bookmark.
        let snapshot = self.block_on(repo.snapshot())?;
        if snapshot.dirty || snapshot.conflicted {
            return Err(VcsError::Runtime(format!(
                "workspace {path:?} is not clean before proving durable branch {branch} (dirty={}, conflicted={})",
                snapshot.dirty, snapshot.conflicted
            )));
        }
        match self.backend {
            BackendKind::Git => {
                if snapshot.branch.as_deref() != Some(branch) {
                    return Err(VcsError::Runtime(format!(
                        "Git workspace {path:?} is not checked out on {branch} (branch={:?})",
                        snapshot.branch
                    )));
                }
                snapshot.head.ok_or_else(|| {
                    VcsError::Runtime(format!(
                        "Git workspace {path:?} has no readable {branch} tip"
                    ))
                })
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(path))?;
                jj_bookmark_target(&bookmarks, branch)
            }
            _ => Err(VcsError::Runtime("unsupported VCS backend".into())),
        }
    }

    fn workspace_branch_tip_allowing_in_progress_merge(
        &self,
        path: &Path,
        branch: &str,
    ) -> Result<String> {
        validate_ref("workspace branch", branch)?;
        let repo = self.repo()?.at(path);
        let snapshot = self.block_on(repo.snapshot())?;
        match self.backend {
            BackendKind::Git => {
                if snapshot.branch.as_deref() != Some(branch) {
                    return Err(VcsError::Runtime(format!(
                        "Git workspace {path:?} is not checked out on {branch} during merge resolution (branch={:?})",
                        snapshot.branch
                    )));
                }
                snapshot.head.ok_or_else(|| {
                    VcsError::Runtime(format!(
                        "Git workspace {path:?} has no readable {branch} tip during merge resolution"
                    ))
                })
            }
            BackendKind::Jj => {
                let jj = repo.jj().ok_or_else(|| {
                    VcsError::Runtime("JJ repository has no typed JJ client".into())
                })?;
                let bookmarks =
                    self.block_on_processkit(jj.bookmarks_ignoring_working_copy(path))?;
                jj_bookmark_target(&bookmarks, branch)
            }
            _ => Err(VcsError::Runtime("unsupported VCS backend".into())),
        }
    }

    /// Confirm that a registered workspace belongs to the expected durable branch/bookmark. Git
    /// worktrees expose that directly; JJ needs the explicit direct-child proof below after a
    /// commit has advanced `@` past its bookmark.
    fn workspace_registration_matches(
        &self,
        observed_branch: Option<&str>,
        path: &Path,
        branch: &str,
    ) -> Result<bool> {
        Ok(observed_branch == Some(branch)
            || (self.backend == BackendKind::Jj
                && self.jj_workspace_matches_bookmark(path, branch)?))
    }

    /// `jj commit` deliberately advances the workspace to an empty child while retaining the
    /// managed bookmark on its parent. `vcs-core` therefore reports that workspace with no
    /// nearest branch even though it remains the legitimate task/integration checkout. Accept
    /// that exact relationship, but not a merely co-located arbitrary JJ workspace: the current
    /// revision must be the bookmark target itself or its direct child.
    fn jj_workspace_matches_bookmark(&self, path: &Path, branch: &str) -> Result<bool> {
        debug_assert_eq!(self.backend, BackendKind::Jj);
        let repo = self.repo()?.at(path);
        let jj = repo.jj().ok_or_else(|| {
            VcsError::Runtime("JJ backend did not expose its typed client".into())
        })?;
        let bookmarks = self.block_on_processkit(jj.bookmarks_ignoring_working_copy(path))?;
        let target = jj_bookmark_target(&bookmarks, branch)?;
        let current = self
            .block_on_processkit(jj.template_query(
                path,
                &RevsetExpr::new("@".to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid JJ current revision: {error}"))
                })?,
                "commit_id",
                Some(1),
            ))?
            .trim()
            .to_string();
        if current == target {
            return Ok(true);
        }
        let parent = self
            .block_on_processkit(jj.template_query(
                path,
                &RevsetExpr::new("parents(@)".to_string()).map_err(|error| {
                    VcsError::Runtime(format!("invalid JJ parent revision: {error}"))
                })?,
                "commit_id",
                Some(1),
            ))?
            .trim()
            .to_string();
        Ok(parent == target)
    }
}

/// Read one live local JJ bookmark target from the typed, non-snapshotting listing. An empty
/// target is a conflicted bookmark and cannot be a publication boundary.
fn jj_bookmark_target(bookmarks: &[vcs_jj::Bookmark], name: &str) -> Result<String> {
    let Some(bookmark) = bookmarks.iter().find(|bookmark| bookmark.name == name) else {
        return Err(VcsError::Runtime(format!(
            "JJ bookmark {name:?} is absent; refusing publication"
        )));
    };
    if bookmark.target.trim().is_empty() {
        return Err(VcsError::Runtime(format!(
            "JJ bookmark {name:?} has no single target; refusing publication"
        )));
    }
    Ok(bookmark.target.clone())
}

/// Read one exact remote-tracking JJ bookmark target after a typed fetch.  A remote target is
/// intentionally not inferred from a local bookmark with the same name: that local bookmark is
/// precisely the one a failed publication may have advanced before push rejection.
fn jj_remote_bookmark_target(
    bookmarks: &[vcs_jj::BookmarkRef],
    name: &str,
    remote: &str,
) -> Result<String> {
    let Some(bookmark) = bookmarks
        .iter()
        .find(|bookmark| bookmark.name == name && bookmark.remote.as_deref() == Some(remote))
    else {
        return Err(VcsError::Runtime(format!(
            "JJ remote bookmark {name:?}@{remote} is absent after fetch"
        )));
    };
    if bookmark.target.trim().is_empty() {
        return Err(VcsError::Runtime(format!(
            "JJ remote bookmark {name:?}@{remote} has no single target"
        )));
    }
    Ok(bookmark.target.clone())
}

fn managed_work_root(path: &Path) -> Result<PathBuf> {
    let Some(worktrees) = path.parent() else {
        return Err(VcsError::ManagedPath(format!(
            "managed task path {} has no worktrees parent",
            path.display()
        )));
    };
    let Some(work) = worktrees.parent() else {
        return Err(VcsError::ManagedPath(format!(
            "managed task path {} has no work root",
            path.display()
        )));
    };
    Ok(work.to_path_buf())
}

fn validate_task_id(task_id: &str) -> Result<()> {
    reject_flag_like("orchestrail-engine", "task id", task_id)
        .map_err(|error| VcsError::InvalidInput(error.to_string()))?;
    if is_task_id(task_id) {
        Ok(())
    } else {
        Err(VcsError::InvalidInput(format!(
            "invalid task id {task_id:?}; expected T- followed by ASCII digits"
        )))
    }
}

fn validate_batch_id(batch_id: &str) -> Result<()> {
    reject_flag_like("orchestrail-engine", "batch id", batch_id)
        .map_err(|error| VcsError::InvalidInput(error.to_string()))?;
    if batch_id
        .strip_prefix("B-")
        .is_some_and(|suffix| !suffix.is_empty() && !suffix.chars().any(char::is_whitespace))
    {
        Ok(())
    } else {
        Err(VcsError::InvalidInput(format!(
            "invalid batch id {batch_id:?}; expected B- followed by a non-empty token"
        )))
    }
}

fn validate_ref(what: &str, value: &str) -> Result<()> {
    reject_flag_like("orchestrail-engine", what, value)
        .map_err(|error| VcsError::InvalidInput(error.to_string()))?;
    if value.contains('\0') {
        return Err(VcsError::InvalidInput(format!(
            "{what} contains a NUL byte"
        )));
    }
    Ok(())
}

fn validate_release_text(what: &str, value: &str, maximum: usize) -> Result<()> {
    validate_ref(what, value)?;
    if value.trim().is_empty()
        || value != value.trim()
        || value.encode_utf16().count() > maximum
        || value.chars().any(char::is_control)
    {
        return Err(VcsError::InvalidInput(format!(
            "{what} must be trimmed single-line text of at most {maximum} characters"
        )));
    }
    Ok(())
}

fn validate_commit_message(message: &str) -> Result<()> {
    if message.trim().is_empty() || message.contains('\0') {
        return Err(VcsError::InvalidInput(
            "commit message must be non-empty and contain no NUL byte".into(),
        ));
    }
    Ok(())
}

fn validate_workspace_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(VcsError::InvalidInput(format!(
            "reported commit path {} must be a non-empty relative path",
            path.display()
        )));
    }
    for component in path.components() {
        match component {
            Component::Normal(part) if part != ".git" && part != ".jj" => {}
            Component::Normal(_) => {
                return Err(VcsError::InvalidInput(format!(
                    "reported commit path {} may not address VCS metadata",
                    path.display()
                )));
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(VcsError::InvalidInput(format!(
                    "reported commit path {} must not contain traversal or a prefix",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn manifest_path(path: &Path) -> Result<String> {
    validate_workspace_relative_path(path)?;
    path.to_str().map(str::to_owned).ok_or_else(|| {
        VcsError::Runtime(format!(
            "typed VCS diff reported a non-Unicode path that cannot be persisted in an approval manifest: {}",
            path.display()
        ))
    })
}

const MAX_WORK_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024;

/// Map one confined-filesystem failure onto this module's error contract.
///
/// [`work_fs`] reports every confinement, limit, and encoding violation as `InvalidData` or
/// `InvalidInput`. Those describe a managed path that may not be trusted rather than a transport
/// failure, so recovery must keep seeing them as [`VcsError::ManagedPath`] exactly as this
/// module's own former copies of those checks reported them.
fn managed_path_error(error: std::io::Error) -> VcsError {
    match error.kind() {
        std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput => {
            VcsError::ManagedPath(error.to_string())
        }
        _ => VcsError::Io(error),
    }
}

/// Read a bounded UTF-8 artifact below `.work` without following a replaced file or parent.
/// Recovery inputs are authority-bearing state, so ordinary `read_to_string` is not sufficient;
/// [`work_fs::read_optional_text`] is the single confined reader shared with the control plane. An
/// absent artifact — or an absent parent chain — stays `None` so recovery treats a not-yet-written
/// merge report as missing rather than as a broken control plane.
fn read_plain_work_artifact(work: &Path, path: &Path) -> Result<Option<String>> {
    work_fs::read_optional_text(work, path, MAX_WORK_ARTIFACT_BYTES).map_err(managed_path_error)
}

fn update_manifest_field(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn fingerprint_merge_path(workspace: &Path, path: &Path) -> Result<Option<String>> {
    validate_workspace_relative_path(path)?;
    let full_path = workspace.join(path);
    let metadata = match fs::symlink_metadata(&full_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(VcsError::Io(error)),
    };
    if work_fs::redirected(&metadata) || !metadata.is_file() {
        return Err(VcsError::Runtime(format!(
            "merge path {} must be a regular file or be absent",
            path.display()
        )));
    }
    let bytes = work_fs::read_required_bytes(workspace, &full_path, MAX_FINGERPRINT_BYTES)?;
    Ok(Some(format!("{:x}", Sha256::digest(bytes))))
}

fn verify_merge_path_fingerprints(
    workspace: &Path,
    merge_paths: &[PathBuf],
    conflict_paths: &[PathBuf],
    fingerprints: &[MergePathFingerprint],
) -> Result<()> {
    let expected = merge_paths
        .iter()
        .filter(|path| !conflict_paths.contains(path))
        .collect::<BTreeSet<_>>();
    let actual = fingerprints
        .iter()
        .map(|fingerprint| &fingerprint.path)
        .collect::<BTreeSet<_>>();
    if actual != expected || actual.len() != fingerprints.len() {
        return Err(VcsError::InvalidInput(
            "merge-path fingerprints do not cover exactly the non-conflicting typed merge surface"
                .into(),
        ));
    }
    for fingerprint in fingerprints {
        let current = fingerprint_merge_path(workspace, &fingerprint.path)?;
        if current != fingerprint.sha256 {
            return Err(VcsError::Runtime(format!(
                "resolved merge changed protected clean path {}",
                fingerprint.path.display()
            )));
        }
    }
    Ok(())
}

/// Match `vcs-core`'s stable JJ workspace-name derivation when a durable bookmark already
/// exists and only its physical workspace must be restored. The published common facade exposes
/// creation of a *new* bookmark but not this attach-to-existing-bookmark operation; the direct
/// `vcs-jj` call above keeps the same deterministic name and retains a collision-resistant
/// identity without falling back to a shell command.
fn workspace_name_for_branch(branch: &str) -> String {
    let normalized: String = branch
        .chars()
        .map(|character| match character {
            '/' | '\\' | '.' | ':' | ' ' | '\t' | '\n' | '\r' => '_',
            other => other,
        })
        .collect();
    let hash = branch
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    format!("{normalized}-{hash:016x}")
}

fn managed_task_path(repo_root: &Path, work: &Path, task_id: &str) -> Result<PathBuf> {
    managed_workspace_path(repo_root, work, task_id)
}

fn managed_integration_path(repo_root: &Path, work: &Path) -> Result<PathBuf> {
    managed_workspace_path(repo_root, work, "_integration")
}

fn managed_workspace_path(repo_root: &Path, work: &Path, name: &str) -> Result<PathBuf> {
    let repo_root = canonical_path(repo_root)?;
    match fs::symlink_metadata(work) {
        Ok(_) => work_fs::require_plain_directory(work)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = work.parent().ok_or_else(|| {
                VcsError::ManagedPath(format!(
                    "managed work root has no parent: {}",
                    work.display()
                ))
            })?;
            if canonical_path(parent)? != repo_root || work.file_name() != Some(OsStr::new(".work"))
            {
                return Err(VcsError::ManagedPath(format!(
                    "refusing to create non-canonical managed work root {}",
                    work.display()
                )));
            }
            fs::create_dir(work)?;
            work_fs::require_plain_directory(work)?;
        }
        Err(error) => return Err(error.into()),
    }
    let work = canonical_path(work)?;
    if !work.starts_with(&repo_root) {
        return Err(VcsError::ManagedPath(format!(
            "work directory {} escapes repository root {}",
            work.display(),
            repo_root.display()
        )));
    }
    let managed_root = work.join("worktrees");
    work_fs::ensure_plain_parent(&work, &managed_root.join(".managed-path-guard"))?;
    work_fs::require_plain_directory(&managed_root)?;
    let managed_root = canonical_path(managed_root)?;
    let path = managed_root.join(name);
    if managed_directory_present(&path)? {
        let resolved = canonical_path(&path)?;
        if !resolved.starts_with(&managed_root) {
            return Err(VcsError::ManagedPath(format!(
                "managed task path {} escapes {}",
                resolved.display(),
                managed_root.display()
            )));
        }
    }
    Ok(path)
}

/// The read-only counterpart of [`managed_workspace_path`]. It validates the existing physical
/// work root and any existing candidate but deliberately never creates `.work/worktrees` while
/// Phase 0 is collecting evidence.
fn observed_managed_workspace_path(repo_root: &Path, work: &Path, name: &str) -> Result<PathBuf> {
    let repo_root = canonical_path(repo_root)?;
    work_fs::require_plain_directory(work)?;
    let work = canonical_path(work)?;
    if !work.starts_with(&repo_root) {
        return Err(VcsError::ManagedPath(format!(
            "work directory {} escapes repository root {}",
            work.display(),
            repo_root.display()
        )));
    }
    let managed_root = work.join("worktrees");
    match fs::symlink_metadata(&managed_root) {
        Ok(_) => work_fs::require_plain_directory(&managed_root)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let path = managed_root.join(name);
    if managed_directory_present(&path)? {
        let resolved_root = canonical_path(&managed_root)?;
        let resolved = canonical_path(&path)?;
        if !resolved.starts_with(&resolved_root) {
            return Err(VcsError::ManagedPath(format!(
                "managed recovery path {} escapes {}",
                resolved.display(),
                resolved_root.display()
            )));
        }
    }
    Ok(path)
}

fn managed_directory_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !work_fs::redirected(&metadata) => Ok(true),
        Ok(_) => Err(VcsError::ManagedPath(format!(
            "managed workspace path is not a plain directory: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (canonical_path(left), canonical_path(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

/// `std::fs::canonicalize` produces a Win32 verbatim `\\?\` path.  It is useful for safe
/// prefix checks, but Git rejects that representation when asked to create a nested worktree.
/// Keep the resolved physical path while converting it back to normal Win32 spelling before it
/// crosses into `vcs-core`; Unix leaves the canonical path untouched.
fn canonical_path(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    #[cfg(windows)]
    {
        let text = canonical.to_string_lossy();
        if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
            return Ok(PathBuf::from(format!(r"\\{rest}")));
        }
        if let Some(rest) = text.strip_prefix(r"\\?\") {
            return Ok(PathBuf::from(rest));
        }
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use vcs_git::{Git, GitPush, RefName};
    use vcs_jj::{GitClone, Jj, JjApi};

    static TEST_REPOSITORY_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    /// A test-owned directory below the platform temporary directory. Its name is generated
    /// before creation and Drop removes only that exact directory.
    struct TestRepository {
        path: PathBuf,
    }

    impl TestRepository {
        fn new() -> Self {
            let sequence = TEST_REPOSITORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "orchestrail-vcs-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create test repository directory");
            Self { path }
        }
    }

    impl Drop for TestRepository {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn git_block_on<T>(
        future: impl std::future::Future<Output = std::result::Result<T, processkit::Error>>,
    ) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create test Git runtime");
        runtime.block_on(future).expect("typed Git operation")
    }

    fn jj_run(jj: &Jj, dir: &Path, args: &[&str]) {
        let args = args
            .iter()
            .map(|arg| (*arg).to_string())
            .collect::<Vec<_>>();
        git_block_on(jj.run_in(dir, &args));
    }

    #[test]
    fn work_artifact_reader_is_bounded_and_does_not_follow_redirects() {
        let repository = TestRepository::new();
        let work = repository.path.join(".work");
        fs::create_dir(&work).unwrap();
        let report = work.join("merge_report.md");
        fs::write(&report, "merged=T-1\n").unwrap();
        assert_eq!(
            read_plain_work_artifact(&work, &report).unwrap(),
            Some("merged=T-1\n".to_owned())
        );

        fs::write(&report, vec![b'x'; (MAX_WORK_ARTIFACT_BYTES + 1) as usize]).unwrap();
        assert!(matches!(
            read_plain_work_artifact(&work, &report),
            Err(VcsError::ManagedPath(message)) if message.contains("exceeds")
        ));

        fs::remove_file(&report).unwrap();
        let external = repository.path.join("external-report.md");
        fs::write(&external, "quarantined=T-2\n").unwrap();
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&external, &report).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&external, &report).is_ok();
        if linked {
            assert!(matches!(
                read_plain_work_artifact(&work, &report),
                Err(VcsError::ManagedPath(message)) if message.contains("plain regular file")
            ));
        }
    }

    #[test]
    fn managed_workspace_creation_refuses_a_dangling_destination_entry() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        let worktrees = repository.path.join(".work/worktrees");
        fs::create_dir_all(&worktrees).unwrap();
        let managed = worktrees.join("T-1");
        let missing = worktrees.join("missing-target");
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(&missing, &managed).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&missing, &managed).is_ok();
        if !linked {
            return;
        }

        let service = VcsService::discover(&repository.path).unwrap();
        assert!(matches!(
            service.ensure_task_workspace(repository.path.join(".work"), "T-1", "main"),
            Err(VcsError::ManagedPath(message))
                if message.contains("managed workspace path is not a plain directory")
        ));
        assert!(
            fs::symlink_metadata(&managed)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn task_ids_are_strict_and_never_flag_like() {
        assert!(validate_task_id("T-1").is_ok());
        assert!(validate_task_id("T-009").is_ok());
        assert!(validate_task_id("P-1").is_err());
        assert!(validate_task_id("T-").is_err());
        assert!(validate_task_id("T-1 --upload-pack=x").is_err());
        assert!(validate_task_id("-T-1").is_err());
    }

    #[test]
    fn commit_message_rejects_empty_and_nul() {
        assert!(validate_commit_message("Implement T-1").is_ok());
        assert!(validate_commit_message(" \n").is_err());
        assert!(validate_commit_message("bad\0message").is_err());
    }

    #[test]
    fn phase_zero_ignore_preflight_is_private_and_idempotent_in_a_git_repository() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&repository.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let work = repository.path.join(".work");
        fs::create_dir(&work).unwrap();

        let service = VcsService::discover(&repository.path).unwrap();
        assert!(service.ensure_control_plane_ignored(&work).unwrap());
        assert!(!service.ensure_control_plane_ignored(&work).unwrap());
        let exclude = fs::read_to_string(repository.path.join(".git/info/exclude")).unwrap();
        assert_eq!(exclude.lines().filter(|line| *line == ".work/").count(), 1);
        assert!(!repository.path.join(".gitignore").exists());

        fs::write(work.join("must-not-enter-vcs.txt"), "control plane\n").unwrap();
        assert!(
            !service.snapshot().unwrap().dirty,
            "the private exclusion must keep a populated control plane out of Git status"
        );
    }

    #[test]
    fn phase_zero_ignore_preflight_refuses_a_redirected_ignore_file() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        let work = repository.path.join(".work");
        fs::create_dir(&work).unwrap();
        let exclude = repository.path.join(".git/info/exclude");
        fs::remove_file(&exclude).unwrap();
        fs::create_dir(&exclude).unwrap();

        let service = VcsService::discover(&repository.path).unwrap();
        assert!(matches!(
            service.ensure_control_plane_ignored(&work),
            Err(VcsError::ManagedPath(message)) if message.contains("non-file ignore path")
        ));
    }

    #[test]
    fn phase_zero_private_exclusion_is_honored_by_colocated_jj_snapshotting() {
        let repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        let work = repository.path.join(".work");
        fs::create_dir(&work).unwrap();

        let service = VcsService::discover(&repository.path).unwrap();
        assert_eq!(service.backend(), BackendKind::Jj);
        assert!(service.ensure_control_plane_ignored(&work).unwrap());
        fs::write(work.join("must-not-enter-jj.txt"), "control plane\n").unwrap();
        assert!(
            git_block_on(jj.status(&repository.path)).is_empty(),
            "colocated JJ must honor Git's private info/exclude during a real snapshot"
        );
        assert!(!repository.path.join(".gitignore").exists());
    }

    #[test]
    fn phase_zero_private_exclusion_keeps_a_pure_jj_working_copy_clean() {
        let repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(
            &jj,
            &repository.path,
            &["git", "init", "--no-colocate", "."],
        );
        let work = repository.path.join(".work");
        fs::create_dir(&work).unwrap();

        let service = VcsService::discover(&repository.path).unwrap();
        assert_eq!(service.backend(), BackendKind::Jj);
        assert!(service.ensure_control_plane_ignored(&work).unwrap());
        fs::write(work.join("must-not-enter-jj.txt"), "control plane\n").unwrap();
        assert!(
            git_block_on(jj.status(&repository.path)).is_empty(),
            "pure JJ must honor its private Git store exclusion during a real snapshot"
        );
        assert!(!repository.path.join(".gitignore").exists());
        let exclude =
            fs::read_to_string(repository.path.join(".jj/repo/store/git/info/exclude")).unwrap();
        assert_eq!(exclude.lines().filter(|line| *line == ".work/").count(), 1);
    }

    #[test]
    fn phase_zero_ignore_preflight_is_a_no_op_for_an_external_control_plane() {
        let repository = TestRepository::new();
        let external = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));

        let service = VcsService::discover(&repository.path).unwrap();
        assert!(
            !service
                .ensure_control_plane_ignored(&external.path)
                .unwrap()
        );
        assert!(!repository.path.join(".gitignore").exists());
        assert!(
            !fs::read_to_string(repository.path.join(".git/info/exclude"))
                .unwrap()
                .lines()
                .any(|line| line.contains("orchestrail-vcs-test"))
        );
    }

    #[test]
    fn release_preflight_refuses_a_residual_managed_workspace() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        let work = repository.path.join(".work");
        fs::create_dir_all(work.join("worktrees/T-404")).unwrap();
        let service = VcsService::discover(&repository.path).unwrap();
        assert!(matches!(
            service.ensure_no_managed_workspaces(&work),
            Err(VcsError::ManagedPath(message)) if message.contains("unfinished managed workspace")
        ));
    }

    #[test]
    fn live_control_plane_cannot_be_anchored_to_a_repository_subdirectory() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        let nested = repository.path.join("nested");
        fs::create_dir(&nested).unwrap();

        let service = VcsService::discover(&nested).unwrap();
        assert!(
            service
                .ensure_selected_repository_root(&repository.path)
                .is_ok()
        );
        assert!(matches!(
            service.ensure_selected_repository_root(&nested),
            Err(VcsError::ManagedPath(message)) if message.contains("not the discovered repository root")
        ));
    }

    #[test]
    fn release_identity_rejects_malformed_text_before_vcs_discovery() {
        assert!(VcsService::validate_release_identity("1.2.3", Some("v1.2.3")).is_ok());
        assert!(VcsService::validate_release_trunk("main").is_ok());
        assert!(VcsService::validate_release_identity("1.2.3", Some("v1.2.3\nnext")).is_err());
        assert!(VcsService::validate_release_identity(" 1.2.3", None).is_err());
        assert!(VcsService::validate_release_trunk(" main").is_err());
    }

    #[test]
    fn publication_remote_probe_uses_typed_origin_config_for_git_and_pure_jj() {
        let git_repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&git_repository.path));
        let git_service = VcsService::discover(&git_repository.path).unwrap();
        assert!(!git_service.publication_remote_configured().unwrap());
        git_block_on(git.config_set(
            &git_repository.path,
            "remote.origin.pushurl",
            "https://example.invalid/orchestrail.git",
        ));
        assert!(git_service.publication_remote_configured().unwrap());

        let jj_repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(
            &jj,
            &jj_repository.path,
            &["git", "init", "--no-colocate", "."],
        );
        let jj_service = VcsService::discover(&jj_repository.path).unwrap();
        assert_eq!(jj_service.backend(), BackendKind::Jj);
        assert!(!jj_service.publication_remote_configured().unwrap());
        jj_run(
            &jj,
            &jj_repository.path,
            &[
                "git",
                "remote",
                "add",
                "origin",
                "https://example.invalid/orchestrail.git",
            ],
        );
        assert!(jj_service.publication_remote_configured().unwrap());
    }

    #[test]
    fn release_sync_fast_forwards_git_and_proves_the_fetched_tag() {
        let remote = TestRepository::new();
        let local = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "user.name", "Release Test"));
        git_block_on(git.config_set(&remote.path, "user.email", "release@example.invalid"));
        fs::write(remote.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&remote.path, "Initial release base"));
        let base = git_block_on(git.current_branch(&remote.path)).expect("initial branch");
        git_block_on(git.clone_repo(
            remote.path.to_str().expect("UTF-8 remote path"),
            &local.path,
            CloneSpec::new().branch(base.clone()),
        ));

        fs::write(remote.path.join("release.txt"), "published\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("release.txt")]));
        git_block_on(git.commit(&remote.path, "Publish 1.2.3"));
        git_block_on(git.tag_create(&remote.path, &RefName::new("v1.2.3").unwrap(), None));
        let release_revision =
            git_block_on(git.resolve_commit(&remote.path, &RevSpec::new("HEAD").unwrap()));
        fs::write(remote.path.join("after.txt"), "after release\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("after.txt")]));
        git_block_on(git.commit(&remote.path, "Post-release trunk change"));
        let expected =
            git_block_on(git.resolve_commit(&remote.path, &RevSpec::new("HEAD").unwrap()));

        let service = VcsService::discover(&local.path).unwrap();
        let synced = service.sync_release_trunk(&base).unwrap();
        assert_ne!(synced.previous, synced.current);
        assert_eq!(synced.current, expected);
        service
            .verify_release_primary(&base, &synced.current)
            .unwrap();
        let tag = service
            .verify_release_tag("1.2.3", None, &synced.current, &base)
            .unwrap();
        assert_eq!(tag.tag, "v1.2.3");
        assert_eq!(tag.revision, release_revision);
        let evidence = service
            .release_notes_range_evidence(&synced.previous, &tag.revision)
            .unwrap();
        assert_eq!(evidence.schema, "orchestrail/release-notes-range@1");
        assert!(evidence.files.iter().any(|file| file.path == "release.txt"));
        assert!(!evidence.files.iter().any(|file| file.path == "after.txt"));
        assert_eq!(
            service
                .release_notes_range_base(&synced.previous, &tag.revision)
                .unwrap(),
            synced.previous
        );
        assert_eq!(
            service
                .release_notes_range_base(&synced.current, &tag.revision)
                .unwrap(),
            tag.revision
        );
        assert!(!service.snapshot().unwrap().dirty);
    }

    #[test]
    fn release_sync_cancellation_after_remote_refresh_preserves_local_head() {
        use std::cell::Cell;

        let remote = TestRepository::new();
        let local = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "user.name", "Release Test"));
        git_block_on(git.config_set(&remote.path, "user.email", "release@example.invalid"));
        fs::write(remote.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&remote.path, "Initial release base"));
        let base = git_block_on(git.current_branch(&remote.path)).expect("initial branch");
        git_block_on(git.clone_repo(
            remote.path.to_str().expect("UTF-8 remote path"),
            &local.path,
            CloneSpec::new().branch(base.clone()),
        ));
        let local_head =
            git_block_on(git.resolve_commit(&local.path, &RevSpec::new("HEAD").unwrap()));
        fs::write(remote.path.join("release.txt"), "published\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("release.txt")]));
        git_block_on(git.commit(&remote.path, "Publish release"));

        let checks = Cell::new(0_u8);
        let service = VcsService::discover(&local.path).unwrap();
        assert!(
            service
                .sync_release_trunk_with_cancellation(&base, || {
                    let current = checks.get();
                    checks.set(current + 1);
                    current >= 1
                })
                .is_err()
        );
        assert_eq!(
            git_block_on(git.resolve_commit(&local.path, &RevSpec::new("HEAD").unwrap())),
            local_head
        );
    }

    #[test]
    fn release_sync_refuses_to_hide_a_local_ahead_commit() {
        let remote = TestRepository::new();
        let local = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "user.name", "Release Test"));
        git_block_on(git.config_set(&remote.path, "user.email", "release@example.invalid"));
        fs::write(remote.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&remote.path, "Initial release base"));
        let base = git_block_on(git.current_branch(&remote.path)).expect("initial branch");
        git_block_on(git.clone_repo(
            remote.path.to_str().expect("UTF-8 remote path"),
            &local.path,
            CloneSpec::new().branch(base.clone()),
        ));
        git_block_on(git.config_set(&local.path, "user.name", "Local Test"));
        git_block_on(git.config_set(&local.path, "user.email", "local@example.invalid"));
        fs::write(local.path.join("unpublished.txt"), "local only\n").unwrap();
        git_block_on(git.add(&local.path, &[PathBuf::from("unpublished.txt")]));
        git_block_on(git.commit(&local.path, "Unpublished local commit"));
        let local_head =
            git_block_on(git.resolve_commit(&local.path, &RevSpec::new("HEAD").unwrap()));

        let service = VcsService::discover(&local.path).unwrap();
        assert!(matches!(
            service.sync_release_trunk(&base),
            Err(VcsError::PublicationLocalDivergence(_))
        ));
        assert_eq!(
            git_block_on(git.resolve_commit(&local.path, &RevSpec::new("HEAD").unwrap())),
            local_head
        );
    }

    #[test]
    fn release_verification_rejects_a_local_only_tag() {
        let remote = TestRepository::new();
        let local = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "user.name", "Release Test"));
        git_block_on(git.config_set(&remote.path, "user.email", "release@example.invalid"));
        fs::write(remote.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&remote.path, "Initial release base"));
        let base = git_block_on(git.current_branch(&remote.path)).expect("initial branch");
        git_block_on(git.clone_repo(
            remote.path.to_str().expect("UTF-8 remote path"),
            &local.path,
            CloneSpec::new().branch(base.clone()),
        ));
        git_block_on(git.tag_create(&local.path, &RefName::new("v9.9.9").unwrap(), None));
        let service = VcsService::discover(&local.path).unwrap();
        let synced = service.sync_release_trunk(&base).unwrap();
        assert!(
            service
                .verify_release_tag("9.9.9", None, &synced.current, &base)
                .is_err()
        );
    }

    #[test]
    fn release_sync_moves_jj_bookmark_and_reanchors_a_clean_child() {
        let remote = TestRepository::new();
        let local = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "user.name", "Release Test"));
        git_block_on(git.config_set(&remote.path, "user.email", "release@example.invalid"));
        fs::write(remote.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&remote.path, "Initial JJ release base"));
        let base = git_block_on(git.current_branch(&remote.path)).expect("initial branch");
        let jj = Jj::new();
        git_block_on(jj.git_clone(
            remote.path.to_str().expect("UTF-8 remote path"),
            &local.path,
            GitClone::colocated(),
        ));

        fs::write(remote.path.join("release.txt"), "published\n").unwrap();
        git_block_on(git.add(&remote.path, &[PathBuf::from("release.txt")]));
        git_block_on(git.commit(&remote.path, "Publish JJ 2.0.0"));
        git_block_on(git.tag_create(&remote.path, &RefName::new("v2.0.0").unwrap(), None));
        let expected =
            git_block_on(git.resolve_commit(&remote.path, &RevSpec::new("HEAD").unwrap()));

        let service = VcsService::discover(&local.path).unwrap();
        assert_eq!(service.backend(), BackendKind::Jj);
        let synced = service.sync_release_trunk(&base).unwrap();
        assert_eq!(synced.current, expected);
        service
            .verify_release_primary(&base, &synced.current)
            .unwrap();
        let evidence = service
            .verify_release_tag("2.0.0", None, &synced.current, &base)
            .unwrap();
        assert_eq!(evidence.revision, expected);
        let published_workspace = service
            .published_primary_workspace(&base, &expected)
            .unwrap();
        assert!(
            same_path(&published_workspace, &local.path),
            "published release workspace must resolve to the local checkout"
        );
        assert!(!service.snapshot().unwrap().dirty);
    }

    #[test]
    fn reported_commit_paths_are_workspace_relative_and_never_metadata_or_traversal() {
        assert!(validate_workspace_relative_path(Path::new("src/lib.rs")).is_ok());
        assert!(validate_workspace_relative_path(Path::new("nested/file.txt")).is_ok());
        assert!(validate_workspace_relative_path(Path::new("")).is_err());
        assert!(validate_workspace_relative_path(Path::new("../outside.txt")).is_err());
        assert!(validate_workspace_relative_path(Path::new("./file.txt")).is_err());
        assert!(validate_workspace_relative_path(Path::new(".git/config")).is_err());
        assert!(validate_workspace_relative_path(Path::new(".jj/repo")).is_err());
    }

    #[test]
    fn restores_a_missing_git_task_worktree_from_its_existing_branch_without_resetting_it() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(
            &repository.path,
            &[PathBuf::from(".gitignore"), PathBuf::from("base.txt")],
        ));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let initial_branch = git_block_on(git.current_branch(&repository.path)).unwrap();
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).unwrap(),
                &RefName::new("main").unwrap(),
            ));
        }

        let service = VcsService::discover(&repository.path).unwrap();
        let work = repository.path.join(".work");
        let task = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        fs::write(task.path.join("implementation.txt"), "durable work\n").unwrap();
        let expected_tip = service
            .commit_workspace_paths(
                &task,
                &[PathBuf::from("implementation.txt")],
                "Implement T-1",
            )
            .unwrap();

        fs::remove_dir_all(&task.path).unwrap();
        let missing = service
            .task_recovery_observation(&work, "T-1", "main")
            .unwrap();
        assert!(missing.branch_exists);
        assert!(!missing.workspace_present);
        assert_eq!(missing.branch_head.as_deref(), Some(expected_tip.as_str()));

        let restored = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        assert!(restored.path.is_dir());
        assert_eq!(service.task_workspace_tip(&restored).unwrap(), expected_tip);
        assert!(!service.snapshot().unwrap().dirty);
    }

    #[test]
    fn typed_range_paths_cover_the_committed_integration_surface() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&repository.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let initial_branch = git_block_on(git.current_branch(&repository.path)).unwrap();
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).unwrap(),
                &RefName::new("main").unwrap(),
            ));
        }
        let base =
            git_block_on(git.resolve_commit(&repository.path, &RevSpec::new("HEAD").unwrap()));
        fs::create_dir_all(repository.path.join("docs")).unwrap();
        fs::write(repository.path.join("docs/guide.md"), "guide\n").unwrap();
        fs::write(repository.path.join("engine.rs"), "fn main() {}\n").unwrap();
        git_block_on(git.add(
            &repository.path,
            &[PathBuf::from("docs/guide.md"), PathBuf::from("engine.rs")],
        ));
        git_block_on(git.commit(&repository.path, "Change docs and code"));
        let head =
            git_block_on(git.resolve_commit(&repository.path, &RevSpec::new("HEAD").unwrap()));

        let service = VcsService::discover(&repository.path).unwrap();
        assert_eq!(
            service.changed_paths_between(&base, &head).unwrap(),
            vec![PathBuf::from("docs/guide.md"), PathBuf::from("engine.rs")]
        );
        let work = repository.path.join(".work");
        fs::create_dir_all(&work).unwrap();
        let integration = service
            .ensure_integration_workspace(&work, "B-approval", "main")
            .unwrap();
        let manifest = service
            .integration_approval_manifest(&integration, &base, &head)
            .unwrap();
        assert_eq!(manifest.schema, "orchestrail/approval-change-manifest@1");
        assert_eq!(manifest.base, base);
        assert_eq!(manifest.head, head);
        assert_eq!(
            manifest
                .changes
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["docs/guide.md", "engine.rs"]
        );
        assert!(manifest.changes.iter().all(|entry| {
            entry.old_path.is_none()
                && entry.diff_sha256.len() == 64
                && entry
                    .diff_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
        }));
        assert_eq!(manifest.fingerprint().len(), 64);
        assert!(matches!(
            service.integration_approval_manifest(&integration, &base, &base),
            Err(VcsError::Runtime(_))
        ));
    }

    #[test]
    fn task_review_evidence_is_content_bound_to_the_clean_durable_range() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(&repository.path, &[PathBuf::from("base.txt")]));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let initial_branch = git_block_on(git.current_branch(&repository.path)).unwrap();
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).unwrap(),
                &RefName::new("main").unwrap(),
            ));
        }
        let base =
            git_block_on(git.resolve_commit(&repository.path, &RevSpec::new("main").unwrap()));
        let service = VcsService::discover(&repository.path).unwrap();
        let task = service
            .ensure_task_workspace(repository.path.join(".work"), "T-1", "main")
            .unwrap();
        fs::create_dir_all(task.path.join("engine")).unwrap();
        fs::write(task.path.join("engine/lib.rs"), "pub fn reviewed() {}\n").unwrap();
        let head = service
            .commit_workspace_paths(&task, &[PathBuf::from("engine/lib.rs")], "Implement T-1")
            .unwrap();

        let evidence = service
            .task_review_range_evidence(&task, &base, &head)
            .unwrap();
        assert_eq!(evidence.schema, "orchestrail/task-review-range@1");
        assert_eq!(evidence.base, base);
        assert_eq!(evidence.head, head);
        assert_eq!(evidence.files.len(), 1);
        assert_eq!(evidence.files[0].path, "engine/lib.rs");
        assert!(evidence.files[0].raw.contains("pub fn reviewed"));
        assert_eq!(evidence.files[0].diff_sha256.len(), 64);
        assert_eq!(evidence.fingerprint().len(), 64);

        fs::write(task.path.join("uncommitted.txt"), "must hold\n").unwrap();
        assert!(
            service
                .task_review_range_evidence(&task, &base, &head)
                .is_err(),
            "a reviewer must not receive a durable range proof from a now-dirty workspace"
        );
    }

    #[test]
    fn jj_task_review_evidence_resolves_the_bookmark_to_an_immutable_commit() {
        let repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "test@example.invalid",
            ],
        );
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        let base = jj_bookmark_target(
            &git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path)),
            "main",
        )
        .unwrap();
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );
        let service = VcsService::discover(&repository.path).unwrap();
        assert_eq!(service.backend(), BackendKind::Jj);
        let task = service
            .ensure_task_workspace(repository.path.join(".work"), "T-1", "main")
            .unwrap();
        fs::write(task.path.join("implementation.txt"), "jj reviewed\n").unwrap();
        let head = service
            .commit_workspace_paths(
                &task,
                &[PathBuf::from("implementation.txt")],
                "Implement T-1",
            )
            .unwrap();

        let evidence = service
            .task_review_range_evidence(&task, "main", &head)
            .unwrap();
        assert_eq!(evidence.base, base);
        assert_eq!(evidence.head, head);
        assert_eq!(evidence.files.len(), 1);
        assert_eq!(evidence.files[0].path, "implementation.txt");
        assert!(evidence.files[0].raw.contains("jj reviewed"));
    }

    #[test]
    fn restores_a_missing_jj_task_workspace_from_its_existing_bookmark_without_moving_it() {
        let repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "test@example.invalid",
            ],
        );
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );

        let service = VcsService::discover(&repository.path).unwrap();
        let work = repository.path.join(".work");
        let task = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        fs::write(task.path.join("implementation.txt"), "durable JJ work\n").unwrap();
        let expected_tip = service
            .commit_workspace_paths(
                &task,
                &[PathBuf::from("implementation.txt")],
                "Implement T-1",
            )
            .unwrap();

        fs::remove_dir_all(&task.path).unwrap();
        let missing = service
            .task_recovery_observation(&work, "T-1", "main")
            .unwrap();
        assert!(missing.branch_exists);
        assert!(!missing.workspace_present);
        assert_eq!(missing.branch_head.as_deref(), Some(expected_tip.as_str()));

        let restored = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        assert!(restored.path.is_dir());
        assert_eq!(service.task_workspace_tip(&restored).unwrap(), expected_tip);
        assert!(!service.snapshot().unwrap().dirty);
    }

    #[test]
    fn actual_git_merge_requires_a_clean_reviewed_task_workspace() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "core.autocrlf", "false"));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join(".gitignore"), ".work/\n").expect("ignore control plane");
        fs::write(repository.path.join("base.txt"), "base\n").expect("write base file");
        git_block_on(git.add(
            &repository.path,
            &[PathBuf::from(".gitignore"), PathBuf::from("base.txt")],
        ));
        git_block_on(git.commit(&repository.path, "Initial base"));

        let initial_branch =
            git_block_on(git.current_branch(&repository.path)).expect("initial branch name");
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).expect("valid initial branch"),
                &RefName::new("main").expect("valid main branch"),
            ));
        }

        let service = VcsService::discover(&repository.path).expect("discover Git repository");
        let work = repository.path.join(".work");
        let task = service
            .ensure_task_workspace(&work, "T-1", "main")
            .expect("create task workspace");
        fs::write(task.path.join("implementation.txt"), "implemented\n")
            .expect("write task implementation");
        fs::write(task.path.join("operator-note.md"), "leave me uncommitted\n")
            .expect("write unrelated edit");
        let task_head = service
            .commit_workspace_paths(
                &task,
                &[PathBuf::from("implementation.txt")],
                "Implement T-1",
            )
            .expect("commit exactly reported task path");

        let integration = service
            .ensure_integration_workspace(&work, "B-20260724T000000Z", "main")
            .expect("create integration workspace");
        assert!(
            service
                .merge_task_into_integration(&integration, &task, &task_head, None)
                .is_err(),
            "merge must refuse an unrelated uncommitted task edit"
        );
        fs::remove_file(task.path.join("operator-note.md"))
            .expect("remove deliberate unrelated task edit");
        assert!(
            service
                .merge_task_into_integration(&integration, &task, "unexpected-task-tip", None)
                .is_err(),
            "merge must reject a task branch whose durable reviewed tip changed"
        );
        assert!(matches!(
            service
                .preflight_task_merge(&integration, "T-1")
                .expect("preflight merge"),
            MergeProbe::Clean
        ));

        let merge_head = service
            .merge_task_into_integration(&integration, &task, &task_head, None)
            .expect("perform typed Git merge");
        assert!(!merge_head.is_empty());
        assert_eq!(
            service
                .merge_task_into_integration(&integration, &task, &task_head, Some(&merge_head),)
                .expect("replay already-integrated Git task"),
            merge_head,
            "legacy merger replay must not create a second Git merge"
        );
        assert_eq!(
            fs::read_to_string(integration.path.join("implementation.txt"))
                .expect("read merged implementation")
                .trim(),
            "implemented"
        );

        let task_status = git_block_on(git.status(&task.path));
        assert!(
            task_status.is_empty(),
            "merged task workspace must remain clean: {task_status:?}"
        );
    }

    #[test]
    fn typed_git_conflict_resolution_is_explicitly_started_then_finalized_or_aborted() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join("shared.txt"), "base\n").unwrap();
        git_block_on(git.add(&repository.path, &[PathBuf::from("shared.txt")]));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let initial_branch = git_block_on(git.current_branch(&repository.path)).unwrap();
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).unwrap(),
                &RefName::new("main").unwrap(),
            ));
        }

        let service = VcsService::discover(&repository.path).unwrap();
        let work = repository.path.join(".work");
        let task = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        fs::write(task.path.join("shared.txt"), "task version\n").unwrap();
        fs::write(task.path.join("clean.txt"), "task clean version\n").unwrap();
        let task_head = service
            .commit_workspace_paths(
                &task,
                &[PathBuf::from("shared.txt"), PathBuf::from("clean.txt")],
                "Implement T-1",
            )
            .unwrap();

        fs::write(repository.path.join("shared.txt"), "main version\n").unwrap();
        git_block_on(git.add(&repository.path, &[PathBuf::from("shared.txt")]));
        git_block_on(git.commit(&repository.path, "Main change"));
        let integration = service
            .ensure_integration_workspace(&work, "B-20260725T120000Z", "main")
            .unwrap();
        let pre_merge_head = service.integration_workspace_tip(&integration).unwrap();
        assert!(matches!(
            service.preflight_task_merge(&integration, "T-1").unwrap(),
            MergeProbe::Conflicts(paths) if paths == vec![PathBuf::from("shared.txt")]
        ));

        let session = service
            .begin_merge_conflict_resolution(&integration, &task, &task_head, &pre_merge_head)
            .unwrap();
        assert_eq!(session.pre_merge_head, pre_merge_head);
        assert_eq!(session.paths, vec![PathBuf::from("shared.txt")]);
        assert_eq!(
            session
                .protected_paths
                .iter()
                .map(|fingerprint| fingerprint.path.clone())
                .collect::<Vec<_>>(),
            vec![PathBuf::from("clean.txt")]
        );
        assert!(session.protected_paths[0].sha256.is_some());
        assert_eq!(
            service
                .integration_workspace_tip_during_merge_resolution(&integration)
                .unwrap(),
            pre_merge_head
        );
        assert!(
            git_block_on(git.conflicted_files(&integration.path))
                .contains(&PathBuf::from("shared.txt"))
        );

        fs::write(integration.path.join("shared.txt"), "resolved version\n").unwrap();
        let protected_contents = fs::read(integration.path.join("clean.txt")).unwrap();
        fs::write(integration.path.join("clean.txt"), "tampered by merger\n").unwrap();
        assert!(matches!(
            service.finalize_merge_conflict_resolution(
                &integration,
                &task,
                MergeResolutionFinalization {
                    task_head: &task_head,
                    pre_merge_head: &session.pre_merge_head,
                    merge_paths: &session.merge_paths,
                    conflict_paths: &session.paths,
                    protected_paths: &session.protected_paths,
                },
            ),
            Err(VcsError::Runtime(message)) if message.contains("protected clean path clean.txt")
        ));
        fs::write(integration.path.join("clean.txt"), protected_contents).unwrap();
        let resolved_head = service
            .finalize_merge_conflict_resolution(
                &integration,
                &task,
                MergeResolutionFinalization {
                    task_head: &task_head,
                    pre_merge_head: &session.pre_merge_head,
                    merge_paths: &session.merge_paths,
                    conflict_paths: &session.paths,
                    protected_paths: &session.protected_paths,
                },
            )
            .unwrap();
        assert_ne!(resolved_head, pre_merge_head);
        assert_eq!(
            fs::read_to_string(integration.path.join("shared.txt")).unwrap(),
            "resolved version\n"
        );
        let resolved_snapshot = service.repo().unwrap().at(&integration.path);
        assert!(
            !service
                .block_on(resolved_snapshot.snapshot_readonly())
                .unwrap()
                .dirty
        );

        // A second fresh conflict proves that abort restores the exact durable pre-merge tip and
        // is idempotent when a later control-plane acknowledgement needs to retry.
        let second_task = service.ensure_task_workspace(&work, "T-2", "main").unwrap();
        fs::write(second_task.path.join("shared.txt"), "second task version\n").unwrap();
        let second_head = service
            .commit_workspace_paths(
                &second_task,
                &[PathBuf::from("shared.txt")],
                "Implement T-2",
            )
            .unwrap();
        fs::write(repository.path.join("shared.txt"), "second main version\n").unwrap();
        git_block_on(git.add(&repository.path, &[PathBuf::from("shared.txt")]));
        git_block_on(git.commit(&repository.path, "Second main change"));
        // Existing integration stays on its resolved commit; update it with the typed service's
        // normal merge shape is intentionally not part of this fixture, so use a newly created
        // task against the current integration tip only after its branch has diverged from it.
        let abort_base = service.integration_workspace_tip(&integration).unwrap();
        let session = service
            .begin_merge_conflict_resolution(&integration, &second_task, &second_head, &abort_base)
            .unwrap();
        service
            .abort_merge_conflict_resolution(&integration, &session.pre_merge_head)
            .unwrap();
        service
            .abort_merge_conflict_resolution(&integration, &session.pre_merge_head)
            .unwrap();
        assert_eq!(
            service.integration_workspace_tip(&integration).unwrap(),
            session.pre_merge_head
        );
        let aborted_snapshot = service.repo().unwrap().at(&integration.path);
        let snapshot = service
            .block_on(aborted_snapshot.snapshot_readonly())
            .unwrap();
        assert!(!snapshot.dirty && !snapshot.conflicted);
    }

    #[test]
    fn actual_git_publication_fast_forwards_main_from_the_managed_integration_branch() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join(".gitignore"), ".work/\n").expect("ignore control plane");
        fs::write(repository.path.join("base.txt"), "base\n").expect("write base file");
        git_block_on(git.add(
            &repository.path,
            &[PathBuf::from(".gitignore"), PathBuf::from("base.txt")],
        ));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let initial_branch =
            git_block_on(git.current_branch(&repository.path)).expect("initial branch name");
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).expect("valid initial branch"),
                &RefName::new("main").expect("valid main branch"),
            ));
        }

        let service = VcsService::discover(&repository.path).expect("discover Git repository");
        let integration = service
            .ensure_integration_workspace(
                repository.path.join(".work"),
                "B-20260724T000000Z",
                "main",
            )
            .expect("create integration workspace");
        fs::write(integration.path.join("published.txt"), "released\n")
            .expect("write integration change");
        let integration_head = service
            .commit_integration_workspace_paths(
                &integration,
                &[PathBuf::from("published.txt")],
                "Prepare publication",
            )
            .expect("commit integration change");
        assert_eq!(
            service
                .local_integration_publication_observation("B-20260724T000000Z", "main")
                .expect("observe unpublished local Git integration"),
            PublicationObservation::NotPublished
        );

        assert!(
            service
                .publish_integration(&integration, "main", "unexpected-integration-tip", false)
                .is_err(),
            "publication must reject a stale precondition before changing main"
        );

        let published = service
            .publish_integration(&integration, "main", &integration_head, false)
            .expect("fast-forward publication");
        assert_eq!(published, integration_head);
        assert_eq!(
            service
                .local_integration_publication_observation("B-20260724T000000Z", "main")
                .expect("observe published local Git integration"),
            PublicationObservation::Published
        );
        assert_eq!(
            service.snapshot().expect("read snapshot").head,
            Some(published.clone())
        );
        assert_eq!(
            fs::read_to_string(repository.path.join("published.txt"))
                .expect("read fast-forwarded file")
                .trim(),
            "released"
        );
        let published_workspace = service
            .published_primary_workspace("main", &published)
            .expect("prove clean published Git workspace");
        assert!(
            same_path(&published_workspace, &repository.path),
            "published Git workspace must resolve to the test repository"
        );
        fs::write(repository.path.join("ci-fix.txt"), "green\n").expect("write primary CI repair");
        let repaired = service
            .commit_published_ci_fix(
                "main",
                &published,
                &[PathBuf::from("ci-fix.txt")],
                "Fix required CI",
                false,
            )
            .expect("commit exact primary CI repair");
        assert_ne!(repaired, published);
        assert_eq!(
            service.snapshot().expect("read repaired snapshot").head,
            Some(repaired)
        );
        assert_eq!(
            fs::read_to_string(repository.path.join("ci-fix.txt"))
                .expect("read CI repair")
                .trim(),
            "green"
        );
    }

    #[test]
    fn local_fast_forward_divergence_is_not_misclassified_as_a_remote_push_failure() {
        let repository = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(
            &repository.path,
            &[PathBuf::from(".gitignore"), PathBuf::from("base.txt")],
        ));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let initial_branch = git_block_on(git.current_branch(&repository.path)).unwrap();
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).unwrap(),
                &RefName::new("main").unwrap(),
            ));
        }

        let service = VcsService::discover(&repository.path).unwrap();
        let work = repository.path.join(".work");
        let integration = service
            .ensure_integration_workspace(&work, "B-20260726T010000Z", "main")
            .unwrap();
        fs::write(integration.path.join("candidate.txt"), "candidate\n").unwrap();
        let integration_head = service
            .commit_integration_workspace_paths(
                &integration,
                &[PathBuf::from("candidate.txt")],
                "Prepare candidate",
            )
            .unwrap();

        // An unrelated local writer advances the primary after the integration base. The
        // publication call is configured to push, but it must stop at the failed local
        // fast-forward and must not be exposed as a later remote-push rejection.
        fs::write(repository.path.join("external.txt"), "external\n").unwrap();
        git_block_on(git.add(&repository.path, &[PathBuf::from("external.txt")]));
        git_block_on(git.commit(&repository.path, "External primary advance"));
        let primary_before = service.snapshot().unwrap().head.unwrap();

        assert!(matches!(
            service.publish_integration(&integration, "main", &integration_head, true),
            Err(VcsError::PublicationLocalDivergence(message))
                if message.contains("cannot fast-forward")
        ));
        assert_eq!(
            service.snapshot().unwrap().head.as_deref(),
            Some(primary_before.as_str())
        );
        assert_eq!(
            service
                .reanchor_after_local_divergence(
                    &work,
                    "B-20260726T010000Z",
                    "main",
                    &integration_head,
                )
                .unwrap(),
            PublicationReanchorOutcome::Reanchored
        );
        assert_eq!(
            service.snapshot().unwrap().head.as_deref(),
            Some(primary_before.as_str()),
            "local re-anchor must retain the external local primary advance"
        );
        assert!(!integration.path.exists());
        assert_eq!(
            service
                .reanchor_after_local_divergence(
                    &work,
                    "B-20260726T010000Z",
                    "main",
                    &integration_head,
                )
                .unwrap(),
            PublicationReanchorOutcome::Reanchored,
            "a crash after integration cleanup must retain the same local primary on retry"
        );
    }

    #[test]
    fn rejected_git_push_reanchors_primary_to_remote_and_preserves_task_candidates() {
        let repository = TestRepository::new();
        let remote = TestRepository::new();
        let racer = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&repository.path));
        git_block_on(git.config_set(&repository.path, "user.name", "Orchestrail Test"));
        git_block_on(git.config_set(
            &repository.path,
            "user.email",
            "orchestrail-test@example.invalid",
        ));
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        git_block_on(git.add(
            &repository.path,
            &[PathBuf::from(".gitignore"), PathBuf::from("base.txt")],
        ));
        git_block_on(git.commit(&repository.path, "Initial base"));
        let initial_branch =
            git_block_on(git.current_branch(&repository.path)).expect("initial branch name");
        if initial_branch != "main" {
            git_block_on(git.rename_branch(
                &repository.path,
                &RefName::new(initial_branch).unwrap(),
                &RefName::new("main").unwrap(),
            ));
        }

        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "receive.denyCurrentBranch", "ignore"));
        git_block_on(git.remote_add(
            &repository.path,
            "origin",
            remote.path.to_str().expect("UTF-8 remote path"),
        ));
        git_block_on(git.push(
            &repository.path,
            GitPush::branch(RefName::new("main").unwrap()),
        ));

        let service = VcsService::discover(&repository.path).unwrap();
        let work = repository.path.join(".work");
        let task = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        fs::write(task.path.join("task.txt"), "candidate\n").unwrap();
        let task_head = service
            .commit_workspace_paths(&task, &[PathBuf::from("task.txt")], "Implement T-1")
            .unwrap();
        let integration = service
            .ensure_integration_workspace(&work, "B-20260726T000000Z", "main")
            .unwrap();
        fs::write(integration.path.join("integration.txt"), "candidate\n").unwrap();
        let integration_head = service
            .commit_integration_workspace_paths(
                &integration,
                &[PathBuf::from("integration.txt")],
                "Prepare integration",
            )
            .unwrap();
        service
            .publish_integration(&integration, "main", &integration_head, false)
            .unwrap();

        git_block_on(git.clone_repo(
            remote.path.to_str().expect("UTF-8 remote path"),
            &racer.path,
            CloneSpec::new().branch("main"),
        ));
        git_block_on(git.config_set(&racer.path, "user.name", "Remote racer"));
        git_block_on(git.config_set(&racer.path, "user.email", "racer@example.invalid"));
        fs::write(racer.path.join("racer.txt"), "remote advanced\n").unwrap();
        git_block_on(git.add(&racer.path, &[PathBuf::from("racer.txt")]));
        git_block_on(git.commit(&racer.path, "Advance remote main"));
        git_block_on(git.push(&racer.path, GitPush::branch(RefName::new("main").unwrap())));
        let remote_head =
            git_block_on(git.resolve_commit(&remote.path, &RevSpec::new("main").unwrap()));

        assert!(matches!(
            service.publish_integration(&integration, "main", &integration_head, true),
            Err(VcsError::PublicationPushFailed(_))
        ));

        assert_eq!(
            service
                .reanchor_after_remote_rejection(
                    &work,
                    "B-20260726T000000Z",
                    "main",
                    &integration_head,
                )
                .unwrap(),
            PublicationReanchorOutcome::Reanchored
        );
        let primary = service.snapshot().unwrap();
        assert_eq!(primary.branch.as_deref(), Some("main"));
        assert_eq!(primary.head.as_deref(), Some(remote_head.as_str()));
        assert!(!primary.dirty && !primary.conflicted);
        assert!(!integration.path.exists());
        assert!(
            !git_block_on(git.branch_exists(
                &repository.path,
                &RefName::new("integration/B-20260726T000000Z").unwrap(),
            )),
            "only the integration ref is removed"
        );
        assert!(task.path.exists());
        assert!(git_block_on(git.branch_exists(
            &repository.path,
            &RefName::new("task/T-1").unwrap(),
        )));
        assert_eq!(service.task_workspace_tip(&task).unwrap(), task_head);

        assert_eq!(
            service
                .reanchor_after_remote_rejection(
                    &work,
                    "B-20260726T000000Z",
                    "main",
                    &integration_head,
                )
                .unwrap(),
            PublicationReanchorOutcome::Reanchored,
            "a restart after VCS cleanup must re-prove the exact anchored postcondition"
        );
    }

    #[test]
    fn jj_rollback_rewinds_the_integration_bookmark_and_restores_a_clean_merge_surface() {
        let repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "test@example.invalid",
            ],
        );
        fs::write(repository.path.join(".gitignore"), ".work/\n").expect("ignore control plane");
        fs::write(repository.path.join("base.txt"), "base\n").expect("write base file");
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );

        let service = VcsService::discover(&repository.path).expect("discover JJ repository");
        let task = service
            .ensure_task_workspace(repository.path.join(".work"), "T-1", "main")
            .expect("create JJ task workspace");
        fs::write(task.path.join("candidate.txt"), "candidate\n").expect("write candidate");
        let task_tip = service
            .commit_workspace_paths(&task, &[PathBuf::from("candidate.txt")], "candidate")
            .expect("commit candidate");
        let integration = service
            .ensure_integration_workspace(
                repository.path.join(".work"),
                "B-20260724T000000Z",
                "main",
            )
            .expect("create JJ integration workspace");
        let base_tip = service
            .integration_workspace_tip(&integration)
            .expect("read pre-merge integration tip");
        let merged_tip = service
            .merge_task_into_integration(&integration, &task, &task_tip, Some(&base_tip))
            .expect("merge candidate");
        assert_ne!(merged_tip, base_tip);

        service
            .rollback_integration_merge(&integration, &merged_tip, &base_tip)
            .expect("typed JJ rollback of known-red merge candidate");
        assert_eq!(
            service.integration_workspace_tip(&integration).unwrap(),
            base_tip,
            "integration bookmark must be restored to the exact pre-merge tip"
        );
        assert!(
            !service
                .task_is_merged_into_integration("T-1", "B-20260724T000000Z")
                .unwrap(),
            "the rolled-back task must not remain an integration ancestor"
        );
        assert!(
            !service.snapshot().unwrap().dirty,
            "rollback leaves the primary JJ workspace clean"
        );
        let retry_tip = service
            .merge_task_into_integration(&integration, &task, &task_tip, Some(&base_tip))
            .expect("a later deterministic retry can merge from the restored integration base");
        assert_ne!(retry_tip, base_tip);
    }

    #[test]
    fn typed_jj_conflict_resolution_starts_from_the_durable_bookmark_and_aborts_cleanly() {
        let repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "test@example.invalid",
            ],
        );
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("shared.txt"), "base\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );

        let service = VcsService::discover(&repository.path).unwrap();
        let work = repository.path.join(".work");
        let task = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        fs::write(task.path.join("shared.txt"), "task version\n").unwrap();
        let task_head = service
            .commit_workspace_paths(&task, &[PathBuf::from("shared.txt")], "Implement T-1")
            .unwrap();

        fs::write(repository.path.join("shared.txt"), "main version\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Main change"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "set", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );

        let integration = service
            .ensure_integration_workspace(&work, "B-20260725T120001Z", "main")
            .unwrap();
        let pre_merge_head = service.integration_workspace_tip(&integration).unwrap();
        assert!(matches!(
            service.preflight_task_merge(&integration, "T-1").unwrap(),
            MergeProbe::Conflicts(paths) if paths == vec![PathBuf::from("shared.txt")]
        ));
        let session = service
            .begin_merge_conflict_resolution(&integration, &task, &task_head, &pre_merge_head)
            .unwrap();
        assert_eq!(session.pre_merge_head, pre_merge_head);
        assert_eq!(session.paths, vec![PathBuf::from("shared.txt")]);
        assert_eq!(
            service
                .integration_workspace_tip_during_merge_resolution(&integration)
                .unwrap(),
            pre_merge_head
        );
        assert!(git_block_on(jj.has_workingcopy_conflict(&integration.path)));

        service
            .abort_merge_conflict_resolution(&integration, &session.pre_merge_head)
            .unwrap();
        assert_eq!(
            service.integration_workspace_tip(&integration).unwrap(),
            session.pre_merge_head
        );
        assert!(!git_block_on(
            jj.has_workingcopy_conflict(&integration.path)
        ));
        let integration_repo = service.repo().unwrap().at(&integration.path);
        let snapshot = service
            .block_on(integration_repo.snapshot_readonly())
            .unwrap();
        assert!(!snapshot.dirty && !snapshot.conflicted);
    }

    #[test]
    fn typed_jj_conflict_resolution_finalization_moves_the_bookmark_only_after_resolution() {
        let repository = TestRepository::new();
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "test@example.invalid",
            ],
        );
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("shared.txt"), "base\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );

        let service = VcsService::discover(&repository.path).unwrap();
        let work = repository.path.join(".work");
        let task = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        fs::write(task.path.join("shared.txt"), "task version\n").unwrap();
        let task_head = service
            .commit_workspace_paths(&task, &[PathBuf::from("shared.txt")], "Implement T-1")
            .unwrap();
        fs::write(repository.path.join("shared.txt"), "main version\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Main change"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "set", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );

        let integration = service
            .ensure_integration_workspace(&work, "B-20260725T120002Z", "main")
            .unwrap();
        let pre_merge_head = service.integration_workspace_tip(&integration).unwrap();
        let session = service
            .begin_merge_conflict_resolution(&integration, &task, &task_head, &pre_merge_head)
            .unwrap();
        assert!(git_block_on(jj.has_workingcopy_conflict(&integration.path)));
        fs::write(integration.path.join("shared.txt"), "resolved version\n").unwrap();
        assert!(
            !git_block_on(jj.has_workingcopy_conflict(&integration.path)),
            "replacing the conflict marker must resolve the JJ working-copy conflict"
        );
        let resolved_head = service
            .finalize_merge_conflict_resolution(
                &integration,
                &task,
                MergeResolutionFinalization {
                    task_head: &task_head,
                    pre_merge_head: &session.pre_merge_head,
                    merge_paths: &session.merge_paths,
                    conflict_paths: &session.paths,
                    protected_paths: &session.protected_paths,
                },
            )
            .unwrap();
        assert_ne!(resolved_head, session.pre_merge_head);
        assert_eq!(
            service.integration_workspace_tip(&integration).unwrap(),
            resolved_head
        );
        assert_eq!(
            fs::read_to_string(integration.path.join("shared.txt")).unwrap(),
            "resolved version\n"
        );
        assert!(!git_block_on(
            jj.has_workingcopy_conflict(&integration.path)
        ));
    }

    #[test]
    fn actual_jj_publication_moves_main_only_to_the_verified_integration_bookmark() {
        let repository = TestRepository::new();
        let remote = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        // A test-owned non-bare remote is sufficient for the real transport boundary when it
        // explicitly ignores updates to its checked-out (initially unborn) branch.
        git_block_on(git.config_set(&remote.path, "receive.denyCurrentBranch", "ignore"));
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "test@example.invalid",
            ],
        );
        fs::write(repository.path.join(".gitignore"), ".work/\n").expect("ignore control plane");
        fs::write(repository.path.join("base.txt"), "base\n").expect("write base file");
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "git",
                "remote",
                "add",
                "origin",
                remote.path.to_str().expect("UTF-8 test remote path"),
            ],
        );
        // Keep the primary workspace clean and on the nearest `main` bookmark before native
        // publication checks its typed snapshot.
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );

        let service = VcsService::discover(&repository.path).expect("discover JJ repository");
        assert_eq!(service.backend(), BackendKind::Jj);
        // Seed the bare remote before managed work begins. Product publication below deliberately
        // does not use this JJ working-copy command: it pushes the exact verified commit through
        // `vcs-git` from JJ's private Git store.
        jj_run(
            &jj,
            &repository.path,
            &[
                "git",
                "push",
                "--ignore-working-copy",
                "--allow-empty-description",
                "-b",
                "main",
            ],
        );
        let task = service
            .ensure_task_workspace(repository.path.join(".work"), "T-1", "main")
            .expect("create JJ task workspace");
        fs::write(task.path.join("implementation.txt"), "implemented\n")
            .expect("write task implementation");
        let task_head = service
            .commit_workspace_paths(
                &task,
                &[PathBuf::from("implementation.txt")],
                "Implement T-1",
            )
            .expect("commit JJ task change");
        let bookmarks = git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path));
        assert_eq!(
            task_head,
            jj_bookmark_target(&bookmarks, &task.branch).expect("task bookmark target"),
            "JJ commits must report the durable task bookmark, not the empty successor change"
        );

        let integration = service
            .ensure_integration_workspace(
                repository.path.join(".work"),
                "B-20260724T000000Z",
                "main",
            )
            .expect("create JJ integration workspace");
        assert!(
            !service
                .task_is_merged_into_integration("T-1", "B-20260724T000000Z")
                .expect("prove an unmerged JJ task is not an integration ancestor")
        );
        assert!(matches!(
            service
                .preflight_task_merge(&integration, "T-1")
                .expect("preflight JJ merge"),
            MergeProbe::Clean
        ));
        let merge_head = service
            .merge_task_into_integration(&integration, &task, &task_head, None)
            .expect("merge JJ task into integration");
        assert_eq!(
            service
                .merge_task_into_integration(&integration, &task, &task_head, Some(&merge_head),)
                .expect("replay already-integrated JJ task"),
            merge_head,
            "legacy merger replay must not create a redundant JJ change"
        );
        assert_eq!(
            merge_head,
            jj_bookmark_target(
                &git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path)),
                &integration.branch
            )
            .expect("merged integration bookmark target")
        );
        assert_eq!(
            service
                .integration_workspace_tip(&integration)
                .expect("merged JJ integration workspace must be a clean child"),
            merge_head
        );
        assert!(
            service
                .task_is_merged_into_integration("T-1", "B-20260724T000000Z")
                .expect("prove the merged JJ task is an integration ancestor")
        );
        fs::write(integration.path.join("published.txt"), "released\n")
            .expect("write integration change");
        let integration_head = service
            .commit_integration_workspace_paths(
                &integration,
                &[PathBuf::from("published.txt")],
                "Prepare release",
            )
            .expect("commit integration change");
        assert_eq!(
            service
                .integration_workspace_tip(&integration)
                .expect("JJ integration bookmark advances to the exact committed path set"),
            integration_head
        );
        assert_eq!(
            service
                .local_integration_publication_observation("B-20260724T000000Z", "main")
                .expect("observe unpublished local JJ integration"),
            PublicationObservation::NotPublished
        );
        assert_eq!(
            service
                .remote_integration_publication_observation("B-20260724T000000Z", "main")
                .expect("observe an unpushed JJ integration against the real remote"),
            PublicationObservation::NotPublished
        );
        let published = service
            .publish_integration(&integration, "main", &integration_head, true)
            .expect("typed JJ fast-forward and exact-target remote publication");
        assert_eq!(published, integration_head);
        assert_eq!(
            service
                .publish_integration(&integration, "main", &integration_head, true)
                .expect("retry a completed JJ remote publication"),
            integration_head
        );
        let operation_before_remote_proof = git_block_on(jj.op_head(&repository.path));
        let bookmarks_before_remote_proof =
            git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path));
        assert_eq!(
            service
                .remote_integration_publication_observation("B-20260724T000000Z", "main")
                .expect("prove published JJ integration against the real remote"),
            PublicationObservation::Published
        );
        assert_eq!(
            git_block_on(jj.op_head(&repository.path)),
            operation_before_remote_proof,
            "the isolated remote proof must not record a JJ operation"
        );
        assert_eq!(
            git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path)),
            bookmarks_before_remote_proof,
            "the isolated remote proof must not reconcile or move a local JJ bookmark"
        );
        assert_eq!(
            service
                .local_integration_publication_observation("B-20260724T000000Z", "main")
                .expect("observe published local JJ integration"),
            PublicationObservation::Published
        );

        let bookmarks = git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path));
        assert_eq!(
            jj_bookmark_target(&bookmarks, "main").expect("published main target"),
            jj_bookmark_target(&bookmarks, &integration.branch)
                .expect("integration target after publication")
        );
        let published_workspace = service
            .published_primary_workspace("main", &published)
            .expect("prove clean published JJ primary child");
        assert!(
            same_path(&published_workspace, &repository.path),
            "published JJ workspace must resolve to the test repository"
        );
        fs::write(repository.path.join("ci-fix.txt"), "green\n")
            .expect("write primary JJ CI repair");
        let repaired = service
            .commit_published_ci_fix(
                "main",
                &published,
                &[PathBuf::from("ci-fix.txt")],
                "Fix required CI",
                false,
            )
            .expect("commit exact JJ primary CI repair");
        assert_ne!(repaired, published);
        let repaired_bookmarks = git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path));
        assert_eq!(
            jj_bookmark_target(&repaired_bookmarks, "main").expect("repaired main target"),
            repaired
        );
        assert!(
            !service.snapshot().expect("read repaired JJ snapshot").dirty,
            "JJ CI repair must leave a fresh clean primary child"
        );
    }

    #[test]
    fn pure_jj_publication_pushes_the_exact_target_without_opening_its_working_copy() {
        let repository = TestRepository::new();
        let remote = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "receive.denyCurrentBranch", "ignore"));
        let jj = Jj::new();
        jj_run(
            &jj,
            &repository.path,
            &["git", "init", "--no-colocate", "."],
        );
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "test@example.invalid",
            ],
        );
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "git",
                "remote",
                "add",
                "origin",
                remote.path.to_str().expect("UTF-8 remote path"),
            ],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "git",
                "push",
                "--ignore-working-copy",
                "--allow-empty-description",
                "-b",
                "main",
            ],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "main", "-m", "primary working copy"],
        );

        let service = VcsService::discover(&repository.path).unwrap();
        assert_eq!(service.backend(), BackendKind::Jj);
        let integration = service
            .ensure_integration_workspace(
                repository.path.join(".work"),
                "B-20260726T020000Z",
                "main",
            )
            .unwrap();
        fs::write(integration.path.join("published.txt"), "published\n").unwrap();
        let head = service
            .commit_integration_workspace_paths(
                &integration,
                &[PathBuf::from("published.txt")],
                "Prepare pure JJ publication",
            )
            .unwrap();

        assert_eq!(
            service
                .publish_integration(&integration, "main", &head, true)
                .unwrap(),
            head
        );
        assert_eq!(
            service
                .remote_integration_publication_observation("B-20260726T020000Z", "main")
                .unwrap(),
            PublicationObservation::Published
        );
        let operation_before_retry = git_block_on(jj.op_head(&repository.path));
        assert_eq!(
            service
                .publish_integration(&integration, "main", &head, true)
                .unwrap(),
            head
        );
        assert_eq!(
            git_block_on(jj.op_head(&repository.path)),
            operation_before_retry,
            "an idempotent exact-target push must not snapshot or mutate the pure JJ working copy"
        );
    }

    #[test]
    fn rejected_jj_push_reanchors_primary_to_remote_and_preserves_task_candidates() {
        let repository = TestRepository::new();
        let remote = TestRepository::new();
        let racer = TestRepository::new();
        let git = Git::hardened();
        git_block_on(git.init(&remote.path));
        git_block_on(git.config_set(&remote.path, "receive.denyCurrentBranch", "ignore"));
        let jj = Jj::new();
        jj_run(&jj, &repository.path, &["git", "init", "--colocate", "."]);
        jj_run(
            &jj,
            &repository.path,
            &["config", "set", "--repo", "user.name", "Orchestrail Test"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "orchestrail-test@example.invalid",
            ],
        );
        fs::write(repository.path.join(".gitignore"), ".work/\n").unwrap();
        fs::write(repository.path.join("base.txt"), "base\n").unwrap();
        jj_run(&jj, &repository.path, &["describe", "-m", "Initial base"]);
        jj_run(
            &jj,
            &repository.path,
            &["bookmark", "create", "main", "-r", "@"],
        );
        jj_run(
            &jj,
            &repository.path,
            &[
                "git",
                "remote",
                "add",
                "origin",
                remote.path.to_str().expect("UTF-8 remote path"),
            ],
        );
        jj_run(
            &jj,
            &repository.path,
            &["new", "-m", "primary working copy"],
        );
        let service = VcsService::discover(&repository.path).unwrap();
        assert_eq!(service.backend(), BackendKind::Jj);
        jj_run(
            &jj,
            &repository.path,
            &[
                "git",
                "push",
                "--ignore-working-copy",
                "--allow-empty-description",
                "-b",
                "main",
            ],
        );

        let work = repository.path.join(".work");
        let task = service.ensure_task_workspace(&work, "T-1", "main").unwrap();
        fs::write(task.path.join("task.txt"), "candidate\n").unwrap();
        let task_head = service
            .commit_workspace_paths(&task, &[PathBuf::from("task.txt")], "Implement T-1")
            .unwrap();
        let integration = service
            .ensure_integration_workspace(&work, "B-20260726T000000Z", "main")
            .unwrap();
        fs::write(integration.path.join("integration.txt"), "candidate\n").unwrap();
        let integration_head = service
            .commit_integration_workspace_paths(
                &integration,
                &[PathBuf::from("integration.txt")],
                "Prepare integration",
            )
            .unwrap();
        service
            .publish_integration(&integration, "main", &integration_head, false)
            .unwrap();

        git_block_on(git.clone_repo(
            remote.path.to_str().expect("UTF-8 remote path"),
            &racer.path,
            CloneSpec::new().branch("main"),
        ));
        git_block_on(git.config_set(&racer.path, "user.name", "Remote racer"));
        git_block_on(git.config_set(&racer.path, "user.email", "racer@example.invalid"));
        fs::write(racer.path.join("racer.txt"), "remote advanced\n").unwrap();
        git_block_on(git.add(&racer.path, &[PathBuf::from("racer.txt")]));
        git_block_on(git.commit(&racer.path, "Advance remote main"));
        git_block_on(git.push(&racer.path, GitPush::branch(RefName::new("main").unwrap())));
        let remote_head =
            git_block_on(git.resolve_commit(&remote.path, &RevSpec::new("main").unwrap()));

        assert!(matches!(
            service.publish_integration(&integration, "main", &integration_head, true),
            Err(VcsError::PublicationPushFailed(_))
        ));

        assert_eq!(
            service
                .reanchor_after_remote_rejection(
                    &work,
                    "B-20260726T000000Z",
                    "main",
                    &integration_head,
                )
                .unwrap(),
            PublicationReanchorOutcome::Reanchored
        );
        let bookmarks = git_block_on(jj.bookmarks_ignoring_working_copy(&repository.path));
        assert_eq!(
            jj_bookmark_target(&bookmarks, "main").unwrap(),
            remote_head,
            "the local main bookmark must move backward only to the freshly fetched remote"
        );
        assert!(!service.snapshot().unwrap().dirty);
        assert!(!integration.path.exists());
        assert!(
            !bookmarks
                .iter()
                .any(|bookmark| bookmark.name == "integration/B-20260726T000000Z"),
            "only the managed integration bookmark is deleted"
        );
        assert!(task.path.exists());
        assert_eq!(service.task_workspace_tip(&task).unwrap(), task_head);

        assert_eq!(
            service
                .reanchor_after_remote_rejection(
                    &work,
                    "B-20260726T000000Z",
                    "main",
                    &integration_head,
                )
                .unwrap(),
            PublicationReanchorOutcome::Reanchored
        );
    }
}
