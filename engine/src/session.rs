//! Durable provider-conversation coordinates ("leaf sessions") and their non-destructive probes.
//!
//! A review/fix cycle calls the SAME leaf role several times for one task. Without a session
//! coordinate every call is stateless: the provider re-reads the descriptor, the diff, and the
//! review artifact from scratch. Claude Code and Codex both keep the conversation on disk and can
//! continue it (`claude --resume <id>`, `codex exec resume <id>`), so the engine only has to
//! remember which conversation belonged to which leaf lineage.
//!
//! The coordinate is deliberately an ORTHOGONAL runtime detail. It never reaches the deterministic
//! reducer's decision surface: no `TaskPhase`, transition, escalation, or acknowledgement key
//! depends on whether a session happens to be alive. Losing it (crash, cleaned provider home,
//! expired transcript) costs exactly one re-seeded call, which is the behaviour the engine had
//! before durable sessions existed. That asymmetry is what makes the probe below safe.
//!
//! The probe itself is read-only: it stats an expected transcript path (Claude) or looks for a
//! `rollout-*-<id>.jsonl` file under the Codex session root, mirroring
//! `find ~/.codex/sessions -name "rollout-*-<id>.jsonl"`. A missing home, a missing directory, an
//! unreadable directory, and a genuinely absent session are all reported the same way — "no live
//! session" — because every one of them must lead to the same safe action: re-seed with full
//! context. This is deliberately unlike the fail-loud control-plane stop probe (K-008): there,
//! an I/O error and an operator stop demand DIFFERENT actions, so a bool would hide a real
//! failure; here both branches are safe and the fallback is the pre-existing full-context path.

use std::path::{Path, PathBuf};

use crate::processor::LeafKind;

/// Cap the Codex rollout search at the `YYYY/MM/DD/<file>` shape codex writes, plus one level of
/// slack. A deeper tree is not a codex session root and must not turn a probe into a full scan.
const CODEX_ROLLOUT_MAX_DEPTH: usize = 4;
/// Bound the probe's total work so an unexpectedly huge session archive cannot stall a leaf call.
/// Exhausting the budget reports "no live session", i.e. the safe re-seed path.
const CODEX_ROLLOUT_MAX_ENTRIES: usize = 50_000;
/// Provider conversation ids are opaque, but they are used to build a filesystem path and an argv
/// element, so their alphabet is pinned rather than trusted.
const SESSION_ID_MAX_LEN: usize = 128;

/// Which external provider owns a conversation id. The two providers never share an id space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SessionProvider {
    Claude,
    Codex,
}

impl SessionProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }
}

/// Which leaf role lineage a conversation belongs to.
///
/// `Implement` and `Fix` are ONE lineage on purpose: the fix call is exactly the repeated call of
/// the same maker inside a review/fix cycle, and continuing that conversation is the whole point.
/// `Review` is a separate lineage, equally on purpose: an independent reviewer must never inherit
/// the maker's conversation, so no resume path can ever hand it the maker's self-justification.
/// Leaf roles that are not per-task (merger, integration, CI fix) have no task-scoped lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SessionLineage {
    Coder,
    Reviewer,
}

impl SessionLineage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Coder => "coder",
            Self::Reviewer => "reviewer",
        }
    }

    /// Map a supervised leaf role onto its task-scoped conversation lineage.
    pub fn for_leaf(kind: LeafKind) -> Option<Self> {
        match kind {
            LeafKind::Implement | LeafKind::Fix => Some(Self::Coder),
            LeafKind::Review => Some(Self::Reviewer),
            LeafKind::Merger
            | LeafKind::IntegrationReview
            | LeafKind::IntegrationFix
            | LeafKind::CiFix
            | LeafKind::KnowledgeCurator => None,
        }
    }
}

/// Durable key of one conversation inside a task's session map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeafSessionKey {
    pub provider: SessionProvider,
    pub lineage: SessionLineage,
}

impl LeafSessionKey {
    pub fn new(provider: SessionProvider, lineage: SessionLineage) -> Self {
        Self { provider, lineage }
    }

    /// Stable durable key (`claude:coder`, `codex:reviewer`) used in the checkpoint map.
    pub fn as_durable_key(&self) -> String {
        format!("{}:{}", self.provider.as_str(), self.lineage.as_str())
    }
}

/// Orthogonal bookkeeping produced by a leaf call for its own conversation coordinate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafSessionUpdate {
    /// The provider reported this conversation id for the named lineage.
    Observed { key: LeafSessionKey, id: String },
    /// A call that DID resume this coordinate failed. Forgetting it guarantees the next attempt
    /// re-seeds with full context instead of retrying an unusable resume, so a provider whose CLI
    /// cannot resume at all costs at most one call per lineage before the engine is back to its
    /// previous behaviour.
    Invalidated { key: LeafSessionKey },
}

impl LeafSessionUpdate {
    pub fn key(&self) -> LeafSessionKey {
        match self {
            Self::Observed { key, .. } | Self::Invalidated { key } => *key,
        }
    }
}

/// Accept only opaque provider ids: non-empty, bounded, free of any character that could escape
/// the transcript directory when the id is joined into a probe path, and not option-shaped, since
/// the same value is handed to a CLI as a separate argv element and a leading `-` would let a
/// hostile transcript turn `--resume <id>` into another flag. Both providers issue UUID-shaped
/// ids, so this rejects nothing legitimate.
pub fn is_valid_session_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= SESSION_ID_MAX_LEN
        && !id.starts_with('-')
        && id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

/// Claude Code stores a conversation under a directory named after the working directory it ran
/// in, with every character outside `[A-Za-z0-9]` replaced by `-` (so `D:\GitHub\Personal\App`
/// becomes `D--GitHub-Personal-App`).
pub fn claude_project_slug(cwd: &Path) -> String {
    cwd.to_string_lossy()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect()
}

/// Read-only locator for both providers' on-disk conversation archives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionProbe {
    claude_projects: PathBuf,
    codex_sessions: PathBuf,
}

impl SessionProbe {
    pub fn new(claude_projects: impl Into<PathBuf>, codex_sessions: impl Into<PathBuf>) -> Self {
        Self {
            claude_projects: claude_projects.into(),
            codex_sessions: codex_sessions.into(),
        }
    }

    /// Default layout under one home directory: `~/.claude/projects` and `~/.codex/sessions`.
    pub fn from_home(home: impl AsRef<Path>) -> Self {
        let home = home.as_ref();
        Self::new(
            home.join(".claude").join("projects"),
            home.join(".codex").join("sessions"),
        )
    }

    /// Resolve the real archives, honouring the providers' own home overrides
    /// (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`) before falling back to the user's home directory.
    /// A machine with no discoverable home yields a probe that simply never finds a session,
    /// which is the safe re-seed path rather than an error.
    pub fn from_env() -> Self {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .unwrap_or_default();
        let claude_projects = std::env::var_os("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude"))
            .join("projects");
        let codex_sessions = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"))
            .join("sessions");
        Self::new(claude_projects, codex_sessions)
    }

    /// Expected transcript path for a Claude conversation started in `cwd`.
    ///
    /// `cwd` must be the very path the child was given as its working directory, not a
    /// canonicalized form of it: Claude slugs the directory as it saw it, and Windows
    /// canonicalization would introduce a `\\?\` prefix that no archive entry carries. The slug
    /// maps `/` and `\` alike, so path separator style is the one difference that cannot matter.
    pub fn claude_transcript(&self, cwd: &Path, id: &str) -> Option<PathBuf> {
        is_valid_session_id(id).then(|| {
            self.claude_projects
                .join(claude_project_slug(cwd))
                .join(format!("{id}.jsonl"))
        })
    }

    /// Non-destructive existence probe. It never creates, opens for writing, or removes anything,
    /// and it reports "no live session" for every failure mode (missing home, missing directory,
    /// permission error, malformed id) because they all require the same safe re-seed.
    pub fn is_live(&self, provider: SessionProvider, cwd: &Path, id: &str) -> bool {
        if !is_valid_session_id(id) {
            return false;
        }
        match provider {
            SessionProvider::Claude => self
                .claude_transcript(cwd, id)
                .is_some_and(|path| is_regular_file(&path)),
            SessionProvider::Codex => codex_rollout_exists(&self.codex_sessions, id),
        }
    }
}

fn is_regular_file(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.is_file())
}

/// Bounded equivalent of `find <root> -name "rollout-*-<id>.jsonl"`. Codex files a rollout under
/// `sessions/YYYY/MM/DD/`, so the walk stays shallow and never follows the tree indefinitely.
fn codex_rollout_exists(root: &Path, id: &str) -> bool {
    let suffix = format!("-{id}.jsonl");
    let mut pending = vec![(root.to_path_buf(), 0_usize)];
    let mut visited = 0_usize;
    while let Some((directory, depth)) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            visited = visited.saturating_add(1);
            if visited > CODEX_ROLLOUT_MAX_ENTRIES {
                return false;
            }
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                if depth.saturating_add(1) < CODEX_ROLLOUT_MAX_DEPTH {
                    pending.push((entry.path(), depth.saturating_add(1)));
                }
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with("rollout-") && name.ends_with(&suffix) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "orchestrail-session-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temp root");
        root
    }

    #[test]
    fn claude_slug_replaces_every_non_alphanumeric_character() {
        assert_eq!(
            claude_project_slug(Path::new(r"D:\GitHub\Personal\Orchestrail")),
            "D--GitHub-Personal-Orchestrail"
        );
        assert_eq!(
            claude_project_slug(Path::new("/home/user/work_tree.1")),
            "-home-user-work-tree-1"
        );
        // Separator style cannot change the answer, so a worktree path spelled either way still
        // finds the conversation the child actually created.
        assert_eq!(
            claude_project_slug(Path::new("D:/GitHub/Personal/Orchestrail")),
            claude_project_slug(Path::new(r"D:\GitHub\Personal\Orchestrail"))
        );
    }

    #[test]
    fn session_ids_reject_path_escapes_and_unbounded_input() {
        assert!(is_valid_session_id("019f054f-5e70-7d42-8586-ee66e3ac1d1e"));
        assert!(is_valid_session_id("abc_DEF-123"));
        assert!(!is_valid_session_id(""));
        // The id is joined into a probe path and handed to a CLI: traversal and separators are
        // rejected before either use.
        assert!(!is_valid_session_id("../../etc/passwd"));
        assert!(!is_valid_session_id("a/b"));
        assert!(!is_valid_session_id(r"a\b"));
        assert!(!is_valid_session_id("with space"));
        // Option-shaped ids would be re-read as flags by the provider CLI.
        assert!(!is_valid_session_id("--resume"));
        assert!(!is_valid_session_id("-C"));
        assert!(!is_valid_session_id(&"a".repeat(SESSION_ID_MAX_LEN + 1)));
    }

    #[test]
    fn claude_probe_finds_only_an_existing_transcript() {
        let home = temp_root("claude-live");
        let probe = SessionProbe::from_home(&home);
        let cwd = Path::new("/w/tree");
        let id = "11111111-2222-3333-4444-555555555555";
        // Absent home directory is the ordinary "no session yet" case, never an error.
        assert!(!probe.is_live(SessionProvider::Claude, cwd, id));
        let project = home.join(".claude").join("projects").join("-w-tree");
        std::fs::create_dir_all(&project).expect("project dir");
        // An existing project directory without this transcript is still "no session".
        assert!(!probe.is_live(SessionProvider::Claude, cwd, id));
        std::fs::write(project.join(format!("{id}.jsonl")), "{}\n").expect("transcript");
        assert!(probe.is_live(SessionProvider::Claude, cwd, id));
        // A directory that merely shares the name is not a transcript.
        let other = "66666666-7777-8888-9999-000000000000";
        std::fs::create_dir_all(project.join(format!("{other}.jsonl"))).expect("decoy");
        assert!(!probe.is_live(SessionProvider::Claude, cwd, other));
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn claude_probe_rejects_a_malformed_id_before_touching_the_filesystem() {
        let probe = SessionProbe::from_home(Path::new("/nonexistent-home"));
        assert!(
            probe
                .claude_transcript(Path::new("/w"), "../escape")
                .is_none()
        );
        assert!(!probe.is_live(SessionProvider::Claude, Path::new("/w"), "../escape"));
    }

    #[test]
    fn codex_probe_matches_the_rollout_naming_convention() {
        let home = temp_root("codex-live");
        let probe = SessionProbe::from_home(&home);
        let cwd = Path::new("/w/tree");
        let id = "019f054f-5e70-7d42-8586-ee66e3ac1d1e";
        assert!(!probe.is_live(SessionProvider::Codex, cwd, id));
        let day = home
            .join(".codex")
            .join("sessions")
            .join("2026")
            .join("06")
            .join("26");
        std::fs::create_dir_all(&day).expect("session day dir");
        std::fs::write(day.join("rollout-2026-06-26T22-01-55-other.jsonl"), "{}\n")
            .expect("unrelated rollout");
        assert!(!probe.is_live(SessionProvider::Codex, cwd, id));
        std::fs::write(
            day.join(format!("rollout-2026-06-26T22-01-55-{id}.jsonl")),
            "{}\n",
        )
        .expect("rollout");
        assert!(probe.is_live(SessionProvider::Codex, cwd, id));
        // The Codex archive is not cwd-scoped, so the probe deliberately ignores the worktree.
        assert!(probe.is_live(SessionProvider::Codex, Path::new("/elsewhere"), id));
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn codex_probe_stays_within_its_depth_budget() {
        let home = temp_root("codex-depth");
        let probe = SessionProbe::from_home(&home);
        let id = "019f054f-5e70-7d42-8586-ee66e3ac1d1e";
        let too_deep = home
            .join(".codex")
            .join("sessions")
            .join("a")
            .join("b")
            .join("c")
            .join("d");
        std::fs::create_dir_all(&too_deep).expect("deep dir");
        std::fs::write(too_deep.join(format!("rollout-x-{id}.jsonl")), "{}\n").expect("rollout");
        assert!(!probe.is_live(SessionProvider::Codex, Path::new("/w"), id));
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn lineage_keeps_the_maker_and_the_reviewer_apart() {
        assert_eq!(
            SessionLineage::for_leaf(LeafKind::Implement),
            Some(SessionLineage::Coder)
        );
        assert_eq!(
            SessionLineage::for_leaf(LeafKind::Fix),
            Some(SessionLineage::Coder)
        );
        assert_eq!(
            SessionLineage::for_leaf(LeafKind::Review),
            Some(SessionLineage::Reviewer)
        );
        assert_eq!(SessionLineage::for_leaf(LeafKind::Merger), None);
        assert_eq!(SessionLineage::for_leaf(LeafKind::CiFix), None);
        assert_ne!(
            LeafSessionKey::new(SessionProvider::Claude, SessionLineage::Coder).as_durable_key(),
            LeafSessionKey::new(SessionProvider::Claude, SessionLineage::Reviewer).as_durable_key()
        );
        assert_eq!(
            LeafSessionKey::new(SessionProvider::Codex, SessionLineage::Coder).as_durable_key(),
            "codex:coder"
        );
    }
}
