//! Strict, narrow support for the optional `.work/roadmap.md` runtime artifact.
//!
//! The processor is not the owner of the roadmap axis.  It may only observe the current
//! milestone and, after its linked work has been archived, replace the derived
//! `## Текущее состояние` section.  This module deliberately parses only the fixed contract
//! shape and refuses to rewrite an ambiguous or malformed document.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;

use crate::state::archive_header_task_id;
use crate::work_fs::{self, MAX_CONTROL_BYTES};

const ROADMAP_FILE: &str = "roadmap.md";
const CURRENT_STATE_HEADING: &str = "## Текущее состояние";
const MILESTONES_HEADING: &str = "## Вехи";

/// The one active milestone that a valid roadmap exposes to the processor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMilestone {
    pub id: String,
    pub title: String,
    pub tasks: BTreeSet<String>,
}

/// Read-only completion relation between the active milestone and `Tasks_Done.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MilestoneProgress {
    /// No optional roadmap artifact is configured for this project.
    Absent,
    /// A current milestone exists but has no task links yet (`Задачи: —`).
    NoTasks { milestone: CurrentMilestone },
    /// At least one linked task has not been published and archived.
    Incomplete {
        milestone: CurrentMilestone,
        remaining: BTreeSet<String>,
    },
    /// Every nonempty linked task id has an authoritative archive header in `Tasks_Done.md`.
    Complete { milestone: CurrentMilestone },
}

/// Result of the one authorized roadmap mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressWrite {
    Absent,
    NotReady,
    Unchanged,
    Updated,
}

#[derive(Debug)]
pub enum RoadmapError {
    Io(io::Error),
    Malformed(String),
}

impl fmt::Display for RoadmapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "roadmap I/O error: {error}"),
            Self::Malformed(message) => write!(f, "invalid roadmap: {message}"),
        }
    }
}

impl std::error::Error for RoadmapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Malformed(_) => None,
        }
    }
}

impl From<io::Error> for RoadmapError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, RoadmapError>;

/// Inspect the exact optional artifact under the selected `.work` directory.  Completion uses
/// the same archive-header predicate as queue readiness, so a descriptor merely marked
/// `опубликована` never advances a roadmap signal.
pub fn current_progress(work: &Path) -> Result<MilestoneProgress> {
    let Some((text, document)) = load_document(work)? else {
        return Ok(MilestoneProgress::Absent);
    };
    let completed = completed_ids(work)?;
    Ok(progress_for(&text, &document, &completed))
}

/// Return the exact operator-facing message required when a cohort closes because the queue is
/// empty while its current roadmap milestone is still waiting for linked work.  A malformed or
/// absent roadmap is deliberately not converted into a fictional message.
pub fn queue_empty_waiting_note(work: &Path) -> Result<Option<String>> {
    match current_progress(work)? {
        MilestoneProgress::Incomplete { milestone, .. } => Ok(Some(format!(
            "очередь пуста; веха {} ещё не достигнута — ожидается следующая порция задач",
            milestone.id
        ))),
        MilestoneProgress::Absent
        | MilestoneProgress::NoTasks { .. }
        | MilestoneProgress::Complete { .. } => Ok(None),
    }
}

/// After Phase 6 has archived the cohort's published tasks, write the derived progress signal
/// only when the current milestone's nonempty task list is fully archived.  The replacement is
/// confined to `## Текущее состояние`; milestone headers, task links, statuses, and achievement
/// declarations remain byte-for-byte untouched.
pub fn write_completion_progress(work: &Path) -> Result<ProgressWrite> {
    let Some((text, document)) = load_document(work)? else {
        return Ok(ProgressWrite::Absent);
    };
    let completed = completed_ids(work)?;
    let MilestoneProgress::Complete { milestone } = progress_for(&text, &document, &completed)
    else {
        return Ok(ProgressWrite::NotReady);
    };

    let eol = if text.contains("\r\n") { "\r\n" } else { "\n" };
    let summary = completion_summary(&milestone, eol);
    if text[document.current_state_body.clone()] == summary {
        return Ok(ProgressWrite::Unchanged);
    }
    let mut updated = String::with_capacity(text.len() + summary.len());
    updated.push_str(&text[..document.current_state_body.start]);
    updated.push_str(&summary);
    updated.push_str(&text[document.current_state_body.end..]);
    work_fs::replace_file(
        work,
        &work.join(ROADMAP_FILE),
        updated.as_bytes(),
        MAX_CONTROL_BYTES,
    )
    .map_err(RoadmapError::Io)?;
    Ok(ProgressWrite::Updated)
}

fn completion_summary(milestone: &CurrentMilestone, eol: &str) -> String {
    format!(
        "Текущая веха: {} — {}{eol}{eol}По {} все поставленные задачи находятся в Tasks_Done.md; критерий Достижение ждёт подтверждения оператором.{eol}{eol}",
        milestone.id, milestone.title, milestone.id
    )
}

fn completed_ids(work: &Path) -> Result<BTreeSet<String>> {
    let path = work.join("Tasks_Done.md");
    match work_fs::read_optional_text(work, &path, MAX_CONTROL_BYTES)? {
        Some(text) => Ok(text
            .lines()
            .filter_map(archive_header_task_id)
            .map(str::to_owned)
            .collect()),
        None => Ok(BTreeSet::new()),
    }
}

fn progress_for(
    _text: &str,
    document: &Document,
    completed: &BTreeSet<String>,
) -> MilestoneProgress {
    let milestone = document.current.clone();
    if milestone.tasks.is_empty() {
        return MilestoneProgress::NoTasks { milestone };
    }
    let remaining = milestone
        .tasks
        .iter()
        .filter(|task_id| !completed.contains(*task_id))
        .cloned()
        .collect::<BTreeSet<_>>();
    if remaining.is_empty() {
        MilestoneProgress::Complete { milestone }
    } else {
        MilestoneProgress::Incomplete {
            milestone,
            remaining,
        }
    }
}

#[derive(Debug, Clone)]
struct Document {
    current_state_body: Range<usize>,
    current: CurrentMilestone,
}

/// Read one exact optional path; no ambient discovery is ever permitted for a roadmap artifact.
fn load_document(work: &Path) -> Result<Option<(String, Document)>> {
    let path = work.join(ROADMAP_FILE);
    let text = match work_fs::read_optional_text(work, &path, MAX_CONTROL_BYTES)? {
        Some(text) => text,
        None => return Ok(None),
    };
    let document = parse_document(&text)?;
    Ok(Some((text, document)))
}

fn parse_document(text: &str) -> Result<Document> {
    let lines = lines(text);
    let current_state = unique_heading(&lines, CURRENT_STATE_HEADING)?;
    let milestones = unique_heading(&lines, MILESTONES_HEADING)?;
    if current_state >= milestones {
        return Err(RoadmapError::Malformed(format!(
            "{CURRENT_STATE_HEADING:?} must precede {MILESTONES_HEADING:?}"
        )));
    }
    if lines[current_state].end == lines[current_state].start
        || (lines[current_state].end < text.len()
            && !text[lines[current_state].end - 1..].starts_with('\n'))
    {
        return Err(RoadmapError::Malformed(
            "current-state heading must end with a line break".into(),
        ));
    }

    let milestone_lines = &lines[milestones + 1..];
    let mut current_headers = Vec::new();
    for (offset, line) in milestone_lines.iter().enumerate() {
        if line.text.starts_with("## ") {
            break;
        }
        if let Some((id, title)) = parse_current_header(line.text)? {
            current_headers.push((milestones + 1 + offset, id, title));
        }
    }
    let [(header_index, id, title)] = current_headers.as_slice() else {
        return Err(RoadmapError::Malformed(
            "there must be exactly one current milestone in ## Вехи".into(),
        ));
    };
    let body_end = lines
        .iter()
        .enumerate()
        .skip(*header_index + 1)
        .find_map(|(index, line)| line.text.starts_with("### ").then_some(index))
        .or_else(|| {
            lines
                .iter()
                .enumerate()
                .skip(*header_index + 1)
                .find_map(|(index, line)| line.text.starts_with("## ").then_some(index))
        })
        .unwrap_or(lines.len());
    let tasks = milestone_tasks(&lines[*header_index + 1..body_end])?;

    Ok(Document {
        current_state_body: lines[current_state].end..lines[milestones].start,
        current: CurrentMilestone {
            id: id.clone(),
            title: title.clone(),
            tasks,
        },
    })
}

fn unique_heading(lines: &[Line<'_>], heading: &str) -> Result<usize> {
    let matches = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| (line.text == heading).then_some(index))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(RoadmapError::Malformed(format!(
            "missing required heading {heading:?}"
        ))),
        _ => Err(RoadmapError::Malformed(format!(
            "required heading {heading:?} appears more than once"
        ))),
    }
}

fn parse_current_header(line: &str) -> Result<Option<(String, String)>> {
    if !line.starts_with("### [") || !line.contains("статус: текущая") {
        return Ok(None);
    }
    let rest = &line[5..];
    let close = rest.find(']').ok_or_else(|| {
        RoadmapError::Malformed(format!(
            "current milestone header has no closing ']': {line:?}"
        ))
    })?;
    let id = &rest[..close];
    if !valid_milestone_id(id) {
        return Err(RoadmapError::Malformed(format!(
            "current milestone has invalid id {id:?}"
        )));
    }
    let tail = rest[close + 1..].strip_prefix(' ').ok_or_else(|| {
        RoadmapError::Malformed(format!("invalid current milestone header {line:?}"))
    })?;
    let (title, status) = tail.rsplit_once(" — статус: ").ok_or_else(|| {
        RoadmapError::Malformed(format!("invalid current milestone header {line:?}"))
    })?;
    if title.trim().is_empty() || status != "текущая" {
        return Err(RoadmapError::Malformed(format!(
            "invalid current milestone header {line:?}"
        )));
    }
    Ok(Some((id.to_owned(), title.to_owned())))
}

fn milestone_tasks(lines: &[Line<'_>]) -> Result<BTreeSet<String>> {
    let task_lines = lines
        .iter()
        .filter_map(|line| {
            line.text
                .strip_prefix("Задачи:")
                .or_else(|| line.text.strip_prefix("Tasks:"))
        })
        .collect::<Vec<_>>();
    let [raw] = task_lines.as_slice() else {
        return Err(RoadmapError::Malformed(
            "current milestone must contain exactly one Задачи:/Tasks: line".into(),
        ));
    };
    let raw = raw.trim();
    if raw == "—" {
        return Ok(BTreeSet::new());
    }
    if raw.is_empty() {
        return Err(RoadmapError::Malformed(
            "current milestone task list is empty; use — when it has no tasks".into(),
        ));
    }
    let mut tasks = BTreeSet::new();
    for task in raw.split(',').map(str::trim) {
        if !valid_task_id(task) || !tasks.insert(task.to_owned()) {
            return Err(RoadmapError::Malformed(format!(
                "current milestone has invalid or duplicate task id {task:?}"
            )));
        }
    }
    Ok(tasks)
}

fn valid_milestone_id(id: &str) -> bool {
    id.strip_prefix('M').is_some_and(|number| {
        !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn valid_task_id(id: &str) -> bool {
    id.strip_prefix("T-").is_some_and(|number| {
        !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
    })
}

#[derive(Debug, Clone, Copy)]
struct Line<'a> {
    start: usize,
    end: usize,
    text: &'a str,
}

fn lines(text: &str) -> Vec<Line<'_>> {
    let mut output = Vec::new();
    let mut start = 0;
    for segment in text.split_inclusive('\n') {
        let end = start + segment.len();
        output.push(Line {
            start,
            end,
            text: segment.trim_end_matches(['\r', '\n']),
        });
        start = end;
    }
    if start < text.len() || text.is_empty() {
        output.push(Line {
            start,
            end: text.len(),
            text: &text[start..],
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct Work {
        path: std::path::PathBuf,
    }

    impl Work {
        fn new() -> Self {
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "orchestrail-roadmap-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn write(&self, file: &str, text: &str) {
            fs::write(self.path.join(file), text).unwrap();
        }
    }

    impl Drop for Work {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn roadmap(tasks: &str) -> String {
        format!(
            "# Дорожная карта проекта\n\n## Текущее состояние\nТекущая веха: M2 — API\nСтарый прогресс\n\n## Вехи\n### [M1] Foundation — статус: достигнута\nЦель: base\nДостижение: base exists\nЗадачи: T-1\n\n### [M2] API — статус: текущая\nЦель: endpoint\nДостижение: published API exists\nЗадачи: {tasks}\n\n### [M3] Release — статус: запланирована\nЦель: release\nДостижение: shipped\nЗадачи: —\n"
        )
    }

    #[test]
    fn absent_roadmap_is_a_no_op() {
        let work = Work::new();
        assert_eq!(
            current_progress(&work.path).unwrap(),
            MilestoneProgress::Absent
        );
        assert_eq!(
            write_completion_progress(&work.path).unwrap(),
            ProgressWrite::Absent
        );
    }

    #[test]
    fn incomplete_current_milestone_produces_the_queue_empty_note_without_rewrite() {
        let work = Work::new();
        let original = roadmap("T-10, T-11");
        work.write(ROADMAP_FILE, &original);
        work.write("Tasks_Done.md", "## [T-10] done\n");

        assert!(matches!(
            current_progress(&work.path).unwrap(),
            MilestoneProgress::Incomplete { ref remaining, .. } if remaining == &BTreeSet::from(["T-11".to_string()])
        ));
        assert_eq!(
            queue_empty_waiting_note(&work.path).unwrap().as_deref(),
            Some("очередь пуста; веха M2 ещё не достигнута — ожидается следующая порция задач")
        );
        assert_eq!(
            write_completion_progress(&work.path).unwrap(),
            ProgressWrite::NotReady
        );
        assert_eq!(
            fs::read_to_string(work.path.join(ROADMAP_FILE)).unwrap(),
            original
        );
    }

    #[test]
    fn completion_replaces_only_current_state_and_is_idempotent() {
        let work = Work::new();
        let original = roadmap("T-10, T-11");
        work.write(ROADMAP_FILE, &original);
        work.write("Tasks_Done.md", "## [T-10] done\n### [T-11] done\n");

        assert_eq!(
            write_completion_progress(&work.path).unwrap(),
            ProgressWrite::Updated
        );
        let updated = fs::read_to_string(work.path.join(ROADMAP_FILE)).unwrap();
        assert!(updated.contains("Текущая веха: M2 — API\n\nПо M2 все поставленные задачи находятся в Tasks_Done.md; критерий Достижение ждёт подтверждения оператором."));
        assert!(updated.contains("### [M2] API — статус: текущая\nЦель: endpoint\nДостижение: published API exists\nЗадачи: T-10, T-11"));
        assert!(updated.contains("### [M3] Release — статус: запланирована"));
        assert_eq!(
            write_completion_progress(&work.path).unwrap(),
            ProgressWrite::Unchanged
        );
        assert_eq!(
            fs::read_to_string(work.path.join(ROADMAP_FILE)).unwrap(),
            updated
        );
    }

    #[test]
    fn zero_task_milestone_does_not_claim_completion() {
        let work = Work::new();
        work.write(ROADMAP_FILE, &roadmap("—"));
        assert!(matches!(
            current_progress(&work.path).unwrap(),
            MilestoneProgress::NoTasks { .. }
        ));
        assert_eq!(
            write_completion_progress(&work.path).unwrap(),
            ProgressWrite::NotReady
        );
        assert_eq!(queue_empty_waiting_note(&work.path).unwrap(), None);
    }

    #[test]
    fn malformed_roadmap_is_never_rewritten() {
        let work = Work::new();
        let original = roadmap("T-10").replace(
            "### [M2] API — статус: текущая",
            "### [M2] API — статус: текущая\n### [M4] Duplicate — статус: текущая",
        );
        work.write(ROADMAP_FILE, &original);
        work.write("Tasks_Done.md", "## [T-10] done\n");
        assert!(matches!(
            write_completion_progress(&work.path),
            Err(RoadmapError::Malformed(_))
        ));
        assert_eq!(
            fs::read_to_string(work.path.join(ROADMAP_FILE)).unwrap(),
            original
        );
    }

    #[test]
    fn archive_body_mentions_do_not_satisfy_task_ids() {
        let work = Work::new();
        work.write(ROADMAP_FILE, &roadmap("T-10"));
        work.write("Tasks_Done.md", "## [T-11] done\nbody mentions T-10\n");
        assert!(matches!(
            current_progress(&work.path).unwrap(),
            MilestoneProgress::Incomplete { .. }
        ));
    }
}
