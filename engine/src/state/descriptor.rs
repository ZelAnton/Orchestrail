//! Parse a task descriptor `.work/tasks/<T-ID>/task.md` — its `Статус:` field (§13.1),
//! `Предпосылки:` list, and planner-provided `Конфликт-домен:` globs — and enumerate all
//! descriptors under `.work/tasks/`.
//!
//! The descriptor is the processor-owned per-task lifecycle record; its `Статус:` is the
//! authoritative task state once a task is captured (`не начата` lives only in the queue, before
//! a descriptor exists — §13.1). Read-only: files are opened for reading only.

use std::io;
use std::path::Path;

use crate::resolvers::{Level, NetworkNeed, Risk, network_need};
use crate::work_fs::{self, MAX_CONTROL_BYTES};

use super::canonical::TaskState;
use super::util::{line_field, parse_task_id_list};

/// One decoded `task.md` descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descriptor {
    /// The T-ID (the descriptor directory name).
    pub id: String,
    /// Canonical state from `Статус:`, or `None` if absent/unrecognized.
    pub state: Option<TaskState>,
    /// The raw `Статус:` literal, when present.
    pub status_literal: Option<String>,
    /// T-ids from the descriptor's `Предпосылки:` line.
    pub prerequisites: Vec<String>,
    /// Path globs from `Конфликт-домен:`. `None` means the field was absent or malformed, so a
    /// caller admitting work must conservatively treat this task as conflicting with everything.
    pub conflict_domain: Option<Vec<String>>,
    /// Planner-selected executor level. Missing or unknown is intentionally `None`: planner
    /// routing must fail closed rather than silently upgrade/downgrade the task after a restart.
    pub level: Option<Level>,
    /// Planner-provided informational blast-radius classification. Missing or malformed risk is
    /// `None`; the live headless planner requires it before admitting a fresh descriptor, while
    /// recovery continues to read older legacy descriptors without fabricating a value.
    pub risk: Option<Risk>,
    /// An explicit network requirement and its ecosystem, decoded from the planner-owned
    /// `Сеть:` / `Экосистема:` pair.  It is retained so every implementation and R-fix route
    /// can apply the Codex network gate from the same durable descriptor after resume.
    pub network: Option<NetworkNeed>,
    /// The batch that captured this task, when the descriptor has crossed the capture
    /// transaction.  Recovery must never infer this from a directory name: a descriptor can
    /// survive a crash before its queue label or batch line is updated.
    pub batch_id: Option<String>,
    /// Persistent task branch recorded by the planner/capture transaction.
    pub branch: Option<String>,
    /// Persistent managed worktree path recorded by the capture transaction.
    pub worktree: Option<String>,
    /// Ordered authors of committed implementation/fix ranges. The last value is the only
    /// valid input to maker/checker re-election after a restart.
    pub implementation_authors: Vec<String>,
    /// The task tip that has already passed a complete reviewer protocol, if any.
    pub review_sha: Option<String>,
    /// Durable completed-review counter. A missing value means that no reviewer pass was
    /// recorded yet; it must not be fabricated as zero during recovery.
    pub review_cycles: Option<u32>,
}

/// Decode one planner-provided conflict-domain into individual relative glob/path patterns.
/// A descriptor with an absent, empty, or non-path-shaped field stays unknown so admission fails
/// closed rather than packing it as conflict-free.
pub(crate) fn parse_conflict_domain(value: &str) -> Option<Vec<String>> {
    let globs: Vec<String> = value
        .split([',', ' ', '\t'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();

    (!globs.is_empty()
        && globs.iter().all(|glob| {
            !glob.starts_with(['/', '\\'])
                && !glob.contains(':')
                && !glob.contains(['<', '>'])
                && !glob.split(['/', '\\']).any(|part| part == "..")
        }))
    .then_some(globs)
}

/// The persistent review-cycle counter `Циклов-ревью: N` a task's descriptor carries once it
/// enters the review fix cycle (`agents/processor.md` phases 2.5 / 2.8; `REVIEW_LOOP_MAX`). It is
/// the inverse of the writer the engine's review round emits, and — like the queue `попытка=N` and
/// the cohort wave — is the durable coordinate `docs/queue_contract.md` §19 reconstructs the
/// per-cycle `task.status_changed` event fingerprint from. Absent / non-numeric reads as `None`
/// (a task that has not yet been reviewed carries no counter), never an error.
pub fn parse_review_cycles(text: &str) -> Option<u32> {
    line_field(text, "Циклов-ревью:")?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Decode one descriptor's Markdown text under the given `id`.
pub fn parse_descriptor(id: &str, text: &str) -> Descriptor {
    let status_literal = line_field(text, "Статус:").map(str::to_string);
    let state = status_literal.as_deref().and_then(TaskState::from_markdown);
    let prerequisites = line_field(text, "Предпосылки:")
        .map(parse_task_id_list)
        .unwrap_or_default();
    let conflict_domain = line_field(text, "Конфликт-домен:").and_then(parse_conflict_domain);
    let level = line_field(text, "Рекомендуемый исполнитель:").and_then(Level::from_field);
    let risk = line_field(text, "Риск:").and_then(Risk::from_field);
    let network = network_need(line_field(text, "Сеть:"), line_field(text, "Экосистема:"));
    let implementation_authors = line_field(text, "Реализовано:")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|author| !author.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Descriptor {
        id: id.to_string(),
        state,
        status_literal,
        prerequisites,
        conflict_domain,
        level,
        risk,
        network,
        batch_id: line_field(text, "Батч:").map(str::to_string),
        branch: line_field(text, "Ветка:").map(str::to_string),
        worktree: line_field(text, "Worktree:").map(str::to_string),
        implementation_authors,
        review_sha: line_field(text, "Ревью-SHA:").map(str::to_string),
        review_cycles: parse_review_cycles(text),
    }
}

/// Enumerate every `<work_dir>/tasks/<id>/task.md`, in id order. Directories without a `task.md`
/// (e.g. `_integration`, which carries only `status.md`) are skipped. A missing `tasks/`
/// directory reads as an empty list; other I/O errors are returned so a control-plane decision
/// cannot mistake an unreadable live descriptor set for an idle one.
pub fn try_load_descriptors(work_dir: &Path) -> io::Result<Vec<Descriptor>> {
    let tasks_dir = work_dir.join("tasks");
    let Some(entries) = work_fs::plain_directory_entries(work_dir, &tasks_dir)? else {
        return Ok(Vec::new());
    };
    let mut dirs = entries
        .into_iter()
        .filter_map(|entry| {
            let path = entry.path();
            match work_fs::require_plain_directory(&path) {
                Ok(()) => Some(Ok(entry.file_name())),
                Err(_error) if entry.file_type().is_ok_and(|kind| !kind.is_dir()) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect::<io::Result<Vec<_>>>()?;
    dirs.sort();

    let mut out = Vec::new();
    for name in dirs {
        let id = name.to_string_lossy().to_string();
        let task_md = tasks_dir.join(&name).join("task.md");
        // A task directory can disappear during a read-only observer's scan after a completed
        // archive. Treat that ordinary race like an absent descriptor.
        if let Some(text) = work_fs::read_optional_text(work_dir, &task_md, MAX_CONTROL_BYTES)? {
            out.push(parse_descriptor(&id, &text));
        }
    }
    Ok(out)
}

/// Best-effort compatibility loader for passive observers. Production decision paths use
/// [`try_load_descriptors`] so unreadable state fails closed rather than becoming an empty list.
pub fn load_descriptors(work_dir: &Path) -> Vec<Descriptor> {
    try_load_descriptors(work_dir).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DESC: &str = "# Активная задача T-103\n\n\
Статус: в работе\n\
Исходная задача: [T-103] Дать движку read-only снимок control-plane\n\
Батч: B-20260711T113948Z\n\
Предпосылки: T-101\n\n\
## Критерии выполнения\n- ...\n";

    #[test]
    fn reads_status_and_prerequisites() {
        let d = parse_descriptor("T-103", DESC);
        assert_eq!(d.id, "T-103");
        assert_eq!(d.state, Some(TaskState::Working));
        assert_eq!(d.status_literal.as_deref(), Some("в работе"));
        assert_eq!(d.prerequisites, vec!["T-101"]);
        assert_eq!(d.batch_id.as_deref(), Some("B-20260711T113948Z"));
    }

    #[test]
    fn every_lifecycle_literal_is_recognized() {
        for (lit, st) in [
            ("на ревью", TaskState::InReview),
            ("готова к слиянию", TaskState::Ready),
            ("слита", TaskState::Merged),
            ("опубликована", TaskState::Published),
            ("выполнена", TaskState::Done),
            ("конфликт", TaskState::Conflict),
        ] {
            let text = format!("# T\nСтатус: {lit}\n");
            assert_eq!(parse_descriptor("T-1", &text).state, Some(st));
        }
    }

    #[test]
    fn missing_status_is_none_not_error() {
        let d = parse_descriptor("T-9", "# T-9\nБатч: B-1\n");
        assert_eq!(d.state, None);
        assert_eq!(d.status_literal, None);
        assert!(d.prerequisites.is_empty());
    }

    #[test]
    fn review_cycles_counter_parses_and_is_optional() {
        // Present + numeric: the review round wrote `Циклов-ревью: N`.
        let text = "# T-1\nСтатус: на ревью\nЦиклов-ревью: 3\n";
        assert_eq!(parse_review_cycles(text), Some(3));
        // A trailing inline note after the number is tolerated (first token wins).
        let noted = "# T-1\nСтатус: на ревью\nЦиклов-ревью: 2 (fix cycle)\n";
        assert_eq!(parse_review_cycles(noted), Some(2));
        // Absent (a not-yet-reviewed task) or non-numeric reads as None, never an error.
        assert_eq!(parse_review_cycles("# T-1\nСтатус: в работе\n"), None);
        assert_eq!(parse_review_cycles("# T-1\nЦиклов-ревью: many\n"), None);
        // Adding the counter does not disturb the existing descriptor fields.
        let d = parse_descriptor("T-1", text);
        assert_eq!(d.state, Some(TaskState::InReview));
        assert_eq!(d.status_literal.as_deref(), Some("на ревью"));
        assert_eq!(d.review_cycles, Some(3));
    }

    #[test]
    fn network_requirement_is_preserved_for_resumed_codex_routing() {
        let d = parse_descriptor(
            "T-1",
            "# T-1\nСтатус: в работе\nСеть: требуется\nЭкосистема: nuget\n",
        );
        assert_eq!(
            d.network,
            Some(NetworkNeed {
                ecosystem: crate::resolvers::Ecosystem::Managed,
            })
        );
        assert_eq!(
            parse_descriptor("T-2", "# T-2\nСеть: не требуется\nЭкосистема: cargo\n").network,
            None
        );
    }

    #[test]
    fn planner_risk_is_preserved_with_its_human_explanation() {
        let d = parse_descriptor(
            "T-1",
            "# T-1\nСтатус: не начата\nРиск: high — public API and auth boundary\n",
        );
        assert_eq!(d.risk, Some(Risk::High));
        assert_eq!(
            parse_descriptor("T-2", "# T-2\nРиск: severe\n").risk,
            None,
            "unknown risk vocabulary must not be admitted as a planner classification"
        );
    }

    #[test]
    fn recovery_coordinates_keep_the_persisted_review_and_workspace_fields() {
        let d = parse_descriptor(
            "T-1",
            "# T-1\nСтатус: на ревью\nБатч: B-1\nВетка: task/T-1\nWorktree: .work/worktrees/T-1\nРеализовано: coder, coder_codex\nРевью-SHA: abc123\nЦиклов-ревью: 2\n",
        );
        assert_eq!(d.branch.as_deref(), Some("task/T-1"));
        assert_eq!(d.worktree.as_deref(), Some(".work/worktrees/T-1"));
        assert_eq!(d.implementation_authors, ["coder", "coder_codex"]);
        assert_eq!(d.review_sha.as_deref(), Some("abc123"));
        assert_eq!(d.review_cycles, Some(2));
    }

    #[test]
    fn load_descriptors_missing_tasks_dir_is_empty() {
        let dir = std::env::temp_dir().join("orchestra-state-no-such-work-xyz");
        assert!(load_descriptors(&dir).is_empty());
    }
}
