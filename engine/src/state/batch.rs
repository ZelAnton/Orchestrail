//! Parse `.work/batch.md` — the current batch manifest (base, integration branch, admitted
//! tasks with their level / branch / worktree / domain / wave).
//!
//! The file (created in Phase 1, removed in 6.4) is append-only over its `## Задачи` list; each
//! task line is `- [T-NNN] уровень=… ветка=… worktree=… домен=… волна=…`. Its **absence** is the
//! normal idle state (no active batch) and reads as `None`, not an error. Read-only.

use std::io;
use std::path::Path;

use crate::work_fs::{self, MAX_CONTROL_BYTES};

use super::util::{find_batch_id, line_field};
use crate::task_id::is_task_id;

/// One admitted task line from `## Задачи`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchTask {
    pub id: String,
    pub level: Option<String>,
    pub branch: Option<String>,
    pub worktree: Option<String>,
    pub domain: Option<String>,
    pub wave: Option<u32>,
}

/// Decoded batch manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchState {
    pub batch_id: Option<String>,
    pub base: Option<String>,
    pub integration_branch: Option<String>,
    pub tasks: Vec<BatchTask>,
}

/// Decode `batch.md` text.
pub fn parse_batch(text: &str) -> BatchState {
    BatchState {
        batch_id: find_batch_id(text),
        base: line_field(text, "База:").map(str::to_string),
        integration_branch: line_field(text, "Интеграционная ветка:").map(str::to_string),
        tasks: text.lines().filter_map(parse_task_line).collect(),
    }
}

/// Load `<work_dir>/batch.md`. `None` = file absent = no active batch; an existing but unreadable
/// file is an error.
pub fn try_load_batch(work_dir: &Path) -> io::Result<Option<BatchState>> {
    let path = work_dir.join("batch.md");
    Ok(
        work_fs::read_optional_text(work_dir, &path, MAX_CONTROL_BYTES)?
            .map(|text| parse_batch(&text)),
    )
}

/// Best-effort compatibility loader for passive observers; control-plane decisions use
/// [`try_load_batch`].
pub fn load_batch(work_dir: &Path) -> Option<BatchState> {
    try_load_batch(work_dir).ok().flatten()
}

/// Decode one `- [T-NNN] key=value …` task line; `None` for any other line.
fn parse_task_line(line: &str) -> Option<BatchTask> {
    let rest = line.trim().strip_prefix('-')?.trim_start();
    let rest = rest.strip_prefix('[')?;
    let close = rest.find(']')?;
    let id = rest[..close].trim().to_string();
    if !is_task_id(&id) {
        return None;
    }
    let fields = &rest[close + 1..];
    let field = |key: &str| {
        fields
            .split_whitespace()
            .find_map(|t| t.strip_prefix(key).map(String::from))
    };
    Some(BatchTask {
        id,
        level: field("уровень="),
        branch: field("ветка="),
        worktree: field("worktree="),
        domain: field("домен="),
        wave: field("волна=").and_then(|v| v.parse().ok()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const BATCH: &str = "# Batch B-20260711T113948Z\n\
База: 4851279ec958efd095b5c01fa608f9d38f15bc29\n\
Интеграционная ветка: integration/B-20260711T113948Z\n\n\
## Задачи\n\
- [T-102] уровень=coder_deep ветка=task/T-102 worktree=.work/worktrees/T-102 домен=tui/** волна=1\n\
- [T-103] уровень=coder_deep ветка=task/T-103 worktree=.work/worktrees/T-103 домен=engine/src/state/**,engine/src/lib.rs волна=1\n";

    #[test]
    fn parses_header_fields() {
        let b = parse_batch(BATCH);
        assert_eq!(b.batch_id.as_deref(), Some("B-20260711T113948Z"));
        assert_eq!(
            b.base.as_deref(),
            Some("4851279ec958efd095b5c01fa608f9d38f15bc29")
        );
        assert_eq!(
            b.integration_branch.as_deref(),
            Some("integration/B-20260711T113948Z")
        );
    }

    #[test]
    fn parses_task_lines_with_all_fields() {
        let b = parse_batch(BATCH);
        assert_eq!(b.tasks.len(), 2);
        let t = &b.tasks[0];
        assert_eq!(t.id, "T-102");
        assert_eq!(t.level.as_deref(), Some("coder_deep"));
        assert_eq!(t.branch.as_deref(), Some("task/T-102"));
        assert_eq!(t.worktree.as_deref(), Some(".work/worktrees/T-102"));
        assert_eq!(t.domain.as_deref(), Some("tui/**"));
        assert_eq!(t.wave, Some(1));
        // Comma-separated domain with no spaces survives whitespace tokenization.
        assert_eq!(
            b.tasks[1].domain.as_deref(),
            Some("engine/src/state/**,engine/src/lib.rs")
        );
    }

    #[test]
    fn absent_file_is_no_active_batch() {
        let dir = std::env::temp_dir().join("orchestra-state-no-batch-xyz");
        assert!(load_batch(&dir).is_none());
    }
}
