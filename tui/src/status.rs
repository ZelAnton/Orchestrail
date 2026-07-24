//! Read `.work/status.md` as *additional human context* for the overview screen.
//!
//! The event stream (`events.jsonl`) is the authoritative source for the batch/cohort/task
//! projection; `status.md` supplies things that are not unambiguously derivable from events —
//! most usefully the human task *names* (events carry only ids), plus the orchestrator's own
//! one-line summary and the freshest snapshot timestamp. Parsing is deliberately lenient: a
//! missing / malformed / partially-written file yields `None` or a best-effort partial snapshot,
//! never an error that could crash the TUI.
//!
//! **Read-only:** this module only ever *reads* the file. It never writes, locks, or creates it.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// Human metadata for one task, lifted from the status.md table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskMeta {
    pub name: Option<String>,
    pub agent: Option<String>,
    pub phase: Option<String>,
    pub branch: Option<String>,
    pub worktree: Option<String>,
}

/// A parsed snapshot of `.work/status.md`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusSnapshot {
    /// The "Обновлено:" timestamp bullet, if present.
    pub updated: Option<String>,
    /// The bullet context lines under the heading (orchestrator stage, batch line, codex, …).
    pub context_lines: Vec<String>,
    /// task_id -> human metadata from the table.
    pub task_meta: BTreeMap<String, TaskMeta>,
}

/// Load and parse `path` (e.g. `.work/status.md`). Returns `None` if the file is absent or
/// unreadable — the TUI degrades to the event-only projection.
pub fn load(path: &Path) -> Option<StatusSnapshot> {
    let text = fs::read_to_string(path).ok()?;
    Some(parse(&text))
}

/// Pure parse of status.md content (separated from IO for testability).
pub fn parse(text: &str) -> StatusSnapshot {
    let mut snap = StatusSnapshot::default();
    for raw in text.lines() {
        let line = raw.trim();
        if let Some(rest) = line.strip_prefix("- ") {
            let rest = rest.trim();
            if let Some(v) = rest.strip_prefix("Обновлено:") {
                snap.updated = Some(v.trim().to_string());
            }
            // Keep the informative bullets (skip an empty one).
            if !rest.is_empty() {
                snap.context_lines.push(rest.to_string());
            }
            continue;
        }
        if line.starts_with('|')
            && let Some((id, meta)) = parse_table_row(line)
        {
            snap.task_meta.insert(id, meta);
        }
    }
    snap
}

/// Parse one markdown table row into `(task_id, meta)` when the first cell is a T-id. The header
/// row (`| Задача | Название | … |`) and the separator row (`|---|---|`) are skipped because their
/// first cell is not a T-id.
fn parse_table_row(line: &str) -> Option<(String, TaskMeta)> {
    // Split on '|', dropping the empty leading/trailing cells from the border pipes.
    let cells: Vec<String> = line
        .trim()
        .trim_matches('|')
        .split('|')
        .map(|c| c.trim().to_string())
        .collect();
    let id = cells.first()?.clone();
    if !is_task_id(&id) {
        return None;
    }
    let cell = |i: usize| cells.get(i).cloned().filter(|s| !s.is_empty());
    Some((
        id,
        TaskMeta {
            name: cell(1),
            agent: cell(2),
            phase: cell(3),
            branch: cell(4),
            worktree: cell(5),
        },
    ))
}

/// `^T-\d` — matches the id shape used across the queue/journal.
fn is_task_id(s: &str) -> bool {
    match s.strip_prefix("T-") {
        Some(rest) => rest.chars().next().is_some_and(|c| c.is_ascii_digit()),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A snapshot in the real `.work/status.md` shape.
    const SAMPLE: &str = "# Оркестр — обзор\n\
- Обновлено: 2026-07-11T11:39:48Z\n\
- Оркестратор: processor — этап: исполнение когорты (реализация)\n\
- Батч: B-20260711T113948Z · активно 2 / cap 5 · приём: открыт (волна 1)\n\
- Codex: CODEX_CODER=fast+std · permission=ok\n\
\n\
| Задача | Название | Агент | Этап | Ветка | Worktree |\n\
|--------|----------|-------|------|-------|----------|\n\
| T-102  | Превратить tui/ в живой обзорный экран | coder_deep | реализация | task/T-102 | .work/worktrees/T-102 |\n\
| T-103  | Дать движку read-only снимок control-plane | coder_deep | реализация | task/T-103 | .work/worktrees/T-103 |\n";

    #[test]
    fn parses_updated_and_context_bullets() {
        let s = parse(SAMPLE);
        assert_eq!(s.updated.as_deref(), Some("2026-07-11T11:39:48Z"));
        assert!(
            s.context_lines
                .iter()
                .any(|l| l.contains("Оркестратор: processor"))
        );
        assert!(s.context_lines.iter().any(|l| l.starts_with("Батч:")));
    }

    #[test]
    fn parses_task_table_rows_only() {
        let s = parse(SAMPLE);
        assert_eq!(
            s.task_meta.len(),
            2,
            "header + separator rows must be skipped"
        );
        let t102 = s.task_meta.get("T-102").expect("T-102");
        assert_eq!(
            t102.name.as_deref(),
            Some("Превратить tui/ в живой обзорный экран")
        );
        assert_eq!(t102.agent.as_deref(), Some("coder_deep"));
        assert_eq!(t102.phase.as_deref(), Some("реализация"));
        assert_eq!(t102.branch.as_deref(), Some("task/T-102"));
        assert!(s.task_meta.contains_key("T-103"));
    }

    #[test]
    fn empty_or_headingless_input_is_harmless() {
        let s = parse("");
        assert_eq!(s, StatusSnapshot::default());
        let s2 = parse("just some prose\nno table here\n");
        assert!(s2.task_meta.is_empty());
        assert!(s2.updated.is_none());
    }

    #[test]
    fn ignores_non_task_table_rows() {
        let s = parse("| Provider | Model |\n|---|---|\n| codex | fast |\n");
        assert!(s.task_meta.is_empty());
    }
}
