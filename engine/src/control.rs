//! Native, fail-closed mutation boundary for the `.work` control plane.
//!
//! The legacy processor delegated every queue and descriptor mutation to PowerShell scripts.
//! The deterministic engine keeps that data model, but owns the exact mutations here: callers
//! provide typed state and an owner lease has already serialized them.  No operation scans or
//! rewrites unrelated task blocks, and every file replacement is same-directory atomic.

use std::io;
use std::path::{Path, PathBuf};

use crate::processor::CloseReasonWire;
use crate::resolvers::Risk;
use crate::roadmap;
use crate::state::{Snapshot, TaskState, archive_header_task_id};
use crate::telemetry::{BatchTelemetrySummary, batch_telemetry_summary};
use crate::work_fs;

const QUEUE_FILE: &str = "Tasks_Queue.md";
const DONE_FILE: &str = "Tasks_Done.md";
const MAX_CONTROL_ARTIFACT_BYTES: u64 = work_fs::MAX_CONTROL_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorPatch {
    pub state: TaskState,
    pub batch_id: Option<String>,
    pub branch: Option<String>,
    pub worktree: Option<String>,
    pub wave: Option<u32>,
    pub implementation_author: Option<String>,
    /// A processor-validated, strictly higher coder classification. `None` leaves the planner's
    /// original descriptor text untouched.
    pub risk: Option<Risk>,
    pub review_sha: Option<String>,
    pub review_cycles: Option<u32>,
    pub reason: Option<String>,
}

impl DescriptorPatch {
    pub fn state(state: TaskState) -> Self {
        Self {
            state,
            batch_id: None,
            branch: None,
            worktree: None,
            wave: None,
            implementation_author: None,
            risk: None,
            review_sha: None,
            review_cycles: None,
            reason: None,
        }
    }
}

#[derive(Debug)]
pub enum ControlError {
    Io(io::Error),
    InvalidInput(String),
    Contradiction(String),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "control-plane I/O error: {error}"),
            Self::InvalidInput(message) | Self::Contradiction(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidInput(_) | Self::Contradiction(_) => None,
        }
    }
}

impl From<io::Error> for ControlError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, ControlError>;

/// Test presence of a confined control-plane entry without following its final component.
/// Callers use this for fail-closed routing before a typed reader validates the entry itself.
pub fn entry_exists(work: &Path, path: &Path) -> Result<bool> {
    work_fs::entry_exists(work, path).map_err(ControlError::Io)
}

/// Mutates a single already-owned `.work` directory.  The owner lease is deliberately not
/// reimplemented here: [`crate::ownership::LeaseStore`] is the only lease authority.
#[derive(Debug, Clone)]
pub struct ControlPlane {
    work: PathBuf,
}

impl ControlPlane {
    pub fn new(work: impl AsRef<Path>) -> Result<Self> {
        let work = std::path::absolute(work.as_ref())?;
        if work_fs::require_plain_directory(&work).is_err() {
            return Err(ControlError::InvalidInput(format!(
                "control-plane work directory does not exist: {}",
                work.display()
            )));
        }
        Ok(Self { work })
    }

    pub fn work(&self) -> &Path {
        &self.work
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        Snapshot::try_load(&self.work).map_err(ControlError::Io)
    }

    /// Create the append-only batch manifest. An existing manifest must already identify the
    /// same immutable batch/base pair; a resumed run may therefore call this idempotently.
    pub fn open_batch(&self, batch_id: &str, base: &str) -> Result<()> {
        validate_batch_id(batch_id)?;
        validate_ref(base, "base")?;
        let path = self.work.join("batch.md");
        match work_fs::read_optional_text(&self.work, &path, MAX_CONTROL_ARTIFACT_BYTES) {
            Ok(Some(existing)) => {
                let snapshot = self.snapshot()?;
                let Some(batch) = snapshot.batch else {
                    return Err(ControlError::Contradiction(format!(
                        "{} exists but is not a valid batch manifest",
                        path.display()
                    )));
                };
                if batch.batch_id.as_deref() != Some(batch_id) || batch.base.as_deref() != Some(base)
                {
                    return Err(ControlError::Contradiction(format!(
                        "active batch manifest conflicts with requested batch {batch_id} at base {base}"
                    )));
                }
                if !existing.contains("## Задачи") {
                    return Err(ControlError::Contradiction(
                        "batch manifest has no append-only task section".into(),
                    ));
                }
                Ok(())
            }
            Ok(None) => self.write_text(
                &path,
                &format!(
                    "# Batch {batch_id}\n\nБаза: {base}\nИнтеграционная ветка: integration/{batch_id}\n\n## Задачи\n"
                ),
            ),
            Err(error) => Err(error.into()),
        }
    }

    /// Append one captured task to the batch manifest. Existing task rows are accepted only when
    /// they are byte-for-byte the same, preventing a restart from silently changing its recorded
    /// branch, domain, or wave.
    pub fn append_batch_task(
        &self,
        task_id: &str,
        level: &str,
        branch: &str,
        domain: &[String],
        wave: u32,
    ) -> Result<()> {
        validate_task_id(task_id)?;
        validate_ref(branch, "task branch")?;
        if level.trim().is_empty() || domain.is_empty() {
            return Err(ControlError::InvalidInput(format!(
                "batch task {task_id} requires a level and nonempty conflict domain"
            )));
        }
        let path = self.work.join("batch.md");
        let mut content = self.read_required(&path)?;
        let row = format!(
            "- [{task_id}] уровень={level} ветка={branch} домен={} волна={wave}",
            domain.join(",")
        );
        let existing_rows: Vec<_> = content
            .lines()
            .filter(|line| line.trim_start().starts_with(&format!("- [{task_id}]")))
            .collect();
        match existing_rows.as_slice() {
            [] => {
                if !content.ends_with('\n') {
                    content.push('\n');
                }
                content.push_str(&row);
                content.push('\n');
                self.write_text(&path, &content)
            }
            [existing] if existing.trim() == row => Ok(()),
            _ => Err(ControlError::Contradiction(format!(
                "batch manifest already records incompatible coordinates for {task_id}"
            ))),
        }
    }

    /// Capture an eligible queue task and annotate its descriptor. The queue must currently say
    /// `не начата`; arbitrary labels and missing descriptors are contradictions rather than a
    /// reason to invent a new task record. A prior quarantine counter stays attached to the new
    /// capture so a later return cannot restart the bounded retry budget from zero. The old
    /// quarantine reason describes the completed attempt and is deliberately not carried forward.
    pub fn capture_task(
        &self,
        task_id: &str,
        batch_id: &str,
        branch: &str,
        worktree: &str,
        wave: u32,
    ) -> Result<()> {
        validate_task_id(task_id)?;
        validate_batch_id(batch_id)?;
        validate_ref(branch, "task branch")?;
        let queue = self.queue_path();
        let content = self.read_required(&queue)?;
        let entry = find_queue_block(&content, task_id)?;
        let current = parse_status(&entry.status)?;
        if current != TaskState::NotStarted {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} is {}, not eligible for capture",
                current.as_str()
            )));
        }
        let prior_attempt = queue_attempt(&entry.status);
        let mut status =
            format!("в работе · батч={batch_id} · worktree={worktree} · ветка={branch}");
        if let Some(attempt) = prior_attempt {
            status.push_str(&format!(" · попытка={attempt}"));
        }
        self.write_text(&queue, &replace_block_status(&content, &entry, &status))?;
        let mut patch = DescriptorPatch::state(TaskState::Working);
        patch.batch_id = Some(batch_id.into());
        patch.branch = Some(branch.into());
        patch.worktree = Some(worktree.into());
        patch.wave = Some(wave);
        self.patch_descriptor(task_id, patch)
    }

    /// Apply a processor-owned descriptor transition while preserving planner criteria, prose,
    /// review reports, and all fields the native engine does not own.
    pub fn patch_descriptor(&self, task_id: &str, patch: DescriptorPatch) -> Result<()> {
        validate_task_id(task_id)?;
        let path = self.descriptor_path(task_id);
        let mut content = self.read_required(&path)?;
        let old = crate::state::descriptor::parse_descriptor(task_id, &content);
        if let Some(previous) = old.state
            && previous != patch.state
            && !valid_task_transition(previous, patch.state)
        {
            return Err(ControlError::Contradiction(format!(
                "illegal descriptor transition for {task_id}: {} -> {}",
                previous.as_str(),
                patch.state.as_str()
            )));
        }
        set_line_field(&mut content, "Статус:", task_literal(patch.state));
        if let Some(value) = patch.batch_id {
            set_line_field(&mut content, "Батч:", &value);
        }
        if let Some(value) = patch.branch {
            set_line_field(&mut content, "Ветка:", &value);
        }
        if let Some(value) = patch.worktree {
            set_line_field(&mut content, "Worktree:", &value);
        }
        if let Some(value) = patch.wave {
            set_line_field(&mut content, "Волна:", &value.to_string());
        }
        if let Some(value) = patch.implementation_author {
            let mut authors = old.implementation_authors;
            if authors.last().map(String::as_str) != Some(value.as_str()) {
                authors.push(value);
            }
            set_line_field(&mut content, "Реализовано:", &authors.join(", "));
        }
        if let Some(value) = patch.risk {
            match old.risk {
                Some(previous) if value < previous => {
                    return Err(ControlError::Contradiction(format!(
                        "descriptor risk for {task_id} cannot decrease from {} to {}",
                        previous.as_str(),
                        value.as_str()
                    )));
                }
                Some(previous) if value == previous => {}
                Some(previous) => set_line_field(
                    &mut content,
                    "Риск:",
                    &format!(
                        "{} — elevated by deterministic engine (previous: {})",
                        value.as_str(),
                        previous.as_str()
                    ),
                ),
                None => set_line_field(
                    &mut content,
                    "Риск:",
                    &format!("{} — elevated by deterministic engine", value.as_str()),
                ),
            }
        }
        if let Some(value) = patch.review_sha {
            validate_ref(&value, "review SHA")?;
            set_line_field(&mut content, "Ревью-SHA:", &value);
        }
        if let Some(value) = patch.review_cycles {
            set_line_field(&mut content, "Циклов-ревью:", &value.to_string());
        }
        if let Some(value) = patch.reason {
            set_line_field(&mut content, "Причина:", &sanitize_reason(&value));
        }
        self.write_text(&path, &content)
    }

    /// Update only the lifecycle suffix of one queue header. The descriptor carries richer
    /// processor metadata; the queue remains the concise, user-visible source for eligibility.
    /// A re-captured task's quarantine attempt remains part of every non-terminal lifecycle
    /// label until that task is returned or archived, so Phase 6 never restarts its retry budget.
    /// Terminal escalation deliberately drops the completed counter, matching the legacy queue
    /// contract.
    pub fn patch_queue_state(
        &self,
        task_id: &str,
        state: TaskState,
        reason: Option<&str>,
    ) -> Result<()> {
        validate_task_id(task_id)?;
        let queue = self.queue_path();
        let content = self.read_required(&queue)?;
        let entry = find_queue_block(&content, task_id)?;
        let previous = parse_status(&entry.status)?;
        if previous != state && !valid_task_transition(previous, state) {
            return Err(ControlError::Contradiction(format!(
                "illegal queue transition for {task_id}: {} -> {}",
                previous.as_str(),
                state.as_str()
            )));
        }
        let prior_attempt = queue_attempt(&entry.status);
        let mut literal = task_literal(state).to_string();
        if matches!(state, TaskState::Escalated | TaskState::Conflict)
            && let Some(reason) = reason
                .map(sanitize_reason)
                .filter(|value| !value.is_empty())
        {
            literal.push_str(" · причина=");
            literal.push_str(&reason);
        }
        // Escalation is terminal: legacy `queue-tx return` and `escalate` deliberately replace
        // the complete status with its final reason.  Every non-terminal lifecycle state keeps
        // the retry coordinate so another quarantine can increment the original budget.
        if state != TaskState::Escalated {
            append_attempt(&mut literal, prior_attempt);
        }
        self.write_text(&queue, &replace_block_status(&content, &entry, &literal))
    }

    /// Return exactly one still-unpublished merged candidate to `ready` after the VCS adapter
    /// has proved a rejected remote publication was re-anchored to the current remote base.
    /// The legacy queue remains captured as `working` while its descriptor traverses normal
    /// review/join/publication states, so that queue coordinate is deliberately preserved. Older
    /// native checkpoints may already have mirrored `merged`/`ready` into the queue and remain
    /// recoverable through the same idempotent repair.
    pub fn reanchor_merged_task(&self, task_id: &str) -> Result<()> {
        validate_task_id(task_id)?;

        let descriptor = self.descriptor_path(task_id);
        let mut descriptor_text = self.read_required(&descriptor)?;
        let descriptor_state =
            crate::state::descriptor::parse_descriptor(task_id, &descriptor_text)
                .state
                .ok_or_else(|| {
                    ControlError::Contradiction(format!(
                        "descriptor for re-anchor task {task_id} has no readable state"
                    ))
                })?;
        if !matches!(descriptor_state, TaskState::Merged | TaskState::Ready) {
            return Err(ControlError::Contradiction(format!(
                "descriptor task {task_id} is {}, not a re-anchorable merged candidate",
                descriptor_state.as_str()
            )));
        }

        let queue = self.queue_path();
        let queue_text = self.read_required(&queue)?;
        let entry = find_queue_block(&queue_text, task_id)?;
        let queue_state = parse_status(&entry.status)?;
        if !matches!(
            queue_state,
            TaskState::Working | TaskState::Merged | TaskState::Ready
        ) {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} is {}, not a re-anchorable merged candidate",
                queue_state.as_str()
            )));
        }

        // Validate both independent artifacts before changing either one. A pre-existing
        // contradiction (for example a stray published queue row) must remain diagnosable, not
        // be turned into a new half-reanchored descriptor by an otherwise rejected operation.
        if descriptor_state == TaskState::Merged {
            set_line_field(
                &mut descriptor_text,
                "Статус:",
                task_literal(TaskState::Ready),
            );
            self.write_text(&descriptor, &descriptor_text)?;
        }
        if queue_state == TaskState::Merged {
            let mut status = task_literal(TaskState::Ready).to_string();
            append_attempt(&mut status, queue_attempt(&entry.status));
            self.write_text(&queue, &replace_block_status(&queue_text, &entry, &status))?;
        }
        Ok(())
    }

    /// Return an active queue label whose descriptor is absent to ordinary eligibility without
    /// inventing a descriptor, batch, or quarantine reason. This is intentionally narrower than
    /// [`Self::return_task`]: Phase-0 has no trustworthy terminal descriptor to transition, and
    /// the existing attempt counter must be preserved exactly when one was already recorded.
    pub fn return_orphaned_queue(&self, task_id: &str, attempt: Option<u32>) -> Result<()> {
        validate_task_id(task_id)?;
        let descriptor = self.descriptor_path(task_id);
        if work_fs::entry_exists(&self.work, &descriptor)? {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} is not orphaned because descriptor {} exists",
                descriptor.display()
            )));
        }
        let queue = self.queue_path();
        let content = self.read_required(&queue)?;
        let entry = find_queue_block(&content, task_id)?;
        let current = parse_status(&entry.status)?;
        if !matches!(
            current,
            TaskState::Working | TaskState::InReview | TaskState::Ready | TaskState::Merged
        ) {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} cannot be recovered as an orphan from {}",
                current.as_str()
            )));
        }
        let status = attempt.map_or_else(
            || "не начата".to_string(),
            |value| format!("не начата · попытка={value}"),
        );
        self.write_text(&queue, &replace_block_status(&content, &entry, &status))
    }

    /// Restore the queue half of a previously captured task without changing its descriptor.
    /// Phase 0 uses this only when the descriptor and batch coordinates already prove ownership;
    /// it must not turn a `ready` task back into ordinary work or consume a quarantine attempt.
    pub fn restore_queue_capture(
        &self,
        task_id: &str,
        batch_id: &str,
        branch: &str,
        worktree: &str,
    ) -> Result<()> {
        validate_task_id(task_id)?;
        validate_batch_id(batch_id)?;
        validate_ref(branch, "task branch")?;
        if worktree.trim().is_empty()
            || worktree.starts_with('-')
            || worktree.contains(['\0', '\r', '\n'])
        {
            return Err(ControlError::InvalidInput(format!(
                "invalid task worktree: {worktree:?}"
            )));
        }
        let descriptor_path = self.descriptor_path(task_id);
        let descriptor_text = self.read_required(&descriptor_path)?;
        let descriptor = crate::state::descriptor::parse_descriptor(task_id, &descriptor_text);
        let state = descriptor.state.ok_or_else(|| {
            ControlError::Contradiction(format!(
                "descriptor {task_id} has no recognized status for queue-capture recovery"
            ))
        })?;
        if !matches!(
            state,
            TaskState::Working | TaskState::InReview | TaskState::Ready
        ) {
            return Err(ControlError::Contradiction(format!(
                "descriptor {task_id} is {}, not a live captured task",
                state.as_str()
            )));
        }
        if descriptor.batch_id.as_deref() != Some(batch_id)
            || descriptor
                .branch
                .as_deref()
                .is_some_and(|value| value != branch)
            || descriptor
                .worktree
                .as_deref()
                .is_some_and(|value| value.replace('\\', "/") != worktree.replace('\\', "/"))
        {
            return Err(ControlError::Contradiction(format!(
                "descriptor {task_id} does not match the recovered batch coordinates"
            )));
        }
        let queue = self.queue_path();
        let content = self.read_required(&queue)?;
        let entry = find_queue_block(&content, task_id)?;
        if parse_status(&entry.status)? != TaskState::NotStarted
            || entry.status.contains("попытка=")
        {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} is not an uncaptured eligible entry"
            )));
        }
        let status = format!(
            "{} · батч={batch_id} · worktree={worktree} · ветка={branch}",
            task_literal(state)
        );
        self.write_text(&queue, &replace_block_status(&content, &entry, &status))
    }

    /// Return a conflict/escalated task to the queue with a durable quarantine counter. The
    /// caller must have first written the terminal descriptor state; its descriptor is retained
    /// as an audit record until cleanup acknowledges it.
    pub fn return_task(&self, task_id: &str, reason: &str, attempt: u32) -> Result<()> {
        validate_task_id(task_id)?;
        let queue = self.queue_path();
        let content = self.read_required(&queue)?;
        let entry = find_queue_block(&content, task_id)?;
        let current = parse_status(&entry.status)?;
        if !matches!(
            current,
            TaskState::Conflict | TaskState::Escalated | TaskState::Working | TaskState::Ready
        ) {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} cannot be returned from {}",
                current.as_str()
            )));
        }
        let status = format!(
            "не начата · попытка={attempt} · карантин={}",
            sanitize_reason(reason)
        );
        self.write_text(&queue, &replace_block_status(&content, &entry, &status))
    }

    /// Archive one published task by moving its exact queue block to `Tasks_Done.md` and setting
    /// the descriptor to `выполнена`. Repeating after the queue entry has moved is idempotent as
    /// long as the done archive already contains the task header.
    pub fn archive_task(&self, task_id: &str) -> Result<()> {
        validate_task_id(task_id)?;
        let descriptor_path = self.descriptor_path(task_id);
        let descriptor_state = match work_fs::read_optional_text(
            &self.work,
            &descriptor_path,
            MAX_CONTROL_ARTIFACT_BYTES,
        ) {
            Ok(Some(text)) => Some(
                crate::state::descriptor::parse_descriptor(task_id, &text)
                    .state
                    .ok_or_else(|| {
                        ControlError::Contradiction(format!(
                            "descriptor {task_id} has no recognized state for archive"
                        ))
                    })?,
            ),
            Ok(None) => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(descriptor_state) = descriptor_state
            && !matches!(descriptor_state, TaskState::Published | TaskState::Done)
        {
            return Err(ControlError::Contradiction(format!(
                "descriptor task {task_id} is {}, not publishable for archive",
                descriptor_state.as_str()
            )));
        }
        let queue = self.queue_path();
        let content = self.read_required(&queue)?;
        let entry = match find_queue_block(&content, task_id) {
            Ok(entry) => entry,
            Err(ControlError::Contradiction(message))
                if message == format!("queue has no task block for {task_id}") =>
            {
                let done_path = self.work.join(DONE_FILE);
                let done = match work_fs::read_optional_text(
                    &self.work,
                    &done_path,
                    MAX_CONTROL_ARTIFACT_BYTES,
                ) {
                    Ok(Some(done)) => done,
                    Ok(None) => {
                        return Err(ControlError::Contradiction(message));
                    }
                    Err(error) => return Err(error.into()),
                };
                if done.contains(&format!("### [{task_id}]")) {
                    // A later Phase-6 workspace cleanup may already have removed the terminal
                    // descriptor when a process dies before acknowledging a subsequent effect.
                    // The exact archive header is then the durable proof that this task was
                    // accounted for; retrying the archive must not turn that completed cleanup
                    // prefix into a permanent recovery failure.
                    if !work_fs::entry_exists(&self.work, &self.descriptor_path(task_id))? {
                        return Ok(());
                    }
                    return self.patch_descriptor(task_id, DescriptorPatch::state(TaskState::Done));
                }
                return Err(ControlError::Contradiction(message));
            }
            Err(error) => return Err(error),
        };
        if descriptor_state.is_none() {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} cannot be archived without a published descriptor"
            )));
        }
        let state = parse_status(&entry.status)?;
        if !matches!(
            state,
            // `working` is the normal legacy queue coordinate. `ready`/`merged`/`published`
            // are accepted only so a native runtime can finish cohorts written by earlier engine
            // versions or imported from a prior compatibility projection.
            TaskState::Working
                | TaskState::Ready
                | TaskState::Merged
                | TaskState::Published
                | TaskState::Done
        ) {
            return Err(ControlError::Contradiction(format!(
                "queue task {task_id} is {}, not publishable for archive",
                state.as_str()
            )));
        }
        let done_path = self.work.join(DONE_FILE);
        let mut done =
            match work_fs::read_optional_text(&self.work, &done_path, MAX_CONTROL_ARTIFACT_BYTES) {
                Ok(Some(text)) => text,
                Ok(None) => "# Completed tasks\n\n".into(),
                Err(error) => return Err(error.into()),
            };
        if !done.contains(&format!("### [{task_id}]")) {
            if !done.ends_with('\n') {
                done.push('\n');
            }
            done.push('\n');
            done.push_str(&replace_block_status(
                &entry.text,
                &local_block(&entry.text)?,
                "выполнена",
            ));
            if !done.ends_with('\n') {
                done.push('\n');
            }
            self.write_text(&done_path, &done)?;
        }
        self.write_text(&queue, &remove_block(&content, &entry))?;
        self.patch_descriptor(task_id, DescriptorPatch::state(TaskState::Done))
    }

    /// Complete the destructive half of Phase 6.1 without yet deleting the live descriptor.
    /// The descriptor is moved to `done` first, making a crash before queue removal recoverable
    /// as a terminal task instead of leaving a published descriptor with no queue coordinate.
    pub fn mark_task_done_for_archive(&self, task_id: &str) -> Result<()> {
        validate_task_id(task_id)?;
        let descriptor_path = self.descriptor_path(task_id);
        let descriptor_text = self.read_required(&descriptor_path)?;
        let descriptor = crate::state::descriptor::parse_descriptor(task_id, &descriptor_text);
        let state = descriptor.state.ok_or_else(|| {
            ControlError::Contradiction(format!(
                "descriptor {task_id} has no recognized state for archive"
            ))
        })?;
        if !matches!(state, TaskState::Published | TaskState::Done) {
            return Err(ControlError::Contradiction(format!(
                "descriptor task {task_id} is {}, not publishable for archive",
                state.as_str()
            )));
        }

        let queue = self.queue_path();
        let queue_text = self.read_required(&queue)?;
        let queue_entry = match find_queue_block(&queue_text, task_id) {
            Ok(entry) => Some(entry),
            Err(ControlError::Contradiction(message))
                if message == format!("queue has no task block for {task_id}") =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        if let Some(entry) = queue_entry.as_ref() {
            let queue_state = parse_status(&entry.status)?;
            if !matches!(
                queue_state,
                TaskState::Working
                    | TaskState::Ready
                    | TaskState::Merged
                    | TaskState::Published
                    | TaskState::Done
            ) {
                return Err(ControlError::Contradiction(format!(
                    "queue task {task_id} is {}, not publishable for archive",
                    queue_state.as_str()
                )));
            }
        } else if state != TaskState::Done {
            return Err(ControlError::Contradiction(format!(
                "published descriptor {task_id} has no queue block"
            )));
        }

        if state == TaskState::Published {
            self.patch_descriptor(task_id, DescriptorPatch::state(TaskState::Done))?;
        }
        if let Some(entry) = queue_entry {
            self.write_text(&queue, &remove_block(&queue_text, &entry))?;
        }
        Ok(())
    }

    /// Atomically append or repair one immutable `descriptor + metrics` archive section. A
    /// complete marker in the same section is the idempotency proof; a header-only crash residue
    /// is replaced wholesale while the live descriptor still exists.
    pub fn project_task_archive(&self, task_id: &str, batch_id: &str, metrics: &str) -> Result<()> {
        validate_task_id(task_id)?;
        validate_batch_id(batch_id)?;
        validate_task_metrics_block(metrics, task_id, batch_id)?;

        let descriptor_path = self.descriptor_path(task_id);
        let descriptor_text = self.read_required(&descriptor_path)?;
        let descriptor = crate::state::descriptor::parse_descriptor(task_id, &descriptor_text);
        if descriptor.state != Some(TaskState::Done) {
            return Err(ControlError::Contradiction(format!(
                "descriptor {task_id} must be done before archive projection"
            )));
        }
        let desired = format_archive_section(task_id, &descriptor_text, metrics);
        let done_path = self.work.join(DONE_FILE);
        let mut done =
            match work_fs::read_optional_text(&self.work, &done_path, MAX_CONTROL_ARTIFACT_BYTES) {
                Ok(Some(text)) => text,
                Ok(None) => "# Completed tasks\n\n".into(),
                Err(error) => return Err(error.into()),
            };
        let sections = archive_sections(&done);
        let matches = sections
            .iter()
            .filter(|section| section.task_id == task_id)
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err(ControlError::Contradiction(format!(
                "archive has duplicate task sections for {task_id}"
            )));
        }
        if let Some(existing) = matches.first() {
            let section = &done[existing.start..existing.end];
            let marker = task_metrics_marker(task_id, batch_id);
            let marker_count = section.matches(&marker).count();
            if marker_count > 1 {
                return Err(ControlError::Contradiction(format!(
                    "archive section {task_id} has duplicate metrics markers"
                )));
            }
            if marker_count == 0 {
                done.replace_range(existing.start..existing.end, &desired);
                self.write_text(&done_path, &done)?;
            }
        } else {
            if !done.ends_with('\n') {
                done.push('\n');
            }
            if !done.ends_with("\n\n") {
                done.push('\n');
            }
            done.push_str(&desired);
            self.write_text(&done_path, &done)?;
        }

        let verified = self.read_required(&done_path)?;
        let sections = archive_sections(&verified);
        let matches = sections
            .iter()
            .filter(|section| section.task_id == task_id)
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(ControlError::Contradiction(format!(
                "archive verification found {} sections for {task_id}",
                matches.len()
            )));
        }
        let section = &verified[matches[0].start..matches[0].end];
        if section
            .matches(&task_metrics_marker(task_id, batch_id))
            .count()
            != 1
        {
            return Err(ControlError::Contradiction(format!(
                "archive verification found an incomplete metrics block for {task_id}"
            )));
        }
        Ok(())
    }

    /// Read-only proof used when Phase 0 reconstructs an already-acknowledged archive effect
    /// after the live descriptor was removed. Only the exact task section plus its matching
    /// schema marker counts; a legacy/header-only section still requires a live descriptor.
    pub fn task_archive_complete(&self, task_id: &str, batch_id: &str) -> Result<bool> {
        validate_task_id(task_id)?;
        validate_batch_id(batch_id)?;
        let done_path = self.work.join(DONE_FILE);
        let done =
            match work_fs::read_optional_text(&self.work, &done_path, MAX_CONTROL_ARTIFACT_BYTES) {
                Ok(Some(text)) => text,
                Ok(None) => return Ok(false),
                Err(error) => return Err(error.into()),
            };
        let matches = archive_sections(&done)
            .into_iter()
            .filter(|section| section.task_id == task_id)
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Ok(false);
        }
        let section = &done[matches[0].start..matches[0].end];
        Ok(section
            .matches(&task_metrics_marker(task_id, batch_id))
            .count()
            == 1)
    }

    /// Remove one terminal task descriptor directory after its queue/archive and VCS workspace
    /// cleanup have completed.  The caller never supplies a filesystem path: the task id is
    /// validated and resolved under this control plane's owned `tasks/` directory.
    pub fn remove_terminal_task_descriptor(&self, task_id: &str) -> Result<()> {
        validate_task_id(task_id)?;
        let path = self.descriptor_path(task_id);
        let directory = path.parent().ok_or_else(|| {
            ControlError::InvalidInput(format!("descriptor path has no parent: {}", path.display()))
        })?;
        if !work_fs::entry_exists(&self.work, &path)? {
            return Ok(());
        }
        let text = self.read_required(&path)?;
        let descriptor = crate::state::descriptor::parse_descriptor(task_id, &text);
        let state = descriptor.state.ok_or_else(|| {
            ControlError::Contradiction(format!(
                "descriptor {task_id} has no recognized terminal status for cleanup"
            ))
        })?;
        if !matches!(
            state,
            TaskState::Done | TaskState::Conflict | TaskState::Escalated
        ) {
            return Err(ControlError::Contradiction(format!(
                "descriptor {task_id} is {}, not terminal for cleanup",
                state.as_str()
            )));
        }
        work_fs::remove_plain_directory_all(&self.work, directory)?;
        Ok(())
    }

    /// Remove a descriptor that Phase 0 has proved was never durably captured by the active
    /// cohort. This is intentionally distinct from terminal cleanup: the caller must first
    /// remove the guarded VCS worktree/branch, and this method re-reads the control plane so an
    /// intervening capture cannot be erased.
    ///
    /// A stale queue row may still carry a live label at this crash boundary. It is retained for
    /// the next Phase-0 reconciliation, which will return that now-orphaned row through the
    /// normal idempotent transaction instead of guessing a replacement batch coordinate here.
    pub fn remove_uncaptured_descriptor(&self, task_id: &str) -> Result<()> {
        validate_task_id(task_id)?;
        let path = self.descriptor_path(task_id);
        if !work_fs::entry_exists(&self.work, &path)? {
            return Ok(());
        }
        let snapshot = self.snapshot()?;
        let batch_path = self.work.join("batch.md");
        if work_fs::entry_exists(&self.work, &batch_path)? {
            let batch = snapshot.batch.as_ref().ok_or_else(|| {
                ControlError::Contradiction(format!(
                    "refusing to remove {task_id}: active batch manifest is malformed"
                ))
            })?;
            if batch.tasks.iter().any(|task| task.id == task_id) {
                return Err(ControlError::Contradiction(format!(
                    "refusing to remove {task_id}: it is still present in active batch {:?}",
                    batch.batch_id
                )));
            }
        }
        if let Some(queue) = snapshot.queue.iter().find(|entry| entry.id == task_id)
            && !matches!(
                queue.state,
                Some(
                    TaskState::NotStarted
                        | TaskState::Working
                        | TaskState::InReview
                        | TaskState::Ready
                        | TaskState::Merged
                )
            )
        {
            return Err(ControlError::Contradiction(format!(
                "refusing to remove uncaptured descriptor {task_id}: queue is {:?}",
                queue.state
            )));
        }
        let text = self.read_required(&path)?;
        let descriptor = crate::state::descriptor::parse_descriptor(task_id, &text);
        if !matches!(
            descriptor.state,
            Some(TaskState::NotStarted | TaskState::Working | TaskState::InReview)
        ) {
            return Err(ControlError::Contradiction(format!(
                "descriptor {task_id} is {:?}, not an uncaptured Phase-0 state",
                descriptor.state
            )));
        }
        let directory = path.parent().ok_or_else(|| {
            ControlError::InvalidInput(format!("descriptor path has no parent: {}", path.display()))
        })?;
        work_fs::remove_plain_directory_all(&self.work, directory)?;
        Ok(())
    }

    /// Delete the cohort-only Markdown control artifacts after the journal and every task's
    /// terminal accounting have been acknowledged.  This is intentionally idempotent: a crash
    /// after one file is removed leaves the same owned set for the next cleanup effect retry.
    /// The durable runtime checkpoint/outbox and user queue/archive are deliberately retained.
    pub fn remove_cohort_artifacts(&self, batch_id: &str) -> Result<()> {
        validate_batch_id(batch_id)?;
        let snapshot = self.snapshot()?;
        let batch_path = self.work.join("batch.md");
        if work_fs::entry_exists(&self.work, &batch_path)? {
            let batch = snapshot.batch.ok_or_else(|| {
                ControlError::Contradiction(format!(
                    "refusing to remove unreadable or malformed active batch manifest {}",
                    batch_path.display()
                ))
            })?;
            if batch.batch_id.as_deref() != Some(batch_id) {
                return Err(ControlError::Contradiction(format!(
                    "refusing to clean batch {:?}; native state owns {batch_id}",
                    batch.batch_id
                )));
            }
        }
        for name in [
            "batch.md",
            "cohort_state.md",
            "merge_report.md",
            "integration_state.md",
        ] {
            let path = self.work.join(name);
            work_fs::remove_plain_file(&self.work, &path)?;
        }
        for name in ["_planner", "_integration"] {
            let path = self.work.join("tasks").join(name);
            work_fs::remove_plain_directory_all(&self.work, &path)?;
        }
        Ok(())
    }

    /// Cohort state is intentionally mutable between rounds; this document is never used as a
    /// source for task ownership, only as an operator/recovery summary.
    pub fn write_cohort(
        &self,
        batch_id: &str,
        admission: &str,
        reason: Option<&str>,
        wave: u32,
        admitted_total: u32,
    ) -> Result<()> {
        validate_batch_id(batch_id)?;
        let reason = reason
            .map(sanitize_reason)
            .filter(|value| !value.is_empty())
            .map(|value| format!(" · причина={value}"))
            .unwrap_or_default();
        self.write_text(
            &self.work.join("cohort_state.md"),
            &format!(
                "# Cohort state — Batch {batch_id}\n\nПриём: {admission}{reason}\nВолна: {wave}\nAdmitted всего: {admitted_total}\n"
            ),
        )
    }

    pub fn write_integration(
        &self,
        batch_id: &str,
        state: &str,
        review_sha: Option<&str>,
        f_cycles: u32,
        reason: Option<&str>,
    ) -> Result<()> {
        validate_batch_id(batch_id)?;
        let mut text = format!(
            "# Integration state — Batch {batch_id}\n\nСостояние: {state}\nF-циклов: {f_cycles}\n"
        );
        if let Some(sha) = review_sha {
            validate_ref(sha, "integration review SHA")?;
            text.push_str(&format!("Ревью-SHA: {sha}\n"));
        }
        if let Some(reason) = reason {
            text.push_str(&format!("Причина: {}\n", sanitize_reason(reason)));
        }
        self.write_text(&self.work.join("integration_state.md"), &text)
    }

    /// Materialize the processor's compact overview and append one redacted journal line. These
    /// are operator-facing derived artifacts; the runtime checkpoint and JSONL outbox remain the
    /// durable transition authorities.
    pub fn write_journal_and_status(
        &self,
        state: &crate::processor::ProcessorState,
        occurred_at: &str,
    ) -> Result<()> {
        self.write_journal_and_status_with_pause(state, occurred_at, false)
    }

    /// Record an operator PAUSE at a safe scheduler boundary. This only derives the existing
    /// operator-facing status and journal files; the processor checkpoint remains the resume
    /// authority.
    pub fn write_pause_status(
        &self,
        state: &crate::processor::ProcessorState,
        occurred_at: &str,
    ) -> Result<()> {
        self.write_journal_and_status_with_pause(state, occurred_at, true)
    }

    /// Append the one safe, stable record of a best-effort notification attempt. The dispatcher
    /// exposes no child output, command text, approval content, or CI response here; retrying an
    /// already-finalized receipt therefore cannot create duplicate journal history.
    pub fn append_notification_journal(
        &self,
        outcome: &crate::notification::NotificationOutcome,
    ) -> Result<()> {
        let journal = self.work.join("journal.md");
        let mut existing =
            match work_fs::read_optional_text(&self.work, &journal, MAX_CONTROL_ARTIFACT_BYTES) {
                Ok(Some(text)) => text,
                Ok(None) => "# Journal\n".into(),
                Err(error) => return Err(error.into()),
            };
        if !existing.ends_with('\n') {
            existing.push('\n');
        }
        let entry = outcome.journal_entry();
        if !existing.lines().any(|line| line == entry) {
            existing.push_str(&entry);
            existing.push('\n');
        }
        self.write_text(&journal, &existing)
    }

    /// Record that native policy rejected one planner candidate before capture. The exact denied
    /// domain/pattern can be sensitive policy data, so the durable operator artifact retains
    /// only the validated task ID and a controlled classifier. Repeating the same planner effect
    /// after a crash leaves one stable entry rather than an unbounded append stream.
    pub fn append_planner_denial_journal(&self, task_id: &str) -> Result<()> {
        validate_task_id(task_id)?;
        let journal = self.work.join("journal.md");
        let mut existing =
            match work_fs::read_optional_text(&self.work, &journal, MAX_CONTROL_ARTIFACT_BYTES) {
                Ok(Some(text)) => text,
                Ok(None) => "# Journal\n".into(),
                Err(error) => return Err(error.into()),
            };
        if !existing.ends_with('\n') {
            existing.push('\n');
        }
        let entry = format!(
            "- planner-candidate-rejected task={task_id} reason=denylisted_conflict_domain"
        );
        if !existing.lines().any(|line| line == entry) {
            existing.push_str(&entry);
            existing.push('\n');
        }
        self.write_text(&journal, &existing)
    }

    fn write_journal_and_status_with_pause(
        &self,
        state: &crate::processor::ProcessorState,
        occurred_at: &str,
        paused: bool,
    ) -> Result<()> {
        let batch = state
            .batch
            .as_ref()
            .map(|batch| batch.id.as_str())
            .unwrap_or("none");
        let token_budget = state.batch.as_ref().and_then(|cohort| {
            cohort.cohort_token_budget.map(|limit| {
                let actual = cohort
                    .token_budget_actual_tokens
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unavailable".into());
                let remaining = cohort
                    .token_budget_actual_tokens
                    .map(|actual| limit.saturating_sub(actual).to_string())
                    .unwrap_or_else(|| "unavailable".into());
                format!("actual={actual} limit={limit} remaining={remaining}")
            })
        });
        let telemetry = state.batch.as_ref().map(|cohort| {
            batch_telemetry_summary(&self.work, &cohort.id, cohort.events_outbox_enabled)
        });
        let mut status = format!(
            "# Orchestrail status\n\nPhase: {:?}\nBatch: {batch}\nTasks:\n",
            state.phase
        );
        if let Some(token_budget) = &token_budget {
            status.push_str(&format!("Cohort token budget: {token_budget}\n"));
        }
        if let Some(telemetry) = &telemetry {
            status.push_str(&format_codex_status(telemetry));
            status.push_str(&format_usage_status(telemetry));
        }
        for task in state.tasks.values() {
            status.push_str(&format!("- {}: {:?}\n", task.id, task.phase));
        }
        if let Some(reason) = &state.blocked_reason {
            status.push_str(&format!("\nBlocked: {}\n", sanitize_reason(reason)));
        }
        if !state.integration.degradations.is_empty() {
            status.push_str("\nDegradations:\n");
            for degradation in &state.integration.degradations {
                status.push_str(&format!("- {}\n", sanitize_reason(degradation)));
            }
        }
        if !state.integration.pending_knowledge_curations.is_empty() {
            status.push_str("\nPending knowledge curation:\n");
            for pending_batch_id in state.integration.pending_knowledge_curations.keys() {
                status.push_str(&format!("- {pending_batch_id}\n"));
            }
        }
        if paused {
            status.push_str("\nPaused: .work/PAUSE is active\n");
        }
        let roadmap_waiting_note = state
            .batch
            .as_ref()
            .filter(|batch| batch.admission_closed == Some(CloseReasonWire::QueueEmpty))
            .and_then(|_| roadmap::queue_empty_waiting_note(&self.work).ok().flatten());
        if let Some(note) = &roadmap_waiting_note {
            status.push('\n');
            status.push_str(note);
            status.push('\n');
        }
        self.write_text(&self.work.join("status.md"), &status)?;

        let journal = self.work.join("journal.md");
        let mut existing =
            match work_fs::read_optional_text(&self.work, &journal, MAX_CONTROL_ARTIFACT_BYTES) {
                Ok(Some(text)) => text,
                Ok(None) => "# Journal\n".into(),
                Err(error) => return Err(error.into()),
            };
        if !existing.ends_with('\n') {
            existing.push('\n');
        }
        let entry = match (paused, token_budget) {
            (true, Some(token_budget)) => format!(
                "- {occurred_at}: paused=.work/PAUSE, phase={:?}, batch={batch}, token_budget={token_budget}",
                state.phase
            ),
            (true, None) => format!(
                "- {occurred_at}: paused=.work/PAUSE, phase={:?}, batch={batch}",
                state.phase
            ),
            (false, Some(token_budget)) => format!(
                "- {occurred_at}: phase={:?}, batch={batch}, token_budget={token_budget}",
                state.phase
            ),
            (false, None) => format!("- {occurred_at}: phase={:?}, batch={batch}", state.phase),
        };
        // The runtime can crash after this physical append and before it acknowledges the
        // keyed effect in its checkpoint. Make retrying that exact materialization idempotent:
        // the derived journal line is a stable state/clock identity, not an event stream whose
        // duplicate would carry additional meaning.
        if !existing.lines().any(|line| line == entry) {
            existing.push_str(&entry);
            existing.push('\n');
        }
        for degradation in &state.integration.degradations {
            let degradation = format!(
                "- batch={batch}: degradation={}",
                sanitize_reason(degradation)
            );
            if !existing.lines().any(|line| line == degradation) {
                existing.push_str(&degradation);
                existing.push('\n');
            }
        }
        if matches!(state.phase, crate::processor::Phase::Cleaning)
            && !state.integration.cleanup_journaled
            && let Some(telemetry) = &telemetry
        {
            let codex_entry = format!(
                "- batch={batch}: {}",
                format_codex_status(telemetry).trim_end()
            );
            if !existing.lines().any(|line| line == codex_entry) {
                existing.push_str(&codex_entry);
                existing.push('\n');
            }
            let usage_entry = format!(
                "- batch={batch}: {}",
                format_usage_status(telemetry).trim_end()
            );
            if !existing.lines().any(|line| line == usage_entry) {
                existing.push_str(&usage_entry);
                existing.push('\n');
            }
        }
        if let Some(note) = roadmap_waiting_note {
            let roadmap_entry = format!("- {occurred_at}: {note}");
            if !existing.lines().any(|line| line == roadmap_entry) {
                existing.push_str(&roadmap_entry);
                existing.push('\n');
            }
        }
        self.write_text(&journal, &existing)
    }

    fn queue_path(&self) -> PathBuf {
        self.work.join(QUEUE_FILE)
    }

    fn descriptor_path(&self, task_id: &str) -> PathBuf {
        self.work.join("tasks").join(task_id).join("task.md")
    }

    fn write_text(&self, path: &Path, text: &str) -> Result<()> {
        work_fs::replace_file(
            &self.work,
            path,
            text.as_bytes(),
            MAX_CONTROL_ARTIFACT_BYTES,
        )
        .map_err(ControlError::Io)
    }

    fn read_required(&self, path: &Path) -> Result<String> {
        work_fs::read_required_text(&self.work, path, MAX_CONTROL_ARTIFACT_BYTES).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                ControlError::Contradiction(format!(
                    "required control-plane artifact is absent: {}",
                    path.display()
                ))
            } else {
                ControlError::Io(error)
            }
        })
    }
}

fn format_codex_status(
    summary: &std::result::Result<BatchTelemetrySummary, crate::telemetry::TelemetryUnavailable>,
) -> String {
    let Ok(summary) = summary else {
        return format!(
            "Codex attempts: unavailable ({})\n",
            summary.as_ref().unwrap_err().as_str()
        );
    };
    let reasons = if summary.codex_fallback_reasons.is_empty() {
        String::new()
    } else {
        format!(
            " ({})",
            summary
                .codex_fallback_reasons
                .iter()
                .map(|(reason, count)| format!("{reason}={count}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "Codex attempts: {} ok, {} fallback{reasons}, {} failed\n",
        summary.codex_successes, summary.codex_fallbacks, summary.codex_failures
    )
}

fn format_usage_status(
    summary: &std::result::Result<BatchTelemetrySummary, crate::telemetry::TelemetryUnavailable>,
) -> String {
    let Ok(summary) = summary else {
        return format!(
            "Usage: unavailable ({})\n",
            summary.as_ref().unwrap_err().as_str()
        );
    };
    let by_source = if summary.actual_by_source.is_empty() {
        "none".into()
    } else {
        summary
            .actual_by_source
            .iter()
            .map(|(source, tokens)| format!("{source}={tokens}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "Usage: actual={} tokens (by source: {by_source}), estimated={} tokens, calls={}, unmetered={}\n",
        summary.usage.actual_tokens,
        summary.usage.estimated_tokens,
        summary
            .usage
            .actual_events
            .saturating_add(summary.usage.estimated_events)
            .saturating_add(summary.usage.unmetered_events),
        summary.usage.unmetered_events,
    )
}

#[derive(Debug, Clone)]
struct QueueBlock {
    start: usize,
    end: usize,
    header_end: usize,
    status_start: usize,
    text: String,
    status: String,
}

fn find_queue_block(content: &str, task_id: &str) -> Result<QueueBlock> {
    let mut starts: Vec<usize> = content
        .match_indices("### [")
        .filter_map(|(index, _)| is_line_start(content, index).then_some(index))
        .collect();
    starts.push(content.len());
    let wanted = format!("### [{task_id}]");
    let matches: Vec<_> = starts
        .windows(2)
        .filter_map(|pair| {
            let start = pair[0];
            content[start..]
                .starts_with(&wanted)
                .then_some((start, pair[1]))
        })
        .collect();
    let (start, end) = match matches.as_slice() {
        [(start, end)] => (*start, *end),
        [] => {
            return Err(ControlError::Contradiction(format!(
                "queue has no task block for {task_id}"
            )));
        }
        _ => {
            return Err(ControlError::Contradiction(format!(
                "queue has duplicate task blocks for {task_id}"
            )));
        }
    };
    let header_end = content[start..end]
        .find('\n')
        .map(|offset| start + offset)
        .unwrap_or(end);
    let header = &content[start..header_end];
    let marker = header
        .find("статус:")
        .or_else(|| header.find("Статус:"))
        .ok_or_else(|| {
            ControlError::Contradiction(format!("queue task {task_id} has no статус field"))
        })?;
    let status_start = start + marker + "статус:".len();
    let status = content[status_start..header_end].trim().to_string();
    if status.is_empty() {
        return Err(ControlError::Contradiction(format!(
            "queue task {task_id} has an empty status"
        )));
    }
    Ok(QueueBlock {
        start,
        end,
        header_end,
        status_start,
        text: content[start..end].to_string(),
        status,
    })
}

fn local_block(text: &str) -> Result<QueueBlock> {
    let id = text
        .strip_prefix("### [")
        .and_then(|tail| tail.find(']').map(|end| &tail[..end]))
        .ok_or_else(|| ControlError::Contradiction("invalid local queue block".into()))?;
    find_queue_block(text, id)
}

fn replace_block_status(content: &str, block: &QueueBlock, status: &str) -> String {
    let mut output = String::with_capacity(content.len() + status.len());
    output.push_str(&content[..block.status_start]);
    output.push(' ');
    output.push_str(status);
    output.push_str(&content[block.header_end..]);
    output
}

/// The queue parser and mutator share this deliberately tolerant suffix rule: a malformed
/// `попытка=` is not a valid counter and cannot be carried into an otherwise valid transition.
/// A valid counter is always rewritten canonically at the end of the generated status.
fn queue_attempt(status: &str) -> Option<u32> {
    crate::state::canonical::suffix_field(status, "попытка=")
        .and_then(|value| value.parse::<u32>().ok())
}

fn append_attempt(status: &mut String, attempt: Option<u32>) {
    if let Some(attempt) = attempt {
        status.push_str(&format!(" · попытка={attempt}"));
    }
}

fn remove_block(content: &str, block: &QueueBlock) -> String {
    let mut output = String::with_capacity(content.len().saturating_sub(block.end - block.start));
    output.push_str(&content[..block.start]);
    output.push_str(&content[block.end..]);
    output
}

fn is_line_start(text: &str, index: usize) -> bool {
    index == 0 || text.as_bytes().get(index.saturating_sub(1)) == Some(&b'\n')
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArchiveSection<'a> {
    task_id: &'a str,
    start: usize,
    end: usize,
}

fn archive_sections(text: &str) -> Vec<ArchiveSection<'_>> {
    let mut headings = Vec::new();
    let mut offset = 0_usize;
    for line in text.split_inclusive('\n') {
        if let Some(task_id) = archive_header_task_id(line.trim_end_matches(['\r', '\n'])) {
            headings.push((task_id, offset));
        }
        offset = offset.saturating_add(line.len());
    }
    if offset < text.len() {
        let line = &text[offset..];
        if let Some(task_id) = archive_header_task_id(line) {
            headings.push((task_id, offset));
        }
    }
    headings
        .iter()
        .enumerate()
        .map(|(index, (task_id, start))| ArchiveSection {
            task_id,
            start: *start,
            end: headings
                .get(index + 1)
                .map_or(text.len(), |(_, next)| *next),
        })
        .collect()
}

fn task_metrics_marker(task_id: &str, batch_id: &str) -> String {
    format!("<!-- orchestra/task-execution-metrics@1 task_id={task_id} batch_id={batch_id} status=")
}

fn validate_task_metrics_block(metrics: &str, task_id: &str, batch_id: &str) -> Result<()> {
    if !metrics.starts_with("#### Метрики выполнения\n")
        || metrics
            .matches(&task_metrics_marker(task_id, batch_id))
            .count()
            != 1
        || metrics
            .lines()
            .any(|line| archive_header_task_id(line).is_some())
    {
        return Err(ControlError::InvalidInput(format!(
            "metrics block for {task_id} does not match orchestra/task-execution-metrics@1"
        )));
    }
    Ok(())
}

fn format_archive_section(task_id: &str, descriptor: &str, metrics: &str) -> String {
    let descriptor_has_header = descriptor
        .lines()
        .find(|line| !line.trim().is_empty())
        .and_then(archive_header_task_id)
        == Some(task_id);
    let mut output = String::new();
    if !descriptor_has_header {
        output.push_str(&format!("### [{task_id}] выполнена\n\n"));
    }
    output.push_str(descriptor.trim_end());
    output.push_str("\n\n");
    output.push_str(metrics.trim_end());
    output.push('\n');
    output
}

fn parse_status(status: &str) -> Result<TaskState> {
    TaskState::from_markdown(status)
        .ok_or_else(|| ControlError::Contradiction(format!("unrecognized queue status {status:?}")))
}

fn set_line_field(text: &mut String, field: &str, value: &str) {
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    if let Some(line) = lines.iter_mut().find(|line| line.starts_with(field)) {
        *line = format!("{field} {value}");
    } else {
        if !lines.is_empty() && lines.last().is_some_and(|line| !line.is_empty()) {
            lines.push(String::new());
        }
        lines.push(format!("{field} {value}"));
    }
    *text = lines.join("\n");
    text.push('\n');
}

fn task_literal(state: TaskState) -> &'static str {
    match state {
        TaskState::NotStarted => "не начата",
        TaskState::Working => "в работе",
        TaskState::InReview => "на ревью",
        TaskState::Ready => "готова к слиянию",
        TaskState::Merged => "слита",
        TaskState::Published => "опубликована",
        TaskState::Done => "выполнена",
        TaskState::Escalated => "эскалирована",
        TaskState::Conflict => "конфликт",
    }
}

fn valid_task_transition(from: TaskState, to: TaskState) -> bool {
    matches!(
        (from, to),
        (TaskState::NotStarted, TaskState::Working)
            | (
                TaskState::Working,
                TaskState::InReview | TaskState::Escalated | TaskState::Conflict
            )
            | (
                TaskState::InReview,
                TaskState::Working | TaskState::Ready | TaskState::Escalated
            )
            | (
                TaskState::Ready,
                TaskState::Merged | TaskState::Conflict | TaskState::Escalated
            )
            | (
                TaskState::Merged,
                // A pre-publication policy approval can be terminally rejected after a typed
                // merge but before the primary branch moves. This is a one-way disposition of
                // that exact batch, not permission to rewrite an already-published task.
                TaskState::Published | TaskState::Conflict | TaskState::Escalated
            )
            | (TaskState::Published, TaskState::Done)
            | (TaskState::Conflict, TaskState::NotStarted)
    )
}

fn validate_task_id(task_id: &str) -> Result<()> {
    let valid = task_id.strip_prefix("T-").is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    });
    valid.then_some(()).ok_or_else(|| {
        ControlError::InvalidInput(format!("invalid task id {task_id:?}; expected T-<digits>"))
    })
}

fn validate_batch_id(batch_id: &str) -> Result<()> {
    (!batch_id.is_empty() && !batch_id.starts_with('-') && !batch_id.contains(['\0', '\r', '\n']))
        .then_some(())
        .ok_or_else(|| ControlError::InvalidInput(format!("invalid batch id {batch_id:?}")))
}

fn validate_ref(value: &str, name: &str) -> Result<()> {
    (!value.trim().is_empty() && !value.starts_with('-') && !value.contains(['\0', '\r', '\n']))
        .then_some(())
        .ok_or_else(|| ControlError::InvalidInput(format!("invalid {name}: {value:?}")))
}

/// Keep control-plane prose single-line and bounded. The append-only JSONL outbox has a separate
/// stricter redaction path; this prevents arbitrary multiline tool output from changing Markdown
/// structure before it reaches that boundary.
fn sanitize_reason(reason: &str) -> String {
    let compact = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    compact.chars().take(240).collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct Work {
        path: PathBuf,
    }

    impl Work {
        fn new() -> Self {
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "orchestrail-control-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn write(&self, relative: &str, content: &str) {
            let path = self.path.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        }
    }

    impl Drop for Work {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn capture_preserves_descriptor_body_and_updates_only_owned_fields() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] First task — статус: не начата\nBody\n\n### [T-2] Second — статус: не начата\n",
        );
        work.write(
            "tasks/T-1/task.md",
            "# T-1\n\nСтатус: не начата\nКонфликт-домен: engine/**\n\n## Criteria\n- preserve this\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        control
            .capture_task(
                "T-1",
                "B-20260724T000000Z",
                "task/T-1",
                ".work/worktrees/T-1",
                1,
            )
            .unwrap();
        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.contains("[T-1] First task — статус: в работе · батч=B-20260724T000000Z"));
        assert!(queue.contains("[T-2] Second — статус: не начата"));
        let descriptor = fs::read_to_string(work.path.join("tasks/T-1/task.md")).unwrap();
        assert!(descriptor.contains("## Criteria\n- preserve this"));
        assert!(descriptor.contains("Ветка: task/T-1"));
    }

    #[test]
    fn capture_preserves_prior_quarantine_attempt_but_not_the_stale_reason() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] Retried task — статус: не начата · попытка=2 · карантин=merge conflict\n",
        );
        work.write(
            "tasks/T-1/task.md",
            "# T-1\n\nСтатус: не начата\nКонфликт-домен: engine/**\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();

        control
            .capture_task(
                "T-1",
                "B-20260724T000000Z",
                "task/T-1",
                ".work/worktrees/T-1",
                1,
            )
            .unwrap();

        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.contains(
            "статус: в работе · батч=B-20260724T000000Z · worktree=.work/worktrees/T-1 · ветка=task/T-1 · попытка=2"
        ));
        assert!(
            !queue.contains("карантин="),
            "a completed attempt's quarantine reason must not describe a new capture"
        );
    }

    #[test]
    fn queue_lifecycle_preserves_a_captured_quarantine_attempt() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] Retried task — статус: в работе · батч=B-1 · worktree=.work/worktrees/T-1 · ветка=task/T-1 · попытка=2\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();

        for state in [
            TaskState::InReview,
            TaskState::Ready,
            TaskState::Merged,
            TaskState::Published,
        ] {
            control.patch_queue_state("T-1", state, None).unwrap();
            let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
            assert!(
                queue.contains("попытка=2"),
                "{state:?} must retain the prior quarantine counter"
            );
        }
        assert!(
            fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("статус: опубликована · попытка=2")
        );
    }

    #[test]
    fn terminal_escalation_discards_a_completed_quarantine_attempt() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] Retried task — статус: готова к слиянию · попытка=2\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();

        control
            .patch_queue_state("T-1", TaskState::Escalated, Some("retry budget exhausted"))
            .unwrap();

        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.contains("статус: эскалирована · причина=retry budget exhausted"));
        assert!(!queue.contains("попытка="));
    }

    #[test]
    fn publication_reanchor_preserves_a_quarantine_attempt() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] Candidate — статус: слита · попытка=2\n",
        );
        work.write(
            "tasks/T-1/task.md",
            "# T-1\nСтатус: слита\nКонфликт-домен: engine/**\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        control.reanchor_merged_task("T-1").unwrap();
        assert!(
            fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("статус: готова к слиянию · попытка=2")
        );
    }

    #[test]
    fn publication_reanchor_preserves_the_legacy_captured_queue_label() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] Candidate — статус: в работе · попытка=2\n",
        );
        work.write(
            "tasks/T-1/task.md",
            "# T-1\nСтатус: слита\nКонфликт-домен: engine/**\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();

        control.reanchor_merged_task("T-1").unwrap();

        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.contains("статус: в работе · попытка=2"));
        let descriptor = fs::read_to_string(work.path.join("tasks/T-1/task.md")).unwrap();
        assert!(descriptor.contains("Статус: готова к слиянию"));
    }

    #[test]
    fn publication_reanchor_reverts_only_a_matched_merged_candidate_and_is_idempotent() {
        let work = Work::new();
        work.write(QUEUE_FILE, "### [T-1] Candidate — статус: слита\n");
        work.write(
            "tasks/T-1/task.md",
            "# T-1\n\nСтатус: слита\nКонфликт-домен: engine/**\n\n## Criteria\n- preserve\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        control.reanchor_merged_task("T-1").unwrap();
        control
            .reanchor_merged_task("T-1")
            .expect("a crash after either owned write must retry safely");
        assert!(
            fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("статус: готова к слиянию")
        );
        let descriptor = fs::read_to_string(work.path.join("tasks/T-1/task.md")).unwrap();
        assert!(descriptor.contains("Статус: готова к слиянию"));
        assert!(descriptor.contains("## Criteria\n- preserve"));
    }

    #[test]
    fn publication_reanchor_refuses_a_mismatched_queue_without_mutating_the_descriptor() {
        let work = Work::new();
        work.write(QUEUE_FILE, "### [T-1] Candidate — статус: опубликована\n");
        let descriptor_path = "tasks/T-1/task.md";
        let descriptor = "# T-1\n\nСтатус: слита\nКонфликт-домен: engine/**\n";
        work.write(descriptor_path, descriptor);
        let control = ControlPlane::new(&work.path).unwrap();

        assert!(matches!(
            control.reanchor_merged_task("T-1"),
            Err(ControlError::Contradiction(message)) if message.contains("not a re-anchorable")
        ));
        assert_eq!(
            fs::read_to_string(work.path.join(descriptor_path)).unwrap(),
            descriptor,
            "validation must inspect both artifacts before changing either one"
        );
    }

    #[test]
    fn task_commit_persists_only_a_monotonic_risk_elevation() {
        let work = Work::new();
        work.write(
            "tasks/T-1/task.md",
            "# T-1\n\nСтатус: в работе\nРиск: low — local implementation\n\n## Criteria\n- preserve\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        let mut elevated = DescriptorPatch::state(TaskState::InReview);
        elevated.risk = Some(Risk::High);
        control.patch_descriptor("T-1", elevated).unwrap();
        let descriptor = fs::read_to_string(work.path.join("tasks/T-1/task.md")).unwrap();
        assert!(
            descriptor.contains("Риск: high — elevated by deterministic engine (previous: low)")
        );

        let mut lowered = DescriptorPatch::state(TaskState::InReview);
        lowered.risk = Some(Risk::Medium);
        assert!(matches!(
            control.patch_descriptor("T-1", lowered),
            Err(ControlError::Contradiction(message)) if message.contains("cannot decrease")
        ));
    }

    #[test]
    fn archive_moves_only_the_exact_queue_block_and_is_idempotent_after_move() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "# Queue\n\n### [T-1] First — статус: опубликована\nBody 1\n\n### [T-2] Second — статус: не начата\nBody 2\n",
        );
        work.write("tasks/T-1/task.md", "# T-1\nСтатус: опубликована\n");
        let control = ControlPlane::new(&work.path).unwrap();
        control.archive_task("T-1").unwrap();
        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(!queue.contains("[T-1]"));
        assert!(queue.contains("[T-2]"));
        let done = fs::read_to_string(work.path.join(DONE_FILE)).unwrap();
        assert!(done.contains("[T-1] First — статус: выполнена"));
        assert!(
            fs::read_to_string(work.path.join("tasks/T-1/task.md"))
                .unwrap()
                .contains("Статус: выполнена")
        );
        fs::remove_dir_all(work.path.join("tasks/T-1")).unwrap();
        control.archive_task("T-1").unwrap();
    }

    #[test]
    fn metrics_archive_repairs_a_header_only_crash_residue_and_replays_exactly_once() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "# Queue\n\n### [T-1] First — статус: опубликована\nBody 1\n",
        );
        work.write(
            "tasks/T-1/task.md",
            "# Активная задача T-1\n\nСтатус: опубликована\n\n## Criteria\n- retain full descriptor\n",
        );
        work.write(
            DONE_FILE,
            "# Completed tasks\n\n# Активная задача T-1\n\ncrash residue\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        control.mark_task_done_for_archive("T-1").unwrap();
        control
            .mark_task_done_for_archive("T-1")
            .expect("terminal transition and queue removal must replay");
        let metrics = "#### Метрики выполнения\n<!-- orchestra/task-execution-metrics@1 task_id=T-1 batch_id=B-1 status=no_data -->\n- Метрики недоступны.\n";

        control.project_task_archive("T-1", "B-1", metrics).unwrap();
        let first = fs::read_to_string(work.path.join(DONE_FILE)).unwrap();
        control
            .project_task_archive("T-1", "B-1", metrics)
            .expect("complete descriptor+metrics projection must replay");
        let replay = fs::read_to_string(work.path.join(DONE_FILE)).unwrap();

        assert_eq!(first, replay);
        assert_eq!(first.matches("# Активная задача T-1").count(), 1);
        assert_eq!(
            first
                .matches("orchestra/task-execution-metrics@1 task_id=T-1")
                .count(),
            1
        );
        assert!(first.contains("Статус: выполнена"));
        assert!(first.contains("## Criteria\n- retain full descriptor"));
        assert!(!first.contains("crash residue"));
        assert!(
            !fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("[T-1]")
        );
    }

    #[test]
    fn metrics_archive_wraps_a_descriptor_without_a_normative_archive_header() {
        let work = Work::new();
        work.write(QUEUE_FILE, "# Queue\n");
        work.write(
            "tasks/T-7/task.md",
            "# T-7\n\nСтатус: выполнена\n\nAcceptance stays here.\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        let metrics = "#### Метрики выполнения\n<!-- orchestra/task-execution-metrics@1 task_id=T-7 batch_id=B-7 status=error -->\n- Метрики недоступны.\n";

        control.project_task_archive("T-7", "B-7", metrics).unwrap();

        let done = fs::read_to_string(work.path.join(DONE_FILE)).unwrap();
        assert!(done.contains("### [T-7] выполнена\n\n# T-7"));
        assert_eq!(
            done.matches("orchestra/task-execution-metrics@1 task_id=T-7")
                .count(),
            1
        );
    }

    #[test]
    fn archive_accepts_a_published_descriptor_with_legacy_captured_queue() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] Published candidate — статус: в работе\nBody 1\n",
        );
        work.write("tasks/T-1/task.md", "# T-1\nСтатус: опубликована\n");
        let control = ControlPlane::new(&work.path).unwrap();

        control.archive_task("T-1").unwrap();

        assert!(
            !fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("[T-1]")
        );
        assert!(
            fs::read_to_string(work.path.join(DONE_FILE))
                .unwrap()
                .contains("[T-1] Published candidate")
        );
        assert!(
            fs::read_to_string(work.path.join("tasks/T-1/task.md"))
                .unwrap()
                .contains("Статус: выполнена")
        );
    }

    #[test]
    fn archive_refuses_a_captured_queue_without_a_published_descriptor() {
        let work = Work::new();
        let queue = "### [T-1] Unpublished candidate — статус: в работе\n";
        let descriptor = "# T-1\nСтатус: слита\n";
        work.write(QUEUE_FILE, queue);
        work.write("tasks/T-1/task.md", descriptor);
        let control = ControlPlane::new(&work.path).unwrap();

        assert!(matches!(
            control.archive_task("T-1"),
            Err(ControlError::Contradiction(message)) if message.contains("not publishable")
        ));
        assert_eq!(
            fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap(),
            queue
        );
        assert_eq!(
            fs::read_to_string(work.path.join("tasks/T-1/task.md")).unwrap(),
            descriptor
        );
        assert!(!work.path.join(DONE_FILE).exists());
    }

    #[test]
    fn cohort_cleanup_refuses_a_malformed_manifest_but_is_idempotent_after_removal() {
        let work = Work::new();
        work.write("batch.md", "# incomplete batch\n");
        let control = ControlPlane::new(&work.path).unwrap();
        assert!(matches!(
            control.remove_cohort_artifacts("B-1"),
            Err(ControlError::Contradiction(_))
        ));
        assert!(work.path.join("batch.md").exists());

        fs::remove_file(work.path.join("batch.md")).unwrap();
        control.remove_cohort_artifacts("B-1").unwrap();
        control.remove_cohort_artifacts("B-1").unwrap();
    }

    #[test]
    fn journal_materialization_is_idempotent_for_a_retried_effect() {
        let work = Work::new();
        let control = ControlPlane::new(&work.path).unwrap();
        let mut state = crate::processor::ProcessorState {
            phase: crate::processor::Phase::Cleaning,
            batch: Some(crate::processor::CohortRuntime {
                id: "B-1".into(),
                base: "main".into(),
                started_at_secs: 1,
                wave: 1,
                admitted_total: 1,
                admission_closed: None,
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: None,
                events_outbox_enabled: true,
            }),
            integration: crate::processor::IntegrationRuntime {
                degradations: vec![
                    "publication CI was not confirmed: manual confirmation is recommended".into(),
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        state.integration.pending_knowledge_curations.insert(
            "B-0".into(),
            crate::processor::PendingKnowledgeCuration {
                base: "base".into(),
                published_head: "head".into(),
                merged_tasks: Default::default(),
                fixed_task_findings: 0,
                integration_or_ci_signatures: 0,
                ci_failure_cycles: 0,
                quarantined_tasks: Default::default(),
                escalated_tasks: Default::default(),
                degradations: 1,
            },
        );

        control
            .write_journal_and_status(&state, "2026-07-25T12:00:00Z")
            .unwrap();
        control
            .write_journal_and_status(&state, "2026-07-25T12:00:00Z")
            .unwrap();

        let journal = fs::read_to_string(work.path.join("journal.md")).unwrap();
        assert_eq!(
            journal
                .lines()
                .filter(|line| { *line == "- 2026-07-25T12:00:00Z: phase=Cleaning, batch=B-1" })
                .count(),
            1
        );
        assert!(
            fs::read_to_string(work.path.join("status.md"))
                .unwrap()
                .contains("Degradations:\n- publication CI was not confirmed")
        );
        assert!(
            fs::read_to_string(work.path.join("status.md"))
                .unwrap()
                .contains("Pending knowledge curation:\n- B-0")
        );
        assert_eq!(
            journal
                .lines()
                .filter(|line| line.starts_with("- batch=B-1: degradation=publication CI"))
                .count(),
            1,
            "durable degradation accounting is retry-idempotent"
        );
    }

    #[test]
    fn cleaning_status_and_journal_report_deduplicated_codex_and_usage_telemetry() {
        use crate::events::{
            Actor, ActorKind, Event, EventType, Outbox, SCHEMA_VERSION, deterministic_event_id,
        };
        use serde_json::{Map, Value};

        let work = Work::new();
        let outbox = Outbox::new(&work.path);
        let mut codex_payload = Map::new();
        codex_payload.insert("task_id".into(), Value::from("T-1"));
        codex_payload.insert("role".into(), Value::from("coder"));
        codex_payload.insert("mode".into(), Value::from("full"));
        codex_payload.insert("attempt_number".into(), Value::from(1));
        codex_payload.insert("started_at".into(), Value::from("2026-07-25T12:00:00Z"));
        codex_payload.insert("ended_at".into(), Value::from("2026-07-25T12:00:01Z"));
        codex_payload.insert("duration_ms".into(), Value::from(1_000));
        codex_payload.insert("effective_model".into(), Value::from("default"));
        codex_payload.insert("effective_reasoning".into(), Value::from("high"));
        codex_payload.insert("effective_sandbox".into(), Value::from("workspace-write"));
        codex_payload.insert("effective_network".into(), Value::from("on"));
        codex_payload.insert("exit_code".into(), Value::from(1));
        codex_payload.insert("outcome".into(), Value::from("fallback"));
        codex_payload.insert("outcome_reason".into(), Value::from("ENV_LIMIT/network"));
        let codex = Event {
            schema_version: SCHEMA_VERSION,
            event_id: deterministic_event_id("orchestra/codex.attempt/T-1/coder/full/1"),
            occurred_at: "2026-07-25T12:00:01Z".into(),
            event_type: EventType::CodexAttempt,
            actor: Actor {
                kind: ActorKind::Agent,
                name: "processor".into(),
            },
            batch_id: Some("B-1".into()),
            task_id: Some("T-1".into()),
            payload_version: 1,
            payload: codex_payload,
        };
        outbox.append_idempotent(&codex).unwrap();
        outbox.append_idempotent(&codex).unwrap();

        let mut usage_payload = Map::new();
        usage_payload.insert("source".into(), Value::from("codex"));
        usage_payload.insert("estimated".into(), Value::Bool(false));
        usage_payload.insert("total_tokens".into(), Value::from(42));
        usage_payload.insert("usage_availability".into(), Value::from("available"));
        outbox
            .append_idempotent(&Event {
                schema_version: SCHEMA_VERSION,
                event_id: deterministic_event_id("test/usage/available"),
                occurred_at: "2026-07-25T12:00:01Z".into(),
                event_type: EventType::UsageRecorded,
                actor: Actor {
                    kind: ActorKind::Tool,
                    name: "codex".into(),
                },
                batch_id: Some("B-1".into()),
                task_id: Some("T-1".into()),
                payload_version: 1,
                payload: usage_payload,
            })
            .unwrap();

        let state = crate::processor::ProcessorState {
            phase: crate::processor::Phase::Cleaning,
            batch: Some(crate::processor::CohortRuntime {
                id: "B-1".into(),
                base: "main".into(),
                started_at_secs: 1,
                wave: 1,
                admitted_total: 1,
                admission_closed: None,
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: None,
                events_outbox_enabled: true,
            }),
            ..Default::default()
        };
        let control = ControlPlane::new(&work.path).unwrap();
        control
            .write_journal_and_status(&state, "2026-07-25T12:00:02Z")
            .unwrap();
        control
            .write_journal_and_status(&state, "2026-07-25T12:00:02Z")
            .unwrap();

        let status = fs::read_to_string(work.path.join("status.md")).unwrap();
        assert!(
            status.contains("Codex attempts: 0 ok, 1 fallback (ENV_LIMIT/network=1), 0 failed")
        );
        assert!(status.contains(
            "Usage: actual=42 tokens (by source: codex=42), estimated=0 tokens, calls=1, unmetered=0"
        ));
        let journal = fs::read_to_string(work.path.join("journal.md")).unwrap();
        assert_eq!(
            journal
                .lines()
                .filter(|line| line.starts_with("- batch=B-1: Codex attempts:"))
                .count(),
            1
        );
        assert_eq!(
            journal
                .lines()
                .filter(|line| line.starts_with("- batch=B-1: Usage:"))
                .count(),
            1
        );
    }

    #[test]
    fn queue_empty_current_milestone_is_called_out_in_status_and_journal() {
        let work = Work::new();
        work.write(
            "roadmap.md",
            "# Дорожная карта проекта\n\n## Текущее состояние\nТекущая веха: M1 — Delivery\n\n## Вехи\n### [M1] Delivery — статус: текущая\nЦель: ship\nДостижение: release published\nЗадачи: T-1, T-2\n",
        );
        work.write(DONE_FILE, "## [T-1] done\n");
        let control = ControlPlane::new(&work.path).unwrap();
        let state = crate::processor::ProcessorState {
            phase: crate::processor::Phase::Cleaning,
            batch: Some(crate::processor::CohortRuntime {
                id: "B-1".into(),
                base: "main".into(),
                started_at_secs: 1,
                wave: 1,
                admitted_total: 1,
                admission_closed: Some(CloseReasonWire::QueueEmpty),
                cohort_budget_secs: None,
                cohort_token_budget: None,
                cohort_token_budget_strict: false,
                token_budget_actual_tokens: None,
                events_outbox_enabled: true,
            }),
            ..Default::default()
        };

        control
            .write_journal_and_status(&state, "2026-07-25T12:00:00Z")
            .unwrap();
        control
            .write_journal_and_status(&state, "2026-07-25T12:00:00Z")
            .unwrap();

        let note = "очередь пуста; веха M1 ещё не достигнута — ожидается следующая порция задач";
        assert!(
            fs::read_to_string(work.path.join("status.md"))
                .unwrap()
                .contains(note)
        );
        assert_eq!(
            fs::read_to_string(work.path.join("journal.md"))
                .unwrap()
                .lines()
                .filter(|line| line.ends_with(note))
                .count(),
            1,
            "the derived queue-empty note is retry-idempotent like the primary journal entry"
        );
    }

    #[test]
    fn pause_status_is_derived_without_changing_the_processor_state() {
        let work = Work::new();
        let control = ControlPlane::new(&work.path).unwrap();
        let state = crate::processor::ProcessorState {
            phase: crate::processor::Phase::Rolling,
            ..Default::default()
        };

        control
            .write_pause_status(&state, "2026-07-25T12:00:00Z")
            .unwrap();
        control
            .write_pause_status(&state, "2026-07-25T12:00:00Z")
            .unwrap();

        let status = fs::read_to_string(work.path.join("status.md")).unwrap();
        assert!(status.contains("Phase: Rolling"));
        assert!(status.contains("Paused: .work/PAUSE is active"));
        let journal = fs::read_to_string(work.path.join("journal.md")).unwrap();
        assert_eq!(
            journal
                .lines()
                .filter(|line| {
                    *line == "- 2026-07-25T12:00:00Z: paused=.work/PAUSE, phase=Rolling, batch=none"
                })
                .count(),
            1
        );
        assert_eq!(state.phase, crate::processor::Phase::Rolling);
    }

    #[test]
    fn incompatible_capture_fails_without_rewriting_the_queue() {
        let work = Work::new();
        work.write(QUEUE_FILE, "### [T-1] First — статус: в работе\n");
        work.write("tasks/T-1/task.md", "# T-1\nСтатус: в работе\n");
        let control = ControlPlane::new(&work.path).unwrap();
        assert!(
            control
                .capture_task("T-1", "B-1", "task/T-1", ".work/worktrees/T-1", 1)
                .is_err()
        );
        assert_eq!(
            fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap(),
            "### [T-1] First — статус: в работе\n"
        );
    }

    #[test]
    fn orphaned_active_queue_label_returns_to_eligibility_without_a_descriptor() {
        let work = Work::new();
        work.write(
            QUEUE_FILE,
            "### [T-1] Orphaned — статус: в работе · попытка=3\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        control.return_orphaned_queue("T-1", Some(3)).unwrap();

        assert_eq!(
            fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap(),
            "### [T-1] Orphaned — статус: не начата · попытка=3\n"
        );
        assert!(
            control.return_orphaned_queue("T-1", Some(3)).is_err(),
            "a retried transaction must not reclassify an already eligible queue task"
        );
    }

    #[test]
    fn uncaptured_descriptor_is_removed_without_rewriting_its_stale_queue_row() {
        let work = Work::new();
        work.write(QUEUE_FILE, "### [T-1] Interrupted — статус: в работе\n");
        work.write(
            "tasks/T-1/task.md",
            "# T-1\nСтатус: в работе\nКонфликт-домен: engine/**\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();

        control.remove_uncaptured_descriptor("T-1").unwrap();
        assert!(!work.path.join("tasks/T-1").exists());
        assert_eq!(
            fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap(),
            "### [T-1] Interrupted — статус: в работе\n"
        );
        control.remove_uncaptured_descriptor("T-1").unwrap();
    }

    #[test]
    fn lost_queue_capture_is_restored_without_downgrading_a_ready_descriptor() {
        let work = Work::new();
        work.write(QUEUE_FILE, "### [T-1] Ready task — статус: не начата\n");
        work.write(
            "tasks/T-1/task.md",
            "# T-1\nСтатус: готова к слиянию\nБатч: B-1\nВетка: task/T-1\nWorktree: .work/worktrees/T-1\n",
        );
        let control = ControlPlane::new(&work.path).unwrap();
        control
            .restore_queue_capture("T-1", "B-1", "task/T-1", ".work/worktrees/T-1")
            .unwrap();

        assert!(
            fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("статус: готова к слиянию · батч=B-1")
        );
        assert!(
            fs::read_to_string(work.path.join("tasks/T-1/task.md"))
                .unwrap()
                .contains("Статус: готова к слиянию")
        );
    }
}
