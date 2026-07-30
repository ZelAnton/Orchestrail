//! The aggregated, read-only control-plane [`Snapshot`]: queue + descriptors + cohort +
//! integration + batch, loaded from a `.work/` directory per contract §13.
//!
//! Missing artifacts degrade to empty / `none` (no active cohort/integration/batch), because an
//! idle repository is valid. The fallible [`Snapshot::try_load`] distinguishes that case from an
//! existing artifact that cannot be read; deterministic decision paths must use it rather than
//! treating an I/O failure as idle. Nothing here writes, locks, or emits. Presentation is a compact
//! JSON line (`--json`, hand-built like `events::Event::to_json_line`) or a human-readable summary.

use std::ffi::OsString;
use std::fmt::Write as _;
use std::io;
use std::path::{Path, PathBuf};

use crate::work_fs::{self, MAX_CONTROL_BYTES};
use serde_json::{Value, json};

use super::batch::{BatchState, BatchTask, try_load_batch};
use super::cohort::{CohortState, try_load_cohort};
use super::descriptor::{Descriptor, try_load_descriptors};
use super::integration::{IntegrationSnapshot, try_load_integration};
use super::queue::{DeliveryTarget, QueueEntry, parse_queue};

/// A single deterministic snapshot of the control plane, sourced from one `.work/` directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub work_dir: PathBuf,
    pub queue: Vec<QueueEntry>,
    pub descriptors: Vec<Descriptor>,
    pub cohort: Option<CohortState>,
    pub integration: IntegrationSnapshot,
    pub batch: Option<BatchState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotInputStamp {
    work: PathBuf,
    queue: Option<work_fs::PlainFileStamp>,
    cohort: Option<work_fs::PlainFileStamp>,
    integration: Option<work_fs::PlainFileStamp>,
    batch: Option<work_fs::PlainFileStamp>,
    descriptors: Vec<(OsString, Option<work_fs::PlainFileStamp>)>,
}

/// Metadata-invalidated cache for passive observers that poll the same control-plane snapshot.
///
/// Every probe and reload uses [`crate::work_fs`], so cache hits do not weaken confinement or
/// size limits. Only successfully loaded snapshots are cached; any error (metadata or read) clears
/// the cache while preserving [`Snapshot::load`]'s existing best-effort return behavior.
#[derive(Debug, Default)]
pub struct SnapshotCache {
    cached: Option<(SnapshotInputStamp, Snapshot)>,
}

impl SnapshotCache {
    /// Load the current snapshot, reusing the parsed value while all source metadata is unchanged.
    pub fn load(&mut self, work_dir: impl AsRef<Path>) -> Snapshot {
        let work = work_dir.as_ref();
        let before = match snapshot_input_stamp(work) {
            Ok(stamp) => stamp,
            Err(_) => {
                self.cached = None;
                return Snapshot::load(work);
            }
        };
        if let Some((cached_stamp, snapshot)) = &self.cached
            && cached_stamp == &before
        {
            return snapshot.clone();
        }

        let snapshot = match Snapshot::try_load(work) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                self.cached = None;
                return Snapshot::empty(work);
            }
        };
        match snapshot_input_stamp(work) {
            Ok(after) if after == before => {
                self.cached = Some((after, snapshot.clone()));
            }
            _ => self.cached = None,
        }
        snapshot
    }

    /// Force the next [`load`](Self::load) to re-read and parse all snapshot sources.
    pub fn invalidate(&mut self) {
        self.cached = None;
    }
}

fn snapshot_input_stamp(work: &Path) -> io::Result<SnapshotInputStamp> {
    let stamp =
        |name: &str| work_fs::optional_plain_file_stamp(work, &work.join(name), MAX_CONTROL_BYTES);
    let tasks = work.join("tasks");
    let mut descriptors = Vec::new();
    if let Some(entries) = work_fs::plain_directory_entries(work, &tasks)? {
        for entry in entries {
            let path = entry.path();
            match work_fs::require_plain_directory(&path) {
                Ok(()) => {}
                Err(_) if entry.file_type().is_ok_and(|kind| !kind.is_dir()) => continue,
                Err(error) => return Err(error),
            }
            let name = entry.file_name();
            let task_md = path.join("task.md");
            descriptors.push((
                name,
                work_fs::optional_plain_file_stamp(work, &task_md, MAX_CONTROL_BYTES)?,
            ));
        }
        descriptors.sort_by(|left, right| left.0.cmp(&right.0));
    }

    Ok(SnapshotInputStamp {
        work: work.to_path_buf(),
        queue: stamp("Tasks_Queue.md")?,
        cohort: stamp("cohort_state.md")?,
        integration: stamp("integration_state.md")?,
        batch: stamp("batch.md")?,
        descriptors,
    })
}

impl Snapshot {
    /// Best-effort compatibility loader for passive observers. Missing **and unreadable** artifacts
    /// degrade to empty / `none`; a deterministic engine command or run must use
    /// [`Self::try_load`]
    /// instead so it never acts on an invented idle snapshot.
    pub fn load(work_dir: impl AsRef<Path>) -> Snapshot {
        let work = work_dir.as_ref();
        Self::try_load(work).unwrap_or_else(|_| Self::empty(work))
    }

    /// Load a read-only snapshot from a `.work/` directory. Missing individual artifacts degrade
    /// to empty / `none`; any other I/O error is returned with its original kind and context.
    pub fn try_load(work_dir: impl AsRef<Path>) -> io::Result<Snapshot> {
        let work = work_dir.as_ref();
        let queue_path = work.join("Tasks_Queue.md");
        let queue = work_fs::read_optional_text(work, &queue_path, MAX_CONTROL_BYTES)?
            .map(|text| parse_queue(&text))
            .unwrap_or_default();
        Ok(Snapshot {
            work_dir: work.to_path_buf(),
            queue,
            descriptors: try_load_descriptors(work)?,
            cohort: try_load_cohort(work)?,
            integration: try_load_integration(work)?,
            batch: try_load_batch(work)?,
        })
    }

    fn empty(work: &Path) -> Snapshot {
        Snapshot {
            work_dir: work.to_path_buf(),
            queue: Vec::new(),
            descriptors: Vec::new(),
            cohort: None,
            integration: IntegrationSnapshot {
                state: super::canonical::IntegrationState::None,
                review_sha: None,
                f_cycles: None,
            },
            batch: None,
        }
    }

    /// Render the snapshot as one compact JSON object (stable field order, canonical ASCII
    /// state names, `null` for absent cohort/batch and optional fields).
    pub fn to_json(&self) -> String {
        json!({
            "work_dir": self.work_dir.display().to_string(),
            "queue": self.queue.iter().map(queue_entry_json).collect::<Vec<_>>(),
            "descriptors": self.descriptors.iter().map(descriptor_json).collect::<Vec<_>>(),
            "cohort": self.cohort.as_ref().map(cohort_json),
            "integration": integration_json(&self.integration),
            "batch": self.batch.as_ref().map(batch_json),
        })
        .to_string()
    }

    /// Render a human-readable multi-line summary (ends with a newline).
    pub fn to_human(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "Control-plane snapshot (WORK={})",
            self.work_dir.display()
        );
        let _ = writeln!(s);

        match &self.cohort {
            Some(c) => {
                let adm = c.admission.map(|a| a.as_str()).unwrap_or("?");
                let _ = write!(
                    s,
                    "Cohort: {} · admission={}",
                    c.batch_id.as_deref().unwrap_or("(no batch id)"),
                    adm
                );
                if let Some(r) = &c.admission_reason {
                    let _ = write!(s, " (reason={r})");
                }
                if let Some(w) = c.wave {
                    let _ = write!(s, " · wave={w}");
                }
                if let Some(a) = c.admitted_total {
                    let _ = write!(s, " · admitted={a}");
                }
                let _ = writeln!(s);
            }
            None => {
                let _ = writeln!(s, "Cohort: none (no active cohort)");
            }
        }

        let i = &self.integration;
        let _ = write!(s, "Integration: {}", i.state.as_str());
        if let Some(f) = i.f_cycles {
            let _ = write!(s, " · F-cycles={f}");
        }
        if let Some(sha) = &i.review_sha {
            let _ = write!(s, " · review-sha={sha}");
        }
        let _ = writeln!(s);

        match &self.batch {
            Some(b) => {
                let _ = writeln!(
                    s,
                    "Batch: {} · base={} · integration-branch={}",
                    b.batch_id.as_deref().unwrap_or("?"),
                    b.base.as_deref().unwrap_or("?"),
                    b.integration_branch.as_deref().unwrap_or("?"),
                );
                for t in &b.tasks {
                    let _ = writeln!(
                        s,
                        "  {} · level={} · wave={} · domain={}",
                        t.id,
                        t.level.as_deref().unwrap_or("?"),
                        t.wave.map(|w| w.to_string()).unwrap_or_else(|| "?".into()),
                        t.domain.as_deref().unwrap_or("?"),
                    );
                }
            }
            None => {
                let _ = writeln!(s, "Batch: none");
            }
        }
        let _ = writeln!(s);

        let _ = writeln!(s, "Queue ({} entries):", self.queue.len());
        for e in &self.queue {
            let st = e.state.map(|x| x.as_str()).unwrap_or("?");
            let _ = write!(s, "  {:<7} {:<12} {}", e.id, st, e.title);
            let _ = write!(s, " · delivery_target={}", e.delivery_target.as_str());
            if e.delivery_target == DeliveryTarget::NextMajor {
                let _ = write!(s, " (parked, not admitted)");
            }
            if let Some(a) = e.attempt {
                let _ = write!(s, " · attempt={a}");
            }
            if let Some(q) = &e.quarantine {
                let _ = write!(s, " · quarantine={q}");
            }
            if let Some(r) = &e.escalation_reason {
                let _ = write!(s, " · reason={r}");
            }
            if !e.prerequisites.is_empty() {
                let _ = write!(s, " · prereqs=[{}]", e.prerequisites.join(", "));
            }
            let _ = writeln!(s);
        }
        let _ = writeln!(s);

        let _ = writeln!(s, "Descriptors ({}):", self.descriptors.len());
        for d in &self.descriptors {
            let st = d.state.map(|x| x.as_str()).unwrap_or("?");
            let _ = write!(s, "  {:<7} {}", d.id, st);
            if !d.prerequisites.is_empty() {
                let _ = write!(s, " · prereqs=[{}]", d.prerequisites.join(", "));
            }
            if let Some(domain) = &d.conflict_domain {
                let _ = write!(s, " · domain=[{}]", domain.join(", "));
            }
            let _ = writeln!(s);
        }
        s
    }
}

fn queue_entry_json(e: &QueueEntry) -> Value {
    json!({
        "id": e.id,
        "title": e.title,
        "state": e.state.map(|s| s.as_str()),
        "status_literal": e.status_literal,
        "attempt": e.attempt,
        "quarantine": e.quarantine,
        "escalation_reason": e.escalation_reason,
        "prerequisites": e.prerequisites,
        "delivery_target": e.delivery_target.as_str(),
    })
}

fn descriptor_json(d: &Descriptor) -> Value {
    json!({
        "id": d.id,
        "state": d.state.map(|s| s.as_str()),
        "status_literal": d.status_literal,
        "prerequisites": d.prerequisites,
        "conflict_domain": d.conflict_domain,
    })
}

fn cohort_json(c: &CohortState) -> Value {
    json!({
        "batch_id": c.batch_id,
        "admission": c.admission.map(|a| a.as_str()),
        "admission_literal": c.admission_literal,
        "admission_reason": c.admission_reason,
        "started_at": c.started_at,
        "wave": c.wave,
        "admitted_total": c.admitted_total,
    })
}

fn integration_json(i: &IntegrationSnapshot) -> Value {
    json!({
        "state": i.state.as_str(),
        "review_sha": i.review_sha,
        "f_cycles": i.f_cycles,
    })
}

fn batch_json(b: &BatchState) -> Value {
    json!({
        "batch_id": b.batch_id,
        "base": b.base,
        "integration_branch": b.integration_branch,
        "tasks": b.tasks.iter().map(batch_task_json).collect::<Vec<_>>(),
    })
}

fn batch_task_json(t: &BatchTask) -> Value {
    json!({
        "id": t.id,
        "level": t.level,
        "branch": t.branch,
        "worktree": t.worktree,
        "domain": t.domain,
        "wave": t.wave,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A throwaway `.work/`-shaped directory populated by the caller.
    struct TmpWork {
        dir: PathBuf,
    }
    impl TmpWork {
        fn new() -> TmpWork {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "orchestra-state-snap-{}-{nanos}-{n}",
                std::process::id()
            ));
            fs::create_dir_all(&dir).unwrap();
            TmpWork { dir }
        }
        fn write(&self, rel: &str, contents: &str) {
            let path = self.dir.join(rel);
            if let Some(p) = path.parent() {
                fs::create_dir_all(p).unwrap();
            }
            fs::write(path, contents).unwrap();
        }
    }
    impl Drop for TmpWork {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn empty_work_dir_is_idle_and_json_valid() {
        let w = TmpWork::new();
        let snap = Snapshot::load(&w.dir);
        assert!(snap.queue.is_empty());
        assert!(snap.descriptors.is_empty());
        assert!(snap.cohort.is_none());
        assert!(snap.batch.is_none());
        assert_eq!(snap.integration.state.as_str(), "none");
        // The JSON must be well-formed and reflect the idle state.
        let v: Value = serde_json::from_str(&snap.to_json()).expect("valid JSON");
        assert!(v["cohort"].is_null());
        assert!(v["batch"].is_null());
        assert_eq!(v["integration"]["state"], "none");
    }

    #[test]
    fn fallible_load_does_not_mask_an_unreadable_queue_artifact() {
        let w = TmpWork::new();
        // A directory at the queue path reliably makes `read_to_string` fail on every supported
        // platform, without relying on ACL manipulation in a test process.
        fs::create_dir(w.dir.join("Tasks_Queue.md")).expect("create directory at queue path");
        let error = Snapshot::try_load(&w.dir).expect_err("existing unreadable queue must fail");
        assert_ne!(error.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn full_work_dir_aggregates_all_sources() {
        let w = TmpWork::new();
        w.write(
            "Tasks_Queue.md",
            "### [T-102] TUI экран — статус: в работе · батч=B-1\nПредпосылки: T-101\n",
        );
        w.write("tasks/T-102/task.md", "# T-102\nСтатус: на ревью\n");
        w.write(
            "cohort_state.md",
            "# Cohort state — Batch B-1\nПриём: открыт\nВолна: 1\nAdmitted всего: 1\n",
        );
        w.write("integration_state.md", "# int\nF-циклов: 1\n");
        w.write(
            "batch.md",
            "# Batch B-1\nБаза: abc123\n## Задачи\n- [T-102] уровень=coder ветка=task/T-102 домен=tui/** волна=1\n",
        );

        let snap = Snapshot::load(&w.dir);
        assert_eq!(snap.queue.len(), 1);
        assert_eq!(snap.descriptors.len(), 1);
        assert_eq!(
            snap.descriptors[0].state.map(|s| s.as_str()),
            Some("in-review")
        );
        assert_eq!(
            snap.cohort.as_ref().unwrap().admission.map(|a| a.as_str()),
            Some("open")
        );
        assert_eq!(snap.integration.state.as_str(), "in-progress");
        assert_eq!(snap.batch.as_ref().unwrap().tasks.len(), 1);

        let v: Value = serde_json::from_str(&snap.to_json()).expect("valid JSON");
        assert_eq!(v["queue"][0]["state"], "working");
        assert_eq!(v["descriptors"][0]["state"], "in-review");
        assert_eq!(v["cohort"]["admission"], "open");
        assert_eq!(v["integration"]["state"], "in-progress");
        assert_eq!(v["batch"]["tasks"][0]["level"], "coder");

        // Human render mentions the key facts.
        let human = snap.to_human();
        assert!(human.contains("Integration: in-progress"));
        assert!(human.contains("admission=open"));
        assert!(human.contains("T-102"));
    }

    #[test]
    fn snapshot_cache_reuses_unchanged_inputs_and_observes_metadata_changes() {
        let w = TmpWork::new();
        w.write(
            "Tasks_Queue.md",
            "### [T-102] First title — статус: в работе\n",
        );
        w.write("tasks/T-102/task.md", "# T-102\nСтатус: в работе\n");
        let mut cache = SnapshotCache::default();

        let first = cache.load(&w.dir);
        let first_stamp = cache
            .cached
            .as_ref()
            .expect("stable initial load is cached")
            .0
            .clone();
        let unchanged = cache.load(&w.dir);
        assert_eq!(unchanged, first);
        assert_eq!(
            cache.cached.as_ref().expect("cache remains populated").0,
            first_stamp,
            "unchanged metadata must retain the same cache key"
        );

        w.write(
            "Tasks_Queue.md",
            "### [T-102] A longer changed title — статус: на ревью\n",
        );
        let changed = cache.load(&w.dir);
        assert_ne!(changed.queue, first.queue);
        assert_eq!(changed.queue[0].title, "A longer changed title");
        assert_ne!(
            cache.cached.as_ref().expect("changed load is recached").0,
            first_stamp,
            "changed length/mtime must invalidate the snapshot parse"
        );
    }

    #[test]
    fn snapshot_cache_retries_after_a_read_error() {
        let w = TmpWork::new();
        fs::write(w.dir.join("Tasks_Queue.md"), [0xff])
            .expect("write invalid UTF-8 queue artifact");
        let mut cache = SnapshotCache::default();

        let degraded = cache.load(&w.dir);
        assert!(degraded.queue.is_empty());
        assert!(
            cache.cached.is_none(),
            "a degraded snapshot must not be cached"
        );

        w.write(
            "Tasks_Queue.md",
            "### [T-102] Recovered title — статус: в работе\n",
        );
        let recovered = cache.load(&w.dir);
        assert_eq!(recovered.queue.len(), 1);
        assert_eq!(recovered.queue[0].title, "Recovered title");
        assert!(
            cache.cached.is_some(),
            "the successful retry should be cached"
        );
    }
}
