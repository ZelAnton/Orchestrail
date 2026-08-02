//! Native drain for `.work/queue_inbox` proposals created while the processor lease is active.
//!
//! Producers never edit `Tasks_Queue.md` under the owner lease.  Instead they atomically leave a
//! small JSON proposal in this directory; at the next safe admission boundary the processor
//! validates and consumes a stable snapshot.  The queue replacement happens before source-file
//! consumption, so a crash is harmless: a retry recognizes the already-appended title and only
//! removes the duplicate inbox record.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::state::{DeliveryTarget, QueueEntry, TaskState, archive_header_task_id};
use crate::task_id::is_task_id;
use crate::work_fs::{self, MAX_CONTROL_BYTES};

const QUEUE_FILE: &str = "Tasks_Queue.md";
const DONE_FILE: &str = "Tasks_Done.md";
const QUEUE_STATE_FILE: &str = "queue_state.json";
const INBOX_DIRECTORY: &str = "queue_inbox";
const REJECTED_DIRECTORY: &str = "rejected";
/// A private, durable intent record for the otherwise non-atomic queue + generation update.
/// It deliberately does not have a `.json` suffix so producer scanning can never mistake it for
/// an inbox proposal.
const DRAIN_TRANSACTION_FILE: &str = ".native-drain.transaction";
const MAX_RECORD_BYTES: u64 = 524_288;
const MAX_TRANSACTION_BYTES: u64 = 4_194_304;
const MAX_RECORDS_PER_DRAIN: usize = 4_096;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DrainResult {
    pub added_tasks: Vec<String>,
    pub added_proposals: Vec<String>,
    pub skipped_duplicates: Vec<String>,
    pub quarantined: Vec<String>,
}

impl DrainResult {
    pub fn changed(&self) -> bool {
        !(self.added_tasks.is_empty()
            && self.added_proposals.is_empty()
            && self.skipped_duplicates.is_empty()
            && self.quarantined.is_empty())
    }
}

#[derive(Debug)]
pub enum QueueInboxError {
    Io(io::Error),
    Invalid(String),
}

impl fmt::Display for QueueInboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "queue inbox I/O error: {error}"),
            Self::Invalid(message) => write!(f, "invalid queue inbox: {message}"),
        }
    }
}

impl std::error::Error for QueueInboxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

impl From<io::Error> for QueueInboxError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, QueueInboxError>;

/// Recovery data for exactly one queue-inbox commit.  Renaming two independent files cannot be
/// atomic on a normal filesystem; this intent record makes the small commit recoverable before a
/// later planner sees either its old or new generation as authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DrainTransaction {
    queue_before_hash: String,
    queue_after_hash: String,
    queue_state_before_hash: Option<String>,
    queue_state_after: String,
    /// The original digest protects a replacement producer record from being consumed during
    /// crash recovery.  Files already consumed before the crash are represented by absence.
    consumed: BTreeMap<String, String>,
}

/// Drain only direct `*.json` children in their lexical filename order. `occurred_at` is used
/// solely to name a human-auditable quarantine pair; it cannot affect task/proposal allocation.
pub fn drain(work: &Path, occurred_at: &str) -> Result<DrainResult> {
    let work_metadata = fs::symlink_metadata(work)?;
    if !work_metadata.is_dir() || work_fs::redirected(&work_metadata) {
        return Err(QueueInboxError::Invalid(format!(
            "work directory is not a plain directory: {}",
            work.display()
        )));
    }
    let inbox = work.join(INBOX_DIRECTORY);
    recover_pending_transaction(work, &inbox)?;
    let entries = direct_json_entries(work, &inbox)?;
    if entries.is_empty() {
        return Ok(DrainResult::default());
    }

    let queue_path = work.join(QUEUE_FILE);
    let original =
        work_fs::read_optional_text(work, &queue_path, MAX_CONTROL_BYTES)?.unwrap_or_default();
    let queued = crate::state::queue::parse_queue(&original);
    let completed = completed_ids(work)?;
    let active = active_task_ids(work)?;
    let mut task_ids = queued
        .iter()
        .filter_map(|entry| canonical_task_id(&entry.id))
        .chain(completed.iter().filter_map(|id| canonical_task_id(id)))
        .chain(active.iter().filter_map(|id| canonical_task_id(id)))
        .collect::<BTreeSet<_>>();
    let mut task_spellings = known_task_spellings(&queued, &completed, work)?;
    let mut titles = known_task_titles(&original, &queued, work)?;
    let mut proposal_titles = known_proposal_titles(&original);
    let mut task_max = max_known_task_number(work, &queued)?;
    let mut proposal_max = max_known_proposal_number(work, &original)?;
    let mut dependencies = queued
        .iter()
        .filter_map(|entry| {
            let id = canonical_task_id(&entry.id)?;
            Some((
                id,
                entry
                    .prerequisites
                    .iter()
                    .filter_map(|id| canonical_task_id(id))
                    .collect::<BTreeSet<_>>(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let escalated = queued
        .iter()
        .filter(|entry| entry.state == Some(TaskState::Escalated))
        .filter_map(|entry| canonical_task_id(&entry.id))
        .collect::<BTreeSet<_>>();
    let mut delivery = queued
        .iter()
        .filter_map(|entry| canonical_task_id(&entry.id).map(|id| (id, entry.delivery_target)))
        .collect::<BTreeMap<_, _>>();

    let mut queue_blocks = Vec::new();
    let mut consume = Vec::new();
    let mut quarantine = Vec::new();
    let mut result = DrainResult::default();

    for path in entries {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                QueueInboxError::Invalid(format!("invalid inbox filename {}", path.display()))
            })?
            .to_owned();
        match read_record(work, &path) {
            Ok((
                Incoming::Task {
                    title,
                    body,
                    predecessors,
                },
                source_hash,
            )) => {
                let normalized = normalize_title(&title);
                if titles.contains(&normalized) {
                    result.skipped_duplicates.push(name);
                    consume.push((path, source_hash));
                    continue;
                }
                let next = task_max.saturating_add(1);
                let id = format!("T-{next:03}");
                match validate_task(
                    &id,
                    &body,
                    &predecessors,
                    &task_ids,
                    &dependencies,
                    &delivery,
                    &escalated,
                ) {
                    Ok(()) => {
                        task_max = next;
                        task_ids.insert(id.clone());
                        task_spellings.insert(id.clone(), id.clone());
                        titles.insert(normalized);
                        dependencies.insert(id.clone(), predecessors.iter().cloned().collect());
                        delivery.insert(id.clone(), delivery_target(&body));
                        let rendered_predecessors = predecessors
                            .iter()
                            .map(|predecessor| {
                                task_spellings
                                    .get(predecessor)
                                    .cloned()
                                    .unwrap_or_else(|| predecessor.clone())
                            })
                            .collect::<Vec<_>>();
                        queue_blocks.push(render_task(&id, &title, &body, &rendered_predecessors));
                        result.added_tasks.push(id);
                        consume.push((path, source_hash));
                    }
                    Err(reason) => quarantine.push((path, name, reason)),
                }
            }
            Ok((
                Incoming::Proposal {
                    title,
                    body,
                    source,
                    suggested_target,
                },
                source_hash,
            )) => {
                let normalized = normalize_title(&title);
                if proposal_titles.contains(&normalized) {
                    result.skipped_duplicates.push(name);
                    consume.push((path, source_hash));
                    continue;
                }
                proposal_max = proposal_max.saturating_add(1);
                let id = format!("P-{:03}", proposal_max);
                proposal_titles.insert(normalized);
                queue_blocks.push(render_proposal(
                    &id,
                    &title,
                    &body,
                    source.as_deref(),
                    suggested_target.as_deref(),
                ));
                result.added_proposals.push(id);
                consume.push((path, source_hash));
            }
            Err(reason) => quarantine.push((path, name, reason)),
        }
    }

    if !queue_blocks.is_empty() {
        let generation_update = next_queue_generation_update(work, queue_blocks.len())?;
        let updated = append_blocks(&original, &queue_blocks);
        let transaction = DrainTransaction {
            queue_before_hash: content_hash(original.as_bytes()),
            queue_after_hash: content_hash(updated.as_bytes()),
            queue_state_before_hash: read_optional_hash(
                work,
                &work.join(QUEUE_STATE_FILE),
                MAX_CONTROL_BYTES,
            )?,
            queue_state_after: String::from_utf8(generation_update.clone()).map_err(|error| {
                QueueInboxError::Invalid(format!(
                    "native queue generation payload is not UTF-8: {error}"
                ))
            })?,
            consumed: consume
                .iter()
                .map(|(path, source_hash)| {
                    let name = path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or_else(|| {
                            QueueInboxError::Invalid(format!(
                                "invalid inbox filename {}",
                                path.display()
                            ))
                        })?
                        .to_owned();
                    Ok((name, source_hash.clone()))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
        };
        create_transaction_marker(work, &inbox, &transaction)?;
        work_fs::replace_file(work, &queue_path, updated.as_bytes(), MAX_CONTROL_BYTES)
            .map_err(QueueInboxError::Io)?;
        work_fs::replace_file(
            work,
            &work.join(QUEUE_STATE_FILE),
            &generation_update,
            MAX_CONTROL_BYTES,
        )
        .map_err(QueueInboxError::Io)?;
        consume_transaction_sources(work, &inbox, &transaction)?;
        remove_transaction_marker(work, &inbox)?;
        // The transaction owns all successful/duplicate source removals.  Do not attempt to
        // remove the same paths again below.
        consume.clear();
    }
    for (path, expected_hash) in consume {
        if plain_file_hash(work, &path, MAX_RECORD_BYTES)? != expected_hash {
            return Err(QueueInboxError::Invalid(format!(
                "queue inbox source changed before consumption: {}",
                path.display()
            )));
        }
        work_fs::remove_plain_file(work, &path)?;
    }
    for (path, name, reason) in quarantine {
        move_to_quarantine(work, &inbox, &path, &name, occurred_at, &reason.to_string())?;
        result.quarantined.push(name);
    }
    Ok(result)
}

/// Finish or reject an interrupted queue/generation commit before inspecting new records.  The
/// queue file is replaced first and therefore has only two valid crash observations: its exact
/// pre-commit or post-commit digest.  Any third value proves that a different writer modified the
/// control plane and is a fail-closed condition.
fn recover_pending_transaction(work: &Path, inbox: &Path) -> Result<()> {
    let marker = inbox.join(DRAIN_TRANSACTION_FILE);
    let metadata = match fs::symlink_metadata(&marker) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || work_fs::redirected(&metadata) {
        return Err(QueueInboxError::Invalid(format!(
            "queue inbox transaction marker is not a plain file: {}",
            marker.display()
        )));
    }
    if metadata.len() > MAX_TRANSACTION_BYTES {
        return Err(QueueInboxError::Invalid(format!(
            "queue inbox transaction marker exceeds the {MAX_TRANSACTION_BYTES}-byte limit: {}",
            marker.display()
        )));
    }
    let text = work_fs::read_required_text(work, &marker, MAX_TRANSACTION_BYTES)?;
    let transaction: DrainTransaction = serde_json::from_str(&text).map_err(|error| {
        QueueInboxError::Invalid(format!(
            "queue inbox transaction marker is invalid JSON at {}: {error}",
            marker.display()
        ))
    })?;
    validate_transaction(&transaction)?;

    let queue_path = work.join(QUEUE_FILE);
    let queue =
        work_fs::read_optional_bytes(work, &queue_path, MAX_CONTROL_BYTES)?.unwrap_or_default();
    let queue_hash = content_hash(&queue);
    let state_path = work.join(QUEUE_STATE_FILE);
    let state_hash = read_optional_hash(work, &state_path, MAX_CONTROL_BYTES)?;

    if queue_hash == transaction.queue_before_hash {
        if state_hash != transaction.queue_state_before_hash {
            return Err(QueueInboxError::Invalid(
                "queue generation changed while a native inbox transaction was pending".into(),
            ));
        }
        // No queue mutation survived.  The source records remain, so dropping the abandoned
        // intent lets the normal deterministic drain reconstruct the same commit.
        remove_transaction_marker(work, inbox)?;
        return Ok(());
    }
    if queue_hash != transaction.queue_after_hash {
        return Err(QueueInboxError::Invalid(
            "queue changed outside a pending native inbox transaction; refusing recovery".into(),
        ));
    }

    let expected_after_state_hash = content_hash(transaction.queue_state_after.as_bytes());
    match state_hash {
        Some(ref hash) if hash == &expected_after_state_hash => {}
        current if current == transaction.queue_state_before_hash => {
            work_fs::replace_file(
                work,
                &state_path,
                transaction.queue_state_after.as_bytes(),
                MAX_CONTROL_BYTES,
            )
            .map_err(QueueInboxError::Io)?;
        }
        _ => {
            return Err(QueueInboxError::Invalid(
                "queue generation changed outside a pending native inbox transaction; refusing recovery"
                    .into(),
            ));
        }
    }
    consume_transaction_sources(work, inbox, &transaction)?;
    remove_transaction_marker(work, inbox)
}

fn create_transaction_marker(
    work: &Path,
    inbox: &Path,
    transaction: &DrainTransaction,
) -> Result<()> {
    if work_fs::plain_directory_entries(work, inbox)?.is_none() {
        return Err(QueueInboxError::Invalid(format!(
            "queue inbox directory disappeared before transaction creation: {}",
            inbox.display()
        )));
    }
    work_fs::require_plain_directory(inbox)?;
    let marker = inbox.join(DRAIN_TRANSACTION_FILE);
    let payload = serde_json::to_vec(transaction).expect("serializable native queue transaction");
    if payload.len() as u64 > MAX_TRANSACTION_BYTES {
        return Err(QueueInboxError::Invalid(format!(
            "native queue inbox transaction exceeds the {MAX_TRANSACTION_BYTES}-byte limit"
        )));
    }
    let mut file = work_fs::create_new_plain_file(&marker).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            QueueInboxError::Invalid(
                "native queue inbox transaction is already pending; recovery was skipped".into(),
            )
        } else {
            QueueInboxError::Io(error)
        }
    })?;
    if let Err(error) = file.write_all(&payload).and_then(|()| file.sync_all()) {
        let _ = work_fs::remove_plain_file(work, &marker);
        return Err(error.into());
    }
    work_fs::require_plain_directory(inbox)?;
    Ok(())
}

fn remove_transaction_marker(work: &Path, inbox: &Path) -> Result<()> {
    let marker = inbox.join(DRAIN_TRANSACTION_FILE);
    work_fs::remove_plain_file(work, &marker)?;
    Ok(())
}

fn consume_transaction_sources(
    work: &Path,
    inbox: &Path,
    transaction: &DrainTransaction,
) -> Result<()> {
    for (name, expected_hash) in &transaction.consumed {
        if !is_plain_inbox_name(name) {
            return Err(QueueInboxError::Invalid(
                "queue inbox transaction has an unsafe source filename".into(),
            ));
        }
        let path = inbox.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file() || work_fs::redirected(&metadata) {
                    return Err(QueueInboxError::Invalid(format!(
                        "queue inbox transaction source is not a plain file: {}",
                        path.display()
                    )));
                }
                if plain_file_hash(work, &path, MAX_RECORD_BYTES)? != *expected_hash {
                    return Err(QueueInboxError::Invalid(format!(
                        "queue inbox transaction source changed before consumption: {}",
                        path.display()
                    )));
                }
                work_fs::remove_plain_file(work, &path)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn validate_transaction(transaction: &DrainTransaction) -> Result<()> {
    let generation_is_valid = serde_json::from_str::<Value>(&transaction.queue_state_after)
        .ok()
        .and_then(|value| {
            value
                .as_object()
                .and_then(|object| object.get("generation"))
                .and_then(Value::as_u64)
        })
        .is_some();
    if !is_sha256_hex(&transaction.queue_before_hash)
        || !is_sha256_hex(&transaction.queue_after_hash)
        || transaction
            .queue_state_before_hash
            .as_deref()
            .is_some_and(|hash| !is_sha256_hex(hash))
        || transaction.queue_state_after.len() > 4_096
        || !generation_is_valid
        || transaction
            .consumed
            .iter()
            .any(|(name, hash)| !is_plain_inbox_name(name) || !is_sha256_hex(hash))
    {
        return Err(QueueInboxError::Invalid(
            "queue inbox transaction marker has invalid fields".into(),
        ));
    }
    Ok(())
}

fn read_optional_hash(work: &Path, path: &Path, max_bytes: u64) -> Result<Option<String>> {
    Ok(work_fs::read_optional_bytes(work, path, max_bytes)?.map(|bytes| content_hash(&bytes)))
}

fn plain_file_hash(work: &Path, path: &Path, max_bytes: u64) -> Result<String> {
    let bytes = work_fs::read_required_text(work, path, max_bytes)
        .map(String::into_bytes)
        .map_err(QueueInboxError::Io)?;
    Ok(content_hash(&bytes))
}

fn content_hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_plain_inbox_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains(['/', '\\', ':', '\0'])
        && Path::new(name)
            .file_name()
            .is_some_and(|file_name| file_name == name)
}

/// Validate and construct the legacy-compatible queue generation update before touching the
/// queue. The owner lease makes this a single-writer transaction; consumers that use an
/// optimistic `expected-generation` guard will observe every native inbox insertion.
fn next_queue_generation_update(work: &Path, delta: usize) -> Result<Vec<u8>> {
    let path = work.join(QUEUE_STATE_FILE);
    let generation = match work_fs::read_optional_text(work, &path, MAX_CONTROL_BYTES)? {
        Some(text) => {
            let value: Value = serde_json::from_str(&text).map_err(|error| {
                QueueInboxError::Invalid(format!(
                    "queue generation state is not JSON at {}: {error}",
                    path.display()
                ))
            })?;
            value
                .as_object()
                .and_then(|object| object.get("generation"))
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    QueueInboxError::Invalid(format!(
                        "queue generation state has no unsigned generation at {}",
                        path.display()
                    ))
                })?
        }
        None => 0,
    };
    let delta = u64::try_from(delta)
        .map_err(|_| QueueInboxError::Invalid("queue generation delta cannot fit in u64".into()))?;
    let next = generation
        .checked_add(delta)
        .ok_or_else(|| QueueInboxError::Invalid("queue generation would overflow u64".into()))?;
    let payload = serde_json::json!({ "generation": next });
    Ok(format!(
        "{}\n",
        serde_json::to_string(&payload).expect("serializable generation")
    )
    .into_bytes())
}

#[derive(Debug)]
enum Incoming {
    Task {
        title: String,
        body: String,
        predecessors: Vec<String>,
    },
    Proposal {
        title: String,
        body: String,
        source: Option<String>,
        suggested_target: Option<String>,
    },
}

fn direct_json_entries(work: &Path, inbox: &Path) -> Result<Vec<PathBuf>> {
    let Some(entries) = work_fs::plain_directory_entries(work, inbox)? else {
        return Ok(Vec::new());
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry.path();
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if !name.to_ascii_lowercase().ends_with(".json") {
            continue;
        }
        let metadata = fs::symlink_metadata(&path)?;
        // The contract scans only direct JSON *files*. A plain directory whose name happens to
        // end in `.json` is outside that set (like the permanent `rejected/` directory), while
        // a redirect or non-file special object remains unsafe and therefore fails closed.
        if metadata.is_dir() && !work_fs::redirected(&metadata) {
            continue;
        }
        if !metadata.is_file() || work_fs::redirected(&metadata) {
            return Err(QueueInboxError::Invalid(format!(
                "queue inbox entry is not a plain file: {}",
                path.display()
            )));
        }
        paths.push(path);
        if paths.len() > MAX_RECORDS_PER_DRAIN {
            return Err(QueueInboxError::Invalid(format!(
                "queue inbox exceeds the {MAX_RECORDS_PER_DRAIN}-record drain limit"
            )));
        }
    }
    paths.sort();
    Ok(paths)
}

fn read_record(work: &Path, path: &Path) -> Result<(Incoming, String)> {
    let bytes = work_fs::read_required_bytes(work, path, MAX_RECORD_BYTES)?;
    let source_hash = content_hash(&bytes);
    let text = String::from_utf8(bytes).map_err(|error| {
        QueueInboxError::Invalid(format!(
            "queue inbox record is not UTF-8 at {}: {error}",
            path.display()
        ))
    })?;
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        QueueInboxError::Invalid(format!("{} is not valid JSON: {error}", path.display()))
    })?;
    let object = value.as_object().ok_or_else(|| {
        QueueInboxError::Invalid(format!("{} must contain a JSON object", path.display()))
    })?;
    let kind = string_field(object, "kind")?.unwrap_or_else(|| "task".into());
    let title = required_title(object)?;
    let body = optional_text(object, "body")?.unwrap_or_default();
    validate_body(&body)?;
    match kind.trim().to_ascii_lowercase().as_str() {
        "task" => {
            let predecessors = match object.get("predecessors") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(values)) => values
                    .iter()
                    .map(|value| {
                        let raw = value.as_str().ok_or_else(|| {
                            QueueInboxError::Invalid("task predecessor must be a string".into())
                        })?;
                        canonical_task_id(raw).ok_or_else(|| {
                            QueueInboxError::Invalid(format!(
                                "task predecessor is not a whole T-NNN id: {raw:?}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                Some(_) => {
                    return Err(QueueInboxError::Invalid(
                        "task predecessors must be an array".into(),
                    ));
                }
            };
            Ok((
                Incoming::Task {
                    title,
                    body,
                    predecessors,
                },
                source_hash,
            ))
        }
        "proposal" => Ok((
            Incoming::Proposal {
                title,
                body,
                source: optional_line(object, "source")?,
                suggested_target: optional_line(object, "suggested_target")?,
            },
            source_hash,
        )),
        other => Err(QueueInboxError::Invalid(format!(
            "unknown queue inbox record kind {other:?}"
        ))),
    }
}

fn string_field(object: &serde_json::Map<String, Value>, key: &str) -> Result<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(QueueInboxError::Invalid(format!(
            "record field {key} must be a string"
        ))),
    }
}

fn required_nonempty_line(object: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    let Some(value) = string_field(object, key)? else {
        return Err(QueueInboxError::Invalid(format!("record has no {key}")));
    };
    validate_line(&value, key)?;
    Ok(value)
}

fn required_title(object: &serde_json::Map<String, Value>) -> Result<String> {
    let title = required_nonempty_line(object, "title")?;
    if title.contains("статус:")
        || title.contains("Статус:")
        || title.contains("— kind:")
        || title.contains("— status:")
    {
        return Err(QueueInboxError::Invalid(
            "record title may not contain a queue-header delimiter".into(),
        ));
    }
    Ok(title)
}

fn optional_line(object: &serde_json::Map<String, Value>, key: &str) -> Result<Option<String>> {
    let value = string_field(object, key)?;
    if let Some(value) = &value {
        validate_line(value, key)?;
    }
    Ok(value.filter(|value| !value.is_empty()))
}

fn optional_text(object: &serde_json::Map<String, Value>, key: &str) -> Result<Option<String>> {
    string_field(object, key)
}

fn validate_line(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 4_096 || value.contains(['\0', '\r', '\n']) {
        return Err(QueueInboxError::Invalid(format!(
            "record field {field} must be a nonempty single line up to 4096 bytes"
        )));
    }
    Ok(())
}

fn validate_body(body: &str) -> Result<()> {
    if body.len() > 262_144 || body.contains('\0') {
        return Err(QueueInboxError::Invalid(
            "record body exceeds 262144 bytes or contains NUL".into(),
        ));
    }
    if body.lines().any(|line| line.starts_with("### [")) {
        return Err(QueueInboxError::Invalid(
            "record body may not inject a queue record header".into(),
        ));
    }
    if body.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with("Предпосылки:") || trimmed.starts_with("Predecessors:")
    }) {
        return Err(QueueInboxError::Invalid(
            "record body may not override the JSON predecessors field".into(),
        ));
    }
    Ok(())
}

fn validate_task(
    id: &str,
    body: &str,
    predecessors: &[String],
    known_ids: &BTreeSet<String>,
    dependencies: &BTreeMap<String, BTreeSet<String>>,
    delivery: &BTreeMap<String, DeliveryTarget>,
    escalated: &BTreeSet<String>,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for predecessor in predecessors {
        if !is_task_id(predecessor) || !seen.insert(predecessor.clone()) {
            return Err(QueueInboxError::Invalid(format!(
                "task {id} has invalid or duplicate predecessor {predecessor:?}"
            )));
        }
        if predecessor == id || !known_ids.contains(predecessor) {
            return Err(QueueInboxError::Invalid(format!(
                "task {id} has missing or self predecessor {predecessor}"
            )));
        }
        if escalated.contains(predecessor) {
            return Err(QueueInboxError::Invalid(format!(
                "task {id} has infeasible escalated predecessor {predecessor}"
            )));
        }
        if delivery_target(body) == DeliveryTarget::Current
            && delivery.get(predecessor) == Some(&DeliveryTarget::NextMajor)
        {
            return Err(QueueInboxError::Invalid(format!(
                "current-lane task {id} cannot depend on next_major predecessor {predecessor}"
            )));
        }
    }
    let mut graph = dependencies.clone();
    graph.insert(id.to_owned(), seen);
    if graph_has_cycle(&graph) {
        return Err(QueueInboxError::Invalid(format!(
            "task {id} would create a dependency cycle"
        )));
    }
    Ok(())
}

fn graph_has_cycle(graph: &BTreeMap<String, BTreeSet<String>>) -> bool {
    fn visit(
        id: &str,
        graph: &BTreeMap<String, BTreeSet<String>>,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
    ) -> bool {
        if !visiting.insert(id.to_owned()) {
            return true;
        }
        if !visited.insert(id.to_owned()) {
            visiting.remove(id);
            return false;
        }
        let cycle = graph
            .get(id)
            .into_iter()
            .flatten()
            .filter(|next| graph.contains_key(*next))
            .any(|next| visit(next, graph, visiting, visited));
        visiting.remove(id);
        cycle
    }
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    graph
        .keys()
        .any(|id| visit(id, graph, &mut visiting, &mut visited))
}

fn completed_ids(work: &Path) -> Result<BTreeSet<String>> {
    match work_fs::read_optional_text(work, &work.join(DONE_FILE), MAX_CONTROL_BYTES)? {
        Some(text) => Ok(text
            .lines()
            .filter_map(archive_header_task_id)
            .map(str::to_owned)
            .collect()),
        None => Ok(BTreeSet::new()),
    }
}

fn active_task_ids(work: &Path) -> Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    for entry in plain_task_entries(work)? {
        let name = entry.file_name();
        let Some(id) = canonical_task_id(&name.to_string_lossy()) else {
            continue;
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_dir() || work_fs::redirected(&metadata) {
            return Err(QueueInboxError::Invalid(format!(
                "active task id path is not a plain directory: {}",
                entry.path().display()
            )));
        }
        ids.insert(id);
    }
    Ok(ids)
}

/// Preserve the spelling from the source that current Markdown readers will later consult.
/// Validation uses a numeric canonical form, but rendering `T-001` for an existing legacy
/// `T-1` would create a prerequisite that the read-only queue/archive parsers cannot satisfy.
/// Queue spelling has priority because it remains present while a task is active; archive then
/// active descriptor names fill the historical/legacy cases.
fn known_task_spellings(
    queued: &[QueueEntry],
    completed: &BTreeSet<String>,
    work: &Path,
) -> Result<BTreeMap<String, String>> {
    let mut spellings = BTreeMap::new();
    for entry in queued {
        if let Some(canonical) = canonical_task_id(&entry.id) {
            spellings
                .entry(canonical)
                .or_insert_with(|| entry.id.clone());
        }
    }
    for id in completed {
        if let Some(canonical) = canonical_task_id(id) {
            spellings.entry(canonical).or_insert_with(|| id.clone());
        }
    }
    for entry in plain_task_entries(work)? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(canonical) = canonical_task_id(&name) else {
            continue;
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_dir() || work_fs::redirected(&metadata) {
            return Err(QueueInboxError::Invalid(format!(
                "active task id path is not a plain directory: {}",
                entry.path().display()
            )));
        }
        spellings.entry(canonical).or_insert(name);
    }
    Ok(spellings)
}

fn known_task_titles(
    work_text: &str,
    queued: &[QueueEntry],
    work: &Path,
) -> Result<BTreeSet<String>> {
    let mut titles = queued
        .iter()
        .map(|entry| normalize_title(&entry.title))
        .collect::<BTreeSet<_>>();
    for source in [work.join(DONE_FILE)] {
        if let Some(text) = work_fs::read_optional_text(work, &source, MAX_CONTROL_BYTES)? {
            titles.extend(archive_titles(&text));
            titles.extend(original_task_titles(&text));
        }
    }
    titles.extend(active_task_titles(work)?);
    titles.extend(archive_titles(work_text));
    Ok(titles)
}

fn archive_titles(text: &str) -> impl Iterator<Item = String> + '_ {
    text.lines().filter_map(|line| {
        let rest = line.strip_prefix("### [T-")?;
        let close = rest.find(']')?;
        let title = rest[close + 1..].split("— статус:").next()?.trim();
        (!title.is_empty()).then(|| normalize_title(title))
    })
}

fn original_task_titles(text: &str) -> impl Iterator<Item = String> + '_ {
    text.lines().filter_map(|line| {
        let rest = line.trim_start().strip_prefix("Исходная задача:")?.trim();
        let rest = rest.strip_prefix("[T-")?;
        let close = rest.find(']')?;
        canonical_task_id(&format!("T-{}", &rest[..close]))?;
        let title = rest[close + 1..].trim();
        (!title.is_empty()).then(|| normalize_title(title))
    })
}

fn active_task_titles(work: &Path) -> Result<BTreeSet<String>> {
    let mut titles = BTreeSet::new();
    for entry in plain_task_entries(work)? {
        if canonical_task_id(&entry.file_name().to_string_lossy()).is_none() {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_dir() || work_fs::redirected(&metadata) {
            return Err(QueueInboxError::Invalid(format!(
                "active task id path is not a plain directory: {}",
                entry.path().display()
            )));
        }
        if let Some(text) =
            work_fs::read_optional_text(work, &entry.path().join("task.md"), MAX_CONTROL_BYTES)?
        {
            titles.extend(original_task_titles(&text));
        }
    }
    Ok(titles)
}

fn known_proposal_titles(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("### [P-")?;
            let close = rest.find(']')?;
            let title = rest[close + 1..].split("— kind: proposal").next()?.trim();
            (!title.is_empty()).then(|| normalize_title(title))
        })
        .collect()
}

fn max_known_task_number(work: &Path, queued: &[QueueEntry]) -> Result<u32> {
    let mut max = queued
        .iter()
        .filter_map(|entry| task_number(&entry.id))
        .max()
        .unwrap_or_default();
    for text in queue_history_texts(work)? {
        max = max.max(task_numbers_in(&text).into_iter().max().unwrap_or_default());
    }
    Ok(max)
}

fn max_known_proposal_number(work: &Path, queue_text: &str) -> Result<u32> {
    let mut max = proposal_numbers_in(queue_text)
        .into_iter()
        .max()
        .unwrap_or_default();
    if let Some(text) = work_fs::read_optional_text(work, &work.join(DONE_FILE), MAX_CONTROL_BYTES)?
    {
        max = max.max(
            proposal_numbers_in(&text)
                .into_iter()
                .max()
                .unwrap_or_default(),
        );
    }
    Ok(max)
}

fn queue_history_texts(work: &Path) -> Result<Vec<String>> {
    let mut texts = Vec::new();
    if let Some(text) = work_fs::read_optional_text(work, &work.join(DONE_FILE), MAX_CONTROL_BYTES)?
    {
        texts.push(text);
    }
    for entry in plain_task_entries(work)? {
        let name = entry.file_name();
        let Some(number) = task_number(&name.to_string_lossy()) else {
            continue;
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_dir() || work_fs::redirected(&metadata) {
            return Err(QueueInboxError::Invalid(format!(
                "active task id path is not a plain directory: {}",
                entry.path().display()
            )));
        }
        texts.push(format!("T-{number}"));
        if let Some(text) =
            work_fs::read_optional_text(work, &entry.path().join("task.md"), MAX_CONTROL_BYTES)?
        {
            texts.push(text);
        }
    }
    Ok(texts)
}

fn plain_task_entries(work: &Path) -> Result<Vec<fs::DirEntry>> {
    Ok(work_fs::plain_directory_entries(work, &work.join("tasks"))?.unwrap_or_default())
}

fn task_numbers_in(text: &str) -> BTreeSet<u32> {
    identifier_numbers_in(text, "T-")
}

fn proposal_numbers_in(text: &str) -> BTreeSet<u32> {
    identifier_numbers_in(text, "P-")
}

fn identifier_numbers_in(text: &str, prefix: &str) -> BTreeSet<u32> {
    text.match_indices(prefix)
        .filter_map(|(offset, _)| {
            let digits = text[offset + prefix.len()..]
                .bytes()
                .take_while(u8::is_ascii_digit)
                .collect::<Vec<_>>();
            (!digits.is_empty())
                .then(|| std::str::from_utf8(&digits).ok()?.parse::<u32>().ok())
                .flatten()
        })
        .collect()
}

fn delivery_target(body: &str) -> DeliveryTarget {
    body.lines()
        .find_map(|line| line.strip_prefix("Delivery target:"))
        .map(DeliveryTarget::from_field)
        .unwrap_or_default()
}

fn render_task(id: &str, title: &str, body: &str, predecessors: &[String]) -> String {
    let mut lines = vec![format!("### [{id}] {title} — статус: не начата")];
    lines.extend(body.lines().map(str::to_owned));
    if !predecessors.is_empty() {
        while lines.last().is_some_and(|line| line.trim().is_empty()) {
            lines.pop();
        }
        lines.push(format!("Предпосылки: {}", predecessors.join(", ")));
    }
    lines.join("\n")
}

fn render_proposal(
    id: &str,
    title: &str,
    body: &str,
    source: Option<&str>,
    suggested_target: Option<&str>,
) -> String {
    let mut lines = vec![format!(
        "### [{id}] {title} — kind: proposal — status: proposed"
    )];
    lines.extend(body.lines().map(str::to_owned));
    if source.is_some() || suggested_target.is_some() {
        while lines.last().is_some_and(|line| line.trim().is_empty()) {
            lines.pop();
        }
    }
    if let Some(suggested_target) = suggested_target {
        lines.push(format!("Suggested target: {suggested_target}"));
    }
    if let Some(source) = source {
        lines.push(format!("Source: {source}"));
    }
    lines.join("\n")
}

fn append_blocks(original: &str, blocks: &[String]) -> String {
    let mut output = if original.is_empty() {
        "# Очередь задач\n\n".to_owned()
    } else {
        original.to_owned()
    };
    if !output.is_empty() && !output.ends_with('\n') {
        output.push('\n');
    }
    for block in blocks {
        if !output.is_empty() && !output.ends_with("\n\n") {
            output.push('\n');
        }
        output.push_str(block);
        output.push('\n');
    }
    output
}

fn move_to_quarantine(
    work: &Path,
    inbox: &Path,
    source: &Path,
    name: &str,
    occurred_at: &str,
    reason: &str,
) -> Result<()> {
    let source_metadata = fs::symlink_metadata(source)?;
    if !source_metadata.is_file() || work_fs::redirected(&source_metadata) {
        return Err(QueueInboxError::Invalid(format!(
            "queue inbox quarantine source is not a plain file: {}",
            source.display()
        )));
    }
    let rejected = inbox.join(REJECTED_DIRECTORY);
    match fs::create_dir(&rejected) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    if !rejected.is_dir() {
        return Err(QueueInboxError::Invalid(format!(
            "queue inbox rejected path is not a directory: {}",
            rejected.display()
        )));
    }
    let rejected_metadata = fs::symlink_metadata(&rejected)?;
    if work_fs::redirected(&rejected_metadata) {
        return Err(QueueInboxError::Invalid(format!(
            "queue inbox rejected path is redirected: {}",
            rejected.display()
        )));
    }
    let stamp = occurred_at
        .bytes()
        .filter(|byte| byte.is_ascii_alphanumeric())
        .map(char::from)
        .collect::<String>();
    let original = name.strip_suffix(".json").unwrap_or(name);
    let mut suffix = 0_u32;
    loop {
        let tail = if suffix == 0 {
            String::new()
        } else {
            format!("-{suffix}")
        };
        let base = format!("{stamp}-{original}{tail}");
        let record = rejected.join(format!("{base}.json"));
        let metadata = rejected.join(format!("{base}.metadata.txt"));
        if work_fs::entry_exists(work, &record).map_err(QueueInboxError::Io)?
            || work_fs::entry_exists(work, &metadata).map_err(QueueInboxError::Io)?
        {
            suffix = suffix.saturating_add(1);
            continue;
        }
        let text = format!("Rejection reason: {reason}\nTimestamp of rejection: {occurred_at}\n");
        work_fs::replace_file(work, &metadata, text.as_bytes(), MAX_RECORD_BYTES)
            .map_err(QueueInboxError::Io)?;
        fs::rename(source, &record)?;
        let record_metadata = fs::symlink_metadata(&record)?;
        if !record_metadata.is_file() || work_fs::redirected(&record_metadata) {
            return Err(QueueInboxError::Invalid(format!(
                "queue inbox quarantined record is not a plain file: {}",
                record.display()
            )));
        }
        work_fs::require_plain_directory(&rejected)?;
        return Ok(());
    }
}

fn normalize_title(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn task_number(id: &str) -> Option<u32> {
    if !is_task_id(id) {
        return None;
    }
    let digits = id.strip_prefix("T-")?;
    digits.parse().ok()
}

fn canonical_task_id(id: &str) -> Option<String> {
    task_number(id).map(|number| format!("T-{number:03}"))
}

#[cfg(test)]
mod tests {
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
                "orchestrail-queue-inbox-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn write(&self, relative: &str, value: Value) {
            let path = self.path.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, value.to_string()).unwrap();
        }

        fn text(&self, relative: &str, value: &str) {
            let path = self.path.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, value).unwrap();
        }
    }

    impl Drop for Work {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn task(title: &str, body: &str, predecessors: &[&str]) -> Value {
        serde_json::json!({
            "kind": "task",
            "title": title,
            "body": body,
            "predecessors": predecessors,
        })
    }

    #[test]
    fn missing_queue_inbox_is_a_no_op() {
        let work = Work::new();
        assert_eq!(
            drain(&work.path, "2026-07-25T12:00:00Z").unwrap(),
            DrainResult::default()
        );
    }

    #[test]
    fn drains_task_and_proposal_in_stable_order() {
        let work = Work::new();
        work.text(
            QUEUE_FILE,
            "### [T-007] Existing — статус: не начата\n\n### [P-002] Existing proposal — kind: proposal — status: proposed\n",
        );
        work.text(QUEUE_STATE_FILE, r#"{"generation": 7}"#);
        work.write(
            "queue_inbox/a-task.json",
            task("Imported task", "Inbox message: msg-00000001", &["T-007"]),
        );
        work.write(
            "queue_inbox/b-proposal.json",
            serde_json::json!({
                "kind": "proposal",
                "title": "Imported proposal",
                "body": "Need a decision",
                "source": "inbox",
                "suggested_target": "engine",
            }),
        );
        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.added_tasks, ["T-008"]);
        assert_eq!(result.added_proposals, ["P-003"]);
        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.contains("### [T-008] Imported task — статус: не начата"));
        assert!(queue.contains("Предпосылки: T-007"));
        assert!(
            queue.contains("### [P-003] Imported proposal — kind: proposal — status: proposed")
        );
        assert_eq!(
            serde_json::from_str::<Value>(
                &fs::read_to_string(work.path.join(QUEUE_STATE_FILE)).unwrap()
            )
            .unwrap()["generation"],
            9,
            "each task or proposal insertion advances the queue generation"
        );
        assert!(!work.path.join("queue_inbox/a-task.json").exists());
        assert!(!work.path.join("queue_inbox/b-proposal.json").exists());
    }

    #[test]
    fn retry_after_queue_commit_deduplicates_and_consumes_the_source_record() {
        let work = Work::new();
        work.text(QUEUE_FILE, "### [T-001] Same title — статус: не начата\n");
        work.write("queue_inbox/retry.json", task("Same   title", "", &[]));
        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.skipped_duplicates, ["retry.json"]);
        assert!(!work.path.join("queue_inbox/retry.json").exists());
        assert_eq!(
            crate::state::queue::parse_queue(
                &fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap()
            )
            .len(),
            1
        );
    }

    #[test]
    fn recovers_queue_written_before_generation_and_consumes_only_the_original_record() {
        let work = Work::new();
        let original = "### [T-001] Existing — статус: не начата\n";
        let state_before = "{\"generation\": 7}\n";
        let source = work.path.join("queue_inbox/recover.json");
        work.text(QUEUE_FILE, original);
        work.text(QUEUE_STATE_FILE, state_before);
        work.write("queue_inbox/recover.json", task("Recovered", "", &[]));

        let inserted = render_task("T-002", "Recovered", "", &[]);
        let updated = append_blocks(original, &[inserted]);
        let state_after = "{\"generation\":8}\n";
        let transaction = DrainTransaction {
            queue_before_hash: content_hash(original.as_bytes()),
            queue_after_hash: content_hash(updated.as_bytes()),
            queue_state_before_hash: Some(content_hash(state_before.as_bytes())),
            queue_state_after: state_after.into(),
            consumed: BTreeMap::from([(
                "recover.json".into(),
                content_hash(&fs::read(&source).unwrap()),
            )]),
        };
        create_transaction_marker(&work.path, &work.path.join(INBOX_DIRECTORY), &transaction)
            .unwrap();
        // This is the exact old crash window: the queue rename survived, but the generation
        // rename and source consumption did not.
        work.text(QUEUE_FILE, &updated);

        assert_eq!(
            drain(&work.path, "2026-07-25T12:00:00Z").unwrap(),
            DrainResult::default(),
            "recovery completes the pending transaction before admitting another record"
        );
        assert_eq!(
            fs::read_to_string(work.path.join(QUEUE_STATE_FILE)).unwrap(),
            state_after
        );
        assert!(!source.exists());
        assert!(
            !work
                .path
                .join(INBOX_DIRECTORY)
                .join(DRAIN_TRANSACTION_FILE)
                .exists()
        );
        assert!(
            fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("[T-002] Recovered")
        );
    }

    #[test]
    fn oversized_transaction_marker_fails_before_reading_or_consuming_proposals() {
        let work = Work::new();
        work.write("queue_inbox/proposal.json", task("Must remain", "", &[]));
        fs::write(
            work.path.join(INBOX_DIRECTORY).join(DRAIN_TRANSACTION_FILE),
            vec![b' '; MAX_TRANSACTION_BYTES as usize + 1],
        )
        .unwrap();

        assert!(drain(&work.path, "2026-07-25T12:00:00Z").is_err());
        assert!(work.path.join("queue_inbox/proposal.json").is_file());
        assert!(!work.path.join(QUEUE_FILE).exists());
    }

    #[test]
    fn bad_dependency_is_quarantined_but_a_later_valid_record_is_drained() {
        let work = Work::new();
        work.write("queue_inbox/a-bad.json", task("Bad", "", &["T-999"]));
        work.write("queue_inbox/b-good.json", task("Good", "", &[]));
        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.added_tasks, ["T-001"]);
        assert_eq!(result.quarantined, ["a-bad.json"]);
        assert!(work.path.join("queue_inbox/rejected").is_dir());
        assert!(!work.path.join("queue_inbox/a-bad.json").exists());
        assert!(
            fs::read_to_string(work.path.join(QUEUE_FILE))
                .unwrap()
                .contains("[T-001] Good")
        );
    }

    #[test]
    fn quarantine_does_not_replace_a_dangling_destination_entry() {
        let work = Work::new();
        let inbox = work.path.join(INBOX_DIRECTORY);
        let rejected = inbox.join(REJECTED_DIRECTORY);
        fs::create_dir_all(&rejected).unwrap();
        let source = inbox.join("bad.json");
        fs::write(&source, "invalid\n").unwrap();
        let occupied = rejected.join("20260725T120000Z-bad.json");
        let missing = rejected.join("missing-target.json");
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&missing, &occupied).is_ok();
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&missing, &occupied).is_ok();
        if !linked {
            return;
        }

        move_to_quarantine(
            &work.path,
            &inbox,
            &source,
            "bad.json",
            "2026-07-25T12:00:00Z",
            "invalid fixture",
        )
        .unwrap();

        assert!(
            fs::symlink_metadata(&occupied)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read_to_string(rejected.join("20260725T120000Z-bad-1.json")).unwrap(),
            "invalid\n"
        );
        assert!(
            rejected
                .join("20260725T120000Z-bad-1.metadata.txt")
                .is_file()
        );
    }

    #[test]
    fn body_cannot_inject_a_second_queue_header() {
        let work = Work::new();
        work.write(
            "queue_inbox/injection.json",
            task(
                "Safe title",
                "body\n### [T-999] injected — статус: не начата",
                &[],
            ),
        );
        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.quarantined, ["injection.json"]);
        assert!(!work.path.join(QUEUE_FILE).exists());
    }

    #[test]
    fn body_cannot_override_json_predecessors() {
        let work = Work::new();
        work.write(
            "queue_inbox/override.json",
            task("Safe title", "Предпосылки: T-999", &[]),
        );
        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.quarantined, ["override.json"]);
        assert!(!work.path.join(QUEUE_FILE).exists());
    }

    #[test]
    fn title_cannot_change_the_queue_header_semantics() {
        let work = Work::new();
        work.write(
            "queue_inbox/header.json",
            task("Looks safe — статус: эскалирована", "", &[]),
        );
        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.quarantined, ["header.json"]);
        assert!(!work.path.join(QUEUE_FILE).exists());
    }

    #[test]
    fn oversized_or_uppercase_extension_record_is_processed_without_memory_bypass() {
        let work = Work::new();
        work.write(
            "queue_inbox/valid.JSON",
            task("Uppercase extension", "", &[]),
        );
        let oversized = work.path.join("queue_inbox/oversized.json");
        fs::create_dir_all(oversized.parent().unwrap()).unwrap();
        fs::write(&oversized, vec![b' '; MAX_RECORD_BYTES as usize + 1]).unwrap();

        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.added_tasks, ["T-001"]);
        assert_eq!(result.quarantined, ["oversized.json"]);
        assert!(!oversized.exists());
    }

    #[test]
    fn a_plain_json_named_directory_is_not_an_inbox_record() {
        let work = Work::new();
        fs::create_dir_all(work.path.join("queue_inbox/ignored.json")).unwrap();
        work.write("queue_inbox/valid.json", task("Only file", "", &[]));

        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.added_tasks, ["T-001"]);
        assert!(work.path.join("queue_inbox/ignored.json").is_dir());
    }

    #[test]
    fn invalid_queue_generation_fails_before_mutating_or_consuming_the_record() {
        let work = Work::new();
        work.text(QUEUE_STATE_FILE, "not-json");
        work.write("queue_inbox/valid.json", task("Must not persist", "", &[]));

        assert!(drain(&work.path, "2026-07-25T12:00:00Z").is_err());
        assert!(!work.path.join(QUEUE_FILE).exists());
        assert!(work.path.join("queue_inbox/valid.json").is_file());
    }

    #[test]
    fn non_file_queue_control_plane_path_fails_before_consuming_the_record() {
        let work = Work::new();
        fs::create_dir_all(work.path.join(QUEUE_FILE)).unwrap();
        work.write("queue_inbox/valid.json", task("Must remain", "", &[]));

        assert!(drain(&work.path, "2026-07-25T12:00:00Z").is_err());
        assert!(work.path.join("queue_inbox/valid.json").is_file());
    }

    #[test]
    fn deduplicates_against_an_active_descriptor_and_never_reuses_its_mentions() {
        let work = Work::new();
        work.text(
            "tasks/T-001/task.md",
            "# Active task T-001\nИсходная задача: [T-001] Already underway\nRelated: T-777\n",
        );
        work.write(
            "queue_inbox/a-duplicate.json",
            task("Already   underway", "", &[]),
        );
        work.write("queue_inbox/b-new.json", task("New work", "", &[]));

        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.skipped_duplicates, ["a-duplicate.json"]);
        assert_eq!(result.added_tasks, ["T-778"]);
        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.starts_with("# Очередь задач\n\n"));
        assert!(queue.contains("### [T-778] New work — статус: не начата"));
    }

    #[test]
    fn canonicalizes_predecessors_and_rejects_escalated_or_next_major_edges() {
        let work = Work::new();
        work.text(
            QUEUE_FILE,
            "### [T-001] Escalated — статус: эскалирована\n\n\
### [T-002] Breaking — статус: не начата\nDelivery target: next_major\n",
        );
        work.write(
            "queue_inbox/a-escalated.json",
            task("Blocked", "", &["T-1"]),
        );
        work.write(
            "queue_inbox/b-lane.json",
            task("Wrong lane", "", &["T-002"]),
        );
        work.write(
            "queue_inbox/c-valid.json",
            task("Parked dependent", "Delivery target: next_major", &["T-2"]),
        );

        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.quarantined, ["a-escalated.json", "b-lane.json"]);
        assert_eq!(result.added_tasks, ["T-003"]);
        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.contains("Предпосылки: T-002"));
    }

    #[test]
    fn preserves_a_legacy_predecessor_spelling_for_later_markdown_readiness() {
        let work = Work::new();
        work.text(
            QUEUE_FILE,
            "### [T-1] Legacy spelling — статус: не начата\n",
        );
        work.write(
            "queue_inbox/dependent.json",
            task("Depends on legacy", "", &["T-001"]),
        );

        let result = drain(&work.path, "2026-07-25T12:00:00Z").unwrap();
        assert_eq!(result.added_tasks, ["T-002"]);
        let queue = fs::read_to_string(work.path.join(QUEUE_FILE)).unwrap();
        assert!(queue.contains("Предпосылки: T-1"));
        assert!(!queue.contains("Предпосылки: T-001"));
    }
}
