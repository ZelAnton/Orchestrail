//! Deterministic, fail-closed projection of an optional Orchestra cross-project inbox.
//!
//! The inbox is external input, not a task assignment mechanism.  This module performs only the
//! mechanical half of the published inbox contract: validate local message records, recover
//! task provenance from the local control plane, and calculate which records need a curator.
//! It never interprets a message body, creates work, or routes a reply to another repository.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dependency_graph::{self, DependencyGraphError, RegisteredProject};
use crate::state::archive_header_task_id;
use crate::task_id::is_task_id;
use crate::work_fs::{self, MAX_CONTROL_BYTES};

const INBOX_DIRECTORY: &str = ".inbox";
const MESSAGES_DIRECTORY: &str = "messages";
const LOCK_FILE: &str = "inbox.lock";
const MESSAGE_SCHEMA: &str = "orchestra/inbox-message@1";
const FINAL_REPLY_CANDIDATE_SCHEMA: &str = "orchestrail/inbox-final-reply@1";
const FINAL_REPLY_DEDUPE_KEY: &str = "final-v1";
// Orchestra's PowerShell contract uses `.Length`, i.e. UTF-16 code units, for endpoint names
// and subjects.  Bodies alone are bounded in UTF-8 bytes.
const MAX_SUBJECT_UTF16_UNITS: usize = 240;
const MAX_BODY_BYTES: usize = 262_144;
const MAX_PROJECT_NAME_UTF16_UNITS: usize = 120;
/// The body itself is bounded to 256 KiB, but escaped Unicode and bounded metadata make a
/// serialized message larger.  Cap the raw untrusted record before allocating/parsing it.
const MAX_MESSAGE_RECORD_BYTES: u64 = 4_194_304;
const MAX_MESSAGES: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InboxMessage {
    pub id: String,
    pub message_type: MessageType,
    pub processing_status: ProcessingStatus,
    pub reply_status: ReplyStatus,
    pub in_reply_to: Option<String>,
    pub queue_tasks: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MessageType {
    Request,
    Reply,
    Release,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProcessingStatus {
    New,
    Read,
    Queued,
    Implemented,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplyStatus {
    None,
    Acknowledged,
    Final,
}

/// The current inbox payload and the direct `.work` provenance graph.  The optional inbox is
/// represented explicitly, so callers do not conflate an unconfigured legacy project with an
/// initialized but empty inbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum InboxProjection {
    Absent,
    Present { messages: Vec<InboxMessage> },
}

/// The lists use the message's stable id and are sorted by that id for reproducible dispatch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Actionable {
    pub new: Vec<String>,
    pub unresolved: Vec<String>,
    pub completable: Vec<String>,
    pub reply_pending: Vec<String>,
}

impl Actionable {
    pub fn count(&self) -> usize {
        self.new.len() + self.unresolved.len() + self.completable.len() + self.reply_pending.len()
    }

    pub fn needs_initial_curation(&self) -> bool {
        !self.new.is_empty() || !self.unresolved.is_empty()
    }

    pub fn needs_finalization(&self) -> bool {
        !self.completable.is_empty() || !self.reply_pending.is_empty()
    }
}

/// Result of an idempotent provenance reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum ReconcileResult {
    Absent,
    Reconciled { updated: Vec<String> },
}

#[derive(Debug)]
pub enum InboxError {
    Io(io::Error),
    Registry(DependencyGraphError),
    Malformed(String),
    Busy,
}

impl fmt::Display for InboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "inbox I/O error: {error}"),
            Self::Registry(error) => write!(f, "inbox registry error: {error}"),
            Self::Malformed(message) => write!(f, "invalid inbox: {message}"),
            Self::Busy => f.write_str("inbox is held by another writer"),
        }
    }
}

impl std::error::Error for InboxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Registry(error) => Some(error),
            Self::Malformed(_) | Self::Busy => None,
        }
    }
}

impl From<io::Error> for InboxError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<DependencyGraphError> for InboxError {
    fn from(error: DependencyGraphError) -> Self {
        Self::Registry(error)
    }
}

pub type Result<T> = std::result::Result<T, InboxError>;

/// Read and validate all direct message files. A missing `.inbox` is a legacy-project no-op;
/// a partially initialized, redirected, unreadable, or malformed inbox fails closed.
pub fn inspect(root: &Path) -> Result<InboxProjection> {
    let Some(paths) = paths(root)? else {
        return Ok(InboxProjection::Absent);
    };
    let _ = work_directory(root)?;
    let messages = load_messages(&paths)?
        .into_iter()
        .map(|record| record.message)
        .collect();
    Ok(InboxProjection::Present { messages })
}

/// Classify messages using exact queue/archive provenance and the same archive-header predicate
/// used by scheduler readiness.  No message text participates in the decision.
pub fn actionable(root: &Path) -> Result<Actionable> {
    let Some(paths) = paths(root)? else {
        return Ok(Actionable::default());
    };
    let messages = load_messages(&paths)?;
    let done = completed_ids(root)?;
    let mut result = Actionable::default();
    for record in messages {
        let message = record.message;
        match message.processing_status {
            ProcessingStatus::Implemented | ProcessingStatus::Rejected
                if message.reply_status != ReplyStatus::Final =>
            {
                result.reply_pending.push(message.id);
            }
            ProcessingStatus::New => result.new.push(message.id),
            ProcessingStatus::Read
                if message.reply_status == ReplyStatus::None && message.in_reply_to.is_none() =>
            {
                result.unresolved.push(message.id);
            }
            ProcessingStatus::Queued
                if !message.queue_tasks.is_empty()
                    && message.queue_tasks.iter().all(|task| done.contains(task)) =>
            {
                result.completable.push(message.id);
            }
            _ => {}
        }
    }
    Ok(result)
}

/// Recover message-to-task links after a queue inbox drain or a crash between the drain and the
/// message update.  The caller provides an already-validated UTC timestamp so retrying the same
/// durable effect can be made observable but does not invent an ambient clock dependency.
pub fn reconcile(root: &Path, occurred_at: &str) -> Result<ReconcileResult> {
    if !looks_like_utc(occurred_at) {
        return Err(InboxError::Malformed(format!(
            "reconciliation timestamp must be a nonempty UTC Z instant: {occurred_at:?}"
        )));
    }
    let Some(paths) = paths(root)? else {
        return Ok(ReconcileResult::Absent);
    };
    let _lock = InboxLock::acquire(&paths.lock)?;
    let links = task_links(root)?;
    let mut updated = Vec::new();
    for mut record in load_messages(&paths)? {
        let Some(linked) = links.get(&record.message.id) else {
            continue;
        };
        if matches!(
            record.message.processing_status,
            ProcessingStatus::Implemented | ProcessingStatus::Rejected
        ) {
            continue;
        }
        let mut merged = record.message.queue_tasks.clone();
        merged.extend(linked.iter().cloned());
        let became_queued =
            record.message.processing_status == ProcessingStatus::Read && !merged.is_empty();
        if merged == record.message.queue_tasks && !became_queued {
            continue;
        }
        set_task_links(&mut record.value, &merged)?;
        if became_queued {
            set_string(&mut record.value, "processing_status", "queued")?;
            append_remark(
                &mut record.value,
                occurred_at,
                "inbox-reconcile",
                &format!(
                    "Linked to queue task(s): {}",
                    merged.iter().cloned().collect::<Vec<_>>().join(", ")
                ),
            )?;
        }
        set_string(&mut record.value, "updated_at", occurred_at)?;
        write_message(&record.path, &record.value)?;
        updated.push(record.message.id);
    }
    Ok(ReconcileResult::Reconciled { updated })
}

/// Deliver every locally prepared `final-v1` reply that remains pending after the inbox
/// finalizer has made its local status decision.  The model may author a reply *candidate* below
/// `.work`, but it never receives the registered sender's root or authority to write there:
/// this native boundary resolves both endpoints from the shared registry, validates the candidate
/// and performs the two idempotent inbox updates in the legacy-compatible order.
///
/// A crash after remote delivery but before the local reply-status update is safe.  A retry uses
/// the deterministic reply id and requires the already delivered content to be identical before
/// it records the source-side final marker.
pub fn deliver_final_replies(
    root: &Path,
    work: &Path,
    registry_path: &Path,
    occurred_at: &str,
) -> Result<Vec<String>> {
    if !looks_like_utc(occurred_at) {
        return Err(InboxError::Malformed(format!(
            "final reply timestamp must be a nonempty UTC Z instant: {occurred_at:?}"
        )));
    }
    let actionable = actionable(root)?;
    if actionable.reply_pending.is_empty() {
        return Ok(Vec::new());
    }
    let current = dependency_graph::registered_project_for_root(registry_path, root)?;
    let mut delivered = Vec::with_capacity(actionable.reply_pending.len());
    for message_id in actionable.reply_pending {
        deliver_final_reply(
            root,
            work,
            registry_path,
            &current,
            &message_id,
            occurred_at,
        )?;
        delivered.push(message_id);
    }
    Ok(delivered)
}

fn deliver_final_reply(
    root: &Path,
    work: &Path,
    registry_path: &Path,
    current: &RegisteredProject,
    message_id: &str,
    occurred_at: &str,
) -> Result<()> {
    let source_paths = paths(root)?.ok_or_else(|| {
        InboxError::Malformed(
            "cannot deliver a final reply because the source inbox is absent".into(),
        )
    })?;
    let source_path = message_path(&source_paths, message_id)?;
    let original = load_message(source_path)?;
    if !matches!(
        original.message.processing_status,
        ProcessingStatus::Implemented | ProcessingStatus::Rejected
    ) || original.message.reply_status == ReplyStatus::Final
    {
        return Err(InboxError::Malformed(format!(
            "message {message_id} is not a pending terminal conversation"
        )));
    }
    let original_to = endpoint_id(&original.value, "to_project", message_id)?;
    if original_to != current.id {
        return Err(InboxError::Malformed(format!(
            "message {message_id} names {original_to} as recipient, not registered current project {}",
            current.id
        )));
    }
    let sender_id = endpoint_id(&original.value, "from_project", message_id)?;
    let sender = dependency_graph::registered_project_by_id(registry_path, sender_id)?;
    if sender.id == current.id {
        return Err(InboxError::Malformed(format!(
            "message {message_id} cannot route a reply to the current project"
        )));
    }
    let candidate = read_final_reply_candidate(work, message_id)?;
    let reply_id = stable_reply_id(message_id, &current.id, FINAL_REPLY_DEDUPE_KEY);
    let expected = new_final_reply(
        &reply_id,
        message_id,
        &original.value,
        current,
        &sender,
        &candidate.body,
        occurred_at,
    )?;

    let target_paths = paths(&sender.root)?.ok_or_else(|| {
        InboxError::Malformed(format!(
            "registered sender {} has no initialized inbox: {}",
            sender.id,
            sender.root.display()
        ))
    })?;
    let target_path = message_path(&target_paths, &reply_id)?;
    let _target_lock = InboxLock::acquire(&target_paths.lock)?;
    match fs::symlink_metadata(&target_path) {
        Ok(metadata) => {
            assert_plain_file(&target_path, &metadata)?;
            let existing = load_message(target_path.clone())?;
            assert_identical_reply(&existing, &expected, &reply_id)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            write_message(&target_path, &expected)?;
        }
        Err(error) => return Err(error.into()),
    }
    drop(_target_lock);

    let _source_lock = InboxLock::acquire(&source_paths.lock)?;
    let mut current_source = load_message(message_path(&source_paths, message_id)?)?;
    if !matches!(
        current_source.message.processing_status,
        ProcessingStatus::Implemented | ProcessingStatus::Rejected
    ) {
        return Err(InboxError::Malformed(format!(
            "message {message_id} stopped being terminal before final reply recording"
        )));
    }
    let current_to = endpoint_id(&current_source.value, "to_project", message_id)?;
    if current_to != current.id {
        return Err(InboxError::Malformed(format!(
            "message {message_id} recipient changed before final reply recording"
        )));
    }
    let reply_ids = reply_ids_mut(&mut current_source.value, message_id)?;
    let already_recorded = reply_ids.iter().any(|value| value == &reply_id);
    if already_recorded && current_source.message.reply_status != ReplyStatus::Final {
        return Err(InboxError::Malformed(format!(
            "message {message_id} already used final-v1 reply id without a final reply status"
        )));
    }
    if !already_recorded {
        reply_ids.push(Value::String(reply_id.clone()));
        reply_ids.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        append_remark(
            &mut current_source.value,
            occurred_at,
            "orchestrail-inbox-finalizer",
            &format!("Reply sent: {reply_id} (final)"),
        )?;
    }
    set_string(&mut current_source.value, "reply_status", "final")?;
    set_string(&mut current_source.value, "updated_at", occurred_at)?;
    write_message(&current_source.path, &current_source.value)?;
    let candidate_path = final_reply_candidate_path(work, message_id)?;
    if let Ok(metadata) = fs::symlink_metadata(&candidate_path) {
        assert_plain_file(&candidate_path, &metadata)?;
        work_fs::remove_plain_file(work, &candidate_path)?;
    }
    Ok(())
}

#[derive(Debug)]
struct FinalReplyCandidate {
    body: String,
}

fn read_final_reply_candidate(work: &Path, message_id: &str) -> Result<FinalReplyCandidate> {
    let path = final_reply_candidate_path(work, message_id)?;
    let metadata = fs::symlink_metadata(&path).map_err(|error| {
        InboxError::Malformed(format!(
            "missing final-v1 reply candidate for {message_id}: {} ({error})",
            path.display()
        ))
    })?;
    assert_plain_file(&path, &metadata)?;
    if metadata.len() > MAX_MESSAGE_RECORD_BYTES {
        return Err(InboxError::Malformed(format!(
            "final-v1 reply candidate exceeds the {MAX_MESSAGE_RECORD_BYTES}-byte limit: {}",
            path.display()
        )));
    }
    let text = read_inbox_text(&path, "final-v1 reply candidate", MAX_MESSAGE_RECORD_BYTES)?;
    let raw: Value = serde_json::from_str(&text).map_err(|error| {
        InboxError::Malformed(format!(
            "final-v1 reply candidate is not valid JSON {}: {error}",
            path.display()
        ))
    })?;
    let object = raw.as_object().ok_or_else(|| {
        InboxError::Malformed(format!(
            "final-v1 reply candidate {message_id} is not an object"
        ))
    })?;
    if required_string(object, "schema", message_id)? != FINAL_REPLY_CANDIDATE_SCHEMA {
        return Err(InboxError::Malformed(format!(
            "final-v1 reply candidate {message_id} has an unsupported schema"
        )));
    }
    if required_string(object, "message_id", message_id)? != message_id {
        return Err(InboxError::Malformed(format!(
            "final-v1 reply candidate {message_id} targets another message"
        )));
    }
    let body = required_string(object, "body", message_id)?.to_owned();
    if body.trim().is_empty() || body.len() > MAX_BODY_BYTES {
        return Err(InboxError::Malformed(format!(
            "final-v1 reply candidate {message_id} has an empty or oversized body"
        )));
    }
    Ok(FinalReplyCandidate { body })
}

fn final_reply_candidate_path(work: &Path, message_id: &str) -> Result<PathBuf> {
    if !valid_message_id(message_id) {
        return Err(InboxError::Malformed(format!(
            "invalid final-v1 candidate message id {message_id:?}"
        )));
    }
    let metadata = fs::symlink_metadata(work).map_err(|error| {
        InboxError::Malformed(format!(
            "final-v1 reply work directory is missing or unreadable {}: {error}",
            work.display()
        ))
    })?;
    assert_plain_directory(work, &metadata)?;
    let directory = work.join("inbox_reply_candidates");
    let metadata = fs::symlink_metadata(&directory).map_err(|error| {
        InboxError::Malformed(format!(
            "final-v1 reply candidate directory is missing or unreadable {}: {error}",
            directory.display()
        ))
    })?;
    assert_plain_directory(&directory, &metadata)?;
    Ok(directory.join(format!("{message_id}-final-v1.json")))
}

fn new_final_reply(
    reply_id: &str,
    original_id: &str,
    original: &Value,
    current: &RegisteredProject,
    sender: &RegisteredProject,
    body: &str,
    occurred_at: &str,
) -> Result<Value> {
    let original_subject = required_string(
        original.as_object().ok_or_else(|| {
            InboxError::Malformed(format!("message {original_id} is not an object"))
        })?,
        "subject",
        original_id,
    )?;
    let conversation_id = optional_string(
        original.as_object().expect("checked above"),
        "conversation_id",
        original_id,
    )?
    .filter(|value| !value.is_empty())
    .unwrap_or_else(|| original_id.to_owned());
    let subject = truncate_utf16(&format!("Re: {original_subject}"), MAX_SUBJECT_UTF16_UNITS);
    if subject.trim().is_empty() {
        return Err(InboxError::Malformed(format!(
            "message {original_id} has an unusable reply subject"
        )));
    }
    Ok(serde_json::json!({
        "schema": MESSAGE_SCHEMA,
        "id": reply_id,
        "from_project": { "id": current.id, "name": current.name },
        "to_project": { "id": sender.id, "name": sender.name },
        "created_at": occurred_at,
        "updated_at": occurred_at,
        "subject": subject,
        "body": body,
        "message_type": "reply",
        "release": Value::Null,
        "in_reply_to": original_id,
        "conversation_id": conversation_id,
        "dedupe_key": FINAL_REPLY_DEDUPE_KEY,
        "processing_status": "new",
        "reply_status": "none",
        "queue_tasks": [],
        "remarks": [],
        "reply_ids": []
    }))
}

fn stable_reply_id(original_id: &str, from_id: &str, dedupe_key: &str) -> String {
    let digest = Sha256::digest(format!("{original_id}|{from_id}|{dedupe_key}").as_bytes());
    let mut encoded = String::with_capacity(32);
    for byte in &digest[..16] {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    format!("msg-reply-{encoded}")
}

fn truncate_utf16(value: &str, maximum: usize) -> String {
    let mut used = 0;
    let mut result = String::new();
    for character in value.chars() {
        let units = character.len_utf16();
        if used + units > maximum {
            break;
        }
        result.push(character);
        used += units;
    }
    result
}

fn endpoint_id<'a>(value: &'a Value, field: &str, message_id: &str) -> Result<&'a str> {
    value
        .as_object()
        .and_then(|object| object.get(field))
        .and_then(Value::as_object)
        .and_then(|endpoint| endpoint.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            InboxError::Malformed(format!("message {message_id} has no {field}.id string"))
        })
}

fn message_path(paths: &InboxPaths, id: &str) -> Result<PathBuf> {
    if !valid_message_id(id) {
        return Err(InboxError::Malformed(format!(
            "invalid inbox message id {id:?}"
        )));
    }
    Ok(paths.messages.join(format!("{id}.json")))
}

fn reply_ids_mut<'a>(value: &'a mut Value, message_id: &str) -> Result<&'a mut Vec<Value>> {
    value
        .as_object_mut()
        .and_then(|object| object.get_mut("reply_ids"))
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            InboxError::Malformed(format!("message {message_id} has no reply_ids array"))
        })
}

fn assert_identical_reply(existing: &Record, expected: &Value, reply_id: &str) -> Result<()> {
    // Validate the candidate record too, but do not compare recipient-owned processing fields:
    // a sender may already have read or closed the delivered reply when a source-side retry
    // occurs.  This is the same content/idempotency boundary as Orchestra's `Write-NewMessage`.
    let _ = parse_message(expected, reply_id)?;
    for field in [
        "subject",
        "body",
        "message_type",
        "in_reply_to",
        "conversation_id",
        "dedupe_key",
        "release",
    ] {
        if existing.value.get(field) != expected.get(field) {
            return Err(InboxError::Malformed(format!(
                "idempotent final reply conflicts with existing message {reply_id}"
            )));
        }
    }
    for field in ["from_project", "to_project"] {
        if endpoint_id(&existing.value, field, reply_id)? != endpoint_id(expected, field, reply_id)?
        {
            return Err(InboxError::Malformed(format!(
                "idempotent final reply conflicts with existing message {reply_id}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn paths(root: &Path) -> Result<Option<InboxPaths>> {
    let root_metadata = fs::symlink_metadata(root).map_err(|error| {
        InboxError::Malformed(format!(
            "repository root does not exist or is unreadable {}: {error}",
            root.display()
        ))
    })?;
    assert_plain_directory(root, &root_metadata)?;
    let inbox = root.join(INBOX_DIRECTORY);
    match fs::symlink_metadata(&inbox) {
        Ok(metadata) => assert_plain_directory(&inbox, &metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let messages = inbox.join(MESSAGES_DIRECTORY);
    let metadata = fs::symlink_metadata(&messages).map_err(|error| {
        InboxError::Malformed(format!(
            "initialized inbox is missing its messages directory {}: {error}",
            messages.display()
        ))
    })?;
    assert_plain_directory(&messages, &metadata)?;
    let lock = inbox.join(LOCK_FILE);
    if let Ok(metadata) = fs::symlink_metadata(&lock) {
        assert_plain_file(&lock, &metadata)?;
    }
    Ok(Some(InboxPaths { messages, lock }))
}

#[derive(Debug, Clone)]
pub(crate) struct InboxPaths {
    pub(crate) messages: PathBuf,
    pub(crate) lock: PathBuf,
}

pub(crate) struct InboxLock {
    path: PathBuf,
    token: String,
}

impl InboxLock {
    /// Compatible with Orchestra's `Acquire-Lock`: create a single-owner sentinel and never
    /// delete a lock we did not create. The processor treats a live foreign lock as a retryable
    /// boundary instead of racing a sender/curator over message JSON.
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match work_fs::create_new_plain_file(path) {
                Ok(mut file) => {
                    let token = format!("{}\n", Uuid::new_v4());
                    file.write_all(token.as_bytes())?;
                    file.sync_all()?;
                    let parent = path.parent().ok_or_else(|| {
                        InboxError::Malformed("inbox lock has no parent directory".into())
                    })?;
                    let observed = work_fs::read_required_text(parent, path, 1_024)?;
                    if observed != token {
                        return Err(InboxError::Malformed(
                            "inbox lock ownership changed before use".into(),
                        ));
                    }
                    return Ok(Self {
                        path: path.to_owned(),
                        token,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    if Instant::now() >= deadline {
                        return Err(InboxError::Busy);
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// Deliver one already-constructed release record under the same inbox lock as ordinary
/// messages. A retry accepts only byte-semantic identity for sender-owned content while leaving
/// recipient-owned processing fields untouched.
pub(crate) fn deliver_release_message(
    root: &Path,
    id: &str,
    expected: &Value,
    cancelled: &dyn Fn() -> bool,
) -> Result<()> {
    let target_paths = paths(root)?.ok_or_else(|| {
        InboxError::Malformed(format!(
            "registered dependent has no initialized inbox: {}",
            root.display()
        ))
    })?;
    let target_path = message_path(&target_paths, id)?;
    let _lock = InboxLock::acquire(&target_paths.lock)?;
    if cancelled() {
        return Err(InboxError::Malformed(
            "release delivery lost owner authority while waiting for the target inbox lock".into(),
        ));
    }
    match fs::symlink_metadata(&target_path) {
        Ok(metadata) => {
            assert_plain_file(&target_path, &metadata)?;
            let existing = load_message(target_path)?;
            assert_identical_reply(&existing, expected, id)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let _ = parse_message(expected, id)?;
            if cancelled() {
                return Err(InboxError::Malformed(
                    "release delivery lost owner authority before the target inbox write".into(),
                ));
            }
            write_message(&target_path, expected)
        }
        Err(error) => Err(error.into()),
    }
}

impl Drop for InboxLock {
    fn drop(&mut self) {
        let Some(parent) = self.path.parent() else {
            return;
        };
        if work_fs::read_optional_text(parent, &self.path, 1_024)
            .is_ok_and(|value| value.as_deref() == Some(self.token.as_str()))
        {
            let _ = work_fs::remove_plain_file(parent, &self.path);
        }
    }
}

#[derive(Debug)]
struct Record {
    path: PathBuf,
    value: Value,
    message: InboxMessage,
}

fn load_messages(paths: &InboxPaths) -> Result<Vec<Record>> {
    let mut file_paths = Vec::new();
    for entry in fs::read_dir(&paths.messages)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("msg-") || !name.ends_with(".json") {
            continue;
        }
        let metadata = fs::symlink_metadata(&path)?;
        assert_plain_file(&path, &metadata)?;
        file_paths.push(path);
        if file_paths.len() > MAX_MESSAGES {
            return Err(InboxError::Malformed(format!(
                "inbox exceeds the {MAX_MESSAGES}-message inspection limit"
            )));
        }
    }
    file_paths.sort();
    let mut records = file_paths
        .into_iter()
        .map(load_message)
        .collect::<Result<Vec<_>>>()?;
    records.sort_by(|left, right| left.message.id.cmp(&right.message.id));
    Ok(records)
}

fn load_message(path: PathBuf) -> Result<Record> {
    // Re-check the entry after `read_dir` collection.  Inbox writers are outside this process,
    // and a record replaced with a redirect in that small window must not be followed.
    let metadata = fs::symlink_metadata(&path)?;
    assert_plain_file(&path, &metadata)?;
    if metadata.len() > MAX_MESSAGE_RECORD_BYTES {
        return Err(InboxError::Malformed(format!(
            "message record exceeds the {MAX_MESSAGE_RECORD_BYTES}-byte limit: {}",
            path.display()
        )));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            InboxError::Malformed(format!(
                "message path is not valid UTF-8: {}",
                path.display()
            ))
        })?;
    let id = name
        .strip_prefix("msg-")
        .and_then(|name| name.strip_suffix(".json"))
        .map(|suffix| format!("msg-{suffix}"))
        .ok_or_else(|| InboxError::Malformed(format!("invalid message filename {name:?}")))?;
    let text = read_inbox_text(&path, "inbox message", MAX_MESSAGE_RECORD_BYTES)?;
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        InboxError::Malformed(format!("{} is not valid JSON: {error}", path.display()))
    })?;
    let message = parse_message(&value, &id)?;
    Ok(Record {
        path,
        value,
        message,
    })
}

fn parse_message(value: &Value, file_id: &str) -> Result<InboxMessage> {
    let map = value
        .as_object()
        .ok_or_else(|| InboxError::Malformed(format!("message {file_id} must be a JSON object")))?;
    let schema = required_string(map, "schema", file_id)?;
    if schema != MESSAGE_SCHEMA {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has unsupported schema {schema:?}"
        )));
    }
    let id = required_string(map, "id", file_id)?;
    if id != file_id || !valid_message_id(id) {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has a mismatched or invalid id {id:?}"
        )));
    }
    validate_endpoint(map, "from_project", file_id)?;
    validate_endpoint(map, "to_project", file_id)?;
    let from = map["from_project"]["id"]
        .as_str()
        .expect("validated endpoint");
    let to = map["to_project"]["id"]
        .as_str()
        .expect("validated endpoint");
    if from == to {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has identical sender and recipient"
        )));
    }
    validate_text(
        required_string(map, "subject", file_id)?,
        MAX_SUBJECT_UTF16_UNITS,
        false,
        "subject",
        file_id,
    )?;
    validate_body(required_string(map, "body", file_id)?, file_id)?;

    let in_reply_to =
        optional_string(map, "in_reply_to", file_id)?.filter(|value| !value.is_empty());
    if let Some(value) = &in_reply_to
        && !valid_message_id(value)
    {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has invalid in_reply_to {value:?}"
        )));
    }
    let conversation = optional_string(map, "conversation_id", file_id)?
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| in_reply_to.clone().unwrap_or_else(|| id.to_owned()));
    if !valid_message_id(&conversation) {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has invalid conversation_id {conversation:?}"
        )));
    }
    let message_type = match optional_string(map, "message_type", file_id)?
        .unwrap_or_else(|| {
            if in_reply_to.is_some() {
                "reply".into()
            } else {
                "request".into()
            }
        })
        .as_str()
    {
        "request" => MessageType::Request,
        "reply" => MessageType::Reply,
        "release" => MessageType::Release,
        value => {
            return Err(InboxError::Malformed(format!(
                "message {file_id} has invalid message_type {value:?}"
            )));
        }
    };
    if matches!(message_type, MessageType::Reply) != in_reply_to.is_some() {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has incompatible message_type and in_reply_to"
        )));
    }
    validate_release_metadata(map, message_type, from, file_id)?;
    validate_text(
        required_string(map, "dedupe_key", file_id)?,
        120,
        true,
        "dedupe_key",
        file_id,
    )?;
    let processing_status =
        parse_processing(required_string(map, "processing_status", file_id)?, file_id)?;
    let reply_status = parse_reply(required_string(map, "reply_status", file_id)?, file_id)?;
    let queue_tasks = parse_task_ids(map.get("queue_tasks"), file_id)?;
    parse_message_ids(map.get("reply_ids"), file_id, "reply_ids")?;
    Ok(InboxMessage {
        id: id.to_owned(),
        message_type,
        processing_status,
        reply_status,
        in_reply_to,
        queue_tasks,
    })
}

fn validate_release_metadata(
    map: &Map<String, Value>,
    message_type: MessageType,
    source_id: &str,
    file_id: &str,
) -> Result<()> {
    let release = map.get("release").filter(|value| !value.is_null());
    if !matches!(message_type, MessageType::Release) {
        if release.is_some() {
            return Err(InboxError::Malformed(format!(
                "non-release message {file_id} carries release metadata"
            )));
        }
        return Ok(());
    }
    let release = release.and_then(Value::as_object).ok_or_else(|| {
        InboxError::Malformed(format!("release message {file_id} has no release object"))
    })?;
    let release_id = required_string(release, "id", file_id)?;
    let version = required_string(release, "version", file_id)?;
    validate_text(version, 120, false, "release.version", file_id)?;
    if version != version.trim() || release_id != stable_release_id(source_id, version) {
        return Err(InboxError::Malformed(format!(
            "release message {file_id} has a non-canonical release identity"
        )));
    }
    validate_text(
        required_string(release, "release_url", file_id)?,
        2_048,
        true,
        "release.release_url",
        file_id,
    )?;
    validate_text(
        required_string(release, "source_revision", file_id)?,
        240,
        true,
        "release.source_revision",
        file_id,
    )?;
    let products = release
        .get("products")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            InboxError::Malformed(format!("release message {file_id} has no products array"))
        })?;
    if products.len() > 100 {
        return Err(InboxError::Malformed(format!(
            "release message {file_id} has too many products"
        )));
    }
    let mut unique = BTreeSet::new();
    for product in products {
        let product = product.as_str().ok_or_else(|| {
            InboxError::Malformed(format!(
                "release message {file_id} has a non-string product"
            ))
        })?;
        validate_product(product, file_id)?;
        if !unique.insert(product.to_lowercase()) {
            return Err(InboxError::Malformed(format!(
                "release message {file_id} duplicates product {product:?}"
            )));
        }
    }
    Ok(())
}

fn stable_release_id(source_id: &str, version: &str) -> String {
    let digest = Sha256::digest(format!("{source_id}|{version}").as_bytes());
    format!("rel-{}", hex_lower(&digest[..16]))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 0x0f) as usize] as char);
    }
    text
}

fn validate_product(value: &str, file_id: &str) -> Result<()> {
    let Some((ecosystem, name)) = value.split_once(':') else {
        return Err(InboxError::Malformed(format!(
            "release message {file_id} has invalid product {value:?}"
        )));
    };
    if ecosystem.is_empty()
        || ecosystem.len() > 32
        || name.is_empty()
        || name.encode_utf16().count() > 200
        || value.encode_utf16().count() > 240
        || !ecosystem.as_bytes()[0].is_ascii_alphanumeric()
        || ecosystem
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'-'))
        || name.chars().any(char::is_control)
        || name.trim().is_empty()
        || name != name.trim()
        || name.starts_with(':')
    {
        return Err(InboxError::Malformed(format!(
            "release message {file_id} has invalid product {value:?}"
        )));
    }
    Ok(())
}

fn validate_endpoint(map: &Map<String, Value>, field: &str, file_id: &str) -> Result<()> {
    let endpoint = map
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| InboxError::Malformed(format!("message {file_id} has no object {field}")))?;
    let id = required_string(endpoint, "id", file_id)?;
    if !valid_project_id(id) {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has invalid {field}.id {id:?}"
        )));
    }
    validate_text(
        required_string(endpoint, "name", file_id)?,
        MAX_PROJECT_NAME_UTF16_UNITS,
        false,
        &format!("{field}.name"),
        file_id,
    )
}

fn parse_processing(value: &str, file_id: &str) -> Result<ProcessingStatus> {
    match value {
        "new" => Ok(ProcessingStatus::New),
        "read" => Ok(ProcessingStatus::Read),
        "queued" => Ok(ProcessingStatus::Queued),
        "implemented" => Ok(ProcessingStatus::Implemented),
        "rejected" => Ok(ProcessingStatus::Rejected),
        _ => Err(InboxError::Malformed(format!(
            "message {file_id} has invalid processing_status {value:?}"
        ))),
    }
}

fn parse_reply(value: &str, file_id: &str) -> Result<ReplyStatus> {
    match value {
        "none" => Ok(ReplyStatus::None),
        "acknowledged" => Ok(ReplyStatus::Acknowledged),
        "final" => Ok(ReplyStatus::Final),
        _ => Err(InboxError::Malformed(format!(
            "message {file_id} has invalid reply_status {value:?}"
        ))),
    }
}

fn parse_task_ids(value: Option<&Value>, file_id: &str) -> Result<BTreeSet<String>> {
    let values = value.and_then(Value::as_array).ok_or_else(|| {
        InboxError::Malformed(format!("message {file_id} has no array queue_tasks"))
    })?;
    let mut tasks = BTreeSet::new();
    for value in values {
        let task = value.as_str().ok_or_else(|| {
            InboxError::Malformed(format!(
                "message {file_id} queue_tasks contains a non-string"
            ))
        })?;
        if !is_task_id(task) || !tasks.insert(task.to_owned()) {
            return Err(InboxError::Malformed(format!(
                "message {file_id} has invalid or duplicate queue task {task:?}"
            )));
        }
    }
    Ok(tasks)
}

fn parse_message_ids(value: Option<&Value>, file_id: &str, field: &str) -> Result<()> {
    let values = value
        .and_then(Value::as_array)
        .ok_or_else(|| InboxError::Malformed(format!("message {file_id} has no array {field}")))?;
    for value in values {
        let id = value.as_str().ok_or_else(|| {
            InboxError::Malformed(format!("message {file_id} {field} contains a non-string"))
        })?;
        if !valid_message_id(id) {
            return Err(InboxError::Malformed(format!(
                "message {file_id} has invalid {field} entry {id:?}"
            )));
        }
    }
    Ok(())
}

fn required_string<'a>(map: &'a Map<String, Value>, field: &str, file_id: &str) -> Result<&'a str> {
    map.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| InboxError::Malformed(format!("message {file_id} has no string {field}")))
}

fn optional_string(map: &Map<String, Value>, field: &str, file_id: &str) -> Result<Option<String>> {
    match map.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(InboxError::Malformed(format!(
            "message {file_id} has non-string {field}"
        ))),
    }
}

fn validate_text(
    value: &str,
    max_utf16_units: usize,
    allow_empty: bool,
    field: &str,
    file_id: &str,
) -> Result<()> {
    if (!allow_empty && value.trim().is_empty())
        || value.encode_utf16().count() > max_utf16_units
        || value.chars().any(|character| character.is_control())
    {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has invalid {field}"
        )));
    }
    Ok(())
}

fn validate_body(value: &str, file_id: &str) -> Result<()> {
    // Legacy bodies are UTF-8 documents, not single-line endpoint fields. Newlines and tabs are
    // meaningful release/request formatting; the legacy envelope permits an empty body, while
    // the UTF-8 byte limit and NUL remain fail-closed native bounds.
    if value.len() > MAX_BODY_BYTES || value.contains('\0') {
        return Err(InboxError::Malformed(format!(
            "message {file_id} has invalid body"
        )));
    }
    Ok(())
}

fn task_links(root: &Path) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut links = BTreeMap::new();
    let Some(work) = work_directory(root)? else {
        return Ok(links);
    };
    for file in ["Tasks_Queue.md", "Tasks_Done.md"] {
        if let Some(text) = read_optional_inbox_text(&work, &work.join(file))? {
            add_links_from_text(&text, None, &mut links);
        }
    }
    let tasks = work.join("tasks");
    let entries = match fs::symlink_metadata(&tasks) {
        Ok(metadata) => {
            assert_plain_directory(&tasks, &metadata)?;
            Some(fs::read_dir(&tasks)?)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(entries) = entries {
        for entry in entries {
            let entry = entry?;
            let task_id = entry.file_name().to_string_lossy().into_owned();
            if !is_task_id(&task_id) {
                continue;
            }
            let task_directory = entry.path();
            let metadata = fs::symlink_metadata(&task_directory)?;
            assert_plain_directory(&task_directory, &metadata)?;
            if let Some(text) = read_optional_inbox_text(&work, &task_directory.join("task.md"))? {
                add_links_from_text(&text, Some(&task_id), &mut links);
            }
        }
    }
    Ok(links)
}

/// `.work` is part of the reconciliation authority, not merely a convenience path.  A redirect
/// would let an inbox message make the engine attribute task provenance from an unrelated tree,
/// so a present redirected work directory is rejected rather than silently skipped.
fn work_directory(root: &Path) -> Result<Option<PathBuf>> {
    let work = root.join(".work");
    match fs::symlink_metadata(&work) {
        Ok(metadata) => {
            assert_plain_directory(&work, &metadata)?;
            Ok(Some(work))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_optional_inbox_text(work: &Path, path: &Path) -> Result<Option<String>> {
    work_fs::read_optional_bytes(work, path, MAX_CONTROL_BYTES)
        .map_err(|error| {
            map_plain_read_error(path, "inbox control-plane file", MAX_CONTROL_BYTES, error)
        })?
        .map(decode_plain_text)
        .transpose()
}

fn read_inbox_text(path: &Path, label: &str, maximum_bytes: u64) -> Result<String> {
    let bytes = work_fs::read_plain_bytes(path, maximum_bytes)
        .map_err(|error| map_plain_read_error(path, label, maximum_bytes, error))?;
    decode_plain_text(bytes)
}

fn map_plain_read_error(
    path: &Path,
    label: &str,
    maximum_bytes: u64,
    error: io::Error,
) -> InboxError {
    match work_fs::plain_read_violation(&error) {
        Some(work_fs::PlainReadViolation::GrewWhileReading { .. }) => {
            InboxError::Malformed(format!(
                "{label} grew beyond the {maximum_bytes}-byte limit: {}",
                path.display()
            ))
        }
        Some(work_fs::PlainReadViolation::Oversize { .. }) => InboxError::Malformed(format!(
            "{label} exceeds the {maximum_bytes}-byte limit: {}",
            path.display()
        )),
        Some(work_fs::PlainReadViolation::NotPlain { .. }) => InboxError::Malformed(format!(
            "inbox path is not a plain file: {}",
            path.display()
        )),
        Some(work_fs::PlainReadViolation::ParentNotPlain { path: directory }) => {
            InboxError::Malformed(format!(
                "inbox path is not a plain directory: {}",
                directory.display()
            ))
        }
        None if error.kind() == io::ErrorKind::InvalidData => InboxError::Malformed(format!(
            "inbox path is not a plain file: {}",
            path.display()
        )),
        None => InboxError::Io(error),
    }
}

fn decode_plain_text(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(|_| {
        InboxError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        ))
    })
}

fn add_links_from_text(
    text: &str,
    fixed_task: Option<&str>,
    links: &mut BTreeMap<String, BTreeSet<String>>,
) {
    let mut current_task = fixed_task.map(str::to_owned);
    for line in text.lines() {
        if fixed_task.is_none() {
            // A non-header line belongs to the preceding queue/archive record.  In particular,
            // the provenance marker below must not erase the T-ID that scopes it.  A bracketed
            // P-ID record deliberately clears that scope, matching `inbox.ps1` exactly.
            if is_bracketed_record_header(line) {
                current_task = task_header_id(line);
            } else if let Some(task_id) = legacy_task_header_id(line) {
                current_task = Some(task_id);
            }
        }
        if let (Some(message_id), Some(task_id)) = (inbox_marker_id(line), current_task.as_ref()) {
            links
                .entry(message_id)
                .or_default()
                .insert(task_id.to_owned());
        }
    }
}

fn task_header_id(line: &str) -> Option<String> {
    let rest = heading_rest(line)?;
    let bracketed = rest.strip_prefix('[').and_then(|tail| {
        let end = tail.find(']')?;
        let id = &tail[..end];
        tail[end + 1..]
            .starts_with(char::is_whitespace)
            .then(|| id.to_owned())
    });
    if let Some(id) = bracketed {
        return is_task_id(&id).then_some(id);
    }
    legacy_task_header_id(line)
}

fn is_bracketed_record_header(line: &str) -> bool {
    heading_rest(line).is_some_and(|rest| rest.starts_with('['))
}

fn legacy_task_header_id(line: &str) -> Option<String> {
    let rest = heading_rest(line)?;
    let prefix = "Активная задача ";
    let id = rest.strip_prefix(prefix)?.split_whitespace().next()?;
    is_task_id(id).then_some(id.to_owned())
}

fn heading_rest(line: &str) -> Option<&str> {
    let hashes = line.len() - line.trim_start_matches('#').len();
    if !(1..=6).contains(&hashes) {
        return None;
    }
    let rest = &line[hashes..];
    rest.starts_with(char::is_whitespace)
        .then(|| rest.trim_start())
}

fn inbox_marker_id(line: &str) -> Option<String> {
    let rest = line.trim();
    let id = rest.strip_prefix("Inbox message:")?.trim();
    valid_message_id(id).then_some(id.to_owned())
}

fn completed_ids(root: &Path) -> Result<BTreeSet<String>> {
    let Some(work) = work_directory(root)? else {
        return Ok(BTreeSet::new());
    };
    match read_optional_inbox_text(&work, &work.join("Tasks_Done.md"))? {
        Some(text) => Ok(text
            .lines()
            .filter_map(archive_header_task_id)
            .map(str::to_owned)
            .collect()),
        None => Ok(BTreeSet::new()),
    }
}

fn set_task_links(value: &mut Value, tasks: &BTreeSet<String>) -> Result<()> {
    let map = value.as_object_mut().ok_or_else(|| {
        InboxError::Malformed("message changed from a JSON object during reconciliation".into())
    })?;
    map.insert(
        "queue_tasks".into(),
        Value::Array(tasks.iter().cloned().map(Value::String).collect()),
    );
    Ok(())
}

fn set_string(value: &mut Value, field: &str, replacement: &str) -> Result<()> {
    let map = value.as_object_mut().ok_or_else(|| {
        InboxError::Malformed("message changed from a JSON object during reconciliation".into())
    })?;
    map.insert(field.into(), Value::String(replacement.into()));
    Ok(())
}

fn append_remark(value: &mut Value, at: &str, actor: &str, text: &str) -> Result<()> {
    let map = value.as_object_mut().ok_or_else(|| {
        InboxError::Malformed("message changed from a JSON object during reconciliation".into())
    })?;
    let remarks = map
        .get_mut("remarks")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| InboxError::Malformed("message has no remarks array".into()))?;
    remarks.push(serde_json::json!({ "at": at, "actor": actor, "text": text }));
    Ok(())
}

fn write_message(path: &Path, value: &Value) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        assert_plain_file(path, &metadata)?;
    }
    let text = serde_json::to_string_pretty(value).map_err(|error| {
        InboxError::Malformed(format!(
            "cannot serialize reconciled inbox message: {error}"
        ))
    })?;
    let parent = path.parent().ok_or_else(|| {
        InboxError::Malformed(format!("inbox message has no parent: {}", path.display()))
    })?;
    work_fs::replace_file(
        parent,
        path,
        format!("{text}\n").as_bytes(),
        MAX_MESSAGE_RECORD_BYTES,
    )
    .map_err(InboxError::Io)
}

fn assert_plain_directory(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_dir() {
        return Err(InboxError::Malformed(format!(
            "inbox path is not a plain directory: {}",
            path.display()
        )));
    }
    work_fs::require_plain_directory(path).map_err(|error| {
        if error.kind() == io::ErrorKind::InvalidData {
            InboxError::Malformed(format!(
                "inbox path is not a plain directory: {}",
                path.display()
            ))
        } else {
            InboxError::Io(error)
        }
    })
}

fn assert_plain_file(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    work_fs::require_plain_file(path, metadata).map_err(|_| {
        InboxError::Malformed(format!(
            "inbox path is not a plain file: {}",
            path.display()
        ))
    })
}

fn valid_message_id(id: &str) -> bool {
    id.strip_prefix("msg-").is_some_and(|suffix| {
        (8..=120).contains(&suffix.len())
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    })
}

fn valid_project_id(id: &str) -> bool {
    id.strip_prefix("repo-").is_some_and(|suffix| {
        suffix.len() == 20
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn looks_like_utc(value: &str) -> bool {
    !value.is_empty()
        && value.ends_with('Z')
        && value.bytes().all(|byte| {
            byte.is_ascii_digit() || matches!(byte, b'-' | b':' | b'.' | b'T' | b'Z' | b' ')
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct Root {
        path: PathBuf,
    }

    impl Root {
        fn new() -> Self {
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::current_dir()
                .unwrap()
                .join("target/test-temp")
                .join(format!(
                    "orchestrail-inbox-{}-{sequence}",
                    std::process::id()
                ));
            fs::create_dir_all(&path).unwrap();
            let path = crate::dependency_graph::canonical_project_root(&path).unwrap();
            Self { path }
        }

        fn write(&self, relative: &str, text: &str) {
            let path = self.path.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, text).unwrap();
        }

        fn message(&self, id: &str, status: &str, reply: &str, tasks: &[&str]) {
            self.message_from_to(
                id,
                status,
                reply,
                tasks,
                "repo-0123456789abcdef0123",
                "Sender",
                "repo-abcdef01234567890123",
                "Receiver",
            );
        }

        #[allow(clippy::too_many_arguments)]
        fn message_from_to(
            &self,
            id: &str,
            status: &str,
            reply: &str,
            tasks: &[&str],
            from_id: &str,
            from_name: &str,
            to_id: &str,
            to_name: &str,
        ) {
            self.write(
                &format!(".inbox/messages/{id}.json"),
                &serde_json::json!({
                    "schema": MESSAGE_SCHEMA,
                    "id": id,
                    "from_project": { "id": from_id, "name": from_name },
                    "to_project": { "id": to_id, "name": to_name },
                    "created_at": "2026-07-25T12:00:00.000Z",
                    "updated_at": "2026-07-25T12:00:00.000Z",
                    "subject": "Request",
                    "body": "Evidence",
                    "message_type": "request",
                    "release": null,
                    "in_reply_to": "",
                    "conversation_id": id,
                    "dedupe_key": "test",
                    "processing_status": status,
                    "reply_status": reply,
                    "queue_tasks": tasks,
                    "remarks": [],
                    "reply_ids": []
                })
                .to_string(),
            );
        }
    }

    #[cfg(windows)]
    fn symlink_file(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }

    #[cfg(unix)]
    fn symlink_file(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn symlink_directory(target: &Path, link: &Path) -> io::Result<()> {
        use std::process::{Command, Stdio};

        match std::os::windows::fs::symlink_dir(target, link) {
            Ok(()) => Ok(()),
            Err(error) if error.raw_os_error() == Some(1_314) => {
                let parent = target.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "test target has no parent")
                })?;
                let relative_link = link.strip_prefix(parent).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "test junction paths do not share a parent",
                    )
                })?;
                let target_name = target.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "test target has no name")
                })?;
                let command = format!(
                    "mklink /j {} {}",
                    relative_link.to_string_lossy().replace('/', "\\"),
                    target_name.to_string_lossy()
                );
                let output = Command::new("cmd.exe")
                    .args(["/d", "/c", &command])
                    .current_dir(parent)
                    .stdin(Stdio::null())
                    .output()?;
                if output.status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "failed to create test junction {} -> {}: stdout={} stderr={}",
                        link.display(),
                        target.display(),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    fn symlink_directory(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn remove_directory_link(path: &Path) -> io::Result<()> {
        fs::remove_dir(path)
    }

    #[cfg(unix)]
    fn remove_directory_link(path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    #[test]
    fn multiline_utf8_bodies_are_valid_but_release_metadata_is_fail_closed() {
        let id = "msg-send-0123456789abcdef0123456789abcdef";
        let from = "repo-0123456789abcdef0123";
        let base = serde_json::json!({
            "schema": MESSAGE_SCHEMA,
            "id": id,
            "from_project": { "id": from, "name": "Sender" },
            "to_project": { "id": "repo-abcdef01234567890123", "name": "Receiver" },
            "created_at": "2026-07-26T00:00:00Z",
            "updated_at": "2026-07-26T00:00:00Z",
            "subject": "Release",
            "body": "First line\n\nSecond line\twith detail",
            "message_type": "request",
            "release": null,
            "in_reply_to": null,
            "conversation_id": id,
            "dedupe_key": "test",
            "processing_status": "new",
            "reply_status": "none",
            "queue_tasks": [],
            "remarks": [],
            "reply_ids": []
        });
        parse_message(&base, id).expect("multiline request body");
        let mut empty_body = base.clone();
        empty_body["body"] = Value::String(String::new());
        parse_message(&empty_body, id).expect("legacy envelope permits an empty body");
        let mut invalid_dedupe = base.clone();
        invalid_dedupe["dedupe_key"] = Value::String("bad\nkey".into());
        assert!(parse_message(&invalid_dedupe, id).is_err());

        let mut release = base.clone();
        release["message_type"] = Value::String("release".into());
        release["release"] = serde_json::json!({
            "id": stable_release_id(from, "1.2.3"),
            "version": "1.2.3",
            "products": ["cargo:source"],
            "release_url": "https://example.invalid/1.2.3",
            "source_revision": "abc123"
        });
        parse_message(&release, id).expect("valid structured release metadata");

        release["release"]["id"] = Value::String("rel-wrong".into());
        assert!(parse_message(&release, id).is_err());
        let mut request_with_release = base;
        request_with_release["release"] = serde_json::json!({"id": "rel-wrong"});
        assert!(parse_message(&request_with_release, id).is_err());
    }

    impl Drop for Root {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn absent_inbox_is_a_legacy_no_op() {
        let root = Root::new();
        assert_eq!(inspect(&root.path).unwrap(), InboxProjection::Absent);
        assert_eq!(actionable(&root.path).unwrap(), Actionable::default());
        assert_eq!(
            reconcile(&root.path, "2026-07-25T12:00:00Z").unwrap(),
            ReconcileResult::Absent
        );
    }

    #[test]
    fn actionable_classification_uses_archived_task_headers_only() {
        let root = Root::new();
        root.message("msg-00000001", "new", "none", &[]);
        root.message("msg-00000002", "read", "none", &[]);
        root.message("msg-00000003", "queued", "none", &["T-1"]);
        root.message("msg-00000004", "implemented", "none", &["T-2"]);
        root.write(".work/Tasks_Done.md", "## [T-1] done\nBody mentions T-2\n");

        assert_eq!(
            actionable(&root.path).unwrap(),
            Actionable {
                new: vec!["msg-00000001".into()],
                unresolved: vec!["msg-00000002".into()],
                completable: vec!["msg-00000003".into()],
                reply_pending: vec!["msg-00000004".into()],
            }
        );
    }

    #[test]
    fn reconciliation_recovers_provenance_and_is_idempotent() {
        let root = Root::new();
        let id = "msg-00000001";
        root.message(id, "read", "none", &[]);
        root.write(
            ".work/Tasks_Queue.md",
            "### [T-5] Imported work — статус: не начата\nInbox message: msg-00000001\n",
        );
        assert_eq!(
            reconcile(&root.path, "2026-07-25T12:00:00Z").unwrap(),
            ReconcileResult::Reconciled {
                updated: vec![id.into()]
            }
        );
        let value: Value = serde_json::from_str(
            &fs::read_to_string(root.path.join(format!(".inbox/messages/{id}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(value["processing_status"], "queued");
        assert_eq!(value["queue_tasks"], serde_json::json!(["T-5"]));
        assert_eq!(value["remarks"][0]["actor"], "inbox-reconcile");
        assert_eq!(
            reconcile(&root.path, "2026-07-25T12:00:01Z").unwrap(),
            ReconcileResult::Reconciled { updated: vec![] }
        );
    }

    #[test]
    fn malformed_or_redirected_inbox_is_not_silently_ignored() {
        let root = Root::new();
        root.write(".inbox/messages/msg-00000001.json", "not json");
        assert!(matches!(inspect(&root.path), Err(InboxError::Malformed(_))));
    }

    #[test]
    fn redirected_inbox_file_fails_closed_without_reading_the_target() {
        let root = Root::new();
        let id = "msg-00000001";
        root.message(id, "new", "none", &[]);
        let message = root.path.join(format!(".inbox/messages/{id}.json"));
        fs::remove_file(&message).unwrap();
        let external = root.path.with_extension("external-message.json");
        fs::write(&external, "external sentinel\n").unwrap();
        if symlink_file(&external, &message).is_err() {
            fs::remove_file(external).unwrap();
            return;
        }

        let error = inspect(&root.path).unwrap_err();
        assert!(matches!(
            &error,
            InboxError::Malformed(diagnostic)
                if diagnostic.contains("not a plain file") && diagnostic.contains(id)
        ));
        assert_eq!(
            fs::read_to_string(&external).unwrap(),
            "external sentinel\n"
        );

        fs::remove_file(&message).unwrap();
        fs::remove_file(external).unwrap();
    }

    #[test]
    fn redirected_messages_directory_fails_closed_without_reading_the_target() {
        let root = Root::new();
        root.message("msg-00000001", "new", "none", &[]);
        let messages = root.path.join(".inbox").join("messages");
        fs::remove_dir_all(&messages).unwrap();
        let external = root.path.with_extension("external-messages");
        fs::create_dir(&external).unwrap();
        let sentinel = external.join("msg-00000002.json");
        fs::write(&sentinel, "external sentinel\n").unwrap();
        symlink_directory(&external, &messages).unwrap();

        let error = inspect(&root.path).unwrap_err();
        let InboxError::Malformed(diagnostic) = error else {
            panic!("redirected messages directory returned {error}")
        };
        assert!(
            diagnostic.contains("not a plain directory")
                && diagnostic.contains(&messages.display().to_string()),
            "unexpected redirect diagnostic: {diagnostic}"
        );
        assert_eq!(
            fs::read_to_string(&sentinel).unwrap(),
            "external sentinel\n"
        );

        remove_directory_link(&messages).unwrap();
        fs::remove_dir_all(external).unwrap();
    }

    #[test]
    fn redirected_work_root_fails_closed_without_reading_provenance() {
        let root = Root::new();
        root.message("msg-00000001", "queued", "none", &["T-1"]);
        let work = root.path.join(".work");
        let external = root.path.with_extension("external-work");
        fs::create_dir(&external).unwrap();
        let sentinel = external.join("Tasks_Done.md");
        fs::write(&sentinel, "## [T-1] external\n").unwrap();
        symlink_directory(&external, &work).unwrap();

        let error = inspect(&root.path).unwrap_err();
        assert!(matches!(
            &error,
            InboxError::Malformed(diagnostic)
                if diagnostic.contains("not a plain directory")
                    && diagnostic.contains(&work.display().to_string())
        ));
        assert_eq!(
            fs::read_to_string(&sentinel).unwrap(),
            "## [T-1] external\n"
        );

        remove_directory_link(&work).unwrap();
        fs::remove_dir_all(external).unwrap();
    }

    #[test]
    fn redirected_task_directory_fails_closed_without_reading_provenance() {
        let root = Root::new();
        let id = "msg-00000001";
        root.message(id, "read", "none", &[]);
        root.write(
            ".work/tasks/T-001/task.md",
            "# Task\n\nInbox message: msg-original\n",
        );
        let task = root.path.join(".work/tasks/T-001");
        fs::remove_dir_all(&task).unwrap();
        let external = root.path.with_extension("external-task");
        fs::create_dir(&external).unwrap();
        let sentinel = external.join("task.md");
        fs::write(&sentinel, format!("Inbox message: {id}\n")).unwrap();
        symlink_directory(&external, &task).unwrap();

        let error = reconcile(&root.path, "2026-07-25T12:00:00Z").unwrap_err();
        assert!(
            matches!(
                &error,
                InboxError::Malformed(diagnostic)
                    if diagnostic.contains("not a plain directory")
                        && diagnostic.contains("T-001")
            ),
            "unexpected task redirect diagnostic: {error}"
        );
        assert_eq!(
            fs::read_to_string(&sentinel).unwrap(),
            format!("Inbox message: {id}\n")
        );

        remove_directory_link(&task).unwrap();
        fs::remove_dir_all(external).unwrap();
    }

    #[test]
    fn plain_read_mapping_distinguishes_size_file_and_parent_violations() {
        let root = Root::new();
        let oversized = root.path.join("oversized.txt");
        fs::write(&oversized, "12345").unwrap();
        let error = read_inbox_text(&oversized, "test artifact", 4).unwrap_err();
        assert!(matches!(
            &error,
            InboxError::Malformed(diagnostic)
                if diagnostic.contains("test artifact exceeds the 4-byte limit")
        ));

        let parent = root.path.join("not-a-directory");
        fs::write(&parent, "plain file\n").unwrap();
        let leaf = parent.join("task.md");
        let error = read_optional_inbox_text(&root.path, &leaf).unwrap_err();
        assert!(matches!(
            &error,
            InboxError::Malformed(diagnostic)
                if diagnostic.contains("not a plain directory")
                    && diagnostic.contains(&parent.display().to_string())
                    && !diagnostic.contains(&leaf.display().to_string())
        ));
    }

    #[test]
    fn subject_and_project_name_limits_follow_the_legacy_utf16_contract() {
        let root = Root::new();
        let id = "msg-00000001";
        root.message(id, "new", "none", &[]);
        let path = root.path.join(format!(".inbox/messages/{id}.json"));
        let mut value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        value["subject"] = Value::String("😀".repeat(120));
        value["from_project"]["name"] = Value::String("😀".repeat(60));
        fs::write(&path, serde_json::to_string(&value).unwrap()).unwrap();
        assert!(matches!(
            inspect(&root.path),
            Ok(InboxProjection::Present { .. })
        ));

        value["subject"] = Value::String("😀".repeat(121));
        fs::write(&path, serde_json::to_string(&value).unwrap()).unwrap();
        assert!(matches!(inspect(&root.path), Err(InboxError::Malformed(_))));
    }

    #[test]
    fn oversized_message_record_is_rejected_before_json_parsing() {
        let root = Root::new();
        root.write(
            ".inbox/messages/msg-00000001.json",
            &" ".repeat(MAX_MESSAGE_RECORD_BYTES as usize + 1),
        );

        assert!(matches!(inspect(&root.path), Err(InboxError::Malformed(_))));
    }

    #[test]
    fn a_present_non_directory_work_path_cannot_supply_inbox_provenance() {
        let root = Root::new();
        root.message("msg-00000001", "queued", "none", &["T-1"]);
        root.write(".work", "not a directory");

        assert!(matches!(
            actionable(&root.path),
            Err(InboxError::Malformed(_))
        ));
        assert!(matches!(
            reconcile(&root.path, "2026-07-25T12:00:00Z"),
            Err(InboxError::Malformed(_))
        ));
    }

    fn registry_id(root: &Path) -> String {
        crate::dependency_graph::project_id(root)
    }

    fn write_registry(path: &Path, current: &Root, sender: &Root) {
        let current_id = registry_id(&current.path);
        let sender_id = registry_id(&sender.path);
        current.write(
            path.strip_prefix(&current.path).unwrap().to_str().unwrap(),
            &serde_json::json!({
                "schema": crate::dependency_graph::REGISTRY_SCHEMA,
                "generation": 0,
                "updated_at": "2026-07-25T12:00:00Z",
                "projects": [
                    {"id": current_id, "name": "Current", "root": current.path, "products": [], "dependencies": [], "graph_generation": 0},
                    {"id": sender_id, "name": "Sender", "root": sender.path, "products": [], "dependencies": [], "graph_generation": 0}
                ]
            })
            .to_string(),
        );
    }

    fn write_final_candidate(root: &Root, id: &str, body: &str) {
        root.write(
            &format!(".work/inbox_reply_candidates/{id}-final-v1.json"),
            &serde_json::json!({
                "schema": FINAL_REPLY_CANDIDATE_SCHEMA,
                "message_id": id,
                "body": body,
            })
            .to_string(),
        );
    }

    #[test]
    fn native_final_reply_delivery_is_registered_idempotent_and_source_marked() {
        let current = Root::new();
        let sender = Root::new();
        fs::create_dir_all(sender.path.join(".inbox/messages")).unwrap();
        let current_id = registry_id(&current.path);
        let sender_id = registry_id(&sender.path);
        let message_id = "msg-00000001";
        current.message_from_to(
            message_id,
            "implemented",
            "none",
            &[],
            &sender_id,
            "Sender",
            &current_id,
            "Current",
        );
        let source_path = current
            .path
            .join(format!(".inbox/messages/{message_id}.json"));
        let mut source: Value =
            serde_json::from_str(&fs::read_to_string(&source_path).unwrap()).unwrap();
        source["subject"] = Value::String("😀".repeat(120));
        fs::write(&source_path, serde_json::to_string(&source).unwrap()).unwrap();
        let registry = current.path.join(".work/registry/projects.json");
        write_registry(&registry, &current, &sender);
        write_final_candidate(&current, message_id, "Implemented and verified.");

        assert_eq!(
            deliver_final_replies(
                &current.path,
                &current.path.join(".work"),
                &registry,
                "2026-07-25T12:00:01Z",
            )
            .unwrap(),
            vec![message_id]
        );
        let reply_id = stable_reply_id(message_id, &current_id, FINAL_REPLY_DEDUPE_KEY);
        let remote: Value = serde_json::from_str(
            &fs::read_to_string(sender.path.join(format!(".inbox/messages/{reply_id}.json")))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(remote["in_reply_to"], message_id);
        assert_eq!(remote["dedupe_key"], FINAL_REPLY_DEDUPE_KEY);
        assert_eq!(remote["body"], "Implemented and verified.");
        assert!(matches!(
            inspect(&sender.path),
            Ok(InboxProjection::Present { .. })
        ));
        let local: Value = serde_json::from_str(
            &fs::read_to_string(
                current
                    .path
                    .join(format!(".inbox/messages/{message_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(local["reply_status"], "final");
        assert_eq!(local["reply_ids"], serde_json::json!([reply_id]));
        assert!(
            !current
                .path
                .join(format!(
                    ".work/inbox_reply_candidates/{message_id}-final-v1.json"
                ))
                .exists()
        );
        assert!(
            deliver_final_replies(
                &current.path,
                &current.path.join(".work"),
                &registry,
                "2026-07-25T12:00:01Z",
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn final_reply_recovery_accepts_a_delivered_reply_already_read_by_the_sender() {
        let current = Root::new();
        let sender = Root::new();
        fs::create_dir_all(sender.path.join(".inbox/messages")).unwrap();
        let current_id = registry_id(&current.path);
        let sender_id = registry_id(&sender.path);
        let message_id = "msg-00000001";
        current.message_from_to(
            message_id,
            "rejected",
            "none",
            &[],
            &sender_id,
            "Sender",
            &current_id,
            "Current",
        );
        let registry = current.path.join(".work/registry/projects.json");
        write_registry(&registry, &current, &sender);
        write_final_candidate(&current, message_id, "Rejected with evidence.");
        let original: Value = serde_json::from_str(
            &fs::read_to_string(
                current
                    .path
                    .join(format!(".inbox/messages/{message_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        let reply_id = stable_reply_id(message_id, &current_id, FINAL_REPLY_DEDUPE_KEY);
        let mut reply = new_final_reply(
            &reply_id,
            message_id,
            &original,
            &RegisteredProject {
                id: current_id,
                name: "Current".into(),
                root: current.path.clone(),
                products: vec![],
                graph_generation: 0,
            },
            &RegisteredProject {
                id: sender_id,
                name: "Sender".into(),
                root: sender.path.clone(),
                products: vec![],
                graph_generation: 0,
            },
            "Rejected with evidence.",
            "2026-07-25T12:00:01Z",
        )
        .unwrap();
        reply["processing_status"] = Value::String("read".into());
        fs::write(
            sender.path.join(format!(".inbox/messages/{reply_id}.json")),
            serde_json::to_string(&reply).unwrap(),
        )
        .unwrap();

        assert_eq!(
            deliver_final_replies(
                &current.path,
                &current.path.join(".work"),
                &registry,
                "2026-07-25T12:00:01Z",
            )
            .unwrap(),
            vec![message_id]
        );
        let local: Value = serde_json::from_str(
            &fs::read_to_string(
                current
                    .path
                    .join(format!(".inbox/messages/{message_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(local["reply_status"], "final");
    }
}
