//! Crash-safe release audience freezing and cross-project inbox delivery.
//!
//! This is the deterministic half of the processor's separate `release-sync` mode. It never
//! discovers dependents by walking disk: the strictly validated dependency registry is the only
//! audience authority. Initial delivery freezes both content and audience; resume accepts no new
//! content and completes only missing deliveries from that canonical record.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::dependency_graph::{self, DependencyGraphError, ReleaseRegistryProject};
use crate::inbox::{self, InboxError, InboxLock};
use crate::work_fs;

const RELEASE_SCHEMA: &str = "orchestra/release-notification@1";
const MESSAGE_SCHEMA: &str = "orchestra/inbox-message@1";
const MAX_RELEASE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_BODY_BYTES: usize = 262_144;
const NOTES_RECEIPT_SCHEMA: &str = "orchestrail/release-notes-receipt@1";
const MAX_PROTECTED_WORK_ENTRIES: usize = 8_192;
// One directory can also contain the small set of leaf-owned exclusions that are deliberately
// omitted from the global protected-path count. Keep that allowance bounded too.
const MAX_PROTECTED_DIRECTORY_ENTRIES: usize = MAX_PROTECTED_WORK_ENTRIES + 8;
const MAX_PROTECTED_WORK_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseLeafSurface {
    DependencyCurator,
    Notes,
}

#[derive(Debug)]
pub enum ReleaseError {
    Io(io::Error),
    Json(serde_json::Error),
    Inbox(InboxError),
    DependencyGraph(DependencyGraphError),
    Invalid(String),
}

impl fmt::Display for ReleaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "release I/O error: {error}"),
            Self::Json(error) => write!(f, "release JSON error: {error}"),
            Self::Inbox(error) => write!(f, "release inbox error: {error}"),
            Self::DependencyGraph(error) => write!(f, "release dependency graph error: {error}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ReleaseError {}

impl From<io::Error> for ReleaseError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ReleaseError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<InboxError> for ReleaseError {
    fn from(value: InboxError) -> Self {
        Self::Inbox(value)
    }
}

impl From<DependencyGraphError> for ReleaseError {
    fn from(value: DependencyGraphError) -> Self {
        Self::DependencyGraph(value)
    }
}

pub type Result<T> = std::result::Result<T, ReleaseError>;

/// Fingerprint release-protected `.work` state around a semantic leaf. The leaf-owned candidate,
/// notes and evidence directories plus native transcript output are excluded; owner lease lock
/// directories are concurrently maintained by the heartbeat. Everything else is immutable while
/// the separate release mode runs.
pub fn protected_work_fingerprint(
    work: &Path,
    surface: ReleaseLeafSurface,
    release_id: &str,
) -> Result<String> {
    validate_single_line(release_id, 160, false, "release id")?;
    work_fs::require_plain_directory(work)?;
    let mut paths = Vec::new();
    collect_protected_paths(work, work, surface, release_id, &mut paths)?;
    paths.sort();
    if paths.len() > MAX_PROTECTED_WORK_ENTRIES {
        return Err(ReleaseError::Invalid(format!(
            "release control plane exceeds the {MAX_PROTECTED_WORK_ENTRIES}-entry guard limit"
        )));
    }
    let mut digest = Sha256::new();
    digest.update(b"orchestrail/release-protected-work@1\0");
    let mut total = 0_u64;
    for path in paths {
        let relative = path.strip_prefix(work).map_err(|_| {
            ReleaseError::Invalid(format!(
                "release control artifact escapes work root: {}",
                path.display()
            ))
        })?;
        let metadata = std::fs::symlink_metadata(&path)?;
        if work_fs::redirected(&metadata) {
            return Err(ReleaseError::Invalid(format!(
                "release control artifact is redirected: {}",
                path.display()
            )));
        }
        let relative = relative.to_string_lossy();
        digest.update((relative.len() as u64).to_le_bytes());
        digest.update(relative.as_bytes());
        if metadata.is_dir() {
            digest.update(b"d");
        } else if metadata.is_file() {
            digest.update(b"f");
            let remaining = MAX_PROTECTED_WORK_BYTES.saturating_sub(total);
            let bytes = work_fs::read_required_bytes(work, &path, remaining)?;
            total = total.checked_add(bytes.len() as u64).ok_or_else(|| {
                ReleaseError::Invalid("release control-plane byte count overflowed".into())
            })?;
            digest.update((bytes.len() as u64).to_le_bytes());
            digest.update(&bytes);
        } else {
            return Err(ReleaseError::Invalid(format!(
                "release control artifact is not a plain file or directory: {}",
                path.display()
            )));
        }
    }
    Ok(hex_lower(&digest.finalize()))
}

fn collect_protected_paths(
    work: &Path,
    directory: &Path,
    surface: ReleaseLeafSurface,
    release_id: &str,
    paths: &mut Vec<PathBuf>,
) -> Result<()> {
    let entries = protected_directory_entries(work, directory, MAX_PROTECTED_DIRECTORY_ENTRIES)?;
    let mut entries = entries
        .into_iter()
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        let relative = path.strip_prefix(work).map_err(|_| {
            ReleaseError::Invalid(format!(
                "release control artifact escapes work root: {}",
                path.display()
            ))
        })?;
        let first = relative
            .components()
            .next()
            .and_then(|component| match component {
                std::path::Component::Normal(name) => Some(name),
                _ => None,
            });
        let allowed_evidence = match surface {
            ReleaseLeafSurface::DependencyCurator => "dependency-curator-0-post_archive.md".into(),
            ReleaseLeafSurface::Notes => format!("release-notes-{release_id}.md"),
        };
        let leaf_directory = match surface {
            ReleaseLeafSurface::DependencyCurator => "dependency_graph_candidates",
            ReleaseLeafSurface::Notes => "release_notifications",
        };
        let allowed_leaf_path = match surface {
            ReleaseLeafSurface::DependencyCurator => {
                relative
                    == Path::new(leaf_directory)
                        .join(format!("depgraph-{release_id}-post_archive.json"))
            }
            ReleaseLeafSurface::Notes => {
                let notes = format!("{release_id}.md");
                relative == Path::new(leaf_directory).join(&notes)
                    || relative == Path::new(leaf_directory).join(format!("{notes}.receipt.json"))
                    || relative
                        == Path::new(leaf_directory).join(format!("{release_id}.range.json"))
            }
        };
        let excluded = relative == Path::new("native-evidence").join(allowed_evidence)
            || allowed_leaf_path
            || first
                .and_then(|name| name.to_str())
                .is_some_and(|name| matches!(name, "orchestrator.lock" | "state-tx.lock"));
        if excluded {
            continue;
        }
        if paths.len() >= MAX_PROTECTED_WORK_ENTRIES {
            return Err(ReleaseError::Invalid(format!(
                "release control plane exceeds the {MAX_PROTECTED_WORK_ENTRIES}-entry guard limit"
            )));
        }
        let metadata = std::fs::symlink_metadata(&path)?;
        if work_fs::redirected(&metadata) {
            return Err(ReleaseError::Invalid(format!(
                "release control artifact is redirected: {}",
                path.display()
            )));
        }
        if relative == Path::new("native-evidence") || relative == Path::new(leaf_directory) {
            if !metadata.is_dir() {
                return Err(ReleaseError::Invalid(format!(
                    "release leaf-owned root is not a plain directory: {}",
                    path.display()
                )));
            }
            collect_protected_paths(work, &path, surface, release_id, paths)?;
            continue;
        }
        paths.push(path.clone());
        if metadata.is_dir() {
            collect_protected_paths(work, &path, surface, release_id, paths)?;
        }
    }
    Ok(())
}

fn protected_directory_entries(
    work: &Path,
    directory: &Path,
    max_entries: usize,
) -> Result<Vec<std::fs::DirEntry>> {
    if directory == work {
        work_fs::require_plain_directory(work)?;
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(work)? {
            if entries.len() >= max_entries {
                return Err(ReleaseError::Invalid(format!(
                    "release control directory exceeds {max_entries} entries: {}",
                    work.display()
                )));
            }
            entries.push(entry?);
        }
        work_fs::require_plain_directory(work)?;
        Ok(entries)
    } else {
        work_fs::plain_directory_entries_bounded(work, directory, max_entries)?.ok_or_else(|| {
            ReleaseError::Invalid(format!(
                "release control directory disappeared during fingerprinting: {}",
                directory.display()
            ))
        })
    }
}

/// Read an operator- or curator-produced canonical notes file. The file must be an immediate
/// child of `.work/release_notifications`; redirects and oversized files are refused.
pub fn read_canonical_notes(work: &Path, path: &Path) -> Result<String> {
    let directory = work.join("release_notifications");
    let parent = path.parent().ok_or_else(|| {
        ReleaseError::Invalid("release notes path has no parent directory".into())
    })?;
    if parent != directory || path.file_name().is_none() {
        return Err(ReleaseError::Invalid(format!(
            "release notes must be a direct file under {}",
            directory.display()
        )));
    }
    let notes = work_fs::read_required_text(work, path, MAX_BODY_BYTES as u64)?;
    if notes.trim().is_empty() {
        return Err(ReleaseError::Invalid("release notes are empty".into()));
    }
    Ok(notes)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReleaseNotesReceipt {
    schema: String,
    release_id: String,
    version: String,
    tag: String,
    release_revision: String,
    products: Vec<String>,
    release_url: String,
    notes_file: String,
    notes_sha256: String,
}

#[derive(Debug, Clone)]
pub struct ReleaseNotesBinding<'a> {
    pub release_id: &'a str,
    pub version: &'a str,
    pub tag: &'a str,
    pub release_revision: &'a str,
    pub products: &'a [String],
    pub release_url: &'a str,
}

pub fn validate_notes_binding(binding: &ReleaseNotesBinding<'_>) -> Result<()> {
    validate_version(binding.version)?;
    validate_single_line(binding.release_id, 160, false, "release id")?;
    validate_single_line(binding.tag, 240, false, "release tag")?;
    validate_single_line(binding.release_revision, 240, false, "release revision")?;
    validate_single_line(binding.release_url, 2_048, true, "release URL")?;
    if binding.products.len() > 100 {
        return Err(ReleaseError::Invalid(
            "release notes binding has more than 100 products".into(),
        ));
    }
    let mut products = BTreeSet::new();
    for product in binding.products {
        validate_product(product)?;
        if !products.insert(product.to_lowercase()) {
            return Err(ReleaseError::Invalid(format!(
                "release notes binding duplicates product {product:?}"
            )));
        }
    }
    Ok(())
}

/// Canonicalize the optional CLI product selection and prove every selected identity belongs to
/// the refreshed source graph. Product identity comparison follows the legacy registry contract:
/// case-insensitive across the whole `ecosystem:name` key, while the persisted spelling remains
/// the source/first-request spelling after whitespace and ecosystem normalization.
pub fn release_products_for_source(
    source_products: &[String],
    requested_products: &[String],
) -> Result<Vec<String>> {
    let source_keys = source_products
        .iter()
        .map(|product| product.to_lowercase())
        .collect::<BTreeSet<_>>();
    let selected = if requested_products.is_empty() {
        source_products.to_vec()
    } else {
        requested_products
            .iter()
            .map(|product| normalize_product(product))
            .collect::<Result<Vec<_>>>()?
    };
    let mut unique = std::collections::BTreeMap::new();
    for product in selected {
        validate_product(&product)?;
        unique.entry(product.to_lowercase()).or_insert(product);
    }
    if unique.len() > 100 {
        return Err(ReleaseError::Invalid(
            "release has more than 100 products".into(),
        ));
    }
    if let Some(product) = unique
        .iter()
        .find(|(key, _)| !source_keys.contains(*key))
        .map(|(_, product)| product)
    {
        return Err(ReleaseError::Invalid(format!(
            "release product {product:?} is not advertised by source project"
        )));
    }
    Ok(unique.into_values().collect())
}

/// Bind a successfully completed semantic leaf to the exact notes bytes and release inputs.
pub fn record_composed_notes(
    work: &Path,
    notes_path: &Path,
    binding: &ReleaseNotesBinding<'_>,
) -> Result<()> {
    let notes = read_canonical_notes(work, notes_path)?;
    let receipt = expected_notes_receipt(notes_path, binding, &notes)?;
    let receipt_path = notes_receipt_path(work, notes_path)?;
    let text = serde_json::to_string_pretty(&receipt)?;
    work_fs::replace_file(
        work,
        &receipt_path,
        format!("{text}\n").as_bytes(),
        64 * 1024,
    )?;
    Ok(())
}

/// Return true only for the exact completed bytes and inputs. A file without a receipt is an
/// interrupted attempt and may be safely regenerated at the same confined coordinate.
pub fn composed_notes_complete(
    work: &Path,
    notes_path: &Path,
    binding: &ReleaseNotesBinding<'_>,
) -> Result<bool> {
    // Inspect the final entry even when no receipt exists. An interrupted regular file may be
    // regenerated, but a redirect must never be handed to the model as its output coordinate.
    let notes = work_fs::read_optional_text(work, notes_path, MAX_BODY_BYTES as u64)?;
    let receipt_path = notes_receipt_path(work, notes_path)?;
    let Some(text) = work_fs::read_optional_text(work, &receipt_path, 64 * 1024)? else {
        return Ok(false);
    };
    let receipt: ReleaseNotesReceipt = serde_json::from_str(&text)?;
    let notes = notes.ok_or_else(|| {
        ReleaseError::Invalid(format!(
            "release notes receipt exists but its file is absent: {}",
            notes_path.display()
        ))
    })?;
    if notes.trim().is_empty() {
        return Err(ReleaseError::Invalid("release notes are empty".into()));
    }
    let expected = expected_notes_receipt(notes_path, binding, &notes)?;
    if receipt != expected {
        return Err(ReleaseError::Invalid(format!(
            "release notes receipt does not match the current file or release inputs: {}",
            receipt_path.display()
        )));
    }
    Ok(true)
}

fn notes_receipt_path(work: &Path, notes_path: &Path) -> Result<PathBuf> {
    let directory = work.join("release_notifications");
    if notes_path.parent() != Some(directory.as_path()) {
        return Err(ReleaseError::Invalid(format!(
            "release notes must be a direct file under {}",
            directory.display()
        )));
    }
    let name = notes_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ReleaseError::Invalid("release notes filename is not UTF-8".into()))?;
    Ok(directory.join(format!("{name}.receipt.json")))
}

fn expected_notes_receipt(
    notes_path: &Path,
    binding: &ReleaseNotesBinding<'_>,
    notes: &str,
) -> Result<ReleaseNotesReceipt> {
    validate_notes_binding(binding)?;
    let notes_file = notes_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ReleaseError::Invalid("release notes filename is not UTF-8".into()))?;
    Ok(ReleaseNotesReceipt {
        schema: NOTES_RECEIPT_SCHEMA.into(),
        release_id: binding.release_id.into(),
        version: binding.version.into(),
        tag: binding.tag.into(),
        release_revision: binding.release_revision.into(),
        products: binding.products.to_vec(),
        release_url: binding.release_url.into(),
        notes_file: notes_file.into(),
        notes_sha256: hex_lower(&Sha256::digest(notes.as_bytes())),
    })
}

#[derive(Debug, Clone)]
pub struct ReleaseRequest {
    pub source_root: PathBuf,
    pub registry_path: PathBuf,
    pub version: String,
    pub verified_source_revision: String,
    pub content: Option<ReleaseContent>,
    pub occurred_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseContent {
    pub subject: String,
    pub body: String,
    pub products: Option<Vec<String>>,
    pub release_url: String,
    pub source_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeliveryResult {
    pub project_id: String,
    pub name: String,
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeliveryFailure {
    pub project_id: String,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReleaseResult {
    pub release_id: String,
    pub version: String,
    pub target_count: usize,
    pub delivered_count: usize,
    pub failure_count: usize,
    pub deliveries: Vec<DeliveryResult>,
    pub failures: Vec<DeliveryFailure>,
    pub unaudited_projects: Vec<String>,
}

/// Inspect the canonical release coordinate under the source inbox lock. This is a CLI preflight,
/// not a substitute for [`distribute_with_cancellation`]'s transactional recheck.
pub fn canonical_release_present(
    source_root: &Path,
    source_id: &str,
    version: &str,
    verified_source_revision: &str,
) -> Result<bool> {
    Ok(
        canonical_release_fingerprint(source_root, source_id, version, verified_source_revision)?
            .is_some(),
    )
}

/// Content identity for the frozen release authority, used to prove semantic leaves did not
/// create or rewrite that ignored source-inbox record before native delivery begins.
pub fn canonical_release_fingerprint(
    source_root: &Path,
    source_id: &str,
    version: &str,
    verified_source_revision: &str,
) -> Result<Option<String>> {
    validate_version(version)?;
    validate_single_line(
        verified_source_revision,
        240,
        false,
        "verified release source revision",
    )?;
    let release_id = stable_release_id(source_id, version);
    let inbox_paths = inbox::paths(source_root)?.ok_or_else(|| {
        ReleaseError::Invalid("source project has no initialized .inbox/messages directory".into())
    })?;
    let inbox_root = inbox_paths
        .messages
        .parent()
        .ok_or_else(|| ReleaseError::Invalid("source inbox has no parent".into()))?;
    let release_path = inbox_root
        .join("releases")
        .join(format!("{release_id}.json"));
    let _lock = InboxLock::acquire(&inbox_paths.lock)?;
    let Some(record) = read_record(inbox_root, &release_path)? else {
        return Ok(None);
    };
    validate_record(&record, &release_id, source_id)?;
    if record.source_revision != verified_source_revision {
        return Err(ReleaseError::Invalid(format!(
            "frozen release source revision {} differs from currently verified tag revision {}",
            record.source_revision, verified_source_revision
        )));
    }
    let bytes = serde_json::to_vec(&record)?;
    Ok(Some(hex_lower(&Sha256::digest(bytes))))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Endpoint {
    id: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Delivery {
    project_id: String,
    message_id: String,
    delivered_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SkippedTarget {
    project_id: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    skipped_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddedTarget {
    project_id: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    added_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReleaseRecord {
    schema: String,
    id: String,
    source_project: Endpoint,
    version: String,
    subject: String,
    body: String,
    products: Vec<String>,
    release_url: String,
    source_revision: String,
    target_project_ids: Vec<String>,
    deliveries: Vec<Delivery>,
    #[serde(default)]
    skipped_targets: Vec<SkippedTarget>,
    #[serde(default)]
    added_targets: Vec<AddedTarget>,
    #[serde(default)]
    unaudited_projects: Vec<String>,
    created_at: String,
    updated_at: String,
}

/// Freeze or resume one release and idempotently deliver its remaining messages.
pub fn distribute(request: &ReleaseRequest) -> Result<ReleaseResult> {
    distribute_with_cancellation(request, || false)
}

pub fn distribute_with_cancellation(
    request: &ReleaseRequest,
    cancelled: impl Fn() -> bool,
) -> Result<ReleaseResult> {
    if cancelled() {
        return Err(ReleaseError::Invalid(
            "release delivery did not start because owner lease authority was lost".into(),
        ));
    }
    validate_version(&request.version)?;
    validate_single_line(
        &request.verified_source_revision,
        240,
        false,
        "verified release source revision",
    )?;
    if !crate::time::is_iso_utc(&request.occurred_at) {
        return Err(ReleaseError::Invalid(
            "release timestamp is not an ISO-8601 UTC instant".into(),
        ));
    }
    let (projects, source_id) = dependency_graph::release_registry_projects_for_root(
        &request.registry_path,
        &request.source_root,
    )?;
    let source = projects
        .iter()
        .find(|project| project.id == source_id)
        .ok_or_else(|| {
            ReleaseError::Invalid(format!(
                "release source disappeared from its validated registry snapshot: {source_id}"
            ))
        })?;
    let release_id = stable_release_id(&source.id, &request.version);
    let inbox_paths = inbox::paths(&request.source_root)?.ok_or_else(|| {
        ReleaseError::Invalid("source project has no initialized .inbox/messages directory".into())
    })?;
    let inbox_root = inbox_paths
        .messages
        .parent()
        .ok_or_else(|| ReleaseError::Invalid("source inbox has no parent".into()))?;
    let release_path = inbox_root
        .join("releases")
        .join(format!("{release_id}.json"));

    let mut record = {
        let _lock = InboxLock::acquire(&inbox_paths.lock)?;
        if cancelled() {
            return Err(ReleaseError::Invalid(
                "release delivery lost owner authority while waiting for the source inbox lock"
                    .into(),
            ));
        }
        match read_record(inbox_root, &release_path)? {
            Some(existing) => {
                validate_record(&existing, &release_id, &source.id)?;
                if existing.source_revision != request.verified_source_revision {
                    return Err(ReleaseError::Invalid(format!(
                        "frozen release source revision {} differs from currently verified tag revision {}",
                        existing.source_revision, request.verified_source_revision
                    )));
                }
                if let Some(content) = &request.content {
                    let expected = normalized_content(content, source)?;
                    if existing.subject != expected.subject
                        || existing.body != expected.body
                        || existing.products != expected.products
                        || existing.release_url != expected.release_url
                        || existing.source_revision != expected.source_revision
                    {
                        return Err(ReleaseError::Invalid(format!(
                            "release {} already has canonical content; retry without new content",
                            request.version
                        )));
                    }
                }
                existing
            }
            None => {
                let content = request.content.as_ref().ok_or_else(|| {
                    ReleaseError::Invalid(format!(
                        "cannot resume release {} because no canonical record exists",
                        request.version
                    ))
                })?;
                let content = normalized_content(content, source)?;
                if content.source_revision != request.verified_source_revision {
                    return Err(ReleaseError::Invalid(
                        "release content source revision differs from the verified tag revision"
                            .into(),
                    ));
                }
                let targets = select_dependents(&projects, &source.id, &content.products);
                let unaudited_projects = projects
                    .iter()
                    .filter(|project| project.id != source.id && project.graph_generation == 0)
                    .map(|project| project.id.clone())
                    .collect();
                let record = ReleaseRecord {
                    schema: RELEASE_SCHEMA.into(),
                    id: release_id.clone(),
                    source_project: Endpoint {
                        id: source.id.clone(),
                        name: source.name.clone(),
                    },
                    version: request.version.clone(),
                    subject: content.subject,
                    body: content.body,
                    products: content.products,
                    release_url: content.release_url,
                    source_revision: content.source_revision,
                    target_project_ids: targets,
                    deliveries: Vec::new(),
                    skipped_targets: Vec::new(),
                    added_targets: Vec::new(),
                    unaudited_projects,
                    created_at: request.occurred_at.clone(),
                    updated_at: request.occurred_at.clone(),
                };
                if cancelled() {
                    return Err(ReleaseError::Invalid(
                        "release delivery lost owner authority before freezing its canonical record"
                            .into(),
                    ));
                }
                write_record(inbox_root, &release_path, &record)?;
                record
            }
        }
    };

    let mut deliveries = Vec::new();
    let mut failures = Vec::new();
    for target_id in record.target_project_ids.clone() {
        if cancelled() {
            return Err(ReleaseError::Invalid(
                "release delivery stopped because owner lease authority was lost".into(),
            ));
        }
        if record
            .skipped_targets
            .iter()
            .any(|skip| skip.project_id == target_id)
        {
            continue;
        }
        if let Some(delivery) = record
            .deliveries
            .iter()
            .find(|delivery| delivery.project_id == target_id)
        {
            let name = projects
                .iter()
                .find(|project| project.id == target_id)
                .map(|project| project.name.clone())
                .unwrap_or_default();
            deliveries.push(DeliveryResult {
                project_id: target_id,
                name,
                message_id: delivery.message_id.clone(),
            });
            continue;
        }
        let delivery = deliver_one(
            &record,
            &target_id,
            &projects,
            &request.occurred_at,
            &cancelled,
        );
        match delivery {
            Ok(delivery) => {
                if cancelled() {
                    // The target message is intentionally left for idempotent recovery. Do not
                    // mutate the source record after this process has lost owner authority.
                    return Err(ReleaseError::Invalid(
                        "release delivery stopped after target write because owner lease authority was lost"
                            .into(),
                    ));
                }
                let _lock = InboxLock::acquire(&inbox_paths.lock)?;
                if cancelled() {
                    return Err(ReleaseError::Invalid(
                        "release delivery lost owner authority while waiting to record delivery"
                            .into(),
                    ));
                }
                let mut current = read_record(inbox_root, &release_path)?.ok_or_else(|| {
                    ReleaseError::Invalid(
                        "canonical release record disappeared during delivery".into(),
                    )
                })?;
                validate_record(&current, &release_id, &source.id)?;
                validate_frozen_record_unchanged(&record, &current)?;
                if !current
                    .deliveries
                    .iter()
                    .any(|item| item.project_id == delivery.project_id)
                {
                    current.deliveries.push(Delivery {
                        project_id: delivery.project_id.clone(),
                        message_id: delivery.message_id.clone(),
                        delivered_at: request.occurred_at.clone(),
                    });
                    current
                        .deliveries
                        .sort_by(|left, right| left.project_id.cmp(&right.project_id));
                    current.updated_at = request.occurred_at.clone();
                    if cancelled() {
                        return Err(ReleaseError::Invalid(
                            "release delivery lost owner authority before recording its acknowledgement"
                                .into(),
                        ));
                    }
                    write_record(inbox_root, &release_path, &current)?;
                }
                record = current;
                deliveries.push(delivery);
            }
            Err(error) => failures.push(DeliveryFailure {
                project_id: target_id,
                error: error.to_string(),
            }),
        }
    }
    deliveries.sort_by(|left, right| left.project_id.cmp(&right.project_id));
    failures.sort_by(|left, right| left.project_id.cmp(&right.project_id));
    Ok(ReleaseResult {
        release_id,
        version: request.version.clone(),
        target_count: record.target_project_ids.len(),
        delivered_count: deliveries.len(),
        failure_count: failures.len(),
        deliveries,
        failures,
        unaudited_projects: record.unaudited_projects,
    })
}

fn deliver_one(
    record: &ReleaseRecord,
    target_id: &str,
    projects: &[ReleaseRegistryProject],
    occurred_at: &str,
    cancelled: &dyn Fn() -> bool,
) -> Result<DeliveryResult> {
    let target = projects
        .iter()
        .find(|project| project.id == target_id)
        .ok_or_else(|| {
            ReleaseError::Invalid(format!(
                "frozen dependent {target_id} is no longer registered"
            ))
        })?;
    let dedupe_key = format!("release:{}", record.id);
    let message_id = stable_send_id(&record.source_project.id, &target.id, &dedupe_key);
    let message = serde_json::json!({
        "schema": MESSAGE_SCHEMA,
        "id": message_id,
        "from_project": record.source_project,
        "to_project": { "id": target.id, "name": target.name },
        "created_at": occurred_at,
        "updated_at": occurred_at,
        "subject": record.subject,
        "body": record.body,
        "message_type": "release",
        "release": {
            "id": record.id,
            "version": record.version,
            "products": record.products,
            "release_url": record.release_url,
            "source_revision": record.source_revision
        },
        "in_reply_to": Value::Null,
        "conversation_id": message_id,
        "dedupe_key": dedupe_key,
        "processing_status": "new",
        "reply_status": "none",
        "queue_tasks": [],
        "remarks": [],
        "reply_ids": []
    });
    inbox::deliver_release_message(&target.root, &message_id, &message, cancelled)?;
    Ok(DeliveryResult {
        project_id: target.id.clone(),
        name: target.name.clone(),
        message_id,
    })
}

fn normalized_content(
    content: &ReleaseContent,
    source: &ReleaseRegistryProject,
) -> Result<NormalizedContent> {
    validate_single_line(&content.subject, 240, false, "release subject")?;
    if content.body.trim().is_empty()
        || content.body.len() > MAX_BODY_BYTES
        || content.body.chars().any(|character| character == '\0')
    {
        return Err(ReleaseError::Invalid(
            "release notes are empty or oversized".into(),
        ));
    }
    validate_single_line(&content.release_url, 2_048, true, "release URL")?;
    validate_single_line(
        &content.source_revision,
        240,
        true,
        "release source revision",
    )?;
    let products = release_products_for_source(
        &source.products,
        content.products.as_deref().unwrap_or_default(),
    )?;
    Ok(NormalizedContent {
        subject: content.subject.clone(),
        body: content.body.clone(),
        products,
        release_url: content.release_url.clone(),
        source_revision: content.source_revision.clone(),
    })
}

struct NormalizedContent {
    subject: String,
    body: String,
    products: Vec<String>,
    release_url: String,
    source_revision: String,
}

fn select_dependents(
    projects: &[ReleaseRegistryProject],
    source_id: &str,
    products: &[String],
) -> Vec<String> {
    projects
        .iter()
        .filter(|project| {
            project.dependencies.iter().any(|dependency| {
                dependency.upstream_id == source_id
                    && (products.is_empty()
                        || dependency.products.is_empty()
                        || dependency.products.iter().any(|product| {
                            let product = product.to_lowercase();
                            products
                                .iter()
                                .any(|released| released.to_lowercase() == product)
                        }))
            })
        })
        .map(|project| project.id.clone())
        .collect()
}

fn read_record(inbox_root: &Path, path: &Path) -> Result<Option<ReleaseRecord>> {
    let Some(text) = work_fs::read_optional_text(inbox_root, path, MAX_RELEASE_BYTES)? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_str(&text)?))
}

fn write_record(inbox_root: &Path, path: &Path, record: &ReleaseRecord) -> Result<()> {
    let text = serde_json::to_string_pretty(record)?;
    work_fs::replace_file(
        inbox_root,
        path,
        format!("{text}\n").as_bytes(),
        MAX_RELEASE_BYTES,
    )?;
    Ok(())
}

fn validate_record(record: &ReleaseRecord, id: &str, source_id: &str) -> Result<()> {
    if record.schema != RELEASE_SCHEMA
        || record.id != id
        || record.source_project.id != source_id
        || record.id != stable_release_id(source_id, &record.version)
    {
        return Err(ReleaseError::Invalid(format!(
            "canonical release record {id} has an invalid identity"
        )));
    }
    validate_version(&record.version)?;
    validate_single_line(&record.subject, 240, false, "release subject")?;
    validate_single_line(
        &record.source_project.name,
        120,
        false,
        "release source project name",
    )?;
    validate_single_line(&record.release_url, 2_048, true, "release URL")?;
    validate_single_line(
        &record.source_revision,
        240,
        true,
        "release source revision",
    )?;
    if !crate::time::is_iso_utc(&record.created_at) || !crate::time::is_iso_utc(&record.updated_at)
    {
        return Err(ReleaseError::Invalid(format!(
            "canonical release record {id} has invalid audit timestamps"
        )));
    }
    if record.body.trim().is_empty()
        || record.body.len() > MAX_BODY_BYTES
        || record.body.contains('\0')
    {
        return Err(ReleaseError::Invalid(format!(
            "canonical release record {id} has invalid notes"
        )));
    }
    if record.products.len() > 100 {
        return Err(ReleaseError::Invalid(format!(
            "canonical release record {id} has too many products"
        )));
    }
    let mut products = BTreeSet::new();
    for product in &record.products {
        validate_product(product)?;
        if !products.insert(product.to_lowercase()) {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} duplicates product {product:?}"
            )));
        }
    }
    let mut targets = BTreeSet::new();
    for target in &record.target_project_ids {
        if !valid_project_id(target) || !targets.insert(target) || target == source_id {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has an invalid or duplicate target"
            )));
        }
    }
    let mut delivered = BTreeSet::new();
    for delivery in &record.deliveries {
        if !crate::time::is_iso_utc(&delivery.delivered_at) {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has an invalid delivery timestamp"
            )));
        }
        let expected = stable_send_id(source_id, &delivery.project_id, &format!("release:{id}"));
        if !targets.contains(&delivery.project_id)
            || !delivered.insert(&delivery.project_id)
            || delivery.message_id != expected
        {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has invalid deliveries"
            )));
        }
    }
    let mut skipped = BTreeSet::new();
    for skip in &record.skipped_targets {
        if !targets.contains(&skip.project_id)
            || delivered.contains(&skip.project_id)
            || !skipped.insert(&skip.project_id)
        {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has invalid skipped targets"
            )));
        }
        validate_single_line(&skip.reason, 1_024, false, "release skip reason")?;
        if !crate::time::is_iso_utc(&skip.skipped_at) {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has an invalid skip timestamp"
            )));
        }
    }
    let mut added = BTreeSet::new();
    for addition in &record.added_targets {
        if !targets.contains(&addition.project_id) || !added.insert(&addition.project_id) {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has invalid added targets"
            )));
        }
        validate_single_line(&addition.reason, 1_024, false, "release add reason")?;
        if !crate::time::is_iso_utc(&addition.added_at) {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has an invalid add timestamp"
            )));
        }
    }
    let mut unaudited = BTreeSet::new();
    for project in &record.unaudited_projects {
        if !valid_project_id(project) || project == source_id || !unaudited.insert(project) {
            return Err(ReleaseError::Invalid(format!(
                "canonical release record {id} has invalid unaudited projects"
            )));
        }
    }
    Ok(())
}

fn validate_frozen_record_unchanged(
    expected: &ReleaseRecord,
    observed: &ReleaseRecord,
) -> Result<()> {
    if expected.schema != observed.schema
        || expected.id != observed.id
        || expected.source_project != observed.source_project
        || expected.version != observed.version
        || expected.subject != observed.subject
        || expected.body != observed.body
        || expected.products != observed.products
        || expected.release_url != observed.release_url
        || expected.source_revision != observed.source_revision
        || expected.target_project_ids != observed.target_project_ids
        || expected.skipped_targets != observed.skipped_targets
        || expected.added_targets != observed.added_targets
        || expected.unaudited_projects != observed.unaudited_projects
        || expected.created_at != observed.created_at
    {
        return Err(ReleaseError::Invalid(format!(
            "canonical release record {} changed while delivery was in progress",
            expected.id
        )));
    }
    Ok(())
}

fn valid_project_id(value: &str) -> bool {
    value.len() == 25
        && value.starts_with("repo-")
        && value[5..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_product(value: &str) -> Result<()> {
    let Some((ecosystem, name)) = value.split_once(':') else {
        return Err(ReleaseError::Invalid(format!(
            "release product must use ecosystem:name: {value:?}"
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
        return Err(ReleaseError::Invalid(format!(
            "release product is invalid: {value:?}"
        )));
    }
    Ok(())
}

fn normalize_product(value: &str) -> Result<String> {
    let value = value.trim();
    let Some((ecosystem, name)) = value.split_once(':') else {
        return Err(ReleaseError::Invalid(format!(
            "release product must use ecosystem:name: {value:?}"
        )));
    };
    let normalized = format!("{}:{}", ecosystem.to_ascii_lowercase(), name.trim());
    validate_product(&normalized)?;
    Ok(normalized)
}

fn validate_version(value: &str) -> Result<()> {
    validate_single_line(value, 120, false, "release version")?;
    if value != value.trim() {
        return Err(ReleaseError::Invalid(
            "release version must be canonical trimmed text".into(),
        ));
    }
    Ok(())
}

pub fn validate_release_version(value: &str) -> Result<()> {
    validate_version(value)
}

fn validate_single_line(value: &str, maximum: usize, allow_empty: bool, label: &str) -> Result<()> {
    if (!allow_empty && value.trim().is_empty())
        || value.encode_utf16().count() > maximum
        || value.chars().any(char::is_control)
    {
        return Err(ReleaseError::Invalid(format!("{label} is invalid")));
    }
    Ok(())
}

pub fn stable_release_id(source_id: &str, version: &str) -> String {
    stable_id("rel", &format!("{source_id}|{version}"))
}

fn stable_send_id(source_id: &str, target_id: &str, dedupe_key: &str) -> String {
    stable_id("msg-send", &format!("{source_id}|{target_id}|{dedupe_key}"))
}

fn stable_id(prefix: &str, input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    let encoded = hex_lower(&digest[..16]);
    format!("{prefix}-{encoded}")
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn temp(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "orchestrail-release-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        crate::dependency_graph::canonical_project_root(&path).unwrap()
    }

    #[test]
    fn stable_ids_match_legacy_shape() {
        assert_eq!(
            stable_release_id("repo-12345678901234567890", "1.2.3").len(),
            36
        );
        assert!(stable_release_id("repo-12345678901234567890", "1.2.3").starts_with("rel-"));
        assert_eq!(
            stable_send_id("repo-a", "repo-b", "release:rel-a").len(),
            41
        );
    }

    #[test]
    fn protected_work_guard_allows_only_the_selected_leaf_surface() {
        let work = temp("protected-work");
        fs::create_dir_all(&work).unwrap();
        fs::write(work.join("Tasks_Queue.md"), "queue\n").unwrap();
        let release_id = "rel-0123456789abcdef0123456789abcdef";
        let dependency_before =
            protected_work_fingerprint(&work, ReleaseLeafSurface::DependencyCurator, release_id)
                .unwrap();
        fs::create_dir_all(work.join("dependency_graph_candidates")).unwrap();
        fs::write(
            work.join(format!(
                "dependency_graph_candidates/depgraph-{release_id}-post_archive.json"
            )),
            "candidate\n",
        )
        .unwrap();
        fs::create_dir_all(work.join("native-evidence")).unwrap();
        fs::write(
            work.join("native-evidence/dependency-curator-0-post_archive.md"),
            "report\n",
        )
        .unwrap();
        assert_eq!(
            protected_work_fingerprint(&work, ReleaseLeafSurface::DependencyCurator, release_id)
                .unwrap(),
            dependency_before
        );

        let notes_before =
            protected_work_fingerprint(&work, ReleaseLeafSurface::Notes, release_id).unwrap();
        fs::create_dir_all(work.join("release_notifications")).unwrap();
        fs::write(
            work.join(format!("release_notifications/{release_id}.md")),
            "notes\n",
        )
        .unwrap();
        fs::write(
            work.join(format!(
                "release_notifications/{release_id}.md.receipt.json"
            )),
            "receipt\n",
        )
        .unwrap();
        fs::write(
            work.join(format!("release_notifications/{release_id}.range.json")),
            "range\n",
        )
        .unwrap();
        fs::write(
            work.join(format!("native-evidence/release-notes-{release_id}.md")),
            "report\n",
        )
        .unwrap();
        assert_eq!(
            protected_work_fingerprint(&work, ReleaseLeafSurface::Notes, release_id).unwrap(),
            notes_before
        );
        assert_ne!(
            protected_work_fingerprint(&work, ReleaseLeafSurface::DependencyCurator, release_id)
                .unwrap(),
            dependency_before
        );

        let notes_before_foreign_release =
            protected_work_fingerprint(&work, ReleaseLeafSurface::Notes, release_id).unwrap();
        fs::write(
            work.join("release_notifications/rel-foreign.md"),
            "foreign\n",
        )
        .unwrap();
        assert_ne!(
            protected_work_fingerprint(&work, ReleaseLeafSurface::Notes, release_id).unwrap(),
            notes_before_foreign_release
        );

        let dependency_before_foreign_candidate =
            protected_work_fingerprint(&work, ReleaseLeafSurface::DependencyCurator, release_id)
                .unwrap();
        fs::write(
            work.join("dependency_graph_candidates/depgraph-foreign-post_archive.json"),
            "foreign\n",
        )
        .unwrap();
        assert_ne!(
            protected_work_fingerprint(&work, ReleaseLeafSurface::DependencyCurator, release_id)
                .unwrap(),
            dependency_before_foreign_candidate
        );

        let notes_before_queue_change =
            protected_work_fingerprint(&work, ReleaseLeafSurface::Notes, release_id).unwrap();
        fs::write(work.join("Tasks_Queue.md"), "tampered\n").unwrap();
        assert_ne!(
            protected_work_fingerprint(&work, ReleaseLeafSurface::Notes, release_id).unwrap(),
            notes_before_queue_change
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn protected_work_directory_enumeration_fails_loudly_before_unbounded_collection() {
        let work = temp("bounded-protected-directory");
        fs::write(work.join("one"), "1\n").unwrap();
        fs::write(work.join("two"), "2\n").unwrap();

        let error = protected_directory_entries(&work, &work, 1).unwrap_err();
        assert!(matches!(error, ReleaseError::Invalid(_)));
        assert!(error.to_string().contains("exceeds 1 entries"));
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn composed_notes_require_an_exact_content_bound_receipt() {
        let work = temp("notes-receipt");
        let directory = work.join("release_notifications");
        fs::create_dir_all(&directory).unwrap();
        let notes = directory.join("rel-test.md");
        fs::write(&notes, "# Release 1.2.3\n").unwrap();
        let products = vec!["cargo:source".into()];
        let binding = ReleaseNotesBinding {
            release_id: "rel-0123456789abcdef0123456789abcdef",
            version: "1.2.3",
            tag: "v1.2.3",
            release_revision: "def456",
            products: &products,
            release_url: "https://example.invalid/1.2.3",
        };
        assert!(!composed_notes_complete(&work, &notes, &binding).unwrap());
        record_composed_notes(&work, &notes, &binding).unwrap();
        assert!(composed_notes_complete(&work, &notes, &binding).unwrap());
        fs::write(&notes, "# Tampered\n").unwrap();
        assert!(composed_notes_complete(&work, &notes, &binding).is_err());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn notes_binding_rejects_an_oversized_product_set() {
        let products = (0..101)
            .map(|index| format!("cargo:product-{index}"))
            .collect::<Vec<_>>();
        let binding = ReleaseNotesBinding {
            release_id: "rel-0123456789abcdef0123456789abcdef",
            version: "1.2.3",
            tag: "v1.2.3",
            release_revision: "def456",
            products: &products,
            release_url: "",
        };
        assert!(matches!(
            validate_notes_binding(&binding),
            Err(ReleaseError::Invalid(message)) if message.contains("more than 100 products")
        ));
    }

    #[test]
    fn requested_products_are_normalized_and_owned_case_insensitively() {
        let source = vec!["cargo:ProcessKit".into(), "npm:@scope/pkg".into()];
        assert_eq!(
            release_products_for_source(
                &source,
                &[" Cargo:processkit ".into(), "NPM:@SCOPE/PKG".into()]
            )
            .unwrap(),
            ["cargo:processkit", "npm:@SCOPE/PKG"]
        );
        assert!(release_products_for_source(&source, &["cargo:other".into()]).is_err());
        assert!(release_products_for_source(&source, &[".cargo:item".into()]).is_err());
        assert!(release_products_for_source(&source, &["cargo:   ".into()]).is_err());
    }

    #[test]
    fn selection_uses_only_direct_matching_edges() {
        let projects = vec![ReleaseRegistryProject {
            id: "target".into(),
            name: "Target".into(),
            root: PathBuf::from("target"),
            products: vec![],
            dependencies: vec![crate::dependency_graph::GraphDependency {
                upstream_id: "source".into(),
                products: vec!["cargo:wanted".into()],
                evidence: vec![],
            }],
            graph_generation: 1,
        }];
        assert_eq!(
            select_dependents(&projects, "source", &["cargo:wanted".into()]),
            ["target"]
        );
        assert_eq!(
            select_dependents(&projects, "source", &["CARGO:WANTED".into()]),
            ["target"]
        );
        assert!(select_dependents(&projects, "source", &["cargo:other".into()]).is_empty());
    }

    #[test]
    fn resume_requires_an_existing_record() {
        let root = temp("missing");
        fs::create_dir_all(root.join(".inbox/messages")).unwrap();
        let result = distribute(&ReleaseRequest {
            source_root: root.clone(),
            registry_path: root.join("missing-registry.json"),
            version: "1.0.0".into(),
            verified_source_revision: "abc123".into(),
            content: None,
            occurred_at: "2026-07-26T00:00:00Z".into(),
        });
        assert!(result.is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivery_freezes_audience_and_recovers_after_target_write() {
        let root = temp("delivery");
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir_all(source.join(".inbox/messages")).unwrap();
        fs::create_dir_all(target.join(".inbox/messages")).unwrap();
        let source_id = crate::dependency_graph::project_id(&source);
        let target_id = crate::dependency_graph::project_id(&target);
        let registry = root.join("projects.json");
        fs::write(
            &registry,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema": crate::dependency_graph::REGISTRY_SCHEMA,
                "generation": 1,
                "projects": [
                    {
                        "id": source_id,
                        "name": "Source",
                        "root": source,
                        "products": ["cargo:source"],
                        "dependencies": [],
                        "graph_generation": 2
                    },
                    {
                        "id": target_id,
                        "name": "Target",
                        "root": target,
                        "products": [],
                        "dependencies": [{
                            "upstream_id": source_id,
                            "products": ["cargo:source"],
                            "evidence": ["Cargo.toml"]
                        }],
                        "graph_generation": 1
                    }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let request = ReleaseRequest {
            source_root: source.clone(),
            registry_path: registry,
            version: "1.2.3".into(),
            verified_source_revision: "abc123".into(),
            content: Some(ReleaseContent {
                subject: "Release Source 1.2.3".into(),
                body: "Useful changes\n".into(),
                products: Some(vec!["cargo:source".into()]),
                release_url: "https://example.invalid/releases/1.2.3".into(),
                source_revision: "abc123".into(),
            }),
            occurred_at: "2026-07-26T00:00:00Z".into(),
        };
        let first = distribute(&request).unwrap();
        assert_eq!(first.target_count, 1);
        assert_eq!(first.delivered_count, 1);
        assert!(canonical_release_present(&source, &source_id, "1.2.3", "abc123").unwrap());
        assert!(canonical_release_present(&source, &source_id, "1.2.3", "moved").is_err());
        let message = target
            .join(".inbox/messages")
            .join(format!("{}.json", first.deliveries[0].message_id));
        assert!(message.is_file());
        let mut moved_tag = request.clone();
        moved_tag.content = None;
        moved_tag.verified_source_revision = "different-tag-revision".into();
        assert!(distribute(&moved_tag).is_err());

        // Simulate a crash after the target write but before source-side acknowledgement.
        let record_path = source
            .join(".inbox/releases")
            .join(format!("{}.json", first.release_id));
        let mut record: ReleaseRecord =
            serde_json::from_str(&fs::read_to_string(&record_path).unwrap()).unwrap();
        record.deliveries.clear();
        fs::write(&record_path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
        let resume_request = ReleaseRequest {
            content: None,
            occurred_at: "2026-07-26T00:01:00Z".into(),
            ..request
        };
        let checks = Cell::new(0);
        assert!(
            distribute_with_cancellation(&resume_request, || {
                let current = checks.get();
                checks.set(current + 1);
                current == 4
            })
            .is_err()
        );
        let interrupted: ReleaseRecord =
            serde_json::from_str(&fs::read_to_string(&record_path).unwrap()).unwrap();
        assert!(interrupted.deliveries.is_empty());
        let resumed = distribute(&resume_request).unwrap();
        assert_eq!(resumed.delivered_count, 1);
        assert_eq!(resumed.failure_count, 0);
        let restored: ReleaseRecord =
            serde_json::from_str(&fs::read_to_string(record_path).unwrap()).unwrap();
        assert_eq!(restored.deliveries.len(), 1);
        let _ = fs::remove_dir_all(root);
    }
}
