//! Native, narrowly-owned synchronization of the cross-project dependency graph.
//!
//! The graph is deliberately not a lockfile or an installation mechanism.  A curator may derive
//! a candidate only from a committed repository tip, but this module is the sole code allowed to
//! validate its CAS coordinate and replace the current project's graph slice in the interoperable
//! user registry.  It never discovers arbitrary neighbouring repositories or changes another
//! project's record.

use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::time::is_iso_utc;
use crate::work_fs;

pub const REGISTRY_SCHEMA: &str = "orchestra/project-registry@1";
pub const SNAPSHOT_SCHEMA: &str = "orchestra/project-graph-snapshot@1";
const MAX_PRODUCTS: usize = 100;
const MAX_DEPENDENCIES: usize = 100;
const MAX_EVIDENCE: usize = 100;
const MAX_REGISTRY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CANDIDATE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefreshBoundary {
    CohortOpen,
    PostArchive,
}

impl RefreshBoundary {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CohortOpen => "cohort-open",
            Self::PostArchive => "post-archive",
        }
    }
}

/// A read-only, deliberately compact view supplied to the dependency curator.  It exposes only
/// registered identities and advertised products; roots are included only for the registered
/// projects that may legitimately be inspected to disambiguate a manifest dependency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyGraphRequest {
    pub registry_path: PathBuf,
    pub current_project_id: String,
    pub base_graph_generation: u64,
    pub committed_base: String,
    pub boundary: RefreshBoundary,
    pub candidate_path: PathBuf,
    // An external curator receives `candidate_path` but cannot manufacture a request that asks
    // native sync to consume a similarly named file elsewhere on disk.
    candidate_directory: PathBuf,
    pub projects: Vec<RegisteredProject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredProject {
    pub id: String,
    pub name: String,
    pub root: PathBuf,
    pub products: Vec<String>,
    pub graph_generation: u64,
}

/// Complete read-only registry projection used to freeze a release audience. Unlike
/// [`RegisteredProject`], this includes the already-validated direct dependency edges, but it
/// grants no registry write authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseRegistryProject {
    pub id: String,
    pub name: String,
    pub root: PathBuf,
    pub products: Vec<String>,
    pub dependencies: Vec<GraphDependency>,
    pub graph_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDependency {
    pub upstream_id: String,
    pub products: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphSnapshot {
    pub base_graph_generation: u64,
    pub products: Vec<String>,
    pub dependencies: Vec<GraphDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphSyncResult {
    pub changed: bool,
    pub project_id: String,
    pub products: Vec<String>,
    pub dependencies: Vec<GraphDependency>,
}

#[derive(Debug)]
pub enum DependencyGraphError {
    Io(io::Error),
    Json(serde_json::Error),
    Invalid(String),
    NotRegistered { root: PathBuf },
    Busy { lock: PathBuf },
    StaleGeneration { expected: u64, actual: u64 },
}

impl fmt::Display for DependencyGraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "dependency graph I/O error: {error}"),
            Self::Json(error) => write!(f, "dependency graph JSON error: {error}"),
            Self::Invalid(message) => f.write_str(message),
            Self::NotRegistered { root } => write!(
                f,
                "project root is not registered for dependency-graph refresh: {}",
                root.display()
            ),
            Self::Busy { lock } => write!(
                f,
                "dependency registry is busy; retained candidate for a later refresh: {}",
                lock.display()
            ),
            Self::StaleGeneration { expected, actual } => write!(
                f,
                "dependency graph candidate generation is stale (expected {expected}, current {actual})"
            ),
        }
    }
}

impl std::error::Error for DependencyGraphError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Invalid(_)
            | Self::NotRegistered { .. }
            | Self::Busy { .. }
            | Self::StaleGeneration { .. } => None,
        }
    }
}

impl From<io::Error> for DependencyGraphError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for DependencyGraphError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub type Result<T> = std::result::Result<T, DependencyGraphError>;

/// Resolve the shared legacy-compatible registry coordinate. An explicit environment override is
/// useful for isolated operators and tests; it is intentionally a path, not a command to invoke.
pub fn default_registry_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("ORCHESTRA_REGISTRY_PATH").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    let home = env::var_os("USERPROFILE")
        .or_else(|| env::var_os("HOME"))
        .ok_or_else(|| {
            DependencyGraphError::Invalid(
                "cannot determine user profile for dependency registry".into(),
            )
        })?;
    Ok(PathBuf::from(home).join(".orchestra").join("projects.json"))
}

/// Resolve a live local root through the registry without granting any graph-write authority.
/// Inbox delivery uses this narrow lookup to prove that the current repository is the registered
/// recipient named by a message before it writes either side of a cross-project conversation.
pub fn registered_project_for_root(registry_path: &Path, root: &Path) -> Result<RegisteredProject> {
    let root = canonical_root(root)?;
    let registry = read_registry(registry_path)?;
    registry
        .projects
        .iter()
        .find(|project| same_root(&project.root, &root))
        .map(RegisteredProject::from)
        .ok_or(DependencyGraphError::NotRegistered { root })
}

/// Resolve one registered endpoint by its stable identity.  The caller must separately verify
/// that the returned root is currently live and owns the target resource it intends to touch.
/// Keeping moved registrations visible mirrors the shared registry contract and avoids scanning
/// neighbouring directories based on untrusted inbox content.
pub fn registered_project_by_id(registry_path: &Path, id: &str) -> Result<RegisteredProject> {
    if !valid_project_id(id) {
        return Err(DependencyGraphError::Invalid(format!(
            "invalid registered project id {id:?}"
        )));
    }
    let registry = read_registry(registry_path)?;
    registry
        .projects
        .iter()
        .find(|project| project.id == id)
        .map(RegisteredProject::from)
        .ok_or_else(|| {
            DependencyGraphError::Invalid(format!("registered project {id} was not found"))
        })
}

/// Read one strictly validated, stable-order snapshot for release audience selection.
pub fn release_registry_projects_for_root(
    registry_path: &Path,
    root: &Path,
) -> Result<(Vec<ReleaseRegistryProject>, String)> {
    let root = canonical_root(root)?;
    let registry = read_registry(registry_path)?;
    let source_id = registry
        .projects
        .iter()
        .find(|project| same_root(&project.root, &root))
        .map(|project| project.id.clone())
        .ok_or(DependencyGraphError::NotRegistered { root })?;
    let mut projects = registry
        .projects
        .iter()
        .map(|project| ReleaseRegistryProject {
            id: project.id.clone(),
            name: project.name.clone(),
            root: project.root.clone(),
            products: project.products.clone(),
            dependencies: project.dependencies.clone(),
            graph_generation: project.graph_generation,
        })
        .collect::<Vec<_>>();
    projects.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    Ok((projects, source_id))
}

/// Read the registered-project graph and prepare the only candidate location owned by this run.
/// The caller supplies a durable cohort id, so a retry never creates a second ambiguous candidate.
pub fn prepare(
    registry_path: &Path,
    root: &Path,
    work: &Path,
    cohort_id: &str,
    committed_base: &str,
    boundary: RefreshBoundary,
) -> Result<DependencyGraphRequest> {
    validate_cohort_id(cohort_id)?;
    validate_nonempty_ref(committed_base, "committed dependency graph base")?;
    let root = canonical_root(root)?;
    let registry = read_registry(registry_path)?;
    let current = registry
        .projects
        .iter()
        .find(|project| same_root(&project.root, &root))
        .ok_or_else(|| DependencyGraphError::NotRegistered { root: root.clone() })?;
    let candidate_dir = work.join("dependency_graph_candidates");
    ensure_plain_directory_or_absent(&candidate_dir, "dependency graph candidate directory")?;
    let candidate_path =
        candidate_dir.join(format!("depgraph-{}-{}.json", cohort_id, boundary.as_str()));
    Ok(DependencyGraphRequest {
        registry_path: registry_path.to_path_buf(),
        current_project_id: current.id.clone(),
        base_graph_generation: current.graph_generation,
        committed_base: committed_base.into(),
        boundary,
        candidate_path,
        candidate_directory: candidate_dir,
        projects: registry
            .projects
            .iter()
            .map(RegisteredProject::from)
            .collect(),
    })
}

/// Validate and atomically apply one candidate. The lock guards the read/CAS/write interval; a
/// lock conflict preserves the candidate, while a successful idempotent sync removes it.
pub fn sync(request: &DependencyGraphRequest, occurred_at: &str) -> Result<GraphSyncResult> {
    sync_with_cancellation(request, occurred_at, || false)
}

pub fn sync_with_cancellation(
    request: &DependencyGraphRequest,
    occurred_at: &str,
    cancelled: impl Fn() -> bool,
) -> Result<GraphSyncResult> {
    if !is_iso_utc(occurred_at) {
        return Err(DependencyGraphError::Invalid(format!(
            "dependency graph sync timestamp is not ISO-8601 UTC: {occurred_at:?}"
        )));
    }
    if cancelled() {
        return Err(DependencyGraphError::Invalid(
            "dependency graph sync lost owner authority before locking".into(),
        ));
    }
    validate_candidate_path(&request.candidate_path, &request.candidate_directory)?;
    let _lock = RegistryLock::acquire(&request.registry_path)?;
    if cancelled() {
        return Err(DependencyGraphError::Invalid(
            "dependency graph sync lost owner authority while acquiring the registry lock".into(),
        ));
    }
    let mut registry = read_registry(&request.registry_path)?;
    let index = registry
        .projects
        .iter()
        .position(|project| project.id == request.current_project_id)
        .ok_or_else(|| {
            DependencyGraphError::Invalid(format!(
                "registered project {} disappeared before graph sync",
                request.current_project_id
            ))
        })?;
    let current = &registry.projects[index];
    if current.graph_generation != request.base_graph_generation {
        return Err(DependencyGraphError::StaleGeneration {
            expected: request.base_graph_generation,
            actual: current.graph_generation,
        });
    }
    let candidate = read_snapshot(
        &request.candidate_path,
        &registry,
        &request.current_project_id,
    )?;
    if cancelled() {
        return Err(DependencyGraphError::Invalid(
            "dependency graph sync lost owner authority before registry mutation".into(),
        ));
    }
    if candidate.base_graph_generation != request.base_graph_generation {
        return Err(DependencyGraphError::StaleGeneration {
            expected: candidate.base_graph_generation,
            actual: current.graph_generation,
        });
    }
    let changed =
        current.products != candidate.products || current.dependencies != candidate.dependencies;
    if changed {
        apply_snapshot(&mut registry, index, &candidate, occurred_at)?;
        if cancelled() {
            return Err(DependencyGraphError::Invalid(
                "dependency graph sync lost owner authority before registry replacement".into(),
            ));
        }
        write_registry(&request.registry_path, &registry)?;
    }
    if cancelled() {
        return Err(DependencyGraphError::Invalid(
            "dependency graph sync lost owner authority before candidate cleanup".into(),
        ));
    }
    work_fs::remove_plain_file(&request.candidate_directory, &request.candidate_path)?;
    Ok(GraphSyncResult {
        changed,
        project_id: request.current_project_id.clone(),
        products: candidate.products,
        dependencies: candidate.dependencies,
    })
}

#[derive(Debug)]
struct Registry {
    raw: Value,
    generation: u64,
    projects: Vec<RegisteredProjectRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegisteredProjectRecord {
    id: String,
    name: String,
    root: PathBuf,
    products: Vec<String>,
    dependencies: Vec<GraphDependency>,
    graph_generation: u64,
}

impl From<&RegisteredProjectRecord> for RegisteredProject {
    fn from(value: &RegisteredProjectRecord) -> Self {
        Self {
            id: value.id.clone(),
            name: value.name.clone(),
            root: value.root.clone(),
            products: value.products.clone(),
            graph_generation: value.graph_generation,
        }
    }
}

fn read_registry(path: &Path) -> Result<Registry> {
    let parent = path.parent().ok_or_else(|| {
        DependencyGraphError::Invalid("dependency registry path has no parent".into())
    })?;
    let text = work_fs::read_required_text(parent, path, MAX_REGISTRY_BYTES)?;
    let raw: Value = serde_json::from_str(&text)?;
    parse_registry(raw)
}

fn parse_registry(raw: Value) -> Result<Registry> {
    let object = raw.as_object().ok_or_else(|| {
        DependencyGraphError::Invalid("dependency registry must be a JSON object".into())
    })?;
    required_string(object, "schema", "dependency registry").and_then(|schema| {
        (schema == REGISTRY_SCHEMA).then_some(()).ok_or_else(|| {
            DependencyGraphError::Invalid(format!(
                "unsupported dependency registry schema {schema:?}"
            ))
        })
    })?;
    let generation = optional_u64(object, "generation", "dependency registry")?.unwrap_or(0);
    let values = object
        .get("projects")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DependencyGraphError::Invalid("dependency registry has no projects array".into())
        })?;
    let mut projects = Vec::with_capacity(values.len());
    let mut ids = BTreeSet::new();
    for value in values {
        let item = value.as_object().ok_or_else(|| {
            DependencyGraphError::Invalid("dependency registry project is not an object".into())
        })?;
        let root = canonical_registered_root(required_string(item, "root", "registry project")?)?;
        let id = required_string(item, "id", "registry project")?.to_string();
        let expected_id = project_id(&root);
        if id != expected_id || !valid_project_id(&id) {
            return Err(DependencyGraphError::Invalid(format!(
                "dependency registry project id {id:?} does not match root {} ({root:?}; expected {expected_id})",
                root.display(),
            )));
        }
        if !ids.insert(id.clone()) {
            return Err(DependencyGraphError::Invalid(format!(
                "dependency registry duplicates project id {id}"
            )));
        }
        let name = required_string(item, "name", "registry project")?.to_string();
        validate_name(&name)?;
        let products = product_list(item.get("products"), "registry project products")?;
        let dependencies =
            dependency_list(item.get("dependencies"), "registry project dependencies")?;
        let graph_generation =
            optional_u64(item, "graph_generation", "registry project")?.unwrap_or(0);
        projects.push(RegisteredProjectRecord {
            id,
            name,
            root,
            products,
            dependencies,
            graph_generation,
        });
    }
    for project in &projects {
        let mut upstreams = BTreeSet::new();
        for dependency in &project.dependencies {
            if dependency.upstream_id == project.id || !ids.contains(&dependency.upstream_id) {
                return Err(DependencyGraphError::Invalid(format!(
                    "dependency registry has invalid upstream {} for {}",
                    dependency.upstream_id, project.id
                )));
            }
            if !upstreams.insert(dependency.upstream_id.clone()) {
                return Err(DependencyGraphError::Invalid(format!(
                    "dependency registry duplicates edge {} -> {}",
                    project.id, dependency.upstream_id
                )));
            }
        }
    }
    Ok(Registry {
        raw,
        generation,
        projects,
    })
}

fn read_snapshot(path: &Path, registry: &Registry, current_id: &str) -> Result<GraphSnapshot> {
    let parent = path.parent().ok_or_else(|| {
        DependencyGraphError::Invalid("dependency graph candidate has no parent".into())
    })?;
    let text = work_fs::read_required_text(parent, path, MAX_CANDIDATE_BYTES)?;
    let raw: Value = serde_json::from_str(&text)?;
    let object = raw.as_object().ok_or_else(|| {
        DependencyGraphError::Invalid("dependency graph candidate must be a JSON object".into())
    })?;
    required_string(object, "schema", "dependency graph candidate").and_then(|schema| {
        (schema == SNAPSHOT_SCHEMA).then_some(()).ok_or_else(|| {
            DependencyGraphError::Invalid(format!(
                "unsupported dependency graph candidate schema {schema:?}"
            ))
        })
    })?;
    let base_graph_generation = required_u64(
        object,
        "base_graph_generation",
        "dependency graph candidate",
    )?;
    let products = product_list(
        object.get("products"),
        "dependency graph candidate products",
    )?;
    let dependencies = candidate_dependency_list(object.get("dependencies"), registry, current_id)?;
    Ok(GraphSnapshot {
        base_graph_generation,
        products,
        dependencies,
    })
}

fn apply_snapshot(
    registry: &mut Registry,
    index: usize,
    snapshot: &GraphSnapshot,
    occurred_at: &str,
) -> Result<()> {
    let root = registry
        .raw
        .as_object_mut()
        .ok_or_else(|| DependencyGraphError::Invalid("registry changed from an object".into()))?;
    {
        let projects = root
            .get_mut("projects")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| {
                DependencyGraphError::Invalid("registry projects changed from an array".into())
            })?;
        let project = projects
            .get_mut(index)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                DependencyGraphError::Invalid("registry project changed from an object".into())
            })?;
        project.insert(
            "products".into(),
            Value::Array(
                snapshot
                    .products
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        project.insert(
            "dependencies".into(),
            Value::Array(
                snapshot
                    .dependencies
                    .iter()
                    .map(|dependency| {
                        json!({
                            "upstream_id": dependency.upstream_id,
                            "products": dependency.products,
                            "evidence": dependency.evidence,
                        })
                    })
                    .collect(),
            ),
        );
        project.insert("graph_updated_at".into(), Value::String(occurred_at.into()));
        project.insert(
            "graph_generation".into(),
            Value::Number((registry.projects[index].graph_generation + 1).into()),
        );
        projects.sort_by(|left, right| {
            let left = left.as_object();
            let right = right.as_object();
            let left_name = left
                .and_then(|value| value.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let right_name = right
                .and_then(|value| value.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let left_id = left
                .and_then(|value| value.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let right_id = right
                .and_then(|value| value.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            left_name
                .cmp(right_name)
                .then_with(|| left_id.cmp(right_id))
        });
    }
    let generation = registry.generation.checked_add(1).ok_or_else(|| {
        DependencyGraphError::Invalid("dependency registry generation overflow".into())
    })?;
    root.insert("generation".into(), Value::Number(generation.into()));
    root.insert("updated_at".into(), Value::String(occurred_at.into()));
    Ok(())
}

fn write_registry(path: &Path, registry: &Registry) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(&registry.raw)?;
    let parent = path.parent().ok_or_else(|| {
        DependencyGraphError::Invalid("dependency registry path has no parent".into())
    })?;
    work_fs::replace_file(
        parent,
        path,
        &[bytes, b"\n".to_vec()].concat(),
        MAX_REGISTRY_BYTES,
    )?;
    Ok(())
}

fn dependency_list(value: Option<&Value>, label: &str) -> Result<Vec<GraphDependency>> {
    let values = optional_array(value, label)?;
    if values.len() > MAX_DEPENDENCIES {
        return Err(DependencyGraphError::Invalid(format!(
            "{label} has too many entries"
        )));
    }
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        let item = value.as_object().ok_or_else(|| {
            DependencyGraphError::Invalid(format!("{label} contains a non-object"))
        })?;
        let upstream_id = required_string(item, "upstream_id", label)?.to_string();
        if !valid_project_id(&upstream_id) {
            return Err(DependencyGraphError::Invalid(format!(
                "{label} has invalid upstream id {upstream_id:?}"
            )));
        }
        result.push(GraphDependency {
            upstream_id,
            products: product_list(item.get("products"), &format!("{label} products"))?,
            evidence: evidence_list(item.get("evidence"), &format!("{label} evidence"))?,
        });
    }
    result.sort_by(|left, right| left.upstream_id.cmp(&right.upstream_id));
    Ok(result)
}

fn candidate_dependency_list(
    value: Option<&Value>,
    registry: &Registry,
    current_id: &str,
) -> Result<Vec<GraphDependency>> {
    let values = value.and_then(Value::as_array).ok_or_else(|| {
        DependencyGraphError::Invalid("dependency graph candidate has no dependencies array".into())
    })?;
    if values.len() > MAX_DEPENDENCIES {
        return Err(DependencyGraphError::Invalid(
            "dependency graph candidate has too many dependencies".into(),
        ));
    }
    let known: BTreeSet<_> = registry
        .projects
        .iter()
        .map(|project| project.id.as_str())
        .collect();
    let mut result = Vec::with_capacity(values.len());
    let mut upstreams = BTreeSet::new();
    for value in values {
        let item = value.as_object().ok_or_else(|| {
            DependencyGraphError::Invalid("dependency graph candidate contains a non-object".into())
        })?;
        let upstream_id =
            required_string(item, "upstream", "dependency graph candidate")?.to_string();
        if !valid_project_id(&upstream_id)
            || upstream_id == current_id
            || !known.contains(upstream_id.as_str())
        {
            return Err(DependencyGraphError::Invalid(format!(
                "dependency graph candidate has unknown/self upstream {upstream_id:?}"
            )));
        }
        if !upstreams.insert(upstream_id.clone()) {
            return Err(DependencyGraphError::Invalid(format!(
                "dependency graph candidate duplicates upstream {upstream_id}"
            )));
        }
        result.push(GraphDependency {
            upstream_id,
            products: product_list(
                item.get("products"),
                "dependency graph candidate dependency products",
            )?,
            evidence: evidence_list(
                item.get("evidence"),
                "dependency graph candidate dependency evidence",
            )?,
        });
    }
    result.sort_by(|left, right| left.upstream_id.cmp(&right.upstream_id));
    Ok(result)
}

fn product_list(value: Option<&Value>, label: &str) -> Result<Vec<String>> {
    let values = optional_array(value, label)?;
    if values.len() > MAX_PRODUCTS {
        return Err(DependencyGraphError::Invalid(format!(
            "{label} has too many entries"
        )));
    }
    let mut normalized = BTreeSet::new();
    for value in values_from(values) {
        normalized.insert(normalize_product(value)?);
    }
    Ok(normalized.into_iter().collect())
}

fn values_from(values: &[Value]) -> impl Iterator<Item = &str> {
    values.iter().map(|value| value.as_str().unwrap_or(""))
}

fn optional_array<'a>(value: Option<&'a Value>, label: &str) -> Result<&'a [Value]> {
    match value {
        None | Some(Value::Null) => Ok(&[]),
        Some(Value::Array(values)) => Ok(values),
        Some(_) => Err(DependencyGraphError::Invalid(format!(
            "{label} must be an array"
        ))),
    }
}

fn evidence_list(value: Option<&Value>, label: &str) -> Result<Vec<String>> {
    let values = optional_array(value, label)?;
    if values.len() > MAX_EVIDENCE {
        return Err(DependencyGraphError::Invalid(format!(
            "{label} has too many entries"
        )));
    }
    let mut result = BTreeSet::new();
    for value in values_from(values) {
        let text = value.trim();
        if text.is_empty() || text.len() > 500 || text.chars().any(char::is_control) {
            return Err(DependencyGraphError::Invalid(format!(
                "{label} has invalid evidence text"
            )));
        }
        result.insert(text.to_string());
    }
    Ok(result.into_iter().collect())
}

fn normalize_product(value: &str) -> Result<String> {
    let value = value.trim();
    let Some((ecosystem, name)) = value.split_once(':') else {
        return Err(DependencyGraphError::Invalid(format!(
            "product identity must use ecosystem:name: {value:?}"
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
        || name.starts_with(':')
    {
        return Err(DependencyGraphError::Invalid(format!(
            "invalid product identity {value:?}"
        )));
    }
    Ok(format!(
        "{}:{}",
        ecosystem.to_ascii_lowercase(),
        name.trim()
    ))
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            DependencyGraphError::Invalid(format!("{label} has no non-empty {key:?} string"))
        })
}

fn required_u64(object: &Map<String, Value>, key: &str, label: &str) -> Result<u64> {
    optional_u64(object, key, label)?.ok_or_else(|| {
        DependencyGraphError::Invalid(format!("{label} has no non-negative integer {key:?}"))
    })
}

fn optional_u64(object: &Map<String, Value>, key: &str, label: &str) -> Result<Option<u64>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            DependencyGraphError::Invalid(format!(
                "{label} {key:?} must be a non-negative JSON integer"
            ))
        }),
    }
}

fn canonical_root(root: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.is_dir() || work_fs::redirected(&metadata) {
        return Err(DependencyGraphError::Invalid(format!(
            "project root must be a plain directory: {}",
            root.display()
        )));
    }
    // Windows' `canonicalize` returns a verbatim `\\?\` path while the interoperable
    // registry deliberately records the ordinary `GetFullPath` spelling.  Hashing the former
    // would create a different `repo-*` id for the same checkout and make every legacy entry
    // look unregistered.  Remove only that representation prefix after the live-root check;
    // this is a normalization, not a redirect-following shortcut.
    let canonical = portable_canonical_root(root.canonicalize()?);
    Ok(trim_root(canonical))
}

#[cfg(windows)]
fn portable_canonical_root(root: PathBuf) -> PathBuf {
    let text = root.to_string_lossy();
    if let Some(unc) = text.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{unc}"))
    } else if let Some(ordinary) = text.strip_prefix(r"\\?\") {
        PathBuf::from(ordinary)
    } else {
        root
    }
}

#[cfg(not(windows))]
fn portable_canonical_root(root: PathBuf) -> PathBuf {
    root
}

fn canonical_registered_root(root: &str) -> Result<PathBuf> {
    if root.trim().is_empty() {
        return Err(DependencyGraphError::Invalid(
            "registered project has an empty root".into(),
        ));
    }
    // A stale registered checkout must remain visible: it may be an upstream of another current
    // project and legacy registry reads intentionally do not invalidate the whole graph merely
    // because a sibling directory was moved. Registration itself canonicalizes a live root, so
    // this read path performs only deterministic lexical normalization.
    let root = PathBuf::from(root);
    let absolute = if root.is_absolute() {
        root
    } else {
        env::current_dir()?.join(root)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(DependencyGraphError::Invalid(
                        "registered project root escapes its filesystem root".into(),
                    ));
                }
            }
        }
    }
    Ok(trim_root(normalized))
}

fn trim_root(path: PathBuf) -> PathBuf {
    let mut result = path;
    while result.parent().is_some() && result.as_os_str().to_string_lossy().ends_with(['\\', '/']) {
        result.pop();
    }
    result
}

fn same_root(left: &Path, right: &Path) -> bool {
    if cfg!(windows) {
        left.as_os_str()
            .to_string_lossy()
            .eq_ignore_ascii_case(&right.as_os_str().to_string_lossy())
    } else {
        left == right
    }
}

pub(crate) fn project_id(root: &Path) -> String {
    let mut identity = root.as_os_str().to_string_lossy().into_owned();
    if cfg!(windows) {
        identity.make_ascii_uppercase();
    }
    let hash = Sha256::digest(identity.as_bytes());
    format!("repo-{}", hex::encode(&hash[..10]))
}

fn valid_project_id(value: &str) -> bool {
    value.len() == 25
        && value.starts_with("repo-")
        && value[5..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_name(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 120 || value.chars().any(char::is_control) {
        return Err(DependencyGraphError::Invalid(
            "registry project has invalid name".into(),
        ));
    }
    Ok(())
}

fn validate_cohort_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 160
        || value.chars().any(char::is_control)
        || value.contains(['/', '\\'])
    {
        return Err(DependencyGraphError::Invalid(format!(
            "invalid cohort id for dependency graph candidate: {value:?}"
        )));
    }
    Ok(())
}

fn validate_nonempty_ref(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(DependencyGraphError::Invalid(format!(
            "invalid {label}: {value:?}"
        )));
    }
    Ok(())
}

fn ensure_plain_directory_or_absent(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => work_fs::require_plain_directory(path).map_err(|_| {
            DependencyGraphError::Invalid(format!(
                "{label} must be a plain directory: {}",
                path.display()
            ))
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_candidate_path(path: &Path, candidate_directory: &Path) -> Result<()> {
    if path.parent() != Some(candidate_directory) {
        return Err(DependencyGraphError::Invalid(format!(
            "dependency graph candidate escapes its prepared directory: {}",
            path.display()
        )));
    }
    let parent = path.parent().ok_or_else(|| {
        DependencyGraphError::Invalid("dependency graph candidate has no parent".into())
    })?;
    ensure_plain_directory_or_absent(parent, "dependency graph candidate directory")?;
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            DependencyGraphError::Invalid("dependency graph candidate has invalid name".into())
        })?;
    if !file.starts_with("depgraph-")
        || !file.ends_with(".json")
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(DependencyGraphError::Invalid(format!(
            "dependency graph candidate path is not owned: {}",
            path.display()
        )));
    }
    Ok(())
}

struct RegistryLock {
    path: PathBuf,
    token: String,
    _file: File,
}

impl RegistryLock {
    fn acquire(registry: &Path) -> Result<Self> {
        let parent = registry.parent().ok_or_else(|| {
            DependencyGraphError::Invalid("dependency registry path has no parent".into())
        })?;
        match fs::symlink_metadata(parent) {
            Ok(_) => work_fs::require_plain_directory(parent)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(DependencyGraphError::Invalid(format!(
                    "dependency registry directory does not exist: {}",
                    parent.display()
                )));
            }
            Err(error) => return Err(error.into()),
        }
        let path = registry.with_extension("json.lock");
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                let token = format!("{}\n", Uuid::new_v4());
                file.write_all(token.as_bytes())?;
                file.sync_all()?;
                work_fs::require_plain_directory(parent)?;
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.is_file() || work_fs::redirected(&metadata) {
                    return Err(DependencyGraphError::Invalid(format!(
                        "dependency registry lock is not a plain file: {}",
                        path.display()
                    )));
                }
                let observed = work_fs::read_required_text(parent, &path, 1_024)?;
                if observed != token {
                    return Err(DependencyGraphError::Invalid(
                        "dependency registry lock ownership changed before use".into(),
                    ));
                }
                Ok(Self {
                    path,
                    token,
                    _file: file,
                })
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Err(DependencyGraphError::Busy { lock: path })
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for RegistryLock {
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

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(output, "{byte:02x}");
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: PathBuf,
        work: PathBuf,
        upstream: PathBuf,
        registry: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "orchestrail-dependency-graph-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            let work = root.join(".work");
            let upstream = root.join("upstream");
            let registry = root.join("registry/projects.json");
            fs::create_dir_all(&work).unwrap();
            fs::create_dir_all(&upstream).unwrap();
            let current = canonical_root(&root).unwrap();
            let upstream_canonical = canonical_root(&upstream).unwrap();
            fs::create_dir_all(registry.parent().unwrap()).unwrap();
            fs::write(
                &registry,
                json!({
                    "schema": REGISTRY_SCHEMA,
                    "generation": 4,
                    "updated_at": "2026-07-25T12:00:00Z",
                    "projects": [
                        {"id": project_id(&current), "name": "Current", "root": current, "registered_at": "", "last_configured_at": "", "products": ["cargo:current"], "dependencies": [], "graph_updated_at": "", "graph_generation": 7},
                        {"id": project_id(&upstream_canonical), "name": "Upstream", "root": upstream_canonical, "registered_at": "", "last_configured_at": "", "products": ["cargo:upstream"], "dependencies": [], "graph_updated_at": "", "graph_generation": 2}
                    ]
                })
                .to_string(),
            )
            .unwrap();
            Self {
                root,
                work,
                upstream,
                registry,
            }
        }

        fn request(&self) -> DependencyGraphRequest {
            prepare(
                &self.registry,
                &self.root,
                &self.work,
                "B-20260725T120000Z",
                "base",
                RefreshBoundary::CohortOpen,
            )
            .unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn sync_replaces_only_current_graph_slice_under_a_matching_generation() {
        let fixture = Fixture::new();
        let request = fixture.request();
        fs::create_dir_all(request.candidate_path.parent().unwrap()).unwrap();
        fs::write(
            &request.candidate_path,
            json!({
                "schema": SNAPSHOT_SCHEMA,
                "base_graph_generation": request.base_graph_generation,
                "products": ["cargo:current"],
                "dependencies": [{
                    "upstream": project_id(&canonical_root(&fixture.upstream).unwrap()),
                    "products": ["cargo:upstream"],
                    "evidence": ["Cargo.toml: dependencies.upstream"]
                }]
            })
            .to_string(),
        )
        .unwrap();

        let result = sync(&request, "2026-07-25T12:00:01Z").unwrap();
        assert!(result.changed);
        assert!(!request.candidate_path.exists());
        let registry = read_registry(&fixture.registry).unwrap();
        let current = registry
            .projects
            .iter()
            .find(|project| project.id == request.current_project_id)
            .unwrap();
        assert_eq!(current.graph_generation, 8);
        assert_eq!(current.dependencies.len(), 1);
        let upstream = registry
            .projects
            .iter()
            .find(|project| project.root == canonical_root(&fixture.upstream).unwrap())
            .unwrap();
        assert_eq!(
            upstream.graph_generation, 2,
            "must not mutate another project"
        );
    }

    #[test]
    fn cancellation_after_registry_lock_preserves_candidate_and_registry() {
        use std::cell::Cell;

        let fixture = Fixture::new();
        let request = fixture.request();
        fs::create_dir_all(request.candidate_path.parent().unwrap()).unwrap();
        fs::write(
            &request.candidate_path,
            json!({
                "schema": SNAPSHOT_SCHEMA,
                "base_graph_generation": request.base_graph_generation,
                "products": ["cargo:changed"],
                "dependencies": []
            })
            .to_string(),
        )
        .unwrap();
        let before = fs::read(&fixture.registry).unwrap();
        let checks = Cell::new(0_u8);
        assert!(
            sync_with_cancellation(&request, "2026-07-25T12:00:01Z", || {
                let current = checks.get();
                checks.set(current + 1);
                current >= 1
            })
            .is_err()
        );
        assert_eq!(fs::read(&fixture.registry).unwrap(), before);
        assert!(request.candidate_path.exists());
    }

    #[test]
    fn stale_candidate_and_busy_lock_preserve_the_diagnostic_candidate() {
        let fixture = Fixture::new();
        let request = fixture.request();
        fs::create_dir_all(request.candidate_path.parent().unwrap()).unwrap();
        fs::write(
            &request.candidate_path,
            json!({
                "schema": SNAPSHOT_SCHEMA,
                "base_graph_generation": 6,
                "products": [],
                "dependencies": []
            })
            .to_string(),
        )
        .unwrap();
        assert!(matches!(
            sync(&request, "2026-07-25T12:00:01Z"),
            Err(DependencyGraphError::StaleGeneration { .. })
        ));
        assert!(request.candidate_path.exists());
        let lock = RegistryLock::acquire(&fixture.registry).unwrap();
        assert!(matches!(
            RegistryLock::acquire(&fixture.registry),
            Err(DependencyGraphError::Busy { .. })
        ));
        drop(lock);
    }

    #[test]
    fn candidate_cannot_target_an_unknown_or_self_project() {
        let fixture = Fixture::new();
        let request = fixture.request();
        fs::create_dir_all(request.candidate_path.parent().unwrap()).unwrap();
        fs::write(
            &request.candidate_path,
            json!({
                "schema": SNAPSHOT_SCHEMA,
                "base_graph_generation": request.base_graph_generation,
                "products": [],
                "dependencies": [{"upstream": request.current_project_id, "products": [], "evidence": ["x"]}]
            })
            .to_string(),
        )
        .unwrap();
        assert!(matches!(
            sync(&request, "2026-07-25T12:00:01Z"),
            Err(DependencyGraphError::Invalid(message)) if message.contains("unknown/self upstream")
        ));
    }

    #[test]
    fn sync_refuses_a_candidate_path_tampered_outside_the_prepared_directory() {
        let fixture = Fixture::new();
        let mut request = fixture.request();
        let outside = fixture.root.join("other-candidates");
        fs::create_dir_all(&outside).unwrap();
        request.candidate_path = outside.join("depgraph-tampered.json");

        assert!(matches!(
            sync(&request, "2026-07-25T12:00:01Z"),
            Err(DependencyGraphError::Invalid(message)) if message.contains("escapes its prepared directory")
        ));
    }

    #[test]
    fn a_stale_unrelated_registered_root_does_not_block_current_graph_refresh() {
        let fixture = Fixture::new();
        let stale = fixture.root.join("moved-away-project");
        let mut registry: Value =
            serde_json::from_str(&fs::read_to_string(&fixture.registry).unwrap()).unwrap();
        registry["projects"].as_array_mut().unwrap().push(json!({
            "id": project_id(&stale),
            "name": "Stale",
            "root": stale,
            "registered_at": "",
            "last_configured_at": "",
            "products": [],
            "dependencies": [],
            "graph_updated_at": "",
            "graph_generation": 0
        }));
        fs::write(&fixture.registry, registry.to_string()).unwrap();

        let request = fixture.request();
        assert_eq!(
            request.current_project_id,
            project_id(&canonical_root(&fixture.root).unwrap())
        );
        assert_eq!(request.projects.len(), 3);
    }
}
