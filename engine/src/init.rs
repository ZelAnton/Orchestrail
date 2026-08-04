//! First-run creation of a safe, minimal control-plane skeleton.
//!
//! `init` is deliberately additive: every artifact is created with an exclusive open and an
//! existing path is left byte-for-byte untouched.  The generated configuration makes the two
//! authority-bearing defaults that are permissive in the backwards-compatible parser explicit:
//! publication is off and Codex network access is off.

use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use crate::config::{self, ConfigError, EngineConfig};
use crate::policy::{self, Policy, PolicyError};
use crate::work_fs;

/// The four files that make up the initial control-plane skeleton.
const ARTIFACTS: [(&str, &str); 4] = [
    ("config.md", CONFIG_TEMPLATE),
    ("constraints.md", CONSTRAINTS_TEMPLATE),
    ("Tasks_Queue.md", QUEUE_TEMPLATE),
    ("Tasks_Done.md", ARCHIVE_TEMPLATE),
];

/// A complete, parser-compatible configuration seed.
///
/// Most settings remain comments so an operator can opt into them deliberately.  The active
/// safety settings must stay explicit because the compatibility parser's historical defaults for
/// `PUSH` and `CODEX_NETWORK` are intentionally not safe for a new project.
pub const CONFIG_TEMPLATE: &str = r#"# Сгенерировано `orchestrail-engine init`.
#
# Все поддерживаемые ключи перечислены ниже. Закомментированные строки используют дефолт
# движка; меняйте их только после проверки проекта. Публикация и сеть выключены явно.

# --- Процессор и rolling cohort ---
# MAX_PARALLEL: 3
# COHORT_SIZE: 9
# COHORT_MAX_AGE: 90
# REVIEW_MIN_PASSES: 2
# REVIEW_LOOP_MAX: 8
# INTEGRATION_LOOP_MAX: 8
# CI_FIX_MAX: 3
# STAGNATION_LIMIT: 2
# QUARANTINE_MAX_ATTEMPTS: 3
# CALL_MAX_ATTEMPTS: 2
# COHORT_BUDGET_SEC: 0
# COHORT_TOKEN_BUDGET: 0
# COHORT_TOKEN_BUDGET_STRICT: false

# --- Локальные события и база знаний ---
EVENTS_OUTBOX: on
# EVENTS_ROTATION_ENABLED: off
# EVENTS_ROTATION_MIN_BYTES: 8388608
KB: on
# KB_TTL: 8
# KB_CAP: 12

# --- Проверки и публикация ---
PUSH: false
CI_WATCH: false
# FORGE: github
# PUBLISH_LINEAR_HISTORY: false
# PUBLISH_CI_DEADLINE_SEC: 1800
# PUBLISH_CI_BACKOFF_SEC: 30
APPROVAL_DEADLINE_SEC: 86400
REVIEWER_TIERING: true
# MAIN_BRANCH: main
# VERIFICATION_MODE: disabled
# VERIFICATION_COMMANDS: ["cargo fmt --all --check", "cargo test --workspace"]
# REVIEW_CYCLE_VERIFICATION: false
# REVIEW_CYCLE_VERIFICATION_COMMANDS: ["cargo fmt --all --check"]
# SMOKE_CMD: cargo test --workspace
# NOTIFY_CMD: notify-send "Orchestrail"

# --- Containment ---
# CALL_DEADLINE_SEC: 1800
# CALL_OUTPUT_MAX_BYTES: 1048576

# --- Codex (opt-in; network remains off until explicitly changed) ---
CODEX_CODER: off
CODEX_REVIEWER: off
CODEX_CIFIX: off
# CODEX_MODEL: codex
# CODEX_REASONING: auto
CODEX_SANDBOX: read-only
CODEX_NETWORK: off
# CODEX_CMD: codex

# --- Model pricing overrides ---
# MODEL_PRICES_EFFECTIVE_DATE: 2026-07-30
# MODEL_PRICES_USD_PER_MILLION: model=input,output[,cached-input[,cache-creation-input]]
"#;

/// A parser-compatible policy seed.  The active push rule is intentionally human-gated, while
/// all path/check lists begin empty and can be filled in by the project operator.
pub const CONSTRAINTS_TEMPLATE: &str = r#"# Политика ограничений — создано `orchestrail-engine init`
#
# Активные ограничения ниже имеют механическое значение. Примеры и пояснения не применяются.
# Заполните denylist и обязательные проверки после изучения проекта.

## Запрещённые пути (denylist)

**Активные ограничения**

- (пусто — запрещённых путей нет)

**Пример**

- .work/constraints.md
- **/secrets/**

## Разрешённые ветки и remotes

**Активные ограничения**

- Ветки публикации: (по умолчанию — определяет processor)
- Remotes: (по умолчанию — origin)

## Push/merge policy

**Активные ограничения**

- Публикация (push): требует ручного подтверждения
- Слияние в trunk: только ff-merge после интеграционного ревью

## Обязательные проверки

**Активные ограничения**

- (пусто — дополнительных проверок нет; профиль задаётся в config.md)

## Обязательные CI-проверки публикации

**Активные ограничения**

- (пусто — обязательных CI-проверок нет)

## Пороги размера изменений

**Активные ограничения**

- Максимум файлов в одной задаче: (не задано)
- Максимум затронутых подсистем/модулей: (не задано)

## Категории обязательного human review

**Активные категории**

- изменение архитектурных границ и публичных контрактов/API;
- безопасность, auth, permissions, секреты, PII;
- production-инфраструктура и миграции данных;
- изменение этой политики или обязательных проверок.
"#;

/// An empty queue in the canonical Markdown shape.
pub const QUEUE_TEMPLATE: &str = "# Очередь задач\n\n";

/// An empty archive whose H2/H3 task headers are reserved for published entries.
pub const ARCHIVE_TEMPLATE: &str = "# Архив выполненных задач\n\n";

/// Result of one init operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitReport {
    pub work_dir: PathBuf,
    pub created: Vec<String>,
    pub existing: Vec<String>,
    pub config: EngineConfig,
    pub policy: Policy,
}

#[derive(Debug)]
pub enum InitError {
    InvalidWorkPath(String),
    Io { path: PathBuf, source: io::Error },
    Config(ConfigError),
    Policy(PolicyError),
}

impl fmt::Display for InitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWorkPath(message) => formatter.write_str(message),
            Self::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Self::Config(error) => write!(formatter, "config.md validation failed: {error}"),
            Self::Policy(error) => write!(formatter, "constraints.md validation failed: {error}"),
        }
    }
}

impl Error for InitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Config(error) => Some(error),
            Self::Policy(error) => Some(error),
            Self::InvalidWorkPath(_) => None,
        }
    }
}

/// Create the initial control-plane artifacts below `work`, validate the resulting config and
/// policy, and report which entries were created versus preserved.
pub fn initialize(work: impl AsRef<Path>) -> Result<InitReport, InitError> {
    let work = absolute_work_path(work.as_ref())?;
    ensure_plain_directory_tree(&work)?;

    let mut created = Vec::new();
    let mut existing = Vec::new();
    for (name, contents) in ARTIFACTS {
        match create_artifact(&work, name, contents)? {
            ArtifactResult::Created => created.push(name.to_string()),
            ArtifactResult::Existing => existing.push(name.to_string()),
        }
    }

    // Validate after all create attempts so a partially initialized directory is still brought
    // to a useful state on a later run, while an operator-owned invalid file remains fail-closed.
    let config = config::load(&work).map_err(InitError::Config)?;
    let policy = policy::load(&work).map_err(InitError::Policy)?;
    Ok(InitReport {
        work_dir: work,
        created,
        existing,
        config,
        policy,
    })
}

fn absolute_work_path(work: &Path) -> Result<PathBuf, InitError> {
    if work.as_os_str().is_empty() {
        return Err(InitError::InvalidWorkPath(
            "--work requires a non-empty path".into(),
        ));
    }
    let absolute = if work.is_absolute() {
        work.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|source| InitError::Io {
                path: PathBuf::from("."),
                source,
            })?
            .join(work)
    };
    if absolute
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(InitError::InvalidWorkPath(format!(
            "--work must not contain a parent-directory component: {}",
            work.display()
        )));
    }
    Ok(absolute)
}

/// Create missing directories one component at a time and prove every existing component is a
/// plain directory. `create_dir_all` is intentionally not used: it would make it too easy to
/// follow a symlink/reparse point in a path supplied by an operator.
fn ensure_plain_directory_tree(path: &Path) -> Result<(), InitError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(InitError::InvalidWorkPath(format!(
                    "--work must not contain a parent-directory component: {}",
                    path.display()
                )));
            }
            Component::Prefix(_) | Component::RootDir => current.push(component.as_os_str()),
            Component::Normal(name) => {
                current.push(name);
                match fs::symlink_metadata(&current) {
                    Ok(_) => work_fs::ensure_plain_directory(&current),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        match fs::create_dir(&current) {
                            Ok(()) => Ok(()),
                            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                                work_fs::ensure_plain_directory(&current)
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                }
                .map_err(|source| InitError::Io {
                    path: current.clone(),
                    source,
                })?;
            }
        }
    }
    work_fs::ensure_plain_directory(&current).map_err(|source| InitError::Io {
        path: current,
        source,
    })
}

enum ArtifactResult {
    Created,
    Existing,
}

fn create_artifact(work: &Path, name: &str, contents: &str) -> Result<ArtifactResult, InitError> {
    let path = work.join(name);
    if work_fs::entry_exists(work, &path).map_err(|source| InitError::Io {
        path: path.clone(),
        source,
    })? {
        return Ok(ArtifactResult::Existing);
    }

    let mut file = match work_fs::create_new_plain_file_rooted(work, &path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if work_fs::entry_exists(work, &path).map_err(|source| InitError::Io {
                path: path.clone(),
                source,
            })? {
                return Ok(ArtifactResult::Existing);
            }
            return Err(InitError::Io {
                path,
                source: error,
            });
        }
        Err(source) => return Err(InitError::Io { path, source }),
    };

    let write_result = (|| {
        file.write_all(contents.as_bytes())?;
        file.sync_all()
    })();
    if let Err(source) = write_result {
        // The exclusive create proves this process owns the new path. Remove an incomplete seed
        // rather than leaving a durable zero/partial-byte artifact that future init calls refuse.
        let _ = work_fs::remove_plain_file(work, &path);
        return Err(InitError::Io { path, source });
    }
    work_fs::sync_parent_directory(work, &path).map_err(|source| InitError::Io {
        path: path.clone(),
        source,
    })?;
    Ok(ArtifactResult::Created)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_templates_are_self_validating_and_safe() {
        let work = test_work("safe");
        let report = initialize(&work).unwrap();
        assert_eq!(
            report.created,
            vec![
                "config.md",
                "constraints.md",
                "Tasks_Queue.md",
                "Tasks_Done.md"
            ]
        );
        assert!(!report.config.push);
        assert!(!report.config.codex.network);
        assert_eq!(report.config.codex.sandbox, crate::codex::Sandbox::ReadOnly);
        assert!(report.policy.push_requires_approval);
        assert!(
            crate::state::queue::parse_queue(
                &fs::read_to_string(work.join("Tasks_Queue.md")).unwrap()
            )
            .is_empty()
        );
        cleanup(&work);
    }

    #[test]
    fn rerun_preserves_existing_bytes() {
        let work = test_work("no-overwrite");
        initialize(&work).unwrap();
        let config = work.join("config.md");
        let original = fs::read(&config).unwrap();
        let report = initialize(&work).unwrap();
        assert!(report.created.is_empty());
        assert_eq!(report.existing.len(), 4);
        assert_eq!(fs::read(config).unwrap(), original);
        cleanup(&work);
    }

    fn test_work(label: &str) -> PathBuf {
        // macOS exposes its temporary directory through /var, which is a symlink to /private/var.
        // Pass the physical path to the production confinement checks without weakening them.
        let root = if cfg!(target_os = "macos") {
            fs::canonicalize(std::env::temp_dir()).expect("macOS temporary directory must exist")
        } else {
            std::env::temp_dir()
        };
        root.join(format!("orchestrail-init-{label}-{}", std::process::id()))
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }
}
