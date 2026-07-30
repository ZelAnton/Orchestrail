//! One-shot, model-assisted discovery of a repository verification profile.
//!
//! The model is advisory only. Its complete response is decoded before any mutation, and every
//! proposed command is then checked locally for typed argv, an executable, and a repository
//! witness file. Only the resulting deterministic projection is allowed into `config.md`.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;

use crate::command_line::parse_typed_argv;
use crate::config;
use crate::work_fs::{self, MAX_CONTROL_BYTES};

const CONFIG_FILE: &str = "config.md";
const SKIPPED_PREFIX: &str = "VERIFICATION_COMMANDS_DISCOVERY_SKIPPED_";

/// The exact JSON object accepted from the read-only model call.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelResponse {
    candidates: Vec<ModelCandidate>,
}

/// One advisory command and the repository file that makes it applicable.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelCandidate {
    command: String,
    witness: String,
}

/// Process lookup inputs captured once so validation is deterministic and hermetically testable.
#[derive(Debug, Clone)]
pub struct DiscoveryEnvironment {
    search_path: Vec<PathBuf>,
    executable_extensions: Vec<OsString>,
}

impl DiscoveryEnvironment {
    pub fn from_process() -> Self {
        let search_path = std::env::var_os("PATH")
            .map(|value| std::env::split_paths(&value).collect())
            .unwrap_or_default();
        let executable_extensions = if cfg!(windows) {
            std::env::var_os("PATHEXT")
                .map(|value| {
                    value
                        .to_string_lossy()
                        .split(';')
                        .filter(|extension| !extension.is_empty())
                        .map(OsString::from)
                        .collect()
                })
                .unwrap_or_else(|| {
                    [".COM", ".EXE", ".BAT", ".CMD"]
                        .into_iter()
                        .map(OsString::from)
                        .collect()
                })
        } else {
            Vec::new()
        };
        Self {
            search_path,
            executable_extensions,
        }
    }

    #[cfg(test)]
    fn hermetic(search_path: Vec<PathBuf>, executable_extensions: Vec<OsString>) -> Self {
        Self {
            search_path,
            executable_extensions,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryOutcome {
    pub accepted: usize,
    pub skipped: usize,
    /// False when an operator already supplied a verification profile.
    pub changed: bool,
}

#[derive(Debug)]
pub enum DiscoveryError {
    Io(io::Error),
    InvalidExistingConfig(config::ConfigError),
    Backend(String),
    InvalidModelOutput(String),
    EmptyModelOutput,
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::InvalidExistingConfig(error) => {
                write!(formatter, "existing config.md is invalid: {error}")
            }
            Self::Backend(error) => write!(formatter, "model backend failed: {error}"),
            Self::InvalidModelOutput(error) => {
                write!(formatter, "model returned invalid discovery JSON: {error}")
            }
            Self::EmptyModelOutput => {
                formatter.write_str("model returned no verification candidates")
            }
        }
    }
}

impl std::error::Error for DiscoveryError {}

impl From<io::Error> for DiscoveryError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Ask one read-only backend for candidates, validate them locally, and atomically augment
/// `config.md`. Backend or JSON failure returns before the write boundary.
pub fn discover_and_write(
    repository_root: &Path,
    work: &Path,
    environment: &DiscoveryEnvironment,
    backend: impl FnOnce(&str) -> Result<String, String>,
) -> Result<DiscoveryOutcome, DiscoveryError> {
    let config_path = work.join(CONFIG_FILE);
    let existing = match work_fs::read_optional_text(work, &config_path, MAX_CONTROL_BYTES) {
        Ok(Some(text)) => text,
        Ok(None) => String::new(),
        Err(error) => return Err(error.into()),
    };
    let parsed_existing =
        config::parse(&existing).map_err(DiscoveryError::InvalidExistingConfig)?;
    if !parsed_existing.verification_commands.is_empty() || parsed_existing.smoke_cmd.is_some() {
        return Ok(DiscoveryOutcome {
            accepted: 0,
            skipped: 0,
            changed: false,
        });
    }

    let raw = backend(DISCOVERY_PROMPT).map_err(DiscoveryError::Backend)?;
    if raw.trim().is_empty() {
        return Err(DiscoveryError::EmptyModelOutput);
    }
    let response: ModelResponse = serde_json::from_str(raw.trim())
        .map_err(|error| DiscoveryError::InvalidModelOutput(error.to_string()))?;
    if response.candidates.is_empty() {
        return Err(DiscoveryError::EmptyModelOutput);
    }

    let mut accepted = Vec::new();
    let mut skipped = Vec::new();
    let mut seen = BTreeSet::new();
    for candidate in response.candidates {
        let command = candidate.command.trim().to_string();
        let witness = candidate.witness.trim().to_string();
        let rejection = validate_candidate(
            repository_root,
            environment,
            &command,
            &witness,
            &mut seen,
        );
        match rejection {
            Some(reason) => skipped.push(reason),
            None => accepted.push(command),
        }
    }

    // Reuse the configuration decoder itself rather than maintaining a second JSON profile
    // contract at the discovery boundary.
    let commands_json = serde_json::to_string(&accepted)
        .map_err(|error| DiscoveryError::InvalidModelOutput(error.to_string()))?;
    if !accepted.is_empty() {
        config::parse_verification_commands("VERIFICATION_COMMANDS", &commands_json)
            .map_err(DiscoveryError::InvalidExistingConfig)?;
    }

    let mut addition = String::new();
    if !existing.is_empty() && !existing.ends_with('\n') {
        addition.push('\n');
    }
    if !existing.is_empty() {
        addition.push('\n');
    }
    addition.push_str(
        "# Verification profile proposed by read-only config discovery and validated locally.\n",
    );
    if !accepted.is_empty() {
        addition.push_str("VERIFICATION_COMMANDS: ");
        addition.push_str(&commands_json);
        addition.push('\n');
    }
    let first_skipped_index = next_skipped_index(&existing);
    for (offset, reason) in skipped.iter().enumerate() {
        addition.push_str(SKIPPED_PREFIX);
        addition.push_str(&(first_skipped_index + offset).to_string());
        addition.push_str(": off  # ");
        addition.push_str(&sanitize_comment(reason));
        addition.push('\n');
    }

    let mut updated = existing;
    updated.push_str(&addition);
    config::parse(&updated).map_err(DiscoveryError::InvalidExistingConfig)?;
    work_fs::replace_file(
        work,
        &config_path,
        updated.as_bytes(),
        MAX_CONTROL_BYTES,
    )?;
    Ok(DiscoveryOutcome {
        accepted: accepted.len(),
        skipped: skipped.len(),
        changed: true,
    })
}

fn validate_candidate(
    repository_root: &Path,
    environment: &DiscoveryEnvironment,
    command: &str,
    witness: &str,
    seen: &mut BTreeSet<String>,
) -> Option<String> {
    let argv = match parse_typed_argv(command) {
        Ok(argv) => argv,
        Err(error) => return Some(format!("{command}: {error}")),
    };
    if !seen.insert(command.to_string()) {
        return Some(format!("{command}: duplicate candidate"));
    }
    let program = &argv[0];
    if !program_is_available(repository_root, environment, program) {
        return Some(format!("{program}: not found on PATH"));
    }
    if let Some(required) = required_witness(program)
        && Path::new(witness) != Path::new(required)
    {
        return Some(format!(
            "{command}: expected witness {required}, got {witness}"
        ));
    }
    if witness.is_empty() {
        return Some(format!("{command}: witness file is empty"));
    }
    let witness_path = Path::new(witness);
    if witness_path.is_absolute()
        || witness_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Some(format!(
            "{command}: witness must be a repository-relative file"
        ));
    }
    match fs::metadata(repository_root.join(witness_path)) {
        Ok(metadata) if metadata.is_file() => None,
        Ok(_) => Some(format!("{command}: {witness} is not a file")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Some(format!("{command}: {witness} not found"))
        }
        Err(error) => Some(format!("{command}: cannot inspect {witness}: {error}")),
    }
}

fn required_witness(program: &str) -> Option<&'static str> {
    let executable = program
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(program)
        .split('.')
        .next()
        .unwrap_or(program)
        .to_ascii_lowercase();
    match executable.as_str() {
        "cargo" | "rustfmt" => Some("Cargo.toml"),
        "npm" | "npx" | "pnpm" | "yarn" | "bun" => Some("package.json"),
        _ => None,
    }
}

fn program_is_available(
    repository_root: &Path,
    environment: &DiscoveryEnvironment,
    program: &str,
) -> bool {
    let path = Path::new(program);
    if path.is_absolute() || path.components().count() > 1 {
        let candidate = if path.is_absolute() {
            path.to_path_buf()
        } else {
            repository_root.join(path)
        };
        return is_executable_file(&candidate);
    }
    environment.search_path.iter().any(|directory| {
        executable_names(program, &environment.executable_extensions)
            .into_iter()
            .any(|name| is_executable_file(&directory.join(name)))
    })
}

fn executable_names(program: &str, extensions: &[OsString]) -> Vec<OsString> {
    let mut names = vec![OsString::from(program)];
    if cfg!(windows) && Path::new(program).extension().is_none() {
        names.extend(extensions.iter().map(|extension| {
            let mut name = OsString::from(program);
            name.push(extension);
            name
        }));
    }
    names
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn next_skipped_index(existing: &str) -> usize {
    existing
        .lines()
        .filter_map(|line| {
            line.strip_prefix(SKIPPED_PREFIX)?
                .split_once(':')?
                .0
                .trim()
                .parse::<usize>()
                .ok()
        })
        .max()
        .unwrap_or(0)
        + 1
}

fn sanitize_comment(reason: &str) -> String {
    reason
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

pub const DISCOVERY_PROMPT: &str = r#"Inspect this repository read-only and propose its format, lint, and test commands.
Return exactly one JSON object and no Markdown or prose:
{"candidates":[{"command":"cargo fmt --all","witness":"Cargo.toml"}]}
Each command must directly name one executable with typed arguments (no shell operators).
Each witness must be one repository-relative file whose presence proves that command applies.
Include only commands you infer from repository files; do not run commands and do not modify files."#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: PathBuf,
        work: PathBuf,
        bin: PathBuf,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "orchestrail-config-discovery-{label}-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            let work = root.join("control");
            let bin = root.join("bin");
            fs::create_dir_all(&work).unwrap();
            fs::create_dir(&bin).unwrap();
            fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
            let program = if cfg!(windows) { "cargo.exe" } else { "cargo" };
            let executable = bin.join(program);
            fs::write(&executable, b"fixture").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let mut permissions = fs::metadata(&executable).unwrap().permissions();
                permissions.set_mode(0o755);
                fs::set_permissions(&executable, permissions).unwrap();
            }
            Self {
                root,
                work,
                bin,
            }
        }

        fn environment(&self) -> DiscoveryEnvironment {
            DiscoveryEnvironment::hermetic(
                vec![self.bin.clone()],
                if cfg!(windows) {
                    vec![OsString::from(".EXE")]
                } else {
                    Vec::new()
                },
            )
        }

        fn config(&self) -> String {
            fs::read_to_string(self.work.join(CONFIG_FILE)).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn happy_path_writes_all_locally_valid_candidates() {
        let fixture = Fixture::new("happy");
        let response = r#"{"candidates":[
            {"command":"cargo fmt --all","witness":"Cargo.toml"},
            {"command":"cargo clippy --workspace --all-targets -- -D warnings","witness":"Cargo.toml"},
            {"command":"cargo test --workspace","witness":"Cargo.toml"}
        ]}"#;
        let mut calls = 0;
        let outcome =
            discover_and_write(&fixture.root, &fixture.work, &fixture.environment(), |_| {
                calls += 1;
                Ok(response.into())
            })
            .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(
            outcome,
            DiscoveryOutcome {
                accepted: 3,
                skipped: 0,
                changed: true
            }
        );
        let config = fixture.config();
        assert_eq!(
            config::parse(&config).unwrap().verification_commands,
            vec![
                "cargo fmt --all",
                "cargo clippy --workspace --all-targets -- -D warnings",
                "cargo test --workspace"
            ]
        );
    }

    #[test]
    fn partial_rejection_records_each_reason_as_an_explicit_off_entry() {
        let fixture = Fixture::new("partial");
        let response = r#"{"candidates":[
            {"command":"cargo fmt --all","witness":"Cargo.toml"},
            {"command":"npm run lint","witness":"package.json"},
            {"command":"cargo test --workspace","witness":"missing.toml"}
        ]}"#;
        let outcome =
            discover_and_write(&fixture.root, &fixture.work, &fixture.environment(), |_| {
                Ok(response.into())
            })
            .unwrap();
        assert_eq!(outcome.accepted, 1);
        assert_eq!(outcome.skipped, 2);
        let config = fixture.config();
        assert!(config.contains("VERIFICATION_COMMANDS: [\"cargo fmt --all\"]"));
        assert!(config.contains(
            "VERIFICATION_COMMANDS_DISCOVERY_SKIPPED_1: off  # npm: not found on PATH"
        ));
        assert!(config.contains(
            "VERIFICATION_COMMANDS_DISCOVERY_SKIPPED_2: off  # cargo test --workspace: expected witness Cargo.toml, got missing.toml"
        ));
    }

    #[test]
    fn backend_or_json_failure_never_touches_config() {
        let fixture = Fixture::new("failure");
        let original = "MAX_PARALLEL: 2\n";
        fs::write(fixture.work.join(CONFIG_FILE), original).unwrap();

        let backend_error =
            discover_and_write(&fixture.root, &fixture.work, &fixture.environment(), |_| {
                Err("unavailable".into())
            })
            .unwrap_err();
        assert!(matches!(backend_error, DiscoveryError::Backend(_)));
        assert_eq!(fixture.config(), original);

        let json_error =
            discover_and_write(&fixture.root, &fixture.work, &fixture.environment(), |_| {
                Ok("not json".into())
            })
            .unwrap_err();
        assert!(matches!(
            json_error,
            DiscoveryError::InvalidModelOutput(_)
        ));
        assert_eq!(fixture.config(), original);
    }

    #[test]
    fn empty_output_is_explicit_and_manual_profiles_are_never_overwritten() {
        let fixture = Fixture::new("empty-manual");
        let original = "VERIFICATION_COMMANDS: [\"cargo test --workspace\"]\n";
        fs::write(fixture.work.join(CONFIG_FILE), original).unwrap();
        let mut called = false;
        let outcome =
            discover_and_write(&fixture.root, &fixture.work, &fixture.environment(), |_| {
                called = true;
                Ok(r#"{"candidates":[]}"#.into())
            })
            .unwrap();
        assert!(!called);
        assert!(!outcome.changed);
        assert_eq!(fixture.config(), original);

        fs::write(fixture.work.join(CONFIG_FILE), "MAX_PARALLEL: 2\n").unwrap();
        let error =
            discover_and_write(&fixture.root, &fixture.work, &fixture.environment(), |_| {
                Ok(r#"{"candidates":[]}"#.into())
            })
            .unwrap_err();
        assert!(matches!(error, DiscoveryError::EmptyModelOutput));
        assert_eq!(fixture.config(), "MAX_PARALLEL: 2\n");
    }
}
