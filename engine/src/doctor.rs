//! Read-only environment diagnostics for the `engine doctor` command.
//!
//! Every probe is independent: a missing executable, malformed control-plane document, or
//! incomplete checkout identity becomes one structured finding rather than aborting the report.
//! The system implementation invokes only bounded `--version` commands through ProcessKit.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;

use crate::{config, policy, supervise, toolscript};

const VERSION_DEADLINE: Duration = Duration::from_secs(10);
const VERSION_OUTPUT_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Warn,
    Fail,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

/// One independent, machine-readable doctor finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckResult {
    pub name: String,
    pub status: Status,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CheckResult {
    fn ok(name: &str, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Ok,
            message: message.into(),
            reason: None,
        }
    }

    fn warn(name: &str, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Warn,
            message: message.into(),
            reason: None,
        }
    }

    fn fail(name: &str, message: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Fail,
            message: message.into(),
            reason: Some(reason.into()),
        }
    }
}

/// The complete report emitted by `engine doctor`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DoctorReport {
    pub root: PathBuf,
    pub work: PathBuf,
    pub checks: Vec<CheckResult>,
}

impl DoctorReport {
    pub fn has_failures(&self) -> bool {
        self.checks.iter().any(|check| check.status == Status::Fail)
    }

    pub fn to_human(&self) -> String {
        let mut output = format!(
            "Engine doctor (read-only)\nroot: {}\nwork: {}\n",
            self.root.display(),
            self.work.display()
        );
        for check in &self.checks {
            output.push_str(&format!(
                "{}  {}: {}\n",
                check.status.label().to_uppercase(),
                check.name,
                check.message
            ));
            if let Some(reason) = &check.reason {
                output.push_str(&format!("      reason: {reason}\n"));
            }
        }
        output
    }
}

/// Injectable boundary for deterministic doctor tests. The production implementation below is
/// the only one that reads the host environment or starts a ProcessKit-contained version probe.
pub trait DoctorProbe {
    fn exists(&self, path: &Path) -> bool;
    fn is_dir(&self, path: &Path) -> bool;
    fn version(&self, program: &str) -> Result<String, String>;
    fn load_config(&self, work: &Path) -> Result<(), String>;
    fn load_policy(&self, work: &Path) -> Result<(), String>;
    fn mirror_scripts_dir(&self) -> Option<PathBuf>;
}

/// Collect diagnostics from the current machine. This function never creates files, contacts the
/// network, or invokes a program except through bounded `<program> --version` probes.
pub fn collect(root: PathBuf, work: PathBuf) -> DoctorReport {
    collect_with(&SystemProbe, root, work)
}

/// Pure orchestration of doctor checks over an injectable environment.
pub fn collect_with(probe: &impl DoctorProbe, root: PathBuf, work: PathBuf) -> DoctorReport {
    let mut checks = Vec::new();
    checks.push(check_pwsh(probe));
    for program in ["claude", "codex", "git", "jj"] {
        checks.push(check_program(probe, program));
    }
    checks.push(check_tools_directory(probe, &root));
    checks.push(check_config(probe, &work));
    checks.push(check_constraints(probe, &work));
    checks.extend(check_layout(probe, &work));
    DoctorReport { root, work, checks }
}

fn check_pwsh(probe: &impl DoctorProbe) -> CheckResult {
    let output = match probe.version("pwsh") {
        Ok(output) => output,
        Err(error) => {
            return CheckResult::fail(
                "pwsh",
                "PowerShell 7 (`pwsh`) is not available in PATH",
                error,
            );
        }
    };
    let Some(version) = extract_version(&output) else {
        return CheckResult::warn(
            "pwsh",
            "`pwsh` is available, but its PowerShell version could not be extracted",
        );
    };
    match version_major(&version) {
        Some(major) if major >= 7 => CheckResult::ok(
            "pwsh",
            format!("PowerShell {version} is available via `pwsh`"),
        ),
        Some(_) => CheckResult::fail(
            "pwsh",
            format!("`pwsh` reported PowerShell {version}, but PowerShell 7 or newer is required"),
            "Windows PowerShell 5.1 is not a supported replacement for PowerShell 7".to_string(),
        ),
        None => CheckResult::warn(
            "pwsh",
            "`pwsh` is available, but its PowerShell major version could not be extracted",
        ),
    }
}

fn check_program(probe: &impl DoctorProbe, program: &str) -> CheckResult {
    match probe.version(program) {
        Ok(output) => match extract_version(&output) {
            Some(version) => CheckResult::ok(
                program,
                format!("`{program}` is available (version {version})"),
            ),
            None => CheckResult::warn(
                program,
                format!("`{program}` is available, but its version could not be extracted"),
            ),
        },
        Err(error) => CheckResult::fail(
            program,
            format!("`{program}` is not available in PATH"),
            error,
        ),
    }
}

fn check_tools_directory(probe: &impl DoctorProbe, root: &Path) -> CheckResult {
    if toolscript::has_checkout_identity(root, |path| probe.exists(path)) {
        return CheckResult::ok(
            "tools",
            format!(
                "trusted checkout tools directory resolved at {}",
                root.join("tools").display()
            ),
        );
    }
    match probe.mirror_scripts_dir() {
        Some(mirror) if probe.is_dir(&mirror) => CheckResult::warn(
            "tools",
            format!(
                "root lacks the complete checkout identity; using cc-sync mirror {}",
                mirror.display()
            ),
        ),
        Some(mirror) => CheckResult::fail(
            "tools",
            "tools scripts directory could not be resolved",
            format!(
                "root is not a trusted checkout and cc-sync mirror is absent: {}",
                mirror.display()
            ),
        ),
        None => CheckResult::fail(
            "tools",
            "tools scripts directory could not be resolved",
            "root is not a trusted checkout and HOME/USERPROFILE does not identify a cc-sync mirror"
                .to_string(),
        ),
    }
}

fn check_config(probe: &impl DoctorProbe, work: &Path) -> CheckResult {
    match probe.load_config(work) {
        Ok(()) if probe.exists(&work.join("config.md")) => {
            CheckResult::ok("config", "config.md parsed successfully")
        }
        Ok(()) => CheckResult::warn(
            "config",
            "config.md is absent; the engine's documented defaults apply",
        ),
        Err(error) => CheckResult::fail("config", "config.md could not be parsed", error),
    }
}

fn check_constraints(probe: &impl DoctorProbe, work: &Path) -> CheckResult {
    let path = work.join("constraints.md");
    if !probe.exists(&path) {
        return CheckResult::ok(
            "constraints",
            "constraints.md is absent; optional policy checks are skipped",
        );
    }
    match probe.load_policy(work) {
        Ok(()) => CheckResult::ok("constraints", "constraints.md parsed successfully"),
        Err(error) => CheckResult::fail("constraints", "constraints.md could not be parsed", error),
    }
}

fn check_layout(probe: &impl DoctorProbe, work: &Path) -> [CheckResult; 3] {
    [
        check_layout_path(probe, &work.join("Tasks_Queue.md"), "queue", false),
        check_layout_path(probe, &work.join("Tasks_Done.md"), "done", false),
        check_layout_path(probe, &work.join("tasks"), "tasks", true),
    ]
}

fn check_layout_path(
    probe: &impl DoctorProbe,
    path: &Path,
    name: &str,
    directory: bool,
) -> CheckResult {
    let present = if directory {
        probe.is_dir(path)
    } else {
        probe.exists(path)
    };
    if present {
        CheckResult::ok("layout", format!("{name}: {} is present", path.display()))
    } else {
        CheckResult::fail(
            "layout",
            format!("{name}: {} is missing", path.display()),
            if directory {
                "required tasks directory is not present".to_string()
            } else {
                "required .work control-plane file is not present".to_string()
            },
        )
    }
}

fn extract_version(output: &str) -> Option<String> {
    let bytes = output.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if !bytes[index].is_ascii_digit() {
            index += 1;
            continue;
        }
        let start = index;
        while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'.') {
            index += 1;
        }
        let candidate = &output[start..index];
        return Some(candidate.trim_end_matches('.').to_string());
    }
    None
}

fn version_major(version: &str) -> Option<u32> {
    version.split('.').next()?.parse().ok()
}

struct SystemProbe;

impl DoctorProbe for SystemProbe {
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn version(&self, program: &str) -> Result<String, String> {
        let verdict = supervise::run(
            &supervise::SpawnSpec::new(program, vec!["--version".into()])
                .deadline(Some(VERSION_DEADLINE))
                .output_max_bytes(VERSION_OUTPUT_MAX_BYTES),
        );
        if verdict.reason != supervise::Reason::Ok || verdict.exit_code != Some(0) {
            let detail = if verdict.stderr.trim().is_empty() {
                verdict.outcome_reason
            } else {
                verdict.stderr.trim().to_string()
            };
            return Err(format!(
                "`{program} --version` ended with {} (exit {:?}): {detail}",
                verdict.reason.as_str(),
                verdict.exit_code
            ));
        }
        Ok(format!("{}\n{}", verdict.stdout, verdict.stderr))
    }

    fn load_config(&self, work: &Path) -> Result<(), String> {
        config::load(work)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn load_policy(&self, work: &Path) -> Result<(), String> {
        policy::load(work)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn mirror_scripts_dir(&self) -> Option<PathBuf> {
        toolscript::cc_sync_mirror_dir()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    struct Fixture {
        exists: BTreeSet<PathBuf>,
        directories: BTreeSet<PathBuf>,
        versions: BTreeMap<String, Result<String, String>>,
        config: Result<(), String>,
        policy: Result<(), String>,
        mirror: Option<PathBuf>,
    }

    impl Default for Fixture {
        fn default() -> Self {
            Self {
                exists: BTreeSet::new(),
                directories: BTreeSet::new(),
                versions: BTreeMap::new(),
                config: Ok(()),
                policy: Ok(()),
                mirror: None,
            }
        }
    }

    impl Fixture {
        fn healthy(root: &Path, work: &Path) -> Self {
            let mut fixture = Self {
                config: Ok(()),
                policy: Ok(()),
                ..Self::default()
            };
            for program in ["pwsh", "claude", "codex", "git", "jj"] {
                fixture
                    .versions
                    .insert(program.into(), Ok("version 7.4.1".into()));
            }
            for marker in toolscript::CHECKOUT_IDENTITY_MARKERS {
                fixture.exists.insert(root.join(marker));
            }
            for path in [
                "config.md",
                "constraints.md",
                "Tasks_Queue.md",
                "Tasks_Done.md",
            ] {
                fixture.exists.insert(work.join(path));
            }
            fixture.directories.insert(work.join("tasks"));
            fixture
        }
    }

    impl DoctorProbe for Fixture {
        fn exists(&self, path: &Path) -> bool {
            self.exists.contains(path)
        }

        fn is_dir(&self, path: &Path) -> bool {
            self.directories.contains(path)
        }

        fn version(&self, program: &str) -> Result<String, String> {
            self.versions
                .get(program)
                .cloned()
                .unwrap_or_else(|| Err("not on PATH".into()))
        }

        fn load_config(&self, _work: &Path) -> Result<(), String> {
            self.config.clone()
        }

        fn load_policy(&self, _work: &Path) -> Result<(), String> {
            self.policy.clone()
        }

        fn mirror_scripts_dir(&self) -> Option<PathBuf> {
            self.mirror.clone()
        }
    }

    #[test]
    fn healthy_fixture_reports_all_required_checks_as_ok() {
        let root = PathBuf::from("fixture-root");
        let work = root.join(".work");
        let report = collect_with(&Fixture::healthy(&root, &work), root, work);
        assert!(!report.has_failures());
        assert!(report.checks.iter().all(|check| check.status == Status::Ok));
    }

    #[test]
    fn mirror_fallback_and_missing_config_are_warnings_not_failures() {
        let root = PathBuf::from("fixture-root");
        let work = root.join(".work");
        let mut fixture = Fixture::healthy(&root, &work);
        for marker in toolscript::CHECKOUT_IDENTITY_MARKERS {
            fixture.exists.remove(&root.join(marker));
        }
        fixture.exists.remove(&work.join("config.md"));
        let mirror = PathBuf::from("fixture-mirror");
        fixture.directories.insert(mirror.clone());
        fixture.mirror = Some(mirror);

        let report = collect_with(&fixture, root, work);
        assert!(!report.has_failures());
        assert_eq!(
            report
                .checks
                .iter()
                .filter(|check| check.status == Status::Warn)
                .count(),
            2
        );
    }

    #[test]
    fn missing_tool_and_invalid_policy_are_independent_failures() {
        let root = PathBuf::from("fixture-root");
        let work = root.join(".work");
        let mut fixture = Fixture::healthy(&root, &work);
        fixture
            .versions
            .insert("claude".into(), Err("program not found".into()));
        fixture.policy = Err("unmatched backtick".into());

        let report = collect_with(&fixture, root, work);
        let failures = report
            .checks
            .iter()
            .filter(|check| check.status == Status::Fail)
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 2);
        assert!(failures.iter().all(|check| check.reason.is_some()));
        assert!(failures.iter().any(|check| check.name == "claude"));
        assert!(failures.iter().any(|check| check.name == "constraints"));
    }

    #[test]
    fn pwsh_five_is_rejected() {
        let root = PathBuf::from("fixture-root");
        let work = root.join(".work");
        let mut fixture = Fixture::healthy(&root, &work);
        fixture
            .versions
            .insert("pwsh".into(), Ok("PowerShell 5.1.19041.1".into()));

        let report = collect_with(&fixture, root, work);
        let pwsh = report
            .checks
            .iter()
            .find(|check| check.name == "pwsh")
            .unwrap();
        assert_eq!(pwsh.status, Status::Fail);
    }
}
