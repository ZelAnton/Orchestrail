//! Deterministic, fail-closed subset of `.work/constraints.md`.
//!
//! This replaces the policy *decisions* formerly delegated to a script: a candidate domain is
//! checked before capture, exact changed paths before commit/merge/publication, and publication
//! destination before a VCS operation. Human approvals remain an adapter concern; this module
//! never invents an implicit approval.

use std::path::Path;

use sha2::{Digest, Sha256};

use crate::command_line::parse_typed_argv;
use crate::resolvers::Domain;
use crate::work_fs::{self, MAX_CONTROL_BYTES};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Policy {
    /// Active denylist globs. Empty means the documented permissive default.
    pub denied_paths: Vec<String>,
    /// Explicit branch allowlist. Empty means the configured/detected trunk is allowed.
    pub allowed_branches: Vec<String>,
    /// Explicit remote allowlist. Empty means the default `origin` is allowed.
    pub allowed_remotes: Vec<String>,
    /// An explicit human-gated push policy prevents an automatic publication.
    pub push_requires_approval: bool,
    /// Additional typed commands that must pass in the integration worktree before publication.
    /// They supplement, rather than replace, the operator's `VERIFICATION_COMMANDS`/`SMOKE_CMD`
    /// profile and never execute through a shell.
    pub required_verification_commands: Vec<String>,
    /// Exact GitHub/check-context names that must be green on the published revision before
    /// cleanup may archive a cohort. Empty preserves the documented `CI_WATCH`-only mode.
    pub required_ci_checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    Io(String),
    Malformed(String),
    DeniedPath { path: String, pattern: String },
    DeniedBranch(String),
    DeniedRemote(String),
    ApprovalRequired,
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) | Self::Malformed(message) => f.write_str(message),
            Self::DeniedPath { path, pattern } => {
                write!(
                    f,
                    "path {path:?} intersects denied policy pattern {pattern:?}"
                )
            }
            Self::DeniedBranch(branch) => write!(f, "publication branch {branch:?} is not allowed"),
            Self::DeniedRemote(remote) => write!(f, "publication remote {remote:?} is not allowed"),
            Self::ApprovalRequired => f.write_str("publication requires explicit human approval"),
        }
    }
}

impl std::error::Error for PolicyError {}

/// A missing constraints file is the documented no-extra-restrictions mode. Other I/O errors must
/// halt mutation rather than turn a damaged policy into a permissive one.
pub fn load(work: &Path) -> Result<Policy, PolicyError> {
    let path = work.join("constraints.md");
    match work_fs::read_optional_text(work, &path, MAX_CONTROL_BYTES) {
        Ok(Some(text)) => parse(&text),
        Ok(None) => Ok(Policy::default()),
        Err(error) => Err(PolicyError::Io(format!("read constraints.md: {error}"))),
    }
}

/// Return the immutable identity of the exact active policy source.  A missing constraints file
/// has a stable empty-source identity, while any other read failure remains fail-closed.  Human
/// approval records store this value and re-check it before allowing a prior decision to apply.
pub fn snapshot_hash(work: &Path) -> Result<String, PolicyError> {
    let path = work.join("constraints.md");
    let source = match work_fs::read_optional_bytes(work, &path, MAX_CONTROL_BYTES) {
        Ok(Some(source)) => source,
        Ok(None) => Vec::new(),
        Err(error) => return Err(PolicyError::Io(format!("read constraints.md: {error}"))),
    };
    Ok(Sha256::digest(source)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Decode active bullets only. Values under an `**Пример**` block are never interpreted.
pub fn parse(text: &str) -> Result<Policy, PolicyError> {
    let mut policy = Policy::default();
    let mut section = Section::None;
    let mut active = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(title) = trimmed.strip_prefix("## ") {
            section = Section::from_heading(title);
            active = false;
            continue;
        }
        if trimmed.starts_with("**Активные ограничения**") {
            active = true;
            continue;
        }
        if trimmed.starts_with("**Пример**") || trimmed.starts_with("## ") {
            active = false;
            continue;
        }
        if !active {
            continue;
        }
        let Some(value) = trimmed.strip_prefix("- ").map(str::trim) else {
            continue;
        };
        if value.is_empty() || value.starts_with("(пусто") || value.contains("по умолчанию")
        {
            continue;
        }
        match section {
            Section::Denylist => {
                for value in split_active_values(value, "denylist")? {
                    policy
                        .denied_paths
                        .push(validate_relative_glob(&value, "denylist")?);
                }
            }
            Section::Branches => {
                if let Some(value) = value.strip_prefix("Ветки публикации:") {
                    let branches = split_active_values(value, "publication branch")?;
                    branches
                        .iter()
                        .try_for_each(|branch| validate_ref(branch, "publication branch"))?;
                    policy.allowed_branches.extend(branches);
                }
                if let Some(value) = value.strip_prefix("Remotes:") {
                    let remotes = split_active_values(value, "remote")?;
                    remotes
                        .iter()
                        .try_for_each(|remote| validate_ref(remote, "remote"))?;
                    policy.allowed_remotes.extend(remotes);
                }
            }
            Section::Push => {
                if value.starts_with("Публикация (push):") && value.contains("требует ручного")
                {
                    policy.push_requires_approval = true;
                }
            }
            Section::RequiredChecks => policy
                .required_verification_commands
                .push(parse_required_verification_command(value)?),
            Section::PublishCi => {
                for check in parse_required_ci_checks(value)? {
                    if !policy.required_ci_checks.contains(&check) {
                        policy.required_ci_checks.push(check);
                    }
                }
            }
            Section::None => {}
        }
    }
    Ok(policy)
}

impl Policy {
    /// Reject a planned conflict domain when it may overlap any denied path. The `Domain` matcher
    /// is intentionally conservative, so an ambiguous glob is denied rather than silently run.
    pub fn check_domain(&self, domain: &str) -> Result<(), PolicyError> {
        if domain.trim().is_empty() {
            return Err(PolicyError::Malformed(
                "candidate has no usable conflict domain".into(),
            ));
        }
        let candidate = Domain::parse(domain);
        for pattern in &self.denied_paths {
            if candidate.intersects(&Domain::parse(pattern)) {
                return Err(PolicyError::DeniedPath {
                    path: domain.into(),
                    pattern: pattern.clone(),
                });
            }
        }
        Ok(())
    }

    /// Check exact repository-relative changed paths before commit/merge/publish.
    pub fn check_paths(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<Path>>,
    ) -> Result<(), PolicyError> {
        for path in paths {
            let path = normalise_relative_path(path.as_ref())?;
            for pattern in &self.denied_paths {
                if glob_matches(pattern, &path) {
                    return Err(PolicyError::DeniedPath {
                        path,
                        pattern: pattern.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Validate the irreversible local publication target. This applies even when remote push is
    /// disabled: a local fast-forward still changes the configured primary branch.
    pub fn check_publication_branch(&self, branch: &str) -> Result<(), PolicyError> {
        validate_ref(branch, "publication branch")?;
        if !self.allowed_branches.is_empty()
            && !self
                .allowed_branches
                .iter()
                .any(|allowed| allowed == branch)
        {
            return Err(PolicyError::DeniedBranch(branch.into()));
        }
        Ok(())
    }

    /// Validate the branch, remote, and explicit human push gate before a remote publication.
    /// The branch rule is shared with local-only publication, while the remote and approval
    /// rules are meaningful only when a push is actually requested.
    pub fn check_publication(&self, branch: &str, remote: &str) -> Result<(), PolicyError> {
        self.check_publication_branch(branch)?;
        validate_ref(remote, "remote")?;
        if !self.allowed_remotes.is_empty()
            && !self.allowed_remotes.iter().any(|allowed| allowed == remote)
        {
            return Err(PolicyError::DeniedRemote(remote.into()));
        }
        if self.push_requires_approval {
            return Err(PolicyError::ApprovalRequired);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Section {
    None,
    Denylist,
    Branches,
    Push,
    RequiredChecks,
    PublishCi,
}

impl Section {
    fn from_heading(heading: &str) -> Self {
        if heading.starts_with("Запрещённые пути") {
            Self::Denylist
        } else if heading.starts_with("Разрешённые ветки") {
            Self::Branches
        } else if heading.starts_with("Push/merge policy") {
            Self::Push
        } else if heading.starts_with("Обязательные проверки") {
            Self::RequiredChecks
        } else if heading.starts_with("Обязательные CI-проверки публикации")
        {
            Self::PublishCi
        } else {
            Self::None
        }
    }
}

fn parse_required_verification_command(value: &str) -> Result<String, PolicyError> {
    let value = value.trim();
    let value = value
        .strip_prefix('`')
        .and_then(|command| command.strip_suffix('`'))
        .unwrap_or(value)
        .trim();
    parse_typed_argv(value).map_err(|error| {
        PolicyError::Malformed(format!(
            "required verification command {value:?} is not typed argv: {error}"
        ))
    })?;
    Ok(value.into())
}

/// Match the legacy active-bullet form: one or more backtick-quoted check names, or one bare
/// name with optional explanatory parentheses. An unmatched quote/parenthesis is rejected so a
/// damaged policy cannot silently weaken a required CI gate.
fn parse_required_ci_checks(value: &str) -> Result<Vec<String>, PolicyError> {
    let quote_count = value.chars().filter(|character| *character == '`').count();
    if quote_count % 2 != 0 {
        return Err(PolicyError::Malformed(format!(
            "unmatched backtick in required CI check {value:?}"
        )));
    }
    if quote_count > 0 {
        let mut checks = Vec::new();
        let mut rest = value;
        while let Some(open) = rest.find('`') {
            let after_open = &rest[open + 1..];
            let close = after_open.find('`').expect("backticks were counted above");
            checks.push(validate_required_ci_check(&after_open[..close])?);
            rest = &after_open[close + 1..];
        }
        return Ok(checks);
    }

    let mut bare = String::new();
    let mut parentheses = 0_u32;
    for character in value.chars() {
        match character {
            '(' => parentheses = parentheses.saturating_add(1),
            ')' if parentheses == 0 => {
                return Err(PolicyError::Malformed(format!(
                    "unmatched parenthesis in required CI check {value:?}"
                )));
            }
            ')' => parentheses = parentheses.saturating_sub(1),
            _ if parentheses == 0 => bare.push(character),
            _ => {}
        }
    }
    if parentheses != 0 {
        return Err(PolicyError::Malformed(format!(
            "unmatched parenthesis in required CI check {value:?}"
        )));
    }
    Ok(vec![validate_required_ci_check(
        bare.trim().trim_matches(',').trim(),
    )?])
}

fn validate_required_ci_check(value: &str) -> Result<String, PolicyError> {
    let value = value.trim();
    if value.is_empty() || value.contains(['\0', '\n', '\r']) {
        return Err(PolicyError::Malformed(format!(
            "invalid required CI check name {value:?}"
        )));
    }
    Ok(value.into())
}

/// Decode the shared active-bullet list syntax used by denylist and publication targets. The
/// template commonly uses backticks to make comma-separated values unambiguous; accepting that
/// form as one literal glob would silently disable every individual restriction.
fn split_active_values(value: &str, what: &str) -> Result<Vec<String>, PolicyError> {
    let quote_count = value.chars().filter(|character| *character == '`').count();
    if quote_count % 2 != 0 {
        return Err(PolicyError::Malformed(format!(
            "unmatched backtick in {what} list {value:?}"
        )));
    }
    let values = if quote_count > 0 {
        let mut values = Vec::new();
        let mut rest = value;
        while let Some(open) = rest.find('`') {
            let after_open = &rest[open + 1..];
            let close = after_open.find('`').expect("backticks were counted above");
            values.push(after_open[..close].trim().to_string());
            rest = &after_open[close + 1..];
        }
        values
    } else {
        value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_string)
            .collect()
    };
    if values.is_empty() || values.iter().any(|item| item.is_empty()) {
        return Err(PolicyError::Malformed(format!(
            "{what} list has an empty value: {value:?}"
        )));
    }
    Ok(values)
}

fn validate_relative_glob(value: &str, what: &str) -> Result<String, PolicyError> {
    let value = value.trim();
    if value.is_empty()
        || value.starts_with(['/', '\\'])
        || value.contains(':')
        || value.contains('\0')
        || value.split(['/', '\\']).any(|segment| segment == "..")
    {
        return Err(PolicyError::Malformed(format!(
            "invalid {what} path glob {value:?}"
        )));
    }
    Ok(value.into())
}

fn normalise_relative_path(path: &Path) -> Result<String, PolicyError> {
    if path.is_absolute() {
        return Err(PolicyError::Malformed(format!(
            "changed path must be repository-relative: {}",
            path.display()
        )));
    }
    let value = path.to_string_lossy().replace('\\', "/");
    validate_relative_glob(&value, "changed")
}

fn validate_ref(value: &str, what: &str) -> Result<(), PolicyError> {
    if value.trim().is_empty() || value.starts_with('-') || value.contains(['\0', '\n', '\r']) {
        Err(PolicyError::Malformed(format!("invalid {what} {value:?}")))
    } else {
        Ok(())
    }
}

/// Minimal path-aware glob matcher for exact changed-file policy checks. `*` and `?` never cross
/// a slash; `**/` may match zero or more complete path segments, so `**/*.pem` matches both
/// `cert.pem` and `nested/cert.pem`. The parser rejects absolute/traversal inputs before this runs.
pub(crate) fn glob_matches(pattern: &str, path: &str) -> bool {
    fn matches(pattern: &[char], path: &[char], pi: usize, si: usize) -> bool {
        if pi == pattern.len() {
            return si == path.len();
        }
        if pattern[pi] == '*' {
            if pattern.get(pi + 1) == Some(&'*') {
                let after_stars = pi + 2;
                if pattern.get(after_stars) == Some(&'/') {
                    // `**/` includes the empty directory prefix and every whole directory prefix.
                    for next in si..=path.len() {
                        if (next == si || (next > 0 && path[next - 1] == '/'))
                            && matches(pattern, path, after_stars + 1, next)
                        {
                            return true;
                        }
                    }
                    return false;
                }
                return (si..=path.len()).any(|next| matches(pattern, path, after_stars, next));
            }
            let mut next = si;
            loop {
                if matches(pattern, path, pi + 1, next) {
                    return true;
                }
                if next == path.len() || path[next] == '/' {
                    return false;
                }
                next += 1;
            }
        }
        if pattern[pi] == '?' {
            return si < path.len() && path[si] != '/' && matches(pattern, path, pi + 1, si + 1);
        }
        si < path.len() && pattern[pi] == path[si] && matches(pattern, path, pi + 1, si + 1)
    }

    matches(
        &pattern.replace('\\', "/").chars().collect::<Vec<_>>(),
        &path.replace('\\', "/").chars().collect::<Vec<_>>(),
        0,
        0,
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn example_bullets_are_never_enforced() {
        let policy = parse(
            "## Запрещённые пути (denylist)\n**Активные ограничения**\n- (пусто — запрещённых путей нет)\n**Пример**\n- secrets/**\n",
        )
        .unwrap();
        assert!(policy.denied_paths.is_empty());
        assert!(policy.check_paths([PathBuf::from("secrets/key")]).is_ok());
    }

    #[test]
    fn denied_domain_and_exact_changed_path_are_blocked() {
        let policy = parse(
            "## Запрещённые пути (denylist)\n**Активные ограничения**\n- infra/**\n- **/*.pem\n",
        )
        .unwrap();
        assert!(matches!(
            policy.check_domain("infra/deploy/**"),
            Err(PolicyError::DeniedPath { .. })
        ));
        assert!(matches!(
            policy.check_paths([PathBuf::from("cert.pem")]),
            Err(PolicyError::DeniedPath { .. })
        ));
        assert!(
            policy
                .check_paths([PathBuf::from("engine/src/lib.rs")])
                .is_ok()
        );
    }

    #[test]
    fn code_quoted_policy_lists_are_split_into_independent_restrictions() {
        let policy = parse(
            "## Запрещённые пути (denylist)\n**Активные ограничения**\n- `infra/**`, `**/*.pem`\n## Разрешённые ветки и remotes\n**Активные ограничения**\n- Ветки публикации: `main`, `release`\n- Remotes: `origin`, `upstream`\n",
        )
        .unwrap();
        assert_eq!(policy.denied_paths, vec!["infra/**", "**/*.pem"]);
        assert_eq!(policy.allowed_branches, vec!["main", "release"]);
        assert_eq!(policy.allowed_remotes, vec!["origin", "upstream"]);
        assert!(matches!(
            policy.check_paths([PathBuf::from("cert.pem")]),
            Err(PolicyError::DeniedPath { .. })
        ));
    }

    #[test]
    fn publication_allowlists_and_human_gate_are_explicit() {
        let policy = parse(
            "## Разрешённые ветки и remotes\n**Активные ограничения**\n- Ветки публикации: main\n- Remotes: origin\n## Push/merge policy\n**Активные ограничения**\n- Публикация (push): требует ручного подтверждения\n",
        )
        .unwrap();
        assert!(matches!(
            policy.check_publication("release", "origin"),
            Err(PolicyError::DeniedBranch(_))
        ));
        assert!(matches!(
            policy.check_publication("main", "fork"),
            Err(PolicyError::DeniedRemote(_))
        ));
        assert_eq!(
            policy.check_publication("main", "origin"),
            Err(PolicyError::ApprovalRequired)
        );
    }

    #[test]
    fn publication_branch_rule_still_applies_when_no_remote_push_is_requested() {
        let policy = parse(
            "## Разрешённые ветки и remotes\n**Активные ограничения**\n- Ветки публикации: main\n- Remotes: origin\n",
        )
        .unwrap();
        assert!(matches!(
            policy.check_publication_branch("release"),
            Err(PolicyError::DeniedBranch(branch)) if branch == "release"
        ));
        assert_eq!(policy.check_publication_branch("main"), Ok(()));
    }

    #[test]
    fn policy_snapshot_hash_changes_with_the_exact_source_and_handles_no_policy() {
        let work =
            std::env::temp_dir().join(format!("orchestrail-policy-hash-{}", std::process::id()));
        let _ = fs::remove_dir_all(&work);
        fs::create_dir_all(&work).unwrap();
        let empty = snapshot_hash(&work).unwrap();
        fs::write(work.join("constraints.md"), "# policy\n").unwrap();
        let present = snapshot_hash(&work).unwrap();
        assert_ne!(empty, present);
        assert_eq!(present.len(), 64);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn required_ci_checks_use_active_bullets_and_preserve_exact_names() {
        let policy = parse(
            "## Обязательные CI-проверки публикации\n**Активные ограничения**\n- `validate`, `crash matrix`\n- lint (required on release)\n**Пример**\n- `ignored`\n",
        )
        .unwrap();
        assert_eq!(
            policy.required_ci_checks,
            vec!["validate", "crash matrix", "lint"]
        );
        assert!(parse(
            "## Обязательные CI-проверки публикации\n**Активные ограничения**\n- `unterminated\n"
        )
        .is_err());
    }

    #[test]
    fn required_verification_commands_are_typed_and_ignore_example_bullets() {
        let policy = parse(
            "## Обязательные проверки\n**Активные ограничения**\n- `cargo fmt --check`\n- cargo test -p orchestrail-engine\n**Пример**\n- `npm test`\n",
        )
        .unwrap();
        assert_eq!(
            policy.required_verification_commands,
            vec!["cargo fmt --check", "cargo test -p orchestrail-engine"]
        );
        assert!(
            parse(
                "## Обязательные проверки\n**Активные ограничения**\n- cargo test && echo unsafe\n"
            )
            .is_err()
        );
    }

    #[test]
    fn exact_path_globs_do_not_treat_a_leading_double_star_as_every_path() {
        assert!(glob_matches("**/*.pem", "cert.pem"));
        assert!(glob_matches("**/*.pem", "nested/cert.pem"));
        assert!(!glob_matches("**/*.pem", "engine/src/lib.rs"));
        assert!(glob_matches("infra/**", "infra/deploy/file.yml"));
        assert!(!glob_matches("infra/**", "infrastructure/file.yml"));
    }
}
