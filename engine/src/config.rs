//! Strict parsing of the deterministic processor's `.work/config.md` keys.
//!
//! The legacy file is intentionally human-editable Markdown-like `KEY: value` text. We accept
//! comments and unrelated documented keys, but every key that affects engine control flow is
//! decoded once into [`EngineConfig`]. Duplicate active keys and malformed values fail closed.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use crate::codex::Sandbox;
use crate::command_line::{parse_typed_argv, validate_direct_program};
use crate::processor::{ProcessorConfig, ProcessorError};
use crate::resolvers::{CodexCoder, CodexReviewer};
use crate::work_fs::{self, MAX_CONTROL_BYTES};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub processor: ProcessorConfig,
    pub events_outbox: bool,
    pub knowledge_base: bool,
    /// Expire unconfirmed singleton knowledge entries after this many completed cohorts.
    pub knowledge_ttl_batches: u64,
    /// Keep at most this many knowledge entries in each curated area.
    pub knowledge_cap_per_area: usize,
    pub push: bool,
    pub ci_watch: bool,
    /// Require a byte-identical, merge-free publication history.  The current native port must
    /// recognise this legacy key even while its typed VCS surface cannot yet perform the required
    /// crash-safe rewrite; callers reject `true` before taking an owner lease rather than silently
    /// publishing the merge topology.
    pub publish_linear_history: bool,
    pub main_branch: Option<String>,
    pub verification_mode: VerificationMode,
    /// Whether the operator wrote `VERIFICATION_MODE` rather than relying on the historical
    /// implicit default. Phase 4 uses this to preserve explicit-disable precedence over a
    /// mechanically proved docs-only exemption.
    pub verification_mode_explicit: bool,
    /// Exact operator-owned shell commands for Phase 4 verification. They are parsed as JSON so
    /// a Markdown value cannot be split ambiguously into executable fragments.
    pub verification_commands: Vec<String>,
    /// Whether the verification profile also runs inside every task review/fix cycle (phases
    /// 2.5/2.8), not only at the Phase-4 publication gate. Off by default: enabling it changes the
    /// cost and the artifact content of every review round, so it stays an explicit operator
    /// decision.
    pub review_cycle_verification: bool,
    /// Optional narrower profile for the review/fix cycle, so an operator can gate each round on
    /// a cheap lint/build subset while the full (test-bearing) profile still guards publication.
    /// Empty means the cycle reuses `VERIFICATION_COMMANDS`/`SMOKE_CMD`.
    pub review_cycle_verification_commands: Vec<String>,
    /// Legacy one-command fallback used only when `VERIFICATION_COMMANDS` is absent.
    pub smoke_cmd: Option<String>,
    /// Optional typed argv for best-effort operator notifications. An absent or empty legacy
    /// `NOTIFY_CMD` remains a successful no-op; shell grammar is rejected before a child exists.
    pub notify_command: Option<Vec<String>>,
    pub call_deadline_secs: u64,
    pub call_output_max_bytes: usize,
    pub publish_ci_deadline_secs: u64,
    pub publish_ci_backoff_secs: u64,
    pub review_min_passes: u32,
    pub quarantine_max_attempts: u32,
    pub approval_deadline_secs: u64,
    pub reviewer_tiering: bool,
    pub codex: CodexConfig,
}

/// Strictly resolved Codex routing/runtime configuration. The two routing flags may inherit an
/// environment value only when their file entry is absent or empty; the rest is file-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexConfig {
    pub coder: CodexCoder,
    pub reviewer: CodexReviewer,
    pub ci_fix: bool,
    pub reasoning: CodexReasoning,
    pub sandbox: Sandbox,
    pub network: bool,
    pub model: Option<String>,
    pub command: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexReasoning {
    Auto,
    Low,
    Medium,
    High,
    XHigh,
}

impl CodexReasoning {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "auto" => Self::Auto,
            "low" => Self::Low,
            "medium" => Self::Medium,
            "high" => Self::High,
            "xhigh" => Self::XHigh,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationMode {
    Disabled,
    Auto,
    Required,
}

/// Policy-required checks are an executable profile even when `config.md` has no verification
/// settings. An operator's explicit `VERIFICATION_MODE: disabled` remains the documented
/// higher-priority exemption.
pub fn mode_with_required_policy_commands(
    mode: VerificationMode,
    mode_was_explicit: bool,
    has_required_policy_commands: bool,
) -> VerificationMode {
    if has_required_policy_commands && !mode_was_explicit && mode == VerificationMode::Disabled {
        VerificationMode::Auto
    } else {
        mode
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            processor: ProcessorConfig::default(),
            events_outbox: true,
            knowledge_base: true,
            knowledge_ttl_batches: 8,
            knowledge_cap_per_area: 12,
            push: true,
            ci_watch: true,
            publish_linear_history: false,
            main_branch: None,
            verification_mode: VerificationMode::Disabled,
            verification_mode_explicit: false,
            verification_commands: Vec::new(),
            review_cycle_verification: false,
            review_cycle_verification_commands: Vec::new(),
            smoke_cmd: None,
            notify_command: None,
            call_deadline_secs: 1800,
            call_output_max_bytes: 1024 * 1024,
            publish_ci_deadline_secs: 1800,
            publish_ci_backoff_secs: 30,
            review_min_passes: 2,
            quarantine_max_attempts: 3,
            approval_deadline_secs: 86_400,
            reviewer_tiering: true,
            codex: CodexConfig {
                coder: CodexCoder::Off,
                reviewer: CodexReviewer::Off,
                ci_fix: false,
                reasoning: CodexReasoning::Auto,
                sandbox: Sandbox::WorkspaceWrite,
                network: true,
                model: None,
                command: "codex".into(),
            },
        }
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Io(io::Error),
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "config I/O error: {error}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

impl From<io::Error> for ConfigError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Read `config.md`. A missing file supplies no fields, so only the explicitly documented
/// Codex/knowledge-base environment fallbacks can still take effect. Any other I/O error is
/// surfaced and blocks a mutating run.
pub fn load(work: &Path) -> Result<EngineConfig, ConfigError> {
    load_with_environment(work, |key| std::env::var(key).ok())
}

fn load_with_environment(
    work: &Path,
    environment: impl Fn(&str) -> Option<String>,
) -> Result<EngineConfig, ConfigError> {
    match fs::symlink_metadata(work) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return parse_with_environment("", environment);
        }
        Err(error) => return Err(error.into()),
    }
    let path = work.join("config.md");
    match work_fs::read_optional_text(work, &path, MAX_CONTROL_BYTES) {
        Ok(Some(text)) => parse_with_environment(&text, environment),
        Ok(None) => parse_with_environment("", environment),
        Err(error) => Err(error.into()),
    }
}

/// Parse active `KEY: value` lines. An active line starts at column zero; commented lines and
/// explanatory indented prose are ignored. Inline comments after a value are supported.
pub fn parse(text: &str) -> Result<EngineConfig, ConfigError> {
    parse_with_environment(text, |_| None)
}

/// Parse configuration with an explicit environment source. It keeps the ambient process
/// environment at the outer [`load`] boundary, making every environment-fallback rule testable
/// and keeping pure parser callers deterministic.
fn parse_with_environment(
    text: &str,
    environment: impl Fn(&str) -> Option<String>,
) -> Result<EngineConfig, ConfigError> {
    let fields = active_fields(text)?;
    let mut config = EngineConfig::default();
    let max_parallel =
        optional_u64(&fields, "MAX_PARALLEL")?.unwrap_or(config.processor.max_parallel as u64);
    config.processor.max_parallel =
        usize::try_from(max_parallel).map_err(|_| invalid("MAX_PARALLEL is too large"))?;
    let cohort_size =
        optional_u64(&fields, "COHORT_SIZE")?.unwrap_or_else(|| max_parallel.saturating_mul(3));
    config.processor.cohort_size =
        u32::try_from(cohort_size).map_err(|_| invalid("COHORT_SIZE is too large"))?;
    config.processor.cohort_max_age_minutes =
        optional_u64(&fields, "COHORT_MAX_AGE")?.unwrap_or(config.processor.cohort_max_age_minutes);
    config.processor.review_loop_max =
        optional_u32(&fields, "REVIEW_LOOP_MAX")?.unwrap_or(config.processor.review_loop_max);
    config.processor.integration_loop_max = optional_u32(&fields, "INTEGRATION_LOOP_MAX")?
        .unwrap_or(config.processor.integration_loop_max);
    config.processor.ci_fix_max =
        optional_u32(&fields, "CI_FIX_MAX")?.unwrap_or(config.processor.ci_fix_max);
    config.processor.stagnation_limit =
        optional_u32(&fields, "STAGNATION_LIMIT")?.unwrap_or(config.processor.stagnation_limit);
    config.processor.leaf_max_attempts =
        optional_u32(&fields, "CALL_MAX_ATTEMPTS")?.unwrap_or(config.processor.leaf_max_attempts);
    config.processor.cohort_budget_secs =
        optional_u64(&fields, "COHORT_BUDGET_SEC")?.filter(|budget| *budget > 0);
    config.processor.cohort_token_budget =
        optional_u64(&fields, "COHORT_TOKEN_BUDGET")?.filter(|budget| *budget > 0);
    config.processor.cohort_token_budget_strict =
        optional_true_false(&fields, "COHORT_TOKEN_BUDGET_STRICT")?
            .unwrap_or(config.processor.cohort_token_budget_strict);
    config.events_outbox = optional_bool(&fields, "EVENTS_OUTBOX")?.unwrap_or(config.events_outbox);
    config.processor.events_outbox_enabled = config.events_outbox;
    config.knowledge_base = resolve_knowledge_base(&fields, &environment)?;
    config.knowledge_ttl_batches =
        optional_positive_u64(&fields, "KB_TTL")?.unwrap_or(config.knowledge_ttl_batches);
    config.knowledge_cap_per_area = usize::try_from(
        optional_positive_u64(&fields, "KB_CAP")?.unwrap_or(config.knowledge_cap_per_area as u64),
    )
    .map_err(|_| invalid("KB_CAP is too large"))?;
    config.push = optional_bool(&fields, "PUSH")?.unwrap_or(config.push);
    config.ci_watch = optional_bool(&fields, "CI_WATCH")?.unwrap_or(config.ci_watch);
    config.publish_linear_history =
        optional_bool(&fields, "PUBLISH_LINEAR_HISTORY")?.unwrap_or(config.publish_linear_history);
    config.call_deadline_secs =
        optional_u64(&fields, "CALL_DEADLINE_SEC")?.unwrap_or(config.call_deadline_secs);
    config.call_output_max_bytes =
        optional_usize(&fields, "CALL_OUTPUT_MAX_BYTES")?.unwrap_or(config.call_output_max_bytes);
    config.publish_ci_deadline_secs = optional_u64(&fields, "PUBLISH_CI_DEADLINE_SEC")?
        .unwrap_or(config.publish_ci_deadline_secs);
    config.publish_ci_backoff_secs =
        optional_u64(&fields, "PUBLISH_CI_BACKOFF_SEC")?.unwrap_or(config.publish_ci_backoff_secs);
    config.review_min_passes =
        optional_u32(&fields, "REVIEW_MIN_PASSES")?.unwrap_or(config.review_min_passes);
    config.quarantine_max_attempts =
        optional_u32(&fields, "QUARANTINE_MAX_ATTEMPTS")?.unwrap_or(config.quarantine_max_attempts);
    config.approval_deadline_secs =
        optional_u64(&fields, "APPROVAL_DEADLINE_SEC")?.unwrap_or(config.approval_deadline_secs);
    config.reviewer_tiering =
        optional_bool(&fields, "REVIEWER_TIERING")?.unwrap_or(config.reviewer_tiering);
    config.main_branch = fields
        .get("MAIN_BRANCH")
        .cloned()
        .filter(|value| !value.is_empty());
    let configured_verification_mode = fields.get("VERIFICATION_MODE").map(String::as_str);
    config.verification_mode_explicit = configured_verification_mode.is_some();
    config.verification_commands = match fields.get("VERIFICATION_COMMANDS") {
        None => Vec::new(),
        Some(value) => parse_verification_commands("VERIFICATION_COMMANDS", value)?,
    };
    config.review_cycle_verification = optional_bool(&fields, "REVIEW_CYCLE_VERIFICATION")?
        .unwrap_or(config.review_cycle_verification);
    config.review_cycle_verification_commands = match fields
        .get("REVIEW_CYCLE_VERIFICATION_COMMANDS")
    {
        None => Vec::new(),
        Some(value) => parse_verification_commands("REVIEW_CYCLE_VERIFICATION_COMMANDS", value)?,
    };
    config.smoke_cmd = configured_value(&fields, "SMOKE_CMD")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    config.notify_command = configured_value(&fields, "NOTIFY_CMD")
        .map(|command| {
            parse_typed_argv(command).map_err(|error| {
                invalid(format!(
                    "NOTIFY_CMD must name one executable with typed arguments: {error}"
                ))
            })
        })
        .transpose()?;
    // Legacy `verification.ps1` treats an omitted mode as an implicit `auto` when an operator
    // supplied a concrete profile.  Only a completely absent profile defaults to the historical
    // disabled/exempt path.  Collapsing both cases into `Disabled` silently skipped an explicit
    // `VERIFICATION_COMMANDS`/`SMOKE_CMD` gate before publication.
    config.verification_mode = match configured_verification_mode {
        Some("disabled") => VerificationMode::Disabled,
        Some("auto") => VerificationMode::Auto,
        Some("required") => VerificationMode::Required,
        None if !config.verification_commands.is_empty() || config.smoke_cmd.is_some() => {
            VerificationMode::Auto
        }
        None => VerificationMode::Disabled,
        Some(value) => {
            return Err(invalid(format!(
                "VERIFICATION_MODE must be auto, required, or disabled; got {value:?}"
            )));
        }
    };
    // An explicit disabled mode is an operator override: legacy keeps it exempt even when old
    // shell-shaped profile text remains in config.md. Validate argv only when a profile can
    // actually reach a ProcessKit launch.
    if config.verification_mode != VerificationMode::Disabled {
        validate_typed_verification_profile(
            &config.verification_commands,
            config.smoke_cmd.as_deref(),
        )?;
    }
    // The review-cycle subset is a new key, so no legacy shell-shaped text can exist for it and
    // there is nothing to keep compatible: validate typed argv whenever it is present, even while
    // the gate itself is off, rather than deferring the failure to the first enabled cohort.
    for command in &config.review_cycle_verification_commands {
        parse_typed_argv(command).map_err(|error| {
            invalid(format!(
                "REVIEW_CYCLE_VERIFICATION_COMMANDS entries must name one executable with typed arguments: {error}"
            ))
        })?;
    }
    // An enabled gate with nothing to execute would silently degrade to "always green" on every
    // 2.5/2.8 cycle, which is exactly the failure this option exists to prevent. `SMOKE_CMD` alone
    // is a legal profile here, so validate its argv even when Phase 4 exempted it as legacy text.
    if config.review_cycle_verification {
        if config.review_cycle_verification_commands.is_empty()
            && config.verification_commands.is_empty()
            && config.smoke_cmd.is_none()
        {
            return Err(invalid(
                "REVIEW_CYCLE_VERIFICATION requires REVIEW_CYCLE_VERIFICATION_COMMANDS, VERIFICATION_COMMANDS, or SMOKE_CMD",
            ));
        }
        validate_typed_verification_profile(
            &config.verification_commands,
            config.smoke_cmd.as_deref(),
        )?;
    }
    config.codex.coder = resolve_codex_coder(&fields, &environment)?;
    config.codex.reviewer = resolve_codex_reviewer(&fields, &environment)?;
    config.codex.ci_fix = optional_on_off(&fields, "CODEX_CIFIX")?.unwrap_or(config.codex.ci_fix);
    config.codex.reasoning = match configured_value(&fields, "CODEX_REASONING") {
        None => config.codex.reasoning,
        Some(value) => CodexReasoning::parse(value).ok_or_else(|| {
            invalid(format!(
                "CODEX_REASONING must be auto, low, medium, high, or xhigh; got {value:?}"
            ))
        })?,
    };
    config.codex.sandbox = match configured_value(&fields, "CODEX_SANDBOX") {
        None => config.codex.sandbox,
        Some("read-only") => Sandbox::ReadOnly,
        Some("workspace-write") => Sandbox::WorkspaceWrite,
        Some(value) => {
            return Err(invalid(format!(
                "CODEX_SANDBOX must be read-only or workspace-write; got {value:?}"
            )));
        }
    };
    config.codex.network =
        optional_on_off(&fields, "CODEX_NETWORK")?.unwrap_or(config.codex.network);
    config.codex.model = configured_value(&fields, "CODEX_MODEL").map(str::to_string);
    if let Some(command) = configured_value(&fields, "CODEX_CMD") {
        validate_direct_program(command).map_err(|error| {
            invalid(format!("CODEX_CMD must name a direct executable: {error}"))
        })?;
        config.codex.command = command.to_string();
    }

    config
        .processor
        .validate()
        .map_err(processor_config_error)?;
    if config.call_deadline_secs == 0 {
        return Err(invalid("CALL_DEADLINE_SEC must be at least 1"));
    }
    if config.call_output_max_bytes == 0 {
        return Err(invalid("CALL_OUTPUT_MAX_BYTES must be at least 1"));
    }
    if config.publish_ci_deadline_secs == 0 || config.publish_ci_backoff_secs == 0 {
        return Err(invalid(
            "PUBLISH_CI_DEADLINE_SEC and PUBLISH_CI_BACKOFF_SEC must be at least 1",
        ));
    }
    if config.review_min_passes == 0
        || config.quarantine_max_attempts == 0
        || config.approval_deadline_secs == 0
    {
        return Err(invalid(
            "REVIEW_MIN_PASSES, QUARANTINE_MAX_ATTEMPTS, and APPROVAL_DEADLINE_SEC must be at least 1",
        ));
    }
    if let Some(branch) = &config.main_branch
        && (branch.starts_with('-') || branch.contains(['\0', '\n', '\r']))
    {
        return Err(invalid(format!("invalid MAIN_BRANCH {branch:?}")));
    }
    Ok(config)
}

fn configured_value<'a>(fields: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    fields
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

fn resolve_codex_coder(
    fields: &BTreeMap<String, String>,
    environment: &impl Fn(&str) -> Option<String>,
) -> Result<CodexCoder, ConfigError> {
    if let Some(value) = configured_value(fields, "CODEX_CODER") {
        return CodexCoder::parse(value).ok_or_else(|| {
            invalid(format!(
                "CODEX_CODER must be off, fast, or fast+std; got {value:?}"
            ))
        });
    }
    Ok(environment("CODEX_CODER")
        .as_deref()
        .and_then(CodexCoder::parse)
        .unwrap_or(CodexCoder::Off))
}

fn resolve_knowledge_base(
    fields: &BTreeMap<String, String>,
    environment: &impl Fn(&str) -> Option<String>,
) -> Result<bool, ConfigError> {
    if let Some(value) = configured_value(fields, "KB") {
        return match value {
            "on" => Ok(true),
            "off" => Ok(false),
            _ => Err(invalid(format!("KB must be on/off; got {value:?}"))),
        };
    }
    Ok(environment("KB")
        .as_deref()
        .and_then(|value| match value {
            "on" => Some(true),
            "off" => Some(false),
            _ => None,
        })
        .unwrap_or(true))
}

fn resolve_codex_reviewer(
    fields: &BTreeMap<String, String>,
    environment: &impl Fn(&str) -> Option<String>,
) -> Result<CodexReviewer, ConfigError> {
    if let Some(value) = configured_value(fields, "CODEX_REVIEWER") {
        return CodexReviewer::parse(value).ok_or_else(|| {
            invalid(format!(
                "CODEX_REVIEWER must be off, fast, fast+std, or deep; got {value:?}"
            ))
        });
    }
    Ok(environment("CODEX_REVIEWER")
        .as_deref()
        .and_then(CodexReviewer::parse)
        .unwrap_or(CodexReviewer::Off))
}

fn active_fields(text: &str) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut fields = BTreeMap::new();
    for (line_number, line) in text.lines().enumerate() {
        if line.starts_with('#') || line.starts_with(char::is_whitespace) {
            continue;
        }
        let Some((key, raw_value)) = line.split_once(':') else {
            continue;
        };
        if !key
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'_')
            || key.is_empty()
        {
            continue;
        }
        let value = strip_inline_comment(raw_value).trim();
        if fields.insert(key.to_string(), value.to_string()).is_some() {
            return Err(invalid(format!(
                "duplicate active config key {key} (line {})",
                line_number + 1
            )));
        }
    }
    Ok(fields)
}

/// Strip a Markdown-style inline comment without corrupting a literal `#` in a typed command
/// or JSON verification profile.  Comments start at an unquoted hash which is either the first
/// value character or follows whitespace; a literal hash can always be kept by quoting it.
fn strip_inline_comment(value: &str) -> &str {
    let mut quote = None;
    let mut escaped = false;
    let mut previous_was_whitespace = true;

    for (index, character) in value.char_indices() {
        match quote {
            Some('"') => {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    quote = None;
                }
            }
            Some('\'') => {
                if character == '\'' {
                    quote = None;
                }
            }
            Some(_) => unreachable!("only single and double quotes enter config comment parsing"),
            None => match character {
                '"' | '\'' => quote = Some(character),
                '#' if previous_was_whitespace => return &value[..index],
                _ => {}
            },
        }
        previous_was_whitespace = character.is_whitespace();
    }
    value
}

/// Decode one JSON command-array key. `key` is echoed into every diagnostic so the Phase-4
/// profile and the review/fix-cycle subset cannot report each other's malformed value.
fn parse_verification_commands(key: &str, value: &str) -> Result<Vec<String>, ConfigError> {
    let commands: Vec<String> = serde_json::from_str(value).map_err(|error| {
        invalid(format!(
            "{key} must be a JSON array of command strings: {error}"
        ))
    })?;
    if commands.is_empty() {
        return Err(invalid(format!(
            "{key} must contain at least one non-empty command"
        )));
    }
    for command in &commands {
        if command.trim().is_empty() || command.contains('\0') {
            return Err(invalid(format!(
                "{key} entries must be non-empty and contain no NUL byte"
            )));
        }
    }
    Ok(commands)
}

fn validate_typed_verification_profile(
    commands: &[String],
    smoke_cmd: Option<&str>,
) -> Result<(), ConfigError> {
    for command in commands {
        parse_typed_argv(command).map_err(|error| {
            invalid(format!(
                "VERIFICATION_COMMANDS entries must name one executable with typed arguments: {error}"
            ))
        })?;
    }
    if commands.is_empty()
        && let Some(smoke) = smoke_cmd
    {
        parse_typed_argv(smoke).map_err(|error| {
            invalid(format!(
                "SMOKE_CMD must name one executable with typed arguments: {error}"
            ))
        })?;
    }
    Ok(())
}

fn optional_u64(fields: &BTreeMap<String, String>, key: &str) -> Result<Option<u64>, ConfigError> {
    fields
        .get(key)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| invalid(format!("{key} must be an unsigned integer; got {value:?}")))
        })
        .transpose()
}

fn optional_positive_u64(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<u64>, ConfigError> {
    let value = optional_u64(fields, key)?;
    if value == Some(0) {
        return Err(invalid(format!("{key} must be at least 1")));
    }
    Ok(value)
}

fn optional_u32(fields: &BTreeMap<String, String>, key: &str) -> Result<Option<u32>, ConfigError> {
    fields
        .get(key)
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| invalid(format!("{key} must be an unsigned integer; got {value:?}")))
        })
        .transpose()
}

fn optional_usize(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<usize>, ConfigError> {
    fields
        .get(key)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| invalid(format!("{key} must be an unsigned integer; got {value:?}")))
        })
        .transpose()
}

fn optional_bool(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<bool>, ConfigError> {
    fields
        .get(key)
        .map(|value| match value.as_str() {
            "true" | "on" => Ok(true),
            "false" | "off" => Ok(false),
            _ => Err(invalid(format!(
                "{key} must be true/false or on/off; got {value:?}"
            ))),
        })
        .transpose()
}

fn optional_on_off(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<bool>, ConfigError> {
    configured_value(fields, key)
        .map(|value| match value {
            "on" => Ok(true),
            "off" => Ok(false),
            _ => Err(invalid(format!("{key} must be on/off; got {value:?}"))),
        })
        .transpose()
}

fn optional_true_false(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<bool>, ConfigError> {
    configured_value(fields, key)
        .map(|value| match value {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(invalid(format!("{key} must be true/false; got {value:?}"))),
        })
        .transpose()
}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(message.into())
}

fn processor_config_error(error: ProcessorError) -> ConfigError {
    ConfigError::Invalid(format!("invalid processor configuration: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn defaults_use_three_times_max_parallel_only_when_size_is_absent() {
        let parsed = parse("MAX_PARALLEL: 5\n# COHORT_SIZE: 7\n").unwrap();
        assert_eq!(parsed.processor.max_parallel, 5);
        assert_eq!(parsed.processor.cohort_size, 15);
        let explicit = parse("MAX_PARALLEL: 5\nCOHORT_SIZE: 4\n").unwrap();
        assert_eq!(explicit.processor.cohort_size, 4);
    }

    #[test]
    fn parser_handles_documented_boolean_spellings_and_zero_budget() {
        let parsed = parse(
            "EVENTS_OUTBOX: off # disabled\nKB: on\nKB_TTL: 5\nKB_CAP: 7\nPUSH: false\nCI_WATCH: true\nPUBLISH_LINEAR_HISTORY: on\nCOHORT_BUDGET_SEC: 0\nCOHORT_TOKEN_BUDGET: 0\n",
        )
        .unwrap();
        assert!(!parsed.events_outbox);
        assert!(parsed.knowledge_base);
        assert_eq!(parsed.knowledge_ttl_batches, 5);
        assert_eq!(parsed.knowledge_cap_per_area, 7);
        assert!(!parsed.push);
        assert!(parsed.ci_watch);
        assert!(parsed.publish_linear_history);
        assert_eq!(parsed.processor.cohort_budget_secs, None);
        assert_eq!(parsed.processor.cohort_token_budget, None);
        assert!(!parsed.processor.events_outbox_enabled);
    }

    #[test]
    fn parses_a_positive_cohort_token_budget() {
        let parsed = parse("COHORT_TOKEN_BUDGET: 123456\n").unwrap();
        assert_eq!(parsed.processor.cohort_token_budget, Some(123_456));
    }

    #[test]
    fn parses_explicit_strict_unmetered_policy_and_rejects_unknown_boolean() {
        assert!(
            parse("COHORT_TOKEN_BUDGET_STRICT: true\n")
                .unwrap()
                .processor
                .cohort_token_budget_strict
        );
        assert!(
            !parse("COHORT_TOKEN_BUDGET_STRICT: false\n")
                .unwrap()
                .processor
                .cohort_token_budget_strict
        );
        assert!(parse("COHORT_TOKEN_BUDGET_STRICT: off\n").is_err());
        assert!(parse("COHORT_TOKEN_BUDGET_STRICT: maybe\n").is_err());
    }

    #[test]
    fn configured_output_limit_is_preserved_and_zero_is_rejected() {
        assert_eq!(
            parse("CALL_OUTPUT_MAX_BYTES: 12345\n")
                .unwrap()
                .call_output_max_bytes,
            12_345
        );
        assert!(parse("CALL_OUTPUT_MAX_BYTES: 0\n").is_err());
    }

    #[test]
    fn invalid_or_duplicate_active_values_fail_closed() {
        assert!(parse("MAX_PARALLEL: 0\n").is_err());
        assert!(parse("CALL_MAX_ATTEMPTS: 0\n").is_err());
        assert!(parse("INTEGRATION_LOOP_MAX: 0\n").is_err());
        assert!(parse("MAX_PARALLEL: nope\n").is_err());
        assert!(parse("PUSH: maybe\n").is_err());
        assert!(parse("PUBLISH_LINEAR_HISTORY: maybe\n").is_err());
        assert!(parse("KB_TTL: 0\n").is_err());
        assert!(parse("KB_CAP: 0\n").is_err());
        assert!(parse("MAX_PARALLEL: 2\nMAX_PARALLEL: 3\n").is_err());
        assert!(parse("MAX_PARALLEL: 2\n# MAX_PARALLEL: 3\n").is_ok());
    }

    #[test]
    fn unknown_keys_remain_forward_compatible_but_known_enum_is_strict() {
        assert!(parse("FUTURE_OPTION: whatever\n").is_ok());
        assert!(parse("VERIFICATION_MODE: surprise\n").is_err());
        assert_eq!(
            parse("VERIFICATION_MODE: required\n")
                .unwrap()
                .verification_mode,
            VerificationMode::Required
        );
    }

    #[test]
    fn verification_profile_uses_json_commands_before_the_legacy_smoke_fallback() {
        let configured = parse(
            "VERIFICATION_MODE: required\nSMOKE_CMD: cargo test\nVERIFICATION_COMMANDS: [\"cargo fmt --check\", \"cargo test -p engine\"]\n",
        )
        .unwrap();
        assert_eq!(
            configured.verification_commands,
            vec!["cargo fmt --check", "cargo test -p engine"]
        );
        assert_eq!(configured.smoke_cmd.as_deref(), Some("cargo test"));
        assert!(parse("VERIFICATION_COMMANDS: []\n").is_err());
        assert!(parse("VERIFICATION_COMMANDS: not-json\n").is_err());
        assert!(parse("VERIFICATION_COMMANDS: [\"\"]\n").is_err());
        assert!(parse("VERIFICATION_COMMANDS: [\"cargo test && cargo fmt\"]\n").is_err());
        assert!(parse("SMOKE_CMD: pwsh -Command cargo test\n").is_err());
    }

    #[test]
    fn review_cycle_verification_is_off_by_default_and_needs_an_executable_profile() {
        let default = parse("VERIFICATION_COMMANDS: [\"cargo test\"]\n").unwrap();
        assert!(!default.review_cycle_verification);
        assert!(default.review_cycle_verification_commands.is_empty());

        let reused = parse(
            "REVIEW_CYCLE_VERIFICATION: on\nVERIFICATION_COMMANDS: [\"cargo fmt --check\", \"cargo test\"]\n",
        )
        .unwrap();
        assert!(reused.review_cycle_verification);
        assert!(reused.review_cycle_verification_commands.is_empty());

        let subset = parse(
            "REVIEW_CYCLE_VERIFICATION: true\nREVIEW_CYCLE_VERIFICATION_COMMANDS: [\"cargo clippy --workspace\"]\nVERIFICATION_COMMANDS: [\"cargo test\"]\n",
        )
        .unwrap();
        assert!(subset.review_cycle_verification);
        assert_eq!(
            subset.review_cycle_verification_commands,
            vec!["cargo clippy --workspace"]
        );

        // The legacy one-command fallback is a legal cycle profile, but its argv must be typed
        // once it can actually reach a ProcessKit launch on every round.
        assert!(parse("REVIEW_CYCLE_VERIFICATION: on\nSMOKE_CMD: cargo test\n").is_ok());
        assert!(
            parse("REVIEW_CYCLE_VERIFICATION: on\nVERIFICATION_MODE: disabled\nSMOKE_CMD: pwsh -Command cargo test\n").is_err()
        );

        // An enabled gate with no profile at all would be a silent always-green round.
        assert!(parse("REVIEW_CYCLE_VERIFICATION: on\n").is_err());
        assert!(parse("REVIEW_CYCLE_VERIFICATION: maybe\n").is_err());
    }

    #[test]
    fn review_cycle_subset_is_validated_even_while_the_gate_is_off() {
        // The key is new, so no legacy shell text can exist for it: a malformed subset must fail
        // at parse time rather than at the first cohort that switches the gate on.
        assert!(
            parse("REVIEW_CYCLE_VERIFICATION_COMMANDS: [\"cargo fmt && cargo test\"]\n").is_err()
        );
        assert!(parse("REVIEW_CYCLE_VERIFICATION_COMMANDS: []\n").is_err());
        assert!(parse("REVIEW_CYCLE_VERIFICATION_COMMANDS: not-json\n").is_err());
        assert!(parse("REVIEW_CYCLE_VERIFICATION_COMMANDS: [\"\"]\n").is_err());
        let error = parse("REVIEW_CYCLE_VERIFICATION_COMMANDS: not-json\n").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("REVIEW_CYCLE_VERIFICATION_COMMANDS"),
            "each JSON command key must name itself in diagnostics: {error}"
        );
        let inert = parse("REVIEW_CYCLE_VERIFICATION_COMMANDS: [\"cargo clippy\"]\n").unwrap();
        assert!(!inert.review_cycle_verification);
        assert_eq!(
            inert.review_cycle_verification_commands,
            vec!["cargo clippy"]
        );
    }

    #[test]
    fn inline_comments_do_not_truncate_hashes_inside_typed_or_json_arguments() {
        let configured = parse(
            "VERIFICATION_COMMANDS: [\"tool --label \\\"#build\\\"\"] # required label\nSMOKE_CMD: tool '#fallback' # ignored because the JSON profile wins\n",
        )
        .unwrap();
        assert_eq!(
            configured.verification_commands,
            vec!["tool --label \"#build\""]
        );
        assert_eq!(configured.smoke_cmd.as_deref(), Some("tool '#fallback'"));

        assert_eq!(
            strip_inline_comment(" value#suffix"),
            " value#suffix",
            "an unquoted hash remains an argument unless it begins a comment"
        );
    }

    #[test]
    fn notification_command_is_typed_and_optional() {
        let configured =
            parse("NOTIFY_CMD: notifier --channel ops # operator-owned endpoint\n").unwrap();
        assert_eq!(
            configured.notify_command,
            Some(vec!["notifier".into(), "--channel".into(), "ops".into()])
        );
        assert_eq!(parse("NOTIFY_CMD: \n").unwrap().notify_command, None);
        assert!(parse("NOTIFY_CMD: notifier && exfiltrate\n").is_err());
    }

    #[test]
    fn an_implicit_mode_still_enforces_an_explicit_verification_profile() {
        let implicit_commands = parse("VERIFICATION_COMMANDS: [\"cargo test\"]\n").unwrap();
        assert_eq!(implicit_commands.verification_mode, VerificationMode::Auto);
        assert!(!implicit_commands.verification_mode_explicit);
        assert_eq!(
            parse("SMOKE_CMD: cargo test\n").unwrap().verification_mode,
            VerificationMode::Auto
        );
        assert_eq!(
            parse("# no verification profile\n")
                .unwrap()
                .verification_mode,
            VerificationMode::Disabled
        );
        let explicit_disabled =
            parse("VERIFICATION_MODE: disabled\nVERIFICATION_COMMANDS: [\"cargo test\"]\n")
                .unwrap();
        assert_eq!(
            explicit_disabled.verification_mode,
            VerificationMode::Disabled
        );
        assert!(explicit_disabled.verification_mode_explicit);
        assert!(parse(
            "VERIFICATION_MODE: disabled\nVERIFICATION_COMMANDS: [\"cargo test && cargo fmt\"]\nSMOKE_CMD: pwsh -Command cargo test\n"
        )
        .is_ok());
    }

    #[test]
    fn policy_required_commands_enable_auto_only_without_an_explicit_disable() {
        assert_eq!(
            mode_with_required_policy_commands(VerificationMode::Disabled, false, true),
            VerificationMode::Auto
        );
        assert_eq!(
            mode_with_required_policy_commands(VerificationMode::Disabled, true, true),
            VerificationMode::Disabled
        );
        assert_eq!(
            mode_with_required_policy_commands(VerificationMode::Required, false, true),
            VerificationMode::Required
        );
    }

    #[test]
    fn parses_remaining_phase_and_codex_controls_fail_closed() {
        let parsed = parse(
            "REVIEW_MIN_PASSES: 3\nQUARANTINE_MAX_ATTEMPTS: 4\nAPPROVAL_DEADLINE_SEC: 60\nREVIEWER_TIERING: false\nCODEX_CODER: fast+std\nCODEX_REVIEWER: deep\nCODEX_CIFIX: on\nCODEX_REASONING: xhigh\nCODEX_SANDBOX: read-only\nCODEX_NETWORK: off\nCODEX_MODEL: gpt-test\nCODEX_CMD: custom-codex\n",
        )
        .unwrap();
        assert_eq!(parsed.review_min_passes, 3);
        assert_eq!(parsed.quarantine_max_attempts, 4);
        assert_eq!(parsed.approval_deadline_secs, 60);
        assert!(!parsed.reviewer_tiering);
        assert_eq!(parsed.codex.coder, CodexCoder::FastStd);
        assert_eq!(parsed.codex.reviewer, CodexReviewer::Deep);
        assert!(parsed.codex.ci_fix);
        assert_eq!(parsed.codex.reasoning, CodexReasoning::XHigh);
        assert_eq!(parsed.codex.sandbox, Sandbox::ReadOnly);
        assert!(!parsed.codex.network);
        assert_eq!(parsed.codex.model.as_deref(), Some("gpt-test"));
        assert_eq!(parsed.codex.command, "custom-codex");
        assert!(parse("CODEX_CMD: cmd.exe\n").is_err());
        assert!(parse("CODEX_CMD: C:\\Windows\\System32\\pwsh.exe\n").is_err());
        assert!(parse("CODEX_SANDBOX: danger-full-access\n").is_err());
        assert!(parse("CODEX_CODER: surprise\n").is_err());
        assert!(parse("QUARANTINE_MAX_ATTEMPTS: 0\n").is_err());
        assert!(parse("APPROVAL_DEADLINE_SEC: 0\n").is_err());
    }

    #[test]
    fn only_documented_codex_and_kb_keys_inherit_environment_values() {
        let from_env =
            parse_with_environment("CODEX_CODER: \nCODEX_REVIEWER: \nKB: \n", |key| match key {
                "CODEX_CODER" => Some("fast".into()),
                "CODEX_REVIEWER" => Some("fast+std".into()),
                "KB" => Some("off".into()),
                _ => None,
            })
            .unwrap();
        assert_eq!(from_env.codex.coder, CodexCoder::Fast);
        assert_eq!(from_env.codex.reviewer, CodexReviewer::FastStd);
        assert!(!from_env.knowledge_base);

        let invalid_env = parse_with_environment("", |_| Some("invalid".into())).unwrap();
        assert_eq!(invalid_env.codex.coder, CodexCoder::Off);
        assert_eq!(invalid_env.codex.reviewer, CodexReviewer::Off);
        assert!(invalid_env.knowledge_base);

        // A nonempty file value takes precedence and is strict; it may never be silently rescued
        // by a valid environment value.
        assert!(parse_with_environment("CODEX_CODER: invalid\n", |_| Some("fast".into())).is_err());
        assert!(parse_with_environment("KB: invalid\n", |_| Some("off".into())).is_err());
    }

    #[test]
    fn missing_config_file_still_uses_only_documented_environment_fallbacks() {
        let missing_directory = std::env::temp_dir().join(format!(
            "orchestrail-config-missing-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        assert!(!missing_directory.exists());

        let config = load_with_environment(&missing_directory, |key| match key {
            "CODEX_CODER" => Some("fast".into()),
            "CODEX_REVIEWER" => Some("fast".into()),
            "KB" => Some("off".into()),
            _ => Some("unexpected".into()),
        })
        .unwrap();

        assert_eq!(config.codex.coder, CodexCoder::Fast);
        assert_eq!(config.codex.reviewer, CodexReviewer::Fast);
        assert!(!config.knowledge_base);
        assert_eq!(config.processor.max_parallel, 3);
    }
}
