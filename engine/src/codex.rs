//! Adapter that turns a leaf-agent call into a headless `codex exec` invocation.
//!
//! Spawning `codex exec` from outside Claude Code is ALREADY a solved problem in this
//! repo: `tools/codex-runtime.ps1` builds a safe argv and runs codex as a child process
//! today (the processor invokes THAT wrapper, and codex itself spawns its sandboxed child
//! past the Bash permission gate). This adapter proves the engine can construct the same
//! fail-closed argv natively, kept in PARITY with
//! `tools/codex-runtime.ps1::Build-CodexArgv` (see `$ROOT/tools/codex-runtime.ps1`,
//! lines ~280-337). The argv here mirrors that function's sandbox/network branches:
//!
//!   codex exec -C &lt;worktree&gt; --sandbox &lt;mode&gt;
//!     [--add-dir &lt;worktree&gt;/.work/codex-cache]                    (read-only only)
//!     [-c sandbox_workspace_write.exclude_slash_tmp=true
//!      -c sandbox_workspace_write.exclude_tmpdir_env_var=true]    (workspace-write on Windows only)
//!     -c approval_policy=never
//!     [--skip-git-repo-check] [-m &lt;model&gt;]
//!     [-c sandbox_workspace_write.network_access=true
//!      -c shell_environment_policy.set={GIT_CONFIG_COUNT="1",...}] (network only)
//!     -c model_reasoning_effort=&lt;r&gt; -
//!
//! The pinned `-c approval_policy=never` and an explicit `--sandbox` are the fail-closed
//! contract (task T-069): a sandbox-init failure must ERROR, never silently run
//! unsandboxed. The read-only `--add-dir` cache exception and the Windows workspace-write
//! `exclude_slash_tmp`/`exclude_tmpdir_env_var` pair are the ENV_LIMIT/sandbox-init-worktree
//! root-cause fix (T-279/K-054): codex's default workspace-write policy grants a SPLIT
//! writable set `[workdir, /tmp, $TMPDIR]` that the native Windows unelevated sandbox cannot
//! enforce; excluding /tmp and $TMPDIR collapses it back to the single `[workdir]` root.
//! The network pair is the T-063 outbound-network + openssl git-TLS override. The trailing
//! `-` makes codex read the prompt from stdin (never a shell fragment).
//!
//! `--json` is opt-in because it changes stdout into a JSONL event stream. The native headless
//! port enables it to capture exact `turn.completed.usage` counters; ordinary callers retain
//! the existing plain-output default.

use std::path::Path;

/// Codex sandbox modes accepted by the existing wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sandbox {
    ReadOnly,
    WorkspaceWrite,
}

impl Sandbox {
    pub fn as_flag(self) -> &'static str {
        match self {
            Sandbox::ReadOnly => "read-only",
            Sandbox::WorkspaceWrite => "workspace-write",
        }
    }
}

pub struct CodexCall {
    pub worktree: String,
    pub sandbox: Sandbox,
    pub model: Option<String>,
    pub reasoning: String,
    pub skip_git_repo_check: bool,
    /// Open the workspace-write sandbox's outbound network and route git TLS through openssl
    /// (T-063). Default `false`; mirrors Build-CodexArgv's `-Network on`. Emitted whenever set,
    /// matching the PS wrapper (which keys the pair purely off `$Network -eq 'on'`).
    pub network: bool,
    /// Emit Codex's JSONL event stream, including `turn.completed.usage`.
    pub emit_json: bool,
    /// Continue an existing conversation through `codex exec resume <id>` (see [`Self::resume`]).
    resume: Option<String>,
}

impl CodexCall {
    pub fn new(worktree: impl Into<String>, sandbox: Sandbox) -> Self {
        CodexCall {
            worktree: worktree.into(),
            sandbox,
            model: None,
            reasoning: "medium".into(),
            skip_git_repo_check: false,
            network: false,
            emit_json: false,
            resume: None,
        }
    }

    /// Request continuation of an existing Codex conversation, reporting whether the call will
    /// actually resume.
    ///
    /// The `exec resume` SUBCOMMAND accepts neither `--sandbox`, nor `-C/--cd`, nor `--add-dir`
    /// (only `-c key=value` overrides), so the fail-closed shape has to be re-expressed:
    /// `sandbox_mode` and `approval_policy` become pinned config overrides and the workspace root
    /// stays the child's own working directory, which the caller already sets to the worktree.
    /// The read-only writable-cache exception (`--add-dir <worktree>/.work/codex-cache`) has NO
    /// config equivalent that is valid outside workspace-write, so a read-only call deliberately
    /// REFUSES to resume rather than silently dropping part of its sandbox contract (T-069,
    /// T-279/K-054). The caller must honour the returned flag when it decides whether to send a
    /// short continuation or a full seed prompt.
    pub fn resume(&mut self, session_id: impl Into<String>) -> bool {
        if !matches!(self.sandbox, Sandbox::WorkspaceWrite) {
            return false;
        }
        self.resume = Some(session_id.into());
        true
    }

    /// The conversation this call continues, if any.
    pub fn resumed_session(&self) -> Option<&str> {
        self.resume.as_deref()
    }

    /// Build the argv for `codex` (program name prepended by the caller). The prompt is
    /// delivered on stdin (trailing `-`), matching tools/codex-runtime.ps1.
    pub fn to_argv(&self) -> Vec<String> {
        if let Some(session) = &self.resume {
            return self.to_resume_argv(session);
        }
        let mut a: Vec<String> = vec![
            "exec".into(),
            "-C".into(),
            self.worktree.clone(),
            "--sandbox".into(),
            self.sandbox.as_flag().into(),
        ];
        // Sandbox-scoped writable-root shaping, mirroring Build-CodexArgv's
        // `if ($Sandbox -eq 'read-only') { ... } elseif (workspace-write -and OnWindows) { ... }`.
        match self.sandbox {
            Sandbox::ReadOnly => {
                // read-only still needs the narrow internal exception for disposable caches.
                // Path join keeps this cross-platform (not byte-for-byte with PS Join-Path).
                a.push("--add-dir".into());
                a.push(
                    Path::new(&self.worktree)
                        .join(".work/codex-cache")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
            Sandbox::WorkspaceWrite => {
                // ROOT-CAUSE FIX for ENV_LIMIT/sandbox-init-worktree (T-279/K-054). workspace-write
                // already makes the nested `.work/` cache writable, so NO `--add-dir` here: re-adding
                // an already-writable path turns the single-root worktree into the split-root shape
                // the native Windows unelevated sandbox rejects. Instead, Windows-only, drop codex's
                // OWN extra `/tmp`/`$TMPDIR` roots so the writable set collapses to `[workdir]`.
                // POSIX's landlock/seccomp enforces the split fine, so it is left untouched there.
                if cfg!(target_os = "windows") {
                    a.push("-c".into());
                    a.push("sandbox_workspace_write.exclude_slash_tmp=true".into());
                    a.push("-c".into());
                    a.push("sandbox_workspace_write.exclude_tmpdir_env_var=true".into());
                }
            }
        }
        // Fail-closed approval policy — pinned literal, never lowered (T-069).
        a.push("-c".into());
        a.push("approval_policy=never".into());
        if self.skip_git_repo_check {
            a.push("--skip-git-repo-check".into());
        }
        if let Some(m) = &self.model {
            a.push("-m".into());
            a.push(m.clone());
        }
        if self.network {
            // T-063 network overrides: open outbound network in the workspace-write sandbox and
            // route git through the openssl TLS backend. Discrete `-c key=value` pairs (no spaces
            // inside the value), byte-for-byte with Build-CodexArgv's `if ($Network -eq 'on')`.
            a.push("-c".into());
            a.push("sandbox_workspace_write.network_access=true".into());
            a.push("-c".into());
            a.push(
                r#"shell_environment_policy.set={GIT_CONFIG_COUNT="1",GIT_CONFIG_KEY_0="http.sslBackend",GIT_CONFIG_VALUE_0="openssl"}"#
                    .into(),
            );
        }
        a.push("-c".into());
        a.push(format!("model_reasoning_effort={}", self.reasoning));
        if self.emit_json {
            a.push("--json".into());
        }
        a.push("-".into());
        a
    }

    /// `codex exec resume <id> ... -`: the same fail-closed posture expressed with the option set
    /// the resume subcommand actually has.
    ///
    /// Divergence from [`Self::to_argv`]'s fresh-call shape is deliberate and forced by the CLI,
    /// not a relaxation: `--sandbox <mode>` becomes the pinned `-c sandbox_mode=<mode>` override
    /// (the same key the no-model sandbox probe already uses), `-C <worktree>` becomes the child's
    /// working directory, which the spawn spec sets to that exact worktree, and
    /// `-c approval_policy=never` stays a pinned literal. [`Self::resume`] refuses read-only, so
    /// the missing `--add-dir` cache exception can never be silently dropped here.
    fn to_resume_argv(&self, session: &str) -> Vec<String> {
        let mut a: Vec<String> = vec!["exec".into(), "resume".into(), session.into()];
        a.push("-c".into());
        a.push(format!("sandbox_mode={}", self.sandbox.as_flag()));
        if cfg!(target_os = "windows") && matches!(self.sandbox, Sandbox::WorkspaceWrite) {
            // Same ENV_LIMIT/sandbox-init-worktree collapse to a single `[workdir]` writable root
            // as the fresh call (T-279/K-054).
            a.push("-c".into());
            a.push("sandbox_workspace_write.exclude_slash_tmp=true".into());
            a.push("-c".into());
            a.push("sandbox_workspace_write.exclude_tmpdir_env_var=true".into());
        }
        a.push("-c".into());
        a.push("approval_policy=never".into());
        if self.skip_git_repo_check {
            a.push("--skip-git-repo-check".into());
        }
        if let Some(m) = &self.model {
            a.push("-m".into());
            a.push(m.clone());
        }
        if self.network {
            a.push("-c".into());
            a.push("sandbox_workspace_write.network_access=true".into());
            a.push("-c".into());
            a.push(
                r#"shell_environment_policy.set={GIT_CONFIG_COUNT="1",GIT_CONFIG_KEY_0="http.sslBackend",GIT_CONFIG_VALUE_0="openssl"}"#
                    .into(),
            );
        }
        a.push("-c".into());
        a.push(format!("model_reasoning_effort={}", self.reasoning));
        if self.emit_json {
            a.push("--json".into());
        }
        a.push("-".into());
        a
    }
}

/// Build the no-model `codex sandbox` probe used before the first live Codex route in a native
/// engine session. The caller supplies a trusted executable plus a hidden no-op subcommand;
/// there is no shell fragment and the child runs inside the same workspace-write sandbox shape
/// as a real coder call.
pub fn sandbox_probe_argv(noop_program: &Path, noop_subcommand: &str) -> Vec<String> {
    let mut args = vec![
        "sandbox".into(),
        "-c".into(),
        "sandbox_mode=workspace-write".into(),
    ];
    if cfg!(target_os = "windows") {
        args.push("-c".into());
        args.push("sandbox_workspace_write.exclude_slash_tmp=true".into());
        args.push("-c".into());
        args.push("sandbox_workspace_write.exclude_tmpdir_env_var=true".into());
    }
    args.push("--".into());
    args.push(noop_program.to_string_lossy().into_owned());
    args.push(noop_subcommand.into());
    args
}

/// Usable data from a `codex exec --json` transcript.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JsonTranscript {
    /// Last completed agent message, for the existing leaf-result protocol parser.
    pub report: Option<String>,
    /// Sum of provider-exact usage blocks. This parser never manufactures an estimate.
    pub usage: Option<crate::telemetry::ProviderUsage>,
    /// Conversation id announced by `thread.started`, usable with `codex exec resume` on a later
    /// call of the same leaf lineage. Orthogonal runtime data: a transcript without it simply
    /// leaves the next call re-seeding full context.
    pub session_id: Option<String>,
}

/// Parse Codex JSONL. Unknown additive events are ignored, while only machine-readable provider
/// counters may become usage telemetry.
pub fn parse_json_transcript(transcript: &str) -> JsonTranscript {
    let mut report = None;
    let mut input = 0_u64;
    let mut output = 0_u64;
    let mut cache_read = 0_u64;
    let mut cache_creation = 0_u64;
    let mut total = 0_u64;
    let mut saw_usage = false;
    let mut session_id = None;

    for line in transcript.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
            continue;
        };
        // `thread.started` names the conversation this call created or continued. Reading the id
        // from any event carrying one keeps a resumed run correct even if codex renames the
        // announcing event, and an absent id just means the next call re-seeds.
        if let Some(thread_id) = value
            .get("thread_id")
            .or_else(|| value.get("session_id"))
            .and_then(serde_json::Value::as_str)
        {
            session_id = Some(thread_id.to_owned());
        }
        if value.get("type").and_then(serde_json::Value::as_str) == Some("item.completed")
            && value
                .get("item")
                .and_then(|item| item.get("type"))
                .and_then(serde_json::Value::as_str)
                == Some("agent_message")
        {
            report = value
                .get("item")
                .and_then(|item| item.get("text"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        }
        let Some(usage) = value
            .get("usage")
            .or_else(|| value.get("token_usage"))
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };
        let field = |names: &[&str]| {
            names
                .iter()
                .find_map(|name| usage.get(*name).and_then(serde_json::Value::as_u64))
        };
        let Some(parsed) = crate::telemetry::ProviderUsage::from_fields(
            field(&["input_tokens", "prompt_tokens", "input"]),
            field(&["output_tokens", "completion_tokens", "output"]),
            field(&[
                "cache_read_input_tokens",
                "cached_input_tokens",
                "cache_read",
            ]),
            field(&["cache_creation_input_tokens", "cache_creation_tokens"]),
            field(&["total_tokens", "tokens", "token_count"]),
        ) else {
            continue;
        };
        let Some(next_input) = input.checked_add(parsed.input_tokens.unwrap_or(0)) else {
            return JsonTranscript {
                report,
                usage: None,
                session_id,
            };
        };
        let Some(next_output) = output.checked_add(parsed.output_tokens.unwrap_or(0)) else {
            return JsonTranscript {
                report,
                usage: None,
                session_id,
            };
        };
        let Some(next_cache_read) =
            cache_read.checked_add(parsed.cache_read_input_tokens.unwrap_or(0))
        else {
            return JsonTranscript {
                report,
                usage: None,
                session_id,
            };
        };
        let Some(next_cache_creation) =
            cache_creation.checked_add(parsed.cache_creation_input_tokens.unwrap_or(0))
        else {
            return JsonTranscript {
                report,
                usage: None,
                session_id,
            };
        };
        let Some(next_total) = total.checked_add(parsed.total_tokens.unwrap_or(0)) else {
            return JsonTranscript {
                report,
                usage: None,
                session_id,
            };
        };
        input = next_input;
        output = next_output;
        cache_read = next_cache_read;
        cache_creation = next_cache_creation;
        total = next_total;
        saw_usage = true;
    }

    JsonTranscript {
        report,
        usage: saw_usage.then_some(crate::telemetry::ProviderUsage {
            input_tokens: Some(input),
            output_tokens: Some(output),
            cache_read_input_tokens: Some(cache_read),
            cache_creation_input_tokens: Some(cache_creation),
            total_tokens: Some(total),
        }),
        session_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_probe_is_typed_no_model_argv() {
        let executable = Path::new("C:/Program Files/Orchestrail/orchestrail-engine.exe");
        let argv = sandbox_probe_argv(executable, "__sandbox-probe-noop");
        assert_eq!(argv[0], "sandbox");
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["-c", "sandbox_mode=workspace-write"])
        );
        let separator = argv.iter().position(|arg| arg == "--").unwrap();
        assert_eq!(
            &argv[separator + 1..],
            &[
                executable.to_string_lossy().into_owned(),
                "__sandbox-probe-noop".into()
            ]
        );
        assert!(!argv.iter().any(|arg| arg == "exec" || arg == "-"));
        if cfg!(target_os = "windows") {
            assert!(
                argv.windows(2).any(|pair| {
                    pair == ["-c", "sandbox_workspace_write.exclude_slash_tmp=true"]
                })
            );
            assert!(argv.windows(2).any(|pair| {
                pair == ["-c", "sandbox_workspace_write.exclude_tmpdir_env_var=true"]
            }));
        }
    }

    #[test]
    fn argv_pins_fail_closed_contract() {
        let call = CodexCall {
            worktree: "/abs/wt".into(),
            sandbox: Sandbox::WorkspaceWrite,
            model: Some("gpt-5-codex".into()),
            reasoning: "high".into(),
            skip_git_repo_check: true,
            network: false,
            emit_json: false,
            resume: None,
        };
        let argv = call.to_argv();
        assert_eq!(argv[0], "exec");
        assert!(argv.windows(2).any(|w| w[0] == "-C" && w[1] == "/abs/wt"));
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--sandbox" && w[1] == "workspace-write")
        );
        // approval_policy=never must be present and pinned.
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "approval_policy=never")
        );
        assert!(argv.iter().any(|s| s == "--skip-git-repo-check"));
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-m" && w[1] == "gpt-5-codex")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "model_reasoning_effort=high")
        );
        // Prompt comes from stdin.
        assert_eq!(argv.last().map(|s| s.as_str()), Some("-"));
    }

    #[test]
    fn read_only_default_reasoning() {
        let call = CodexCall::new("/w", Sandbox::ReadOnly);
        let argv = call.to_argv();
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--sandbox" && w[1] == "read-only")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "model_reasoning_effort=medium")
        );
        assert!(!argv.iter().any(|s| s == "--skip-git-repo-check"));
    }

    #[test]
    fn resume_keeps_the_fail_closed_posture_with_the_subcommand_option_set() {
        let mut call = CodexCall::new("/abs/wt", Sandbox::WorkspaceWrite);
        call.model = Some("gpt-5-codex".into());
        call.reasoning = "high".into();
        call.skip_git_repo_check = true;
        call.emit_json = true;
        assert!(call.resume("019f054f-5e70-7d42-8586-ee66e3ac1d1e"));
        assert_eq!(
            call.resumed_session(),
            Some("019f054f-5e70-7d42-8586-ee66e3ac1d1e")
        );
        let argv = call.to_argv();
        assert_eq!(
            &argv[..3],
            &["exec", "resume", "019f054f-5e70-7d42-8586-ee66e3ac1d1e"]
        );
        // `exec resume` has no `--sandbox`/`-C`/`--add-dir`, so the sandbox is pinned through the
        // config override instead and the workspace root stays the child's working directory.
        assert!(
            !argv
                .iter()
                .any(|arg| arg == "--sandbox" || arg == "-C" || arg == "--add-dir"),
            "the resume subcommand accepts none of these flags: {argv:?}"
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "sandbox_mode=workspace-write")
        );
        // Everything the fail-closed contract pins is still pinned (T-069).
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "approval_policy=never")
        );
        assert!(argv.iter().any(|s| s == "--skip-git-repo-check"));
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-m" && w[1] == "gpt-5-codex")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "model_reasoning_effort=high")
        );
        assert!(argv.iter().any(|s| s == "--json"));
        assert_eq!(argv.last().map(String::as_str), Some("-"));
        if cfg!(target_os = "windows") {
            assert!(
                argv.windows(2)
                    .any(|w| w[0] == "-c"
                        && w[1] == "sandbox_workspace_write.exclude_slash_tmp=true")
            );
            assert!(
                argv.windows(2).any(|w| w[0] == "-c"
                    && w[1] == "sandbox_workspace_write.exclude_tmpdir_env_var=true")
            );
        }
    }

    #[test]
    fn a_read_only_call_refuses_to_resume_rather_than_lose_its_cache_exception() {
        let mut call = CodexCall::new("/abs/wt", Sandbox::ReadOnly);
        // `exec resume` cannot express `--add-dir <worktree>/.work/codex-cache`, so the call keeps
        // its exact fresh-seed shape instead of resuming under a quietly different sandbox.
        assert!(!call.resume("019f054f-5e70-7d42-8586-ee66e3ac1d1e"));
        assert_eq!(call.resumed_session(), None);
        let argv = call.to_argv();
        assert_eq!(argv[0], "exec");
        assert!(argv.iter().all(|arg| arg != "resume"));
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--sandbox" && w[1] == "read-only")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--add-dir" && w[1].ends_with(".work/codex-cache"))
        );
    }

    #[test]
    fn json_transcript_reports_the_thread_it_started() {
        let transcript = concat!(
            r#"{"type":"thread.started","thread_id":"019f054f-5e70-7d42-8586-ee66e3ac1d1e"}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"agent_message","text":"done"}}"#,
            "\n",
        );
        assert_eq!(
            parse_json_transcript(transcript).session_id.as_deref(),
            Some("019f054f-5e70-7d42-8586-ee66e3ac1d1e")
        );
        // A transcript that never names a thread simply leaves the next call re-seeding.
        assert_eq!(
            parse_json_transcript(r#"{"type":"turn.completed","usage":{"input_tokens":1}}"#)
                .session_id,
            None
        );
    }

    #[test]
    fn json_transcript_captures_report_and_exact_usage() {
        let transcript = concat!(
            r#"{"type":"thread.started","thread_id":"t"}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"agent_message","text":"ИТОГ: готово · режим=1"}}"#,
            "\n",
            r#"{"type":"turn.completed","usage":{"input_tokens":1200,"output_tokens":450,"cached_input_tokens":300}}"#,
            "\n",
        );
        let parsed = parse_json_transcript(transcript);
        assert_eq!(parsed.report.as_deref(), Some("ИТОГ: готово · режим=1"));
        assert_eq!(
            parsed.usage.unwrap().total_tokens,
            Some(1950),
            "only provider counters contribute to the exact total"
        );
    }

    #[test]
    fn read_only_adds_codex_cache_add_dir_before_approval() {
        let call = CodexCall::new("/w", Sandbox::ReadOnly);
        let argv = call.to_argv();
        // `--add-dir <worktree>/.work/codex-cache`, mirroring the PS `if ($Sandbox -eq 'read-only')`
        // branch. It sits after `--sandbox read-only` and before the pinned approval policy.
        let add_dir = argv
            .windows(2)
            .position(|w| w[0] == "--add-dir" && w[1].ends_with(".work/codex-cache"))
            .expect("read-only must add the .work/codex-cache writable-cache dir");
        let sandbox = argv
            .windows(2)
            .position(|w| w[0] == "--sandbox" && w[1] == "read-only")
            .unwrap();
        let approval = argv
            .windows(2)
            .position(|w| w[0] == "-c" && w[1] == "approval_policy=never")
            .unwrap();
        assert!(
            sandbox < add_dir && add_dir < approval,
            "--add-dir sits between --sandbox and approval_policy: {argv:?}"
        );
        // The workspace-write-only exclusion pair never appears under read-only.
        assert!(
            !argv
                .iter()
                .any(|s| s.starts_with("sandbox_workspace_write.exclude"))
        );
    }

    #[test]
    fn workspace_write_shapes_writable_root_per_platform() {
        let call = CodexCall::new("/w", Sandbox::WorkspaceWrite);
        let argv = call.to_argv();
        let sandbox = argv
            .windows(2)
            .position(|w| w[0] == "--sandbox" && w[1] == "workspace-write")
            .unwrap();
        let approval = argv
            .windows(2)
            .position(|w| w[0] == "-c" && w[1] == "approval_policy=never")
            .unwrap();
        let has_slash_tmp = argv
            .windows(2)
            .any(|w| w[0] == "-c" && w[1] == "sandbox_workspace_write.exclude_slash_tmp=true");
        let has_tmpdir = argv
            .windows(2)
            .any(|w| w[0] == "-c" && w[1] == "sandbox_workspace_write.exclude_tmpdir_env_var=true");
        // workspace-write NEVER gets a `--add-dir` cache (would reintroduce the rejected split root).
        assert!(
            !argv.iter().any(|s| s == "--add-dir"),
            "workspace-write must NOT add a --add-dir cache: {argv:?}"
        );
        if cfg!(target_os = "windows") {
            assert!(
                has_slash_tmp && has_tmpdir,
                "Windows workspace-write must exclude the /tmp and $TMPDIR split roots: {argv:?}"
            );
            // The pair comes immediately after `--sandbox workspace-write`, before the approval policy.
            let first = argv
                .windows(2)
                .position(|w| {
                    w[0] == "-c" && w[1] == "sandbox_workspace_write.exclude_slash_tmp=true"
                })
                .unwrap();
            assert_eq!(
                first,
                sandbox + 2,
                "exclude flags come right after --sandbox workspace-write: {argv:?}"
            );
            assert!(first < approval);
        } else {
            assert!(
                !has_slash_tmp && !has_tmpdir,
                "non-Windows workspace-write leaves the split roots untouched: {argv:?}"
            );
        }
    }

    #[test]
    fn network_true_emits_overrides_between_model_and_reasoning() {
        let mut call = CodexCall::new("/w", Sandbox::WorkspaceWrite);
        call.model = Some("gpt-5-codex".into());
        call.network = true;
        let argv = call.to_argv();
        let net_access = argv
            .windows(2)
            .position(|w| w[0] == "-c" && w[1] == "sandbox_workspace_write.network_access=true")
            .expect("network=true must emit the network_access override");
        // The environment-policy value is copied byte-for-byte from the PS T-063 override.
        let env_policy = argv
            .windows(2)
            .position(|w| {
                w[0] == "-c"
                    && w[1]
                        == r#"shell_environment_policy.set={GIT_CONFIG_COUNT="1",GIT_CONFIG_KEY_0="http.sslBackend",GIT_CONFIG_VALUE_0="openssl"}"#
            })
            .expect("network=true must emit the exact openssl git-TLS env-policy override");
        let model = argv
            .windows(2)
            .position(|w| w[0] == "-m" && w[1] == "gpt-5-codex")
            .unwrap();
        let reasoning = argv
            .windows(2)
            .position(|w| w[0] == "-c" && w[1].starts_with("model_reasoning_effort="))
            .unwrap();
        assert!(
            model < net_access && net_access < env_policy && env_policy < reasoning,
            "the network pair sits after -m <model> and before model_reasoning_effort: {argv:?}"
        );
    }

    #[test]
    fn network_false_default_omits_overrides() {
        let call = CodexCall::new("/w", Sandbox::WorkspaceWrite);
        assert!(!call.network, "network defaults to false");
        let argv = call.to_argv();
        assert!(
            !argv
                .iter()
                .any(|s| s == "sandbox_workspace_write.network_access=true")
        );
        assert!(
            !argv
                .iter()
                .any(|s| s.starts_with("shell_environment_policy.set="))
        );
    }
}
