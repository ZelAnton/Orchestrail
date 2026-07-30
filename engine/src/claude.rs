//! Adapter that turns a leaf-agent call into a headless `claude` invocation and parses
//! its `--output-format stream-json` transcript back into a structured result.
//!
//! KEY FINDING (intent doc risk R1 + T-057). Today `agents/processor.md` runs INSIDE
//! Claude Code and spawns leaf agents with the in-process "Use the X subagent" directive,
//! which rides the session's permission/classifier model. An engine OUTSIDE Claude Code
//! must instead run `claude -p` as a child process — and, crucially, EACH invocation
//! carries its own permission configuration explicitly on its own argv. That side-steps
//! the T-057 failure ("consent is not inherited through a subagent"): there is no
//! parent->subagent consent hand-off to lose, because the engine states the permission
//! posture on the very call it makes. Consent stays "in the context of the call itself".

/// How the spawned `claude` child is allowed to act. The engine chooses this per call;
/// it is never inherited or guessed mid-run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionPosture {
    /// `--permission-mode <mode>` with an explicit allowlist via `--allowedTools`.
    /// The safe default for autonomous leaf work: tools are enumerated, not "anything".
    Allowlisted,
    /// `--permission-mode bypassPermissions` — only for a hermetic, sandboxed worktree
    /// where the blast radius is already contained. Explicit and auditable, never
    /// silently applied.
    BypassInSandbox,
}

/// A leaf-agent call to run headlessly through `claude`.
pub struct ClaudeCall {
    pub prompt: String,
    pub model: Option<String>,
    pub max_turns: Option<u32>,
    pub allowed_tools: Vec<String>,
    pub append_system_prompt: Option<String>,
    pub add_dirs: Vec<String>,
    pub posture: PermissionPosture,
    /// Continue an existing conversation instead of starting a fresh one. The caller must have
    /// proved the transcript still exists (`crate::session`); `claude --resume` fails on an
    /// unknown id, so an unproven guess would turn a repeat call into a lost one. The permission
    /// posture is still stated on THIS argv: resuming a conversation never resumes consent.
    pub resume: Option<String>,
}

impl ClaudeCall {
    pub fn new(prompt: impl Into<String>) -> Self {
        ClaudeCall {
            prompt: prompt.into(),
            model: None,
            max_turns: None,
            allowed_tools: Vec::new(),
            append_system_prompt: None,
            add_dirs: Vec::new(),
            posture: PermissionPosture::Allowlisted,
            resume: None,
        }
    }

    /// Build the argv for `claude` (the program name is prepended by the caller). The
    /// prompt is passed as an argv element (never a shell fragment), and stream-json is
    /// requested so the transcript is machine-parseable line by line.
    pub fn to_argv(&self) -> Vec<String> {
        // stream-json in --print requires --verbose to emit the incremental events.
        let mut a: Vec<String> = vec![
            "-p".into(),
            self.prompt.clone(),
            "--output-format".into(),
            "stream-json".into(),
            "--verbose".into(),
        ];
        if let Some(session) = &self.resume {
            a.push("--resume".into());
            a.push(session.clone());
        }
        if let Some(m) = &self.model {
            a.push("--model".into());
            a.push(m.clone());
        }
        if let Some(t) = self.max_turns {
            a.push("--max-turns".into());
            a.push(t.to_string());
        }
        match self.posture {
            PermissionPosture::Allowlisted => {
                a.push("--permission-mode".into());
                a.push("acceptEdits".into());
                if !self.allowed_tools.is_empty() {
                    a.push("--allowedTools".into());
                    // Claude Code accepts a space-joined tool list here.
                    a.push(self.allowed_tools.join(" "));
                }
            }
            PermissionPosture::BypassInSandbox => {
                a.push("--permission-mode".into());
                a.push("bypassPermissions".into());
            }
        }
        if let Some(sp) = &self.append_system_prompt {
            a.push("--append-system-prompt".into());
            a.push(sp.clone());
        }
        for d in &self.add_dirs {
            a.push("--add-dir".into());
            a.push(d.clone());
        }
        a
    }
}

/// The distilled result of a stream-json transcript: the final `type":"result"` event.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamResult {
    pub result_seen: bool,
    pub subtype: Option<String>,
    pub is_error: Option<bool>,
    pub num_turns: Option<u32>,
    pub result_text: Option<String>,
    /// Provider-exact counters from the final `result.usage` object, when supplied.
    pub usage: Option<crate::telemetry::ProviderUsage>,
    /// Conversation id reported by the transcript, usable with `--resume` on a later call of the
    /// same leaf lineage. It is orthogonal runtime data: no decision here depends on it, and an
    /// older CLI that omits the field simply leaves the next call re-seeding full context.
    pub session_id: Option<String>,
}

/// Parse a full stream-json transcript (newline-delimited JSON objects). Only the LAST
/// `{"type":"result", ...}` line is authoritative; earlier lines are assistant / tool
/// events the engine can log but does not decide on.
pub fn parse_transcript(transcript: &str) -> StreamResult {
    let mut out = StreamResult {
        result_seen: false,
        subtype: None,
        is_error: None,
        num_turns: None,
        result_text: None,
        usage: None,
        session_id: None,
    };
    for line in transcript.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        // The conversation id is announced by the `system`/`init` event and repeated on the final
        // result. Read it from any event so a transcript that was cut short still identifies the
        // conversation it created; the last announcement wins, exactly like the result itself.
        if let Some(session_id) = value.get("session_id").and_then(serde_json::Value::as_str) {
            out.session_id = Some(session_id.to_owned());
        }
        if value.get("type").and_then(serde_json::Value::as_str) != Some("result") {
            continue;
        }
        out.result_seen = true;
        out.subtype = value
            .get("subtype")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        out.is_error = value.get("is_error").and_then(serde_json::Value::as_bool);
        out.num_turns = value
            .get("num_turns")
            .and_then(serde_json::Value::as_u64)
            .and_then(|turns| u32::try_from(turns).ok());
        out.result_text = value
            .get("result")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        out.usage = usage_from_result(&value);
    }
    out
}

/// Extract Claude's exact scalar usage payload. Both direct component names and documented
/// cache aliases are accepted; missing fields are never guessed.
fn usage_from_result(value: &serde_json::Value) -> Option<crate::telemetry::ProviderUsage> {
    let usage = value.get("usage")?.as_object()?;
    let field = |names: &[&str]| {
        names
            .iter()
            .find_map(|name| usage.get(*name).and_then(serde_json::Value::as_u64))
    };
    crate::telemetry::ProviderUsage::from_fields(
        field(&["input_tokens", "prompt_tokens", "input"]),
        field(&["output_tokens", "completion_tokens", "output"]),
        field(&[
            "cache_read_input_tokens",
            "cached_input_tokens",
            "cache_read",
        ]),
        field(&["cache_creation_input_tokens", "cache_creation_tokens"]),
        field(&["total_tokens", "tokens", "token_count"]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_has_headless_shape() {
        let call = ClaudeCall {
            prompt: "Use the coder subagent to implement task T-1.".into(),
            model: Some("sonnet".into()),
            max_turns: Some(40),
            allowed_tools: vec!["Read".into(), "Edit".into()],
            append_system_prompt: None,
            add_dirs: vec![],
            posture: PermissionPosture::Allowlisted,
            resume: None,
        };
        let argv = call.to_argv();
        assert_eq!(argv[0], "-p");
        assert_eq!(argv[1], "Use the coder subagent to implement task T-1.");
        assert!(argv.iter().any(|s| s == "stream-json"));
        assert!(argv.iter().any(|s| s == "--verbose"));
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--max-turns" && w[1] == "40")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--model" && w[1] == "sonnet")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--allowedTools" && w[1] == "Read Edit")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--permission-mode" && w[1] == "acceptEdits")
        );
    }

    #[test]
    fn bypass_posture_is_explicit() {
        let mut call = ClaudeCall::new("x");
        call.posture = PermissionPosture::BypassInSandbox;
        let argv = call.to_argv();
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--permission-mode" && w[1] == "bypassPermissions")
        );
        // Bypass never also emits an allowlist (it is all-or-nothing, and auditable).
        assert!(!argv.iter().any(|s| s == "--allowedTools"));
    }

    #[test]
    fn a_proven_conversation_is_continued_without_weakening_the_call() {
        let mut call = ClaudeCall::new("Continue task T-1.");
        call.model = Some("sonnet".into());
        call.allowed_tools = vec!["Read".into(), "Edit".into()];
        assert!(
            !call.to_argv().iter().any(|arg| arg == "--resume"),
            "a call with no proven conversation seeds a fresh one"
        );
        call.resume = Some("11111111-2222-3333-4444-555555555555".into());
        let argv = call.to_argv();
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--resume" && w[1] == "11111111-2222-3333-4444-555555555555")
        );
        // The prompt is still this turn's instruction, and the permission posture and allowlist
        // are still stated on this argv: continuing a conversation continues no authority.
        assert_eq!(argv[0], "-p");
        assert_eq!(argv[1], "Continue task T-1.");
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--permission-mode" && w[1] == "acceptEdits")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--allowedTools" && w[1] == "Read Edit")
        );
        assert!(argv.iter().any(|s| s == "stream-json"));
    }

    #[test]
    fn transcript_reports_the_conversation_id_from_init_or_result() {
        let init_only = concat!(
            r#"{"type":"system","subtype":"init","session_id":"11111111-2222-3333-4444-555555555555"}"#,
            "\n",
            r#"{"type":"assistant","message":{"role":"assistant"}}"#,
            "\n",
        );
        assert_eq!(
            parse_transcript(init_only).session_id.as_deref(),
            Some("11111111-2222-3333-4444-555555555555"),
            "a transcript cut short still identifies the conversation it created"
        );
        let full = concat!(
            r#"{"type":"system","subtype":"init","session_id":"11111111-2222-3333-4444-555555555555"}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"11111111-2222-3333-4444-555555555555"}"#,
            "\n",
        );
        let parsed = parse_transcript(full);
        assert_eq!(
            parsed.session_id.as_deref(),
            Some("11111111-2222-3333-4444-555555555555")
        );
        assert_eq!(parsed.result_text.as_deref(), Some("ok"));
        // An older CLI that never announces one leaves the next call re-seeding.
        assert_eq!(
            parse_transcript(r#"{"type":"result","subtype":"success","result":"ok"}"#).session_id,
            None
        );
    }

    #[test]
    fn parses_final_result_event() {
        let transcript = concat!(
            r#"{"type":"system","subtype":"init","model":"sonnet"}"#,
            "\n",
            r#"{"type":"assistant","message":{"type":"message","role":"assistant"}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"num_turns":5,"result":"done: implemented T-1","usage":{"input_tokens":1200,"output_tokens":450,"cache_read_input_tokens":300}}"#,
            "\n",
        );
        let r = parse_transcript(transcript);
        assert!(r.result_seen);
        assert_eq!(r.subtype.as_deref(), Some("success"));
        assert_eq!(r.is_error, Some(false));
        assert_eq!(r.num_turns, Some(5));
        assert_eq!(r.result_text.as_deref(), Some("done: implemented T-1"));
        assert_eq!(
            r.usage,
            Some(crate::telemetry::ProviderUsage {
                input_tokens: Some(1200),
                output_tokens: Some(450),
                cache_read_input_tokens: Some(300),
                cache_creation_input_tokens: None,
                total_tokens: Some(1950),
            })
        );
    }

    #[test]
    fn missing_result_line_is_reported() {
        let transcript = r#"{"type":"system","subtype":"init"}"#;
        let r = parse_transcript(transcript);
        assert!(!r.result_seen);
        assert_eq!(r.result_text, None);
    }

    #[test]
    fn last_result_wins() {
        let transcript = concat!(
            r#"{"type":"result","subtype":"error_max_turns","is_error":true,"num_turns":1}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"num_turns":2,"result":"ok"}"#,
            "\n",
        );
        let r = parse_transcript(transcript);
        assert_eq!(r.subtype.as_deref(), Some("success"));
        assert_eq!(r.is_error, Some(false));
    }

    #[test]
    fn malformed_or_nested_result_markers_never_override_a_valid_result() {
        let transcript = concat!(
            r#"{"type":"result","subtype":"success","is_error":false,"num_turns":2,"result":"ok"}"#,
            "\n",
            r#"{"message":{"type":"result"},"result":"nested"}"#,
            "\n",
            r#"{"type":"result","subtype":"broken","result":"trailing"} garbage"#,
            "\n",
        );
        let parsed = parse_transcript(transcript);
        assert!(parsed.result_seen);
        assert_eq!(parsed.subtype.as_deref(), Some("success"));
        assert_eq!(parsed.result_text.as_deref(), Some("ok"));
        assert_eq!(parsed.num_turns, Some(2));
    }

    #[test]
    fn result_scalars_follow_strict_serde_json_types_and_u32_range() {
        for invalid_turns in ["-1", "1.5", "4294967296", r#""2""#] {
            let parsed = parse_transcript(&format!(
                r#"{{"type":"result","is_error":"false","num_turns":{invalid_turns},"result":7}}"#
            ));
            assert!(parsed.result_seen);
            assert_eq!(parsed.is_error, None);
            assert_eq!(parsed.num_turns, None);
            assert_eq!(parsed.result_text, None);
        }
    }
}
