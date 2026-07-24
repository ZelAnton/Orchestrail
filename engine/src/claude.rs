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
}

/// Parse a full stream-json transcript (newline-delimited JSON objects). Only the LAST
/// `{"type":"result", ...}` line is authoritative; earlier lines are assistant / tool
/// events the engine can log but does not decide on.
pub fn parse_transcript(transcript: &str) -> StreamResult {
    use crate::jsonline::top_level;
    let mut out = StreamResult {
        result_seen: false,
        subtype: None,
        is_error: None,
        num_turns: None,
        result_text: None,
        usage: None,
    };
    for line in transcript.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match top_level(line, "type").and_then(|v| v.as_str().map(|s| s.to_string())) {
            Some(t) if t == "result" => {
                out.result_seen = true;
                out.subtype =
                    top_level(line, "subtype").and_then(|v| v.as_str().map(|s| s.to_string()));
                out.is_error = top_level(line, "is_error").and_then(|v| v.as_bool());
                out.num_turns = top_level(line, "num_turns")
                    .and_then(|v| v.as_num())
                    .map(|n| n as u32);
                out.result_text =
                    top_level(line, "result").and_then(|v| v.as_str().map(|s| s.to_string()));
                out.usage = serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .and_then(|value| usage_from_result(&value));
            }
            _ => {}
        }
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
}
