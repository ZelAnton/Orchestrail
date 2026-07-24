//! CLI for the T-097 Stage 1 spike.
//!
//! Subcommands:
//!   selfcheck                 Run the hermetic supervision + parse demo (no network,
//!                             no model call) and print a JSON verdict. This is what the
//!                             self-check / a future CI job runs.
//!   claude   --live "<prompt>"  Spawn a REAL `claude -p --output-format stream-json`
//!                             child (opt-in; needs auth; consumes tokens). Prints the
//!                             supervised verdict + parsed stream-json result.
//!   codex    --live "<prompt>"  Spawn a REAL `codex exec` child (opt-in).
//!   argv     claude|codex     Print the argv the engine WOULD spawn (offline, safe).
//!   events   tail [--follow] <file>
//!                             Read a `.work/events.jsonl`-shaped file and print each decoded
//!                             event (contract §19) as one normalized JSON line. Without
//!                             `--follow` it reads to EOF and exits; with `--follow` it keeps
//!                             polling for newly appended lines. Read-only.
//!   state    [--json] [<work-dir>]
//!                             Load a read-only control-plane snapshot (contract §13) from a
//!                             `.work/` directory (default `.work`) — queue, task descriptors,
//!                             cohort admission, integration/join state, batch manifest — and
//!                             print it human-readably, or as one JSON object with `--json`.
//!                             Read-only: never writes, locks, or emits.
//!   plan     --dry-run [--work <dir>]
//!                             Print the cohort + per-task decisions the engine WOULD make now
//!                             over a read-only snapshot (default `.work`): the cohort
//!                             budget/circuit-breaker gate, the admission plan the planner's
//!                             "Выбор батча" resolver yields, and the per-active-task reviewer tier
//!                             (T-105). STRICTLY read-only: takes no `.work/orchestrator.lock`,
//!                             calls no mutating `queue-tx`/`state-tx`, creates no worktree/branch,
//!                             writes nothing. `--dry-run` is required (the only mode).
//!   lease    <acquire|takeover|heartbeat|release|status> [--work <dir>] [--root <dir>]
//!            [--script <state-tx.ps1>] [--owner <id>] [--ttl <sec>] [--session <id>]
//!            [--pid <n>] [--json]
//!                             Take / renew / release / inspect the engine's owner lease on
//!                             `.work/orchestrator.lock`, the mutual-exclusion interlock with a
//!                             running `processor` (contract §14–§17, T-107). `takeover` is an
//!                             explicit, liveness-gated adoption of a stale structured lease;
//!                             it refuses live, legacy, corrupt, or forced replacement. This is
//!                             the ONE
//!                             subcommand that mutates `.work/` through a native owner-checked,
//!                             liveness-checked transaction under the engine's own role
//!                             (`engine`). It never force-removes a foreign lease: `acquire`
//!                             succeeds only when the lock is vacant; `takeover` is required for
//!                             stale records and refuses a live one; `release` presents the
//!                             engine's own owner id so it can only remove its own lease. It never executes a
//!                             target-local ownership script.
//!   inbox <inspect|actionable|reconcile> --root <repo> [--json]
//!                             Inspect or reconcile the optional cross-project inbox through
//!                             the native contract. `reconcile` writes only proven local links.
//!   processor --once --live --work <.work-dir> [--root <repo>] [--base <branch>] [--continue]
//!                             Run the native deterministic processor across all current-lane
//!                             cohorts under one owner lease. It recovers/imports a compatible
//!                             legacy control plane fail-closed, contains agents with ProcessKit,
//!                             and uses typed VCS/forge operations.
//!   release-sync --live --work <.work-dir> --version <version> [--resume]
//!                             Run the separate published-release fast-forward/tag/graph/notes/
//!                             dependent-notification mode under the owner lease. It never
//!                             processes the task queue.
//!   run      --once --work <sandbox> [--root <dir>] [--tools <dir>] [--base <ref>]
//!            [--batch <id>] [--cohort-size <n>] [--ttl <sec>] [--inject-escalate <T-ID>]
//!            [--live] [--codex-coder <off|fast|fast+std>] [--codex-network] [--json]
//!                             Drive ONE cohort/phase end-to-end over a SANDBOX `.work` (task
//!                             T-109): take the engine lease, admit a cohort (T-106), capture each
//!                             task through `queue-tx`, run ONE supervised leaf round, validate every
//!                             descriptor/cohort transition through `state-tx check-transition`, emit
//!                             the events through `outbox`, and release the lease. By default the leaf
//!                             round drives the deterministic offline `__fake-agent` stand-in;
//!                             `--live` (task T-244) opts into REAL `claude -p`/`codex exec` leaf
//!                             calls routed through the executor resolvers, each stating its
//!                             permission posture on its own argv. `--work` is REQUIRED and has no
//!                             default, so this can never touch the repository's live `.work`.
//!                             The compatibility fixture runs exactly one cohort. The native
//!                             `processor --once --live` command is the queue-draining path.
//!   __fake-agent ...          Hidden: a deterministic stand-in child used by the
//!                             hermetic tests, by `selfcheck`, and by `run`'s round (emits
//!                             stream-json; `--mode leaf` carries a parseable leaf report, and
//!                             other modes can sleep / exit with a chosen code).
//!
//! `lease`, `processor`, `release-sync`, and sandbox-only `run` are the mutating commands. The
//! production modes use native guarded stores, typed VCS/forge APIs, and ProcessKit-contained
//! leaves; `run` retains its legacy transactional tool fixture only for sandbox compatibility.
//! Live model calls are strictly opt-in via `--live`; the default path is offline and token-free.

use std::collections::BTreeSet;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::process::exit;
use std::time::Duration;

use orchestrail_engine::approval::{
    ApprovalDecision, ApprovalRequest, ApprovalStatus, ApprovalStore,
};
use orchestrail_engine::claude::{ClaudeCall, PermissionPosture};
use orchestrail_engine::codex::{CodexCall, Sandbox};
use orchestrail_engine::control::{ControlPlane, entry_exists as control_entry_exists};
use orchestrail_engine::dependency_graph;
use orchestrail_engine::events::TailReader;
use orchestrail_engine::headless::{HeadlessConfig, HeadlessExternalPort};
use orchestrail_engine::inbox;
use orchestrail_engine::lease::{LeaseOp, exit as lease_exit};
use orchestrail_engine::native::{NativeExecutor, ProcessorPort};
use orchestrail_engine::native_loop::{
    NativeLoopConfig, NativeLoopOutcome, run_until_queue_exhausted,
};
use orchestrail_engine::native_port::{FileVcsPort, ReleaseNotesRequest};
use orchestrail_engine::ownership::{
    ENGINE_ROLE, LeaseError, LeaseHeartbeat, LeaseRecord, LeaseStatus, LeaseStore, roots_equivalent,
};
use orchestrail_engine::policy;
use orchestrail_engine::processor::{Phase, ProcessorState};
use orchestrail_engine::recovery::{RecoveryDisposition, RecoveryPlan};
use orchestrail_engine::release::{self, ReleaseContent, ReleaseRequest};
use orchestrail_engine::resolvers::{
    ActiveClass, ActiveTask, AdmissionGate, AdmissionOutcome, Candidate, CodexCoder,
    CohortCounters, CohortThresholds, Domain, Level, admission_gate, base_reviewer, is_ready,
    plan_admission, unmet_prerequisites,
};
use orchestrail_engine::run::{self, RunConfig};
use orchestrail_engine::runtime::{ProcessorRuntime, RUNTIME_CHECKPOINT_FILE};
use orchestrail_engine::state::{
    DeliveryTarget, IntegrationState, Snapshot, TaskState, completed_ids, now_epoch_secs,
};
use orchestrail_engine::supervise::{self, SpawnSpec};
use orchestrail_engine::time::{days_from_civil, epoch_to_iso};
use orchestrail_engine::toolscript;
use orchestrail_engine::vcs::VcsService;
use orchestrail_engine::verification;

fn main() {
    let args: Vec<String> = env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("");
    match cmd {
        "selfcheck" => cmd_selfcheck(),
        "argv" => cmd_argv(&args),
        "claude" => cmd_claude(&args),
        "codex" => cmd_codex(&args),
        "events" => cmd_events(&args),
        "state" => cmd_state(&args),
        "plan" => cmd_plan(&args),
        "approval" => cmd_approval(&args),
        "inbox" => cmd_inbox(&args),
        "lease" => cmd_lease(&args),
        "processor" => cmd_processor(&args),
        "release-sync" => cmd_release_sync(&args),
        "run" => cmd_run(&args),
        // Trusted typed argv target for `codex sandbox --`; intentionally absent from help.
        // It performs no I/O and is never a user-facing orchestration command.
        "__sandbox-probe-noop" => {}
        "__fake-agent" => cmd_fake_agent(&args),
        "version" | "--version" => println!("orchestrail-engine 0.0.1"),
        _ => {
            eprintln!(
                "usage: orchestrail-engine <selfcheck|argv|claude|codex|events|state|plan|approval|inbox|lease|processor|release-sync|run|version>\n\
                 (see src/main.rs; live model calls require --live and are opt-in)"
            );
            exit(2);
        }
    }
}

/// Native inspection/reconciliation for the optional cross-project inbox.  The command never
/// acts on message prose: only the curator can make an acceptance/rejection decision.
fn cmd_inbox(args: &[String]) {
    let Some(action) = args.get(2).map(String::as_str) else {
        inbox_usage();
    };
    let Some(root) = opt(args, "--root") else {
        eprintln!("inbox: --root <repository-root> is required");
        exit(2);
    };
    let root = Path::new(&root);
    let json = args.iter().any(|arg| arg == "--json");
    let result = match action {
        "inspect" => inbox::inspect(root).map(|value| serde_json::to_value(value).unwrap()),
        "actionable" => inbox::actionable(root).map(|value| serde_json::to_value(value).unwrap()),
        "reconcile" => {
            let occurred_at =
                opt(args, "--occurred-at").unwrap_or_else(|| epoch_to_iso(now_epoch_secs()));
            inbox::reconcile(root, &occurred_at).map(|value| serde_json::to_value(value).unwrap())
        }
        _ => inbox_usage(),
    };
    match result {
        Ok(value) if json => println!("{value}"),
        Ok(value) => println!("inbox: {value}"),
        Err(error) => {
            eprintln!("inbox: {error}");
            exit(11);
        }
    }
}

fn inbox_usage() -> ! {
    eprintln!(
        "usage: orchestrail-engine inbox <inspect|actionable|reconcile> --root <repository-root> [--occurred-at <ISO-8601-Z>] [--json]"
    );
    exit(2)
}

/// Operator-owned lifecycle for a one-time policy approval. The running processor never calls
/// `approve` or `reject`; it only consumes an already-fresh `status` result at a policy gate.
fn cmd_approval(args: &[String]) {
    let Some(action) = args.get(2).map(String::as_str) else {
        approval_usage();
    };
    let Some(work) = opt(args, "--work") else {
        eprintln!("approval: --work <.work-dir> is required");
        exit(2);
    };
    let store = match ApprovalStore::new(&work) {
        Ok(store) => store,
        Err(error) => {
            eprintln!("approval: {error}");
            exit(2);
        }
    };
    let now_secs = match opt(args, "--now") {
        Some(value) => match value.parse::<u64>() {
            Ok(value) => value,
            Err(_) => {
                eprintln!("approval: --now must be an unsigned epoch-second value");
                exit(2);
            }
        },
        None => now_epoch_secs(),
    };
    let json = args.iter().any(|arg| arg == "--json");
    match action {
        "request" => {
            let deadline_secs = match opt(args, "--deadline-sec") {
                Some(value) => match value.parse::<u64>() {
                    Ok(value) if value > 0 => value,
                    _ => {
                        eprintln!("approval: --deadline-sec must be at least 1");
                        exit(2);
                    }
                },
                None => 86_400,
            };
            let request = ApprovalRequest {
                task_id: opt(args, "--task"),
                batch_id: opt(args, "--batch"),
                reason: required_approval_option(args, "--reason"),
                fingerprint: required_approval_option(args, "--fingerprint"),
                policy_hash: approval_policy_hash(&work),
                now_secs,
                deadline_secs,
            };
            match store.request(request) {
                Ok(record) => print_approval_record(&record, json),
                Err(error) => approval_error(error),
            }
        }
        "approve" | "reject" => {
            let id = required_approval_option(args, "--id");
            let by = required_approval_option(args, "--by");
            let decision = if action == "approve" {
                ApprovalDecision::Approve
            } else {
                ApprovalDecision::Reject
            };
            match store.decide(&id, decision, &by, opt(args, "--note"), now_secs) {
                Ok(record) => print_approval_record(&record, json),
                Err(error) => approval_error(error),
            }
        }
        "status" => {
            let id = required_approval_option(args, "--id");
            let fingerprint = required_approval_option(args, "--fingerprint");
            let policy_hash = approval_policy_hash(&work);
            match store.status(&id, &fingerprint, &policy_hash, now_secs) {
                Ok(status) => {
                    print_approval_status(&status, json);
                    if !status.allows_progress() {
                        exit(match status {
                            ApprovalStatus::Pending { .. } => 12,
                            _ => 11,
                        });
                    }
                }
                Err(error) => approval_error(error),
            }
        }
        _ => approval_usage(),
    }
}

fn required_approval_option(args: &[String], key: &str) -> String {
    match opt(args, key).filter(|value| !value.trim().is_empty()) {
        Some(value) => value,
        None => {
            eprintln!("approval: {key} <value> is required");
            exit(2);
        }
    }
}

fn approval_error(error: impl std::fmt::Display) -> ! {
    eprintln!("approval: {error}");
    exit(11)
}

fn approval_usage() -> ! {
    eprintln!(
        "usage:\n  orchestrail-engine approval request --work <.work> (--task <T-ID>|--batch <B-ID>) --reason <category> --fingerprint <sha256> [--deadline-sec <seconds>] [--json]\n  orchestrail-engine approval approve|reject --work <.work> --id <apr-id> --by <operator> [--note <text>] [--json]\n  orchestrail-engine approval status --work <.work> --id <apr-id> --fingerprint <sha256> [--json]"
    );
    exit(2)
}

fn approval_policy_hash(work: &str) -> String {
    match policy::snapshot_hash(Path::new(work)) {
        Ok(hash) => hash,
        Err(error) => {
            eprintln!("approval: {error}");
            exit(2);
        }
    }
}

fn print_approval_record(record: &orchestrail_engine::approval::ApprovalRecord, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "id": record.id,
                "subject": record.subject,
                "reason": record.reason,
                "deadline_at_secs": record.deadline_at_secs,
                "decision": record.decision,
            })
        );
    } else {
        println!(
            "approval id={} subject={} reason={} deadline={} decision={}",
            record.id,
            record.subject,
            record.reason,
            record.deadline_at_secs,
            record
                .decision
                .map(|decision| match decision {
                    ApprovalDecision::Approve => "approve",
                    ApprovalDecision::Reject => "reject",
                })
                .unwrap_or("pending")
        );
    }
}

fn print_approval_status(status: &ApprovalStatus, json: bool) {
    let (verdict, deadline_at_secs) = match status {
        ApprovalStatus::Approved { .. } => ("approved", None),
        ApprovalStatus::Pending {
            deadline_at_secs, ..
        } => ("pending", Some(*deadline_at_secs)),
        ApprovalStatus::Rejected { .. } => ("rejected", None),
        ApprovalStatus::ExpiredTimeout { .. } => ("expired-timeout", None),
        ApprovalStatus::ExpiredStale { .. } => ("expired-stale", None),
        ApprovalStatus::Missing { .. } => ("missing", None),
    };
    if json {
        println!(
            "{}",
            serde_json::json!({
                "id": status.id(),
                "verdict": verdict,
                "deadline_at_secs": deadline_at_secs,
            })
        );
    } else {
        println!("approval {verdict} id={}", status.id());
    }
}

/// Locate this very binary so the spike can spawn itself as the hermetic `__fake-agent`.
fn self_exe() -> String {
    env::current_exe()
        .ok()
        .and_then(|p| p.to_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "orchestrail-engine".to_string())
}

/// Hermetic proof: supervise a stand-in child that emits a stream-json transcript, then
/// parse it. Also proves the deadline path by supervising a child that would outlive it.
fn cmd_selfcheck() {
    let exe = self_exe();
    let mut ok = true;

    // (1) A well-behaved "agent": emits a stream-json transcript, exits 0.
    let spec = SpawnSpec::new(
        &exe,
        vec!["__fake-agent".into(), "--mode".into(), "success".into()],
    )
    .deadline(Some(Duration::from_secs(30)));
    let v = supervise::run(&spec);
    let parsed = orchestrail_engine::claude::parse_transcript(&v.stdout);
    let success_ok = v.reason == supervise::Reason::Ok
        && parsed.result_seen
        && parsed.is_error == Some(false)
        && parsed.subtype.as_deref() == Some("success");
    ok &= success_ok;

    // (2) The deadline path: a child that sleeps past a short deadline must be classified
    // as `timeout` (reason 3) and its tree terminated.
    let spec = SpawnSpec::new(
        &exe,
        vec!["__fake-agent".into(), "--mode".into(), "hang".into()],
    )
    .deadline(Some(Duration::from_millis(400)));
    let v2 = supervise::run(&spec);
    let timeout_ok = v2.reason == supervise::Reason::Timeout && v2.timed_out;
    ok &= timeout_ok;

    // (3) A substantive error exit (nonzero, not a crash code) must be `error` (reason 6).
    let spec = SpawnSpec::new(
        &exe,
        vec![
            "__fake-agent".into(),
            "--mode".into(),
            "success".into(),
            "--exit".into(),
            "6".into(),
        ],
    )
    .deadline(Some(Duration::from_secs(30)));
    let v3 = supervise::run(&spec);
    let error_ok = v3.reason == supervise::Reason::Error && v3.exit_code == Some(6);
    ok &= error_ok;

    // Emit a small JSON verdict (hand-built; the spike is dependency-free).
    println!(
        "{{\"selfcheck\":\"{}\",\"success_case\":{{\"reason\":\"{}\",\"result_seen\":{},\"subtype\":\"{}\"}},\"timeout_case\":{{\"reason\":\"{}\",\"timed_out\":{}}},\"error_case\":{{\"reason\":\"{}\",\"exit_code\":{}}}}}",
        if ok { "pass" } else { "fail" },
        v.reason.as_str(),
        parsed.result_seen,
        parsed.subtype.as_deref().unwrap_or(""),
        v2.reason.as_str(),
        v2.timed_out,
        v3.reason.as_str(),
        v3.exit_code.unwrap_or(-1),
    );
    if !ok {
        exit(1);
    }
}

fn cmd_argv(args: &[String]) {
    let which = args.get(2).map(|s| s.as_str()).unwrap_or("");
    match which {
        "claude" => {
            let mut call = ClaudeCall::new(
                "Use the coder subagent to implement task T-1. Worktree=<abs>. WORK=<abs>.",
            );
            call.model = Some("sonnet".into());
            call.max_turns = Some(40);
            call.allowed_tools = vec!["Read".into(), "Edit".into(), "Bash".into()];
            call.posture = PermissionPosture::Allowlisted;
            println!("claude {}", join_argv(&call.to_argv()));
        }
        "codex" => {
            let mut call = CodexCall::new("/abs/worktree", Sandbox::WorkspaceWrite);
            call.model = Some("gpt-5-codex".into());
            call.skip_git_repo_check = false;
            println!("codex {}", join_argv(&call.to_argv()));
        }
        _ => {
            eprintln!("usage: argv <claude|codex>");
            exit(2);
        }
    }
}

/// Failure modes of [`parse_live_prompt`]: kept distinct so each `cmd_claude`/`cmd_codex`
/// caller can print its own `--live`-specific refusal (they already had different wording)
/// while sharing the actual parsing/validation logic.
#[derive(Debug)]
enum LivePromptError {
    /// The required `--live` opt-in flag was never given.
    MissingLive,
    /// `--live` was present, but the prompt itself is missing, duplicated, or ambiguous.
    Bad(String),
}

/// Parse the arguments to `claude --live <prompt>` / `codex --live <prompt>` (T-272).
///
/// The prompt is deliberately NOT `args.last()`: with that, a bare `--live` (no prompt)
/// spawns a REAL, paid model call using the literal string `--live` as the prompt, and any
/// flag placed after (or instead of) the prompt silently steals its slot. Instead the prompt
/// must be given as a single explicit positional argument — which must be the FINAL token;
/// nothing, flag or otherwise, may follow it — or via `--prompt <value>`. Any other `--flag`,
/// a second positional, or a flag trailing the prompt is a hard parse error, never a silent
/// prompt substitution.
///
/// `rest` is the subcommand's own arguments, i.e. everything after `claude`/`codex`.
fn parse_live_prompt(rest: &[String]) -> Result<String, LivePromptError> {
    let mut live = false;
    let mut prompt: Option<String> = None;
    let n = rest.len();
    let mut i = 0;
    while i < n {
        let a = rest[i].as_str();
        match a {
            "--live" => live = true,
            "--prompt" => {
                i += 1;
                let value = rest
                    .get(i)
                    .ok_or_else(|| LivePromptError::Bad("--prompt requires a value".into()))?;
                if prompt.is_some() {
                    return Err(LivePromptError::Bad("prompt given more than once".into()));
                }
                prompt = Some(value.clone());
            }
            _ if a.starts_with("--") => {
                return Err(LivePromptError::Bad(format!("unrecognized flag: {a}")));
            }
            _ => {
                if prompt.is_some() {
                    return Err(LivePromptError::Bad("prompt given more than once".into()));
                }
                if i != n - 1 {
                    return Err(LivePromptError::Bad(format!(
                        "unexpected argument after the prompt: {}",
                        rest[i + 1]
                    )));
                }
                prompt = Some(a.to_string());
            }
        }
        i += 1;
    }
    if !live {
        return Err(LivePromptError::MissingLive);
    }
    match prompt {
        Some(p) if !p.is_empty() => Ok(p),
        _ => Err(LivePromptError::Bad(
            "missing prompt: pass it as a positional argument, or as --prompt <value>".into(),
        )),
    }
}

fn cmd_claude(args: &[String]) {
    let prompt = match parse_live_prompt(&args[2..]) {
        Ok(p) => p,
        Err(LivePromptError::MissingLive) => {
            eprintln!(
                "refusing to spawn a real model call without --live (this consumes tokens and needs auth).\n\
                 Use `argv claude` to see the argv offline, or `selfcheck` for the hermetic demo."
            );
            exit(2);
        }
        Err(LivePromptError::Bad(msg)) => {
            eprintln!(
                "usage: claude --live <prompt>  (the prompt must be a single positional \
                 argument, or --prompt <value>, with nothing after it): {msg}"
            );
            exit(2);
        }
    };
    let mut call = ClaudeCall::new(prompt);
    call.model = Some("sonnet".into());
    call.max_turns = Some(40);
    let argv = call.to_argv();
    let spec = SpawnSpec::new("claude", argv).deadline(Some(Duration::from_secs(600)));
    let v = supervise::run(&spec);
    let parsed = orchestrail_engine::claude::parse_transcript(&v.stdout);
    println!(
        "reason={} exit={:?} duration_ms={} result_seen={} is_error={:?} subtype={:?}",
        v.reason.as_str(),
        v.exit_code,
        v.duration_ms,
        parsed.result_seen,
        parsed.is_error,
        parsed.subtype
    );
    exit(v.reason.exit_code());
}

fn cmd_codex(args: &[String]) {
    let prompt = match parse_live_prompt(&args[2..]) {
        Ok(p) => p,
        Err(LivePromptError::MissingLive) => {
            eprintln!("refusing to spawn a real codex call without --live.");
            exit(2);
        }
        Err(LivePromptError::Bad(msg)) => {
            eprintln!(
                "usage: codex --live <prompt>  (the prompt must be a single positional \
                 argument, or --prompt <value>, with nothing after it): {msg}"
            );
            exit(2);
        }
    };
    let call = CodexCall::new(
        env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
        Sandbox::ReadOnly,
    );
    let spec = SpawnSpec::new("codex", call.to_argv())
        .stdin(prompt)
        .deadline(Some(Duration::from_secs(600)));
    let v = supervise::run(&spec);
    println!(
        "reason={} exit={:?} duration_ms={} output_bytes={}",
        v.reason.as_str(),
        v.exit_code,
        v.duration_ms,
        v.stdout.len() + v.stderr.len()
    );
    exit(v.reason.exit_code());
}

/// `events tail [--follow] <file>` — decode a `.work/events.jsonl`-shaped file and print each
/// new, unique, fully-committed event as one normalized JSON line (contract §19). Read-only:
/// it opens the file for reading, never writes or locks it. A torn/unterminated tail is never
/// printed. With `--follow` it polls indefinitely for appended lines (like `tail -f`).
fn cmd_events(args: &[String]) {
    let sub = args.get(2).map(|s| s.as_str()).unwrap_or("");
    if sub != "tail" {
        eprintln!("usage: events tail [--follow] <file>");
        exit(2);
    }
    let follow = args.iter().any(|a| a == "--follow");
    // The file is the first non-flag argument after `events tail`.
    let path = args.iter().skip(3).find(|a| !a.starts_with("--")).cloned();
    let path = match path {
        Some(p) => p,
        None => {
            eprintln!("usage: events tail [--follow] <file>");
            exit(2);
        }
    };

    // Without --follow a missing file is a user error; with --follow we tolerate it and wait.
    if !follow && !std::path::Path::new(&path).exists() {
        eprintln!("events tail: file not found: {path}");
        exit(2);
    }

    let mut reader = TailReader::new(&path);
    let poll_interval = Duration::from_millis(200);
    loop {
        match reader.poll() {
            Ok(events) => {
                for ev in events {
                    println!("{}", ev.to_json_line());
                }
            }
            Err(e) => {
                eprintln!("events tail: read error on {path}: {e}");
                exit(3);
            }
        }
        if !follow {
            break;
        }
        std::thread::sleep(poll_interval);
    }
}

/// `state [--json] [<work-dir>]` — load a read-only control-plane snapshot (contract §13) from a
/// `.work/` directory and print it. Human-readable by default; one compact JSON object with
/// `--json`. The work directory is the first non-flag argument (default `.work`). Read-only: the
/// snapshot only reads `.work/`, never writes, locks, or emits.
fn cmd_state(args: &[String]) {
    let json = args.iter().any(|a| a == "--json");
    // The work dir is the first non-flag argument after `state`; default to the project `.work`.
    let work = args
        .iter()
        .skip(2)
        .find(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| ".work".to_string());

    // A wholly missing work directory is a usage error (likely a wrong path); missing individual
    // artifacts inside a real `.work/` are tolerated by `Snapshot::load` as the idle state.
    if !std::path::Path::new(&work).is_dir() {
        eprintln!("state: work directory not found: {work}");
        exit(2);
    }

    let snap = Snapshot::load(&work);
    if json {
        println!("{}", snap.to_json());
    } else {
        print!("{}", snap.to_human());
    }
}

/// `plan --dry-run [--work <dir>]` — print the cohort + per-task decisions the engine WOULD make
/// now over a read-only snapshot (default `.work`): the cohort budget/circuit-breaker gate
/// ([`resolvers::budget`]), the admission plan ([`resolvers::admission::plan_admission`]), and the
/// per-active-task reviewer tier ([`resolvers::tiering::base_reviewer`], T-105). STRICTLY
/// read-only: it only reads `.work/` (snapshot + `config.md` + `Tasks_Done.md`) and the wall clock;
/// it takes no lock, calls no mutating `queue-tx`/`state-tx`, and creates no worktree/branch.
fn cmd_plan(args: &[String]) {
    if !args.iter().any(|a| a == "--dry-run") {
        eprintln!(
            "usage: plan --dry-run [--work <dir>]\n\
             (dry-run is the only mode: it prints the cohort + per-task decisions the engine WOULD\n\
              make now over a READ-ONLY snapshot — it never locks, mutates, spawns, or writes)"
        );
        exit(2);
    }
    // `--work <dir>`, or a bare positional after `plan` (like `state`), default `.work`.
    let work = opt(args, "--work")
        .or_else(|| args.iter().skip(2).find(|a| !a.starts_with("--")).cloned())
        .unwrap_or_else(|| ".work".to_string());

    if !Path::new(&work).is_dir() {
        eprintln!("plan: work directory not found: {work}");
        exit(2);
    }

    let snap = Snapshot::load(&work);
    let cfg = match PlanConfig::load(&work) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("plan: invalid .work/config.md: {error}");
            exit(2);
        }
    };
    let completed = completed_ids(Path::new(&work), &snap);
    print!("{}", render_plan(&snap, &cfg, &completed, now_epoch_secs()));
}

/// The strict native configuration projected down to the fields the dry-run actually consumes.
/// Keeping this projection after [`orchestrail_engine::config::load`] prevents `plan --dry-run`
/// from previewing a different set of limits than `processor` would accept.
struct PlanConfig {
    max_parallel: usize,
    reviewer_tiering: bool,
    thresholds: CohortThresholds,
}

impl PlanConfig {
    fn load(work: &str) -> Result<PlanConfig, String> {
        let config =
            orchestrail_engine::config::load(Path::new(work)).map_err(|error| error.to_string())?;
        ensure_supported_processor_config(&config)?;
        Ok(PlanConfig {
            max_parallel: config.processor.max_parallel,
            reviewer_tiering: config.reviewer_tiering,
            thresholds: CohortThresholds {
                size: config.processor.cohort_size,
                max_age_minutes: config.processor.cohort_max_age_minutes,
                budget_sec: config.processor.cohort_budget_secs,
            },
        })
    }
}

/// Render the dry-run report as one human-readable block. Pure over its inputs (the wall clock is
/// passed in as `now_epoch`), so it prints exactly what the resolvers decide for this snapshot.
fn render_plan(
    snap: &Snapshot,
    cfg: &PlanConfig,
    completed: &BTreeSet<String>,
    now_epoch: u64,
) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "Engine plan (dry-run) — WORK={}",
        snap.work_dir.display()
    );
    let _ = writeln!(
        s,
        "(read-only: no orchestrator.lock, no queue-tx/state-tx, no worktrees, no writes)"
    );
    let _ = writeln!(s);

    // --- Cohort + budget/circuit-breaker gate (resolvers::budget) -------------------------------
    match &snap.cohort {
        Some(c) => {
            let admitted = c.admitted_total.unwrap_or(0);
            let _ = write!(
                s,
                "Cohort: {} · admission={}",
                c.batch_id.as_deref().unwrap_or("(no batch id)"),
                c.admission.map(|a| a.as_str()).unwrap_or("?"),
            );
            if let Some(r) = &c.admission_reason {
                let _ = write!(s, " (reason={r})");
            }
            if let Some(w) = c.wave {
                let _ = write!(s, " · wave={w}");
            }
            let _ = write!(s, " · admitted={admitted}");
            let elapsed_sec = c
                .started_at
                .as_deref()
                .and_then(parse_iso_utc)
                .map(|start| now_epoch.saturating_sub(start));
            match elapsed_sec {
                Some(e) => {
                    let _ = writeln!(s, " · age={}m", e / 60);
                }
                None => {
                    let _ = writeln!(s, " · age=? (start time unknown)");
                }
            }

            let counters = CohortCounters {
                admitted_total: admitted,
                age_minutes: elapsed_sec.unwrap_or(0) / 60,
                elapsed_sec: elapsed_sec.unwrap_or(0),
            };
            let _ = write!(s, "Budget/circuit-breaker gate: ");
            match admission_gate(counters, cfg.thresholds) {
                AdmissionGate::Continue => {
                    let _ = write!(s, "keep admitting (Continue)");
                }
                AdmissionGate::Close(reason) => {
                    let _ = write!(s, "close admission · причина={}", reason.as_str());
                }
            }
            let budget_disp = cfg
                .thresholds
                .budget_sec
                .map(|b| b.to_string())
                .unwrap_or_else(|| "0".to_string());
            let _ = writeln!(
                s,
                "  [COHORT_SIZE={}, COHORT_MAX_AGE={}m, COHORT_BUDGET_SEC={}]",
                cfg.thresholds.size, cfg.thresholds.max_age_minutes, budget_disp
            );
        }
        None => {
            let _ = writeln!(
                s,
                "Cohort: none (no active cohort) — budget/circuit-breaker gate N/A"
            );
        }
    }
    let _ = writeln!(s);

    // --- Active tasks: blocking class + per-task base reviewer tier (T-105) ----------------------
    // Domains/classes come from the batch manifest joined with descriptor states.
    let mut active_tasks: Vec<ActiveTask> = Vec::new();
    let _ = writeln!(
        s,
        "Active tasks (status · blocking class · base reviewer tier — REVIEWER_TIERING={}):",
        cfg.reviewer_tiering
    );
    let mut any_active = false;
    if let Some(b) = &snap.batch {
        for t in &b.tasks {
            any_active = true;
            let state = snap
                .descriptors
                .iter()
                .find(|d| d.id == t.id)
                .and_then(|d| d.state);
            let class = state.and_then(ActiveClass::from_state);
            let domain = Domain::parse(t.domain.as_deref().unwrap_or(""));
            if let Some(cls) = class {
                active_tasks.push(ActiveTask { domain, class: cls });
            }
            let class_disp = match class {
                Some(ActiveClass::Active) => "active",
                Some(ActiveClass::Terminal) => "terminal",
                None => "non-blocking",
            };
            let _ = write!(
                s,
                "  {} · status={} · class={class_disp}",
                t.id,
                state.map(|st| st.as_str()).unwrap_or("?"),
            );
            match t.level.as_deref().and_then(Level::from_field) {
                Some(level) => {
                    let _ = writeln!(
                        s,
                        " · reviewer={}",
                        base_reviewer(cfg.reviewer_tiering, level).as_str()
                    );
                }
                None => {
                    let _ = writeln!(s, " · reviewer=? (level unknown)");
                }
            }
        }
    }
    if !any_active {
        let _ = writeln!(s, "  (none — no batch manifest)");
    } else {
        let _ = writeln!(
            s,
            "  (only base_reviewer of the T-105 per-task resolvers is derivable from a static"
        );
        let _ = writeln!(
            s,
            "   snapshot; route_coder/route_reviewer/review_gate/review_cycle_decision need"
        );
        let _ = writeln!(
            s,
            "   per-round inputs — review.md, config flags, Реализовано: history — not held here.)"
        );
    }
    let _ = writeln!(s);

    // --- Admission plan over not-started queue candidates (resolvers::admission) -----------------
    let active_working = snap
        .descriptors
        .iter()
        .filter(|d| {
            matches!(
                d.state,
                Some(TaskState::Working) | Some(TaskState::InReview)
            )
        })
        .count();
    let free_slots = cfg.max_parallel.saturating_sub(active_working);

    let not_started: Vec<&_> = snap
        .queue
        .iter()
        .filter(|e| e.state == Some(TaskState::NotStarted))
        .collect();
    // Fresh-candidate conflict-domains are NOT in the read-only snapshot (the planner derives them
    // from task text); an empty domain conflicts with nothing, so packing here reflects readiness +
    // delivery lane (§11.1 — `next_major` is parked) + capacity + known active-task domains only.
    // This is stated in the NOTE below.
    let candidates: Vec<Candidate> = not_started
        .iter()
        .map(|e| Candidate {
            id: e.id.clone(),
            ready: is_ready(&e.prerequisites, completed),
            domain: Domain::parse(""),
            delivery: e.delivery_target,
        })
        .collect();

    let _ = writeln!(
        s,
        "Admission plan (capacity={free_slots} free slot(s) of MAX_PARALLEL={}):",
        cfg.max_parallel
    );
    match plan_admission(&candidates, &active_tasks, free_slots) {
        AdmissionOutcome::Admitted(ids) => {
            let _ = writeln!(s, "  would admit: {}", ids.join(", "));
        }
        AdmissionOutcome::Empty(reason) => {
            let _ = write!(s, "  admit nothing · причина={}", reason.as_str());
            match reason.to_close_reason() {
                Some(cr) => {
                    let _ = writeln!(s, " (would close admission · причина={})", cr.as_str());
                }
                None => {
                    let _ = writeln!(s, " (keep admission open, retry next round)");
                }
            }
        }
    }
    let _ = writeln!(s);

    let _ = writeln!(s, "Not-started candidates ({}):", not_started.len());
    for e in &not_started {
        // A next_major entry is parked out of the ordinary current-lane admission (§11.1), so it
        // is never "ready" for capture regardless of its prerequisites — label it as such.
        if e.delivery_target == DeliveryTarget::NextMajor {
            let _ = writeln!(s, "  {} · next_major (parked, not admitted)", e.id);
            continue;
        }
        let unmet = unmet_prerequisites(&e.prerequisites, completed);
        if unmet.is_empty() {
            let _ = writeln!(s, "  {} · ready", e.id);
        } else {
            let _ = writeln!(
                s,
                "  {} · blocked (unmet prereqs: {})",
                e.id,
                unmet.join(", ")
            );
        }
    }
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "NOTE: fresh-candidate conflict-domains are derived by the planner from task text and are"
    );
    let _ = writeln!(
        s,
        "      NOT part of the read-only snapshot; the admission plan reflects readiness + capacity"
    );
    let _ = writeln!(s, "      + known active-task domains only.");
    s
}

/// Parse a `YYYY-MM-DDTHH:MM:SS` UTC timestamp (as `cohort_state.md` `Начало когорты:` writes;
/// any trailing zone suffix `Z`/`+00:00` is ignored and treated as UTC) into epoch seconds.
/// `None` for a malformed timestamp.
///
/// This is deliberately MORE LENIENT than the engine's strict `orchestrail_engine::time::is_iso_utc`
/// / `iso_to_epoch` pair: the date/time separator may be `T` or a space, no trailing `Z` is
/// required, and fractional seconds are not supported — the historical shape `cohort_state.md`
/// emits. So it keeps its own lenient field scan here and reuses only the shared calendar core
/// `orchestrail_engine::time::days_from_civil` (previously duplicated inline), which is the actual
/// arithmetic this task consolidates.
fn parse_iso_utc(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    let year: i64 = s.get(0..4)?.parse().ok()?;
    if b.get(4) != Some(&b'-') || b.get(7) != Some(&b'-') {
        return None;
    }
    let month: i64 = s.get(5..7)?.parse().ok()?;
    let day: i64 = s.get(8..10)?.parse().ok()?;
    match b.get(10) {
        Some(&b'T') | Some(&b' ') => {}
        _ => return None,
    }
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    let min: i64 = s.get(14..16)?.parse().ok()?;
    let sec: i64 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Shared calendar core: days since 1970-01-01 for this civil (proleptic Gregorian) date.
    let days = days_from_civil(year, month, day);
    let secs = days * 86400 + hour * 3600 + min * 60 + sec;
    u64::try_from(secs).ok()
}

/// `lease <acquire|heartbeat|release|status>` — take / renew / release / inspect the engine's
/// interoperable owner lease. This native command deliberately does not resolve or execute a
/// `state-tx.ps1`: it uses the same structured on-disk record and transaction interlock directly,
/// so an arbitrary target-local script can never become an ownership authority.
fn native_lease_error(operation: &str, error: LeaseError) -> i32 {
    let code = match error {
        LeaseError::HeldLive { .. } => lease_exit::HELD_LIVE,
        LeaseError::NotOwner { .. } => lease_exit::NOT_OWNER,
        LeaseError::LegacyLock { .. } => lease_exit::LEGACY_LOCK,
        LeaseError::Corrupt { .. } => lease_exit::CORRUPT,
        LeaseError::InvalidInput(_) => lease_exit::USAGE,
        LeaseError::Io(_)
        | LeaseError::Json(_)
        | LeaseError::Busy
        | LeaseError::Stale { .. }
        | LeaseError::AddressMismatch { .. } => lease_exit::FAILED,
    };
    eprintln!("lease {operation}: {error}");
    code
}

/// `processor --once --live --work <.work>` — native queue-draining replacement for the legacy
/// processor. The compatibility `run` command remains a sandbox fixture; this command owns the
/// native checkpoint/effect ledger, ProcessKit-contained leaves, typed VCS, and owner lease.
const PROCESSOR_USAGE_ERROR_PREFIX: &str = "orchestrail-processor-usage:";

const RELEASE_VALUE_OPTIONS: &[&str] = &[
    "--work",
    "--root",
    "--base",
    "--owner",
    "--ttl",
    "--version",
    "--notes-file",
    "--subject",
    "--product",
    "--release-url",
    "--tag",
];
const RELEASE_SWITCH_OPTIONS: &[&str] = &["--live", "--resume", "--json"];

fn cmd_release_sync(args: &[String]) {
    if let Err(error) = validate_release_args(args) {
        eprintln!("release-sync: {error}");
        release_sync_usage();
    }
    let work = abs_path(&opt(args, "--work").expect("validated required --work"));
    let root = opt(args, "--root")
        .map(|root| abs_path(&root))
        .or_else(|| work.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| work.clone());
    if !roots_equivalent(&work, &root.join(".work")) {
        eprintln!("release-sync: --work must address the selected project root's .work directory");
        release_sync_usage();
    }
    let version = opt(args, "--version").expect("validated required --version");
    let resume = args.iter().any(|arg| arg == "--resume");
    let json = args.iter().any(|arg| arg == "--json");
    if let Err(error) = release::validate_release_version(&version) {
        eprintln!("release-sync: {error}");
        release_sync_usage();
    }
    if let Err(error) =
        VcsService::validate_release_identity(&version, opt(args, "--tag").as_deref())
    {
        eprintln!("release-sync: {error}");
        release_sync_usage();
    }
    let config = match orchestrail_engine::config::load(&work) {
        Ok(config) => config,
        Err(error) => release_sync_error(format!("invalid .work/config.md: {error}")),
    };
    if let Some(base) = opt(args, "--base").or(config.main_branch.clone())
        && let Err(error) = VcsService::validate_release_trunk(&base)
    {
        release_sync_error(error.to_string());
    }
    let minimum_ttl = config.call_deadline_secs.saturating_add(60);
    let ttl = match positive_u64_option(args, "--ttl", minimum_ttl.max(900)) {
        Ok(value) if value >= minimum_ttl => value,
        Ok(value) => release_sync_error(format!(
            "--ttl ({value}s) must exceed one contained leaf deadline ({}s) plus 60 seconds",
            config.call_deadline_secs
        )),
        Err(error) => release_sync_error(error),
    };
    let owner = opt(args, "--owner")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(fresh_owner_id);
    let lease = LeaseStore::new(&work);
    let lease_record =
        match acquire_processor_lease(&lease, resume, &owner, &root, ttl, now_epoch_secs()) {
            Ok(record) => record,
            Err(error) => exit(native_lease_error("release-sync acquire", error)),
        };
    let owner = lease_record.owner_id.clone();
    let heartbeat = LeaseHeartbeat::start(lease.clone(), &lease_record);
    let cancellation = heartbeat.cancellation_probe();
    let lost_authority = || {
        cancellation.is_cancelled()
            || !matches!(
                lease.status(now_epoch_secs()),
                Ok(LeaseStatus::Live { record, .. })
                    if record.owner_id == owner
                        && record.role == ENGINE_ROLE
                        && roots_equivalent(Path::new(&record.root), &root)
            )
    };

    let run = (|| -> Result<serde_json::Value, String> {
        if control_entry_exists(&work, &work.join("PAUSE")).map_err(|error| error.to_string())? {
            return Err("paused by .work/PAUSE before release synchronization".into());
        }
        let snapshot = Snapshot::try_load(&work).map_err(|error| error.to_string())?;
        if snapshot.batch.is_some() || snapshot.cohort.is_some() {
            return Err(
                "release synchronization requires no unfinished legacy cohort/batch".into(),
            );
        }
        let active_queue = snapshot.queue.iter().find(|entry| {
            !matches!(entry.state, Some(TaskState::NotStarted))
                && !entry.state.is_some_and(|state| state.is_terminal())
        });
        if !snapshot.descriptors.is_empty()
            || snapshot.integration.state != IntegrationState::None
            || active_queue.is_some()
        {
            let queue_detail = active_queue
                .map(|entry| format!("{}={}", entry.id, entry.status_literal))
                .unwrap_or_else(|| "none".into());
            return Err(format!(
                "release synchronization requires an idle recovered control plane (descriptors={}, integration={}, active_queue={queue_detail})",
                snapshot.descriptors.len(),
                snapshot.integration.state.as_str(),
            ));
        }
        let runtime = ProcessorRuntime::resume(config.processor.clone(), &work)
            .map_err(|error| format!("cannot inspect native runtime checkpoint: {error}"))?;
        if runtime.state().batch.is_some()
            || !runtime.state().tasks.is_empty()
            || runtime.state().blocked_reason.is_some()
            || !matches!(runtime.state().phase, Phase::Recovery | Phase::Idle)
            || !runtime.pending_effects().is_empty()
        {
            return Err(
                "release synchronization requires no unfinished native cohort/tasks/effects".into(),
            );
        }
        if lost_authority() {
            return Err("release-sync lease ownership was lost before VCS synchronization".into());
        }
        let vcs = VcsService::discover(&root).map_err(|error| error.to_string())?;
        vcs.ensure_selected_repository_root(&root)
            .map_err(|error| error.to_string())?;
        vcs.ensure_no_managed_workspaces(&work)
            .map_err(|error| error.to_string())?;
        vcs.ensure_control_plane_ignored(&work)
            .map_err(|error| error.to_string())?;
        let base = match opt(args, "--base").or(config.main_branch.clone()) {
            Some(base) => base,
            None => vcs
                .snapshot()
                .map_err(|error| error.to_string())?
                .branch
                .ok_or_else(|| {
                    "primary checkout has no branch; pass --base or set MAIN_BRANCH".to_string()
                })?,
        };
        let synced = vcs
            .sync_release_trunk_with_cancellation(&base, lost_authority)
            .map_err(|error| error.to_string())?;
        let tag = vcs
            .verify_release_tag(
                &version,
                opt(args, "--tag").as_deref(),
                &synced.current,
                &base,
            )
            .map_err(|error| error.to_string())?;
        if lost_authority() {
            return Err("release-sync lease ownership was lost after VCS synchronization".into());
        }

        let registry =
            dependency_graph::default_registry_path().map_err(|error| error.to_string())?;
        let source = dependency_graph::registered_project_for_root(&registry, &root)
            .map_err(|error| error.to_string())?;
        let release_id = release::stable_release_id(&source.id, &version);
        let frozen_fingerprint =
            release::canonical_release_fingerprint(&root, &source.id, &version, &tag.revision)
                .map_err(|error| error.to_string())?;
        let frozen = frozen_fingerprint.is_some();
        if resume && !frozen {
            return Err(format!(
                "cannot resume release {version} because no canonical release record exists"
            ));
        }
        if !resume && frozen {
            return Err(format!(
                "release {version} already has canonical content; retry with --resume"
            ));
        }
        if lost_authority() {
            return Err("release-sync lease ownership was lost before dependency refresh".into());
        }
        let content = if resume {
            None
        } else {
            let notes_base = vcs
                .release_notes_range_base(&synced.previous, &tag.revision)
                .map_err(|error| error.to_string())?;
            let verification_policy = policy::load(&work).map_err(|error| error.to_string())?;
            let mut external_config =
                HeadlessConfig::new(work.clone(), root.clone(), config.codex.clone());
            external_config.call_deadline = Duration::from_secs(config.call_deadline_secs);
            external_config.call_output_max_bytes = config.call_output_max_bytes;
            external_config.cohort_budget_secs = config.processor.cohort_budget_secs;
            external_config.reviewer_tiering = config.reviewer_tiering;
            external_config.review_min_passes = config.review_min_passes;
            external_config.knowledge_base = config.knowledge_base;
            external_config.knowledge_ttl_batches = config.knowledge_ttl_batches;
            external_config.knowledge_cap_per_area = config.knowledge_cap_per_area;
            external_config.ci_watch = false;
            external_config.verification_mode = config.verification_mode;
            external_config.verification_commands = config.verification_commands.clone();
            external_config.policy_verification_commands =
                verification_policy.required_verification_commands.clone();
            external_config.smoke_cmd = config.smoke_cmd.clone();
            external_config.cancellation_probe = Some(cancellation.clone());
            let external =
                HeadlessExternalPort::new(external_config).map_err(|error| error.to_string())?;
            let mut port =
                FileVcsPort::discover(&work, &root, external).map_err(|error| error.to_string())?;
            let protected_before = release::protected_work_fingerprint(
                &work,
                release::ReleaseLeafSurface::DependencyCurator,
                &release_id,
            )
            .map_err(|error| error.to_string())?;
            let graph_outcome = port.refresh_dependency_graph_for_release(
                &release_id,
                &synced.current,
                now_epoch_secs(),
                lost_authority,
            );
            let protected_after = release::protected_work_fingerprint(
                &work,
                release::ReleaseLeafSurface::DependencyCurator,
                &release_id,
            )
            .map_err(|error| error.to_string())?;
            if protected_before != protected_after {
                return Err(
                    "dependency-curator leaf modified release-protected .work state".into(),
                );
            }
            let observed_release =
                release::canonical_release_fingerprint(&root, &source.id, &version, &tag.revision)
                    .map_err(|error| error.to_string())?;
            if observed_release != frozen_fingerprint {
                return Err(
                    "dependency-curator leaf changed the canonical release authority".into(),
                );
            }
            match graph_outcome.map_err(|error| error.to_string())? {
                orchestrail_engine::processor::LeafOutcome::Completed { .. } => {}
                outcome => {
                    return Err(format!(
                        "dependency graph refresh blocked release delivery: {outcome:?}"
                    ));
                }
            }
            vcs.verify_release_primary(&base, &synced.current)
                .map_err(|error| error.to_string())?;
            if lost_authority() {
                return Err(
                    "release-sync lease ownership was lost after dependency refresh".into(),
                );
            }
            let source = dependency_graph::registered_project_for_root(&registry, &root)
                .map_err(|error| error.to_string())?;
            let requested_products = repeated_options(args, "--product");
            let release_products =
                release::release_products_for_source(&source.products, &requested_products)
                    .map_err(|error| error.to_string())?;
            let release_url = opt(args, "--release-url").unwrap_or_default();
            let notes_path = opt(args, "--notes-file")
                .map(|path| abs_path(&path))
                .unwrap_or_else(|| {
                    work.join("release_notifications")
                        .join(format!("{release_id}.md"))
                });
            let notes_binding = release::ReleaseNotesBinding {
                release_id: &release_id,
                version: &version,
                tag: &tag.tag,
                release_revision: &tag.revision,
                products: &release_products,
                release_url: &release_url,
            };
            release::validate_notes_binding(&notes_binding).map_err(|error| error.to_string())?;
            if opt(args, "--notes-file").is_none()
                && !release::composed_notes_complete(&work, &notes_path, &notes_binding)
                    .map_err(|error| error.to_string())?
            {
                let protected_before = release::protected_work_fingerprint(
                    &work,
                    release::ReleaseLeafSurface::Notes,
                    &release_id,
                )
                .map_err(|error| error.to_string())?;
                let notes_outcome = port.compose_release_notes_for_release(
                    &release_id,
                    &synced.current,
                    now_epoch_secs(),
                    &ReleaseNotesRequest {
                        version: version.clone(),
                        tag: tag.tag.clone(),
                        release_revision: tag.revision.clone(),
                        previous_head: notes_base.clone(),
                        current_head: tag.revision.clone(),
                        products: release_products.clone(),
                        release_url: release_url.clone(),
                        notes_path: notes_path.clone(),
                        evidence_path: work
                            .join("release_notifications")
                            .join(format!("{release_id}.range.json")),
                    },
                    lost_authority,
                );
                let protected_after = release::protected_work_fingerprint(
                    &work,
                    release::ReleaseLeafSurface::Notes,
                    &release_id,
                )
                .map_err(|error| error.to_string())?;
                if protected_before != protected_after {
                    return Err("release-notes leaf modified protected .work state".into());
                }
                let observed_release = release::canonical_release_fingerprint(
                    &root,
                    &source.id,
                    &version,
                    &tag.revision,
                )
                .map_err(|error| error.to_string())?;
                if observed_release != frozen_fingerprint {
                    return Err("release-notes leaf changed the canonical release authority".into());
                }
                match notes_outcome.map_err(|error| error.to_string())? {
                    orchestrail_engine::processor::LeafOutcome::Completed { .. } => {}
                    outcome => {
                        return Err(format!(
                            "release-notes composition blocked release delivery: {outcome:?}"
                        ));
                    }
                }
            }
            let body = release::read_canonical_notes(&work, &notes_path)
                .map_err(|error| error.to_string())?;
            Some(ReleaseContent {
                subject: opt(args, "--subject")
                    .unwrap_or_else(|| format!("Release {} {version}", source.name)),
                body,
                products: (!requested_products.is_empty()).then_some(release_products),
                release_url,
                source_revision: tag.revision.clone(),
            })
        };
        vcs.verify_release_primary(&base, &synced.current)
            .map_err(|error| error.to_string())?;
        if lost_authority() {
            return Err("release-sync lease ownership was lost before inbox delivery".into());
        }
        let result = release::distribute_with_cancellation(
            &ReleaseRequest {
                source_root: root.clone(),
                registry_path: registry,
                version: version.clone(),
                verified_source_revision: tag.revision.clone(),
                content,
                occurred_at: epoch_to_iso(now_epoch_secs()),
            },
            lost_authority,
        )
        .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({
            "release_sync": if result.failure_count == 0 { "completed" } else { "partial" },
            "trunk": base,
            "previous_head": synced.previous,
            "synced_head": synced.current,
            "tag": tag.tag,
            "tag_revision": tag.revision,
            "release": result,
        }))
    })();

    let heartbeat_result = heartbeat.stop().map_err(|error| error.to_string());
    let release_result = lease.release(&owner, now_epoch_secs());
    match (run, heartbeat_result, release_result) {
        (Ok(value), Ok(()), Ok(_)) => {
            let release = &value["release"];
            if json {
                println!("{value}");
            } else {
                println!(
                    "release-sync: trunk={} tag={} release={} targets={} delivered={} failures={} unaudited={}",
                    value["synced_head"].as_str().unwrap_or(""),
                    value["tag"].as_str().unwrap_or(""),
                    release["release_id"].as_str().unwrap_or(""),
                    release["target_count"].as_u64().unwrap_or(0),
                    release["delivered_count"].as_u64().unwrap_or(0),
                    release["failure_count"].as_u64().unwrap_or(0),
                    release["unaudited_projects"].as_array().map_or(0, Vec::len),
                );
            }
            if release["target_count"].as_u64().unwrap_or(0) == 0 {
                eprintln!(
                    "release-sync: no registered dependents matched this release; every existing consumer must run its processor dependency-graph refresh before it can join a future frozen audience"
                );
            }
            if let Some(projects) = release["unaudited_projects"].as_array()
                && !projects.is_empty()
            {
                let projects = projects
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ");
                eprintln!(
                    "release-sync: warning: registered projects with no audited dependency graph may be missing from the frozen audience: {projects}"
                );
            }
            if release["failure_count"].as_u64().unwrap_or(0) > 0 {
                exit(6);
            }
        }
        (run, heartbeat, release) => {
            let mut errors = Vec::new();
            if let Err(error) = run {
                errors.push(error);
            }
            if let Err(error) = heartbeat {
                errors.push(format!("lease renewal failed: {error}"));
            }
            if let Err(error) = release {
                errors.push(format!("lease release failed: {error}"));
            }
            release_sync_error(errors.join("; "));
        }
    }
}

fn validate_release_args(args: &[String]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    let mut index = 2;
    while index < args.len() {
        let option = args[index].as_str();
        if !seen.insert(option.to_owned()) && option != "--product" {
            return Err(format!("duplicate option {option}"));
        }
        if RELEASE_SWITCH_OPTIONS.contains(&option) {
            index += 1;
            continue;
        }
        if RELEASE_VALUE_OPTIONS.contains(&option) {
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("{option} requires a non-empty value"))?;
            if value.is_empty() || value.starts_with("--") {
                return Err(format!("{option} requires a non-empty value"));
            }
            index += 2;
            continue;
        }
        return Err(format!("unknown release-sync argument {option:?}"));
    }
    for required in ["--work", "--version", "--live"] {
        if !seen.contains(required) {
            return Err(format!("{required} is required"));
        }
    }
    let resume = seen.contains("--resume");
    if resume {
        for option in ["--notes-file", "--subject", "--product", "--release-url"] {
            if seen.contains(option) {
                return Err(format!(
                    "--resume cannot be combined with {option}; canonical release content is frozen"
                ));
            }
        }
    }
    Ok(())
}

fn repeated_options(args: &[String], key: &str) -> Vec<String> {
    args.windows(2)
        .filter(|pair| pair[0] == key)
        .map(|pair| pair[1].clone())
        .collect()
}

fn release_sync_error(message: impl AsRef<str>) -> ! {
    eprintln!("release-sync: {}", message.as_ref());
    exit(1)
}

fn release_sync_usage() -> ! {
    eprintln!(
        "usage: orchestrail-engine release-sync --live --work <.work-dir> [--root <repo>] [--base <branch>] --version <version> [--tag <tag>]\n\
         initial: [--notes-file <.work/release_notifications/file>] [--subject <text>] [--product <ecosystem:name>]... [--release-url <url>]\n\
                  (without --notes-file, a contained release-notes leaf creates the canonical file)\n\
         resume:  --resume (canonical content and audience are reused) [--json]"
    );
    exit(2)
}

fn cmd_processor(args: &[String]) {
    if let Err(error) = validate_processor_args(args) {
        eprintln!("processor: {error}");
        eprintln!(
            "usage: processor --once --live --work <.work-dir> [--root <repo>] [--base <branch>]\n\
             \x20                 [--batch <id>] [--owner <id>] [--ttl <seconds>] [--continue] [--max-turns <n>]\n\
             \x20                 [--max-effects <n>] [--json]"
        );
        exit(2);
    }
    let work = match opt(args, "--work").filter(|value| !value.is_empty()) {
        Some(value) => abs_path(&value),
        None => {
            eprintln!("processor: --work <.work-dir> is required");
            exit(2);
        }
    };
    let root = match opt(args, "--root") {
        Some(value) => abs_path(&value),
        None => match work.parent() {
            Some(parent) => parent.to_path_buf(),
            None => {
                eprintln!("processor: --root is required when --work has no parent directory");
                exit(2);
            }
        },
    };
    if !roots_equivalent(&work, &root.join(".work")) {
        eprintln!("processor: --work must address the selected project root's .work directory");
        exit(2);
    }
    let config = match orchestrail_engine::config::load(&work) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("processor: invalid .work/config.md: {error}");
            exit(2);
        }
    };
    if let Err(error) = ensure_supported_processor_config(&config) {
        eprintln!("processor: {error}");
        exit(2);
    }
    // A configured/explicit base is pure input. The fallback typed-VCS branch discovery is
    // intentionally deferred until after the cold-start PAUSE gate below, so an operator hold
    // can succeed even when the checkout is temporarily unavailable.
    let configured_base = opt(args, "--base")
        .filter(|value| !value.is_empty())
        .or(config.main_branch.clone());
    let batch_id = opt(args, "--batch")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| native_default_batch_id(now_epoch_secs(), std::process::id()));
    let max_turns = match positive_usize_option(args, "--max-turns", 256) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("processor: {error}");
            exit(2);
        }
    };
    let max_effects_per_turn = match positive_usize_option(args, "--max-effects", 1_024) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("processor: {error}");
            exit(2);
        }
    };
    let minimum_ttl = config.call_deadline_secs.saturating_add(60);
    let ttl = match positive_u64_option(args, "--ttl", minimum_ttl.max(900)) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("processor: {error}");
            exit(2);
        }
    };
    if ttl < minimum_ttl {
        eprintln!(
            "processor: --ttl ({ttl}s) must exceed one contained leaf deadline ({}s) plus 60 seconds",
            config.call_deadline_secs
        );
        exit(2);
    }
    let requested_owner = opt(args, "--owner")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(fresh_owner_id);
    let continue_requested = args.iter().any(|arg| arg == "--continue");
    let json = args.iter().any(|arg| arg == "--json");
    let lease = LeaseStore::new(&work);
    let lease_record = match acquire_processor_lease(
        &lease,
        continue_requested,
        &requested_owner,
        &root,
        ttl,
        now_epoch_secs(),
    ) {
        Ok(record) => record,
        Err(error) => exit(native_lease_error("processor acquire/continue", error)),
    };
    let owner = lease_record.owner_id.clone();
    let heartbeat = LeaseHeartbeat::start(lease.clone(), &lease_record);
    let lease_cancellation = heartbeat.cancellation_probe();

    let run_result = (|| -> Result<NativeLoopOutcome, String> {
        // A cold-start PAUSE must stop before Phase-0 inspection *and* before creating the VCS or
        // headless-process boundaries. Keep the later port-level check as well: it closes the
        // race where an operator creates PAUSE while those read-only boundaries initialize.
        let control = ControlPlane::new(&work).map_err(|error| error.to_string())?;
        if control_entry_exists(&work, &work.join("PAUSE")).map_err(|error| error.to_string())? {
            control
                .write_pause_status(&ProcessorState::default(), &epoch_to_iso(now_epoch_secs()))
                .map_err(|error| error.to_string())?;
            return Ok(NativeLoopOutcome::Held {
                reason:
                    "paused by .work/PAUSE before phase-0 recovery; remove it and rerun to resume"
                        .into(),
            });
        }
        // Legacy Phase 0.1 establishes this invariant before it enumerates or repairs any
        // control-plane/VCS state. In a colocated checkout the typed VCS boundary writes only
        // `.git/info/exclude`, which is honored by both Git and JJ and cannot dirty the product
        // tree. Pure JJ uses the equivalent exclude file in its private Git store.
        if lease_cancellation.is_cancelled() {
            return Err(
                "native lease ownership was lost before Phase-0 VCS safety preflight".into(),
            );
        }
        let phase_zero_vcs = match VcsService::discover(&root) {
            Ok(vcs) => vcs,
            Err(error) if configured_base.is_none() => {
                return Err(format!(
                    "{PROCESSOR_USAGE_ERROR_PREFIX}cannot determine publication branch through typed VCS: {error}"
                ));
            }
            Err(error) => {
                return Err(format!("phase-0 VCS safety preflight failed: {error}"));
            }
        };
        phase_zero_vcs
            .ensure_selected_repository_root(&root)
            .map_err(|error| format!("phase-0 VCS safety preflight failed: {error}"))?;
        phase_zero_vcs
            .ensure_control_plane_ignored(&work)
            .map_err(|error| format!("phase-0 VCS safety preflight failed: {error}"))?;
        if lease_cancellation.is_cancelled() {
            return Err(
                "native lease ownership was lost during Phase-0 VCS safety preflight".into(),
            );
        }
        let base = match configured_base {
            Some(value) => value,
            None => match phase_zero_vcs.snapshot() {
                Ok(snapshot) => match snapshot.branch {
                    Some(branch) => branch,
                    None => {
                        return Err(format!(
                            "{PROCESSOR_USAGE_ERROR_PREFIX}primary checkout has no branch; pass --base or set MAIN_BRANCH"
                        ));
                    }
                },
                Err(error) => {
                    return Err(format!(
                        "{PROCESSOR_USAGE_ERROR_PREFIX}cannot determine publication branch through typed VCS: {error}"
                    ));
                }
            },
        };
        // Required policy commands are an additive Phase-4 gate, not prose for a model to
        // remember. Capture the same decoded list in the headless ProcessKit profile and the
        // native evidence validator; a later policy edit is detected by `FileVcsPort` and held
        // for a restart before it can authorize publication.
        let verification_policy = policy::load(&work).map_err(|error| error.to_string())?;
        let verification_mode = orchestrail_engine::config::mode_with_required_policy_commands(
            config.verification_mode,
            config.verification_mode_explicit,
            !verification_policy
                .required_verification_commands
                .is_empty(),
        );
        let mut external_config =
            HeadlessConfig::new(work.clone(), root.clone(), config.codex.clone());
        external_config.call_deadline = Duration::from_secs(config.call_deadline_secs);
        external_config.call_output_max_bytes = config.call_output_max_bytes;
        external_config.cohort_budget_secs = config.processor.cohort_budget_secs;
        external_config.reviewer_tiering = config.reviewer_tiering;
        external_config.review_min_passes = config.review_min_passes;
        external_config.knowledge_base = config.knowledge_base;
        external_config.knowledge_ttl_batches = config.knowledge_ttl_batches;
        external_config.knowledge_cap_per_area = config.knowledge_cap_per_area;
        external_config.ci_watch = config.ci_watch;
        external_config.ci_deadline = Duration::from_secs(config.publish_ci_deadline_secs);
        external_config.ci_backoff = Duration::from_secs(config.publish_ci_backoff_secs);
        external_config.verification_mode = verification_mode;
        external_config.verification_commands = config.verification_commands.clone();
        external_config.policy_verification_commands =
            verification_policy.required_verification_commands.clone();
        external_config.smoke_cmd = config.smoke_cmd.clone();
        external_config.cancellation_probe = Some(lease_cancellation.clone());
        let external =
            HeadlessExternalPort::new(external_config).map_err(|error| error.to_string())?;
        let mut port = FileVcsPort::discover_with_publication(&work, &root, external, config.push)
            .map_err(|error| error.to_string())?
            .with_approval_deadline_secs(config.approval_deadline_secs)
            .with_notification_command(config.notify_command.clone())
            .with_verification_profile(verification::profile_with_policy_commands(
                verification_mode,
                &config.verification_commands,
                config.smoke_cmd.as_deref(),
                &verification_policy.required_verification_commands,
            ))
            .with_docs_only_exemption(
                verification_policy
                    .required_verification_commands
                    .is_empty()
                    && (!config.verification_mode_explicit
                        || !matches!(
                            config.verification_mode,
                            orchestrail_engine::config::VerificationMode::Disabled
                        )),
            );
        // PAUSE is checked before Phase-0 compatibility recovery as well: that recovery can
        // repair control-plane artifacts, while Orchestra requires a cold-start pause to begin
        // no new mutation at all. No runtime checkpoint is created here; status/journal are the
        // existing derived operator artifacts and the caller releases its owner-checked lease.
        if port.pause_requested().map_err(|error| error.to_string())? {
            port.write_pause_status(&ProcessorState::default())
                .map_err(|error| error.to_string())?;
            return Ok(NativeLoopOutcome::Held {
                reason:
                    "paused by .work/PAUSE before phase-0 recovery; remove it and rerun to resume"
                        .into(),
            });
        }
        let mut imported_runtime = None;
        if !control_entry_exists(&work, &work.join(RUNTIME_CHECKPOINT_FILE))
            .map_err(|error| error.to_string())?
        {
            if lease_cancellation.is_cancelled() {
                return Err("native lease ownership was lost before Phase-0 recovery".into());
            }
            let mut plan = port.recovery_plan().map_err(|error| error.to_string())?;
            if lease_cancellation.is_cancelled() {
                return Err("native lease ownership was lost during Phase-0 inspection".into());
            }
            if legacy_recovery_requires_operator(&plan) {
                let (stabilized, repaired) = port
                    .stabilize_safe_control_recovery(plan)
                    .map_err(|error| format!("phase-0 recovery hold: {error}"))?;
                plan = stabilized;
                if let Some(mut state) = port
                    .import_legacy_cohort(&plan, now_epoch_secs())
                    .map_err(|error| format!("phase-0 legacy import failed: {error}"))?
                {
                    port.recheck_legacy_imported_admission(
                        &mut state,
                        &config.processor,
                        now_epoch_secs(),
                    )
                    .map_err(|error| format!("phase-0 legacy admission recheck failed: {error}"))?;
                    imported_runtime = Some(
                        ProcessorRuntime::import_legacy(config.processor.clone(), &work, state)
                            .map_err(|error| format!("phase-0 legacy import failed: {error}"))?,
                    );
                } else if legacy_recovery_requires_operator(&plan) {
                    return Err(format!(
                        "phase-0 recovery hold after {repaired} safe control repair(s): {}",
                        describe_legacy_recovery_hold(&plan),
                    ));
                }
            }
        }
        let mut executor = NativeExecutor::new(port).with_cancellation_probe(lease_cancellation);
        let mut runtime = match imported_runtime {
            Some(runtime) => runtime,
            None => ProcessorRuntime::resume(config.processor, &work)
                .map_err(|error| error.to_string())?,
        };
        run_until_queue_exhausted(
            &mut runtime,
            &mut executor,
            &NativeLoopConfig {
                batch_id: batch_id.clone(),
                base: base.clone(),
                occurred_at: epoch_to_iso(now_epoch_secs()),
                max_turns,
                max_effects_per_turn,
            },
        )
        .map_err(|error| error.to_string())
    })();

    let heartbeat_result = heartbeat.stop().map_err(|error| error.to_string());
    let run_result = match (run_result, heartbeat_result) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(format!("processor lease renewal failed: {error}")),
        (Err(error), Err(heartbeat_error)) => Err(format!(
            "{error}; additionally processor lease renewal failed: {heartbeat_error}"
        )),
    };
    let release_result = lease.release(&owner, now_epoch_secs());
    match (run_result, release_result) {
        (Ok(outcome), Ok(_)) => {
            if json {
                let (outcome, reason) = match outcome {
                    NativeLoopOutcome::Completed => ("completed", None),
                    NativeLoopOutcome::Idle => ("idle", None),
                    NativeLoopOutcome::Held { reason } => ("held", Some(reason)),
                    NativeLoopOutcome::Escalated { count } => (
                        "escalated",
                        Some(format!("{count} task(s) require manual review")),
                    ),
                };
                println!(
                    "{}",
                    serde_json::json!({
                        "processor": outcome,
                        "batch": batch_id,
                        "reason": reason,
                    })
                );
            } else {
                match outcome {
                    NativeLoopOutcome::Completed => println!(
                        "processor: completed all current-lane cohorts (starting {batch_id})"
                    ),
                    NativeLoopOutcome::Idle => println!("processor: idle"),
                    NativeLoopOutcome::Held { reason } => println!("processor: held · {reason}"),
                    NativeLoopOutcome::Escalated { count } => println!(
                        "processor: queue stopped · {count} escalated task(s) require manual review"
                    ),
                }
            }
        }
        (Err(error), Ok(_)) => {
            let (exit_code, error) = processor_error_exit(&error);
            eprintln!("processor: {error}");
            exit(exit_code);
        }
        (Ok(_), Err(error)) => {
            eprintln!("processor: completed but failed to release its lease: {error}");
            exit(1);
        }
        (Err(error), Err(release_error)) => {
            eprintln!("processor: {error}; additionally failed to release lease: {release_error}");
            exit(1);
        }
    }
}

const PROCESSOR_VALUE_OPTIONS: &[&str] = &[
    "--work",
    "--root",
    "--base",
    "--batch",
    "--owner",
    "--ttl",
    "--max-turns",
    "--max-effects",
];
const PROCESSOR_SWITCH_OPTIONS: &[&str] = &["--once", "--live", "--continue", "--json"];

/// Validate the complete native processor argv before acquiring its owner lease. Unknown,
/// duplicate, missing, and empty arguments are errors: silently accepting a misspelled safety
/// limit would run a materially different orchestration policy than the operator requested.
fn validate_processor_args(args: &[String]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    let mut index = 2;
    while index < args.len() {
        let option = args[index].as_str();
        if !seen.insert(option.to_owned()) {
            return Err(format!("duplicate option {option}"));
        }
        if PROCESSOR_SWITCH_OPTIONS.contains(&option) {
            index += 1;
            continue;
        }
        if PROCESSOR_VALUE_OPTIONS.contains(&option) {
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("{option} requires a non-empty value"))?;
            if value.is_empty() || value.starts_with("--") {
                return Err(format!("{option} requires a non-empty value"));
            }
            index += 2;
            continue;
        }
        return Err(format!("unknown processor argument {option:?}"));
    }
    for required in ["--once", "--live"] {
        if !seen.contains(required) {
            return Err(format!("{required} is required"));
        }
    }
    Ok(())
}

fn positive_usize_option(args: &[String], key: &str, default: usize) -> Result<usize, String> {
    match opt(args, key) {
        None => Ok(default),
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("{key} must be a positive integer")),
    }
}

fn positive_u64_option(args: &[String], key: &str, default: u64) -> Result<u64, String> {
    match opt(args, key) {
        None => Ok(default),
        Some(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("{key} must be a positive integer")),
    }
}

fn processor_error_exit(error: &str) -> (i32, &str) {
    match error.strip_prefix(PROCESSOR_USAGE_ERROR_PREFIX) {
        Some(message) => (2, message),
        None => (1, error),
    }
}

/// Acquire this native processor's lease, or resume only an address-matched predecessor.
///
/// `--continue` deliberately never adopts a record from another root or role. A matching live
/// record retains its owner identity and is renewed through the generation CAS; a matching stale
/// record receives a new owner through the normal stale-only takeover transaction. All other
/// states use the ordinary cold acquire path, which continues to refuse a foreign live/stale
/// lease rather than silently treating it as resumable.
fn acquire_processor_lease(
    store: &LeaseStore,
    continue_requested: bool,
    requested_owner: &str,
    root: &Path,
    ttl_seconds: u64,
    now_secs: u64,
) -> Result<LeaseRecord, LeaseError> {
    if !continue_requested {
        return store.acquire(requested_owner, root, ttl_seconds, now_secs);
    }

    match store.status(now_secs)? {
        LeaseStatus::Live { record, .. } if processor_resume_address_matches(&record, root) => {
            store.heartbeat(&record.owner_id, Some(record.generation), now_secs)
        }
        LeaseStatus::Stale { record, .. } if processor_resume_address_matches(&record, root) => {
            store.takeover_addressed(requested_owner, root, ENGINE_ROLE, ttl_seconds, now_secs)
        }
        _ => store.acquire(requested_owner, root, ttl_seconds, now_secs),
    }
}

fn processor_resume_address_matches(record: &LeaseRecord, root: &Path) -> bool {
    record.role == ENGINE_ROLE && roots_equivalent(Path::new(&record.root), root)
}

/// A lease owner is an authority token, not a process label.  PIDs can be reused after a crash,
/// so the default must be fresh for every invocation just as Orchestra's `state-tx` owner is.
fn fresh_owner_id() -> String {
    format!("orchestrail-engine-{}", uuid::Uuid::new_v4().simple())
}

/// Reject recognised legacy switches whose safety contract has not yet been implemented through
/// the typed VCS boundary.  This runs before repository discovery or owner-lease acquisition, so
/// a configuration that promises linear history can never start a run that later publishes merge
/// commits instead.
fn ensure_supported_processor_config(
    config: &orchestrail_engine::config::EngineConfig,
) -> Result<(), String> {
    if config.publish_linear_history {
        return Err(
            "PUBLISH_LINEAR_HISTORY: true requires a crash-safe, byte-identical typed linearizer; the native engine refuses to publish a merge topology until that mechanism is available".into(),
        );
    }
    Ok(())
}

fn legacy_recovery_requires_operator(plan: &RecoveryPlan) -> bool {
    plan.is_blocked()
        || !plan.actions.is_empty()
        || !matches!(plan.disposition, RecoveryDisposition::Idle)
}

fn describe_legacy_recovery_hold(plan: &RecoveryPlan) -> String {
    let blocker = plan
        .blockers
        .first()
        .map(|reason| format!("; first blocker: {reason}"))
        .unwrap_or_default();
    format!(
        "existing control plane requires disposition {:?} with {} planned action(s){blocker}",
        plan.disposition,
        plan.actions.len(),
    )
}

fn native_default_batch_id(epoch_secs: u64, process_id: u32) -> String {
    format!("B-{epoch_secs}-p{process_id}")
}

fn cmd_lease(args: &[String]) {
    let op = match args.get(2).map(|s| s.as_str()).and_then(LeaseOp::from_arg) {
        Some(op) => op,
        None => {
            eprintln!(
                "usage: lease <acquire|takeover|heartbeat|release|status> [--work <dir>] [--root <dir>]\n\
                 \x20            [--script <state-tx.ps1>] [--owner <id>] [--ttl <sec>] [--session <id>]\n\
                 \x20            [--pid <n>] [--json]\n\
                 (uses the native interoperable lease transaction; `--script` is a compatibility no-op,\n\
                  role=engine — never impersonates processor, permits only explicit stale takeover, never --force,\n\
                  never rm -rf a lease)"
            );
            exit(lease_exit::USAGE);
        }
    };

    // All paths are absolutised, but no CLI-supplied `--script` is resolved or executed. It is
    // tolerated as a compatibility no-op while callers migrate from the legacy wrapper.
    let work = abs_path(&opt(args, "--work").unwrap_or_else(|| ".work".to_string()));
    let root = match opt(args, "--root") {
        Some(r) => abs_path(&r),
        None => work
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| work.clone()),
    };
    exit(native_lease_command(op, &work, &root, args));
}

fn native_lease_command(op: LeaseOp, work: &Path, root: &Path, args: &[String]) -> i32 {
    let store = LeaseStore::new(work);
    let now = now_epoch_secs();
    let json = args.iter().any(|arg| arg == "--json");
    let explicit_owner = opt(args, "--owner").filter(|value| !value.trim().is_empty());
    let owner = explicit_owner.clone().unwrap_or_else(fresh_owner_id);
    match op {
        LeaseOp::Acquire => {
            let ttl = match opt(args, "--ttl") {
                Some(value) => match value.parse::<u64>() {
                    Ok(value) if value > 0 => value,
                    _ => {
                        eprintln!("lease acquire: --ttl must be a positive integer");
                        return lease_exit::USAGE;
                    }
                },
                None => 900,
            };
            match store.acquire(&owner, root, ttl, now) {
                Ok(record) => {
                    print_native_lease_acquired(&record.owner_id, record.generation, json);
                    lease_exit::OK
                }
                Err(error) => native_lease_error("acquire", error),
            }
        }
        LeaseOp::Takeover => {
            let ttl = match opt(args, "--ttl") {
                Some(value) => match value.parse::<u64>() {
                    Ok(value) if value > 0 => value,
                    _ => {
                        eprintln!("lease takeover: --ttl must be a positive integer");
                        return lease_exit::USAGE;
                    }
                },
                None => 900,
            };
            match store.takeover(&owner, root, ttl, now) {
                Ok(record) => {
                    print_native_lease_taken_over(
                        &record.owner_id,
                        record.generation,
                        record.taken_over_from.as_deref(),
                        json,
                    );
                    lease_exit::OK
                }
                Err(error) => native_lease_error("takeover", error),
            }
        }
        LeaseOp::Heartbeat => {
            if explicit_owner.is_none() {
                eprintln!("lease heartbeat: a non-empty --owner <id> is required");
                return lease_exit::USAGE;
            }
            match store.heartbeat(&owner, None, now) {
                Ok(record) => {
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "owner": record.owner_id,
                                "generation": record.generation,
                                "renewed": true,
                            })
                        );
                    } else {
                        println!(
                            "lease heartbeat renewed owner={} generation={}",
                            record.owner_id, record.generation
                        );
                    }
                    lease_exit::OK
                }
                Err(error) => native_lease_error("heartbeat", error),
            }
        }
        LeaseOp::Release => {
            if explicit_owner.is_none() {
                eprintln!("lease release: a non-empty --owner <id> is required");
                return lease_exit::USAGE;
            }
            match store.release(&owner, now) {
                Ok(released) => {
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({ "released": released, "owner": owner })
                        );
                    } else if released {
                        println!("lease released owner={owner}");
                    } else {
                        println!("lease not-held owner={owner}");
                    }
                    lease_exit::OK
                }
                Err(error) => native_lease_error("release", error),
            }
        }
        LeaseOp::Status => match store.status(now) {
            Ok(status) => print_native_lease_status(status, json),
            Err(error) => native_lease_error("status", error),
        },
    }
}

fn print_native_lease_acquired(owner: &str, generation: u64, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "role": "engine",
                "owner": owner,
                "generation": generation,
                "adopted_stale": false,
            })
        );
    } else {
        println!("lease acquired role=engine owner={owner} generation={generation}");
    }
}

fn print_native_lease_taken_over(
    owner: &str,
    generation: u64,
    taken_over_from: Option<&str>,
    json: bool,
) {
    let adopted_stale = taken_over_from.is_some();
    if json {
        println!(
            "{}",
            serde_json::json!({
                "role": "engine",
                "owner": owner,
                "generation": generation,
                "adopted_stale": adopted_stale,
                "taken_over_from": taken_over_from,
            })
        );
    } else {
        match taken_over_from {
            Some(prior) => println!(
                "lease took over stale record role=engine owner={owner} generation={generation} taken_over_from={prior}"
            ),
            None => println!(
                "lease acquired through takeover verb role=engine owner={owner} generation={generation}"
            ),
        }
    }
}

fn print_native_lease_status(status: LeaseStatus, json: bool) -> i32 {
    match status {
        LeaseStatus::Vacant => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({ "present": false, "state": "vacant" })
                );
            } else {
                println!("lease: none (free — no owner holds .work/orchestrator.lock)");
            }
        }
        LeaseStatus::Live { record, liveness } | LeaseStatus::Stale { record, liveness } => {
            let state = if liveness.live { "live" } else { "stale" };
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "present": true,
                        "state": state,
                        "live": liveness.live,
                        "role": record.role,
                        "owner": record.owner_id,
                        "heartbeat_age_secs": liveness.heartbeat_age_secs,
                        "basis": liveness.basis,
                        "lease": record,
                    })
                );
            } else {
                println!(
                    "lease: {state} role={} owner={} age={}s generation={}",
                    record.role, record.owner_id, liveness.heartbeat_age_secs, record.generation
                );
            }
        }
        LeaseStatus::LegacyLock { detail } => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({ "present": true, "state": "legacy", "detail": detail })
                );
            } else {
                println!("lease: legacy lock · {detail}");
            }
        }
        LeaseStatus::Corrupt { detail } => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({ "present": true, "state": "corrupt", "detail": detail })
                );
            } else {
                println!("lease: corrupt record · {detail}");
            }
        }
    }
    lease_exit::OK
}

/// Make a path absolute against the current working directory without resolving symlinks
/// (so it stays a plain, tool-friendly path rather than a Windows `\\?\` extended path).
fn abs_path(p: &str) -> std::path::PathBuf {
    let path = Path::new(p);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .map(|c| c.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

/// `run --once --work <sandbox>` — drive one cohort/phases over a SANDBOX `.work`
/// (task T-109). `--work` is REQUIRED and has NO default, so `run` can never silently resolve the
/// repository's live `.work`; exactly one mode is required. Offline by DEFAULT: each round drives the
/// deterministic `__fake-agent` stand-in. Opt into real leaf model calls with `--live` (task
/// T-244): each leaf then spawns a real `claude -p`/`codex exec` child with its permission posture
/// stated explicitly on its own argv; the transactional invariants (K-006) are unchanged.
fn cmd_run(args: &[String]) {
    let once = args.iter().any(|a| a == "--once");
    if !once || args.iter().any(|a| a == "--drain") {
        eprintln!(
            "usage: run --once --work <sandbox> [--root <dir>] [--tools <dir>] [--base <ref>]\n\
             \x20          [--batch <id>] [--cohort-size <n>] [--ttl <sec>] [--inject-escalate <T-ID>]\n\
             \x20          [--review] [--inject-findings <T-ID>] [--review-loop-max <n>]\n\
             \x20          [--converge-after <n>] [--join] [--integration-loop-max <n>]\n\
             \x20          [--inject-merge-conflict <T-ID>] [--inject-f-findings]\n\
             \x20          [--integration-converge-after <n>] [--live] [--leaf-deadline <sec>]\n\
             \x20          [--codex-coder <off|fast|fast+std>] [--codex-network] [--json]\n\
             (--once runs one sandbox cohort; queue draining belongs to `processor --once --live`; --work is REQUIRED and has no default.\n\
             \x20 --live opts into real claude/codex leaf calls — off by default the round stays hermetic.\n\
             \x20 --leaf-deadline overrides the per-leaf wall-clock budget; the default is 60s offline, minutes under --live)"
        );
        exit(run::exit::USAGE);
    }
    let work = match opt(args, "--work") {
        Some(w) if !w.is_empty() => abs_path(&w),
        _ => {
            eprintln!("run: --work <sandbox-dir> is required (run has no default work dir)");
            exit(run::exit::USAGE);
        }
    };
    let root = match opt(args, "--root") {
        Some(r) => abs_path(&r),
        None => work
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| work.clone()),
    };
    // An explicit `--tools <dir>` (harness/tests/fixtures) is used as-is — the current contract for
    // fixtures is unchanged. The DEFAULT follows the SAME checkout-vs-mirror identity rule
    // `cmd_lease` already uses for its `--script` (`toolscript::resolve_tool_script`,
    // `docs/queue_contract.md` §9, K-052): resolve `state-tx.ps1` (present in both a proven
    // checkout's `tools/` and the cc-sync mirror) and take its containing directory as `tools`.
    // This must NEVER silently fall back to a bare `root.join("tools")`: a non-checkout `root` may
    // itself carry a foreign/stale `tools/` directory, and trusting it unconditionally would let
    // `run` execute an unproven script tree from a caller-controlled `--root` — the exact
    // trust-boundary bug this resolver exists to close.
    let tools = match opt(args, "--tools") {
        Some(t) => abs_path(&t),
        None => match toolscript::resolve_tool_script(&root, "state-tx.ps1") {
            Some(p) => p
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| root.join("tools")),
            None => {
                eprintln!(
                    "run: tools directory not found (no Orchestra checkout identity markers under {} \
                     and no cc-sync mirror at ~/.claude/scripts; pass --tools <dir> or --root <project root>)",
                    root.display()
                );
                exit(run::exit::USAGE);
            }
        },
    };
    let batch_id = opt(args, "--batch")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(run::default_batch_id);
    let base = opt(args, "--base").unwrap_or_else(|| "sandbox-base".to_string());
    let cohort_size = opt(args, "--cohort-size")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(3)
        .max(1);
    let ttl_secs = opt(args, "--ttl")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(900);
    let inject_escalate = opt(args, "--inject-escalate").filter(|s| !s.is_empty());
    let review = args.iter().any(|a| a == "--review");
    let inject_findings = opt(args, "--inject-findings").filter(|s| !s.is_empty());
    let review_loop_max = opt(args, "--review-loop-max")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(8)
        .max(1);
    let converge_after_cycles = opt(args, "--converge-after").and_then(|s| s.parse::<u32>().ok());
    // The join barrier (phases 4–6) is opt-in and implies --review (it consumes the ready tasks the
    // review round produces).
    let join = args.iter().any(|a| a == "--join");
    let review = review || join;
    let integration_loop_max = opt(args, "--integration-loop-max")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(8)
        .max(1);
    let inject_merge_conflict = opt(args, "--inject-merge-conflict").filter(|s| !s.is_empty());
    let inject_f_findings = args.iter().any(|a| a == "--inject-f-findings");
    let integration_converge_after =
        opt(args, "--integration-converge-after").and_then(|s| s.parse::<u32>().ok());
    // Opt-in live mode (task T-244): real `claude`/`codex` leaf calls instead of the offline
    // `__fake-agent` stand-in. Off by default, so the run stays hermetic and token-free.
    let live = args.iter().any(|a| a == "--live");
    // The Codex maker-routing flag fed to the coder resolver (default `off` = Claude-only). Only
    // meaningful under `--live`; an unrecognized value falls back to `off`.
    let codex_coder = opt(args, "--codex-coder")
        .as_deref()
        .and_then(CodexCoder::parse)
        .unwrap_or(CodexCoder::Off);
    let codex_network = args.iter().any(|a| a == "--codex-network");
    // Wall-clock budget for one supervised leaf. Separate fake/live defaults (60s vs minutes) so
    // the offline baseline is untouched while a real `--live` leaf gets a budget adequate for a
    // headless call; `--leaf-deadline <sec>` overrides either (clamped to ≥1s) for retuning without
    // a rebuild — a shared 60s cap would time-out→tree-kill every live leaf into escalation (R-01).
    let leaf_deadline_override = opt(args, "--leaf-deadline").and_then(|s| s.parse::<u64>().ok());
    let json = args.iter().any(|a| a == "--json");
    let cfg = RunConfig {
        work,
        root,
        tools,
        self_exe: self_exe(),
        batch_id,
        base,
        cohort_size,
        reviewer_tiering: true,
        ttl_secs,
        inject_escalate,
        review,
        inject_findings,
        review_loop_max,
        converge_after_cycles,
        join,
        integration_loop_max,
        inject_merge_conflict,
        inject_f_findings,
        integration_converge_after,
        live,
        codex_coder,
        codex_network,
        leaf_deadline: run::resolve_leaf_deadline(leaf_deadline_override, live),
    };

    match run::run_once(&cfg) {
        Ok(report) => {
            if json {
                println!("{}", report.to_json());
            } else {
                print!("{}", report.to_human());
            }
        }
        Err(e) => {
            eprintln!("run: {}", e.message);
            exit(e.code);
        }
    }
}

/// Hidden deterministic stand-in child for hermetic tests / selfcheck.
///   --mode success   emit a valid stream-json transcript, exit (0 unless --exit given)
///   --mode hang      sleep 30s (so a short deadline fires) — the tree-kill target
///   --mode leaf      emit a stream-json transcript whose `result` carries a parseable leaf
///                    report (contract markers) — used by `run`'s execution round. `--task <id>`
///                    names the task; `--verdict готово|эскалация` selects the terminal `ИТОГ:` line.
///   --mode review    the reviewer stand-in for `run`'s review round: write the task's `review.md`
///                    (the phase-2.6 gate input) under `--work`, then emit a reviewer transcript.
///                    `--task <id>` names the task; `--outcome clean|findings` selects a fresh
///                    `SUMMARY-R` (clean) vs an open `R-` (with-findings); `--summary-ts <iso>` is
///                    the fresh clean-pass summary timestamp the engine hands it.
///   --mode merge     the merger stand-in for `run`'s join barrier: write `merge_report.md` under
///                    `--work` (one `- [T-ID] merged=<SHA>` line per `--tasks T-101,T-102`, or
///                    `quarantined=<reason>` for each id in `--quarantine`), then emit a merger
///                    transcript. Deterministic, offline; no real VCS.
///   --mode integration-review  the full_reviewer stand-in for `run`'s integration review cycle:
///                    write `review_integration.md` under `--work` (a fresh `SUMMARY-F` for
///                    `--outcome clean`, an open `F-` for `findings`; `--summary-ts <iso>` is the
///                    fresh clean-pass timestamp), then emit a reviewer transcript.
///   --exit N         override the exit code
fn cmd_fake_agent(args: &[String]) {
    let mode = opt(args, "--mode").unwrap_or_else(|| "success".to_string());
    let exit_code: i32 = opt(args, "--exit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    match mode.as_str() {
        "success" => {
            println!(r#"{{"type":"system","subtype":"init","model":"fake"}}"#);
            println!(r#"{{"type":"assistant","message":{{"type":"message","role":"assistant"}}}}"#);
            println!(
                r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":3,"result":"fake agent done"}}"#
            );
            exit(exit_code);
        }
        "hang" => {
            std::thread::sleep(Duration::from_secs(30));
            exit(exit_code);
        }
        "leaf" => {
            // A deterministic leaf-agent stand-in for `run`'s round: emit a stream-json
            // transcript whose final `result` carries a parseable leaf report (the contract
            // markers `Изменённые файлы:` + the terminal `ИТОГ:` line). Offline, token-free.
            let task = opt(args, "--task").unwrap_or_else(|| "T-000".to_string());
            let verdict = opt(args, "--verdict").unwrap_or_else(|| "готово".to_string());
            let itog: &str = if verdict == "эскалация" {
                "ИТОГ: эскалация \u{00B7} режим=1 \u{00B7} причина=sandbox-fault"
            } else {
                "ИТОГ: готово \u{00B7} режим=1"
            };
            let report = format!(
                "Реализовал {task} в песочнице.\nИзменённые файлы: engine/src/{task}.rs\n{itog}"
            );
            println!(r#"{{"type":"system","subtype":"init","model":"fake"}}"#);
            println!(r#"{{"type":"assistant","message":{{"type":"message","role":"assistant"}}}}"#);
            // Build the result line via serde_json so the report (newlines, Cyrillic, the
            // middle-dot separator) is escaped correctly and round-trips through parse_transcript.
            let result_line = serde_json::json!({
                "type": "result",
                "subtype": "success",
                "is_error": false,
                "num_turns": 3,
                "result": report,
            });
            println!("{result_line}");
            exit(exit_code);
        }
        "review" => {
            // The reviewer stand-in for `run`'s review round. It writes the task's `review.md`
            // (the phase-2.6 gate input the engine reads back) and returns a machine-readable
            // reviewer report. Offline, token-free.
            let task = opt(args, "--task").unwrap_or_else(|| "T-000".to_string());
            let work = opt(args, "--work").unwrap_or_default();
            let outcome = opt(args, "--outcome").unwrap_or_else(|| "clean".to_string());
            let summary_ts =
                opt(args, "--summary-ts").unwrap_or_else(|| "2026-01-01T00:00:00Z".to_string());
            let findings = outcome == "findings";
            // A with-findings pass leaves ONE open `R-`; a clean pass writes a fresh `SUMMARY-R`
            // (newer than the engine's freshness mark) plus a resolved `R-` to exercise that the
            // gate ignores non-`новая` findings.
            let review_md = if findings {
                format!(
                    "# Review {task}\n\
                     ### [R-01] Missing error handling in the sandbox change — статус: новая\n\
                     - Файл: engine/src/{task}.rs\n"
                )
            } else {
                format!(
                    "# Review {task}\n\
                     ### [R-01] Minor naming nit (addressed) — статус: исправлено\n\
                     ### [SUMMARY-R-{summary_ts}] Итог ревью задачи — статус: готово к слиянию\n\
                     - Открытых проблем: 0\n"
                )
            };
            if !work.is_empty() {
                let dir = Path::new(&work).join("tasks").join(&task);
                let _ = fs::create_dir_all(&dir);
                let _ = fs::write(dir.join("review.md"), &review_md);
            }
            let itog: &str = if findings {
                "ИТОГ: есть находки \u{00B7} режим=ревью \u{00B7} открытых=1"
            } else {
                "ИТОГ: готово к слиянию \u{00B7} режим=ревью \u{00B7} открытых=0"
            };
            let report = format!("Ревью {task} в песочнице.\n{itog}");
            println!(r#"{{"type":"system","subtype":"init","model":"fake"}}"#);
            println!(r#"{{"type":"assistant","message":{{"type":"message","role":"assistant"}}}}"#);
            let result_line = serde_json::json!({
                "type": "result",
                "subtype": "success",
                "is_error": false,
                "num_turns": 3,
                "result": report,
            });
            println!("{result_line}");
            exit(exit_code);
        }
        "merge" => {
            // The merger stand-in for `run`'s join barrier. It writes `merge_report.md` (the Phase
            // 4.3 decision input the engine reads back) in the `agents/merger.md` format: one line
            // per task, `merged=<SHA>` by default or `quarantined=<reason>` for a `--quarantine` id.
            // Offline, token-free; no real VCS.
            let work = opt(args, "--work").unwrap_or_default();
            let batch = opt(args, "--batch").unwrap_or_else(|| "B-sandbox".to_string());
            let tasks: Vec<String> = opt(args, "--tasks")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let quarantine: Vec<String> = opt(args, "--quarantine")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let mut report_md = format!(
                "# Merge Report — Batch {batch}\nИнтеграционная ветка: integration/{batch}\nБаза: sandbox-base\n\n## Результаты\n"
            );
            let mut any_quarantine = false;
            for t in &tasks {
                if quarantine.iter().any(|q| q == t) {
                    any_quarantine = true;
                    report_md.push_str(&format!(
                        "- [{t}] quarantined=конфликт слияния в песочнице\n"
                    ));
                } else {
                    report_md.push_str(&format!("- [{t}] merged=sandbox-{t}\n"));
                }
            }
            report_md.push_str("\nИтоговая сборка интеграционной ветки: ok\n");
            if !work.is_empty() {
                let _ = fs::write(Path::new(&work).join("merge_report.md"), &report_md);
            }
            let merged_n = tasks
                .len()
                .saturating_sub(quarantine.iter().filter(|q| tasks.contains(q)).count());
            let itog = if any_quarantine {
                format!(
                    "ИТОГ: есть карантин \u{00B7} слито={merged_n} \u{00B7} карантин={} \u{00B7} сборка=ok",
                    quarantine.iter().filter(|q| tasks.contains(q)).count()
                )
            } else {
                format!(
                    "ИТОГ: слито всё \u{00B7} слито={merged_n} \u{00B7} карантин=0 \u{00B7} сборка=ok"
                )
            };
            let report = format!("Слил ветки батча {batch} в песочнице.\n{itog}");
            println!(r#"{{"type":"system","subtype":"init","model":"fake"}}"#);
            println!(r#"{{"type":"assistant","message":{{"type":"message","role":"assistant"}}}}"#);
            let result_line = serde_json::json!({
                "type": "result",
                "subtype": "success",
                "is_error": false,
                "num_turns": 3,
                "result": report,
            });
            println!("{result_line}");
            exit(exit_code);
        }
        "integration-review" => {
            // The full_reviewer stand-in for `run`'s integration review cycle. It writes
            // `review_integration.md` (the phase-5.2 gate input the engine reads back): a fresh
            // `SUMMARY-F` for a clean pass, an open `F-` for a with-findings pass. Offline.
            let work = opt(args, "--work").unwrap_or_default();
            let outcome = opt(args, "--outcome").unwrap_or_else(|| "clean".to_string());
            let summary_ts =
                opt(args, "--summary-ts").unwrap_or_else(|| "2026-01-01T00:00:00Z".to_string());
            let findings = outcome == "findings";
            let review_md = if findings {
                "# Integration review\n\
                 ### [F-01] Build break after integrating the batch — статус: новая\n\
                 - Область: интеграционная ветка\n"
                    .to_string()
            } else {
                format!(
                    "# Integration review\n\
                     ### [F-01] Minor integration nit (addressed) — статус: исправлено\n\
                     ### [SUMMARY-F-{summary_ts}] Итог интеграционного ревью — статус: готово к слиянию\n\
                     - Открытых F-: 0\n"
                )
            };
            if !work.is_empty() {
                let _ = fs::write(Path::new(&work).join("review_integration.md"), &review_md);
            }
            let itog = if findings {
                "ИТОГ: есть находки \u{00B7} режим=ревью \u{00B7} открытых=1"
            } else {
                "ИТОГ: готово к слиянию \u{00B7} режим=ревью \u{00B7} открытых=0"
            };
            let report = format!("Интеграционное ревью в песочнице.\n{itog}");
            println!(r#"{{"type":"system","subtype":"init","model":"fake"}}"#);
            println!(r#"{{"type":"assistant","message":{{"type":"message","role":"assistant"}}}}"#);
            let result_line = serde_json::json!({
                "type": "result",
                "subtype": "success",
                "is_error": false,
                "num_turns": 3,
                "result": report,
            });
            println!("{result_line}");
            exit(exit_code);
        }
        other => {
            eprintln!("__fake-agent: unknown --mode {other}");
            exit(2);
        }
    }
}

fn opt(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1).cloned())
}

fn join_argv(argv: &[String]) -> String {
    argv.iter()
        .map(|a| {
            if a.chars().any(|c| c.is_whitespace()) {
                format!("\"{a}\"")
            } else {
                a.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod live_prompt_tests {
    use super::{LivePromptError, parse_live_prompt};

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    /// T-272: `claude --live` / `codex --live` with no prompt at all must refuse (not spawn
    /// with `--live` itself as the prompt).
    #[test]
    fn missing_prompt_is_rejected() {
        let err = parse_live_prompt(&s(&["--live"])).unwrap_err();
        assert!(matches!(err, LivePromptError::Bad(_)));
    }

    /// T-272: a flag placed after the prompt must never silently steal the prompt slot —
    /// the whole call is rejected instead.
    #[test]
    fn flag_after_prompt_is_rejected() {
        let err = parse_live_prompt(&s(&["--live", "do the thing", "--live"])).unwrap_err();
        assert!(matches!(err, LivePromptError::Bad(_)));

        let err = parse_live_prompt(&s(&["--live", "do the thing", "--bogus"])).unwrap_err();
        assert!(matches!(err, LivePromptError::Bad(_)));
    }

    /// The plain, valid call must keep working exactly as before.
    #[test]
    fn normal_prompt_without_trailing_flags_is_accepted() {
        let prompt = parse_live_prompt(&s(&["--live", "do the thing"])).expect("valid call");
        assert_eq!(prompt, "do the thing");
    }

    /// `--live` missing entirely is its own distinct, more specific error so callers can
    /// print the "needs --live" refusal rather than a generic usage error.
    #[test]
    fn missing_live_flag_is_reported_distinctly() {
        let err = parse_live_prompt(&s(&["do the thing"])).unwrap_err();
        assert!(matches!(err, LivePromptError::MissingLive));
    }

    /// `--prompt <value>` is accepted as an explicit alternative to a bare positional.
    #[test]
    fn prompt_flag_form_is_accepted() {
        let prompt =
            parse_live_prompt(&s(&["--live", "--prompt", "do the thing"])).expect("valid call");
        assert_eq!(prompt, "do the thing");
    }

    /// A stray unrecognized flag before the prompt must also be rejected, not ignored.
    #[test]
    fn unrecognized_flag_before_prompt_is_rejected() {
        let err = parse_live_prompt(&s(&["--live", "--bogus", "do the thing"])).unwrap_err();
        assert!(matches!(err, LivePromptError::Bad(_)));
    }
}

#[cfg(test)]
mod parse_iso_utc_tests {
    use super::parse_iso_utc;

    /// The lenient `cohort_state.md` timestamp parser must keep its distinctive contract after
    /// moving the calendar arithmetic into `orchestrail_engine::time::days_from_civil`: it accepts a
    /// `T` OR a space separator, needs no trailing `Z`, and computes the same epoch either way.
    #[test]
    fn accepts_both_t_and_space_separators() {
        // 2021-01-01T00:00:00Z == 1_609_459_200 epoch seconds.
        assert_eq!(parse_iso_utc("2021-01-01T00:00:00"), Some(1_609_459_200));
        assert_eq!(parse_iso_utc("2021-01-01 00:00:00"), Some(1_609_459_200));
        assert_eq!(parse_iso_utc("1970-01-01T00:00:00"), Some(0));
    }

    /// A trailing zone suffix (`Z`, `+00:00`, …) past the seconds is ignored, and within-day
    /// components land on the right second.
    #[test]
    fn ignores_trailing_zone_suffix_and_reads_time_of_day() {
        assert_eq!(parse_iso_utc("2021-01-01T00:00:00Z"), Some(1_609_459_200));
        assert_eq!(
            parse_iso_utc("2021-01-01T01:01:01+00:00"),
            Some(1_609_462_861)
        );
        // A leap-day instant confirms the shared calendar core: 2020-02-29T12:00:00 == 1_582_977_600.
        assert_eq!(parse_iso_utc("2020-02-29 12:00:00"), Some(1_582_977_600));
    }

    /// Malformed timestamps and out-of-range calendar fields are rejected (None), unchanged.
    #[test]
    fn rejects_malformed_and_out_of_range() {
        assert_eq!(parse_iso_utc(""), None);
        assert_eq!(parse_iso_utc("2021/01/01T00:00:00"), None); // wrong date separators
        assert_eq!(parse_iso_utc("2021-01-01X00:00:00"), None); // bad date/time separator
        assert_eq!(parse_iso_utc("2021-13-01T00:00:00"), None); // month out of range
        assert_eq!(parse_iso_utc("2021-01-32T00:00:00"), None); // day out of range
        assert_eq!(parse_iso_utc("2021-01-01T0a:00:00"), None); // non-digit time field
    }
}

#[cfg(test)]
mod native_processor_argument_tests {
    use super::{positive_u64_option, positive_usize_option, validate_processor_args};

    fn args(tail: &[&str]) -> Vec<String> {
        std::iter::once("orchestrail-engine")
            .chain(std::iter::once("processor"))
            .chain(tail.iter().copied())
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn processor_arguments_reject_unknown_duplicate_and_missing_values() {
        for invalid in [
            args(&["--once", "--live", "--work", ".work", "--typo"]),
            args(&[
                "--once",
                "--live",
                "--work",
                ".work",
                "--max-turns",
                "4",
                "--max-turns",
                "5",
            ]),
            args(&["--once", "--live", "--work", "--json"]),
            args(&["--once", "--work", ".work"]),
        ] {
            assert!(validate_processor_args(&invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn processor_numeric_limits_never_fall_back_after_invalid_input() {
        for value in ["0", "not-a-number", "-1"] {
            let input = args(&[
                "--once",
                "--live",
                "--work",
                ".work",
                "--max-turns",
                value,
                "--max-effects",
                value,
                "--ttl",
                value,
            ]);
            validate_processor_args(&input).unwrap();
            assert!(positive_usize_option(&input, "--max-turns", 256).is_err());
            assert!(positive_usize_option(&input, "--max-effects", 1_024).is_err());
            assert!(positive_u64_option(&input, "--ttl", 900).is_err());
        }
    }

    #[test]
    fn processor_arguments_accept_the_documented_vocabulary_and_defaults() {
        let input = args(&[
            "--once",
            "--live",
            "--continue",
            "--json",
            "--work",
            ".work",
            "--root",
            ".",
            "--base",
            "main",
            "--batch",
            "B-1",
            "--owner",
            "owner-1",
            "--ttl",
            "901",
            "--max-turns",
            "12",
            "--max-effects",
            "34",
        ]);
        validate_processor_args(&input).unwrap();
        assert_eq!(positive_usize_option(&input, "--max-turns", 256), Ok(12));
        assert_eq!(
            positive_usize_option(&input, "--max-effects", 1_024),
            Ok(34)
        );
        assert_eq!(positive_u64_option(&input, "--ttl", 900), Ok(901));

        let defaults = args(&["--once", "--live", "--work", ".work"]);
        assert_eq!(
            positive_usize_option(&defaults, "--max-turns", 256),
            Ok(256)
        );
        assert_eq!(positive_u64_option(&defaults, "--ttl", 900), Ok(900));
    }
}

#[cfg(test)]
mod release_sync_argument_tests {
    use super::{repeated_options, validate_release_args};

    fn args(tail: &[&str]) -> Vec<String> {
        std::iter::once("orchestrail-engine")
            .chain(std::iter::once("release-sync"))
            .chain(tail.iter().copied())
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn initial_release_requires_live_work_and_version_and_accepts_canonical_notes_input() {
        let valid = args(&[
            "--live",
            "--work",
            ".work",
            "--version",
            "1.2.3",
            "--notes-file",
            ".work/release_notifications/1.2.3.md",
            "--product",
            "cargo:a",
            "--product",
            "cargo:b",
        ]);
        validate_release_args(&valid).unwrap();
        assert_eq!(
            repeated_options(&valid, "--product"),
            ["cargo:a", "cargo:b"]
        );
        for invalid in [
            args(&["--work", ".work", "--version", "1", "--notes-file", "n"]),
            args(&["--live", "--version", "1", "--notes-file", "n"]),
            args(&["--live", "--work", ".work", "--notes-file", "n"]),
        ] {
            assert!(validate_release_args(&invalid).is_err(), "{invalid:?}");
        }
        validate_release_args(&args(&["--live", "--work", ".work", "--version", "1"])).unwrap();
    }

    #[test]
    fn resume_rejects_any_replacement_content() {
        let valid = args(&[
            "--live",
            "--work",
            ".work",
            "--version",
            "1.2.3",
            "--resume",
            "--tag",
            "release-1.2.3",
        ]);
        validate_release_args(&valid).unwrap();
        for option in ["--notes-file", "--subject", "--product", "--release-url"] {
            let mut invalid = valid.clone();
            invalid.push(option.into());
            invalid.push("replacement".into());
            assert!(validate_release_args(&invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn release_arguments_reject_unknown_duplicate_and_missing_values() {
        for invalid in [
            args(&[
                "--live",
                "--work",
                ".work",
                "--version",
                "1",
                "--notes-file",
                "n",
                "--typo",
            ]),
            args(&[
                "--live",
                "--work",
                ".work",
                "--work",
                "other",
                "--version",
                "1",
                "--notes-file",
                "n",
            ]),
            args(&[
                "--live",
                "--work",
                ".work",
                "--version",
                "1",
                "--notes-file",
                "--json",
            ]),
        ] {
            assert!(validate_release_args(&invalid).is_err(), "{invalid:?}");
        }
    }
}

#[cfg(test)]
mod native_batch_id_tests {
    use super::native_default_batch_id;

    #[test]
    fn native_batch_id_distinguishes_same_second_process_restarts() {
        assert_ne!(
            native_default_batch_id(1_752_000_000, 1_001),
            native_default_batch_id(1_752_000_000, 1_002)
        );
    }

    #[test]
    fn native_batch_id_is_stable_for_one_process_and_instant() {
        assert_eq!(
            native_default_batch_id(1_752_000_000, 1_001),
            "B-1752000000-p1001"
        );
    }
}

#[cfg(test)]
mod processor_config_tests {
    use orchestrail_engine::config::EngineConfig;

    use super::ensure_supported_processor_config;

    #[test]
    fn linear_history_opt_in_is_rejected_before_native_publication_exists() {
        let config = EngineConfig {
            publish_linear_history: true,
            ..EngineConfig::default()
        };
        assert!(
            ensure_supported_processor_config(&config)
                .unwrap_err()
                .contains("PUBLISH_LINEAR_HISTORY")
        );
    }
}

#[cfg(test)]
mod plan_config_tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::PlanConfig;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn dry_run_uses_the_same_strict_configuration_decoder_as_processor() {
        let work = std::env::temp_dir().join(format!(
            "orchestrail-plan-config-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&work).unwrap();
        fs::write(
            work.join("config.md"),
            "MAX_PARALLEL: 4\nCOHORT_SIZE: 11\nCOHORT_MAX_AGE: 45\nCOHORT_BUDGET_SEC: 120\nREVIEWER_TIERING: false\n",
        )
        .unwrap();

        let config = PlanConfig::load(work.to_str().unwrap()).unwrap();
        assert_eq!(config.max_parallel, 4);
        assert_eq!(config.thresholds.size, 11);
        assert_eq!(config.thresholds.max_age_minutes, 45);
        assert_eq!(config.thresholds.budget_sec, Some(120));
        assert!(!config.reviewer_tiering);

        fs::write(work.join("config.md"), "MAX_PARALLEL: 0\n").unwrap();
        assert!(PlanConfig::load(work.to_str().unwrap()).is_err());
        fs::write(work.join("config.md"), "PUBLISH_LINEAR_HISTORY: true\n").unwrap();
        assert!(PlanConfig::load(work.to_str().unwrap()).is_err());
        let _ = fs::remove_dir_all(work);
    }
}

#[cfg(test)]
mod native_lease_input_tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use orchestrail_engine::lease::{LeaseOp, exit as lease_exit};
    use orchestrail_engine::ownership::{LeaseError, LeaseStatus, LeaseStore};
    use orchestrail_engine::state::now_epoch_secs;

    use super::{acquire_processor_lease, fresh_owner_id, native_lease_command};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temporary_work(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-native-lease-cli-{label}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn args(op: &str) -> Vec<String> {
        ["orchestrail-engine", "lease", op, "--owner", ""]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn heartbeat_and_release_refuse_an_empty_owner_instead_of_defaulting_one() {
        for op in [LeaseOp::Heartbeat, LeaseOp::Release] {
            assert_eq!(
                native_lease_command(
                    op,
                    Path::new(".missing-work"),
                    Path::new("."),
                    &args(op.as_str())
                ),
                lease_exit::USAGE
            );
        }
    }

    #[test]
    fn acquire_does_not_silently_adopt_a_stale_lease() {
        let work = temporary_work("stale");
        let store = LeaseStore::new(&work);
        let now = now_epoch_secs();
        store
            .acquire("prior-owner", Path::new("."), 1, now.saturating_sub(2))
            .expect("seed stale lease");

        let args = [
            "orchestrail-engine",
            "lease",
            "acquire",
            "--owner",
            "new-owner",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        assert_eq!(
            native_lease_command(LeaseOp::Acquire, &work, Path::new("."), &args),
            lease_exit::FAILED,
            "acquire must require an explicit, separately audited takeover decision"
        );
        assert!(matches!(
            store.status(now),
            Ok(LeaseStatus::Stale { ref record, .. }) if record.owner_id == "prior-owner"
        ));

        let _ = std::fs::remove_dir_all(work);
    }

    #[test]
    fn takeover_explicitly_adopts_only_the_stale_record() {
        let work = temporary_work("takeover");
        let store = LeaseStore::new(&work);
        let now = now_epoch_secs();
        store
            .acquire("prior-owner", Path::new("."), 1, now.saturating_sub(2))
            .expect("seed stale lease");

        let args = [
            "orchestrail-engine",
            "lease",
            "takeover",
            "--owner",
            "new-owner",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        assert_eq!(
            native_lease_command(LeaseOp::Takeover, &work, Path::new("."), &args),
            lease_exit::OK
        );
        assert!(matches!(
            store.status(now),
            Ok(LeaseStatus::Live { ref record, .. })
                if record.owner_id == "new-owner"
                    && record.generation == 2
                    && record.taken_over_from.as_deref() == Some("prior-owner")
        ));

        let _ = std::fs::remove_dir_all(work);
    }

    #[test]
    fn continue_renews_only_the_addressed_live_processor_lease() {
        let work = temporary_work("continue-live");
        let store = LeaseStore::new(&work);
        let root = std::env::current_dir().expect("current repository root");
        let now = now_epoch_secs();
        let seeded = store
            .acquire("prior-owner", &root, 60, now)
            .expect("seed addressed live processor lease");

        let renewed = acquire_processor_lease(&store, true, "new-process-owner", &root, 60, now)
            .expect("continue should renew the addressed live lease");
        assert_eq!(renewed.owner_id, "prior-owner");
        assert_eq!(renewed.generation, seeded.generation + 1);
        assert!(renewed.taken_over_from.is_none());
        assert!(store.release("prior-owner", now).unwrap());
        let _ = std::fs::remove_dir_all(work);
    }

    #[test]
    fn continue_takes_over_only_an_addressed_stale_processor_lease() {
        let work = temporary_work("continue-stale");
        let store = LeaseStore::new(&work);
        let root = std::env::current_dir().expect("current repository root");
        let now = now_epoch_secs();
        store
            .acquire("stale-owner", &root, 60, now.saturating_sub(61))
            .expect("seed addressed stale processor lease");

        let adopted = acquire_processor_lease(&store, true, "resumed-owner", &root, 60, now)
            .expect("continue should adopt only the addressed stale lease");
        assert_eq!(adopted.owner_id, "resumed-owner");
        assert_eq!(adopted.generation, 2);
        assert_eq!(adopted.taken_over_from.as_deref(), Some("stale-owner"));
        assert!(matches!(
            acquire_processor_lease(&store, true, "resumed-owner", &root, 60, now),
            Ok(record) if record.owner_id == "resumed-owner"
        ));
        assert!(store.release("resumed-owner", now).unwrap());
        let _ = std::fs::remove_dir_all(work);
    }

    #[test]
    fn continue_never_adopts_a_live_lease_addressed_to_another_root() {
        let work = temporary_work("continue-foreign-root");
        let store = LeaseStore::new(&work);
        let root = std::env::current_dir().expect("current repository root");
        let other_root = root.join("different-project");
        let now = now_epoch_secs();
        store
            .acquire("foreign-owner", &other_root, 60, now)
            .expect("seed foreign-root lease");

        assert!(matches!(
            acquire_processor_lease(&store, true, "new-owner", &root, 60, now),
            Err(LeaseError::HeldLive { owner, .. }) if owner == "foreign-owner"
        ));
        assert!(matches!(
            store.status(now),
            Ok(LeaseStatus::Live { ref record, .. })
                if record.owner_id == "foreign-owner" && record.root == other_root.to_string_lossy()
        ));
        assert!(store.release("foreign-owner", now).unwrap());
        let _ = std::fs::remove_dir_all(work);
    }

    #[test]
    fn continue_recognises_an_equivalent_lexical_root_address() {
        let work = temporary_work("continue-root-normalization");
        let store = LeaseStore::new(&work);
        let root = std::env::current_dir().expect("current repository root");
        let same_root_spelling = root.join("nested").join("..").join(".");
        let now = now_epoch_secs();
        store
            .acquire("prior-owner", &root, 60, now)
            .expect("seed addressed live processor lease");

        let renewed = acquire_processor_lease(
            &store,
            true,
            "new-process-owner",
            &same_root_spelling,
            60,
            now,
        )
        .expect("equivalent lexical root spelling must resume the live lease");
        assert_eq!(renewed.owner_id, "prior-owner");
        assert!(store.release("prior-owner", now).unwrap());
        let _ = std::fs::remove_dir_all(work);
    }

    #[test]
    fn generated_owner_ids_are_fresh_authority_tokens() {
        let first = fresh_owner_id();
        let second = fresh_owner_id();
        assert_ne!(
            first, second,
            "two launch attempts must never share a PID-derived owner"
        );
        for owner in [first, second] {
            let token = owner
                .strip_prefix("orchestrail-engine-")
                .expect("engine owner prefix");
            uuid::Uuid::parse_str(token).expect("fresh owner suffix is a UUID");
        }
    }
}
