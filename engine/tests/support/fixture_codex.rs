//! Hermetic stand-in for the `codex exec … --json -` reviewer protocol.
//!
//! The fixture reads the native prompt from stdin, consumes the same immutable range sentinel as
//! the Claude fixture, participates in the shared two-reviewer rendezvous, and emits the smallest
//! JSONL agent-message shape accepted by `codex::parse_json_transcript`.

use std::fs;
use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|arg| arg == "sandbox") {
        validate_sandbox_probe(&args);
        return;
    }
    let Some(worktree) = argv_worktree(&args) else {
        fail("expected typed read-only Codex argv");
    };
    let Ok(current_dir) = std::env::current_dir() else {
        fail("could not read child working directory");
    };
    if !same_path(&current_dir, Path::new(worktree)) {
        fail("child current directory differs from Codex -C worktree");
    }
    let mut prompt = String::new();
    if std::io::stdin().read_to_string(&mut prompt).is_err() {
        fail("could not read prompt from stdin");
    }
    if !prompt.contains("independent read-only reviewer") {
        fail("reviewer prompt marker is missing");
    }
    let Some(task_id) = token_after(&prompt, "TASK=").filter(|id| canonical_task_id(id)) else {
        fail("review task coordinate is missing");
    };
    let Some(work) = token_after(&prompt, "WORK=") else {
        fail("work coordinate is missing");
    };
    let work = Path::new(work);
    let sentinel = format!("fixture-review-sentinel:{task_id}");
    let evidence = fs::read_to_string(
        work.join("native-evidence")
            .join(format!("review-range-{task_id}-1.json")),
    )
    .unwrap_or_default();
    if !evidence.contains(&sentinel) {
        fail("immutable review range sentinel is missing");
    }
    if !review_batch_barrier_released(work, task_id) {
        fail("review batch barrier was not released");
    }
    let Some(summary_timestamp) = prompt
        .split_once("strictly later than ")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .and_then(|since| since.strip_suffix('Z'))
        .map(|since| format!("{since}.5Z"))
    else {
        fail("review freshness window is missing");
    };
    let review = work.join("tasks").join(task_id).join("review.md");
    if fs::create_dir_all(review.parent().expect("review parent")).is_err()
        || fs::write(
            review,
            format!(
                "Evidence sentinel: {sentinel}\n### [SUMMARY-R-{summary_timestamp}] fixture — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n"
            ),
        )
        .is_err()
    {
        fail("could not write review artifact");
    }
    println!(
        r#"{{"type":"item.completed","item":{{"type":"agent_message","text":"fixture Codex reviewer read immutable range"}}}}"#
    );
}

fn validate_sandbox_probe(args: &[String]) {
    if !args
        .windows(2)
        .any(|pair| pair == ["-c", "sandbox_mode=workspace-write"])
        || cfg!(target_os = "windows")
            && (!args
                .windows(2)
                .any(|pair| pair == ["-c", "sandbox_workspace_write.exclude_slash_tmp=true"])
                || !args.windows(2).any(|pair| {
                    pair == ["-c", "sandbox_workspace_write.exclude_tmpdir_env_var=true"]
                }))
    {
        fail("sandbox probe does not match the workspace-write runtime shape");
    }
    let Some(separator) = args.iter().position(|arg| arg == "--") else {
        fail("sandbox probe has no typed command separator");
    };
    if args.get(separator + 1).is_none_or(String::is_empty)
        || args.get(separator + 2).map(String::as_str) != Some("__sandbox-probe-noop")
        || args.len() != separator + 3
    {
        fail("sandbox probe does not target the engine no-op subcommand");
    }
}

/// Accept only the native `codex exec -C <worktree> --sandbox read-only ... --json -` shape.
/// This keeps the fixture an actual check of the ProcessKit/Codex boundary rather than a generic
/// stdin program that would also pass if routing lost its read-only contract.
fn argv_worktree(args: &[String]) -> Option<&str> {
    if args.get(1).is_none_or(|arg| arg != "exec")
        || !args
            .windows(2)
            .any(|pair| pair == ["--sandbox", "read-only"])
        || !args.iter().any(|arg| arg == "--json")
        || args.last().is_none_or(|arg| arg != "-")
    {
        return None;
    }
    args.windows(2)
        .find_map(|pair| (pair[0] == "-C").then_some(pair[1].as_str()))
}

fn token_after<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    text.split_whitespace()
        .find_map(|word| word.strip_prefix(prefix))
}

fn same_path(left: &Path, right: &Path) -> bool {
    fs::canonicalize(left)
        .ok()
        .zip(fs::canonicalize(right).ok())
        .is_some_and(|(left, right)| left == right)
}

fn canonical_task_id(value: &str) -> bool {
    value.strip_prefix("T-").is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// Same test-only rendezvous contract as `fixture_claude`: clean output from either provider is
/// impossible until both contained reviewer children have started.
fn review_batch_barrier_released(work: &Path, task_id: &str) -> bool {
    let evidence_dir = work.join("native-evidence");
    let barrier = evidence_dir.join("fixture-review-batch.barrier");
    let peers = match fs::read_to_string(&barrier) {
        Ok(contents) => contents
            .lines()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    };
    if peers.len() < 2
        || !peers.iter().any(|peer| peer == task_id)
        || !peers.iter().all(|peer| canonical_task_id(peer))
    {
        return false;
    }
    if fs::write(
        evidence_dir.join(format!("fixture-review-started-{task_id}")),
        "",
    )
    .is_err()
    {
        return false;
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if peers.iter().all(|peer| {
            evidence_dir
                .join(format!("fixture-review-started-{peer}"))
                .is_file()
        }) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

fn fail(message: &str) -> ! {
    eprintln!("fixture_codex: {message}");
    std::process::exit(2)
}
