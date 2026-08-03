//! Hermetic stand-in for the `claude -p … --output-format stream-json` protocol.
//!
//! It accepts arbitrary Claude CLI flags, validates the one prompt shape required by the fixture,
//! and emits the final stream-json result that production parsing consumes. It is test-only and
//! is launched by the real ProcessKit-backed `HeadlessExternalPort`.

use std::env;
use std::fs;
use std::time::{Duration, Instant};

/// Hang backstop for the peer rendezvous, not a concurrency budget — see
/// [`review_batch_barrier_released`]. Every real caller bounds this child with its own ProcessKit
/// deadline, which is always shorter than this.
const PEER_START_BACKSTOP: Duration = Duration::from_secs(60);

fn main() {
    let args = env::args().collect::<Vec<_>>();
    let prompt = args
        .windows(2)
        .find_map(|pair| (pair[0] == "-p").then_some(pair[1].as_str()))
        .unwrap_or_default();
    let work = args
        .windows(2)
        .find_map(|pair| (pair[0] == "--add-dir").then_some(pair[1].as_str()));
    let required = [
        "You are knowledge_curator.",
        "KB_TTL=5",
        "KB_CAP=7",
        "batch B-1",
    ];
    let knowledge_valid =
        required.iter().all(|fragment| prompt.contains(fragment)) && work.is_some();
    let reviewer_valid = prompt.contains("independent read-only reviewer") && work.is_some();
    if knowledge_valid {
        let knowledge = std::path::Path::new(work.expect("validated work path")).join("knowledge");
        let prompt_marker = knowledge.join("architecture/fixture-prompt.md");
        let conventions = knowledge.join("conventions/fixture-retention.md");
        let pitfalls = knowledge.join("pitfalls/fixture-retention.md");
        let index = knowledge.join("INDEX.md");
        let curated = knowledge.join(".curated/B-1.done");
        let expired = knowledge.join("pitfalls/expired-fixture.md");
        let _ = fs::create_dir_all(prompt_marker.parent().expect("fixture marker parent"));
        let _ = fs::create_dir_all(conventions.parent().expect("fixture conventions parent"));
        let _ = fs::create_dir_all(pitfalls.parent().expect("fixture pitfalls parent"));
        let _ = fs::create_dir_all(curated.parent().expect("fixture curated parent"));
        let _ = fs::write(prompt_marker, format!("{prompt}\n"));
        let _ = fs::write(conventions, "fixture retained convention\n");
        let _ = fs::write(pitfalls, "fixture retained pitfall\n");
        let _ = fs::write(index, "# Fixture knowledge index\n");
        let _ = fs::write(curated, "done\n");
        let _ = fs::remove_file(expired);
        println!(
            r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"ИТОГ: готово · режим=1"}}"#
        );
    } else if reviewer_valid {
        let work = std::path::Path::new(work.expect("validated work path"));
        let Some(task_id) = prompt
            .split_whitespace()
            .find_map(|word| word.strip_prefix("TASK="))
            .filter(|task_id| canonical_task_id(task_id))
        else {
            println!(
                r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"ИТОГ: эскалация · причина=review task coordinate missing"}}"#
            );
            return;
        };
        let sentinel = format!("fixture-review-sentinel:{task_id}");
        let evidence = fs::read_to_string(
            work.join("native-evidence")
                .join(format!("review-range-{task_id}-1.json")),
        )
        .unwrap_or_default();
        if evidence.contains(&sentinel) {
            if !review_batch_barrier_released(work, task_id) {
                println!(
                    r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"ИТОГ: эскалация · причина=review batch barrier not released"}}"#
                );
                return;
            }
            // An opt-in test marker lets one reviewer exceed its own ProcessKit deadline after
            // both peers have started. This proves batch collection cannot turn a timed-out
            // child into a clean result or contaminate the other reviewer's artifact.
            if work
                .join("native-evidence")
                .join(format!("fixture-review-delay-{task_id}"))
                .is_file()
            {
                std::thread::sleep(Duration::from_secs(2));
            }
            let Some(summary_timestamp) = prompt
                .split_once("strictly later than ")
                .and_then(|(_, rest)| rest.split_whitespace().next())
                .and_then(|since| since.strip_suffix('Z'))
                .map(|since| format!("{since}.5Z"))
            else {
                println!(
                    r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"ИТОГ: эскалация · причина=review freshness window missing"}}"#
                );
                return;
            };
            let review = work.join("tasks").join(task_id).join("review.md");
            let _ = fs::create_dir_all(review.parent().expect("fixture review parent"));
            let _ = fs::write(
                review,
                format!(
                    "Evidence sentinel: {sentinel}\n### [SUMMARY-R-{summary_timestamp}] fixture — статус: готово к слиянию\nИТОГ: готово к слиянию · открытых=0\n"
                ),
            );
            println!(
                r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"fixture reviewer read immutable range"}}"#
            );
        } else {
            println!(
                r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"ИТОГ: эскалация · причина=review range sentinel missing"}}"#
            );
        }
    } else {
        println!(
            r#"{{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"ИТОГ: эскалация · причина=fixture prompt mismatch"}}"#
        );
    }
}

fn canonical_task_id(value: &str) -> bool {
    value.strip_prefix("T-").is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// A test-only rendezvous. The production prompt never creates the marker, while the dual-review
/// fixture supplies canonical peer ids. A sequential launcher leaves the first child waiting and
/// cannot manufacture two clean review results.
///
/// The wait is deliberately far longer than any real start skew and is NOT the thing that proves
/// concurrency. The caller's own ProcessKit deadline is: a child still waiting here when that
/// deadline expires is killed and can never report a clean review, so a sequential launcher fails
/// the batch proofs deterministically. A short self-timeout cannot make that distinction — it
/// reports "not concurrent" for a launcher that submitted both slots together but whose second
/// `CreateProcess` merely landed late, which on Windows routinely costs seconds the first time a
/// freshly built fixture image is executed (the same cold-start latency
/// `supervise::tests::batch_starts_real_contained_children_before_collecting_any_result` budgets
/// fifteen seconds for). This backstop only keeps a fixture invoked without any deadline at all
/// from hanging forever.
fn review_batch_barrier_released(work: &std::path::Path, task_id: &str) -> bool {
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
    let deadline = Instant::now() + PEER_START_BACKSTOP;
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
