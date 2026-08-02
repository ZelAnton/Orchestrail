//! Supervise one external tool invocation through ProcessKit.
//!
//! ProcessKit is the sole production process boundary: it drains standard streams without
//! deadlock, owns the child tree in an OS containment primitive, tears it down on timeout,
//! cancellation, or drop, and can harden parent-death cleanup.  This module deliberately keeps
//! the engine's existing synchronous verdict contract while hosting the async ProcessKit call on
//! a small current-thread Tokio runtime.
//!
//! The reason / exit-code mapping remains compatible with the historical supervisor:
//! `ok=0`, `timeout=3`, `cancelled=4`, `crash=5`, `error=6`.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use processkit::{
    CancellationToken, Command, Error, JobRunner, OutputBufferPolicy, ProcessResult, Stdin,
};

/// A command transcript should be enough to diagnose an agent/tool failure, but no child may
/// turn a long-lived engine into an unbounded memory sink.  Crossing either ceiling fails loud:
/// the caller receives an `error` verdict rather than silently acting on a truncated transcript.
const MAX_CAPTURED_OUTPUT_LINES: usize = 50_000;
/// Legacy-compatible default for callers that do not originate from `config.md` (for example,
/// the ownership adapter). Model leaves and configured verification override this through
/// [`SpawnSpec::output_max_bytes`].
pub const DEFAULT_CAPTURED_OUTPUT_BYTES: usize = 1024 * 1024;

/// The four supervised stop reasons (plus `ok`), matching the legacy supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Ok,
    Timeout,
    Cancelled,
    Crash,
    Error,
}

impl Reason {
    /// The exit code this reason maps to (identical to the legacy supervisor).
    pub fn exit_code(self) -> i32 {
        match self {
            Reason::Ok => 0,
            Reason::Timeout => 3,
            Reason::Cancelled => 4,
            Reason::Crash => 5,
            Reason::Error => 6,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Reason::Ok => "ok",
            Reason::Timeout => "timeout",
            Reason::Cancelled => "cancelled",
            Reason::Crash => "crash",
            Reason::Error => "error",
        }
    }

    /// Transient reasons a supervisor may safely retry (bounded): timeout / crash.
    pub fn is_transient(self) -> bool {
        matches!(self, Reason::Timeout | Reason::Crash)
    }
}

/// A structured, non-sensitive verdict for one supervised call.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub reason: Reason,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub duration_ms: u128,
    pub stdout: String,
    pub stderr: String,
    pub outcome_reason: String,
}

/// A cloneable, in-process cancellation source for a contained call. Unlike a pause marker this
/// does not write project state; it is used for facts such as lost lease ownership that must stop
/// the currently running child without impersonating an operator pause.
#[derive(Clone)]
pub struct CancellationProbe {
    check: Arc<dyn Fn() -> bool + Send + Sync + 'static>,
}

impl CancellationProbe {
    pub fn new(check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            check: Arc::new(check),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        (self.check)()
    }
}

impl fmt::Debug for CancellationProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CancellationProbe(..)")
    }
}

/// What to spawn and how to bound it.
pub struct SpawnSpec {
    pub program: String,
    pub args: Vec<String>,
    pub stdin: String,
    /// The child's working directory.  It is part of the spawn specification rather than a
    /// process-global `set_current_dir`, so concurrent contained calls cannot race each other.
    pub current_dir: Option<PathBuf>,
    pub deadline: Option<Duration>,
    /// Hard ProcessKit capture ceiling for stdout and stderr together. Exceeding it is a typed
    /// `error` verdict; partial output is never interpreted as a successful agent transcript.
    pub output_max_bytes: usize,
    /// Poll interval for the cooperative-cancel file watcher.
    pub poll: Duration,
    /// If set, appearance of this file requests a cooperative cancel (e.g. `.work/PAUSE`).
    pub cancel_file: Option<PathBuf>,
    /// Optional in-process cancellation fact, polled alongside [`Self::cancel_file`].
    pub cancel_probe: Option<CancellationProbe>,
}

impl SpawnSpec {
    pub fn new(program: impl Into<String>, args: Vec<String>) -> Self {
        SpawnSpec {
            program: program.into(),
            args,
            stdin: String::new(),
            current_dir: None,
            deadline: None,
            output_max_bytes: DEFAULT_CAPTURED_OUTPUT_BYTES,
            poll: Duration::from_millis(50),
            cancel_file: None,
            cancel_probe: None,
        }
    }

    pub fn stdin(mut self, s: impl Into<String>) -> Self {
        self.stdin = s.into();
        self
    }

    pub fn current_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(path.into());
        self
    }

    pub fn deadline(mut self, d: Option<Duration>) -> Self {
        self.deadline = d;
        self
    }

    /// Override the configured bounded output capture ceiling for this one contained child.
    pub fn output_max_bytes(mut self, bytes: usize) -> Self {
        self.output_max_bytes = bytes;
        self
    }

    pub fn cancel_file(mut self, p: Option<PathBuf>) -> Self {
        self.cancel_file = p;
        self
    }

    pub fn cancel_probe(mut self, probe: Option<CancellationProbe>) -> Self {
        self.cancel_probe = probe;
        self
    }
}

/// Run one child under ProcessKit containment and classify the outcome.
pub fn run(spec: &SpawnSpec) -> Verdict {
    let started = Instant::now();

    // A pause is a safety boundary, not merely a request to stop work already begun.  Checking it
    // before a runtime or child exists guarantees a pre-existing PAUSE file cannot launch a tool.
    if cancellation_requested(spec) {
        return cancelled_before_spawn(started);
    }

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return crash_verdict(started, format!("runtime initialization failed: {error}"));
        }
    };
    runtime.block_on(run_async(spec, started))
}

/// Start an independent ProcessKit container for every request before collecting any result.
/// The returned vector preserves the caller's order, not completion order.  This is the native
/// Phase-2 fan-out primitive: a slow first task cannot prevent later slots from being spawned,
/// yet every child retains its own timeout, cancellation token, cwd, and kill-on-parent-death
/// group.
pub fn run_batch(specs: Vec<SpawnSpec>) -> Vec<Verdict> {
    let started: Vec<_> = specs.iter().map(|_| Instant::now()).collect();
    let workers = specs.len().clamp(1, 8);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return started
                .into_iter()
                .map(|at| crash_verdict(at, format!("runtime initialization failed: {error}")))
                .collect();
        }
    };
    runtime.block_on(run_batch_async(specs, started))
}

async fn run_async(spec: &SpawnSpec, started: Instant) -> Verdict {
    let cancellation = CancellationToken::new();
    let watcher = (spec.cancel_file.is_some() || spec.cancel_probe.is_some()).then(|| {
        tokio::spawn(watch_cancel_file(
            spec.cancel_file.clone(),
            spec.cancel_probe.clone(),
            cancellation.clone(),
            nonzero_poll_interval(spec.poll),
        ))
    });

    // `Command::output_string` creates a private ProcessKit group.  The group owns the whole
    // descendant tree and kills it on deadline/cancel/drop; `kill_on_parent_death` covers abrupt
    // engine death as far as each platform permits (whole tree on Windows, direct child on Linux).
    let result = contained_command(spec, cancellation).output_string().await;

    if let Some(watcher) = watcher {
        watcher.abort();
        let _ = watcher.await;
    }

    match result {
        Ok(result) => verdict_from_result(result, spec),
        Err(error) => verdict_from_error(error, spec, started),
    }
}

/// Fan out a set of LABELED contained calls through [`run_batch`] and pair each verdict back to its
/// label, preserving the caller's order (not completion order). This is the review-roster fan-out
/// primitive (`resolvers::reviewer::ReviewerRoster`): the caller names each review dimension, builds
/// one [`SpawnSpec`] per dimension, and dispatches the WHOLE set through the same parallel
/// supervisor as every other batched call — no dimension review spins up its own container runner,
/// so each child still gets its private ProcessKit group/timeout/cancel/kill-on-parent-death from
/// [`run_batch`].
pub fn run_labeled_batch<L>(labeled: Vec<(L, SpawnSpec)>) -> Vec<(L, Verdict)> {
    let (labels, specs): (Vec<L>, Vec<SpawnSpec>) = labeled.into_iter().unzip();
    labels.into_iter().zip(run_batch(specs)).collect()
}

async fn run_batch_async(specs: Vec<SpawnSpec>, started: Vec<Instant>) -> Vec<Verdict> {
    debug_assert_eq!(specs.len(), started.len());
    let mut results = std::iter::repeat_with(|| None)
        .take(specs.len())
        .collect::<Vec<Option<Verdict>>>();
    let mut active = Vec::new();
    let mut commands = Vec::new();

    for (index, (spec, started)) in specs.into_iter().zip(started).enumerate() {
        if cancellation_requested(&spec) {
            results[index] = Some(cancelled_before_spawn(started));
            continue;
        }
        let cancellation = CancellationToken::new();
        let watcher = (spec.cancel_file.is_some() || spec.cancel_probe.is_some()).then(|| {
            tokio::spawn(watch_cancel_file(
                spec.cancel_file.clone(),
                spec.cancel_probe.clone(),
                cancellation.clone(),
                nonzero_poll_interval(spec.poll),
            ))
        });
        commands.push(contained_command(&spec, cancellation));
        active.push((index, spec, started, watcher));
    }

    // ProcessKit's native batch driver polls all output futures together and preserves their
    // input order.  Unlike a hand-written `for command.output_string().await`, it reaches the
    // requested fan-out cap while every command still receives its own private Job/container.
    let outputs = processkit::output_all(commands, active.len().max(1), &JobRunner::new()).await;
    for ((index, spec, started, watcher), result) in active.into_iter().zip(outputs) {
        if let Some(watcher) = watcher {
            watcher.abort();
            let _ = watcher.await;
        }
        results[index] = Some(match result {
            Ok(result) => verdict_from_result(result, &spec),
            Err(error) => verdict_from_error(error, &spec, started),
        });
    }

    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.unwrap_or_else(|| {
                crash_verdict(
                    Instant::now(),
                    format!("batch slot {index} completed without a ProcessKit verdict"),
                )
            })
        })
        .collect()
}

async fn watch_cancel_file(
    path: Option<PathBuf>,
    probe: Option<CancellationProbe>,
    cancellation: CancellationToken,
    poll: Duration,
) {
    loop {
        if path.as_ref().is_some_and(|path| path.exists())
            || probe.as_ref().is_some_and(CancellationProbe::is_cancelled)
        {
            cancellation.cancel();
            return;
        }
        tokio::time::sleep(poll).await;
    }
}

fn contained_command(spec: &SpawnSpec, cancellation: CancellationToken) -> Command {
    let mut command = Command::new(&spec.program).args(&spec.args);
    if let Some(path) = &spec.current_dir {
        command = command.current_dir(path);
    }
    command
        .stdin(Stdin::from_string(spec.stdin.clone()))
        .timeout_opt(spec.deadline)
        .timeout_grace(Duration::from_millis(200))
        .cancel_on(cancellation)
        .create_no_window()
        .kill_on_parent_death()
        .output_buffer(capture_policy(spec.output_max_bytes))
}

fn capture_policy(max_bytes: usize) -> OutputBufferPolicy {
    OutputBufferPolicy::fail_loud(MAX_CAPTURED_OUTPUT_LINES).with_max_bytes(max_bytes)
}

fn nonzero_poll_interval(poll: Duration) -> Duration {
    if poll.is_zero() {
        Duration::from_millis(1)
    } else {
        poll
    }
}

fn cancellation_requested(spec: &SpawnSpec) -> bool {
    spec.cancel_file.as_ref().is_some_and(|path| path.exists())
        || spec
            .cancel_probe
            .as_ref()
            .is_some_and(CancellationProbe::is_cancelled)
}

fn cancelled_before_spawn(started: Instant) -> Verdict {
    Verdict {
        reason: Reason::Cancelled,
        exit_code: None,
        timed_out: false,
        cancelled: true,
        duration_ms: started.elapsed().as_millis(),
        stdout: String::new(),
        stderr: String::new(),
        outcome_reason: "cancel requested before spawn".into(),
    }
}

fn crash_verdict(started: Instant, outcome_reason: String) -> Verdict {
    Verdict {
        reason: Reason::Crash,
        exit_code: None,
        timed_out: false,
        cancelled: false,
        duration_ms: started.elapsed().as_millis(),
        stdout: String::new(),
        stderr: String::new(),
        outcome_reason,
    }
}

fn verdict_from_result(result: ProcessResult<String>, spec: &SpawnSpec) -> Verdict {
    let stdout = result.stdout().clone();
    let stderr = result.stderr().to_string();
    let duration_ms = result.duration().as_millis();

    if result.timed_out() {
        let secs = spec.deadline.map(|timeout| timeout.as_secs()).unwrap_or(0);
        return Verdict {
            reason: Reason::Timeout,
            exit_code: None,
            timed_out: true,
            cancelled: false,
            duration_ms,
            stdout,
            stderr,
            outcome_reason: format!("deadline exceeded ({secs}s)"),
        };
    }

    match result.code() {
        Some(0) => Verdict {
            reason: Reason::Ok,
            exit_code: Some(0),
            timed_out: false,
            cancelled: false,
            duration_ms,
            stdout,
            stderr,
            outcome_reason: "exit code 0".into(),
        },
        Some(code) => Verdict {
            reason: Reason::Error,
            exit_code: Some(code),
            timed_out: false,
            cancelled: false,
            duration_ms,
            stdout,
            stderr,
            outcome_reason: format!("exit code {code}"),
        },
        None => Verdict {
            reason: Reason::Crash,
            exit_code: None,
            timed_out: false,
            cancelled: false,
            duration_ms,
            stdout,
            stderr,
            outcome_reason: match result.signal() {
                Some(signal) => format!("terminated by signal {signal}"),
                None => "terminated without an exit code".into(),
            },
        },
    }
}

fn verdict_from_error(error: Error, spec: &SpawnSpec, started: Instant) -> Verdict {
    let stdout = error.stdout().unwrap_or_default().to_string();
    let stderr = error.stderr().unwrap_or_default().to_string();
    let duration_ms = started.elapsed().as_millis();

    if error.is_cancelled() {
        return Verdict {
            reason: Reason::Cancelled,
            exit_code: None,
            timed_out: false,
            cancelled: true,
            duration_ms,
            stdout,
            stderr,
            outcome_reason: "cancel requested".into(),
        };
    }
    if error.is_timeout() {
        let secs = spec
            .deadline
            .map(|deadline| deadline.as_secs())
            .unwrap_or(0);
        return Verdict {
            reason: Reason::Timeout,
            exit_code: None,
            timed_out: true,
            cancelled: false,
            duration_ms,
            stdout,
            stderr,
            outcome_reason: format!("deadline exceeded ({secs}s)"),
        };
    }

    // ProcessKit 2.3 exposes a non-exhaustive structured error enum.  Match only the two launch
    // variants that mean no child was ever started; lease's host fallback depends on this exact
    // distinction, while every other present/future variant fails as an infrastructure crash.
    let spawn_failure = matches!(&error, Error::NotFound { .. } | Error::Spawn { .. });
    let output_overflow = matches!(&error, Error::OutputTooLarge { .. });
    let reason = if output_overflow {
        Reason::Error
    } else {
        Reason::Crash
    };
    let outcome_reason = if spawn_failure {
        format!("spawn failed: {error}")
    } else if output_overflow {
        format!("output capture limit exceeded: {error}")
    } else {
        format!("processkit failed: {error}")
    };

    Verdict {
        reason,
        exit_code: error.code(),
        timed_out: false,
        cancelled: false,
        duration_ms,
        stdout,
        stderr,
        outcome_reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_temp_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "orchestrail-supervise-{name}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn test_child(test_name: &str) -> SpawnSpec {
        let test_exe = std::env::current_exe().expect("resolve current test executable");
        SpawnSpec::new(
            test_exe.to_string_lossy(),
            vec![
                "--ignored".into(),
                "--exact".into(),
                test_name.into(),
                "--nocapture".into(),
            ],
        )
    }

    #[test]
    fn reason_exit_codes_match_legacy_supervisor() {
        assert_eq!(Reason::Ok.exit_code(), 0);
        assert_eq!(Reason::Timeout.exit_code(), 3);
        assert_eq!(Reason::Cancelled.exit_code(), 4);
        assert_eq!(Reason::Crash.exit_code(), 5);
        assert_eq!(Reason::Error.exit_code(), 6);
        assert!(Reason::Timeout.is_transient());
        assert!(Reason::Crash.is_transient());
        assert!(!Reason::Error.is_transient());
        assert!(!Reason::Ok.is_transient());
    }

    #[test]
    fn spawn_failure_is_a_crash_with_compatibility_prefix() {
        let spec = SpawnSpec::new("this-binary-does-not-exist-xyzzy", vec!["--nope".into()]);
        let verdict = run(&spec);
        assert_eq!(verdict.reason, Reason::Crash);
        assert!(verdict.outcome_reason.starts_with("spawn failed"));
    }

    #[test]
    fn preexisting_pause_prevents_any_spawn_attempt() {
        let pause = unique_temp_file("pause");
        std::fs::write(&pause, "pause").expect("create pause marker");
        let verdict = run(
            &SpawnSpec::new("this-binary-does-not-exist-xyzzy", Vec::new())
                .cancel_file(Some(pause.clone())),
        );
        let _ = std::fs::remove_file(&pause);

        assert_eq!(verdict.reason, Reason::Cancelled);
        assert!(verdict.cancelled);
        assert_eq!(verdict.outcome_reason, "cancel requested before spawn");
    }

    #[test]
    fn in_process_cancellation_probe_stops_an_active_child() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let trigger = Arc::clone(&cancelled);
        let trigger_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(80));
            trigger.store(true, Ordering::Release);
        });
        let verdict = run(&test_child("supervise::tests::long_running_test_child")
            .deadline(Some(Duration::from_secs(5)))
            .cancel_probe(Some(CancellationProbe::new(move || {
                cancelled.load(Ordering::Acquire)
            }))));
        trigger_thread.join().expect("join cancellation trigger");
        assert_eq!(
            verdict.reason,
            Reason::Cancelled,
            "{}",
            verdict.outcome_reason
        );
        assert!(verdict.cancelled);
    }

    #[test]
    fn captures_clean_child_output() {
        let verdict = run(&test_child("supervise::tests::clean_output_test_child"));
        assert_eq!(verdict.reason, Reason::Ok, "{}", verdict.outcome_reason);
        assert!(verdict.stdout.contains("processkit-supervise-output"));
    }

    /// The review-roster fan-out primitive: labels (review-dimension names) pair back to their own
    /// verdict in the caller's order, and the whole set runs through `run_batch` rather than any
    /// ad-hoc runner. Two clean children stand in for a two-dimension review cycle.
    #[test]
    fn labeled_batch_pairs_each_verdict_to_its_label_in_order() {
        let results = run_labeled_batch(vec![
            (
                "functionality".to_string(),
                test_child("supervise::tests::clean_output_test_child"),
            ),
            (
                "security".to_string(),
                test_child("supervise::tests::clean_output_test_child"),
            ),
        ]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "functionality");
        assert_eq!(results[1].0, "security");
        assert!(
            results
                .iter()
                .all(|(_, verdict)| verdict.reason == Reason::Ok),
            "{:?}",
            results
                .iter()
                .map(|(label, verdict)| (label, &verdict.outcome_reason))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn deadline_terminates_a_long_running_child() {
        let verdict = run(&test_child("supervise::tests::long_running_test_child")
            .deadline(Some(Duration::from_millis(100))));
        assert_eq!(
            verdict.reason,
            Reason::Timeout,
            "{}",
            verdict.outcome_reason
        );
        assert!(verdict.timed_out);
    }

    #[test]
    fn capture_policy_is_bounded_and_fail_loud() {
        let policy = capture_policy(123);
        assert_eq!(policy.max_lines, Some(MAX_CAPTURED_OUTPUT_LINES));
        assert_eq!(policy.max_bytes, Some(123));
        assert!(matches!(policy.overflow, processkit::OverflowMode::Error));
    }

    #[test]
    fn configured_byte_ceiling_is_passed_to_processkit_and_fails_loud() {
        let verdict =
            run(&test_child("supervise::tests::clean_output_test_child").output_max_bytes(1));
        assert_eq!(verdict.reason, Reason::Error, "{}", verdict.outcome_reason);
        assert!(
            verdict
                .outcome_reason
                .contains("output capture limit exceeded")
        );
    }

    #[test]
    fn zero_poll_interval_is_never_a_busy_loop() {
        assert_eq!(
            nonzero_poll_interval(Duration::ZERO),
            Duration::from_millis(1)
        );
    }

    #[cfg(windows)]
    #[test]
    fn batch_starts_real_contained_children_before_collecting_any_result() {
        let root = unique_temp_file("batch-barrier");
        let first = root.join("T-1");
        let second = root.join("T-2");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let first_for_parent = first.clone();
        let second_for_parent = second.clone();
        let results = std::thread::spawn(move || {
            run_batch(vec![
                test_child("supervise::tests::batch_barrier_child").current_dir(first),
                test_child("supervise::tests::batch_barrier_child").current_dir(second),
            ])
        });

        // A full Windows workspace run creates many real Git/JJ fixtures in parallel. Process
        // creation can legitimately be delayed beyond five seconds under that load even though
        // both batch slots were submitted together. A truly sequential implementation still
        // fails this proof: the first child waits for `release`, so the second cannot become
        // ready before this entire deadline expires.
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline
            && !(first_for_parent.join("ready").is_file()
                && second_for_parent.join("ready").is_file())
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        let both_ready =
            first_for_parent.join("ready").is_file() && second_for_parent.join("ready").is_file();
        // Do not leave a test child behind when this assertion exposes a scheduler regression.
        std::fs::write(root.join("release"), "release").unwrap();
        let results = results.join().unwrap();
        assert!(
            both_ready,
            "the second ProcessKit child did not start before the first result was collected"
        );
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.reason == Reason::Ok));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[ignore = "spawned explicitly by captures_clean_child_output"]
    fn clean_output_test_child() {
        println!("processkit-supervise-output");
    }

    #[test]
    #[ignore = "spawned explicitly by deadline_terminates_a_long_running_child"]
    fn long_running_test_child() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "spawned explicitly by batch_starts_real_contained_children_before_collecting_any_result"]
    fn batch_barrier_child() {
        let cwd = std::env::current_dir().unwrap();
        std::fs::write(cwd.join("ready"), "ready").unwrap();
        let release = cwd.parent().unwrap().join("release");
        while !release.is_file() {
            std::thread::sleep(Duration::from_millis(10));
        }
        println!("processkit-batch-child");
    }

    // A supervised child that spawns a longer-lived grandchild must, on deadline, have its
    // whole tree torn down.  The assertion uses a delayed side effect rather than PID existence:
    // containers whose PID 1 does not reap promptly can keep a correctly-killed zombie visible.
    #[cfg(unix)]
    #[test]
    fn timeout_reaps_grandchildren_without_orphans() {
        let marker = unique_temp_file("orphan-marker");
        let script = format!("( sleep 2 && : > '{}' ) & wait", marker.display());
        let verdict = run(&SpawnSpec::new("/bin/sh", vec!["-c".into(), script])
            .deadline(Some(Duration::from_millis(300))));
        assert_eq!(
            verdict.reason,
            Reason::Timeout,
            "{}",
            verdict.outcome_reason
        );

        std::thread::sleep(Duration::from_millis(2600));
        let orphan_ran = marker.exists();
        let _ = std::fs::remove_file(&marker);
        assert!(
            !orphan_ran,
            "grandchild kept running after containment teardown"
        );
    }
}
