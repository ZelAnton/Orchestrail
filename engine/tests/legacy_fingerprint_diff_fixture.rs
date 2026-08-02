//! Opt-in differential final-state fingerprint proof against the legacy Orchestra harness (task
//! T-037): the main cutover criterion of `plans/DETERMINISTIC_ORCHESTRATOR_INTENT.md` §9.2 — the
//! native engine and the legacy prose processor, driven over an equivalent scenario, must converge
//! on ONE timestamp/event-id-independent final-state fingerprint. `.work/processor_coverage.md`
//! names exactly this as the last-open item: "run the legacy harness fingerprint against
//! equivalent Git and JJ round/merge/quarantine scenarios."
//!
//! For every scenario (an admission+integration ROUND, a clean MERGE/publish, and a merge
//! QUARANTINE) on BOTH a Git and a JJ repository (2×3 = six cases) this test:
//!   (a) runs the REAL native engine (`engine run --once`) on a hermetic, disposable repository
//!       through the same `Sandbox`/`engine_run` pattern the sibling fixtures use — never a new
//!       runner — so the engine performs its own typed VCS merges/publications;
//!   (b) computes the native fingerprint through the crate's own projector,
//!       `legacy_fingerprint::final_state_fingerprint`, fed by the typed
//!       `legacy_fingerprint::committed_tree_inventory` (typed `vcs-git`/`vcs-jj`, never CLI);
//!   (c) invokes the READ-ONLY legacy harness (`tools/harness.ps1`) from the legacy Orchestra
//!       checkout — located ONLY through the `ORCHESTRAIL_LEGACY_ORCHESTRA` environment variable,
//!       never a hardcoded absolute path to someone else's repository — for the equivalent
//!       scenario+backend, and reads its reference fingerprint by the same four-component
//!       (`tree`/`queue`/`archive`/`outbox`) + `combined` envelope protocol;
//!   (d) compares the `combined` field and, on divergence, fails with a per-component diagnostic
//!       diff so the drifting component (tree vs queue vs archive vs outbox) is obvious.
//!
//! ## Why every case is `#[ignore]`d
//! The reference oracle needs a read-only legacy Orchestra checkout that supplies BOTH the
//! transactional tools (`queue-tx.ps1`/`state-tx.ps1`/`outbox.ps1`, which `engine run --tools`
//! also drives) AND `tools/harness.ps1`. That checkout is a network/filesystem resource OUTSIDE
//! this self-contained Orchestrail workspace, so — exactly like `join_fixture`/`lease_fixture`/
//! `review_fixture`/`run_fixture` — every case carries the identical
//! `#[ignore = "requires read-only legacy Orchestra transactional tools; …"]` marker. A plain
//! `cargo test` (no `--ignored`) never picks them up and never needs the legacy checkout.
//!
//! ## Running the differential class
//! With a legacy Orchestra checkout available:
//! ```text
//! ORCHESTRAIL_LEGACY_ORCHESTRA=/path/to/legacy/orchestra \
//!   cargo test -p orchestrail-engine --test legacy_fingerprint_diff_fixture -- --ignored legacy_fingerprint
//! ```
//! When `ORCHESTRAIL_LEGACY_ORCHESTRA` is unset, points nowhere, or lacks `tools/harness.ps1` —
//! and when no `pwsh`/`powershell` host is present — each case SKIPS with a diagnostic note rather
//! than panicking or reporting a false failure (the `#[ignore]` gate already keeps a stray local/CI
//! run from touching them without an explicit `--ignored`).
//!
//! ## Harness protocol contract (assumed §9.2 interface)
//! `harness.ps1` already runs a cohort end-to-end over the real tools with fault injection and
//! prints the timestamp/event-id-independent fingerprint. This adapter invokes it as
//! `pwsh -File <checkout>/tools/harness.ps1 --scenario <round|merge|quarantine> --backend <git|jj>`
//! and reads a `tree=`/`queue=`/`archive=`/`outbox=`/`combined=` line-oriented envelope (the exact
//! component names the native projector emits) from its stdout. The precise flag/output surface of
//! the real `harness.ps1` must be confirmed against the legacy checkout; this contract is the point
//! to adapt, and it is intentionally the ONLY place the harness interface is encoded.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod common;
use common::{Command, Output};

use orchestrail_engine::legacy_fingerprint::{
    FinalStateFingerprint, committed_tree_inventory, final_state_fingerprint,
};

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

/// The legacy Orchestra checkout that supplies both the transactional tools and `harness.ps1`.
const LEGACY_CHECKOUT_ENV: &str = "ORCHESTRAIL_LEGACY_ORCHESTRA";

/// The base ref both engines publish onto; equivalent scenarios must anchor on the same name.
const BASE_REF: &str = "sandbox-base";

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// The two VCS backends the cutover criterion must hold on.
#[derive(Debug, Clone, Copy)]
enum Backend {
    Git,
    Jj,
}

impl Backend {
    /// The `--backend` token passed to `harness.ps1`.
    fn harness_arg(self) -> &'static str {
        match self {
            Backend::Git => "git",
            Backend::Jj => "jj",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Backend::Git => "Git",
            Backend::Jj => "JJ",
        }
    }
}

/// The three equivalent scenarios named by the coverage matrix.
#[derive(Debug, Clone, Copy)]
enum Scenario {
    /// Admission + one supervised leaf round (no review/join): the base ref does not move.
    Round,
    /// A clean review + join barrier: both tasks merge and the batch publishes onto the base ref.
    Merge,
    /// A merge quarantine: one branch is rolled back/re-queued, the rest still publishes.
    Quarantine,
}

impl Scenario {
    /// The `--scenario` token passed to `harness.ps1`.
    fn harness_arg(self) -> &'static str {
        match self {
            Scenario::Round => "round",
            Scenario::Merge => "merge",
            Scenario::Quarantine => "quarantine",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Scenario::Round => "round",
            Scenario::Merge => "merge",
            Scenario::Quarantine => "quarantine",
        }
    }

    /// Extra `engine run` flags that reproduce this scenario natively. Batch id is scenario-stable
    /// so an event-id-independent comparison is not perturbed by a random batch token.
    fn engine_flags(self, batch: &str) -> Vec<String> {
        let mut flags = vec![
            "--batch".to_string(),
            batch.to_string(),
            "--cohort-size".to_string(),
            "2".to_string(),
        ];
        match self {
            Scenario::Round => {}
            Scenario::Merge => {
                flags.extend(["--review".to_string(), "--join".to_string()]);
            }
            Scenario::Quarantine => {
                flags.extend([
                    "--review".to_string(),
                    "--join".to_string(),
                    "--inject-merge-conflict".to_string(),
                    "T-102".to_string(),
                ]);
            }
        }
        flags
    }
}

/// The legacy checkout root, or `None` (then the caller self-skips). Read only from the
/// environment so no foreign absolute path is baked into the test.
fn legacy_checkout() -> Option<PathBuf> {
    let raw = match std::env::var(LEGACY_CHECKOUT_ENV) {
        Ok(raw) if !raw.trim().is_empty() => raw,
        _ => {
            eprintln!(
                "SKIP: {LEGACY_CHECKOUT_ENV} is unset/empty — no legacy Orchestra checkout to diff against"
            );
            return None;
        }
    };
    let root = PathBuf::from(raw);
    if !root.join("tools").join("harness.ps1").is_file() {
        eprintln!(
            "SKIP: {LEGACY_CHECKOUT_ENV}={} has no tools/harness.ps1 (legacy oracle unavailable)",
            root.display()
        );
        return None;
    }
    Some(root)
}

/// The first PowerShell host that can actually launch, or `None` (then the caller self-skips).
fn pwsh_host() -> Option<String> {
    for host in ["pwsh", "powershell"] {
        let ok = Command::new(host)
            .args(["-NoProfile", "-Command", "exit 0"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            return Some(host.to_string());
        }
    }
    None
}

/// Resolve the shared preconditions (pwsh host + legacy checkout) or return `None` to self-skip.
macro_rules! oracle_or_skip {
    () => {{
        let Some(host) = pwsh_host() else {
            eprintln!("SKIP: no PowerShell host (pwsh/powershell) for the legacy harness/tools");
            return;
        };
        let Some(checkout) = legacy_checkout() else {
            return;
        };
        (host, checkout)
    }};
}

/// A throwaway repository + `.work` for the native engine side, removed on drop.
struct NativeRepo {
    root: PathBuf,
    work: PathBuf,
}

impl NativeRepo {
    fn new(scenario: Scenario, backend: Backend) -> NativeRepo {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "orchestra-fingerprint-{}-{}-{}-{nanos}-{n}",
            scenario.label(),
            backend.harness_arg(),
            std::process::id()
        ));
        let work = root.join(".work");
        fs::create_dir_all(&work).expect("create native sandbox .work");
        NativeRepo { root, work }
    }

    fn read(&self, rel: &str) -> String {
        fs::read_to_string(self.work.join(rel)).unwrap_or_default()
    }
}

impl Drop for NativeRepo {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Initialize the disposable Git/JJ repository with a seed commit on `BASE_REF` that already
/// ignores the control plane and build outputs (K-009: parallel worktrees carry their own
/// multi-GB `target/`, and an un-ignored `.work` would otherwise leak into the committed tree).
fn init_repo(backend: Backend, repo: &NativeRepo) {
    fs::write(repo.root.join(".gitignore"), ".work/\ntarget/\n").expect("seed .gitignore");
    fs::write(repo.root.join("seed.txt"), "seed\n").expect("seed file");
    match backend {
        Backend::Git => {
            run_setup(
                Command::new("git")
                    .args(["init", "--initial-branch", BASE_REF])
                    .current_dir(&repo.root),
                "git init",
            );
            run_setup(
                Command::new("git")
                    .args(["config", "user.name", "Orchestrail Test"])
                    .current_dir(&repo.root),
                "git config name",
            );
            run_setup(
                Command::new("git")
                    .args(["config", "user.email", "test@example.invalid"])
                    .current_dir(&repo.root),
                "git config email",
            );
            run_setup(
                Command::new("git")
                    .args(["config", "core.autocrlf", "false"])
                    .current_dir(&repo.root),
                "git config autocrlf",
            );
            run_setup(
                Command::new("git")
                    .args(["add", "-A"])
                    .current_dir(&repo.root),
                "git add",
            );
            run_setup(
                Command::new("git")
                    .args(["commit", "-m", "seed"])
                    .current_dir(&repo.root),
                "git commit",
            );
        }
        Backend::Jj => {
            let config = repo.root.join(".jj-identity.toml");
            fs::write(
                &config,
                "[user]\nname = \"Orchestrail Test\"\nemail = \"test@example.invalid\"\n",
            )
            .expect("jj identity config");
            let config = config.to_string_lossy().into_owned();
            run_setup(
                Command::new("jj")
                    .args(["git", "init"])
                    .current_dir(&repo.root)
                    .env("JJ_CONFIG", &config),
                "jj git init",
            );
            run_setup(
                Command::new("jj")
                    .args(["describe", "-m", "seed"])
                    .current_dir(&repo.root)
                    .env("JJ_CONFIG", &config),
                "jj describe",
            );
            run_setup(
                Command::new("jj")
                    .args(["bookmark", "create", BASE_REF, "-r", "@"])
                    .current_dir(&repo.root)
                    .env("JJ_CONFIG", &config),
                "jj bookmark",
            );
            run_setup(
                Command::new("jj")
                    .args(["new"])
                    .current_dir(&repo.root)
                    .env("JJ_CONFIG", &config),
                "jj new",
            );
        }
    }
}

/// Run a scaffolding setup command, panicking with context on failure. Repository set-up is not
/// the boundary under test, so a broken host tool is a hard error rather than a silent skip.
fn run_setup(command: &mut Command, what: &str) {
    let output = command
        .output()
        .unwrap_or_else(|e| panic!("spawn {what}: {e}"));
    assert!(
        output.status.success(),
        "{what} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The real `tools/` in the legacy checkout — the same transactional tools `engine run` drives.
fn tools_dir(checkout: &Path) -> PathBuf {
    checkout.join("tools")
}

/// Seed one not-started task with a planner-owned descriptor supplying its conflict-domain.
fn seed_task(host: &str, checkout: &Path, work: &Path, id: &str, title: &str, domain: &str) {
    let script = tools_dir(checkout).join("queue-tx.ps1");
    let mut cmd = Command::new(host);
    cmd.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(&script)
        .args(["propose", "--work"])
        .arg(work)
        .args(["--id", id, "--title", title]);
    let out = cmd.output().expect("spawn queue-tx propose");
    assert!(
        out.status.success(),
        "propose {id}: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let dir = work.join("tasks").join(id);
    fs::create_dir_all(&dir).expect("create planned descriptor directory");
    fs::write(
        dir.join("task.md"),
        format!("# {id}\nСтатус: не начата\nКонфликт-домен: {domain}\n"),
    )
    .expect("write planned descriptor");
}

/// Drive ONE cohort/phase end-to-end over the disposable repository with the legacy tools.
fn run_native_engine(
    checkout: &Path,
    repo: &NativeRepo,
    scenario: Scenario,
    batch: &str,
) -> Output {
    let mut cmd = Command::new(BIN);
    cmd.args(["run", "--once", "--work"])
        .arg(&repo.work)
        .arg("--root")
        .arg(&repo.root)
        .arg("--tools")
        .arg(tools_dir(checkout))
        .args(["--base", BASE_REF])
        .args(["--json"]);
    for flag in scenario.engine_flags(batch) {
        cmd.arg(flag);
    }
    cmd.output().expect("spawn engine run")
}

/// The native final-state fingerprint: the crate projector fed by the typed committed-tree
/// inventory of the base ref (which the join barrier fast-forwards on publish).
fn native_fingerprint(repo: &NativeRepo) -> FinalStateFingerprint {
    let tree = committed_tree_inventory(&repo.root, BASE_REF)
        .expect("typed committed-tree inventory of the base ref");
    final_state_fingerprint(&repo.work, tree).expect("native final-state fingerprint")
}

/// Invoke the read-only legacy harness for the equivalent scenario+backend and parse its
/// reference fingerprint. See the module-level "Harness protocol contract".
fn harness_fingerprint(
    host: &str,
    checkout: &Path,
    scenario: Scenario,
    backend: Backend,
) -> FinalStateFingerprint {
    let script = tools_dir(checkout).join("harness.ps1");
    let out = Command::new(host)
        .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(&script)
        .args(["--scenario", scenario.harness_arg()])
        .args(["--backend", backend.harness_arg()])
        .output()
        .expect("spawn legacy harness.ps1");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "legacy harness ({} / {}) failed: {}\n{stdout}",
        scenario.label(),
        backend.label(),
        String::from_utf8_lossy(&out.stderr)
    );
    parse_fingerprint(&stdout).unwrap_or_else(|| {
        panic!(
            "legacy harness ({} / {}) did not print a tree/queue/archive/outbox/combined envelope:\n{stdout}",
            scenario.label(),
            backend.label()
        )
    })
}

/// Parse a `key=value` fingerprint envelope, tolerant of surrounding harness log lines. Requires
/// all five components so a partial/garbled read cannot masquerade as a match.
fn parse_fingerprint(text: &str) -> Option<FinalStateFingerprint> {
    let mut fields: BTreeMap<&str, String> = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        for key in ["tree", "queue", "archive", "outbox", "combined"] {
            if let Some(rest) = line.strip_prefix(&format!("{key}=")) {
                fields.insert(key, rest.trim().to_string());
            }
        }
    }
    Some(FinalStateFingerprint {
        tree: fields.remove("tree")?,
        queue: fields.remove("queue")?,
        archive: fields.remove("archive")?,
        outbox: fields.remove("outbox")?,
        combined: fields.remove("combined")?,
    })
}

/// Compare the two fingerprints and, on divergence, fail with a per-component diagnostic diff so
/// the drifting envelope component (tree/queue/archive/outbox) is immediately visible.
fn assert_fingerprints_match(
    native: &FinalStateFingerprint,
    legacy: &FinalStateFingerprint,
    scenario: Scenario,
    backend: Backend,
) {
    if native.combined == legacy.combined {
        return;
    }
    let mut diff = String::new();
    for (component, native_value, legacy_value) in [
        ("tree", &native.tree, &legacy.tree),
        ("queue", &native.queue, &legacy.queue),
        ("archive", &native.archive, &legacy.archive),
        ("outbox", &native.outbox, &legacy.outbox),
        ("combined", &native.combined, &legacy.combined),
    ] {
        let mark = if native_value == legacy_value {
            "=="
        } else {
            "!="
        };
        diff.push_str(&format!(
            "  {component} {mark}\n    native: {native_value}\n    legacy: {legacy_value}\n"
        ));
    }
    panic!(
        "native vs legacy final-state fingerprint diverged for the {} scenario on {}:\n{diff}",
        scenario.label(),
        backend.label()
    );
}

/// The shared body of every case: seed equivalent inputs, run the native engine, compute the
/// native fingerprint, and diff it against the legacy harness oracle.
fn run_case(scenario: Scenario, backend: Backend) {
    let (host, checkout) = oracle_or_skip!();
    let repo = NativeRepo::new(scenario, backend);
    init_repo(backend, &repo);

    // Two independent (non-overlapping) tasks so both are admitted into one cohort; T-102 is the
    // quarantine target under `--inject-merge-conflict`.
    seed_task(
        &host,
        &checkout,
        &repo.work,
        "T-101",
        "fingerprint one",
        "alpha/**",
    );
    seed_task(
        &host,
        &checkout,
        &repo.work,
        "T-102",
        "fingerprint two",
        "beta/**",
    );

    let batch = format!(
        "B-fingerprint-{}-{}",
        scenario.harness_arg(),
        backend.harness_arg()
    );
    let run = run_native_engine(&checkout, &repo, scenario, &batch);
    assert!(
        run.status.success(),
        "native engine run ({} / {}) exits 0: {}\n{}",
        scenario.label(),
        backend.label(),
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    // Touch the sandbox projection so an accidental empty `.work` surfaces here, not as a cryptic
    // fingerprint mismatch.
    assert!(
        !repo.read("events.jsonl").is_empty(),
        "the native round emitted an events outbox to project"
    );

    let native = native_fingerprint(&repo);
    let legacy = harness_fingerprint(&host, &checkout, scenario, backend);
    assert_fingerprints_match(&native, &legacy, scenario, backend);
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn legacy_fingerprint_round_matches_on_git() {
    run_case(Scenario::Round, Backend::Git);
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn legacy_fingerprint_round_matches_on_jj() {
    run_case(Scenario::Round, Backend::Jj);
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn legacy_fingerprint_merge_matches_on_git() {
    run_case(Scenario::Merge, Backend::Git);
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn legacy_fingerprint_merge_matches_on_jj() {
    run_case(Scenario::Merge, Backend::Jj);
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn legacy_fingerprint_quarantine_matches_on_git() {
    run_case(Scenario::Quarantine, Backend::Git);
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn legacy_fingerprint_quarantine_matches_on_jj() {
    run_case(Scenario::Quarantine, Backend::Jj);
}
