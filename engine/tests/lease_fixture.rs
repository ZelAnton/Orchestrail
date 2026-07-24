//! Hermetic, offline proof of the `engine lease {acquire|heartbeat|release|status}` subcommand
//! end to end (task T-107): drive the REAL built binary against a throwaway `.work/` and the REAL
//! `tools/state-tx.ps1`, and assert the engine takes / renews / releases / inspects its owner lease
//! **strictly** through `state-tx.ps1` under its own role (`engine`) — and, crucially, that it
//! cleanly REFUSES a live foreign (`processor`) lease without deleting it, and adopts only a
//! provably-stale one. Nothing here touches this repository's own `.work/`; every lease lives in a
//! per-test temp directory that is removed on drop.
//!
//! The test needs a PowerShell host to run `state-tx.ps1`. When neither `pwsh` (PowerShell 7) nor
//! `powershell` (Windows PowerShell 5.1) is available it self-skips with a note (it never fails on
//! a host without PowerShell); GitHub's windows-latest and ubuntu-latest both ship `pwsh`, so it
//! runs fully in the engine CI job.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod common;
use common::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// The real `tools/state-tx.ps1`, resolved from this crate's manifest dir (`.../engine`) →
/// repo root → `tools/state-tx.ps1`.
fn state_tx_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine crate has a parent (repo root)")
        .join("tools")
        .join("state-tx.ps1")
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

/// Resolve a host or print a skip note and yield `None`. Keeps every test's guard one line.
macro_rules! host_or_skip {
    () => {
        match pwsh_host() {
            Some(h) => h,
            None => {
                eprintln!("SKIP: no PowerShell host (pwsh/powershell) available for state-tx.ps1");
                return;
            }
        }
    };
}

struct TmpWork {
    dir: PathBuf,
}

impl TmpWork {
    fn new() -> TmpWork {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "orchestra-lease-fixture-{}-{nanos}-{n}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        TmpWork { dir }
    }
    /// The raw `lease.json` text, if a structured lease is present.
    fn lease_json(&self) -> Option<String> {
        fs::read_to_string(self.dir.join("orchestrator.lock").join("lease.json")).ok()
    }
}
impl Drop for TmpWork {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// Run `engine lease <extra…> --work <w> --script <state-tx.ps1>`.
fn engine_lease(work: &Path, script: &Path, extra: &[&str]) -> Output {
    let mut cmd = Command::new(BIN);
    cmd.arg("lease");
    for a in extra {
        cmd.arg(a);
    }
    cmd.arg("--work").arg(work).arg("--script").arg(script);
    cmd.output().expect("spawn engine lease")
}

/// Seed a foreign lease directly through the real `state-tx.ps1` (bypassing the engine), to model
/// a lease held by a *different* owner/role than the engine — e.g. a live `processor`.
fn seed_lease(host: &str, script: &Path, work: &Path, role: &str, ttl: &str) -> Output {
    let mut cmd = Command::new(host);
    cmd.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(script)
        .args(["acquire", "--work"])
        .arg(work)
        .args(["--root", "/seed/root", "--role", role, "--ttl", ttl]);
    cmd.output().expect("spawn state-tx acquire (seed)")
}

/// Extract the `owner=<id>` token from the engine's acquire output.
fn parse_owner(s: &str) -> String {
    let i = s.find("owner=").expect("acquire output carries owner=") + "owner=".len();
    let rest = &s[i..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    rest[..end].to_string()
}

fn stdout_of(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}
fn stderr_of(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn acquire_status_heartbeat_release_lifecycle() {
    let host = host_or_skip!();
    let _ = host; // host only needed to gate; the engine spawns state-tx itself.
    let script = state_tx_script();
    let w = TmpWork::new();

    // Idle: status reports the lock is free (a successful read, exit 0).
    let st = engine_lease(&w.dir, &script, &["status"]);
    assert!(
        st.status.success(),
        "idle status exits 0: {}",
        stderr_of(&st)
    );
    assert!(
        stdout_of(&st).contains("none (free"),
        "idle status says free: {}",
        stdout_of(&st)
    );

    // Acquire when idle -> lease taken under role=engine.
    let ac = engine_lease(&w.dir, &script, &["acquire", "--ttl", "600"]);
    assert!(
        ac.status.success(),
        "acquire when idle succeeds: {} / {}",
        stdout_of(&ac),
        stderr_of(&ac)
    );
    let out = stdout_of(&ac);
    assert!(
        out.contains("role=engine"),
        "acquired under engine role: {out}"
    );
    let owner = parse_owner(&out);
    assert!(!owner.is_empty(), "acquire surfaces an owner id");
    let lease = w.lease_json().expect("lease.json exists after acquire");
    assert!(
        lease.contains("\"role\": \"engine\"") || lease.contains("\"role\":\"engine\""),
        "lease.json records role=engine, never processor: {lease}"
    );
    assert!(
        !lease.contains("processor"),
        "engine never writes a processor lease: {lease}"
    );

    // Status (live) reflects owner + role.
    let st = engine_lease(&w.dir, &script, &["status", "--json"]);
    assert!(st.status.success());
    let sj = stdout_of(&st);
    assert!(
        sj.contains("\"role\":\"engine\""),
        "status --json role=engine: {sj}"
    );
    assert!(sj.contains("\"live\":true"), "fresh lease is live: {sj}");
    assert!(sj.contains(&owner), "status shows the owner id: {sj}");

    // Heartbeat by the owner renews it (generation advances 1 -> 2).
    let hb = engine_lease(&w.dir, &script, &["heartbeat", "--owner", &owner]);
    assert!(
        hb.status.success(),
        "owner heartbeat renews: {}",
        stderr_of(&hb)
    );
    let after = w.lease_json().unwrap();
    assert!(
        after.contains("\"generation\": 2") || after.contains("\"generation\":2"),
        "heartbeat bumped generation: {after}"
    );

    // Release by the owner clears the lease; status then reads free again.
    let rel = engine_lease(&w.dir, &script, &["release", "--owner", &owner]);
    assert!(
        rel.status.success(),
        "owner release succeeds: {}",
        stderr_of(&rel)
    );
    assert!(w.lease_json().is_none(), "release removed the lease.json");
    let st = engine_lease(&w.dir, &script, &["status"]);
    assert!(st.status.success());
    assert!(stdout_of(&st).contains("none (free"), "free after release");
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn heartbeat_and_release_are_owner_checked() {
    let _host = host_or_skip!();
    let script = state_tx_script();
    let w = TmpWork::new();

    let ac = engine_lease(&w.dir, &script, &["acquire", "--ttl", "600"]);
    assert!(ac.status.success(), "acquire: {}", stderr_of(&ac));
    let owner = parse_owner(&stdout_of(&ac));

    // A non-owner heartbeat/release must be refused (nonzero) and the lease must survive.
    let hb = engine_lease(&w.dir, &script, &["heartbeat", "--owner", "not-the-owner"]);
    assert!(!hb.status.success(), "non-owner heartbeat refused");
    assert_eq!(hb.status.code(), Some(4), "NOT_OWNER exit code");
    assert!(
        stderr_of(&hb).contains("refused"),
        "clean refusal message, not a panic: {}",
        stderr_of(&hb)
    );

    let rel = engine_lease(&w.dir, &script, &["release", "--owner", "not-the-owner"]);
    assert!(!rel.status.success(), "non-owner release refused");
    assert_eq!(rel.status.code(), Some(4), "NOT_OWNER exit code");
    assert!(
        w.lease_json().is_some(),
        "the lease survived the refused non-owner release"
    );

    // The real owner can still release it (proving the lease was intact and owner-checked).
    let rel = engine_lease(&w.dir, &script, &["release", "--owner", &owner]);
    assert!(rel.status.success(), "owner release: {}", stderr_of(&rel));
    assert!(w.lease_json().is_none());
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn refuses_a_live_foreign_processor_lease_and_never_deletes_it() {
    let host = host_or_skip!();
    let script = state_tx_script();
    let w = TmpWork::new();

    // A live processor holds the lease (fresh heartbeat, generous TTL).
    let seed = seed_lease(&host, &script, &w.dir, "processor", "600");
    assert!(
        seed.status.success(),
        "seeded a processor lease: {}",
        stderr_of(&seed)
    );
    let proc_owner = parse_owner(&stdout_of(&seed));

    // The engine's acquire must CLEANLY refuse — nonzero, with a diagnostic, no panic.
    let ac = engine_lease(&w.dir, &script, &["acquire", "--ttl", "600"]);
    assert!(
        !ac.status.success(),
        "engine refuses a live processor lease"
    );
    assert_eq!(ac.status.code(), Some(3), "HELD_LIVE exit code");
    let err = stderr_of(&ac);
    assert!(err.contains("refused"), "clean refusal: {err}");
    assert!(
        err.contains("live") || err.contains("held"),
        "explains the lease is held/live: {err}"
    );
    assert!(
        !err.to_lowercase().contains("panic"),
        "no Rust panic in the refusal: {err}"
    );

    // The engine's release of the FOREIGN lease (presenting its own owner) must also be refused,
    // and the processor's lease must be intact and unchanged.
    let rel = engine_lease(&w.dir, &script, &["release", "--owner", "engine-not-owner"]);
    assert!(
        !rel.status.success(),
        "engine cannot release a foreign lease"
    );
    assert_eq!(rel.status.code(), Some(4), "NOT_OWNER exit code");

    let lease = w.lease_json().expect("processor lease still present");
    assert!(
        lease.contains("\"role\": \"processor\"") || lease.contains("\"role\":\"processor\""),
        "the foreign lease is still a processor lease: {lease}"
    );
    assert!(
        lease.contains(&proc_owner),
        "the processor owner is unchanged (engine did not steal or rewrite it): {lease}"
    );
}

#[test]
#[ignore = "requires read-only legacy Orchestra transactional tools; excluded from the self-contained Orchestrail workspace"]
fn adopts_a_provably_stale_lease_via_takeover() {
    let host = host_or_skip!();
    let script = state_tx_script();
    let w = TmpWork::new();

    // Seed a processor lease with a 1s TTL and NO pid, so liveness is judged purely by heartbeat
    // freshness; after sleeping past the TTL it is provably stale (the dead-owner case).
    let seed = seed_lease(&host, &script, &w.dir, "processor", "1");
    assert!(
        seed.status.success(),
        "seeded a short-TTL lease: {}",
        stderr_of(&seed)
    );
    std::thread::sleep(Duration::from_millis(2000));

    // The engine's acquire adopts a stale lease via the liveness-gated takeover (never --force);
    // the resulting lease is now an `engine` lease.
    let ac = engine_lease(&w.dir, &script, &["acquire", "--ttl", "600"]);
    assert!(
        ac.status.success(),
        "engine adopts a provably-stale lease: {} / {}",
        stdout_of(&ac),
        stderr_of(&ac)
    );
    let out = stdout_of(&ac);
    assert!(
        out.contains("adopted a stale lease"),
        "output notes the safe takeover of a stale lease: {out}"
    );
    assert!(
        out.contains("role=engine"),
        "adopted lease is under role=engine: {out}"
    );
    let lease = w.lease_json().expect("lease.json present after takeover");
    assert!(
        lease.contains("\"role\": \"engine\"") || lease.contains("\"role\":\"engine\""),
        "the adopted lease is now an engine lease: {lease}"
    );
    assert!(
        lease.contains("taken_over_from"),
        "the takeover recorded the previous owner for audit: {lease}"
    );
}
