//! The single, shared **"checkout vs cc-sync mirror" resolver** for a `tools/<script>.ps1`
//! runner (`state-tx.ps1`, `policy.ps1`, …), so the normative rule lives in exactly ONE place
//! reused by both the engine (`engine/src/main.rs::cmd_lease`) and the TUI command channel
//! (`tui/src/commands.rs`). See `docs/queue_contract.md` §9 and `knowledge.md` "Резолвинг
//! раннеров `tools/*.ps1` (чекаут vs зеркало)".
//!
//! **The rule (and why the naive "does `<root>/tools/<name>` exist?" check is unsafe).** The mere
//! existence of `<root>/tools/<name>.ps1` does **not** prove an Orchestra checkout layout: a
//! *target* project that merely consumes Orchestra through `~/.claude` may carry its own — or a
//! stale, gitignored — `tools/` folder that would shadow the real runtime. Executing that
//! same-named file would run a **foreign / outdated** script with the operator's authority
//! (including the mutating `policy.ps1 approve/reject`). A checkout is proven only when the root
//! **simultaneously** carries all three identity markers ([`CHECKOUT_IDENTITY_MARKERS`]); without
//! them the resolver goes **straight to the cc-sync mirror** `~/.claude/scripts/<name>.ps1` and
//! never touches `<root>/tools/<name>.ps1`, even if that file physically exists.
//!
//! This module is pure filesystem inspection: it does not spawn anything and holds no lock.

use std::path::{Path, PathBuf};

/// The three identity markers that **jointly** prove an Orchestra *checkout* layout at a project
/// root (the "checkout vs mirror" rule — `docs/queue_contract.md` §9, `knowledge.md` "Резолвинг
/// раннеров `tools/*.ps1`"). All three must be present; any subset is treated as "not a checkout"
/// and routed to the cc-sync mirror instead. Paths are relative to the candidate root, using `/`
/// separators (Rust's `Path::join` accepts them on every platform).
pub const CHECKOUT_IDENTITY_MARKERS: [&str; 3] = [
    "agents/processor.md",
    "generate-codex-agents.ps1",
    "tools/sync-runtime.ps1",
];

/// `true` iff `root` carries **all three** [`CHECKOUT_IDENTITY_MARKERS`] — i.e. it is a proven
/// Orchestra checkout whose `tools/<name>.ps1` may be trusted. Missing even one marker yields
/// `false` (route to the mirror), which is the whole point: a lone `tools/` folder is not proof.
pub fn is_orchestra_checkout(root: &Path) -> bool {
    has_checkout_identity(root, Path::exists)
}

/// Check the checkout identity with an injected existence probe. Keeping this core public lets
/// read-only diagnostics report the same trusted-root decision without a second marker list.
pub fn has_checkout_identity(root: &Path, exists: impl Fn(&Path) -> bool) -> bool {
    CHECKOUT_IDENTITY_MARKERS
        .iter()
        .all(|marker| exists(&root.join(marker)))
}

/// The cc-sync mirror directory (`~/.claude/scripts`) where `tools/sync-runtime.ps1` mirrors the
/// whole `tools/*.ps1` set for any project that uses Orchestra without being its checkout (T-115).
/// `None` when neither `HOME` nor `USERPROFILE` is set (then there is no mirror to fall back to).
pub fn cc_sync_mirror_dir() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)?;
    Some(home.join(".claude").join("scripts"))
}

/// Pure core of [`resolve_tool_script`], with the mirror directory injected so it can be unit-tested
/// without touching the real `HOME`. A checkout-local `<root>/tools/<name>` is returned **only**
/// when `root` is a proven checkout (all identity markers) **and** the file exists; otherwise the
/// resolver falls to `<mirror_dir>/<name>` if that exists, else `None`. It never returns the
/// checkout-local path for a non-checkout root, which is the security property this whole module
/// exists to guarantee.
fn resolve_with_mirror(root: &Path, name: &str, mirror_dir: Option<&Path>) -> Option<PathBuf> {
    if is_orchestra_checkout(root) {
        let checkout = root.join("tools").join(name);
        if checkout.exists() {
            return Some(checkout);
        }
    }
    let mirror = mirror_dir?.join(name);
    if mirror.exists() { Some(mirror) } else { None }
}

/// Resolve the `tools/<name>.ps1` runner for a project rooted at `root`, following the single
/// checkout-vs-mirror rule (module doc). Prefer the project's own `<root>/tools/<name>` **only when
/// `root` is a proven Orchestra checkout** ([`is_orchestra_checkout`]); otherwise (or if the
/// checkout-local file is absent) fall back to the cc-sync mirror `~/.claude/scripts/<name>`.
/// `None` when neither a trusted checkout copy nor a mirror copy exists — the caller must surface
/// that rather than execute an untrusted same-named file.
pub fn resolve_tool_script(root: &Path, name: &str) -> Option<PathBuf> {
    resolve_with_mirror(root, name, cc_sync_mirror_dir().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TmpDir {
        dir: PathBuf,
    }
    impl TmpDir {
        fn new(tag: &str) -> TmpDir {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "orchestra-toolscript-{tag}-{}-{nanos}-{n}",
                std::process::id()
            ));
            fs::create_dir_all(&dir).unwrap();
            TmpDir { dir }
        }
        fn touch(&self, rel: &str) -> PathBuf {
            let p = self.dir.join(rel);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(&p, b"x").unwrap();
            p
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    fn seed_all_markers(root: &TmpDir) {
        for m in CHECKOUT_IDENTITY_MARKERS {
            root.touch(m);
        }
    }

    #[test]
    fn checkout_requires_all_three_markers() {
        let root = TmpDir::new("markers");
        // A lone tools/ folder is NOT a checkout, no matter what it contains.
        root.touch("tools/state-tx.ps1");
        assert!(
            !is_orchestra_checkout(&root.dir),
            "tools/ alone is not proof"
        );

        // Any strict subset of the markers still fails.
        for missing in 0..CHECKOUT_IDENTITY_MARKERS.len() {
            let partial = TmpDir::new("partial");
            for (i, m) in CHECKOUT_IDENTITY_MARKERS.iter().enumerate() {
                if i != missing {
                    partial.touch(m);
                }
            }
            assert!(
                !is_orchestra_checkout(&partial.dir),
                "missing marker #{missing} must not count as a checkout"
            );
        }

        // All three present -> proven checkout.
        seed_all_markers(&root);
        assert!(
            is_orchestra_checkout(&root.dir),
            "all three markers present"
        );
    }

    #[test]
    fn proven_checkout_returns_target_local_path() {
        let root = TmpDir::new("checkout");
        seed_all_markers(&root);
        let target = root.touch("tools/state-tx.ps1");
        // Even with a mirror also holding the script, the proven checkout copy wins.
        let mirror = TmpDir::new("mirror-a");
        mirror.touch("state-tx.ps1");
        assert_eq!(
            resolve_with_mirror(&root.dir, "state-tx.ps1", Some(&mirror.dir)),
            Some(target)
        );
    }

    #[test]
    fn non_checkout_never_returns_target_local_even_when_present() {
        // The core security property: a target-local tools/<name> present, but NO identity markers
        // -> the resolver must never hand back that checkout-local path. It falls to the mirror
        // (here present) instead; without a mirror it would be None. Either way, not target-local.
        let root = TmpDir::new("foreign");
        let stray = root.touch("tools/state-tx.ps1");
        let mirror = TmpDir::new("mirror-b");
        let mirror_script = mirror.touch("state-tx.ps1");

        let resolved = resolve_with_mirror(&root.dir, "state-tx.ps1", Some(&mirror.dir));
        assert_ne!(
            resolved.as_ref(),
            Some(&stray),
            "must not execute a foreign target-local tools/ script"
        );
        assert_eq!(resolved, Some(mirror_script), "falls through to the mirror");

        // With no mirror at all -> None (never the stray target-local file).
        assert_eq!(
            resolve_with_mirror(&root.dir, "state-tx.ps1", None),
            None,
            "no trusted copy anywhere -> None, never the foreign file"
        );
    }

    #[test]
    fn checkout_with_missing_tool_falls_through_to_mirror() {
        // Proven checkout, but this particular tool is absent from <root>/tools -> use the mirror.
        let root = TmpDir::new("checkout-missing-tool");
        seed_all_markers(&root);
        let mirror = TmpDir::new("mirror-c");
        let policy = mirror.touch("policy.ps1");
        assert_eq!(
            resolve_with_mirror(&root.dir, "policy.ps1", Some(&mirror.dir)),
            Some(policy)
        );
    }
}
