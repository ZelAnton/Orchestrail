//! orchestrail-tui — a live operator overview of a running orchestrator, **read-only by default for
//! observation** but able to send a small, named command subset "downward", with two switchable
//! screens (`Tab`): the §6.1 overview and the §6.2 Decision Inbox.
//!
//! It tails `<work>/events.jsonl` through the engine crate's cursor reader
//! ([`orchestrail_engine::events::TailReader`] — the SAME reader the future engine uses, so
//! there is no duplicated tail/dedup/torn-tail logic) and folds the `cohort.*` / `task.*` stream
//! into the current batch/cohort/task projection ([`app::AppState`]), overlaying human context
//! from `<work>/status.md` ([`status`]). The Decision Inbox ([`inbox`]) is rebuilt on the same
//! cadence from a metadata-invalidated [`orchestrail_engine::state::SnapshotCache`] (queue + task
//! descriptors), whether `<work>/PAUSE` exists, and the task ids already archived to
//! `<work>/Tasks_Done.md` (used only to confirm, not invent, a predecessor's completion — see
//! [`done_task_ids`]). Snapshot, status, and archive parses are reused until their confined
//! `(mtime, len)` metadata changes.
//!
//! **Command channel ([`commands`]).** Every module above only *observes* `.work/`; the sole way
//! this TUI writes is the deliberately narrow §5/§6.2 command subset, driven only by an explicit
//! keystroke: `p` pause (create `.work/PAUSE`, mirroring `cc-pause.sh`), `u` resume (remove it,
//! mirroring `cc-unpause.sh`), `s` lease-status (read `.work/orchestrator.lock` via the engine
//! crate's owner-checked `tools/state-tx.ps1 status` path), and `x` force-lock — the one
//! destructive command, gated behind an explicit confirmation modal (`y` to confirm), routing
//! through the single transactional `tools/state-tx.ps1 release --force` path (the same path
//! `cc-processor.sh --force-lock` now uses). On the Decision Inbox,
//! `a`/`d` arm approve/reject for the selected pending request. Native Orchestrail approvals go
//! through the engine's typed [`orchestrail_engine::approval::ApprovalStore`]; legacy Orchestra
//! approvals retain the contained `tools/policy.ps1` compatibility path. The TUI never touches
//! the queue / task descriptors / code and never calls `processor` or a launcher. Both approval
//! actions require a second explicit confirmation.
//!
//! The terminal is always restored — normal quit, error return, or panic (see [`terminal`]).

mod app;
mod cache;
mod cli;
mod commands;
mod inbox;
mod status;
mod terminal;
mod ui;

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event as CEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use orchestrail_engine::config;
use orchestrail_engine::events::TailReader;
use orchestrail_engine::state::SnapshotCache;
use orchestrail_engine::telemetry::batch_telemetry_summary_with_pricing;
use orchestrail_engine::work_fs;

use app::{AppState, InboxPanel, Modal, Screen};
use cli::{Cli, Config};

const MAX_PAUSE_BYTES: u64 = 4 * 1024;

#[derive(Debug, Default)]
struct ControlPlaneCache {
    snapshot: SnapshotCache,
    done_ids: cache::PlainFileCache<BTreeSet<String>>,
    status: status::Cache,
}

impl ControlPlaneCache {
    fn invalidate(&mut self) {
        self.snapshot.invalidate();
        self.done_ids.invalidate();
        self.status.invalidate();
    }
}

fn main() {
    let cfg = match cli::parse(std::env::args().skip(1)) {
        Ok(Cli::Run(cfg)) => cfg,
        Ok(Cli::Print(text)) => {
            print!("{text}");
            return;
        }
        Err(msg) => {
            eprintln!("orchestrail-tui: {msg}");
            std::process::exit(2);
        }
    };

    if let Err(e) = run(cfg) {
        // The TerminalGuard in `run` has already restored the terminal by the time we get here.
        eprintln!("orchestrail-tui: {e}");
        std::process::exit(1);
    }
}

fn run(cfg: Config) -> io::Result<()> {
    let events_path: PathBuf = cfg.work_dir.join("events.jsonl");
    let status_path: PathBuf = cfg.work_dir.join("status.md");

    let mut reader = TailReader::new(&events_path);
    let mut app = AppState::new();
    let mut control_plane_cache = ControlPlaneCache::default();
    // Prime the projection with everything already in the journal (a cold observer of a
    // long-running orchestra) before drawing the first frame.
    app.apply_all(&reader.poll_all()?);
    app.status = status::load(&mut control_plane_cache.status, &status_path);
    app.replace_inbox(load_inbox(&cfg.work_dir, &mut control_plane_cache));
    refresh_batch_telemetry(&mut app, &cfg.work_dir);

    terminal::install_panic_hook();
    let mut term = terminal::init()?;
    let _guard = terminal::TerminalGuard; // restores on any exit path

    let tick = Duration::from_millis(cfg.tick_ms);
    let status_reload_every = Duration::from_millis(500);
    let mut last_status_reload = Instant::now();

    loop {
        // 1. Pull any newly-appended events (cursor reader: only new, unique, committed lines).
        let new = reader.poll()?;
        if !new.is_empty() {
            app.apply_all(&new);
            refresh_batch_telemetry(&mut app, &cfg.work_dir);
        }

        // 2. Refresh the status.md overlay and Decision Inbox on a gentle cadence. Parsed
        // snapshot/status/archive state is metadata-cached; safety- and time-sensitive inputs
        // remain fresh. `replace_inbox` also invalidates an approve/reject modal if its captured
        // one-time approval disappeared during the flow.
        if last_status_reload.elapsed() >= status_reload_every {
            app.status = status::load(&mut control_plane_cache.status, &status_path);
            app.replace_inbox(load_inbox(&cfg.work_dir, &mut control_plane_cache));
            last_status_reload = Instant::now();
        }

        // 3. Paint.
        term.draw(|f| ui::render(f, &app))?;

        // 4. Handle input, blocking up to one tick so the loop also serves as the refresh timer.
        if event::poll(tick)?
            && let CEvent::Key(k) = event::read()?
            && k.kind == KeyEventKind::Press
        {
            // An open modal captures ALL input until dismissed, so no navigation or other
            // command can leak past the force-lock confirmation gate (§6.2).
            if app.has_modal() {
                handle_modal_key(&mut app, &cfg.work_dir, &mut control_plane_cache, k);
            } else if handle_key(
                &mut app,
                &cfg,
                &status_path,
                &mut control_plane_cache,
                &mut last_status_reload,
                k,
            ) {
                break;
            }
        }
    }

    Ok(())
}

/// Route a keystroke while no modal is open. Returns `true` when the app should quit. Besides the
/// read-only navigation, this routes the §5/§6.2 safe command subset (see the module docs): `p`
/// pause, `u` resume, `s` lease-status, `x` *arm* force-lock (which only opens the confirmation
/// modal — the removal itself needs the explicit second keystroke handled by [`handle_modal_key`]).
fn handle_key(
    app: &mut AppState,
    cfg: &Config,
    status_path: &Path,
    control_plane_cache: &mut ControlPlaneCache,
    last_status_reload: &mut Instant,
    k: KeyEvent,
) -> bool {
    match k.code {
        KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Char('q') if k.modifiers.is_empty() => return true,
        // Esc first dismisses a lease-status overlay if one is showing, otherwise it quits.
        KeyCode::Esc => {
            if !app.dismiss_lease() {
                return true;
            }
        }
        KeyCode::Tab if k.modifiers.is_empty() => app.toggle_screen(),
        KeyCode::Char('r') if k.modifiers.is_empty() => {
            control_plane_cache.invalidate();
            app.status = status::load(&mut control_plane_cache.status, status_path);
            app.replace_inbox(load_inbox(&cfg.work_dir, control_plane_cache));
            refresh_batch_telemetry(app, &cfg.work_dir);
            *last_status_reload = Instant::now();
        }
        // ---- §5/§6.2 safe command subset --------------------------------------------------
        KeyCode::Char('p') if k.modifiers.is_empty() => {
            run_pause(app, &cfg.work_dir, control_plane_cache, last_status_reload)
        }
        KeyCode::Char('u') if k.modifiers.is_empty() => {
            run_resume(app, &cfg.work_dir, control_plane_cache, last_status_reload)
        }
        // lease-status runs `state-tx.ps1 status` synchronously (a brief, read-only pwsh call);
        // the loop redraws right after, so the momentary block is acceptable for a single command.
        KeyCode::Char('s') if k.modifiers.is_empty() => {
            app.set_lease(commands::query_lease_status(&cfg.work_dir))
        }
        // `x` only ARMS force-lock (opens the confirm modal); it never removes the lock by itself.
        KeyCode::Char('x') if k.modifiers.is_empty() => app.arm_force_lock(),
        // Approval keys are intentionally scoped to Decision Inbox, so they cannot collide with
        // commands on the overview screen. Each only arms a modal; no decision fires here.
        KeyCode::Char('a') if k.modifiers.is_empty() && app.screen == Screen::DecisionInbox => {
            if !app.arm_approve() {
                app.notice = Some("нет выбранного pending approval для approve".to_string());
            }
        }
        KeyCode::Char('d') if k.modifiers.is_empty() && app.screen == Screen::DecisionInbox => {
            if !app.arm_reject() {
                app.notice = Some("нет выбранного pending approval для reject".to_string());
            }
        }
        // ---- Decision Inbox panel navigation (R-3): independent per-panel scrolling so cards
        // beyond the visible height stay reachable instead of silently clipped. ---------------
        KeyCode::Left if app.screen == Screen::DecisionInbox => app.focus_prev_inbox_panel(),
        KeyCode::Char('h') if k.modifiers.is_empty() && app.screen == Screen::DecisionInbox => {
            app.focus_prev_inbox_panel()
        }
        KeyCode::Right if app.screen == Screen::DecisionInbox => app.focus_next_inbox_panel(),
        KeyCode::Char('l') if k.modifiers.is_empty() && app.screen == Screen::DecisionInbox => {
            app.focus_next_inbox_panel()
        }
        KeyCode::Up if app.screen == Screen::DecisionInbox => {
            if app.inbox_focus == InboxPanel::Approvals {
                app.select_approval(-1);
            } else {
                app.scroll_inbox(-1);
            }
        }
        KeyCode::Char('k') if k.modifiers.is_empty() && app.screen == Screen::DecisionInbox => {
            if app.inbox_focus == InboxPanel::Approvals {
                app.select_approval(-1);
            } else {
                app.scroll_inbox(-1);
            }
        }
        KeyCode::Down if app.screen == Screen::DecisionInbox => {
            if app.inbox_focus == InboxPanel::Approvals {
                app.select_approval(1);
            } else {
                app.scroll_inbox(1);
            }
        }
        KeyCode::Char('j') if k.modifiers.is_empty() && app.screen == Screen::DecisionInbox => {
            if app.inbox_focus == InboxPanel::Approvals {
                app.select_approval(1);
            } else {
                app.scroll_inbox(1);
            }
        }
        KeyCode::PageUp if app.screen == Screen::DecisionInbox => {
            if app.inbox_focus == InboxPanel::Approvals {
                app.select_approval(-10);
            } else {
                app.scroll_inbox(-10);
            }
        }
        KeyCode::PageDown if app.screen == Screen::DecisionInbox => {
            if app.inbox_focus == InboxPanel::Approvals {
                app.select_approval(10);
            } else {
                app.scroll_inbox(10);
            }
        }
        _ => {}
    }
    false
}

fn refresh_batch_telemetry(app: &mut AppState, work: &Path) {
    let Ok(config) = config::load(work) else {
        app.cohort_token_budget = None;
        app.batch_telemetry = None;
        return;
    };
    app.cohort_token_budget = config.processor.cohort_token_budget;
    let Some(batch_id) = app.batch.as_ref().map(|batch| batch.batch_id.clone()) else {
        app.batch_telemetry = None;
        return;
    };
    app.batch_telemetry = batch_telemetry_summary_with_pricing(
        work,
        &batch_id,
        config.events_outbox,
        &config.model_pricing,
    )
    .ok();
}

/// Input while the force-lock confirmation modal is open: only an explicit confirm (`y`/`Y`/Enter)
/// removes `.work/orchestrator.lock`; `n`/Esc/anything else cancels without touching it. The
/// removal fires strictly through the [`AppState::take_force_lock_confirmation`] gate, so it is
/// impossible for a single stray keystroke to have triggered it.
fn handle_modal_key(
    app: &mut AppState,
    work_dir: &Path,
    control_plane_cache: &mut ControlPlaneCache,
    k: KeyEvent,
) {
    match app.modal {
        Modal::EnterRejectReason => match k.code {
            KeyCode::Esc => app.dismiss_modal(),
            KeyCode::Backspace if k.modifiers.is_empty() => app.pop_rejection_char(),
            KeyCode::Enter if k.modifiers.is_empty() => {
                if !app.confirm_rejection_reason() {
                    app.notice = Some("для reject укажите непустую причину".to_string());
                }
            }
            KeyCode::Char(ch) if k.modifiers.is_empty() => app.push_rejection_char(ch),
            _ => {}
        },
        Modal::ConfirmApprove | Modal::ConfirmReject => {
            if is_plain_confirmation(&k) {
                if let Some(action) = app.take_approval_confirmation() {
                    let result = commands::decide_approval(
                        work_dir,
                        &action.id,
                        action.backend,
                        action.decision,
                        action.rejection_reason.as_deref(),
                    );
                    app.notice = Some(result.summary());
                    // The selected backend may have consumed the card or found it expired/consumed
                    // by another operator. Reload immediately so no stale actionable card remains.
                    app.replace_inbox(load_inbox(work_dir, control_plane_cache));
                } else if app.notice.is_none() {
                    // Defensive fallback: AppState normally supplies the specific mismatch notice.
                    app.notice = Some("выбор approval изменился; попробуйте снова".to_string());
                }
            } else {
                app.dismiss_modal();
            }
        }
        Modal::ConfirmForceLock => {
            if is_plain_confirmation(&k) {
                if app.take_force_lock_confirmation() {
                    // force-lock routes through the single transactional `state-tx release --force`
                    // path; the structured outcome carries its own footer summary.
                    app.notice = Some(commands::force_lock(work_dir).summary());
                }
            } else {
                app.dismiss_modal();
            }
        }
        Modal::None => {}
    }
}

/// A command confirmation must be an unmodified `y`/`Y` or Enter. Terminal shortcuts such as
/// Ctrl+Y must never be able to confirm a force-lock or an irreversible approval decision.
fn is_plain_confirmation(k: &KeyEvent) -> bool {
    k.modifiers.is_empty()
        && matches!(
            k.code,
            KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter
        )
}
/// **pause** command: create `.work/PAUSE` (mirroring `cc-pause.sh`) and refresh the inbox so the
/// pause banner reflects it immediately. Any IO error is surfaced as a footer notice, not a crash.
fn run_pause(
    app: &mut AppState,
    work_dir: &Path,
    control_plane_cache: &mut ControlPlaneCache,
    last_status_reload: &mut Instant,
) {
    let now = commands::now_iso8601();
    app.notice = Some(match commands::pause(work_dir, &now) {
        Ok(_) => {
            "пауза поднята — .work/PAUSE создан (процессор остановится на границе фазы/раунда)"
                .to_string()
        }
        Err(e) => format!("не удалось поднять паузу: {e}"),
    });
    app.replace_inbox(load_inbox(work_dir, control_plane_cache));
    *last_status_reload = Instant::now();
}

/// **resume** command: remove `.work/PAUSE` (mirroring `cc-unpause.sh`, tolerant of an absent
/// file) and refresh the inbox so the banner clears immediately.
fn run_resume(
    app: &mut AppState,
    work_dir: &Path,
    control_plane_cache: &mut ControlPlaneCache,
    last_status_reload: &mut Instant,
) {
    app.notice = Some(match commands::resume(work_dir) {
        Ok(true) => "пауза снята — .work/PAUSE удалён".to_string(),
        Ok(false) => "паузы не было — .work/PAUSE отсутствует (нечего снимать)".to_string(),
        Err(e) => format!("не удалось снять паузу: {e}"),
    });
    app.replace_inbox(load_inbox(work_dir, control_plane_cache));
    *last_status_reload = Instant::now();
}

/// Build the Decision Inbox (§6.2) from the current `.work/` contents: a fresh, read-only
/// `Snapshot` (queue + task descriptors), whether `.work/PAUSE` currently exists, and the set of
/// task ids already archived to `Tasks_Done.md` (R-2: lets `inbox::build` positively confirm a
/// predecessor absent from the live snapshot is truly done, instead of silently assuming it).
/// Only the pause file's *existence* is meaningful (see `agents/processor.md`, "Пауза — kill
/// switch `.work/PAUSE`"); its content, if any, is carried through as an informational note only.
fn load_inbox(
    work_dir: &Path,
    control_plane_cache: &mut ControlPlaneCache,
) -> inbox::DecisionInbox {
    let snapshot = control_plane_cache.snapshot.load(work_dir);
    let pause_path = work_dir.join("PAUSE");
    // PAUSE deliberately bypasses the metadata cache. It is a tiny safety marker whose existence
    // is authoritative, so a fresh confined probe is preferable to trusting filesystem timestamp
    // granularity for the command that stops orchestration.
    let (paused, pause_note, pause_error) = match work_fs::entry_exists(work_dir, &pause_path) {
        Ok(false) => (false, None, None),
        Ok(true) => match work_fs::read_optional_text(work_dir, &pause_path, MAX_PAUSE_BYTES) {
            Ok(Some(text)) => (
                true,
                Some(text.trim().to_string()).filter(|text| !text.is_empty()),
                None,
            ),
            Ok(None) => (false, None, None),
            Err(error) => (
                true,
                Some("PAUSE присутствует, но не является читаемым plain-файлом".to_string()),
                Some(format!("PAUSE: {error}")),
            ),
        },
        Err(error) => (
            true,
            Some("состояние PAUSE не удалось безопасно проверить".to_string()),
            Some(format!("PAUSE: {error}")),
        ),
    };
    let done_ids = done_task_ids(work_dir, &mut control_plane_cache.done_ids);
    let mut decision_inbox = inbox::build(&snapshot, paused, pause_note, &done_ids);
    // Approval projection is intentionally not cached wholesale: pending cards become expired as
    // wall time advances even when no file metadata changes, and native approvals are validated
    // against their manifests on every refresh. Reads remain entry-count and byte bounded.
    let approvals = inbox::load_approvals(work_dir, &commands::now_iso8601());
    decision_inbox.approvals = approvals.pending;
    decision_inbox.expired_approvals = approvals.expired;
    decision_inbox.approval_errors = approvals.errors;
    if let Some(error) = pause_error {
        decision_inbox.approval_errors.push(error);
    }
    decision_inbox
}

/// Task ids already archived to `.work/Tasks_Done.md`, decoded per the SINGLE normative
/// archive-header contract by reusing `orchestrail_engine::state::archive_header_task_id` — the
/// exact same resolver `tools/queue-tx.ps1 ready` and the engine's `completed_ids` use, so one
/// archive record satisfies (or fails) a predecessor's prerequisite identically in all three
/// (T-293). This replaces this file's earlier independent copy, which accepted only `###` headers
/// and used a weak `starts_with("T-")` id check (it let a digitless `T-`/`T-abc` through).
/// Read-only, best-effort: a missing/unreadable file degrades to an empty set, matching the rest
/// of this observer's "total loading" convention (see `Snapshot::load`) — used only by
/// `inbox::build` to confirm, not to invent, a predecessor's completion (R-2).
fn done_task_ids(
    work_dir: &Path,
    cache: &mut cache::PlainFileCache<BTreeSet<String>>,
) -> BTreeSet<String> {
    let path = work_dir.join("Tasks_Done.md");
    cache
        .load_with(work_dir, &path, work_fs::MAX_CONTROL_BYTES, |text| {
            text.unwrap_or_default()
                .lines()
                .filter_map(orchestrail_engine::state::archive_header_task_id)
                .map(str::to_owned)
                .collect()
        })
        .ok()
        .cloned()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_card() -> inbox::ApprovalCard {
        inbox::ApprovalCard {
            backend: inbox::ApprovalBackend::Legacy,
            id: "apr-key".to_string(),
            subject: "task:T-250|batch:".to_string(),
            task: Some("T-250".to_string()),
            batch: None,
            reason: "human-review".to_string(),
            created_at: None,
            deadline: Some("2099-01-01T00:00:00Z".to_string()),
            fingerprint: Some("aa".to_string()),
            policy_hash: Some("bb".to_string()),
        }
    }

    #[test]
    fn approval_keys_are_scoped_to_decision_inbox_and_only_arm() {
        let mut app = AppState::new();
        app.inbox.approvals.push(pending_card());
        let cfg = Config {
            work_dir: PathBuf::from("unused"),
            tick_ms: 250,
        };
        let mut control_plane_cache = ControlPlaneCache::default();
        let mut reloaded = Instant::now();
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);

        assert!(!handle_key(
            &mut app,
            &cfg,
            Path::new("unused/status.md"),
            &mut control_plane_cache,
            &mut reloaded,
            key,
        ));
        assert_eq!(app.modal, Modal::None, "overview must ignore approve");

        app.screen = Screen::DecisionInbox;
        assert!(!handle_key(
            &mut app,
            &cfg,
            Path::new("unused/status.md"),
            &mut control_plane_cache,
            &mut reloaded,
            key,
        ));
        assert_eq!(app.modal, Modal::ConfirmApprove);
        assert!(app.take_approval_confirmation().is_some());
    }

    #[test]
    fn modified_shortcuts_do_not_mutate_or_arm_commands() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let work = std::env::temp_dir().join(format!(
            "orchestrail-tui-modified-keys-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&work).expect("create test work directory");
        let cfg = Config {
            work_dir: work.clone(),
            tick_ms: 250,
        };
        let mut app = AppState::new();
        app.screen = Screen::DecisionInbox;
        app.inbox.approvals.push(pending_card());
        let mut control_plane_cache = ControlPlaneCache::default();
        let mut reloaded = Instant::now();

        for key in ['p', 'u', 'x', 'a', 'd'] {
            assert!(!handle_key(
                &mut app,
                &cfg,
                &work.join("status.md"),
                &mut control_plane_cache,
                &mut reloaded,
                KeyEvent::new(KeyCode::Char(key), KeyModifiers::CONTROL),
            ));
        }

        assert!(!work.join("PAUSE").exists(), "Ctrl+P must not pause");
        assert_eq!(app.modal, Modal::None, "modified keys must not arm a modal");
        assert!(
            app.notice.is_none(),
            "modified keys must not issue commands"
        );
        std::fs::remove_dir_all(&work).expect("remove test work directory");
    }

    #[test]
    fn modified_confirmation_cancels_instead_of_force_releasing() {
        let mut app = AppState::new();
        let mut control_plane_cache = ControlPlaneCache::default();
        app.arm_force_lock();
        handle_modal_key(
            &mut app,
            Path::new("unused"),
            &mut control_plane_cache,
            KeyEvent::new(KeyCode::Char('y'), KeyModifiers::CONTROL),
        );
        assert_eq!(app.modal, Modal::None);
        assert!(app.notice.is_none(), "Ctrl+Y must not force-release a lock");
    }

    /// T-293: `done_task_ids` now delegates to `orchestrail_engine::state::archive_header_task_id`,
    /// so the Decision Inbox recognizes the SAME normative archive-header shapes as
    /// `tools/queue-tx.ps1 ready` and the engine's `completed_ids` — no longer just `###`, and no
    /// longer letting a digitless `T-`/`T-abc` through the old weak `starts_with("T-")` check.
    #[test]
    fn done_task_ids_matches_the_shared_archive_header_contract() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let work = std::env::temp_dir().join(format!(
            "orchestrail-tui-done-ids-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&work).expect("create test work directory");
        let archive = "# Выполненные задачи\n\n\
             ## [T-090] H2 archive entry — статус: завершена\n\
             ### [T-091] H3 archive entry — статус: завершена\n\
             # Активная задача T-092\nСостояние: завершена\n\
             # Active task T-093\n\n\
             Body mention of T-999 must not count\n\
             ### [T-] digitless header must not count\n";
        std::fs::write(work.join("Tasks_Done.md"), archive).expect("write archive fixture");

        let mut cache = cache::PlainFileCache::default();
        let ids = done_task_ids(&work, &mut cache);
        let expected: BTreeSet<String> = ["T-090", "T-091", "T-092", "T-093"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(ids, expected);

        std::fs::write(
            work.join("Tasks_Done.md"),
            format!("{archive}## [T-094] New archive entry — статус: завершена\n"),
        )
        .expect("change archive fixture");
        assert!(
            done_task_ids(&work, &mut cache).contains("T-094"),
            "changed archive metadata must invalidate the cached task ids"
        );

        std::fs::remove_dir_all(&work).expect("remove test work directory");
    }
}
