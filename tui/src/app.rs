//! Fold the `.work/events.jsonl` stream (`cohort.*` / `task.*`) into the display state for the
//! main overview screen (plan §6.1).
//!
//! This module is the *testable core* of the TUI: it is pure data + a fold function and carries
//! no terminal / ratatui dependency, so the aggregation logic can be exercised by unit tests over
//! fixture event lines (the same fixture shape as `engine/tests/events_fixture.rs`) without ever
//! opening a terminal.
//!
//! **Read-only by construction (this module).** Nothing in *this* module writes a file, takes a
//! lock, or emits an event: it only *consumes* the typed [`Event`] values handed over by the engine
//! crate's cursor reader, plus a little UI state (screen, inbox focus, the force-lock confirmation
//! modal, Event Log filters/scroll, the last command notice). The one place the crate may write is
//! the deliberately narrow
//! §5/§6.2 command channel in [`crate::commands`] (pause / resume / lease-status / force-lock /
//! approval decisions),
//! driven only by an explicit keystroke. The events are the source of truth for the
//! batch/cohort/task projection; `status.md` (see [`crate::status`]) is folded in separately as
//! human context.

use std::collections::BTreeMap;

use orchestrail_engine::events::{Event, EventType};
use orchestrail_engine::telemetry::BatchTelemetrySummary;
use serde_json::{Map, Value};

use crate::commands::{ApprovalDecision, LeaseStatus};
use crate::inbox::{ApprovalBackend, ApprovalCard, DecisionInbox};

/// Which operator screen is currently drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Screen {
    #[default]
    Overview,
    DecisionInbox,
    EventLog,
}

/// Event Log filter whose value is currently being edited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLogFilterField {
    TaskId,
    EventType,
    Cohort,
}

/// Which Decision Inbox panel currently holds focus: pending approvals plus the existing
/// escalated/quarantined/blocked panels. `←`/`→` cycles focus; `↑`/`↓` selects an approval card
/// or scrolls the other panels. Fieldless, so `as usize` indexes `AppState::inbox_scroll` in
/// declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InboxPanel {
    #[default]
    Approvals,
    Escalated,
    Quarantined,
    Blocked,
}

/// A modal overlay that captures input until dismissed. Every irreversible operation requires a
/// second explicit confirmation; rejection additionally captures a non-empty operator reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Modal {
    #[default]
    None,
    /// Force-lock armed: awaiting the explicit confirm keystroke.
    ConfirmForceLock,
    /// Approval armed: awaiting `y`/Enter.
    ConfirmApprove,
    /// Reject armed: capture a reason, then Enter advances to the confirmation step.
    EnterRejectReason,
    /// Reject reason captured: awaiting `y`/Enter before applying it.
    ConfirmReject,
}

/// A fully confirmed approval decision ready for the command channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedApproval {
    pub id: String,
    pub backend: ApprovalBackend,
    pub decision: ApprovalDecision,
    pub rejection_reason: Option<String>,
}

/// The stage a task's status maps to, for the "deviations first, green collapsed" §6.1 layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    /// A normal in-flight phase (including the automatic `разрешение конфликта` transition).
    Active,
    /// Requires human attention: escalated / merge-conflict / blocked.
    Attention,
    /// Reached the terminal healthy outcome (выполнена).
    Done,
}

/// Classify the exact projector vocabulary from `events::projector::task_status`.
///
/// Unknown strings deliberately remain active: an unrecognized future automatic transition must
/// not raise a false human-attention alarm merely because it contains an alarming substring.
pub fn classify(status: &str) -> StatusClass {
    match status.trim() {
        "конфликт" | "эскалирована" => StatusClass::Attention,
        "выполнена" => StatusClass::Done,
        "в работе"
        | "на ревью"
        | "готова к слиянию"
        | "разрешение конфликта"
        | "слита"
        | "опубликована" => StatusClass::Active,
        _ => StatusClass::Active,
    }
}

/// Where a cohort/batch is in its lifecycle, derived from the `cohort.*` family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CohortPhase {
    Opened,
    RoundStarted,
    RoundClosed,
    AdmissionClosed,
    JoinStarted,
    Published,
    Closed,
}

impl CohortPhase {
    /// A short human label for the header.
    pub fn label(self) -> &'static str {
        match self {
            CohortPhase::Opened => "когорта открыта (приём)",
            CohortPhase::RoundStarted => "волна выполняется",
            CohortPhase::RoundClosed => "волна закрыта",
            CohortPhase::AdmissionClosed => "приём закрыт",
            CohortPhase::JoinStarted => "джойн/интеграция",
            CohortPhase::Published => "опубликована",
            CohortPhase::Closed => "когорта закрыта",
        }
    }
}

/// The current batch/cohort projection.
#[derive(Debug, Clone)]
pub struct BatchState {
    pub batch_id: String,
    pub base: Option<String>,
    pub wave: Option<i64>,
    pub planned_tasks: Vec<String>,
    pub max_parallel: Option<i64>,
    pub phase: CohortPhase,
    pub opened_at: String,
    pub admission_reason: Option<String>,
    pub published_sha: Option<String>,
    pub close_stats: Option<CloseStats>,
}

/// The `cohort.closed` outcome counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseStats {
    pub merged: i64,
    pub quarantined: i64,
    pub escalated: i64,
}

/// One task's projection within the current cohort.
#[derive(Debug, Clone)]
pub struct TaskState {
    pub task_id: String,
    pub batch_id: Option<String>,
    pub level: Option<String>,
    pub branch: Option<String>,
    pub worktree: Option<String>,
    pub domain: Option<String>,
    pub status: Option<String>,
    pub wave: Option<i64>,
    /// Capture order, so the display is stable regardless of `BTreeMap` key ordering.
    pub seq: u64,
    pub last_at: String,
    pub codex_attempts: u32,
}

impl TaskState {
    pub fn class(&self) -> StatusClass {
        match &self.status {
            Some(s) => classify(s),
            None => StatusClass::Active,
        }
    }
}

/// Whether a recent notable transition is a good outcome or something needing attention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecentKind {
    Good,
    Attention,
}

/// A recently observed notable fact for the "recently completed / deviations" feed.
#[derive(Debug, Clone)]
pub struct RecentItem {
    pub at: String,
    pub label: String,
    pub kind: RecentKind,
}

const RECENT_CAP: usize = 40;
const EVENT_LOG_CAP: usize = 500;

/// The full display state, folded from the event stream (+ status.md overlay).
#[derive(Debug, Default)]
pub struct AppState {
    pub batch: Option<BatchState>,
    tasks: BTreeMap<String, TaskState>,
    /// Newest-first feed of notable transitions; survives cohort resets, bounded to `RECENT_CAP`.
    pub recent: Vec<RecentItem>,
    /// Newest-first read-only event window; survives cohort resets and is bounded independently
    /// from the smaller notable-transition feed.
    pub event_log: Vec<Event>,
    /// Exact-match Event Log filters. Empty values match every event.
    pub event_log_filter_task_id: String,
    pub event_log_filter_event_type: String,
    pub event_log_filter_cohort: String,
    /// Event Log viewport offset in rendered event rows.
    pub event_log_scroll: u16,
    /// Active filter editor and its uncommitted buffer. Editing affects only in-memory UI state.
    pub event_log_filter_input: Option<EventLogFilterField>,
    pub event_log_filter_buffer: String,
    /// Overlay parsed from `.work/status.md` (task names, orchestrator context).
    pub status: Option<crate::status::StatusSnapshot>,
    /// Total events consumed (for the header).
    pub events_seen: u64,
    /// Operator-configured post-charge token ceiling and the current batch's read-only usage/cost
    /// projection. Both are refreshed beside status.md and never influence engine decisions.
    pub cohort_token_budget: Option<u64>,
    pub batch_telemetry: Option<BatchTelemetrySummary>,
    /// `occurred_at` of the most recent event (fallback "updated" when status.md is absent).
    pub last_event_at: Option<String>,
    /// Which screen is currently drawn (§6.1 overview vs §6.2 Decision Inbox).
    pub screen: Screen,
    /// The Decision Inbox projection (§6.2), rebuilt from `engine::state::Snapshot` +
    /// `.work/PAUSE` on the same cadence as the `status.md` overlay (see `main.rs`).
    pub inbox: DecisionInbox,
    /// Which Decision Inbox panel currently holds scroll focus (R-3, see `InboxPanel`).
    pub inbox_focus: InboxPanel,
    /// Per-panel scroll offset (lines), indexed by `InboxPanel as usize` (R-3). Approval cards
    /// calculate their visual offset at render time because wrapped text has variable height.
    pub inbox_scroll: [u16; 4],
    /// Selected pending approval card; clamped whenever the inbox refreshes.
    pub approval_selected: usize,
    /// Approval id captured when an approve/reject flow is armed. The confirmation gate is bound
    /// to this immutable id rather than whichever card happens to be selected after a refresh.
    approval_modal_id: Option<String>,
    /// Backend captured with [`Self::approval_modal_id`]. Reusing an id under the other durable
    /// schema during a modal refresh invalidates the confirmation instead of switching mutators.
    approval_modal_backend: Option<ApprovalBackend>,
    /// Rejection explanation being entered in the modal.
    pub rejection_reason: String,
    /// An open modal overlay capturing input for a destructive command.
    pub modal: Modal,
    /// The most recent lease-status query result (§5 lease-status command), shown as an overlay
    /// until dismissed; `None` before the operator ever queries it.
    pub lease: Option<LeaseStatus>,
    /// A one-line result of the most recent command (pause/resume/force-lock), shown in the footer
    /// as operator feedback; `None` until the first command is issued.
    pub notice: Option<String>,
    next_seq: u64,
}

impl AppState {
    pub fn new() -> AppState {
        AppState::default()
    }

    /// Fold a batch of freshly-polled events (in file order) into the projection.
    pub fn apply_all(&mut self, events: &[Event]) {
        for ev in events {
            self.apply(ev);
        }
    }

    /// Fold one event into the projection.
    pub fn apply(&mut self, ev: &Event) {
        self.events_seen += 1;
        self.last_event_at = Some(ev.occurred_at.clone());
        self.event_log.insert(0, ev.clone());
        if self.event_log.len() > EVENT_LOG_CAP {
            self.event_log.truncate(EVENT_LOG_CAP);
        }
        match ev.event_type {
            EventType::CohortOpened => self.on_cohort_opened(ev),
            EventType::CohortRoundStarted => self.set_phase(CohortPhase::RoundStarted, ev),
            EventType::CohortRoundClosed => self.set_phase(CohortPhase::RoundClosed, ev),
            EventType::CohortAdmissionClosed => {
                self.set_phase(CohortPhase::AdmissionClosed, ev);
                if let Some(b) = self.batch.as_mut() {
                    b.admission_reason = pstr(&ev.payload, "reason");
                }
            }
            EventType::CohortJoinStarted => self.set_phase(CohortPhase::JoinStarted, ev),
            EventType::CohortPublished => self.on_cohort_published(ev),
            EventType::CohortClosed => self.on_cohort_closed(ev),
            EventType::TaskCaptured => self.on_task_captured(ev),
            EventType::TaskStatusChanged => self.on_task_status_changed(ev),
            EventType::CodexAttempt => self.on_codex_attempt(ev),
            // Deliberately inert here: usage and operation telemetry are recognized by the
            // engine's durable event-log reader and archive projection, but do not change the
            // operator control-state view.
            EventType::UsageRecorded | EventType::OperationCompleted => {}
        }
    }

    fn on_cohort_opened(&mut self, ev: &Event) {
        // A new cohort opening means the previous one is done: reset the per-cohort task view so
        // the screen shows the CURRENT batch. The `recent` feed intentionally survives.
        self.tasks.clear();
        self.next_seq = 0;
        self.batch = Some(BatchState {
            batch_id: ev.batch_id.clone().unwrap_or_default(),
            base: pstr(&ev.payload, "base"),
            wave: pi64(&ev.payload, "wave"),
            planned_tasks: pstrs(&ev.payload, "tasks"),
            max_parallel: pi64(&ev.payload, "max_parallel"),
            phase: CohortPhase::Opened,
            opened_at: ev.occurred_at.clone(),
            admission_reason: None,
            published_sha: None,
            close_stats: None,
        });
    }

    fn on_cohort_published(&mut self, ev: &Event) {
        self.set_phase(CohortPhase::Published, ev);
        if let Some(b) = self.batch.as_mut() {
            b.published_sha = pstr(&ev.payload, "main_sha");
        }
        let ci = pstr(&ev.payload, "ci").unwrap_or_else(|| "?".into());
        let batch = ev.batch_id.clone().unwrap_or_default();
        self.push_recent(
            &ev.occurred_at,
            format!("когорта {batch} опубликована (CI: {ci})"),
            RecentKind::Good,
        );
    }

    fn on_cohort_closed(&mut self, ev: &Event) {
        self.set_phase(CohortPhase::Closed, ev);
        // `cohort.closed` is an engine-authored outcome contract: native projector counters are
        // derived from terminal task states, while the TUI only presents and classifies them.
        let stats = CloseStats {
            merged: pi64(&ev.payload, "merged").unwrap_or(0),
            quarantined: pi64(&ev.payload, "quarantined").unwrap_or(0),
            escalated: pi64(&ev.payload, "escalated").unwrap_or(0),
        };
        if let Some(b) = self.batch.as_mut() {
            b.close_stats = Some(stats);
        }
        let kind = if stats.quarantined > 0 || stats.escalated > 0 {
            RecentKind::Attention
        } else {
            RecentKind::Good
        };
        let batch = ev.batch_id.clone().unwrap_or_default();
        self.push_recent(
            &ev.occurred_at,
            format!(
                "когорта {batch} закрыта (слито {}, карантин {}, эскалировано {})",
                stats.merged, stats.quarantined, stats.escalated
            ),
            kind,
        );
    }

    fn on_task_captured(&mut self, ev: &Event) {
        let task_id = match &ev.task_id {
            Some(t) => t.clone(),
            None => return,
        };
        let seq = self.alloc_seq();
        let entry = self.task_entry(&task_id, seq, &ev.occurred_at);
        entry.batch_id = ev.batch_id.clone();
        entry.level = pstr(&ev.payload, "level");
        entry.branch = pstr(&ev.payload, "branch");
        entry.worktree = pstr(&ev.payload, "worktree");
        entry.domain = pstr(&ev.payload, "domain");
        entry.wave = pi64(&ev.payload, "wave");
        // A freshly captured task is implicitly "в работе" until its first status change.
        if entry.status.is_none() {
            entry.status = Some("в работе".to_string());
        }
        entry.last_at = ev.occurred_at.clone();
    }

    fn on_task_status_changed(&mut self, ev: &Event) {
        let task_id = match &ev.task_id {
            Some(t) => t.clone(),
            None => return,
        };
        let to = pstr(&ev.payload, "to");
        let seq = self.alloc_seq();
        {
            let entry = self.task_entry(&task_id, seq, &ev.occurred_at);
            if entry.batch_id.is_none() {
                entry.batch_id = ev.batch_id.clone();
            }
            entry.status = to.clone();
            entry.last_at = ev.occurred_at.clone();
        }
        if let Some(to) = to {
            self.maybe_record_transition(ev, &task_id, &to);
        }
    }

    fn on_codex_attempt(&mut self, ev: &Event) {
        if let Some(task_id) = ev.task_id.clone() {
            let seq = self.alloc_seq();
            let entry = self.task_entry(&task_id, seq, &ev.occurred_at);
            entry.codex_attempts += 1;
        }
    }

    fn maybe_record_transition(&mut self, ev: &Event, task_id: &str, to: &str) {
        match classify(to) {
            StatusClass::Attention => self.push_recent(
                &ev.occurred_at,
                format!("{task_id} → {to}"),
                RecentKind::Attention,
            ),
            StatusClass::Done => self.push_recent(
                &ev.occurred_at,
                format!("{task_id} → {to}"),
                RecentKind::Good,
            ),
            StatusClass::Active => {
                // "опубликована" is still active but is a notable positive milestone.
                if to.trim() == "опубликована" {
                    self.push_recent(
                        &ev.occurred_at,
                        format!("{task_id} → {to}"),
                        RecentKind::Good,
                    );
                }
            }
        }
    }

    // ---- small internal helpers ------------------------------------------------------------

    fn alloc_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    /// Get or insert a task entry. `seq`/`at` are only used when a new entry is created.
    fn task_entry(&mut self, task_id: &str, seq: u64, at: &str) -> &mut TaskState {
        self.tasks
            .entry(task_id.to_string())
            .or_insert_with(|| TaskState {
                task_id: task_id.to_string(),
                batch_id: None,
                level: None,
                branch: None,
                worktree: None,
                domain: None,
                status: None,
                wave: None,
                seq,
                last_at: at.to_string(),
                codex_attempts: 0,
            })
    }

    fn set_phase(&mut self, phase: CohortPhase, _ev: &Event) {
        if let Some(b) = self.batch.as_mut() {
            b.phase = phase;
        }
    }

    fn push_recent(&mut self, at: &str, label: String, kind: RecentKind) {
        self.recent.insert(
            0,
            RecentItem {
                at: at.to_string(),
                label,
                kind,
            },
        );
        if self.recent.len() > RECENT_CAP {
            self.recent.truncate(RECENT_CAP);
        }
    }

    // ---- read-side accessors for the renderer ---------------------------------------------

    /// Tasks in a given class, in capture order.
    fn tasks_by_class(&self, class: StatusClass) -> Vec<&TaskState> {
        let mut v: Vec<&TaskState> = self.tasks.values().filter(|t| t.class() == class).collect();
        v.sort_by_key(|t| t.seq);
        v
    }

    /// Escalated / conflict / blocked tasks — shown first (§6.1 "deviations forward").
    pub fn attention_tasks(&self) -> Vec<&TaskState> {
        self.tasks_by_class(StatusClass::Attention)
    }

    /// Normal in-flight tasks and their current phase.
    pub fn active_tasks(&self) -> Vec<&TaskState> {
        self.tasks_by_class(StatusClass::Active)
    }

    /// Tasks that reached the terminal healthy outcome within the current cohort.
    pub fn done_tasks(&self) -> Vec<&TaskState> {
        self.tasks_by_class(StatusClass::Done)
    }

    /// Count of tasks needing human attention (the §6.1 "requires human" figure).
    pub fn attention_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|t| t.class() == StatusClass::Attention)
            .count()
    }

    /// Best "updated at": status.md's own timestamp if present, else the last event time.
    pub fn updated_at(&self) -> Option<String> {
        self.status
            .as_ref()
            .and_then(|s| s.updated.clone())
            .or_else(|| self.last_event_at.clone())
    }

    /// Whether `event` passes the active T-ID filter.
    pub fn filter_by_task_id(&self, event: &Event) -> bool {
        filter_matches(&self.event_log_filter_task_id, event.task_id.as_deref())
    }

    /// Whether `event` passes the active event-type filter.
    pub fn filter_by_event_type(&self, event: &Event) -> bool {
        let filter = self.event_log_filter_event_type.trim();
        filter.is_empty() || event.event_type.as_str().eq_ignore_ascii_case(filter)
    }

    /// Whether `event` passes the active cohort filter. The v1 envelope uses `batch_id` as the
    /// durable cohort coordinate; payload fallbacks keep the viewer useful for imported events.
    pub fn filter_by_cohort(&self, event: &Event) -> bool {
        filter_matches(&self.event_log_filter_cohort, Self::event_cohort_id(event))
    }

    /// Events passing all active filters, retaining the newest-first journal-window order.
    pub fn get_filtered_events(&self) -> Vec<&Event> {
        self.event_log
            .iter()
            .filter(|event| {
                self.filter_by_task_id(event)
                    && self.filter_by_event_type(event)
                    && self.filter_by_cohort(event)
            })
            .collect()
    }

    /// Resolve the event's cohort coordinate from the envelope, then known payload aliases.
    pub fn event_cohort_id(event: &Event) -> Option<&str> {
        event.batch_id.as_deref().or_else(|| {
            ["cohort_id", "cohort", "batch_id"]
                .into_iter()
                .find_map(|key| event.payload.get(key).and_then(Value::as_str))
        })
    }

    /// Begin editing one Event Log filter. Cohort editing starts from the current cohort when no
    /// cohort filter is active, making `c`, Enter a quick "current cohort" action.
    pub fn begin_event_log_filter(&mut self, field: EventLogFilterField) {
        self.event_log_filter_buffer = match field {
            EventLogFilterField::TaskId => self.event_log_filter_task_id.clone(),
            EventLogFilterField::EventType => self.event_log_filter_event_type.clone(),
            EventLogFilterField::Cohort => {
                if self.event_log_filter_cohort.is_empty() {
                    self.batch
                        .as_ref()
                        .map(|batch| batch.batch_id.clone())
                        .unwrap_or_default()
                } else {
                    self.event_log_filter_cohort.clone()
                }
            }
        };
        self.event_log_filter_input = Some(field);
    }

    pub fn push_event_log_filter_char(&mut self, ch: char) {
        if self.event_log_filter_input.is_some() && !ch.is_control() {
            self.event_log_filter_buffer.push(ch);
        }
    }

    pub fn pop_event_log_filter_char(&mut self) {
        if self.event_log_filter_input.is_some() {
            self.event_log_filter_buffer.pop();
        }
    }

    pub fn commit_event_log_filter(&mut self) {
        let Some(field) = self.event_log_filter_input.take() else {
            return;
        };
        let value = self.event_log_filter_buffer.trim().to_string();
        match field {
            EventLogFilterField::TaskId => self.event_log_filter_task_id = value,
            EventLogFilterField::EventType => self.event_log_filter_event_type = value,
            EventLogFilterField::Cohort => self.event_log_filter_cohort = value,
        }
        self.event_log_filter_buffer.clear();
        self.event_log_scroll = 0;
    }

    pub fn cancel_event_log_filter(&mut self) {
        self.event_log_filter_input = None;
        self.event_log_filter_buffer.clear();
    }

    pub fn clear_event_log_filter(&mut self, field: EventLogFilterField) {
        match field {
            EventLogFilterField::TaskId => self.event_log_filter_task_id.clear(),
            EventLogFilterField::EventType => self.event_log_filter_event_type.clear(),
            EventLogFilterField::Cohort => self.event_log_filter_cohort.clear(),
        }
        self.event_log_scroll = 0;
    }

    /// Scroll the Event Log viewport, saturating at both representable bounds.
    pub fn scroll_event_log(&mut self, delta: i16) {
        let current = i32::from(self.event_log_scroll);
        self.event_log_scroll = (current + i32::from(delta)).clamp(0, i32::from(u16::MAX)) as u16;
    }

    /// Cycle through overview, Decision Inbox, and the read-only Event Log.
    pub fn toggle_screen(&mut self) {
        self.screen = match self.screen {
            Screen::Overview => Screen::DecisionInbox,
            Screen::DecisionInbox => Screen::EventLog,
            Screen::EventLog => Screen::Overview,
        };
    }

    /// Move Decision Inbox scroll focus to the next panel (R-3; wraps around).
    pub fn focus_next_inbox_panel(&mut self) {
        self.inbox_focus = match self.inbox_focus {
            InboxPanel::Approvals => InboxPanel::Escalated,
            InboxPanel::Escalated => InboxPanel::Quarantined,
            InboxPanel::Quarantined => InboxPanel::Blocked,
            InboxPanel::Blocked => InboxPanel::Approvals,
        };
    }

    /// Move Decision Inbox scroll focus to the previous panel (R-3; wraps around).
    pub fn focus_prev_inbox_panel(&mut self) {
        self.inbox_focus = match self.inbox_focus {
            InboxPanel::Approvals => InboxPanel::Blocked,
            InboxPanel::Escalated => InboxPanel::Approvals,
            InboxPanel::Quarantined => InboxPanel::Escalated,
            InboxPanel::Blocked => InboxPanel::Quarantined,
        };
    }

    /// Scroll the currently-focused Decision Inbox panel by `delta` lines (negative scrolls up;
    /// R-3). Saturates at both representable bounds. Ratatui clips an offset past the content's
    /// end, so no content-dependent upper bound is needed, but allowing the `u16` conversion to
    /// wrap would send a user pressing Down at the end back near the top.
    pub fn scroll_inbox(&mut self, delta: i16) {
        let idx = self.inbox_focus as usize;
        let cur = i32::from(self.inbox_scroll[idx]);
        self.inbox_scroll[idx] = (cur + i32::from(delta)).clamp(0, i32::from(u16::MAX)) as u16;
    }

    /// Replace the periodically rebuilt inbox while preserving the selected approval by id when
    /// possible. A consumed/expired card disappears or moves out of `approvals`, so selection is
    /// clamped immediately rather than pointing at stale data. If that card has an active
    /// approve/reject modal, dismiss the modal before the clamped neighbour can become its target.
    pub fn replace_inbox(&mut self, inbox: DecisionInbox) {
        let selected_id = self.pending_approval().map(|card| card.id.clone());
        self.inbox = inbox;
        self.approval_selected = selected_id
            .as_deref()
            .and_then(|id| self.inbox.approvals.iter().position(|card| card.id == id))
            .unwrap_or_else(|| {
                self.approval_selected
                    .min(self.inbox.approvals.len().saturating_sub(1))
            });
        if self.approval_modal_active()
            && let Some(captured_id) = self.approval_modal_id.as_deref()
            && !self.inbox.approvals.iter().any(|card| {
                card.id == captured_id && Some(card.backend) == self.approval_modal_backend
            })
        {
            let captured_id = captured_id.to_string();
            self.dismiss_modal();
            self.notice = Some(format!(
                "approval {captured_id} больше не pending; выбор изменился, попробуйте снова"
            ));
        }
    }

    /// Currently selected actionable approval, if any.
    pub fn pending_approval(&self) -> Option<&ApprovalCard> {
        self.inbox.approvals.get(self.approval_selected)
    }

    /// Move selection among pending approval cards. Unlike other panels this changes the card
    /// selected for approve/reject instead of merely scrolling rendered lines.
    pub fn select_approval(&mut self, delta: i16) {
        if self.inbox.approvals.is_empty() {
            self.approval_selected = 0;
            return;
        }
        let max = self.inbox.approvals.len().saturating_sub(1) as i32;
        self.approval_selected =
            (self.approval_selected as i32 + i32::from(delta)).clamp(0, max) as usize;
    }

    /// Arm approval of the selected pending card. Returns false when there is no actionable card.
    pub fn arm_approve(&mut self) -> bool {
        let Some((id, backend)) = self
            .pending_approval()
            .map(|card| (card.id.clone(), card.backend))
        else {
            return false;
        };
        self.approval_modal_id = Some(id);
        self.approval_modal_backend = Some(backend);
        self.rejection_reason.clear();
        self.modal = Modal::ConfirmApprove;
        true
    }

    /// Start the reject flow for the selected pending card. The reason is entered before the
    /// separate confirmation step.
    pub fn arm_reject(&mut self) -> bool {
        let Some((id, backend)) = self
            .pending_approval()
            .map(|card| (card.id.clone(), card.backend))
        else {
            return false;
        };
        self.approval_modal_id = Some(id);
        self.approval_modal_backend = Some(backend);
        self.rejection_reason.clear();
        self.modal = Modal::EnterRejectReason;
        true
    }

    pub fn push_rejection_char(&mut self, ch: char) {
        if self.modal == Modal::EnterRejectReason && !ch.is_control() {
            self.rejection_reason.push(ch);
        }
    }

    pub fn pop_rejection_char(&mut self) {
        if self.modal == Modal::EnterRejectReason {
            self.rejection_reason.pop();
        }
    }

    /// Advance a non-empty reject explanation to the independent confirmation step.
    pub fn confirm_rejection_reason(&mut self) -> bool {
        if self.modal == Modal::EnterRejectReason && !self.rejection_reason.trim().is_empty() {
            self.rejection_reason = self.rejection_reason.trim().to_string();
            self.modal = Modal::ConfirmReject;
            true
        } else {
            false
        }
    }

    /// Consume an explicitly confirmed approve/reject modal and return the immutable command
    /// request. The selected card must still match the id captured when the flow was armed; a
    /// changed selection dismisses the modal and fails closed. A bare `y` without a previously
    /// armed modal can never produce an action.
    pub fn take_approval_confirmation(&mut self) -> Option<ConfirmedApproval> {
        let decision = match self.modal {
            Modal::ConfirmApprove => ApprovalDecision::Approve,
            Modal::ConfirmReject => ApprovalDecision::Reject,
            _ => return None,
        };
        let captured_id = self.approval_modal_id.clone();
        let captured_backend = self.approval_modal_backend;
        let selection_matches = captured_id.as_deref().is_some_and(|id| {
            self.pending_approval()
                .is_some_and(|card| card.id == id && Some(card.backend) == captured_backend)
        });
        if !selection_matches {
            self.dismiss_modal();
            self.notice = Some("выбор approval изменился; попробуйте снова".to_string());
            return None;
        }
        let id = captured_id.expect("captured approval id checked above");
        let backend = captured_backend.expect("captured approval backend checked above");
        let rejection_reason = if decision == ApprovalDecision::Reject {
            Some(self.rejection_reason.clone())
        } else {
            None
        };
        self.modal = Modal::None;
        self.approval_modal_id = None;
        self.approval_modal_backend = None;
        Some(ConfirmedApproval {
            id,
            backend,
            decision,
            rejection_reason,
        })
    }
    // ---- command channel state (§5/§6.2 safe command subset) ------------------------------

    /// Arm the destructive force-lock command: open its confirmation modal (step 1). This never
    /// removes the lock by itself — only [`AppState::take_force_lock_confirmation`], after an
    /// explicit second keystroke, does (§6.2).
    pub fn arm_force_lock(&mut self) {
        self.approval_modal_id = None;
        self.approval_modal_backend = None;
        self.modal = Modal::ConfirmForceLock;
    }

    /// Consume an armed force-lock confirmation (step 2): if the force-lock modal is currently
    /// open, close it and return `true` (the caller should now perform the removal); otherwise
    /// return `false` and do nothing. This is the confirmation *gate* — a `true` result is
    /// impossible without a prior [`AppState::arm_force_lock`], so force-lock can never fire from
    /// one stray keystroke.
    pub fn take_force_lock_confirmation(&mut self) -> bool {
        if self.modal == Modal::ConfirmForceLock {
            self.modal = Modal::None;
            true
        } else {
            false
        }
    }

    /// Dismiss any open modal without acting (Esc / n).
    pub fn dismiss_modal(&mut self) {
        self.modal = Modal::None;
        self.approval_modal_id = None;
        self.approval_modal_backend = None;
        self.rejection_reason.clear();
    }

    fn approval_modal_active(&self) -> bool {
        matches!(
            self.modal,
            Modal::ConfirmApprove | Modal::EnterRejectReason | Modal::ConfirmReject
        )
    }

    /// Whether a modal is currently capturing input.
    pub fn has_modal(&self) -> bool {
        self.modal != Modal::None
    }

    /// Record a lease-status query result to show as an overlay.
    pub fn set_lease(&mut self, status: LeaseStatus) {
        self.lease = Some(status);
    }

    /// Dismiss the lease-status overlay. Returns whether one was showing, so the caller can tell
    /// an Esc that consumed the overlay from an Esc that should fall through to quit.
    pub fn dismiss_lease(&mut self) -> bool {
        self.lease.take().is_some()
    }

    /// Friendly display name for a task: status.md's name column if we have it, else the id.
    pub fn task_name(&self, task_id: &str) -> Option<String> {
        self.status
            .as_ref()
            .and_then(|s| s.task_meta.get(task_id))
            .and_then(|m| m.name.clone())
    }
}

// ---- payload extraction helpers (opaque Map<String, Value>) --------------------------------

fn filter_matches(filter: &str, value: Option<&str>) -> bool {
    let filter = filter.trim();
    filter.is_empty() || value.is_some_and(|value| value.eq_ignore_ascii_case(filter))
}

fn pstr(p: &Map<String, Value>, key: &str) -> Option<String> {
    p.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn pi64(p: &Map<String, Value>, key: &str) -> Option<i64> {
    p.get(key).and_then(|v| v.as_i64())
}

fn pstrs(p: &Map<String, Value>, key: &str) -> Vec<String> {
    p.get(key)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrail_engine::events::parse_line;

    /// Decode fixture lines (real `.work/events.jsonl` shape) into typed events, like the reader
    /// would hand them over.
    fn events(lines: &[&str]) -> Vec<Event> {
        lines
            .iter()
            .map(|l| parse_line(l).unwrap_or_else(|e| panic!("fixture line invalid: {e}\n{l}")))
            .collect()
    }

    const OPENED: &str = r#"{"schema_version":1,"event_id":"e-open","occurred_at":"2026-07-11T11:46:29Z","type":"cohort.opened","batch_id":"B-2","actor":{"kind":"agent","name":"processor"},"payload":{"base":"deadbeef","wave":1,"tasks":["T-10","T-11"],"max_parallel":5}}"#;
    const CAP_10: &str = r#"{"schema_version":1,"event_id":"e-c10","occurred_at":"2026-07-11T11:46:59Z","type":"task.captured","batch_id":"B-2","task_id":"T-10","actor":{"kind":"agent","name":"processor"},"payload":{"level":"coder_deep","branch":"task/T-10","worktree":".work/worktrees/T-10","domain":"tui/**","wave":1}}"#;
    const CAP_11: &str = r#"{"schema_version":1,"event_id":"e-c11","occurred_at":"2026-07-11T11:47:01Z","type":"task.captured","batch_id":"B-2","task_id":"T-11","actor":{"kind":"agent","name":"processor"},"payload":{"level":"coder","branch":"task/T-11","worktree":".work/worktrees/T-11","domain":"engine/**","wave":1}}"#;
    const REVIEW_10: &str = r#"{"schema_version":1,"event_id":"e-r10","occurred_at":"2026-07-11T12:00:12Z","type":"task.status_changed","batch_id":"B-2","task_id":"T-10","actor":{"kind":"agent","name":"processor"},"payload":{"from":"в работе","to":"на ревью"}}"#;
    // Copied from `engine/tests/events_fixture.rs` so the Event Log tests exercise the same
    // journal envelope shapes as the engine's end-to-end tail fixture.
    const ENGINE_FIXTURE_A: &str = r#"{"schema_version":1,"event_id":"evt-a","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","batch_id":"B-1","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1,"tasks":["T-1"]}}"#;
    const ENGINE_FIXTURE_B: &str = r#"{"schema_version":1,"event_id":"evt-b","occurred_at":"2026-07-08T12:24:11Z","type":"task.captured","batch_id":"B-1","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"level":"coder","wave":1}}"#;

    #[test]
    fn event_log_filters_are_pure_exact_predicates_and_compose() {
        let mut app = AppState::new();
        app.apply_all(&events(&[ENGINE_FIXTURE_A, ENGINE_FIXTURE_B]));

        assert_eq!(app.get_filtered_events().len(), 2);

        app.event_log_filter_task_id = "t-1".into();
        let filtered = app.get_filtered_events();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].event_id, "evt-b");

        app.event_log_filter_task_id.clear();
        app.event_log_filter_event_type = "cohort.opened".into();
        assert_eq!(app.get_filtered_events()[0].event_id, "evt-a");

        app.event_log_filter_event_type.clear();
        app.event_log_filter_cohort = "B-1".into();
        assert_eq!(app.get_filtered_events().len(), 2);

        app.event_log_filter_task_id = "T-1".into();
        app.event_log_filter_event_type = "task.captured".into();
        assert_eq!(app.get_filtered_events()[0].event_id, "evt-b");

        app.event_log_filter_cohort = "B-missing".into();
        assert!(app.get_filtered_events().is_empty());
    }

    #[test]
    fn event_log_cohort_filter_accepts_payload_coordinate_fallback() {
        let mut event = events(&[ENGINE_FIXTURE_B]).remove(0);
        event.batch_id = None;
        event
            .payload
            .insert("cohort_id".into(), Value::String("B-payload".into()));
        let mut app = AppState::new();
        app.event_log_filter_cohort = "B-payload".into();
        assert!(app.filter_by_cohort(&event));
    }

    #[test]
    fn event_log_window_is_bounded_and_keeps_newest_events() {
        let template = events(&[ENGINE_FIXTURE_B]).remove(0);
        let mut app = AppState::new();
        for index in 0..=EVENT_LOG_CAP {
            let mut event = template.clone();
            event.event_id = format!("evt-{index}");
            app.apply(&event);
        }

        assert_eq!(app.event_log.len(), EVENT_LOG_CAP);
        assert_eq!(app.event_log.first().unwrap().event_id, "evt-500");
        assert_eq!(app.event_log.last().unwrap().event_id, "evt-1");
    }

    #[test]
    fn cohort_opened_sets_current_batch() {
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED]));
        let b = app.batch.as_ref().expect("batch set");
        assert_eq!(b.batch_id, "B-2");
        assert_eq!(b.wave, Some(1));
        assert_eq!(b.max_parallel, Some(5));
        assert_eq!(b.planned_tasks, vec!["T-10", "T-11"]);
        assert_eq!(b.phase, CohortPhase::Opened);
        assert_eq!(app.events_seen, 1);
    }

    #[test]
    fn captured_tasks_start_active_in_capture_order() {
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED, CAP_11, CAP_10]));
        let active = app.active_tasks();
        let ids: Vec<&str> = active.iter().map(|t| t.task_id.as_str()).collect();
        // capture order (T-11 then T-10), not BTreeMap key order.
        assert_eq!(ids, ["T-11", "T-10"]);
        assert_eq!(active[0].status.as_deref(), Some("в работе"));
        assert_eq!(active[0].level.as_deref(), Some("coder"));
        assert_eq!(app.attention_count(), 0);
    }

    #[test]
    fn status_change_updates_phase() {
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED, CAP_10, REVIEW_10]));
        let t = app
            .active_tasks()
            .into_iter()
            .find(|t| t.task_id == "T-10")
            .expect("T-10 active");
        assert_eq!(t.status.as_deref(), Some("на ревью"));
        assert_eq!(t.class(), StatusClass::Active);
    }

    #[test]
    fn escalation_moves_task_to_attention_and_recent() {
        let esc = r#"{"schema_version":1,"event_id":"e-esc","occurred_at":"2026-07-11T12:05:00Z","type":"task.status_changed","batch_id":"B-2","task_id":"T-11","actor":{"kind":"agent","name":"processor"},"payload":{"from":"в работе","to":"эскалирована"}}"#;
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED, CAP_10, CAP_11, esc]));
        assert_eq!(app.attention_count(), 1);
        let att = app.attention_tasks();
        assert_eq!(att.len(), 1);
        assert_eq!(att[0].task_id, "T-11");
        // active list no longer contains the escalated task.
        assert!(app.active_tasks().iter().all(|t| t.task_id != "T-11"));
        // and it shows up in the recent feed as an attention item, newest-first.
        assert_eq!(
            app.recent.first().map(|r| r.kind),
            Some(RecentKind::Attention)
        );
        assert!(app.recent[0].label.contains("T-11"));
    }

    #[test]
    fn published_and_done_land_in_recent_feed() {
        let done = r#"{"schema_version":1,"event_id":"e-done","occurred_at":"2026-07-11T12:10:00Z","type":"task.status_changed","batch_id":"B-2","task_id":"T-10","actor":{"kind":"agent","name":"processor"},"payload":{"from":"опубликована","to":"выполнена"}}"#;
        let published = r#"{"schema_version":1,"event_id":"e-pub","occurred_at":"2026-07-11T12:20:00Z","type":"cohort.published","batch_id":"B-2","actor":{"kind":"agent","name":"processor"},"payload":{"main_sha":"cafef00d","pushed":true,"tasks":["T-10","T-11"],"ci":"confirmed"}}"#;
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED, CAP_10, done, published]));
        assert_eq!(app.batch.as_ref().unwrap().phase, CohortPhase::Published);
        assert_eq!(
            app.batch.as_ref().unwrap().published_sha.as_deref(),
            Some("cafef00d")
        );
        assert_eq!(app.done_tasks().len(), 1);
        // recent, newest-first: cohort published, then the done transition.
        assert!(app.recent[0].label.contains("опубликована"));
        assert!(app.recent[0].label.contains("B-2"));
        assert!(
            app.recent
                .iter()
                .any(|r| r.label.contains("T-10 → выполнена"))
        );
        assert!(app.recent.iter().all(|r| r.kind == RecentKind::Good));
    }

    #[test]
    fn cohort_closed_classifies_engine_supplied_outcomes() {
        let good = r#"{"schema_version":1,"event_id":"e-good","occurred_at":"2026-07-11T12:30:00Z","type":"cohort.closed","batch_id":"B-2","actor":{"kind":"agent","name":"engine"},"payload":{"merged":2,"quarantined":0,"escalated":0}}"#;
        let attention = r#"{"schema_version":1,"event_id":"e-attention","occurred_at":"2026-07-11T12:31:00Z","type":"cohort.closed","batch_id":"B-2","actor":{"kind":"agent","name":"engine"},"payload":{"merged":1,"quarantined":1,"escalated":1}}"#;
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED, good]));
        let stats = app.batch.as_ref().unwrap().close_stats.unwrap();
        assert_eq!(stats.merged, 2);
        assert_eq!(app.recent[0].kind, RecentKind::Good);

        app.apply_all(&events(&[attention]));
        let stats = app.batch.as_ref().unwrap().close_stats.unwrap();
        assert_eq!(
            stats,
            CloseStats {
                merged: 1,
                quarantined: 1,
                escalated: 1
            }
        );
        // Nonzero engine-supplied quarantine/escalation counts are attention, never silent-green.
        assert_eq!(app.recent[0].kind, RecentKind::Attention);
    }

    #[test]
    fn new_cohort_opening_resets_task_view_but_keeps_recent() {
        let opened2 = r#"{"schema_version":1,"event_id":"e-open2","occurred_at":"2026-07-11T13:00:00Z","type":"cohort.opened","batch_id":"B-3","actor":{"kind":"agent","name":"processor"},"payload":{"base":"beefcafe","wave":1,"tasks":["T-20"],"max_parallel":5}}"#;
        let done = r#"{"schema_version":1,"event_id":"e-done","occurred_at":"2026-07-11T12:10:00Z","type":"task.status_changed","batch_id":"B-2","task_id":"T-10","actor":{"kind":"agent","name":"processor"},"payload":{"from":"опубликована","to":"выполнена"}}"#;
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED, CAP_10, done, opened2]));
        // the projection now reflects the NEW cohort; T-10 no longer shown.
        assert_eq!(app.batch.as_ref().unwrap().batch_id, "B-3");
        assert!(app.active_tasks().is_empty());
        assert!(app.done_tasks().is_empty());
        // but the recent feed retains the T-10 completion.
        assert!(
            app.recent
                .iter()
                .any(|r| r.label.contains("T-10 → выполнена"))
        );
    }

    #[test]
    fn codex_attempts_counted_even_before_capture() {
        let attempt = r#"{"schema_version":1,"event_id":"e-at","occurred_at":"2026-07-11T11:50:00Z","type":"codex.attempt","batch_id":"B-2","task_id":"T-10","actor":{"kind":"tool","name":"codex"},"payload":{"role":"coder","attempt_number":1}}"#;
        let mut app = AppState::new();
        app.apply_all(&events(&[OPENED, attempt, CAP_10]));
        let t = app
            .active_tasks()
            .into_iter()
            .find(|t| t.task_id == "T-10")
            .unwrap();
        assert_eq!(t.codex_attempts, 1);
        // capture after the attempt still fills in the metadata.
        assert_eq!(t.level.as_deref(), Some("coder_deep"));
    }

    #[test]
    fn inbox_panel_focus_cycles_and_wraps() {
        let mut app = AppState::new();
        assert_eq!(app.inbox_focus, InboxPanel::Approvals);
        app.focus_next_inbox_panel();
        assert_eq!(app.inbox_focus, InboxPanel::Escalated);
        app.focus_next_inbox_panel();
        assert_eq!(app.inbox_focus, InboxPanel::Quarantined);
        app.focus_next_inbox_panel();
        assert_eq!(app.inbox_focus, InboxPanel::Blocked);
        app.focus_next_inbox_panel();
        assert_eq!(app.inbox_focus, InboxPanel::Approvals);
        app.focus_prev_inbox_panel();
        assert_eq!(app.inbox_focus, InboxPanel::Blocked);
    }

    #[test]
    fn inbox_scroll_is_per_panel_and_saturates_at_zero() {
        let mut app = AppState::new();
        app.inbox_focus = InboxPanel::Escalated;
        app.scroll_inbox(5);
        assert_eq!(app.inbox_scroll[InboxPanel::Escalated as usize], 5);
        app.focus_next_inbox_panel();
        app.scroll_inbox(3);
        assert_eq!(app.inbox_scroll[InboxPanel::Quarantined as usize], 3);
        // the other panel's offset is untouched.
        assert_eq!(app.inbox_scroll[InboxPanel::Escalated as usize], 5);
        // scrolling up past 0 saturates instead of underflowing.
        app.scroll_inbox(-100);
        assert_eq!(app.inbox_scroll[InboxPanel::Quarantined as usize], 0);

        // The other end must saturate too: casting a value above `u16::MAX` used to wrap a
        // down-arrow at the end of a long panel back near the top.
        app.inbox_scroll[InboxPanel::Quarantined as usize] = u16::MAX;
        app.scroll_inbox(1);
        assert_eq!(app.inbox_scroll[InboxPanel::Quarantined as usize], u16::MAX);
    }

    fn approval(id: &str) -> ApprovalCard {
        ApprovalCard {
            backend: ApprovalBackend::Legacy,
            id: id.to_string(),
            subject: "task:T-250|batch:".to_string(),
            task: Some("T-250".to_string()),
            batch: None,
            reason: "human-review".to_string(),
            created_at: None,
            deadline: Some("2026-07-17T00:00:00Z".to_string()),
            fingerprint: Some("aa".to_string()),
            policy_hash: Some("bb".to_string()),
        }
    }

    #[test]
    fn approve_and_reject_require_explicit_confirmation() {
        let mut app = AppState::new();
        app.inbox.approvals = vec![approval("apr-a")];

        assert!(app.arm_approve());
        assert_eq!(app.modal, Modal::ConfirmApprove);
        let action = app.take_approval_confirmation().unwrap();
        assert_eq!(action.id, "apr-a");
        assert_eq!(action.backend, ApprovalBackend::Legacy);
        assert_eq!(action.decision, ApprovalDecision::Approve);
        assert!(action.rejection_reason.is_none());
        assert!(app.take_approval_confirmation().is_none());

        app.inbox.approvals.push(approval("apr-b"));
        app.inbox.approvals.push(approval("apr-c"));
        app.select_approval(2);
        assert_eq!(app.approval_selected, 2);
        app.approval_selected = 0;

        assert!(app.arm_reject());
        assert!(!app.confirm_rejection_reason());
        for ch in "неверный scope".chars() {
            app.push_rejection_char(ch);
        }
        assert!(app.confirm_rejection_reason());
        assert_eq!(app.modal, Modal::ConfirmReject);
        let action = app.take_approval_confirmation().unwrap();
        assert_eq!(action.decision, ApprovalDecision::Reject);
        assert_eq!(action.rejection_reason.as_deref(), Some("неверный scope"));
    }

    #[test]
    fn inbox_refresh_preserves_or_clamps_approval_selection() {
        let mut app = AppState::new();
        app.inbox.approvals = vec![approval("apr-a"), approval("apr-b")];
        app.approval_selected = 1;
        let refreshed = DecisionInbox {
            approvals: vec![approval("apr-b"), approval("apr-c")],
            ..DecisionInbox::default()
        };
        app.replace_inbox(refreshed);
        assert_eq!(app.pending_approval().map(|a| a.id.as_str()), Some("apr-b"));

        app.replace_inbox(DecisionInbox::default());
        assert_eq!(app.approval_selected, 0);
        assert!(app.pending_approval().is_none());
    }

    #[test]
    fn reload_during_approve_modal_does_not_confirm_clamped_neighbour() {
        let mut app = AppState::new();
        app.inbox.approvals = vec![approval("apr-a"), approval("apr-b")];
        assert!(app.arm_approve());

        app.replace_inbox(DecisionInbox {
            approvals: vec![approval("apr-b")],
            ..DecisionInbox::default()
        });

        assert_eq!(app.pending_approval().map(|a| a.id.as_str()), Some("apr-b"));
        assert_eq!(app.modal, Modal::None);
        assert!(app.take_approval_confirmation().is_none());
        assert!(
            app.notice
                .as_deref()
                .is_some_and(|notice| notice.contains("выбор изменился"))
        );
    }

    #[test]
    fn reload_during_approval_modal_rejects_the_same_id_on_another_backend() {
        let mut app = AppState::new();
        app.inbox.approvals = vec![approval("apr-a")];
        assert!(app.arm_approve());

        let mut replacement = approval("apr-a");
        replacement.backend = ApprovalBackend::Native;
        app.replace_inbox(DecisionInbox {
            approvals: vec![replacement],
            ..DecisionInbox::default()
        });

        assert_eq!(app.modal, Modal::None);
        assert!(app.take_approval_confirmation().is_none());
        assert!(
            app.notice
                .as_deref()
                .is_some_and(|notice| notice.contains("выбор изменился"))
        );
    }

    #[test]
    fn reload_during_reject_modal_does_not_apply_reason_to_clamped_neighbour() {
        let mut app = AppState::new();
        app.inbox.approvals = vec![approval("apr-a"), approval("apr-b")];
        assert!(app.arm_reject());
        for ch in "причина для apr-a".chars() {
            app.push_rejection_char(ch);
        }
        assert!(app.confirm_rejection_reason());

        app.replace_inbox(DecisionInbox {
            approvals: vec![approval("apr-b")],
            ..DecisionInbox::default()
        });

        assert_eq!(app.pending_approval().map(|a| a.id.as_str()), Some("apr-b"));
        assert_eq!(app.modal, Modal::None);
        assert!(app.take_approval_confirmation().is_none());
        assert!(app.rejection_reason.is_empty());
    }

    #[test]
    fn confirmation_rejects_live_selection_divergence() {
        let mut app = AppState::new();
        app.inbox.approvals = vec![approval("apr-a"), approval("apr-b")];
        assert!(app.arm_approve());
        app.approval_selected = 1;

        assert!(app.take_approval_confirmation().is_none());
        assert_eq!(app.modal, Modal::None);
        assert!(
            app.notice
                .as_deref()
                .is_some_and(|notice| notice.contains("выбор approval изменился"))
        );
    }

    #[test]
    fn force_lock_needs_arming_then_explicit_confirmation() {
        let mut app = AppState::new();
        // No modal by default; a bare confirmation does NOT fire — force-lock can never happen
        // from a single stray keystroke (§6.2 "не одно случайное нажатие").
        assert!(!app.has_modal());
        assert!(!app.take_force_lock_confirmation());
        // Arming opens the modal but still removes nothing.
        app.arm_force_lock();
        assert!(app.has_modal());
        assert_eq!(app.modal, Modal::ConfirmForceLock);
        // The explicit second confirmation fires exactly once and closes the modal.
        assert!(app.take_force_lock_confirmation());
        assert!(!app.has_modal());
        // A repeat confirmation after the modal closed does nothing.
        assert!(!app.take_force_lock_confirmation());
    }

    #[test]
    fn force_lock_modal_can_be_cancelled_without_firing() {
        let mut app = AppState::new();
        app.arm_force_lock();
        assert!(app.has_modal());
        app.dismiss_modal();
        assert!(!app.has_modal());
        // After cancelling, a later confirmation attempt does not fire.
        assert!(!app.take_force_lock_confirmation());
    }

    #[test]
    fn lease_overlay_set_and_dismiss() {
        let mut app = AppState::new();
        assert!(app.lease.is_none());
        // Dismissing when nothing is showing reports "nothing consumed".
        assert!(!app.dismiss_lease());
        app.set_lease(crate::commands::LeaseStatus::Absent);
        assert!(app.lease.is_some());
        assert!(app.dismiss_lease());
        assert!(app.lease.is_none());
    }

    #[test]
    fn classify_buckets() {
        assert_eq!(classify("в работе"), StatusClass::Active);
        assert_eq!(classify("на ревью"), StatusClass::Active);
        assert_eq!(classify("готова к слиянию"), StatusClass::Active);
        assert_eq!(classify("разрешение конфликта"), StatusClass::Active);
        assert_eq!(classify("слита"), StatusClass::Active);
        assert_eq!(classify("опубликована"), StatusClass::Active);
        assert_eq!(classify("выполнена"), StatusClass::Done);
        assert_eq!(classify("эскалирована"), StatusClass::Attention);
        assert_eq!(classify("конфликт"), StatusClass::Attention);
        assert_eq!(classify("предконфликтная проверка"), StatusClass::Active);
        assert_eq!(
            classify("неизвестная автоматическая фаза"),
            StatusClass::Active
        );
    }
}
