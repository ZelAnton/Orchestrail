//! Render the overview, Decision Inbox, and Event Log screens from [`AppState`].
//!
//! Layout intent (§6.1): *deviations and actions first, green normal work collapsed*. So the
//! left column leads with an "attention" panel (escalated / conflict / blocked tasks) and only
//! then the compact list of healthy active tasks; the right column shows the recently-completed
//! feed and the human context lifted from `status.md`. The header carries the current
//! batch/cohort, its phase, and the headline counts.
//!
//! This module only reads [`AppState`] and paints frames — no IO, no state mutation.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::app::{
    AppState, CohortPhase, EventLogFilterField, InboxPanel, Modal, RecentKind, Screen, TaskState,
};
use crate::commands::LeaseStatus;
use crate::inbox::{BlockedCard, DecisionInbox, EscalatedCard, QuarantineCard};

const RED: Color = Color::Red;
const YELLOW: Color = Color::Yellow;
const GREEN: Color = Color::Green;
const CYAN: Color = Color::Cyan;
const DIM: Color = Color::DarkGray;

/// Dispatch to whichever screen is currently active (§6.1 overview / §6.2 Decision Inbox), both
/// switchable from either side with the `Tab` key (see `main.rs`). Command overlays (the
/// lease-status popup and the force-lock confirmation modal) draw on top of the active screen.
pub fn render(f: &mut Frame, app: &AppState) {
    match app.screen {
        Screen::Overview => render_overview(f, app),
        Screen::DecisionInbox => render_decision_inbox(f, app),
        Screen::EventLog => render_event_log(f, app),
    }
    // Overlays, drawn over whichever screen is active. The destructive force-lock modal is drawn
    // last so it sits on top of everything, including an open lease-status popup.
    if let Some(lease) = &app.lease {
        render_lease_overlay(f, lease);
    }
    match app.modal {
        Modal::ConfirmForceLock => render_force_lock_modal(f),
        Modal::ConfirmApprove | Modal::EnterRejectReason | Modal::ConfirmReject => {
            render_approval_modal(f, app)
        }
        Modal::None => {}
    }
}

fn render_overview(f: &mut Frame, app: &AppState) {
    let root = Layout::vertical([
        Constraint::Length(6), // header (4 content lines inside the border)
        Constraint::Min(3),    // body
        Constraint::Length(1), // footer
    ])
    .split(f.area());

    render_header(f, root[0], app);

    let body =
        Layout::horizontal([Constraint::Percentage(58), Constraint::Percentage(42)]).split(root[1]);
    let left =
        Layout::vertical([Constraint::Percentage(42), Constraint::Percentage(58)]).split(body[0]);
    let right =
        Layout::vertical([Constraint::Percentage(55), Constraint::Percentage(45)]).split(body[1]);

    render_attention(f, left[0], app);
    render_active(f, left[1], app);
    render_recent(f, right[0], app);
    render_context(f, right[1], app);

    render_footer(f, root[2], app);
}

fn block(title: &str) -> Block<'_> {
    Block::default().borders(Borders::ALL).title(Span::styled(
        format!(" {title} "),
        Style::default().add_modifier(Modifier::BOLD),
    ))
}

fn render_header(f: &mut Frame, area: Rect, app: &AppState) {
    let mut lines: Vec<Line> = Vec::new();

    let updated = app.updated_at().unwrap_or_else(|| "—".to_string());
    lines.push(Line::from(vec![
        Span::styled(
            "Оркестр — обзор",
            Style::default().add_modifier(Modifier::BOLD).fg(CYAN),
        ),
        Span::raw("   обновлено: "),
        Span::styled(updated, Style::default().fg(DIM)),
    ]));

    match &app.batch {
        Some(b) => {
            let wave = b.wave.map(|w| w.to_string()).unwrap_or_else(|| "?".into());
            let cap = b
                .max_parallel
                .map(|m| m.to_string())
                .unwrap_or_else(|| "?".into());
            let mut spans = vec![
                Span::raw("Батч "),
                Span::styled(b.batch_id.clone(), Style::default().fg(CYAN)),
                Span::raw("  ·  фаза: "),
                Span::styled(b.phase.label(), Style::default().fg(phase_color(b.phase))),
                Span::raw(format!("  ·  волна {wave}  ·  cap {cap}")),
            ];
            spans.push(Span::styled(
                format!("  ·  открыт {}", short_time(&b.opened_at)),
                Style::default().fg(DIM),
            ));
            if let Some(base) = &b.base {
                let short: String = base.chars().take(8).collect();
                spans.push(Span::styled(
                    format!("  ·  база {short}"),
                    Style::default().fg(DIM),
                ));
            }
            lines.push(Line::from(spans));
        }
        None => lines.push(Line::from(Span::styled(
            "Батч: ждём первое событие cohort.opened…",
            Style::default().fg(DIM),
        ))),
    }

    let active = app.active_tasks().len();
    let attention = app.attention_count();
    let done = app.done_tasks().len();
    let planned = app
        .batch
        .as_ref()
        .map(|b| b.planned_tasks.len())
        .unwrap_or(0);
    lines.push(Line::from(vec![
        Span::styled(format!("активные {active}"), Style::default().fg(GREEN)),
        Span::raw("  ·  "),
        Span::styled(
            format!("требуют внимания {attention}"),
            Style::default().fg(if attention > 0 { RED } else { DIM }),
        ),
        Span::raw("  ·  "),
        Span::raw(format!("завершено {done}")),
        Span::raw("  ·  "),
        Span::raw(format!("в плане {planned}")),
        Span::raw("  ·  "),
        Span::styled(
            format!("событий {}", app.events_seen),
            Style::default().fg(DIM),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::raw(token_budget_label(app)),
        Span::raw("  ·  "),
        Span::styled(cost_label(app), Style::default().fg(CYAN)),
    ]));

    let para =
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(Span::styled(
            " control-plane ",
            Style::default().add_modifier(Modifier::BOLD),
        )));
    f.render_widget(para, area);
}

fn token_budget_label(app: &AppState) -> String {
    let used = match &app.batch_telemetry {
        Some(summary) => format_tokens(summary.usage.actual_tokens),
        None if app.batch.is_some() => "?".into(),
        None => "0".into(),
    };
    let incomplete = app
        .batch_telemetry
        .as_ref()
        .is_some_and(|summary| summary.usage.unmetered_events > 0);
    let suffix = if incomplete { "+" } else { "" };
    match app.cohort_token_budget {
        Some(limit) => format!("токены {used}{suffix} / {}", format_tokens(limit)),
        None => format!("токены {used}{suffix} / ∞"),
    }
}

fn cost_label(app: &AppState) -> String {
    app.batch_telemetry.as_ref().map_or_else(
        || "стоимость —".into(),
        |summary| {
            let usage_events = summary
                .usage
                .actual_events
                .saturating_add(summary.usage.estimated_events)
                .saturating_add(summary.usage.unmetered_events);
            if usage_events == 0 {
                return "стоимость —".into();
            }
            let model_note = if summary
                .cost_by_model
                .get("default")
                .is_some_and(|cost| cost.unknown)
            {
                " (модель не настроена)"
            } else {
                ""
            };
            format!(
                "стоимость {}{model_note}",
                summary.estimated_cost.format_usd()
            )
        },
    )
}

fn format_tokens(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn render_attention(f: &mut Frame, area: Rect, app: &AppState) {
    let att = app.attention_tasks();
    let has_attention = !att.is_empty();
    let title = format!("Отклонения / требуют внимания ({})", att.len());
    let mut lines: Vec<Line> = Vec::new();
    if has_attention {
        for t in att {
            lines.push(task_line(app, t, RED));
        }
    } else {
        // Green-normal state collapses to a single dim line with a calm (non-red) border (§6.1).
        lines.push(Line::from(Span::styled(
            "нет отклонений — норма",
            Style::default().fg(DIM),
        )));
    }
    let border_color = if has_attention { RED } else { DIM };
    let para =
        Paragraph::new(lines).block(block(&title).border_style(Style::default().fg(border_color)));
    f.render_widget(para, area);
}

fn render_active(f: &mut Frame, area: Rect, app: &AppState) {
    let active = app.active_tasks();
    let title = format!("Активные задачи ({})", active.len());
    let mut lines: Vec<Line> = Vec::new();
    if active.is_empty() {
        lines.push(Line::from(Span::styled(
            "нет активных задач",
            Style::default().fg(DIM),
        )));
    } else {
        for t in active {
            lines.push(task_line(app, t, GREEN));
        }
    }
    let para = Paragraph::new(lines).block(block(&title));
    f.render_widget(para, area);
}

/// One task row: `T-102  coder_deep  · фаза  — name  (attempts)`.
fn task_line<'a>(app: &AppState, t: &'a TaskState, phase_color: Color) -> Line<'a> {
    let mut spans: Vec<Span> = vec![Span::styled(
        format!("{:<7}", t.task_id),
        Style::default().add_modifier(Modifier::BOLD),
    )];
    if let Some(level) = &t.level {
        spans.push(Span::styled(
            format!("{level:<11}"),
            Style::default().fg(DIM),
        ));
    }
    if let Some(status) = &t.status {
        spans.push(Span::styled(
            format!("· {status}"),
            Style::default().fg(phase_color),
        ));
    }
    if let Some(name) = app.task_name(&t.task_id) {
        spans.push(Span::raw("  — "));
        spans.push(Span::raw(name));
    }
    if t.codex_attempts > 0 {
        spans.push(Span::styled(
            format!("  [codex×{}]", t.codex_attempts),
            Style::default().fg(YELLOW),
        ));
    }
    Line::from(spans)
}

fn render_recent(f: &mut Frame, area: Rect, app: &AppState) {
    let mut lines: Vec<Line> = Vec::new();
    if app.recent.is_empty() {
        lines.push(Line::from(Span::styled(
            "пока ничего не завершено",
            Style::default().fg(DIM),
        )));
    } else {
        for item in &app.recent {
            let color = match item.kind {
                RecentKind::Good => GREEN,
                RecentKind::Attention => RED,
            };
            let mark = match item.kind {
                RecentKind::Good => "✓",
                RecentKind::Attention => "!",
            };
            lines.push(Line::from(vec![
                Span::styled(format!("{mark} "), Style::default().fg(color)),
                Span::styled(short_time(&item.at), Style::default().fg(DIM)),
                Span::raw("  "),
                Span::styled(item.label.clone(), Style::default().fg(color)),
            ]));
        }
    }
    let para = Paragraph::new(lines).block(block("Недавно завершено / события"));
    f.render_widget(para, area);
}

fn render_context(f: &mut Frame, area: Rect, app: &AppState) {
    let mut lines: Vec<Line> = Vec::new();
    match &app.status {
        Some(s) if !s.context_lines.is_empty() => {
            for l in &s.context_lines {
                lines.push(Line::from(Span::raw(l.clone())));
            }
        }
        Some(_) => lines.push(Line::from(Span::styled(
            "status.md без контекстных строк",
            Style::default().fg(DIM),
        ))),
        None => lines.push(Line::from(Span::styled(
            "status.md недоступен — показываю только поток событий",
            Style::default().fg(DIM),
        ))),
    }
    let para = Paragraph::new(lines)
        .block(block("Контекст (status.md)"))
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn render_footer(f: &mut Frame, area: Rect, app: &AppState) {
    let inbox_count = app.inbox.card_count();
    let mut spans = vec![
        Span::styled(" q/Esc ", Style::default().fg(CYAN)),
        Span::raw("выход  "),
        Span::styled(" Tab ", Style::default().fg(CYAN)),
        Span::raw("Decision Inbox  "),
        Span::styled(" r ", Style::default().fg(CYAN)),
        Span::raw("status.md  "),
    ];
    spans.extend(command_hint_spans());
    if !app.inbox.is_empty() {
        spans.push(Span::styled(
            format!("   ⚠ требует внимания: {inbox_count}"),
            Style::default().fg(RED),
        ));
    }
    spans.extend(notice_spans(app));
    f.render_widget(
        Paragraph::new(Line::from(spans)).alignment(Alignment::Left),
        area,
    );
}

// ---- read-only Event Log screen ------------------------------------------------------------

fn render_event_log(f: &mut Frame, app: &AppState) {
    let root = Layout::vertical([
        Constraint::Length(5),
        Constraint::Min(3),
        Constraint::Length(2),
    ])
    .split(f.area());

    let filtered = app.get_filtered_events();
    render_event_log_header(f, root[0], app, filtered.len());

    let lines = if filtered.is_empty() {
        vec![Line::from(Span::styled(
            if app.event_log.is_empty() {
                "журнал пока пуст — ожидаются события events.jsonl"
            } else {
                "нет событий, соответствующих активным фильтрам"
            },
            Style::default().fg(DIM),
        ))]
    } else {
        filtered.into_iter().map(event_log_line).collect()
    };
    f.render_widget(
        Paragraph::new(lines)
            .block(block("Последние события (новые сверху)"))
            .scroll((app.event_log_scroll, 0)),
        root[1],
    );

    render_event_log_footer(f, root[2], app);
}

fn render_event_log_header(f: &mut Frame, area: Rect, app: &AppState, filtered_count: usize) {
    let mut lines = vec![Line::from(vec![
        Span::styled(
            "Event Log",
            Style::default().add_modifier(Modifier::BOLD).fg(CYAN),
        ),
        Span::raw("   показано "),
        Span::styled(
            format!("{filtered_count} / {}", app.event_log.len()),
            Style::default().fg(DIM),
        ),
    ])];
    lines.push(Line::from(vec![
        Span::raw("T-ID "),
        filter_value(&app.event_log_filter_task_id),
        Span::raw("   тип "),
        filter_value(&app.event_log_filter_event_type),
        Span::raw("   когорта "),
        filter_value(&app.event_log_filter_cohort),
    ]));
    if let Some(field) = app.event_log_filter_input {
        lines.push(Line::from(vec![
            Span::styled(
                format!("ввод {}: ", event_log_filter_label(field)),
                Style::default().fg(YELLOW),
            ),
            Span::raw(app.event_log_filter_buffer.clone()),
            Span::styled("█", Style::default().fg(YELLOW)),
            Span::styled("   Enter применить · Esc отмена", Style::default().fg(DIM)),
        ]));
    }
    f.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(Span::styled(
            " events.jsonl · read-only ",
            Style::default().add_modifier(Modifier::BOLD),
        ))),
        area,
    );
}

fn event_log_line(event: &orchestrail_engine::events::Event) -> Line<'static> {
    let type_color = if event.event_type.is_cohort() {
        CYAN
    } else if event.event_type.is_task() {
        GREEN
    } else {
        YELLOW
    };
    let mut coordinates = Vec::new();
    if let Some(task_id) = &event.task_id {
        coordinates.push(format!("task={task_id}"));
    }
    if let Some(batch_id) = &event.batch_id {
        coordinates.push(format!("batch={batch_id}"));
    }
    if let Some(cohort_id) = AppState::event_cohort_id(event)
        && event.batch_id.as_deref() != Some(cohort_id)
    {
        coordinates.push(format!("cohort={cohort_id}"));
    }
    if coordinates.is_empty() {
        coordinates.push("без координат".to_string());
    }

    Line::from(vec![
        Span::styled(
            format!("{:<23}", human_event_time(&event.occurred_at)),
            Style::default().fg(DIM),
        ),
        Span::styled(
            format!("{:<25}", event.event_type.as_str()),
            Style::default().fg(type_color),
        ),
        Span::raw(coordinates.join("  ")),
    ])
}

fn filter_value(value: &str) -> Span<'static> {
    if value.is_empty() {
        Span::styled("<все>", Style::default().fg(DIM))
    } else {
        Span::styled(value.to_string(), Style::default().fg(YELLOW))
    }
}

fn event_log_filter_label(field: EventLogFilterField) -> &'static str {
    match field {
        EventLogFilterField::TaskId => "T-ID",
        EventLogFilterField::EventType => "типа события",
        EventLogFilterField::Cohort => "когорты",
    }
}

fn render_event_log_footer(f: &mut Frame, area: Rect, app: &AppState) {
    let first = Line::from(vec![
        Span::styled(" q/Esc ", Style::default().fg(CYAN)),
        Span::raw("выход  "),
        Span::styled(" Tab ", Style::default().fg(CYAN)),
        Span::raw("обзор  "),
        Span::styled(" ↑/↓ j/k PgUp/PgDn ", Style::default().fg(CYAN)),
        Span::raw("прокрутка  "),
        Span::styled(" t/y/c ", Style::default().fg(CYAN)),
        Span::raw("фильтры  "),
        Span::styled(" Ctrl-u/y/c ", Style::default().fg(CYAN)),
        Span::raw("сброс"),
    ]);
    let mut second = command_hint_spans();
    second.extend(notice_spans(app));
    f.render_widget(Paragraph::new(vec![first, Line::from(second)]), area);
}

/// The §5/§6.2 command-key hints shared by all screens' footers: pause / resume / lease-status,
/// and the destructive force-lock (in red — it opens a confirmation modal, `main.rs`).
fn command_hint_spans() -> Vec<Span<'static>> {
    vec![
        Span::styled(" p ", Style::default().fg(CYAN)),
        Span::raw("пауза  "),
        Span::styled(" u ", Style::default().fg(CYAN)),
        Span::raw("снять  "),
        Span::styled(" s ", Style::default().fg(CYAN)),
        Span::raw("аренда  "),
        Span::styled(" x ", Style::default().fg(RED)),
        Span::raw("force-lock"),
    ]
}

/// The trailing footer segment carrying the most recent command's result notice, if any.
fn notice_spans(app: &AppState) -> Vec<Span<'static>> {
    match &app.notice {
        Some(notice) => vec![
            Span::styled("   ⟩ ", Style::default().fg(DIM)),
            Span::styled(notice.clone(), Style::default().fg(YELLOW)),
        ],
        None => Vec::new(),
    }
}

// ---- §6.2 Decision Inbox screen -------------------------------------------------------------
//
// Rendering only: cards come from `AppState::inbox` (Snapshot + PAUSE + read-only approval JSON
// projection). Input handling and every command invocation remain in `main.rs` / `commands.rs`.

fn render_decision_inbox(f: &mut Frame, app: &AppState) {
    let root = Layout::vertical([
        Constraint::Length(if app.inbox.paused { 5 } else { 3 }), // header
        Constraint::Min(3),                                       // body
        Constraint::Length(2),                                    // actions + result
    ])
    .split(f.area());

    render_inbox_header(f, root[0], &app.inbox);
    render_inbox_body(f, root[1], app);
    render_inbox_footer(f, root[2], app);
}

fn render_inbox_header(f: &mut Frame, area: Rect, inbox: &DecisionInbox) {
    let mut lines: Vec<Line> = vec![Line::from(vec![
        Span::styled(
            "Decision Inbox",
            Style::default().add_modifier(Modifier::BOLD).fg(CYAN),
        ),
        Span::raw("   "),
        Span::styled(
            format!("решения {}", inbox.approvals.len()),
            Style::default().fg(if inbox.approvals.is_empty() { DIM } else { RED }),
        ),
        Span::raw("  ·  "),
        Span::styled(
            format!("эскалировано {}", inbox.escalated.len()),
            Style::default().fg(if inbox.escalated.is_empty() { DIM } else { RED }),
        ),
        Span::raw("  ·  "),
        Span::styled(
            format!("карантин {}", inbox.quarantined.len()),
            Style::default().fg(if inbox.quarantined.is_empty() {
                DIM
            } else {
                YELLOW
            }),
        ),
        Span::raw("  ·  "),
        Span::styled(
            format!("заблокировано {}", inbox.blocked.len()),
            Style::default().fg(if inbox.blocked.is_empty() {
                DIM
            } else {
                YELLOW
            }),
        ),
    ])];
    if inbox.paused {
        let mut spans = vec![Span::styled(
            "ПАУЗА активна (.work/PAUSE) — конвейер не начнёт новую фазу/раунд",
            Style::default().fg(RED).add_modifier(Modifier::BOLD),
        )];
        if let Some(note) = &inbox.pause_note {
            spans.push(Span::styled(
                format!("  · {note}"),
                Style::default().fg(DIM),
            ));
        }
        lines.push(Line::from(spans));
    }
    let para = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(Span::styled(
                " control-plane ",
                Style::default().add_modifier(Modifier::BOLD),
            ))
            .border_style(Style::default().fg(if inbox.paused { RED } else { CYAN })),
    );
    f.render_widget(para, area);
}

/// The Decision Inbox body: either the empty-state message (R-1: distinguishes "nothing at all"
/// from "no cards, but paused" — the pause banner in the header still demands attention even
/// with an empty card list) or the four panels (approvals + the existing three), each focusable
/// with `←`/`→` and scrollable with `↑`/`↓`, so cards beyond the visible height are reachable
/// rather than silently clipped).
fn render_inbox_body(f: &mut Frame, area: Rect, app: &AppState) {
    let inbox = &app.inbox;
    if inbox.card_count() == 0 {
        let message = if inbox.paused {
            "карточек нет, но конвейер на паузе (см. баннер выше) — новая фаза/раунд не начнётся, пока пауза снята"
        } else {
            "ничего не требует решения оператора прямо сейчас"
        };
        let border = if inbox.paused { RED } else { DIM };
        let para = Paragraph::new(Line::from(Span::styled(message, Style::default().fg(DIM))))
            .block(block("Decision Inbox").border_style(Style::default().fg(border)));
        f.render_widget(para, area);
        return;
    }

    let panels = Layout::vertical([
        Constraint::Percentage(34),
        Constraint::Percentage(22),
        Constraint::Percentage(22),
        Constraint::Percentage(22),
    ])
    .split(area);

    render_approval_panel(f, panels[0], app);
    render_escalated_panel(
        f,
        panels[1],
        &inbox.escalated,
        app.inbox_focus == InboxPanel::Escalated,
        app.inbox_scroll[InboxPanel::Escalated as usize],
    );
    render_quarantine_panel(
        f,
        panels[2],
        &inbox.quarantined,
        app.inbox_focus == InboxPanel::Quarantined,
        app.inbox_scroll[InboxPanel::Quarantined as usize],
    );
    render_blocked_panel(
        f,
        panels[3],
        &inbox.blocked,
        app.inbox_focus == InboxPanel::Blocked,
        app.inbox_scroll[InboxPanel::Blocked as usize],
    );
}

/// `▶ ` prefix on a panel title when it currently holds scroll focus (see `main.rs` for the
/// `←`/`→` key handling that moves it, and `↑`/`↓` for the accompanying `scroll` offset).
fn focus_marker(focused: bool) -> &'static str {
    if focused { "▶ " } else { "" }
}

fn render_approval_panel(f: &mut Frame, area: Rect, app: &AppState) {
    let inbox = &app.inbox;
    let focused = app.inbox_focus == InboxPanel::Approvals;
    let mut lines: Vec<Line> = Vec::new();
    // Paragraph wrapping makes a card's rendered height depend on its content and terminal
    // width. Remember the selected card's logical start and translate it to physical rows below;
    // the old fixed `selected * 5` offset left later cards unreachable after a long subject or
    // reason wrapped onto extra rows.
    let mut selected_start = None;
    if inbox.approvals.is_empty() {
        lines.push(dim_line("pending approvals нет"));
    } else {
        for (index, card) in inbox.approvals.iter().enumerate() {
            let selected = index == app.approval_selected;
            if selected {
                selected_start = Some(lines.len());
            }
            lines.push(Line::from(vec![
                Span::styled(
                    if selected { "▶ " } else { "  " },
                    Style::default().fg(if selected { CYAN } else { DIM }),
                ),
                Span::styled(
                    card.id.clone(),
                    Style::default().fg(RED).add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(card.subject.clone(), Style::default().fg(Color::White)),
            ]));
            lines.push(field_line("    причина: ", &card.reason));
            lines.push(field_line(
                "    deadline: ",
                card.deadline.as_deref().unwrap_or("не указан"),
            ));
            let binding = format!(
                "fingerprint={} · policy={}",
                short_hash(card.fingerprint.as_deref()),
                short_hash(card.policy_hash.as_deref())
            );
            lines.push(field_line("    привязка: ", &binding));
            if selected {
                lines.push(Line::from(vec![
                    Span::styled(
                        "    a ",
                        Style::default().fg(GREEN).add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("approve  "),
                    Span::styled(" d ", Style::default().fg(RED).add_modifier(Modifier::BOLD)),
                    Span::raw("reject (с причиной)"),
                ]));
            } else {
                // Keep every pending card at five lines so selection -> scroll is deterministic.
                lines.push(dim_line("    ↑/↓ выбрать карточку"));
            }
        }
    }
    for card in &inbox.expired_approvals {
        lines.push(Line::from(vec![
            Span::styled("  ПРОСРОЧЕНО ", Style::default().fg(YELLOW)),
            Span::styled(card.id.clone(), Style::default().fg(DIM)),
            Span::raw("  "),
            Span::styled(card.subject.clone(), Style::default().fg(DIM)),
        ]));
        if let Some(deadline) = &card.deadline {
            lines.push(field_line("    deadline: ", deadline));
        }
    }
    for error in &inbox.approval_errors {
        lines.push(Line::from(vec![
            Span::styled("  ОШИБКА approval: ", Style::default().fg(RED)),
            Span::styled(error.clone(), Style::default().fg(YELLOW)),
        ]));
    }

    let title = format!(
        "{}Approval requests (pending {}, expired {})",
        focus_marker(focused),
        inbox.approvals.len(),
        inbox.expired_approvals.len()
    );
    let border = if inbox.approvals.is_empty() { DIM } else { RED };
    let scroll = selected_start
        .map(|start| {
            // `line_count` is run without a block over the panel's inner width, which exactly
            // matches the wrapped content passed to the final bordered paragraph below.
            Paragraph::new(lines[..start].to_vec())
                .wrap(Wrap { trim: true })
                .line_count(area.width.saturating_sub(2))
                .min(usize::from(u16::MAX)) as u16
        })
        .unwrap_or(app.inbox_scroll[InboxPanel::Approvals as usize]);
    let para = Paragraph::new(lines)
        .block(block(&title).border_style(Style::default().fg(border)))
        .wrap(Wrap { trim: true })
        .scroll((scroll, 0));
    f.render_widget(para, area);
}

fn short_hash(value: Option<&str>) -> String {
    value
        .map(|text| text.chars().take(12).collect())
        .unwrap_or_else(|| "?".to_string())
}
fn render_escalated_panel(
    f: &mut Frame,
    area: Rect,
    cards: &[EscalatedCard],
    focused: bool,
    scroll: u16,
) {
    let mut lines: Vec<Line> = Vec::new();
    if cards.is_empty() {
        lines.push(dim_line("эскалированных задач нет"));
    } else {
        for c in cards {
            lines.push(card_title_line(&c.id, &c.title, RED));
            lines.push(dim_line(
                "  требуется решение оператора — терминальное состояние, само не продолжится",
            ));
            lines.push(field_line(
                "  причина: ",
                c.reason.as_deref().unwrap_or("не указана"),
            ));
            if !c.blocks.is_empty() {
                lines.push(field_line("  разблокирует: ", &c.blocks.join(", ")));
            }
        }
    }
    let title = format!("{}Эскалировано ({})", focus_marker(focused), cards.len());
    let border = if cards.is_empty() { DIM } else { RED };
    let para = Paragraph::new(lines)
        .block(block(&title).border_style(Style::default().fg(border)))
        .wrap(Wrap { trim: true })
        .scroll((scroll, 0));
    f.render_widget(para, area);
}

fn render_quarantine_panel(
    f: &mut Frame,
    area: Rect,
    cards: &[QuarantineCard],
    focused: bool,
    scroll: u16,
) {
    let mut lines: Vec<Line> = Vec::new();
    if cards.is_empty() {
        lines.push(dim_line("нет задач в карантине"));
    } else {
        for c in cards {
            lines.push(card_title_line(&c.id, &c.title, YELLOW));
            let attempt = c
                .attempt
                .map(|a| a.to_string())
                .unwrap_or_else(|| "?".into());
            lines.push(dim_line(&format!(
                "  карантин, попытка {attempt} — вернётся в исполнение автоматически; стоит проверить причину"
            )));
            lines.push(field_line(
                "  причина: ",
                c.reason.as_deref().unwrap_or("не указана"),
            ));
            if !c.blocks.is_empty() {
                lines.push(field_line("  разблокирует: ", &c.blocks.join(", ")));
            }
        }
    }
    let title = format!(
        "{}Карантин / повторы ({})",
        focus_marker(focused),
        cards.len()
    );
    let border = if cards.is_empty() { DIM } else { YELLOW };
    let para = Paragraph::new(lines)
        .block(block(&title).border_style(Style::default().fg(border)))
        .wrap(Wrap { trim: true })
        .scroll((scroll, 0));
    f.render_widget(para, area);
}

fn render_blocked_panel(
    f: &mut Frame,
    area: Rect,
    cards: &[BlockedCard],
    focused: bool,
    scroll: u16,
) {
    let mut lines: Vec<Line> = Vec::new();
    if cards.is_empty() {
        lines.push(dim_line("нет заблокированных задач"));
    } else {
        for c in cards {
            lines.push(card_title_line(&c.id, &c.title, YELLOW));
            if c.blocking_infeasible {
                lines.push(Line::from(vec![
                    Span::styled("  блокировано: ", Style::default().fg(DIM)),
                    Span::styled(
                        format!(
                            "{} эскалирована — без решения по ней не продолжится",
                            c.blocking_on
                        ),
                        Style::default().fg(RED),
                    ),
                ]));
            } else if c.blocking_unknown {
                lines.push(Line::from(vec![
                    Span::styled("  блокировано: ", Style::default().fg(DIM)),
                    Span::styled(
                        format!(
                            "{} не найден ни в очереди, ни в Tasks_Done.md — проверьте Предпосылки:",
                            c.blocking_on
                        ),
                        Style::default().fg(RED),
                    ),
                ]));
            } else {
                lines.push(field_line("  ожидает: ", &c.blocking_on));
            }
        }
    }
    let title = format!("{}Заблокировано ({})", focus_marker(focused), cards.len());
    let border = if cards.is_empty() { DIM } else { YELLOW };
    let para = Paragraph::new(lines)
        .block(block(&title).border_style(Style::default().fg(border)))
        .wrap(Wrap { trim: true })
        .scroll((scroll, 0));
    f.render_widget(para, area);
}

fn render_inbox_footer(f: &mut Frame, area: Rect, app: &AppState) {
    let action_style = if app.pending_approval().is_some() {
        GREEN
    } else {
        DIM
    };
    let first = Line::from(vec![
        Span::styled(" q/Esc ", Style::default().fg(CYAN)),
        Span::raw("выход  "),
        Span::styled(" Tab ", Style::default().fg(CYAN)),
        Span::raw("обзор  "),
        Span::styled(" ←/→ ↑/↓ ", Style::default().fg(CYAN)),
        Span::raw("панель/карточка  "),
        Span::styled(" a ", Style::default().fg(action_style)),
        Span::raw("approve  "),
        Span::styled(
            " d ",
            Style::default().fg(if app.pending_approval().is_some() {
                RED
            } else {
                DIM
            }),
        ),
        Span::raw("reject"),
    ]);
    let mut second = command_hint_spans();
    second.extend(notice_spans(app));
    f.render_widget(
        Paragraph::new(vec![first, Line::from(second)]).alignment(Alignment::Left),
        area,
    );
}
// ---- command overlays: lease-status plus destructive-operation confirmation modals ----------

/// A centered rectangle `percent_x` × `percent_y` of `area`, for a modal/popup overlay.
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(vertical[1])[1]
}

/// The lease-status popup (§5 lease-status command): who owns `.work/orchestrator.lock` and
/// whether the lease is live, read via the engine crate's owner-checked `state-tx.ps1 status`
/// path (see `commands::query_lease_status`). Dismissed with Esc; refreshed with `s` (`main.rs`).
fn render_lease_overlay(f: &mut Frame, lease: &LeaseStatus) {
    let area = centered_rect(70, 55, f.area());
    f.render_widget(Clear, area);

    let mut lines: Vec<Line> = vec![Line::from(Span::styled(
        lease.summary(),
        Style::default().add_modifier(Modifier::BOLD),
    ))];
    if let LeaseStatus::Present(l) = lease {
        lines.push(Line::from(""));
        lines.push(field_line("  роль: ", l.role.as_deref().unwrap_or("?")));
        lines.push(field_line(
            "  владелец: ",
            l.owner_id.as_deref().unwrap_or("?"),
        ));
        if let Some(host) = &l.host {
            lines.push(field_line("  хост: ", host));
        }
        if let Some(pid) = l.pid {
            lines.push(field_line("  pid: ", &pid.to_string()));
        }
        let liveness = if l.live {
            "жива"
        } else {
            "устарела"
        };
        lines.push(Line::from(vec![
            Span::styled("  состояние: ", Style::default().fg(DIM)),
            Span::styled(
                liveness,
                Style::default().fg(if l.live { GREEN } else { YELLOW }),
            ),
        ]));
        if let Some(reason) = &l.reason {
            lines.push(field_line("  почему: ", reason));
        }
        if let (Some(age), Some(ttl)) = (l.heartbeat_age_secs, l.ttl_seconds) {
            lines.push(field_line("  heartbeat: ", &format!("{age}s / ttl {ttl}s")));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled(" s ", Style::default().fg(CYAN)),
        Span::raw("обновить   "),
        Span::styled(" Esc ", Style::default().fg(CYAN)),
        Span::raw("закрыть"),
    ]));

    let border = match lease {
        LeaseStatus::Present(l) if l.live => GREEN,
        LeaseStatus::Unavailable(_) | LeaseStatus::Degraded(_) => YELLOW,
        _ => CYAN,
    };
    let para = Paragraph::new(lines)
        .block(block("Аренда orchestrator.lock").border_style(Style::default().fg(border)))
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

/// The force-lock confirmation modal (§6.2 "опасные операции — с явным confirm"): the destructive
/// removal of `.work/orchestrator.lock`, mirroring `cc-processor.sh --force-lock`, only fires on an
/// explicit `y`/Enter here (see `main.rs::handle_modal_key`), never a single stray keystroke.
fn render_approval_modal(f: &mut Frame, app: &AppState) {
    let area = centered_rect(76, 52, f.area());
    f.render_widget(Clear, area);
    let card = app.pending_approval();
    let (title, prompt, color) = match app.modal {
        Modal::ConfirmApprove => (
            " подтверждение approve ",
            "Одобрить и навсегда потребить этот one-time approval?",
            GREEN,
        ),
        Modal::EnterRejectReason => (
            " причина reject ",
            "Введите причину отказа; Enter перейдёт к отдельному подтверждению.",
            YELLOW,
        ),
        Modal::ConfirmReject => (
            " подтверждение reject ",
            "Отклонить и навсегда потребить этот one-time approval?",
            RED,
        ),
        _ => return,
    };
    let mut lines = vec![Line::from(Span::styled(
        prompt,
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ))];
    if let Some(card) = card {
        lines.push(Line::from(""));
        lines.push(field_line("  id: ", &card.id));
        lines.push(field_line("  subject: ", &card.subject));
        lines.push(field_line("  причина запроса: ", &card.reason));
        if let Some(deadline) = &card.deadline {
            lines.push(field_line("  deadline: ", deadline));
        }
    } else {
        lines.push(Line::from(Span::styled(
            "  выбранная заявка больше не pending; отмените и обновите inbox",
            Style::default().fg(RED),
        )));
    }
    match app.modal {
        Modal::EnterRejectReason => {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("  > ", Style::default().fg(CYAN)),
                Span::raw(app.rejection_reason.clone()),
                Span::styled("█", Style::default().fg(CYAN)),
            ]));
            lines.push(Line::from(vec![
                Span::styled(" Enter ", Style::default().fg(CYAN)),
                Span::raw("далее   "),
                Span::styled(" Esc ", Style::default().fg(CYAN)),
                Span::raw("отмена"),
            ]));
        }
        Modal::ConfirmApprove | Modal::ConfirmReject => {
            if app.modal == Modal::ConfirmReject {
                lines.push(field_line("  причина отказа: ", &app.rejection_reason));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled(
                    " y / Enter ",
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::raw("подтвердить   "),
                Span::styled(" n / Esc ", Style::default().fg(CYAN)),
                Span::raw("отмена"),
            ]));
        }
        _ => {}
    }
    let para = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    title,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ))
                .border_style(Style::default().fg(color)),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}
fn render_force_lock_modal(f: &mut Frame) {
    let area = centered_rect(72, 45, f.area());
    f.render_widget(Clear, area);

    let lines = vec![
        Line::from(Span::styled(
            "Удалить .work/orchestrator.lock (force-lock)?",
            Style::default().fg(RED).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::raw(
            "Разрушительно. Делайте это ТОЛЬКО если предыдущий processor точно не работает —",
        )),
        Line::from(Span::raw(
            "иначе два управляющих цикла столкнутся на одном .work/. Зеркалит",
        )),
        Line::from(Span::raw(
            "cc-processor.sh --force-lock (rm -rf каталога замка целиком).",
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled(" y ", Style::default().fg(RED).add_modifier(Modifier::BOLD)),
            Span::raw("подтвердить удаление    "),
            Span::styled(" n / Esc ", Style::default().fg(CYAN)),
            Span::raw("отмена"),
        ]),
    ];
    let para = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    " подтверждение force-lock ",
                    Style::default().add_modifier(Modifier::BOLD).fg(RED),
                ))
                .border_style(Style::default().fg(RED)),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn card_title_line<'a>(id: &'a str, title: &'a str, color: Color) -> Line<'a> {
    Line::from(vec![
        Span::styled(
            format!("{id:<7}"),
            Style::default().add_modifier(Modifier::BOLD).fg(color),
        ),
        Span::raw(title),
    ])
}

fn dim_line(text: &str) -> Line<'static> {
    Line::from(Span::styled(text.to_string(), Style::default().fg(DIM)))
}

fn field_line(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(label.to_string(), Style::default().fg(DIM)),
        Span::raw(value.to_string()),
    ])
}

fn phase_color(phase: CohortPhase) -> Color {
    match phase {
        CohortPhase::Published | CohortPhase::Closed => GREEN,
        CohortPhase::JoinStarted => YELLOW,
        _ => CYAN,
    }
}

/// Show just the `HH:MM:SS` of an ISO-8601 `occurred_at`, best-effort (falls back to the raw
/// string). Purely cosmetic — no timezone math.
fn short_time(iso: &str) -> String {
    match (iso.find('T'), iso.find('Z')) {
        (Some(t), Some(z)) if z > t + 1 => {
            let time = &iso[t + 1..z];
            // trim optional fractional seconds
            time.split('.').next().unwrap_or(time).to_string()
        }
        _ => iso.to_string(),
    }
}

/// Render the validated UTC event timestamp as a compact, human-readable value.
fn human_event_time(iso: &str) -> String {
    let Some((date, clock)) = iso.split_once('T') else {
        return iso.to_string();
    };
    let Some(clock) = clock.strip_suffix('Z') else {
        return iso.to_string();
    };
    let clock = clock.split('.').next().unwrap_or(clock);
    format!("{date} {clock} UTC")
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrail_engine::events::parse_line;
    use orchestrail_engine::telemetry::{BatchTelemetrySummary, CostEstimate, TokenUsage};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use std::collections::BTreeMap;

    #[test]
    fn short_time_extracts_clock() {
        assert_eq!(short_time("2026-07-11T11:39:48Z"), "11:39:48");
        assert_eq!(short_time("2026-07-11T11:39:48.783Z"), "11:39:48");
        assert_eq!(short_time("garbage"), "garbage");
    }

    #[test]
    fn human_event_time_formats_validated_utc_timestamp() {
        assert_eq!(
            human_event_time("2026-07-11T11:39:48.783Z"),
            "2026-07-11 11:39:48 UTC"
        );
        assert_eq!(human_event_time("garbage"), "garbage");
    }

    #[test]
    fn budget_and_estimated_cost_share_the_compact_header_surface() {
        let mut app = AppState::new();
        app.cohort_token_budget = Some(100_000);
        app.batch_telemetry = Some(BatchTelemetrySummary {
            codex_successes: 0,
            codex_fallbacks: 0,
            codex_failures: 0,
            codex_fallback_reasons: BTreeMap::new(),
            usage: TokenUsage {
                actual_tokens: 12_345,
                estimated_tokens: 0,
                actual_events: 1,
                estimated_events: 0,
                unmetered_events: 1,
            },
            actual_by_source: BTreeMap::new(),
            estimated_cost: CostEstimate {
                nano_usd: 420_000_000,
                estimated: true,
                unknown: true,
            },
            cost_by_model: BTreeMap::new(),
            cost_by_role: BTreeMap::new(),
        });
        assert_eq!(token_budget_label(&app), "токены 12.3k+ / 100.0k");
        assert_eq!(cost_label(&app), "стоимость ≈$0.4200+?");
    }

    #[test]
    fn cost_label_distinguishes_missing_usage_from_a_measured_zero_cost() {
        let mut app = AppState::new();
        app.batch_telemetry = Some(BatchTelemetrySummary {
            codex_successes: 0,
            codex_fallbacks: 0,
            codex_failures: 0,
            codex_fallback_reasons: BTreeMap::new(),
            usage: TokenUsage {
                actual_tokens: 0,
                estimated_tokens: 0,
                actual_events: 0,
                estimated_events: 0,
                unmetered_events: 0,
            },
            actual_by_source: BTreeMap::new(),
            estimated_cost: CostEstimate::default(),
            cost_by_model: BTreeMap::new(),
            cost_by_role: BTreeMap::new(),
        });
        assert_eq!(cost_label(&app), "стоимость —");

        let summary = app.batch_telemetry.as_mut().unwrap();
        summary.usage.actual_events = 1;
        summary.estimated_cost = CostEstimate {
            nano_usd: 0,
            estimated: true,
            unknown: false,
        };
        assert_eq!(cost_label(&app), "стоимость ≈$0.0000");
    }

    #[test]
    fn cost_label_explains_an_unconfigured_default_model() {
        let mut app = AppState::new();
        let mut cost_by_model = BTreeMap::new();
        cost_by_model.insert("default".into(), CostEstimate::unknown());
        app.batch_telemetry = Some(BatchTelemetrySummary {
            codex_successes: 0,
            codex_fallbacks: 0,
            codex_failures: 0,
            codex_fallback_reasons: BTreeMap::new(),
            usage: TokenUsage {
                actual_tokens: 1,
                estimated_tokens: 0,
                actual_events: 1,
                estimated_events: 0,
                unmetered_events: 0,
            },
            actual_by_source: BTreeMap::new(),
            estimated_cost: CostEstimate::unknown(),
            cost_by_model,
            cost_by_role: BTreeMap::new(),
        });
        assert_eq!(cost_label(&app), "стоимость ≈$? (модель не настроена)");
    }

    /// Render the whole §6.1 screen to an in-memory backend (no real terminal) and assert the
    /// key facts land on screen. This exercises the full layout/paint path headlessly.
    #[test]
    fn renders_main_screen_headlessly() {
        let lines = [
            r#"{"schema_version":1,"event_id":"e1","occurred_at":"2026-07-11T11:46:29Z","type":"cohort.opened","batch_id":"B-9","actor":{"kind":"agent","name":"processor"},"payload":{"base":"abc12345","wave":1,"tasks":["T-77","T-88"],"max_parallel":5}}"#,
            r#"{"schema_version":1,"event_id":"e2","occurred_at":"2026-07-11T11:47:00Z","type":"task.captured","batch_id":"B-9","task_id":"T-77","actor":{"kind":"agent","name":"processor"},"payload":{"level":"coder_deep","branch":"task/T-77","worktree":".work/worktrees/T-77","domain":"tui/**","wave":1}}"#,
            r#"{"schema_version":1,"event_id":"e3","occurred_at":"2026-07-11T11:48:00Z","type":"task.status_changed","batch_id":"B-9","task_id":"T-88","actor":{"kind":"agent","name":"processor"},"payload":{"from":"в работе","to":"эскалирована"}}"#,
        ];
        let mut app = AppState::new();
        for l in lines {
            app.apply(&parse_line(l).unwrap());
        }
        app.status = Some(crate::status::parse(
            "- Обновлено: 2026-07-11T11:48:10Z\n\
- Оркестратор: processor — этап: исполнение когорты\n\
| T-77 | Живой TUI | coder_deep | реализация | task/T-77 | .work/worktrees/T-77 |\n",
        ));

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();

        let buf = terminal.backend().buffer();
        let screen: String = buf.content.iter().map(|c| c.symbol()).collect();

        // header + current batch
        assert!(screen.contains("Оркестр — обзор"), "missing title");
        assert!(screen.contains("B-9"), "missing batch id");
        // deviations-forward: the escalated task and the attention count are visible
        assert!(screen.contains("Отклонения"), "missing attention panel");
        assert!(screen.contains("T-88"), "escalated task not shown");
        assert!(
            screen.contains("эскалирована"),
            "escalation status not shown"
        );
        // active task + its name overlaid from status.md
        assert!(screen.contains("T-77"), "active task not shown");
        assert!(
            screen.contains("Живой TUI"),
            "status.md name overlay missing"
        );
        // recent feed reflects the escalation
        assert!(screen.contains("Недавно завершено"), "missing recent panel");
        // the command-channel key hints are on the footer (pause + the destructive force-lock)
        assert!(
            screen.contains("пауза") && screen.contains("force-lock"),
            "missing command-channel footer hints"
        );
    }

    /// Render the §6.2 Decision Inbox screen headlessly and assert every card category, the
    /// pause banner, and the command-channel footer (with the out-of-scope note) land on screen.
    #[test]
    fn renders_decision_inbox_screen_headlessly() {
        let mut app = AppState::new();
        app.screen = Screen::DecisionInbox;
        app.inbox = DecisionInbox {
            paused: true,
            pause_note: Some("оператор остановил конвейер на ночь".to_string()),
            approvals: vec![crate::inbox::ApprovalCard {
                backend: crate::inbox::ApprovalBackend::Legacy,
                id: "apr-t250".to_string(),
                subject: "task:T-250|batch:".to_string(),
                task: Some("T-250".to_string()),
                batch: None,
                reason: "human-review".to_string(),
                created_at: Some("2026-07-16T00:00:00Z".to_string()),
                deadline: Some("2026-07-17T00:00:00Z".to_string()),
                fingerprint: Some("abcdef0123456789".to_string()),
                policy_hash: Some("9876543210abcdef".to_string()),
            }],
            expired_approvals: vec![],
            approval_errors: vec![],
            escalated: vec![crate::inbox::EscalatedCard {
                id: "T-050".to_string(),
                title: "Старая задача".to_string(),
                reason: Some("INTEGRATION_LOOP_MAX".to_string()),
                blocks: vec!["T-060".to_string()],
            }],
            quarantined: vec![crate::inbox::QuarantineCard {
                id: "T-104".to_string(),
                title: "Экран Decision Inbox".to_string(),
                attempt: Some(2),
                reason: Some("merge-conflict".to_string()),
                blocks: vec![],
            }],
            blocked: vec![crate::inbox::BlockedCard {
                id: "T-060".to_string(),
                title: "Ждёт T-050".to_string(),
                blocking_on: "T-050".to_string(),
                blocking_infeasible: true,
                blocking_unknown: false,
            }],
        };

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();

        let buf = terminal.backend().buffer();
        let screen: String = buf.content.iter().map(|c| c.symbol()).collect();

        assert!(screen.contains("Decision Inbox"), "missing screen title");
        assert!(screen.contains("ПАУЗА активна"), "missing pause banner");
        assert!(screen.contains("T-050"), "escalated card not shown");
        assert!(
            screen.contains("INTEGRATION_LOOP_MAX"),
            "escalation reason not shown"
        );
        assert!(screen.contains("T-104"), "quarantine card not shown");
        assert!(
            screen.contains("merge-conflict"),
            "quarantine reason not shown"
        );
        assert!(screen.contains("T-060"), "blocked card not shown");
        assert!(screen.contains("Заблокировано"), "missing blocked panel");
        assert!(
            screen.contains("apr-t250"),
            "pending approval card not shown"
        );
        assert!(
            screen.contains("approve") && screen.contains("reject"),
            "missing approval action hints"
        );
    }

    #[test]
    fn toggle_screen_cycles_through_all_three_screens() {
        let mut app = AppState::new();
        assert_eq!(app.screen, Screen::Overview);
        app.toggle_screen();
        assert_eq!(app.screen, Screen::DecisionInbox);
        app.toggle_screen();
        assert_eq!(app.screen, Screen::EventLog);
        app.toggle_screen();
        assert_eq!(app.screen, Screen::Overview);
    }

    #[test]
    fn renders_filtered_event_log_headlessly() {
        let mut app = AppState::new();
        app.apply_all(&[
            parse_line(r#"{"schema_version":1,"event_id":"event-open","occurred_at":"2026-07-11T11:46:29Z","type":"cohort.opened","batch_id":"B-9","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1}}"#).unwrap(),
            parse_line(r#"{"schema_version":1,"event_id":"event-task","occurred_at":"2026-07-11T11:47:00Z","type":"task.captured","batch_id":"B-9","task_id":"T-77","actor":{"kind":"agent","name":"processor"},"payload":{"level":"coder"}}"#).unwrap(),
        ]);
        app.screen = Screen::EventLog;
        app.event_log_filter_task_id = "T-77".into();

        let backend = TestBackend::new(140, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, &app)).unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(screen.contains("Event Log"));
        assert!(screen.contains("2026-07-11 11:47:00 UTC"));
        assert!(screen.contains("task.captured"));
        assert!(screen.contains("task=T-77"));
        assert!(screen.contains("batch=B-9"));
        assert!(!screen.contains("cohort.opened"));
        assert!(screen.contains("Ctrl-u/y/c"));
    }

    #[test]
    fn renders_approval_confirmation_with_selected_card() {
        let mut app = AppState::new();
        app.screen = Screen::DecisionInbox;
        app.inbox.approvals = vec![crate::inbox::ApprovalCard {
            backend: crate::inbox::ApprovalBackend::Legacy,
            id: "apr-confirm".to_string(),
            subject: "task:T-250|batch:".to_string(),
            task: Some("T-250".to_string()),
            batch: None,
            reason: "human-review".to_string(),
            created_at: None,
            deadline: Some("2026-07-17T00:00:00Z".to_string()),
            fingerprint: Some("aa".to_string()),
            policy_hash: Some("bb".to_string()),
        }];
        assert!(app.arm_approve());

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(screen.contains("подтверждение approve"));
        assert!(screen.contains("apr-confirm"));
        assert!(screen.contains("y / Enter"));
    }

    #[test]
    fn wrapped_approval_before_selection_does_not_hide_selected_card() {
        let mut app = AppState::new();
        app.screen = Screen::DecisionInbox;
        app.inbox.approvals = vec![
            crate::inbox::ApprovalCard {
                backend: crate::inbox::ApprovalBackend::Legacy,
                id: "apr-long".to_string(),
                subject: "task:T-250|batch: this deliberately long approval subject wraps across several narrow terminal rows".to_string(),
                task: Some("T-250".to_string()),
                batch: None,
                reason: "this deliberately long reason must also wrap before the next approval card starts".to_string(),
                created_at: None,
                deadline: Some("2099-01-01T00:00:00Z".to_string()),
                fingerprint: Some("aa".to_string()),
                policy_hash: Some("bb".to_string()),
            },
            crate::inbox::ApprovalCard {
                backend: crate::inbox::ApprovalBackend::Legacy,
                id: "apr-selected".to_string(),
                subject: "task:T-251|batch:".to_string(),
                task: Some("T-251".to_string()),
                batch: None,
                reason: "human-review".to_string(),
                created_at: None,
                deadline: Some("2099-01-01T00:00:00Z".to_string()),
                fingerprint: Some("aa".to_string()),
                policy_hash: Some("bb".to_string()),
            },
        ];
        app.approval_selected = 1;

        let backend = TestBackend::new(60, 16);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(
            screen.contains("apr-selected"),
            "the selected approval must remain visible after a preceding wrapped card"
        );
    }
    /// The force-lock confirmation modal overlays the active screen with an explicit destructive
    /// warning and the y/cancel affordances (§6.2 confirm gate) — rendered headlessly.
    #[test]
    fn renders_force_lock_confirm_modal_headlessly() {
        let mut app = AppState::new();
        app.arm_force_lock();

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let buf = terminal.backend().buffer();
        let screen: String = buf.content.iter().map(|c| c.symbol()).collect();

        assert!(
            screen.contains("подтверждение force-lock"),
            "missing modal title"
        );
        assert!(
            screen.contains("orchestrator.lock"),
            "missing lock path in the warning"
        );
        assert!(
            screen.contains("подтвердить") && screen.contains("отмена"),
            "missing confirm/cancel affordances"
        );
    }

    /// The lease-status popup surfaces owner / role / liveness from a `commands::LeaseStatus`.
    #[test]
    fn renders_lease_overlay_headlessly() {
        let mut app = AppState::new();
        app.set_lease(crate::commands::LeaseStatus::Present(
            crate::commands::LeasePresent {
                role: Some("processor".to_string()),
                owner_id: Some("ab12cd34".to_string()),
                host: Some("HOSTA".to_string()),
                pid: Some(4321),
                live: true,
                heartbeat_age_secs: Some(12),
                ttl_seconds: Some(900),
                generation: Some(3),
                reason: Some("pid 4321 alive (start-time matches)".to_string()),
            },
        ));

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let buf = terminal.backend().buffer();
        let screen: String = buf.content.iter().map(|c| c.symbol()).collect();

        assert!(
            screen.contains("Аренда orchestrator.lock"),
            "missing lease overlay title"
        );
        assert!(screen.contains("ab12cd34"), "missing owner id");
        assert!(screen.contains("processor"), "missing role");
        assert!(screen.contains("жива"), "missing liveness label");
    }

    /// A brand-new `AppState` (no applied events, no status.md, no batch) still renders a
    /// coherent Overview screen rather than an empty or panicking one — every panel falls back to
    /// its own explanatory placeholder text.
    #[test]
    fn renders_empty_cold_start_headlessly() {
        let app = AppState::new();
        assert_eq!(app.screen, Screen::Overview);

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();

        let buf = terminal.backend().buffer();
        let screen: String = buf.content.iter().map(|c| c.symbol()).collect();

        assert!(
            screen.contains("Батч: ждём первое событие cohort.opened…"),
            "missing cold-start batch placeholder"
        );
        assert!(
            screen.contains("нет активных задач"),
            "missing empty active-tasks placeholder"
        );
        assert!(
            screen.contains("пока ничего не завершено"),
            "missing empty recent placeholder"
        );
    }

    /// The `Modal::EnterRejectReason` step: after arming a reject on a pending card and typing a
    /// non-empty reason, the modal shows the reason-entry title, the typed text, a text cursor,
    /// and the "далее"/"отмена" hints (§6.2 confirm gate, step 1 of 2).
    #[test]
    fn renders_reject_reason_entry_modal_headlessly() {
        let mut app = AppState::new();
        app.screen = Screen::DecisionInbox;
        app.inbox.approvals = vec![crate::inbox::ApprovalCard {
            backend: crate::inbox::ApprovalBackend::Legacy,
            id: "apr-reject".to_string(),
            subject: "task:T-250|batch:".to_string(),
            task: Some("T-250".to_string()),
            batch: None,
            reason: "human-review".to_string(),
            created_at: None,
            deadline: Some("2099-01-01T00:00:00Z".to_string()),
            fingerprint: Some("aa".to_string()),
            policy_hash: Some("bb".to_string()),
        }];
        assert!(app.arm_reject());
        for ch in "неверный scope".chars() {
            app.push_rejection_char(ch);
        }

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(screen.contains(" причина reject "), "missing modal title");
        assert!(screen.contains("█"), "missing text cursor");
        assert!(
            screen.contains("неверный scope"),
            "missing typed reject reason"
        );
        assert!(screen.contains("далее"), "missing next-step hint");
        assert!(screen.contains("отмена"), "missing cancel hint");
    }

    /// The `Modal::ConfirmReject` step: after confirming a non-empty reject reason, the modal
    /// shows the confirmation title, the previously entered reason, and the confirm/cancel hints
    /// (§6.2 confirm gate, step 2 of 2).
    #[test]
    fn renders_reject_confirmation_modal_headlessly() {
        let mut app = AppState::new();
        app.screen = Screen::DecisionInbox;
        app.inbox.approvals = vec![crate::inbox::ApprovalCard {
            backend: crate::inbox::ApprovalBackend::Legacy,
            id: "apr-reject".to_string(),
            subject: "task:T-250|batch:".to_string(),
            task: Some("T-250".to_string()),
            batch: None,
            reason: "human-review".to_string(),
            created_at: None,
            deadline: Some("2099-01-01T00:00:00Z".to_string()),
            fingerprint: Some("aa".to_string()),
            policy_hash: Some("bb".to_string()),
        }];
        assert!(app.arm_reject());
        for ch in "неверный scope".chars() {
            app.push_rejection_char(ch);
        }
        assert!(app.confirm_rejection_reason());

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(
            screen.contains(" подтверждение reject "),
            "missing modal title"
        );
        assert!(
            screen.contains("причина отказа: ") && screen.contains("неверный scope"),
            "missing previously entered reject reason"
        );
        assert!(screen.contains("y / Enter"), "missing confirm hint");
        assert!(screen.contains("подтвердить"), "missing confirm label");
        assert!(screen.contains("отмена"), "missing cancel hint");
    }

    /// Decision Inbox with both a pending and an expired approval: the expired card is marked
    /// "ПРОСРОЧЕНО" and shown alongside (not instead of) the still-pending card, and the panel
    /// title counts both categories independently (§6.2 "pending/expired approvals").
    #[test]
    fn renders_decision_inbox_with_expired_approval_headlessly() {
        let mut app = AppState::new();
        app.screen = Screen::DecisionInbox;
        app.inbox.approvals = vec![crate::inbox::ApprovalCard {
            backend: crate::inbox::ApprovalBackend::Legacy,
            id: "apr-pending".to_string(),
            subject: "task:T-251|batch:".to_string(),
            task: Some("T-251".to_string()),
            batch: None,
            reason: "human-review".to_string(),
            created_at: None,
            deadline: Some("2099-01-01T00:00:00Z".to_string()),
            fingerprint: Some("aa".to_string()),
            policy_hash: Some("bb".to_string()),
        }];
        app.inbox.expired_approvals = vec![crate::inbox::ApprovalCard {
            backend: crate::inbox::ApprovalBackend::Legacy,
            id: "apr-expired".to_string(),
            subject: "task:T-252|batch:".to_string(),
            task: Some("T-252".to_string()),
            batch: None,
            reason: "human-review".to_string(),
            created_at: None,
            deadline: Some("2020-01-01T00:00:00Z".to_string()),
            fingerprint: Some("cc".to_string()),
            policy_hash: Some("dd".to_string()),
        }];

        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(screen.contains("ПРОСРОЧЕНО"), "missing expired marker");
        assert!(screen.contains("apr-expired"), "missing expired card id");
        assert!(
            screen.contains("task:T-252|batch:"),
            "missing expired card subject"
        );
        assert!(
            screen.contains("pending 1, expired 1)"),
            "panel title should count pending and expired independently"
        );
        assert!(
            screen.contains("apr-pending"),
            "pending card must still be visible alongside the expired one"
        );
    }
}
