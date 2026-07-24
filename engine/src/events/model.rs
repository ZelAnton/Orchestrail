//! Typed model of one `.work/events.jsonl` envelope (contract `docs/queue_contract.md` §19).
//!
//! The event *envelope* (§19.1) is the versioned, machine-checked outer frame every line
//! shares: `schema_version`, `event_id`, `occurred_at`, `type`, `actor`, `payload`, plus the
//! optional `batch_id` / `task_id` / `payload_version`. The `payload` itself is free-form per
//! `type` (§19.3), so this crate models the envelope precisely and keeps `payload` as an opaque
//! JSON object — a reader must not need to understand every payload shape to consume the stream.
//!
//! Parsing (envelope validation, lenient-forward reading) lives in [`super::parse`]; the
//! cursor / tail reader in [`super::reader`]. This module is data + presentation only.

use serde_json::{Map, Value};

/// Envelope schema version this reader speaks (§19.1). `schema_version` grows only on an
/// incompatible envelope change; a v1 reader deliberately rejects a future major it cannot
/// vouch for (mirrors `tools/outbox.ps1`).
pub const SCHEMA_VERSION: i64 = 1;

/// The closed set of event `type`s (§19.3). Namespaced `cohort.*` / `task.*` plus the
/// `codex.attempt`, `usage.recorded`, and `operation.completed` telemetry events. Unknown types
/// are rejected on read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    CohortOpened,
    CohortRoundStarted,
    CohortRoundClosed,
    CohortAdmissionClosed,
    CohortJoinStarted,
    CohortPublished,
    CohortClosed,
    TaskCaptured,
    TaskStatusChanged,
    CodexAttempt,
    UsageRecorded,
    OperationCompleted,
}

impl EventType {
    /// The wire spelling of this type (exactly as it appears in the `"type"` field).
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::CohortOpened => "cohort.opened",
            EventType::CohortRoundStarted => "cohort.round_started",
            EventType::CohortRoundClosed => "cohort.round_closed",
            EventType::CohortAdmissionClosed => "cohort.admission_closed",
            EventType::CohortJoinStarted => "cohort.join_started",
            EventType::CohortPublished => "cohort.published",
            EventType::CohortClosed => "cohort.closed",
            EventType::TaskCaptured => "task.captured",
            EventType::TaskStatusChanged => "task.status_changed",
            EventType::CodexAttempt => "codex.attempt",
            EventType::UsageRecorded => "usage.recorded",
            EventType::OperationCompleted => "operation.completed",
        }
    }

    /// Parse a wire `type` string into a known variant, or `None` for an unknown type.
    pub fn parse(s: &str) -> Option<EventType> {
        Some(match s {
            "cohort.opened" => EventType::CohortOpened,
            "cohort.round_started" => EventType::CohortRoundStarted,
            "cohort.round_closed" => EventType::CohortRoundClosed,
            "cohort.admission_closed" => EventType::CohortAdmissionClosed,
            "cohort.join_started" => EventType::CohortJoinStarted,
            "cohort.published" => EventType::CohortPublished,
            "cohort.closed" => EventType::CohortClosed,
            "task.captured" => EventType::TaskCaptured,
            "task.status_changed" => EventType::TaskStatusChanged,
            "codex.attempt" => EventType::CodexAttempt,
            "usage.recorded" => EventType::UsageRecorded,
            "operation.completed" => EventType::OperationCompleted,
            _ => return None,
        })
    }

    /// True for the `cohort.*` family.
    pub fn is_cohort(&self) -> bool {
        self.as_str().starts_with("cohort.")
    }

    /// True for the `task.*` family.
    pub fn is_task(&self) -> bool {
        self.as_str().starts_with("task.")
    }
}

/// Who emitted the event (§19.1 `actor`): a `{kind, name}` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorKind {
    Agent,
    Human,
    Tool,
}

impl ActorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActorKind::Agent => "agent",
            ActorKind::Human => "human",
            ActorKind::Tool => "tool",
        }
    }

    pub fn parse(s: &str) -> Option<ActorKind> {
        Some(match s {
            "agent" => ActorKind::Agent,
            "human" => ActorKind::Human,
            "tool" => ActorKind::Tool,
            _ => return None,
        })
    }
}

/// The actor object: `{ "kind": agent|human|tool, "name": "<role>" }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub kind: ActorKind,
    pub name: String,
}

/// One decoded, envelope-validated event line.
///
/// Unknown future top-level fields are tolerated on read (§19.4) and intentionally NOT
/// retained here — a v1 consumer models the v1 envelope. `payload` is kept opaque.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub schema_version: i64,
    pub event_id: String,
    pub occurred_at: String,
    pub event_type: EventType,
    pub actor: Actor,
    pub batch_id: Option<String>,
    pub task_id: Option<String>,
    /// Absent `payload_version` reads as `1` (§19.1 default).
    pub payload_version: i64,
    pub payload: Map<String, Value>,
}

impl Event {
    /// Render a normalized compact JSON line for this event: the known envelope fields in a
    /// stable order (unknown-forward fields dropped, `payload_version` defaulted). Used by
    /// `engine events tail` to print one decoded event per line, and round-trippable through
    /// [`super::parse::parse_line`].
    pub fn to_json_line(&self) -> String {
        let mut obj = Map::new();
        obj.insert("schema_version".into(), Value::from(self.schema_version));
        obj.insert("event_id".into(), Value::from(self.event_id.clone()));
        obj.insert("occurred_at".into(), Value::from(self.occurred_at.clone()));
        obj.insert("type".into(), Value::from(self.event_type.as_str()));
        if let Some(b) = &self.batch_id {
            obj.insert("batch_id".into(), Value::from(b.clone()));
        }
        if let Some(t) = &self.task_id {
            obj.insert("task_id".into(), Value::from(t.clone()));
        }
        obj.insert("payload_version".into(), Value::from(self.payload_version));
        let mut actor = Map::new();
        actor.insert("kind".into(), Value::from(self.actor.kind.as_str()));
        actor.insert("name".into(), Value::from(self.actor.name.clone()));
        obj.insert("actor".into(), Value::Object(actor));
        obj.insert("payload".into(), Value::Object(self.payload.clone()));
        // `Value::Object` serialization cannot fail for these primitive contents.
        Value::Object(obj).to_string()
    }
}
