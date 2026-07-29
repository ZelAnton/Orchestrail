//! Decode + validate ONE `.work/events.jsonl` line into a typed [`Event`] (§19.1, §19.4).
//!
//! Read validation is **strict on the envelope, lenient forward on the future** (§19.4):
//!
//! * Required fields and their formats are checked: `schema_version` (this reader's major),
//!   `event_id` (non-empty, whitespace-free token — a UUIDv5, a `evt-` fallback id, or any
//!   opaque id), `occurred_at` (ISO-8601 UTC ending in `Z`), a known `type`, an `actor`
//!   object with a known `kind` and a non-empty `name`, and an object `payload`. `batch_id`
//!   (`^B-`) / `task_id` (`^T-\d`) / `payload_version` (int ≥1) are checked *when present*.
//! * Unknown future top-level fields and a missing `payload_version` are **tolerated** — so a
//!   line written by a newer emitter still reads, and history never needs rewriting.
//! * A broken / malformed envelope is **rejected with a clear error, never a panic**.
//!
//! Unlike the writer (`tools/outbox.ps1`, strict mode), the reader does NOT reject unknown
//! top-level keys, absolute paths, or a non-allowlisted `codex.attempt` payload: those are
//! write-time privacy/shape guards, not read-time envelope invariants.

use serde_json::Value;

use crate::task_id::is_task_id;

use super::model::{Actor, ActorKind, Event, EventType, SCHEMA_VERSION};

/// A human-readable reason one line could not be decoded into a valid [`Event`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
}

impl ParseError {
    fn new(msg: impl Into<String>) -> ParseError {
        ParseError {
            message: msg.into(),
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseError {}

/// Decode and envelope-validate one line. `line` is a single JSON object (no embedded
/// newline). Returns a typed [`Event`] or a [`ParseError`] describing the first violation.
pub fn parse_line(line: &str) -> Result<Event, ParseError> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(ParseError::new("empty line"));
    }
    let value: Value = serde_json::from_str(trimmed)
        .map_err(|e| ParseError::new(format!("not valid JSON: {e}")))?;
    let obj = value
        .as_object()
        .ok_or_else(|| ParseError::new("event must be a JSON object"))?;

    // schema_version: required integer, must be the major this reader speaks.
    let schema_version = require_int(obj.get("schema_version"), "schema_version")?;
    if schema_version != SCHEMA_VERSION {
        return Err(ParseError::new(format!(
            "unsupported schema_version {schema_version} (this reader speaks {SCHEMA_VERSION})"
        )));
    }

    // event_id: required, non-empty, whitespace-free token.
    let event_id = require_str(obj.get("event_id"), "event_id")?;
    if event_id.is_empty() || event_id.chars().any(char::is_whitespace) {
        return Err(ParseError::new(
            "event_id must be a non-empty whitespace-free token",
        ));
    }

    // occurred_at: required, ISO-8601 UTC ending in Z. Validated by the crate's single strict
    // matcher (`crate::time::is_iso_utc`), shared with the freshness gate in `contract.rs`.
    let occurred_at = require_str(obj.get("occurred_at"), "occurred_at")?;
    if !crate::time::is_iso_utc(occurred_at) {
        return Err(ParseError::new(format!(
            "occurred_at '{occurred_at}' is not ISO-8601 UTC (…Z)"
        )));
    }

    // type: required, must be a known type.
    let type_str = require_str(obj.get("type"), "type")?;
    let event_type = EventType::parse(type_str)
        .ok_or_else(|| ParseError::new(format!("unknown type '{type_str}'")))?;

    // actor: required object { kind, name }.
    let actor = parse_actor(obj.get("actor"))?;

    // payload: required object.
    let payload = obj
        .get("payload")
        .ok_or_else(|| ParseError::new("missing required field 'payload'"))?;
    let payload = payload
        .as_object()
        .ok_or_else(|| ParseError::new("payload must be an object"))?
        .clone();

    // Optional envelope fields — validated only when present.
    let batch_id = match obj.get("batch_id") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| ParseError::new("batch_id must be a string"))?;
            if !s.starts_with("B-") {
                return Err(ParseError::new(format!(
                    "batch_id '{s}' does not look like a B-id"
                )));
            }
            Some(s.to_string())
        }
    };
    let task_id = match obj.get("task_id") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| ParseError::new("task_id must be a string"))?;
            if !is_task_coordinate(event_type, s) {
                return Err(ParseError::new(format!(
                    "task_id '{s}' is not a T-id or documented usage pseudo-id for {type_str}"
                )));
            }
            Some(s.to_string())
        }
    };
    let payload_version = match obj.get("payload_version") {
        None | Some(Value::Null) => 1,
        Some(v) => {
            let n = v
                .as_i64()
                .filter(|_| v.is_i64() || v.is_u64())
                .ok_or_else(|| ParseError::new("payload_version must be an integer"))?;
            if n < 1 {
                return Err(ParseError::new(
                    "payload_version must be a positive integer",
                ));
            }
            n
        }
    };

    Ok(Event {
        schema_version,
        event_id: event_id.to_string(),
        occurred_at: occurred_at.to_string(),
        event_type,
        actor,
        batch_id,
        task_id,
        payload_version,
        payload,
    })
}

fn parse_actor(v: Option<&Value>) -> Result<Actor, ParseError> {
    let actor = v.ok_or_else(|| ParseError::new("missing required field 'actor'"))?;
    let actor = actor
        .as_object()
        .ok_or_else(|| ParseError::new("actor must be an object"))?;
    let kind_str = require_str(actor.get("kind"), "actor.kind")?;
    let kind = ActorKind::parse(kind_str).ok_or_else(|| {
        ParseError::new(format!("actor.kind '{kind_str}' is not agent/human/tool"))
    })?;
    let name = require_str(actor.get("name"), "actor.name")?;
    if name.is_empty() {
        return Err(ParseError::new("actor.name is required"));
    }
    Ok(Actor {
        kind,
        name: name.to_string(),
    })
}

fn require_str<'a>(v: Option<&'a Value>, field: &str) -> Result<&'a str, ParseError> {
    match v {
        None | Some(Value::Null) => {
            Err(ParseError::new(format!("missing required field '{field}'")))
        }
        Some(v) => v
            .as_str()
            .ok_or_else(|| ParseError::new(format!("{field} must be a string"))),
    }
}

fn require_int(v: Option<&Value>, field: &str) -> Result<i64, ParseError> {
    match v {
        None | Some(Value::Null) => {
            Err(ParseError::new(format!("missing required field '{field}'")))
        }
        Some(v) => v
            .as_i64()
            .filter(|_| v.is_i64() || v.is_u64())
            .ok_or_else(|| ParseError::new(format!("{field} must be an integer"))),
    }
}

fn is_task_coordinate(event_type: EventType, value: &str) -> bool {
    is_task_id(value)
        || (event_type == EventType::UsageRecorded
            && matches!(value, "_cohort" | "_integration" | "_release"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A real-format line copied from `.work/events.jsonl` (see task: real lines may be used
    // as the fixture's format oracle).
    const REAL_OPENED: &str = r#"{"schema_version":1,"event_id":"evt-20260708T122410Z-15286","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","batch_id":"B-20260708T121913Z","actor":{"kind":"agent","name":"processor"},"payload":{"base":"0c55b21a","wave":1,"tasks":["T-039"],"max_parallel":5}}"#;
    const REAL_STATUS: &str = r#"{"schema_version":1,"event_id":"evt-20260708T122747Z-30924","occurred_at":"2026-07-08T12:27:47Z","type":"task.status_changed","batch_id":"B-20260708T121913Z","task_id":"T-042","actor":{"kind":"agent","name":"processor"},"payload":{"from":"в работе","to":"на ревью"}}"#;

    #[test]
    fn parses_a_real_cohort_line() {
        let ev = parse_line(REAL_OPENED).expect("valid");
        assert_eq!(ev.schema_version, 1);
        assert_eq!(ev.event_id, "evt-20260708T122410Z-15286");
        assert_eq!(ev.event_type, EventType::CohortOpened);
        assert_eq!(ev.actor.kind, ActorKind::Agent);
        assert_eq!(ev.actor.name, "processor");
        assert_eq!(ev.batch_id.as_deref(), Some("B-20260708T121913Z"));
        assert_eq!(ev.task_id, None);
        assert_eq!(ev.payload_version, 1); // absent -> default 1
        assert_eq!(ev.payload.get("wave").and_then(|v| v.as_i64()), Some(1));
    }

    #[test]
    fn parses_a_real_status_change_with_task_id() {
        let ev = parse_line(REAL_STATUS).expect("valid");
        assert_eq!(ev.event_type, EventType::TaskStatusChanged);
        assert_eq!(ev.task_id.as_deref(), Some("T-042"));
        assert_eq!(
            ev.payload.get("to").and_then(|v| v.as_str()),
            Some("на ревью")
        );
    }

    #[test]
    fn usage_accepts_only_the_documented_pseudo_task_coordinates() {
        let usage = r#"{"schema_version":1,"event_id":"usage-1","occurred_at":"2026-07-25T12:00:00Z","type":"usage.recorded","batch_id":"B-1","task_id":"_integration","actor":{"kind":"tool","name":"claude"},"payload":{"usage_availability":"unavailable"}}"#;
        assert_eq!(
            parse_line(usage).unwrap().task_id.as_deref(),
            Some("_integration")
        );
        assert!(parse_line(&usage.replace("_integration", "_unknown")).is_err());
        assert!(
            parse_line(&usage.replace("usage.recorded", "operation.completed")).is_err(),
            "operation facts may never use a pseudo task id"
        );
    }

    #[test]
    fn parses_uuid_event_id() {
        let line = r#"{"schema_version":1,"event_id":"208af7d9-b848-4bd9-a215-3791e2b5c94d","occurred_at":"2026-07-10T00:32:13Z","type":"codex.attempt","task_id":"T-076","actor":{"kind":"tool","name":"codex"},"payload":{"role":"coder","attempt_number":1}}"#;
        let ev = parse_line(line).expect("valid");
        assert_eq!(ev.event_type, EventType::CodexAttempt);
        assert_eq!(ev.event_id, "208af7d9-b848-4bd9-a215-3791e2b5c94d");
    }

    #[test]
    fn parses_usage_recorded() {
        let line = r#"{"schema_version":1,"event_id":"739b5bde-c58b-51d8-b46f-d5c10f15e214","occurred_at":"2026-07-17T10:00:00Z","type":"usage.recorded","batch_id":"B-20260717T095500Z","task_id":"T-258","actor":{"kind":"tool","name":"codex"},"payload":{"task_id":"T-258","role":"coder","mode":"full","attempt_number":1,"source":"codex","model":"gpt-5-codex","input_tokens":1200,"output_tokens":450,"cache_read_input_tokens":300,"cache_creation_input_tokens":0,"total_tokens":1950,"estimated":false}}"#;
        let ev = parse_line(line).expect("valid");
        assert_eq!(ev.event_type, EventType::UsageRecorded);
    }

    #[test]
    fn all_known_types_parse() {
        for t in [
            "cohort.opened",
            "cohort.round_started",
            "cohort.round_closed",
            "cohort.admission_closed",
            "cohort.join_started",
            "cohort.published",
            "cohort.closed",
            "task.captured",
            "task.status_changed",
            "codex.attempt",
            "usage.recorded",
        ] {
            let line = format!(
                r#"{{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"{t}","actor":{{"kind":"agent","name":"processor"}},"payload":{{}}}}"#
            );
            let ev = parse_line(&line).unwrap_or_else(|e| panic!("{t}: {e}"));
            assert_eq!(ev.event_type.as_str(), t);
        }
    }

    // ---- lenient forward reading (§19.4) ---------------------------------------------

    #[test]
    fn tolerates_unknown_top_level_fields() {
        let line = r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","actor":{"kind":"agent","name":"processor"},"payload":{},"future_field":{"nested":true},"trace_id":"abc"}"#;
        let ev = parse_line(line).expect("unknown top-level fields must be tolerated");
        assert_eq!(ev.event_type, EventType::CohortOpened);
    }

    #[test]
    fn tolerates_missing_payload_version_defaulting_to_one() {
        let line = r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","actor":{"kind":"agent","name":"processor"},"payload":{}}"#;
        assert_eq!(parse_line(line).unwrap().payload_version, 1);
    }

    #[test]
    fn honors_explicit_payload_version() {
        let line = r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","payload_version":3,"actor":{"kind":"agent","name":"processor"},"payload":{}}"#;
        assert_eq!(parse_line(line).unwrap().payload_version, 3);
    }

    // ---- broken envelopes are rejected, never panic ----------------------------------

    fn err(line: &str) -> String {
        parse_line(line).expect_err("should be rejected").message
    }

    #[test]
    fn rejects_torn_json() {
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occ"#).contains("not valid JSON"));
    }

    #[test]
    fn rejects_missing_required_fields() {
        assert!(err(r#"{"schema_version":1,"occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("event_id"));
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","type":"cohort.opened","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("occurred_at"));
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","payload":{}}"#).contains("actor"));
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","actor":{"kind":"agent","name":"p"}}"#).contains("payload"));
    }

    #[test]
    fn rejects_unknown_type() {
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.exploded","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("unknown type"));
    }

    #[test]
    fn rejects_non_z_timestamp() {
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10+02:00","type":"cohort.opened","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("occurred_at"));
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08 12:24:10Z","type":"cohort.opened","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("occurred_at"));
    }

    #[test]
    fn rejects_bad_actor_and_ids() {
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","actor":{"kind":"robot","name":"p"},"payload":{}}"#).contains("actor.kind"));
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","actor":{"kind":"agent","name":""},"payload":{}}"#).contains("actor.name"));
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"task.captured","task_id":"X-1","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("T-id"));
        assert!(err(r#"{"schema_version":1,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","batch_id":"C-1","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("B-id"));
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        assert!(err(r#"{"schema_version":2,"event_id":"e-1","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","actor":{"kind":"agent","name":"p"},"payload":{}}"#).contains("schema_version"));
    }

    #[test]
    fn rejects_non_object_line() {
        assert!(err("[1,2,3]").contains("must be a JSON object"));
        assert!(err("42").contains("must be a JSON object"));
    }

    #[test]
    fn round_trips_through_to_json_line() {
        let ev = parse_line(REAL_STATUS).expect("valid");
        let reparsed = parse_line(&ev.to_json_line()).expect("re-parse normalized line");
        assert_eq!(ev, reparsed);
    }

    // The `occurred_at` matcher's per-format unit coverage now lives beside its implementation in
    // `crate::time::is_iso_utc` (`time.rs::tests::is_iso_utc_accepts_and_rejects_per_format`); the
    // parser's use of it stays covered here by `rejects_non_z_timestamp`.
}
