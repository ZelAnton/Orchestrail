//! Legacy-compatible outbox identity fingerprints for differential cutover tests.
//!
//! The legacy harness compares a sorted, event-id-deduplicated multiset of
//! `type|batch_id|task_id|from>to` identities, then hashes the semicolon-joined result using
//! SHA-256.  This module implements the same projection over typed [`super::Event`] values so a
//! Rust scenario can be compared without timestamps, UUIDs, or JSON formatting becoming noise.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use super::Event;

/// Return the legacy-sorted identity multiset. Duplicate `event_id`s are ignored before the
/// identity projection, just as the append/replay contract requires, but distinct event IDs with
/// the same semantic identity remain separate entries. That slightly surprising detail is part of
/// Orchestra's `tools/harness.ps1` byte contract (`Sort-Object`, not `Sort-Object -Unique`).
pub fn identities<'a>(events: impl IntoIterator<Item = &'a Event>) -> Vec<String> {
    let mut event_ids = BTreeSet::new();
    let mut identities = Vec::new();
    for event in events {
        if !event_ids.insert(event.event_id.clone()) {
            continue;
        }
        let from = event
            .payload
            .get("from")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let to = event
            .payload
            .get("to")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let transition = if from.is_empty() && to.is_empty() {
            String::new()
        } else {
            format!("{from}>{to}")
        };
        identities.push(format!(
            "{}|{}|{}|{}",
            event.event_type.as_str(),
            event.batch_id.as_deref().unwrap_or_default(),
            event.task_id.as_deref().unwrap_or_default(),
            transition
        ));
    }
    identities.sort();
    identities
}

/// Semicolon-separated sorted identity digest — byte-for-byte the legacy harness's `outbox`
/// component before its enclosing final-state hash.
pub fn digest<'a>(events: impl IntoIterator<Item = &'a Event>) -> String {
    identities(events).join(";")
}

/// Lowercase hexadecimal SHA-256 of [`digest`].
pub fn sha256<'a>(events: impl IntoIterator<Item = &'a Event>) -> String {
    let digest = digest(events);
    let hash = Sha256::digest(digest.as_bytes());
    format!("{hash:x}")
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;
    use crate::events::{Actor, ActorKind, EventType, SCHEMA_VERSION};

    fn event(
        id: &str,
        kind: EventType,
        batch: Option<&str>,
        task: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Event {
        let mut payload = Map::new();
        if let Some(from) = from {
            payload.insert("from".into(), Value::from(from));
        }
        if let Some(to) = to {
            payload.insert("to".into(), Value::from(to));
        }
        Event {
            schema_version: SCHEMA_VERSION,
            event_id: id.into(),
            occurred_at: "2026-07-24T12:00:00Z".into(),
            event_type: kind,
            actor: Actor {
                kind: ActorKind::Agent,
                name: "engine".into(),
            },
            batch_id: batch.map(str::to_string),
            task_id: task.map(str::to_string),
            payload_version: 1,
            payload,
        }
    }

    #[test]
    fn identity_is_timestamp_independent_and_dedups_by_event_id() {
        let mut duplicate = event(
            "evt-1",
            EventType::TaskStatusChanged,
            None,
            Some("T-1"),
            Some("в работе"),
            Some("на ревью"),
        );
        duplicate.occurred_at = "2027-01-01T00:00:00Z".into();
        let events = vec![
            event(
                "evt-2",
                EventType::CohortOpened,
                Some("B-1"),
                None,
                None,
                None,
            ),
            event(
                "evt-1",
                EventType::TaskStatusChanged,
                None,
                Some("T-1"),
                Some("в работе"),
                Some("на ревью"),
            ),
            duplicate,
        ];
        assert_eq!(
            identities(&events),
            vec![
                "cohort.opened|B-1||".to_string(),
                "task.status_changed||T-1|в работе>на ревью".to_string(),
            ]
        );
        assert_eq!(
            digest(&events),
            "cohort.opened|B-1||;task.status_changed||T-1|в работе>на ревью"
        );
        assert_eq!(sha256(&events).len(), 64);
    }

    #[test]
    fn changing_one_transition_changes_the_parity_hash() {
        let a = vec![event(
            "evt-1",
            EventType::TaskStatusChanged,
            None,
            Some("T-1"),
            Some("на ревью"),
            Some("готова к слиянию"),
        )];
        let b = vec![event(
            "evt-1",
            EventType::TaskStatusChanged,
            None,
            Some("T-1"),
            Some("на ревью"),
            Some("эскалирована"),
        )];
        assert_ne!(sha256(&a), sha256(&b));
    }

    #[test]
    fn distinct_event_ids_with_the_same_identity_remain_in_the_legacy_multiset() {
        let events = vec![
            event(
                "evt-1",
                EventType::TaskStatusChanged,
                None,
                Some("T-1"),
                Some("в работе"),
                Some("на ревью"),
            ),
            event(
                "evt-2",
                EventType::TaskStatusChanged,
                None,
                Some("T-1"),
                Some("в работе"),
                Some("на ревью"),
            ),
        ];
        assert_eq!(
            digest(&events),
            "task.status_changed||T-1|в работе>на ревью;task.status_changed||T-1|в работе>на ревью"
        );
    }
}
