# 0005. Deterministic UUIDv5 event identities

Status: Accepted · Date: 2026-04-22

## Decision

Events written to `.work/events.jsonl` use deterministic UUIDv5 identifiers derived from a durable semantic key. `engine/src/events/outbox.rs` implements `deterministic_event_id(key)` with `Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes())`; the key must contain durable coordinates such as event type, batch or task identity, and transition or cycle, never a clock or random run identifier.

The append-only outbox appends an event idempotently by this stable identity. It never generates a random replacement identity on replay and rejects an existing event ID whose content differs from the requested event.

## Why

- Replaying the same semantic transition must produce the same event identity regardless of when the replay occurs.
- A stable ID lets the outbox distinguish an already-present event from a new event without making duplicate history.
- Collision detection prevents an incorrect caller from silently turning a replay into a divergent stream.
- The implementation test fixes `cohort.opened|B-1` to UUID `3511e4d4-81ca-5434-8916-48671f482067` and proves repeated derivation is stable.

## Consequences

- Event producers must define durable semantic keys rather than deriving identities from timestamps or generated run IDs.
- Retried appends return an already-present result for equivalent event content, while the same ID with different content is an error.
- The event journal can be replayed and consumed across independent engine components without substituting random identities.
- This event identity rule complements [ADR 0003](0003-markdown-control-plane-canonical-states.md)'s durable control-plane representation.
