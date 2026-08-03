//! Typed access to the `.work/events.jsonl` durable event outbox
//! (contract `docs/queue_contract.md` §19).
//!
//! `.work/events.jsonl` is the machine-readable journal of orchestrator facts (cohort / task /
//! codex-attempt transitions) that complements the human-readable Markdown artifacts. This
//! module gives the future engine and TUI a shared, typed way to *consume* that journal:
//!
//! * [`model`] — the typed envelope ([`Event`], [`Actor`], [`EventType`]).
//! * [`parse`] — decode + validate one line (strict envelope, lenient-forward reading, §19.4).
//! * [`reader`] — a cursor / tail reader ([`TailReader`]) that yields only new, unique,
//!   fully-committed events and never hands out a torn tail (§19.5 / §19.7).
//! * [`outbox`] — the engine-owned append-only writer ([`Outbox`]), idempotent by a stable
//!   semantic event id. It is called only while the processor owns its single-writer lease.
//!
//! The reader remains safe for an independent TUI process; the writer does not acquire an
//! orchestration lease itself and therefore cannot be used to bypass the runtime's ownership
//! boundary.

pub mod fingerprint;
pub mod model;
pub mod outbox;
pub mod parse;
pub mod projector;
pub mod reader;

pub use fingerprint::{
    digest as fingerprint_digest, identities as fingerprint_identities,
    sha256 as fingerprint_sha256,
};
pub use model::{Actor, ActorKind, Event, EventType, SCHEMA_VERSION};
pub use outbox::{
    AppendOutcome, OUTBOX_FILE, Outbox, OutboxError, RotationPolicy, deterministic_event_id,
};
pub use parse::{ParseError, parse_line};
pub use projector::{project_processor_transition, project_task_done_transition};
pub use reader::{Cursor, PollStats, TailReader};
