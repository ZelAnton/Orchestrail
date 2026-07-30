# `events.jsonl` event contract

This document is the consumer reference for Orchestrail's durable event journal at
`.work/events.jsonl`. It describes the contract implemented by the current engine. The
authoritative implementation is in `engine/src/events/`, with telemetry emitters and validators
in `engine/src/headless.rs`, `engine/src/runtime.rs`, and `engine/src/telemetry.rs`.

The journal complements the processor checkpoint and human-readable control-plane files. It is
intended for independent consumers such as the TUI and future observability clients. Consumers
must treat it as an ordered stream of facts, not as a complete serialized processor state.

## File and record format

The file is UTF-8 JSON Lines. Each committed record is one JSON object followed by a line-feed
byte (`\n`). The line feed is the commit marker: even a complete and valid JSON object at end of
file is not committed until its trailing line feed exists.

The engine writes compact JSON. Consumers must not depend on object key order or whitespace.
Blank committed lines are tolerated by `TailReader` and do not represent events.

The generic v1 envelope is:

```json
{
  "schema_version": 1,
  "event_id": "evt-example",
  "occurred_at": "2026-07-30T12:00:00Z",
  "type": "task.captured",
  "batch_id": "B-1",
  "task_id": "T-1",
  "payload_version": 1,
  "actor": {
    "kind": "agent",
    "name": "engine"
  },
  "payload": {}
}
```

`evt-example` above is an opaque ID accepted by the generic parser for illustration. Events
emitted by the native engine use the deterministic UUIDv5 schemes documented below.

### Envelope fields

| Field | Presence | Wire type | Meaning and validation |
| --- | --- | --- | --- |
| `schema_version` | Required | Integer | Envelope major version. The current reader accepts exactly `1`. |
| `event_id` | Required | String | Deduplication identity. The generic reader requires a non-empty, whitespace-free token; it intentionally accepts legacy opaque IDs as well as UUIDs. Native emitters use UUIDv5. |
| `occurred_at` | Required | String | Event presentation time in the syntactic form `YYYY-MM-DDTHH:MM:SSZ` or with one to three fractional digits before `Z`. The generic envelope matcher checks the shape and trailing `Z`; telemetry-specific validators additionally validate calendar/time ranges where stated below. |
| `type` | Required | String | One of the 12 closed v1 types listed below. Unknown types are rejected. |
| `batch_id` | Optional | String or `null` | Cohort coordinate. When present, the generic reader requires a string beginning with `B-`. Individual telemetry validators can impose a stricter whole-token shape. |
| `task_id` | Optional | String or `null` | Task coordinate. Normally exactly `T-` followed by one or more ASCII digits. `usage.recorded` additionally permits `_cohort`, `_integration`, and `_release`. |
| `payload_version` | Optional | Positive integer | Version of the type-specific payload. Absence or `null` reads as `1`; native emitters currently write `1`. |
| `actor` | Required | Object | Contains `kind` and `name`. |
| `actor.kind` | Required | String | One of `agent`, `human`, or `tool`. |
| `actor.name` | Required | String | Non-empty emitter name. |
| `payload` | Required | Object | Type-specific fields. The generic reader keeps this object opaque. |

Unknown top-level fields are accepted and discarded by the v1 reader. This is deliberate
forward compatibility at the envelope level. Missing required fields, an unsupported
`schema_version`, an unknown `type`, or a malformed known field rejects the line. Payload
strictness is type-specific: the lifecycle projector emits fixed shapes, `codex.attempt` and
`operation.completed` have strict semantic validators, while the generic event parser does not
validate arbitrary payload keys.

## Event type index

| Type | Envelope coordinates emitted by the engine | Actor | Payload |
| --- | --- | --- | --- |
| `cohort.opened` | `batch_id` | `agent:engine` | Empty |
| `cohort.round_started` | `batch_id` | `agent:engine` | `wave`, `admission` |
| `cohort.round_closed` | `batch_id` | `agent:engine` | `wave`, `admission` |
| `cohort.admission_closed` | `batch_id` | `agent:engine` | `reason` |
| `cohort.join_started` | `batch_id` | `agent:engine` | Empty |
| `cohort.published` | `batch_id` | `agent:engine` | `main_sha`, `pushed`, `tasks`, `ci` |
| `cohort.closed` | `batch_id` | `agent:engine` | Empty |
| `task.captured` | `batch_id`, `task_id` | `agent:engine` | Empty |
| `task.status_changed` | `task_id`; no `batch_id` | `agent:engine` | `from`, `to` |
| `codex.attempt` | `batch_id`, `task_id` | `agent:processor` | Strict Codex attempt payload |
| `usage.recorded` | `batch_id`, `task_id` or usage pseudo-ID | `tool:claude` or `tool:codex` | Provider usage or an explicit unavailable marker |
| `operation.completed` | `batch_id`, `task_id` | `agent:engine` | Strict operation timing payload |

All native events use `schema_version: 1` and `payload_version: 1`.

### Ordering within one processor transition

When one accepted reducer command projects multiple lifecycle events, the current engine appends
them in this order: cohort open, round start, admission close, join start, publication, round
close, task events, then cohort close. Tasks are visited in task-ID order from the processor's
ordered map; for one task, capture precedes status change. Any `operation.completed` events
projected for that acknowledged effect are appended after its lifecycle events. This ordering is
deterministic, but consumers should derive state from event meanings rather than require every
transition to contain every possible event.

## Cohort lifecycle events

### `cohort.opened`

Emitted when an accepted processor transition changes from no active batch to an active batch.
The payload is empty. The durable identity coordinate is the batch plus the fixed `open`
coordinate.

```json
{"schema_version":1,"event_id":"evt-example-open","occurred_at":"2026-07-30T12:00:00Z","type":"cohort.opened","batch_id":"B-1","payload_version":1,"actor":{"kind":"agent","name":"engine"},"payload":{}}
```

### `cohort.round_started`

Emitted for an accepted `Admit` command while the processor was in its rolling phase.

| Payload field | Type | Semantics |
| --- | --- | --- |
| `wave` | Non-negative JSON integer produced from `u32` | The pre-transition batch wave. Normal operation uses a positive wave. |
| `admission` | String | `open` when admission was open before the transition; otherwise `closed`. |

Its identity coordinate is `round:<wave>`.

### `cohort.round_closed`

Emitted for an accepted `Advance` command. `wave` is the batch's next-wave counter minus one,
saturating at zero and then clamped to at least `1`. This preserves wave `1` for an empty or
no-admission close. `admission` is `closed` when the selected before/after batch has an admission
close reason, otherwise `open`. Its identity coordinate is `round:<wave>`.

The payload fields have the same types as `cohort.round_started`.

### `cohort.admission_closed`

Emitted exactly when the batch's admission-close state changes from absent to present.

| Payload field | Type | Semantics |
| --- | --- | --- |
| `reason` | String | The stable legacy-facing close literal. |

Current reason values are:

| Processor close reason | Payload value |
| --- | --- |
| Cohort size reached | `COHORT_SIZE` |
| Cohort maximum age reached | `COHORT_MAX_AGE` |
| Cohort token budget reached | `COHORT_TOKEN_BUDGET` |
| Legacy cohort state absent during recovery | `LEGACY_COHORT_STATE_ABSENT` |
| Queue empty | `очередь-пуста` |
| Only conflicts with ready tasks remain | `только-конфликты-с-готовыми` |

The identity coordinate is `admission-closed:<reason>`.

### `cohort.join_started`

Emitted when the processor enters its joining phase from another phase. The payload is empty and
the fixed identity coordinate is `join`.

### `cohort.published`

Emitted when the processor durably establishes a new terminal CI disposition for a published
head. A required-but-unconfirmed observation remains an operator hold and does not emit this
fact; archive CI reconfirmation also does not claim a new publication identity.

| Payload field | Type | Semantics |
| --- | --- | --- |
| `main_sha` | String | Published head recorded by the processor. |
| `pushed` | Boolean | Whether publication was pushed. |
| `tasks` | Array of strings | The processor's merged task IDs, emitted in the deterministic order of its ordered set. |
| `ci` | String | `confirmed`, `disabled`, or `unconfirmed-degraded`. |

The identity coordinate is `main_sha`. Re-observing the same publication head therefore
reconstructs the same ID even when `occurred_at` changes.

### `cohort.closed`

Emitted when a transition removes the active batch and leaves the processor idle. The payload is
empty and the fixed identity coordinate is `close`.

## Task lifecycle events

### `task.captured`

Emitted when a task moves from the internal `Capturing` phase to `Implementing`. It carries both
`batch_id` and `task_id`, has an empty payload, and uses the fixed `capture` identity coordinate.

### `task.status_changed`

Emitted when the human-readable status projection changes. It is also emitted for a review loop
that remains `на ревью` after review findings or an incomplete review. It carries `task_id` but
intentionally omits `batch_id`.

| Payload field | Type | Semantics |
| --- | --- | --- |
| `from` | String | Projected status before the accepted transition. |
| `to` | String | Projected status after the accepted transition. |

The event identity includes the task's current `review_cycles` counter as well as the two status
strings. This allows distinct review rounds with the same visible transition to remain distinct,
while replay of one round remains idempotent.

For the special transition from internal `Committing` directly to `Escalated` after at least one
review cycle, `from` is projected as `на ревью`, because the descriptor was already in review
rather than observably returning to implementation.

## Projector status dictionary

`task.status_changed` exposes the following stable, human-readable values:

| Internal `TaskPhase` | Event status |
| --- | --- |
| `Capturing` | `в работе` |
| `Implementing` | `в работе` |
| `Committing` | `в работе` |
| `Reviewing` | `на ревью` |
| `Fixing` | `на ревью` |
| `Ready` | `готова к слиянию` |
| `ResolvingMerge` | `разрешение конфликта` |
| `Merged` | `слита` |
| `Published` | `опубликована` |
| `Done` | `выполнена` |
| `Conflict` | `конфликт` |
| `Returned` | `эскалирована` |
| `Escalated` | `эскалирована` |

These strings are wire values, including their language and spelling. Consumers should display
them as-is or map them explicitly; they must not infer internal phases by translating arbitrary
text.

## `codex.attempt`

This is the finalized boundary record for one Codex attempt. The engine first reserves a durable
attempt number, then emits the event after the attempt terminates. A finalized reservation can
restore the identical event to the outbox after a crash.

The payload contains exactly these 14 fields:

| Payload field | Type | Semantics |
| --- | --- | --- |
| `task_id` | String | Must equal the envelope `task_id` and be a normal `T-<digits>` ID. |
| `role` | String | `coder` or `reviewer`. |
| `mode` | String | `full`, `augment`, or `fix`. |
| `attempt_number` | Positive integer | Public attempt number, at most `u32::MAX`. It is monotonic for a task/role/mode coordinate across durable outbox history and reservations. |
| `started_at` | String | Calendar-valid UTC timestamp accepted by the telemetry timestamp parser. |
| `ended_at` | String | Calendar-valid UTC timestamp; equals envelope `occurred_at`. |
| `duration_ms` | Non-negative integer | Exactly `ended_at - started_at` in milliseconds. |
| `effective_model` | String | Non-empty model value, at most 160 bytes and without control characters. |
| `effective_reasoning` | String | `low`, `medium`, `high`, or `xhigh`. |
| `effective_sandbox` | String | `read-only` or `workspace-write`. |
| `effective_network` | String | `on` or `off`. |
| `exit_code` | Integer or `null` | Child exit code when available. |
| `outcome` | String | `success`, `fallback`, or `failed`. |
| `outcome_reason` | String or `null` | Must be `null` for success. Required and allowlisted for fallback or failure. |

The fixed failure/fallback reasons are `DIFF_TOO_LARGE`, `SMOKE_FAILED`, `JJ_DRIFT`,
`EMPTY_DIFF`, `CODEX_UNAVAILABLE`, `CODEX_FAILED`, and `OTHER_FAILURE`. An environment
limitation may instead use `ENV_LIMIT/<class>`, where `<class>` is a non-empty ASCII
alphanumeric, hyphen, or underscore token.

The strict telemetry validator rejects missing or extra payload fields, coordinate mismatches,
invalid enum values, inconsistent duration/timestamps, the wrong actor, and the wrong UUID.

## `usage.recorded`

This event records provider usage for one completed model call. It always includes these payload
coordinates:

| Payload field | Type | Semantics |
| --- | --- | --- |
| `task_id` | String | Repeats the envelope task coordinate. Non-task model calls use `_cohort`, `_integration`, or `_release`. |
| `role` | String | Stable model role coordinate, for example `coder`, `reviewer`, `planner`, or an integration role. |
| `mode` | String | Stable mode coordinate such as `full`, `augment`, `fix`, or an operation-specific mode. |
| `attempt_number` | Positive integer | Durable attempt coordinate used to join usage to the logical call. |
| `source` | String | Current emitters use `claude` or `codex`. |
| `model` | String | Configured model name, or `default` when no explicit model is configured. |
| `usage_availability` | String | `available` or `unavailable`. |

When provider counters are available, the engine also writes `estimated: false` and writes each
counter reported by the provider:

| Optional counter | Type | Semantics |
| --- | --- | --- |
| `input_tokens` | Non-negative integer | Provider input-token count. |
| `output_tokens` | Non-negative integer | Provider output-token count. |
| `cache_read_input_tokens` | Non-negative integer | Provider cache-read input count. |
| `cache_creation_input_tokens` | Non-negative integer | Provider cache-creation input count. |
| `total_tokens` | Non-negative integer | Provider total when supplied. |

An unavailable marker contains `usage_availability: "unavailable"` and deliberately omits
`estimated` and every token counter. Unavailable usage is not zero usage.

The engine records actual provider counters only; it does not invent an estimate at the emitter.
The telemetry consumer prefers `total_tokens` (and accepts legacy `tokens` or `token_count`);
otherwise it sums the available input/output/cache fields with checked arithmetic.

Usage is independently durable from `operation.completed`. A failed best-effort operation append
must not cause a completed model mutation to be replayed, and a usage append failure becomes a
hard protocol error only while a cohort token budget requires durable usage availability.

## `operation.completed`

This event is the task-facing timing spine for completed work. It always has a normal envelope
`batch_id` and `task_id`, even for cohort- or integration-scoped work. Shared work is materialized
as one event per affected task.

The payload contains exactly these 11 fields:

| Payload field | Type | Semantics |
| --- | --- | --- |
| `operation` | Lowercase token | Operation name. Allowed token characters after the first lowercase letter are lowercase ASCII letters, digits, `.`, `_`, and `-`. |
| `role` | Lowercase token | Executor role coordinate. |
| `mode` | Lowercase token | Execution mode coordinate. |
| `attempt_number` | Positive integer | Replay-stable operation attempt. |
| `scope` | String | `task`, `cohort`, or `integration`. |
| `executor_kind` | String | `model`, `tool`, or `external`. |
| `started_at` | String | Calendar-valid UTC start time. |
| `ended_at` | String | Calendar-valid UTC end time, not earlier than `started_at`. |
| `duration_ms` | Non-negative integer | Recorded operation duration. |
| `outcome` | String | `success`, `fallback`, `failed`, `cancelled`, `timeout`, or `skipped`. |
| `shared_task_count` | Positive integer | Number of tasks sharing the operation. Must be `1` for `scope: "task"`. |

Current engine operation names are:

| Operation | Typical scope/kind | Semantics |
| --- | --- | --- |
| `planning` | `cohort` / `model` | Wave planning, materialized for newly admitted tasks. |
| `coding` | `task` / `model` | Task implementation. |
| `review` | `task` / `model` | Task review, including a distinct `augment` call when used. |
| `review_fix` | `task` / `model` | Task finding repair. |
| `vcs_merge` | `task` / `tool` | Typed VCS merge attempt. |
| `merge` | `task` or `integration` / `model` | Merger work. A clean typed VCS merge also emits a zero-duration `skipped` model gate. |
| `integration_review` | `integration` / `model` | Full cohort integration review. |
| `integration_fix` | `integration` / `model` | Integration finding repair. |
| `ci_fix` | `integration` / `model` | CI repair; a Codex-to-Claude route is folded into one logical fallback span. |
| `knowledge_curate` | `integration` / `model` | Knowledge curation. |
| `verification` | `integration` / `tool` | Local/final integration verification. |
| `publish` | `integration` / `external` | Publication attempt. |
| `ci_wait` | `integration` / `external` | CI observation, with `full` or archive mode. |

The strict validator requires `model` executor kind for `planning`, `coding`, `review`,
`review_fix`, `merge`, `integration_review`, `integration_fix`, `ci_fix`, and
`knowledge_curate`. It rejects `model` for `verification`, `publish`, and `ci_wait`.

For shared cohort/integration work, every affected task receives the same timing and operation
coordinates, and `shared_task_count` is the number of affected tasks. No operation events are
materialized when the affected task set is empty.

## Deterministic UUIDv5 identities

`deterministic_event_id` computes UUIDv5 with the standard URL namespace
(`Uuid::NAMESPACE_URL`) and the UTF-8 bytes of a stable semantic key. Time, random run IDs, and
JSON serialization are not identity inputs.

### Processor lifecycle identity

Lifecycle events from `events/projector.rs` use:

```text
<type>|<batch_id-or-empty>|<task_id-or-empty>|<coordinate>
```

The per-type coordinate is:

| Type | Coordinate |
| --- | --- |
| `cohort.opened` | `open` |
| `cohort.round_started` | `round:<wave>` |
| `cohort.round_closed` | `round:<wave>` |
| `cohort.admission_closed` | `admission-closed:<reason>` |
| `cohort.join_started` | `join` |
| `cohort.published` | `<main_sha>` |
| `cohort.closed` | `close` |
| `task.captured` | `capture` |
| `task.status_changed` | `<task_id>:<review_cycles>:<from>><to>` |

The `>` between `from` and `to` is literal. For example, the semantic key for a first visible
review transition is:

```text
task.status_changed||T-1|T-1:0:в работе>на ревью
```

### Codex attempt identity

```text
orchestra/codex.attempt/<task_id>/<role>/<mode>/<attempt_number>
```

The batch is intentionally not in this key. Public attempt-number allocation consults durable
outbox history and reservations for the task/role/mode coordinate.

### Usage identity

```text
orchestra/usage.recorded/<source>/<task-or-pseudo-id>/<batch_id>/<role>/<mode>/<attempt_number>
```

This separates provider halves of one fallback while retaining common task/role/mode/attempt
coordinates for telemetry joins.

### Operation identity

```text
orchestra/operation.completed/<batch_id>/<task_id>/<operation>/<role>/<mode>/<attempt_number>
```

Shared operations therefore have one deterministic ID per affected task.

## Idempotency and deduplication

The native writer indexes committed history by `event_id`. Its semantic fingerprint is SHA-256
over the normalized known v1 event with `occurred_at` cleared.

- The same `event_id` and the same normalized content, differing only in `occurred_at`, returns
  `AlreadyPresent` and appends nothing.
- The same `event_id` with different semantic content is an `EventIdCollision`; the writer
  refuses the append.
- A new `event_id` is appended once.
- Existing unknown top-level fields are not retained by the v1 parser and therefore are not part
  of this normalized v1 fingerprint.

This makes replay after a crash safe: the same accepted processor command reconstructs the same
event IDs. The runtime intentionally appends projected events before persisting the new processor
checkpoint. If the checkpoint write fails, replay sees already-durable events and the idempotent
writer leaves them unchanged.

The reader independently deduplicates delivery by `event_id`. Consumers should still persist the
provided cursor; relying only on process memory causes a restart from byte zero.

## Append-only commit and recovery semantics

The processor's orchestration lease is the production single-writer boundary. The outbox also
uses a process-local mutex so parallel native leaf threads cannot interleave writes. The writer
itself does not acquire the orchestration lease, so callers must not use it to bypass runtime
ownership.

For each append, the writer:

1. Opens only the expected plain work directory and plain file; redirected files/directories are
   rejected.
2. Finds the committed prefix ending at the last line feed.
3. Validates and indexes newly observed committed records.
4. Resolves the requested ID as already present, collision, or new. A semantic collision is
   rejected without modifying the file.
5. For an already-present or new event, removes an unterminated tail, if any, and synchronizes
   that repair.
6. Returns without appending for an already-present event; otherwise writes one compact JSON
   object plus `\n`, then calls `sync_all`.

Earlier committed records are never rewritten or removed. Tail truncation is limited to bytes
after the last commit line feed. If the whole file is one unterminated record, recovery truncates
it to zero before a new append.

A newline-terminated malformed existing record makes the writer fail closed before appending.
An unterminated tail is not parsed as a committed record; it is discarded on the next append.
Both writer indexing and reader delivery impose a 1 MiB per-record ceiling. An oversized
committed record, or an oversized unterminated tail for which a safe commit boundary cannot be
found, is an error.

These guarantees depend on the append-only invariant. Consumers must not rotate, truncate,
replace, or rewrite `events.jsonl` underneath an active cursor.

## `TailReader` and persistent cursors

`TailReader` is the reference incremental consumer. It returns new, unique, fully committed
events in file order.

### Cursor shape

`Cursor::to_json` serializes the reference `events_cursor.json` shape:

```json
{
  "byte_offset": 12345,
  "delivered_ids": [
    "evt-example-a",
    "evt-example-b"
  ],
  "dedupe_filter": "0000000000000000"
}
```

The displayed filter is shortened for readability and is not a loadable cursor value. A real
`dedupe_filter` is exactly 65,536 hexadecimal characters, encoding a fixed 32 KiB membership
filter.

| Cursor field | Presence | Semantics |
| --- | --- | --- |
| `byte_offset` | Required | Byte position immediately after the last processed committed newline. It advances past valid events, duplicates, invalid committed lines, and blank lines. It never advances into an unterminated tail. |
| `delivered_ids` | Required | Exact ordered window of up to the 512 most recently delivered IDs. More than 512 input IDs are accepted but only the newest 512 are retained. |
| `dedupe_filter` | Optional for legacy cursors | Fixed hexadecimal membership filter covering delivered IDs known to the current cursor. Parsing a legacy cursor without it builds a filter from `delivered_ids`. |

`Cursor::from_json` rejects non-JSON, a non-object, `{}`, missing required fields, a negative or
non-integer offset, non-array `delivered_ids`, non-string entries, and a malformed filter.
Unknown cursor object fields are ignored. The events library serializes and parses cursor state;
the consuming application is responsible for durably persisting it.

### Resolving the next read position

Create `TailReader::new(path)` to start at byte zero. To resume, parse the consumer's cursor and
create `TailReader::with_cursor(path, &cursor)`. The reader starts exactly at `byte_offset`; it
does not search by timestamp or by the last ID.

A missing journal reads as empty, which allows a follow-mode consumer to wait for creation. If
the file has unexpectedly shrunk to or below the stored offset, the reader returns no events; it
does not reset or replay because shrinking violates the append-only precondition.

### `poll()` versus `poll_all()`

- `poll()` reads at most 1 MiB from the current offset, processes every complete line in that
  chunk, advances through the last processed newline, and returns the newly delivered events.
  A long-lived consumer calls it repeatedly as the file grows.
- `poll_all()` repeatedly calls `poll()` until a call makes no byte-offset progress. It returns
  one complete snapshot of all records committed at the time of reading, while preserving the
  per-record bound. It does not wait for future appends.

After either method, obtain `reader.cursor()` and persist it only after the consumer has durably
applied the returned events. Persisting first can lose delivery after a consumer crash.

### Torn tails, invalid lines, and counters

Only newline-terminated records are processed. A final unterminated fragment remains unread;
`has_unterminated_tail()` reports it after the poll that reached physical EOF. Once the writer
repairs or completes the tail and a newline exists, a later poll can process it.

A committed non-UTF-8 or envelope-invalid line is skipped, increments `skipped_invalid`, and
advances the cursor so one corrupt record cannot wedge a live stream. A duplicate increments
`skipped_dup`. A delivered event increments `delivered`. Blank lines affect none of these
counters. `PollStats` is cumulative for the life of that reader.

Continuous UI consumers may expose invalid/torn-tail diagnostics and continue according to
their policy. Complete telemetry snapshot consumers in the engine fail closed when any invalid
committed record was skipped or when an unterminated tail makes the snapshot incomplete.

### Bounded, exact deduplication

The reader keeps:

- an exact recent set/window of 512 IDs;
- a 32 KiB persisted membership filter using four SHA-256-derived bit positions per ID.

An exact-window hit is a duplicate. For an older possible filter hit, the reader performs a
bounded streaming scan of the immutable committed prefix before the current line. Therefore a
probabilistic filter false positive cannot suppress a genuinely new event. With a current cursor,
older replayed IDs remain deduplicated without unbounded cursor growth. A legacy cursor without
the filter can only seed historical membership from the exact IDs it contains.

## Legacy parity fingerprint

`events/fingerprint.rs` defines a comparison surface for differential cutover tests; it is not
the event's UUID identity scheme.

For each event, after dropping repeated `event_id` values, it projects:

```text
<type>|<batch_id-or-empty>|<task_id-or-empty>|<from>><to>
```

The transition component is empty when both payload fields are absent. Identities are sorted and
joined with semicolons; the parity hash is lowercase SHA-256 of that joined string. Distinct
event IDs with the same projected identity remain duplicate entries in this sorted multiset.
Timestamps, UUID text, payload fields other than `from`/`to`, and JSON formatting do not affect
the parity fingerprint.

## Consumer checklist

1. Read JSONL by bytes and regard only newline-terminated records as committed.
2. Use the v1 envelope rules, but do not assume payloads of unknown future versions have the
   current shape.
3. Process records in file order and deduplicate by `event_id`.
4. Persist the cursor only after the corresponding consumer state is durable.
5. Do not use `occurred_at` as an identity or ordering substitute; file order is authoritative.
6. Treat Russian task status strings and telemetry enum values as stable wire literals.
7. Surface invalid committed lines and torn tails rather than silently presenting a complete
   snapshot.
8. Never rewrite, rotate, or truncate the journal while an engine or cursor-based consumer is
   active.
