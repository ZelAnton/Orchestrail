# TUI operator guide

`orchestrail-tui` is a live console for observing an Orchestrail work directory and issuing a
small set of explicit operator commands. Observation is read-only by default. The console can
pause or resume processing, inspect or force-release the orchestration lease, and decide pending
approval requests; it does not run the processor or edit work definitions.

## Running

Run the installed binary with an optional path to the work directory:

```text
orchestrail-tui [OPTIONS] [WORK_DIR]
```

With no path argument, `WORK_DIR` is `.work` relative to the current directory. The `-w` or
`--work <PATH>` option is an alternative way to supply the same path:

```text
orchestrail-tui /srv/project/.work
orchestrail-tui --work /srv/project/.work
```

| Argument or option | Meaning |
| --- | --- |
| `[WORK_DIR]` | Work directory to observe. Defaults to `.work` relative to the current directory. |
| `-w <PATH>`, `--work <PATH>` | Supplies `WORK_DIR` as a named option instead of a positional argument. |
| `--tick-ms <N>` | Sets the UI refresh and input-poll cadence in milliseconds. The default is `250`; `N` must be greater than zero. |
| `-h`, `--help` | Prints command-line help and exits. |
| `-V`, `--version` | Prints the application version and exits. |

Only one work-directory value may be supplied. An unknown option, an extra positional argument,
or a zero or invalid `--tick-ms` value is reported as a command-line error.

## Screens

Press `Tab` to switch between the two screens.

### Overview (section 6.1)

The Overview is the live batch, cohort, and task projection. It tails `events.jsonl` through the
engine's `TailReader`, applying newly committed events in journal order and avoiding duplicate or
torn-tail handling in the UI itself. Existing journal events are loaded at startup before the
first frame is drawn.

The projection is supplemented with human-readable context from `status.md`, including its
summary lines and task metadata where available. This file is an overlay: it makes the event
projection easier to interpret, but it is not required for the screen to operate.

### Decision Inbox (section 6.2)

The Decision Inbox brings together the information needed for operator intervention. It shows:

- pending and expired approval requests;
- a read-only snapshot of the task queue and task descriptors; and
- whether the `PAUSE` kill-switch file currently exists.

Use `h` and `l`, or the Left and Right arrow keys, to move between panels. Vertical navigation
acts on the focused panel: it selects approvals in the approvals panel and scrolls other panels.
Approval decisions are available only on this screen.

## Key bindings

Unless a confirmation or rejection-reason modal is open, the following bindings apply. Letter
commands require an unmodified key unless the table explicitly says otherwise.

| Key | Scope | Action |
| --- | --- | --- |
| `q` | Global | Quit. |
| `Ctrl+C` | Global | Quit. |
| `Esc` | Global | Close the lease-status popup if it is open; otherwise quit. |
| `Tab` | Global | Switch between Overview and Decision Inbox. |
| `r` | Global | Invalidate the caches and reload `status.md`, the inbox snapshot, and current batch telemetry. |
| `h` / `l` | Decision Inbox | Focus the previous or next panel. |
| Left / Right | Decision Inbox | Focus the previous or next panel. |
| `k` / `j` | Decision Inbox | Select the previous or next approval, or scroll the focused non-approval panel up or down. |
| Up / Down | Decision Inbox | Select the previous or next approval, or scroll the focused non-approval panel up or down. |
| Page Up / Page Down | Decision Inbox | Move the approval selection or focused-panel scroll position by ten items. |
| `p` | Global | Run **pause**: create `PAUSE`. |
| `u` | Global | Run **resume**: remove `PAUSE`. |
| `s` | Global | Run **lease-status** and show the lease owner and liveness popup. |
| `x` | Global | Arm the destructive **force-lock** command and open its confirmation modal. |
| `a` | Decision Inbox | Arm approval of the selected pending request and open its confirmation modal. |
| `d` | Decision Inbox | Begin rejection of the selected pending request. Enter a non-empty reason before reaching the confirmation modal. |

An open modal captures all input, so navigation and other commands cannot leak through it.

| Key | Modal | Action |
| --- | --- | --- |
| Unmodified `y`, `Y`, or Enter | Confirmation | Confirm the armed force-lock, approval, or rejection. |
| Any other key, including `Esc` | Confirmation | Cancel the armed action and close the modal. |
| Printable unmodified character | Rejection reason | Append the character to the rejection reason. |
| Backspace | Rejection reason | Remove the last character. |
| Enter | Rejection reason | Accept a non-empty reason and proceed to the rejection confirmation modal. An empty reason is refused. |
| `Esc` | Rejection reason | Cancel the rejection flow. |

## Commands (semantics)

### `pause`

Pause creates `<WORK_DIR>/PAUSE`, the same kill switch written by
`launchers/cc-pause.sh`. Its contents identify when and where the pause was requested, but the
processor acts on the file's existence. A running processor checks the kill switch at a phase or
round boundary, so the command does not interrupt an operation already in progress.

### `resume`

Resume removes `<WORK_DIR>/PAUSE`, mirroring `launchers/cc-unpause.sh`. It is idempotent: if the
file is already absent, there is nothing to clear and the command is still treated as successful.

### `lease-status`

Lease-status is a read-only query. It runs `tools/state-tx.ps1 status --work <WORK_DIR> --json`
through the supervised engine path and reports the lease's `owner_id`, `role`, liveness,
`heartbeat_age_secs`, and `ttl_seconds`. An absent lease, a legacy or corrupt lock, or an
unavailable status tool is shown as a diagnostic rather than crashing the console.

### `force-lock`

Force-lock is an operator force-takeover. After confirmation, it invokes
`tools/state-tx.ps1 release --work <WORK_DIR> --force`. This is the single transactional release
path also used by `cc-processor.sh --force-lock`, giving both operator interfaces the same
serialization and diagnostics. The TUI does **not** delete the lock directory directly with a raw
filesystem removal. Because a force release may displace a live or foreign owner, use it only
after establishing that takeover is intended.

### `approval-approve`

Approval consumes the selected one-time request. A native Orchestrail request is decided through
the engine's typed `ApprovalStore`. A legacy `orchestra/approval@1` request uses the contained
`tools/policy.ps1` compatibility path. If another operator has already consumed the request, or
the request expired while the modal was open, the command reports that outcome and refreshes the
inbox instead of applying a stale decision.

### `approval-reject`

Rejection uses the same native or legacy backend as approval, but requires the operator to enter
a non-empty reason. The reason is included with the consumed rejection record. Submitting the
reason does not decide the request: the subsequent confirmation is still required.

## Two-step arm-and-confirm model

Irreversible decisions and force takeover require deliberate separation between selection and
execution:

1. Press `x` to arm force-lock, or `a`/`d` to start an approval decision. The TUI opens a modal;
   rejection also collects its required reason before showing the final confirmation.
2. Confirm with an explicit, unmodified `y`, `Y`, or Enter in the confirmation modal.

The first keystroke never performs the destructive action. Arrow keys, modified keystrokes such
as `Ctrl+Y`, and all accidental or stray keys do not confirm; in a confirmation modal they cancel
the armed action. In particular, terminal shortcuts such as `Ctrl+Y` can never confirm an
irreversible decision.

## What the TUI never does

The TUI has a deliberately narrow authority boundary:

- It never writes to `<WORK_DIR>/Tasks_Queue.md` (normally `.work/Tasks_Queue.md`).
- It never modifies task descriptors under `<WORK_DIR>/tasks/*/task.md` (normally
  `.work/tasks/*/task.md`).
- It never touches repository code files.
- It never calls the processor or any launcher, including `cc-processor.sh`, `cc-pause.sh`, and
  `cc-unpause.sh`. References to the launchers describe matching semantics, not subprocess calls.
- Its only mutation boundary is `tui/src/commands.rs`, which implements the named operations
  through the checked `PAUSE` file operations, `tools/state-tx.ps1`, and the native
  `ApprovalStore` (with contained `tools/policy.ps1` only for legacy approvals).

Everything outside that command boundary observes `events.jsonl`, `status.md`, and snapshot
metadata without modifying them.

## Cold start and missing work artifacts

The observer is intentionally lenient when an orchestration workspace is incomplete:

- If `events.jsonl` is absent, `TailReader` treats it as an empty journal and continues polling.
- If `status.md` is absent, unreadable, malformed, or partially written, the TUI uses an empty or
  best-effort status overlay. The event projection remains functional.
- If the entire work directory is absent, the TUI starts without crashing in a read-only degraded
  mode. Mutating commands cannot succeed until their required work-directory boundary exists.
- An absent `PAUSE` file means processing is not paused. Missing `orchestrator.lock`, queue data,
  or task descriptors is represented as absent or empty state; observation remains operational.

Status, queue/descriptor snapshots, and archive-derived metadata are checked on a gentle cadence
of about 500 milliseconds. Parsed data is cached while its confined `(mtime, len)` metadata is
unchanged, so the display can retain the cached parse until either timestamp or length changes.
If a file changes during a bounded read, that result is not pinned in the cache and the next tick
retries. Press `r` to invalidate these caches and request an immediate reload.

## Implementation references

These files define the behavior summarized by this operator guide:

- [`tui/src/cli.rs`](../tui/src/cli.rs) — arguments, flags, defaults, and validation;
- [`tui/src/main.rs`](../tui/src/main.rs) — screens, refresh loop, input routing, and confirmation
  gates;
- [`tui/src/commands.rs`](../tui/src/commands.rs) — the complete command mutation boundary; and
- [`tui/src/status.rs`](../tui/src/status.rs) — lenient status overlay loading.
