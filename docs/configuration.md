# Engine configuration reference

This document describes the configuration contract implemented by the current
engine. The parser in `engine/src/config.rs` is the source of truth for
`.work/config.md`; the typed policy parser in `engine/src/policy.rs` is the
source of truth for the supported subset of `.work/constraints.md`.

Both files are operator-owned inputs. A missing file is valid and supplies the
defaults documented below. Any other read error stops the run.

## `.work/config.md` syntax

An active setting is a `KEY: value` line that starts in column zero. Keys consist
only of uppercase ASCII letters and underscores. Lines that start with `#` or
any whitespace are ignored, as are lines without a colon and lines whose key
does not match that grammar. Unknown well-formed keys are accepted for forward
compatibility but do not affect the engine.

An unquoted `#` starts an inline comment when it is the first value character or
follows whitespace. A `#` inside single or double quotes, or one attached
directly to another value character, remains part of the value. Values are
trimmed after comment removal.

Every active key, including an unknown key, may occur only once. A duplicate
active key, a malformed value for a recognized key, or an out-of-range value
rejects the entire configuration before a mutating processor run begins.

Unless a row says otherwise:

- “unsigned integer” means a base-10 Rust unsigned integer with no sign;
- `true/false or on/off` accepts exactly those four lowercase spellings;
- an empty active value is malformed, rather than equivalent to an omitted key;
- an omitted key uses the default shown in its row;
- parser and validation failures are fail-closed and stop the run.

### Typed command values

Verification, notification, and policy commands are direct executable plus
typed argument vectors, not shell programs. Whitespace separates arguments;
single and double quotes preserve an argument containing whitespace. Unquoted
shell operators (`|`, `&`, `;`, `<`, `>`, `$`, `(`, `)`, and backtick) are
rejected. Shell hosts such as `cmd`, `sh`, `bash`, `pwsh`, and `powershell`
(including executable extensions and paths to them) are also rejected.

For example, `cargo test -p orchestrail-engine` is valid, while
`cargo test && cargo fmt` and `pwsh -Command cargo test` are not.

## Processor configuration

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `MAX_PARALLEL` | Positive unsigned integer representable as `usize` | `3` | Maximum number of non-terminal task leaves admitted in one rolling wave. | Zero, non-numeric text, or platform overflow rejects the configuration. |
| `COHORT_SIZE` | Positive unsigned integer representable as `u32` | Three times the resolved `MAX_PARALLEL` (normally `9`) | Maximum number of tasks admitted to the cohort across all rolling waves. | Zero, non-numeric text, or `u32` overflow rejects the configuration. The dynamic default is used only when this key is absent. |
| `COHORT_MAX_AGE` | Positive unsigned integer, minutes | `90` | Stops rolling admission after the cohort reaches this age. | Zero or malformed text rejects the configuration. |
| `COHORT_TOKEN_BUDGET` | Unsigned integer, tokens | `0` (unlimited) | Post-charge ceiling for deduplicated provider-actual usage in the cohort. A positive value enables the token gate. | `0` is deliberately converted to “no budget.” A positive budget requires `EVENTS_OUTBOX` at runtime; without trustworthy event telemetry, model dispatch is refused. |
| `COHORT_TOKEN_BUDGET_STRICT` | `true` or `false` only | `false` | Controls unmetered-call handling. `false` gates on known actuals while retaining unmetered markers; `true` treats an explicit unmetered marker as unavailable telemetry. | Empty is treated as absent. `on/off` and any other non-empty spelling are rejected. |
| `CALL_MAX_ATTEMPTS` | Positive unsigned integer representable as `u32` | `2` | Maximum total transient execution attempts for one leaf kind, including the first launch. | Zero, malformed text, or overflow rejects the configuration. |
| `COHORT_BUDGET_SEC` | Unsigned integer, seconds | `0` (unlimited) | Total cohort wall-clock circuit breaker. | `0` is deliberately converted to “no deadline”; malformed text rejects the configuration. |

The cohort time budget, token budget, strictness flag, and event-outbox setting
are snapshotted into an active cohort. Resuming a checkpoint with different
values is rejected rather than silently changing cohort safety policy.

## Review, integration, and CI loops

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `REVIEW_LOOP_MAX` | Positive unsigned integer representable as `u32` | `8` | Maximum per-task review/fix cycles before escalation. | Zero, malformed text, or overflow rejects the configuration. |
| `INTEGRATION_LOOP_MAX` | Positive unsigned integer representable as `u32` | `8` | Maximum integration review/fix cycles. | Zero, malformed text, or overflow rejects the configuration. |
| `CI_FIX_MAX` | Positive unsigned integer representable as `u32` | `3` | Maximum CI repair cycles after published CI fails. | Zero, malformed text, or overflow rejects the configuration. |
| `STAGNATION_LIMIT` | Unsigned integer representable as `u32`, at least `2` | `2` | Escalates when the same finding or error signature repeats this many times. | Values below `2`, malformed text, or overflow reject the configuration. |

## Verification profile

All configured commands run sequentially as contained direct processes.
`VERIFICATION_COMMANDS` takes precedence over `SMOKE_CMD`.

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `VERIFICATION_MODE` | `disabled`, `auto`, or `required` | `disabled` when no profile exists; otherwise implicit `auto` | Selects the Phase 4 publication-verification profile state. `disabled` is an operator exemption; `auto` and `required` execute a configured profile and block when none is available. | Any other value, including empty, is rejected. An explicit `disabled` remains authoritative even when command text is present. |
| `VERIFICATION_COMMANDS` | Non-empty JSON array of non-empty command strings | Empty list | Ordered Phase 4 command profile. It wins over `SMOKE_CMD`. | Invalid JSON, an empty array, empty/NUL entries, or unsafe typed argv is rejected. For legacy compatibility, typed-argv validation is skipped only when `VERIFICATION_MODE` is explicitly `disabled` and review-cycle verification cannot execute the profile. |
| `REVIEW_CYCLE_VERIFICATION` | `true/false` or `on/off` | `false` | Runs a verification profile during every task review/fix cycle, in addition to Phase 4. | When enabled, at least one of the cycle subset, full command list, or smoke command must exist. Otherwise parsing fails. |
| `REVIEW_CYCLE_VERIFICATION_COMMANDS` | Non-empty JSON array of non-empty command strings | Empty list | Optional cheaper profile for review/fix cycles. Precedence is this key, then `VERIFICATION_COMMANDS`, then `SMOKE_CMD`. | Always validated as typed argv, even while the cycle gate is off. Invalid JSON, an empty array, empty/NUL entries, or shell grammar rejects the configuration. |
| `SMOKE_CMD` | One non-empty typed command string | Unset | Legacy one-command fallback used only when `VERIFICATION_COMMANDS` is absent. | Empty is treated as unset. Unsafe argv is rejected whenever the command can execute. Explicit Phase 4 `disabled` may preserve legacy shell-shaped text, but enabling review-cycle verification validates it and fails closed. |

Required verification commands from `.work/constraints.md` are appended to the
operator profile rather than replacing it. Their presence promotes an implicit
disabled mode to `auto`, but never overrides an explicit
`VERIFICATION_MODE: disabled`.

## Publication and CI watching

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `PUSH` | `true/false` or `on/off` | `true` | Requests remote publication. `false` suppresses only the remote push; the local fast-forward of the selected primary branch still occurs and still passes path and branch policy. | Invalid boolean text rejects the configuration. A requested push becomes local-only when no publication remote is configured. |
| `CI_WATCH` | `true/false` or `on/off` | `true` | Enables the post-push forge watcher. With required CI names, those exact contexts gate completion; without them, watching is best-effort. The watcher polls whichever forge `FORGE` names. | Invalid boolean text rejects the configuration. No watcher runs for local-only publication. |
| `FORGE` | `github`, `gitlab`, or `gitea` | `github` | Selects the typed forge client the CI watcher polls for the exact published commit. See [Forge selection](#forge-selection). | Any other value, including a differently cased spelling, rejects the configuration. The forge is never guessed from a remote URL. |
| `PUBLISH_LINEAR_HISTORY` | `true/false` or `on/off` | `false` | Declares that publication must use a byte-identical, merge-free history. | **Current implementation divergence:** the parser recognizes `true`, but the native engine has no crash-safe typed linearizer and rejects the processor run before repository discovery or lease acquisition. `false` is the only runnable value today. |
| `PUBLISH_CI_DEADLINE_SEC` | Positive unsigned integer, seconds | `1800` | Overall deadline supplied to published-CI watching. | Zero or malformed text rejects the configuration. |
| `PUBLISH_CI_BACKOFF_SEC` | Positive unsigned integer, seconds | `30` | Delay between CI polling attempts. | Zero or malformed text rejects the configuration. |
| `MAIN_BRANCH` | Optional non-empty string | Unset | Overrides the primary/base branch when no explicit CLI `--base` is supplied. Without either override, the typed VCS layer detects the trunk. | Empty is treated as unset. A value starting with `-` or containing NUL/CR/LF is rejected here; the typed VCS layer may apply stricter ref validation later. |

### Forge selection

`FORGE` names the typed client used to observe CI for the exact commit that was
published. Every supported value polls a commit-SHA-bound endpoint, never a
"latest run on the branch" listing, so a later push on the same branch can never
be mistaken for the published commit's result:

| `FORGE` | Client | Endpoint polled |
| --- | --- | --- |
| `github` | `vcs-github` (`gh`) | `repos/{owner}/{repo}/commits/<sha>/check-runs` |
| `gitlab` | `vcs-gitlab` (`glab`) | `projects/:fullpath/repository/commits/<sha>/statuses` (newest first) |
| `gitea` | `vcs-gitea` (`tea`) | `/repos/{owner}/{repo}/commits/<sha>/status` (latest status per context, newest first) |

The forge is a configuration decision, not an inference: the engine does not
derive it from a remote URL, because a mis-derived forge would poll the wrong
API and surface only as a deadline-shaped degradation after publication was
already attempted. An unrecognized `FORGE` rejects the run outright. Each
request runs in the repository directory, so the client resolves the project
from that repository's own remote, and each request is bounded by
`PUBLISH_CI_BACKOFF_SEC` (never under 30 seconds).

All three forges share one contract. A pass requires a positive terminal
success for every selected check on the published commit. A response that could
be a truncated page, a response describing a different commit, an absent
required check, a state the engine does not recognize, an unavailable endpoint,
and an exhausted `PUBLISH_CI_DEADLINE_SEC` are all fail-closed: with required
check names they report unconfirmed required checks (which never dispatches a CI
repair), and without them they degrade the best-effort observation. Only a
proven red check reports a CI failure.

Two forge-specific rules follow the forge's own semantics rather than inventing
stricter ones:

- **GitLab `allow_failure`.** A job GitLab itself treats as non-blocking is
  ignored during best-effort watching, exactly as GitLab's pipeline status
  ignores it. Naming that job in the required CI checks is an explicit operator
  contract that overrides the hint, and a red result then fails the gate.
- **GitLab `manual`.** A job waiting for a human is reported as pending, not as
  a failure. It is not a red check any CI repair could fix, so it must not
  dispatch one; the deadline turns it into an unconfirmed result instead. (This
  differs from GitHub's `action_required`, which the GitHub adapter treats as a
  failure.)

Gitea's client (`tea`) is narrower than `gh`/`glab` and models no typed `api`
method, so the adapter uses the crate's documented directory-bound raw argv
escape hatch to invoke `tea api`. A `tea` build without that subcommand simply
fails the request, which the watcher treats as an outage — never as a confirmed
CI.

**Gitea page completeness.** The three forges prove differently that the page
they classified was the whole set, because they offer different evidence:
GitHub reports a true `total_count`, GitLab has a fixed 100-entry page cap, and
Gitea has neither — its `total_count` is the length of the page it just
returned, and its page size is clamped to the instance's `[api]
MAX_RESPONSE_ITEMS` setting (default 50). A page cut by that setting therefore
looks complete in the body. Rather than assume a minimum server setting, the
Gitea adapter asks for the page after the one it read: an empty next page proves
completeness whatever the instance is configured to, and a non-empty one is
reported as a possibly-truncated page and refused. The extra request is spent
only on a snapshot that would otherwise pass — never while CI is pending, red,
or unreachable — so a confirmed publication costs one additional call and a
watch that never confirms costs none.

The limit this leaves is explicit and fail-closed, and it matches GitLab's: a
commit whose statuses do not fit one page — for Gitea more distinct check
contexts than `min(50, MAX_RESPONSE_ITEMS)`, for GitLab more than 100 status
entries — is never confirmed, and resolves as unconfirmed required checks or a
degraded best-effort observation. Raising `MAX_RESPONSE_ITEMS` above the number
of contexts a commit reports is what keeps such a Gitea repository confirmable.

Unlike GitLab, no sort order is requested from Gitea: the combined-status route
takes only `page` and `limit`, and the server already returns the newest status
per context first, so a sort parameter would be silently ignored.

## Approval and quarantine

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `APPROVAL_DEADLINE_SEC` | Positive unsigned integer, seconds | `86400` (24 hours) | Lifetime of a generated human approval request, including policy-bypass publication approval. | Zero or malformed text rejects the configuration. |
| `REVIEW_MIN_PASSES` | Positive unsigned integer representable as `u32` | `2` | Minimum independent passes requested from a task reviewer. The runtime retains the documented small, local `coder_fast` first-clean-pass exception. | Zero, malformed text, or overflow rejects the configuration. |
| `QUARANTINE_MAX_ATTEMPTS` | Positive unsigned integer representable as `u32` | `3` | Reserved limit for repeated quarantine handling. | **Current implementation divergence:** the value is recognized, validated, and stored, but current engine runtime code does not consume it. Zero, malformed text, or overflow still rejects configuration. |
| `REVIEWER_TIERING` | `true/false` or `on/off` | `true` | With tiering on, `coder_fast` uses `reviewer_std` and `coder`/`coder_deep` use `reviewer`. With tiering off, every task uses `reviewer` as its base Claude tier. | Invalid boolean text rejects the configuration. Codex reviewer routing preserves the independence rules described below. |

## Notifications

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `NOTIFY_CMD` | Optional direct executable plus typed arguments | Unset (successful no-op) | Best-effort operator notification program. | Empty is treated as unset. Shell grammar, a shell executable, NUL, or malformed quoting rejects the configuration. Notification execution itself remains best-effort. |

## Knowledge base

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `KB` | `on` or `off` only | `on` | Enables knowledge-base lookup and curation. If the file value is absent or empty, a valid `KB` environment value may supply `on/off`; otherwise the built-in default is used. | A non-empty invalid file value rejects configuration. An invalid environment fallback is ignored and resolves to `on`. |
| `KB_TTL` | Positive unsigned integer, completed cohorts | `8` | Expires unconfirmed singleton knowledge entries after this many completed cohorts. | Zero or malformed text rejects the configuration. |
| `KB_CAP` | Positive unsigned integer representable as `usize` | `12` | Maximum entries retained in each curated knowledge area. | Zero, malformed text, or platform overflow rejects the configuration. |

## Events outbox

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `EVENTS_OUTBOX` | `true/false` or `on/off` | `true` | Enables durable engine event and usage telemetry. Its value is snapshotted per cohort. | Invalid boolean text rejects the configuration. Disabling it makes a positive cohort token budget unusable, so model dispatch fails closed at the token gate. |

## Codex agents

Codex routing is optional. A Codex-authored range is always reviewed by Claude,
regardless of `CODEX_REVIEWER`, so the author and reviewer remain independent.
The `fast`, `fast+std`, and `deep` names select routing coverage, not model
names.

Only `CODEX_CODER`, `CODEX_REVIEWER`, and `KB` may inherit environment values.
All other recognized settings are file-only.

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `CODEX_CODER` | `off`, `fast`, or `fast+std` | `off` | `off` keeps all implementation on Claude; `fast` permits Codex for `coder_fast`; `fast+std` permits it for `coder_fast` and `coder`. `coder_deep` always remains on Claude. Network and known environment-limit gates may still route an eligible task back to Claude. | An absent or empty file value may inherit a valid environment value. Invalid environment text is ignored as `off`; a non-empty invalid file value rejects configuration. |
| `CODEX_REVIEWER` | `off`, `fast`, `fast+std`, or `deep` | `off` | For Claude-authored ranges, `fast` replaces the base reviewer for `coder_fast`; `fast+std` also covers `coder`; `deep` additionally runs a Codex augment pass before the base reviewer for `coder_deep`. | A Codex-authored range always uses the base Claude reviewer. Environment fallback and invalid-value behavior match `CODEX_CODER`. |
| `CODEX_CIFIX` | `on` or `off` | `off` | Enables Codex preparation of CI fixes. When off, that Codex preparation route is skipped. | Empty is treated as absent. Any non-empty value other than `on/off` rejects configuration. |
| `CODEX_MODEL` | Optional non-empty string | Unset (Codex CLI default model) | Passes an explicit `-m` model to Codex calls. | Empty is treated as unset. The config parser performs no model-name validation; Codex may reject an unsupported name. |
| `CODEX_REASONING` | `auto`, `low`, `medium`, `high`, or `xhigh` | `auto` | Sets Codex reasoning effort. `auto` resolves to `xhigh` for reviewers and `high` for other Codex roles. | Empty is treated as absent. Any other non-empty value rejects configuration. |
| `CODEX_SANDBOX` | `read-only` or `workspace-write` | `workspace-write` | Selects the explicit fail-closed Codex sandbox. Reviewers use read-only routing where required by the runtime; the configured mode is the general Codex call setting. | Empty is treated as absent. Any other non-empty value rejects configuration; there is no danger-full-access option. |
| `CODEX_NETWORK` | `on` or `off` | `on` | Controls direct outbound network access in the Codex sandbox. Managed dependency ecosystems may still use the separate broker path; arbitrary network needs require this setting to be on. | Empty is treated as absent. Any other non-empty value rejects configuration. |
| `CODEX_CMD` | One direct executable name or path, without arguments | `codex` | Replaces the Codex executable while the engine continues to construct all arguments. | Empty is treated as absent. Shell executables, NUL/CR/LF, and empty programs are rejected. Put no flags in this value. |

Every Codex call pins `approval_policy=never` and passes the prompt through
standard input. Sandbox initialization failure causes fallback or failure; it
never silently lowers sandbox protection.

## Call containment

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `CALL_DEADLINE_SEC` | Positive unsigned integer, seconds | `1800` | Per-contained-call deadline, also used by configured verification commands. | Zero or malformed text rejects the configuration. Release/process leases are required to outlive this value plus a safety margin. |
| `CALL_OUTPUT_MAX_BYTES` | Positive unsigned integer representable as `usize` | `1048576` (1 MiB) | Maximum captured output per contained external call. | Zero, malformed text, or platform overflow rejects the configuration. |

## Model pricing and monitoring

Pricing values estimate operator telemetry; they do not select models and do not
change the token admission gate.

| Key | Type and accepted forms | Default when absent | Meaning | Invalid or special behavior |
| --- | --- | --- | --- | --- |
| `MODEL_PRICES_USD_PER_MILLION` | Semicolon-separated `model=input,output[,cached-input[,cache-creation-input]]` entries | No overrides; use the built-in table below | Replaces a built-in exact model entry or adds a new model rate, in USD per one million tokens. Missing cached and cache-creation rates each inherit ordinary input. | An empty whole value is treated as absent. Otherwise, model names are 1–160 ASCII alphanumeric or `.`, `_`, `-`, `:`. Rates are unsigned decimals with at most six fractional digits. Empty/duplicate entries, signs, missing input/output, more than four rates, or overflow reject configuration. |
| `MODEL_PRICES_EFFECTIVE_DATE` | Ten-character digit pattern `YYYY-MM-DD` | `2026-07-30` | Effective date assigned to every entry supplied by `MODEL_PRICES_USD_PER_MILLION`. | Empty is treated as absent. The parser checks non-empty values for shape only, not calendar validity. With no price override, this key is validated but does not rewrite dates on built-in entries. |

The built-in table is:

| Model | Input | Cached input | Cache creation input | Output |
| --- | ---: | ---: | ---: | ---: |
| `gpt-5.6-sol` | 5.00 | 0.50 | 5.00 | 30.00 |
| `gpt-5.6-terra` | 2.50 | 0.25 | 2.50 | 15.00 |
| `gpt-5.6-luna` | 1.00 | 0.10 | 1.00 | 6.00 |
| `gpt-5-codex` | 1.25 | 0.125 | 1.25 | 10.00 |
| `gpt-5` | 1.25 | 0.125 | 1.25 | 10.00 |
| `claude-opus-4-8` | 5.00 | 0.50 | 6.25 | 25.00 |
| `claude-opus-4-7` | 5.00 | 0.50 | 6.25 | 25.00 |
| `claude-opus-4-6` | 5.00 | 0.50 | 6.25 | 25.00 |
| `claude-opus-4-5` | 5.00 | 0.50 | 6.25 | 25.00 |
| `opus` | 5.00 | 0.50 | 6.25 | 25.00 |
| `claude-sonnet-5` | 2.00 | 0.20 | 2.50 | 10.00 |
| `claude-sonnet-4-6` | 3.00 | 0.30 | 3.75 | 15.00 |
| `claude-sonnet-4-5` | 3.00 | 0.30 | 3.75 | 15.00 |
| `sonnet` | 2.00 | 0.20 | 2.50 | 10.00 |
| `claude-haiku-4-5` | 1.00 | 0.10 | 1.25 | 5.00 |
| `haiku` | 1.00 | 0.10 | 1.25 | 5.00 |

All amounts are USD per one million tokens and carry the built-in effective date
shown above. Exact model names resolve first; supported dated model suffixes
then resolve to the longest matching base name. Unknown models produce unknown
cost rather than a guessed rate.

## `.work/constraints.md` reference

The policy parser intentionally supports a typed subset of the human-oriented
constraints document. It recognizes exact Russian section-heading prefixes and
active bullet blocks; prose elsewhere is not policy.

Within a recognized `##` section:

1. `**Активные ограничения**` starts the active block.
2. Each policy value must be a `- ` bullet.
3. `**Пример**` stops the active block, so example bullets are never enforced.
4. Empty-placeholder bullets beginning with `(пусто` or containing
   `по умолчанию` are ignored.
5. A new `##` heading changes the section and deactivates the prior block.

A missing file produces an empty `Policy`: no extra denylist, branch, remote,
approval, verification-command, or CI-name restrictions. Malformed recognized
values or any non-not-found read error fail closed.

### Denied paths

Recognized heading prefix: `## Запрещённые пути`.

Each active bullet is one repository-relative glob, or a list written either as
comma-separated bare values or separately backtick-quoted values:

```markdown
## Запрещённые пути (denylist)
**Активные ограничения**
- `infra/**`, `**/*.pem`
```

Absolute paths, drive/URI-style values containing `:`, NUL, and any `..` path
segment are rejected. `*` and `?` do not cross a slash; `**` may cross slashes;
`**/` may match zero or more complete directory segments. Both candidate
conflict domains and exact changed paths are checked conservatively. Exact paths
are rechecked before commit, merge, and publication.

### Branch and remote policy

Recognized heading prefix: `## Разрешённые ветки`.

Two exact bullet prefixes are supported:

```markdown
## Разрешённые ветки и remotes
**Активные ограничения**
- Ветки публикации: `main`, `release`
- Remotes: `origin`, `upstream`
```

Lists may also be comma-separated without backticks. Empty allowlists add no
extra restriction: publication uses the configured/detected trunk and the
engine's publication remote (`origin`). An explicit branch allowlist is checked
for both local-only and remote publication. The remote allowlist is checked only
when a remote push is attempted. Empty values, values beginning with `-`, and
values containing NUL/CR/LF are malformed.

### Push approval and merge behavior

Recognized heading prefix: `## Push/merge policy`.

The parser enables the human gate only for an active bullet that starts with
`Публикация (push):` and contains `требует ручного`, for example:

```markdown
## Push/merge policy
**Активные ограничения**
- Публикация (push): требует ручного подтверждения
```

This gate creates an approval request bound to the exact publication manifest
and policy snapshot. It applies to remote push, not to local-only publication.
There is no additional free-form merge-policy field in the typed subset.
Merge safety is enforced through the denylist, exact changed-path checks, the
local publication-branch allowlist, and the engine's typed VCS workflow.

### Required verification commands

Recognized heading prefix: `## Обязательные проверки`.

Each active bullet is one typed command. Optional backticks may surround the
whole command:

```markdown
## Обязательные проверки
**Активные ограничения**
- `cargo fmt --check`
- cargo test -p orchestrail-engine
```

Commands use the same no-shell typed-argv grammar as `.work/config.md`.
Malformed or shell-shaped commands reject the policy. These commands supplement
the selected config profile and, unless verification was explicitly disabled,
execute in the integration worktree before publication. They do not participate
in the per-review-cycle subset. A required policy command disables the automatic
docs-only exemption and enables implicit `auto` verification unless the operator
explicitly selected `VERIFICATION_MODE: disabled`.

### Required CI checks

Recognized heading prefix: `## Обязательные CI-проверки публикации`.

An active bullet may contain one bare check name, optionally followed by
parenthesized explanation, or one or more backtick-quoted exact names:

```markdown
## Обязательные CI-проверки публикации
**Активные ограничения**
- `validate`, `crash matrix`
- lint (required on release)
```

The example produces the exact names `validate`, `crash matrix`, and `lint`.
Duplicates are removed while preserving first-seen order. Empty names, NUL/line
breaks, unmatched backticks, and unmatched parentheses reject the policy.

After an actual remote push with `CI_WATCH` enabled, these exact contexts are
passed to the watcher and must be green before cleanup can archive the cohort.
With no required names, `CI_WATCH` uses best-effort repository CI discovery.
`CI_WATCH: false` or a local-only publication disables the watcher even when
policy names are present.

### Size change thresholds

The constraints template includes a `## Пороги размера изменений` section for
describing change-size thresholds.

**Current implementation divergence:** the engine does not recognize this
heading, so it does not parse or enforce any bullets in the section. The
thresholds remain guidelines for planning and review roles only; they do not
create an engine gate or automatically change task handling.

### Mandatory human-review categories

The constraints template includes a
`## Категории обязательного human review` section for identifying changes that
should receive human review.

**Current implementation divergence:** the engine does not recognize this
heading, so the listed categories are informational human guidelines rather
than enforced policy. They do not generate approval requests. The only
implemented human publication gate from `.work/constraints.md` is the active
`Публикация (push): требует ручного подтверждения` rule under
`## Push/merge policy`; as described above, that gate applies only to an actual
remote push.
