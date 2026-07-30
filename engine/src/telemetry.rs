//! Fail-closed token telemetry snapshots for the deterministic processor.
//!
//! A non-zero `COHORT_TOKEN_BUDGET` is a post-charge ceiling: before a new model call the
//! processor must know the sum of already-durable *actual* `usage.recorded` events for its
//! batch. This module is the native equivalent of legacy `metrics.ps1 budget`: it reads the
//! typed event stream through [`crate::events::TailReader`], deduplicates by `event_id`, and
//! treats every incomplete/corrupt signal as unavailable rather than guessing that usage is
//! zero. Estimated usage is reported separately and never contributes to `actual_tokens`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::events::outbox::lock_outbox;
use crate::events::{
    ActorKind, Event, EventType, OUTBOX_FILE, SCHEMA_VERSION, TailReader, deterministic_event_id,
};
use crate::task_id::is_task_id;
use crate::time::iso_to_epoch_millis;

/// Provider-exact token counters captured for one completed model invocation.
///
/// The fields deliberately remain optional: providers can omit an individual component while
/// still supplying an exact total. [`Self::from_fields`] refuses an entirely empty shape, so an
/// absent provider usage block can never be materialized as an actual zero-token event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

/// Provider token counters use different cache accounting conventions. This comes from the
/// durable usage event source, never from a model-name heuristic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageSource {
    Claude,
    Codex,
}

impl UsageSource {
    fn from_event(value: &str) -> Option<Self> {
        match value {
            "claude" => Some(Self::Claude),
            "codex" => Some(Self::Codex),
            _ => None,
        }
    }
}

impl ProviderUsage {
    /// Construct an exact usage record from scalar backend fields. An explicit total wins;
    /// otherwise the known components form a total only when their unsigned sum cannot overflow.
    pub fn from_fields(
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cache_read_input_tokens: Option<u64>,
        cache_creation_input_tokens: Option<u64>,
        total_tokens: Option<u64>,
    ) -> Option<Self> {
        let any_component = input_tokens.is_some()
            || output_tokens.is_some()
            || cache_read_input_tokens.is_some()
            || cache_creation_input_tokens.is_some();
        if total_tokens.is_none() && !any_component {
            return None;
        }
        // An explicit provider total is authoritative and remains valid even if optional
        // diagnostic components cannot be added without overflowing `u64`.  Only a missing
        // total asks us to derive one from those components.
        let computed = if total_tokens.is_none() {
            Some(
                [
                    input_tokens,
                    output_tokens,
                    cache_read_input_tokens,
                    cache_creation_input_tokens,
                ]
                .into_iter()
                .flatten()
                .try_fold(0_u64, u64::checked_add)?,
            )
        } else {
            None
        };
        Some(Self {
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_creation_input_tokens,
            total_tokens: total_tokens.or(any_component.then_some(computed.unwrap_or_default())),
        })
    }
}

/// One USD-denominated rate per million tokens, stored as millionths of a dollar so pricing
/// arithmetic never depends on binary floating point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsdPerMillion(u64);

impl UsdPerMillion {
    pub const fn from_micro_usd(value: u64) -> Self {
        Self(value)
    }

    pub const fn micro_usd(self) -> u64 {
        self.0
    }
}

/// A dated model rate card. Cache creation is explicit because Claude bills cache writes at a
/// different rate; operator overrides that omit it inherit the ordinary input rate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPricing {
    pub model: String,
    pub input: UsdPerMillion,
    pub cached_input: UsdPerMillion,
    pub cache_creation_input: UsdPerMillion,
    pub output: UsdPerMillion,
    pub effective_date: String,
}

/// Built-in rates plus operator overrides from `config.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingTable {
    entries: BTreeMap<String, ModelPricing>,
}

pub const DEFAULT_PRICING_EFFECTIVE_DATE: &str = "2026-07-30";

impl Default for PricingTable {
    fn default() -> Self {
        let mut table = Self {
            entries: BTreeMap::new(),
        };
        // Standard API rates in USD per 1M tokens, current on the date above. Claude cache
        // creation uses the default five-minute write price.
        for (model, input, cached, creation, output) in [
            ("gpt-5.6-sol", 5_000_000, 500_000, 5_000_000, 30_000_000),
            ("gpt-5.6-terra", 2_500_000, 250_000, 2_500_000, 15_000_000),
            ("gpt-5.6-luna", 1_000_000, 100_000, 1_000_000, 6_000_000),
            ("gpt-5-codex", 1_250_000, 125_000, 1_250_000, 10_000_000),
            ("gpt-5", 1_250_000, 125_000, 1_250_000, 10_000_000),
            ("claude-opus-4-8", 5_000_000, 500_000, 6_250_000, 25_000_000),
            ("claude-opus-4-7", 5_000_000, 500_000, 6_250_000, 25_000_000),
            ("claude-opus-4-6", 5_000_000, 500_000, 6_250_000, 25_000_000),
            ("claude-opus-4-5", 5_000_000, 500_000, 6_250_000, 25_000_000),
            ("opus", 5_000_000, 500_000, 6_250_000, 25_000_000),
            ("claude-sonnet-5", 2_000_000, 200_000, 2_500_000, 10_000_000),
            (
                "claude-sonnet-4-6",
                3_000_000,
                300_000,
                3_750_000,
                15_000_000,
            ),
            (
                "claude-sonnet-4-5",
                3_000_000,
                300_000,
                3_750_000,
                15_000_000,
            ),
            ("sonnet", 2_000_000, 200_000, 2_500_000, 10_000_000),
            ("claude-haiku-4-5", 1_000_000, 100_000, 1_250_000, 5_000_000),
            ("haiku", 1_000_000, 100_000, 1_250_000, 5_000_000),
        ] {
            table.insert(ModelPricing {
                model: model.into(),
                input: UsdPerMillion::from_micro_usd(input),
                cached_input: UsdPerMillion::from_micro_usd(cached),
                cache_creation_input: UsdPerMillion::from_micro_usd(creation),
                output: UsdPerMillion::from_micro_usd(output),
                effective_date: DEFAULT_PRICING_EFFECTIVE_DATE.into(),
            });
        }
        table
    }
}

impl PricingTable {
    pub fn insert(&mut self, pricing: ModelPricing) {
        self.entries.insert(pricing.model.clone(), pricing);
    }

    pub fn entries(&self) -> impl Iterator<Item = &ModelPricing> {
        self.entries.values()
    }

    /// Resolve an exact model first, then a dated snapshot suffix such as `-20251015` or
    /// `-2025-10-15`. The `default` sentinel is never a priced model: it records that the
    /// invocation's concrete model was not configured. Longest base wins so a general family
    /// cannot shadow a more specific model.
    pub fn resolve(&self, model: &str) -> Option<&ModelPricing> {
        if model == "default" {
            return None;
        }
        if let Some(exact) = self.entries.get(model) {
            return Some(exact);
        }
        self.entries
            .iter()
            .filter_map(|(base, pricing)| {
                let suffix = model.strip_prefix(base)?.strip_prefix('-')?;
                dated_model_suffix(suffix).then_some((base.len(), pricing))
            })
            .max_by_key(|(length, _)| *length)
            .map(|(_, pricing)| pricing)
    }
}

fn dated_model_suffix(suffix: &str) -> bool {
    (suffix.len() == 8 && suffix.bytes().all(|byte| byte.is_ascii_digit()))
        || (suffix.len() == 10
            && suffix.as_bytes().get(4) == Some(&b'-')
            && suffix.as_bytes().get(7) == Some(&b'-')
            && suffix
                .bytes()
                .enumerate()
                .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit()))
}

/// A composable cost estimate. `unknown` means at least one contribution could not be priced;
/// `nano_usd` still retains any known partial sum for operator visibility.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CostEstimate {
    pub nano_usd: u128,
    pub estimated: bool,
    pub unknown: bool,
}

impl CostEstimate {
    pub const fn unknown() -> Self {
        Self {
            nano_usd: 0,
            estimated: true,
            unknown: true,
        }
    }

    pub fn merge(&mut self, other: Self) {
        if let Some(total) = self.nano_usd.checked_add(other.nano_usd) {
            self.nano_usd = total;
        } else {
            self.nano_usd = u128::MAX;
            self.unknown = true;
        }
        self.estimated |= other.estimated;
        self.unknown |= other.unknown;
    }

    pub fn format_usd(self) -> String {
        if self.unknown && self.nano_usd == 0 {
            return "≈$?".into();
        }
        let cents_100 = self.nano_usd.saturating_add(50_000) / 100_000;
        let dollars = cents_100 / 10_000;
        let fraction = cents_100 % 10_000;
        let marker = if self.estimated { "≈" } else { "" };
        let unknown = if self.unknown { "+?" } else { "" };
        format!("{marker}${dollars}.{fraction:04}{unknown}")
    }
}

/// Convert provider usage into a rate-card estimate. Missing pricing or a provider total that
/// cannot be reconciled with its category counters yields an explicit unknown contribution.
pub fn estimate_usage_cost(
    usage: ProviderUsage,
    source: UsageSource,
    model: &str,
    pricing: &PricingTable,
) -> CostEstimate {
    let Some(rate) = pricing.resolve(model) else {
        return CostEstimate::unknown();
    };
    if usage.input_tokens.is_none()
        && usage.output_tokens.is_none()
        && usage.cache_read_input_tokens.is_none()
        && usage.cache_creation_input_tokens.is_none()
    {
        return CostEstimate::unknown();
    }
    let input = usage.input_tokens.unwrap_or(0);
    let cached_input = usage.cache_read_input_tokens.unwrap_or(0);
    // Codex/OpenAI reports cached input as part of `input_tokens`; Anthropic reports it as an
    // independent category. Price only the non-cached portion at the ordinary input rate.
    let non_cached_input = match source {
        UsageSource::Codex => input.saturating_sub(cached_input),
        UsageSource::Claude => input,
    };
    let minimum_total = match source {
        UsageSource::Codex => non_cached_input.checked_add(usage.output_tokens.unwrap_or(0)),
        UsageSource::Claude => input
            .checked_add(usage.output_tokens.unwrap_or(0))
            .and_then(|total| total.checked_add(cached_input))
            .and_then(|total| total.checked_add(usage.cache_creation_input_tokens.unwrap_or(0))),
    };
    let Some(minimum_total) = minimum_total else {
        return CostEstimate::unknown();
    };
    // Anthropic totals cover independent cache categories. Codex totals can include cached input
    // within `input_tokens`, so their lower bound deliberately uses normalized input instead.
    if usage.total_tokens.is_none_or(|total| total < minimum_total) {
        return CostEstimate::unknown();
    }
    let Some(priced) = [
        (non_cached_input, rate.input),
        (usage.output_tokens.unwrap_or(0), rate.output),
        (cached_input, rate.cached_input),
        (
            usage.cache_creation_input_tokens.unwrap_or(0),
            rate.cache_creation_input,
        ),
    ]
    .into_iter()
    .try_fold(0_u128, |total, (tokens, rate)| {
        let contribution = u128::from(tokens)
            .checked_mul(u128::from(rate.micro_usd()))?
            .checked_mul(1_000)?;
        total.checked_add(contribution)
    }) else {
        return CostEstimate::unknown();
    };
    CostEstimate {
        nano_usd: priced.saturating_add(500_000) / 1_000_000,
        estimated: true,
        unknown: false,
    }
}

/// A complete per-batch usage snapshot, safe to compare against a token budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenUsage {
    pub actual_tokens: u64,
    pub estimated_tokens: u64,
    pub actual_events: u64,
    pub estimated_events: u64,
    /// Completed model invocations whose provider supplied no token counters. These are never
    /// interpreted as zero; the default gate reports them while enforcing against known actuals.
    pub unmetered_events: u64,
}

/// Why a safety snapshot cannot vouch for actual usage. Variants intentionally contain no raw
/// event text, paths, or I/O diagnostics because callers may surface this in status and journals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryUnavailable {
    EventsOutboxDisabled,
    EventsFileMissing,
    InvalidEventRecord,
    UnterminatedEventRecord,
    MalformedActualUsage,
    UnmeteredUsage,
    ReadFailed,
}

impl TelemetryUnavailable {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EventsOutboxDisabled => "events-outbox-disabled",
            Self::EventsFileMissing => "events-file-missing",
            Self::InvalidEventRecord => "invalid-event-record",
            Self::UnterminatedEventRecord => "unterminated-event-record",
            Self::MalformedActualUsage => "malformed-actual-usage",
            Self::UnmeteredUsage => "unmetered-usage",
            Self::ReadFailed => "events-read-failed",
        }
    }
}

/// The only two outcomes a model-call safety gate may use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenTelemetrySnapshot {
    Available(TokenUsage),
    Unavailable(TelemetryUnavailable),
}

/// Read a complete, deduplicated usage snapshot for `batch_id`.
///
/// An enabled budget refuses to proceed when events are off, the outbox is absent, an event line
/// was invalid/torn, the file cannot be read, or any non-estimated usage event lacks an exact
/// non-negative integer total. A batch with no actual usage is *available* with a zero total;
/// that is the only safe zero assumption after the complete outbox has been validated.
pub fn cohort_token_usage(
    work: &Path,
    batch_id: &str,
    events_outbox_enabled: bool,
) -> TokenTelemetrySnapshot {
    cohort_token_usage_with_strict(work, batch_id, events_outbox_enabled, false)
}

/// Read the token snapshot using the cohort's immutable unmetered-usage policy. In strict mode
/// one explicit `usage_availability=unavailable` marker makes the snapshot unavailable; in the
/// default mode it remains a visible undercount and metered actuals continue to gate admission.
pub fn cohort_token_usage_with_strict(
    work: &Path,
    batch_id: &str,
    events_outbox_enabled: bool,
    strict_unmetered: bool,
) -> TokenTelemetrySnapshot {
    if !events_outbox_enabled {
        return TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::EventsOutboxDisabled);
    }

    let path = work.join(OUTBOX_FILE);
    if !path.is_file() {
        return TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::EventsFileMissing);
    }
    let _guard = match lock_outbox() {
        Ok(guard) => guard,
        Err(_) => return TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::ReadFailed),
    };
    let mut reader = TailReader::new(path);
    let events = match reader.poll_all() {
        Ok(events) => events,
        Err(_) => return TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::ReadFailed),
    };
    if reader.stats().skipped_invalid > 0 {
        return TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::InvalidEventRecord);
    }
    if reader.has_unterminated_tail() {
        return TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::UnterminatedEventRecord);
    }

    let mut usage = TokenUsage {
        actual_tokens: 0,
        estimated_tokens: 0,
        actual_events: 0,
        estimated_events: 0,
        unmetered_events: 0,
    };
    let mut task_batches = BTreeMap::<String, String>::new();
    for event in temporally_ordered(events) {
        observe_task_batch(&event, &mut task_batches);
        if event.event_type != EventType::UsageRecorded
            || !usage_belongs_to_batch(&event, &task_batches, batch_id)
        {
            continue;
        }
        match event
            .payload
            .get("usage_availability")
            .and_then(Value::as_str)
        {
            Some("unavailable") => {
                if !is_unavailable_marker(&event.payload) {
                    return TokenTelemetrySnapshot::Unavailable(
                        TelemetryUnavailable::MalformedActualUsage,
                    );
                }
                usage.unmetered_events = usage.unmetered_events.saturating_add(1);
                continue;
            }
            // Absence remains read-compatible with already-durable pre-marker events.
            Some("available") | None => {}
            Some(_) => {
                return TokenTelemetrySnapshot::Unavailable(
                    TelemetryUnavailable::MalformedActualUsage,
                );
            }
        }
        let estimated = match event.payload.get("estimated") {
            Some(Value::Bool(value)) => *value,
            _ => {
                return TokenTelemetrySnapshot::Unavailable(
                    TelemetryUnavailable::MalformedActualUsage,
                );
            }
        };
        let total = usage_total(&event.payload);
        if estimated {
            // Estimated telemetry is informative but never gates execution. The legacy metrics
            // contract also permits an estimate that carries no usable total.
            if let Some(total) = total {
                let Some(next) = usage.estimated_tokens.checked_add(total) else {
                    return TokenTelemetrySnapshot::Unavailable(
                        TelemetryUnavailable::MalformedActualUsage,
                    );
                };
                usage.estimated_tokens = next;
                usage.estimated_events = usage.estimated_events.saturating_add(1);
            }
        } else {
            let Some(total) = total else {
                return TokenTelemetrySnapshot::Unavailable(
                    TelemetryUnavailable::MalformedActualUsage,
                );
            };
            let Some(next) = usage.actual_tokens.checked_add(total) else {
                return TokenTelemetrySnapshot::Unavailable(
                    TelemetryUnavailable::MalformedActualUsage,
                );
            };
            usage.actual_tokens = next;
            usage.actual_events = usage.actual_events.saturating_add(1);
        }
    }
    if strict_unmetered && usage.unmetered_events > 0 {
        return TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::UnmeteredUsage);
    }
    TokenTelemetrySnapshot::Available(usage)
}

fn is_unavailable_marker(payload: &Map<String, Value>) -> bool {
    const FORBIDDEN: [&str; 8] = [
        "estimated",
        "total_tokens",
        "tokens",
        "token_count",
        "input_tokens",
        "output_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
    ];
    FORBIDDEN.iter().all(|key| !payload.contains_key(*key))
}

/// Best-effort operator summary for one validated, deduplicated batch event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchTelemetrySummary {
    pub codex_successes: u64,
    pub codex_fallbacks: u64,
    pub codex_failures: u64,
    pub codex_fallback_reasons: BTreeMap<String, u64>,
    pub usage: TokenUsage,
    pub actual_by_source: BTreeMap<String, u64>,
    pub estimated_cost: CostEstimate,
    pub cost_by_model: BTreeMap<String, CostEstimate>,
    pub cost_by_role: BTreeMap<String, CostEstimate>,
}

/// Aggregate operator telemetry without changing execution safety. Unlike the budget gate this
/// surface never treats an unavailable marker as a zero-token call and never blocks cleanup.
pub fn batch_telemetry_summary(
    work: &Path,
    batch_id: &str,
    events_outbox_enabled: bool,
) -> Result<BatchTelemetrySummary, TelemetryUnavailable> {
    batch_telemetry_summary_with_pricing(
        work,
        batch_id,
        events_outbox_enabled,
        &PricingTable::default(),
    )
}

pub fn batch_telemetry_summary_with_pricing(
    work: &Path,
    batch_id: &str,
    events_outbox_enabled: bool,
    pricing: &PricingTable,
) -> Result<BatchTelemetrySummary, TelemetryUnavailable> {
    if !events_outbox_enabled {
        return Err(TelemetryUnavailable::EventsOutboxDisabled);
    }
    let path = work.join(OUTBOX_FILE);
    if !path.is_file() {
        return Err(TelemetryUnavailable::EventsFileMissing);
    }
    let _guard = lock_outbox().map_err(|_| TelemetryUnavailable::ReadFailed)?;
    let mut reader = TailReader::new(path);
    let events = reader
        .poll_all()
        .map_err(|_| TelemetryUnavailable::ReadFailed)?;
    if reader.stats().skipped_invalid > 0 {
        return Err(TelemetryUnavailable::InvalidEventRecord);
    }
    if reader.has_unterminated_tail() {
        return Err(TelemetryUnavailable::UnterminatedEventRecord);
    }

    let mut summary = BatchTelemetrySummary {
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
    };
    let mut task_batches = BTreeMap::<String, String>::new();
    for event in temporally_ordered(events) {
        observe_task_batch(&event, &mut task_batches);
        match event.event_type {
            EventType::CodexAttempt if event.batch_id.as_deref() == Some(batch_id) => {
                summarize_codex_attempt(&mut summary, &event)?
            }
            EventType::UsageRecorded if usage_belongs_to_batch(&event, &task_batches, batch_id) => {
                summarize_usage(&mut summary, &event, pricing)?
            }
            _ => {}
        }
    }
    Ok(summary)
}

fn temporally_ordered(events: Vec<Event>) -> Vec<Event> {
    let mut indexed = events.into_iter().enumerate().collect::<Vec<_>>();
    indexed.sort_by_key(|(sequence, event)| {
        (
            telemetry_epoch_millis(&event.occurred_at).unwrap_or(u64::MAX),
            *sequence,
        )
    });
    indexed.into_iter().map(|(_, event)| event).collect()
}

fn observe_task_batch(event: &Event, task_batches: &mut BTreeMap<String, String>) {
    if event.event_type == EventType::TaskCaptured
        && let (Some(task_id), Some(batch_id)) = (&event.task_id, &event.batch_id)
    {
        task_batches.insert(task_id.clone(), batch_id.clone());
    }
}

fn usage_belongs_to_batch(
    event: &Event,
    task_batches: &BTreeMap<String, String>,
    batch_id: &str,
) -> bool {
    event
        .task_id
        .as_ref()
        .and_then(|task_id| task_batches.get(task_id))
        .map(String::as_str)
        .or(event.batch_id.as_deref())
        == Some(batch_id)
}

fn summarize_codex_attempt(
    summary: &mut BatchTelemetrySummary,
    event: &Event,
) -> Result<(), TelemetryUnavailable> {
    validate_complete_codex_attempt(event)?;
    match event.payload.get("outcome").and_then(Value::as_str) {
        Some("success") => summary.codex_successes = summary.codex_successes.saturating_add(1),
        Some("fallback") => {
            let reason = event
                .payload
                .get("outcome_reason")
                .and_then(Value::as_str)
                .filter(|reason| safe_codex_reason(reason))
                .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
            summary.codex_fallbacks = summary.codex_fallbacks.saturating_add(1);
            let count = summary
                .codex_fallback_reasons
                .entry(reason.to_owned())
                .or_default();
            *count = count.saturating_add(1);
        }
        Some("failed")
            if event
                .payload
                .get("outcome_reason")
                .and_then(Value::as_str)
                .is_some_and(safe_codex_reason) =>
        {
            summary.codex_failures = summary.codex_failures.saturating_add(1)
        }
        _ => return Err(TelemetryUnavailable::InvalidEventRecord),
    }
    Ok(())
}

pub(crate) fn validate_complete_codex_attempt(event: &Event) -> Result<(), TelemetryUnavailable> {
    let task_id = event
        .task_id
        .as_deref()
        .filter(|task_id| is_task_id(task_id))
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let payload_task = event
        .payload
        .get("task_id")
        .and_then(Value::as_str)
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let role = event
        .payload
        .get("role")
        .and_then(Value::as_str)
        .filter(|role| matches!(*role, "coder" | "reviewer"))
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let mode = event
        .payload
        .get("mode")
        .and_then(Value::as_str)
        .filter(|mode| matches!(*mode, "full" | "augment" | "fix"))
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let attempt = event
        .payload
        .get("attempt_number")
        .and_then(Value::as_u64)
        .filter(|attempt| *attempt > 0 && *attempt <= u64::from(u32::MAX))
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let started_at = event
        .payload
        .get("started_at")
        .and_then(Value::as_str)
        .and_then(telemetry_epoch_millis)
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let ended_at_text = event
        .payload
        .get("ended_at")
        .and_then(Value::as_str)
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let ended_at =
        telemetry_epoch_millis(ended_at_text).ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let duration = event
        .payload
        .get("duration_ms")
        .and_then(Value::as_u64)
        .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
    let scalar_config_valid = event
        .payload
        .get("effective_model")
        .and_then(Value::as_str)
        .is_some_and(safe_scalar)
        && event
            .payload
            .get("effective_reasoning")
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "low" | "medium" | "high" | "xhigh"))
        && event
            .payload
            .get("effective_sandbox")
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "read-only" | "workspace-write"))
        && event
            .payload
            .get("effective_network")
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "on" | "off"));
    let exit_valid = matches!(event.payload.get("exit_code"), Some(Value::Null))
        || event
            .payload
            .get("exit_code")
            .and_then(Value::as_i64)
            .is_some();
    let outcome = event.payload.get("outcome").and_then(Value::as_str);
    let reason = event.payload.get("outcome_reason");
    let outcome_valid = match outcome {
        Some("success") => reason == Some(&Value::Null),
        Some("fallback" | "failed") => reason
            .and_then(Value::as_str)
            .is_some_and(safe_codex_reason),
        _ => false,
    };
    let expected_id = deterministic_event_id(&format!(
        "orchestra/codex.attempt/{task_id}/{role}/{mode}/{attempt}"
    ));
    if event.schema_version != SCHEMA_VERSION
        || event.payload_version != 1
        || event.actor.kind != ActorKind::Agent
        || event.actor.name != "processor"
        || event.payload.len() != 14
        || payload_task != task_id
        || ended_at_text != event.occurred_at
        || ended_at < started_at
        || duration != ended_at.saturating_sub(started_at)
        || !scalar_config_valid
        || !exit_valid
        || !outcome_valid
        || event.event_id != expected_id
    {
        return Err(TelemetryUnavailable::InvalidEventRecord);
    }
    Ok(())
}

fn safe_scalar(value: &str) -> bool {
    !value.is_empty() && value.len() <= 160 && !value.chars().any(char::is_control)
}

fn telemetry_epoch_millis(value: &str) -> Option<u64> {
    let year = value.get(0..4)?.parse::<u32>().ok()?;
    let month = value.get(5..7)?.parse::<u32>().ok()?;
    let day = value.get(8..10)?.parse::<u32>().ok()?;
    let hour = value.get(11..13)?.parse::<u32>().ok()?;
    let minute = value.get(14..16)?.parse::<u32>().ok()?;
    let second = value.get(17..19)?.parse::<u32>().ok()?;
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if day == 0 || day > days || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    iso_to_epoch_millis(value)
}

fn safe_codex_reason(reason: &str) -> bool {
    if matches!(
        reason,
        "DIFF_TOO_LARGE"
            | "SMOKE_FAILED"
            | "JJ_DRIFT"
            | "EMPTY_DIFF"
            | "CODEX_UNAVAILABLE"
            | "CODEX_FAILED"
            | "OTHER_FAILURE"
    ) {
        return true;
    }
    reason.strip_prefix("ENV_LIMIT/").is_some_and(|class| {
        !class.is_empty()
            && class.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
    })
}

fn summarize_usage(
    summary: &mut BatchTelemetrySummary,
    event: &Event,
    pricing: &PricingTable,
) -> Result<(), TelemetryUnavailable> {
    match event
        .payload
        .get("usage_availability")
        .and_then(Value::as_str)
    {
        Some("unavailable") if is_unavailable_marker(&event.payload) => {
            summary.usage.unmetered_events = summary.usage.unmetered_events.saturating_add(1);
            aggregate_cost(summary, event, CostEstimate::unknown());
            return Ok(());
        }
        Some("available") | None => {}
        _ => return Err(TelemetryUnavailable::MalformedActualUsage),
    }
    let estimated = event
        .payload
        .get("estimated")
        .and_then(Value::as_bool)
        .ok_or(TelemetryUnavailable::MalformedActualUsage)?;
    let total = usage_total(&event.payload);
    let provider_usage = ProviderUsage::from_fields(
        event.payload.get("input_tokens").and_then(Value::as_u64),
        event.payload.get("output_tokens").and_then(Value::as_u64),
        event
            .payload
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64),
        event
            .payload
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64),
        total,
    );
    let model = event
        .payload
        .get("model")
        .and_then(Value::as_str)
        .filter(|value| safe_scalar(value));
    let source = event
        .payload
        .get("source")
        .and_then(Value::as_str)
        .and_then(UsageSource::from_event);
    let cost = provider_usage
        .zip(source)
        .zip(model)
        .map_or_else(CostEstimate::unknown, |((usage, source), model)| {
            estimate_usage_cost(usage, source, model, pricing)
        });
    aggregate_cost(summary, event, cost);
    if estimated {
        if let Some(total) = total {
            summary.usage.estimated_tokens = summary
                .usage
                .estimated_tokens
                .checked_add(total)
                .ok_or(TelemetryUnavailable::MalformedActualUsage)?;
            summary.usage.estimated_events = summary.usage.estimated_events.saturating_add(1);
        }
    } else {
        let total = total.ok_or(TelemetryUnavailable::MalformedActualUsage)?;
        summary.usage.actual_tokens = summary
            .usage
            .actual_tokens
            .checked_add(total)
            .ok_or(TelemetryUnavailable::MalformedActualUsage)?;
        summary.usage.actual_events = summary.usage.actual_events.saturating_add(1);
        let source = event
            .payload
            .get("source")
            .and_then(Value::as_str)
            .and_then(UsageSource::from_event)
            .ok_or(TelemetryUnavailable::MalformedActualUsage)?;
        let value = summary
            .actual_by_source
            .entry(match source {
                UsageSource::Claude => "claude".to_owned(),
                UsageSource::Codex => "codex".to_owned(),
            })
            .or_default();
        *value = value
            .checked_add(total)
            .ok_or(TelemetryUnavailable::MalformedActualUsage)?;
    }
    Ok(())
}

fn aggregate_cost(summary: &mut BatchTelemetrySummary, event: &Event, cost: CostEstimate) {
    summary.estimated_cost.merge(cost);
    let model = event
        .payload
        .get("model")
        .and_then(Value::as_str)
        .filter(|value| safe_scalar(value))
        .unwrap_or("unknown-model");
    summary
        .cost_by_model
        .entry(model.to_owned())
        .or_default()
        .merge(cost);
    let role = event
        .payload
        .get("role")
        .and_then(Value::as_str)
        .filter(|value| safe_scalar(value))
        .unwrap_or("unknown-role");
    summary
        .cost_by_role
        .entry(role.to_owned())
        .or_default()
        .merge(cost);
}

/// Legacy metrics prefers an explicit total. The native writer uses `total_tokens`, while the
/// aliases/components retain read compatibility with old, already-durable usage events.
fn usage_total(payload: &Map<String, Value>) -> Option<u64> {
    for key in ["total_tokens", "tokens", "token_count"] {
        if let Some(value) = payload.get(key) {
            return value.as_u64();
        }
    }
    let mut total = 0_u64;
    let mut found = false;
    for key in [
        "input_tokens",
        "output_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
    ] {
        if let Some(value) = payload.get(key) {
            let value = value.as_u64()?;
            total = total.checked_add(value)?;
            found = true;
        }
    }
    found.then_some(total)
}

/// Strict task-facing timing fact carried by `operation.completed`.  The event remains an
/// envelope-level [`Event`] on disk; this projection is the privacy allowlist and semantic
/// validator shared by native writers and the immutable task-archive renderer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationCompleted {
    pub operation: String,
    pub role: String,
    pub mode: String,
    pub attempt_number: u64,
    pub scope: OperationScope,
    pub executor_kind: OperationExecutorKind,
    pub started_at: String,
    pub ended_at: String,
    pub duration_ms: u64,
    pub outcome: OperationOutcome,
    pub shared_task_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationScope {
    Task,
    Cohort,
    Integration,
}

impl OperationScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Cohort => "cohort",
            Self::Integration => "integration",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "task" => Self::Task,
            "cohort" => Self::Cohort,
            "integration" => Self::Integration,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationExecutorKind {
    Model,
    Tool,
    External,
}

impl OperationExecutorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Tool => "tool",
            Self::External => "external",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "model" => Self::Model,
            "tool" => Self::Tool,
            "external" => Self::External,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationOutcome {
    Success,
    Fallback,
    Failed,
    Cancelled,
    Timeout,
    Skipped,
}

impl OperationOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Fallback => "fallback",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Skipped => "skipped",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "success" => Self::Success,
            "fallback" => Self::Fallback,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            "timeout" => Self::Timeout,
            "skipped" => Self::Skipped,
            _ => return None,
        })
    }
}

const OPERATION_KEYS: [&str; 10] = [
    "operation",
    "role",
    "mode",
    "attempt_number",
    "scope",
    "executor_kind",
    "started_at",
    "ended_at",
    "duration_ms",
    "outcome",
];

const MODEL_OPERATIONS: [&str; 9] = [
    "planning",
    "coding",
    "review",
    "review_fix",
    "merge",
    "integration_review",
    "integration_fix",
    "ci_fix",
    "knowledge_curate",
];

const NON_MODEL_OPERATIONS: [&str; 3] = ["verification", "publish", "ci_wait"];
const CORE_OPERATIONS: [&str; 7] = [
    "planning",
    "coding",
    "review",
    "merge",
    "integration_review",
    "verification",
    "publish",
];

impl OperationCompleted {
    /// Decode the strict scalar payload and verify the replay-stable UUID coordinate.  Unknown
    /// keys are rejected here even though the generic event reader remains forward-compatible.
    pub fn from_event(event: &Event) -> Result<Self, TelemetryUnavailable> {
        if event.event_type != EventType::OperationCompleted
            || event.schema_version != SCHEMA_VERSION
            || event.payload_version != 1
            || event
                .batch_id
                .as_deref()
                .is_none_or(|id| !valid_batch_id(id))
            || event.task_id.as_deref().is_none_or(|id| !is_task_id(id))
            || event.payload.len() != OPERATION_KEYS.len() + 1
            || !OPERATION_KEYS
                .iter()
                .all(|key| event.payload.contains_key(*key))
            || !event.payload.contains_key("shared_task_count")
        {
            return Err(TelemetryUnavailable::InvalidEventRecord);
        }
        let token = |key: &str| {
            event
                .payload
                .get(key)
                .and_then(Value::as_str)
                .filter(|value| lower_token(value))
                .map(str::to_owned)
                .ok_or(TelemetryUnavailable::InvalidEventRecord)
        };
        let operation = token("operation")?;
        let role = token("role")?;
        let mode = token("mode")?;
        let attempt_number = event
            .payload
            .get("attempt_number")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0)
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
        let shared_task_count = event
            .payload
            .get("shared_task_count")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0)
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
        let scope = event
            .payload
            .get("scope")
            .and_then(Value::as_str)
            .and_then(OperationScope::parse)
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
        let executor_kind = event
            .payload
            .get("executor_kind")
            .and_then(Value::as_str)
            .and_then(OperationExecutorKind::parse)
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
        let outcome = event
            .payload
            .get("outcome")
            .and_then(Value::as_str)
            .and_then(OperationOutcome::parse)
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
        let started_at = event
            .payload
            .get("started_at")
            .and_then(Value::as_str)
            .filter(|value| telemetry_epoch_millis(value).is_some())
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?
            .to_owned();
        let ended_at = event
            .payload
            .get("ended_at")
            .and_then(Value::as_str)
            .filter(|value| telemetry_epoch_millis(value).is_some())
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?
            .to_owned();
        let duration_ms = event
            .payload
            .get("duration_ms")
            .and_then(Value::as_u64)
            .ok_or(TelemetryUnavailable::InvalidEventRecord)?;
        if telemetry_epoch_millis(&ended_at) < telemetry_epoch_millis(&started_at)
            || (scope == OperationScope::Task && shared_task_count != 1)
            || (MODEL_OPERATIONS.contains(&operation.as_str())
                && executor_kind != OperationExecutorKind::Model)
            || (NON_MODEL_OPERATIONS.contains(&operation.as_str())
                && executor_kind == OperationExecutorKind::Model)
        {
            return Err(TelemetryUnavailable::InvalidEventRecord);
        }
        let expected_id = deterministic_event_id(&format!(
            "orchestra/operation.completed/{}/{}/{operation}/{role}/{mode}/{attempt_number}",
            event.batch_id.as_deref().expect("validated above"),
            event.task_id.as_deref().expect("validated above"),
        ));
        if event.event_id != expected_id {
            return Err(TelemetryUnavailable::InvalidEventRecord);
        }
        Ok(Self {
            operation,
            role,
            mode,
            attempt_number,
            scope,
            executor_kind,
            started_at,
            ended_at,
            duration_ms,
            outcome,
            shared_task_count,
        })
    }

    /// Build a fully validated event. The caller chooses only closed typed values and safe
    /// lowercase role/mode/operation tokens; no arbitrary diagnostic text can enter the payload.
    pub fn to_event(
        &self,
        batch_id: &str,
        task_id: &str,
        occurred_at: &str,
    ) -> Result<Event, TelemetryUnavailable> {
        if !valid_batch_id(batch_id)
            || !is_task_id(task_id)
            || !lower_token(&self.operation)
            || !lower_token(&self.role)
            || !lower_token(&self.mode)
            || self.attempt_number == 0
            || self.shared_task_count == 0
        {
            return Err(TelemetryUnavailable::InvalidEventRecord);
        }
        let mut payload = Map::new();
        payload.insert("operation".into(), Value::from(self.operation.clone()));
        payload.insert("role".into(), Value::from(self.role.clone()));
        payload.insert("mode".into(), Value::from(self.mode.clone()));
        payload.insert("attempt_number".into(), Value::from(self.attempt_number));
        payload.insert("scope".into(), Value::from(self.scope.as_str()));
        payload.insert(
            "executor_kind".into(),
            Value::from(self.executor_kind.as_str()),
        );
        payload.insert("started_at".into(), Value::from(self.started_at.clone()));
        payload.insert("ended_at".into(), Value::from(self.ended_at.clone()));
        payload.insert("duration_ms".into(), Value::from(self.duration_ms));
        payload.insert("outcome".into(), Value::from(self.outcome.as_str()));
        payload.insert(
            "shared_task_count".into(),
            Value::from(self.shared_task_count),
        );
        let event = Event {
            schema_version: SCHEMA_VERSION,
            event_id: deterministic_event_id(&format!(
                "orchestra/operation.completed/{batch_id}/{task_id}/{}/{}/{}/{}",
                self.operation, self.role, self.mode, self.attempt_number
            )),
            occurred_at: occurred_at.to_owned(),
            event_type: EventType::OperationCompleted,
            actor: crate::events::Actor {
                kind: ActorKind::Agent,
                name: "engine".into(),
            },
            batch_id: Some(batch_id.to_owned()),
            task_id: Some(task_id.to_owned()),
            payload_version: 1,
            payload,
        };
        Self::from_event(&event)?;
        Ok(event)
    }
}

fn valid_batch_id(value: &str) -> bool {
    value.strip_prefix("B-").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    })
}

fn lower_token(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskMetricsStatus {
    Ok,
    Partial,
    NoData,
}

impl TaskMetricsStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Partial => "partial",
            Self::NoData => "no_data",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskOperationMetrics {
    pub operation: OperationCompleted,
    pub allocated_duration_ms: f64,
    pub usage_status: &'static str,
    pub actual_tokens: Option<f64>,
    pub estimated_tokens: Option<f64>,
    pub unavailable_events: u64,
    pub matched_events: u64,
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskExecutionMetrics {
    pub status: TaskMetricsStatus,
    pub task_id: String,
    pub batch_id: String,
    pub lead_time_ms: Option<f64>,
    pub operation_time_ms: Option<f64>,
    pub actual_tokens: Option<f64>,
    pub estimated_tokens: Option<f64>,
    pub model_operation_count: u64,
    pub unmetered_operation_count: u64,
    pub operations: Vec<TaskOperationMetrics>,
    pub reasons: Vec<String>,
    pub events_outbox_enabled: bool,
    pub events_present: bool,
    pub event_count: usize,
    pub skipped_jsonl_lines: u64,
}

/// Read-only immutable task archive projection. Invalid operation facts are omitted and called
/// out as partial telemetry; missing/disabled telemetry is explicit no-data, never a zero.
pub fn task_execution_metrics(
    work: &Path,
    task_id: &str,
    batch_id: &str,
    events_outbox_enabled: bool,
) -> Result<TaskExecutionMetrics, TelemetryUnavailable> {
    if !is_task_id(task_id) || !valid_batch_id(batch_id) {
        return Err(TelemetryUnavailable::InvalidEventRecord);
    }
    let path = work.join(OUTBOX_FILE);
    let events_present = path.is_file();
    let mut reader = TailReader::new(path);
    let events = reader
        .poll_all()
        .map_err(|_| TelemetryUnavailable::ReadFailed)?;
    let skipped_jsonl_lines =
        reader.stats().skipped_invalid + u64::from(reader.has_unterminated_tail());
    let event_count = events.len();
    let mut reasons = Vec::new();
    if !events_outbox_enabled {
        reasons.push("EVENTS_OUTBOX=off".into());
    }
    if !events_present {
        reasons.push("events.jsonl отсутствует".into());
    }
    if skipped_jsonl_lines > 0 {
        reasons.push(format!(
            "пропущено некорректных строк JSONL: {skipped_jsonl_lines}"
        ));
    }

    let mut task_batches = BTreeMap::<String, String>::new();
    let mut captured = None::<u64>;
    let mut done = None::<u64>;
    let mut operation_events = Vec::<(Event, OperationCompleted)>::new();
    let mut usage_events = Vec::<Event>::new();
    for event in temporally_ordered(events) {
        observe_task_batch(&event, &mut task_batches);
        let attributed_batch = event
            .task_id
            .as_ref()
            .and_then(|id| task_batches.get(id))
            .map(String::as_str)
            .or(event.batch_id.as_deref());
        match event.event_type {
            EventType::TaskCaptured
                if event.task_id.as_deref() == Some(task_id)
                    && attributed_batch == Some(batch_id) =>
            {
                if let Some(at) = telemetry_epoch_millis(&event.occurred_at) {
                    captured = Some(captured.map_or(at, |current| current.min(at)));
                }
            }
            EventType::TaskStatusChanged
                if event.task_id.as_deref() == Some(task_id)
                    && attributed_batch == Some(batch_id)
                    && event
                        .payload
                        .get("to")
                        .and_then(Value::as_str)
                        .is_some_and(|to| {
                            matches!(to.to_lowercase().as_str(), "выполнена" | "done")
                        }) =>
            {
                if let Some(at) = telemetry_epoch_millis(&event.occurred_at) {
                    done = Some(done.map_or(at, |current| current.max(at)));
                }
            }
            EventType::OperationCompleted
                if event.task_id.as_deref() == Some(task_id)
                    && event.batch_id.as_deref() == Some(batch_id) =>
            {
                match OperationCompleted::from_event(&event) {
                    Ok(operation) => operation_events.push((event, operation)),
                    Err(_) => reasons.push(format!(
                        "повреждённая operation.completed для строки {}",
                        operation_events.len() + 1
                    )),
                }
            }
            EventType::UsageRecorded if usage_belongs_to_batch(&event, &task_batches, batch_id) => {
                usage_events.push(event)
            }
            _ => {}
        }
    }
    if operation_events.is_empty() {
        reasons.push("operation.completed отсутствуют".into());
    }
    operation_events.sort_by(|(_, left), (_, right)| {
        telemetry_epoch_millis(&left.started_at)
            .cmp(&telemetry_epoch_millis(&right.started_at))
            .then_with(|| left.operation.cmp(&right.operation))
    });

    let mut operations = Vec::new();
    let mut observed = BTreeSet::new();
    let mut seen_model_calls = BTreeSet::new();
    let mut operation_time_ms = 0.0;
    let mut actual_total = 0.0;
    let mut estimated_total = 0.0;
    let mut actual_observed = false;
    let mut estimated_observed = false;
    let mut model_operation_count = 0_u64;
    let mut unmetered_operation_count = 0_u64;
    for (_, operation) in operation_events {
        observed.insert(operation.operation.clone());
        let share = operation.shared_task_count as f64;
        // Match the legacy immutable projection: round each task's share before adding it to
        // the task total. Deferring this until Markdown formatting can make the total disagree
        // with the sum of its visible rows for non-even cohort sizes.
        let allocated_duration_ms = round_to_two(operation.duration_ms as f64 / share);
        operation_time_ms += allocated_duration_ms;
        let expects_usage = operation.executor_kind == OperationExecutorKind::Model
            && operation.outcome != OperationOutcome::Skipped;
        if operation.executor_kind == OperationExecutorKind::Model {
            model_operation_count = model_operation_count.saturating_add(1);
        }
        let call_key = format!(
            "{}|{}|{}|{}",
            operation.scope.as_str(),
            operation.role,
            operation.mode,
            operation.attempt_number
        );
        let duplicate = expects_usage && !seen_model_calls.insert(call_key);
        let usage_task_id = match operation.scope {
            OperationScope::Task => task_id,
            OperationScope::Cohort => "_cohort",
            OperationScope::Integration => "_integration",
        };
        let mut actual = 0_u64;
        let mut estimated = 0_u64;
        let mut actual_here = false;
        let mut estimated_here = false;
        let mut unavailable_events = 0_u64;
        let mut matched_events = 0_u64;
        let mut sources = BTreeSet::new();
        if expects_usage && !duplicate {
            for usage in &usage_events {
                if usage.task_id.as_deref() != Some(usage_task_id)
                    || usage.payload.get("role").and_then(Value::as_str)
                        != Some(operation.role.as_str())
                    || usage.payload.get("mode").and_then(Value::as_str)
                        != Some(operation.mode.as_str())
                    || usage.payload.get("attempt_number").and_then(Value::as_u64)
                        != Some(operation.attempt_number)
                {
                    continue;
                }
                matched_events = matched_events.saturating_add(1);
                if let Some(source) = usage.payload.get("source").and_then(Value::as_str) {
                    sources.insert(source.to_owned());
                }
                if usage
                    .payload
                    .get("usage_availability")
                    .and_then(Value::as_str)
                    == Some("unavailable")
                {
                    unavailable_events = unavailable_events.saturating_add(1);
                    continue;
                }
                let Some(total) = usage_total(&usage.payload) else {
                    continue;
                };
                if usage.payload.get("estimated").and_then(Value::as_bool) == Some(true) {
                    estimated = estimated.saturating_add(total);
                    estimated_here = true;
                } else if usage.payload.get("estimated").and_then(Value::as_bool) == Some(false) {
                    actual = actual.saturating_add(total);
                    actual_here = true;
                }
            }
        }
        let usage_status = if !expects_usage {
            "not_applicable"
        } else if duplicate {
            "ambiguous"
        } else if matched_events == 0 {
            "missing"
        } else if unavailable_events > 0 && (actual_here || estimated_here) {
            "partial"
        } else if unavailable_events > 0 {
            "unavailable"
        } else if actual_here || estimated_here {
            "available"
        } else {
            "missing"
        };
        if expects_usage && usage_status != "available" {
            unmetered_operation_count = unmetered_operation_count.saturating_add(1);
            reasons.push(format!(
                "usage {usage_status}: {}/{}/{}/{}",
                operation.operation, operation.role, operation.mode, operation.attempt_number
            ));
        }
        let allocated_actual = actual_here.then_some(round_to_two(actual as f64 / share));
        let allocated_estimated = estimated_here.then_some(round_to_two(estimated as f64 / share));
        if let Some(value) = allocated_actual {
            actual_total += value;
            actual_observed = true;
        }
        if let Some(value) = allocated_estimated {
            estimated_total += value;
            estimated_observed = true;
        }
        operations.push(TaskOperationMetrics {
            operation,
            allocated_duration_ms,
            usage_status,
            actual_tokens: allocated_actual,
            estimated_tokens: allocated_estimated,
            unavailable_events,
            matched_events,
            sources: sources.into_iter().collect(),
        });
    }
    for required in CORE_OPERATIONS {
        if !observed.contains(required) {
            reasons.push(format!("обязательная операция отсутствует: {required}"));
        }
    }
    let lead_time_ms = captured
        .zip(done)
        .and_then(|(capture, done)| (done >= capture).then_some((done - capture) as f64));
    if lead_time_ms.is_none() {
        reasons.push("lead time capture→done недоступен".into());
    }
    let mut unique_reasons = BTreeSet::new();
    reasons.retain(|reason| unique_reasons.insert(reason.clone()));
    let status = if operations.is_empty() {
        TaskMetricsStatus::NoData
    } else if reasons.is_empty() {
        TaskMetricsStatus::Ok
    } else {
        TaskMetricsStatus::Partial
    };
    Ok(TaskExecutionMetrics {
        status,
        task_id: task_id.into(),
        batch_id: batch_id.into(),
        lead_time_ms,
        operation_time_ms: (!operations.is_empty()).then_some(round_to_two(operation_time_ms)),
        actual_tokens: actual_observed.then_some(round_to_two(actual_total)),
        estimated_tokens: estimated_observed.then_some(round_to_two(estimated_total)),
        model_operation_count,
        unmetered_operation_count,
        operations,
        reasons,
        events_outbox_enabled,
        events_present,
        event_count,
        skipped_jsonl_lines,
    })
}

fn round_to_two(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// Render the exact immutable Markdown block appended after a task descriptor.
pub fn format_task_execution_metrics(metrics: &TaskExecutionMetrics) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "#### Метрики выполнения");
    let _ = writeln!(
        output,
        "<!-- orchestra/task-execution-metrics@1 task_id={} batch_id={} status={} -->",
        metrics.task_id,
        metrics.batch_id,
        metrics.status.as_str()
    );
    let _ = writeln!(
        output,
        "- Полное время задачи (capture → done): {}.",
        format_duration_exact(metrics.lead_time_ms)
    );
    let _ = writeln!(
        output,
        "- Сумма операций с распределением общих затрат: {}; операций: {}.",
        format_duration_exact(metrics.operation_time_ms),
        metrics.operations.len()
    );
    let _ = writeln!(
        output,
        "- Токены: actual={}; estimated={}; операций без полного token usage: {}.",
        format_number(metrics.actual_tokens),
        format_number(metrics.estimated_tokens),
        metrics.unmetered_operation_count
    );
    let reasons = if metrics.reasons.is_empty() {
        "все ожидаемые данные присутствуют".into()
    } else {
        metrics.reasons.join("; ")
    };
    let _ = writeln!(
        output,
        "- Полнота телеметрии: {} — {reasons}.",
        metrics.status.as_str()
    );
    output.push('\n');
    output.push_str("| # | Операция | Итерация | Scope / доля | Роль / режим | Время | В зачёт задачи | Actual tokens | Estimated tokens | Usage | Результат |\n");
    output.push_str("|---:|---|---:|---|---|---:|---:|---:|---:|---|---|\n");
    for (index, operation) in metrics.operations.iter().enumerate() {
        let share = if operation.operation.shared_task_count > 1 {
            format!(
                "{} · 1/{}",
                operation.operation.scope.as_str(),
                operation.operation.shared_task_count
            )
        } else {
            operation.operation.scope.as_str().into()
        };
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} / {} | {} | {} | {} | {} | {} | {} |",
            index + 1,
            operation.operation.operation,
            operation.operation.attempt_number,
            share,
            operation.operation.role,
            operation.operation.mode,
            format_duration_exact(Some(operation.operation.duration_ms as f64)),
            format_duration_exact(Some(operation.allocated_duration_ms)),
            format_optional_number(operation.actual_tokens),
            format_optional_number(operation.estimated_tokens),
            operation.usage_status,
            operation.operation.outcome.as_str(),
        );
    }
    output
}

pub fn format_task_execution_metrics_error(task_id: &str, batch_id: &str) -> String {
    format!(
        "#### Метрики выполнения\n<!-- orchestra/task-execution-metrics@1 task_id={task_id} batch_id={batch_id} status=error -->\n- Метрики недоступны.\n"
    )
}

fn format_optional_number(value: Option<f64>) -> String {
    value.map_or_else(|| "—".into(), |value| format_number(Some(value)))
}

fn format_number(value: Option<f64>) -> String {
    value.map_or_else(
        || "недоступно".into(),
        |value| {
            let rounded = (value * 100.0).round() / 100.0;
            if rounded.fract() == 0.0 {
                format!("{rounded:.0}")
            } else {
                format!("{rounded:.2}").trim_end_matches('0').to_owned()
            }
        },
    )
}

fn format_duration_exact(value: Option<f64>) -> String {
    let Some(milliseconds) = value else {
        return "недоступно".into();
    };
    let human = if milliseconds >= 86_400_000.0 {
        format!("{} д", format_number(Some(milliseconds / 86_400_000.0)))
    } else if milliseconds >= 3_600_000.0 {
        format!("{} ч", format_number(Some(milliseconds / 3_600_000.0)))
    } else if milliseconds >= 60_000.0 {
        format!("{} мин", format_number(Some(milliseconds / 60_000.0)))
    } else {
        format!("{} с", format_number(Some(milliseconds / 1_000.0)))
    };
    format!("{human} ({} ms)", format_number(Some(milliseconds)))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temp_work(name: &str) -> std::path::PathBuf {
        let work = std::env::temp_dir().join(format!(
            "orchestrail-telemetry-{name}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&work).unwrap();
        work
    }

    fn usage(event_id: &str, batch: &str, total: u64, estimated: bool) -> String {
        format!(
            r#"{{"schema_version":1,"event_id":"{event_id}","occurred_at":"2026-07-25T12:00:00Z","type":"usage.recorded","batch_id":"{batch}","actor":{{"kind":"tool","name":"supervisor"}},"payload":{{"total_tokens":{total},"estimated":{estimated}}}}}"#
        )
    }

    fn unavailable_usage(event_id: &str, batch: &str) -> String {
        format!(
            r#"{{"schema_version":1,"event_id":"{event_id}","occurred_at":"2026-07-25T12:00:00Z","type":"usage.recorded","batch_id":"{batch}","actor":{{"kind":"tool","name":"claude"}},"payload":{{"task_id":"T-1","role":"coder","mode":"full","attempt_number":1,"source":"claude","model":"default","usage_availability":"unavailable"}}}}"#
        )
    }

    #[test]
    fn explicit_provider_total_survives_unaddable_optional_components() {
        let usage = ProviderUsage::from_fields(Some(u64::MAX), Some(1), None, None, Some(42))
            .expect("an exact provider total remains usable");
        assert_eq!(usage.total_tokens, Some(42));
    }

    #[test]
    fn pricing_resolves_exact_then_dated_suffix_and_rejects_other_prefixes() {
        let pricing = PricingTable::default();
        let exact = pricing.resolve("gpt-5.6-terra").unwrap();
        assert_eq!(exact.model, "gpt-5.6-terra");
        let compact_date = pricing.resolve("gpt-5.6-terra-20251015").unwrap();
        assert_eq!(compact_date.model, "gpt-5.6-terra");
        let dashed_date = pricing.resolve("claude-sonnet-4-6-2025-10-15").unwrap();
        assert_eq!(dashed_date.model, "claude-sonnet-4-6");
        assert!(pricing.resolve("gpt-5.6-terra-preview").is_none());
        assert!(pricing.resolve("default").is_none());
        assert!(pricing.resolve("missing-model").is_none());
    }

    #[test]
    fn provider_usage_prices_codex_and_claude_cache_conventions_without_double_counting() {
        let usage = ProviderUsage::from_fields(
            Some(9_700_000),
            Some(100_000),
            Some(9_400_000),
            Some(0),
            Some(9_800_000),
        )
        .unwrap();
        let cost = estimate_usage_cost(
            usage,
            UsageSource::Codex,
            "gpt-5.6-terra",
            &PricingTable::default(),
        );
        assert_eq!(
            cost.nano_usd, 4_600_000_000,
            "0.30M non-cached input + 9.40M cached input + 0.10M output"
        );
        assert!(cost.estimated);
        assert!(!cost.unknown);

        let claude_usage = ProviderUsage::from_fields(
            Some(300_000),
            Some(100_000),
            Some(9_400_000),
            Some(0),
            Some(9_800_000),
        )
        .unwrap();
        let claude_cost = estimate_usage_cost(
            claude_usage,
            UsageSource::Claude,
            "gpt-5.6-terra",
            &PricingTable::default(),
        );
        assert_eq!(claude_cost.nano_usd, 4_600_000_000);
    }

    #[test]
    fn missing_price_or_unreconciled_components_are_unknown_without_panicking() {
        let usage = ProviderUsage::from_fields(Some(10), Some(5), None, None, Some(15)).unwrap();
        assert!(
            estimate_usage_cost(
                usage,
                UsageSource::Claude,
                "missing-model",
                &PricingTable::default()
            )
            .unknown
        );
        let total_only = ProviderUsage::from_fields(None, None, None, None, Some(15)).unwrap();
        assert!(
            estimate_usage_cost(
                total_only,
                UsageSource::Codex,
                "gpt-5.6-terra",
                &PricingTable::default()
            )
            .unknown
        );
    }

    #[test]
    fn estimated_and_unknown_flags_are_contagious_when_costs_aggregate() {
        let mut total = CostEstimate {
            nano_usd: 10,
            estimated: false,
            unknown: false,
        };
        total.merge(CostEstimate {
            nano_usd: 20,
            estimated: true,
            unknown: false,
        });
        total.merge(CostEstimate::unknown());
        assert_eq!(total.nano_usd, 30);
        assert!(total.estimated);
        assert!(total.unknown);
    }

    #[test]
    fn telemetry_clock_rejects_calendar_impossibilities_like_legacy_datetime() {
        assert_eq!(
            telemetry_epoch_millis("2024-02-29T23:59:59.9Z"),
            Some(1_709_251_199_900)
        );
        for invalid in [
            "2023-02-29T00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-01-00T00:00:00Z",
            "2026-01-01T24:00:00Z",
        ] {
            assert_eq!(telemetry_epoch_millis(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn counts_only_deduplicated_explicit_actual_usage_for_the_requested_batch() {
        let work = temp_work("actual");
        fs::write(
            work.join(OUTBOX_FILE),
            format!(
                "{}\n{}\n{}\n{}\n",
                usage("u-1", "B-1", 10, false),
                usage("u-1", "B-1", 999, false),
                usage("u-2", "B-1", 7, true),
                usage("u-3", "B-2", 50, false),
            ),
        )
        .unwrap();

        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Available(TokenUsage {
                actual_tokens: 10,
                estimated_tokens: 7,
                actual_events: 1,
                estimated_events: 1,
                unmetered_events: 0,
            })
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn attributes_legacy_task_usage_to_its_preceding_capture_batch() {
        let work = temp_work("legacy-attribution");
        let captured = r#"{"schema_version":1,"event_id":"c-1","occurred_at":"2026-07-25T12:00:00Z","type":"task.captured","batch_id":"B-1","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1}}"#;
        let legacy_usage = r#"{"schema_version":1,"event_id":"u-1","occurred_at":"2026-07-25T12:00:01Z","type":"usage.recorded","task_id":"T-1","actor":{"kind":"tool","name":"supervisor"},"payload":{"source":"claude","total_tokens":9,"estimated":false}}"#;
        fs::write(
            work.join(OUTBOX_FILE),
            format!("{captured}\n{legacy_usage}\n"),
        )
        .unwrap();

        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Available(TokenUsage {
                actual_tokens: 9,
                estimated_tokens: 0,
                actual_events: 1,
                estimated_events: 0,
                unmetered_events: 0,
            })
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn delayed_usage_is_attributed_in_event_time_not_append_order() {
        let work = temp_work("delayed-attribution");
        let first_capture = r#"{"schema_version":1,"event_id":"c-1","occurred_at":"2026-07-25T12:00:00Z","type":"task.captured","batch_id":"B-1","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1}}"#;
        let later_capture = r#"{"schema_version":1,"event_id":"c-2","occurred_at":"2026-07-25T13:00:00Z","type":"task.captured","batch_id":"B-2","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1}}"#;
        let delayed_usage = r#"{"schema_version":1,"event_id":"u-1","occurred_at":"2026-07-25T12:30:00Z","type":"usage.recorded","batch_id":"B-2","task_id":"T-1","actor":{"kind":"tool","name":"supervisor"},"payload":{"source":"claude","total_tokens":9,"estimated":false}}"#;
        fs::write(
            work.join(OUTBOX_FILE),
            format!("{first_capture}\n{later_capture}\n{delayed_usage}\n"),
        )
        .unwrap();

        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Available(TokenUsage {
                actual_tokens: 9,
                estimated_tokens: 0,
                actual_events: 1,
                estimated_events: 0,
                unmetered_events: 0,
            })
        );
        assert_eq!(
            cohort_token_usage(&work, "B-2", true),
            TokenTelemetrySnapshot::Available(TokenUsage {
                actual_tokens: 0,
                estimated_tokens: 0,
                actual_events: 0,
                estimated_events: 0,
                unmetered_events: 0,
            })
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn fails_closed_for_disabled_missing_invalid_or_incomplete_actual_telemetry() {
        let work = temp_work("unavailable");
        assert_eq!(
            cohort_token_usage(&work, "B-1", false),
            TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::EventsOutboxDisabled)
        );
        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::EventsFileMissing)
        );

        fs::write(work.join(OUTBOX_FILE), b"not-json\n").unwrap();
        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::InvalidEventRecord)
        );

        fs::write(
            work.join(OUTBOX_FILE),
            r#"{"schema_version":1,"event_id":"u-1","occurred_at":"2026-07-25T12:00:00Z","type":"usage.recorded","batch_id":"B-1","actor":{"kind":"tool","name":"supervisor"},"payload":{"estimated":false}}"#,
        )
        .unwrap();
        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::UnterminatedEventRecord)
        );

        fs::write(
            work.join(OUTBOX_FILE),
            format!(
                "{}\n",
                r#"{"schema_version":1,"event_id":"u-1","occurred_at":"2026-07-25T12:00:00Z","type":"usage.recorded","batch_id":"B-1","actor":{"kind":"tool","name":"supervisor"},"payload":{"estimated":false}}"#
            ),
        )
        .unwrap();
        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::MalformedActualUsage)
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn accepts_a_valid_zero_actual_snapshot() {
        let work = temp_work("zero");
        fs::write(work.join(OUTBOX_FILE), b"").unwrap();
        assert_eq!(
            cohort_token_usage(&work, "B-1", true),
            TokenTelemetrySnapshot::Available(TokenUsage {
                actual_tokens: 0,
                estimated_tokens: 0,
                actual_events: 0,
                estimated_events: 0,
                unmetered_events: 0,
            })
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn unmetered_marker_is_visible_by_default_and_fail_closed_only_when_strict() {
        let work = temp_work("unmetered");
        fs::write(
            work.join(OUTBOX_FILE),
            format!(
                "{}\n{}\n",
                usage("u-1", "B-1", 10, false),
                unavailable_usage("u-2", "B-1")
            ),
        )
        .unwrap();

        assert_eq!(
            cohort_token_usage_with_strict(&work, "B-1", true, false),
            TokenTelemetrySnapshot::Available(TokenUsage {
                actual_tokens: 10,
                estimated_tokens: 0,
                actual_events: 1,
                estimated_events: 0,
                unmetered_events: 1,
            })
        );
        assert_eq!(
            cohort_token_usage_with_strict(&work, "B-1", true, true),
            TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::UnmeteredUsage)
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn unavailable_marker_refuses_invented_token_fields() {
        let work = temp_work("bad-unmetered");
        let line = unavailable_usage("u-1", "B-1").replace(
            r#""usage_availability":"unavailable""#,
            r#""usage_availability":"unavailable","total_tokens":0"#,
        );
        fs::write(work.join(OUTBOX_FILE), format!("{line}\n")).unwrap();
        assert_eq!(
            cohort_token_usage_with_strict(&work, "B-1", true, false),
            TokenTelemetrySnapshot::Unavailable(TelemetryUnavailable::MalformedActualUsage)
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn operator_summary_rejects_an_incomplete_codex_attempt_payload() {
        let work = temp_work("incomplete-codex");
        let incomplete = r#"{"schema_version":1,"event_id":"a-1","occurred_at":"2026-07-25T12:00:00Z","type":"codex.attempt","batch_id":"B-1","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"outcome":"fallback","outcome_reason":"CODEX_FAILED"}}"#;
        fs::write(work.join(OUTBOX_FILE), format!("{incomplete}\n")).unwrap();
        assert_eq!(
            batch_telemetry_summary(&work, "B-1", true),
            Err(TelemetryUnavailable::InvalidEventRecord)
        );
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn operator_summary_aggregates_cost_by_model_and_role_with_unknown_contagion() {
        let work = temp_work("cost-summary");
        let known = r#"{"schema_version":1,"event_id":"u-known","occurred_at":"2026-07-25T12:00:00Z","type":"usage.recorded","batch_id":"B-1","task_id":"T-1","actor":{"kind":"tool","name":"codex"},"payload":{"task_id":"T-1","role":"coder","mode":"full","attempt_number":1,"source":"codex","model":"gpt-5.6-terra","input_tokens":1000000,"output_tokens":1000000,"cache_read_input_tokens":1000000,"cache_creation_input_tokens":1000000,"total_tokens":4000000,"estimated":false,"usage_availability":"available"}}"#;
        let unknown = r#"{"schema_version":1,"event_id":"u-unknown","occurred_at":"2026-07-25T12:00:01Z","type":"usage.recorded","batch_id":"B-1","task_id":"T-2","actor":{"kind":"tool","name":"codex"},"payload":{"task_id":"T-2","role":"reviewer","mode":"full","attempt_number":1,"source":"codex","model":"future-model","input_tokens":10,"output_tokens":5,"total_tokens":15,"estimated":true,"usage_availability":"available"}}"#;
        fs::write(work.join(OUTBOX_FILE), format!("{known}\n{unknown}\n")).unwrap();

        let summary = batch_telemetry_summary(&work, "B-1", true).unwrap();
        assert_eq!(summary.usage.actual_tokens, 4_000_000);
        assert_eq!(summary.usage.estimated_tokens, 15);
        assert_eq!(summary.estimated_cost.nano_usd, 17_750_000_000);
        assert!(summary.estimated_cost.estimated);
        assert!(summary.estimated_cost.unknown);
        assert_eq!(
            summary.cost_by_model["gpt-5.6-terra"].nano_usd,
            17_750_000_000
        );
        assert!(summary.cost_by_model["future-model"].unknown);
        assert_eq!(summary.cost_by_role["coder"].nano_usd, 17_750_000_000);
        assert!(summary.cost_by_role["reviewer"].unknown);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn operator_summary_has_no_cost_contribution_without_usage_events() {
        let work = temp_work("empty-cost-summary");
        fs::write(work.join(OUTBOX_FILE), b"").unwrap();

        let summary = batch_telemetry_summary(&work, "B-1", true).unwrap();
        assert_eq!(summary.usage.actual_events, 0);
        assert_eq!(summary.usage.estimated_events, 0);
        assert_eq!(summary.usage.unmetered_events, 0);
        assert_eq!(summary.estimated_cost, CostEstimate::default());
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn operator_summary_marks_an_unconfigured_default_model_as_unknown() {
        let work = temp_work("default-model-cost");
        let event = r#"{"schema_version":1,"event_id":"u-default","occurred_at":"2026-07-25T12:00:00Z","type":"usage.recorded","batch_id":"B-1","task_id":"T-1","actor":{"kind":"tool","name":"codex"},"payload":{"task_id":"T-1","role":"coder","mode":"full","attempt_number":1,"source":"codex","model":"default","input_tokens":9700000,"output_tokens":100000,"cache_read_input_tokens":9400000,"total_tokens":9800000,"estimated":false,"usage_availability":"available"}}"#;
        fs::write(work.join(OUTBOX_FILE), format!("{event}\n")).unwrap();

        let summary = batch_telemetry_summary(&work, "B-1", true).unwrap();
        assert_eq!(summary.usage.actual_events, 1);
        assert!(summary.estimated_cost.unknown);
        assert!(summary.cost_by_model["default"].unknown);
        let _ = fs::remove_dir_all(work);
    }

    #[test]
    fn task_archive_projection_allocates_shared_operations_and_never_invents_usage() {
        let work = temp_work("task-archive");
        let lifecycle = |event_type, event_id: &str, at: &str, payload: Map<String, Value>| {
            Event {
                schema_version: SCHEMA_VERSION,
                event_id: event_id.into(),
                occurred_at: at.into(),
                event_type,
                actor: crate::events::Actor {
                    kind: ActorKind::Agent,
                    name: "engine".into(),
                },
                batch_id: Some("B-task".into()),
                task_id: Some("T-500".into()),
                payload_version: 1,
                payload,
            }
            .to_json_line()
        };
        let operation = |name: &str,
                         role: &str,
                         mode: &str,
                         scope,
                         kind,
                         start: &str,
                         end: &str,
                         duration,
                         shared| {
            OperationCompleted {
                operation: name.into(),
                role: role.into(),
                mode: mode.into(),
                attempt_number: 1,
                scope,
                executor_kind: kind,
                started_at: start.into(),
                ended_at: end.into(),
                duration_ms: duration,
                outcome: OperationOutcome::Success,
                shared_task_count: shared,
            }
            .to_event("B-task", "T-500", end)
            .unwrap()
            .to_json_line()
        };
        let usage_line = |id: &str, task: &str, role: &str, total: Option<u64>| {
            let mut payload = Map::new();
            payload.insert("task_id".into(), Value::from(task));
            payload.insert("role".into(), Value::from(role));
            payload.insert("mode".into(), Value::from("full"));
            payload.insert("attempt_number".into(), Value::from(1));
            payload.insert("source".into(), Value::from("claude"));
            payload.insert("model".into(), Value::from("default"));
            if let Some(total) = total {
                payload.insert("total_tokens".into(), Value::from(total));
                payload.insert("estimated".into(), Value::Bool(false));
                payload.insert("usage_availability".into(), Value::from("available"));
            } else {
                payload.insert("usage_availability".into(), Value::from("unavailable"));
            }
            Event {
                schema_version: SCHEMA_VERSION,
                event_id: id.into(),
                occurred_at: "2026-07-12T10:04:01Z".into(),
                event_type: EventType::UsageRecorded,
                actor: crate::events::Actor {
                    kind: ActorKind::Tool,
                    name: "claude".into(),
                },
                batch_id: Some("B-task".into()),
                task_id: Some(task.into()),
                payload_version: 1,
                payload,
            }
            .to_json_line()
        };
        let mut done_payload = Map::new();
        done_payload.insert("from".into(), Value::from("опубликована"));
        done_payload.insert("to".into(), Value::from("выполнена"));
        let lines = [
            lifecycle(
                EventType::TaskCaptured,
                "captured",
                "2026-07-12T10:00:00Z",
                Map::new(),
            ),
            operation(
                "coding",
                "coder",
                "full",
                OperationScope::Task,
                OperationExecutorKind::Model,
                "2026-07-12T10:00:00Z",
                "2026-07-12T10:01:00Z",
                60_000,
                1,
            ),
            usage_line("usage-coding", "T-500", "coder", Some(1_000)),
            operation(
                "review",
                "reviewer",
                "full",
                OperationScope::Task,
                OperationExecutorKind::Model,
                "2026-07-12T10:01:00Z",
                "2026-07-12T10:02:00Z",
                60_000,
                1,
            ),
            usage_line("usage-review", "T-500", "reviewer", None),
            operation(
                "merge",
                "merger",
                "full",
                OperationScope::Integration,
                OperationExecutorKind::Model,
                "2026-07-12T10:02:00Z",
                "2026-07-12T10:04:00Z",
                120_001,
                3,
            ),
            usage_line("usage-merge", "_integration", "merger", Some(401)),
            lifecycle(
                EventType::TaskStatusChanged,
                "done",
                "2026-07-12T10:10:00Z",
                done_payload,
            ),
        ];
        fs::write(work.join(OUTBOX_FILE), format!("{}\n", lines.join("\n"))).unwrap();

        let metrics = task_execution_metrics(&work, "T-500", "B-task", true).unwrap();
        assert_eq!(metrics.status, TaskMetricsStatus::Partial);
        assert_eq!(metrics.lead_time_ms, Some(600_000.0));
        assert_eq!(metrics.operation_time_ms, Some(160_000.33));
        assert_eq!(metrics.actual_tokens, Some(1_133.67));
        assert_eq!(metrics.unmetered_operation_count, 1);
        assert_eq!(metrics.operations[2].allocated_duration_ms, 40_000.33);
        assert_eq!(metrics.operations[2].actual_tokens, Some(133.67));
        let markdown = format_task_execution_metrics(&metrics);
        assert!(markdown.contains("orchestra/task-execution-metrics@1"));
        assert!(markdown.contains("2.67 мин (160000.33 ms)"));
        assert!(markdown.contains("| merge | 1 | integration · 1/3 |"));
        assert!(markdown.contains("actual=1133.67"));
        let _ = fs::remove_dir_all(work);
    }
}
