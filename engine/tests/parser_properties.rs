use orchestrail_engine::contract::{
    Sentinel, Status, detect_sentinel, parse_changed_files, parse_outcome, parse_review,
};
use orchestrail_engine::events::{ActorKind, EventType, parse_line};
use orchestrail_engine::jsonline::{JsonValue, top_level};
use orchestrail_engine::state::canonical::{
    CohortAdmission, IntegrationState, TaskState, suffix_field,
};
use orchestrail_engine::state::parse_queue;
use proptest::prelude::*;
use serde_json::{Map, Value, json};

fn ascii_token() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Za-z0-9._/-]{1,32}").expect("valid token regex")
}

fn title_text() -> impl Strategy<Value = String> {
    ascii_token().prop_map(|token| format!("task {token} x"))
}

fn line_suffix() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[^\r\n]{0,80}").expect("valid suffix regex")
}

fn json_scalar() -> BoxedStrategy<Value> {
    prop_oneof![
        any::<bool>().prop_map(Value::Bool),
        (-1_000_000i64..=1_000_000).prop_map(Value::from),
        any::<String>().prop_map(Value::String),
        Just(Value::Null),
    ]
    .boxed()
}

fn json_value() -> BoxedStrategy<Value> {
    json_scalar()
        .prop_recursive(3, 64, 8, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::btree_map(ascii_token(), inner, 0..6)
                    .prop_map(|entries| Value::Object(entries.into_iter().collect())),
            ]
        })
        .boxed()
}

fn json_payload() -> impl Strategy<Value = Map<String, Value>> {
    prop::collection::btree_map(ascii_token(), json_value(), 0..6)
        .prop_map(|entries| entries.into_iter().collect())
}

fn event_type_literal() -> impl Strategy<Value = &'static str> {
    prop::sample::select(vec![
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
    ])
}

fn valid_event_value() -> impl Strategy<Value = Value> {
    (
        ascii_token(),
        event_type_literal(),
        prop::sample::select(vec!["agent", "human", "tool"]),
        ascii_token(),
        json_payload(),
        prop::option::of(1u32..100_000),
        prop::option::of(1u32..100_000),
        1i64..32,
        json_value(),
    )
        .prop_map(
            |(
                event_id,
                event_type,
                actor_kind,
                actor_name,
                payload,
                batch,
                task,
                version,
                future_field,
            )| {
                let mut event = json!({
                    "schema_version": 1,
                    "event_id": event_id,
                    "occurred_at": "2026-07-16T12:34:56.123Z",
                    "type": event_type,
                    "payload_version": version,
                    "actor": {"kind": actor_kind, "name": actor_name},
                    "payload": payload,
                    "future_envelope_field": future_field
                });
                let object = event.as_object_mut().expect("object fixture");
                if let Some(batch) = batch {
                    object.insert("batch_id".into(), Value::String(format!("B-{batch}")));
                }
                if let Some(task) = task {
                    object.insert("task_id".into(), Value::String(format!("T-{task}")));
                }
                event
            },
        )
}

fn corrupt_event(mut event: Value, mutation: u8) -> Value {
    let object = event.as_object_mut().expect("valid event object");
    match mutation % 10 {
        0 => {
            object.remove("schema_version");
        }
        1 => {
            object.insert("schema_version".into(), Value::from(2));
        }
        2 => {
            object.insert("event_id".into(), Value::String("bad id".into()));
        }
        3 => {
            object.insert(
                "occurred_at".into(),
                Value::String("2026-07-16 12:34:56".into()),
            );
        }
        4 => {
            object.insert("type".into(), Value::String("future.unknown".into()));
        }
        5 => {
            object.insert("actor".into(), json!({"kind": "robot", "name": "x"}));
        }
        6 => {
            object.insert("actor".into(), json!({"kind": "agent", "name": ""}));
        }
        7 => {
            object.insert("payload".into(), json!(["not", "an", "object"]));
        }
        8 => {
            object.insert("task_id".into(), Value::String("X-1".into()));
        }
        _ => {
            object.insert("payload_version".into(), Value::from(0));
        }
    }
    event
}

fn task_state() -> impl Strategy<Value = (&'static str, TaskState)> {
    prop::sample::select(vec![
        ("не начата", TaskState::NotStarted),
        ("в работе", TaskState::Working),
        ("на ревью", TaskState::InReview),
        ("готова к слиянию", TaskState::Ready),
        ("слита", TaskState::Merged),
        ("опубликована", TaskState::Published),
        ("выполнена", TaskState::Done),
        ("эскалирована", TaskState::Escalated),
        ("конфликт", TaskState::Conflict),
    ])
}

fn cohort_state() -> impl Strategy<Value = (&'static str, CohortAdmission)> {
    prop::sample::select(vec![
        ("открыт", CohortAdmission::Open),
        ("закрыт", CohortAdmission::Closed),
    ])
}

fn integration_state() -> impl Strategy<Value = IntegrationState> {
    prop::sample::select(vec![
        IntegrationState::None,
        IntegrationState::InProgress,
        IntegrationState::Reviewed,
        IntegrationState::Published,
        IntegrationState::Failed,
        IntegrationState::Cleaned,
    ])
}

fn finding_status() -> impl Strategy<Value = (&'static str, Status)> {
    prop::sample::select(vec![
        ("новая", Status::New),
        ("исправлено", Status::Fixed),
        ("отклонено", Status::Rejected),
        ("готово к слиянию", Status::Ready),
    ])
}

fn finding_id() -> impl Strategy<Value = String> {
    // R-/F- ids carry a monotonic counter that can legitimately be one digit (`R-9`), two
    // (`R-42`), or three-plus (`R-100`) — cover the whole range, not only the zero-padded
    // two-digit form, so the property exercises the loosened `is_marker_id` predicate.
    prop_oneof![
        (0u32..1000).prop_map(|n| format!("R-{n}")),
        (0u32..1000).prop_map(|n| format!("F-{n}")),
        (0u8..60).prop_map(|second| format!("SUMMARY-R-2026-07-16T12:34:{second:02}Z")),
    ]
}

fn json_scalar_pair() -> BoxedStrategy<(Value, JsonValue)> {
    prop_oneof![
        any::<bool>().prop_map(|value| (Value::Bool(value), JsonValue::Bool(value))),
        (-1_000_000i32..=1_000_000)
            .prop_map(|value| (Value::from(value), JsonValue::Num(f64::from(value)))),
        any::<String>().prop_map(|value| (Value::String(value.clone()), JsonValue::Str(value))),
        Just((Value::Null, JsonValue::Null)),
    ]
    .boxed()
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    // events::parse — strict envelope, lenient forward decode (§19.4)

    #[test]
    fn event_parser_never_panics(input in any::<String>()) {
        let _ = parse_line(&input);
    }

    #[test]
    fn valid_events_round_trip_without_loss(value in valid_event_value()) {
        let event = parse_line(&value.to_string()).expect("generated envelope must be valid");
        let reparsed =
            parse_line(&event.to_json_line()).expect("serialized event must reparse");
        prop_assert_eq!(reparsed, event);
    }

    #[test]
    fn corrupted_event_envelopes_are_rejected(
        value in valid_event_value(),
        mutation in any::<u8>(),
    ) {
        let corrupted = corrupt_event(value, mutation);
        prop_assert!(parse_line(&corrupted.to_string()).is_err());
    }

    // contract — R/F/SUMMARY markers, Codex sentinels, changed files, outcomes

    #[test]
    fn contract_parsers_never_panic(input in any::<String>()) {
        let _ = parse_review(&input);
        let _ = detect_sentinel(&input);
        let _ = parse_changed_files(&input);
        let _ = parse_outcome(&input);
    }

    #[test]
    fn valid_review_markers_preserve_id_and_status(
        id in finding_id(),
        title in title_text(),
        (status_literal, expected_status) in finding_status(),
    ) {
        let text = format!("### [{id}] {title} — статус: {status_literal}\n");
        let parsed = parse_review(&text);
        prop_assert_eq!(parsed.findings.len(), 1);
        prop_assert_eq!(&parsed.findings[0].id, &id);
        prop_assert_eq!(&parsed.findings[0].status, &expected_status);

        let normalized = format!(
            "### [{}] {} — статус: {}\n",
            parsed.findings[0].id, parsed.findings[0].title, status_literal,
        );
        let reparsed = parse_review(&normalized);
        prop_assert_eq!(&reparsed.findings[0], &parsed.findings[0]);
    }

    #[test]
    fn structurally_corrupted_review_markers_are_rejected(
        id in finding_id(),
        title in title_text(),
        mutation in 0u8..6,
    ) {
        let heading = match mutation {
            0 => format!("### ({id}] {title} — статус: новая"),
            1 => format!("## [{id}] {title} — статус: новая"),
            // `R-` with NO digits at all stays invalid (the loosening is «≥1 digit», not «any
            // suffix»); a single digit like `R-1` is now legitimate, so it is no longer a
            // corruption case.
            2 => format!("### [R-] {title} — статус: новая"),
            3 => format!("### [F-AA] {title} — статус: новая"),
            4 => format!("### [SUMMARY-R-not-a-time] {title} — статус: готово к слиянию"),
            _ => format!("### [{id}] {title} — статус:"),
        };
        prop_assert!(parse_review(&heading).findings.is_empty());
    }

    #[test]
    fn codex_sentinels_survive_surrounding_garbage(
        prefix in "[a-z0-9 ]{0,30}",
        suffix in "[a-z0-9 ]{0,30}",
        (literal, expected) in prop::sample::select(vec![
            ("CODEX_UNAVAILABLE", Sentinel::Unavailable),
            ("CODEX_FAILED", Sentinel::Failed),
            ("ЭСКАЛАЦИЯ codex: reason", Sentinel::Escalation),
        ]),
    ) {
        prop_assert_eq!(
            detect_sentinel(&format!("{prefix} {literal} {suffix}")),
            Some(expected)
        );
    }

    #[test]
    fn corrupted_codex_sentinels_are_rejected(
        prefix in "[a-z0-9 ]{0,30}",
        corrupted in prop::sample::select(vec![
            "CODEX-FAILED",
            "CODEX-UNAVAILABLE",
            "XCODEX_FAILED",
            "CODEX_FAILED_X",
            "XCODEX_UNAVAILABLE",
            "CODEX_UNAVAILABLE_X",
            "XЭСКАЛАЦИЯ codex: reason",
            "ЭСКАЛАЦИЯ codex reason",
        ]),
    ) {
        let text = format!("{prefix} {corrupted}");
        prop_assert_eq!(detect_sentinel(&text), None);
    }

    #[test]
    fn changed_file_lists_round_trip(
        files in prop::collection::vec(ascii_token(), 1..8),
    ) {
        let report = format!("Изменённые файлы: {}\n", files.join(", "));
        prop_assert_eq!(parse_changed_files(&report), Some(files));
    }

    #[test]
    fn corrupted_changed_files_marker_is_rejected(
        files in prop::collection::vec(ascii_token(), 1..8),
    ) {
        let report = format!("Измененные файлы: {}\n", files.join(", "));
        prop_assert_eq!(parse_changed_files(&report), None);
        prop_assert_eq!(parse_changed_files("Изменённые файлы:   \n"), None);
    }

    #[test]
    fn outcome_markers_round_trip(
        verdict in ascii_token(),
        fields in prop::collection::vec((ascii_token(), ascii_token()), 0..6),
    ) {
        let suffix = fields
            .iter()
            .map(|(key, value)| format!(" · {key}={value}"))
            .collect::<String>();
        let parsed = parse_outcome(&format!("ИТОГ: {verdict}{suffix}\n"))
            .expect("generated outcome must parse");
        prop_assert_eq!(&parsed.verdict, &verdict);
        prop_assert_eq!(&parsed.fields, &fields);
    }

    #[test]
    fn empty_or_corrupted_outcomes_are_rejected(garbage in "[a-z0-9 ]{0,30}") {
        prop_assert_eq!(parse_outcome(&format!("ИТОГ:   \n{garbage}")), None);
        prop_assert_eq!(parse_outcome(&format!("ИТОГОВО: {garbage}")), None);
    }

    // state::canonical — descriptor/status canonicalization (§13.1–§13.3)

    #[test]
    fn canonical_parsers_never_panic(input in any::<String>(), key in any::<String>()) {
        let _ = TaskState::from_markdown(&input);
        let _ = TaskState::from_canonical(&input);
        let _ = CohortAdmission::from_markdown(&input);
        let _ = CohortAdmission::from_canonical(&input);
        let _ = IntegrationState::from_canonical(&input);
        let _ = suffix_field(&input, &key);
    }

    #[test]
    fn task_status_classification_ignores_arbitrary_suffixes(
        (literal, expected) in task_state(),
        suffix in line_suffix(),
    ) {
        let decorated = format!("{literal} · {suffix}");
        prop_assert_eq!(TaskState::from_markdown(&decorated), Some(expected));
        prop_assert_eq!(TaskState::from_canonical(expected.as_str()), Some(expected));
    }

    #[test]
    fn cohort_status_classification_ignores_arbitrary_suffixes(
        (literal, expected) in cohort_state(),
        suffix in line_suffix(),
    ) {
        let decorated = format!("{literal} · {suffix}");
        prop_assert_eq!(CohortAdmission::from_markdown(&decorated), Some(expected));
        prop_assert_eq!(
            CohortAdmission::from_canonical(expected.as_str()),
            Some(expected)
        );
    }

    #[test]
    fn integration_states_round_trip(expected in integration_state()) {
        prop_assert_eq!(
            IntegrationState::from_canonical(expected.as_str()),
            Some(expected)
        );
    }

    #[test]
    fn corrupted_status_base_words_are_rejected(
        (literal, _) in task_state(),
        suffix in line_suffix(),
    ) {
        let corrupted = format!("{literal}x · {suffix}");
        prop_assert_eq!(TaskState::from_markdown(&corrupted), None);
    }

    #[test]
    fn corrupted_canonical_values_are_rejected(garbage in "[A-Za-z0-9._/-]{0,40}") {
        let corrupted = format!("invalid/{garbage}");
        prop_assert_eq!(TaskState::from_markdown(&corrupted), None);
        prop_assert_eq!(TaskState::from_canonical(&corrupted), None);
        prop_assert_eq!(CohortAdmission::from_markdown(&corrupted), None);
        prop_assert_eq!(CohortAdmission::from_canonical(&corrupted), None);
        prop_assert_eq!(IntegrationState::from_canonical(&corrupted), None);
    }

    #[test]
    fn suffix_fields_round_trip(
        key in "[a-z]{1,12}",
        value in "[A-Za-z0-9_./:-]{0,40}",
        noise in "[A-Za-z0-9_./:-]{0,40}",
    ) {
        let literal = format!("не начата · other={noise} · {key}={value}");
        prop_assert_eq!(
            suffix_field(&literal, &format!("{key}=")),
            Some(value)
        );
    }

    // state::queue — queue headers and status classification (§13.1)

    #[test]
    fn queue_parser_never_panics(input in any::<String>()) {
        let _ = parse_queue(&input);
    }

    #[test]
    fn valid_queue_headers_round_trip_and_ignore_status_suffixes(
        id in 1u32..100_000,
        title in title_text(),
        (status, expected) in task_state(),
        suffix in line_suffix(),
    ) {
        let literal = format!("{status} · {suffix}");
        let queue = format!("### [T-{id}] {title} — статус: {literal}\nbody\n");
        let parsed = parse_queue(&queue);
        let expected_id = format!("T-{id}");
        prop_assert_eq!(parsed.len(), 1);
        prop_assert_eq!(&parsed[0].id, &expected_id);
        prop_assert_eq!(&parsed[0].title, &title);
        prop_assert_eq!(parsed[0].state, Some(expected));

        let normalized = format!(
            "### [{}] {} — статус: {}\n",
            parsed[0].id, parsed[0].title, parsed[0].status_literal,
        );
        prop_assert_eq!(parse_queue(&normalized), parsed);
    }

    #[test]
    fn queue_suffix_fields_survive_unrelated_garbage(
        id in 1u32..100_000,
        attempt in 0u32..10_000,
        quarantine in ascii_token(),
        garbage in "[A-Za-z0-9_./:-]{0,40}",
    ) {
        let queue = format!(
            "### [T-{id}] title — статус: не начата · шум={garbage} · попытка={attempt} · карантин={quarantine}\n",
        );
        let parsed = parse_queue(&queue);
        prop_assert_eq!(parsed[0].state, Some(TaskState::NotStarted));
        prop_assert_eq!(parsed[0].attempt, Some(attempt));
        prop_assert_eq!(parsed[0].quarantine.as_deref(), Some(quarantine.as_str()));
    }

    #[test]
    fn corrupted_queue_headers_are_rejected(
        id in 1u32..100_000,
        title in title_text(),
        mutation in 0u8..4,
    ) {
        let header = match mutation {
            0 => format!("## [T-{id}] {title} — статус: не начата"),
            1 => format!("### [X-{id}] {title} — статус: не начата"),
            2 => format!("### [T-{id}] {title} — состояние: не начата"),
            _ => format!("### [T-{id} {title} — статус: не начата"),
        };
        prop_assert!(parse_queue(&header).is_empty());
    }

    // jsonline — stream-json top-level scalar scanner

    #[test]
    fn jsonline_scanner_never_panics(input in any::<String>(), key in any::<String>()) {
        let _ = top_level(&input, &key);
    }

    #[test]
    fn top_level_scalars_survive_json_serialization(
        key in any::<String>(),
        (wire_value, expected) in json_scalar_pair(),
    ) {
        let mut object = Map::new();
        object.insert(key.clone(), wire_value);
        let line = Value::Object(object).to_string();
        prop_assert_eq!(top_level(&line, &key), Some(expected));
    }

    #[test]
    fn nested_scalars_never_shadow_a_missing_top_level_key(
        key in any::<String>(),
        (wire_value, _) in json_scalar_pair(),
    ) {
        let mut nested = Map::new();
        nested.insert(key.clone(), wire_value);
        let mut root = Map::new();
        root.insert("wrapper".into(), Value::Object(nested));
        let line = Value::Object(root).to_string();
        prop_assert_eq!(top_level(&line, &key), None);
    }

    #[test]
    fn corrupted_jsonline_inputs_are_rejected(
        garbage in any::<String>(),
        key in any::<String>(),
    ) {
        prop_assert_eq!(top_level(&format!("not-an-object:{garbage}"), &key), None);
        prop_assert_eq!(top_level(&format!("[{garbage}]"), &key), None);
        prop_assert_eq!(top_level("{\"unterminated", &key), None);
    }

    #[test]
    fn unsupported_composite_top_level_values_are_explicitly_none(
        key in any::<String>(),
        value in prop_oneof![Just(json!([])), Just(json!({"nested": true}))],
    ) {
        let mut object = Map::new();
        object.insert(key.clone(), value);
        let line = Value::Object(object).to_string();
        prop_assert_eq!(top_level(&line, &key), None);
    }
}

#[test]
fn event_wire_enums_round_trip_exhaustively() {
    for literal in [
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
    ] {
        let event_type = EventType::parse(literal).expect("known event type");
        assert_eq!(event_type.as_str(), literal);
    }
    for literal in ["agent", "human", "tool"] {
        let kind = ActorKind::parse(literal).expect("known actor kind");
        assert_eq!(kind.as_str(), literal);
    }
}
