//! Real ProcessKit-backed proof of the Phase-5 knowledge curator boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use orchestrail_engine::headless::{HeadlessConfig, HeadlessExternalPort};
use orchestrail_engine::native_port::ExternalPort;
use orchestrail_engine::processor::{
    CohortRuntime, IntegrationRuntime, LeafKind, LeafOutcome, Phase, ProcessorState,
};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn fixture_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "orchestrail-headless-knowledge-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(root.join(".work")).expect("create fixture work directory");
    root
}

fn files_below(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(directory).expect("read fixture directory") {
            let entry = entry.expect("read fixture entry");
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .expect("fixture entry remains below root")
                .to_path_buf();
            if entry.file_type().expect("read fixture type").is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(relative, fs::read(&path).expect("read fixture file"));
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn changed_paths(
    before: &BTreeMap<PathBuf, Vec<u8>>,
    after: &BTreeMap<PathBuf, Vec<u8>>,
) -> BTreeSet<PathBuf> {
    before
        .keys()
        .chain(after.keys())
        .filter(|path| before.get(*path) != after.get(*path))
        .cloned()
        .collect()
}

#[test]
fn knowledge_curator_uses_processkit_prompt_evidence_and_strict_outcome() {
    let root = fixture_root();
    let work = root.join(".work");
    fs::create_dir_all(work.join("knowledge/pitfalls")).expect("create fixture knowledge shard");
    fs::write(
        work.join("knowledge/pitfalls/expired-fixture.md"),
        "expired singleton knowledge\n",
    )
    .expect("write fixture expired entry");
    fs::write(work.join("knowledge/unowned.md"), "must stay untouched\n")
        .expect("write unowned knowledge sentinel");
    fs::write(work.join("Tasks_Queue.md"), "queue sentinel\n").expect("write queue sentinel");
    fs::write(root.join("source-sentinel.txt"), "source sentinel\n")
        .expect("write source sentinel");
    let before = files_below(&root);
    let mut config = HeadlessConfig::new(
        &work,
        &root,
        orchestrail_engine::config::EngineConfig::default().codex,
    );
    config.claude_command = env!("CARGO_BIN_EXE_orchestrail-fixture-claude").into();
    config.call_deadline = Duration::from_secs(10);
    config.knowledge_ttl_batches = 5;
    config.knowledge_cap_per_area = 7;
    let mut port = HeadlessExternalPort::new(config).expect("headless port");
    let state = ProcessorState {
        phase: Phase::Cleaning,
        batch: Some(CohortRuntime {
            id: "B-1".into(),
            base: "main".into(),
            started_at_secs: 1,
            wave: 1,
            admitted_total: 1,
            admission_closed: None,
            cohort_budget_secs: None,
            cohort_token_budget: None,
            cohort_token_budget_strict: false,
            token_budget_actual_tokens: None,
            events_outbox_enabled: true,
        }),
        integration: IntegrationRuntime {
            leaf_attempts: BTreeMap::from([(LeafKind::KnowledgeCurator.as_str().into(), 1)]),
            published_head: Some("published-head".into()),
            ..IntegrationRuntime::default()
        },
        ..ProcessorState::default()
    };

    assert_eq!(
        port.curate_knowledge(&state)
            .expect("contained curator result"),
        LeafOutcome::Completed {
            author: Some("knowledge_curator".into())
        }
    );
    let prompt = fs::read_to_string(work.join("knowledge/architecture/fixture-prompt.md"))
        .expect("fixture observed the ProcessKit child prompt");
    assert!(prompt.contains("update only WORK/knowledge/"));
    assert!(prompt.contains("KB_TTL=5"));
    assert!(prompt.contains("KB_CAP=7"));
    assert!(prompt.contains("BASE=main"));
    assert!(prompt.contains("PUBLISHED_HEAD=published-head"));
    assert_eq!(
        fs::read_to_string(work.join("native-evidence/knowledge-curator.md"))
            .expect("durable curator evidence")
            .trim(),
        "ИТОГ: готово · режим=1"
    );
    let after = files_below(&root);
    let expected_changes = BTreeSet::from([
        PathBuf::from(".work/events.jsonl"),
        PathBuf::from(".work/knowledge/INDEX.md"),
        PathBuf::from(".work/knowledge/.curated/B-1.done"),
        PathBuf::from(".work/knowledge/architecture/fixture-prompt.md"),
        PathBuf::from(".work/knowledge/conventions/fixture-retention.md"),
        PathBuf::from(".work/knowledge/pitfalls/expired-fixture.md"),
        PathBuf::from(".work/knowledge/pitfalls/fixture-retention.md"),
        PathBuf::from(".work/native-evidence/knowledge-curator.md"),
    ]);
    assert_eq!(
        changed_paths(&before, &after),
        expected_changes,
        "the child may alter only owned knowledge shards/index; engine-owned evidence and usage telemetry are the only separate durable artifacts"
    );
    let _ = fs::remove_dir_all(root);
}
