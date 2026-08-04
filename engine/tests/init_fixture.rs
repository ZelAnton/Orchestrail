//! CLI boundary coverage for the first-run control-plane scaffold.

mod common;

use std::fs;
use std::path::{Path, PathBuf};

use common::Command;

const BIN: &str = env!("CARGO_BIN_EXE_orchestrail-engine");

#[test]
fn init_creates_safe_defaults_and_reports_validation() {
    let work = test_work("safe");
    let output = run_init(&work);
    assert!(output.status.success(), "stderr: {}", text(&output.stderr));

    let config = fs::read_to_string(work.join("config.md")).unwrap();
    let policy = fs::read_to_string(work.join("constraints.md")).unwrap();
    assert!(config.contains("PUSH: false"));
    assert!(config.contains("CODEX_NETWORK: off"));
    assert!(policy.contains("Публикация (push): требует ручного подтверждения"));
    assert_eq!(
        fs::read_to_string(work.join("Tasks_Queue.md")).unwrap(),
        "# Очередь задач\n\n"
    );
    assert_eq!(
        fs::read_to_string(work.join("Tasks_Done.md")).unwrap(),
        "# Архив выполненных задач\n\n"
    );
    let stdout = text(&output.stdout);
    assert!(stdout.contains("config.md valid"), "stdout: {stdout}");
    assert!(stdout.contains("constraints.md valid"), "stdout: {stdout}");
    cleanup(&work);
}

#[test]
fn init_never_overwrites_files_or_directories() {
    let work = test_work("no-overwrite");
    let first = run_init(&work);
    assert!(first.status.success(), "stderr: {}", text(&first.stderr));

    let config = work.join("config.md");
    fs::write(
        &config,
        "PUSH: false\nCODEX_NETWORK: off\n# operator marker\n",
    )
    .unwrap();
    let queue = work.join("Tasks_Queue.md");
    fs::remove_file(&queue).unwrap();
    fs::create_dir(&queue).unwrap();

    let second = run_init(&work);
    assert!(second.status.success(), "stderr: {}", text(&second.stderr));
    assert_eq!(
        fs::read_to_string(&config).unwrap(),
        "PUSH: false\nCODEX_NETWORK: off\n# operator marker\n"
    );
    assert!(queue.is_dir());
    assert!(text(&second.stdout).contains("no overwrite"));
    cleanup(&work);
}

#[test]
fn init_rejects_an_existing_invalid_config_without_replacing_it() {
    let work = test_work("invalid");
    fs::create_dir_all(&work).unwrap();
    let config = work.join("config.md");
    let original = b"PUSH: maybe\n";
    fs::write(&config, original).unwrap();

    let output = run_init(&work);
    assert!(!output.status.success());
    assert!(text(&output.stderr).contains("config.md validation failed"));
    assert_eq!(fs::read(&config).unwrap(), original);
    cleanup(&work);
}

fn run_init(work: &Path) -> common::Output {
    let mut command = Command::new(BIN);
    command.args(["init", "--work", work.to_str().unwrap()]);
    command.output().unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn test_work(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orchestrail-cli-init-{label}-{}",
        std::process::id()
    ))
}

fn cleanup(path: &Path) {
    let _ = fs::remove_dir_all(path);
}
