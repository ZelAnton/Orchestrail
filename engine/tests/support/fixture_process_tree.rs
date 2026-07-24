//! Test-only ProcessKit descendant fixture.
//!
//! `spawn` launches this same executable as `mark` through ProcessKit and waits for it.  The
//! integration test places `spawn` under the engine supervisor then expires the outer deadline.
//! A delayed marker is stronger than PID inspection: Windows can retain an already-terminated
//! process object temporarily, whereas a marker proves a descendant continued to execute.

use std::path::PathBuf;
use std::time::Duration;

use processkit::{Command, OutputBufferPolicy};

const INNER_DEADLINE: Duration = Duration::from_secs(10);
const MAX_OUTPUT_BYTES: usize = 64 * 1024;

fn main() {
    let mut args = std::env::args_os();
    let _program = args.next();
    let Some(mode) = args.next() else {
        fail("usage: fixture_process_tree <spawn|mark> <marker>");
    };
    let Some(marker) = args.next().map(PathBuf::from) else {
        fail("marker path is required");
    };
    if args.next().is_some() {
        fail("unexpected extra arguments");
    }

    match mode.to_string_lossy().as_ref() {
        "spawn" => spawn_delayed_marker(marker),
        "mark" => write_delayed_marker(marker),
        _ => fail("mode must be spawn or mark"),
    }
}

fn spawn_delayed_marker(marker: PathBuf) {
    let executable = std::env::current_exe().unwrap_or_else(|error| {
        fail(&format!("resolve fixture executable: {error}"));
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| fail(&format!("create fixture runtime: {error}")));
    let command = Command::new(executable)
        .arg("mark")
        .arg(marker)
        .timeout(INNER_DEADLINE)
        .create_no_window()
        .kill_on_parent_death()
        .output_buffer(OutputBufferPolicy::fail_loud(1_000).with_max_bytes(MAX_OUTPUT_BYTES));
    let output = runtime
        .block_on(command.output_bytes())
        .unwrap_or_else(|error| fail(&format!("launch nested ProcessKit child: {error}")));
    if output.code() != Some(0) {
        fail("nested marker child exited unsuccessfully");
    }
}

fn write_delayed_marker(marker: PathBuf) {
    let ready = ready_marker(&marker);
    std::fs::write(&ready, "nested-child-started")
        .unwrap_or_else(|error| fail(&format!("write nested-child readiness: {error}")));
    std::thread::sleep(Duration::from_secs(4));
    std::fs::write(marker, "descendant-ran")
        .unwrap_or_else(|error| fail(&format!("write delayed marker: {error}")));
}

fn ready_marker(marker: &std::path::Path) -> PathBuf {
    let mut ready = marker.as_os_str().to_os_string();
    ready.push(".ready");
    PathBuf::from(ready)
}

fn fail(message: &str) -> ! {
    eprintln!("fixture_process_tree: {message}");
    std::process::exit(2)
}
