//! Test-only ProcessKit descendant fixture.
//!
//! `spawn` launches this same executable as `mark` through ProcessKit and waits for it.  The
//! integration test places `spawn` under the engine supervisor then expires the outer deadline.
//! A release-gated marker is stronger than PID inspection: Windows can retain an
//! already-terminated process object temporarily, whereas a marker written after the outer
//! supervisor returned proves a descendant continued to execute.

use std::path::PathBuf;
use std::time::Duration;

use processkit::{Command, OutputBufferPolicy};

// Stay well beyond the outer supervisor's ten-second deadline. Otherwise the nested command's
// own timeout could kill the marker child at the same boundary and make the containment proof
// pass for the wrong reason before the test sends its post-teardown release signal.
const INNER_DEADLINE: Duration = Duration::from_secs(30);
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
    let release = release_marker(&marker);
    while !release.is_file() {
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(marker, "descendant-ran")
        .unwrap_or_else(|error| fail(&format!("write released marker: {error}")));
}

fn ready_marker(marker: &std::path::Path) -> PathBuf {
    let mut ready = marker.as_os_str().to_os_string();
    ready.push(".ready");
    PathBuf::from(ready)
}

fn release_marker(marker: &std::path::Path) -> PathBuf {
    let mut release = marker.as_os_str().to_os_string();
    release.push(".release");
    PathBuf::from(release)
}

fn fail(message: &str) -> ! {
    eprintln!("fixture_process_tree: {message}");
    std::process::exit(2)
}
