//! ProcessKit-only synchronous adapter for integration tests.
//!
//! The product boundary and its real-binary fixtures must exercise the same contained process
//! launcher.  This deliberately mirrors only the small `Command::output` surface the fixtures
//! need; it is not a second production supervisor.

// Each integration-test crate imports this module independently and uses a different subset of
// the small compatibility surface, so a per-crate dead-code warning would be noise.
#![allow(dead_code)]

use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Path;
use std::time::Duration;

use processkit::{Command as ProcessKitCommand, OutputBufferPolicy};

const TEST_DEADLINE: Duration = Duration::from_secs(30);
const MAX_TEST_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

/// Minimal status view compatible with the assertions in the fixture suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitStatus(Option<i32>);

impl ExitStatus {
    /// Whether the child exited with code zero.
    pub fn success(self) -> bool {
        self.0 == Some(0)
    }

    /// The child's exit code, if ProcessKit observed one.
    pub fn code(self) -> Option<i32> {
        self.0
    }
}

/// Captured child output, shaped for the fixture assertions.
#[derive(Debug, Clone)]
pub struct Output {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// A synchronous, ProcessKit-backed command builder for test fixtures.
#[derive(Debug, Clone)]
pub struct Command {
    program: OsString,
    args: Vec<OsString>,
    envs: Vec<(OsString, OsString)>,
    current_dir: Option<OsString>,
}

impl Command {
    pub fn new(program: impl AsRef<OsStr>) -> Command {
        Command {
            program: program.as_ref().to_os_string(),
            args: Vec::new(),
            envs: Vec::new(),
            current_dir: None,
        }
    }

    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(arg.as_ref().to_os_string());
        self
    }

    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_os_string()));
        self
    }

    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.envs
            .push((key.as_ref().to_os_string(), value.as_ref().to_os_string()));
        self
    }

    pub fn current_dir(&mut self, path: impl AsRef<Path>) -> &mut Self {
        self.current_dir = Some(path.as_ref().as_os_str().to_os_string());
        self
    }

    pub fn output(&mut self) -> io::Result<Output> {
        let mut command = ProcessKitCommand::new(&self.program)
            .args(&self.args)
            .timeout(TEST_DEADLINE)
            .create_no_window()
            .kill_on_parent_death()
            .output_buffer(
                OutputBufferPolicy::fail_loud(50_000).with_max_bytes(MAX_TEST_OUTPUT_BYTES),
            );
        if let Some(path) = &self.current_dir {
            command = command.current_dir(path);
        }
        for (key, value) in &self.envs {
            command = command.env(key, value);
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                io::Error::other(format!("create ProcessKit test runtime: {error}"))
            })?;
        let result = runtime
            .block_on(command.output_bytes())
            .map_err(|error| io::Error::other(format!("ProcessKit test launch: {error}")))?;
        let status = ExitStatus(result.code());
        let stderr = result.stderr().as_bytes().to_vec();
        Ok(Output {
            status,
            stdout: result.into_stdout(),
            stderr,
        })
    }
}
