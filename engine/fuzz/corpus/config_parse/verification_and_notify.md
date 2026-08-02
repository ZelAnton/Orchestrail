VERIFICATION_MODE: required
VERIFICATION_COMMANDS: ["cargo test", "cargo clippy --workspace"]
NOTIFY_CMD: notifier --channel ops # operator-owned endpoint
CODEX_CMD: codex
CODEX_SANDBOX: workspace-write
