//! Safe terminal setup / teardown so the operator's terminal is ALWAYS restored — on a normal
//! quit, on an error return, and on a panic. Leaving raw mode / the alternate screen engaged would
//! wedge the user's shell, so restoration is wired three ways:
//!
//! * [`init`] enters raw mode + the alternate screen and hands back a ready [`Tui`].
//! * [`TerminalGuard`]'s `Drop` calls [`restore`] on any scope exit (normal or `?`-error).
//! * [`install_panic_hook`] restores the terminal *before* the default panic handler prints, so a
//!   panic backtrace lands on a sane screen instead of a raw-mode-mangled one.
//!
//! All three restore paths are best-effort (`let _ =`): teardown must never itself panic.

use std::io::{self, Stdout};

use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

/// The concrete terminal type this app draws to.
pub type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Enter raw mode + the alternate screen and return a cleared, ready terminal.
pub fn init() -> io::Result<Tui> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    if let Err(error) = execute!(stdout, EnterAlternateScreen) {
        // `TerminalGuard` is constructed by the caller only after `init` returns successfully.
        // Restore here as well, otherwise a failure while entering the alternate screen leaves
        // the caller's shell in raw mode.
        let _ = restore();
        return Err(error);
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = match Terminal::new(backend) {
        Ok(terminal) => terminal,
        Err(error) => {
            let _ = restore();
            return Err(error);
        }
    };
    if let Err(error) = terminal.clear() {
        let _ = restore();
        return Err(error);
    }
    Ok(terminal)
}

/// Leave the alternate screen and disable raw mode. Idempotent / best-effort per call.
pub fn restore() -> io::Result<()> {
    // Attempt both halves even when one fails. In particular, a raw-mode failure must not keep
    // the alternate screen active, and a screen-write failure must not leave raw mode enabled.
    let raw_result = disable_raw_mode();
    let screen_result = execute!(io::stdout(), LeaveAlternateScreen);
    match (raw_result, screen_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

/// RAII guard: restores the terminal when it drops, covering both the normal-exit and the
/// `?`-early-return paths without an explicit teardown call at every return site.
pub struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = restore();
    }
}

/// Chain a terminal-restoring step in front of the existing panic hook, so a panic never leaves
/// the terminal in raw mode. Call this BEFORE [`init`].
pub fn install_panic_hook() {
    let original = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = restore();
        original(info);
    }));
}
