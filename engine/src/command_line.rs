//! Strict command-line decoding for operator-owned external-tool profiles.
//!
//! The engine's process boundary accepts a program plus typed argv, never a shell command. This
//! small parser preserves ordinary quoted arguments while rejecting shell grammar explicitly so
//! `VERIFICATION_COMMANDS` cannot quietly regain pipes, redirects, expansion, or a second
//! command through `cmd`/`sh`.

use std::path::Path;

/// Decode one operator configuration entry into a program and typed arguments.
///
/// Whitespace separates arguments outside `'…'`/`"…"`; backslash escapes only a quote or another
/// backslash within double quotes, leaving normal Windows paths intact. Shell operators are
/// rejected only when unquoted, where a shell would otherwise interpret them.
pub fn parse_typed_argv(command: &str) -> Result<Vec<String>, String> {
    if command.trim().is_empty() || command.contains('\0') {
        return Err("command must be non-empty and contain no NUL byte".into());
    }
    let mut argv = Vec::new();
    let mut current = String::new();
    let mut token_started = false;
    let mut quote = None;
    let mut characters = command.chars().peekable();
    while let Some(character) = characters.next() {
        match quote {
            Some('\'') => match character {
                '\'' => quote = None,
                _ => current.push(character),
            },
            Some('"') => match character {
                '"' => quote = None,
                '\\' if matches!(characters.peek(), Some('"' | '\\')) => {
                    current.push(characters.next().expect("peeked character exists"));
                }
                _ => current.push(character),
            },
            Some(_) => unreachable!("only single and double quotes enter parser state"),
            None => match character {
                '\'' | '"' => quote = Some(character),
                character if character.is_whitespace() => {
                    if token_started {
                        argv.push(std::mem::take(&mut current));
                        token_started = false;
                    }
                }
                '|' | '&' | ';' | '<' | '>' | '$' | '(' | ')' | '`' => {
                    return Err(format!(
                        "shell operator {character:?} is not permitted; configure one executable and typed arguments"
                    ));
                }
                _ => current.push(character),
            },
        }
        if quote.is_some() || !character.is_whitespace() {
            token_started = true;
        }
    }
    if quote.is_some() {
        return Err("unterminated quoted argument".into());
    }
    if token_started {
        argv.push(current);
    }
    let Some(program) = argv.first() else {
        return Err("command has no executable".into());
    };
    validate_direct_program(program)?;
    Ok(argv)
}

/// Validate a program field whose arguments are generated separately by typed product code.
/// Paths may contain spaces, but the executable itself may not be a shell host.
pub fn validate_direct_program(program: &str) -> Result<(), String> {
    if program.is_empty() || program.contains(['\0', '\n', '\r']) {
        return Err("executable must be non-empty and contain no NUL or line break".into());
    }
    let executable = Path::new(program)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(program)
        .to_ascii_lowercase();
    if matches!(
        executable.as_str(),
        "cmd"
            | "cmd.exe"
            | "sh"
            | "bash"
            | "zsh"
            | "fish"
            | "pwsh"
            | "pwsh.exe"
            | "powershell"
            | "powershell.exe"
    ) {
        return Err(format!(
            "shell executable {program:?} is not permitted; configure the target executable directly"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typed_argv_without_treating_windows_paths_as_escapes() {
        assert_eq!(
            parse_typed_argv(r#"tool --message "two words" C:\work\input.txt"#).unwrap(),
            vec!["tool", "--message", "two words", r"C:\work\input.txt"]
        );
        assert_eq!(
            parse_typed_argv(r#"tool "" tail"#).unwrap(),
            vec!["tool", "", "tail"]
        );
    }

    #[test]
    fn rejects_shell_grammar_and_shell_programs() {
        for command in [
            "cargo test && cargo fmt",
            "cargo test | tee log.txt",
            "cmd.exe /C cargo test",
            "pwsh -Command cargo test",
        ] {
            assert!(parse_typed_argv(command).is_err(), "{command}");
        }
        assert!(validate_direct_program(r"C:\Windows\System32\cmd.exe").is_err());
        assert!(validate_direct_program(r"C:\Program Files\Codex\codex.exe").is_ok());
    }
}
