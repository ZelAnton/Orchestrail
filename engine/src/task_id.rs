//! The single strict validator for task identifiers shared by engine consumers.

/// Returns whether `value` is exactly `T-` followed by one or more ASCII digits.
///
/// This is the whole-token contract used by `tools/queue-tx.ps1`: `T-12` and
/// `T-0012` are valid; an empty suffix, non-ASCII numerals, and trailing text
/// such as `T-1abc` are not.
pub fn is_task_id(value: &str) -> bool {
    value.strip_prefix("T-").is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

#[cfg(test)]
mod tests {
    use super::is_task_id;

    #[test]
    fn accepts_only_whole_ascii_decimal_task_ids() {
        for valid in ["T-12", "T-0012"] {
            assert!(is_task_id(valid), "{valid:?} must be valid");
        }
        for invalid in ["", "T-", "T-abc", "T-1abc", "T-12 ", " T-12", "T-١2"] {
            assert!(!is_task_id(invalid), "{invalid:?} must be invalid");
        }
    }
}
