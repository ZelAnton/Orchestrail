//! Minimal, dependency-free extractor for TOP-LEVEL scalar fields of a single-line
//! JSON object (one line of `claude --output-format stream-json`).
//!
//! This is deliberately NOT a general JSON parser. It scans one root object, tracks
//! string state and nesting depth, and returns the value of a requested key only when
//! that key sits at depth 1 (directly in the root object) — so a `"type"` nested inside
//! a sub-object never shadows the root `"type"`. It handles the exact value shapes a
//! stream-json line uses at the top level: JSON strings (with `\" \\ \n \t \uXXXX`
//! escapes), `true`/`false`/`null`, and numbers.
//!
//! The PRODUCTION engine should parse stream-json with `serde_json` (see README). This
//! spike-grade scanner exists only to prove — offline and with zero dependencies — that
//! the structured leaf-agent transcript is machine-parseable without free-text guessing.

/// A top-level JSON value we care about for the spike's contract.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Str(String),
    Bool(bool),
    Num(f64),
    Null,
}

impl JsonValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            JsonValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            JsonValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
    pub fn as_num(&self) -> Option<f64> {
        match self {
            JsonValue::Num(n) => Some(*n),
            _ => None,
        }
    }
}

/// Extract the value of a TOP-LEVEL `key` from one JSON object `line`.
/// Returns `None` if the line is not a JSON object, the key is absent at depth 1, or the
/// value shape is not one of the supported scalar shapes.
pub fn top_level(line: &str, key: &str) -> Option<JsonValue> {
    let chars: Vec<char> = line.trim().chars().collect();
    let n = chars.len();
    if n == 0 || chars[0] != '{' {
        return None;
    }
    let mut i = 1usize; // step over the opening brace (root object => depth becomes 1)
    let mut depth: i32 = 1; // we are now inside the root object
    while i < n {
        let c = chars[i];
        match c {
            '"' => {
                // A string token. At depth 1 and in "key position", it may be a key.
                let (s, next) = read_string(&chars, i)?;
                i = next;
                if depth == 1 {
                    // Is this a key? Look ahead past whitespace for a ':'.
                    let mut j = i;
                    while j < n && chars[j].is_whitespace() {
                        j += 1;
                    }
                    if j < n && chars[j] == ':' {
                        j += 1;
                        while j < n && chars[j].is_whitespace() {
                            j += 1;
                        }
                        if s == key {
                            return read_value(&chars, j);
                        }
                        // Not our key: skip its value so we don't misread the value's
                        // contents as another key.
                        i = skip_value(&chars, j)?;
                        continue;
                    }
                    // A bare string that is not followed by ':' — a value in an array or
                    // a malformed line; just keep scanning.
                }
            }
            '{' | '[' => depth += 1,
            '}' | ']' => {
                depth -= 1;
                if depth <= 0 {
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Read a JSON string starting at `chars[start] == '"'`; return (decoded, index-after).
fn read_string(chars: &[char], start: usize) -> Option<(String, usize)> {
    let n = chars.len();
    let mut i = start + 1;
    let mut out = String::new();
    while i < n {
        let c = chars[i];
        match c {
            '"' => return Some((out, i + 1)),
            '\\' => {
                i += 1;
                if i >= n {
                    return None;
                }
                match chars[i] {
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    '/' => out.push('/'),
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    'b' => out.push('\u{0008}'),
                    'f' => out.push('\u{000C}'),
                    'u' => {
                        if i + 4 >= n {
                            return None;
                        }
                        let hex: String = chars[i + 1..i + 5].iter().collect();
                        let cp = u32::from_str_radix(&hex, 16).ok()?;
                        i += 4;
                        if (0xD800..=0xDBFF).contains(&cp) {
                            // Possible UTF-16 high surrogate: try to combine with an
                            // immediately following `\uXXXX` low surrogate into one
                            // non-BMP code point (e.g. an emoji). If the next escape is
                            // not a valid low surrogate, this is a lone (unpaired) high
                            // surrogate and decodes to U+FFFD, leaving the scanner
                            // position at the end of just this escape (so the following
                            // `\uXXXX`, if any, is still parsed on its own).
                            let mut combined: Option<char> = None;
                            if i + 6 < n && chars[i + 1] == '\\' && chars[i + 2] == 'u' {
                                let hex2: String = chars[i + 3..i + 7].iter().collect();
                                if let Ok(low) = u32::from_str_radix(&hex2, 16)
                                    && (0xDC00..=0xDFFF).contains(&low)
                                {
                                    let cp2 = 0x10000 + (cp - 0xD800) * 0x400 + (low - 0xDC00);
                                    if let Some(c) = char::from_u32(cp2) {
                                        combined = Some(c);
                                        i += 6;
                                    }
                                }
                            }
                            out.push(combined.unwrap_or('\u{FFFD}'));
                        } else {
                            // Not a high surrogate: a lone low surrogate (0xDC00-0xDFFF)
                            // is not a valid scalar value and falls through to U+FFFD via
                            // `char::from_u32`; a regular BMP code point decodes as before.
                            out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                        }
                    }
                    other => out.push(other),
                }
            }
            _ => out.push(c),
        }
        i += 1;
    }
    None // unterminated string
}

/// Read a scalar value at `chars[start..]`.
fn read_value(chars: &[char], start: usize) -> Option<JsonValue> {
    let n = chars.len();
    if start >= n {
        return None;
    }
    match chars[start] {
        '"' => {
            let (s, _) = read_string(chars, start)?;
            Some(JsonValue::Str(s))
        }
        't' => match_lit(chars, start, "true").map(|_| JsonValue::Bool(true)),
        'f' => match_lit(chars, start, "false").map(|_| JsonValue::Bool(false)),
        'n' => match_lit(chars, start, "null").map(|_| JsonValue::Null),
        c if c == '-' || c.is_ascii_digit() => {
            let mut j = start;
            while j < n
                && (chars[j].is_ascii_digit() || matches!(chars[j], '-' | '+' | '.' | 'e' | 'E'))
            {
                j += 1;
            }
            let num: String = chars[start..j].iter().collect();
            num.parse::<f64>().ok().map(JsonValue::Num)
        }
        _ => None, // object/array value: not a scalar the spike needs
    }
}

fn match_lit(chars: &[char], start: usize, lit: &str) -> Option<()> {
    let litc: Vec<char> = lit.chars().collect();
    if start + litc.len() > chars.len() {
        return None;
    }
    for (k, lc) in litc.iter().enumerate() {
        if chars[start + k] != *lc {
            return None;
        }
    }
    Some(())
}

/// Skip a full JSON value at `chars[start..]` (scalar OR object/array), returning the
/// index just past it, so key-scanning resumes at the next key.
fn skip_value(chars: &[char], start: usize) -> Option<usize> {
    let n = chars.len();
    if start >= n {
        return None;
    }
    match chars[start] {
        '"' => {
            let (_, next) = read_string(chars, start)?;
            Some(next)
        }
        '{' | '[' => {
            let mut depth = 0i32;
            let mut i = start;
            while i < n {
                match chars[i] {
                    '"' => {
                        let (_, next) = read_string(chars, i)?;
                        i = next;
                        continue;
                    }
                    '{' | '[' => depth += 1,
                    '}' | ']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(i + 1);
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            None
        }
        _ => {
            // scalar: read to the next top-level ',' or '}' or ']'
            let mut i = start;
            while i < n && !matches!(chars[i], ',' | '}' | ']') {
                i += 1;
            }
            Some(i)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_top_level_string() {
        let line = r#"{"type":"result","subtype":"success"}"#;
        assert_eq!(top_level(line, "type").unwrap().as_str(), Some("result"));
        assert_eq!(
            top_level(line, "subtype").unwrap().as_str(),
            Some("success")
        );
    }

    #[test]
    fn reads_bool_and_number() {
        let line = r#"{"is_error":false,"num_turns":3,"cost":0.12}"#;
        assert_eq!(top_level(line, "is_error").unwrap().as_bool(), Some(false));
        assert_eq!(top_level(line, "num_turns").unwrap().as_num(), Some(3.0));
        assert_eq!(top_level(line, "cost").unwrap().as_num(), Some(0.12));
    }

    #[test]
    fn nested_key_does_not_shadow_root() {
        // A nested object also has a "type" — must NOT be returned for the root "type".
        let line = r#"{"type":"result","message":{"type":"inner","role":"assistant"}}"#;
        assert_eq!(top_level(line, "type").unwrap().as_str(), Some("result"));
    }

    #[test]
    fn string_with_escaped_quotes_and_braces() {
        let line = r#"{"result":"he said \"hi\" and {maybe}","is_error":true}"#;
        assert_eq!(
            top_level(line, "result").unwrap().as_str(),
            Some(r#"he said "hi" and {maybe}"#)
        );
        // The brace inside the string must not confuse depth tracking for later keys.
        assert_eq!(top_level(line, "is_error").unwrap().as_bool(), Some(true));
    }

    #[test]
    fn missing_key_and_non_object() {
        let line = r#"{"type":"result"}"#;
        assert!(top_level(line, "absent").is_none());
        assert!(top_level("not json", "type").is_none());
        assert!(top_level("[1,2,3]", "type").is_none());
    }

    #[test]
    fn unicode_and_control_escapes() {
        // AB => "AB", plus a tab and newline escape.
        let line = r#"{"result":"AB\ttab\nnl"}"#;
        assert_eq!(
            top_level(line, "result").unwrap().as_str(),
            Some("AB\ttab\nnl")
        );
        // A real \uXXXX escape: AB decodes to "AB".
        let uline = "{\"result\":\"\\u0041\\u0042\"}";
        assert_eq!(top_level(uline, "result").unwrap().as_str(), Some("AB"));
    }

    #[test]
    fn surrogate_pair_joins_into_one_non_bmp_char() {
        // U+1F389 PARTY POPPER (🎉) as a UTF-16 surrogate pair: D83C DF89.
        let line = "{\"result\":\"party \\uD83C\\uDF89 time\"}";
        assert_eq!(
            top_level(line, "result").unwrap().as_str(),
            Some("party 🎉 time")
        );
    }

    #[test]
    fn unpaired_surrogates_become_replacement_char_without_losing_position() {
        // Lone high surrogate not followed by a \u escape at all.
        let line = "{\"result\":\"a\\uD800b\",\"is_error\":true}";
        assert_eq!(
            top_level(line, "result").unwrap().as_str(),
            Some("a\u{FFFD}b")
        );
        // Scanner position must not have drifted: the next key is still readable.
        assert_eq!(top_level(line, "is_error").unwrap().as_bool(), Some(true));

        // High surrogate followed by a \u escape that is NOT a low surrogate.
        let line2 = "{\"result\":\"a\\uD800\\u0041b\",\"is_error\":true}";
        assert_eq!(
            top_level(line2, "result").unwrap().as_str(),
            Some("a\u{FFFD}Ab")
        );
        assert_eq!(top_level(line2, "is_error").unwrap().as_bool(), Some(true));

        // Lone low surrogate, not preceded by a high surrogate.
        let line3 = "{\"result\":\"a\\uDC00b\",\"is_error\":true}";
        assert_eq!(
            top_level(line3, "result").unwrap().as_str(),
            Some("a\u{FFFD}b")
        );
        assert_eq!(top_level(line3, "is_error").unwrap().as_bool(), Some(true));
    }
}
