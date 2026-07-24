//! Dependency-free UTC ISO-8601 helpers — validation plus epoch↔ISO conversion — shared by the
//! engine and its consumers.
//!
//! The same three concerns used to live in four hand-rolled copies across the crate: a strict
//! `YYYY-MM-DDTHH:MM:SS(.d{1,3})?Z` format validator (duplicated in `contract.rs` and
//! `events/parse.rs`), and the two directions of Howard Hinnant's calendar algorithm — the
//! forward `civil_from_days` inside [`epoch_to_iso`] and the inverse `days_from_civil` inside
//! `main.rs`'s timestamp parser. This module is their single home: [`is_iso_utc`] is the one
//! format validator, [`epoch_to_iso`] / [`iso_to_epoch`] the paired conversions, and
//! [`days_from_civil`] the shared calendar core a leniently-formatted parser (`main.rs`) can reuse
//! without re-deriving the arithmetic. Still dependency-free on purpose (see `lib.rs` / Cargo.toml).

/// Convert Unix epoch seconds (UTC) to `YYYY-MM-DDTHH:MM:SSZ` at second precision.
///
/// Uses Howard Hinnant's `civil_from_days` algorithm, so callers do not
/// need a calendar dependency. The fixed-width, most-significant-component-first format is also
/// lexically sortable for the timestamps used by the engine's freshness gate.
pub fn epoch_to_iso(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = (secs % 86_400) as i64;
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Convert Unix epoch milliseconds to fixed-width `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub fn epoch_millis_to_iso(millis: u64) -> String {
    let seconds = millis / 1_000;
    let fraction = millis % 1_000;
    let whole = epoch_to_iso(seconds);
    format!("{}.{fraction:03}Z", whole.trim_end_matches('Z'))
}

/// Parse the strict UTC format accepted by [`is_iso_utc`] into epoch milliseconds.
pub fn iso_to_epoch_millis(value: &str) -> Option<u64> {
    let seconds = iso_to_epoch(value)?;
    let fraction = value
        .get(19..value.len().saturating_sub(1))
        .and_then(|tail| tail.strip_prefix('.'))
        .and_then(|digits| match digits.len() {
            1 => digits.parse::<u64>().ok().map(|value| value * 100),
            2 => digits.parse::<u64>().ok().map(|value| value * 10),
            3 => digits.parse::<u64>().ok(),
            _ => None,
        })
        .unwrap_or(0);
    seconds.checked_mul(1_000)?.checked_add(fraction)
}

/// Strict inverse of [`epoch_to_iso`]: parse the exact `YYYY-MM-DDTHH:MM:SS(.d{1,3})?Z` shape
/// [`is_iso_utc`] accepts back into Unix epoch seconds, truncating any fractional tail to whole
/// seconds. Returns `None` for any string [`is_iso_utc`] rejects, or a pre-1970 instant that does
/// not fit `u64`. Round-trips [`epoch_to_iso`] for every representable second.
///
/// Like the crate's other ISO validators this checks *format*, not calendar field ranges: a
/// digit-well-formed but nonsensical field (e.g. month `99`) is accepted by [`is_iso_utc`] and
/// converted by the same proleptic-Gregorian arithmetic — but the only producer of these strings
/// ([`epoch_to_iso`]) never emits out-of-range fields.
pub fn iso_to_epoch(s: &str) -> Option<u64> {
    if !is_iso_utc(s) {
        return None;
    }
    // `is_iso_utc` has vetted ASCII digits at every fixed offset below, so each slice parses.
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: i64 = s.get(5..7)?.parse().ok()?;
    let day: i64 = s.get(8..10)?.parse().ok()?;
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    let minute: i64 = s.get(14..16)?.parse().ok()?;
    let second: i64 = s.get(17..19)?.parse().ok()?;
    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    u64::try_from(secs).ok()
}

/// Validate a string as `YYYY-MM-DDTHH:MM:SS(.d{1,3})?Z` — ISO-8601 UTC, mandatory trailing `Z`,
/// optional 1–3-digit fractional seconds — with a hand-rolled byte matcher (no `regex`
/// dependency). This is the single validator the freshness gate (`contract.rs::is_utc_timestamp`)
/// and the event-outbox parser (`events/parse.rs`) both delegate to; it checks *format*, not
/// calendar field ranges.
pub fn is_iso_utc(s: &str) -> bool {
    let b = s.as_bytes();
    // Minimum: "YYYY-MM-DDTHH:MM:SSZ" = 20 bytes.
    if b.len() < 20 {
        return false;
    }
    let digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
    let lit = |i: usize, expected: u8| b.get(i) == Some(&expected);
    if !(digit(0)
        && digit(1)
        && digit(2)
        && digit(3)
        && lit(4, b'-')
        && digit(5)
        && digit(6)
        && lit(7, b'-')
        && digit(8)
        && digit(9)
        && lit(10, b'T')
        && digit(11)
        && digit(12)
        && lit(13, b':')
        && digit(14)
        && digit(15)
        && lit(16, b':')
        && digit(17)
        && digit(18))
    {
        return false;
    }
    // optional fractional seconds ".d{1,3}" then a mandatory trailing 'Z' and nothing after it.
    let mut end = 19;
    if lit(end, b'.') {
        let start = end + 1;
        end = start;
        while end < b.len() && b[end].is_ascii_digit() {
            end += 1;
        }
        if !(1..=3).contains(&(end - start)) {
            return false;
        }
    }
    b.get(end) == Some(&b'Z') && end + 1 == b.len()
}

/// Howard Hinnant's `days_from_civil`: Unix days (since 1970-01-01) for a proleptic-Gregorian
/// civil date. The inverse of `civil_from_days` and the shared calendar core reused by
/// [`iso_to_epoch`] and by `main.rs`'s leniently-formatted `cohort_state.md` timestamp parser — no
/// calendar dependency. Validating that the fields are in range is the caller's concern.
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if month > 2 { month - 3 } else { month + 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Howard Hinnant's `civil_from_days`: the `(year, month, day)` proleptic-Gregorian civil date for
/// a count of Unix days. The inverse of [`days_from_civil`]; used by [`epoch_to_iso`].
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // Shift the epoch so the internal year begins on 1 March.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::{
        days_from_civil, epoch_millis_to_iso, epoch_to_iso, is_iso_utc, iso_to_epoch,
        iso_to_epoch_millis,
    };

    #[test]
    fn epoch_to_iso_formats_known_utc_instants() {
        // Well-known epochs (UTC): 1970-01-01 and 2021-01-01, plus a within-day offset.
        assert_eq!(epoch_to_iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(epoch_to_iso(1_609_459_200), "2021-01-01T00:00:00Z");
        assert_eq!(epoch_to_iso(1_609_462_861), "2021-01-01T01:01:01Z");
    }

    #[test]
    fn epoch_milliseconds_round_trip_fractional_utc() {
        assert_eq!(
            epoch_millis_to_iso(1_609_459_200_007),
            "2021-01-01T00:00:00.007Z"
        );
        assert_eq!(
            iso_to_epoch_millis("2021-01-01T00:00:00.007Z"),
            Some(1_609_459_200_007)
        );
        assert_eq!(
            iso_to_epoch_millis("2021-01-01T00:00:00.5Z"),
            Some(1_609_459_200_500)
        );
    }

    #[test]
    fn epoch_to_iso8601_matches_known_instants() {
        assert_eq!(epoch_to_iso(0), "1970-01-01T00:00:00Z");
        // 2001-09-09T01:46:40Z is the classic 1e9 epoch second.
        assert_eq!(epoch_to_iso(1_000_000_000), "2001-09-09T01:46:40Z");
        // A leap-day instant: 2020-02-29T12:00:00Z.
        assert_eq!(epoch_to_iso(1_582_977_600), "2020-02-29T12:00:00Z");
    }

    #[test]
    fn epoch_to_iso_is_lexically_monotonic_across_boundaries() {
        // The freshness gate compares timestamps lexically, so `+1s` must sort strictly greater —
        // including across a minute rollover (…T00:00:59Z < …T00:01:00Z).
        let boundary = 1_609_459_200 + 59; // 2021-01-01T00:00:59Z
        assert_eq!(epoch_to_iso(boundary), "2021-01-01T00:00:59Z");
        assert_eq!(epoch_to_iso(boundary + 1), "2021-01-01T00:01:00Z");
        assert!(epoch_to_iso(boundary + 1) > epoch_to_iso(boundary));
    }

    #[test]
    fn is_iso_utc_accepts_and_rejects_per_format() {
        // Consolidated from `events/parse.rs::iso_utc_matcher` — the strict validator now lives
        // here and both consumers (contract.rs, events/parse.rs) delegate to it. Same assertions.
        assert!(is_iso_utc("2026-07-08T12:24:10Z"));
        assert!(is_iso_utc("2026-07-08T12:24:10.123Z"));
        assert!(is_iso_utc("2026-07-08T12:24:10.1Z"));
        assert!(!is_iso_utc("2026-07-08T12:24:10")); // no Z
        assert!(!is_iso_utc("2026-07-08T12:24:10.1234Z")); // >3 frac digits
        assert!(!is_iso_utc("2026-07-08T12:24:10Z ")); // trailing space
        assert!(!is_iso_utc("2026-7-8T12:24:10Z")); // unpadded
        assert!(!is_iso_utc(""));
    }

    #[test]
    fn iso_to_epoch_inverts_epoch_to_iso() {
        // The strict inverse round-trips every representable second, including a minute-rollover
        // second and a leap-day instant.
        for secs in [
            0u64,
            1_000_000_000,
            1_582_977_600,
            1_609_459_200,
            1_609_459_259,
            1_609_462_861,
        ] {
            assert_eq!(
                iso_to_epoch(&epoch_to_iso(secs)),
                Some(secs),
                "round-trip {secs}"
            );
        }
    }

    #[test]
    fn iso_to_epoch_truncates_fractional_and_rejects_malformed() {
        // Fractional seconds are accepted by the strict format and truncated to whole seconds.
        assert_eq!(
            iso_to_epoch("2021-01-01T00:00:00.500Z"),
            Some(1_609_459_200)
        );
        assert_eq!(iso_to_epoch("1970-01-01T00:00:00Z"), Some(0));
        // Anything `is_iso_utc` rejects yields None (no Z, unpadded date, empty string).
        assert_eq!(iso_to_epoch("2021-01-01T00:00:00"), None);
        assert_eq!(iso_to_epoch("2021-1-1T00:00:00Z"), None);
        assert_eq!(iso_to_epoch(""), None);
    }

    #[test]
    fn days_from_civil_matches_known_dates_and_inverts_the_forward_direction() {
        // Epoch day 0 is 1970-01-01; the calendar core agrees with the epoch-second landmarks.
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2021, 1, 1), 1_609_459_200 / 86_400);
        assert_eq!(days_from_civil(2020, 2, 29), 1_582_934_400 / 86_400);
        // Round-trip through `epoch_to_iso` (which uses the inverse `civil_from_days`) for a
        // leap-day midnight — ties both calendar directions together.
        assert_eq!(iso_to_epoch("2020-02-29T00:00:00Z"), Some(1_582_934_400));
    }
}
