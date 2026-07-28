//! Cursor / tail reader over `.work/events.jsonl` (§19.5, §19.7).
//!
//! A [`TailReader`] is a reference consumer: it returns only **new, unique, fully-committed**
//! events, and NEVER hands out a torn tail. The two guarantees:
//!
//! * **Dedup by `event_id` (§19.7).** A line whose `event_id` was already delivered is dropped —
//!   replay/resume of the same committed fact does not re-emit it. Memory and cursor size stay
//!   bounded: the reader keeps a fixed recent-id window plus a fixed-size persisted membership
//!   filter. A filter hit outside the exact window is confirmed by a bounded streaming scan of the
//!   committed prefix, so probabilistic false positives never suppress a new event.
//! * **Torn-tail safety (§19.5).** Only newline-*terminated* lines are candidates. The trailing
//!   bytes after the last `\n` — a half-written final record from a crash mid-append, or even a
//!   valid line whose newline has not landed yet — are never delivered and the byte cursor is
//!   not advanced past them. On a later `poll`, once the newline arrives, the completed line is
//!   delivered exactly once. (This reader only *reads*; append-repair itself is §19.5's writer,
//!   out of this task's scope.)
//!
//! `events.jsonl` is append-only / single-writer (§19.6), so a byte offset is a stable cursor:
//! everything before it is permanently committed. Newline-terminated but *invalid* lines are
//! skipped (counted, not delivered) AND the cursor advances past them — a permanently corrupt
//! committed line must not wedge the stream forever, matching `tools/outbox.ps1 read`.

use std::collections::{HashSet, VecDeque};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::model::Event;
use super::outbox::open_existing_plain_outbox;
use super::parse::parse_line;

/// Largest byte range one `poll` retains at once.  The reader is long-lived, while an outbox can
/// grow indefinitely; processing it incrementally avoids allocating every unseen event in one
/// burst. A single committed event larger than this is rejected rather than risking an unbounded
/// line buffer or silently skipping a durable fact.
const MAX_POLL_BYTES: u64 = 1024 * 1024;

/// Exact recent ids retained in memory and in `events_cursor.json`. Older ids remain represented
/// by the fixed membership filter and are confirmed against the immutable committed prefix on a
/// possible match. Thus this is a performance window, not a correctness horizon.
const MAX_RECENT_IDS: usize = 512;
const DEDUPE_FILTER_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DedupeFilter {
    bits: Vec<u8>,
}

impl Default for DedupeFilter {
    fn default() -> Self {
        Self {
            bits: vec![0; DEDUPE_FILTER_BYTES],
        }
    }
}

impl DedupeFilter {
    fn insert(&mut self, event_id: &str) {
        for bit in dedupe_bits(event_id) {
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    fn maybe_contains(&self, event_id: &str) -> bool {
        dedupe_bits(event_id)
            .into_iter()
            .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }

    fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(self.bits.len() * 2);
        for byte in &self.bits {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    fn from_hex(encoded: &str) -> Result<Self, String> {
        if encoded.len() != DEDUPE_FILTER_BYTES * 2 {
            return Err(format!(
                "cursor dedupe_filter must contain exactly {} hexadecimal characters",
                DEDUPE_FILTER_BYTES * 2
            ));
        }
        let mut bits = Vec::with_capacity(DEDUPE_FILTER_BYTES);
        for pair in encoded.as_bytes().chunks_exact(2) {
            let high = hex_digit(pair[0]).ok_or("cursor dedupe_filter is not hexadecimal")?;
            let low = hex_digit(pair[1]).ok_or("cursor dedupe_filter is not hexadecimal")?;
            bits.push((high << 4) | low);
        }
        Ok(Self { bits })
    }
}

fn dedupe_bits(event_id: &str) -> [usize; 4] {
    let digest = Sha256::digest(event_id.as_bytes());
    let bit_len = DEDUPE_FILTER_BYTES * 8;
    std::array::from_fn(|index| {
        let start = index * 8;
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&digest[start..start + 8]);
        (u64::from_le_bytes(bytes) as usize) % bit_len
    })
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct RecentIds {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl RecentIds {
    fn from_ids(ids: impl IntoIterator<Item = String>) -> Self {
        let mut recent = Self::default();
        for id in ids {
            recent.insert(id);
        }
        recent
    }

    fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }

    fn insert(&mut self, id: String) {
        if !self.set.insert(id.clone()) {
            return;
        }
        self.order.push_back(id);
        if self.order.len() > MAX_RECENT_IDS
            && let Some(expired) = self.order.pop_front()
        {
            self.set.remove(&expired);
        }
    }

    fn to_vec(&self) -> Vec<String> {
        self.order.iter().cloned().collect()
    }
}

/// A durable cursor: how far the consumer has read, and which ids it has already delivered.
///
/// `byte_offset` alone would suffice for dedup *within* a monotonic file, but `delivered_ids`
/// makes dedup robust to duplicates that are appended later (idempotent replay writes the same
/// `event_id` again, §19.5) and lets a persisted cursor resume without re-emitting.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Cursor {
    pub byte_offset: u64,
    pub delivered_ids: Vec<String>,
    /// Fixed-size hexadecimal membership filter for every delivered id before `byte_offset`.
    /// `None` is accepted for compatibility with legacy cursors and is populated on parse/use.
    pub dedupe_filter: Option<String>,
}

impl Cursor {
    /// Serialize to the compact JSON shape used by the reference consumer's
    /// `events_cursor.json` (`{ "byte_offset": N, "delivered_ids": [...] }`).
    pub fn to_json(&self) -> String {
        let mut obj = Map::new();
        obj.insert("byte_offset".into(), Value::from(self.byte_offset));
        obj.insert(
            "delivered_ids".into(),
            Value::Array(
                self.delivered_ids
                    .iter()
                    .skip(self.delivered_ids.len().saturating_sub(MAX_RECENT_IDS))
                    .cloned()
                    .map(Value::from)
                    .collect(),
            ),
        );
        if let Some(filter) = &self.dedupe_filter {
            obj.insert("dedupe_filter".into(), Value::from(filter.clone()));
        }
        Value::Object(obj).to_string()
    }

    /// Parse a persisted cursor. A malformed / partial cursor is an error (the caller decides
    /// whether to fall back to a fresh cursor): this includes a `{}` empty object, a
    /// `byte_offset` that is not a non-negative integer, and a `delivered_ids` that is not an
    /// array — not just non-JSON / non-object input.
    pub fn from_json(s: &str) -> Result<Cursor, String> {
        let v: Value = serde_json::from_str(s).map_err(|e| format!("cursor is unreadable: {e}"))?;
        let obj = v.as_object().ok_or("cursor must be a JSON object")?;
        let byte_offset = obj
            .get("byte_offset")
            .ok_or("cursor is missing \"byte_offset\"")?
            .as_u64()
            .ok_or("cursor \"byte_offset\" is not a non-negative integer")?;
        let mut delivered_ids = obj
            .get("delivered_ids")
            .ok_or("cursor is missing \"delivered_ids\"")?
            .as_array()
            .ok_or("cursor \"delivered_ids\" is not an array")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .ok_or("cursor \"delivered_ids\" contains a non-string element")
            })
            .collect::<Result<Vec<String>, &str>>()?;
        let mut filter = match obj.get("dedupe_filter") {
            Some(value) => DedupeFilter::from_hex(
                value
                    .as_str()
                    .ok_or("cursor \"dedupe_filter\" is not a string")?,
            )?,
            None => DedupeFilter::default(),
        };
        for id in &delivered_ids {
            filter.insert(id);
        }
        if delivered_ids.len() > MAX_RECENT_IDS {
            delivered_ids.drain(..delivered_ids.len() - MAX_RECENT_IDS);
        }
        Ok(Cursor {
            byte_offset,
            delivered_ids,
            dedupe_filter: Some(filter.to_hex()),
        })
    }
}

/// Outcome counters from a `poll`, for observability / tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollStats {
    pub delivered: u64,
    pub skipped_invalid: u64,
    pub skipped_dup: u64,
}

/// A stateful tail reader over one events-file path.
pub struct TailReader {
    path: PathBuf,
    offset: u64,
    recent: RecentIds,
    dedupe_filter: DedupeFilter,
    stats: PollStats,
    /// Whether the last completed `poll` observed an unterminated final record. Such a tail is
    /// deliberately not delivered; a complete-history consumer can fail closed instead of
    /// treating a torn append as zero telemetry.
    unterminated_tail: bool,
}

impl TailReader {
    /// Start a fresh reader at the beginning of `path` (which need not exist yet).
    pub fn new(path: impl AsRef<Path>) -> TailReader {
        TailReader {
            path: path.as_ref().to_path_buf(),
            offset: 0,
            recent: RecentIds::default(),
            dedupe_filter: DedupeFilter::default(),
            stats: PollStats::default(),
            unterminated_tail: false,
        }
    }

    /// Resume from a persisted [`Cursor`].
    pub fn with_cursor(path: impl AsRef<Path>, cursor: &Cursor) -> TailReader {
        let mut dedupe_filter = cursor
            .dedupe_filter
            .as_deref()
            .and_then(|encoded| DedupeFilter::from_hex(encoded).ok())
            .unwrap_or_default();
        for id in &cursor.delivered_ids {
            dedupe_filter.insert(id);
        }
        TailReader {
            path: path.as_ref().to_path_buf(),
            offset: cursor.byte_offset,
            recent: RecentIds::from_ids(cursor.delivered_ids.iter().cloned()),
            dedupe_filter,
            stats: PollStats::default(),
            unterminated_tail: false,
        }
    }

    /// The cursor capturing all progress so far (persist this to resume later).
    pub fn cursor(&self) -> Cursor {
        Cursor {
            byte_offset: self.offset,
            delivered_ids: self.recent.to_vec(),
            dedupe_filter: Some(self.dedupe_filter.to_hex()),
        }
    }

    /// Cumulative counters across every `poll` on this reader.
    pub fn stats(&self) -> PollStats {
        self.stats
    }

    /// Whether the last completed [`Self::poll`] ended at an unterminated final JSONL record.
    /// The record remains unread, so a normal tail consumer can retry when the writer finishes
    /// it. A complete-snapshot consumer must instead regard this as unavailable telemetry.
    pub fn has_unterminated_tail(&self) -> bool {
        self.unterminated_tail
    }

    /// Read every currently committed record, advancing in bounded chunks. This keeps the
    /// one-megabyte record guard in [`Self::poll`] while providing one complete snapshot to a
    /// reference consumer.
    pub fn poll_all(&mut self) -> io::Result<Vec<Event>> {
        let mut events = Vec::new();
        loop {
            let offset_before = self.offset;
            events.extend(self.poll()?);
            if self.offset == offset_before {
                return Ok(events);
            }
        }
    }

    /// Read everything appended since the last poll and return the new, unique, committed
    /// events in file order. Advances the internal cursor past every newline-terminated line
    /// consumed; leaves any unterminated trailing fragment for a future poll. A missing file
    /// reads as empty (so `--follow` can wait for the file to appear).
    pub fn poll(&mut self) -> io::Result<Vec<Event>> {
        self.unterminated_tail = false;
        let mut file = match open_existing_plain_outbox(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let len = file.metadata()?.len();
        // Defensive: an append-only file should never shrink (§19.6). If it somehow did,
        // there is nothing new past our cursor to read.
        if self.offset >= len {
            return Ok(Vec::new());
        }
        let unread = len - self.offset;
        let read_len = unread.min(MAX_POLL_BYTES);
        file.seek(SeekFrom::Start(self.offset))?;
        let mut buf = Vec::with_capacity(read_len as usize);
        (&mut file).take(read_len).read_to_end(&mut buf)?;

        let mut out = Vec::new();
        let mut consumed: usize = 0; // bytes up to and including the last newline processed
        let mut line_start: usize = 0;
        for i in 0..buf.len() {
            if buf[i] == b'\n' {
                let absolute_line_start = self.offset + line_start as u64;
                let raw = &buf[line_start..i]; // line content, newline excluded
                consumed = i + 1;
                line_start = i + 1;
                self.process_line(raw, absolute_line_start, &mut file, &mut out)?;
            }
        }
        self.unterminated_tail = consumed < buf.len() && read_len == unread;
        // buf[line_start..] is the unterminated trailing fragment (torn tail or not-yet-newline
        // valid line): deliberately NOT consumed and NOT advanced past. If it has filled a whole
        // capped poll while more bytes were already committed, it cannot become a valid bounded
        // event; fail loud instead of allocating the rest of an attacker-controlled line.
        if consumed == 0 && read_len == MAX_POLL_BYTES && unread > read_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("events.jsonl record exceeds {MAX_POLL_BYTES} byte limit"),
            ));
        }
        self.offset += consumed as u64;
        Ok(out)
    }

    fn process_line(
        &mut self,
        raw: &[u8],
        absolute_line_start: u64,
        file: &mut std::fs::File,
        out: &mut Vec<Event>,
    ) -> io::Result<()> {
        // A non-UTF-8 line cannot be a valid event; treat as an invalid (skipped) line.
        let text = match std::str::from_utf8(raw) {
            Ok(t) => t.trim(),
            Err(_) => {
                self.stats.skipped_invalid += 1;
                return Ok(());
            }
        };
        if text.is_empty() {
            return Ok(()); // a blank separator line — neither delivered nor counted
        }
        match parse_line(text) {
            Ok(ev) => {
                let duplicate = self.recent.contains(&ev.event_id)
                    || self.dedupe_filter.maybe_contains(&ev.event_id)
                        && history_contains_event_id(file, absolute_line_start, &ev.event_id)?;
                if duplicate {
                    self.stats.skipped_dup += 1;
                } else {
                    self.dedupe_filter.insert(&ev.event_id);
                    self.recent.insert(ev.event_id.clone());
                    self.stats.delivered += 1;
                    out.push(ev);
                }
            }
            Err(_) => {
                self.stats.skipped_invalid += 1;
            }
        }
        Ok(())
    }
}

fn history_contains_event_id(
    file: &mut std::fs::File,
    committed_end: u64,
    event_id: &str,
) -> io::Result<bool> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let mut position = 0_u64;
    while position < committed_end {
        let remaining = committed_end - position;
        let read_limit = remaining.min(MAX_POLL_BYTES + 2);
        let mut bounded = (&mut reader).take(read_limit);
        let mut raw = Vec::new();
        let read = bounded.read_until(b'\n', &mut raw)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "events history ended before the replay boundary",
            ));
        }
        position = position
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "event offset overflowed"))?;
        if raw.last() != Some(&b'\n') || raw.len() - 1 > MAX_POLL_BYTES as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("events.jsonl record exceeds {MAX_POLL_BYTES} byte limit"),
            ));
        }
        raw.pop();
        let Ok(text) = std::str::from_utf8(&raw) else {
            continue;
        };
        if let Ok(event) = parse_line(text.trim())
            && event.event_id == event_id
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const A: &str = r#"{"schema_version":1,"event_id":"evt-a","occurred_at":"2026-07-08T12:24:10Z","type":"cohort.opened","batch_id":"B-1","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1}}"#;
    const B: &str = r#"{"schema_version":1,"event_id":"evt-b","occurred_at":"2026-07-08T12:24:11Z","type":"task.captured","batch_id":"B-1","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"wave":1}}"#;
    const C: &str = r#"{"schema_version":1,"event_id":"evt-c","occurred_at":"2026-07-08T12:24:12Z","type":"task.status_changed","task_id":"T-1","actor":{"kind":"agent","name":"processor"},"payload":{"from":"в работе","to":"на ревью"}}"#;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TmpFile {
        path: PathBuf,
    }
    impl TmpFile {
        fn new() -> TmpFile {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "orchestra-events-test-{}-{nanos}-{n}.jsonl",
                std::process::id()
            ));
            TmpFile { path }
        }
        /// Overwrite the whole file with `bytes`.
        fn set(&self, bytes: &[u8]) {
            let mut f = File::create(&self.path).unwrap();
            f.write_all(bytes).unwrap();
            f.flush().unwrap();
        }
    }
    impl Drop for TmpFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn missing_file_reads_empty() {
        let mut r = TailReader::new(std::env::temp_dir().join("does-not-exist-xyz.jsonl"));
        assert!(r.poll().unwrap().is_empty());
    }

    #[test]
    fn redirected_events_file_is_rejected() {
        let link = TmpFile::new();
        let target = TmpFile::new();
        target.set(format!("{A}\n").as_bytes());
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&target.path, &link.path);
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&target.path, &link.path);
        if linked.is_err() {
            return;
        }

        let error = TailReader::new(&link.path)
            .poll()
            .expect_err("a redirected event stream must not be consumed");
        assert!(matches!(
            error.kind(),
            io::ErrorKind::InvalidData | io::ErrorKind::Other
        ));
    }

    #[test]
    fn delivers_new_unique_events_in_order() {
        let tf = TmpFile::new();
        tf.set(format!("{A}\n{B}\n{C}\n").as_bytes());
        let mut r = TailReader::new(&tf.path);
        let evs = r.poll().unwrap();
        let ids: Vec<&str> = evs.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids, ["evt-a", "evt-b", "evt-c"]);
        // A second poll with no growth yields nothing.
        assert!(r.poll().unwrap().is_empty());
    }

    #[test]
    fn dedups_by_event_id() {
        let tf = TmpFile::new();
        // A appears twice (idempotent replay write, §19.5) — delivered once.
        tf.set(format!("{A}\n{A}\n{B}\n").as_bytes());
        let mut r = TailReader::new(&tf.path);
        let evs = r.poll().unwrap();
        let ids: Vec<&str> = evs.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids, ["evt-a", "evt-b"]);
        assert_eq!(r.stats().skipped_dup, 1);
    }

    #[test]
    fn never_delivers_torn_tail_then_completes_it() {
        let tf = TmpFile::new();
        // A full line, then a HALF-written record with no trailing newline (crash mid-append).
        let torn = r#"{"schema_version":1,"event_id":"evt-b","occurred_at":"2026-07-08T12:24"#;
        tf.set(format!("{A}\n{torn}").as_bytes());
        let mut r = TailReader::new(&tf.path);
        let evs = r.poll().unwrap();
        assert_eq!(evs.len(), 1, "only the completed line A is delivered");
        assert_eq!(evs[0].event_id, "evt-a");
        assert!(r.has_unterminated_tail());
        // The writer finishes B (repairs the tail by completing the record) and adds a newline.
        tf.set(format!("{A}\n{B}\n").as_bytes());
        let evs2 = r.poll().unwrap();
        assert_eq!(evs2.len(), 1, "the now-complete line B is delivered once");
        assert_eq!(evs2[0].event_id, "evt-b");
        assert!(!r.has_unterminated_tail());
    }

    #[test]
    fn valid_but_unterminated_final_line_waits_for_newline() {
        let tf = TmpFile::new();
        // A valid line whose trailing newline simply has not landed yet: must not be delivered.
        tf.set(format!("{A}\n{B}").as_bytes());
        let mut r = TailReader::new(&tf.path);
        let evs = r.poll().unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event_id, "evt-a");
        // Newline lands.
        tf.set(format!("{A}\n{B}\n").as_bytes());
        let evs2 = r.poll().unwrap();
        assert_eq!(evs2.len(), 1);
        assert_eq!(evs2[0].event_id, "evt-b");
    }

    #[test]
    fn skips_invalid_committed_line_and_advances_past_it() {
        let tf = TmpFile::new();
        let garbage = r#"{"schema_version":1,"event_id":"broken","type":"cohort.exploded"}"#;
        tf.set(format!("{A}\n{garbage}\n{B}\n").as_bytes());
        let mut r = TailReader::new(&tf.path);
        let evs = r.poll().unwrap();
        let ids: Vec<&str> = evs.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(
            ids,
            ["evt-a", "evt-b"],
            "invalid middle line skipped, not wedged"
        );
        assert_eq!(r.stats().skipped_invalid, 1);
        // A second poll does not re-read the invalid line.
        assert!(r.poll().unwrap().is_empty());
    }

    #[test]
    fn incremental_growth_like_follow() {
        let tf = TmpFile::new();
        tf.set(format!("{A}\n").as_bytes());
        let mut r = TailReader::new(&tf.path);
        assert_eq!(r.poll().unwrap().len(), 1);
        tf.set(format!("{A}\n{B}\n").as_bytes());
        let evs = r.poll().unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event_id, "evt-b");
        tf.set(format!("{A}\n{B}\n{C}\n").as_bytes());
        let evs = r.poll().unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event_id, "evt-c");
    }

    #[test]
    fn cursor_round_trip_resumes_without_redelivery() {
        let tf = TmpFile::new();
        tf.set(format!("{A}\n{B}\n").as_bytes());
        let mut r = TailReader::new(&tf.path);
        assert_eq!(r.poll().unwrap().len(), 2);
        let cur = r.cursor();
        let json = cur.to_json();
        let restored = Cursor::from_json(&json).unwrap();
        assert_eq!(restored, cur);
        // A fresh reader resumed from the cursor must not re-deliver A/B, but must see C.
        let mut r2 = TailReader::with_cursor(&tf.path, &restored);
        tf.set(format!("{A}\n{B}\n{C}\n").as_bytes());
        let evs = r2.poll().unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event_id, "evt-c");
    }

    #[test]
    fn from_json_rejects_non_object() {
        assert!(Cursor::from_json("not json").is_err());
        assert!(Cursor::from_json("[1,2,3]").is_err());
    }

    #[test]
    fn from_json_rejects_empty_object() {
        // `{}` is valid JSON but a partial cursor per the documented contract: error, not a
        // silent fallback to byte_offset=0 / empty delivered_ids (which would cause a full
        // replay without any diagnostic for the caller).
        assert!(Cursor::from_json("{}").is_err());
    }

    #[test]
    fn from_json_rejects_non_numeric_byte_offset() {
        let err = Cursor::from_json(r#"{"byte_offset":"nope","delivered_ids":[]}"#).unwrap_err();
        assert!(
            err.contains("byte_offset"),
            "error mentions the field: {err}"
        );
    }

    #[test]
    fn from_json_rejects_non_array_delivered_ids() {
        let err = Cursor::from_json(r#"{"byte_offset":5,"delivered_ids":"nope"}"#).unwrap_err();
        assert!(
            err.contains("delivered_ids"),
            "error mentions the field: {err}"
        );
    }

    #[test]
    fn from_json_rejects_non_string_delivered_id() {
        // A non-string element (int, null, nested object, ...) must not be silently discarded
        // from `delivered_ids` — that would make dedup lose entries without any diagnostic.
        for bad in [
            r#"{"byte_offset":5,"delivered_ids":[123]}"#,
            r#"{"byte_offset":5,"delivered_ids":[null]}"#,
            r#"{"byte_offset":5,"delivered_ids":[{"id":"evt-a"}]}"#,
            r#"{"byte_offset":5,"delivered_ids":["evt-a",42]}"#,
        ] {
            let err = Cursor::from_json(bad).unwrap_err();
            assert!(
                err.contains("delivered_ids"),
                "error mentions the field for {bad}: {err}"
            );
        }
    }

    #[test]
    fn from_json_accepts_well_formed_cursor() {
        let cur = Cursor::from_json(r#"{"byte_offset":5,"delivered_ids":["evt-a"]}"#).unwrap();
        assert_eq!(
            cur,
            Cursor {
                byte_offset: 5,
                delivered_ids: vec!["evt-a".to_string()],
                dedupe_filter: Some({
                    let mut filter = DedupeFilter::default();
                    filter.insert("evt-a");
                    filter.to_hex()
                }),
            }
        );
    }

    #[test]
    fn blank_lines_are_ignored() {
        let tf = TmpFile::new();
        tf.set(format!("{A}\n\n{B}\n").as_bytes());
        let mut r = TailReader::new(&tf.path);
        assert_eq!(r.poll().unwrap().len(), 2);
        assert_eq!(r.stats().skipped_invalid, 0);
    }

    #[test]
    fn bounded_cursor_still_suppresses_a_replay_older_than_the_exact_window() {
        let tf = TmpFile::new();
        let template = parse_line(A).unwrap();
        let mut journal = String::new();
        let mut first_line = String::new();
        for index in 0..MAX_RECENT_IDS + 32 {
            let mut event = template.clone();
            event.event_id = format!("evt-window-{index}");
            let line = event.to_json_line();
            if index == 0 {
                first_line.clone_from(&line);
            }
            journal.push_str(&line);
            journal.push('\n');
        }
        tf.set(journal.as_bytes());
        let mut reader = TailReader::new(&tf.path);
        assert_eq!(reader.poll_all().unwrap().len(), MAX_RECENT_IDS + 32);
        let cursor = reader.cursor();
        assert_eq!(cursor.delivered_ids.len(), MAX_RECENT_IDS);
        assert!(!cursor.delivered_ids.iter().any(|id| id == "evt-window-0"));
        assert!(
            cursor.to_json().len() < 100_000,
            "the persisted cursor must remain under a fixed practical ceiling"
        );
        let restored = Cursor::from_json(&cursor.to_json()).unwrap();

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&tf.path)
            .unwrap();
        writeln!(file, "{first_line}").unwrap();
        file.flush().unwrap();
        let mut resumed = TailReader::with_cursor(&tf.path, &restored);
        assert!(resumed.poll_all().unwrap().is_empty());
        assert_eq!(resumed.stats().skipped_dup, 1);
        assert_eq!(resumed.cursor().delivered_ids.len(), MAX_RECENT_IDS);
    }

    #[test]
    fn refuses_one_committed_record_larger_than_the_poll_bound() {
        let tf = TmpFile::new();
        let mut overlong = vec![b'x'; MAX_POLL_BYTES as usize + 1];
        overlong.push(b'\n');
        tf.set(&overlong);

        let mut reader = TailReader::new(&tf.path);
        let error = reader
            .poll()
            .expect_err("an overlong committed record must fail loud");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            reader.cursor().byte_offset,
            0,
            "no partial record is consumed"
        );
    }
}
