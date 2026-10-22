//! Cursor source: reconstruct chat threads from the Cursor SQLite state DB.
//!
//! Storage shape (verified against a real ~11GB `state.vscdb`, 1,282 threads /
//! 178k bubbles, 2026-07):
//!
//!   table `cursorDiskKV (key TEXT UNIQUE, value BLOB)`. Values are JSON.
//!   key `composerData:<composerId>` -> one thread. The value carries:
//!     - `composerId`, `name`, `createdAt` (epoch ms, always present),
//!       `lastUpdatedAt` (epoch ms, ~2/3 of threads),
//!     - `modelConfig.modelName` (rare),
//!     - the bubbles, in ONE of two layouts:
//!         (a) inline: `conversation: [ {bubbleId, type, text, richText, ...} ]`
//!             (the common case, ~65% of non-empty threads), or
//!         (b) headers-only: `conversation: []` plus
//!             `fullConversationHeadersOnly: [ {bubbleId, type} ]` giving the
//!             canonical order, with each message stored in a separate row
//!             `bubbleId:<composerId>:<bubbleId>` (value JSON with `text`,
//!             `richText`, `type`, ...).
//!   bubble `type`: 1 = user, 2 = assistant. No per-bubble timestamp exists, so
//!   thread times come from `createdAt` / `lastUpdatedAt`.
//!   Empty threads (no conversation and no headers) are skipped.
//!
//! `raw` is LOSSLESS: it is `{ "composerData": <original value>, "bubbles":
//! [<original bubble values, in canonical order>] }`. The original JSON
//! structures are preserved verbatim, never flattened into a text-only shape.
//! The searchable text projection (concatenated bubble `text`) is derived
//! separately so lexical search works without touching `raw`.
//!
//! The DB is opened READ-ONLY / immutable (`?immutable=1`): Cursor may be running
//! and hold a write lock, and we must never write to it.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use chrono::{DateTime, TimeZone, Utc};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row};
use serde_json::{json, Value};

use session_capture::{Metadata, Origin, SessionEnvelope, SCS_VERSION};

/// A thread's collected bubbles: the verbatim JSON values (in canonical order),
/// their extracted plain `text`, and their `type` codes (1 = user, 2 = assistant).
type Bubbles = (Vec<Value>, Vec<String>, Vec<i64>);

/// One reconstructed Cursor thread ready to upload.
pub struct CursorThread {
    /// A stable per-thread fingerprint (`lastUpdatedAt:bubble_count`) so the
    /// exporter skips unchanged threads without re-reading the whole DB payload.
    pub fingerprint: String,
    pub envelope: SessionEnvelope,
}

/// Errors reading the Cursor DB.
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("open cursor db {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("query cursor db: {0}")]
    Query(#[from] rusqlite::Error),
}

/// Open the Cursor state DB read-only and immutable. Immutable mode lets us read
/// even while Cursor holds a write lock, and guarantees we never mutate it.
fn open_immutable(db: &Path) -> Result<Connection, CursorError> {
    // rusqlite honors the `immutable=1` URI parameter when the URI flag is set.
    let uri = format!("file:{}?immutable=1", db.display());
    Connection::open_with_flags(
        &uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|e| CursorError::Open {
        path: db.display().to_string(),
        source: e,
    })
}

/// Read a column as an i64 only when SQLite returned an INTEGER. Mirrors serde's
/// `as_i64`: `json_extract` yields REAL for fractions, exponents and integers
/// beyond i64, and TEXT for strings, none of which `as_i64` accepts either.
fn int_col(row: &Row<'_>, idx: usize) -> Option<i64> {
    match row.get_ref(idx) {
        Ok(ValueRef::Integer(n)) => Some(n),
        _ => None,
    }
}

/// Read a column as an owned UTF-8 string only when SQLite returned TEXT. Invalid
/// UTF-8 maps to None, matching serde (which rejects the whole value).
fn text_col(row: &Row<'_>, idx: usize) -> Option<String> {
    match row.get_ref(idx) {
        Ok(ValueRef::Text(b)) => std::str::from_utf8(b).ok().map(str::to_owned),
        _ => None,
    }
}

/// One thread's cheap change token, computed without materialising the thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadFingerprint {
    /// The `cursorDiskKV` key of the thread's composer row, which `load` reads.
    pub key: String,
    /// The thread's `composerId`, which is its session id and state key.
    pub composer_id: String,
    /// Byte-identical to the `CursorThread::fingerprint` that `reconstruct`
    /// produces for the same row, so state files written by the full reader
    /// stay valid and nothing is re-sent on upgrade.
    pub fingerprint: String,
}

/// An open, immutable Cursor DB, held for one run so the cheap scan and the
/// per-thread loads share a single connection.
pub struct CursorDb {
    conn: Connection,
}

impl CursorDb {
    /// Open `db` read-only and immutable (never written).
    pub fn open(db: &Path) -> Result<Self, CursorError> {
        Ok(Self {
            conn: open_immutable(db)?,
        })
    }

    /// Fingerprint every non-empty thread (newest `limit` by `createdAt`)
    /// without pulling thread values into Rust. See `scan_conn`.
    pub fn scan(&self, limit: Option<usize>) -> Result<Vec<ThreadFingerprint>, CursorError> {
        scan_conn(&self.conn, limit)
    }

    /// Fully reconstruct ONE thread from its composer row `key`. `None` when the
    /// row vanished, is not valid JSON, or reconstructs to an empty thread.
    pub fn load(&self, key: &str, origin: &Origin) -> Result<Option<CursorThread>, CursorError> {
        load_conn(&self.conn, key, origin)
    }
}

/// Per-thread fields the scan pulls out with SQLite's JSON functions. Every
/// `json_*` call runs on `CAST(value AS TEXT)`: on a BLOB, SQLite 3.45+ would
/// otherwise try to read the bytes as JSONB, while serde reads them as JSON text.
///
/// The full reader selects `key LIKE 'composerData:%'`, which is
/// case-insensitive and so cannot use the UNIQUE(key) index: a full table scan.
/// The same `LIKE` is kept here, so exactly the same rows match, but behind two
/// index ranges that cover every ASCII case variant of the prefix in BINARY
/// order (those starting `C`, then those starting `c`), so only keys in those
/// ranges are visited and the filter runs on the index entry alone. The rowid is returned so `scan_conn` can restore the full
/// reader's rowid (table-scan) order in Rust, so a `limit` breaks `createdAt`
/// ties identically. There is deliberately no `ORDER BY`: SQLite would sort
/// every projected row, header lists included, in a temp B-tree, holding them
/// all at once. `json_type(v)` sits inside a `CASE WHEN json_valid(v)` so it is
/// never evaluated on malformed text (where it would raise instead of returning
/// NULL). Each typed column is NULL unless the JSON field has exactly the type
/// serde's accessor accepts. For the timestamps that means a JSON integer that
/// fits i64 (`json_extract` returns REAL beyond that, which `int_col` rejects)
/// and is not the token `-0`, which SQLite calls an integer but serde parses as
/// a float (`as_i64` = None).
///
/// The header list is only extracted when the inline `conversation` is not a
/// non-empty array, the one case `message_count` needs it.
///
/// Known, accepted divergences between SQLite's JSON reader and serde (none
/// present in the real DB, where every thread's scan fingerprint equals the
/// full reader's). SQLite accepts, where serde rejects: numbers beyond f64 range
/// (`1e999`), lone UTF-16 surrogate escapes, invalid UTF-8 inside strings, and
/// nesting deeper than serde's 128-level limit. SQLite also keeps the FIRST of
/// duplicate object keys where serde keeps the last (and the full reader's
/// `createdAt` probe rejects a duplicated `createdAt` outright). Consequences:
///   - a composerData value only SQLite accepts scans as a candidate but loads
///     as `None`: skipped and unmarked, so it is reloaded (and skipped) on every
///     run that is not gated;
///   - a bubble value only SQLite accepts, or a duplicate key, makes the scan's
///     fingerprint differ from `reconstruct`'s. Cursor marks the SCAN
///     fingerprint (see `sources::load`), so this does not re-send every run,
///     but a later repair that leaves `lastUpdatedAt` and the scanned count
///     unchanged stays invisible, the same blindness the fingerprint already
///     has to in-place bubble edits.
const SCAN_SQL: &str = "\
SELECT
  key,
  CASE json_type(v, '$.composerId') WHEN 'text' THEN json_extract(v, '$.composerId') END,
  CASE WHEN json_type(v, '$.createdAt') = 'integer' AND (v -> '$.createdAt') <> '-0'
    THEN json_extract(v, '$.createdAt') END,
  CASE WHEN json_type(v, '$.lastUpdatedAt') = 'integer' AND (v -> '$.lastUpdatedAt') <> '-0'
    THEN json_extract(v, '$.lastUpdatedAt') END,
  CASE json_type(v, '$.conversation') WHEN 'array' THEN json_array_length(v, '$.conversation') ELSE 0 END,
  CASE
    WHEN json_type(v, '$.conversation') = 'array' AND json_array_length(v, '$.conversation') > 0 THEN NULL
    WHEN json_type(v, '$.fullConversationHeadersOnly') = 'array' THEN json_extract(v, '$.fullConversationHeadersOnly')
  END,
  rid
FROM (
  SELECT rowid AS rid, key, CAST(value AS TEXT) AS v
  FROM cursorDiskKV
  WHERE ((key >= 'COMPOSERDATA:' AND key < 'Composerdata;')
      OR (key >= 'cOMPOSERDATA:' AND key < 'composerdata;'))
    AND key LIKE 'composerData:%'
    AND typeof(value) IN ('text', 'blob')
)
WHERE CASE WHEN json_valid(v) THEN json_type(v) END = 'object'";

/// The full reader's first-pass probe of a composer row: only `createdAt`, for
/// ordering. A row this rejects (not JSON, or a duplicated `createdAt`) is not a
/// thread to the full reader.
#[derive(serde::Deserialize)]
struct Created {
    #[serde(rename = "createdAt")]
    created: Option<Value>,
}

/// One scanned composerData row, before the (possibly bubble-row-backed)
/// message count is known.
struct ScannedRow {
    key: String,
    composer_id: Option<String>,
    created_ms: Option<i64>,
    updated_ms: Option<i64>,
    inline_len: i64,
    headers: Option<String>,
    rowid: i64,
}

/// Cheap scan: the per-thread fingerprint `reconstruct` would produce, computed
/// without materialising any thread.
///
/// Why: the full reader parses every `composerData` value (and every bubble of
/// every headers-only thread), which a capture run then throws ~99.5% away as
/// unchanged. Here SQLite extracts only the handful of scalar fields the
/// fingerprint depends on; the only JSON parsed in Rust is a headers-only
/// thread's small header list. Bubble validity is checked in SQL, returning
/// keys, never values.
fn scan_conn(
    conn: &Connection,
    limit: Option<usize>,
) -> Result<Vec<ThreadFingerprint>, CursorError> {
    let mut stmt = conn.prepare(SCAN_SQL)?;
    let rows = stmt
        .query_map([], |row| {
            // Decoding the key as a String keeps the full reader's behaviour: a
            // composerData key that is not UTF-8 aborts the read.
            Ok(ScannedRow {
                key: row.get(0)?,
                composer_id: text_col(row, 1),
                created_ms: int_col(row, 2),
                updated_ms: int_col(row, 3),
                inline_len: int_col(row, 4).unwrap_or(0),
                headers: text_col(row, 5),
                rowid: int_col(row, 6).unwrap_or(0),
            })
        })
        // A fixed SQL statement with no bound parameters can only fail to bind on
        // a placeholder/param-count mismatch, a programming invariant.
        .expect("static composerData scan has no parameters to bind");

    // Count each thread while iterating, so at most one header list is alive at
    // a time; only a small (createdAt, rowid, fingerprint) triple per non-empty
    // thread is kept for the sort. `reconstruct` skips an empty composerId
    // before anything else, and an empty thread (count 0) is dropped before
    // `limit` applies, exactly as the full reader does.
    let mut scanned: Vec<(i64, i64, ThreadFingerprint)> = Vec::new();
    for row in rows {
        let row = row?;
        let Some(composer_id) = row.composer_id.filter(|c| !c.is_empty()) else {
            continue;
        };
        let count = message_count(conn, &composer_id, row.inline_len, row.headers.as_deref())?;
        if count == 0 {
            continue;
        }
        let updated = row.updated_ms.or(row.created_ms).unwrap_or(0);
        scanned.push((
            row.created_ms.unwrap_or(0),
            row.rowid,
            ThreadFingerprint {
                key: row.key,
                fingerprint: format!("{updated}:{count}"),
                composer_id,
            },
        ));
    }
    // Same newest-first order as `visit_threads_conn` (its candidates sort on
    // createdAt, then table-scan i.e. rowid order), so `limit` keeps the same
    // threads.
    scanned.sort_by_key(|(created, rowid, _)| (std::cmp::Reverse(*created), *rowid));
    let Some(limit) = limit else {
        return Ok(scanned.into_iter().map(|(_, _, t)| t).collect());
    };
    // Under a limit, a row the full reader would not emit (SQLite accepts it but
    // serde rejects the composer or its bubbles, see SCAN_SQL) must not take a
    // slot: the full reader fills the slot with the next thread, and a run would
    // otherwise load nothing for that slot and never capture the thread that
    // belongs there. Only the selected rows are probed, one thread at a time.
    let mut kept = Vec::with_capacity(limit.min(scanned.len()));
    for (_, _, thread) in scanned {
        if kept.len() >= limit {
            break;
        }
        if full_reader_emits(conn, &thread.key)? {
            kept.push(thread);
        }
    }
    Ok(kept)
}

/// Whether the full reader would emit the composer row at `key` as a thread:
/// its `createdAt` probe accepts the row and the row reconstructs to a
/// non-empty thread with serde's reading of the composer and bubble rows.
fn full_reader_emits(conn: &Connection, key: &str) -> Result<bool, CursorError> {
    if !created_probe_accepts(conn, key)? {
        return Ok(false);
    }
    Ok(load_conn(conn, key, &Origin::new("probe", "local"))?.is_some())
}

/// The full reader's first-pass `createdAt` probe of the row at `key`.
fn created_probe_accepts(conn: &Connection, key: &str) -> Result<bool, CursorError> {
    let accepted = conn
        .query_row(
            "SELECT value FROM cursorDiskKV WHERE key = ?1",
            [key],
            |row| {
                Ok(match row.get_ref(0)? {
                    ValueRef::Text(b) | ValueRef::Blob(b) => {
                        serde_json::from_slice::<Created>(b).is_ok()
                    }
                    _ => false,
                })
            },
        )
        .optional()?;
    Ok(accepted.unwrap_or(false))
}

/// The number of bubbles `collect_bubbles` would gather, without gathering them.
/// A non-empty inline `conversation` counts every element; otherwise each header
/// with a string `bubbleId` counts once per header (duplicates included, as
/// `push_bubble` does) when its bubble row is non-NULL valid JSON.
fn message_count(
    conn: &Connection,
    composer_id: &str,
    inline_len: i64,
    headers: Option<&str>,
) -> Result<u64, CursorError> {
    if inline_len > 0 {
        return Ok(inline_len as u64);
    }
    // SQLite already validated the header array, so serde accepts it; a parse
    // failure (not observed) would count as zero, like a missing header list.
    let Some(headers) = headers.and_then(|h| serde_json::from_str::<Vec<Value>>(h).ok()) else {
        return Ok(0);
    };
    let valid = valid_bubble_ids(conn, composer_id)?;
    Ok(headers
        .iter()
        .filter_map(|h| h.get("bubbleId").and_then(|v| v.as_str()))
        .filter(|bid| valid.contains(*bid))
        .count() as u64)
}

/// bubbleIds whose `bubbleId:<composerId>:<bubbleId>` row holds non-NULL valid
/// JSON, i.e. exactly the keys `load_bubble_rows` would keep. Only keys leave
/// SQLite; `json_valid` reads each value in place. Same half-open key range as
/// `load_bubble_rows` (index range scan, no cross-thread leakage).
fn valid_bubble_ids(conn: &Connection, composer_id: &str) -> Result<HashSet<String>, CursorError> {
    let prefix = format!("bubbleId:{composer_id}:");
    let upper = format!("bubbleId:{composer_id};");
    let mut stmt = conn.prepare_cached(
        "SELECT key FROM cursorDiskKV WHERE key >= ?1 AND key < ?2 \
         AND typeof(value) IN ('text', 'blob') AND json_valid(CAST(value AS TEXT))",
    )?;
    let rows = stmt
        .query_map([&prefix, &upper], |row| row.get::<_, String>(0))
        // Two bound string parameters against two `?N` placeholders.
        .expect("static bubbleId key query binds exactly its two parameters");
    let mut ids = HashSet::new();
    for key in rows {
        let key = key?;
        ids.insert(key.strip_prefix(&prefix).unwrap_or(&key).to_string());
    }
    Ok(ids)
}

/// Fetch one composerData row by key and reconstruct it, unbounded, exactly as
/// the full reader does. The row is the only thread value materialised, so a
/// run holds at most one thread at a time.
fn load_conn(
    conn: &Connection,
    key: &str,
    origin: &Origin,
) -> Result<Option<CursorThread>, CursorError> {
    let bytes = conn
        .query_row(
            "SELECT value FROM cursorDiskKV WHERE key = ?1",
            [key],
            |row| {
                Ok(match row.get_ref(0)? {
                    ValueRef::Text(b) | ValueRef::Blob(b) => Some(b.to_vec()),
                    _ => None,
                })
            },
        )
        .optional()?
        .flatten();
    let Some(cd) = bytes.and_then(|b| serde_json::from_slice::<Value>(&b).ok()) else {
        return Ok(None);
    };
    // An unbounded budget never reports Oversize, so only a Thread is kept.
    Ok(match reconstruct(conn, &cd, origin, u64::MAX)? {
        Assembled::Thread(thread) => Some(*thread),
        _ => None,
    })
}

/// Reconstruct every non-empty Cursor thread from the DB.
///
/// `limit` caps how many threads to return (newest by `createdAt` first) so a
/// bounded/test ingest is possible; `None` returns all.
pub fn read_threads(
    db: &Path,
    origin: &Origin,
    limit: Option<usize>,
) -> Result<Vec<CursorThread>, CursorError> {
    let conn = open_immutable(db)?;
    read_threads_conn(&conn, origin, limit)
}

/// Explicit bounds on one streaming Cursor read, so peak memory does not grow
/// with the number or size of threads in the database.
#[derive(Debug, Clone, Copy)]
pub struct ThreadBounds {
    /// Most candidate threads held for ordering. Older threads beyond it are
    /// reported as [`ThreadItem::Overflow`], never silently dropped.
    pub max_threads: usize,
    /// Most bytes of composer and bubble rows read for one thread. A larger
    /// thread is reported as [`ThreadItem::Oversize`] without being assembled.
    pub max_bytes: u64,
}

impl ThreadBounds {
    pub const UNBOUNDED: Self = Self {
        max_threads: usize::MAX,
        max_bytes: u64::MAX,
    };
}

/// One streamed Cursor item.
pub enum ThreadItem {
    Thread(Box<CursorThread>),
    /// The composer key of a thread whose rows exceed the byte bound.
    Oversize(String),
    /// Number of threads left out by the count bound.
    Overflow(u64),
}

/// Stream threads newest first, holding one assembled thread at a time.
/// Returns the visitor's own error in the inner `Result`.
pub fn visit_threads<E>(
    db: &Path,
    origin: &Origin,
    limit: Option<usize>,
    bounds: ThreadBounds,
    visit: &mut dyn FnMut(ThreadItem) -> Result<(), E>,
) -> Result<Result<(), E>, CursorError> {
    let conn = open_immutable(db)?;
    visit_threads_conn(&conn, origin, limit, bounds, visit)
}

/// Reconstruct threads from an already-open connection. Split out from
/// `read_threads` so it is unit-testable against an in-memory DB (no file, no
/// immutable-URI open) that mirrors the real `cursorDiskKV` shape.
fn read_threads_conn(
    conn: &Connection,
    origin: &Origin,
    limit: Option<usize>,
) -> Result<Vec<CursorThread>, CursorError> {
    let mut out = Vec::new();
    let visited = visit_threads_conn(conn, origin, limit, ThreadBounds::UNBOUNDED, &mut |item| {
        if let ThreadItem::Thread(thread) = item {
            out.push(*thread);
        }
        Ok::<(), std::convert::Infallible>(())
    })?;
    let Ok(()) = visited;
    Ok(out)
}

/// A candidate thread: ordering key, row order (for a stable tie-break),
/// composer key, and whether its composer row alone is over the byte bound.
type Candidate = (std::cmp::Reverse<i64>, usize, String, bool);

/// Keep the newest `cap` candidates, returning how many were dropped.
fn keep_newest(candidates: &mut Vec<Candidate>, cap: usize) -> u64 {
    candidates.sort();
    let dropped = candidates.len().saturating_sub(cap);
    candidates.truncate(cap);
    dropped as u64
}

/// Two passes. The first streams composer rows, reading only each row's size
/// and `createdAt`, and keeps at most `max_threads` (key, time) candidates.
/// The second fetches, assembles, and hands over one thread at a time.
fn visit_threads_conn<E>(
    conn: &Connection,
    origin: &Origin,
    limit: Option<usize>,
    bounds: ThreadBounds,
    visit: &mut dyn FnMut(ThreadItem) -> Result<(), E>,
) -> Result<Result<(), E>, CursorError> {
    let mut stmt =
        conn.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'")?;
    let mut rows = stmt.query([])?;
    let mut candidates: Vec<Candidate> = Vec::new();
    let mut overflow = 0;
    let mut seq = 0;
    while let Some(row) = rows.next()? {
        // cursorDiskKV.value is TEXT (the vast majority), BLOB, or NULL
        // depending on Cursor's version. Borrow TEXT/BLOB bytes in place and
        // skip anything else. `get_ref(1)` cannot fail for this fixed column.
        let bytes = match row.get_ref(1) {
            Ok(rusqlite::types::ValueRef::Text(b)) | Ok(rusqlite::types::ValueRef::Blob(b)) => b,
            _ => continue,
        };
        let key: String = row.get(0)?;
        seq += 1;
        if bytes.len() as u64 > bounds.max_bytes {
            candidates.push((std::cmp::Reverse(0), seq, key, true));
        } else {
            let Ok(created) = serde_json::from_slice::<Created>(bytes) else {
                continue;
            };
            let created = created.created.and_then(|v| v.as_i64()).unwrap_or(0);
            candidates.push((std::cmp::Reverse(created), seq, key, false));
        }
        if candidates.len() >= bounds.max_threads.saturating_mul(2).max(1024) {
            overflow += keep_newest(&mut candidates, bounds.max_threads);
        }
    }
    drop(rows);
    overflow += keep_newest(&mut candidates, bounds.max_threads);
    if overflow > 0 {
        if let Err(error) = visit(ThreadItem::Overflow(overflow)) {
            return Ok(Err(error));
        }
    }

    let mut emitted = 0;
    for (_, _, key, oversize) in candidates {
        if limit.is_some_and(|n| emitted >= n) {
            break;
        }
        let item = if oversize {
            ThreadItem::Oversize(key)
        } else {
            let value: Option<Vec<u8>> = conn.query_row(
                "SELECT value FROM cursorDiskKV WHERE key=?1",
                [&key],
                |row| {
                    Ok(match row.get_ref(0)? {
                        rusqlite::types::ValueRef::Text(b) | rusqlite::types::ValueRef::Blob(b) => {
                            Some(b.to_vec())
                        }
                        _ => None,
                    })
                },
            )?;
            let Some((cd, size)) = value.and_then(|b| {
                serde_json::from_slice::<Value>(&b)
                    .ok()
                    .map(|cd| (cd, b.len() as u64))
            }) else {
                continue;
            };
            let budget = bounds.max_bytes.saturating_sub(size);
            match reconstruct(conn, &cd, origin, budget)? {
                Assembled::Empty => continue,
                Assembled::Oversize => ThreadItem::Oversize(key),
                Assembled::Thread(thread) => ThreadItem::Thread(thread),
            }
        };
        emitted += 1;
        if let Err(error) = visit(item) {
            return Ok(Err(error));
        }
    }
    Ok(Ok(()))
}

/// What reconstructing one composer produced.
enum Assembled {
    Thread(Box<CursorThread>),
    /// No bubbles: nothing worth storing.
    Empty,
    /// Its bubble rows exceed the byte bound; nothing beyond it was loaded.
    Oversize,
}

/// Build one envelope from a composerData value. Returns `None` for empty
/// threads (no bubbles), which carry no content worth storing. The pure
/// reconstruction logic is measured via the in-memory `read_threads` tests.
fn reconstruct(
    conn: &Connection,
    cd: &Value,
    origin: &Origin,
    budget: u64,
) -> Result<Assembled, CursorError> {
    let composer_id = cd
        .get("composerId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if composer_id.is_empty() {
        return Ok(Assembled::Empty);
    }

    // Gather bubbles in canonical order plus their plain text, verbatim.
    let Some((bubble_values, texts, types)) = collect_bubbles(conn, cd, &composer_id, budget)?
    else {
        return Ok(Assembled::Oversize);
    };
    if bubble_values.is_empty() {
        return Ok(Assembled::Empty);
    }

    let message_count = bubble_values.len() as u64;

    // Times: createdAt is always present; lastUpdatedAt falls back to createdAt
    // when absent (documented). No per-bubble timestamps exist, so a thread with
    // no createdAt (not observed in practice) falls back to now() as a last
    // resort, which we flag by leaving both equal.
    let created_ms = cd.get("createdAt").and_then(|v| v.as_i64());
    let updated_ms = cd.get("lastUpdatedAt").and_then(|v| v.as_i64());
    let (started_at, last_activity_at) = thread_times(created_ms, updated_ms);

    // Model: only when genuinely present (modelConfig.modelName). Never invented.
    let model = cd
        .get("modelConfig")
        .and_then(|m| m.get("modelName"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    // Tags: none native to Cursor. Leave empty (do not invent).
    let metadata = Metadata {
        repo: None,
        git_remote: None,
        cwd: None,
        project: cd
            .get("name")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()),
        model,
        tags: Vec::new(),
        message_count: Some(message_count),
        workflow_id: None,
        source_path: None,
        extra: Default::default(),
    };

    // raw: lossless, verbatim. The original composerData plus the ordered
    // original bubble values. Nothing flattened.
    //
    // The searchable text projection is derived server-side from `raw`: the
    // store's `cursor-state-vscdb` projection reads `raw.bubbles[].text` (the
    // concatenated bubble text), so lexical search works without a separate
    // stored text shape. `texts` / `types` are validated in tests but are not
    // needed on the envelope because the projection reads them back from `raw`.
    let _ = (&texts, &types);
    let raw = json!({
        "composerData": cd.clone(),
        "bubbles": Value::Array(bubble_values),
    });

    let fingerprint = format!(
        "{}:{}",
        updated_ms.or(created_ms).unwrap_or(0),
        message_count
    );

    let envelope = SessionEnvelope {
        scs_version: SCS_VERSION.to_string(),
        origin: origin.clone(),
        agent: "Cursor".to_string(),
        source_format: "cursor-state-vscdb".to_string(),
        session_id: composer_id,
        parent_session_id: None,
        started_at: started_at.to_rfc3339(),
        last_activity_at: last_activity_at.to_rfc3339(),
        content_hash: None,
        metadata: Some(metadata),
        raw,
    };

    Ok(Assembled::Thread(Box::new(CursorThread {
        fingerprint,
        envelope,
    })))
}

/// Collect the thread's bubbles in canonical order, returning their verbatim
/// JSON values, their plain `text`, and their `type` codes. Both layouts (inline
/// and headers-only) are measured via the in-memory `read_threads` tests.
/// `None` when separate bubble rows exceed `budget` bytes.
fn collect_bubbles(
    conn: &Connection,
    cd: &Value,
    composer_id: &str,
    budget: u64,
) -> Result<Option<Bubbles>, CursorError> {
    let mut values = Vec::new();
    let mut texts = Vec::new();
    let mut types = Vec::new();

    // Layout (a): inline conversation.
    if let Some(conv) = cd.get("conversation").and_then(|v| v.as_array()) {
        if !conv.is_empty() {
            for b in conv {
                push_bubble(b, &mut values, &mut texts, &mut types);
            }
            return Ok(Some((values, texts, types)));
        }
    }

    // Layout (b): headers-only + separate bubble rows. The header list gives the
    // canonical order; we fetch each bubble row by key.
    if let Some(headers) = cd
        .get("fullConversationHeadersOnly")
        .and_then(|v| v.as_array())
    {
        // Pre-load all bubble rows for this composer into a map for O(1) lookup,
        // so we do one scan instead of N point queries.
        let Some(bubble_map) = load_bubble_rows(conn, composer_id, budget)? else {
            return Ok(None);
        };
        for h in headers {
            let Some(bid) = h.get("bubbleId").and_then(|v| v.as_str()) else {
                continue;
            };
            if let Some(b) = bubble_map.get(bid) {
                push_bubble(b, &mut values, &mut texts, &mut types);
            }
        }
    }

    Ok(Some((values, texts, types)))
}

/// Load every `bubbleId:<composerId>:<bubbleId>` row's JSON, keyed by bubbleId.
/// Null-valued rows (placeholders observed in real data) are skipped. The scan,
/// coercion, and JSON-decode logic are measured via the headers-only in-memory
/// `read_threads` tests.
fn load_bubble_rows(
    conn: &Connection,
    composer_id: &str,
    budget: u64,
) -> Result<Option<HashMap<String, Value>>, CursorError> {
    // Use a half-open range instead of `LIKE ?1`. A bound LIKE parameter defeats
    // SQLite's index on `key` (it cannot prove the pattern is a prefix at plan
    // time), forcing a full-table scan of ~178k rows PER thread: 1,282 threads
    // times a full scan is minutes. `key >= prefix AND key < upper` is a range
    // scan on the UNIQUE(key) index. `upper` is `prefix` with its trailing ':'
    // (0x3a) bumped to ';' (0x3b), so it covers exactly `prefix*`.
    let prefix = format!("bubbleId:{composer_id}:");
    let upper = format!("bubbleId:{composer_id};");
    let mut stmt =
        conn.prepare("SELECT key, value FROM cursorDiskKV WHERE key >= ?1 AND key < ?2")?;
    let rows = stmt
        .query_map([&prefix, &upper], |row| {
            let key: String = row.get(0)?;
            // cursorDiskKV.value is stored as TEXT (the vast majority), BLOB, or
            // NULL depending on Cursor's version. rusqlite's Vec<u8> reader rejects
            // TEXT, so read the raw ValueRef and coerce TEXT/BLOB to bytes, mapping
            // NULL (and any other type) to None so the row is skipped. `get_ref(1)`
            // cannot error for this fixed in-range column index, so its
            // (unreachable) Err folds into the same skip arm rather than an
            // uncovered `?` edge.
            let value: Option<Vec<u8>> = match row.get_ref(1) {
                Ok(rusqlite::types::ValueRef::Text(b)) | Ok(rusqlite::types::ValueRef::Blob(b)) => {
                    Some(b.to_vec())
                }
                _ => None,
            };
            Ok((key, value))
        })
        // Two bound string parameters against two `?N` placeholders: a bind
        // failure is a param-count invariant violation, not a forceable runtime
        // error.
        .expect("static bubbleId range query binds exactly its two parameters");
    let mut map = HashMap::new();
    let mut total: u64 = 0;
    for row in rows {
        let (key, value) = row?;
        let Some(bytes) = value else { continue };
        total = total.saturating_add(bytes.len() as u64);
        if total > budget {
            return Ok(None);
        }
        let Ok(v) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        let bid = key.strip_prefix(&prefix).unwrap_or(&key).to_string();
        map.insert(bid, v);
    }
    Ok(Some(map))
}

/// Append one bubble's verbatim value + extracted text + type.
fn push_bubble(b: &Value, values: &mut Vec<Value>, texts: &mut Vec<String>, types: &mut Vec<i64>) {
    values.push(b.clone());
    if let Some(t) = b.get("text").and_then(|v| v.as_str()) {
        if !t.is_empty() {
            texts.push(t.to_string());
        }
    }
    types.push(b.get("type").and_then(|v| v.as_i64()).unwrap_or(0));
}

/// Resolve (started_at, last_activity_at) from epoch-ms timestamps.
///
/// Fallbacks (documented): if `lastUpdatedAt` is absent, it equals
/// `createdAt`. If `createdAt` is also absent (not seen in real data), both fall
/// back to `now()`, and being equal signals the missing-timestamp case.
fn thread_times(
    created_ms: Option<i64>,
    updated_ms: Option<i64>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    match created_ms {
        Some(c) => {
            let start = ms_to_dt(c);
            let last = updated_ms
                .map(ms_to_dt)
                .filter(|u| *u >= start)
                .unwrap_or(start);
            (start, last)
        }
        None => {
            let now = Utc::now();
            (now, now)
        }
    }
}

fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn origin() -> Origin {
        Origin::new("test-host", "laptop")
    }

    fn timestamp_millis(value: &str) -> i64 {
        value.parse::<DateTime<Utc>>().unwrap().timestamp_millis()
    }

    /// Regression guard for the two bugs fixed after the first Cursor pass:
    ///
    ///   1. `cursorDiskKV.value` is stored as TEXT (declared BLOB affinity). The
    ///      original `Vec<u8>` read threw `InvalidColumnType(..Text)`. Here every
    ///      value is bound as a JSON STRING (TEXT), so the read path must coerce
    ///      TEXT to bytes or this test fails to reconstruct anything.
    ///   2. The per-thread bubble lookup used `WHERE key LIKE 'bubbleId:CID:%'`,
    ///      which both defeated the UNIQUE(key) index (a full scan per thread)
    ///      and, more importantly for correctness, could match another thread's
    ///      bubbles. The fix is a half-open range scan bounded by the ':' -> ';'
    ///      upper key. The decoy CID2 bubble below MUST NOT leak into CID1.
    ///
    /// Runs entirely in-memory: no file, no immutable-URI open, no network.
    #[test]
    fn cursor_reads_text_valued_rows_and_scopes_bubbles_to_thread() {
        use serde_json::json;

        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB)",
            [],
        )
        .unwrap();

        // Insert a row whose value is a JSON string bound as TEXT (bug #1 crux).
        let put = |key: &str, v: &Value| {
            let text = serde_json::to_string(v).unwrap();
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![key, text], // bound as TEXT, not Vec<u8>
            )
            .unwrap();
        };

        // CID1: headers-only layout with two separate bubble rows.
        put(
            "composerData:CID1",
            &json!({
                "composerId": "CID1",
                "name": "Thread one",
                "createdAt": 1_733_650_720_160i64,
                "lastUpdatedAt": 1_733_650_882_221i64,
                "conversation": [],
                "fullConversationHeadersOnly": [
                    {"bubbleId": "b1", "type": 1},
                    {"bubbleId": "b2", "type": 2}
                ]
            }),
        );
        put(
            "bubbleId:CID1:b1",
            &json!({"bubbleId": "b1", "type": 1, "text": "alpha-question-CID1"}),
        );
        put(
            "bubbleId:CID1:b2",
            &json!({"bubbleId": "b2", "type": 2, "text": "beta-answer-CID1"}),
        );

        // Decoy: a DIFFERENT composer's bubble. Its key sorts adjacent to CID1's
        // range; the fixed upper bound (`< 'bubbleId:CID1;'`) must exclude it.
        put(
            "bubbleId:CID2:bX",
            &json!({"bubbleId": "bX", "type": 1, "text": "DECOY-must-not-leak"}),
        );

        // CID3: inline layout (bubbles carried in `conversation`).
        put(
            "composerData:CID3",
            &json!({
                "composerId": "CID3",
                "createdAt": 1_000i64,
                "conversation": [
                    {"bubbleId": "c1", "type": 1, "text": "gamma-inline-CID3"}
                ]
            }),
        );

        // CID4: empty thread (no conversation, no headers) -> must be skipped.
        put(
            "composerData:CID4",
            &json!({"composerId": "CID4", "createdAt": 5i64, "conversation": []}),
        );

        let threads = read_threads_conn(&conn, &origin(), None).unwrap();

        // Only the two non-empty threads come back (CID4 skipped). Newest first.
        let ids: Vec<&str> = threads
            .iter()
            .map(|t| t.envelope.session_id.as_str())
            .collect();
        assert_eq!(ids, vec!["CID1", "CID3"]);

        // CID1: both text-valued bubble rows reconstructed, in canonical order.
        let cid1 = &threads[0].envelope;
        assert_eq!(cid1.metadata.as_ref().unwrap().message_count, Some(2));
        assert_eq!(timestamp_millis(&cid1.started_at), 1_733_650_720_160);
        assert_eq!(timestamp_millis(&cid1.last_activity_at), 1_733_650_882_221);

        // raw is VERBATIM: composerData + a bubbles array of the original bubble
        // objects (not a flattened text blob).
        let bubbles = cid1
            .raw
            .get("bubbles")
            .and_then(|b| b.as_array())
            .expect("raw.bubbles is an array");
        assert_eq!(bubbles.len(), 2);
        assert_eq!(bubbles[0]["text"], "alpha-question-CID1");
        assert_eq!(bubbles[1]["text"], "beta-answer-CID1");
        assert!(cid1.raw.get("composerData").is_some());
        // The original bubble object survives verbatim (its bubbleId field).
        assert_eq!(bubbles[0]["bubbleId"], "b1");

        // Bug #2 guard: the decoy from CID2 must NOT appear in CID1's raw.
        let cid1_raw_str = serde_json::to_string(&cid1.raw).unwrap();
        assert!(
            !cid1_raw_str.contains("DECOY-must-not-leak"),
            "CID2 decoy bubble leaked into CID1 (range-scan upper bound wrong)"
        );

        // CID3 inline path also reconstructs.
        assert_eq!(
            threads[1].envelope.metadata.as_ref().unwrap().message_count,
            Some(1)
        );
    }

    /// Build an in-memory Cursor-shaped DB for tests. Written to a temp file so
    /// the immutable-URI open path is exercised end to end.
    fn make_db(rows: &[(&str, Value)]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.vscdb");
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB)",
            [],
        )
        .unwrap();
        for (k, v) in rows {
            let bytes = serde_json::to_vec(v).unwrap();
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![k, bytes],
            )
            .unwrap();
        }
        drop(conn);
        (dir, path)
    }

    #[test]
    fn reconstructs_inline_conversation_thread() {
        let cd = json!({
            "composerId": "c-inline",
            "name": "Fix the parser",
            "createdAt": 1733650720160i64,
            "lastUpdatedAt": 1733650882221i64,
            "modelConfig": {"modelName": "claude-4-sonnet"},
            "conversation": [
                {"bubbleId": "b1", "type": 1, "text": "please fix the parser", "richText": "{}"},
                {"bubbleId": "b2", "type": 2, "text": "done, patched the tokenizer", "richText": "{}"}
            ]
        });
        let (_d, path) = make_db(&[("composerData:c-inline", cd)]);

        let threads = read_threads(&path, &origin(), None).unwrap();
        assert_eq!(threads.len(), 1);
        let env = &threads[0].envelope;
        assert_eq!(env.agent, "Cursor");
        assert_eq!(env.source_format, "cursor-state-vscdb");
        assert_eq!(env.session_id, "c-inline");
        assert_eq!(env.origin.host, "test-host");
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.message_count, Some(2));
        assert_eq!(metadata.model.as_deref(), Some("claude-4-sonnet"));
        assert_eq!(metadata.project.as_deref(), Some("Fix the parser"));
        // Real timestamps, not now().
        assert_eq!(timestamp_millis(&env.started_at), 1733650720160);
        assert_eq!(timestamp_millis(&env.last_activity_at), 1733650882221);
        // raw is lossless: composerData + verbatim ordered bubbles.
        assert!(env.raw.get("composerData").is_some());
        let bubbles = env.raw.get("bubbles").and_then(|b| b.as_array()).unwrap();
        assert_eq!(bubbles.len(), 2);
        assert_eq!(bubbles[0]["text"], "please fix the parser");
        assert_eq!(bubbles[1]["type"], 2);
        // The original bubble object is preserved (richText field survives).
        assert!(bubbles[0].get("richText").is_some());
    }

    #[test]
    fn reconstructs_headers_only_thread_with_separate_bubble_rows() {
        let cd = json!({
            "composerId": "c-headers",
            "name": "Styling",
            "createdAt": 1750265882165i64,
            "lastUpdatedAt": 1750266664624i64,
            "conversation": [],
            "fullConversationHeadersOnly": [
                {"bubbleId": "bb1", "type": 1},
                {"bubbleId": "bb2", "type": 2},
                {"bubbleId": "bb-null", "type": 2}
            ]
        });
        let rows = vec![
            ("composerData:c-headers", cd),
            (
                "bubbleId:c-headers:bb1",
                json!({"bubbleId": "bb1", "type": 1, "text": "fix the styling overlay"}),
            ),
            (
                "bubbleId:c-headers:bb2",
                json!({"bubbleId": "bb2", "type": 2, "text": "adjusted the z-index"}),
            ),
            // bb-null intentionally absent (mirrors real null-valued placeholders).
        ];
        let (_d, path) = make_db(&rows);

        let threads = read_threads(&path, &origin(), None).unwrap();
        assert_eq!(threads.len(), 1);
        let env = &threads[0].envelope;
        // Only the two present bubbles are collected, in header order.
        assert_eq!(env.metadata.as_ref().unwrap().message_count, Some(2));
        let bubbles = env.raw.get("bubbles").and_then(|b| b.as_array()).unwrap();
        assert_eq!(bubbles.len(), 2);
        assert_eq!(bubbles[0]["text"], "fix the styling overlay");
        assert_eq!(bubbles[1]["text"], "adjusted the z-index");
    }

    #[test]
    fn empty_threads_are_skipped() {
        let cd = json!({
            "composerId": "c-empty",
            "createdAt": 1000i64,
            "conversation": []
        });
        let (_d, path) = make_db(&[("composerData:c-empty", cd)]);
        let threads = read_threads(&path, &origin(), None).unwrap();
        assert!(threads.is_empty());
    }

    #[test]
    fn last_activity_falls_back_to_created_when_updated_absent() {
        let cd = json!({
            "composerId": "c-noupdate",
            "createdAt": 2000i64,
            "conversation": [{"bubbleId": "b", "type": 1, "text": "hi"}]
        });
        let (_d, path) = make_db(&[("composerData:c-noupdate", cd)]);
        let env = &read_threads(&path, &origin(), None).unwrap()[0].envelope;
        assert_eq!(env.started_at, env.last_activity_at);
        assert_eq!(timestamp_millis(&env.started_at), 2000);
    }

    #[test]
    fn limit_caps_to_newest_threads() {
        let rows = vec![
            (
                "composerData:old",
                json!({"composerId":"old","createdAt":100i64,"conversation":[{"bubbleId":"b","type":1,"text":"old"}]}),
            ),
            (
                "composerData:new",
                json!({"composerId":"new","createdAt":999i64,"conversation":[{"bubbleId":"b","type":1,"text":"new"}]}),
            ),
        ];
        let (_d, path) = make_db(&rows);
        let threads = read_threads(&path, &origin(), Some(1)).unwrap();
        assert_eq!(threads.len(), 1);
        // Newest (createdAt 999) is kept.
        assert_eq!(threads[0].envelope.session_id, "new");
    }

    #[test]
    fn edge_rows_are_coerced_skipped_or_reconstructed() {
        use serde_json::json;

        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB)",
            [],
        )
        .unwrap();
        let put_text = |key: &str, v: &Value| {
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![key, serde_json::to_string(v).unwrap()],
            )
            .unwrap();
        };

        // A composerData row whose value is an INTEGER (not TEXT/BLOB) -> the
        // `_ => None` coercion arm skips it.
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["composerData:int-val", 5i64],
        )
        .unwrap();
        // A composerData row whose value is not valid JSON -> from_slice continue.
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["composerData:bad-json", "{not json"],
        )
        .unwrap();
        // Empty composerId -> reconstruct returns None.
        put_text(
            "composerData:empty-id",
            &json!({"composerId":"","createdAt":10i64,"conversation":[{"bubbleId":"b","type":1,"text":"x"}]}),
        );
        // No createdAt at all -> thread_times falls back to now() (both equal).
        put_text(
            "composerData:nc",
            &json!({"composerId":"nc","conversation":[{"bubbleId":"b","type":1,"text":"x"}]}),
        );
        // Headers-only thread exercising: header missing bubbleId, an empty-text
        // bubble, an integer-valued bubble row, and a bad-json bubble row.
        put_text(
            "composerData:hdr",
            &json!({
                "composerId":"hdr","createdAt":50i64,"conversation":[],
                "fullConversationHeadersOnly":[
                    {"type":1},
                    {"bubbleId":"bok","type":1},
                    {"bubbleId":"bempty","type":1},
                    {"bubbleId":"bint","type":1},
                    {"bubbleId":"bbad","type":1}
                ]
            }),
        );
        put_text(
            "bubbleId:hdr:bok",
            &json!({"bubbleId":"bok","type":1,"text":"hello"}),
        );
        put_text(
            "bubbleId:hdr:bempty",
            &json!({"bubbleId":"bempty","type":1,"text":""}),
        );
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["bubbleId:hdr:bint", 9i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["bubbleId:hdr:bbad", "{bad json"],
        )
        .unwrap();

        let threads = read_threads_conn(&conn, &origin(), None).unwrap();
        let ids: std::collections::HashSet<&str> = threads
            .iter()
            .map(|t| t.envelope.session_id.as_str())
            .collect();
        // int-val, bad-json, empty-id are all skipped; hdr + nc reconstruct.
        assert!(ids.contains("hdr"));
        assert!(ids.contains("nc"));
        assert!(!ids.contains("empty-id"));
        assert_eq!(ids.len(), 2);

        // hdr: only the two present bubble rows (bok, bempty) survive; the
        // missing-bubbleId header, integer row, and bad-json row are excluded.
        let hdr = threads
            .iter()
            .find(|t| t.envelope.session_id == "hdr")
            .unwrap();
        assert_eq!(
            hdr.envelope.metadata.as_ref().unwrap().message_count,
            Some(2)
        );

        // nc: no createdAt -> started_at == last_activity_at (the now() fallback).
        let nc = threads
            .iter()
            .find(|t| t.envelope.session_id == "nc")
            .unwrap();
        assert_eq!(nc.envelope.started_at, nc.envelope.last_activity_at);
    }

    #[test]
    fn limit_above_thread_count_returns_all() {
        // A limit larger than the number of threads never triggers the break, so
        // the `out.len() >= n` false branch is exercised.
        let rows = vec![
            (
                "composerData:a",
                json!({"composerId":"a","createdAt":1i64,"conversation":[{"bubbleId":"b","type":1,"text":"x"}]}),
            ),
            (
                "composerData:b",
                json!({"composerId":"b","createdAt":2i64,"conversation":[{"bubbleId":"b","type":1,"text":"y"}]}),
            ),
        ];
        let (_d, path) = make_db(&rows);
        assert_eq!(read_threads(&path, &origin(), Some(10)).unwrap().len(), 2);
    }

    #[test]
    fn headers_thread_without_conversation_key_and_bubble_without_text() {
        // composerData carries NO `conversation` key at all (the `if let Some(conv)`
        // None branch), and one bubble row omits `text` entirely (push_bubble's
        // `if let Some(t)` None branch).
        let rows = vec![
            (
                "composerData:h",
                json!({
                    "composerId":"h","createdAt":9i64,
                    "fullConversationHeadersOnly":[
                        {"bubbleId":"b1","type":1},
                        {"bubbleId":"b2","type":2}
                    ]
                }),
            ),
            (
                "bubbleId:h:b1",
                json!({"bubbleId":"b1","type":1,"text":"has text"}),
            ),
            // No `text` field on this bubble.
            ("bubbleId:h:b2", json!({"bubbleId":"b2","type":2})),
        ];
        let (_d, path) = make_db(&rows);
        let threads = read_threads(&path, &origin(), None).unwrap();
        assert_eq!(threads.len(), 1);
        let bubbles = threads[0]
            .envelope
            .raw
            .get("bubbles")
            .and_then(|b| b.as_array())
            .unwrap();
        assert_eq!(bubbles.len(), 2);
    }

    // Test-only extractor mapping each `CursorError` variant to a stable kind
    // string. Exercised below across BOTH variants (Open and Query), so every
    // match arm is live with no exclusion, while the assertions stay exact: each
    // kind string uniquely names one variant.
    fn cursor_err_kind(e: &CursorError) -> &'static str {
        match e {
            CursorError::Open { .. } => "open",
            CursorError::Query(_) => "query",
        }
    }

    #[test]
    fn missing_table_surfaces_query_error() {
        // A connection with no `cursorDiskKV` table makes `conn.prepare` fail, so
        // `read_threads_conn` returns the SQLite error through its `?` (covering
        // the prepare error arm without a corrupt DB).
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            cursor_err_kind(&read_threads_conn(&conn, &origin(), None).err().unwrap()),
            "query"
        );
        // A nonexistent DB path fails to open read-only, exercising the Open arm.
        let missing = std::path::Path::new("/nonexistent/cursor-state.vscdb");
        assert_eq!(
            cursor_err_kind(&read_threads(missing, &origin(), None).err().unwrap()),
            "open"
        );
    }

    #[test]
    fn load_bubble_rows_missing_table_surfaces_query_error() {
        // Called directly against a connection with no `cursorDiskKV` table, so
        // load_bubble_rows' own `conn.prepare(..)?` takes its error edge (this
        // arm is unreachable via read_threads_conn, whose main prepare would fail
        // first, so it is exercised at the helper level).
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            cursor_err_kind(&load_bubble_rows(&conn, "CID", u64::MAX).err().unwrap()),
            "query"
        );
    }

    #[test]
    fn main_query_non_utf8_key_surfaces_query_error() {
        // A composerData key stored as TEXT but holding invalid UTF-8 bytes still
        // matches the `LIKE 'composerData:%'` scan, but `row.get::<String>(0)`
        // then fails to decode it: the closure's `?` and the row-iteration `?`
        // both take their error edge.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB)",
            [],
        )
        .unwrap();
        let mut key = b"composerData:bad".to_vec();
        key.push(0xff); // invalid UTF-8 trailing byte
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (CAST(?1 AS TEXT), ?2)",
            params![key, "{}"],
        )
        .unwrap();
        assert_eq!(
            cursor_err_kind(&read_threads_conn(&conn, &origin(), None).err().unwrap()),
            "query"
        );
    }

    #[test]
    fn bubble_query_non_utf8_key_propagates_query_error() {
        use serde_json::json;
        // A valid headers-only thread whose separate bubble row has a TEXT key
        // holding invalid UTF-8. The bubble range scan selects it, the key decode
        // fails, and the error propagates up through load_bubble_rows ->
        // collect_bubbles -> reconstruct -> read_threads_conn (their `?` edges).
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB)",
            [],
        )
        .unwrap();
        let cd = json!({
            "composerId": "CID",
            "createdAt": 1i64,
            "conversation": [],
            "fullConversationHeadersOnly": [{"bubbleId": "b1", "type": 1}]
        });
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["composerData:CID", serde_json::to_string(&cd).unwrap()],
        )
        .unwrap();
        let mut bad_key = b"bubbleId:CID:b".to_vec();
        bad_key.push(0xff); // invalid UTF-8, still within the range scan bounds
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (CAST(?1 AS TEXT), ?2)",
            params![bad_key, "{}"],
        )
        .unwrap();
        assert_eq!(
            cursor_err_kind(&read_threads_conn(&conn, &origin(), None).err().unwrap()),
            "query"
        );
    }

    #[test]
    fn model_and_project_omitted_when_absent() {
        let cd = json!({
            "composerId": "c-bare",
            "createdAt": 1i64,
            "conversation": [{"bubbleId": "b", "type": 1, "text": "hello"}]
        });
        let (_d, path) = make_db(&[("composerData:c-bare", cd)]);
        let env = &read_threads(&path, &origin(), None).unwrap()[0].envelope;
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.model, None);
        assert_eq!(metadata.project, None);
        assert_eq!(metadata.repo, None);
    }

    #[test]
    fn streaming_bounds_report_overflow_and_oversize_and_keep_the_newest() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
        )
        .unwrap();
        let insert = |key: String, value: String| {
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![key, value],
            )
            .unwrap();
        };
        for index in 0..2_000i64 {
            insert(
                format!("composerData:c{index}"),
                json!({"composerId": format!("c{index}"), "createdAt": index,
                    "conversation": [{"bubbleId": "b", "type": 1, "text": "hi"}]})
                .to_string(),
            );
        }
        // A composer row over the byte bound on its own, a thread whose
        // bubble rows push it over, and rows that are not threads at all.
        insert(
            "composerData:huge".into(),
            format!("{{\"pad\":\"{}\"}}", "x".repeat(600)),
        );
        insert(
            "composerData:split".into(),
            json!({"composerId": "split", "createdAt": 9_999,
                "fullConversationHeadersOnly": [{"bubbleId": "a"}]})
            .to_string(),
        );
        insert(
            "bubbleId:split:a".into(),
            json!({"type": 1, "text": "y".repeat(400)}).to_string(),
        );
        insert("composerData:broken".into(), "{".into());
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES ('composerData:null', NULL)",
            [],
        )
        .unwrap();
        let bounds = ThreadBounds {
            max_threads: 10,
            max_bytes: 500,
        };
        let mut seen = Vec::new();
        let result = visit_threads_conn(&conn, &origin(), None, bounds, &mut |item| {
            seen.push(match item {
                ThreadItem::Thread(t) => t.envelope.session_id.clone(),
                ThreadItem::Oversize(key) => format!("oversize {key}"),
                ThreadItem::Overflow(count) => format!("overflow {count}"),
            });
            Ok::<(), ()>(())
        })
        .unwrap();
        assert!(result.is_ok());
        // 2,002 candidates (the broken and null rows are not threads), 10
        // kept: split (newest), then c1999 down to c1991.
        // The oversize composer row sorts oldest and is dropped by the bound.
        let mut expected = vec![
            "overflow 1992".to_string(),
            "oversize composerData:split".into(),
        ];
        expected.extend((1991..2000).rev().map(|i| format!("c{i}")));
        assert_eq!(seen, expected);

        // The limit counts what is emitted; the oversize row surfaces when
        // nothing newer crowds it out; a visitor error stops the sweep.
        let roomy = ThreadBounds {
            max_threads: 5_000,
            max_bytes: 500,
        };
        let mut oversize = 0;
        visit_threads_conn(&conn, &origin(), None, roomy, &mut |item| {
            oversize += usize::from(matches!(item, ThreadItem::Oversize(_)));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
        assert_eq!(oversize, 2);
        let mut count = 0;
        visit_threads_conn(&conn, &origin(), Some(3), roomy, &mut |_| {
            count += 1;
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
        assert_eq!(count, 3);
        let stopped = visit_threads_conn(&conn, &origin(), None, roomy, &mut |_| Err("stop"));
        assert!(matches!(stopped, Ok(Err("stop"))));
        let stopped = visit_threads_conn(&conn, &origin(), None, bounds, &mut |_| Err("stop"));
        assert!(matches!(stopped, Ok(Err("stop"))));
    }

    // --- cheap scan --------------------------------------------------------

    /// Raw-value insert helpers for scan fixtures: TEXT, BLOB, NULL, INTEGER.
    fn put_text(conn: &Connection, key: &str, raw: &str) {
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params![key, raw],
        )
        .unwrap();
    }

    fn put_blob(conn: &Connection, key: &str, raw: &str) {
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params![key, raw.as_bytes()],
        )
        .unwrap();
    }

    fn put_sql(conn: &Connection, key: &str, value_sql: &str) {
        conn.execute(
            &format!("INSERT INTO cursorDiskKV (key, value) VALUES (?1, {value_sql})"),
            params![key],
        )
        .unwrap();
    }

    /// Every shape the scan must agree with `reconstruct` on: inline, headers-only
    /// (missing / NULL / invalid / INTEGER / scalar / BLOB bubble rows, duplicate
    /// and malformed headers), non-integer and `-0` timestamps, empty threads,
    /// bad composerIds, and NULL / INTEGER / non-JSON / non-object values.
    fn scan_fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB)",
            [],
        )
        .unwrap();
        let c = &conn;
        put_text(
            c,
            "composerData:inline",
            r#"{"composerId":"inline","createdAt":100,"lastUpdatedAt":200,"conversation":[{"bubbleId":"a","type":1,"text":"x"},{"bubbleId":"b"}]}"#,
        );
        // BLOB-stored, ties `inline` on createdAt (rowid order breaks the tie).
        put_blob(
            c,
            "composerData:blob",
            r#"{"composerId":"blob","createdAt":100,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:hdr",
            r#"{"composerId":"hdr","createdAt":300,"lastUpdatedAt":250,"conversation":[],
            "fullConversationHeadersOnly":[{"type":1},{"bubbleId":7},"str",{"bubbleId":"ok"},{"bubbleId":"ok"},
            {"bubbleId":"ok"},{"bubbleId":"missing"},{"bubbleId":"null"},{"bubbleId":"bad"},
            {"bubbleId":"int"},{"bubbleId":"scalar"},{"bubbleId":"blobok"}]}"#,
        );
        put_text(c, "bubbleId:hdr:ok", r#"{"text":"x"}"#);
        put_sql(c, "bubbleId:hdr:null", "NULL");
        put_text(c, "bubbleId:hdr:bad", "{bad");
        put_sql(c, "bubbleId:hdr:int", "9");
        put_text(c, "bubbleId:hdr:scalar", "5");
        put_blob(c, "bubbleId:hdr:blobok", r#"{"t":1}"#);
        // Another composer whose key sorts right after hdr's range.
        put_text(c, "bubbleId:hdrX:missing", r#"{"text":"decoy"}"#);
        put_text(
            c,
            "composerData:hdr-noconv",
            r#"{"composerId":"hdr-noconv","createdAt":5,"conversation":{"x":1},"fullConversationHeadersOnly":[{"bubbleId":"b"}]}"#,
        );
        put_text(c, "bubbleId:hdr-noconv:b", r#"{"text":"y"}"#);
        put_text(
            c,
            "composerData:both",
            r#"{"composerId":"both","createdAt":6,"conversation":[{}],"fullConversationHeadersOnly":[{"bubbleId":"z"}]}"#,
        );
        put_text(
            c,
            "composerData:hdr-empty",
            r#"{"composerId":"hdr-empty","createdAt":7,"conversation":[],"fullConversationHeadersOnly":[{"bubbleId":"nope"}]}"#,
        );
        put_text(
            c,
            "composerData:hdr-obj",
            r#"{"composerId":"hdr-obj","createdAt":8,"fullConversationHeadersOnly":{"bubbleId":"b"}}"#,
        );
        put_text(
            c,
            "composerData:empty",
            r#"{"composerId":"empty","createdAt":9,"conversation":[]}"#,
        );
        put_text(
            c,
            "composerData:ts-real",
            r#"{"composerId":"ts-real","createdAt":1.5,"lastUpdatedAt":1e3,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:ts-str",
            r#"{"composerId":"ts-str","createdAt":"123","lastUpdatedAt":true,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:ts-negzero",
            r#"{"composerId":"ts-negzero","createdAt":42,"lastUpdatedAt":-0,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:ts-big",
            r#"{"composerId":"ts-big","createdAt":9223372036854775808,"lastUpdatedAt":-9223372036854775808,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:ts-null",
            r#"{"composerId":"ts-null","createdAt":null,"lastUpdatedAt":11,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:cafe",
            r#"{"composerId":"café","createdAt":12,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:noid",
            r#"{"createdAt":1,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:emptyid",
            r#"{"composerId":"","conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:numid",
            r#"{"composerId":5,"conversation":[1]}"#,
        );
        // The row key and the composerId disagree: the session id (and state
        // key) comes from composerId, the load from the row key.
        put_text(
            c,
            "composerData:old-key",
            r#"{"composerId":"renamed","createdAt":13,"conversation":[1]}"#,
        );
        put_sql(c, "composerData:null", "NULL");
        put_sql(c, "composerData:int", "5");
        put_text(c, "composerData:badjson", "{nope");
        put_text(c, "composerData:arr", "[1,2]");
        put_text(c, "composerData:str", r#""s""#);
        conn
    }

    fn scan_pairs(conn: &Connection, limit: Option<usize>) -> Vec<(String, String)> {
        scan_conn(conn, limit)
            .unwrap()
            .into_iter()
            .map(|t| (t.composer_id, t.fingerprint))
            .collect()
    }

    fn full_pairs(conn: &Connection, limit: Option<usize>) -> Vec<(String, String)> {
        read_threads_conn(conn, &origin(), limit)
            .unwrap()
            .into_iter()
            .map(|t| (t.envelope.session_id, t.fingerprint))
            .collect()
    }

    #[test]
    fn scan_fingerprints_equal_reconstruct_fingerprints() {
        let conn = scan_fixture();
        let full = full_pairs(&conn, None);
        // Same threads, same fingerprints, same (newest-first, rowid-tiebroken)
        // order as the full reader, so state files stay valid across upgrade.
        assert_eq!(scan_pairs(&conn, None), full);
        // And `limit` keeps exactly the same newest-N threads at every N.
        for n in 1..=full.len() + 1 {
            assert_eq!(
                scan_pairs(&conn, Some(n)),
                full_pairs(&conn, Some(n)),
                "limit {n}"
            );
        }
        // Pin the interesting cases so a change to both readers is still caught.
        let get = |id: &str| full.iter().find(|(c, _)| c == id).map(|(_, f)| f.as_str());
        assert_eq!(get("hdr"), Some("250:5")); // ok x3 (dup + escaped) + scalar + blobok
        assert_eq!(get("inline"), Some("200:2"));
        assert_eq!(get("blob"), Some("100:1"));
        assert_eq!(get("hdr-noconv"), Some("5:1"));
        assert_eq!(get("both"), Some("6:1"));
        assert_eq!(get("ts-real"), Some("0:1"));
        assert_eq!(get("ts-str"), Some("0:1"));
        assert_eq!(get("ts-negzero"), Some("42:1"));
        assert_eq!(get("ts-big"), Some("-9223372036854775808:1"));
        assert_eq!(get("ts-null"), Some("11:1"));
        assert_eq!(get("café"), Some("12:1"));
        for dropped in ["hdr-empty", "hdr-obj", "empty", "noid", "emptyid", "numid"] {
            assert_eq!(get(dropped), None, "{dropped} must be dropped");
        }
        assert_eq!(get("renamed"), Some("13:1"));
        assert_eq!(full.len(), 12);
    }

    /// Rows the full reader's case-insensitive `LIKE` matches under another
    /// case, and rows only SQLite accepts, newest of all: a limited scan still
    /// selects exactly the full reader's threads; an unlimited one may list the
    /// SQLite-only rows too, which then load as nothing.
    #[test]
    fn limited_scans_select_the_full_readers_threads_despite_divergent_rows() {
        let conn = scan_fixture();
        let c = &conn;
        put_text(
            c,
            "ComposerData:upper",
            r#"{"composerId":"upper","createdAt":14,"conversation":[1]}"#,
        );
        put_text(
            c,
            "COMPOSERDATA:shout",
            r#"{"composerId":"shout","createdAt":15,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:huge-num",
            r#"{"composerId":"huge-num","createdAt":1000,"x":1e999,"conversation":[1]}"#,
        );
        put_text(
            c,
            "composerData:dup-created",
            r#"{"composerId":"dup-created","createdAt":999,"createdAt":998,"conversation":[1]}"#,
        );
        // Its only bubble is valid to SQLite (a lone surrogate escape) but not
        // to serde, so the full reader finds it empty.
        put_text(
            c,
            "composerData:lone",
            r#"{"composerId":"lone","createdAt":1001,"conversation":[],"fullConversationHeadersOnly":[{"bubbleId":"s"}]}"#,
        );
        put_text(c, "bubbleId:lone:s", r#"{"text":"\ud800"}"#);
        // Outside the prefix in any case: never a thread.
        put_text(
            c,
            "composerDataX:no",
            r#"{"composerId":"no","createdAt":2000,"conversation":[1]}"#,
        );
        let full = full_pairs(&conn, None);
        assert!(full.iter().any(|(id, _)| id == "upper"));
        assert!(full.iter().any(|(id, _)| id == "shout"));
        assert!(!full.iter().any(|(id, _)| id == "no"));
        for n in 1..=full.len() + 1 {
            assert_eq!(
                scan_pairs(&conn, Some(n)),
                full_pairs(&conn, Some(n)),
                "limit {n}"
            );
        }
        let unlimited: Vec<String> = scan_pairs(&conn, None)
            .into_iter()
            .filter(|pair| !full.contains(pair))
            .map(|(id, _)| id)
            .collect();
        assert_eq!(unlimited, vec!["lone", "huge-num", "dup-created"]);
        assert!(load_conn(&conn, "composerData:huge-num", &origin())
            .unwrap()
            .is_none());
        // A probe of a row that vanished, or is not text, accepts nothing.
        assert!(!created_probe_accepts(&conn, "composerData:gone").unwrap());
        assert!(!created_probe_accepts(&conn, "composerData:null").unwrap());
    }

    /// The scan's SQL leans on specific behaviour of the bundled SQLite's JSON
    /// functions; pin each assumption so an upgrade that changes one fails here
    /// rather than silently re-sending (or skipping) threads.
    #[test]
    fn bundled_sqlite_json_functions_behave_as_the_scan_assumes() {
        let conn = Connection::open_in_memory().unwrap();
        let q = |sql: &str| -> rusqlite::types::Value {
            conn.query_row(&format!("SELECT {sql}"), [], |r| r.get(0))
                .unwrap()
        };
        use rusqlite::types::Value as V;
        // json_array_length: 0 on a non-array, NULL on a missing path.
        assert_eq!(q(r#"json_array_length('{"a":{}}', '$.a')"#), V::Integer(0));
        assert_eq!(q(r#"json_array_length('{"a":"s"}', '$.a')"#), V::Integer(0));
        assert_eq!(q(r#"json_array_length('{}', '$.a')"#), V::Null);
        // json_type distinguishes integers from reals / booleans / strings.
        assert_eq!(
            q(r#"json_type('{"a":1.0}', '$.a')"#),
            V::Text("real".into())
        );
        assert_eq!(
            q(r#"json_type('{"a":1e3}', '$.a')"#),
            V::Text("real".into())
        );
        assert_eq!(
            q(r#"json_type('{"a":true}', '$.a')"#),
            V::Text("true".into())
        );
        assert_eq!(
            q(r#"json_type('{"a":"1"}', '$.a')"#),
            V::Text("text".into())
        );
        // An "integer" beyond i64 extracts as REAL (rejected by int_col, like
        // serde's as_i64); i64::MIN still extracts as INTEGER (accepted by both).
        assert_eq!(
            q(r#"typeof(json_extract('{"a":9223372036854775808}', '$.a'))"#),
            V::Text("real".into())
        );
        assert_eq!(
            q(r#"json_extract('{"a":-9223372036854775808}', '$.a')"#),
            V::Integer(i64::MIN)
        );
        // `-0` is an "integer" to SQLite, but `->` keeps its token text.
        assert_eq!(
            q(r#"json_type('{"a":-0}', '$.a')"#),
            V::Text("integer".into())
        );
        assert_eq!(q(r#"'{"a": -0 }' -> '$.a'"#), V::Text("-0".into()));
        // json_valid is strict RFC 8259 (no JSON5), accepts any top-level value.
        assert_eq!(q("json_valid('{bad')"), V::Integer(0));
        assert_eq!(q("json_valid('{a:1}')"), V::Integer(0));
        assert_eq!(q("json_valid('[1,]')"), V::Integer(0));
        assert_eq!(q("json_valid('')"), V::Integer(0));
        assert_eq!(q("json_valid('5')"), V::Integer(1));
        assert_eq!(q("json_valid(' {} ')"), V::Integer(1));
        // A BLOB must be CAST to TEXT to be read as JSON text.
        assert_eq!(q("json_valid(CAST(x'7b7d' AS TEXT))"), V::Integer(1));
        // Arrays extract as JSON text (parsed in Rust for the header list).
        assert_eq!(
            q(r#"json_extract('{"a":[{"b":1}]}', '$.a')"#),
            V::Text(r#"[{"b":1}]"#.into())
        );
        // Serde agrees on the scalar cases the scan mirrors.
        assert_eq!(serde_json::from_str::<Value>("-0").unwrap().as_i64(), None);
        assert_eq!(
            serde_json::from_str::<Value>("5").unwrap().as_i64(),
            Some(5)
        );
    }

    #[test]
    fn cursor_db_opens_scans_and_loads_one_thread() {
        let cd = json!({
            "composerId": "c1",
            "createdAt": 10i64,
            "conversation": [{"bubbleId": "b", "type": 1, "text": "hello"}]
        });
        let (_d, path) = make_db(&[("composerData:c1", cd)]);
        let db = CursorDb::open(&path).unwrap();
        let scanned = db.scan(None).unwrap();
        assert_eq!(
            scanned,
            vec![ThreadFingerprint {
                key: "composerData:c1".into(),
                composer_id: "c1".into(),
                fingerprint: "10:1".into()
            }]
        );
        let thread = db.load("composerData:c1", &origin()).unwrap().unwrap();
        assert_eq!(thread.fingerprint, "10:1");
        assert_eq!(thread.envelope.session_id, "c1");
        // A thread that is not in the DB loads as None.
        assert!(db.load("composerData:gone", &origin()).unwrap().is_none());
        // Opening a missing DB fails cleanly.
        assert_eq!(
            cursor_err_kind(
                &CursorDb::open(std::path::Path::new("/nonexistent/x.vscdb"))
                    .err()
                    .unwrap()
            ),
            "open"
        );
    }

    #[test]
    fn load_skips_null_and_invalid_rows_and_reconstructs_headers() {
        let conn = scan_fixture();
        assert!(load_conn(&conn, "composerData:null", &origin())
            .unwrap()
            .is_none());
        assert!(load_conn(&conn, "composerData:badjson", &origin())
            .unwrap()
            .is_none());
        assert!(load_conn(&conn, "composerData:empty", &origin())
            .unwrap()
            .is_none());
        let hdr = load_conn(&conn, "composerData:hdr", &origin())
            .unwrap()
            .unwrap();
        assert_eq!(hdr.fingerprint, "250:5");
        // Loaded by row key, identified by composerId.
        let renamed = load_conn(&conn, "composerData:old-key", &origin())
            .unwrap()
            .unwrap();
        assert_eq!(renamed.envelope.session_id, "renamed");
        let scanned = scan_conn(&conn, None).unwrap();
        let entry = scanned.iter().find(|t| t.composer_id == "renamed").unwrap();
        assert_eq!(entry.key, "composerData:old-key");
    }

    #[test]
    fn scan_errors_surface_as_query_errors() {
        // No table: the scan's prepare fails.
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            cursor_err_kind(&scan_conn(&conn, None).err().unwrap()),
            "query"
        );
        assert_eq!(
            cursor_err_kind(&load_conn(&conn, "composerData:x", &origin()).err().unwrap()),
            "query"
        );
        // A headers-only thread whose bubble row key is invalid UTF-8: the
        // bubble key scan fails to decode it and the error propagates.
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB)",
            [],
        )
        .unwrap();
        put_text(
            &conn,
            "composerData:CID",
            r#"{"composerId":"CID","createdAt":1,"conversation":[],"fullConversationHeadersOnly":[{"bubbleId":"b"}]}"#,
        );
        let mut bad_key = b"bubbleId:CID:b".to_vec();
        bad_key.push(0xff);
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (CAST(?1 AS TEXT), '{}')",
            params![bad_key],
        )
        .unwrap();
        assert_eq!(
            cursor_err_kind(&scan_conn(&conn, None).err().unwrap()),
            "query"
        );
    }

    #[test]
    fn scan_non_utf8_composer_key_surfaces_query_error() {
        // Same as the full reader: the key decode fails, through the closure's
        // `?` and the row-iteration `?`.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB)",
            [],
        )
        .unwrap();
        let mut key = b"composerData:bad".to_vec();
        key.push(0xff);
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (CAST(?1 AS TEXT), '{}')",
            params![key],
        )
        .unwrap();
        assert_eq!(
            cursor_err_kind(&scan_conn(&conn, None).err().unwrap()),
            "query"
        );
    }

    #[test]
    fn valid_bubble_ids_missing_table_surfaces_query_error() {
        // Unreachable via scan_conn (whose own prepare fails first), so the
        // helper's prepare error edge is exercised directly.
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            cursor_err_kind(&valid_bubble_ids(&conn, "CID").err().unwrap()),
            "query"
        );
    }
}
