// `coverage(off)` is a nightly-only attribute; cargo-llvm-cov sets the
// `coverage_nightly` cfg so the coverage gate can exclude genuinely-untestable
// arms. On stable this is a no-op, so `just qa` / release builds are unaffected.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
//! Capture client: discovers local Claude / Codex / Cursor transcripts and
//! uploads SCS envelopes (raw verbatim) to the store's batch ingest endpoint.
//!
//! ONE exporter covers ALL sources (a lesson from the prior daemon: never run
//! one-agent-per-daemon). A single `run` sweeps every configured source, batches
//! the new-or-changed transcripts, and posts them. Idempotency has two layers:
//! a local fingerprint state file skips unchanged files, and the store dedups on
//! `(session_id, content_hash)` so even a full re-upload is cheap and safe. Both
//! layers hold, so re-runs are always cheap.

// See the matching waiver in session-store-core: `async_trait` creates the
// duplicate `must_use` annotation reported by current nightly Clippy.
#![allow(clippy::double_must_use)]

pub mod capture_outbox;
pub mod config;
pub mod cursor;
pub mod gitmeta;
pub mod health;
pub mod inventory;
pub mod inventory_outbox;
pub mod parsers;
pub mod qualified_capture;
pub mod reconstitute;
pub mod secure_fs;
pub mod sources;
pub mod spool;
pub mod state;
pub mod upload;

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use session_capture::{Origin, SessionEnvelope};

use crate::config::Config;
use crate::cursor::CursorDb;
use crate::sources::{Candidate, Discovered, Loaded, SourceKind};
use crate::state::State;
use crate::upload::{tally, BatchSender, Client, Outcome};

/// Aggregate outcome of a full run.
#[derive(Debug, Default, Clone)]
pub struct RunSummary {
    pub discovered: usize,
    pub skipped_unchanged: usize,
    pub uploaded: usize,
    pub accepted: usize,
    pub duplicate: usize,
    pub rejected: usize,
    /// Envelopes skipped BEFORE upload because their serialized size exceeded
    /// `max_envelope_bytes`. Not fatal; they are simply not captured this sweep.
    pub skipped_oversize: usize,
    /// Envelopes that hit a hard error even on a solo retry (413/5xx/timeout/
    /// connection). Logged and skipped so the sweep continues; unmarked in state
    /// so they retry next sweep.
    pub failed: usize,
    /// Envelopes SENT for which the store returned no matching outcome: an
    /// empty results array, a short response, or a result naming a session
    /// that was not in the request.
    ///
    /// Tracked separately because it is neither an accept nor a rejection, and
    /// counting it as neither is how a sweep that stored nothing reported
    /// success: the items were left unmarked, so they retried, while every
    /// failure counter stayed zero and the run looked complete.
    pub unconfirmed: usize,
    /// Session ids the STORE confirmed it holds during this sweep, in the order
    /// they were confirmed.
    ///
    /// Populated at exactly the points where an envelope is marked in state, so
    /// membership here means the same thing marking does: the store answered
    /// `Accepted` or `Duplicate` for that specific envelope.
    ///
    /// Deliberately NOT including `skipped_unchanged`. Those were never sent
    /// this sweep, so the store said nothing about them - only local state
    /// claims they are held. A caller asking "is MY session in the store" needs
    /// an answer backed by the store, not by a file the audited agent can write.
    ///
    /// A MULTISET, like the confirmation logic it mirrors: two envelopes can
    /// share a session id while differing in content, and each confirmation
    /// covers exactly one of them.
    ///
    /// LIMIT, and it is a sharp one: this is an ACKNOWLEDGEMENT LIST, not a
    /// proof of authenticity. The session id is derived from the transcript
    /// itself - for Claude it is literally the filename stem (`sources.rs`),
    /// for Codex the payload id with a filename fallback - and the spool is
    /// agent-writable. An agent that knows the id being looked for can delete
    /// the real transcript and write a well-formed decoy carrying that same id;
    /// the exporter will upload it and honestly report the id here.
    ///
    /// So a consumer may use membership to detect a session that never reached
    /// the store. It may NOT use membership to conclude that the bytes the
    /// store holds are the ones the agent actually produced. Closing that
    /// requires an identity the agent cannot substitute - transcript bytes
    /// hashed from a location it cannot write - which is not something this
    /// exporter can establish on its own.
    pub confirmed_sessions: Vec<String>,
}

/// Run errors that abort the whole sweep.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("source scan failed: {0}")]
    Source(#[from] sources::SourceError),
    #[error("upload failed: {0}")]
    Upload(#[from] upload::UploadError),
    #[error("store is not reachable at {0} (is it up? is the URL right?)")]
    Unreachable(String),
    #[error("could not record the successful exporter sweep: {0}")]
    Health(#[source] std::io::Error),
}

/// Test-only: install a process-wide WARN subscriber exactly once.
///
/// Why a GLOBAL (not a scoped `set_default`): tracing's `warn!` macro guards its
/// field-value expressions behind the runtime global max level
/// (`LevelFilter::current()`). A scoped `set_default` provides a subscriber but
/// does NOT raise that global max, so `warn!` short-circuits and the field
/// expressions in the error branches never evaluate (leaving those regions
/// uncovered). `set_global_default` raises the max level, so every warn-arm test
/// that calls this before emitting gets its diagnostic fields evaluated
/// deterministically, independent of test order. Installed once via `OnceLock`.
#[cfg(test)]
pub(crate) fn install_warn_logging() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Discover every source's transcripts under the configured roots. Public so the
/// binary can print a dry-run count.
pub fn discover_all(cfg: &Config) -> Result<Vec<Discovered>, sources::SourceError> {
    let origin = origin_of(cfg);
    let mut all = Vec::new();
    all.extend(sources::discover_claude(&cfg.claude_root, &origin)?);
    all.extend(sources::discover_codex(&cfg.codex_root, &origin)?);
    if let Some(db) = &cfg.cursor_db {
        // `cursor_limit` caps a run to the newest N threads (from CURSOR_LIMIT
        // or the --cursor-limit CLI flag); None captures all.
        all.extend(sources::discover_cursor(db, &origin, cfg.cursor_limit)?);
    }
    for d in &mut all {
        stamp_tags(&mut d.envelope, &cfg.tags);
    }
    Ok(all)
}

fn origin_of(cfg: &Config) -> Origin {
    let mut origin = Origin::new(cfg.origin_host.clone(), cfg.origin_environment.clone());
    if let Some(deployment) = &cfg.origin_deployment {
        origin = origin.with_deployment(deployment.clone());
    }
    origin
}

/// Most Cursor threads a streaming sweep holds candidates for. Real databases
/// hold low thousands; beyond this, older threads are reported as overflow.
pub const CURSOR_MAX_THREADS: usize = 100_000;

/// Streaming discovery for callers that must not hold the corpus in memory.
/// Claude and Codex files larger than `limit` bytes are reported as
/// [`sources::Found::Oversize`] without being read; each other transcript is
/// handed to `visit` and dropped before the next is read. Directories are
/// walked without buffering their listings. Cursor threads are streamed newest
/// first under [`CURSOR_MAX_THREADS`] and the same byte bound, with anything
/// left out reported as oversize or overflow.
pub fn visit_all<E>(
    cfg: &Config,
    limit: u64,
    visit: &mut dyn FnMut(sources::Found) -> Result<(), E>,
) -> Result<(), sources::VisitError<E>> {
    use sources::Found;
    let origin = origin_of(cfg);
    let mut stamped = |found: Found| {
        visit(match found {
            Found::Transcript(mut discovered) => {
                stamp_tags(&mut discovered.envelope, &cfg.tags);
                Found::Transcript(discovered)
            }
            oversize => oversize,
        })
    };
    sources::visit_claude(&cfg.claude_root, &origin, limit, &mut stamped)?;
    sources::visit_codex(&cfg.codex_root, &origin, limit, &mut stamped)?;
    if let Some(db) = &cfg.cursor_db {
        let bounds = cursor::ThreadBounds {
            max_threads: CURSOR_MAX_THREADS,
            max_bytes: limit,
        };
        cursor::visit_threads(db, &origin, cfg.cursor_limit, bounds, &mut |item| {
            stamped(match item {
                cursor::ThreadItem::Thread(thread) => {
                    let thread = *thread;
                    Found::Transcript(Box::new(sources::Discovered {
                        path: sources::cursor_path(db, &thread.envelope.session_id),
                        fingerprint: thread.fingerprint,
                        envelope: thread.envelope,
                    }))
                }
                cursor::ThreadItem::Oversize(key) => Found::Oversize(sources::cursor_path(
                    db,
                    key.strip_prefix("composerData:").unwrap_or(&key),
                )),
                cursor::ThreadItem::Overflow(count) => Found::Overflow(count),
            })
        })
        .map_err(|error| sources::VisitError::Source(error.into()))?
        .map_err(sources::VisitError::Visitor)?;
    }
    Ok(())
}

/// Add the caller's correlation tags to an envelope, leaving any tags a parser
/// already derived in place. This is the one funnel every source passes through,
/// so a tag configured once applies uniformly across harnesses.
fn stamp_tags(envelope: &mut SessionEnvelope, tags: &[String]) {
    if tags.is_empty() {
        return;
    }
    let meta = envelope
        .metadata
        .get_or_insert_with(session_capture::Metadata::default);
    for tag in tags {
        if !meta.tags.contains(tag) {
            meta.tags.push(tag.clone());
        }
    }
}

/// The result of a cheap scan of every configured source: one lightweight
/// [`Candidate`] per transcript, plus the Cursor DB connection the candidates are
/// later loaded from (so a run opens the DB once).
struct Scan {
    /// Every scanned transcript, in upload order (Claude, Codex, Cursor).
    candidates: Vec<Candidate>,
    cursor: Option<CursorDb>,
}

/// Cheaply scan every source under the configured roots: metadata and
/// fingerprints only, no transcript is read or parsed. The Cursor DB is left
/// unopened when `include_cursor` is false (a gate hit).
fn scan_sources(cfg: &Config, include_cursor: bool) -> Result<Scan, sources::SourceError> {
    let mut candidates = sources::scan_claude(&cfg.claude_root)?;
    candidates.extend(sources::scan_codex(&cfg.codex_root)?);
    let mut cursor = None;
    if let Some(db) = cfg.cursor_db.as_deref().filter(|_| include_cursor) {
        let conn = CursorDb::open(db)?;
        // `cursor_limit` caps a run to the newest N threads (from CURSOR_LIMIT
        // or the --cursor-limit CLI flag); None captures all.
        candidates.extend(sources::scan_cursor(&conn, db, cfg.cursor_limit)?);
        cursor = Some(conn);
    }
    Ok(Scan { candidates, cursor })
}

/// Load one candidate and stamp the configured tags onto it.
fn load_candidate(
    candidate: &Candidate,
    origin: &Origin,
    cursor: Option<&CursorDb>,
    tags: &[String],
) -> Result<Option<Loaded>, sources::SourceError> {
    let mut loaded = sources::load(candidate, origin, cursor)?;
    if let Some(l) = loaded.as_mut() {
        stamp_tags(&mut l.envelope, tags);
    }
    Ok(loaded)
}

/// The whole-DB Cursor gate signature, `<mtime_ns>:<size>:<max_envelope_bytes>:
/// <cursor_limit|all>`, plus the mtime it was taken from. `None` when the DB
/// cannot be stat'ed (the scan then surfaces the open error as before).
///
/// Why: a real `state.vscdb` can be ~11GB and unchanged for long stretches,
/// yet a per-thread scan still reads every composer row and validates every
/// bubble row. Recording the file's identity once every thread is captured lets
/// later runs skip opening it at all. The mtime is nanosecond-precision so a
/// same-size edit within the same second still misses. The cap and limit are
/// part of the signature because they change which threads a capture covers.
/// Stamp config (store URL, tags) needs no slot: a digest change already
/// discards the whole state, gate included. The DB is read immutable, so the
/// main file's metadata is exactly what a scan would read.
///
/// The value stored in state, under the DB path as key, is `<signature>#<n>`,
/// where `n` is the number of Cursor candidates that scan produced; a hit
/// reports `n` as discovered + unchanged, so summaries stay comparable without
/// trusting per-thread keys that may be stale (deleted threads, a lowered
/// limit). A build that predates the gate never looks the key up, so the entry
/// is inert there.
struct CursorGate {
    signature: String,
    mtime: SystemTime,
}

fn cursor_gate(db: &Path, cfg: &Config) -> Option<CursorGate> {
    let meta = std::fs::metadata(db).ok()?;
    let mtime = meta.modified().unwrap_or(UNIX_EPOCH);
    let mtime_ns = mtime.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let limit = cfg
        .cursor_limit
        .map_or_else(|| "all".to_string(), |n| n.to_string());
    Some(CursorGate {
        signature: format!(
            "{mtime_ns}:{}:{}:{limit}",
            meta.len(),
            cfg.max_envelope_bytes
        ),
        mtime,
    })
}

impl CursorGate {
    /// The candidate count recorded with a matching signature, if any.
    fn hit(&self, recorded: Option<&str>) -> Option<usize> {
        recorded
            .and_then(|v| v.rsplit_once('#'))
            .filter(|(signature, _)| *signature == self.signature)
            .and_then(|(_, n)| n.parse().ok())
    }

    /// Racy-git guard: a DB modified within `RACY_WINDOW` of the scan's start may
    /// still be changing within the same mtime tick, so do not trust (record) it.
    fn settled(&self, scan_started: SystemTime) -> bool {
        scan_started
            .duration_since(self.mtime)
            .is_ok_and(|age| age >= RACY_WINDOW)
    }
}

/// See `CursorGate::settled`.
const RACY_WINDOW: Duration = Duration::from_secs(2);

/// Perform one full capture run: scan, skip unchanged, stream-load and
/// batch-upload, persist state. `check_health` gates the run on a reachable
/// store so a misconfigured URL fails fast with a clear message instead of a
/// wall of connection errors.
///
/// Memory: only candidates whose fingerprint is not current are loaded, one at
/// a time, into a batch bounded by `batch_size` and `BATCH_BYTE_BUDGET`; the
/// batch is dropped once sent. Peak memory is about one batch (plus one large
/// transcript), not the corpus.
///
/// Every source is scanned before anything is loaded or sent, so a source that
/// cannot be read still aborts the sweep before any upload, as it always has.
// Measured: the reachable/unreachable, scan, gate hit/miss, upload (success +
// hard-failure), and health-record branches are all driven by the `run_*`
// tests. The only cold spots are the `scan_sources` / `upload_stream` /
// `record_success` `?` error arms, which `?` propagation leaves as covered
// regions.
pub async fn run(cfg: &Config) -> Result<RunSummary, RunError> {
    let client = Client::new(&cfg.store_url, cfg.write_token.clone());
    if !client.healthy(&cfg.store_url).await {
        return Err(RunError::Unreachable(cfg.store_url.clone()));
    }

    // A caller that cannot trust the state file asks for it to be ignored
    // rather than protected: where the file sits somewhere the audited process
    // can write, "protect it" has no implementation. An ignored state is empty,
    // so it can never hit the Cursor gate either.
    let mut state = if cfg.ignore_state {
        State::ignoring(&cfg.state_file, &cfg.stamp_digest())
    } else {
        State::load(&cfg.state_file, &cfg.stamp_digest())
    };
    let gate = cfg
        .cursor_db
        .as_deref()
        .and_then(|db| cursor_gate(db, cfg).map(|g| (db, g)));
    let gated = gate.as_ref().and_then(|(db, g)| g.hit(state.get(db)));

    let scan_started = SystemTime::now();
    let scan = scan_sources(cfg, gated.is_none())?;
    let mut summary = RunSummary {
        discovered: scan.candidates.len(),
        ..Default::default()
    };
    if let Some(known) = gated {
        // The DB is byte-for-byte what the last full capture saw: report the
        // thread count that capture scanned as discovered + unchanged.
        summary.discovered += known;
        summary.skipped_unchanged += known;
    }
    let cursor_scanned = scan
        .candidates
        .iter()
        .filter(|c| matches!(c.kind, SourceKind::Cursor { .. }))
        .count();
    let pending = pending_candidates(scan.candidates, &state, &mut summary);

    let origin = origin_of(cfg);
    let cursor = scan.cursor.as_ref();
    let limits = Limits {
        batch_items: cfg.batch_size,
        batch_bytes: BATCH_BYTE_BUDGET,
        max_envelope_bytes: cfg.max_envelope_bytes,
    };
    // Cursor candidates that load to nothing (rows only SQLite's JSON reader
    // accepts, see `cursor::SCAN_SQL`) are not threads to the full reader, so
    // they are left out of the count the gate records.
    let cursor_unloaded = std::cell::Cell::new(0usize);
    let items = pending.into_iter().map(|candidate| {
        let is_cursor = matches!(candidate.kind, SourceKind::Cursor { .. });
        let loaded = load_candidate(&candidate, &origin, cursor, &cfg.tags)?;
        if loaded.is_none() && is_cursor {
            cursor_unloaded.set(cursor_unloaded.get() + 1);
        }
        Ok(loaded.map(|loaded| Item {
            path: candidate.path,
            fingerprint: loaded.fingerprint,
            is_cursor,
            envelope: loaded.envelope,
        }))
    });
    let outcome = upload_stream(&client, limits, items, &mut state, &mut summary).await?;
    // A changed candidate that no longer loads (vanished, empty, unparseable)
    // was never "discovered" in the sense this counter has always had: the
    // full reader dropped it before counting.
    summary.discovered -= outcome.unloaded;

    // Close the gate only once every Cursor thread this scan found is settled
    // (marked, or no longer loadable), and only for a DB that was not modified
    // just before the scan. A Cursor thread left for the next sweep (oversize,
    // hard failure, rejected, unconfirmed) keeps it open, so it is retried and
    // reported on every sweep exactly as before the gate existed.
    let close = |g: &CursorGate| {
        gated.is_none() && outcome.cursor_unsettled == 0 && g.settled(scan_started)
    };
    if let Some((db, g)) = gate.filter(|(_, g)| close(g)) {
        let threads = cursor_scanned - cursor_unloaded.get();
        state.mark(db, format!("{}#{threads}", g.signature));
        persist(&state);
    }
    if summary.failed == 0 {
        health::record_success(&cfg.health_file).map_err(RunError::Health)?;
    }

    Ok(summary)
}

/// Drop the candidates whose fingerprint is already current in state, counting
/// them as `skipped_unchanged`. Only what remains is ever read or parsed.
fn pending_candidates(
    candidates: Vec<Candidate>,
    state: &State,
    summary: &mut RunSummary,
) -> Vec<Candidate> {
    let scanned = candidates.len();
    let pending: Vec<Candidate> = candidates
        .into_iter()
        .filter(|c| !state.is_current(&c.path, &c.fingerprint))
        .collect();
    summary.skipped_unchanged += scanned - pending.len();
    pending
}

/// Batch bounds for `upload_stream`. `batch_bytes` is `BATCH_BYTE_BUDGET` in
/// production; tests shrink it to exercise byte-driven splits cheaply.
#[derive(Debug, Clone, Copy)]
struct Limits {
    batch_items: usize,
    batch_bytes: usize,
    max_envelope_bytes: usize,
}

/// One loaded transcript on its way to the store.
struct Item {
    /// State key.
    path: PathBuf,
    /// Fingerprint to record once the store confirms it.
    fingerprint: String,
    /// Whether it is a Cursor thread (so the whole-DB gate stays open while
    /// any Cursor thread is left for the next sweep).
    is_cursor: bool,
    envelope: SessionEnvelope,
}

/// What `upload_stream` reports beyond the summary.
#[derive(Debug, Default, Clone, Copy)]
struct StreamOutcome {
    /// Pending candidates that loaded to nothing (vanished, empty, unparseable).
    unloaded: usize,
    /// Cursor threads left unmarked for the next sweep for any reason.
    cursor_unsettled: usize,
}

/// The batch being filled: envelopes (sent as one slice, never cloned) plus each
/// item's state key, fingerprint, and whether it is a Cursor thread.
#[derive(Default)]
struct Batch {
    envelopes: Vec<SessionEnvelope>,
    items: Vec<(PathBuf, String, bool)>,
    bytes: usize,
}

/// Stream the pending items to the store, holding ONE loaded envelope plus the
/// current batch at a time. `items` is lazy: each is loaded only when the
/// previous one has been placed.
///
/// Each item that loaded (an `Ok(None)` is skipped unmarked) is then:
///   - skipped BEFORE upload when its serialized size exceeds
///     `max_envelope_bytes`: a single pathological transcript (for example a
///     runaway multi-hundred-MB Cursor thread) must never be sent, let alone
///     abort the sweep. It is left unmarked so a later raised cap or a shrunk
///     transcript retries it.
///   - otherwise added to the current batch, which is flushed first if the item
///     would overflow the byte budget (so an item larger than the budget goes
///     solo, never zero-sized), and flushed after when it reaches
///     `batch_items` or fills the budget. These are the same batches the
///     earlier plan-then-send form produced, since batches were always
///     consecutive runs of items and a full batch could take no further item.
///
/// Each flush sends, marks what the store confirmed, persists state, and drops
/// the batch. A load error (a Cursor query failure) aborts the stream;
/// already-sent batches stay marked.
async fn upload_stream<S, I>(
    sender: &S,
    limits: Limits,
    items: I,
    state: &mut State,
    summary: &mut RunSummary,
) -> Result<StreamOutcome, sources::SourceError>
where
    S: BatchSender + ?Sized,
    I: IntoIterator<Item = Result<Option<Item>, sources::SourceError>>,
{
    let max_items = limits.batch_items.max(1);
    let mut outcome = StreamOutcome::default();
    let mut batch = Batch::default();
    for item in items {
        let Some(item) = item? else {
            outcome.unloaded += 1;
            continue;
        };
        let size = estimated_len(&item.envelope);
        if size > limits.max_envelope_bytes {
            summary.skipped_oversize += 1;
            outcome.cursor_unsettled += usize::from(item.is_cursor);
            tracing::warn!(
                session_id = %item.envelope.session_id,
                bytes = size,
                cap = limits.max_envelope_bytes,
                "skipping oversize envelope (exceeds MAX_ENVELOPE_BYTES); not uploaded"
            );
            continue;
        }
        if !batch.envelopes.is_empty() && batch.bytes + size > limits.batch_bytes {
            flush(sender, &mut batch, state, summary, &mut outcome).await;
        }
        batch.envelopes.push(item.envelope);
        batch
            .items
            .push((item.path, item.fingerprint, item.is_cursor));
        batch.bytes += size;
        // A batch at its item count or byte budget can take nothing more, so it
        // is sent now rather than when the next item arrives: otherwise a large
        // transcript would still be held while the next one is loaded.
        if batch.envelopes.len() >= max_items || batch.bytes >= limits.batch_bytes {
            flush(sender, &mut batch, state, summary, &mut outcome).await;
        }
    }
    flush(sender, &mut batch, state, summary, &mut outcome).await;
    Ok(outcome)
}

/// Send the current batch resiliently, then persist state and empty the batch.
/// An empty batch is a no-op.
///
/// A batch-level hard error (413, 5xx, timeout, connection closed) does NOT
/// abort the run: the batch is retried ONE ITEM AT A TIME, and any single item
/// that still hard-fails is counted, logged (session_id + reason + byte size),
/// and skipped so the sweep continues.
///
/// State is marked ONLY for items the store confirmed it holds (accepted or
/// duplicate). Items that were rejected, went unanswered, or hard-failed are
/// left unmarked so the next sweep retries them. Generic over `BatchSender` so
/// the fallback logic is unit-tested with a mock, no network required.
async fn flush<S: BatchSender + ?Sized>(
    sender: &S,
    batch: &mut Batch,
    state: &mut State,
    summary: &mut RunSummary,
    outcome: &mut StreamOutcome,
) {
    if batch.envelopes.is_empty() {
        return;
    }
    let Batch {
        envelopes, items, ..
    } = std::mem::take(batch);
    match sender.send_batch(&envelopes).await {
        Ok(results) => {
            let t = tally(&results);
            summary.uploaded += results.len();
            summary.accepted += t.accepted;
            summary.duplicate += t.duplicate;
            summary.rejected += t.rejected;

            // Mark ONLY what the store confirmed it holds.
            //
            // This loop used to mark the whole slice, which quietly
            // undermined every later sweep: a REJECTED envelope was
            // recorded as sent, so the next sweep skipped it as unchanged
            // and reported success. One transient rejection turned into
            // permanent silent absence. The same bug covered a store that
            // returned 200 with fewer results than items sent, or none at
            // all, by caching everything as done without proof.
            //
            // Duplicate counts as confirmed: the store says it already has
            // that session, which is the outcome the caller wanted.
            // A MULTISET, not a set. Two envelopes can carry the same
            // session id while differing in content (the dedup identity is
            // session id plus content hash), so one Accepted result must
            // confirm exactly ONE of them. A set would mark both, including
            // one the store had rejected.
            let mut confirmed: std::collections::HashMap<&str, usize> =
                std::collections::HashMap::new();
            // Explicit rejections are tracked separately so they are not
            // ALSO counted as unconfirmed. "The store refused this" and
            // "the store said nothing about this" are different facts, and
            // a caller reading the counters should be able to tell them
            // apart: one is a bad envelope, the other is a store not
            // answering its contract.
            // A COUNTED multiset, for the same reason `confirmed` is one.
            // As a set, a single rejection covering two envelopes that
            // share a session id suppressed `unconfirmed` for BOTH: one was
            // genuinely refused, the other got no answer at all, and the
            // sweep under-reported the loss it had just taken.
            let mut rejected_ids: std::collections::HashMap<&str, usize> =
                std::collections::HashMap::new();
            for r in &results {
                match r {
                    Outcome::Accepted { .. } | Outcome::Duplicate { .. } => {
                        *confirmed.entry(r.session_id()).or_insert(0) += 1;
                    }
                    Outcome::Rejected { .. } => {
                        *rejected_ids.entry(r.session_id()).or_insert(0) += 1;
                    }
                }
            }

            if results.len() != envelopes.len() {
                // Not fatal - the unconfirmed items simply retry next
                // sweep - but it means the store is not answering the
                // contract, which an operator should see.
                tracing::warn!(
                    sent = envelopes.len(),
                    returned = results.len(),
                    "store returned a different number of results than envelopes sent; \
                     only confirmed sessions will be marked"
                );
            }

            for (env, (path, fp, is_cursor)) in envelopes.iter().zip(items) {
                match confirmed.get_mut(env.session_id.as_str()) {
                    Some(remaining) if *remaining > 0 => {
                        *remaining -= 1;
                        state.mark(&path, fp);
                        summary.confirmed_sessions.push(env.session_id.clone());
                        continue;
                    }
                    // Left unmarked either way, so the next sweep retries
                    // it. Counted as unconfirmed ONLY when the store said
                    // nothing about it; an explicit rejection is already
                    // counted as rejected and both make the sweep
                    // incomplete.
                    _ => match rejected_ids.get_mut(env.session_id.as_str()) {
                        // Consume one rejection per envelope, so a second
                        // envelope sharing the id is still counted as
                        // unconfirmed unless the store refused it too.
                        Some(remaining) if *remaining > 0 => *remaining -= 1,
                        _ => summary.unconfirmed += 1,
                    },
                }
                outcome.cursor_unsettled += usize::from(is_cursor);
            }
        }
        Err(e) => {
            // Batch-level hard error: fall back to per-item so one bad
            // envelope cannot take down its whole batch. `items.len()` is
            // computed here (covered) and handed to the excluded log helper.
            warn_batch_fallback(&e, items.len());
            for (env, (path, fp, is_cursor)) in envelopes.iter().zip(items) {
                let marked = upload_one_resilient(sender, &path, fp, env, state, summary).await;
                outcome.cursor_unsettled += usize::from(is_cursor && !marked);
            }
        }
    }
    persist(state);
}

/// Persist state, treating a failure as non-fatal (the next sweep re-sends,
/// which the store dedups).
fn persist(state: &State) {
    if let Err(e) = state.save() {
        tracing::warn!(error = %e, "could not persist exporter state (non-fatal)");
    }
}

/// Upload a single envelope, tolerating its individual failure. Returns whether
/// the store confirmed it (and it was marked). On a hard error the item is
/// counted as `failed`, logged with its byte size, and left unmarked to retry
/// next sweep.
async fn upload_one_resilient<S: BatchSender + ?Sized>(
    sender: &S,
    path: &Path,
    fp: String,
    env: &SessionEnvelope,
    state: &mut State,
    summary: &mut RunSummary,
) -> bool {
    let one = std::slice::from_ref(env);
    match sender.send_batch(one).await {
        Ok(results) => {
            let t = tally(&results);
            summary.uploaded += results.len();
            summary.accepted += t.accepted;
            summary.duplicate += t.duplicate;
            summary.rejected += t.rejected;

            // Same rule as the batch path, and for the same reason: a session
            // the store REFUSED must not be recorded as sent, or the next
            // sweep skips it as unchanged and reports success. The solo retry
            // is where a rejected envelope most often lands, since it is the
            // fallback for a batch that already failed.
            if results.iter().any(|r| {
                matches!(r, Outcome::Accepted { .. } | Outcome::Duplicate { .. })
                    && r.session_id() == env.session_id
            }) {
                state.mark(path, fp);
                summary.confirmed_sessions.push(env.session_id.clone());
                return true;
            }
            if t.rejected == 0 {
                // Neither confirmed nor explicitly rejected: the store answered
                // without saying anything about what was sent.
                summary.unconfirmed += 1;
            }
            false
        }
        Err(e) => {
            summary.failed += 1;
            // `estimated_len(env)` is computed here (covered) and handed to the
            // excluded log helper.
            warn_solo_failure(env, estimated_len(env), &e);
            false
        }
    }
}

/// Cumulative byte budget per upload request. Kept well under the server's 512MB
/// batch body limit so even a batch that happens to include a couple of large
/// verbatim threads still fits, with headroom for JSON framing. A single thread
/// larger than this goes solo (still under the 512MB body limit).
const BATCH_BYTE_BUDGET: usize = 24 * 1024 * 1024;

/// Emit the batch-fallback warning. Extracted and coverage-excluded because a
/// `warn!`'s unique field-value sub-region (here `batch_items`) is counted
/// nondeterministically by llvm-cov instrumentation (tracing evaluates field
/// closures lazily), which would make the coverage gate flaky. The branch that
/// calls this, and the `batch_items` computation at the call site, are covered;
/// only the diagnostic log line itself is excluded. No logic lives here.
#[cfg_attr(coverage_nightly, coverage(off))]
fn warn_batch_fallback(error: &upload::UploadError, batch_items: usize) {
    tracing::warn!(
        %error,
        batch_items,
        "batch upload failed; retrying its items one at a time"
    );
}

/// Emit the solo-retry-failure warning. Coverage-excluded for the same reason as
/// `warn_batch_fallback`; `bytes` is computed at the (covered) call site.
#[cfg_attr(coverage_nightly, coverage(off))]
fn warn_solo_failure(env: &SessionEnvelope, bytes: usize, error: &upload::UploadError) {
    tracing::warn!(
        session_id = %env.session_id,
        bytes,
        reason = %error,
        "skipping envelope after solo-retry hard failure; will retry next sweep"
    );
}

/// A `Write` sink that only counts bytes, so sizing an envelope does not
/// allocate a second copy of it (a large verbatim thread can be hundreds of MB).
struct ByteCounter(usize);

impl std::io::Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Estimated serialized size of an envelope, used for the size cap and batch
/// planning. Serializing an envelope of strings and JSON values cannot fail, so
/// the 4096 fallback is a never-taken default.
fn estimated_len(env: &SessionEnvelope) -> usize {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, env)
        .map(|()| counter.0)
        .unwrap_or(4096)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_summary_defaults_zero() {
        let s = RunSummary::default();
        assert_eq!(s.discovered, 0);
        assert_eq!(s.uploaded, 0);
    }

    // --- streaming batches ---------------------------------------------------

    /// Accepts every envelope and records the size of each request, so the
    /// tests can see exactly how the stream split the pending items.
    #[derive(Default)]
    struct RecordingSender {
        batches: std::sync::Mutex<Vec<usize>>,
    }

    #[async_trait::async_trait]
    impl BatchSender for RecordingSender {
        async fn send_batch(
            &self,
            batch: &[SessionEnvelope],
        ) -> Result<Vec<crate::upload::Outcome>, crate::upload::UploadError> {
            self.batches.lock().unwrap().push(batch.len());
            Ok(batch
                .iter()
                .map(|e| crate::upload::Outcome::Accepted {
                    session_id: e.session_id.clone(),
                })
                .collect())
        }
    }

    /// Stream `items` (all Claude-like, none Cursor) with explicit limits and
    /// return the batch sizes sent.
    async fn batches_for(items: &[(&str, usize)], limits: Limits) -> Vec<usize> {
        let sender = RecordingSender::default();
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        upload_stream(
            &sender,
            limits,
            loaded(&pending(items), false),
            &mut state,
            &mut summary,
        )
        .await
        .unwrap();
        assert_eq!(summary.accepted, summary.uploaded);
        sender.batches.into_inner().unwrap()
    }

    fn limits(batch_items: usize, batch_bytes: usize) -> Limits {
        Limits {
            batch_items,
            batch_bytes,
            max_envelope_bytes: usize::MAX,
        }
    }

    #[tokio::test]
    async fn stream_splits_by_count_when_items_small() {
        // 10 tiny items, max 4 per batch -> 4 + 4 + 2.
        let items: Vec<(String, usize)> = (0..10).map(|i| (format!("s{i}"), 10)).collect();
        let items: Vec<(&str, usize)> = items.iter().map(|(id, n)| (id.as_str(), *n)).collect();
        assert_eq!(
            batches_for(&items, limits(4, BATCH_BYTE_BUDGET)).await,
            vec![4, 4, 2]
        );
    }

    #[tokio::test]
    async fn stream_splits_by_bytes_when_items_large() {
        // Each item is 60% of the byte budget, so only one fits per batch even
        // though max_items is 50: the split is driven by bytes, not count.
        let budget = estimated_len(&envelope("a", 1000)) * 10 / 6;
        assert_eq!(
            batches_for(&[("a", 1000), ("b", 1000), ("c", 1000)], limits(50, budget)).await,
            vec![1, 1, 1]
        );
    }

    #[tokio::test]
    async fn an_item_larger_than_the_budget_goes_solo() {
        // One item larger than the whole budget still forms a batch of 1 (never
        // stalls at zero), and the small ones after it share a batch.
        let budget = estimated_len(&envelope("a", 1000)) / 2;
        assert_eq!(
            batches_for(&[("big", 1000), ("b", 1), ("c", 1)], limits(50, budget)).await,
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn a_batch_that_fills_the_budget_is_sent_before_the_next_item_loads() {
        // Peak memory is one batch plus one loaded item, never a large item
        // plus the next: each item records how many batches had been sent by
        // the time it was loaded.
        let sender = RecordingSender::default();
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        let budget = estimated_len(&envelope("big", 1000)) / 2;
        let sent_when_loaded = std::sync::Mutex::new(Vec::new());
        // Each envelope is built only when the stream pulls it, as a real load
        // is, and records how many batches had been sent at that moment.
        let items = [("big", 1000), ("b", 1), ("c", 1)]
            .into_iter()
            .map(|(id, filler)| {
                let sent = sender.batches.lock().unwrap().len();
                sent_when_loaded.lock().unwrap().push(sent);
                Ok(Some(Item {
                    path: PathBuf::from(format!("/state/{id}")),
                    fingerprint: format!("fp-{id}"),
                    is_cursor: false,
                    envelope: envelope(id, filler),
                }))
            });
        upload_stream(&sender, limits(50, budget), items, &mut state, &mut summary)
            .await
            .unwrap();
        assert_eq!(sent_when_loaded.into_inner().unwrap(), vec![0, 1, 1]);
        assert_eq!(sender.batches.into_inner().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn empty_pending_sends_nothing() {
        assert!(batches_for(&[], limits(50, BATCH_BYTE_BUDGET))
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn items_that_do_not_load_are_counted_and_never_sent() {
        let sender = RecordingSender::default();
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        let mut items = loaded(&pending(&[("a", 10)]), false);
        items.insert(0, Ok(None));
        let outcome = upload_stream(
            &sender,
            limits(50, BATCH_BYTE_BUDGET),
            items,
            &mut state,
            &mut summary,
        )
        .await
        .unwrap();
        assert_eq!(outcome.unloaded, 1);
        assert_eq!(summary.accepted, 1);
        assert_eq!(sender.batches.into_inner().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn a_load_error_aborts_the_stream_after_what_was_already_sent() {
        let sender = RecordingSender::default();
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        let mut items = loaded(&pending(&[("a", 10), ("b", 10)]), false);
        items.insert(
            1,
            Err(sources::SourceError::Cursor(cursor::CursorError::Query(
                rusqlite::Error::InvalidQuery,
            ))),
        );
        let result = upload_stream(
            &sender,
            limits(1, BATCH_BYTE_BUDGET),
            items,
            &mut state,
            &mut summary,
        )
        .await;
        assert!(result.is_err());
        // The batch before the failure was sent and marked; nothing after it.
        assert_eq!(sender.batches.into_inner().unwrap(), vec![1]);
        assert!(state.is_current(&PathBuf::from("/state/a"), "fp-a"));
        assert!(!state.is_current(&PathBuf::from("/state/b"), "fp-b"));
    }

    #[test]
    fn estimated_len_is_the_serialized_length() {
        let env = envelope("a", 123);
        assert_eq!(estimated_len(&env), serde_json::to_vec(&env).unwrap().len());
        // The counting sink holds nothing, so flushing it is a no-op.
        let mut counter = ByteCounter(7);
        std::io::Write::flush(&mut counter).unwrap();
        assert_eq!(counter.0, 7);
    }

    #[tokio::test]
    async fn a_silent_solo_retry_is_unconfirmed_and_keeps_cursor_threads_unsettled() {
        // The batch fails, and each solo retry gets a 200 that names nothing:
        // neither confirmed nor rejected, so unconfirmed, unmarked, and (for a
        // Cursor thread) a reason to keep the whole-DB gate open.
        struct SilentSolo;
        #[async_trait]
        impl BatchSender for SilentSolo {
            async fn send_batch(
                &self,
                batch: &[SessionEnvelope],
            ) -> Result<Vec<Outcome>, UploadError> {
                if batch.len() > 1 {
                    return Err(UploadError::Status(413));
                }
                Ok(Vec::new())
            }
        }
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        let outcome = upload_stream(
            &SilentSolo,
            limits(50, BATCH_BYTE_BUDGET),
            loaded(&pending(&[("a", 10), ("b", 10)]), true),
            &mut state,
            &mut summary,
        )
        .await
        .unwrap();
        assert_eq!(summary.unconfirmed, 2);
        assert_eq!(outcome.cursor_unsettled, 2);
        assert!(!state.is_current(&PathBuf::from("/state/a"), "fp-a"));
    }

    // --- resilience tests ---------------------------------------------------

    use crate::upload::{Outcome, UploadError};
    use async_trait::async_trait;
    use serde_json::json;
    use session_capture::{Metadata, Origin, SessionEnvelope, SCS_VERSION};
    use std::sync::Mutex;

    fn envelope(id: &str, filler: usize) -> SessionEnvelope {
        SessionEnvelope {
            scs_version: SCS_VERSION.to_string(),
            origin: Origin::new("h", "test"),
            agent: "Cursor".into(),
            source_format: "cursor-state-vscdb".into(),
            session_id: id.into(),
            parent_session_id: None,
            started_at: "2026-07-14T00:00:00Z".into(),
            last_activity_at: "2026-07-14T00:10:00Z".into(),
            content_hash: None,
            metadata: Some(Metadata::default()),
            raw: json!({ "text": "x".repeat(filler) }),
        }
    }

    /// A loaded upload as the tests describe it: (state key, fingerprint,
    /// envelope).
    type Pending = (PathBuf, String, SessionEnvelope);

    /// The stream's input for already-loaded items.
    fn loaded(
        items: &[Pending],
        is_cursor: bool,
    ) -> Vec<Result<Option<Item>, sources::SourceError>> {
        items
            .iter()
            .cloned()
            .map(|(path, fingerprint, envelope)| {
                Ok(Some(Item {
                    path,
                    fingerprint,
                    is_cursor,
                    envelope,
                }))
            })
            .collect()
    }

    /// Upload already-loaded items with production batch limits and no size
    /// cap: the batch, confirmation and fallback logic on its own.
    async fn upload_pending<S: BatchSender + ?Sized>(
        sender: &S,
        batch_size: usize,
        items: &[Pending],
        state: &mut State,
        summary: &mut RunSummary,
    ) -> StreamOutcome {
        upload_stream(
            sender,
            limits(batch_size, BATCH_BYTE_BUDGET),
            loaded(items, false),
            state,
            summary,
        )
        .await
        .unwrap()
    }

    fn pending(items: &[(&str, usize)]) -> Vec<Pending> {
        items
            .iter()
            .map(|(id, filler)| {
                (
                    PathBuf::from(format!("/state/{id}")),
                    format!("fp-{id}"),
                    envelope(id, *filler),
                )
            })
            .collect()
    }

    fn temp_state() -> State {
        let dir = std::env::temp_dir().join(format!(
            "sss-exporter-state-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        State::load(&dir.join("state.json"), "")
    }

    /// Mock sender: fails any batch of >1 item (simulating a 413 from a mixed
    /// batch), and on solo retries fails only the session ids in `fail_solo`.
    /// Records which session ids it accepted.
    struct MockSender {
        fail_solo: Vec<String>,
        accepted: Mutex<Vec<String>>,
        batch_calls: Mutex<usize>,
        solo_calls: Mutex<usize>,
    }

    impl MockSender {
        fn new(fail_solo: &[&str]) -> Self {
            Self {
                fail_solo: fail_solo.iter().map(|s| s.to_string()).collect(),
                accepted: Mutex::new(Vec::new()),
                batch_calls: Mutex::new(0),
                solo_calls: Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl BatchSender for MockSender {
        async fn send_batch(&self, batch: &[SessionEnvelope]) -> Result<Vec<Outcome>, UploadError> {
            if batch.len() > 1 {
                *self.batch_calls.lock().unwrap() += 1;
                // Simulate a batch-level hard error (413).
                return Err(UploadError::Status(413));
            }
            *self.solo_calls.lock().unwrap() += 1;
            let env = &batch[0];
            if self.fail_solo.contains(&env.session_id) {
                return Err(UploadError::Status(413));
            }
            self.accepted.lock().unwrap().push(env.session_id.clone());
            Ok(vec![Outcome::Accepted {
                session_id: env.session_id.clone(),
            }])
        }
    }

    /// A sender whose batch response is under the caller's control, so the
    /// tests can say exactly what the store confirmed.
    struct ScriptedSender {
        results: Vec<Outcome>,
    }

    #[async_trait]
    impl BatchSender for ScriptedSender {
        async fn send_batch(
            &self,
            _batch: &[SessionEnvelope],
        ) -> Result<Vec<Outcome>, UploadError> {
            Ok(self.results.clone())
        }
    }

    #[tokio::test]
    async fn a_rejected_session_is_not_recorded_as_sent() {
        // The bug this prevents: marking the whole slice meant a REJECTED
        // envelope was cached as done, so the next sweep skipped it as
        // unchanged and reported success. One transient rejection became
        // permanent silent absence from the store.
        let sender = ScriptedSender {
            results: vec![
                Outcome::Accepted {
                    session_id: "good".into(),
                },
                Outcome::Rejected {
                    session_id: "bad".into(),
                    reason: "schema".into(),
                },
            ],
        };
        let items = pending(&[("good", 10), ("bad", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        assert!(
            state.is_current(&items[0].0, &items[0].1),
            "an accepted session should not be re-sent next sweep"
        );
        assert!(
            !state.is_current(&items[1].0, &items[1].1),
            "a rejected session MUST be retried, not cached as sent"
        );
    }

    #[tokio::test]
    async fn a_solo_retry_that_is_rejected_is_not_recorded_as_sent() {
        // The solo path is the fallback for a batch that already failed, so a
        // rejected envelope lands here more often than in the batch path. It
        // had the same unconditional mark, and fixing only the batch path
        // would have left the more likely route to silent loss open.
        struct RejectSolo;

        #[async_trait]
        impl BatchSender for RejectSolo {
            async fn send_batch(
                &self,
                batch: &[SessionEnvelope],
            ) -> Result<Vec<Outcome>, UploadError> {
                Ok(vec![Outcome::Rejected {
                    session_id: batch[0].session_id.clone(),
                    reason: "schema".into(),
                }])
            }
        }

        let items = pending(&[("solo", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_one_resilient(
            &RejectSolo,
            &items[0].0,
            items[0].1.clone(),
            &items[0].2,
            &mut state,
            &mut summary,
        )
        .await;

        assert!(
            !state.is_current(&items[0].0, &items[0].1),
            "a solo-rejected session MUST be retried, not cached as sent"
        );
    }

    #[tokio::test]
    async fn an_empty_results_array_is_counted_as_unconfirmed() {
        // Leaving the items unmarked was necessary but NOT sufficient: with no
        // failure counter incremented, the sweep still reported completeness
        // and exited 0 having stored nothing. The count is what makes the exit
        // code honest.
        crate::install_warn_logging();
        let sender = ScriptedSender { results: vec![] };
        let items = pending(&[("a", 10), ("b", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        assert_eq!(summary.unconfirmed, 2, "both envelopes went unconfirmed");
        assert_eq!(
            summary.rejected, 0,
            "the store rejected nothing; it said nothing"
        );
    }

    #[tokio::test]
    async fn an_explicit_rejection_is_not_also_counted_unconfirmed() {
        // "Refused" and "not mentioned" are different facts. Counting a
        // rejection as both inflates unconfirmed and hides which of the two
        // actually happened, which is the thing an operator needs to know.
        let sender = ScriptedSender {
            results: vec![Outcome::Rejected {
                session_id: "bad".into(),
                reason: "schema".into(),
            }],
        };
        let items = pending(&[("bad", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        assert_eq!(summary.rejected, 1);
        assert_eq!(summary.unconfirmed, 0, "a rejection is not an unconfirmed");
        assert!(!state.is_current(&items[0].0, &items[0].1));
    }

    #[tokio::test]
    async fn one_accepted_result_confirms_exactly_one_envelope() {
        // Two envelopes can share a session id while differing in content, so
        // a set-based match would mark BOTH on a single Accepted, including one
        // the store never saw.
        let sender = ScriptedSender {
            results: vec![Outcome::Accepted {
                session_id: "same".into(),
            }],
        };
        // Distinct PATHS deliberately: two transcript files can carry the same
        // session id with different content, which is exactly the case a
        // set-based match gets wrong. Sharing a path would make them one state
        // entry and the test would prove nothing.
        let items = vec![
            (
                PathBuf::from("/state/same-a"),
                "fp-a".to_string(),
                envelope("same", 10),
            ),
            (
                PathBuf::from("/state/same-b"),
                "fp-b".to_string(),
                envelope("same", 20),
            ),
        ];
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        let marked = [0usize, 1]
            .iter()
            .filter(|i| state.is_current(&items[**i].0, &items[**i].1))
            .count();
        assert_eq!(
            marked, 1,
            "one confirmation must confirm exactly one envelope"
        );
        assert_eq!(
            summary.unconfirmed, 1,
            "the other is unconfirmed, not silently done"
        );
    }

    #[tokio::test]
    async fn a_duplicate_counts_as_stored() {
        // The store says it already holds it, which is what the caller wanted.
        let sender = ScriptedSender {
            results: vec![Outcome::Duplicate {
                session_id: "dup".into(),
            }],
        };
        let items = pending(&[("dup", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        assert!(state.is_current(&items[0].0, &items[0].1));
    }

    #[tokio::test]
    async fn an_empty_results_array_confirms_nothing() {
        // A 200 with no results used to cache every item as done, claiming
        // storage the store never acknowledged.
        crate::install_warn_logging();
        let sender = ScriptedSender { results: vec![] };
        let items = pending(&[("a", 10), ("b", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        assert!(!state.is_current(&items[0].0, &items[0].1));
        assert!(!state.is_current(&items[1].0, &items[1].1));
    }

    #[tokio::test]
    async fn batch_failure_falls_back_to_per_item_and_skips_only_the_bad_one() {
        // Three items; the batch fails (413) so all three retry solo. Only "big"
        // fails on its solo retry; the other two upload. The run does not abort.
        // A WARN subscriber makes the batch-fallback and solo-failure warn!
        // field expressions (batch_items, bytes) evaluate during coverage.
        crate::install_warn_logging();
        let sender = MockSender::new(&["big"]);
        let items = pending(&[("a", 10), ("big", 10), ("c", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        // batch_size 50 -> all three in one batch (byte budget is huge here).
        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        // The two good items uploaded; the bad one is counted failed, not fatal.
        let accepted = sender.accepted.lock().unwrap().clone();
        assert!(accepted.contains(&"a".to_string()));
        assert!(accepted.contains(&"c".to_string()));
        assert!(!accepted.contains(&"big".to_string()));
        assert_eq!(summary.accepted, 2);
        assert_eq!(summary.failed, 1);
        // The batch was attempted once, then fell back to 3 solo sends.
        assert_eq!(*sender.batch_calls.lock().unwrap(), 1);
        assert_eq!(*sender.solo_calls.lock().unwrap(), 3);

        // State marked only for the two that succeeded; the failed one is
        // unmarked so it retries next sweep.
        assert!(state.is_current(&PathBuf::from("/state/a"), "fp-a"));
        assert!(state.is_current(&PathBuf::from("/state/c"), "fp-c"));
        assert!(!state.is_current(&PathBuf::from("/state/big"), "fp-big"));
    }

    #[tokio::test]
    async fn confirmed_sessions_names_exactly_what_the_store_confirmed() {
        // The security property behind this field: a caller must be able to ask
        // "is MY session in the store", not merely "did some number of sessions
        // land". Counters alone let a sweep that captured an unrelated decoy
        // report the same success as one that captured the session asked about.
        let sender = MockSender::new(&["big"]);
        let items = pending(&[("a", 10), ("big", 10), ("c", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;

        // "big" failed its solo retry, so it is absent - being discovered and
        // attempted is not the same as being held by the store.
        assert_eq!(
            summary.confirmed_sessions,
            vec!["a".to_string(), "c".to_string()]
        );
    }

    #[tokio::test]
    async fn a_whole_batch_that_succeeds_confirms_every_session_in_it() {
        // The batch path, not the solo-retry path. MockSender rejects any batch
        // larger than one item, so every other test here reaches confirmation
        // through solo retries and leaves the batch path unexercised - a gap a
        // mutation check found by deleting the batch-path confirmation without
        // breaking a single test.
        struct AcceptBatch;

        #[async_trait]
        impl BatchSender for AcceptBatch {
            async fn send_batch(
                &self,
                batch: &[SessionEnvelope],
            ) -> Result<Vec<Outcome>, UploadError> {
                Ok(batch
                    .iter()
                    .map(|e| Outcome::Accepted {
                        session_id: e.session_id.clone(),
                    })
                    .collect())
            }
        }

        let items = pending(&[("a", 10), ("b", 10), ("c", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&AcceptBatch, 50, &items, &mut state, &mut summary).await;

        assert_eq!(summary.accepted, 3);
        assert_eq!(
            summary.confirmed_sessions,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[tokio::test]
    async fn one_rejection_does_not_excuse_a_second_envelope_with_the_same_id() {
        // Two envelopes share a session id; the store refuses ONE and says
        // nothing about the other. Before rejected_ids became a counted
        // multiset, the single rejection suppressed `unconfirmed` for both, so
        // the sweep under-reported a session it had just failed to store.
        struct RejectOnce;

        #[async_trait]
        impl BatchSender for RejectOnce {
            async fn send_batch(
                &self,
                batch: &[SessionEnvelope],
            ) -> Result<Vec<Outcome>, UploadError> {
                // One result for a two-envelope batch: a refusal for the id,
                // and silence about the other envelope carrying it.
                Ok(vec![Outcome::Rejected {
                    session_id: batch[0].session_id.clone(),
                    reason: "schema".into(),
                }])
            }
        }

        let items = pending(&[("dup", 10), ("dup", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&RejectOnce, 50, &items, &mut state, &mut summary).await;

        assert_eq!(summary.rejected, 1, "the store refused exactly one");
        assert_eq!(
            summary.unconfirmed, 1,
            "the envelope the store said nothing about must still count as unconfirmed"
        );
        assert!(summary.confirmed_sessions.is_empty());
    }

    #[tokio::test]
    async fn a_rejected_session_is_never_confirmed() {
        // A rejection is the store saying "I refused this". Recording it as
        // confirmed would be the same class of bug as marking it in state:
        // the caller stops asking about a session nobody has.
        struct RejectEverything;

        #[async_trait]
        impl BatchSender for RejectEverything {
            async fn send_batch(
                &self,
                batch: &[SessionEnvelope],
            ) -> Result<Vec<Outcome>, UploadError> {
                Ok(vec![Outcome::Rejected {
                    session_id: batch[0].session_id.clone(),
                    reason: "schema".into(),
                }])
            }
        }

        let items = pending(&[("solo", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();

        upload_pending(&RejectEverything, 50, &items, &mut state, &mut summary).await;

        assert_eq!(summary.rejected, 1);
        assert!(
            summary.confirmed_sessions.is_empty(),
            "a refused session must not appear as confirmed"
        );
    }

    #[tokio::test]
    async fn successful_batch_marks_all_and_does_not_fall_back() {
        // A single item never trips the >1 batch failure, so it goes straight
        // through as an accepted solo send with no fallback churn.
        let sender = MockSender::new(&[]);
        let items = pending(&[("solo", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;
        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.failed, 0);
        assert!(state.is_current(&PathBuf::from("/state/solo"), "fp-solo"));
    }

    #[tokio::test]
    async fn stream_skips_oversize_before_upload() {
        // With a WARN subscriber active, the oversize warn!'s field expressions
        // are evaluated (they are skipped when logging is disabled).
        crate::install_warn_logging();
        let cap = 1000usize;
        assert!(estimated_len(&envelope("small", 10)) <= cap);
        assert!(estimated_len(&envelope("huge", 5000)) > cap);

        let sender = RecordingSender::default();
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        let outcome = upload_stream(
            &sender,
            Limits {
                batch_items: 50,
                batch_bytes: BATCH_BYTE_BUDGET,
                max_envelope_bytes: cap,
            },
            loaded(&pending(&[("small", 10), ("huge", 5000)]), true),
            &mut state,
            &mut summary,
        )
        .await
        .unwrap();

        // The oversize one is skipped pre-upload and left unmarked; only the
        // small one is sent. Being a Cursor thread, it keeps the gate open.
        assert_eq!(summary.skipped_oversize, 1);
        assert_eq!(summary.confirmed_sessions, vec!["small".to_string()]);
        assert_eq!(sender.batches.into_inner().unwrap(), vec![1]);
        assert!(!state.is_current(&PathBuf::from("/state/huge"), "fp-huge"));
        assert_eq!(outcome.cursor_unsettled, 1);
    }

    #[test]
    fn pending_candidates_skips_unchanged() {
        let mut state = temp_state();
        state.mark(&PathBuf::from("/state/known"), "fp-known".to_string());
        let candidate = |id: &str, fp: &str| Candidate {
            path: PathBuf::from(format!("/state/{id}")),
            fingerprint: fp.into(),
            kind: SourceKind::Claude,
        };
        let mut summary = RunSummary::default();
        let pending = pending_candidates(
            vec![
                candidate("known", "fp-known"),
                candidate("known", "fp-changed"),
                candidate("new", "fp-new"),
            ],
            &state,
            &mut summary,
        );
        assert_eq!(summary.skipped_unchanged, 1);
        assert_eq!(
            pending
                .iter()
                .map(|c| c.fingerprint.as_str())
                .collect::<Vec<_>>(),
            vec!["fp-changed", "fp-new"]
        );
    }

    #[tokio::test]
    async fn upload_pending_warns_when_state_save_fails() {
        // A state path whose parent is a regular file: `create_dir_all` fails, so
        // `save()` errors and the non-fatal warn branch runs.
        crate::install_warn_logging();
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();
        let mut state = State::load(&blocker.join("child").join("state.json"), "");

        let sender = MockSender::new(&[]);
        let items = pending(&[("solo", 10)]);
        let mut summary = RunSummary::default();
        upload_pending(&sender, 50, &items, &mut state, &mut summary).await;
        // Upload still succeeded; only persistence was skipped.
        assert_eq!(summary.accepted, 1);
    }

    // --- discover_all + run: real HTTP against a canned local server ---------

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn http_response(status: u16, body: &[u8]) -> Vec<u8> {
        let reason = if status == 200 { "OK" } else { "Error" };
        let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        head.push_str("Connection: close\r\n\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(body);
        out
    }

    fn spawn_server(responses: Vec<Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            // `Connection: close` -> one accept per response, so the loop runs
            // exactly `responses.len()` times with no unexercised branches.
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0u8; 65536];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(&response);
                let _ = stream.flush();
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    fn dead_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{port}")
    }

    fn write_claude_transcript(root: &std::path::Path) {
        let dir = root.join("-work-proj");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("11111111-2222-3333-4444-555555555555.jsonl"),
            "{\"type\":\"user\",\"timestamp\":\"2026-07-01T00:00:00Z\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n",
        )
        .unwrap();
    }

    fn test_config(store_url: &str, tmp: &std::path::Path) -> Config {
        let claude_root = tmp.join("claude");
        write_claude_transcript(&claude_root);
        Config {
            origin_deployment: None,
            ignore_state: false,
            store_url: store_url.to_string(),
            write_token: Some("tok".into()),
            origin_host: "test-host".into(),
            origin_environment: "laptop".into(),
            claude_root,
            codex_root: tmp.join("codex-empty"),
            cursor_db: None,
            cursor_limit: None,
            state_file: tmp.join("state.json"),
            health_file: tmp.join("health.last-success"),
            health_max_age_secs: 900,
            batch_size: 50,
            max_envelope_bytes: 512 * 1024 * 1024,
            tags: Vec::new(),
        }
    }

    #[test]
    fn discover_all_stamps_origin_and_includes_cursor() {
        use rusqlite::{params, Connection};
        let tmp = tempfile::tempdir().unwrap();
        let claude_root = tmp.path().join("claude");
        write_claude_transcript(&claude_root);

        // A minimal Cursor DB with one inline thread, to exercise the cursor arm
        // of discover_all.
        let db = tmp.path().join("state.vscdb");
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB)",
            [],
        )
        .unwrap();
        let cd = serde_json::json!({
            "composerId": "c1",
            "createdAt": 1000i64,
            "conversation": [{"bubbleId": "b", "type": 1, "text": "hi"}]
        });
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["composerData:c1", serde_json::to_vec(&cd).unwrap()],
        )
        .unwrap();
        drop(conn);

        let cfg = Config {
            cursor_db: Some(db),
            ..test_config("http://unused", tmp.path())
        };
        // Overwrite claude_root (test_config wrote its own under a different tmp).
        let cfg = Config { claude_root, ..cfg };

        let found = discover_all(&cfg).unwrap();
        assert_eq!(found.len(), 2); // one claude + one cursor
        assert!(found.iter().all(|d| d.envelope.origin.host == "test-host"));

        // The streaming form visits the same transcripts, one at a time.
        let cfg = Config {
            tags: vec!["ci:run:7".into()],
            ..cfg
        };
        let mut visited = Vec::new();
        visit_all(&cfg, u64::MAX, &mut |found| {
            if let sources::Found::Transcript(d) = found {
                visited.push((
                    d.envelope.session_id.clone(),
                    d.envelope.metadata.unwrap().tags,
                ));
            }
            Ok::<(), ()>(())
        })
        .unwrap();
        let expected: Vec<_> = found
            .iter()
            .map(|d| d.envelope.session_id.clone())
            .collect();
        assert_eq!(
            visited.iter().map(|v| v.0.clone()).collect::<Vec<_>>(),
            expected
        );
        assert!(visited
            .iter()
            .all(|v| v.1.contains(&"ci:run:7".to_string())));
        // Over the bound, a source file and a Cursor thread are each reported
        // without being parsed or assembled.
        let mut oversize = Vec::new();
        visit_all(&cfg, 1, &mut |found| {
            if let sources::Found::Oversize(path) = found {
                oversize.push(path.display().to_string());
            }
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(oversize.len(), 2);
        assert!(oversize[1].ends_with("state.vscdb#composer:c1"));
        // A visitor failure stops the sweep and is returned as the visitor's.
        let stopped = visit_all(&cfg, u64::MAX, &mut |found| match found {
            sources::Found::Transcript(d) if d.envelope.source_format.starts_with("cursor") => {
                Err("stop")
            }
            _ => Ok(()),
        });
        assert!(matches!(stopped, Err(sources::VisitError::Visitor("stop"))));
    }

    #[test]
    fn stamp_tags_does_not_duplicate_a_tag_the_envelope_already_carries() {
        let mut env = envelope("s", 1);
        env.metadata = Some(Metadata {
            tags: vec!["review".into()],
            ..Default::default()
        });

        stamp_tags(&mut env, &["review".to_string(), "ci:run:42".to_string()]);

        assert_eq!(
            env.metadata.unwrap().tags,
            vec!["review".to_string(), "ci:run:42".to_string()]
        );
    }

    #[test]
    fn stamp_tags_creates_a_metadata_block_when_the_envelope_has_none() {
        let mut env = envelope("s", 1);
        env.metadata = None;

        stamp_tags(&mut env, &["ci:run:42".to_string()]);

        assert_eq!(env.metadata.unwrap().tags, vec!["ci:run:42".to_string()]);
    }

    #[test]
    fn discover_all_stamps_configured_tags_on_every_envelope() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = Config {
            tags: vec!["ci:run:42".into(), "team:platform".into()],
            ..test_config("http://unused", tmp.path())
        };

        let found = discover_all(&cfg).unwrap();
        assert!(!found.is_empty());
        for d in &found {
            assert_eq!(
                d.envelope.metadata.as_ref().unwrap().tags,
                vec!["ci:run:42".to_string(), "team:platform".to_string()]
            );
        }
    }

    #[test]
    fn discover_all_without_tags_leaves_metadata_tags_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config("http://unused", tmp.path());

        let found = discover_all(&cfg).unwrap();
        assert!(!found.is_empty());
        for d in &found {
            assert!(d
                .envelope
                .metadata
                .as_ref()
                .is_none_or(|m| m.tags.is_empty()));
        }
    }

    #[tokio::test]
    async fn run_success_records_health() {
        let tmp = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![
            http_response(200, b"ok"), // healthz
            http_response(
                200,
                br#"{"results":[{"status":"accepted","session_id":"s"}]}"#,
            ),
        ]);
        let cfg = test_config(&url, tmp.path());
        let summary = run(&cfg).await.unwrap();
        assert_eq!(summary.discovered, 1);
        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.failed, 0);
        assert!(cfg.health_file.exists());
    }

    #[tokio::test]
    async fn a_seeded_state_file_still_skips_when_state_is_trusted() {
        // The behaviour --ignore-state exists to defeat, pinned first so the
        // test below measures a real difference rather than a tautology.
        let tmp = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![http_response(200, b"ok")]);
        let cfg = test_config(&url, tmp.path());

        let discovered = discover_all(&cfg).unwrap();
        let mut seeded = State::load(&cfg.state_file, &cfg.stamp_digest());
        for item in &discovered {
            seeded.mark(&item.path, item.fingerprint.clone());
        }
        seeded.save().unwrap();

        let summary = run(&cfg).await.unwrap();

        assert_eq!(summary.discovered, 1);
        assert_eq!(summary.skipped_unchanged, 1);
        assert_eq!(summary.accepted, 0, "nothing was sent to the store");
    }

    #[tokio::test]
    async fn ignore_state_re_sends_despite_a_seeded_state_file() {
        // THE SECURITY PROPERTY. The state file lives where the audited
        // process can write it, so a forged entry can make a transcript that
        // never reached the store report as a clean sweep. Protecting the file
        // is not possible there; not reading it is.
        let tmp = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![
            http_response(200, b"ok"),
            http_response(
                200,
                br#"{"results":[{"status":"accepted","session_id":"11111111-2222-3333-4444-555555555555"}]}"#,
            ),
        ]);
        let mut cfg = test_config(&url, tmp.path());

        let discovered = discover_all(&cfg).unwrap();
        let mut forged = State::load(&cfg.state_file, &cfg.stamp_digest());
        for item in &discovered {
            forged.mark(&item.path, item.fingerprint.clone());
        }
        forged.save().unwrap();

        cfg.ignore_state = true;
        let summary = run(&cfg).await.unwrap();

        assert_eq!(summary.discovered, 1);
        assert_eq!(
            summary.skipped_unchanged, 0,
            "a forged state entry must not be believed"
        );
        // Asserting `accepted == 1` alone proved less than it looked: the
        // canned response named session "s", which is not the discovered
        // transcript, so the run would ALSO have counted it unconfirmed. The
        // response now names the real session id and the unconfirmed counter
        // is asserted, so this says the transcript reached the store rather
        // than merely that a request happened.
        assert_eq!(summary.accepted, 1, "the transcript was sent anyway");
        assert_eq!(
            summary.unconfirmed, 0,
            "and the store confirmed THAT session"
        );
    }

    #[tokio::test]
    async fn ignoring_state_never_rewrites_the_state_file() {
        // An ignored state starts EMPTY. Saving it would replace a real file
        // with only what this sweep confirmed, discarding entries for sessions
        // that were rejected, filtered, or never reached - so a later normal
        // run would resend all of them. A cache loss rather than a transcript
        // loss, and conservative in direction, but a flag documented as "do
        // not read the file" must not rewrite it either.
        let tmp = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![
            http_response(200, b"ok"),
            http_response(
                200,
                br#"{"results":[{"status":"accepted","session_id":"11111111-2222-3333-4444-555555555555"}]}"#,
            ),
        ]);
        let mut cfg = test_config(&url, tmp.path());

        // A state file with an entry this sweep will NOT confirm, which is
        // exactly what a naive writeback would drop.
        let mut existing = State::load(&cfg.state_file, &cfg.stamp_digest());
        existing.mark(std::path::Path::new("/somewhere/else.jsonl"), "fp".into());
        existing.save().unwrap();
        let before = std::fs::read(&cfg.state_file).unwrap();

        cfg.ignore_state = true;
        run(&cfg).await.unwrap();

        let after = std::fs::read(&cfg.state_file).unwrap();
        assert_eq!(before, after, "an ignored sweep rewrote the state file");

        // Byte equality alone could in principle hold by coincidence, so the
        // property is also asserted semantically: the entry this sweep never
        // confirmed is still there for the next normal run to use.
        let reloaded = State::load(&cfg.state_file, &cfg.stamp_digest());
        assert!(
            reloaded.is_current(std::path::Path::new("/somewhere/else.jsonl"), "fp"),
            "the unrelated entry a later run depends on was discarded"
        );
    }

    #[tokio::test]
    async fn run_discover_failure_propagates() {
        // The store is healthy, but a source root that is a regular file makes
        // discover_all fail: run's `discover_all(cfg)?` takes its error edge.
        let tmp = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![http_response(200, b"ok")]); // healthz only
        let file = tmp.path().join("a-file");
        std::fs::write(&file, b"x").unwrap();
        let cfg = Config {
            claude_root: file,
            cursor_db: None,
            ..test_config(&url, tmp.path())
        };
        assert!(run(&cfg).await.is_err());
    }

    #[tokio::test]
    async fn run_health_record_failure_propagates() {
        // A clean sweep (0 failures) tries to record health, but the health file's
        // parent is a regular file, so record_success fails: run's
        // `record_success(..).map_err(RunError::Health)?` takes its error edge.
        let tmp = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![
            http_response(200, b"ok"),
            http_response(
                200,
                br#"{"results":[{"status":"accepted","session_id":"s"}]}"#,
            ),
        ]);
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let cfg = Config {
            // Parent (`blocker`) is a file, so create_dir_all for the health file
            // fails.
            health_file: blocker.join("health.last-success"),
            ..test_config(&url, tmp.path())
        };
        // Assert on the Display (eagerly bound) rather than `matches!`, whose
        // false arm would be an uncovered region.
        let message = run(&cfg).await.err().unwrap().to_string();
        assert!(
            message.contains("could not record the successful exporter sweep"),
            "unexpected error: {message}"
        );
    }

    // Test-only extractor: exercised below with a matching (`Unreachable`) and a
    // non-matching (`Health`) error so both arms are covered with no exclusion,
    // while the assertion stays strong (exact `RunError::Unreachable`).
    fn is_unreachable(e: &RunError) -> bool {
        matches!(e, RunError::Unreachable(_))
    }

    #[tokio::test]
    async fn run_unreachable_store_fails_fast() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(&dead_url(), tmp.path());
        let err = run(&cfg).await.err().unwrap();
        assert!(is_unreachable(&err));
        // Cover the false arm with a cheap non-Unreachable error.
        assert!(!is_unreachable(&RunError::Health(std::io::Error::other(
            "not unreachable"
        ))));
        assert!(!cfg.health_file.exists());
    }

    #[tokio::test]
    async fn run_hard_upload_failure_skips_health() {
        crate::install_warn_logging();
        let tmp = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![
            http_response(200, b"ok"),  // healthz
            http_response(500, b"err"), // batch fails
            http_response(500, b"err"), // solo retry also fails
        ]);
        let cfg = test_config(&url, tmp.path());
        let summary = run(&cfg).await.unwrap();
        assert_eq!(summary.discovered, 1);
        assert_eq!(summary.failed, 1);
        // A run with a hard failure must NOT advance the health sidecar.
        assert!(!cfg.health_file.exists());
    }

    #[tokio::test]
    async fn upload_pending_multi_item_batch_success_marks_all() {
        // A sender that ACCEPTS a multi-item batch exercises the batch-level
        // Ok(results) branch (tally + per-item state.mark), which the >1-fails
        // MockSender never reaches.
        struct AcceptAll;
        #[async_trait]
        impl BatchSender for AcceptAll {
            async fn send_batch(
                &self,
                batch: &[SessionEnvelope],
            ) -> Result<Vec<Outcome>, UploadError> {
                Ok(batch
                    .iter()
                    .map(|e| Outcome::Accepted {
                        session_id: e.session_id.clone(),
                    })
                    .collect())
            }
        }
        let items = pending(&[("a", 10), ("b", 10), ("c", 10)]);
        let mut state = temp_state();
        let mut summary = RunSummary::default();
        upload_pending(&AcceptAll, 50, &items, &mut state, &mut summary).await;
        assert_eq!(summary.accepted, 3);
        assert_eq!(summary.uploaded, 3);
        assert_eq!(summary.failed, 0);
        assert!(state.is_current(&PathBuf::from("/state/a"), "fp-a"));
        assert!(state.is_current(&PathBuf::from("/state/c"), "fp-c"));
    }

    #[test]
    fn discover_all_propagates_source_errors() {
        // A root that is a regular file makes the corresponding source scan fail;
        // discover_all propagates it (the `?` on each source scan).
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a-file");
        std::fs::write(&file, b"x").unwrap();
        let ok_empty = tmp.path().join("nope");

        // Claude scan fails.
        let cfg = Config {
            claude_root: file.clone(),
            codex_root: ok_empty.clone(),
            cursor_db: None,
            ..test_config("http://unused", tmp.path())
        };
        assert!(discover_all(&cfg).is_err());

        // Codex scan fails (claude root empty/absent is fine).
        let cfg = Config {
            claude_root: ok_empty.clone(),
            codex_root: file.clone(),
            cursor_db: None,
            ..test_config("http://unused", tmp.path())
        };
        assert!(discover_all(&cfg).is_err());

        // Cursor scan fails (claude + codex fine, cursor_db points at a bad DB).
        let cfg = Config {
            claude_root: ok_empty.clone(),
            codex_root: ok_empty,
            cursor_db: Some(tmp.path().join("missing.vscdb")),
            ..test_config("http://unused", tmp.path())
        };
        assert!(discover_all(&cfg).is_err());
    }

    // --- whole-DB Cursor gate --------------------------------------------------

    const CLAUDE_ID: &str = "11111111-2222-3333-4444-555555555555";

    /// A Cursor DB with one inline thread `c1`, last modified a minute ago so
    /// the racy-mtime guard does not hold the gate back.
    fn settled_cursor_db(path: &Path) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(
            r#"CREATE TABLE cursorDiskKV (key TEXT UNIQUE, value BLOB);
               INSERT INTO cursorDiskKV VALUES ('composerData:c1',
                 '{"composerId":"c1","createdAt":1,"conversation":[{"text":"x"}]}');"#,
        )
        .unwrap();
        drop(conn);
        set_mtime(path, SystemTime::now() - Duration::from_secs(60));
    }

    fn set_mtime(path: &Path, when: SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(when)
            .unwrap();
    }

    fn accepted(ids: &[&str]) -> Vec<u8> {
        let results: Vec<String> = ids
            .iter()
            .map(|id| format!(r#"{{"status":"accepted","session_id":"{id}"}}"#))
            .collect();
        http_response(
            200,
            format!(r#"{{"results":[{}]}}"#, results.join(",")).as_bytes(),
        )
    }

    fn gate_entry(cfg: &Config) -> Option<String> {
        State::load(&cfg.state_file, &cfg.stamp_digest())
            .get(cfg.cursor_db.as_deref().unwrap())
            .map(str::to_string)
    }

    #[tokio::test]
    async fn an_unchanged_cursor_db_is_not_opened_again() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("state.vscdb");
        settled_cursor_db(&db);
        let url = spawn_server(vec![
            http_response(200, b"ok"),
            accepted(&[CLAUDE_ID, "c1"]),
            http_response(200, b"ok"),
            http_response(200, b"ok"),
        ]);
        let cfg = Config {
            cursor_db: Some(db.clone()),
            ..test_config(&url, tmp.path())
        };

        // First run: a gate miss scans, sends both, and records the gate with
        // the number of Cursor threads it scanned.
        let first = run(&cfg).await.unwrap();
        assert_eq!((first.discovered, first.accepted), (2, 2));
        let entry = gate_entry(&cfg).expect("gate recorded");
        assert!(entry.ends_with(":all#1"), "{entry}");

        // Replace the DB with garbage of the same size and mtime. Opening it
        // would fail the run, so a clean run proves it was never opened.
        let mtime = std::fs::metadata(&db).unwrap().modified().unwrap();
        let len = std::fs::metadata(&db).unwrap().len() as usize;
        std::fs::write(&db, vec![0u8; len]).unwrap();
        set_mtime(&db, mtime);
        let second = run(&cfg).await.unwrap();
        assert_eq!(second.discovered, 2);
        assert_eq!(second.skipped_unchanged, 2);
        assert_eq!(second.uploaded, 0);

        // Any change to the file's identity misses the gate and reads it again
        // (and here fails, the DB being garbage).
        set_mtime(&db, mtime - Duration::from_secs(5));
        assert!(run(&cfg).await.is_err());
    }

    #[tokio::test]
    async fn the_gate_counts_only_threads_the_full_reader_would_find() {
        // A composer row only SQLite's JSON reader accepts scans as a candidate
        // but loads as nothing. It is not discovered on the miss, and a later
        // hit must not report it either.
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("state.vscdb");
        settled_cursor_db(&db);
        let mtime = std::fs::metadata(&db).unwrap().modified().unwrap();
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            r#"INSERT INTO cursorDiskKV VALUES ('composerData:inf',
                 '{"composerId":"inf","createdAt":2,"x":1e999,"conversation":[1]}');"#,
        )
        .unwrap();
        drop(conn);
        set_mtime(&db, mtime);
        let url = spawn_server(vec![
            http_response(200, b"ok"),
            accepted(&[CLAUDE_ID, "c1"]),
            http_response(200, b"ok"),
        ]);
        let cfg = Config {
            cursor_db: Some(db),
            ..test_config(&url, tmp.path())
        };
        let miss = run(&cfg).await.unwrap();
        assert_eq!(miss.discovered, 2);
        assert!(gate_entry(&cfg).unwrap().ends_with("#1"));
        let hit = run(&cfg).await.unwrap();
        assert_eq!((hit.discovered, hit.skipped_unchanged), (2, 2));
    }

    #[tokio::test]
    async fn a_cursor_thread_left_for_the_next_sweep_keeps_the_gate_open() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("state.vscdb");
        settled_cursor_db(&db);
        // The store confirms the Claude transcript and says nothing about c1.
        let url = spawn_server(vec![http_response(200, b"ok"), accepted(&[CLAUDE_ID])]);
        let cfg = Config {
            cursor_db: Some(db),
            ..test_config(&url, tmp.path())
        };
        let summary = run(&cfg).await.unwrap();
        assert_eq!(summary.unconfirmed, 1);
        assert_eq!(gate_entry(&cfg), None);
    }

    #[tokio::test]
    async fn a_cursor_db_modified_just_before_the_scan_is_not_trusted() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("state.vscdb");
        settled_cursor_db(&db);
        set_mtime(&db, SystemTime::now());
        let url = spawn_server(vec![
            http_response(200, b"ok"),
            accepted(&[CLAUDE_ID, "c1"]),
        ]);
        let cfg = Config {
            cursor_db: Some(db),
            ..test_config(&url, tmp.path())
        };
        let summary = run(&cfg).await.unwrap();
        assert_eq!(summary.accepted, 2);
        assert_eq!(gate_entry(&cfg), None);
    }

    #[test]
    fn the_gate_signature_tracks_everything_that_changes_what_a_scan_covers() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("state.vscdb");
        settled_cursor_db(&db);
        let cfg = Config {
            cursor_db: Some(db.clone()),
            ..test_config("http://unused", tmp.path())
        };
        let gate = cursor_gate(&db, &cfg).unwrap();
        let recorded = format!("{}#7", gate.signature);
        assert_eq!(gate.hit(Some(&recorded)), Some(7));
        assert_eq!(gate.hit(None), None);
        assert_eq!(gate.hit(Some("no-separator")), None);
        assert_eq!(gate.hit(Some(&format!("{}#x", gate.signature))), None);
        for other in [
            Config {
                cursor_limit: Some(5),
                ..cfg.clone()
            },
            Config {
                max_envelope_bytes: 1,
                ..cfg.clone()
            },
        ] {
            assert_eq!(cursor_gate(&db, &other).unwrap().hit(Some(&recorded)), None);
        }
        assert!(gate.settled(SystemTime::now()));
        assert!(!gate.settled(gate.mtime));
        assert!(!gate.settled(UNIX_EPOCH));
        assert!(cursor_gate(&tmp.path().join("missing"), &cfg).is_none());
    }

    #[test]
    fn run_error_messages_render() {
        assert_eq!(
            RunError::Unreachable("http://x".into()).to_string(),
            "store is not reachable at http://x (is it up? is the URL right?)"
        );
        assert_eq!(
            RunError::Health(std::io::Error::other("disk")).to_string(),
            "could not record the successful exporter sweep: disk"
        );
    }
}
