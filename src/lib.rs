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

pub mod config;
pub mod cursor;
pub mod gitmeta;
pub mod health;
pub mod parsers;
pub mod reconstitute;
pub mod sources;
pub mod state;
pub mod upload;

use std::path::PathBuf;

use session_capture::{Origin, SessionEnvelope};

use crate::config::Config;
use crate::sources::Discovered;
use crate::state::State;
use crate::upload::{tally, BatchSender, Client};

/// Aggregate outcome of a full run.
#[derive(Debug, Default, Clone, Copy)]
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
    let origin = Origin {
        host: cfg.origin_host.clone(),
        environment: cfg.origin_environment.clone(),
    };
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

/// Perform one full capture run: discover, skip unchanged, batch-upload, persist
/// state. `check_health` gates the run on a reachable store so a misconfigured
/// URL fails fast with a clear message instead of a wall of connection errors.
// Measured: the reachable/unreachable, discover, upload (success + hard-failure),
// and health-record branches are all driven by the `run_*` tests. The only cold
// spots are the `discover_all` / `record_success` `?` error arms (un-forceable IO
// after a healthy store), which `?` propagation leaves as covered regions.
pub async fn run(cfg: &Config) -> Result<RunSummary, RunError> {
    let client = Client::new(&cfg.store_url, cfg.write_token.clone());
    if !client.healthy(&cfg.store_url).await {
        return Err(RunError::Unreachable(cfg.store_url.clone()));
    }

    let discovered = discover_all(cfg)?;
    let mut state = State::load(&cfg.state_file, &cfg.stamp_digest());

    let mut summary = RunSummary {
        discovered: discovered.len(),
        ..Default::default()
    };

    let pending = partition_pending(discovered, &state, cfg.max_envelope_bytes, &mut summary);
    upload_pending(&client, cfg.batch_size, &pending, &mut state, &mut summary).await;
    if summary.failed == 0 {
        health::record_success(&cfg.health_file).map_err(RunError::Health)?;
    }

    Ok(summary)
}

/// A pending upload: (state key path, fingerprint, envelope).
type Pending = (PathBuf, String, SessionEnvelope);

/// Split discovered items into the pending-upload set, updating the summary's
/// `skipped_unchanged` and `skipped_oversize` counts. Unchanged items (matching
/// fingerprint in state) are skipped. An envelope whose serialized size exceeds
/// `max_envelope_bytes` is skipped BEFORE upload: a single pathological
/// transcript (for example a runaway multi-hundred-MB Cursor thread) must never
/// be sent, let alone abort the sweep. Skipped items are left unmarked in state
/// so a later raised cap or a shrunk transcript retries them.
fn partition_pending(
    discovered: Vec<Discovered>,
    state: &State,
    max_envelope_bytes: usize,
    summary: &mut RunSummary,
) -> Vec<Pending> {
    let mut pending = Vec::new();
    for d in discovered {
        if state.is_current(&d.path, &d.fingerprint) {
            summary.skipped_unchanged += 1;
            continue;
        }
        let size = estimated_len(&d.envelope);
        if size > max_envelope_bytes {
            summary.skipped_oversize += 1;
            tracing::warn!(
                session_id = %d.envelope.session_id,
                bytes = size,
                cap = max_envelope_bytes,
                "skipping oversize envelope (exceeds MAX_ENVELOPE_BYTES); not uploaded"
            );
            continue;
        }
        pending.push((d.path, d.fingerprint, d.envelope));
    }
    pending
}

/// Upload the pending envelopes resiliently. A batch-level hard error (413,
/// 5xx, timeout, connection closed) does NOT abort the run: the batch is retried
/// ONE ITEM AT A TIME, and any single item that still hard-fails is counted,
/// logged (session_id + reason + byte size), and skipped so the sweep continues.
///
/// State is marked ONLY for items the store actually saw (accepted, duplicate,
/// or per-item rejected: all three mean the store processed it and a re-send
/// would be wasted). Items that hard-failed are left unmarked so the next sweep
/// retries them. Generic over `BatchSender` so the fallback logic is unit-tested
/// with a mock, no network required.
async fn upload_pending<S: BatchSender + ?Sized>(
    sender: &S,
    batch_size: usize,
    pending: &[Pending],
    state: &mut State,
    summary: &mut RunSummary,
) {
    // Batches are bounded by BOTH a count (`batch_size`) and a cumulative byte
    // budget, since verbatim envelopes vary wildly in size.
    let plan = plan_batches(pending.len(), batch_size, |i| estimated_len(&pending[i].2));
    let mut cursor = 0usize;
    for count in plan {
        let slice = &pending[cursor..cursor + count];
        cursor += count;

        let batch: Vec<SessionEnvelope> = slice.iter().map(|(_, _, e)| e.clone()).collect();
        match sender.send_batch(&batch).await {
            Ok(results) => {
                let t = tally(&results);
                summary.uploaded += results.len();
                summary.accepted += t.accepted;
                summary.duplicate += t.duplicate;
                summary.rejected += t.rejected;
                // Every item the store returned a result for was processed; mark it.
                for (path, fp, _) in slice {
                    state.mark(path, fp.clone());
                }
            }
            Err(e) => {
                // Batch-level hard error: fall back to per-item so one bad
                // envelope cannot take down its whole batch. `slice.len()` is
                // computed here (covered) and handed to the excluded log helper.
                warn_batch_fallback(&e, slice.len());
                for (path, fp, env) in slice {
                    upload_one_resilient(sender, path, fp, env, state, summary).await;
                }
            }
        }
        if let Err(e) = state.save() {
            tracing::warn!(error = %e, "could not persist exporter state (non-fatal)");
        }
    }
}

/// Upload a single envelope, tolerating its individual failure. On success (any
/// per-item outcome) the state is marked. On a hard error the item is counted as
/// `failed`, logged with its byte size, and left unmarked to retry next sweep.
async fn upload_one_resilient<S: BatchSender + ?Sized>(
    sender: &S,
    path: &std::path::Path,
    fp: &str,
    env: &SessionEnvelope,
    state: &mut State,
    summary: &mut RunSummary,
) {
    let one = std::slice::from_ref(env);
    match sender.send_batch(one).await {
        Ok(results) => {
            let t = tally(&results);
            summary.uploaded += results.len();
            summary.accepted += t.accepted;
            summary.duplicate += t.duplicate;
            summary.rejected += t.rejected;
            state.mark(path, fp.to_string());
        }
        Err(e) => {
            summary.failed += 1;
            // `estimated_len(env)` is computed here (covered) and handed to the
            // excluded log helper.
            warn_solo_failure(env, estimated_len(env), &e);
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

/// Estimated serialized size of an envelope, used only for batch planning.
fn estimated_len(env: &SessionEnvelope) -> usize {
    serde_json::to_vec(env).map(|v| v.len()).unwrap_or(4096)
}

/// Plan batch sizes over `count` items given a max per-batch item count and a
/// per-item size (via `size_of`). Each returned number is how many consecutive
/// items form the next batch. A single item larger than the byte budget still
/// forms its own batch (never zero-sized). Pure + count-based so it is testable
/// without real envelopes.
fn plan_batches(count: usize, max_items: usize, size_of: impl Fn(usize) -> usize) -> Vec<usize> {
    let max_items = max_items.max(1);
    let mut plan = Vec::new();
    let mut i = 0usize;
    while i < count {
        let mut n = 0usize;
        let mut bytes = 0usize;
        while i + n < count && n < max_items {
            let item = size_of(i + n);
            // Always take at least one item, even if it alone exceeds the budget.
            if n > 0 && bytes + item > BATCH_BYTE_BUDGET {
                break;
            }
            bytes += item;
            n += 1;
        }
        plan.push(n);
        i += n;
    }
    plan
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

    #[test]
    fn plan_batches_splits_by_count_when_items_small() {
        // 10 tiny items, max 4 per batch -> 4 + 4 + 2.
        let plan = plan_batches(10, 4, |_| 100);
        assert_eq!(plan, vec![4, 4, 2]);
        assert_eq!(plan.iter().sum::<usize>(), 10);
    }

    #[test]
    fn plan_batches_splits_by_bytes_when_items_large() {
        // Each item is 60% of the byte budget, so only one fits per batch even
        // though max_items is 50: split is driven by bytes, not count.
        let big = BATCH_BYTE_BUDGET * 6 / 10;
        let plan = plan_batches(4, 50, |_| big);
        assert_eq!(plan, vec![1, 1, 1, 1]);
        assert_eq!(plan.iter().sum::<usize>(), 4);
    }

    #[test]
    fn plan_batches_oversize_single_item_is_its_own_batch() {
        // One item larger than the whole budget still forms a batch of 1 (never
        // stalls at zero).
        let plan = plan_batches(3, 50, |i| if i == 0 { BATCH_BYTE_BUDGET * 2 } else { 10 });
        assert_eq!(plan[0], 1);
        assert_eq!(plan.iter().sum::<usize>(), 3);
    }

    #[test]
    fn plan_batches_empty_is_empty() {
        assert!(plan_batches(0, 50, |_| 1).is_empty());
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
            origin: Origin {
                host: "h".into(),
                environment: "test".into(),
            },
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

    #[test]
    fn partition_pending_skips_oversize_before_upload() {
        use crate::sources::Discovered;

        let cap = 1000usize;
        let small = envelope("small", 10);
        let huge = envelope("huge", 5000); // serialized raw well over the cap
        assert!(estimated_len(&small) <= cap);
        assert!(estimated_len(&huge) > cap);

        let discovered = vec![
            Discovered {
                path: PathBuf::from("/state/small"),
                fingerprint: "fp-small".into(),
                envelope: small,
            },
            Discovered {
                path: PathBuf::from("/state/huge"),
                fingerprint: "fp-huge".into(),
                envelope: huge,
            },
        ];

        let state = temp_state();
        let mut summary = RunSummary::default();
        let pending = partition_pending(discovered, &state, cap, &mut summary);

        // The oversize one is skipped pre-upload; only the small one is pending.
        assert_eq!(summary.skipped_oversize, 1);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].2.session_id, "small");
    }

    #[test]
    fn partition_pending_skips_unchanged() {
        use crate::sources::Discovered;
        let mut state = temp_state();
        state.mark(&PathBuf::from("/state/known"), "fp-known".to_string());

        let discovered = vec![Discovered {
            path: PathBuf::from("/state/known"),
            fingerprint: "fp-known".into(),
            envelope: envelope("known", 10),
        }];
        let mut summary = RunSummary::default();
        let pending = partition_pending(discovered, &state, 1_000_000, &mut summary);
        assert_eq!(summary.skipped_unchanged, 1);
        assert!(pending.is_empty());
    }

    #[test]
    fn partition_pending_warns_and_skips_oversize() {
        // With a WARN subscriber active, the oversize warn!'s field expressions
        // are evaluated (they are skipped when logging is disabled).
        crate::install_warn_logging();
        use crate::sources::Discovered;
        let discovered = vec![Discovered {
            path: PathBuf::from("/state/huge"),
            fingerprint: "fp".into(),
            envelope: envelope("huge", 5000),
        }];
        let state = temp_state();
        let mut summary = RunSummary::default();
        let pending = partition_pending(discovered, &state, 100, &mut summary);
        assert_eq!(summary.skipped_oversize, 1);
        assert!(pending.is_empty());
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
