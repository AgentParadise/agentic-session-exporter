//! Local capture must hold one transcript at a time, never the corpus, and
//! must not read an oversized transcript into memory at all. Measured with a
//! counting allocator rather than asserted from the code's shape. This binary
//! holds exactly one test so no other test's allocations share the counters.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            PEAK.fetch_max(now, Ordering::SeqCst);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::SeqCst);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const MIB: usize = 1024 * 1024;

fn config(
    root: &std::path::Path,
    claude: std::path::PathBuf,
    codex: std::path::PathBuf,
    cursor: Option<std::path::PathBuf>,
) -> agentic_session_exporter::config::Config {
    agentic_session_exporter::config::Config {
        store_url: String::new(),
        write_token: None,
        origin_host: "memory-test".into(),
        origin_environment: "local".into(),
        origin_deployment: None,
        claude_root: claude,
        codex_root: codex,
        cursor_db: cursor,
        cursor_limit: None,
        state_file: root.join("state.json"),
        ignore_state: false,
        health_file: root.join("health"),
        health_max_age_secs: 900,
        batch_size: 50,
        max_envelope_bytes: MIB,
        tags: Vec::new(),
    }
}

/// Peak bytes allocated above the level at entry while `body` runs.
fn peak_of<T>(body: impl FnOnce() -> T) -> (T, usize) {
    let baseline = CURRENT.load(Ordering::SeqCst);
    PEAK.store(baseline, Ordering::SeqCst);
    let value = body();
    (value, PEAK.load(Ordering::SeqCst) - baseline)
}

/// Count what a streaming discovery sweep hands over, keeping nothing.
fn visit_counts(cfg: &agentic_session_exporter::config::Config) -> (usize, usize, u64) {
    use agentic_session_exporter::sources::Found;
    let (mut found, mut oversize, mut overflow) = (0, 0, 0);
    agentic_session_exporter::visit_all(cfg, MIB as u64, &mut |item| {
        match item {
            Found::Transcript(_) => found += 1,
            Found::Oversize(_) => oversize += 1,
            Found::Overflow(count) => overflow += count,
        }
        Ok::<(), ()>(())
    })
    .unwrap();
    (found, oversize, overflow)
}

fn line(content: &str) -> String {
    format!(
        "{{\"type\":\"user\",\"timestamp\":\"2026-07-01T00:00:00Z\",\"message\":{{\"role\":\"user\",\"content\":\"{content}\"}}}}\n"
    )
}

/// The directory walk must not buffer listings: many small rollout files cost
/// no more memory than a few.
fn many_small_rollout_files_are_walked_without_buffering(temp: &std::path::Path) {
    let codex = temp.join("codex/2026/07/01");
    std::fs::create_dir_all(&codex).unwrap();
    for index in 0..20_000 {
        std::fs::write(
            codex.join(format!(
                "rollout-2026-07-01T00-00-00-{index:08}-padding-to-a-realistic-name.jsonl"
            )),
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"s{index}\"}}}}\n"),
        )
        .unwrap();
    }
    let cfg = config(temp, temp.join("claude-absent"), temp.join("codex"), None);
    let (counts, peak) = peak_of(|| visit_counts(&cfg));
    assert_eq!(counts, (20_000, 0, 0));
    // Collecting and sorting 20,000 paths alone would take several MiB.
    assert!(
        peak < 512 * 1024,
        "peak allocation {peak} bytes over 20k files"
    );
    eprintln!("peak allocation walking 20k rollout files: {peak} bytes");
}

/// Cursor threads are read one at a time: many medium threads cost one
/// thread's memory, and the count and byte bounds report what they leave out.
fn many_medium_cursor_threads_are_streamed(temp: &std::path::Path) {
    let db = temp.join("state.vscdb");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
    )
    .unwrap();
    let text = "y".repeat(200 * 1024);
    for index in 0..150i64 {
        let composer = serde_json::json!({
            "composerId": format!("c{index}"),
            "createdAt": 1_000 + index,
            "conversation": [{"bubbleId": "b", "type": 1, "text": text}],
        });
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            rusqlite::params![format!("composerData:c{index}"), composer.to_string()],
        )
        .unwrap();
    }
    // One thread whose bubble rows, not its composer row, exceed the bound.
    let header = serde_json::json!({
        "composerId": "big", "createdAt": 5_000,
        "fullConversationHeadersOnly": [{"bubbleId": "a"}, {"bubbleId": "b"}],
    });
    conn.execute(
        "INSERT INTO cursorDiskKV (key, value) VALUES ('composerData:big', ?1)",
        [header.to_string()],
    )
    .unwrap();
    for bubble in ["a", "b"] {
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            rusqlite::params![
                format!("bubbleId:big:{bubble}"),
                serde_json::json!({"type": 1, "text": "z".repeat(600 * 1024)}).to_string()
            ],
        )
        .unwrap();
    }
    drop(conn);
    let cfg = config(
        temp,
        temp.join("claude-absent"),
        temp.join("codex-absent"),
        Some(db),
    );
    let (counts, peak) = peak_of(|| visit_counts(&cfg));
    assert_eq!(counts, (150, 1, 0));
    // The threads total 30 MiB; materializing them is what this rules out.
    assert!(
        peak < 4 * MIB,
        "peak allocation {peak} bytes over 150 threads"
    );
    eprintln!("peak allocation streaming 150 Cursor threads: {peak} bytes");
}

#[test]
fn discovery_and_local_capture_stay_under_a_fixed_memory_budget() {
    let walk = tempfile::tempdir().unwrap();
    many_small_rollout_files_are_walked_without_buffering(walk.path());
    let cursor = tempfile::tempdir().unwrap();
    many_medium_cursor_threads_are_streamed(cursor.path());
    local_capture_streams_sources_under_a_fixed_memory_budget();
}

fn local_capture_streams_sources_under_a_fixed_memory_budget() {
    let temp = tempfile::tempdir().unwrap();
    let claude = temp.path().join("claude");
    std::fs::create_dir_all(&claude).unwrap();

    // One transcript far over the bound. Sparse, so the test stays cheap;
    // reading it would still allocate its full length.
    std::fs::File::create(claude.join("huge.jsonl"))
        .unwrap()
        .set_len(64 * MIB as u64)
        .unwrap();
    // One transcript exactly at the bound whose envelope, once its quotes are
    // escaped again, is over it: rejected while streaming to disk.
    let quoted = line(&"\\\"".repeat(MIB / 4));
    let padded = format!("{quoted}{}", "\n".repeat(MIB - quoted.len()));
    assert_eq!(padded.len(), MIB);
    std::fs::write(claude.join("escaped.jsonl"), padded).unwrap();
    // Many ordinary transcripts: 200 x 200 KiB is 40 MiB of corpus.
    let body = line(&"x".repeat(200 * 1024));
    for index in 0..200 {
        std::fs::write(claude.join(format!("session-{index:03}.jsonl")), &body).unwrap();
    }

    let cfg = config(temp.path(), claude, temp.path().join("codex-absent"), None);
    let spool = temp.path().join("spool");

    let (summary, peak) =
        peak_of(|| agentic_session_exporter::spool::capture_local(&cfg, &spool).unwrap());

    assert_eq!(summary.discovered, 202);
    assert_eq!(summary.skipped_oversize, 2);
    assert_eq!(summary.stored, 200);
    // The corpus is over 100 MiB. One transcript is 200 KiB, held a few times
    // over while parsed and serialized, so 8 MiB is generous for streaming and
    // an order of magnitude below either the corpus or the oversized file.
    assert!(peak < 8 * MIB, "peak allocation {peak} bytes");
    eprintln!("peak allocation during capture: {peak} bytes");
}
