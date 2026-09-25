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

fn line(content: &str) -> String {
    format!(
        "{{\"type\":\"user\",\"timestamp\":\"2026-07-01T00:00:00Z\",\"message\":{{\"role\":\"user\",\"content\":\"{content}\"}}}}\n"
    )
}

#[test]
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

    let cfg = agentic_session_exporter::config::Config {
        store_url: String::new(),
        write_token: None,
        origin_host: "memory-test".into(),
        origin_environment: "local".into(),
        origin_deployment: None,
        claude_root: claude,
        codex_root: temp.path().join("codex-absent"),
        cursor_db: None,
        cursor_limit: None,
        state_file: temp.path().join("state.json"),
        ignore_state: false,
        health_file: temp.path().join("health"),
        health_max_age_secs: 900,
        batch_size: 50,
        max_envelope_bytes: MIB,
        tags: Vec::new(),
    };
    let spool = temp.path().join("spool");

    let baseline = CURRENT.load(Ordering::SeqCst);
    PEAK.store(baseline, Ordering::SeqCst);
    let summary = agentic_session_exporter::spool::capture_local(&cfg, &spool).unwrap();
    let peak = PEAK.load(Ordering::SeqCst) - baseline;

    assert_eq!(summary.discovered, 202);
    assert_eq!(summary.skipped_oversize, 2);
    assert_eq!(summary.stored, 200);
    // The corpus is over 100 MiB. One transcript is 200 KiB, held a few times
    // over while parsed and serialized, so 8 MiB is generous for streaming and
    // an order of magnitude below either the corpus or the oversized file.
    assert!(peak < 8 * MIB, "peak allocation {peak} bytes");
    eprintln!("peak allocation during capture: {peak} bytes");
}
