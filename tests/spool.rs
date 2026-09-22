//! Real filesystem and SQLite durability tests for the local capture boundary.

use agentic_session_exporter::parsers::read_claude;
use agentic_session_exporter::spool::{LocalSpool, SpoolError};
use session_capture::{Origin, SessionEnvelope};
use std::fs;
use std::path::Path;

fn envelope(id: &str, body: &str) -> SessionEnvelope {
    read_claude(
        format!("{{\"type\":\"user\",\"timestamp\":\"2026-07-01T00:00:00Z\",\"message\":{{\"role\":\"user\",\"content\":\"{body}\"}}}}\n").as_bytes(),
        id,
        Origin::new("test", "local"),
    ).unwrap()
}

#[test]
fn exact_envelope_and_all_revisions_survive_restart() {
    let temp = tempfile::tempdir().unwrap();
    let mut spool = LocalSpool::open(temp.path()).unwrap();
    let first = envelope("native/id", "first");
    let expected = serde_json::to_vec(&first).unwrap();
    let (saved, inserted) = spool.store(&first).unwrap();
    assert!(inserted);
    assert!(!spool.store(&first).unwrap().1);
    let second = spool.store(&envelope("native/id", "second")).unwrap().0;
    assert_ne!(saved.archive_sha256, second.archive_sha256);
    drop(spool);
    let restarted = LocalSpool::open_readonly(temp.path()).unwrap();
    assert_eq!(restarted.read(saved.sequence, 100_000).unwrap(), expected);
    assert_eq!(restarted.page(0, None, 500).unwrap().entries.len(), 2);
}

#[test]
fn pagination_freezes_watermark_while_new_revisions_arrive() {
    let temp = tempfile::tempdir().unwrap();
    let mut spool = LocalSpool::open(temp.path()).unwrap();
    for index in 0..507 {
        spool
            .store(&envelope(&format!("native-{index}"), "body"))
            .unwrap();
    }
    let first = spool.page(0, None, 500).unwrap();
    spool.store(&envelope("late", "body")).unwrap();
    let last = spool
        .page(first.next_after.unwrap(), Some(first.watermark), 500)
        .unwrap();
    assert_eq!(last.entries.len(), 7);
    assert!(last.next_after.is_none());
    assert_eq!(last.watermark, first.watermark);
    assert!(matches!(spool.page(0, None, 501), Err(SpoolError::Bounds)));
    assert!(matches!(spool.page(0, None, 0), Err(SpoolError::Bounds)));
    assert!(matches!(
        spool.page(u64::MAX, None, 1),
        Err(SpoolError::Bounds)
    ));
    assert!(matches!(
        spool.page(0, Some(u64::MAX), 1),
        Err(SpoolError::Bounds)
    ));
}

#[test]
fn corrupted_missing_and_oversized_objects_never_read_as_original() {
    let temp = tempfile::tempdir().unwrap();
    let mut spool = LocalSpool::open(temp.path()).unwrap();
    let original = envelope("native", "body");
    let entry = spool.store(&original).unwrap().0;
    let path = temp.path().join("objects").join(&entry.archive_sha256);
    assert!(matches!(
        spool.read(entry.sequence, 1),
        Err(SpoolError::Integrity)
    ));
    fs::write(&path, vec![b'x'; entry.byte_count as usize]).unwrap();
    assert!(matches!(
        spool.read(entry.sequence, 100_000),
        Err(SpoolError::Integrity)
    ));
    assert!(matches!(spool.store(&original), Err(SpoolError::Integrity)));
    fs::write(&path, b"short").unwrap();
    assert!(matches!(
        spool.read(entry.sequence, 100_000),
        Err(SpoolError::Integrity)
    ));
    fs::remove_file(&path).unwrap();
    assert!(matches!(
        spool.read(entry.sequence, 100_000),
        Err(SpoolError::Io(_))
    ));
    assert!(matches!(
        spool.read(999, 100_000),
        Err(SpoolError::NotFound)
    ));
}

#[test]
fn uncommitted_orphan_object_is_not_visible_and_retry_can_publish_it() {
    let temp = tempfile::tempdir().unwrap();
    let spool = LocalSpool::open(temp.path()).unwrap();
    drop(spool);
    // Simulate death after durable object installation and before index commit.
    let original = envelope("native", "body");
    let bytes = serde_json::to_vec(&original).unwrap();
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(temp.path().join("objects").join(digest), &bytes).unwrap();
    let mut restarted = LocalSpool::open(temp.path()).unwrap();
    assert!(restarted.page(0, None, 500).unwrap().entries.is_empty());
    let entry = restarted.store(&original).unwrap().0;
    assert_eq!(restarted.read(entry.sequence, 100_000).unwrap(), bytes);
}

#[test]
fn schema_mismatch_and_absent_readonly_spools_fail_without_creation() {
    let temp = tempfile::tempdir().unwrap();
    let absent = temp.path().join("absent");
    assert!(LocalSpool::open_readonly(&absent).is_err());
    assert!(!absent.exists());
    let spool = LocalSpool::open(temp.path()).unwrap();
    drop(spool);
    let db = rusqlite::Connection::open(temp.path().join("inventory.sqlite3")).unwrap();
    db.pragma_update(None, "user_version", 99).unwrap();
    assert!(matches!(
        LocalSpool::open(temp.path()),
        Err(SpoolError::Schema)
    ));
    assert!(matches!(
        LocalSpool::open_readonly(temp.path()),
        Err(SpoolError::Schema)
    ));
    let blocked = temp.path().join("file");
    fs::write(&blocked, b"not a directory").unwrap();
    assert!(LocalSpool::open(Path::new(&blocked)).is_err());
}
