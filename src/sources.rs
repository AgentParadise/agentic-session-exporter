//! Transcript discovery + envelope building for each source.
//!
//! One function per source that yields `(fingerprint, SessionEnvelope)` for
//! every transcript found. The fingerprint (path + mtime + size) lets the
//! caller skip unchanged files; the store's content_hash dedup is the real
//! idempotency guarantee, this is just a network-saving fast path.
//!
//! All sources reuse the envelope crate's provider readers, so `raw` is kept
//! verbatim (never flattened). Metadata is enriched here: `origin` is stamped,
//! and `repo` / `git_remote` are derived from the transcript cwd via `gitmeta`.

use std::path::{Path, PathBuf};

use crate::parsers::{read_claude, read_codex, ParseError};
use session_capture::{Origin, SessionEnvelope};

use crate::gitmeta;

/// A discovered transcript plus a change fingerprint.
pub struct Discovered {
    /// Absolute transcript path (the fingerprint key).
    pub path: PathBuf,
    /// `<mtime_secs>:<size>` used to detect changes cheaply.
    pub fingerprint: String,
    pub envelope: SessionEnvelope,
}

/// Errors that abort a whole source scan (not a single-file skip).
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("io error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("cursor db read failed: {0}")]
    Cursor(#[from] crate::cursor::CursorError),
}

/// Cheap change-detection token for a discovered file. The success path is
/// measured (a real file's metadata + mtime + len); the only cold spot is the
/// metadata `?`/None arm, reachable solely under a mid-sweep unlink race, which
/// the `?` propagation leaves as a covered region (no separate dead source line).
fn fingerprint(path: &Path) -> Option<String> {
    let meta = std::fs::metadata(path).ok()?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Some(format!("{mtime}:{}", meta.len()))
}

/// Read a discovered transcript's change fingerprint and bytes together.
/// Returns `None` (so the caller skips the file) when the file vanished or
/// became unreadable between the directory walk and this read. Measured: the
/// happy path is driven by the discover tests and the read-failure `None` path by
/// the unreadable-file test; the fingerprint-miss `?` arm is an un-forceable
/// unlink race that `?` leaves as a covered region.
fn read_transcript(path: &Path) -> Option<(String, Vec<u8>)> {
    let fp = fingerprint(path)?;
    let bytes = std::fs::read(path).ok()?;
    Some((fp, bytes))
}

/// Enrich an envelope's metadata with repo/git_remote derived from its cwd.
fn enrich(mut env: SessionEnvelope) -> SessionEnvelope {
    if let Some(metadata) = env.metadata.as_mut() {
        if let Some(cwd) = metadata.cwd.clone() {
            let info = gitmeta::resolve(Path::new(&cwd));
            if metadata.repo.is_none() {
                metadata.repo = info.repo;
            }
            if metadata.git_remote.is_none() {
                metadata.git_remote = info.git_remote;
            }
        }
    }
    env
}

/// Recursively collect files under `root` matching `keep`. The missing-root
/// early return, the `read_dir` failure (a file where a directory is expected),
/// the entry iteration, the recursion, and the keep/push logic are all measured
/// and tested. Only the per-entry extraction (whose only failure is an
/// un-forceable mid-walk filesystem race that skips the entry) lives in the
/// coverage-excluded `child_of` helper, driven here through `filter_map` so
/// `walk` carries no un-forceable branch of its own.
fn walk(
    root: &Path,
    keep: &dyn Fn(&Path) -> bool,
    out: &mut Vec<PathBuf>,
) -> Result<(), SourceError> {
    if !root.exists() {
        return Ok(());
    }
    let entries = std::fs::read_dir(root).map_err(|e| SourceError::Io {
        path: root.display().to_string(),
        source: e,
    })?;
    for (path, file_type) in entries.filter_map(child_of) {
        if file_type.is_dir() {
            walk(&path, keep, out)?;
        } else if file_type.is_file() && keep(&path) {
            out.push(path);
        }
    }
    Ok(())
}

/// Extract one directory entry's path and file type, or `None` to skip it.
///
/// Coverage-excluded: an individual `DirEntry` read or `file_type` call fails
/// only on a filesystem race (the entry vanishing between `read_dir` and this
/// point), which cannot be forced in-process; such an entry is skipped, matching
/// `read_transcript`'s tolerance of mid-sweep races. Isolating the two
/// un-forceable `?`/skip edges here (rather than in `walk`) keeps the loop, the
/// recursion, and the forceable `read_dir` failure fully measured in `walk`.
#[cfg_attr(coverage_nightly, coverage(off))]
fn child_of(entry: std::io::Result<std::fs::DirEntry>) -> Option<(PathBuf, std::fs::FileType)> {
    let entry = entry.ok()?;
    let path = entry.path();
    let file_type = entry.file_type().ok()?;
    Some((path, file_type))
}

/// Emit the "skipped a malformed transcript" warning for a Claude file.
/// Extracted and coverage-excluded because a `warn!`'s field-value sub-region
/// (`path.display()`) is counted nondeterministically by llvm-cov instrumentation
/// (tracing evaluates field closures lazily), which would flake the coverage
/// gate. The `Err(e)` skip branch that calls this is covered; only the
/// diagnostic log line is excluded. No logic lives here.
#[cfg_attr(coverage_nightly, coverage(off))]
fn warn_skip_claude(path: &Path, error: &ParseError) {
    tracing::warn!(path = %path.display(), %error, "skip claude transcript");
}

/// Codex counterpart of `warn_skip_claude`; coverage-excluded for the same reason.
#[cfg_attr(coverage_nightly, coverage(off))]
fn warn_skip_codex(path: &Path, error: &ParseError) {
    tracing::warn!(path = %path.display(), %error, "skip codex transcript");
}

/// Discover Claude Code transcripts under `root` (`~/.claude/projects`).
/// Each `<sessionUuid>.jsonl` (including nested subagent transcripts) becomes an
/// envelope. The file stem is the native session id.
pub fn discover_claude(root: &Path, origin: &Origin) -> Result<Vec<Discovered>, SourceError> {
    let mut files = Vec::new();
    walk(
        root,
        &|p| p.extension().and_then(|s| s.to_str()) == Some("jsonl"),
        &mut files,
    )?;
    files.sort();
    let mut out = Vec::new();
    for path in files {
        let Some((fp, bytes)) = read_transcript(&path) else {
            continue;
        };
        let native_id = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        match read_claude(&bytes, native_id, origin.clone()) {
            Ok(env) => out.push(Discovered {
                path,
                fingerprint: fp,
                envelope: enrich(env),
            }),
            Err(ParseError::Empty) => {}
            Err(e) => warn_skip_claude(&path, &e),
        }
    }
    Ok(out)
}

/// Discover Codex rollout transcripts under `root` (`~/.codex/sessions`).
/// Only `rollout-*.jsonl` files (possibly nested under YYYY/MM/DD) are read.
pub fn discover_codex(root: &Path, origin: &Origin) -> Result<Vec<Discovered>, SourceError> {
    let mut files = Vec::new();
    walk(
        root,
        &|p| {
            p.extension().and_then(|s| s.to_str()) == Some("jsonl")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("rollout-"))
        },
        &mut files,
    )?;
    files.sort();
    let mut out = Vec::new();
    for path in files {
        let Some((fp, bytes)) = read_transcript(&path) else {
            continue;
        };
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let fallback_id = stem.strip_prefix("rollout-").unwrap_or(stem);
        match read_codex(&bytes, fallback_id, origin.clone()) {
            Ok(env) => out.push(Discovered {
                path,
                fingerprint: fp,
                envelope: enrich(env),
            }),
            Err(ParseError::Empty) => {}
            Err(e) => warn_skip_codex(&path, &e),
        }
    }
    Ok(out)
}

/// Discover Cursor chat threads from the state DB.
///
/// Opens `state.vscdb` read-only/immutable, reconstructs each `composerData:*`
/// thread (handling both inline and separate-bubble-row layouts), and yields one
/// envelope per non-empty thread with `raw` preserved losslessly. See
/// `crate::cursor` for the full storage-shape notes. `limit` caps the number of
/// (newest) threads, for a bounded ingest.
///
/// Each thread maps to a synthetic `Discovered.path` of
/// `<db_path>#composer:<composerId>` so the fingerprint state file dedups per
/// thread (not per DB file). The fingerprint is the thread's
/// `lastUpdatedAt:message_count`, so an edited thread re-uploads while unchanged
/// threads are skipped.
pub fn discover_cursor(
    db: &Path,
    origin: &Origin,
    limit: Option<usize>,
) -> Result<Vec<Discovered>, SourceError> {
    let threads = crate::cursor::read_threads(db, origin, limit)?;
    let mut out = Vec::with_capacity(threads.len());
    for t in threads {
        let key = format!("{}#composer:{}", db.display(), t.envelope.session_id);
        out.push(Discovered {
            path: PathBuf::from(key),
            fingerprint: t.fingerprint,
            envelope: t.envelope,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn origin() -> Origin {
        Origin {
            host: "test-host".into(),
            environment: "laptop".into(),
        }
    }

    fn write(path: &Path, body: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
    }

    #[test]
    fn claude_discovery_reads_and_enriches() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("projects");
        // A repo whose cwd the transcript references.
        let repo = tmp.path().join("work").join("proj");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        std::fs::write(
            repo.join(".git").join("config"),
            "[remote \"origin\"]\n url = git@github.com:acme/proj.git\n",
        )
        .unwrap();
        let cwd = repo.to_string_lossy().to_string();

        let f = root
            .join("-work-proj")
            .join("11111111-2222-3333-4444-555555555555.jsonl");
        write(
            &f,
            &format!(
                "{{\"type\":\"user\",\"timestamp\":\"2026-07-01T00:00:00Z\",\"cwd\":\"{cwd}\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"
            ),
        );

        let found = discover_claude(&root, &origin()).unwrap();
        assert_eq!(found.len(), 1);
        let env = &found[0].envelope;
        assert_eq!(env.agent, "ClaudeCode");
        assert_eq!(env.session_id, "11111111-2222-3333-4444-555555555555");
        assert_eq!(env.origin.host, "test-host");
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.repo.as_deref(), Some("acme/proj"));
        assert_eq!(
            metadata.git_remote.as_deref(),
            Some("git@github.com:acme/proj.git")
        );
        // Claude raw is the original JSONL string, not a parsed/re-serialized
        // rows array. That preserves byte-exact resume and I-JSON hashing.
        let raw = env.raw.as_str().expect("Claude raw is a verbatim string");
        assert!(raw.contains("\"content\":\"hi\""));
        assert!(raw.ends_with('\n'));
        assert!(!found[0].fingerprint.is_empty());
    }

    #[test]
    fn codex_discovery_only_rollout_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp
            .path()
            .join("sessions")
            .join("2026")
            .join("07")
            .join("01");
        let good = root.join("rollout-2026-07-01T00-00-00-abc.jsonl");
        write(
            &good,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"cid\"}}\n{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"go\"}}\n",
        );
        let ignored = root.join("notes.jsonl");
        write(&ignored, "{\"type\":\"response_item\"}\n");

        let found = discover_codex(&tmp.path().join("sessions"), &origin()).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].envelope.session_id, "cid");
        assert_eq!(found[0].envelope.agent, "Codex");
    }

    #[test]
    fn missing_roots_are_graceful() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(discover_claude(&tmp.path().join("nope"), &origin())
            .unwrap()
            .is_empty());
        assert!(discover_codex(&tmp.path().join("nope"), &origin())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn cursor_maps_threads_to_discovered_with_synthetic_path_and_limit() {
        use rusqlite::{params, Connection};
        use serde_json::json;

        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("state.vscdb");
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB)",
            [],
        )
        .unwrap();
        for (cid, created) in [("c-old", 100i64), ("c-new", 999i64)] {
            let cd = json!({
                "composerId": cid,
                "createdAt": created,
                "conversation": [{"bubbleId": "b", "type": 1, "text": "hello cursor"}]
            });
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![
                    format!("composerData:{cid}"),
                    serde_json::to_vec(&cd).unwrap()
                ],
            )
            .unwrap();
        }
        drop(conn);

        // Full discovery: two threads, each with a synthetic per-thread path key.
        let all = discover_cursor(&db, &origin(), None).unwrap();
        assert_eq!(all.len(), 2);
        assert!(all
            .iter()
            .all(|d| d.path.to_string_lossy().contains("#composer:")));
        assert!(!all[0].fingerprint.is_empty());

        // Limit caps to the newest thread only.
        let limited = discover_cursor(&db, &origin(), Some(1)).unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].envelope.session_id, "c-new");
    }

    // Test-only extractors. Each is exercised with a matching and a non-matching
    // error across the tests below, so both arms are covered with no exclusion
    // while the assertions stay strong (exact variant).
    fn is_cursor_err(e: &SourceError) -> bool {
        matches!(e, SourceError::Cursor(_))
    }

    fn is_io_err(e: &SourceError) -> bool {
        matches!(e, SourceError::Io { .. })
    }

    #[test]
    fn cursor_missing_db_errors_cleanly() {
        // A non-existent DB path surfaces a Cursor open error, not a panic.
        let tmp = tempfile::tempdir().unwrap();
        let err = discover_cursor(&tmp.path().join("nope.vscdb"), &origin(), None)
            .err()
            .unwrap();
        assert!(is_cursor_err(&err));
        // A Cursor error covers the `is_io_err` false arm.
        assert!(!is_io_err(&err));
    }

    #[test]
    fn walk_on_a_file_root_is_an_io_error() {
        // `root` exists but is a file, not a directory: `read_dir` errors, which
        // surfaces as SourceError::Io (exercises the read_dir map_err branch).
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("not-a-dir.jsonl");
        std::fs::write(&file, b"x").unwrap();
        let err = discover_claude(&file, &origin()).err().unwrap();
        assert!(is_io_err(&err));
        // An Io error covers the `is_cursor_err` false arm.
        assert!(!is_cursor_err(&err));
    }

    #[test]
    fn claude_skips_invalid_utf8_transcript_with_warn() {
        // Non-empty but invalid UTF-8 -> read_claude returns a non-Empty error,
        // so the `Err(e) => warn` skip branch (not the silent Empty branch) runs.
        crate::install_warn_logging();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("projects");
        let f = root.join("-x").join("bad.jsonl");
        write(&f, ""); // create parents
        std::fs::write(&f, [0x7b, 0xff, 0xfe, 0x7d, b'\n']).unwrap(); // { <bad utf8> }
        let found = discover_claude(&root, &origin()).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn codex_skips_transcript_without_session_id_with_warn() {
        // A rollout file whose stem strips to an empty fallback id and whose rows
        // carry no session_meta id -> read_codex returns NoSessionId (non-Empty),
        // hitting the warn skip branch.
        crate::install_warn_logging();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("sessions");
        let f = root.join("rollout-.jsonl");
        write(
            &f,
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\"}}\n",
        );
        let found = discover_codex(&root, &origin()).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn claude_and_codex_skip_empty_transcripts_silently() {
        // Empty (whitespace-only) files -> ParseError::Empty -> silently skipped
        // (the `Err(ParseError::Empty) => {}` arm, distinct from the warn arm).
        let tmp = tempfile::tempdir().unwrap();
        let croot = tmp.path().join("projects");
        write(&croot.join("-x").join("empty.jsonl"), "\n  \n");
        assert!(discover_claude(&croot, &origin()).unwrap().is_empty());

        let droot = tmp.path().join("sessions");
        write(&droot.join("rollout-abc.jsonl"), "\n");
        assert!(discover_codex(&droot, &origin()).unwrap().is_empty());
    }

    #[test]
    fn enrich_keeps_preexisting_repo_and_git_remote() {
        use serde_json::json;
        use session_capture::{Metadata, SCS_VERSION};

        // Metadata already has repo + git_remote: enrich must NOT overwrite them
        // (covers the `is_none()` false branches), even with a real cwd.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".git")).unwrap();
        std::fs::write(
            tmp.path().join(".git").join("config"),
            "[remote \"origin\"]\n url = git@github.com:derived/repo.git\n",
        )
        .unwrap();

        let env = SessionEnvelope {
            scs_version: SCS_VERSION.to_string(),
            origin: origin(),
            agent: "ClaudeCode".into(),
            source_format: "claude-code-jsonl".into(),
            session_id: "s".into(),
            parent_session_id: None,
            started_at: "2026-07-01T00:00:00Z".into(),
            last_activity_at: "2026-07-01T00:00:00Z".into(),
            content_hash: None,
            metadata: Some(Metadata {
                repo: Some("preset/repo".into()),
                git_remote: Some("preset-remote".into()),
                cwd: Some(tmp.path().to_string_lossy().to_string()),
                ..Default::default()
            }),
            raw: json!("x"),
        };
        let out = enrich(env);
        let meta = out.metadata.unwrap();
        assert_eq!(meta.repo.as_deref(), Some("preset/repo"));
        assert_eq!(meta.git_remote.as_deref(), Some("preset-remote"));
    }

    #[test]
    fn codex_on_a_file_root_is_an_io_error() {
        // discover_codex propagates a walk IO error the same way discover_claude
        // does (covers the `?` on the walk call in discover_codex).
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("rollout-x.jsonl");
        std::fs::write(&file, b"x").unwrap();
        let err = discover_codex(&file, &origin()).err().unwrap();
        assert!(is_io_err(&err));
    }

    #[test]
    fn unreadable_transcripts_are_skipped() {
        use std::os::unix::fs::PermissionsExt;
        // A discovered file that becomes unreadable (mode 000) makes
        // read_transcript return None, so the discover loop's skip branch runs.
        let tmp = tempfile::tempdir().unwrap();

        let croot = tmp.path().join("projects");
        let cfile = croot.join("-x").join("11111111.jsonl");
        write(&cfile, "{\"type\":\"user\"}\n");
        std::fs::set_permissions(&cfile, std::fs::Permissions::from_mode(0o000)).unwrap();
        assert!(discover_claude(&croot, &origin()).unwrap().is_empty());

        let droot = tmp.path().join("sessions");
        let dfile = droot.join("rollout-abc.jsonl");
        write(&dfile, "{\"type\":\"response_item\"}\n");
        std::fs::set_permissions(&dfile, std::fs::Permissions::from_mode(0o000)).unwrap();
        assert!(discover_codex(&droot, &origin()).unwrap().is_empty());

        // Restore perms so tempdir cleanup can remove them.
        std::fs::set_permissions(&cfile, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::set_permissions(&dfile, std::fs::Permissions::from_mode(0o644)).unwrap();
    }

    #[test]
    fn enrich_without_metadata_block_is_a_noop() {
        use serde_json::json;
        use session_capture::SCS_VERSION;
        // metadata None -> the `if let Some(metadata)` guard is skipped entirely.
        let env = SessionEnvelope {
            scs_version: SCS_VERSION.to_string(),
            origin: origin(),
            agent: "Cursor".into(),
            source_format: "cursor-state-vscdb".into(),
            session_id: "s".into(),
            parent_session_id: None,
            started_at: "2026-07-01T00:00:00Z".into(),
            last_activity_at: "2026-07-01T00:00:00Z".into(),
            content_hash: None,
            metadata: None,
            raw: json!("x"),
        };
        assert!(enrich(env).metadata.is_none());
    }

    #[test]
    fn enrich_without_cwd_is_a_noop() {
        use serde_json::json;
        use session_capture::{Metadata, SCS_VERSION};
        // metadata present but cwd None -> the inner `if let Some(cwd)` is skipped.
        let env = SessionEnvelope {
            scs_version: SCS_VERSION.to_string(),
            origin: origin(),
            agent: "Cursor".into(),
            source_format: "cursor-state-vscdb".into(),
            session_id: "s".into(),
            parent_session_id: None,
            started_at: "2026-07-01T00:00:00Z".into(),
            last_activity_at: "2026-07-01T00:00:00Z".into(),
            content_hash: None,
            metadata: Some(Metadata {
                cwd: None,
                ..Default::default()
            }),
            raw: json!("x"),
        };
        let out = enrich(env);
        assert!(out.metadata.unwrap().repo.is_none());
    }

    #[test]
    fn fingerprint_and_read_transcript_report_a_real_file() {
        // Measures fingerprint's success path (metadata + mtime + len) and
        // read_transcript's happy path directly.
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("t.jsonl");
        write(&f, "payload");
        let fp = fingerprint(&f).expect("fingerprint of a real file is Some");
        assert!(fp.contains(':'), "fingerprint is `<mtime>:<len>`: {fp}");
        let (fp2, bytes) = read_transcript(&f).expect("read_transcript is Some");
        assert_eq!(fp2, fp);
        assert_eq!(bytes, b"payload");
    }

    #[test]
    fn fingerprint_and_read_transcript_are_none_for_a_missing_file() {
        // A path with no file: `metadata` fails, so `fingerprint` hits its `?`
        // None edge, and `read_transcript` short-circuits through its own `?`.
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("gone.jsonl");
        assert!(fingerprint(&missing).is_none());
        assert!(read_transcript(&missing).is_none());
    }

    #[test]
    fn walk_propagates_an_error_from_an_unreadable_subdirectory() {
        use std::os::unix::fs::PermissionsExt;
        // read_dir(root) succeeds and lists a subdirectory, but recursing into it
        // fails because the subdirectory is unreadable: exercises the recursion
        // `?` error edge in walk.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("projects");
        let locked = root.join("locked");
        std::fs::create_dir_all(&locked).unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let result = discover_claude(&root, &origin());
        // Restore perms so tempdir cleanup can remove the tree.
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_io_err(&result.err().unwrap()));
    }

    #[test]
    fn walk_recurses_keeps_matching_and_ignores_others() {
        // A nested directory (recursion), a kept `.jsonl`, and an ignored `.txt`
        // (the `keep(&path)` false path) all in one walk. discover_claude only
        // returns the matching transcript.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("projects");
        write(
            &root.join("-a").join("11111111.jsonl"),
            "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n",
        );
        write(&root.join("-a").join("notes.txt"), "ignored");
        let found = discover_claude(&root, &origin()).unwrap();
        assert_eq!(found.len(), 1);
    }
}
