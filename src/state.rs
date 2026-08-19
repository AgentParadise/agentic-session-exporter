//! Exporter state: the last-seen fingerprint per transcript path. Lets a re-run
//! skip files that have not changed since the last successful upload without a
//! network round trip. The store's content_hash dedup is the real idempotency
//! guarantee; this is purely a cost optimization, so a missing or corrupt state
//! file is non-fatal (we just re-upload, which dedups server-side).
//!
//! The fingerprint describes the transcript on disk. Config the exporter STAMPS
//! onto the envelope (tags) is not visible to it, so the state also records a
//! digest of that config: when it changes, the prior fingerprints are discarded
//! and everything is re-sent. The store then reconciles metadata on its side.
//! Without this, an established exporter would never apply newly configured
//! tags to transcripts that had not otherwise changed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// On-disk state: the stamp-config digest plus the fingerprint map.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Persisted {
    #[serde(default)]
    config_digest: String,
    #[serde(default)]
    seen: HashMap<String, String>,
}

/// Fingerprint map keyed by absolute transcript path.
#[derive(Debug, Default)]
pub struct State {
    seen: HashMap<String, String>,
    path: PathBuf,
    config_digest: String,
}

impl State {
    /// Load state from `path`. A missing, unreadable, or stale-config file
    /// yields empty state, which just means everything is re-sent once.
    pub fn load(path: &Path, config_digest: &str) -> Self {
        let seen = std::fs::read(path)
            .ok()
            .and_then(|b| serde_json::from_slice::<Persisted>(&b).ok())
            .filter(|p| p.config_digest == config_digest)
            .map(|p| p.seen)
            .unwrap_or_default();
        Self {
            seen,
            path: path.to_path_buf(),
            config_digest: config_digest.to_string(),
        }
    }

    /// True when `path` already has this exact fingerprint recorded.
    pub fn is_current(&self, path: &Path, fingerprint: &str) -> bool {
        self.seen
            .get(&path.to_string_lossy().to_string())
            .map(|f| f == fingerprint)
            .unwrap_or(false)
    }

    /// Record a fingerprint for a path (after a successful upload).
    pub fn mark(&mut self, path: &Path, fingerprint: String) {
        self.seen
            .insert(path.to_string_lossy().to_string(), fingerprint);
    }

    /// Persist state to disk. Creates the parent directory if needed. Errors are
    /// returned but treated as non-fatal by the caller.
    pub fn save(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // A digest string plus a `HashMap<String, String>` always serializes to
        // JSON, so `.expect` never fires; a plain `expect`/`unwrap` has no
        // llvm-cov-flagged dead region (like `assert!`), so no exclusion is
        // needed.
        let bytes = serde_json::to_vec_pretty(&Persisted {
            config_digest: self.config_digest.clone(),
            seen: self.seen.clone(),
        })
        .expect("fingerprint map of strings always serializes to JSON");
        std::fs::write(&self.path, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_detects_change() {
        let tmp = tempfile::tempdir().unwrap();
        let sf = tmp.path().join("state.json");
        let mut state = State::load(&sf, "");
        let p = Path::new("/some/transcript.jsonl");
        assert!(!state.is_current(p, "100:5"));
        state.mark(p, "100:5".to_string());
        state.save().unwrap();

        let reloaded = State::load(&sf, "");
        assert!(reloaded.is_current(p, "100:5"));
        assert!(!reloaded.is_current(p, "200:9")); // changed fingerprint
    }

    #[test]
    fn changed_stamp_config_discards_prior_fingerprints() {
        // Tags are stamped onto the envelope, not derived from the transcript,
        // so a tag change must re-send otherwise-unchanged files. Otherwise an
        // established exporter would never apply newly configured tags.
        let tmp = tempfile::tempdir().unwrap();
        let sf = tmp.path().join("state.json");
        let p = Path::new("/some/transcript.jsonl");

        let mut state = State::load(&sf, "");
        state.mark(p, "100:5".to_string());
        state.save().unwrap();

        // Same config: the optimization still applies.
        assert!(State::load(&sf, "").is_current(p, "100:5"));
        // Different config: prior fingerprints are discarded.
        assert!(!State::load(&sf, "ci:run:42").is_current(p, "100:5"));
    }

    #[test]
    fn legacy_flat_map_state_file_degrades_to_a_one_time_resend() {
        // Pre-config-digest state files were a bare {path: fingerprint} map. They
        // must not crash or be silently honoured: the digest cannot be known, so
        // the cache is dropped and everything is re-sent once, then rewritten in
        // the new format. Verified against a real legacy payload.
        let tmp = tempfile::tempdir().unwrap();
        let sf = tmp.path().join("state.json");
        std::fs::write(&sf, br#"{"/some/transcript.jsonl":"100:5"}"#).unwrap();

        let mut state = State::load(&sf, "");
        assert!(
            !state.is_current(Path::new("/some/transcript.jsonl"), "100:5"),
            "a legacy file must not be honoured as a current cache"
        );

        // After one sweep the file is rewritten in the new format and caches again.
        state.mark(Path::new("/some/transcript.jsonl"), "100:5".to_string());
        state.save().unwrap();
        assert!(State::load(&sf, "").is_current(Path::new("/some/transcript.jsonl"), "100:5"));
    }

    #[test]
    fn missing_file_is_empty_state() {
        let tmp = tempfile::tempdir().unwrap();
        let state = State::load(&tmp.path().join("nope.json"), "");
        assert!(!state.is_current(Path::new("/x"), "1:1"));
    }

    #[test]
    fn save_creates_missing_parent_directories() {
        // A state path several levels below an existing dir: save() must create
        // the parent chain (exercises the create_dir_all branch).
        let tmp = tempfile::tempdir().unwrap();
        let sf = tmp.path().join("a").join("b").join("state.json");
        let mut state = State::load(&sf, "");
        state.mark(Path::new("/t"), "1:1".to_string());
        state.save().unwrap();
        assert!(sf.exists());
        assert!(State::load(&sf, "").is_current(Path::new("/t"), "1:1"));
    }

    #[cfg(unix)]
    #[test]
    fn save_propagates_create_dir_all_error() {
        use std::os::unix::fs::PermissionsExt;
        // A read-only parent directory: `save()` on a path one level deeper must
        // fail inside `create_dir_all(parent)?`, forcing the first `?` error arm.
        let tmp = tempfile::tempdir().unwrap();
        let ro = tmp.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let sf = ro.join("child").join("state.json");
        let mut state = State::load(&sf, "");
        state.mark(Path::new("/t"), "1:1".to_string());
        assert!(state.save().is_err());
        // Restore permissions so the tempdir can be cleaned up.
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn save_propagates_write_error() {
        use std::os::unix::fs::PermissionsExt;
        // Parent exists but is read-only: `create_dir_all` on an existing dir is a
        // no-op success, so the `std::fs::write` at the tail fails instead,
        // forcing the write `?` error arm distinct from create_dir_all.
        let tmp = tempfile::tempdir().unwrap();
        let ro = tmp.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let sf = ro.join("state.json");
        let mut state = State::load(&sf, "");
        state.mark(Path::new("/t"), "1:1".to_string());
        assert!(state.save().is_err());
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn save_with_a_parentless_path_skips_dir_creation() {
        // The filesystem root has no parent, so `path.parent()` is `None` and the
        // `create_dir_all` branch is skipped (exercising the `if let Some` false
        // arm). Writing to "/" then fails as a non-root user, so `save` returns an
        // error, but the parentless branch has been executed.
        let state = State::load(Path::new("/"), "");
        assert!(state.save().is_err());
    }
}
