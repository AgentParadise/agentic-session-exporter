//! Exporter sweep-health sidecar.
//!
//! Fingerprint state is intentionally not used as a health signal: a healthy
//! deduplicated sweep often has no uploads and therefore no state write. This
//! small sidecar advances only when a complete sweep has no hard upload
//! failures.

use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Result of checking the last successful exporter sweep.
#[derive(Debug, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy { age_secs: u64 },
    Stale { age_secs: u64, max_age_secs: u64 },
    Missing,
    Invalid,
}

/// Record a successful sweep atomically as seconds since the Unix epoch.
pub fn record_success(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let timestamp = now_epoch_secs();
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(format!(".tmp-{}", std::process::id()));
    let temporary = std::path::PathBuf::from(temporary);
    std::fs::write(&temporary, format!("{timestamp}\n"))?;
    std::fs::rename(temporary, path)
}

/// Classify the health sidecar against an explicit clock, keeping the boundary
/// deterministic and directly unit-testable.
pub fn check(path: &Path, now_secs: u64, max_age_secs: u64) -> HealthStatus {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return HealthStatus::Missing,
        Err(_) => return HealthStatus::Invalid,
    };
    let Ok(last_success_secs) = contents.trim().parse::<u64>() else {
        return HealthStatus::Invalid;
    };
    if last_success_secs > now_secs {
        return HealthStatus::Invalid;
    }
    let age_secs = now_secs - last_success_secs;
    if age_secs <= max_age_secs {
        HealthStatus::Healthy { age_secs }
    } else {
        HealthStatus::Stale {
            age_secs,
            max_age_secs,
        }
    }
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_record_reports_its_age() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("nested/last-success");
        record_success(&path).unwrap();

        let timestamp = std::fs::read_to_string(&path)
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap();
        assert_eq!(
            check(&path, timestamp + 42, 60),
            HealthStatus::Healthy { age_secs: 42 }
        );
    }

    #[test]
    fn stale_and_missing_records_are_unhealthy() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("last-success");
        assert_eq!(check(&path, 100, 10), HealthStatus::Missing);

        std::fs::write(&path, "20\n").unwrap();
        assert_eq!(
            check(&path, 100, 60),
            HealthStatus::Stale {
                age_secs: 80,
                max_age_secs: 60
            }
        );
    }

    #[test]
    fn malformed_record_is_unhealthy() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("last-success");
        std::fs::write(&path, "not-a-timestamp\n").unwrap();
        assert_eq!(check(&path, 100, 60), HealthStatus::Invalid);
    }

    #[test]
    fn future_record_is_unhealthy() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("last-success");
        std::fs::write(&path, "101\n").unwrap();
        assert_eq!(check(&path, 100, 60), HealthStatus::Invalid);
    }

    #[cfg(unix)]
    #[test]
    fn record_success_propagates_create_dir_all_error() {
        use std::os::unix::fs::PermissionsExt;
        // A read-only parent: recording under a deeper path fails inside
        // `create_dir_all(parent)?`, forcing the first `?` error arm.
        let temp = tempfile::tempdir().unwrap();
        let ro = temp.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let path = ro.join("child").join("last-success");
        assert!(record_success(&path).is_err());
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn record_success_propagates_write_error() {
        use std::os::unix::fs::PermissionsExt;
        // Parent exists but is read-only: `create_dir_all` no-ops on the existing
        // dir, so the temp-file `std::fs::write(...)?` fails instead, forcing the
        // write `?` error arm.
        let temp = tempfile::tempdir().unwrap();
        let ro = temp.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let path = ro.join("last-success");
        assert!(record_success(&path).is_err());
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn record_success_with_a_parentless_path_skips_dir_creation() {
        // The filesystem root has no parent, so `path.parent()` is `None` and the
        // `create_dir_all` branch is skipped (exercising the `if let Some` false
        // arm). The subsequent temp write into "/" fails for a non-root user, so
        // `record_success` returns an error, but the parentless branch has run.
        assert!(record_success(Path::new("/")).is_err());
    }

    #[test]
    fn unreadable_record_is_invalid() {
        // A path that is a directory: read_to_string fails with a non-NotFound
        // error, exercising the `Err(_) => Invalid` arm (distinct from Missing).
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("a-directory");
        std::fs::create_dir(&dir).unwrap();
        assert_eq!(check(&dir, 100, 60), HealthStatus::Invalid);
    }
}
