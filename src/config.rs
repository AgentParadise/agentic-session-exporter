//! Exporter runtime configuration. Loaded from the environment (the daemon reads
//! it from a 0600 env file; interactive runs read the process environment). One
//! struct, one validation point. The write token is the only secret and is kept
//! separate from the non-secret knobs.

use std::env;
use std::path::{Path, PathBuf};

/// All exporter configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URL of the store, for example `http://100.x.y.z:18090`.
    pub store_url: String,
    /// Bearer token with the `sessions:write` scope. Required to ingest.
    pub write_token: Option<String>,
    /// Origin host stamped into every envelope. Defaults to the machine hostname.
    pub origin_host: String,
    /// Origin environment label (for example `laptop`, `vps`, `mini`).
    pub origin_environment: String,
    /// OPTIONAL deployment identity (APS-V1-0004 2.0.0 `origin.deployment`),
    /// from `SESSION_STORE_ORIGIN_DEPLOYMENT`.
    ///
    /// Answers a DIFFERENT question from `origin_environment`, which is the
    /// CLASS of runtime (`local`, `vps`, `container`, `workflow`). Every
    /// containerised run reports the same class, so without this a multi-tier
    /// install is unattributable: dev, beta and prod are indistinguishable once
    /// the sessions reach a store.
    ///
    /// No default. Absent is a meaningful answer - a single-deployment machine
    /// genuinely has none - and inventing one would put a fabricated identity
    /// on every laptop session.
    pub origin_deployment: Option<String>,
    /// Claude projects root. Defaults to `~/.claude/projects`.
    pub claude_root: PathBuf,
    /// Codex sessions root. Defaults to `~/.codex/sessions`.
    pub codex_root: PathBuf,
    /// Cursor state DB path (macOS default). Read-only if present.
    pub cursor_db: Option<PathBuf>,
    /// Cap on the number of (newest) Cursor threads to capture in a run. `None`
    /// captures all. Set via `CURSOR_LIMIT` or the `--cursor-limit` CLI flag;
    /// mainly for a fast bounded test against a large real DB.
    pub cursor_limit: Option<usize>,
    /// State file recording the last-seen fingerprint per transcript so re-runs
    /// skip unchanged files without a network round trip.
    pub state_file: PathBuf,
    /// Sidecar file containing the Unix timestamp of the last completed sweep.
    /// It deliberately does not share the fingerprint-state JSON schema, so a
    /// successful sweep with no changes still has a fresh health record.
    pub health_file: PathBuf,
    /// Maximum age of the last successful sweep before `--health` reports the
    /// exporter as stale.
    pub health_max_age_secs: u64,
    /// Batch size for uploads.
    pub batch_size: usize,
    /// Pre-upload hard cap on a single envelope's serialized size. An envelope
    /// bigger than this is skipped with a warning (never sent), so one
    /// pathological transcript cannot wedge the sweep. Set via
    /// `MAX_ENVELOPE_BYTES`; defaults to 512MB. With server-side blob offload the
    /// store now holds giant threads (the real ones reach ~115MB), so this cap is
    /// just a runaway guard, raised well above real sizes so the giants upload.
    pub max_envelope_bytes: usize,
    /// Free-form correlation tags stamped onto every envelope this exporter
    /// uploads, from `SESSION_STORE_TAGS` (comma-separated). The exporter
    /// assigns them no meaning: a caller that wants its sessions grouped picks
    /// its own namespaced strings (`ci:run:42`, `myorch:phase:abc`) and later
    /// queries them back through the store's existing `tags` search filter.
    pub tags: Vec<String>,
}

/// Configuration errors surfaced at startup.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required env var {0}")]
    Missing(&'static str),
    #[error("could not determine home directory (set HOME)")]
    NoHome,
}

/// Best-effort runtime CLASS when `SESSION_STORE_ORIGIN_ENV` is unset.
///
/// APS-V1-0004 4.2.1 requires `origin.environment` and defines it as one of
/// `local`, `vps`, `container`, `workflow`. The previous default was `laptop`,
/// which is not one of them: every session written without an explicit value
/// was out of spec on a REQUIRED field, and a store filtering by the documented
/// classes matched none of them.
///
/// Detects a container rather than guessing, because that is where a wrong
/// answer costs most - a containerised run labelled `local` is indistinguishable
/// from a developer machine in a shared corpus. Falls back to `local`, which is
/// the honest answer for an unmarked host and is at least a value the standard
/// defines.
///
/// An operator who knows better sets the variable; this only decides what
/// happens when nobody said.
fn detect_environment_class() -> String {
    // The markers the major runtimes leave: /.dockerenv is Docker,
    // /run/.containerenv is Podman, and the cgroup path catches the rest.
    let containerised = std::path::Path::new("/.dockerenv").exists()
        || std::path::Path::new("/run/.containerenv").exists()
        || std::fs::read_to_string("/proc/1/cgroup")
            .map(|c| c.contains("docker") || c.contains("containerd") || c.contains("kubepods"))
            .unwrap_or(false);

    if containerised {
        "container".to_string()
    } else {
        "local".to_string()
    }
}

impl Config {
    /// Load and validate from the environment.
    ///
    /// Required: `SESSION_STORE_URL`. Recommended: `SESSIONS_WRITE_TOKEN`.
    /// Optional with defaults: `SESSION_STORE_ORIGIN_HOST` (hostname),
    /// `SESSION_STORE_ORIGIN_ENV` (`laptop`), `CLAUDE_PROJECTS_ROOT`,
    /// `CODEX_SESSIONS_ROOT`, `CURSOR_STATE_DB`, `EXPORTER_STATE_FILE`,
    /// `EXPORTER_HEALTH_FILE`, `EXPORTER_HEALTH_MAX_AGE`,
    /// `EXPORTER_BATCH_SIZE` (50).
    pub fn from_env() -> Result<Self, ConfigError> {
        let store_url =
            env::var("SESSION_STORE_URL").map_err(|_| ConfigError::Missing("SESSION_STORE_URL"))?;
        let write_token = non_empty(env::var("SESSIONS_WRITE_TOKEN").ok());

        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or(ConfigError::NoHome)?;

        let origin_host = env::var("SESSION_STORE_ORIGIN_HOST")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(default_hostname);
        let origin_environment = non_empty(env::var("SESSION_STORE_ORIGIN_ENV").ok())
            .unwrap_or_else(detect_environment_class);
        let origin_deployment = non_empty(env::var("SESSION_STORE_ORIGIN_DEPLOYMENT").ok());

        let claude_root = env::var_os("CLAUDE_PROJECTS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude").join("projects"));
        let codex_root = env::var_os("CODEX_SESSIONS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex").join("sessions"));

        // Cursor: default to the macOS path when it exists; None otherwise.
        let cursor_db = env::var_os("CURSOR_STATE_DB")
            .map(PathBuf::from)
            .or_else(|| {
                let p = home
                    .join("Library")
                    .join("Application Support")
                    .join("Cursor")
                    .join("User")
                    .join("globalStorage")
                    .join("state.vscdb");
                p.exists().then_some(p)
            });

        let cursor_limit = env::var("CURSOR_LIMIT")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0);

        let state_file = env::var_os("EXPORTER_STATE_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| default_state_file(&home, &origin_host));

        let health_file = env::var_os("EXPORTER_HEALTH_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| default_health_file(&state_file));

        let health_max_age_secs = env::var("EXPORTER_HEALTH_MAX_AGE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(900);

        let batch_size = env::var("EXPORTER_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(50);

        let max_envelope_bytes = env::var("MAX_ENVELOPE_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(512 * 1024 * 1024);

        let tags = env::var("SESSION_STORE_TAGS")
            .map(|s| parse_tags(&s))
            .unwrap_or_default();

        Ok(Self {
            store_url: store_url.trim_end_matches('/').to_string(),
            write_token,
            origin_host,
            origin_deployment,
            origin_environment,
            claude_root,
            codex_root,
            cursor_db,
            cursor_limit,
            state_file,
            health_file,
            health_max_age_secs,
            batch_size,
            max_envelope_bytes,
            tags,
        })
    }
}

impl Config {
    /// Digest of the config this exporter STAMPS onto envelopes, as opposed to
    /// reads off disk. The fingerprint state is keyed on it so a tag change
    /// re-sends otherwise-unchanged transcripts. Tags cannot contain a comma
    /// (they are split on it), so joining is unambiguous and no hash is needed.
    pub fn stamp_digest(&self) -> String {
        self.tags.join(",")
    }
}

/// Split a comma-separated tag list into trimmed, non-empty, de-duplicated
/// tags, preserving first-occurrence order. Duplicates are dropped because the
/// jsonb containment filter treats the tag array as a set anyway, so repeats
/// only inflate the stored envelope.
fn parse_tags(raw: &str) -> Vec<String> {
    let mut tags: Vec<String> = Vec::new();
    for tag in raw.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        if !tags.iter().any(|t| t == tag) {
            tags.push(tag.to_string());
        }
    }
    tags
}

fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.trim().is_empty())
}

/// Best-effort hostname. Falls back to `unknown-host` so an envelope always has
/// a non-empty origin host (the store rejects empty origins).
fn default_hostname() -> String {
    // `hostname` is not in std; read it portably from the env or the system file.
    // macOS + Linux expose it via the `hostname` command; but to stay
    // dependency-free we read /etc/hostname (Linux) and fall back. The pure
    // resolver is split out so every branch is unit-testable without depending on
    // the host's actual `HOSTNAME` / `/etc/hostname` state.
    hostname_from_sources(
        env::var("HOSTNAME").ok(),
        std::fs::read_to_string("/etc/hostname").ok(),
    )
}

/// Resolve a hostname from the two portable sources, in order: the `HOSTNAME`
/// env var, then `/etc/hostname`, then the `unknown-host` fallback. A blank value
/// in either source is treated as absent.
fn hostname_from_sources(env_hostname: Option<String>, etc_hostname: Option<String>) -> String {
    if let Some(h) = env_hostname {
        if !h.trim().is_empty() {
            return h;
        }
    }
    if let Some(h) = etc_hostname {
        let h = h.trim();
        if !h.is_empty() {
            return h.to_string();
        }
    }
    "unknown-host".to_string()
}

/// Scope the default fingerprint state to the exporter origin host. The
/// installer makes the same choice, while an explicit `EXPORTER_STATE_FILE`
/// still takes precedence for operators who need a custom location.
fn default_state_file(home: &Path, origin_host: &str) -> PathBuf {
    let host_key: String = origin_host
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let host_key = if host_key.is_empty() {
        "unknown-host"
    } else {
        &host_key
    };
    home.join(".cache")
        .join("seshmagic-session-store")
        .join(format!("exporter-state-{host_key}.json"))
}

fn default_health_file(state_file: &Path) -> PathBuf {
    let mut path = state_file.as_os_str().to_os_string();
    path.push(".last-success");
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_empty_filters_blank() {
        assert_eq!(non_empty(Some("  ".into())), None);
        assert_eq!(non_empty(Some("x".into())), Some("x".into()));
    }

    #[test]
    fn default_hostname_is_never_empty() {
        assert!(!default_hostname().is_empty());
    }

    #[test]
    fn default_state_file_is_host_scoped_and_sanitized() {
        let home = Path::new("/tmp/exporter-home");
        assert_eq!(
            default_state_file(home, "vps / primary"),
            home.join(".cache/seshmagic-session-store/exporter-state-vps___primary.json")
        );
    }

    #[test]
    fn default_health_file_is_a_state_sidecar() {
        assert_eq!(
            default_health_file(Path::new("/tmp/exporter-state-mac.json")),
            PathBuf::from("/tmp/exporter-state-mac.json.last-success")
        );
    }

    #[test]
    fn hostname_prefers_env_then_etc_then_fallback() {
        // 1. Non-empty HOSTNAME wins.
        assert_eq!(
            hostname_from_sources(Some("box-1".into()), Some("etc-name".into())),
            "box-1"
        );
        // 2. Blank HOSTNAME falls through to a non-empty /etc/hostname (trimmed).
        assert_eq!(
            hostname_from_sources(Some("   ".into()), Some("  etc-name \n".into())),
            "etc-name"
        );
        // 3. HOSTNAME absent, /etc/hostname used.
        assert_eq!(
            hostname_from_sources(None, Some("only-etc\n".into())),
            "only-etc"
        );
        // 4. Both absent/blank -> the never-empty fallback.
        assert_eq!(hostname_from_sources(None, None), "unknown-host");
        assert_eq!(
            hostname_from_sources(Some(" ".into()), Some("\n".into())),
            "unknown-host"
        );
    }

    #[test]
    fn config_error_messages_render() {
        assert_eq!(
            ConfigError::Missing("SESSION_STORE_URL").to_string(),
            "missing required env var SESSION_STORE_URL"
        );
        assert_eq!(
            ConfigError::NoHome.to_string(),
            "could not determine home directory (set HOME)"
        );
    }

    // --- from_env: env-driven, serialized so the global process env is safe -----

    use std::sync::Mutex;

    /// Every env var `from_env` (and its helpers) consults. Cleared before each
    /// case so one test cannot leak into the next.
    const ENV_KEYS: &[&str] = &[
        "SESSION_STORE_URL",
        "SESSIONS_WRITE_TOKEN",
        "HOME",
        "SESSION_STORE_ORIGIN_HOST",
        "SESSION_STORE_ORIGIN_ENV",
        // Must be listed, or with_env cannot clear it and a value set by one
        // test leaks into every test after it.
        "SESSION_STORE_ORIGIN_DEPLOYMENT",
        "CLAUDE_PROJECTS_ROOT",
        "CODEX_SESSIONS_ROOT",
        "CURSOR_STATE_DB",
        "CURSOR_LIMIT",
        "EXPORTER_STATE_FILE",
        "EXPORTER_HEALTH_FILE",
        "EXPORTER_HEALTH_MAX_AGE",
        "EXPORTER_BATCH_SIZE",
        "MAX_ENVELOPE_BYTES",
        "HOSTNAME",
        "SESSION_STORE_TAGS",
    ];

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Restores the saved environment when dropped, so the original process env
    /// is put back even if `body` panics.
    struct EnvGuard<'a> {
        _lock: std::sync::MutexGuard<'a, ()>,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl Drop for EnvGuard<'_> {
        fn drop(&mut self) {
            for (k, v) in &self.saved {
                match v {
                    Some(v) => env::set_var(k, v),
                    None => env::remove_var(k),
                }
            }
        }
    }

    /// Run `body` with a pristine, fully-controlled process environment: all keys
    /// in `ENV_KEYS` are saved, cleared, then the given `pairs` are set. Originals
    /// are restored on drop (including on panic). Serialized via `ENV_LOCK`.
    fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
        let lock = ENV_LOCK.lock().unwrap();
        let saved = ENV_KEYS.iter().map(|k| (*k, env::var(k).ok())).collect();
        let _guard = EnvGuard { _lock: lock, saved };
        for k in ENV_KEYS {
            env::remove_var(k);
        }
        for (k, v) in pairs {
            env::set_var(k, v);
        }
        body()
    }

    // Test-only extractors, each exercised below with a matching and a
    // non-matching error so both arms are covered with no exclusion, while the
    // assertion stays strong (exact variant, and the exact missing-var name).
    fn missing_var(e: &ConfigError) -> Option<&'static str> {
        match e {
            ConfigError::Missing(name) => Some(name),
            _ => None,
        }
    }

    fn is_no_home(e: &ConfigError) -> bool {
        matches!(e, ConfigError::NoHome)
    }

    #[test]
    fn from_env_missing_url_is_error() {
        with_env(&[], || {
            let err = Config::from_env().unwrap_err();
            assert_eq!(missing_var(&err), Some("SESSION_STORE_URL"));
            // Cover the `missing_var` `_ => None` arm with a non-Missing error.
            assert_eq!(missing_var(&ConfigError::NoHome), None);
        });
    }

    #[test]
    fn from_env_missing_home_is_error() {
        with_env(&[("SESSION_STORE_URL", "http://s")], || {
            let err = Config::from_env().unwrap_err();
            assert!(is_no_home(&err));
            // Cover the `is_no_home` false arm with a non-NoHome error.
            assert!(!is_no_home(&ConfigError::Missing("X")));
        });
    }

    #[test]
    fn deployment_is_absent_when_unset() {
        // Absent is a real answer: a single-deployment host genuinely has no
        // deployment identity, and inventing one would stamp a fabricated value
        // on every session.
        with_env(
            &[
                ("SESSION_STORE_URL", "http://store.example"),
                ("HOME", "/tmp/exporter-home-dep"),
            ],
            || assert!(Config::from_env().unwrap().origin_deployment.is_none()),
        );
    }

    #[test]
    fn deployment_is_read_and_namespaces() {
        with_env(
            &[
                ("SESSION_STORE_URL", "http://store.example"),
                ("HOME", "/tmp/exporter-home-dep"),
                ("SESSION_STORE_ORIGIN_DEPLOYMENT", "syntropic137__prod"),
            ],
            || {
                assert_eq!(
                    Config::from_env().unwrap().origin_deployment.as_deref(),
                    Some("syntropic137__prod")
                )
            },
        );
    }

    #[test]
    fn an_empty_deployment_is_treated_as_absent() {
        // "" is what an unset shell variable expands to in a container
        // entrypoint. Storing it would be a deployment identity of nothing,
        // which a store would group on as its own source.
        with_env(
            &[
                ("SESSION_STORE_URL", "http://store.example"),
                ("HOME", "/tmp/exporter-home-dep"),
                ("SESSION_STORE_ORIGIN_DEPLOYMENT", ""),
            ],
            || assert!(Config::from_env().unwrap().origin_deployment.is_none()),
        );
    }

    #[test]
    fn the_detected_environment_is_a_class_the_standard_defines() {
        const DEFINED: [&str; 4] = ["local", "vps", "container", "workflow"];
        let detected = detect_environment_class();
        assert!(
            DEFINED.contains(&detected.as_str()),
            "detected {detected:?}, which APS-V1-0004 4.2.1 does not define"
        );
    }

    #[test]
    fn an_explicit_environment_wins_over_detection() {
        with_env(
            &[
                ("SESSION_STORE_URL", "http://store.example"),
                ("HOME", "/tmp/exporter-home-env"),
                ("SESSION_STORE_ORIGIN_ENV", "vps"),
            ],
            || assert_eq!(Config::from_env().unwrap().origin_environment, "vps"),
        );
    }

    #[test]
    fn an_empty_environment_falls_back_to_detection() {
        // An unset shell variable expands to "" in a container entrypoint.
        // Storing it would put an empty string in a REQUIRED field, which the
        // standard's own validator rejects.
        with_env(
            &[
                ("SESSION_STORE_URL", "http://store.example"),
                ("HOME", "/tmp/exporter-home-env"),
                ("SESSION_STORE_ORIGIN_ENV", ""),
            ],
            || assert!(!Config::from_env().unwrap().origin_environment.is_empty()),
        );
    }

    #[test]
    fn from_env_defaults_when_only_required_present() {
        with_env(
            &[
                ("SESSION_STORE_URL", "http://store:18090/"),
                ("HOME", "/tmp/exporter-home-xyz"),
                ("HOSTNAME", "host-a"),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                // Trailing slash trimmed.
                assert_eq!(cfg.store_url, "http://store:18090");
                assert_eq!(cfg.write_token, None);
                assert_eq!(cfg.origin_host, "host-a");
                // Was "laptop", which APS-V1-0004 4.2.1 does not define. Now
                // detected, and always one of the four classes it names.
                assert!(["local", "container"].contains(&cfg.origin_environment.as_str()));
                assert_eq!(
                    cfg.claude_root,
                    PathBuf::from("/tmp/exporter-home-xyz/.claude/projects")
                );
                assert_eq!(
                    cfg.codex_root,
                    PathBuf::from("/tmp/exporter-home-xyz/.codex/sessions")
                );
                // No CURSOR_STATE_DB and the macOS default doesn't exist under a
                // throwaway HOME -> None.
                assert_eq!(cfg.cursor_db, None);
                assert_eq!(cfg.cursor_limit, None);
                assert_eq!(cfg.health_max_age_secs, 900);
                assert_eq!(cfg.batch_size, 50);
                assert_eq!(cfg.max_envelope_bytes, 512 * 1024 * 1024);
                // Default state file is host-scoped under ~/.cache.
                assert_eq!(
                    cfg.state_file,
                    PathBuf::from(
                        "/tmp/exporter-home-xyz/.cache/seshmagic-session-store/exporter-state-host-a.json"
                    )
                );
                assert_eq!(
                    cfg.health_file,
                    PathBuf::from(
                        "/tmp/exporter-home-xyz/.cache/seshmagic-session-store/exporter-state-host-a.json.last-success"
                    )
                );
            },
        );
    }

    #[test]
    fn from_env_blank_origin_host_falls_back_to_hostname() {
        with_env(
            &[
                ("SESSION_STORE_URL", "http://s"),
                ("HOME", "/tmp/h"),
                ("SESSION_STORE_ORIGIN_HOST", "   "),
                ("HOSTNAME", "resolved-host"),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.origin_host, "resolved-host");
            },
        );
    }

    #[test]
    fn from_env_honors_all_overrides() {
        with_env(
            &[
                ("SESSION_STORE_URL", "http://s"),
                ("HOME", "/tmp/h"),
                ("SESSIONS_WRITE_TOKEN", "tok-123"),
                ("SESSION_STORE_ORIGIN_HOST", "vps-1"),
                ("SESSION_STORE_ORIGIN_ENV", "vps"),
                ("CLAUDE_PROJECTS_ROOT", "/custom/claude"),
                ("CODEX_SESSIONS_ROOT", "/custom/codex"),
                ("CURSOR_STATE_DB", "/custom/state.vscdb"),
                ("CURSOR_LIMIT", "7"),
                ("EXPORTER_STATE_FILE", "/custom/state.json"),
                ("EXPORTER_HEALTH_FILE", "/custom/health"),
                ("EXPORTER_HEALTH_MAX_AGE", "120"),
                ("EXPORTER_BATCH_SIZE", "10"),
                ("MAX_ENVELOPE_BYTES", "2048"),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.write_token.as_deref(), Some("tok-123"));
                assert_eq!(cfg.origin_host, "vps-1");
                assert_eq!(cfg.origin_environment, "vps");
                assert_eq!(cfg.claude_root, PathBuf::from("/custom/claude"));
                assert_eq!(cfg.codex_root, PathBuf::from("/custom/codex"));
                assert_eq!(cfg.cursor_db, Some(PathBuf::from("/custom/state.vscdb")));
                assert_eq!(cfg.cursor_limit, Some(7));
                assert_eq!(cfg.state_file, PathBuf::from("/custom/state.json"));
                assert_eq!(cfg.health_file, PathBuf::from("/custom/health"));
                assert_eq!(cfg.health_max_age_secs, 120);
                assert_eq!(cfg.batch_size, 10);
                assert_eq!(cfg.max_envelope_bytes, 2048);
            },
        );
    }

    #[test]
    fn from_env_blank_token_and_invalid_numbers_fall_to_defaults() {
        with_env(
            &[
                ("SESSION_STORE_URL", "http://s"),
                ("HOME", "/tmp/h"),
                ("HOSTNAME", "host-a"),
                ("SESSIONS_WRITE_TOKEN", "   "),
                // Present but zero / unparseable: every `.filter(|n| *n > 0)` and
                // `and_then(parse)` failure branch falls back to its default.
                ("CURSOR_LIMIT", "0"),
                ("EXPORTER_HEALTH_MAX_AGE", "0"),
                ("EXPORTER_BATCH_SIZE", "not-a-number"),
                ("MAX_ENVELOPE_BYTES", "0"),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.write_token, None);
                assert_eq!(cfg.cursor_limit, None);
                assert_eq!(cfg.health_max_age_secs, 900);
                assert_eq!(cfg.batch_size, 50);
                assert_eq!(cfg.max_envelope_bytes, 512 * 1024 * 1024);
            },
        );
    }

    #[test]
    fn from_env_uses_existing_macos_cursor_db_when_present() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        // Materialize the macOS default Cursor DB path so the `.then_some(p)`
        // true branch is taken (no CURSOR_STATE_DB override).
        let db = home
            .join("Library")
            .join("Application Support")
            .join("Cursor")
            .join("User")
            .join("globalStorage")
            .join("state.vscdb");
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        std::fs::write(&db, b"x").unwrap();

        with_env(
            &[
                ("SESSION_STORE_URL", "http://s"),
                ("HOME", home.to_str().unwrap()),
                ("HOSTNAME", "host-a"),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.cursor_db, Some(db.clone()));
            },
        );
    }

    #[test]
    fn parse_tags_splits_trims_and_drops_blanks() {
        assert_eq!(
            parse_tags("alpha, beta ,,  gamma  ,"),
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
        );
    }

    #[test]
    fn parse_tags_dedupes_preserving_first_occurrence() {
        assert_eq!(
            parse_tags("b,a,b,a"),
            vec!["b".to_string(), "a".to_string()]
        );
    }

    #[test]
    fn parse_tags_of_blank_is_empty() {
        assert!(parse_tags("").is_empty());
        assert!(parse_tags("  , ,, ").is_empty());
    }

    #[test]
    fn stamp_digest_tracks_the_configured_tags() {
        with_env(
            &[("SESSION_STORE_URL", "http://s"), ("HOME", "/tmp/h")],
            || {
                // No tags: a stable empty digest, so an untagged exporter never
                // invalidates its own state.
                assert_eq!(Config::from_env().unwrap().stamp_digest(), "");
            },
        );
        with_env(
            &[
                ("SESSION_STORE_URL", "http://s"),
                ("HOME", "/tmp/h"),
                ("SESSION_STORE_TAGS", "ci:run:42, team:platform"),
            ],
            || {
                assert_eq!(
                    Config::from_env().unwrap().stamp_digest(),
                    "ci:run:42,team:platform"
                );
            },
        );
    }

    #[test]
    fn from_env_tags_default_to_empty() {
        with_env(
            &[("SESSION_STORE_URL", "http://s"), ("HOME", "/tmp/h")],
            || {
                assert!(Config::from_env().unwrap().tags.is_empty());
            },
        );
    }

    #[test]
    fn from_env_reads_session_store_tags() {
        with_env(
            &[
                ("SESSION_STORE_URL", "http://s"),
                ("HOME", "/tmp/h"),
                ("SESSION_STORE_TAGS", "ci:run:42, team:platform"),
            ],
            || {
                assert_eq!(
                    Config::from_env().unwrap().tags,
                    vec!["ci:run:42".to_string(), "team:platform".to_string()]
                );
            },
        );
    }

    #[test]
    fn default_state_file_empty_host_key_falls_back() {
        // An empty host name sanitizes to an empty key, which then uses the
        // `unknown-host` fallback (covers the `if host_key.is_empty()` true arm).
        let home = Path::new("/tmp/h");
        assert_eq!(
            default_state_file(home, ""),
            home.join(".cache/seshmagic-session-store/exporter-state-unknown-host.json")
        );
    }
}
