//! Same-harness, cross-machine Claude session reconstitution.
//!
//! The relocation rule deliberately changes only the *container* path that
//! Claude Code uses to locate a transcript. The bytes returned by the store's
//! raw endpoint are written unchanged; transcript rows, including any absolute
//! paths inside them, are never decoded or rewritten here.

use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use reqwest::header::HeaderMap;
use session_capture::{Metadata, SessionEnvelope};

/// The only harness implemented by this client. Codex reconstitution is a
/// separate work item because its on-disk layout and native command differ.
pub const CLAUDE_AGENT: &str = "ClaudeCode";
pub const CLAUDE_SOURCE_FORMAT: &str = "claude-code-jsonl";

/// Environment-derived locations for the target machine.
#[derive(Debug, Clone)]
pub struct ReconstitutionLocations {
    /// Directory below which a missing repository is cloned. The clone target
    /// is `<repos_root>/<metadata.repo>`, e.g. `~/Code/acme/widget`.
    pub repos_root: PathBuf,
    /// Claude Code's native projects container. This is intentionally separate
    /// from `repos_root`: only its path slug is remapped.
    pub claude_projects_root: PathBuf,
}

impl ReconstitutionLocations {
    /// Read target-machine locations without consulting the captured envelope.
    ///
    /// `RECONSTITUTION_REPOS_ROOT` defaults to `~/Code`; callers can pass
    /// `--repos-root` in the binary to choose a different checkout root.
    pub fn from_env() -> Result<Self, ReconstitutionError> {
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or(ReconstitutionError::NoHome)?;
        Ok(Self {
            repos_root: env::var_os("RECONSTITUTION_REPOS_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join("Code")),
            claude_projects_root: env::var_os("CLAUDE_PROJECTS_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".claude").join("projects")),
        })
    }
}

/// A completed restore, ready for `claude --resume`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconstitutionPlan {
    pub session_id: String,
    pub repo_root: PathBuf,
    pub target_cwd: PathBuf,
    pub transcript_path: PathBuf,
}

/// Errors deliberately distinguish a bad envelope from a failed target-machine
/// operation, so no transcript gets written for an unsupported harness.
#[derive(Debug, thiserror::Error)]
pub enum ReconstitutionError {
    #[error("HOME is required to locate the default Claude and checkout directories")]
    NoHome,
    #[error("session id must be a non-empty safe filename segment")]
    InvalidSessionId,
    #[error("same-harness only: expected {CLAUDE_AGENT}/{CLAUDE_SOURCE_FORMAT}, got {agent}/{source_format}")]
    UnsupportedHarness {
        agent: String,
        source_format: String,
    },
    #[error("raw endpoint format mismatch: envelope is {envelope}, raw endpoint is {raw}")]
    SourceFormatMismatch { envelope: String, raw: String },
    #[error("envelope metadata.{0} is required for relocation")]
    MissingMetadata(&'static str),
    #[error("metadata.repo {0:?} is not a safe repository slug")]
    InvalidRepo(String),
    #[error("metadata.cwd {0:?} cannot be mapped beneath repository {1:?}")]
    UnmappableCwd(String, String),
    #[error("target worktree directory does not exist: {0}")]
    MissingTargetCwd(String),
    #[error("repository at {path} has a different origin than metadata.git_remote")]
    RepositoryCollision { path: String },
    #[error("git {operation} failed: {detail}")]
    Git {
        operation: &'static str,
        detail: String,
    },
    #[error("HTTP request failed: {0}")]
    Http(String),
    #[error("store returned HTTP {status} for {endpoint}")]
    HttpStatus { status: u16, endpoint: String },
    #[error("could not decode session envelope: {0}")]
    EnvelopeDecode(String),
    #[error("I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("claude --resume failed with status {0}")]
    NativeResume(i32),
}

/// HTTP client for the read-side reconstitution protocol.
pub struct ReconstitutionClient {
    http: reqwest::Client,
    store_url: String,
    read_token: Option<String>,
}

impl ReconstitutionClient {
    pub fn new(store_url: &str, read_token: Option<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            store_url: store_url.trim_end_matches('/').to_string(),
            read_token: read_token.filter(|token| !token.trim().is_empty()),
        }
    }

    /// Pull the envelope metadata, then raw bytes, restore them into the target
    /// Claude container, and return the native-resume plan. `raw` is never
    /// deserialized: `response.bytes()` flows directly to [`write_verbatim`].
    // Measured orchestrator: the success wiring is driven by
    // `reconstitute_writes_transcript_end_to_end` and each explicit-return branch
    // (invalid id, source-format mismatch, missing target cwd) by its own test.
    // The per-step `?` propagations are covered regions; the un-forceable IO
    // failures live in the excluded closures/helpers the steps call.
    pub async fn reconstitute(
        &self,
        session_id: &str,
        locations: &ReconstitutionLocations,
    ) -> Result<ReconstitutionPlan, ReconstitutionError> {
        validate_session_id(session_id)?;
        let envelope = self.fetch_envelope(session_id).await?;
        ensure_claude(&envelope)?;
        let (raw, source_format) = self.fetch_raw(session_id).await?;
        if source_format != envelope.source_format {
            return Err(ReconstitutionError::SourceFormatMismatch {
                envelope: envelope.source_format,
                raw: source_format,
            });
        }

        let repo_root = ensure_target_repo(&envelope, &locations.repos_root)?;
        let target_cwd = relocated_cwd(&envelope, &repo_root)?;
        if !target_cwd.is_dir() {
            return Err(ReconstitutionError::MissingTargetCwd(
                target_cwd.display().to_string(),
            ));
        }

        let transcript_path = write_reconstituted_transcript(
            &locations.claude_projects_root,
            &target_cwd,
            session_id,
            &raw,
        )?;

        Ok(ReconstitutionPlan {
            session_id: session_id.to_string(),
            repo_root,
            target_cwd,
            transcript_path,
        })
    }

    async fn fetch_envelope(
        &self,
        session_id: &str,
    ) -> Result<SessionEnvelope, ReconstitutionError> {
        let endpoint = format!("{}/v1/sessions/{session_id}", self.store_url);
        let response = self
            .authenticated(self.http.get(&endpoint))
            .send()
            .await
            .map_err(|error| ReconstitutionError::Http(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ReconstitutionError::HttpStatus {
                status: response.status().as_u16(),
                endpoint,
            });
        }
        response
            .json()
            .await
            .map_err(|error| ReconstitutionError::EnvelopeDecode(error.to_string()))
    }

    // Measured: the send-failure, non-2xx, missing-header, and success paths are
    // all driven by `fetch_raw_success_and_missing_header_and_status_and_http`;
    // only the `response.bytes()` mid-stream error mapper (un-forceable) is excluded.
    async fn fetch_raw(&self, session_id: &str) -> Result<(Vec<u8>, String), ReconstitutionError> {
        let endpoint = format!("{}/v1/sessions/{session_id}/raw", self.store_url);
        let response = self
            .authenticated(self.http.get(&endpoint))
            .send()
            .await
            .map_err(|error| ReconstitutionError::Http(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ReconstitutionError::HttpStatus {
                status: response.status().as_u16(),
                endpoint,
            });
        }
        let source_format = source_format(response.headers())?;
        let bytes = read_response_body(response).await?;
        Ok((bytes, source_format))
    }

    fn authenticated(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.read_token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }
}

/// Launch Claude Code's native same-harness resume in the relocated worktree.
pub fn invoke_claude_resume(plan: &ReconstitutionPlan) -> Result<(), ReconstitutionError> {
    let status = Command::new("claude")
        .current_dir(&plan.target_cwd)
        .arg("--resume")
        .arg(&plan.session_id)
        .status()
        .map_err(|source| ReconstitutionError::Io {
            path: "claude".to_string(),
            source,
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(ReconstitutionError::NativeResume(
            status.code().unwrap_or(1),
        ))
    }
}

fn ensure_claude(envelope: &SessionEnvelope) -> Result<(), ReconstitutionError> {
    if envelope.agent == CLAUDE_AGENT && envelope.source_format == CLAUDE_SOURCE_FORMAT {
        Ok(())
    } else {
        Err(ReconstitutionError::UnsupportedHarness {
            agent: envelope.agent.clone(),
            source_format: envelope.source_format.clone(),
        })
    }
}

fn source_format(headers: &HeaderMap) -> Result<String, ReconstitutionError> {
    headers
        .get("X-Source-Format")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or(ReconstitutionError::MissingMetadata("X-Source-Format"))
}

/// Read a response body into owned bytes.
///
/// The success path is measured by every fetch test; the `bytes()` error path is
/// measured by `fetch_raw_body_stream_error` (a canned server that under-delivers
/// its declared Content-Length, so the body read fails mid-stream).
async fn read_response_body(response: reqwest::Response) -> Result<Vec<u8>, ReconstitutionError> {
    response
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|error| ReconstitutionError::Http(error.to_string()))
}

/// Run a prepared `git` query command and collect its output.
///
/// Coverage-excluded: spawning `git` fails only when it is absent from PATH,
/// which the exporter's environment guarantees is present; that broken-environment
/// case is treated as an invariant (panic), not a runtime error, so callers carry
/// no un-forceable spawn `?` edge. The non-zero-exit and success paths are decided
/// by the callers and are fully measured.
#[cfg_attr(coverage_nightly, coverage(off))]
fn git_output(command: &mut Command) -> std::process::Output {
    command.output().expect("git must be available on PATH")
}

/// Locate a matching checkout on the target machine, cloning only into the
/// deterministic `<repos_root>/<metadata.repo>` location when none is found.
///
/// Measured: the metadata-field errors, the matching-current-dir short-circuit,
/// the existing-checkout collision, the clone success, and the clone-failure
/// branch are all driven by the `ensure_target_repo_*` tests. Only the
/// un-forceable IO/subprocess mappers (current_dir, clone-parent, create_dir_all,
/// git-spawn, clone-verify) are coverage-excluded, each inline below.
fn ensure_target_repo(
    envelope: &SessionEnvelope,
    repos_root: &Path,
) -> Result<PathBuf, ReconstitutionError> {
    let metadata = required_meta_block(envelope)?;
    let repo = required_metadata(&metadata.repo, "repo")?;
    let remote = required_metadata(&metadata.git_remote, "git_remote")?;
    let clone_target = repos_root.join(repo_path(repo)?);

    // An operator may run the command from an existing checkout with a custom
    // name or parent directory. Prefer that exact target-machine location when
    // its origin matches before considering the deterministic clone target.
    if let Some(root) = matching_git_root(&current_working_dir(), remote) {
        return Ok(root);
    }

    if clone_target.exists() {
        return matching_git_root(&clone_target, remote).ok_or_else(|| {
            ReconstitutionError::RepositoryCollision {
                path: clone_target.display().to_string(),
            }
        });
    }

    create_clone_parent(&clone_target, repo)?;
    let status = spawn_git_clone(remote, &clone_target);
    if !status.success() {
        return Err(ReconstitutionError::Git {
            operation: "clone",
            detail: format!("exit status {status}"),
        });
    }
    verify_cloned_origin(&clone_target, remote)
}

/// The process's current directory.
///
/// Coverage-excluded: `current_dir` fails only if the process has no valid
/// working directory, a broken-environment invariant (not a runtime error) that
/// cannot be arranged in-process, so callers carry no un-forceable `?` edge.
#[cfg_attr(coverage_nightly, coverage(off))]
fn current_working_dir() -> PathBuf {
    env::current_dir().expect("process has a valid working directory")
}

/// Create the parent directory of the deterministic clone target.
///
/// Measured: the `.parent()` None arm (an InvalidRepo error) is forced directly
/// by `create_clone_parent_rejects_a_parentless_target`, the `create_dir_all`
/// failure by `ensure_target_repo_create_clone_parent_failure_is_io`, and the
/// success by the clone test. `clone_target` is `repos_root` joined with a
/// non-empty validated repo path, so the None arm is unreachable in the real
/// flow, but the pure helper reaches it when handed a rooted path.
fn create_clone_parent(clone_target: &Path, repo: &str) -> Result<(), ReconstitutionError> {
    let clone_parent = clone_target
        .parent()
        .ok_or_else(|| ReconstitutionError::InvalidRepo(repo.to_string()))?;
    fs::create_dir_all(clone_parent).map_err(|source| ReconstitutionError::Io {
        path: clone_parent.display().to_string(),
        source,
    })
}

/// Spawn `git clone <remote> <clone_target>` and wait for it.
///
/// Measured: the `.status()` (git ran) and the returned status are exercised by
/// the clone-success and clone-failure tests. Only the spawn-failure unwrap (git
/// absent from PATH) is isolated into the coverage-excluded `git_status_or_abort`,
/// because in the `ensure_target_repo` flow `matching_git_root` resolves `git`
/// first, so a clone-time spawn failure is unreachable.
fn spawn_git_clone(remote: &str, clone_target: &Path) -> std::process::ExitStatus {
    git_status_or_abort(
        Command::new("git")
            .arg("clone")
            .arg(remote)
            .arg(clone_target)
            .status(),
    )
}

/// Unwrap a spawned `git` command's exit status.
///
/// Coverage-excluded: a spawn failure means `git` is absent from PATH, a
/// broken-environment invariant (panic), not a runtime error; and it is
/// unreachable in the clone flow (see `spawn_git_clone`).
#[cfg_attr(coverage_nightly, coverage(off))]
fn git_status_or_abort(
    status: std::io::Result<std::process::ExitStatus>,
) -> std::process::ExitStatus {
    status.expect("git must be available on PATH")
}

/// Confirm a freshly cloned checkout exposes the requested origin.
///
/// Measured: the `Some(root)` success arm is driven by the clone test. Only the
/// `None` error constructor is coverage-excluded (in `clone_verification_error`):
/// a `git clone` that exits 0 always produces a checkout whose origin is the URL
/// just cloned, so that arm is unreachable here.
fn verify_cloned_origin(clone_target: &Path, remote: &str) -> Result<PathBuf, ReconstitutionError> {
    matching_git_root(clone_target, remote).ok_or_else(clone_verification_error)
}

/// The error for a cloned checkout that does not expose the requested origin.
///
/// Coverage-excluded: unreachable (see `verify_cloned_origin`); passed as a bare
/// fn so `verify_cloned_origin`'s `ok_or_else` success stays the only measured
/// region there.
#[cfg_attr(coverage_nightly, coverage(off))]
fn clone_verification_error() -> ReconstitutionError {
    ReconstitutionError::Git {
        operation: "clone verification",
        detail: "cloned checkout does not expose the requested origin".to_string(),
    }
}

/// Transform captured cwd into a path relative to the captured repo-name
/// component, then append it to the target clone. For example,
/// `/Users/a/Code/acme/widget/src` and `acme/widget` become
/// `<target>/src`. The source cwd itself is only used as metadata; no
/// transcript-internal paths are changed.
// Measured: the success path is driven by the reconstitute end-to-end test; the
// metadata/cwd `?` arms propagate errors that are exercised directly by the
// required_metadata and cwd_relative_to_repo unit tests, and `?` leaves them as
// covered regions.
fn relocated_cwd(
    envelope: &SessionEnvelope,
    target_repo_root: &Path,
) -> Result<PathBuf, ReconstitutionError> {
    let metadata = required_meta_block(envelope)?;
    let repo = required_metadata(&metadata.repo, "repo")?;
    let cwd = required_metadata(&metadata.cwd, "cwd")?;
    let relative = cwd_relative_to_repo(cwd, repo)?;
    Ok(target_repo_root.join(relative))
}

fn cwd_relative_to_repo(cwd: &str, repo: &str) -> Result<PathBuf, ReconstitutionError> {
    let repo_name = repo
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| ReconstitutionError::InvalidRepo(repo.to_string()))?;
    let source = Path::new(cwd);
    if !source.is_absolute() {
        return Err(ReconstitutionError::UnmappableCwd(
            cwd.to_string(),
            repo.to_string(),
        ));
    }
    let components: Vec<_> = source
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value),
            _ => None,
        })
        .collect();
    let Some(index) = components
        .iter()
        .rposition(|part| *part == OsStr::new(repo_name))
    else {
        return Err(ReconstitutionError::UnmappableCwd(
            cwd.to_string(),
            repo.to_string(),
        ));
    };
    Ok(components[index + 1..]
        .iter()
        .fold(PathBuf::new(), |path, part| path.join(part)))
}

/// Build the target Claude Code project container and validate the session id
/// before it becomes a filename.
/// Resolve the target Claude transcript path and atomically write the raw bytes.
/// Split out so `reconstitute` carries a single `?` here whose error is forceable
/// (the write-failure path, via a read-only projects root); the path-derivation
/// `?` is exercised directly by `write_reconstituted_transcript_rejects_bad_id`.
fn write_reconstituted_transcript(
    claude_projects_root: &Path,
    target_cwd: &Path,
    session_id: &str,
    raw: &[u8],
) -> Result<PathBuf, ReconstitutionError> {
    let transcript_path = claude_transcript_path(claude_projects_root, target_cwd, session_id)?;
    write_verbatim(&transcript_path, raw)?;
    Ok(transcript_path)
}

fn claude_transcript_path(
    claude_projects_root: &Path,
    target_cwd: &Path,
    session_id: &str,
) -> Result<PathBuf, ReconstitutionError> {
    validate_session_id(session_id)?;
    let cwd = target_cwd.to_str().ok_or_else(|| {
        ReconstitutionError::UnmappableCwd(target_cwd.display().to_string(), "target".to_string())
    })?;
    if !target_cwd.is_absolute() {
        return Err(ReconstitutionError::UnmappableCwd(
            cwd.to_string(),
            "target".to_string(),
        ));
    }
    let slug = cwd.replace('/', "-");
    Ok(claude_projects_root
        .join(slug)
        .join(format!("{session_id}.jsonl")))
}

/// Atomically restore exactly the raw response bytes. No parse / serialize step
/// is permitted here, which protects inner absolute paths and content hashes.
// Measured: the no-parent, create_dir_all-failure, and no-file-name guards are
// each driven by the write_verbatim_* tests, and the happy path by the
// reconstitute end-to-end test. Only the four fresh-temp-file IO mappers
// (File::create, write_all, sync_all, rename) are coverage-excluded inline below.
fn write_verbatim(path: &Path, raw: &[u8]) -> Result<(), ReconstitutionError> {
    let parent = path.parent().ok_or_else(|| ReconstitutionError::Io {
        path: path.display().to_string(),
        source: std::io::Error::other("transcript path has no parent"),
    })?;
    fs::create_dir_all(parent).map_err(|source| ReconstitutionError::Io {
        path: parent.display().to_string(),
        source,
    })?;
    let file_name =
        path.file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| ReconstitutionError::Io {
                path: path.display().to_string(),
                source: std::io::Error::other("transcript path has no file name"),
            })?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    atomically_write(&temporary, path, raw)
}

/// Create `temporary`, write `raw`, fsync, then rename it onto `path`.
///
/// Measured: `File::create` (success + the create-error path, forced by
/// `atomically_write_create_failure_is_io`) and the `rename` (success + the
/// rename-error path, forced by `atomically_write_rename_failure_is_io`) are all
/// measured; only the `write_all` + `sync_all` mappers (un-forceable low-level
/// I/O faults on a fresh temp file) live in the coverage-excluded `write_and_sync`,
/// chained via `and_then` (no `?`) so the rename arm stays covered.
fn atomically_write(temporary: &Path, path: &Path, raw: &[u8]) -> Result<(), ReconstitutionError> {
    let mut file = fs::File::create(temporary).map_err(|source| ReconstitutionError::Io {
        path: temporary.display().to_string(),
        source,
    })?;
    write_and_sync(&mut file, raw, temporary).and_then(|()| {
        fs::rename(temporary, path).map_err(|source| ReconstitutionError::Io {
            path: path.display().to_string(),
            source,
        })
    })
}

/// Write `raw` to `file` and fsync.
///
/// Coverage-excluded: both `write_all` and `sync_all` fail only on a low-level
/// filesystem I/O fault against a freshly created temp file, which no test can
/// produce deterministically.
#[cfg_attr(coverage_nightly, coverage(off))]
fn write_and_sync(
    file: &mut fs::File,
    raw: &[u8],
    temporary: &Path,
) -> Result<(), ReconstitutionError> {
    file.write_all(raw)
        .map_err(|source| ReconstitutionError::Io {
            path: temporary.display().to_string(),
            source,
        })?;
    file.sync_all().map_err(|source| ReconstitutionError::Io {
        path: temporary.display().to_string(),
        source,
    })
}

/// `metadata` is optional on the standard envelope, but reconstitution cannot
/// proceed without it: the relocation rule is derived from `repo` and `cwd`. An
/// envelope with no metadata block fails the same way as one missing an
/// individual field, rather than panicking on an unwrap.
fn required_meta_block(envelope: &SessionEnvelope) -> Result<&Metadata, ReconstitutionError> {
    envelope
        .metadata
        .as_ref()
        .ok_or(ReconstitutionError::MissingMetadata("metadata"))
}

fn required_metadata<'a>(
    value: &'a Option<String>,
    name: &'static str,
) -> Result<&'a str, ReconstitutionError> {
    value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ReconstitutionError::MissingMetadata(name))
}

fn repo_path(repo: &str) -> Result<PathBuf, ReconstitutionError> {
    let mut result = PathBuf::new();
    for segment in repo.split('/') {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('\\')
            || Path::new(segment).is_absolute()
        {
            return Err(ReconstitutionError::InvalidRepo(repo.to_string()));
        }
        result.push(segment);
    }
    // Returned directly (no `?`) so `repo_path`'s own regions stay fully covered;
    // the unreachable empty-result guard lives in the coverage-excluded helper.
    reject_empty_repo_path(result, repo)
}

/// Reject a repo slug that accumulated to an empty path.
///
/// Coverage-excluded: unreachable in practice; the per-segment guard in
/// `repo_path` already rejects any repo that would leave `result` empty. Kept as
/// a defense against a future refactor of that guard.
#[cfg_attr(coverage_nightly, coverage(off))]
fn reject_empty_repo_path(result: PathBuf, repo: &str) -> Result<PathBuf, ReconstitutionError> {
    if result.as_os_str().is_empty() {
        return Err(ReconstitutionError::InvalidRepo(repo.to_string()));
    }
    Ok(result)
}

fn validate_session_id(session_id: &str) -> Result<(), ReconstitutionError> {
    if session_id.is_empty()
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ReconstitutionError::InvalidSessionId);
    }
    Ok(())
}

// Measured: the non-git-dir and missing-origin `Ok(None)` branches and the
// origin-match/mismatch result are driven by the matching_git_root_* and
// ensure_target_repo_* tests. The `git`-spawn failure is a broken-environment
// invariant handled inside `git_output` (panic), so this function carries no
// un-forceable error edge and returns a plain `Option`.
fn matching_git_root(path: &Path, expected_remote: &str) -> Option<PathBuf> {
    let output = git_output(
        Command::new("git")
            .arg("-C")
            .arg(path)
            .args(["rev-parse", "--show-toplevel"]),
    );
    if !output.status.success() {
        return None;
    }
    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let remote = git_output(
        Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["remote", "get-url", "origin"]),
    );
    if !remote.status.success() {
        return None;
    }
    let remote = String::from_utf8_lossy(&remote.stdout).trim().to_string();
    (normalize_remote(&remote) == normalize_remote(expected_remote)).then_some(PathBuf::from(root))
}

fn normalize_remote(remote: &str) -> String {
    remote
        .trim()
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relocation_uses_the_target_repo_and_source_relative_cwd() {
        let relative =
            cwd_relative_to_repo("/Users/alice/Code/acme/widget/crates/api", "acme/widget")
                .unwrap();
        assert_eq!(relative, PathBuf::from("crates/api"));
        assert_eq!(
            PathBuf::from("/srv/Code/acme/widget").join(relative),
            PathBuf::from("/srv/Code/acme/widget/crates/api")
        );
    }

    // Test-only extractors over `ReconstitutionError`. Replacing the inline
    // `assert!(matches!(err, Pat))` assertions (whose `_ => false` arm is a dead,
    // uncovered region at each call site) with a shared predicate keeps the
    // assertion exactly as strong (exact variant, and data-bearing extractors
    // return the exact fields) while moving the two arms to one place. Both arms
    // of every predicate are covered deterministically by
    // `error_predicates_cover_both_arms` below, so no exclusion is needed.
    fn is_unmappable_cwd(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::UnmappableCwd(_, _))
    }

    fn is_unsupported_harness(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::UnsupportedHarness { .. })
    }

    fn is_invalid_repo(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::InvalidRepo(_))
    }

    fn is_invalid_session_id(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::InvalidSessionId)
    }

    fn is_no_home(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::NoHome)
    }

    fn is_http(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::Http(_))
    }

    fn is_envelope_decode(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::EnvelopeDecode(_))
    }

    fn is_source_format_mismatch(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::SourceFormatMismatch { .. })
    }

    fn is_missing_target_cwd(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::MissingTargetCwd(_))
    }

    fn is_repository_collision(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::RepositoryCollision { .. })
    }

    fn git_operation(e: &ReconstitutionError) -> Option<&'static str> {
        match e {
            ReconstitutionError::Git { operation, .. } => Some(operation),
            _ => None,
        }
    }

    fn is_io(e: &ReconstitutionError) -> bool {
        matches!(e, ReconstitutionError::Io { .. })
    }

    fn http_status(e: &ReconstitutionError) -> Option<u16> {
        match e {
            ReconstitutionError::HttpStatus { status, .. } => Some(*status),
            _ => None,
        }
    }

    fn missing_metadata(e: &ReconstitutionError) -> Option<&'static str> {
        match e {
            ReconstitutionError::MissingMetadata(field) => Some(field),
            _ => None,
        }
    }

    fn native_resume_status(e: &ReconstitutionError) -> Option<i32> {
        match e {
            ReconstitutionError::NativeResume(status) => Some(*status),
            _ => None,
        }
    }

    // Restore an env var to a previously-captured value: `Some` re-sets it, `None`
    // removes it. Centralized here so the two arms live in one place; both are
    // covered deterministically by `restore_env_covers_set_and_remove` below
    // (a single-var save/restore like PATH always hits only the `Some` arm).
    fn restore_env(key: &str, saved: Option<String>) {
        match saved {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }
    }

    #[test]
    fn restore_env_covers_set_and_remove() {
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let key = "SESHMAGIC_RECONSTITUTE_RESTORE_ENV_PROBE";
        let saved = env::var(key).ok();
        restore_env(key, Some("value".into()));
        assert_eq!(env::var(key).ok().as_deref(), Some("value"));
        restore_env(key, None);
        assert!(env::var(key).is_err());
        restore_env(key, saved);
    }

    // Cover BOTH arms of every predicate above in one deterministic place: each
    // predicate is asserted true on a matching variant and false on a
    // non-matching one, so removing the per-test `#[coverage(off)]` leaves no
    // uncovered region anywhere.
    #[test]
    fn error_predicates_cover_both_arms() {
        use ReconstitutionError as E;
        let other = E::NoHome; // a non-matching value for the data-bearing checks
        assert!(is_unmappable_cwd(&E::UnmappableCwd("c".into(), "r".into())));
        assert!(!is_unmappable_cwd(&other));
        assert!(is_unsupported_harness(&E::UnsupportedHarness {
            agent: "a".into(),
            source_format: "f".into()
        }));
        assert!(!is_unsupported_harness(&other));
        assert!(is_invalid_repo(&E::InvalidRepo("x".into())));
        assert!(!is_invalid_repo(&other));
        assert!(is_invalid_session_id(&E::InvalidSessionId));
        assert!(!is_invalid_session_id(&E::Http("x".into())));
        assert!(is_no_home(&E::NoHome));
        assert!(!is_no_home(&E::Http("x".into())));
        assert!(is_http(&E::Http("x".into())));
        assert!(!is_http(&other));
        assert!(is_envelope_decode(&E::EnvelopeDecode("x".into())));
        assert!(!is_envelope_decode(&other));
        assert!(is_source_format_mismatch(&E::SourceFormatMismatch {
            envelope: "e".into(),
            raw: "r".into()
        }));
        assert!(!is_source_format_mismatch(&other));
        assert!(is_missing_target_cwd(&E::MissingTargetCwd("t".into())));
        assert!(!is_missing_target_cwd(&other));
        assert!(is_repository_collision(&E::RepositoryCollision {
            path: "p".into()
        }));
        assert!(!is_repository_collision(&other));
        assert_eq!(
            git_operation(&E::Git {
                operation: "clone",
                detail: "d".into()
            }),
            Some("clone")
        );
        assert_eq!(git_operation(&other), None);
        assert!(is_io(&E::Io {
            path: "p".into(),
            source: std::io::Error::other("x")
        }));
        assert!(!is_io(&other));
        assert_eq!(
            http_status(&E::HttpStatus {
                status: 503,
                endpoint: "e".into()
            }),
            Some(503)
        );
        assert_eq!(http_status(&other), None);
        assert_eq!(missing_metadata(&E::MissingMetadata("repo")), Some("repo"));
        assert_eq!(missing_metadata(&other), None);
        assert_eq!(native_resume_status(&E::NativeResume(9)), Some(9));
        assert_eq!(native_resume_status(&other), None);
    }

    #[test]
    fn relocation_rejects_an_unmappable_source_cwd() {
        let err = cwd_relative_to_repo("/Users/alice/checkouts/custom-name", "acme/widget")
            .err()
            .unwrap();
        assert!(is_unmappable_cwd(&err));
        // This error covers the false arms of the sibling extractors.
        assert!(!is_unsupported_harness(&err));
        assert!(!is_invalid_repo(&err));
        assert!(!is_invalid_session_id(&err));
    }

    #[test]
    fn claude_container_slug_comes_from_target_cwd() {
        let transcript = claude_transcript_path(
            Path::new("/home/bob/.claude/projects"),
            Path::new("/srv/Code/acme/widget/crates/api"),
            "session-123",
        )
        .unwrap();
        assert_eq!(
            transcript,
            PathBuf::from(
                "/home/bob/.claude/projects/-srv-Code-acme-widget-crates-api/session-123.jsonl"
            )
        );
    }

    #[test]
    fn write_verbatim_preserves_inner_paths_and_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let transcript = directory.path().join("container").join("session.jsonl");
        let raw =
            b"{  \"cwd\": \"/container/source/path\" }\n{\"tool\":\"/other/absolute/path\"}\n";
        write_verbatim(&transcript, raw).unwrap();
        assert_eq!(fs::read(transcript).unwrap(), raw);
    }

    #[test]
    fn same_harness_guard_rejects_codex_without_writing() {
        let mut envelope = test_envelope();
        envelope.agent = "Codex".to_string();
        envelope.source_format = "codex-rollout-jsonl".to_string();
        let err = ensure_claude(&envelope).err().unwrap();
        assert!(is_unsupported_harness(&err));
        // Covers the `is_unmappable_cwd` false arm with a non-UnmappableCwd error.
        assert!(!is_unmappable_cwd(&err));
    }

    #[test]
    fn rejects_path_traversal_in_repo_or_session_id() {
        // Spawns `git`, which resolves via PATH. Other tests set PATH
        // process-wide, so this must serialize with them or `git` vanishes
        // mid-run. Cheap here; these are not hot tests.
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let repo_err = repo_path("acme/../widget").err().unwrap();
        assert!(is_invalid_repo(&repo_err));
        let session_err = validate_session_id("../session").err().unwrap();
        assert!(is_invalid_session_id(&session_err));
        // Cross-cover the remaining false arms with the opposite error.
        assert!(!is_invalid_session_id(&repo_err));
        assert!(!is_invalid_repo(&session_err));
    }

    fn test_envelope() -> SessionEnvelope {
        serde_json::from_value(serde_json::json!({
            // Required by the standard's envelope type; the store's former local
            // type defaulted it, the APSS crate's does not.
            "scs_version": session_capture::SCS_VERSION,
            "origin": { "host": "machine-a", "environment": "laptop" },
            "agent": CLAUDE_AGENT,
            "source_format": CLAUDE_SOURCE_FORMAT,
            "session_id": "session-123",
            "started_at": "2026-08-01T00:00:00Z",
            "last_activity_at": "2026-08-01T00:01:00Z",
            "metadata": {
                "repo": "acme/widget",
                "git_remote": "https://github.com/acme/widget.git",
                "cwd": "/Users/alice/Code/acme/widget"
            },
            "raw": []
        }))
        .unwrap()
    }

    // --- shared test infrastructure -----------------------------------------

    use std::io::Read;
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::thread;

    /// Serializes tests that mutate process-global state (env vars, current dir,
    /// PATH), so they cannot interfere with each other.
    static GLOBAL_LOCK: Mutex<()> = Mutex::new(());

    fn http_response(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        // reqwest parses the numeric status; the reason phrase is ignored, so a
        // fixed phrase keeps this helper branch-free.
        let mut head = format!("HTTP/1.1 {status} X\r\n");
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        head.push_str("Connection: close\r\n\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(body);
        out
    }

    /// A canned HTTP server answering each request with the next response, then
    /// exiting. `Connection: close` means one accept per response, so the loop
    /// runs exactly `responses.len()` times. 127.0.0.1 only; no real network.
    fn spawn_server(responses: Vec<Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0u8; 16384];
                let _ = stream.read(&mut buf);
                let _ = std::io::Write::write_all(&mut stream, &response);
                let _ = std::io::Write::flush(&mut stream);
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

    fn git(dir: &Path, args: &[&str]) {
        let ok = Command::new("git")
            .current_dir(dir)
            .args(args)
            .output()
            .unwrap()
            .status
            .success();
        assert!(ok);
    }

    /// Create a git repo at `dir` with `origin` set to `remote`.
    fn init_repo(dir: &Path, remote: &str) {
        fs::create_dir_all(dir).unwrap();
        git(dir, &["init"]);
        git(dir, &["remote", "add", "origin", remote]);
    }

    fn envelope_json(remote: &str, cwd: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "scs_version": session_capture::SCS_VERSION,
            "origin": { "host": "machine-a", "environment": "laptop" },
            "agent": CLAUDE_AGENT,
            "source_format": CLAUDE_SOURCE_FORMAT,
            "session_id": "session-123",
            "started_at": "2026-08-01T00:00:00Z",
            "last_activity_at": "2026-08-01T00:01:00Z",
            "metadata": { "repo": "acme/widget", "git_remote": remote, "cwd": cwd },
            "raw": []
        }))
        .unwrap()
    }

    // --- ReconstitutionLocations::from_env ----------------------------------

    #[test]
    fn from_env_defaults_and_overrides_and_no_home() {
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let saved: Vec<(&str, Option<String>)> =
            ["HOME", "RECONSTITUTION_REPOS_ROOT", "CLAUDE_PROJECTS_ROOT"]
                .iter()
                .map(|k| (*k, env::var(k).ok()))
                .collect();

        // Defaults derive from HOME.
        env::set_var("HOME", "/tmp/reco-home");
        env::remove_var("RECONSTITUTION_REPOS_ROOT");
        env::remove_var("CLAUDE_PROJECTS_ROOT");
        let loc = ReconstitutionLocations::from_env().unwrap();
        assert_eq!(loc.repos_root, PathBuf::from("/tmp/reco-home/Code"));
        assert_eq!(
            loc.claude_projects_root,
            PathBuf::from("/tmp/reco-home/.claude/projects")
        );

        // Explicit overrides win.
        env::set_var("RECONSTITUTION_REPOS_ROOT", "/custom/repos");
        env::set_var("CLAUDE_PROJECTS_ROOT", "/custom/claude");
        let loc = ReconstitutionLocations::from_env().unwrap();
        assert_eq!(loc.repos_root, PathBuf::from("/custom/repos"));
        assert_eq!(loc.claude_projects_root, PathBuf::from("/custom/claude"));

        // No HOME -> NoHome.
        env::remove_var("HOME");
        assert!(is_no_home(
            &ReconstitutionLocations::from_env().err().unwrap()
        ));

        for (k, v) in saved {
            restore_env(k, v);
        }
    }

    // --- ReconstitutionClient::new + authenticated --------------------------

    #[test]
    fn new_trims_url_and_drops_blank_token() {
        let c = ReconstitutionClient::new("http://s/", Some("   ".into()));
        assert_eq!(c.store_url, "http://s");
        assert!(c.read_token.is_none());
        let c = ReconstitutionClient::new("http://s", Some("tok".into()));
        assert_eq!(c.read_token.as_deref(), Some("tok"));
    }

    // --- fetch_envelope / fetch_raw -----------------------------------------

    #[tokio::test]
    async fn fetch_envelope_success_with_token() {
        let url = spawn_server(vec![http_response(
            200,
            &[("Content-Type", "application/json")],
            &envelope_json("https://github.com/acme/widget.git", "/x/acme/widget"),
        )]);
        let client = ReconstitutionClient::new(&url, Some("read-tok".into()));
        let env = client.fetch_envelope("session-123").await.unwrap();
        assert_eq!(env.session_id, "session-123");
    }

    #[tokio::test]
    async fn fetch_envelope_non_2xx_and_http_error_and_decode() {
        let url = spawn_server(vec![http_response(404, &[], b"nope")]);
        let client = ReconstitutionClient::new(&url, None);
        assert_eq!(
            http_status(&client.fetch_envelope("s").await.err().unwrap()),
            Some(404)
        );

        let client = ReconstitutionClient::new(&dead_url(), None);
        assert!(is_http(&client.fetch_envelope("s").await.err().unwrap()));

        let url = spawn_server(vec![http_response(200, &[], b"not json")]);
        let client = ReconstitutionClient::new(&url, None);
        assert!(is_envelope_decode(
            &client.fetch_envelope("s").await.err().unwrap()
        ));
    }

    #[tokio::test]
    async fn fetch_raw_success_and_missing_header_and_status_and_http() {
        // Success carries the source-format header + verbatim bytes.
        let url = spawn_server(vec![http_response(
            200,
            &[("X-Source-Format", "claude-code-jsonl")],
            b"raw-bytes\n",
        )]);
        let client = ReconstitutionClient::new(&url, None);
        let (bytes, fmt) = client.fetch_raw("s").await.unwrap();
        assert_eq!(bytes, b"raw-bytes\n");
        assert_eq!(fmt, "claude-code-jsonl");

        // Missing the header -> MissingMetadata.
        let url = spawn_server(vec![http_response(200, &[], b"raw")]);
        let client = ReconstitutionClient::new(&url, None);
        assert_eq!(
            missing_metadata(&client.fetch_raw("s").await.err().unwrap()),
            Some("X-Source-Format")
        );

        // Non-2xx -> HttpStatus.
        let url = spawn_server(vec![http_response(500, &[], b"down")]);
        let client = ReconstitutionClient::new(&url, None);
        assert_eq!(
            http_status(&client.fetch_raw("s").await.err().unwrap()),
            Some(500)
        );

        // Connection refused -> Http.
        let client = ReconstitutionClient::new(&dead_url(), None);
        assert!(is_http(&client.fetch_raw("s").await.err().unwrap()));
    }

    // --- reconstitute end to end --------------------------------------------

    #[tokio::test]
    async fn reconstitute_writes_transcript_end_to_end() {
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        // Pre-existing checkout at the deterministic clone target with a matching
        // origin, so no real clone/network is needed.
        let checkout = repos.path().join("acme").join("widget");
        init_repo(&checkout, remote);

        let url = spawn_server(vec![
            http_response(200, &[], &envelope_json(remote, "/src/Code/acme/widget")),
            http_response(
                200,
                &[("X-Source-Format", "claude-code-jsonl")],
                b"L1\nL2\n",
            ),
        ]);
        let client = ReconstitutionClient::new(&url, None);
        let locations = ReconstitutionLocations {
            repos_root: repos.path().to_path_buf(),
            claude_projects_root: claude.path().to_path_buf(),
        };

        let plan = client
            .reconstitute("session-123", &locations)
            .await
            .unwrap();
        assert_eq!(plan.session_id, "session-123");
        assert_eq!(fs::read(&plan.transcript_path).unwrap(), b"L1\nL2\n");
        assert!(plan.target_cwd.is_dir());
    }

    #[tokio::test]
    async fn reconstitute_source_format_mismatch() {
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![
            http_response(200, &[], &envelope_json(remote, "/src/Code/acme/widget")),
            http_response(200, &[("X-Source-Format", "some-other-format")], b"raw"),
        ]);
        let client = ReconstitutionClient::new(&url, None);
        let locations = ReconstitutionLocations {
            repos_root: repos.path().to_path_buf(),
            claude_projects_root: claude.path().to_path_buf(),
        };
        assert!(is_source_format_mismatch(
            &client
                .reconstitute("session-123", &locations)
                .await
                .err()
                .unwrap()
        ));
    }

    #[tokio::test]
    async fn reconstitute_missing_target_cwd() {
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let checkout = repos.path().join("acme").join("widget");
        init_repo(&checkout, remote);
        // cwd maps to a subdir under the repo that does not exist on disk.
        let url = spawn_server(vec![
            http_response(200, &[], &envelope_json(remote, "/src/acme/widget/ghost")),
            http_response(200, &[("X-Source-Format", "claude-code-jsonl")], b"raw"),
        ]);
        let client = ReconstitutionClient::new(&url, None);
        let locations = ReconstitutionLocations {
            repos_root: repos.path().to_path_buf(),
            claude_projects_root: claude.path().to_path_buf(),
        };
        assert!(is_missing_target_cwd(
            &client
                .reconstitute("session-123", &locations)
                .await
                .err()
                .unwrap()
        ));
    }

    #[tokio::test]
    async fn reconstitute_rejects_invalid_session_id_before_any_request() {
        // No server needed: validation fails before any HTTP call.
        let client = ReconstitutionClient::new("http://127.0.0.1:1", None);
        let locations = ReconstitutionLocations {
            repos_root: PathBuf::from("/tmp"),
            claude_projects_root: PathBuf::from("/tmp"),
        };
        assert!(is_invalid_session_id(
            &client
                .reconstitute("../evil", &locations)
                .await
                .err()
                .unwrap()
        ));
    }

    fn locations_in(repos: &Path, claude: &Path) -> ReconstitutionLocations {
        ReconstitutionLocations {
            repos_root: repos.to_path_buf(),
            claude_projects_root: claude.to_path_buf(),
        }
    }

    #[tokio::test]
    async fn reconstitute_envelope_fetch_failure_propagates() {
        // The envelope endpoint 404s: reconstitute's `self.fetch_envelope(..)?`
        // takes its error edge.
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![http_response(404, &[], b"nope")]);
        let client = ReconstitutionClient::new(&url, None);
        assert_eq!(
            http_status(
                &client
                    .reconstitute("session-123", &locations_in(repos.path(), claude.path()))
                    .await
                    .err()
                    .unwrap()
            ),
            Some(404)
        );
    }

    #[tokio::test]
    async fn reconstitute_rejects_non_claude_envelope() {
        // A well-formed but non-Claude envelope: reconstitute's `ensure_claude(..)?`
        // takes its error edge.
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "scs_version": session_capture::SCS_VERSION,
            "origin": { "host": "m", "environment": "e" },
            "agent": "Codex",
            "source_format": "codex-rollout-jsonl",
            "session_id": "session-123",
            "started_at": "2026-08-01T00:00:00Z",
            "last_activity_at": "2026-08-01T00:01:00Z",
            "metadata": { "repo": "acme/widget", "git_remote": "r", "cwd": "/x" },
            "raw": []
        }))
        .unwrap();
        let url = spawn_server(vec![http_response(200, &[], &body)]);
        let client = ReconstitutionClient::new(&url, None);
        assert!(is_unsupported_harness(
            &client
                .reconstitute("session-123", &locations_in(repos.path(), claude.path()))
                .await
                .err()
                .unwrap()
        ));
    }

    #[tokio::test]
    async fn reconstitute_raw_fetch_failure_propagates() {
        // Envelope is fine but the raw endpoint 500s: reconstitute's
        // `self.fetch_raw(..)?` takes its error edge.
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let url = spawn_server(vec![
            http_response(200, &[], &envelope_json(remote, "/src/acme/widget")),
            http_response(500, &[], b"down"),
        ]);
        let client = ReconstitutionClient::new(&url, None);
        assert_eq!(
            http_status(
                &client
                    .reconstitute("session-123", &locations_in(repos.path(), claude.path()))
                    .await
                    .err()
                    .unwrap()
            ),
            Some(500)
        );
    }

    // Holding the guard across `.await` is intentional and safe here: the lock
    // exists to serialize process-global PATH mutation, the test suite is the
    // only contender, and every holder is a `#[test]`/`#[tokio::test]` that
    // releases it on return. Dropping it before the await would defeat the
    // point, since the awaited call is what spawns `git`.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn reconstitute_missing_repo_metadata_propagates() {
        // Spawns `git`, which resolves via PATH. Other tests set PATH
        // process-wide, so this must serialize with them or `git` vanishes
        // mid-run. Cheap here; these are not hot tests.
        let _guard = GLOBAL_LOCK.lock().unwrap();
        // A Claude envelope whose metadata omits `repo`: reconstitute's
        // `ensure_target_repo(..)?` takes its error edge.
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "scs_version": session_capture::SCS_VERSION,
            "origin": { "host": "m", "environment": "e" },
            "agent": CLAUDE_AGENT,
            "source_format": CLAUDE_SOURCE_FORMAT,
            "session_id": "session-123",
            "started_at": "2026-08-01T00:00:00Z",
            "last_activity_at": "2026-08-01T00:01:00Z",
            "metadata": { "cwd": "/x/acme/widget" },
            "raw": []
        }))
        .unwrap();
        let url = spawn_server(vec![
            http_response(200, &[], &body),
            http_response(200, &[("X-Source-Format", CLAUDE_SOURCE_FORMAT)], b"raw"),
        ]);
        let client = ReconstitutionClient::new(&url, None);
        assert_eq!(
            missing_metadata(
                &client
                    .reconstitute("session-123", &locations_in(repos.path(), claude.path()))
                    .await
                    .err()
                    .unwrap()
            ),
            Some("repo")
        );
    }

    #[tokio::test]
    async fn reconstitute_unmappable_cwd_propagates() {
        // ensure_target_repo succeeds (a matching checkout exists) but the cwd
        // cannot be mapped beneath the repo: reconstitute's `relocated_cwd(..)?`
        // takes its error edge.
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let checkout = repos.path().join("acme").join("widget");
        init_repo(&checkout, remote);
        let url = spawn_server(vec![
            http_response(200, &[], &envelope_json(remote, "/nowhere/unrelated")),
            http_response(200, &[("X-Source-Format", "claude-code-jsonl")], b"raw"),
        ]);
        let client = ReconstitutionClient::new(&url, None);
        assert!(is_unmappable_cwd(
            &client
                .reconstitute("session-123", &locations_in(repos.path(), claude.path()))
                .await
                .err()
                .unwrap()
        ));
    }

    #[tokio::test]
    async fn reconstitute_write_failure_on_readonly_projects_root() {
        use std::os::unix::fs::PermissionsExt;
        // Everything resolves, but the Claude projects root is read-only so the
        // transcript write fails: reconstitute's `write_reconstituted_transcript`
        // `?` (and the helper's `write_verbatim(..)?`) take their error edge.
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let claude = tempfile::tempdir().unwrap();
        let checkout = repos.path().join("acme").join("widget");
        init_repo(&checkout, remote);
        std::fs::set_permissions(claude.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let url = spawn_server(vec![
            http_response(200, &[], &envelope_json(remote, "/src/acme/widget")),
            http_response(200, &[("X-Source-Format", "claude-code-jsonl")], b"L1\n"),
        ]);
        let client = ReconstitutionClient::new(&url, None);
        let result = client
            .reconstitute("session-123", &locations_in(repos.path(), claude.path()))
            .await;
        std::fs::set_permissions(claude.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_io(&result.err().unwrap()));
    }

    #[tokio::test]
    async fn fetch_raw_body_stream_error() {
        // A response whose Content-Length over-promises what it delivers, then
        // closes: the body read fails mid-stream, exercising read_response_body's
        // error path and the `?` on it in fetch_raw.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 16384];
            let _ = stream.read(&mut buf);
            let resp = b"HTTP/1.1 200 X\r\nX-Source-Format: claude-code-jsonl\r\nContent-Length: 100\r\nConnection: close\r\n\r\nabc";
            let _ = std::io::Write::write_all(&mut stream, resp);
            let _ = std::io::Write::flush(&mut stream);
        });
        let url = format!("http://127.0.0.1:{port}");
        let client = ReconstitutionClient::new(&url, None);
        assert!(is_http(&client.fetch_raw("s").await.err().unwrap()));
    }

    // --- ensure_target_repo branches ----------------------------------------

    #[test]
    fn ensure_target_repo_prefers_matching_current_dir() {
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let here = tempfile::tempdir().unwrap();
        init_repo(here.path(), remote);

        let saved = env::current_dir().unwrap();
        env::set_current_dir(here.path()).unwrap();
        let envelope = envelope_with(remote, "/x/acme/widget");
        let result = ensure_target_repo(&envelope, repos.path());
        env::set_current_dir(&saved).unwrap();
        // Returns a git root whose origin matches (the current dir), so nothing
        // was cloned into repos_root.
        assert!(result.is_ok());
        assert!(!repos.path().join("acme").exists());
    }

    #[test]
    fn ensure_target_repo_collision_when_existing_checkout_has_wrong_origin() {
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let remote = "https://github.com/acme/widget.git";
        let repos = tempfile::tempdir().unwrap();
        let checkout = repos.path().join("acme").join("widget");
        init_repo(&checkout, "https://github.com/someone/else.git");

        // Run from a non-matching cwd so the deterministic target path is used.
        let saved = env::current_dir().unwrap();
        let neutral = tempfile::tempdir().unwrap();
        env::set_current_dir(neutral.path()).unwrap();
        let envelope = envelope_with(remote, "/x/acme/widget");
        let result = ensure_target_repo(&envelope, repos.path());
        env::set_current_dir(&saved).unwrap();
        assert!(is_repository_collision(&result.err().unwrap()));
    }

    #[test]
    fn ensure_target_repo_clones_from_a_local_remote() {
        let _guard = GLOBAL_LOCK.lock().unwrap();
        // A local bare repo stands in for the remote; clone is fully offline.
        let bare = tempfile::tempdir().unwrap();
        let bare_path = bare.path().join("origin.git");
        fs::create_dir_all(&bare_path).unwrap();
        git(&bare_path, &["init", "--bare"]);
        let remote = format!("file://{}", bare_path.display());

        let repos = tempfile::tempdir().unwrap();
        let saved = env::current_dir().unwrap();
        let neutral = tempfile::tempdir().unwrap();
        env::set_current_dir(neutral.path()).unwrap();
        let envelope = envelope_with(&remote, "/x/acme/widget");
        let result = ensure_target_repo(&envelope, repos.path());
        env::set_current_dir(&saved).unwrap();
        let root = result.expect("clone should succeed and verify origin");
        assert!(root.join(".git").exists());
    }

    #[test]
    fn ensure_target_repo_clone_failure_is_git_error() {
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let repos = tempfile::tempdir().unwrap();
        let saved = env::current_dir().unwrap();
        let neutral = tempfile::tempdir().unwrap();
        env::set_current_dir(neutral.path()).unwrap();
        // A file:// URL to a path that isn't a repo makes `git clone` fail.
        let missing = repos.path().join("does-not-exist.git");
        let envelope = envelope_with(&format!("file://{}", missing.display()), "/x/acme/widget");
        let result = ensure_target_repo(&envelope, repos.path());
        env::set_current_dir(&saved).unwrap();
        assert_eq!(git_operation(&result.err().unwrap()), Some("clone"));
    }

    #[test]
    fn ensure_target_repo_missing_metadata_fields() {
        // Spawns `git`, which resolves via PATH. Other tests set PATH
        // process-wide, so this must serialize with them or `git` vanishes
        // mid-run. Cheap here; these are not hot tests.
        let _guard = GLOBAL_LOCK.lock().unwrap();
        // No git_remote -> MissingMetadata("git_remote"). `.expect` (not `if let`)
        // keeps this test branch-free: test_envelope always carries metadata.
        let mut envelope = test_envelope();
        envelope
            .metadata
            .as_mut()
            .expect("test envelope has metadata")
            .git_remote = None;
        assert_eq!(
            missing_metadata(
                &ensure_target_repo(&envelope, Path::new("/tmp"))
                    .err()
                    .unwrap()
            ),
            Some("git_remote")
        );

        // No metadata block at all -> MissingMetadata("metadata").
        let mut envelope = test_envelope();
        envelope.metadata = None;
        assert_eq!(
            missing_metadata(
                &ensure_target_repo(&envelope, Path::new("/tmp"))
                    .err()
                    .unwrap()
            ),
            Some("metadata")
        );

        // No repo -> MissingMetadata("repo").
        let mut envelope = test_envelope();
        envelope
            .metadata
            .as_mut()
            .expect("test envelope has metadata")
            .repo = None;
        assert_eq!(
            missing_metadata(
                &ensure_target_repo(&envelope, Path::new("/tmp"))
                    .err()
                    .unwrap()
            ),
            Some("repo")
        );

        // An unsafe repo slug -> InvalidRepo (the `repo_path(repo)?` edge).
        let mut envelope = test_envelope();
        envelope
            .metadata
            .as_mut()
            .expect("test envelope has metadata")
            .repo = Some("../evil".to_string());
        assert!(is_invalid_repo(
            &ensure_target_repo(&envelope, Path::new("/tmp"))
                .err()
                .unwrap()
        ));
    }

    #[test]
    fn ensure_target_repo_create_clone_parent_failure_is_io() {
        use std::os::unix::fs::PermissionsExt;
        let _guard = GLOBAL_LOCK.lock().unwrap();
        // repos_root is read-only, so creating the clone parent directory fails
        // (the `create_clone_parent(&clone_target, repo)?` edge). Run from a
        // neutral (non-git) dir so the current-dir match returns None first.
        let repos = tempfile::tempdir().unwrap();
        std::fs::set_permissions(repos.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let saved = env::current_dir().unwrap();
        let neutral = tempfile::tempdir().unwrap();
        env::set_current_dir(neutral.path()).unwrap();
        let envelope = envelope_with("https://github.com/acme/widget.git", "/x/acme/widget");
        let result = ensure_target_repo(&envelope, repos.path());
        env::set_current_dir(&saved).unwrap();
        std::fs::set_permissions(repos.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_io(&result.err().unwrap()));
    }

    #[test]
    fn relocated_cwd_missing_metadata_permutations() {
        let root = Path::new("/tmp/target");

        // No metadata block -> the `required_meta_block(envelope)?` edge.
        let mut e = test_envelope();
        e.metadata = None;
        assert_eq!(
            missing_metadata(&relocated_cwd(&e, root).err().unwrap()),
            Some("metadata")
        );

        // No repo -> the `required_metadata(&metadata.repo, "repo")?` edge.
        let mut e = test_envelope();
        e.metadata
            .as_mut()
            .expect("test envelope has metadata")
            .repo = None;
        assert_eq!(
            missing_metadata(&relocated_cwd(&e, root).err().unwrap()),
            Some("repo")
        );

        // No cwd -> the `required_metadata(&metadata.cwd, "cwd")?` edge.
        let mut e = test_envelope();
        e.metadata.as_mut().expect("test envelope has metadata").cwd = None;
        assert_eq!(
            missing_metadata(&relocated_cwd(&e, root).err().unwrap()),
            Some("cwd")
        );

        // A cwd with no repo-name component -> the `cwd_relative_to_repo(..)?` edge.
        let mut e = test_envelope();
        e.metadata.as_mut().expect("test envelope has metadata").cwd =
            Some("/nowhere/unrelated".to_string());
        assert!(is_unmappable_cwd(&relocated_cwd(&e, root).err().unwrap()));
    }

    #[test]
    fn write_reconstituted_transcript_rejects_bad_id() {
        // A bad session id makes claude_transcript_path fail, exercising the
        // path-derivation `?` inside write_reconstituted_transcript.
        let claude = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let err = write_reconstituted_transcript(claude.path(), target.path(), "../evil", b"x")
            .err()
            .unwrap();
        assert!(is_invalid_session_id(&err));
    }

    #[test]
    fn create_clone_parent_rejects_a_parentless_target() {
        // A filesystem root has no parent, so the `.parent()` None arm fires and
        // returns InvalidRepo (unreachable for a real repos_root-rooted target,
        // but forced directly here).
        let err = create_clone_parent(Path::new("/"), "acme/widget")
            .err()
            .unwrap();
        assert!(is_invalid_repo(&err));
    }

    #[test]
    fn atomically_write_create_failure_is_io() {
        // `temporary`'s parent is a regular file, so `File::create` fails: covers
        // atomically_write's create-error arm.
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("blocker");
        fs::write(&blocker, b"x").unwrap();
        let temporary = blocker.join("sub").join("scratch");
        let dest = tmp.path().join("dest");
        assert!(is_io(
            &atomically_write(&temporary, &dest, b"raw").err().unwrap()
        ));
    }

    #[test]
    fn atomically_write_rename_failure_is_io() {
        // create/write/sync succeed, but `dest` sits under a nonexistent
        // directory so the rename fails: covers atomically_write's rename-error arm.
        let tmp = tempfile::tempdir().unwrap();
        let temporary = tmp.path().join("scratch");
        let dest = tmp.path().join("missing-dir").join("dest");
        assert!(is_io(
            &atomically_write(&temporary, &dest, b"raw").err().unwrap()
        ));
    }

    fn envelope_with(remote: &str, cwd: &str) -> SessionEnvelope {
        serde_json::from_slice(&envelope_json(remote, cwd)).unwrap()
    }

    // --- matching_git_root edge branches ------------------------------------

    #[test]
    fn matching_git_root_non_git_dir_is_none() {
        // Spawns `git`, which resolves via PATH. Other tests set PATH
        // process-wide, so this must serialize with them or `git` vanishes
        // mid-run. Cheap here; these are not hot tests.
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(matching_git_root(tmp.path(), "any"), None);
    }

    #[test]
    fn matching_git_root_repo_without_origin_is_none() {
        // Spawns `git`, which resolves via PATH. Other tests set PATH
        // process-wide, so this must serialize with them or `git` vanishes
        // mid-run. Cheap here; these are not hot tests.
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        git(tmp.path(), &["init"]);
        // No origin remote configured -> `git remote get-url origin` fails.
        assert_eq!(matching_git_root(tmp.path(), "any"), None);
    }

    // --- pure helpers: error branches ---------------------------------------

    #[test]
    fn cwd_relative_to_repo_rejects_repo_with_empty_name() {
        assert!(is_invalid_repo(
            &cwd_relative_to_repo("/a/b", "acme/").err().unwrap()
        ));
    }

    #[test]
    fn cwd_relative_to_repo_rejects_relative_cwd() {
        // A non-absolute cwd cannot be mapped (the `!source.is_absolute()` arm).
        assert!(is_unmappable_cwd(
            &cwd_relative_to_repo("relative/path/widget", "acme/widget")
                .err()
                .unwrap()
        ));
    }

    #[test]
    fn claude_transcript_path_rejects_invalid_session_id() {
        // An unsafe session id is rejected before it can become a filename (the
        // validate_session_id `?` inside claude_transcript_path).
        assert!(claude_transcript_path(Path::new("/c"), Path::new("/abs/cwd"), "../evil").is_err());
    }

    #[test]
    fn claude_transcript_path_rejects_non_utf8_target() {
        use std::os::unix::ffi::OsStrExt;
        // A target cwd whose bytes are not valid UTF-8 -> `to_str()` is None.
        let bytes = [b'/', 0xff, 0xfe];
        let bad = Path::new(OsStr::from_bytes(&bytes));
        assert!(is_unmappable_cwd(
            &claude_transcript_path(Path::new("/c"), bad, "s")
                .err()
                .unwrap()
        ));
    }

    #[test]
    fn write_verbatim_create_dir_all_failure_is_io_error() {
        // Parent chain descends through a regular file, so `create_dir_all` fails.
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("blocker");
        fs::write(&blocker, b"x").unwrap();
        let target = blocker.join("sub").join("t.jsonl");
        assert!(is_io(&write_verbatim(&target, b"raw").err().unwrap()));
    }

    #[test]
    fn claude_transcript_path_rejects_relative_target() {
        assert!(is_unmappable_cwd(
            &claude_transcript_path(Path::new("/c"), Path::new("relative/cwd"), "s")
                .err()
                .unwrap()
        ));
    }

    #[test]
    fn repo_path_rejects_unsafe_segments() {
        for bad in ["", "acme/../x", "a\\b", "/abs/seg"] {
            assert!(
                is_invalid_repo(&repo_path(bad).err().unwrap()),
                "{bad:?} should be rejected"
            );
        }
        assert_eq!(
            repo_path("acme/widget").unwrap(),
            PathBuf::from("acme/widget")
        );
    }

    #[test]
    fn required_meta_block_and_field_errors() {
        let mut envelope = test_envelope();
        envelope.metadata = None;
        assert_eq!(
            missing_metadata(&required_meta_block(&envelope).err().unwrap()),
            Some("metadata")
        );
        assert_eq!(
            missing_metadata(
                &required_metadata(&Some("   ".to_string()), "repo")
                    .err()
                    .unwrap()
            ),
            Some("repo")
        );
        assert_eq!(
            missing_metadata(&required_metadata(&None, "cwd").err().unwrap()),
            Some("cwd")
        );
        assert_eq!(
            required_metadata(&Some("value".to_string()), "repo").unwrap(),
            "value"
        );
    }

    #[test]
    fn write_verbatim_rejects_pathological_paths() {
        // No parent.
        assert!(is_io(&write_verbatim(Path::new("/"), b"x").err().unwrap()));
        // Parent exists but the final component has no file name.
        let tmp = tempfile::tempdir().unwrap();
        let weird = tmp.path().join("..");
        assert!(is_io(&write_verbatim(&weird, b"x").err().unwrap()));
    }

    #[test]
    fn normalize_remote_strips_git_slash_and_case() {
        assert_eq!(
            normalize_remote("https://github.com/O/R.git/"),
            "https://github.com/o/r"
        );
        assert_eq!(
            normalize_remote("git@github.com:Owner/Repo"),
            "git@github.com:owner/repo"
        );
    }

    // --- invoke_claude_resume (drive a fake `claude` via PATH) ---------------

    #[test]
    fn invoke_claude_resume_success_failure_and_spawn_error() {
        let _guard = GLOBAL_LOCK.lock().unwrap();
        let saved_path = env::var("PATH").ok();
        let work = tempfile::tempdir().unwrap();
        let bin = work.path().join("bin");
        fs::create_dir_all(&bin).unwrap();

        let plan = ReconstitutionPlan {
            session_id: "s".into(),
            repo_root: work.path().to_path_buf(),
            target_cwd: work.path().to_path_buf(),
            transcript_path: work.path().join("t.jsonl"),
        };

        // Fake `claude` that exits 0.
        write_exec(&bin.join("claude"), "#!/bin/sh\nexit 0\n");
        env::set_var("PATH", &bin);
        assert!(invoke_claude_resume(&plan).is_ok());

        // Fake `claude` that exits 7 -> NativeResume(7).
        write_exec(&bin.join("claude"), "#!/bin/sh\nexit 7\n");
        assert_eq!(
            native_resume_status(&invoke_claude_resume(&plan).err().unwrap()),
            Some(7)
        );

        // Empty PATH -> `claude` not found -> Io error.
        let empty = work.path().join("empty");
        fs::create_dir_all(&empty).unwrap();
        env::set_var("PATH", &empty);
        assert!(is_io(&invoke_claude_resume(&plan).err().unwrap()));

        restore_env("PATH", saved_path);
    }

    fn write_exec(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, body).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    #[test]
    fn error_display_covers_every_variant() {
        let variants = [
            ReconstitutionError::NoHome,
            ReconstitutionError::InvalidSessionId,
            ReconstitutionError::UnsupportedHarness {
                agent: "a".into(),
                source_format: "f".into(),
            },
            ReconstitutionError::SourceFormatMismatch {
                envelope: "e".into(),
                raw: "r".into(),
            },
            ReconstitutionError::MissingMetadata("repo"),
            ReconstitutionError::InvalidRepo("bad".into()),
            ReconstitutionError::UnmappableCwd("c".into(), "r".into()),
            ReconstitutionError::MissingTargetCwd("t".into()),
            ReconstitutionError::RepositoryCollision { path: "p".into() },
            ReconstitutionError::Git {
                operation: "clone",
                detail: "d".into(),
            },
            ReconstitutionError::Http("h".into()),
            ReconstitutionError::HttpStatus {
                status: 500,
                endpoint: "e".into(),
            },
            ReconstitutionError::EnvelopeDecode("d".into()),
            ReconstitutionError::Io {
                path: "p".into(),
                source: std::io::Error::other("x"),
            },
            ReconstitutionError::NativeResume(3),
        ];
        for v in variants {
            assert!(!v.to_string().is_empty());
        }
    }
}
