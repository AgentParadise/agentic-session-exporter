//! Derive `metadata.repo` and `metadata.git_remote` from a transcript's cwd.
//!
//! Pure filesystem reads (no `git` subprocess): walk up from the cwd to the
//! nearest `.git`, read `.git/config` for the `origin` remote URL, and normalize
//! it into an `owner/repo` slug. Best-effort: any failure yields `None` so the
//! exporter degrades gracefully (a session without a resolvable repo is still
//! captured).

use std::path::{Path, PathBuf};

/// The repo facts we can recover from a working directory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoInfo {
    /// `owner/repo` slug when derivable from the origin remote.
    pub repo: Option<String>,
    /// The raw origin remote URL.
    pub git_remote: Option<String>,
}

/// Resolve repo info for a working directory. Returns an empty `RepoInfo` (all
/// `None`) when `cwd` is not inside a git repo or the config cannot be read.
pub fn resolve(cwd: &Path) -> RepoInfo {
    let Some(git_dir) = find_git_dir(cwd) else {
        return RepoInfo::default();
    };
    let config = git_dir.join("config");
    let Ok(text) = std::fs::read_to_string(&config) else {
        return RepoInfo::default();
    };
    let git_remote = parse_origin_url(&text);
    let repo = git_remote.as_deref().and_then(slug_from_remote);
    RepoInfo { repo, git_remote }
}

/// Walk up from `start` looking for a `.git` directory. Bounded by filesystem
/// root. Returns the `.git` directory path.
fn find_git_dir(start: &Path) -> Option<PathBuf> {
    let mut dir = Some(start);
    while let Some(d) = dir {
        let candidate = d.join(".git");
        if candidate.is_dir() {
            return Some(candidate);
        }
        dir = d.parent();
    }
    None
}

/// Extract the `[remote "origin"] url = ...` value from a git config body.
/// A tiny INI walk: track the current section, capture `url` under origin.
fn parse_origin_url(config: &str) -> Option<String> {
    let mut in_origin = false;
    for line in config.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_origin = line.replace(' ', "") == "[remote\"origin\"]";
            continue;
        }
        if in_origin {
            if let Some(rest) = line.strip_prefix("url") {
                let rest = rest.trim_start();
                if let Some(v) = rest.strip_prefix('=') {
                    let url = v.trim();
                    if !url.is_empty() {
                        return Some(url.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Normalize a git remote URL into an `owner/repo` slug.
///
/// Handles the common forms:
///   git@github.com:owner/repo.git
///   https://github.com/owner/repo.git
///   ssh://git@host/owner/repo
fn slug_from_remote(url: &str) -> Option<String> {
    // Strip a trailing `.git`.
    let url = url.strip_suffix(".git").unwrap_or(url);

    // scp-like syntax: git@host:owner/repo
    let path = if let Some((_, after_colon)) = url.rsplit_once(':') {
        // Only treat as scp-like if there is no `//` before the colon (which
        // would be a scheme). For https/ssh URLs we fall through to the last
        // two path segments below.
        if url.contains("://") {
            url_path_tail(url)?
        } else {
            after_colon.to_string()
        }
    } else {
        url_path_tail(url)?
    };

    let parts: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    if parts.len() >= 2 {
        let owner = parts[parts.len() - 2];
        let repo = parts[parts.len() - 1];
        Some(format!("{owner}/{repo}"))
    } else {
        None
    }
}

/// Return the path portion after the scheme+host of a URL-style remote.
fn url_path_tail(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    // Drop the host (first segment before the first `/`).
    after_scheme.split_once('/').map(|(_, p)| p.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_from_scp_form() {
        assert_eq!(
            slug_from_remote("git@github.com:seshmagic/seshmagic_session_store.git"),
            Some("seshmagic/seshmagic_session_store".to_string())
        );
    }

    #[test]
    fn slug_from_https_form() {
        assert_eq!(
            slug_from_remote("https://github.com/owner/repo.git"),
            Some("owner/repo".to_string())
        );
        assert_eq!(
            slug_from_remote("https://github.com/owner/repo"),
            Some("owner/repo".to_string())
        );
    }

    #[test]
    fn slug_from_ssh_url_form() {
        assert_eq!(
            slug_from_remote("ssh://git@host.example/team/project.git"),
            Some("team/project".to_string())
        );
    }

    #[test]
    fn slug_none_when_unparseable() {
        assert_eq!(slug_from_remote("weirdstring"), None);
    }

    #[test]
    fn parse_origin_url_finds_origin_only() {
        let cfg = r#"
[core]
    bare = false
[remote "upstream"]
    url = git@github.com:up/stream.git
[remote "origin"]
    url = git@github.com:me/mine.git
    fetch = +refs/heads/*:refs/remotes/origin/*
"#;
        assert_eq!(
            parse_origin_url(cfg),
            Some("git@github.com:me/mine.git".to_string())
        );
    }

    #[test]
    fn resolve_walks_up_to_git_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("proj");
        let nested = repo.join("src").join("deep");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        std::fs::write(
            repo.join(".git").join("config"),
            "[remote \"origin\"]\n    url = https://github.com/o/r.git\n",
        )
        .unwrap();

        let info = resolve(&nested);
        assert_eq!(info.repo.as_deref(), Some("o/r"));
        assert_eq!(
            info.git_remote.as_deref(),
            Some("https://github.com/o/r.git")
        );
    }

    #[test]
    fn resolve_empty_outside_repo() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(resolve(tmp.path()), RepoInfo::default());
    }

    #[test]
    fn resolve_default_when_git_dir_has_no_readable_config() {
        // A `.git` directory exists (find_git_dir succeeds) but there is no
        // config file to read -> the read_to_string Err branch returns default.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".git")).unwrap();
        assert_eq!(resolve(tmp.path()), RepoInfo::default());
    }

    #[test]
    fn parse_origin_url_none_variants() {
        // No origin section at all.
        assert_eq!(parse_origin_url("[core]\n bare = false\n"), None);
        // Origin section but the url value is empty.
        assert_eq!(parse_origin_url("[remote \"origin\"]\n url =\n"), None);
        // Origin section with a `url`-prefixed key that has no `=`.
        assert_eq!(
            parse_origin_url("[remote \"origin\"]\n url no-equals\n"),
            None
        );
    }

    #[test]
    fn slug_none_when_single_path_segment() {
        // A URL whose path has only one segment cannot form owner/repo.
        assert_eq!(slug_from_remote("https://host/single"), None);
    }

    #[test]
    fn slug_none_when_scheme_url_has_no_path() {
        // scheme://host with no `/path`: url_path_tail returns None (the `?` None
        // arm), so slug resolution yields None.
        assert_eq!(slug_from_remote("ssh://hostonly"), None);
    }

    #[test]
    fn parse_origin_url_skips_non_url_line_in_origin() {
        // An origin section whose first line is a non-`url` key (here `fetch`)
        // exercises the in-origin branch that does not start with `url`.
        let cfg = "[remote \"origin\"]\n    fetch = +refs/heads/*\n    url = git@h:o/r.git\n";
        assert_eq!(parse_origin_url(cfg), Some("git@h:o/r.git".to_string()));
    }
}
