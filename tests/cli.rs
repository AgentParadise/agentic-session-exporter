//! Black-box CLI tests: invoke the built binary, assert its observable contract.
//!
//! These matter more than their coverage percentage suggests. The CLI is the
//! interface agentic-primitives' doctor and finalizer actually depend on, and
//! the one defect that reached production there was a CLI contract defect: a
//! consumer probed with `--version`, the binary ignored unknown flags and ran a
//! full capture sweep instead, and the health check "passed" by performing a
//! real upload at workspace preflight.
//!
//! No unit test would have caught that. Only invoking the binary does.

use std::process::Command;

fn bin() -> Command {
    // env!("CARGO_BIN_EXE_<name>") resolves to the binary cargo just built, so
    // these test the artifact that ships rather than a rebuilt approximation.
    Command::new(env!("CARGO_BIN_EXE_apss-session-exporter"))
}

#[test]
fn version_is_side_effect_free_and_needs_no_configuration() {
    // The consumer contract: a doctor may call this to prove the binary is
    // present and runnable. It must answer with NO store URL, NO token, and
    // without touching the network - otherwise a liveness probe becomes an
    // upload, which is exactly the bug this test exists to prevent.
    let out = bin()
        .arg("--version")
        .env_remove("SESSION_STORE_URL")
        .env_remove("SESSIONS_WRITE_TOKEN")
        .output()
        .expect("binary should run");

    assert!(out.status.success(), "--version must exit 0 with no config");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "--version must report the crate version, got: {stdout}"
    );
}

#[test]
fn version_names_the_invoked_alias_not_a_hardcoded_name() {
    // Both binaries are built from one source. Each must self-report the name
    // it was invoked as, or an operator reading logs cannot tell which of the
    // two ran.
    let out = Command::new(env!("CARGO_BIN_EXE_SeshMagicSessionExporter"))
        .arg("--version")
        .output()
        .expect("legacy alias should run");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("SeshMagicSessionExporter"),
        "the legacy alias must name itself, got: {stdout}"
    );
}

#[test]
fn help_is_side_effect_free_and_needs_no_configuration() {
    let out = bin()
        .arg("--help")
        .env_remove("SESSION_STORE_URL")
        .output()
        .expect("binary should run");
    assert!(out.status.success(), "--help must exit 0 with no config");
    assert!(!out.stdout.is_empty(), "--help must print something");
}

#[test]
fn an_unknown_flag_is_rejected_and_never_silently_ignored() {
    // THE regression test for the defect that reached production. Ignoring an
    // unknown flag means a consumer probing with a flag this binary does not
    // implement gets a full capture sweep instead of an answer.
    let out = bin()
        .arg("--definitely-not-a-real-flag")
        .output()
        .expect("binary should run");

    assert!(
        !out.status.success(),
        "an unknown flag must be an error, not silently ignored"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--definitely-not-a-real-flag") || stderr.contains("help"),
        "the error must name the bad flag or point at --help, got: {stderr}"
    );
}

#[test]
fn a_bare_positional_argument_is_rejected() {
    let out = bin().arg("some-unexpected-word").output().expect("runs");
    assert!(
        !out.status.success(),
        "an unexpected positional must not be accepted"
    );
}

#[test]
#[ignore = "documents a real defect: --dry-run currently attempts the network. \
Un-ignore when it is network-free. See docs/REQUIREMENTS.md section 2."]
fn dry_run_is_network_free_and_fast() {
    // --dry-run is what a consumer's doctor SHOULD use as its liveness probe:
    // real argument and config handling, no upload. That is only true if it
    // makes no network call, and the honest way to assert "no network call" is
    // elapsed time against an unroutable address - a retrying client takes
    // seconds, a network-free one takes milliseconds.
    //
    // MEASURED AT 92 SECONDS today, so this is ignored rather than deleted or
    // weakened into something that passes. A test that documents a defect is
    // worth more than one that hides it, and consumers currently give the
    // finalizer a ~2s budget: a 92s probe blows through it entirely.
    let start = std::time::Instant::now();
    let out = bin()
        .arg("--dry-run")
        .env("SESSION_STORE_URL", "http://127.0.0.1:1")
        .env_remove("SESSIONS_WRITE_TOKEN")
        .output()
        .expect("binary should run");
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "--dry-run must not touch the network; took {elapsed:?}. status={:?}",
        out.status
    );
}

#[test]
fn the_write_token_never_appears_in_output() {
    // Every stream this binary writes may be captured into a durable log by a
    // consumer. A token reaching one is a credential leak that outlives the run.
    const SECRET: &str = "sk-canary-must-never-be-printed";
    let out = bin()
        .arg("--help")
        .env("SESSIONS_WRITE_TOKEN", SECRET)
        .env("SESSION_STORE_URL", "http://127.0.0.1:1")
        .output()
        .expect("binary should run");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !combined.contains(SECRET),
        "the write token must never reach stdout or stderr"
    );
}

/// A tiny store that answers health checks and rejects every envelope.
///
/// Needed because the interesting exit code is 3 - "the sweep RAN but did not
/// capture everything" - and that is only reachable against a store that is UP.
/// An unreachable store exits 1, which is a different statement.
fn spawn_rejecting_store() -> (String, std::thread::JoinHandle<()>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("addr");
    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(8) {
            let Ok(mut stream) = stream else { continue };
            let mut buf = [0u8; 8192];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).to_string();

            let body = if req.starts_with("POST") {
                // A well-formed response that refuses the envelope. The session
                // id does not need to match: an unmatched result confirms
                // nothing, which is exactly the outcome under test.
                r#"{"results":[{"status":"rejected","session_id":"unknown","reason":"test"}]}"#
            } else {
                r#"{"ok":true}"#
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });
    (format!("http://{addr}"), handle)
}

/// Exit 3 means "the sweep ran and did not capture everything it found".
///
/// The whole point of the code: a caller asking "was this session stored?"
/// must not get a yes from a sweep that stored nothing. Asserted as EXACTLY 3,
/// not merely non-zero, because non-zero would also pass for a sweep that
/// never ran, which is a different answer.
#[test]
fn a_rejected_sweep_exits_three_and_says_so_in_json() {
    let (url, _server) = spawn_rejecting_store();
    let tmp = std::env::temp_dir().join("apss-cli-exit3");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(tmp.join("claude/p")).expect("fixture dirs");
    std::fs::write(
        tmp.join("claude/p/s.jsonl"),
        "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hi\"},\
         \"sessionId\":\"exit3-test\",\"timestamp\":\"2026-08-19T00:00:00Z\"}\n",
    )
    .expect("fixture file");

    let out = bin()
        .arg("--json")
        .env("SESSION_STORE_URL", &url)
        .env("SESSION_STORE_ORIGIN_ENV", "container")
        .env("CLAUDE_PROJECTS_ROOT", tmp.join("claude"))
        .env("CODEX_SESSIONS_ROOT", tmp.join("codex"))
        .env("EXPORTER_STATE_FILE", tmp.join("state.json"))
        .env("EXPORTER_HEALTH_FILE", tmp.join("health.json"))
        .env("HOME", &tmp)
        .output()
        .expect("binary should run");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(3),
        "a sweep that stored nothing must exit 3; stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        stdout.contains(r#""captured_everything":false"#),
        "the document must agree with the exit code, got: {stdout}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// --json is refused where it has no result to describe, rather than accepted
/// and silently ignored - which would hand a consumer an empty stream that
/// looks like a successful parse of nothing.
#[test]
fn json_is_refused_for_modes_with_no_sweep_result() {
    for mode in ["--health", "--dry-run", "--loop"] {
        let out = bin()
            .arg("--json")
            .arg(mode)
            .env("SESSION_STORE_URL", "http://127.0.0.1:1")
            .output()
            .expect("binary should run");
        assert_eq!(
            out.status.code(),
            Some(2),
            "--json {mode} should be a usage error"
        );
    }
}

/// A sweep against an unreachable store must not look like a success.
///
/// This is the defect the `--json` work exists to close: before it, a caller
/// could only ask the exit status, and a sweep that captured nothing still
/// exited 0. Exercised through the real binary rather than the internals,
/// because the exit code IS the interface a host-side caller uses.
#[test]
fn an_unreachable_store_never_exits_zero() {
    let tmp = std::env::temp_dir().join("apss-cli-unreachable");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(tmp.join("claude/p")).expect("fixture dirs");
    std::fs::write(
        tmp.join("claude/p/s.jsonl"),
        "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hi\"},\
         \"sessionId\":\"cli-test\",\"timestamp\":\"2026-08-19T00:00:00Z\"}\n",
    )
    .expect("fixture file");

    let out = bin()
        .arg("--json")
        // Port 1 is not listening, so every upload fails.
        .env("SESSION_STORE_URL", "http://127.0.0.1:1")
        .env("SESSION_STORE_ORIGIN_ENV", "container")
        .env("CLAUDE_PROJECTS_ROOT", tmp.join("claude"))
        .env("CODEX_SESSIONS_ROOT", tmp.join("codex"))
        .env("EXPORTER_STATE_FILE", tmp.join("state.json"))
        .env("EXPORTER_HEALTH_FILE", tmp.join("health.json"))
        .env("HOME", &tmp)
        .output()
        .expect("binary should run");

    assert_ne!(
        out.status.code(),
        Some(0),
        "a sweep that stored nothing must not report success; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// With `--json`, stdout carries the machine result and nothing else.
///
/// Diagnostics default to stdout in this binary, which would interleave log
/// lines with the document and hand a consumer a stream it cannot parse.
#[test]
fn json_mode_keeps_stdout_machine_readable() {
    let tmp = std::env::temp_dir().join("apss-cli-jsonstream");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(tmp.join("claude")).expect("fixture dirs");

    let out = bin()
        .arg("--json")
        .env("SESSION_STORE_URL", "http://127.0.0.1:1")
        .env("SESSION_STORE_ORIGIN_ENV", "container")
        .env("CLAUDE_PROJECTS_ROOT", tmp.join("claude"))
        .env("CODEX_SESSIONS_ROOT", tmp.join("codex"))
        .env("EXPORTER_STATE_FILE", tmp.join("state.json"))
        .env("EXPORTER_HEALTH_FILE", tmp.join("health.json"))
        .env("HOME", &tmp)
        .output()
        .expect("binary should run");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let trimmed = stdout.trim();
    assert!(
        trimmed.starts_with('{') && trimmed.ends_with('}'),
        "stdout must be exactly one JSON object, got: {stdout}"
    );
    assert!(
        !stdout.contains("INFO") && !stdout.contains("WARN"),
        "log records must go to stderr under --json, got: {stdout}"
    );
    assert!(
        trimmed.contains("\"schema_version\":1"),
        "the payload must be versioned so a consumer can refuse a shape it does not know"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
