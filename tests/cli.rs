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

#[test]
fn ignore_state_rejects_the_modes_it_cannot_protect() {
    // Exit 2 is the usage contract. --health reads the health sidecar, which
    // the audited process can forge under exactly the threat model this flag
    // exists for, and --dry-run never consults state so the flag would be a
    // silent no-op. Both refuse rather than hand back a reassuring answer.
    for mode in ["--health", "--dry-run"] {
        let out = bin()
            .arg("--ignore-state")
            .arg(mode)
            .env("SESSION_STORE_URL", "http://127.0.0.1:1")
            .output()
            .expect("binary should run");

        assert_eq!(
            out.status.code(),
            Some(2),
            "--ignore-state {mode} must be a usage error"
        );
    }
}

fn local_fixture() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir(tmp.path().join("claude")).unwrap();
    std::fs::write(
        tmp.path().join("claude/native.jsonl"),
        "{\"type\":\"user\",\"sessionId\":\"native\",\"timestamp\":\"2026-07-01T00:00:00Z\",\"message\":{\"role\":\"user\",\"content\":\"local capture\"}}\r\n",
    ).unwrap();
    tmp
}

fn local_bin(root: &std::path::Path) -> Command {
    let mut command = bin();
    command
        .env_clear()
        .env("HOME", root)
        .env("CLAUDE_PROJECTS_ROOT", root.join("claude"))
        .env("CODEX_SESSIONS_ROOT", root.join("codex-empty"))
        .env("SESSION_STORE_ORIGIN_HOST", "local-test")
        .env("SESSION_STORE_ORIGIN_ENV", "local")
        .env("EXPORTER_SPOOL_DIR", root.join("durable-spool"));
    // Keep instrumentation output while excluding user capture configuration.
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    command
}

#[test]
fn local_capture_requires_no_store_and_reads_after_original_transcript_is_removed() {
    let tmp = local_fixture();
    let first = local_bin(tmp.path())
        .args(["--spool-only", "--json"])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(summary["stored"], 1);
    assert!(summary.get("captured_everything").is_none());
    let repeated = local_bin(tmp.path()).arg("--spool-only").output().unwrap();
    assert!(repeated.status.success());
    let summary: serde_json::Value = serde_json::from_slice(&repeated.stdout).unwrap();
    assert_eq!(summary["duplicate"], 1);
    let listed = local_bin(tmp.path())
        .args(["--spool-list", "0"])
        .env_remove("HOME")
        .output()
        .unwrap();
    assert!(listed.status.success());
    let page: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(page["entries"].as_array().unwrap().len(), 1);
    let sequence = page["entries"][0]["sequence"].as_u64().unwrap().to_string();
    let original = std::fs::read_to_string(tmp.path().join("claude/native.jsonl")).unwrap();
    std::fs::remove_dir_all(tmp.path().join("claude")).unwrap();
    let body = local_bin(tmp.path())
        .args(["--spool-read", &sequence])
        .output()
        .unwrap();
    assert!(body.status.success());
    let envelope: serde_json::Value = serde_json::from_slice(&body.stdout).unwrap();
    assert_eq!(envelope["session_id"], "native");
    assert_eq!(envelope["raw"], original);
    assert!(envelope.get("content_hash").is_none());
}

#[test]
fn local_spool_modes_reject_ambiguous_or_malformed_arguments() {
    for arguments in [
        vec!["--spool-only", "--loop"],
        vec!["--spool-only", "--health"],
        vec!["--spool-only", "--ignore-state"],
        vec!["--spool-only", "--dry-run"],
        vec!["--spool-only", "--cursor-limit", "2"],
        vec!["--spool-only", "--spool-read", "1"],
        vec!["--spool-read", "0"],
        vec!["--spool-read", "oops"],
        vec!["--spool-read"],
        vec!["--spool-list", "1:"],
        vec!["--spool-list", "x:2"],
        vec!["--spool-only", "--spool-only"],
    ] {
        let out = bin().args(arguments).output().unwrap();
        assert_eq!(out.status.code(), Some(2));
    }
}

#[test]
fn local_spool_needs_explicit_absolute_storage_and_reports_oversize_failure() {
    let tmp = local_fixture();
    for directory in [None, Some("relative-spool")] {
        let mut command = local_bin(tmp.path());
        command.arg("--spool-only").env_remove("EXPORTER_SPOOL_DIR");
        if let Some(value) = directory {
            command.env("EXPORTER_SPOOL_DIR", value);
        }
        assert!(!command.output().unwrap().status.success());
    }
    let out = local_bin(tmp.path())
        .arg("--spool-only")
        .env("MAX_ENVELOPE_BYTES", "1")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(3));
    let summary: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(summary["stored"], 0);
    assert_eq!(summary["skipped_oversize"], 1);
}

#[test]
fn inventory_enqueue_is_durable_and_rejects_conflicts_without_network() {
    use std::io::Write;
    use std::process::Stdio;
    let root = tempfile::tempdir().unwrap();
    let body = serde_json::json!({"operation":"publish", "body": {
        "run":{"source_instance_id":"source","execution_id":"run"},
        "revision_id":"r1", "parent_revision_id":null, "revision_sequence":1,
        "producer_id":"producer", "sequence_high_watermark":0,
        "resolver_version":"v1", "coverage":"unknown", "expected_record_count":0
    }});
    let send = |value: &serde_json::Value| {
        let mut child = bin()
            .arg("--inventory-enqueue")
            .env("SESSION_STORE_URL", "http://127.0.0.1:1")
            .env("INVENTORY_WRITE_TOKEN", "never-print-this-secret")
            .env("EXPORTER_INVENTORY_DIR", root.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(value.to_string().as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    };
    let first = send(&body);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&first.stdout).unwrap()["inserted"],
        true
    );
    let retry = send(&body);
    assert!(retry.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&retry.stdout).unwrap()["inserted"],
        false
    );
    let mut changed = body;
    changed["body"]["coverage"] = "missing".into();
    let conflict = send(&changed);
    assert!(!conflict.status.success());
    assert!(!String::from_utf8_lossy(&conflict.stderr).contains("never-print-this-secret"));
    let drain = bin()
        .args(["--inventory-drain", "1"])
        .env("SESSION_STORE_URL", "http://127.0.0.1:1")
        .env("INVENTORY_WRITE_TOKEN", "never-print-this-secret")
        .env("EXPORTER_INVENTORY_DIR", root.path())
        .output()
        .unwrap();
    assert_eq!(drain.status.code(), Some(3));
    let result: serde_json::Value = serde_json::from_slice(&drain.stdout).unwrap();
    assert_eq!(result["failed"], 1);
    assert_eq!(result["remaining"], 1);
}

#[test]
fn inventory_modes_reject_invalid_bounds_and_capture_flags() {
    for args in [
        vec!["--inventory-drain", "0"],
        vec!["--inventory-drain", "501"],
        vec!["--inventory-enqueue", "--spool-only"],
        vec!["--inventory-enqueue", "--ignore-state"],
    ] {
        assert_eq!(bin().args(args).output().unwrap().status.code(), Some(2));
    }
}

#[test]
fn capture_delivery_cli_restarts_without_losing_pending_envelopes() {
    use std::{io::Write, process::Stdio};
    let root = tempfile::tempdir().unwrap();
    let input=serde_json::json!({"identity":{"source_instance_id":"source","harness":"codex","native_session_id":"native"},
        "envelope":{"scs_version":"1.0","origin":{"host":"test","environment":"local"},"agent":"codex",
        "source_format":"codex-rollout-jsonl","session_id":"native","started_at":"2026-09-22T00:00:00Z",
        "last_activity_at":"2026-09-22T00:00:01Z","raw":"exact\r\n"}}).to_string();
    let configured = || {
        let mut command = bin();
        command
            .env("SESSION_STORE_URL", "http://127.0.0.1:1")
            .env("CAPTURE_WRITE_TOKEN", "never-print-this-secret")
            .env("EXPORTER_CAPTURE_DIR", root.path());
        command
    };
    let send = |mode: &str, input: &str| {
        let mut child = configured()
            .arg(mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    };
    for inserted in [true, false] {
        let result = send("--capture-enqueue", &input);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let receipt: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(receipt["schema_version"], 1);
        assert_eq!(receipt["inserted"], inserted);
    }
    let pending = send("--capture-receipt", &input);
    assert!(pending.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&pending.stdout).unwrap(),
        serde_json::json!({"schema_version":1,"receipt":null})
    );
    let invalid = send("--capture-enqueue", "{\"identity\":{},\"identity\":{}}");
    assert!(!invalid.status.success());
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("never-print-this-secret"));
    let result = configured()
        .args(["--capture-drain", "1"])
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(3));
    let result: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(result["failed"], 1);
    assert_eq!(result["remaining"], 1);
    for args in [
        vec!["--capture-receipt", "--json"],
        vec!["--capture-receipt", "--capture-enqueue"],
        vec!["--capture-drain", "0"],
        vec!["--capture-drain", "51"],
        vec!["--capture-enqueue", "--inventory-enqueue"],
        vec!["--capture-enqueue", "--json"],
        vec!["--capture-enqueue", "--ignore-state"],
    ] {
        assert_eq!(bin().args(args).output().unwrap().status.code(), Some(2));
    }
}
