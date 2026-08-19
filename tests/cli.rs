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
