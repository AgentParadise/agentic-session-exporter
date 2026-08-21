//! Exporter entry point.
//!
//! Modes:
//!   (default)          one capture sweep, then exit.
//!   --loop SECONDS     run forever, sweeping every SECONDS (the daemon mode).
//!   --health           report the age of the last successful sweep and exit.
//!   --dry-run          discover + count only; no network, no state writes.
//!   --ignore-state     do not read the state file; re-send everything found.
//!   --cursor-limit N   cap the run to the newest N Cursor threads (alias
//!                      --limit N). Mainly for a fast bounded test against a
//!                      large real Cursor DB. Overrides the CURSOR_LIMIT env.
//!   --version, -V      print the version and exit.
//!   --help, -h         print usage and exit.
//!
//! Unrecognized arguments are a usage error (exit 2). They must NEVER fall
//! through to the default mode: a typo like `--verison` used to run a full
//! capture sweep with real network uploads.
//!
//! Config comes from the environment (see `config::Config`). The daemon reads it
//! from a 0600 env file installed by `just install-exporter`. `--version` and
//! `--help` are answered before config is loaded, so an unconfigured host (a
//! container whose SESSION_STORE_URL is not set yet) can still run a doctor
//! check against the binary.

use std::time::Duration;

use agentic_session_exporter::{
    config::Config,
    discover_all,
    health::{self, HealthStatus},
    run, RunSummary,
};
use session_capture::SCS_VERSION;

/// The mode selected by the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Version,
    Help,
    Health,
    DryRun,
    RunOnce,
    Loop(u64),
}

/// A fully-parsed command line: the mode plus the options that apply to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Invocation {
    command: Command,
    cursor_limit: Option<usize>,
    /// Emit the sweep result as one JSON object on stdout instead of a prose
    /// line. A consumer that has to regex prose is coupled to wording nothing
    /// tests; this is the supported machine interface.
    json: bool,
    /// Do not READ the state file, so nothing it contains can influence the
    /// result. For a caller auditing a process that can write it.
    ignore_state: bool,
}

/// A usage error. Always reported on stderr and always exit code 2.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ArgError {
    UnknownFlag(String),
    UnexpectedArgument(String),
    /// `--json` given with a mode that has no JSON result to emit.
    JsonUnsupportedMode,
    /// `--ignore-state` given with a mode where it would mislead or do nothing.
    IgnoreStateUnsupportedMode,
}

impl std::fmt::Display for ArgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownFlag(flag) => write!(f, "unknown flag: {flag}"),
            Self::UnexpectedArgument(arg) => write!(f, "unexpected argument: {arg}"),
            Self::JsonUnsupportedMode => write!(
                f,
                "--json applies to a capture sweep only; it cannot be combined \
                 with --health, --dry-run or --loop"
            ),
            Self::IgnoreStateUnsupportedMode => write!(
                f,
                "--ignore-state applies to a capture sweep; --health reads the \
                 health sidecar, which it does not protect, and --dry-run never \
                 consults state at all"
            ),
        }
    }
}

impl std::error::Error for ArgError {}

/// Exit code for a usage error, by long-standing CLI convention.
const EXIT_USAGE: i32 = 2;

/// Default daemon sweep interval when `--loop` is given without a usable value.
const DEFAULT_LOOP_SECS: u64 = 300;

/// Exit code for a sweep that RAN but did not capture everything it found.
///
/// Distinct from both 0 and the hard-failure 1 on purpose. A caller asking
/// "was this session stored?" must not be able to get a false yes by checking
/// only the exit status: a completed sweep in which every upload failed is not
/// a success, however normally the process terminated. Callers that only care
/// whether the binary ran can still treat any non-zero as failure.
const EXIT_INCOMPLETE: i32 = 3;

/// Version of the `--json` payload shape. Bump on any incompatible change, so
/// a consumer can refuse a shape it does not understand instead of
/// misreading it.
/// Bumped to 2 when `sessions` was added to the success document.
///
/// A consumer that requires session-level confirmation must check this: on
/// schema 1 the absence of `sessions` means "this exporter cannot tell you",
/// not "nothing was confirmed".
const RESULT_SCHEMA_VERSION: u32 = 2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let invocation = match parse_args(&args) {
        Ok(invocation) => invocation,
        Err(e) => {
            eprintln!("{}: {e}", env!("CARGO_BIN_NAME"));
            eprintln!("try `{} --help`", env!("CARGO_BIN_NAME"));
            std::process::exit(EXIT_USAGE);
        }
    };

    // --version / --help must answer without any configuration at all.
    match invocation.command {
        Command::Version => {
            println!(
                "{} {} (APS-V1-0004 SCS {SCS_VERSION})",
                env!("CARGO_BIN_NAME"),
                env!("CARGO_PKG_VERSION"),
            );
            return Ok(());
        }
        Command::Help => {
            print_usage();
            return Ok(());
        }
        _ => {}
    }

    // With --json, stdout carries the machine result and NOTHING else. The
    // default subscriber writes to stdout, which would interleave log lines
    // with the JSON document and hand a consumer a stream it cannot parse.
    // Diagnostics still go somewhere: stderr.
    let subscriber = tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
    );
    if invocation.json {
        subscriber.with_writer(std::io::stderr).init();
    } else {
        subscriber.init();
    }

    // A configuration failure must still produce a document under --json, for
    // the same reason a sweep failure does: a consumer that always parses
    // stdout would otherwise get an empty stream, which reads as "no result"
    // rather than "this exporter is misconfigured". The store URL is unknown
    // here by definition, so it is reported as null rather than invented.
    let mut cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            if invocation.json {
                println!("{}", render_config_error_json(&e.to_string()));
            }
            return Err(e.into());
        }
    };
    // A CLI --cursor-limit / --limit overrides the CURSOR_LIMIT env value.
    cfg.ignore_state = invocation.ignore_state;
    if let Some(n) = invocation.cursor_limit {
        cfg.cursor_limit = Some(n);
    }

    match invocation.command {
        // Handled above, before config was loaded.
        Command::Version | Command::Help => unreachable!("version/help returned before config"),
        Command::Health => return run_health_check(&cfg),
        Command::DryRun => {
            let found = discover_all(&cfg)?;
            println!(
                "dry-run: discovered {} transcript(s) under claude={} codex={} cursor={}",
                found.len(),
                cfg.claude_root.display(),
                cfg.codex_root.display(),
                cfg.cursor_db
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "(none)".into()),
            );
            return Ok(());
        }
        Command::RunOnce => {
            // A hard failure must still produce a document under --json.
            // Returning early would leave a consumer that always parses stdout
            // with an empty stream, which reads as "no result" rather than
            // "the sweep could not run" - the two are very different to
            // anything deciding whether a session was captured.
            let summary = match run(&cfg).await {
                Ok(summary) => summary,
                Err(e) => {
                    if invocation.json {
                        println!("{}", render_error_json(&e.to_string(), &cfg));
                    }
                    return Err(e.into());
                }
            };
            tracing::info!(?summary, "capture run complete");

            if invocation.json {
                println!("{}", render_result_json(&summary, &cfg));
            } else {
                println!(
                    "run: discovered={} skipped_unchanged={} uploaded={} accepted={} duplicate={} rejected={} skipped_oversize={} failed={}",
                    summary.discovered,
                    summary.skipped_unchanged,
                    summary.uploaded,
                    summary.accepted,
                    summary.duplicate,
                    summary.rejected,
                    summary.skipped_oversize,
                    summary.failed
                );
            }

            // A sweep that ran but did not capture everything it found is not a
            // success, and must not look like one to a caller that checks only
            // the exit status. Previously this returned 0 regardless, so a
            // sweep in which every upload failed was indistinguishable from one
            // that stored everything.
            if !captured_everything(&summary) {
                std::process::exit(EXIT_INCOMPLETE);
            }
        }
        Command::Loop(secs) => {
            tracing::info!(interval_secs = secs, store = %cfg.store_url, "exporter daemon starting");
            loop {
                match run(&cfg).await {
                    Ok(summary) => tracing::info!(?summary, "capture run complete"),
                    Err(e) => tracing::error!(error = %e, "capture run failed; will retry"),
                }
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }
    }

    Ok(())
}

fn print_usage() {
    println!(
        "\
{bin} {version} (APS-V1-0004 SCS {SCS_VERSION})

Capture client: discovers local Claude / Codex / Cursor transcripts and uploads
SCS envelopes to the session store's batch ingest endpoint.

Usage: {bin} [OPTIONS]

Modes (with no mode flag, runs one capture sweep and exits):
  --loop SECONDS     run forever, sweeping every SECONDS (daemon mode).
                     A missing or unusable value defaults to {DEFAULT_LOOP_SECS}.
  --health           report the age of the last successful sweep and exit
                     (exit 1 when stale, missing, or invalid).
  --dry-run          discover + count only; no network, no state writes.
  --version, -V      print the version and exit.
  --help, -h         print this help and exit.

Options:
  --cursor-limit N   cap the run to the newest N Cursor threads (alias
                     --limit N). Overrides the CURSOR_LIMIT env var.
  --json             print the sweep result as one JSON object instead of a
                     prose line. This is the supported machine interface;
                     the prose line is for humans and its wording is not a
                     contract. Applies to a capture sweep ONLY: combining it
                     with --health, --dry-run or --loop is a usage error
                     rather than a silently empty stream.
  --ignore-state     do not READ or WRITE the state file, so nothing it
                     contains can
                     influence the result. Every discovered session is sent
                     again; a conforming store deduplicates on
                     (session_id, content_hash), so the cost is a request
                     rather than a duplicate row. For a caller auditing a
                     process that can WRITE that file - anything able to do
                     so can otherwise make a transcript that never reached
                     the store report as skipped_unchanged, which reads as a
                     clean sweep.

Exit codes for a capture sweep:
  0                  every session found reached the store (or was already
                     there, or was unchanged).
  {EXIT_INCOMPLETE}                  the sweep RAN but did not capture everything it
                     found: something was rejected, oversize, unconfirmed
                     (sent, but the store returned no matching outcome), or
                     failed.
                     Exit 0 alone therefore does not prove a given session
                     was stored; check this code, or read --json.
  1                  the sweep could not run (store unreachable, scan failure).
  {EXIT_USAGE}                  usage error.

Configuration comes from the environment (SESSION_STORE_URL is required for
every mode except --version and --help). Unrecognized arguments exit {EXIT_USAGE}.",
        bin = env!("CARGO_BIN_NAME"),
        version = env!("CARGO_PKG_VERSION"),
    );
}

/// Parse argv (already stripped of argv[0]) in a single left-to-right pass.
///
/// Flags that take a value consume the following token, so a value is never
/// mistaken for an unknown flag. Mode precedence when several are given matches
/// the historical behaviour: version > help > health > dry-run > loop > run-once.
fn parse_args(args: &[String]) -> Result<Invocation, ArgError> {
    let mut version = false;
    let mut help = false;
    let mut health = false;
    let mut dry_run = false;
    let mut json = false;
    let mut ignore_state = false;
    let mut loop_secs: Option<u64> = None;
    let mut cursor_limit: Option<usize> = None;

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--version" | "-V" => {
                version = true;
                i += 1;
            }
            "--help" | "-h" => {
                help = true;
                i += 1;
            }
            "--health" => {
                health = true;
                i += 1;
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            "--json" => {
                json = true;
                i += 1;
            }
            "--ignore-state" => {
                ignore_state = true;
                i += 1;
            }
            "--loop" => {
                let (value, consumed) = take_value(args, i);
                // A missing, zero, or unparseable value keeps the historical
                // 5-minute default rather than failing the daemon at startup.
                loop_secs = Some(
                    value
                        .and_then(|s| s.parse::<u64>().ok())
                        .filter(|n| *n > 0)
                        .unwrap_or(DEFAULT_LOOP_SECS),
                );
                i += consumed;
            }
            "--cursor-limit" | "--limit" => {
                let (value, consumed) = take_value(args, i);
                cursor_limit = value
                    .and_then(|s| s.parse::<usize>().ok())
                    .filter(|n| *n > 0);
                i += consumed;
            }
            other if other.starts_with('-') => {
                return Err(ArgError::UnknownFlag(other.to_string()));
            }
            other => return Err(ArgError::UnexpectedArgument(other.to_string())),
        }
    }

    let command = if version {
        Command::Version
    } else if help {
        Command::Help
    } else if health {
        Command::Health
    } else if dry_run {
        Command::DryRun
    } else if let Some(secs) = loop_secs {
        Command::Loop(secs)
    } else {
        Command::RunOnce
    };

    // --json describes the shape of a SWEEP result. The other modes print
    // prose or nothing, so accepting it there would hand a consumer an empty
    // or unparseable stream while looking like it was honoured.
    //
    // Checked AFTER the command is resolved, not before. Checking first broke
    // the documented precedence: `--version --health --json` exited 2 instead
    // of printing the version, which also violated the rule that --version and
    // --help answer before anything else and never fail.
    if json
        && matches!(
            command,
            Command::Health | Command::DryRun | Command::Loop(_)
        )
    {
        return Err(ArgError::JsonUnsupportedMode);
    }

    // --ignore-state is about a CAPTURE verdict, and pairs badly with two modes:
    //
    //   --health   reads the health sidecar, which the audited process can
    //              forge under exactly the threat model this flag exists for.
    //              Accepting it would hand back a reassuring answer from a
    //              source the flag does not protect - a false assurance is
    //              worse than a usage error.
    //   --dry-run  never consults state at all, so the flag would be a silent
    //              no-op, and a caller who believed it mattered would be wrong.
    if ignore_state && matches!(command, Command::Health | Command::DryRun) {
        return Err(ArgError::IgnoreStateUnsupportedMode);
    }

    Ok(Invocation {
        command,
        cursor_limit,
        json,
        ignore_state,
    })
}

/// True when every session the sweep found reached the store.
///
/// `duplicate` counts as captured: the store already holds that session, which
/// is the outcome the caller wanted. `skipped_unchanged` likewise - nothing
/// changed, so nothing needed sending. `rejected`, `skipped_oversize` and
/// `failed` all mean a session the sweep saw is NOT in the store.
fn captured_everything(summary: &RunSummary) -> bool {
    summary.rejected == 0
        && summary.skipped_oversize == 0
        && summary.failed == 0
        && summary.unconfirmed == 0
}

/// The `--json` payload: one object, one line, versioned.
///
/// Hand-rolled rather than derived so the wire shape is visible here and
/// cannot drift when `RunSummary` gains an internal field. The store URL and
/// resolved origin are included because a consumer needs to know WHERE the
/// sessions went, not merely that a number went up: an exporter pointed at the
/// wrong store reports the same counters as one pointed at the right one.
fn render_result_json(summary: &RunSummary, cfg: &Config) -> String {
    let deployment = match cfg.origin_deployment.as_deref() {
        Some(d) => format!("\"{}\"", escape_json(d)),
        None => "null".to_string(),
    };
    format!(
        concat!(
            r#"{{"schema_version":{},"scs_version":"{}","captured_everything":{},"#,
            r#""store_url":"{}","origin":{{"environment":"{}","deployment":{}}},"#,
            r#""counters":{{"discovered":{},"skipped_unchanged":{},"uploaded":{},"#,
            r#""accepted":{},"duplicate":{},"rejected":{},"skipped_oversize":{},"failed":{},"unconfirmed":{}}},"#,
            r#""sessions":{}}}"#
        ),
        RESULT_SCHEMA_VERSION,
        escape_json(SCS_VERSION),
        captured_everything(summary),
        escape_json(&cfg.store_url),
        escape_json(&cfg.origin_environment),
        deployment,
        summary.discovered,
        summary.skipped_unchanged,
        summary.uploaded,
        summary.accepted,
        summary.duplicate,
        summary.rejected,
        summary.skipped_oversize,
        summary.failed,
        summary.unconfirmed,
        render_confirmed_sessions(&summary.confirmed_sessions),
    )
}

/// The confirmed session ids as a JSON array of strings.
///
/// Emitted so a caller can ask whether the session IT cares about reached the
/// store, rather than only whether some number of sessions did. Counters alone
/// let a sweep that captured an unrelated decoy report the same success as one
/// that captured the session the caller was asking about.
fn render_confirmed_sessions(ids: &[String]) -> String {
    let mut out = String::from("[");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&escape_json(id));
        out.push('"');
    }
    out.push(']');
    out
}

/// The `--json` payload for a sweep that could not run at all.
///
/// Same envelope as the success document so a consumer parses one shape, with
/// `captured_everything` false and the counters absent rather than zeroed:
/// zeroes would claim the sweep looked and found nothing, which is a different
/// and much more reassuring statement than "the sweep never ran".
fn render_error_json(message: &str, cfg: &Config) -> String {
    let deployment = match cfg.origin_deployment.as_deref() {
        Some(d) => format!("\"{}\"", escape_json(d)),
        None => "null".to_string(),
    };
    format!(
        concat!(
            r#"{{"schema_version":{},"scs_version":"{}","captured_everything":false,"#,
            r#""error":"{}","store_url":"{}","origin":{{"environment":"{}","deployment":{}}}}}"#
        ),
        RESULT_SCHEMA_VERSION,
        escape_json(SCS_VERSION),
        escape_json(message),
        escape_json(&cfg.store_url),
        escape_json(&cfg.origin_environment),
        deployment,
    )
}

/// The `--json` payload for a run that could not even load its configuration.
///
/// Deliberately a reduced shape: store URL and origin are unknown at this
/// point, and reporting them as empty strings would look like configured
/// values rather than absent ones.
fn render_config_error_json(message: &str) -> String {
    format!(
        r#"{{"schema_version":{},"scs_version":"{}","captured_everything":false,"error":"{}","store_url":null,"origin":null}}"#,
        RESULT_SCHEMA_VERSION,
        escape_json(SCS_VERSION),
        escape_json(message),
    )
}

/// Escape the characters JSON forbids in a string.
///
/// Values here come from configuration, which is operator-controlled but not
/// necessarily well-formed: a store URL or deployment name containing a quote
/// would otherwise emit a document no consumer can parse.
fn escape_json(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Look at the token after the flag at `idx`.
///
/// Returns the value (when the next token exists and is not itself a flag) and
/// how many argv slots to advance. A flag whose value is absent still advances
/// by one so the following flag is parsed normally.
fn take_value(args: &[String], idx: usize) -> (Option<&str>, usize) {
    match args.get(idx + 1) {
        Some(next) if !next.starts_with('-') => (Some(next.as_str()), 2),
        _ => (None, 1),
    }
}

fn run_health_check(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    match health::check(&cfg.health_file, now_epoch_secs(), cfg.health_max_age_secs) {
        HealthStatus::Healthy { age_secs } => {
            println!(
                "exporter health: last-successful-sweep age={age_secs}s (healthy; max={}s)",
                cfg.health_max_age_secs
            );
            Ok(())
        }
        HealthStatus::Stale {
            age_secs,
            max_age_secs,
        } => {
            println!(
                "exporter health: last-successful-sweep age={age_secs}s (stale; max={max_age_secs}s)"
            );
            std::process::exit(1);
        }
        HealthStatus::Missing => {
            println!(
                "exporter health: last-successful-sweep age=unknown (no successful sweep recorded)"
            );
            std::process::exit(1);
        }
        HealthStatus::Invalid => {
            println!("exporter health: last-successful-sweep age=unknown (invalid health record)");
            std::process::exit(1);
        }
    }
}

fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Config good enough to render a result document against.
    ///
    /// Only the fields the renderer reads matter here (store url, origin), but
    /// the struct has no Default, so the rest are filled with inert values.
    fn test_cfg() -> Config {
        Config {
            store_url: "http://store.example:8797".to_string(),
            write_token: None,
            origin_host: "host-a".to_string(),
            origin_environment: "container".to_string(),
            origin_deployment: Some("syntropic137__development".to_string()),
            claude_root: std::path::PathBuf::from("/nonexistent/claude"),
            codex_root: std::path::PathBuf::from("/nonexistent/codex"),
            cursor_db: None,
            cursor_limit: None,
            state_file: std::path::PathBuf::from("/nonexistent/state.json"),
            ignore_state: true,
            health_file: std::path::PathBuf::from("/nonexistent/health.json"),
            health_max_age_secs: 3600,
            batch_size: 50,
            max_envelope_bytes: 1024 * 1024,
            tags: Vec::new(),
        }
    }

    /// The WIRE CONTRACT, parsed as JSON rather than string-matched.
    ///
    /// The unit tests around `render_confirmed_sessions` call that helper
    /// directly, so they stay green even if its output is never placed into the
    /// document. A mutation check caught exactly that: renaming the `sessions`
    /// key out of `render_result_json` left every test passing. This asserts on
    /// the parsed document, which is what a consumer actually reads.
    #[test]
    fn the_result_document_carries_the_confirmed_sessions() {
        let mut s = summary(0, 0, 0);
        s.confirmed_sessions = vec!["a".to_string(), r#"b"c"#.to_string(), "a".to_string()];

        let doc = render_result_json(&s, &test_cfg());
        let v: serde_json::Value =
            serde_json::from_str(&doc).expect("the result document must be valid JSON");

        assert_eq!(v["schema_version"], 2);
        assert_eq!(
            v["sessions"],
            serde_json::json!(["a", "b\"c", "a"]),
            "the document must name every confirmed session, duplicates included"
        );
    }

    #[test]
    fn a_sweep_that_confirmed_nothing_still_emits_an_array() {
        // Not null and not absent: a consumer that has to branch on presence
        // cannot tell "confirmed nothing" from "this exporter cannot tell you",
        // and those mean very different things during a version rollout.
        let mut s = summary(0, 0, 0);
        s.confirmed_sessions = Vec::new();

        let doc = render_result_json(&s, &test_cfg());
        let v: serde_json::Value = serde_json::from_str(&doc).expect("valid JSON");
        assert_eq!(v["sessions"], serde_json::json!([]));
    }

    fn parse(args: &[&str]) -> Result<Invocation, ArgError> {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        parse_args(&owned)
    }

    fn command(args: &[&str]) -> Command {
        parse(args).expect("expected a valid command line").command
    }

    fn summary(rejected: usize, oversize: usize, failed: usize) -> RunSummary {
        RunSummary {
            discovered: 1,
            skipped_unchanged: 0,
            uploaded: 1,
            accepted: 1,
            duplicate: 0,
            rejected,
            skipped_oversize: oversize,
            failed,
            unconfirmed: 0,
            confirmed_sessions: vec!["sess-a".to_string()],
        }
    }

    #[test]
    fn json_is_off_by_default_and_on_when_asked() {
        assert!(!parse(&[]).unwrap().json);
        assert!(parse(&["--json"]).unwrap().json);
    }

    #[test]
    fn json_does_not_change_which_command_runs() {
        assert_eq!(command(&["--json"]), Command::RunOnce);
        assert!(parse(&["--json"]).unwrap().json);
    }

    #[test]
    fn version_and_help_still_win_over_the_json_restriction() {
        // Checking --json before resolving the command broke the documented
        // precedence: `--version --health --json` exited 2 instead of printing
        // the version. --version and --help answer before anything else and
        // must never fail.
        assert_eq!(
            command(&["--version", "--health", "--json"]),
            Command::Version
        );
        assert_eq!(command(&["--help", "--loop", "--json"]), Command::Help);
    }

    #[test]
    fn an_unconfirmed_envelope_is_not_captured() {
        let mut s = summary(0, 0, 0);
        s.unconfirmed = 1;
        assert!(
            !captured_everything(&s),
            "a store that said nothing about an envelope has not stored it"
        );
    }

    #[test]
    fn json_is_refused_where_there_is_no_sweep_result() {
        // An earlier version of this test asserted the opposite, that --json
        // was simply carried alongside --health and --dry-run. Review pointed
        // out what that means in practice: those modes print prose or nothing,
        // so a consumer passing --json would get an empty or unparseable
        // stream while believing the flag was honoured. Refusing is louder.
        for mode in ["--health", "--dry-run", "--loop"] {
            assert_eq!(
                parse(&["--json", mode]),
                Err(ArgError::JsonUnsupportedMode),
                "--json {mode} should be a usage error"
            );
        }
        // Without --json those modes are still perfectly valid.
        assert_eq!(command(&["--health"]), Command::Health);
        assert_eq!(command(&["--dry-run"]), Command::DryRun);
    }

    #[test]
    fn the_json_mode_error_names_the_problem() {
        assert!(ArgError::JsonUnsupportedMode
            .to_string()
            .contains("capture sweep only"));
    }

    #[test]
    fn a_clean_sweep_captured_everything() {
        assert!(captured_everything(&summary(0, 0, 0)));
    }

    #[test]
    fn anything_left_uncaptured_is_not_everything() {
        // Each of these means a session the sweep SAW is not in the store.
        assert!(!captured_everything(&summary(1, 0, 0)), "rejected");
        assert!(!captured_everything(&summary(0, 1, 0)), "skipped_oversize");
        assert!(!captured_everything(&summary(0, 0, 1)), "failed");
    }

    #[test]
    fn duplicates_and_unchanged_still_count_as_captured() {
        // The caller asked "is it in the store"; for these the answer is yes.
        let s = RunSummary {
            discovered: 3,
            skipped_unchanged: 1,
            uploaded: 1,
            accepted: 0,
            duplicate: 2,
            rejected: 0,
            skipped_oversize: 0,
            failed: 0,
            unconfirmed: 0,
            confirmed_sessions: vec!["sess-a".to_string(), "sess-b".to_string()],
        };
        assert!(captured_everything(&s));
    }

    #[test]
    fn escapes_the_characters_json_forbids() {
        assert_eq!(escape_json(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_json(r"a\b"), r"a\\b");
        assert_eq!(escape_json("a\nb"), r"a\nb");
        assert_eq!(escape_json("a\rb"), r"a\rb");
        assert_eq!(escape_json("a\tb"), r"a\tb");
        assert_eq!(escape_json("a\u{1}b"), r"a\u0001b");
        assert_eq!(escape_json("plain"), "plain");
    }

    #[test]
    fn confirmed_sessions_render_as_a_json_string_array() {
        assert_eq!(render_confirmed_sessions(&[]), "[]");
        assert_eq!(render_confirmed_sessions(&["a".to_string()]), r#"["a"]"#);
        assert_eq!(
            render_confirmed_sessions(&["a".to_string(), "b".to_string()]),
            r#"["a","b"]"#
        );
    }

    #[test]
    fn a_confirmed_session_id_is_escaped_like_every_other_string() {
        // Session ids come from transcript filenames, which the audited agent
        // controls. An unescaped quote here would let it inject arbitrary keys
        // into the result document the host parses.
        assert_eq!(
            render_confirmed_sessions(&[r#"a"b"#.to_string()]),
            r#"["a\"b"]"#
        );
    }

    #[test]
    fn the_same_session_id_can_be_confirmed_more_than_once() {
        // A multiset, mirroring the confirmation logic: two envelopes can share
        // a session id while differing in content, and each confirmation covers
        // exactly one of them. Collapsing them would overstate what was stored.
        assert_eq!(
            render_confirmed_sessions(&["a".to_string(), "a".to_string()]),
            r#"["a","a"]"#
        );
    }

    #[test]
    fn no_args_is_a_single_run() {
        assert_eq!(
            parse(&[]).unwrap(),
            Invocation {
                ignore_state: false,
                command: Command::RunOnce,
                cursor_limit: None,
                json: false,
            }
        );
    }

    #[test]
    fn version_long_and_short_forms_parse() {
        assert_eq!(command(&["--version"]), Command::Version);
        assert_eq!(command(&["-V"]), Command::Version);
    }

    #[test]
    fn help_long_and_short_forms_parse() {
        assert_eq!(command(&["--help"]), Command::Help);
        assert_eq!(command(&["-h"]), Command::Help);
    }

    #[test]
    fn ignore_state_is_rejected_where_it_would_mislead() {
        // --health reads the health sidecar, which the audited process can
        // forge under the same threat model this flag exists for; --dry-run
        // never consults state, so the flag would be a silent no-op. Both are
        // usage errors rather than quiet false assurance.
        assert_eq!(
            parse(&["--ignore-state", "--health"]),
            Err(ArgError::IgnoreStateUnsupportedMode)
        );
        assert_eq!(
            parse(&["--ignore-state", "--dry-run"]),
            Err(ArgError::IgnoreStateUnsupportedMode)
        );
    }

    #[test]
    fn ignore_state_is_accepted_for_a_capture_sweep() {
        assert!(parse(&["--ignore-state"]).unwrap().ignore_state);
        assert!(parse(&["--ignore-state", "--json"]).unwrap().ignore_state);
        // --loop is a repeated capture sweep, so it is meaningful there.
        assert!(
            parse(&["--ignore-state", "--loop", "5"])
                .unwrap()
                .ignore_state
        );
        // Absent by default: only a caller that asks pays the re-send.
        assert!(!parse(&[]).unwrap().ignore_state);
    }

    #[test]
    fn health_and_dry_run_parse() {
        assert_eq!(command(&["--health"]), Command::Health);
        assert_eq!(command(&["--dry-run"]), Command::DryRun);
    }

    #[test]
    fn loop_with_a_value_uses_it() {
        assert_eq!(command(&["--loop", "300"]), Command::Loop(300));
        assert_eq!(command(&["--loop", "17"]), Command::Loop(17));
    }

    #[test]
    fn bare_loop_defaults_to_five_minutes() {
        assert_eq!(command(&["--loop"]), Command::Loop(300));
    }

    #[test]
    fn loop_with_a_degenerate_value_defaults_to_five_minutes() {
        assert_eq!(command(&["--loop", "0"]), Command::Loop(300));
        assert_eq!(command(&["--loop", "abc"]), Command::Loop(300));
    }

    #[test]
    fn loop_followed_by_a_flag_defaults_and_still_parses_the_flag() {
        assert_eq!(command(&["--loop", "--dry-run"]), Command::DryRun);
        let parsed = parse(&["--loop", "--cursor-limit", "5"]).unwrap();
        assert_eq!(parsed.command, Command::Loop(300));
        assert_eq!(parsed.cursor_limit, Some(5));
    }

    #[test]
    fn cursor_limit_and_its_alias_parse() {
        assert_eq!(
            parse(&["--cursor-limit", "50"]).unwrap().cursor_limit,
            Some(50)
        );
        assert_eq!(parse(&["--limit", "50"]).unwrap().cursor_limit, Some(50));
    }

    #[test]
    fn degenerate_cursor_limits_yield_none() {
        assert_eq!(parse(&["--cursor-limit", "0"]).unwrap().cursor_limit, None);
        assert_eq!(
            parse(&["--cursor-limit", "abc"]).unwrap().cursor_limit,
            None
        );
        assert_eq!(parse(&["--limit"]).unwrap().cursor_limit, None);
    }

    #[test]
    fn a_flag_value_is_never_read_as_an_unknown_flag() {
        // The regression this parser exists to prevent: the numeric value must
        // be consumed by its flag, not rejected as a stray argument.
        assert!(parse(&["--loop", "300"]).is_ok());
        assert!(parse(&["--cursor-limit", "50"]).is_ok());
        assert!(parse(&["--limit", "50", "--dry-run"]).is_ok());
        assert_eq!(command(&["--limit", "50", "--dry-run"]), Command::DryRun);
    }

    #[test]
    fn unknown_flag_is_a_usage_error() {
        assert_eq!(
            parse(&["--bogus-flag"]),
            Err(ArgError::UnknownFlag("--bogus-flag".into()))
        );
        // A --version typo must never fall through to a capture sweep.
        assert_eq!(
            parse(&["--verison"]),
            Err(ArgError::UnknownFlag("--verison".into()))
        );
        assert_eq!(parse(&["-x"]), Err(ArgError::UnknownFlag("-x".into())));
    }

    #[test]
    fn stray_positional_is_a_usage_error() {
        assert_eq!(
            parse(&["sweep"]),
            Err(ArgError::UnexpectedArgument("sweep".into()))
        );
    }

    #[test]
    fn unknown_flag_wins_over_a_valid_mode() {
        assert_eq!(
            parse(&["--dry-run", "--nope"]),
            Err(ArgError::UnknownFlag("--nope".into()))
        );
    }

    #[test]
    fn mode_precedence_matches_the_historical_order() {
        assert_eq!(
            command(&["--health", "--dry-run", "--loop", "60"]),
            Command::Health
        );
        assert_eq!(command(&["--dry-run", "--loop", "60"]), Command::DryRun);
        assert_eq!(command(&["--help", "--health"]), Command::Help);
        assert_eq!(command(&["--version", "--help"]), Command::Version);
    }

    #[test]
    fn options_combine_with_modes() {
        let parsed = parse(&["--dry-run", "--cursor-limit", "7"]).unwrap();
        assert_eq!(parsed.command, Command::DryRun);
        assert_eq!(parsed.cursor_limit, Some(7));
    }

    #[test]
    fn arg_error_renders_a_readable_message() {
        assert_eq!(
            ArgError::UnknownFlag("--nope".into()).to_string(),
            "unknown flag: --nope"
        );
        assert_eq!(
            ArgError::UnexpectedArgument("x".into()).to_string(),
            "unexpected argument: x"
        );
    }
}
