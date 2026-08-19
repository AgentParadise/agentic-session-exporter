//! Exporter entry point.
//!
//! Modes:
//!   (default)          one capture sweep, then exit.
//!   --loop SECONDS     run forever, sweeping every SECONDS (the daemon mode).
//!   --health           report the age of the last successful sweep and exit.
//!   --dry-run          discover + count only; no network, no state writes.
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
    run,
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
}

/// A usage error. Always reported on stderr and always exit code 2.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ArgError {
    UnknownFlag(String),
    UnexpectedArgument(String),
}

impl std::fmt::Display for ArgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownFlag(flag) => write!(f, "unknown flag: {flag}"),
            Self::UnexpectedArgument(arg) => write!(f, "unexpected argument: {arg}"),
        }
    }
}

impl std::error::Error for ArgError {}

/// Exit code for a usage error, by long-standing CLI convention.
const EXIT_USAGE: i32 = 2;

/// Default daemon sweep interval when `--loop` is given without a usable value.
const DEFAULT_LOOP_SECS: u64 = 300;

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

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut cfg = Config::from_env()?;
    // A CLI --cursor-limit / --limit overrides the CURSOR_LIMIT env value.
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
            // A completed sweep exits 0 even with per-item skips/failures; only a
            // hard RunError (store unreachable, source scan failure) is non-zero.
            let summary = run(&cfg).await?;
            tracing::info!(?summary, "capture run complete");
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

    Ok(Invocation {
        command,
        cursor_limit,
    })
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

    fn parse(args: &[&str]) -> Result<Invocation, ArgError> {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        parse_args(&owned)
    }

    fn command(args: &[&str]) -> Command {
        parse(args).expect("expected a valid command line").command
    }

    #[test]
    fn no_args_is_a_single_run() {
        assert_eq!(
            parse(&[]).unwrap(),
            Invocation {
                command: Command::RunOnce,
                cursor_limit: None,
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
