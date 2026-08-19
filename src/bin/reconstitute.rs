//! Restore one Claude Code session from the session store onto this machine.
//!
//! Usage:
//!   SESSION_ID              the session to restore (positional, required).
//!   --repos-root PATH       clone/lookup root for the session's repository.
//!   --no-resume             restore the files but do not launch `claude --resume`.
//!   --version, -V           print the version and exit.
//!   --help, -h              print usage and exit.
//!
//! Unrecognized flags are a usage error (exit 2). `--version` and `--help` are
//! answered before any environment lookup, so they work on an unconfigured host.

use std::path::PathBuf;

use agentic_session_exporter::reconstitute::{
    invoke_claude_resume, ReconstitutionClient, ReconstitutionLocations,
};
use session_capture::SCS_VERSION;

/// Exit code for a usage error, by long-standing CLI convention.
const EXIT_USAGE: i32 = 2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let parsed = match parse_args(&argv) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("{}: {e}", env!("CARGO_BIN_NAME"));
            eprintln!("try `{} --help`", env!("CARGO_BIN_NAME"));
            std::process::exit(EXIT_USAGE);
        }
    };

    // --version / --help must answer without configuration or a session id.
    let args = match parsed {
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
        Command::Restore(args) => args,
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let store_url = std::env::var("SESSION_STORE_URL")?;
    let read_token = std::env::var("SESSIONS_READ_TOKEN").ok();
    let mut locations = ReconstitutionLocations::from_env()?;
    if let Some(repos_root) = args.repos_root {
        locations.repos_root = repos_root;
    }

    let client = ReconstitutionClient::new(&store_url, read_token);
    let plan = client.reconstitute(&args.session_id, &locations).await?;
    println!(
        "restored session={} repo={} cwd={} transcript={}",
        plan.session_id,
        plan.repo_root.display(),
        plan.target_cwd.display(),
        plan.transcript_path.display(),
    );

    if args.no_resume {
        println!("native resume skipped (--no-resume)");
    } else {
        invoke_claude_resume(&plan)?;
    }
    Ok(())
}

/// The mode selected by the command line.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Version,
    Help,
    Restore(Arguments),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Arguments {
    session_id: String,
    repos_root: Option<PathBuf>,
    no_resume: bool,
}

/// A usage error. Always reported on stderr and always exit code 2.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ArgError {
    UnknownFlag(String),
    MissingValue(&'static str),
    UnexpectedArgument(String),
    MissingSessionId,
}

impl std::fmt::Display for ArgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownFlag(flag) => write!(f, "unknown option: {flag}"),
            Self::MissingValue(flag) => write!(f, "{flag} requires a value"),
            Self::UnexpectedArgument(arg) => write!(f, "unexpected argument: {arg}"),
            Self::MissingSessionId => write!(f, "session id is required"),
        }
    }
}

impl std::error::Error for ArgError {}

/// Parse argv (already stripped of argv[0]) in a single left-to-right pass.
///
/// Positional arguments remain supported (the session id is positional); only
/// unrecognized *flags* are an error.
fn parse_args(args: &[String]) -> Result<Command, ArgError> {
    let mut session_id: Option<String> = None;
    let mut repos_root: Option<PathBuf> = None;
    let mut no_resume = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--version" | "-V" => return Ok(Command::Version),
            "--help" | "-h" => return Ok(Command::Help),
            "--repos-root" => {
                let value = args
                    .get(index + 1)
                    .ok_or(ArgError::MissingValue("--repos-root"))?;
                repos_root = Some(PathBuf::from(value));
                index += 2;
            }
            "--no-resume" => {
                no_resume = true;
                index += 1;
            }
            value if value.starts_with('-') => {
                return Err(ArgError::UnknownFlag(value.to_string()))
            }
            value if session_id.is_none() => {
                session_id = Some(value.to_string());
                index += 1;
            }
            value => return Err(ArgError::UnexpectedArgument(value.to_string())),
        }
    }

    Ok(Command::Restore(Arguments {
        session_id: session_id.ok_or(ArgError::MissingSessionId)?,
        repos_root,
        no_resume,
    }))
}

fn print_usage() {
    println!(
        "\
{bin} {version} (APS-V1-0004 SCS {SCS_VERSION})

Usage: {bin} SESSION_ID [--repos-root PATH] [--no-resume]
       {bin} [--version | --help]

Options:
  --repos-root PATH  clone/lookup root for the session's repository.
  --no-resume        restore the files but do not launch `claude --resume`.
  --version, -V      print the version and exit.
  --help, -h         print this help and exit.

Requires SESSION_STORE_URL; SESSIONS_READ_TOKEN is used when the store requires it.
Restores ClaudeCode/claude-code-jsonl only. Missing repositories are cloned to
RECONSTITUTION_REPOS_ROOT/<metadata.repo> (default: ~/Code/<metadata.repo>),
then Claude's target-machine container path is recomputed and claude --resume runs.
Unrecognized options exit {EXIT_USAGE}.",
        bin = env!("CARGO_BIN_NAME"),
        version = env!("CARGO_PKG_VERSION"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, ArgError> {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        parse_args(&owned)
    }

    fn restore(args: &[&str]) -> Arguments {
        match parse(args).expect("expected a valid command line") {
            Command::Restore(args) => args,
            other => panic!("expected a restore command, got {other:?}"),
        }
    }

    #[test]
    fn version_long_and_short_forms_parse() {
        assert_eq!(parse(&["--version"]), Ok(Command::Version));
        assert_eq!(parse(&["-V"]), Ok(Command::Version));
    }

    #[test]
    fn help_long_and_short_forms_parse() {
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(parse(&["-h"]), Ok(Command::Help));
    }

    #[test]
    fn version_wins_over_a_session_id() {
        assert_eq!(parse(&["sess-1", "--version"]), Ok(Command::Version));
    }

    #[test]
    fn positional_session_id_is_required() {
        assert_eq!(parse(&[]), Err(ArgError::MissingSessionId));
        assert_eq!(parse(&["--no-resume"]), Err(ArgError::MissingSessionId));
        assert_eq!(restore(&["sess-1"]).session_id, "sess-1");
    }

    #[test]
    fn options_parse_around_the_positional() {
        let args = restore(&["--repos-root", "/tmp/repos", "sess-1", "--no-resume"]);
        assert_eq!(args.session_id, "sess-1");
        assert_eq!(args.repos_root, Some(PathBuf::from("/tmp/repos")));
        assert!(args.no_resume);
    }

    #[test]
    fn repos_root_value_is_never_read_as_a_session_id() {
        let args = restore(&["sess-1", "--repos-root", "/tmp/repos"]);
        assert_eq!(args.session_id, "sess-1");
        assert_eq!(args.repos_root, Some(PathBuf::from("/tmp/repos")));
    }

    #[test]
    fn repos_root_requires_a_value() {
        assert_eq!(
            parse(&["sess-1", "--repos-root"]),
            Err(ArgError::MissingValue("--repos-root"))
        );
    }

    #[test]
    fn unknown_flag_is_a_usage_error() {
        assert_eq!(
            parse(&["sess-1", "--bogus"]),
            Err(ArgError::UnknownFlag("--bogus".into()))
        );
        assert_eq!(
            parse(&["--verison"]),
            Err(ArgError::UnknownFlag("--verison".into()))
        );
    }

    #[test]
    fn a_second_positional_is_a_usage_error() {
        assert_eq!(
            parse(&["sess-1", "sess-2"]),
            Err(ArgError::UnexpectedArgument("sess-2".into()))
        );
    }

    #[test]
    fn arg_error_renders_readable_messages() {
        assert_eq!(
            ArgError::UnknownFlag("--nope".into()).to_string(),
            "unknown option: --nope"
        );
        assert_eq!(
            ArgError::MissingValue("--repos-root").to_string(),
            "--repos-root requires a value"
        );
        assert_eq!(
            ArgError::UnexpectedArgument("x".into()).to_string(),
            "unexpected argument: x"
        );
        assert_eq!(
            ArgError::MissingSessionId.to_string(),
            "session id is required"
        );
    }
}
