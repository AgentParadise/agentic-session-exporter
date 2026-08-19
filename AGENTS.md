# agentic-session-exporter

Reference client for the **APS-V1-0004 Exporter profile**. Reads agent
transcripts written on the local machine, wraps each in an SCS envelope with the
provider's bytes preserved verbatim, and uploads it to any conformant session
store.

Read [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) before changing anything. It
records the properties this repo is held to and why, and a change that breaks
one is wrong even when CI is green.

## Writing style

**NEVER use em dashes or en dashes. Anywhere.** Not in code, comments, commit
messages, docs, PR descriptions, or generated output. Use a plain hyphen, a
comma, or a new sentence. This applies to the literal characters and to their
HTML entities.

Prose should read as though a careful engineer wrote it for another engineer:
say the thing, say why, stop.

## Non-negotiables

**Depend on the standard, never on a store.** This crate depends on
`apss-v1-0004-session-capture` and third-party crates. Adding a dependency on
any store implementation collapses the public-standard / public-client /
private-store split, and every consumer that embeds this binary inherits it. CI
checks this mechanically because it is exactly the kind of edge added innocently
to reuse one type.

**Coverage ratchets toward 100 and never down.** The threshold in
`.github/workflows/ci.yml` is measured, not aspirational. Raise it when coverage
rises. Lowering it to make a build pass is a decision about whether transcripts
may be lost silently, so treat a drop as a defect.

**Tests must exercise real behaviour.** A test that constructs a value and
asserts its own fields back is a tautology that happens to execute lines.
Parsers get real transcript fixtures, the uploader gets a fake store, and the
CLI gets black-box invocation asserting exit codes and output.

**Never lose or leak a session.** A discovered transcript that a store has not
accepted must survive the process dying. The write token must never reach
stdout, stderr, a log line, or a crash report. Both have explicit tests; keep
them passing rather than adjusting them.

## The CLI is a contract, not a convenience

Consumers embed this binary and call it from supervisors and container
finalizers. `--version` and `--help` must answer with no configuration and touch
nothing. Unknown flags must be rejected, never ignored.

That last one is not hypothetical: a consumer once probed with `--version`, an
earlier build ignored unknown flags and ran a full capture sweep instead, and a
health check "passed" by performing a real upload at workspace preflight. The
regression test for it lives in `tests/cli.rs`.

## Harnesses are a registry

Claude Code, Codex, and Cursor today. Adding one should mean adding a module and
registering it. If it requires touching discovery, upload, retry, state, or the
CLI, the abstraction is wrong and the abstraction is what to fix. Every harness
carries its own fixtures and round-trip test, because a parser without a real
transcript behind it is an assumption.

## Branching

`main` is development. `release` publishes.

`release` is protected, and the protection is the point: a gate that a direct
push can bypass is not a gate, it is a suggestion. Enforced on the branch:

- a pull request is required, so nothing lands by push
- **Release Gate Success** must pass, with `strict` on so the branch must be up
  to date with its base first (a gate that passed against stale code proves
  nothing about what actually merges)
- force pushes and deletion are blocked, so published history cannot be rewritten
  underneath a consumer who pinned a digest from it
- conversation resolution is required, so a raised concern cannot be merged past
  in silence

The gate itself is deliberately stricter than CI rather than a rerun of it:
version bumped and untagged, lockfile in sync, all five platforms building in
release mode, OCI image building multi-arch. Tagging `v*` then publishes
binaries, one checksum manifest, one signature over it, and the signed OCI image.

Approvals are set to zero on purpose for now, since this is a single-maintainer
repo and requiring a second approver would only teach people to bypass the
protection. Raise it when there is a second maintainer, not before.

## Before opening a PR

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo llvm-cov --locked --summary-only
cargo test
```
