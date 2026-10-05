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

## Branching and releasing

`main` is the only long-lived branch. **Publishing is a tag on `main`.**

```
PR -> main -> tag v<version> -> release.yml publishes
```

`release.yml` triggers on `push: tags: ["v*"]`. Every release so far was tagged
on a `main` commit; `v0.4.0` is `9e31764`, which is contained in `origin/main`
and in no other branch.

**There is no `release` branch.** An earlier revision of this file described one
("`main` is development, `release` publishes") along with a protected-branch
promotion and its enforcement rules. None of it exists: `git ls-remote --heads
origin` lists no `release`, and nothing has ever been promoted to it. That
description cost real time, because someone reading it stops and waits for a
gated promotion step that has no branch to promote to. Documentation that
invents a safeguard is worse than documentation that admits there is none, since
the reader trusts it and plans around it.

If a `release` branch is ever wanted, add it and restore that section then, not
before.

### Release Gate Success

The gate runs on pull requests and checks more than CI does: the version is
bumped and untagged, the lockfile is in sync, and all five platforms build in
release mode.

It also claims to verify that the OCI image builds multi-arch. **It does not.**
`image-dry-run` builds an unrelated scratch image containing only `.keep`; the
real OCI context is assembled in `release.yml`, after the tag. See issue #22.

Until that is fixed, validate the real image path before tagging by running
`release.yml` manually with `dry_run: true`, which builds and verifies the
actual release artifacts without publishing. Then, after publishing, verify the
image by immutable digest rather than by tag:

```bash
# both platforms present
docker manifest inspect ghcr.io/agentparadise/agentic-session-exporter:v<version>

# binaries present, mode 0755, and correct per architecture
#   COPY --from=<image>@sha256:<index-digest> into a normal Linux image,
#   then ls -l and run --version under --platform linux/amd64 and linux/arm64.
#   The two binaries should differ in SIZE - identical sizes mean one
#   architecture got a copy of the other.

cosign verify \
  --certificate-identity-regexp 'https://github.com/AgentParadise/agentic-session-exporter/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/agentparadise/agentic-session-exporter@sha256:<index-digest>
```

Tagging `v*` publishes the per-platform binaries, one checksum manifest, one
signature over it, and the signed OCI image.

## Before opening a PR

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo llvm-cov --locked --summary-only
cargo test
```
