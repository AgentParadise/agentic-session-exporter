# Requirements

Non-negotiable properties of this repository. A change that breaks one of these
is wrong even if it passes CI, and CI should be taught to catch it.

## 1. Rust, and standalone

The exporter is Rust. It ships as a static-ish binary an operator can drop on a
laptop, a VPS, or into a container with no runtime to install first. That is the
whole reason a client like this can be embedded in a workspace image at all.

It depends on `apss-v1-0004-session-capture` and third-party crates, and on
**nothing else**. In particular it must never depend on a store implementation.
The moment it does, every consumer that embeds it inherits that dependency, and
the public-standard / public-client / private-store split collapses.

## 2. 100% test coverage

Not "high". 100%, enforced in CI, with any exclusion carrying an inline reason
at the exclusion site.

This is a data-capture client whose failure mode is silence: a session that is
not captured produces no error the user sees, it simply never exists. There is
no downstream check that notices. Coverage is the only mechanism that makes the
untested path visible before it costs someone a transcript.

Tests must exercise real behaviour. A test that constructs a value and asserts
its own fields back is not coverage; it is a tautology that happens to execute
lines. Parsers get real transcript fixtures, the uploader gets a fake store, and
the CLI gets black-box invocation asserting exit codes and stdout.

## 3. Cross-platform release matrix

The same binary runs in three quite different places, and all of them are
first-class:

| Target | Why |
| --- | --- |
| `linux/amd64`, `linux/arm64` | workspace containers (both arches are in use) |
| `darwin/arm64`, `darwin/amd64` | developer laptops, the largest live corpus today |
| `windows/amd64` | developer laptops |

Every release publishes all five, signed, with checksums. A platform that is not
built is a platform where capture does not exist, so "we only ship Linux" is a
product decision, not a build detail.

Additionally publish a minimal OCI image carrying the Linux binaries, so a
consumer image can `COPY --from=<image>@sha256:...` by immutable digest. A
GitHub release asset cannot be used that way, and telling image authors to
`curl` a tarball at build time is how unverified binaries end up in images.

## 4. Harnesses are a registry, not a hardcoded list

The exporter reads transcripts written by agent harnesses. Today: **Claude
Code**, **Codex**, **Cursor**. That list will grow, and adding to it must be a
small, obvious, well-shaped change rather than an archaeology exercise.

So each harness is a self-contained unit that declares:

- its **name**, as it appears in the envelope's `agent` field
- its **`source_format`**, the standard's identifier for the raw layout
- **where its transcripts live** by default, and the env var that overrides it
- **how to discover** them (a directory walk, a SQLite database, whatever it is)
- **how to parse** one into an envelope, preserving `raw` byte-for-byte

Adding a harness should mean adding one module and registering it. It must not
mean touching discovery, upload, retry, state, or the CLI. If adding a harness
requires editing any of those, the abstraction is wrong and the fix is the
abstraction, not the harness.

Every harness carries its own fixtures and its own round-trip test, because a
parser without a real transcript behind it is an assumption.

### Currently supported

| Harness | `source_format` | Default location | Override |
| --- | --- | --- | --- |
| Claude Code | `claude-jsonl-v1` | `~/.claude/projects` | `CLAUDE_PROJECTS_ROOT` |
| Codex | `codex-turns-v1` | `~/.codex/sessions` | `CODEX_SESSIONS_ROOT` |
| Cursor | (SQLite state db) | Cursor state database | `CURSOR_STATE_DB` |

## 5. Never lose or leak a session

Two failure modes matter more than throughput:

- **Loss.** A transcript that has been discovered and not yet accepted by a
  store must survive the process dying. State is on disk and re-swept.
- **Leak.** The write token must never reach stdout, stderr, a log line, or a
  crash report. The store URL must never carry credentials in userinfo.

Both need explicit tests, not review vigilance.
