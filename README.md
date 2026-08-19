<p align="center">
  <img src="assets/banner.svg" alt="agentic-session-exporter" width="100%">
</p>

<p align="center">
  <a href="https://github.com/AgentParadise/agentic-session-exporter/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/AgentParadise/agentic-session-exporter/ci.yml?branch=main&label=CI&style=for-the-badge&labelColor=0d1117&color=2dd4a7"></a>
  <a href="https://github.com/AgentParadise/agentic-session-exporter/actions/workflows/release.yml"><img alt="Release" src="https://img.shields.io/github/actions/workflow/status/AgentParadise/agentic-session-exporter/release.yml?label=release&style=for-the-badge&labelColor=0d1117&color=2dd4a7"></a>
  <a href="https://github.com/AgentParadise/agentic-session-exporter/releases"><img alt="Latest release" src="https://img.shields.io/github/v/release/AgentParadise/agentic-session-exporter?style=for-the-badge&labelColor=0d1117&color=2dd4a7"></a>
</p>

<p align="center">
  <a href="LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-8b9bb4?style=flat-square&labelColor=0d1117"></a>
  <img alt="Rust 1.88+" src="https://img.shields.io/badge/rust-1.88%2B-8b9bb4?style=flat-square&labelColor=0d1117">
  <img alt="Platforms" src="https://img.shields.io/badge/platforms-linux%20%C2%B7%20macos%20%C2%B7%20windows-8b9bb4?style=flat-square&labelColor=0d1117">
  <img alt="Harnesses" src="https://img.shields.io/badge/harnesses-claude%20%C2%B7%20codex%20%C2%B7%20cursor-8b9bb4?style=flat-square&labelColor=0d1117">
  <a href="#the-standard-this-implements"><img alt="Implements APS-V1-0004" src="https://img.shields.io/badge/implements-APS--V1--0004-2dd4a7?style=flat-square&labelColor=0d1117"></a>
</p>

# agentic-session-exporter

Your agent sessions are written to disk by whichever harness produced them, in
whatever shape that harness felt like, and then they sit there. This reads them,
wraps each one in an **SCS envelope** with the provider's bytes preserved
verbatim, and uploads it to a session store you choose.

It is a client of a **standard**, not of a product. It depends on
`apss-v1-0004-session-capture` and third-party crates, and on no store
implementation whatsoever. Point it at whichever store you run.

## The standard this implements

**[APS-V1-0004 - Session Capture](https://github.com/AgentParadise/agent-paradise-standards-system/tree/main/standards/v1/APS-V1-0004-session-capture)**

That identifier is opaque until you have seen one, so briefly: APS-V1-0004 is a
public specification from the [Agent Paradise Standards
System](https://github.com/AgentParadise/agent-paradise-standards-system). It
answers one question - *what does a captured agent session look like, so
that any tool can write one and any store can read it* - and it answers it
without requiring anyone to agree on a provider's internal transcript format.

The core idea is a thin **envelope**: a small set of fields a store can sort,
deduplicate and attribute by (who, when, where from, which agent), wrapped
around the provider's raw transcript preserved **byte for byte**. Because the
standard never claims to understand a provider's internals, it cannot rot when
those internals change.

It defines three conformance profiles:

| Profile | Meaning |
| --- | --- |
| **Source** | read a provider's local transcripts into envelopes |
| **Exporter** | push envelopes to a store's batch endpoint |
| **Reconstitutor** | write a stored session back to disk and resume it natively |

**This repository is the reference implementation of the Exporter profile** (and
of Source, since it has to read transcripts to export them). A store implements
the receive side. Neither depends on the other - both depend on the
standard, which is the entire point: you can swap either half without the other
noticing.

Worth reading if you are integrating: the
[specification](https://github.com/AgentParadise/agent-paradise-standards-system/blob/main/standards/v1/APS-V1-0004-session-capture/docs/01_spec.md)
and the
[envelope JSON Schema](https://github.com/AgentParadise/agent-paradise-standards-system/blob/main/standards/v1/APS-V1-0004-session-capture/schemas/session-envelope.schema.json).

## Quick start

Capture the sessions already on your machine:

```bash
export SESSION_STORE_URL="https://sessions.example.com"
export SESSIONS_WRITE_TOKEN="…"        # omit for an unauthenticated store

apss-session-exporter --dry-run        # see what it would send, upload nothing
apss-session-exporter                  # sweep and upload
apss-session-exporter --health         # is the store reachable?
```

It discovers transcripts in each harness's default location, so there is usually
nothing else to configure. Nothing is uploaded twice: the store deduplicates on
`content_hash`, so re-running a sweep is always safe.

Keep it running:

```bash
apss-session-exporter --loop           # sweep continuously
```

Bring a session back to another machine and resume it natively:

```bash
apss-session-reconstitute <session-id>
```

## Supported harnesses

| Harness | `source_format` | Read from | Override with |
| --- | --- | --- | --- |
| Claude Code | `claude-jsonl-v1` | `~/.claude/projects` | `CLAUDE_PROJECTS_ROOT` |
| Codex | `codex-turns-v1` | `~/.codex/sessions` | `CODEX_SESSIONS_ROOT` |
| Cursor | SQLite state db | Cursor's state database | `CURSOR_STATE_DB` |

Adding one is meant to be a small, contained change - one module, registered.
See [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) section 6 for the shape a
harness has to take, and why the rest of the pipeline must not need editing.

## Supported machines

Every release publishes all five, signed, with checksums:

| Platform | Target | Typical use |
| --- | --- | --- |
| Linux x86-64 | `x86_64-unknown-linux-gnu` | workspace containers, servers |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | ARM containers, ARM servers |
| macOS Apple Silicon | `aarch64-apple-darwin` | developer laptops |
| macOS Intel | `x86_64-apple-darwin` | developer laptops |
| Windows x86-64 | `x86_64-pc-windows-msvc` | developer laptops |

A platform that is not built is a platform where capture does not exist, so this
list is a product decision rather than a build detail.

## Install

**From a release** - download the binary for your platform, then verify before
trusting it:

```bash
# One manifest covers every asset; one signature covers the manifest.
cosign verify-blob --signature SHA256SUMS.sig \
  --certificate-identity-regexp 'https://github.com/AgentParadise/agentic-session-exporter/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
sha256sum --check SHA256SUMS --ignore-missing
```

**Into a container image** - copy from the published OCI image by digest, never
by tag:

```dockerfile
COPY --from=ghcr.io/agentparadise/agentic-session-exporter@sha256:… \
     /apss-session-exporter /usr/local/bin/apss-session-exporter
```

A digest is the only immutable reference. Pinning a tag means an upstream push
silently changes what your image ships.

## Binaries

| Name | Purpose |
| --- | --- |
| `apss-session-exporter` | discover local transcripts, upload envelopes |
| `apss-session-reconstitute` | write a stored session back to disk to resume it |

`SeshMagicSessionExporter` and `SeshMagicSessionReconstitute` are built from the
same sources as **compatibility aliases** for deployments that hardcode the
pre-rename names. They are scheduled for removal in a declared major release,
never silently: an operator whose capture stops with no message is worse off
than one told to rename a file.

The `apss-` prefix is deliberate - this is the reference client of a standard, so
its executables are named for the standard rather than for a vendor or for this
repository.

## Documentation

| | |
| --- | --- |
| [Requirements](docs/REQUIREMENTS.md) | what this repo is held to, and why |
| [Quick start runbook](docs/runbooks/quick-start.md) | first capture, start to finish |
| [Container embedding](docs/runbooks/embedding-in-a-container-image.md) | baking it into a workspace image |
| [Troubleshooting](docs/runbooks/troubleshooting.md) | when nothing is arriving |

## Security

Artifacts are signed with cosign keyless OIDC. The write token is never printed
to stdout, stderr, or a log line - [there is a test for it](tests/cli.rs),
because this binary's output is routinely captured into durable logs by whatever
invokes it.

Report vulnerabilities privately via GitHub Security Advisories.

## License

MIT - see [LICENSE](LICENSE).
