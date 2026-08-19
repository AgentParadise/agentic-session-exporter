# agentic-session-exporter

Reference client for the **APS-V1-0004 session-capture Exporter profile**.

It reads agent transcripts written on the local machine, wraps each one in an
SCS envelope with the provider's bytes preserved verbatim, and uploads it to any
conformant session store.

It is a client of a **standard**, not of a product. It depends on
`apss-v1-0004-session-capture` and third-party crates, and on no store
implementation. Point it at whichever store you run.

## Where it runs

The same binary is used in three places, and none is an afterthought:

- **a developer laptop**, capturing local Claude / Codex / Cursor sessions
- **a VPS or long-lived box**, the same
- **inside a workspace container**, invoked by the agentic-primitives
  session-store capability at finalize

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

The `apss-` prefix is deliberate. This is the reference client of a standard, so
its executables are named for the standard rather than for a vendor or for this
repository.

## Supported harnesses

Claude Code, Codex, and Cursor. Adding another is meant to be a small, obvious
change — see [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) §4 for the shape a
harness has to take and why the rest of the pipeline must not need editing.

## Requirements that govern this repo

[docs/REQUIREMENTS.md](docs/REQUIREMENTS.md): Rust and standalone, 100% test
coverage, a five-platform signed release matrix plus an OCI image for
`COPY --from=`, harnesses as a registry, and never lose or leak a session.

## Status

Extracted from a private store repository so that a workspace image can embed it
without depending on one vendor's product. Pinned to
`apss-v1-0004-session-capture` **1.0.0** at present: 2.0.0 carries
`origin.deployment` and is merged upstream but not yet published to crates.io,
so `origin.deployment` cannot be stamped until that release lands.
