# Runbook: first capture, start to finish

Goal: a session that is currently only on your disk becomes a row in a store you
can query. Should take about five minutes.

## 1. Confirm the binary runs

```bash
apss-session-exporter --version
```

Answers with no configuration at all - no store URL, no token, no network. If
this needs configuration to succeed, something is wrong: `--version` is what a
supervisor calls to prove the binary is present, and it must never do work.

## 2. Point it at a store

```bash
export SESSION_STORE_URL="https://sessions.example.com"
export SESSIONS_WRITE_TOKEN="…"
```

The URL must be an **origin only** - `scheme://host[:port]`, no path, no query,
no credentials in userinfo. A credential in a URL ends up in logs.

Omit the token only if the store genuinely accepts unauthenticated writes.

> **Watch this one.** Many stores serve `/healthz` unauthenticated while
> requiring auth to write. So a wrong or missing token can pass every health
> check and fail only at upload. If `--health` passes and sweeps still fail,
> suspect the token first.

## 3. Look before you upload

```bash
apss-session-exporter --dry-run
```

Reports what it found and what it would send, without sending. Run this first on
a machine with a long history - it is also how you discover a harness whose
transcripts live somewhere non-default.

Nothing found? See [troubleshooting](troubleshooting.md).

## 4. Sweep

```bash
apss-session-exporter
```

Ends with a summary line: `discovered`, `skipped_unchanged`, `uploaded`,
`accepted`, `duplicate`, `failed`.

Read `accepted`, not `uploaded`. Uploaded means it was sent; accepted means the
store took it.

## 5. Run it again

```bash
apss-session-exporter
```

The second run should report `skipped_unchanged` for everything and upload
nothing. That is the single most useful check here: it proves the state file
round-trips, which is what stops every sweep re-uploading your entire history.

If the second run uploads everything again, the state file is not being written
or not being found. Fix that before scheduling anything.

## 6. Keep it running

```bash
apss-session-exporter --loop
```

Or drive it from cron, a systemd timer, or a launchd agent - the binary does not
care, and a one-shot invocation is always safe to repeat.

## What "done" looks like

- `--version` answers with no configuration
- a sweep reports `accepted` greater than zero
- a second sweep uploads nothing
- the session is retrievable from the store by id
