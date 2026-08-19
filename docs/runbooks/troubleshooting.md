# Runbook: nothing is arriving

Work down the list. Each step distinguishes two causes rather than guessing.

## Nothing is discovered

```bash
apss-session-exporter --dry-run
```

`discovered=0` means the transcripts are not where it looked. Check the harness
table in the [README](../README.md) and set the relevant override
(`CLAUDE_PROJECTS_ROOT`, `CODEX_SESSIONS_ROOT`, `CURSOR_STATE_DB`).

Inside a container this is the usual culprit: the agent's home directory is
often not the one the exporter runs under.

## Everything is discovered, every time

If each sweep re-uploads the same sessions, the state file is not persisting.
Check `EXPORTER_STATE_FILE` points somewhere writable that **outlives the
process** — a tmpfs path inside a container does not.

Harmless to the store, which deduplicates on `content_hash`, but it wastes the
entire upload budget on every run.

## Uploads fail with no reason given

An embedded caller may deliberately withhold the exporter's own output, because
this binary is operator-supplied and a build that printed its environment would
leak the store credential into durable logs.

Re-run it by hand with the same environment. The spool is retained and the store
deduplicates, so a repeat sweep costs nothing:

```bash
apss-session-exporter        # the withheld output, in your terminal
```

## Health passes but uploads 401

Expected, and the most common misconfiguration. `/healthz` is typically
unauthenticated while writes are not, so a missing, wrong, expired, or
wrong-scoped token passes every preflight check and fails only at upload.

Verify the token independently of the exporter before looking anywhere else.

## The store is unreachable from inside a container

Host resolution differs inside a container. A name that resolves on your machine
— a tailnet MagicDNS name, an `/etc/hosts` entry — usually does **not** resolve
inside a container, because Docker's embedded resolver does not see it.

Test from inside the container, not from the host:

```bash
docker run --rm --entrypoint sh <image> -c 'wget -qO- http://<store>/healthz'
```

If the name fails and the IP works, use the IP.

## Sessions upload but you cannot find them

Filter by the wrong field and everything looks missing. `origin.environment` is
the CLASS of runtime (`local`, `vps`, `container`, `workflow`) — every
containerised run reports the same value. To tell deployments apart, filter on
the deployment identity or on tags, not on the environment class.
