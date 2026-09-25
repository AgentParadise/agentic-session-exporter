# Qualified capture delivery

This draft pins APSS commit `d8924a557cfb114534f4cdb69ed1535f0eec4c8f`
from [APSS PR #139](https://github.com/AgentParadise/agent-paradise-standards-system/pull/139).
The lockfile resolves that public commit without local path overrides. Replace
the Git dependency with the published APSS 2.1 package before release; the
coordinated standard release and consumer release gates remain required.

The optional `QualifiedCaptureClient` uploads a standard envelope to
`POST /v1/transcripts` with its source installation, harness, and native ID.
Native IDs and raw transcript content remain unchanged. The client validates
the receipt against the qualified storage key and original-content hash before
acknowledging success. Redirects, mismatched receipts, responses over 4096 bytes,
and HTTP errors fail without exposing credentials or response bodies.

`CaptureOutbox` combines the durable local spool with a SQLite delivery ledger.
Enqueue publishes the immutable spool object before its delivery reference.
Draining loads one bounded envelope at a time, sends at most 50 queued revisions,
and persists the verified receipt after remote acceptance. Failed or corrupt
objects remain pending. Attempts rotate pending work so one failed capture does
not permanently prevent later captures from being attempted.

The ledger binds to one destination. Token rotation for that destination is
allowed; moving pending work to another store is rejected. Concurrent drains may
repeat uploads, relying on idempotent capture ingestion. A process interrupted
after remote acceptance but before local acknowledgement retries safely.

Tests exercise process-local queue reopening, rejected credentials followed by
token rotation, exact CRLF payload transmission, acknowledgement, duplicate
enqueue, and destination mismatch. Production scheduling, retention
quotas, and production lifecycle integration are not complete yet. This queue
must remain on durable storage until those lifecycle policies are implemented.

Syntropic137's `test_real_capture_delivery_preserves_versions_and_survives_revocation`
also exercises the actual exporter executable, SeshMagic server, and PostgreSQL:
offline enqueue, failed drain, process restart, successful delivery, server-side
redaction, exact CRLF reads with integrity hashes, historical version reads, and
pending work retained after capture-write grants are revoked. This does not yet
prove production scheduling or native harness capture hooks.

## CLI

Set `SESSION_STORE_URL`, `CAPTURE_WRITE_TOKEN`, and an absolute
`EXPORTER_CAPTURE_DIR`. The capture token must have a grant for the exact source
installation and harness. Enqueue itself performs no HTTP request. The directory
is opened as a trusted root, as described in [local-spool.md](local-spool.md).

`apss-session-exporter --capture-enqueue` reads one I-JSON object from stdin:
`{"identity":{"source_instance_id":"...","harness":"...","native_session_id":"..."},"envelope":{...}}`.
The envelope follows APS-V1-0004. Input is limited to 64 MiB. Success emits
`{"schema_version":1,"inserted":true}` only after durable enqueue; an identical
retry emits `inserted:false`.

`apss-session-exporter --capture-drain N` attempts 1 to 50 queued captures and
emits `acknowledged`, `failed`, `remaining`, `deleted`, `withdrawn`, `fenced`, `busy`,
`integrity`, and `quarantined` counts. Exit 0 means no pending work remains and
nothing is quarantined; exit 3 means pending work remains or a row is
quarantined. Invalid flags exit 2. Operational
or input failures return nonzero without emitting an acceptance receipt. These
modes always emit JSON and reject `--json` and capture-sweep options.

`apss-session-exporter --capture-receipt` accepts the same qualified-envelope
input as enqueue. It validates and computes content identity using the standard,
then reads the durable acknowledgement ledger without HTTP. Output is
`{"schema_version":1,"receipt":null}` until that exact version is acknowledged;
a committed receipt includes `storage_key`, `content_hash`,
`stored_content_hash`, and `duplicate`. Receipt lookup survives process restart
and never treats enqueue acceptance as remote acceptance. It shares the capture
input limit and rejects sweep options and `--json`.


## Durable exact-revision deletion

`--capture-delete` reads at most 16 KiB of JSON containing `identity` (the same
qualified source, harness and native ID as capture) and `content_hash` (the exact
original APSS SHA-256). It persists a deletion request in the destination-bound
capture outbox. Repeating it is idempotent. Re-enqueue of that revision is rejected
and receipt lookup no longer reports the historical acceptance as current access.

`--capture-drain` sends pending DELETE requests before uploads, within its existing
operation limit. Only HTTP 204 acknowledges deletion. Failed requests survive
restart; credentials and response bodies are excluded from errors.

Upload and deletion of one revision never overlap, even across processes that
share the outbox. Each remote request holds a lease row for its storage key and
content hash, and the tombstone check before an upload happens only while that
lease is held. A deletion is therefore sent entirely before an upload, which
then sees the tombstone and never goes out, or entirely after it, and removes
what the upload stored. A drain that finds a revision leased skips it for that
pass and reports it as `busy`; it stays pending. A lease expires after five
minutes, far beyond the 30 second request timeout, so a holder that died
releases it unattended.

Leases are fenced. Every grant of a revision's lease carries a token one above
the previous grant, and the holder re-proves it holds that exact token, renewing
the lease, immediately before sending, and again inside the transaction that
records the result. Wall-clock time only decides when an abandoned lease may be
taken over, never who may act. A holder paused or clock-jumped past expiry whose
lease was taken over neither sends nor records anything: it reports `fenced`
and the row stays pending for a later pass. The store's tombstone check remains
the backstop for a request already on the wire when that happens.

The store is the final authority. A store's 410 upload response means it holds a
tombstone for that revision: the upload is `withdrawn`, which is terminal. It is
never retried, the queued body is dropped, and no DELETE is sent because the
store already has one. Re-enqueue of that revision is rejected.

A queued row whose spool object is missing, altered, or undecodable, whose
identity no longer decodes, or whose recorded content hash no longer matches its
bytes is quarantined and counted under `integrity` for that pass. Retrying cannot
repair it, so it is set aside with its identity and never retried, and the pass
continues with unrelated work. `quarantined` reports the total set aside, by
identity only, never transcript content. A failure of the spool as a whole, such
as its object directory being replaced, is `failed` and retried instead. SQLite
schema version 5 preserves existing deliveries during upgrade.

This prevents queued transmission and remote resurrection once the store accepts
the tombstone. Drain also removes deleted envelope files from the outbox spool,
bounded by the operation limit. A shared file remains while another qualified
identity still needs delivery. Legacy hashes are durably backfilled before
unlink, and cleanup repeats safely after interruption. Identity, receipt and
tombstone metadata remain; this does not purge workspace source transcripts.

`--envelope-hash` validates an envelope from bounded stdin and returns its original
APSS content hash. It requires no configuration, writes no state, and performs no
network request. Origin retention uses it to persist deletion identity before
removing local bytes, without duplicating canonical hashing in another language.
