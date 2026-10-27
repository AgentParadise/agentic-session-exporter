# Qualified capture delivery

This pins APSS commit `d8924a557cfb114534f4cdb69ed1535f0eec4c8f`
from [APSS PR #139](https://github.com/AgentParadise/agent-paradise-standards-system/pull/139),
which is merged and reachable from APSS `main` (the crate declares 2.1.0 there).
The lockfile resolves that public commit without local path overrides, and
v0.6.0 is built from it: the release ships binaries and an OCI image, not a
crates.io package. APSS 2.1 is not on crates.io yet; switching to the registry
package once it is published is tracked in
[#27](https://github.com/AgentParadise/agentic-session-exporter/issues/27).

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

## Revision confirmation (DRAFT)

`--capture-confirm` asks the store whether it holds one exact qualified revision,
so a caller can decide whether its own local copy may go. It implements the
unratified APS-V1-0004 draft in
[APSS PR #154](https://github.com/AgentParadise/agent-paradise-standards-system/pull/154)
(base 5.1.1 capabilities, base 5.3.1 confirmation, inventory profile
`HEAD /v1/transcripts`). Every wire detail (paths, query names, the stored-hash
header and the capability keys) lives in `src/revision_confirm.rs`, so a ruling
on the draft changes that module and not this CLI contract.

**Configuration.** `SESSION_STORE_URL` and `CAPTURE_READ_TOKEN`, a read-scoped
token (never the write token). No local state, no outbox, no spool directory.

**Input.** At most 16 KiB of JSON on stdin, unknown fields rejected:
`{"identity":{"source_instance_id":"...","harness":"...","native_session_id":"..."},"content_hash":"sha256:<64 lowercase hex>"}`.
`content_hash` is the original-content hash the caller UPLOADED and persisted.
The exporter never recomputes it.

**Requests.** `GET /v1/capabilities`, then, only when it advertises
`"revision_confirm": true` with a boolean `confirm_verifies_body`,
`HEAD /v1/transcripts?source_instance_id&harness&native_session_id&content_hash`.
Both carry `Authorization: Bearer`. Redirects are never followed. Each request
times out after 10 seconds.

**Output.** Exactly one JSON document on stdout:
`{"schema_version":1,"outcome":"...","status":<u16|null>,"stored_content_hash":<string|null>,"body_verified":<bool|null>}`.

| Store answer | `outcome` |
|---|---|
| capabilities 404, or `revision_confirm` absent or `false` | `not_advertised` (no HEAD sent) |
| capabilities malformed, or `revision_confirm: true` without a boolean `confirm_verifies_body` | `unexpected` |
| HEAD 200 with a valid `X-Stored-Content-Hash` | `confirmed` (`stored_content_hash`, `body_verified` set) |
| HEAD 200 without one | `unexpected` |
| 404 | `absent` (includes a superseded revision) |
| 401, 403 | `unauthorized` |
| 409 | `integrity_failure` |
| 503, timeout, connection failure | `unavailable` (`status` null for transport failure) |
| anything else, including any 3xx | `unexpected` |

No outcome is ever reported as another. A `confirmed` revision is durably
committed; it is NOT backup evidence (draft section 9), and the store may later
supersede it, so confirm immediately before acting.

**Exit codes.** `0`: the document carries the outcome, whatever it is. `1`:
configuration or input error, no document on stdout. `2`: usage error,
including `--json` or a second mode. The token never reaches stdout or stderr.
