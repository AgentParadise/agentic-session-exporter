# Optional workflow inventory replication

This transport implements the draft APS-V1-0004 inventory profile. It uses the
standard's types without depending on a session store implementation. Session
capture and inventory replication are separate operations. A remote outage must
not prevent local transcript capture or local relationship reconstruction.

Set `SESSION_STORE_URL`, `INVENTORY_WRITE_TOKEN`, and an absolute persistent
`EXPORTER_INVENTORY_DIR`. The token grants a specific source and producer
namespace. It is not the ordinary transcript write token.

Pipe one JSON operation to `apss-session-exporter --inventory-enqueue`. The
operation has an `operation` discriminator (`record`, `stage`, `manifest`, or
`publish`) and a `body` containing the corresponding APSS value. Input is bounded
to 2 MiB. The command validates and commits to SQLite with synchronous FULL
before returning `{"schema_version":1,"inserted":true}`. It performs no HTTP.
Replaying the same identity and payload returns `inserted:false`; conflicting
content fails without overwriting the prior operation.

Call `apss-session-exporter --inventory-drain 100` to attempt at most 100 pending
operations. The JSON result reports `acknowledged`, `pending`, `failed`, and the
remaining queue count. Exit 3 means some work remains, including work outside
this bounded pass. Schedule another pass with backoff. Exit 0 means the local
queue was empty at the final count, not that independent workflow capture is
complete.

Pending publication responses remain queued. Attempts rotate so a waiting
publication cannot prevent later evidence from uploading. HTTP errors, malformed
responses, and interrupted processes retain unacknowledged operations. A crash
after remote acceptance can replay the request; immutable server identities make
that retry safe. Acknowledged payloads remain for conflict detection.

The outbox binds to a hash of the destination URL. Token rotation at the same
destination is allowed. Changing destination requires a separate outbox and
explicitly enqueuing the desired inventory. Tokens and raw destination URLs are
not persisted in the database. HTTP redirects are rejected, response bodies are
bounded, and request timeouts are 30 seconds.

## Delivery status

The library and CLI transport are implemented. Syntropic137's production
replication adapter, coordinated release dependency, outbox retention policy,
and complete integration acceptance remain outstanding. Development currently
uses the adjacent APSS 2.1.0 worktree through a command-line Cargo patch; that
patch is not a distributable dependency pin.
