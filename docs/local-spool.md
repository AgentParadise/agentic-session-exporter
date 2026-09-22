# Local envelope capture

Capture without a session-store URL, token, or network connection:

```sh
export EXPORTER_SPOOL_DIR=/persistent/session-envelopes
apss-session-exporter --spool-only
```

Normal transcript-root configuration still applies. Mount the spool and native
transcript roots on persistent storage when running inside a container.
The command performs one discovery sweep and writes a JSON summary to stdout.
Oversized envelopes are counted and cause exit status 3. A successful sweep
does not prove every expected session was discovered or finished.

Each distinct serialized envelope becomes an immutable object addressed by its
SHA-256 digest. The index publishes it only after its bytes are flushed. Capture
retries reuse existing objects; changed envelopes retain their earlier revisions.
The digest identifies exact envelope bytes, not the standard's raw-content hash.

Read the first inventory page:

```sh
apss-session-exporter --spool-list 0
```

Pages contain at most 500 entries, a `watermark`, and an optional `next_after`.
Continue with `--spool-list NEXT_AFTER:WATERMARK` until `next_after` is null.
Reusing the first watermark excludes concurrently added revisions. Sequence
numbers may have gaps. To discover later captures, start another traversal
after the previous watermark without specifying an upper bound.

Read exact envelope bytes with `--spool-read SEQUENCE`. Reads check the stored
size and digest and fail for missing or corrupt objects. Both read commands
require only `EXPORTER_SPOOL_DIR`; they do not rediscover native transcripts.
`MAX_ENVELOPE_BYTES` bounds a read, with the usual 512 MiB default.

Keep the complete spool directory, including its SQLite database and WAL files.
Do not copy a live SQLite database file alone. This interface currently provides
capture and inspection; it does not automatically replicate, prune, or declare
workflow coverage complete.
