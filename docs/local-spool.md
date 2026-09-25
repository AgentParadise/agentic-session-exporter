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

Discovery streams: Claude and Codex transcripts are read, archived, and dropped
one at a time, and a source file larger than `MAX_ENVELOPE_BYTES` is counted as
oversize without being read. Each envelope is serialized once, straight to its
object file, and abandoned if it grows past the bound. Peak memory is therefore
one transcript, not the corpus. Cursor threads still come from one bounded
database query (`CURSOR_LIMIT`).

## Trusted root

`EXPORTER_SPOOL_DIR`, `EXPORTER_CAPTURE_DIR`, and `EXPORTER_INVENTORY_DIR` are
opened as trusted roots, because they commonly sit beside directories an agent
can write. The root is resolved component by component without following a
symlink, except system links owned by root inside root-owned directories that
nobody else can write (macOS `/var` and `/tmp`). Every directory on the way must
be owned by root or the current user, and one writable by others must carry the
sticky bit. The root itself must be owned by the current user and not writable by
group or other; a root opened for writing is narrowed to mode 0700.

After that, objects are created, read, linked, and removed relative to the held
directory descriptors with no-follow semantics. SQLite databases are opened by
their canonical, symlink-free path with `SQLITE_OPEN_NOFOLLOW`, only after the
database and its `-wal`, `-shm`, and `-journal` sidecars are absent or private
single-link regular files. Every spool access re-proves that the root and object
directory are still the ones opened, so a component replaced after open fails
closed rather than redirecting a read or write. On platforms without `openat`
the same checks run against paths, which narrows but cannot close that window.

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
