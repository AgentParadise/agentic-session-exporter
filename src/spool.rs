//! Durable local SCS envelope revisions, independent of a remote session store.
//!
//! Exact serialized objects are immutable. SQLite publishes a reference only
//! after the object and directory are durable. An interrupted object write may
//! leave an unreferenced object; it can never publish a missing acknowledged body.

use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use session_capture::SessionEnvelope;
use sha2::{Digest, Sha256};

use crate::config::Config;
use crate::secure_fs::{sqlite_flags, SecureDir};
use crate::sources::{Found, VisitError};

const DATABASE: &str = "inventory.sqlite3";
pub const SPOOL_SCHEMA_VERSION: u32 = 1;
const MAX_PAGE: usize = 500;

#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    #[error("local spool I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("local spool index failed: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("local envelope encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("source discovery failed: {0}")]
    Source(#[from] crate::sources::SourceError),
    #[error("unsupported local spool schema")]
    Schema,
    #[error("local spool object failed integrity validation")]
    Integrity,
    #[error("invalid local spool pagination bounds")]
    Bounds,
    #[error("local spool revision does not exist")]
    NotFound,
    #[error("local spool directory was replaced or is not trusted")]
    Untrusted,
}

impl SpoolError {
    /// True when this revision's own stored bytes or index row are unusable,
    /// as opposed to the spool as a whole failing (storage, trust, schema).
    /// Retrying an integrity failure cannot succeed; retrying the rest can.
    pub fn is_integrity(&self) -> bool {
        match self {
            Self::Integrity | Self::NotFound | Self::Json(_) => true,
            Self::Io(error) => matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidData
            ),
            _ => false,
        }
    }
}

impl From<VisitError<SpoolError>> for SpoolError {
    fn from(error: VisitError<SpoolError>) -> Self {
        match error {
            VisitError::Source(error) => Self::Source(error),
            VisitError::Visitor(error) => error,
        }
    }
}

/// Counts and hashes serialized bytes on their way to disk, refusing to write
/// past `limit`, so an envelope is serialized exactly once and never held
/// whole in memory.
struct BoundedHasher<W: Write> {
    inner: W,
    hasher: Sha256,
    written: usize,
    limit: usize,
    exceeded: bool,
}

impl<W: Write> Write for BoundedHasher<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.written.saturating_add(buf.len()) > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other("envelope exceeds the size bound"));
        }
        let count = self.inner.write(buf)?;
        self.hasher.update(&buf[..count]);
        self.written += count;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SpoolEntry {
    pub sequence: u64,
    pub archive_sha256: String,
    pub byte_count: u64,
    pub agent: String,
    pub native_session_id: String,
}

#[derive(Debug, Serialize)]
pub struct SpoolPage {
    pub schema_version: u32,
    pub watermark: u64,
    pub entries: Vec<SpoolEntry>,
    pub next_after: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct SpoolSummary {
    pub schema_version: u32,
    pub discovered: usize,
    pub stored: usize,
    pub duplicate: usize,
    pub skipped_oversize: usize,
    pub watermark: u64,
}

pub struct LocalSpool {
    dir: SecureDir,
    objects: SecureDir,
    db: Connection,
}

impl LocalSpool {
    pub fn open(root: &Path) -> Result<Self, SpoolError> {
        Self::open_in(SecureDir::open_root(root, true)?)
    }

    /// Open a spool in a directory already verified beneath a trusted root.
    pub(crate) fn open_in(dir: SecureDir) -> Result<Self, SpoolError> {
        let objects = dir.child("objects", true)?;
        let db = Connection::open_with_flags(dir.sqlite_path(DATABASE)?, sqlite_flags(false))?;
        dir.verify()?;
        db.busy_timeout(Duration::from_secs(30))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let version: u32 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version > SPOOL_SCHEMA_VERSION {
            return Err(SpoolError::Schema);
        }
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS envelope_revisions (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                archive_sha256 TEXT NOT NULL UNIQUE,
                byte_count INTEGER NOT NULL,
                agent TEXT NOT NULL,
                native_session_id TEXT NOT NULL
             );
             PRAGMA user_version=1;",
        )?;
        dir.sync()?;
        Ok(Self { dir, objects, db })
    }

    pub fn open_readonly(root: &Path) -> Result<Self, SpoolError> {
        let dir = SecureDir::open_root(root, false)?;
        let objects = dir.child("objects", false)?;
        let db = Connection::open_with_flags(dir.sqlite_path(DATABASE)?, sqlite_flags(true))?;
        dir.verify()?;
        db.busy_timeout(Duration::from_secs(30))?;
        let version: u32 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version != SPOOL_SCHEMA_VERSION {
            return Err(SpoolError::Schema);
        }
        Ok(Self { dir, objects, db })
    }

    /// Every access re-proves that the root and object directory are the
    /// ones opened, so a component replaced after open fails closed.
    fn guard(&self) -> Result<(), SpoolError> {
        self.dir
            .verify()
            .and_then(|()| self.objects.verify())
            .map_err(|_| SpoolError::Untrusted)
    }

    pub fn store(&mut self, envelope: &SessionEnvelope) -> Result<(SpoolEntry, bool), SpoolError> {
        self.store_bounded(envelope, usize::MAX)?
            .ok_or(SpoolError::Integrity)
    }

    /// Serialize once, straight to a private temporary object, hashing as it
    /// goes. Returns `None`, leaving nothing behind, when the serialized
    /// envelope would exceed `max_bytes`.
    pub fn store_bounded(
        &mut self,
        envelope: &SessionEnvelope,
        max_bytes: usize,
    ) -> Result<Option<(SpoolEntry, bool)>, SpoolError> {
        self.guard()?;
        let mut temporary = self.objects.create_temp()?;
        let mut writer = BoundedHasher {
            inner: std::io::BufWriter::new(temporary.file().try_clone()?),
            hasher: Sha256::new(),
            written: 0,
            limit: max_bytes,
            exceeded: false,
        };
        if let Err(error) = serde_json::to_writer(&mut writer, envelope) {
            return if writer.exceeded {
                Ok(None)
            } else {
                Err(error.into())
            };
        }
        writer.flush()?;
        let BoundedHasher {
            inner,
            hasher,
            written: byte_count,
            ..
        } = writer;
        drop(inner);
        let digest = format!("{:x}", hasher.finalize());
        temporary.file().sync_all()?;
        if !temporary.publish_noclobber(&digest)? && self.digest_of(&digest, byte_count)? != digest
        {
            return Err(SpoolError::Integrity);
        }
        drop(temporary);
        self.objects.sync()?;
        let tx = self.db.transaction()?;
        let inserted = tx.execute(
            "INSERT INTO envelope_revisions (archive_sha256,byte_count,agent,native_session_id)
             VALUES (?1,?2,?3,?4) ON CONFLICT(archive_sha256) DO NOTHING",
            params![
                digest,
                byte_count as u64,
                envelope.agent,
                envelope.session_id
            ],
        )? == 1;
        let entry = tx.query_row(
            "SELECT sequence,archive_sha256,byte_count,agent,native_session_id
             FROM envelope_revisions WHERE archive_sha256=?1",
            [&digest],
            entry_from_row,
        )?;
        tx.commit()?;
        Ok(Some((entry, inserted)))
    }

    /// Stream-hash an existing object, refusing one longer than expected.
    fn digest_of(&self, name: &str, expected: usize) -> Result<String, SpoolError> {
        let mut reader = self.objects.open_read(name)?.take(expected as u64 + 1);
        let mut hasher = Sha256::new();
        let mut buffer = vec![0; 64 * 1024];
        let mut total = 0;
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            total += count;
        }
        if total != expected {
            return Err(SpoolError::Integrity);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    /// The owning outbox must serialize this with its writers and prove that
    /// no undelivered identity still requires these bytes. Metadata is retained.
    pub(crate) fn discard_body(&self, sequence: u64) -> Result<(), SpoolError> {
        self.guard()?;
        let digest: String = self.db.query_row(
            "SELECT archive_sha256 FROM envelope_revisions WHERE sequence=?1",
            [sequence],
            |r| r.get(0),
        )?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(SpoolError::Integrity);
        }
        self.objects.remove(&digest)?;
        self.objects.sync()?;
        Ok(())
    }

    pub fn watermark(&self) -> Result<u64, SpoolError> {
        Ok(self.db.query_row(
            "SELECT COALESCE(MAX(sequence),0) FROM envelope_revisions",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn page(
        &self,
        after: u64,
        through: Option<u64>,
        limit: usize,
    ) -> Result<SpoolPage, SpoolError> {
        let current = self.watermark()?;
        let watermark = through.unwrap_or(current);
        if limit == 0 || limit > MAX_PAGE || after > watermark || watermark > current {
            return Err(SpoolError::Bounds);
        }
        let mut query = self.db.prepare(
            "SELECT sequence,archive_sha256,byte_count,agent,native_session_id
             FROM envelope_revisions WHERE sequence>?1 AND sequence<=?2
             ORDER BY sequence LIMIT ?3",
        )?;
        let mut entries = query
            .query_map(params![after, watermark, limit + 1], entry_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let next_after = if has_more {
            entries.last().map(|item| item.sequence)
        } else {
            None
        };
        Ok(SpoolPage {
            schema_version: SPOOL_SCHEMA_VERSION,
            watermark,
            entries,
            next_after,
        })
    }

    pub fn read(&self, sequence: u64, max_bytes: usize) -> Result<Vec<u8>, SpoolError> {
        self.guard()?;
        let entry = self
            .db
            .query_row(
                "SELECT sequence,archive_sha256,byte_count,agent,native_session_id
             FROM envelope_revisions WHERE sequence=?1",
                [sequence],
                entry_from_row,
            )
            .optional()?
            .ok_or(SpoolError::NotFound)?;
        if entry.archive_sha256.len() != 64
            || !entry.archive_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || entry.byte_count > max_bytes as u64
        {
            return Err(SpoolError::Integrity);
        }
        let file = self.objects.open_read(&entry.archive_sha256)?;
        if file.metadata()?.len() != entry.byte_count {
            return Err(SpoolError::Integrity);
        }
        let mut bytes = Vec::new();
        file.take(entry.byte_count + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 != entry.byte_count
            || format!("{:x}", Sha256::digest(&bytes)) != entry.archive_sha256
        {
            return Err(SpoolError::Integrity);
        }
        Ok(bytes)
    }
}

fn entry_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SpoolEntry> {
    Ok(SpoolEntry {
        sequence: row.get(0)?,
        archive_sha256: row.get(1)?,
        byte_count: row.get(2)?,
        agent: row.get(3)?,
        native_session_id: row.get(4)?,
    })
}

/// Archive one sweep. Sources are read one at a time and each source file is
/// bounded before allocation, so peak memory is one transcript, not the corpus.
pub fn capture_local(cfg: &Config, root: &Path) -> Result<SpoolSummary, SpoolError> {
    let mut spool = LocalSpool::open(root)?;
    let mut summary = SpoolSummary {
        schema_version: SPOOL_SCHEMA_VERSION,
        ..Default::default()
    };
    let limit = cfg.max_envelope_bytes;
    crate::visit_all(cfg, limit as u64, &mut |found| {
        summary.discovered += 1;
        let Found::Transcript(source) = found else {
            summary.skipped_oversize += 1;
            return Ok(());
        };
        match spool.store_bounded(&source.envelope, limit)? {
            None => summary.skipped_oversize += 1,
            Some((_, true)) => summary.stored += 1,
            Some((_, false)) => summary.duplicate += 1,
        }
        Ok::<(), SpoolError>(())
    })?;
    summary.watermark = spool.watermark()?;
    Ok(summary)
}
