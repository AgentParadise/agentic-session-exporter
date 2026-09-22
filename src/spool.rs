//! Durable local SCS envelope revisions, independent of a remote session store.
//!
//! Exact serialized objects are immutable. SQLite publishes a reference only
//! after the object and directory are durable. An interrupted object write may
//! leave an unreferenced object; it can never publish a missing acknowledged body.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use session_capture::SessionEnvelope;
use sha2::{Digest, Sha256};

use crate::{config::Config, discover_all};

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
    root: PathBuf,
    db: Connection,
}

impl LocalSpool {
    pub fn open(root: &Path) -> Result<Self, SpoolError> {
        private_directory(root)?;
        private_directory(&root.join("objects"))?;
        let db = Connection::open(root.join("inventory.sqlite3"))?;
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
        sync_directory(root)?;
        Ok(Self {
            root: root.to_owned(),
            db,
        })
    }

    pub fn open_readonly(root: &Path) -> Result<Self, SpoolError> {
        let db = Connection::open_with_flags(
            root.join("inventory.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        db.busy_timeout(Duration::from_secs(30))?;
        let version: u32 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version != SPOOL_SCHEMA_VERSION {
            return Err(SpoolError::Schema);
        }
        Ok(Self {
            root: root.to_owned(),
            db,
        })
    }

    pub fn store(&mut self, envelope: &SessionEnvelope) -> Result<(SpoolEntry, bool), SpoolError> {
        let bytes = serde_json::to_vec(envelope)?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let directory = self.root.join("objects");
        let target = directory.join(&digest);
        let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        match temporary.persist_noclobber(&target) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                if read_bounded(&target, bytes.len())? != bytes {
                    return Err(SpoolError::Integrity);
                }
            }
            Err(error) => return Err(SpoolError::Io(error.error)),
        }
        sync_directory(&directory)?;
        let tx = self.db.transaction()?;
        let inserted = tx.execute(
            "INSERT INTO envelope_revisions (archive_sha256,byte_count,agent,native_session_id)
             VALUES (?1,?2,?3,?4) ON CONFLICT(archive_sha256) DO NOTHING",
            params![
                digest,
                bytes.len() as u64,
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
        Ok((entry, inserted))
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
        let path = self.root.join("objects").join(&entry.archive_sha256);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.len() != entry.byte_count {
            return Err(SpoolError::Integrity);
        }
        let bytes = read_bounded(&path, max_bytes)?;
        if bytes.len() as u64 != entry.byte_count
            || format!("{:x}", Sha256::digest(&bytes)) != entry.archive_sha256
        {
            return Err(SpoolError::Integrity);
        }
        Ok(bytes)
    }
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, SpoolError> {
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(SpoolError::Integrity);
    }
    Ok(bytes)
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

pub(crate) fn private_directory(path: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

pub(crate) fn sync_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path; // SQLite FULL and the flushed object file provide the portable barrier.
    Ok(())
}

pub fn capture_local(cfg: &Config, root: &Path) -> Result<SpoolSummary, SpoolError> {
    let mut spool = LocalSpool::open(root)?;
    let discovered = discover_all(cfg)?;
    let mut summary = SpoolSummary {
        schema_version: SPOOL_SCHEMA_VERSION,
        discovered: discovered.len(),
        ..Default::default()
    };
    for source in discovered {
        if serde_json::to_vec(&source.envelope)?.len() > cfg.max_envelope_bytes {
            summary.skipped_oversize += 1;
            continue;
        }
        let (_, inserted) = spool.store(&source.envelope)?;
        if inserted {
            summary.stored += 1;
        } else {
            summary.duplicate += 1;
        }
    }
    summary.watermark = spool.watermark()?;
    Ok(summary)
}
