//! Durable qualified delivery references immutable local spool objects.
//!
//! Upload and deletion of one qualified revision (storage key plus content
//! hash) never overlap, across processes sharing this outbox: each holds a
//! lease row for that revision for the whole remote request, and a drain that
//! finds a revision leased skips it for this pass instead of waiting. The
//! tombstone check before an upload happens only after the lease is held, so a
//! deletion is either sent entirely before an upload (which then sees the
//! tombstone and never goes out) or entirely after it (and removes what the
//! upload stored). The store remains the final authority: a revision it holds
//! a tombstone for is refused with 410, which is terminal withdrawal here.
use crate::{
    qualified_capture::{CaptureUploadError, QualifiedCaptureClient},
    secure_fs::{sqlite_flags, SecureDir},
    spool::LocalSpool,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use session_capture::{
    inventory::{CaptureReceipt, QualifiedTranscript},
    SessionEnvelope,
};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum CaptureOutboxError {
    #[error("capture outbox storage failed")]
    Storage,
    #[error("invalid capture outbox input")]
    Invalid,
    #[error("capture outbox destination mismatch")]
    Destination,
    #[error("capture revision was deleted")]
    Deleted,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct CaptureDrainSummary {
    pub acknowledged: usize,
    pub failed: usize,
    pub remaining: u64,
    /// Deletions the store confirmed this pass.
    pub deleted: usize,
    /// Uploads the store refused with 410 because it holds a deletion for the
    /// revision. Terminal: never retried, and the queued body is dropped.
    pub withdrawn: usize,
    /// Revisions skipped this pass because another process holds their lease
    /// (an upload or deletion in flight). They stay pending.
    pub busy: usize,
    /// Rows set aside this pass because their stored object, identity, or
    /// index entry is missing or corrupt. Retrying cannot repair them.
    pub integrity: usize,
    /// Total rows currently set aside for integrity. They keep their identity,
    /// never transcript content, so an operator can see what did not deliver.
    pub quarantined: u64,
}

const DATABASE: &str = "capture-delivery.sqlite3";
const SCHEMA_VERSION: u32 = 4;
const MAX_BODY: usize = 64 * 1024 * 1024;
/// Far longer than the 30 second request timeout, so a live holder never
/// loses its lease, while a holder that died releases it without an operator.
const LEASE_SECS: i64 = 300;

pub struct CaptureOutbox {
    root: SecureDir,
    db: Connection,
    spool: LocalSpool,
    destination: String,
    owner: String,
}

/// Why a queued row could not be turned into a request.
enum Unusable {
    /// The row's own bytes or identity are gone or corrupt.
    Integrity,
    /// The spool as a whole failed; the row may succeed later.
    Transient,
}

/// Exclusive right to send remote requests for one qualified revision.
struct Lease<'a> {
    db: &'a Connection,
    key: String,
    hash: String,
    owner: &'a str,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let _ = self.db.execute(
            "DELETE FROM revision_leases WHERE storage_key=?1 AND content_hash=?2 AND owner=?3",
            params![self.key, self.hash, self.owner],
        );
    }
}

fn destination(client: &QualifiedCaptureClient) -> String {
    format!("{:x}", Sha256::digest(client.destination().as_bytes()))
}
fn storage<E>(_: E) -> CaptureOutboxError {
    CaptureOutboxError::Storage
}
fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
fn owner_token() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}:{}:{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

impl CaptureOutbox {
    /// Recover a committed remote acknowledgement after a supervisor restart.
    /// Queue acceptance alone never produces a receipt. Exact content identity
    /// prevents an older accepted version from acknowledging a newer capture.
    pub fn receipt(
        &self,
        identity: &QualifiedTranscript,
        content_hash: &str,
    ) -> Result<Option<CaptureReceipt>, CaptureOutboxError> {
        if self.deleted(identity, content_hash)? {
            return Ok(None);
        }
        let encoded: Option<String> = self
            .db
            .query_row(
                "SELECT receipt FROM deliveries WHERE storage_key=?1 AND identity=?2
             AND receipt IS NOT NULL AND json_extract(receipt,'$.content_hash')=?3
             ORDER BY sequence DESC LIMIT 1",
                params![
                    identity.storage_key(),
                    serde_json::to_string(identity).map_err(storage)?,
                    content_hash
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        encoded
            .map(|value| {
                let receipt: CaptureReceipt = serde_json::from_str(&value).map_err(storage)?;
                if !receipt.validates_capture(identity, content_hash) {
                    return Err(CaptureOutboxError::Invalid);
                }
                Ok(receipt)
            })
            .transpose()
    }

    /// Opens beneath a verified private root: no component of the root, the
    /// spool, or the database may be a symlink an unprivileged user planted.
    pub fn open(root: &Path, client: &QualifiedCaptureClient) -> Result<Self, CaptureOutboxError> {
        let dir = SecureDir::open_root(root, true).map_err(storage)?;
        let spool =
            LocalSpool::open_in(dir.child("captures", true).map_err(storage)?).map_err(storage)?;
        let mut db = Connection::open_with_flags(
            dir.sqlite_path(DATABASE).map_err(storage)?,
            sqlite_flags(false),
        )
        .map_err(storage)?;
        dir.verify().map_err(storage)?;
        db.busy_timeout(Duration::from_secs(30)).map_err(storage)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(storage)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let version: u32 = tx
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(storage)?;
        if version > SCHEMA_VERSION {
            return Err(CaptureOutboxError::Invalid);
        }
        tx.execute_batch("CREATE TABLE IF NOT EXISTS destination(singleton INTEGER PRIMARY KEY CHECK(singleton=1),hash TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS deliveries(sequence INTEGER PRIMARY KEY AUTOINCREMENT,storage_key TEXT NOT NULL,
            spool_sequence INTEGER NOT NULL,identity TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,receipt TEXT,
            UNIQUE(storage_key,spool_sequence));
            CREATE INDEX IF NOT EXISTS pending_deliveries ON deliveries(attempts,sequence) WHERE receipt IS NULL;
            CREATE TABLE IF NOT EXISTS revision_leases(storage_key TEXT NOT NULL,content_hash TEXT NOT NULL,
            owner TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(storage_key,content_hash));
            ").map_err(storage)?;
        if version < 2 {
            tx.execute_batch(
                "ALTER TABLE deliveries ADD COLUMN cancelled INTEGER NOT NULL DEFAULT 0;
                PRAGMA user_version=2;",
            )
            .map_err(storage)?;
        }
        tx.execute_batch("CREATE TABLE IF NOT EXISTS capture_deletions(storage_key TEXT NOT NULL,
            content_hash TEXT NOT NULL,identity TEXT NOT NULL,acknowledged INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(storage_key,content_hash));").map_err(storage)?;
        if version < 3 {
            tx.execute_batch(
                "ALTER TABLE deliveries ADD COLUMN content_hash TEXT;
                ALTER TABLE deliveries ADD COLUMN erased INTEGER NOT NULL DEFAULT 0;
                CREATE INDEX deliveries_content ON deliveries(storage_key,content_hash);
                PRAGMA user_version=3;",
            )
            .map_err(storage)?;
        }
        if version < 4 {
            tx.execute_batch(
                "ALTER TABLE deliveries ADD COLUMN quarantined INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE deliveries ADD COLUMN withdrawn INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE capture_deletions ADD COLUMN quarantined INTEGER NOT NULL DEFAULT 0;
                PRAGMA user_version=4;",
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        let destination = destination(client);
        db.execute(
            "INSERT OR IGNORE INTO destination VALUES(1,?1)",
            [&destination],
        )
        .map_err(storage)?;
        let actual: String = db
            .query_row("SELECT hash FROM destination WHERE singleton=1", [], |r| {
                r.get(0)
            })
            .map_err(storage)?;
        if actual != destination {
            return Err(CaptureOutboxError::Destination);
        }
        dir.sync().map_err(storage)?;
        Ok(Self {
            root: dir,
            db,
            spool,
            destination,
            owner: owner_token(),
        })
    }

    /// Object fsync and spool publication precede queue acknowledgement. An
    /// interruption between them leaves a reusable object, never lost work.
    pub fn enqueue(
        &mut self,
        identity: &QualifiedTranscript,
        envelope: &SessionEnvelope,
    ) -> Result<bool, CaptureOutboxError> {
        if identity.native_session_id() != envelope.session_id {
            return Err(CaptureOutboxError::Invalid);
        }
        let mut envelope = envelope.clone();
        envelope.content_hash = None;
        envelope
            .validate()
            .map_err(|_| CaptureOutboxError::Invalid)?;
        let hash = session_capture::content_hash_for(&envelope)
            .map_err(|_| CaptureOutboxError::Invalid)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let deleted: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM capture_deletions WHERE storage_key=?1 AND content_hash=?2)",params![identity.storage_key(),&hash],|r|r.get(0)).map_err(storage)?;
        if deleted {
            return Err(CaptureOutboxError::Deleted);
        }
        let Some((entry, _)) = self
            .spool
            .store_bounded(&envelope, MAX_BODY)
            .map_err(storage)?
        else {
            return Err(CaptureOutboxError::Invalid);
        };
        let encoded = serde_json::to_string(identity).map_err(storage)?;
        let inserted=tx.execute("INSERT OR IGNORE INTO deliveries(storage_key,spool_sequence,identity,content_hash) VALUES(?1,?2,?3,?4)",
            params![identity.storage_key(),entry.sequence,encoded,&hash]).map_err(storage)?;
        let actual: String = tx
            .query_row(
                "SELECT identity FROM deliveries WHERE storage_key=?1 AND spool_sequence=?2",
                params![identity.storage_key(), entry.sequence],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if actual != encoded {
            return Err(CaptureOutboxError::Invalid);
        }
        tx.execute(
            "UPDATE deliveries SET erased=0 WHERE spool_sequence=?1",
            [entry.sequence],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(inserted == 1)
    }

    pub async fn drain(
        &self,
        client: &QualifiedCaptureClient,
        limit: usize,
    ) -> Result<CaptureDrainSummary, CaptureOutboxError> {
        if !(1..=50).contains(&limit) {
            return Err(CaptureOutboxError::Invalid);
        }
        if destination(client) != self.destination {
            return Err(CaptureOutboxError::Destination);
        }
        self.root.verify().map_err(storage)?;
        let mut result = CaptureDrainSummary::default();
        let attempted = self.drain_deletions(client, limit, &mut result).await?;
        let mut statement=self.db.prepare("SELECT sequence,spool_sequence,identity,content_hash FROM deliveries
            WHERE receipt IS NULL AND NOT cancelled AND NOT quarantined ORDER BY attempts,sequence LIMIT ?1").map_err(storage)?;
        let rows = statement
            .query_map([(limit - attempted) as u32], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        for (sequence, spool_sequence, identity, stored_hash) in rows {
            let (identity, envelope, hash) =
                match self.prepare(spool_sequence, &identity, stored_hash.as_deref()) {
                    Ok(prepared) => prepared,
                    Err(Unusable::Integrity) => {
                        self.set_delivery(sequence, "quarantined=1")?;
                        result.integrity += 1;
                        continue;
                    }
                    Err(Unusable::Transient) => {
                        self.set_delivery(sequence, "attempts=attempts+1")?;
                        result.failed += 1;
                        continue;
                    }
                };
            let Some(_lease) = self.lease(&identity.storage_key(), &hash)? else {
                result.busy += 1;
                continue;
            };
            self.set_delivery(sequence, "attempts=attempts+1")?;
            // Checked only while holding the lease: no deletion of this
            // revision can be in flight now, and one queued later waits.
            if self.deleted(&identity, &hash)? {
                self.set_delivery(sequence, "cancelled=1")?;
                continue;
            }
            match client.upload(&identity, &envelope).await {
                Ok(receipt) => {
                    self.db.execute("UPDATE deliveries SET receipt=?2 WHERE sequence=?1 AND receipt IS NULL",
                        params![sequence,serde_json::to_string(&receipt).map_err(storage)?]).map_err(storage)?;
                    result.acknowledged += 1;
                }
                Err(CaptureUploadError::Status(410)) => {
                    self.withdraw(sequence, &identity, &hash)?;
                    result.withdrawn += 1;
                }
                Err(_) => result.failed += 1,
            }
        }
        result.integrity += self.cleanup(limit)?;
        (result.remaining, result.quarantined) = self
            .db
            .query_row(
                "SELECT
                   (SELECT count(*) FROM deliveries WHERE receipt IS NULL AND NOT cancelled AND NOT quarantined)
                 + (SELECT count(*) FROM capture_deletions WHERE NOT acknowledged AND NOT quarantined),
                   (SELECT count(*) FROM deliveries WHERE quarantined)
                 + (SELECT count(*) FROM capture_deletions WHERE quarantined)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(storage)?;
        Ok(result)
    }

    /// Decode one queued row. Anything wrong with the row's own identity or
    /// stored bytes is integrity; only a failure of the spool itself is
    /// worth retrying.
    fn prepare(
        &self,
        spool_sequence: u64,
        identity: &str,
        stored_hash: Option<&str>,
    ) -> Result<(QualifiedTranscript, SessionEnvelope, String), Unusable> {
        let integrity = |_| Unusable::Integrity;
        let identity: QualifiedTranscript = serde_json::from_str(identity).map_err(integrity)?;
        let bytes = self.spool.read(spool_sequence, MAX_BODY).map_err(|error| {
            match error.is_integrity() {
                true => Unusable::Integrity,
                false => Unusable::Transient,
            }
        })?;
        let envelope: SessionEnvelope = serde_json::from_slice(&bytes).map_err(integrity)?;
        let hash = session_capture::content_hash_for(&envelope).map_err(|_| Unusable::Integrity)?;
        if stored_hash.is_some_and(|stored| stored != hash)
            || envelope.session_id != identity.native_session_id()
        {
            return Err(Unusable::Integrity);
        }
        Ok((identity, envelope, hash))
    }

    fn set_delivery(&self, sequence: i64, assignment: &str) -> Result<(), CaptureOutboxError> {
        self.db
            .execute(
                &format!("UPDATE deliveries SET {assignment} WHERE sequence=?1"),
                [sequence],
            )
            .map_err(storage)?;
        Ok(())
    }

    /// Take the revision's lease, or `None` while another holder's is live.
    fn lease(&self, key: &str, hash: &str) -> Result<Option<Lease<'_>>, CaptureOutboxError> {
        let now = now();
        let taken = self
            .db
            .execute(
                "INSERT INTO revision_leases(storage_key,content_hash,owner,expires_at) VALUES(?1,?2,?3,?4)
                 ON CONFLICT(storage_key,content_hash) DO UPDATE SET owner=excluded.owner,
                 expires_at=excluded.expires_at WHERE revision_leases.expires_at<=?5",
                params![key, hash, self.owner, now + LEASE_SECS, now],
            )
            .map_err(storage)?;
        Ok((taken == 1).then(|| Lease {
            db: &self.db,
            key: key.to_owned(),
            hash: hash.to_owned(),
            owner: &self.owner,
        }))
    }

    /// The store holds a tombstone for this revision. Record it as an
    /// acknowledged local deletion, so it is never uploaded or re-enqueued,
    /// and cleanup drops the queued body without sending a DELETE.
    fn withdraw(
        &self,
        sequence: i64,
        identity: &QualifiedTranscript,
        hash: &str,
    ) -> Result<(), CaptureOutboxError> {
        let tx = Transaction::new_unchecked(&self.db, TransactionBehavior::Immediate)
            .map_err(storage)?;
        tx.execute("INSERT INTO capture_deletions(storage_key,content_hash,identity,acknowledged) VALUES(?1,?2,?3,1)
            ON CONFLICT(storage_key,content_hash) DO UPDATE SET acknowledged=1",
            params![identity.storage_key(), hash, serde_json::to_string(identity).map_err(storage)?]).map_err(storage)?;
        tx.execute(
            "UPDATE deliveries SET cancelled=1,withdrawn=1 WHERE sequence=?1",
            [sequence],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)
    }

    /// Returns the number of rows newly set aside for integrity. A corrupt
    /// row never fails the drain; only the outbox database itself can.
    fn cleanup(&self, limit: usize) -> Result<usize, CaptureOutboxError> {
        let mut quarantined = 0;
        let tx = Transaction::new_unchecked(&self.db, TransactionBehavior::Immediate)
            .map_err(storage)?;
        // Upgrade legacy rows incrementally while their original bytes remain.
        let legacy = {
            let mut q = tx.prepare("SELECT sequence,spool_sequence FROM deliveries WHERE content_hash IS NULL AND NOT quarantined LIMIT ?1").map_err(storage)?;
            let rows = q
                .query_map([limit as u32], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, u64>(1)?))
                })
                .map_err(storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?;
            rows
        };
        for (sequence, spool) in legacy {
            let hash = match self.spool.read(spool, MAX_BODY) {
                Ok(bytes) => serde_json::from_slice::<SessionEnvelope>(&bytes)
                    .ok()
                    .and_then(|envelope| session_capture::content_hash_for(&envelope).ok()),
                Err(error) if error.is_integrity() => None,
                Err(_) => continue,
            };
            quarantined += usize::from(hash.is_none());
            tx.execute(
                "UPDATE deliveries SET content_hash=coalesce(?2,content_hash),quarantined=?3 WHERE sequence=?1",
                params![sequence, hash, hash.is_none()],
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        let tx = Transaction::new_unchecked(&self.db, TransactionBehavior::Immediate)
            .map_err(storage)?;
        let candidates = {
            let mut q = tx.prepare("SELECT DISTINCT d.spool_sequence FROM deliveries d
                JOIN capture_deletions t ON t.storage_key=d.storage_key AND t.content_hash=d.content_hash
                WHERE NOT d.erased AND NOT d.quarantined AND NOT EXISTS(SELECT 1 FROM deliveries other
                    WHERE other.spool_sequence=d.spool_sequence AND other.receipt IS NULL AND NOT other.quarantined
                    AND NOT EXISTS(SELECT 1 FROM capture_deletions x WHERE x.storage_key=other.storage_key AND x.content_hash=other.content_hash))
                LIMIT ?1").map_err(storage)?;
            let rows = q
                .query_map([limit as u32], |r| r.get::<_, u64>(0))
                .map_err(storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?;
            rows
        };
        for spool in candidates {
            let update = match self.spool.discard_body(spool) {
                Ok(()) => "UPDATE deliveries SET erased=1,cancelled=CASE WHEN receipt IS NULL THEN 1 ELSE cancelled END WHERE spool_sequence=?1",
                Err(error) if error.is_integrity() => "UPDATE deliveries SET quarantined=1 WHERE spool_sequence=?1",
                Err(_) => continue,
            };
            quarantined += tx.execute(update, [spool]).map_err(storage)?
                * usize::from(update.contains("quarantined"));
        }
        tx.commit().map_err(storage)?;
        Ok(quarantined)
    }

    fn deleted(
        &self,
        identity: &QualifiedTranscript,
        hash: &str,
    ) -> Result<bool, CaptureOutboxError> {
        self.db.query_row("SELECT EXISTS(SELECT 1 FROM capture_deletions WHERE storage_key=?1 AND content_hash=?2)",
            params![identity.storage_key(),hash], |r| r.get(0)).map_err(storage)
    }

    pub fn enqueue_deletion(
        &self,
        identity: &QualifiedTranscript,
        hash: &str,
    ) -> Result<bool, CaptureOutboxError> {
        if !crate::qualified_capture::valid_content_hash(hash) {
            return Err(CaptureOutboxError::Invalid);
        }
        let inserted = self.db.execute("INSERT OR IGNORE INTO capture_deletions(storage_key,content_hash,identity) VALUES(?1,?2,?3)",
            params![identity.storage_key(),hash,serde_json::to_string(identity).map_err(storage)?]).map_err(storage)?;
        Ok(inserted == 1)
    }

    /// Returns how many deletions were sent, which count against `limit`.
    async fn drain_deletions(
        &self,
        client: &QualifiedCaptureClient,
        limit: usize,
        result: &mut CaptureDrainSummary,
    ) -> Result<usize, CaptureOutboxError> {
        let rows = {
            let mut statement = self.db.prepare("SELECT storage_key,content_hash,identity FROM capture_deletions
                WHERE NOT acknowledged AND NOT quarantined ORDER BY storage_key,content_hash LIMIT ?1").map_err(storage)?;
            let rows = statement
                .query_map([limit as u32], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .map_err(storage)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(storage)?
        };
        let mut attempted = 0;
        for (key, hash, encoded) in rows {
            let set = |assignment: &str| {
                self.db
                    .execute(
                        &format!("UPDATE capture_deletions SET {assignment} WHERE storage_key=?1 AND content_hash=?2"),
                        params![key, hash],
                    )
                    .map_err(storage)
            };
            let identity = match serde_json::from_str::<QualifiedTranscript>(&encoded) {
                Ok(identity) if identity.storage_key() == key => identity,
                _ => {
                    set("quarantined=1")?;
                    result.integrity += 1;
                    continue;
                }
            };
            let Some(_lease) = self.lease(&key, &hash)? else {
                result.busy += 1;
                continue;
            };
            attempted += 1;
            if client.delete(&identity, &hash).await.is_ok() {
                set("acknowledged=1")?;
                result.deleted += 1;
            } else {
                result.failed += 1;
            }
        }
        Ok(attempted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn cleanup_preserves_shared_pending_identity_and_retries_after_unlink() {
        let root = tempfile::tempdir().unwrap();
        let client = QualifiedCaptureClient::new("http://127.0.0.1:1", "test".into()).unwrap();
        let envelope:SessionEnvelope=serde_json::from_value(serde_json::json!({
            "scs_version":"1.0","origin":{"host":"test","environment":"local"},
            "agent":"codex","source_format":"codex-rollout-jsonl","session_id":"native",
            "started_at":"2026-09-22T00:00:00Z","last_activity_at":"2026-09-22T00:00:01Z","raw":"body"
        })).unwrap();
        let a = QualifiedTranscript::new("a".into(), "codex".into(), "native".into()).unwrap();
        let b = QualifiedTranscript::new("b".into(), "codex".into(), "native".into()).unwrap();
        let hash = session_capture::content_hash_for(&envelope).unwrap();
        let mut queue = CaptureOutbox::open(root.path(), &client).unwrap();
        queue.enqueue(&a, &envelope).unwrap();
        queue.enqueue(&b, &envelope).unwrap();
        // Version-2 rows need a durable hash backfill before any object removal.
        queue
            .db
            .execute("UPDATE deliveries SET content_hash=NULL", [])
            .unwrap();
        queue.enqueue_deletion(&a, &hash).unwrap();
        queue.cleanup(1).unwrap();
        assert!(queue.spool.read(1, 4096).is_ok());
        queue.cleanup(1).unwrap();
        assert!(queue.spool.read(1, 4096).is_ok());
        queue.enqueue_deletion(&b, &hash).unwrap();
        queue.cleanup(1).unwrap();
        assert!(queue.spool.read(1, 4096).is_err());
        assert_eq!(
            std::fs::read_dir(root.path().join("captures/objects"))
                .unwrap()
                .count(),
            0
        );
        // Recreate the state left by interruption after unlink, before the
        // cleanup transaction acknowledged erasure. Persisted hashes survive.
        queue
            .db
            .execute("UPDATE deliveries SET erased=0", [])
            .unwrap();
        drop(queue);
        let mut restarted = CaptureOutbox::open(root.path(), &client).unwrap();
        restarted.cleanup(1).unwrap();
        assert!(matches!(
            restarted.enqueue(&a, &envelope),
            Err(CaptureOutboxError::Deleted)
        ));
        assert!(restarted.spool.read(1, 4096).is_err());
    }

    #[tokio::test]
    async fn deletion_survives_failure_restart_and_cancels_queued_upload() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let client = QualifiedCaptureClient::new(&url, "private-token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let envelope:SessionEnvelope=serde_json::from_value(serde_json::json!({
            "scs_version":"1.0","origin":{"host":"test","environment":"local"},
            "agent":"codex","source_format":"codex-rollout-jsonl","session_id":"native",
            "started_at":"2026-09-22T00:00:00Z","last_activity_at":"2026-09-22T00:00:01Z","raw":"exact\r\n"
        })).unwrap();
        let identity =
            QualifiedTranscript::new("source".into(), "codex".into(), "native".into()).unwrap();
        let hash = session_capture::content_hash_for(&envelope).unwrap();
        let mut queue = CaptureOutbox::open(root.path(), &client).unwrap();
        queue.enqueue(&identity, &envelope).unwrap();
        assert!(queue.enqueue_deletion(&identity, &hash).unwrap());
        assert!(!queue.enqueue_deletion(&identity, &hash).unwrap());
        assert!(matches!(
            queue.enqueue_deletion(&identity, "invalid"),
            Err(CaptureOutboxError::Invalid)
        ));
        assert!(matches!(
            queue.enqueue(&identity, &envelope),
            Err(CaptureOutboxError::Deleted)
        ));
        let server = std::thread::spawn(move || {
            for status in [500, 204] {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                let mut buffer = [0; 2048];
                while !data.windows(4).any(|v| v == b"\r\n\r\n") {
                    let n = stream.read(&mut buffer).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&buffer[..n]);
                }
                let request = String::from_utf8(data).unwrap();
                assert!(request.starts_with("DELETE /v1/transcripts?"));
                assert!(request.contains("content_hash=sha256%3A"));
                write!(
                    stream,
                    "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            }
        });
        let first = queue.drain(&client, 1).await.unwrap();
        assert_eq!((first.failed, first.remaining), (1, 1));
        drop(queue);
        let queue = CaptureOutbox::open(root.path(), &client).unwrap();
        assert_eq!(queue.drain(&client, 1).await.unwrap().remaining, 0);
        drop(queue);
        let queue = CaptureOutbox::open(root.path(), &client).unwrap();
        let last = queue.drain(&client, 1).await.unwrap();
        assert_eq!((last.acknowledged, last.failed, last.remaining), (0, 0, 0));
        assert!(queue.receipt(&identity, &hash).unwrap().is_none());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn restart_failure_token_rotation_and_acknowledgement_preserve_work() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let client = QualifiedCaptureClient::new(&url, "old-token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let envelope:SessionEnvelope=serde_json::from_value(serde_json::json!({
            "scs_version":"1.0","origin":{"host":"test","environment":"local"},
            "agent":"codex","source_format":"codex-rollout-jsonl","session_id":"native",
            "started_at":"2026-09-22T00:00:00Z","last_activity_at":"2026-09-22T00:00:01Z","raw":"exact\r\n"
        })).unwrap();
        let identity =
            QualifiedTranscript::new("source".into(), "codex".into(), "native".into()).unwrap();
        let mut queue = CaptureOutbox::open(root.path(), &client).unwrap();
        assert!(queue.enqueue(&identity, &envelope).unwrap());
        let content_hash = session_capture::content_hash_for(&envelope).unwrap();
        assert!(queue.receipt(&identity, &content_hash).unwrap().is_none());
        assert!(!queue.enqueue(&identity, &envelope).unwrap());
        drop(queue);
        let receipt=serde_json::json!({"storage_key":identity.storage_key(),"content_hash":session_capture::content_hash_for(&envelope).unwrap(),
            "stored_content_hash":format!("sha256:{}","a".repeat(64)),"duplicate":true}).to_string();
        let worker = std::thread::spawn(move || {
            for (status, body, token) in [
                (401, "denied".to_string(), "old-token"),
                (200, receipt, "new-token"),
            ] {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let count = socket.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                    if let Some(end) = data.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..end]).to_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|v| v.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if data.len() < end + 4 + length {
                            continue;
                        }
                        assert!(headers.contains(&format!("authorization: bearer {token}")));
                        let sent: serde_json::Value =
                            serde_json::from_slice(&data[end + 4..]).unwrap();
                        assert_eq!(sent["raw"], "exact\r\n");
                        break;
                    }
                }
                write!(
                    socket,
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let queue = CaptureOutbox::open(root.path(), &client).unwrap();
        let failed = queue.drain(&client, 1).await.unwrap();
        assert_eq!((failed.failed, failed.remaining), (1, 1));
        assert!(queue.receipt(&identity, &content_hash).unwrap().is_none());
        drop(queue);
        let rotated = QualifiedCaptureClient::new(&url, "new-token".into()).unwrap();
        let queue = CaptureOutbox::open(root.path(), &rotated).unwrap();
        let sent = queue.drain(&rotated, 1).await.unwrap();
        assert_eq!((sent.acknowledged, sent.remaining), (1, 0));
        drop(queue);
        let mut queue = CaptureOutbox::open(root.path(), &rotated).unwrap();
        assert!(queue.receipt(&identity, &content_hash).unwrap().is_some());
        assert!(queue
            .receipt(&identity, &format!("sha256:{}", "b".repeat(64)))
            .unwrap()
            .is_none());
        let foreign =
            QualifiedTranscript::new("other".into(), "codex".into(), "native".into()).unwrap();
        assert!(queue.receipt(&foreign, &content_hash).unwrap().is_none());
        assert!(!queue.enqueue(&identity, &envelope).unwrap());
        assert_eq!(queue.drain(&rotated, 1).await.unwrap().acknowledged, 0);
        let other = QualifiedCaptureClient::new("http://127.0.0.1:1", "new-token".into()).unwrap();
        assert!(matches!(
            queue.drain(&other, 1).await,
            Err(CaptureOutboxError::Destination)
        ));
        assert!(matches!(
            CaptureOutbox::open(root.path(), &other),
            Err(CaptureOutboxError::Destination)
        ));
        assert!(matches!(
            queue.drain(&rotated, 0).await,
            Err(CaptureOutboxError::Invalid)
        ));
        worker.join().unwrap();
    }

    /// A store that honours tombstones the way the real one must: a DELETE
    /// removes the body and records a tombstone, and a later upload of that
    /// revision is refused with 410. It can pause each request after it
    /// arrives and before it takes effect, to hold a drain on the far side of
    /// a remote call.
    mod fake {
        use super::*;
        use std::collections::{HashMap, HashSet};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};

        type Revision = (String, String);

        #[derive(Default)]
        pub struct State {
            pub bodies: HashMap<Revision, serde_json::Value>,
            pub tombstones: HashSet<Revision>,
            pub log: Vec<&'static str>,
        }

        pub struct Store {
            pub url: String,
            pub state: Arc<Mutex<State>>,
            pub pause: Arc<AtomicBool>,
            pub arrived: tokio::sync::mpsc::UnboundedReceiver<String>,
            pub release: std::sync::mpsc::Sender<()>,
        }

        pub fn start() -> Store {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let state = Arc::new(Mutex::new(State::default()));
            let pause = Arc::new(AtomicBool::new(false));
            let (arrived_tx, arrived) = tokio::sync::mpsc::unbounded_channel();
            let (release, release_rx) = std::sync::mpsc::channel::<()>();
            let (shared, paused) = (state.clone(), pause.clone());
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = stream.unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut data = Vec::new();
                    let mut buffer = [0u8; 65536];
                    let (head, body) = loop {
                        let count = stream.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        data.extend_from_slice(&buffer[..count]);
                        if let Some(end) = data.windows(4).position(|v| v == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&data[..end]).to_string();
                            let length: usize = head
                                .to_lowercase()
                                .lines()
                                .find_map(|v| v.strip_prefix("content-length: "))
                                .map_or(0, |v| v.parse().unwrap());
                            if data.len() >= end + 4 + length {
                                break (head, data[end + 4..end + 4 + length].to_vec());
                            }
                        }
                    };
                    let mut words = head.split_whitespace();
                    let method = words.next().unwrap().to_string();
                    let target =
                        reqwest::Url::parse(&format!("http://fake{}", words.next().unwrap()))
                            .unwrap();
                    let pairs: HashMap<_, _> = target.query_pairs().into_owned().collect();
                    let identity = QualifiedTranscript::new(
                        pairs["source_instance_id"].clone(),
                        pairs["harness"].clone(),
                        pairs["native_session_id"].clone(),
                    )
                    .unwrap();
                    arrived_tx.send(method.clone()).unwrap();
                    if paused.load(Ordering::SeqCst) {
                        release_rx.recv().unwrap();
                    }
                    let mut state = shared.lock().unwrap();
                    let (status, reply) = match method.as_str() {
                        "POST" => {
                            let envelope: SessionEnvelope = serde_json::from_slice(&body).unwrap();
                            let hash = session_capture::content_hash_for(&envelope).unwrap();
                            let revision = (identity.storage_key(), hash.clone());
                            if state.tombstones.contains(&revision) {
                                state.log.push("refused");
                                (410, String::new())
                            } else {
                                state.log.push("stored");
                                state.bodies.insert(revision, envelope.raw.clone());
                                let receipt = serde_json::json!({"storage_key":identity.storage_key(),
                                    "content_hash":hash,"stored_content_hash":format!("sha256:{}","a".repeat(64)),
                                    "duplicate":false});
                                (200, receipt.to_string())
                            }
                        }
                        "DELETE" => {
                            let revision = (identity.storage_key(), pairs["content_hash"].clone());
                            state.log.push("deleted");
                            state.bodies.remove(&revision);
                            state.tombstones.insert(revision);
                            (204, String::new())
                        }
                        _ => {
                            let served = state
                                .bodies
                                .keys()
                                .any(|(key, _)| *key == identity.storage_key());
                            (if served { 200 } else { 404 }, String::new())
                        }
                    };
                    drop(state);
                    write!(
                        stream,
                        "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                        reply.len()
                    )
                    .unwrap();
                }
            });
            Store {
                url,
                state,
                pause,
                arrived,
                release,
            }
        }

        /// What any reader of the store would be served for this identity.
        pub async fn served(url: &str, identity: &QualifiedTranscript) -> bool {
            let mut target = reqwest::Url::parse(&format!("{url}/v1/transcripts")).unwrap();
            target
                .query_pairs_mut()
                .append_pair("source_instance_id", identity.source_instance_id())
                .append_pair("harness", identity.harness())
                .append_pair("native_session_id", identity.native_session_id());
            reqwest::get(target).await.unwrap().status() == 200
        }
    }

    fn capture(raw: &str) -> SessionEnvelope {
        serde_json::from_value(serde_json::json!({
            "scs_version":"1.0","origin":{"host":"test","environment":"local"},
            "agent":"codex","source_format":"codex-rollout-jsonl","session_id":"native",
            "started_at":"2026-09-22T00:00:00Z","last_activity_at":"2026-09-22T00:00:01Z","raw":raw
        }))
        .unwrap()
    }

    fn source(name: &str) -> QualifiedTranscript {
        QualifiedTranscript::new(name.into(), "codex".into(), "native".into()).unwrap()
    }

    fn objects(root: &Path) -> usize {
        std::fs::read_dir(root.join("captures/objects"))
            .unwrap()
            .count()
    }

    #[tokio::test]
    async fn an_upload_in_flight_holds_back_the_deletion_until_it_lands() {
        use std::sync::atomic::Ordering;
        let mut store = fake::start();
        let client = QualifiedCaptureClient::new(&store.url, "token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let (identity, envelope) = (source("source"), capture("body\r\n"));
        let hash = session_capture::content_hash_for(&envelope).unwrap();
        // Two outboxes on one root are two exporter processes.
        let mut uploader = CaptureOutbox::open(root.path(), &client).unwrap();
        uploader.enqueue(&identity, &envelope).unwrap();
        let deleter = CaptureOutbox::open(root.path(), &client).unwrap();
        store.pause.store(true, Ordering::SeqCst);
        let upload = uploader.drain(&client, 1);
        let script = async {
            // The upload has reached the store and not yet taken effect.
            assert_eq!(store.arrived.recv().await.unwrap(), "POST");
            assert!(deleter.enqueue_deletion(&identity, &hash).unwrap());
            let blocked = tokio::time::timeout(Duration::from_secs(5), deleter.drain(&client, 2))
                .await
                .expect("no request may start while the revision is leased")
                .unwrap();
            assert_eq!((blocked.busy, blocked.deleted, blocked.failed), (2, 0, 0));
            store.pause.store(false, Ordering::SeqCst);
            store.release.send(()).unwrap();
        };
        let (uploaded, ()) = tokio::join!(upload, script);
        assert_eq!(uploaded.unwrap().acknowledged, 1);
        // Only now, with the upload landed, may the deletion go out.
        let deleted = deleter.drain(&client, 2).await.unwrap();
        assert_eq!(
            (deleted.deleted, deleted.remaining, deleted.busy),
            (1, 0, 0)
        );
        assert_eq!(store.state.lock().unwrap().log, ["stored", "deleted"]);
        assert!(!fake::served(&store.url, &identity).await);
        assert_eq!(objects(root.path()), 0);
        assert!(uploader.receipt(&identity, &hash).unwrap().is_none());
        let idle = uploader.drain(&client, 2).await.unwrap();
        assert_eq!((idle.acknowledged, idle.deleted, idle.remaining), (0, 0, 0));
        assert!(!fake::served(&store.url, &identity).await);
    }

    #[tokio::test]
    async fn a_deletion_in_flight_holds_back_the_upload_which_then_never_goes_out() {
        use std::sync::atomic::Ordering;
        let mut store = fake::start();
        let client = QualifiedCaptureClient::new(&store.url, "token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let (identity, envelope) = (source("source"), capture("body\r\n"));
        let hash = session_capture::content_hash_for(&envelope).unwrap();
        let mut uploader = CaptureOutbox::open(root.path(), &client).unwrap();
        uploader.enqueue(&identity, &envelope).unwrap();
        let deleter = CaptureOutbox::open(root.path(), &client).unwrap();
        deleter.enqueue_deletion(&identity, &hash).unwrap();
        store.pause.store(true, Ordering::SeqCst);
        let deleting = deleter.drain(&client, 1);
        let script = async {
            assert_eq!(store.arrived.recv().await.unwrap(), "DELETE");
            let blocked = tokio::time::timeout(Duration::from_secs(5), uploader.drain(&client, 2))
                .await
                .expect("no upload may start while the deletion is in flight")
                .unwrap();
            assert_eq!((blocked.busy, blocked.acknowledged), (2, 0));
            store.pause.store(false, Ordering::SeqCst);
            store.release.send(()).unwrap();
        };
        let (deleted, ()) = tokio::join!(deleting, script);
        assert_eq!(deleted.unwrap().deleted, 1);
        let after = uploader.drain(&client, 2).await.unwrap();
        assert_eq!(
            (after.acknowledged, after.failed, after.remaining),
            (0, 0, 0)
        );
        assert_eq!(store.state.lock().unwrap().log, ["deleted"]);
        assert!(!fake::served(&store.url, &identity).await);
        assert_eq!(objects(root.path()), 0);
    }

    #[tokio::test]
    async fn gone_is_terminal_withdrawal_that_drops_the_body_without_a_delete() {
        let store = fake::start();
        let client = QualifiedCaptureClient::new(&store.url, "token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let (identity, envelope) = (source("source"), capture("withdrawn"));
        let hash = session_capture::content_hash_for(&envelope).unwrap();
        // The store deleted this revision through another exporter.
        store
            .state
            .lock()
            .unwrap()
            .tombstones
            .insert((identity.storage_key(), hash.clone()));
        let mut queue = CaptureOutbox::open(root.path(), &client).unwrap();
        queue.enqueue(&identity, &envelope).unwrap();
        // A lease abandoned by a dead process expires rather than wedging.
        queue
            .db
            .execute(
                "INSERT INTO revision_leases VALUES(?1,?2,'dead',0)",
                params![identity.storage_key(), hash],
            )
            .unwrap();
        let first = queue.drain(&client, 5).await.unwrap();
        assert_eq!(
            (
                first.withdrawn,
                first.failed,
                first.remaining,
                first.deleted
            ),
            (1, 0, 0, 0)
        );
        assert_eq!(objects(root.path()), 0);
        let second = queue.drain(&client, 5).await.unwrap();
        assert_eq!(
            (second.withdrawn, second.failed, second.remaining),
            (0, 0, 0)
        );
        assert_eq!(store.state.lock().unwrap().log, ["refused"]);
        assert!(matches!(
            queue.enqueue(&identity, &envelope),
            Err(CaptureOutboxError::Deleted)
        ));
        assert!(queue.receipt(&identity, &hash).unwrap().is_none());
    }

    #[tokio::test]
    async fn corrupt_rows_are_quarantined_by_identity_and_later_rows_still_deliver() {
        let store = fake::start();
        let client = QualifiedCaptureClient::new(&store.url, "token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut queue = CaptureOutbox::open(root.path(), &client).unwrap();
        let names = [
            "altered",
            "missing",
            "identity",
            "valid",
            "index",
            "tombstone",
            "rehashed",
        ];
        let mut digests = Vec::new();
        for name in names {
            let envelope = capture(&format!("{name} body"));
            queue.enqueue(&source(name), &envelope).unwrap();
            digests.push(serde_json::to_vec(&envelope).unwrap());
        }
        let object = |index: usize| {
            use sha2::{Digest, Sha256};
            root.path()
                .join("captures/objects")
                .join(format!("{:x}", Sha256::digest(&digests[index])))
        };
        // Current rows: bytes altered in place, object gone, identity garbled.
        let length = std::fs::metadata(object(0)).unwrap().len() as usize;
        std::fs::write(object(0), vec![b'x'; length]).unwrap();
        std::fs::remove_file(object(1)).unwrap();
        queue
            .db
            .execute("UPDATE deliveries SET identity='{' WHERE sequence=3", [])
            .unwrap();
        // A delivered row whose spool index entry is corrupt, reached only by
        // cleanup once its deletion arrives.
        queue
            .db
            .execute("UPDATE deliveries SET receipt='{}' WHERE sequence=5", [])
            .unwrap();
        let hash = session_capture::content_hash_for(&capture("index body")).unwrap();
        queue.enqueue_deletion(&source("index"), &hash).unwrap();
        rusqlite::Connection::open(root.path().join("captures/inventory.sqlite3"))
            .unwrap()
            .execute(
                "UPDATE envelope_revisions SET archive_sha256='corrupt' WHERE sequence=5",
                [],
            )
            .unwrap();
        // A row whose recorded content hash no longer matches its bytes.
        queue
            .db
            .execute(
                "UPDATE deliveries SET content_hash=?1 WHERE sequence=7",
                [format!("sha256:{}", "b".repeat(64))],
            )
            .unwrap();
        // A deletion whose stored identity no longer decodes.
        let hash = session_capture::content_hash_for(&capture("tombstone body")).unwrap();
        queue.enqueue_deletion(&source("tombstone"), &hash).unwrap();
        queue
            .db
            .execute(
                "UPDATE capture_deletions SET identity='[' WHERE storage_key=?1",
                [source("tombstone").storage_key()],
            )
            .unwrap();

        let summary = queue.drain(&client, 20).await.unwrap();
        assert_eq!(summary.acknowledged, 1);
        assert_eq!(summary.deleted, 1);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.integrity, 6);
        assert_eq!(summary.quarantined, 6);
        assert_eq!(summary.remaining, 0);
        assert!(!serde_json::to_string(&summary).unwrap().contains("body"));
        assert_eq!(store.state.lock().unwrap().log, ["deleted", "stored"]);
        assert!(fake::served(&store.url, &source("valid")).await);

        // Quarantined rows keep identity only and are never retried.
        drop(queue);
        let queue = CaptureOutbox::open(root.path(), &client).unwrap();
        let again = queue.drain(&client, 20).await.unwrap();
        assert_eq!(
            (again.integrity, again.quarantined, again.remaining),
            (0, 6, 0)
        );
        drop(queue);
        // A database from a newer exporter is refused, never downgraded.
        rusqlite::Connection::open(root.path().join(DATABASE))
            .unwrap()
            .pragma_update(None, "user_version", 99)
            .unwrap();
        assert!(matches!(
            CaptureOutbox::open(root.path(), &client),
            Err(CaptureOutboxError::Invalid)
        ));
        assert_eq!(store.state.lock().unwrap().log.len(), 2);
    }

    #[tokio::test]
    async fn corrupt_legacy_rows_do_not_fail_cleanup_and_valid_rows_deliver() {
        let store = fake::start();
        let client = QualifiedCaptureClient::new(&store.url, "token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut queue = CaptureOutbox::open(root.path(), &client).unwrap();
        for name in ["legacy", "undecodable", "valid"] {
            queue
                .enqueue(&source(name), &capture(&format!("{name} body")))
                .unwrap();
        }
        // Delivered rows from before content hashes were recorded, one whose
        // object has since vanished and one whose object no longer decodes.
        queue
            .db
            .execute(
                "UPDATE deliveries SET receipt='{}',content_hash=NULL WHERE sequence IN (1,2)",
                [],
            )
            .unwrap();
        let objects_dir = root.path().join("captures/objects");
        let mut paths: Vec<_> = std::fs::read_dir(&objects_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        paths.sort();
        let find = |raw: &str| {
            use sha2::{Digest, Sha256};
            objects_dir.join(format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&capture(raw)).unwrap())
            ))
        };
        std::fs::remove_file(find("legacy body")).unwrap();
        let undecodable = find("undecodable body");
        let length = std::fs::metadata(&undecodable).unwrap().len() as usize;
        let garbage = vec![b'!'; length];
        use sha2::{Digest, Sha256};
        std::fs::write(&undecodable, &garbage).unwrap();
        rusqlite::Connection::open(root.path().join("captures/inventory.sqlite3"))
            .unwrap()
            .execute(
                "UPDATE envelope_revisions SET archive_sha256=?1 WHERE sequence=2",
                [format!("{:x}", Sha256::digest(&garbage))],
            )
            .unwrap();
        std::fs::rename(
            &undecodable,
            objects_dir.join(format!("{:x}", Sha256::digest(&garbage))),
        )
        .unwrap();

        let summary = queue.drain(&client, 10).await.unwrap();
        assert_eq!(
            (
                summary.acknowledged,
                summary.integrity,
                summary.quarantined,
                summary.remaining
            ),
            (1, 2, 2, 0)
        );
        assert_eq!(store.state.lock().unwrap().log, ["stored"]);
    }

    #[tokio::test]
    async fn a_spool_that_fails_as_a_whole_is_retried_not_quarantined() {
        let store = fake::start();
        let client = QualifiedCaptureClient::new(&store.url, "token".into()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut queue = CaptureOutbox::open(root.path(), &client).unwrap();
        let (kept, deleted) = (capture("kept body"), capture("deleted body"));
        queue.enqueue(&source("kept"), &kept).unwrap();
        queue.enqueue(&source("deleted"), &deleted).unwrap();
        let hash = session_capture::content_hash_for(&deleted).unwrap();
        queue.enqueue_deletion(&source("deleted"), &hash).unwrap();
        queue
            .db
            .execute(
                "UPDATE deliveries SET content_hash=NULL WHERE sequence=1",
                [],
            )
            .unwrap();
        // The object directory is swapped after open: every spool access now
        // fails as untrusted, which says nothing about any single row.
        let objects_dir = root.path().join("captures/objects");
        let moved = root.path().join("captures/moved");
        std::fs::rename(&objects_dir, &moved).unwrap();
        std::fs::create_dir(&objects_dir).unwrap();
        let summary = queue.drain(&client, 10).await.unwrap();
        assert_eq!(
            (
                summary.deleted,
                summary.failed,
                summary.integrity,
                summary.quarantined
            ),
            (1, 2, 0, 0)
        );
        assert_eq!(summary.remaining, 2);
        std::fs::remove_dir(&objects_dir).unwrap();
        std::fs::rename(&moved, &objects_dir).unwrap();
        let recovered = queue.drain(&client, 10).await.unwrap();
        assert_eq!(
            (
                recovered.acknowledged,
                recovered.failed,
                recovered.remaining
            ),
            (1, 0, 0)
        );
        assert_eq!(store.state.lock().unwrap().log, ["deleted", "stored"]);
        assert_eq!(objects(root.path()), 1);
    }
}
