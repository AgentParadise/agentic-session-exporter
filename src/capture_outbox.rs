//! Durable qualified delivery references immutable local spool objects.
use crate::{qualified_capture::QualifiedCaptureClient, spool::LocalSpool};
use rusqlite::{params, Connection, OptionalExtension};
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
}

#[derive(Debug, Default, serde::Serialize)]
pub struct CaptureDrainSummary {
    pub acknowledged: usize,
    pub failed: usize,
    pub remaining: u64,
}

pub struct CaptureOutbox {
    db: Connection,
    spool: LocalSpool,
    destination: String,
}

fn destination(client: &QualifiedCaptureClient) -> String {
    format!("{:x}", Sha256::digest(client.destination().as_bytes()))
}
fn storage<E>(_: E) -> CaptureOutboxError {
    CaptureOutboxError::Storage
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

    pub fn open(root: &Path, client: &QualifiedCaptureClient) -> Result<Self, CaptureOutboxError> {
        crate::spool::private_directory(root).map_err(storage)?;
        let spool = LocalSpool::open(&root.join("captures")).map_err(storage)?;
        let db = Connection::open(root.join("capture-delivery.sqlite3")).map_err(storage)?;
        db.busy_timeout(Duration::from_secs(30)).map_err(storage)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(storage)?;
        let version: u32 = db
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(storage)?;
        if version > 1 {
            return Err(CaptureOutboxError::Invalid);
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS destination(singleton INTEGER PRIMARY KEY CHECK(singleton=1),hash TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS deliveries(sequence INTEGER PRIMARY KEY AUTOINCREMENT,storage_key TEXT NOT NULL,
            spool_sequence INTEGER NOT NULL,identity TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,receipt TEXT,
            UNIQUE(storage_key,spool_sequence));
            CREATE INDEX IF NOT EXISTS pending_deliveries ON deliveries(attempts,sequence) WHERE receipt IS NULL;
            PRAGMA user_version=1;").map_err(storage)?;
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
        crate::spool::sync_directory(root).map_err(storage)?;
        Ok(Self {
            db,
            spool,
            destination,
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
        session_capture::content_hash_for(&envelope).map_err(|_| CaptureOutboxError::Invalid)?;
        if serde_json::to_vec(&envelope).map_err(storage)?.len() > 64 * 1024 * 1024 {
            return Err(CaptureOutboxError::Invalid);
        }
        let (entry, _) = self.spool.store(&envelope).map_err(storage)?;
        let encoded = serde_json::to_string(identity).map_err(storage)?;
        let inserted=self.db.execute("INSERT OR IGNORE INTO deliveries(storage_key,spool_sequence,identity) VALUES(?1,?2,?3)",
            params![identity.storage_key(),entry.sequence,encoded]).map_err(storage)?;
        let actual: String = self
            .db
            .query_row(
                "SELECT identity FROM deliveries WHERE storage_key=?1 AND spool_sequence=?2",
                params![identity.storage_key(), entry.sequence],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if actual != encoded {
            return Err(CaptureOutboxError::Invalid);
        }
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
        let mut statement=self.db.prepare("SELECT sequence,spool_sequence,identity FROM deliveries WHERE receipt IS NULL ORDER BY attempts,sequence LIMIT ?1").map_err(storage)?;
        let rows = statement
            .query_map([limit as u32], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        let mut result = CaptureDrainSummary::default();
        for (sequence, spool_sequence, identity) in rows {
            self.db
                .execute(
                    "UPDATE deliveries SET attempts=attempts+1 WHERE sequence=?1",
                    [sequence],
                )
                .map_err(storage)?;
            let prepared = (|| {
                let identity: QualifiedTranscript =
                    serde_json::from_str(&identity).map_err(storage)?;
                let bytes = self
                    .spool
                    .read(spool_sequence, 64 * 1024 * 1024)
                    .map_err(storage)?;
                let envelope: SessionEnvelope = serde_json::from_slice(&bytes).map_err(storage)?;
                Ok::<_, CaptureOutboxError>((identity, envelope))
            })();
            let Ok((identity, envelope)) = prepared else {
                result.failed += 1;
                continue;
            };
            match client.upload(&identity, &envelope).await {
                Ok(receipt) => {
                    self.db.execute("UPDATE deliveries SET receipt=?2 WHERE sequence=?1 AND receipt IS NULL",
                        params![sequence,serde_json::to_string(&receipt).map_err(storage)?]).map_err(storage)?;
                    result.acknowledged += 1;
                }
                Err(_) => result.failed += 1,
            }
        }
        result.remaining = self
            .db
            .query_row(
                "SELECT count(*) FROM deliveries WHERE receipt IS NULL",
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

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
}
