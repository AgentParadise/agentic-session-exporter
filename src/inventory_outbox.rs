//! Durable, destination-bound inventory operations. Acknowledgement follows remote acceptance.
use crate::inventory::{InventoryClient, InventoryUploadError};
use rusqlite::{params, Connection};
use serde::Serialize;
use session_capture::inventory::InventoryPublication;
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};

pub use session_capture::inventory::InventoryOperation;
trait OperationTransport {
    fn key(&self) -> Result<String, OutboxError>;
    async fn send(&self, client: &InventoryClient) -> Result<bool, InventoryUploadError>;
}
impl OperationTransport for InventoryOperation {
    fn key(&self) -> Result<String, OutboxError> {
        let value = match self {
            Self::Record(r) => {
                r.validate().map_err(|_| OutboxError::Invalid)?;
                serde_json::json!([
                    "record",
                    r.run.source_instance_id(),
                    r.producer_id,
                    r.record_id
                ])
            }
            Self::Stage(r) | Self::Publish(r) => {
                r.validate().map_err(|_| OutboxError::Invalid)?;
                let kind = if matches!(self, Self::Stage(_)) {
                    "stage"
                } else {
                    "publish"
                };
                serde_json::json!([kind, r.run, r.revision_id])
            }
            Self::Manifest(b) => {
                b.validate().map_err(|_| OutboxError::Invalid)?;
                serde_json::json!(["manifest", b.revision.run, b.revision.revision_id, b.start])
            }
        };
        Ok(value.to_string())
    }
    async fn send(&self, client: &InventoryClient) -> Result<bool, InventoryUploadError> {
        match self {
            Self::Record(r) => {
                client.record(r).await?;
            }
            Self::Stage(r) => {
                client.stage(r).await?;
            }
            Self::Manifest(b) => {
                client.manifest(b).await?;
            }
            Self::Publish(r) => {
                return Ok(matches!(
                    client.publish(r).await?,
                    InventoryPublication::Published | InventoryPublication::AlreadyPublished
                ))
            }
        }
        Ok(true)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("inventory outbox storage failed")]
    Sql(#[from] rusqlite::Error),
    #[error("inventory outbox filesystem failed")]
    Io(#[from] std::io::Error),
    #[error("invalid inventory outbox data")]
    Json(#[from] serde_json::Error),
    #[error("invalid inventory operation or batch bound")]
    Invalid,
    #[error("immutable inventory operation conflicts with existing content")]
    Conflict,
    #[error("inventory outbox belongs to another destination")]
    Destination,
    #[error("unsupported inventory outbox version")]
    Version,
}

#[derive(Debug, Default, Serialize)]
pub struct DrainSummary {
    pub acknowledged: usize,
    pub pending: usize,
    pub failed: usize,
    pub remaining: i64,
}

pub struct InventoryOutbox {
    db: Connection,
    destination_hash: String,
}
impl InventoryOutbox {
    pub fn open(root: &Path, client: &InventoryClient) -> Result<Self, OutboxError> {
        crate::spool::private_directory(root)?;
        let db = Connection::open(root.join("inventory-outbox.sqlite3"))?;
        db.busy_timeout(Duration::from_secs(30))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let version: u32 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version > 1 {
            return Err(OutboxError::Version);
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS destination (singleton INTEGER PRIMARY KEY CHECK(singleton=1), hash TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations (sequence INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0,1)));
            CREATE INDEX IF NOT EXISTS pending_operations ON operations(acknowledged, attempts, sequence);
            PRAGMA user_version=1;")?;
        let destination_hash = destination_hash(client);
        db.execute(
            "INSERT OR IGNORE INTO destination VALUES(1, ?1)",
            [&destination_hash],
        )?;
        let actual: String =
            db.query_row("SELECT hash FROM destination WHERE singleton=1", [], |r| {
                r.get(0)
            })?;
        if actual != destination_hash {
            return Err(OutboxError::Destination);
        }
        crate::spool::sync_directory(root)?;
        Ok(Self {
            db,
            destination_hash,
        })
    }

    /// Same identity and bytes is a retry, including after acknowledgement.
    /// Conflicting bytes cannot silently replace an operation that may already be remote.
    pub fn enqueue(&self, operation: &InventoryOperation) -> Result<bool, OutboxError> {
        let identity = operation.key()?;
        let payload = serde_json::to_string(operation)?;
        if payload.len() > 2 * 1024 * 1024 {
            return Err(OutboxError::Invalid);
        }
        let inserted = self.db.execute(
            "INSERT OR IGNORE INTO operations(identity,payload) VALUES(?1,?2)",
            params![identity, payload],
        )?;
        let actual: String = self.db.query_row(
            "SELECT payload FROM operations WHERE identity=?1",
            [&identity],
            |r| r.get(0),
        )?;
        if actual != payload {
            return Err(OutboxError::Conflict);
        }
        Ok(inserted == 1)
    }

    /// One bounded pass. Pending publications do not prevent later evidence from uploading.
    /// Concurrent drains can repeat requests safely; neither can erase unacknowledged work.
    pub async fn drain(
        &self,
        client: &InventoryClient,
        limit: usize,
    ) -> Result<DrainSummary, OutboxError> {
        if !(1..=500).contains(&limit) {
            return Err(OutboxError::Invalid);
        }
        if destination_hash(client) != self.destination_hash {
            return Err(OutboxError::Destination);
        }
        let ids: Vec<i64> = self
            .db
            .prepare(
                "SELECT sequence FROM operations WHERE acknowledged=0 ORDER BY attempts, sequence LIMIT ?1",
            )?
            .query_map([limit as i64], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        let mut summary = DrainSummary::default();
        for id in ids {
            let payload: String = self.db.query_row(
                "SELECT payload FROM operations WHERE sequence=?1",
                [id],
                |r| r.get(0),
            )?;
            let operation: InventoryOperation = serde_json::from_str(&payload)?;
            self.db.execute(
                "UPDATE operations SET attempts=attempts+1 WHERE sequence=?1",
                [id],
            )?;
            match operation.send(client).await {
                Ok(true) => {
                    self.db.execute(
                        "UPDATE operations SET acknowledged=1 WHERE sequence=?1",
                        [id],
                    )?;
                    summary.acknowledged += 1;
                }
                Ok(false) => summary.pending += 1,
                Err(_) => summary.failed += 1,
            }
        }
        summary.remaining = self.db.query_row(
            "SELECT count(*) FROM operations WHERE acknowledged=0",
            [],
            |r| r.get(0),
        )?;
        Ok(summary)
    }
}
fn destination_hash(client: &InventoryClient) -> String {
    format!("{:x}", Sha256::digest(client.destination().as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_capture::inventory::{InventoryCoverage, InventoryRevision, QualifiedRun};
    use std::io::{Read, Write};

    fn revision(id: &str) -> InventoryRevision {
        InventoryRevision {
            run: QualifiedRun::new("source".into(), "run".into()).unwrap(),
            revision_id: id.into(),
            parent_revision_id: None,
            revision_sequence: 1,
            producer_id: "producer".into(),
            sequence_high_watermark: 0,
            resolver_version: "v1".into(),
            coverage: InventoryCoverage::Unknown,
            expected_record_count: 0,
        }
    }

    #[tokio::test]
    async fn restart_preserves_pending_work_and_rotates_past_waiting_publication() {
        let root = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let worker = std::thread::spawn(move || {
            for (route, status, body) in [
                ("publish", 200, "\"pending_records\""),
                ("revisions", 503, "unavailable"),
                ("publish", 200, "\"already_published\""),
                ("revisions", 200, "{\"duplicate\":true}"),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buf = [0; 4096];
                    let count = stream.read(&mut buf).unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&buf[..count]);
                    if let Some(i) = request.windows(4).position(|b| b == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&request[..i]).to_lowercase();
                        let length: usize = header
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if request.len() >= i + 4 + length {
                            break;
                        }
                    }
                }
                assert!(String::from_utf8(request)
                    .unwrap()
                    .starts_with(&format!("POST /v1/inventory/{route} ")));
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let client = InventoryClient::new(&url, "secret".into()).unwrap();
        let publish = InventoryOperation::Publish(revision("r1"));
        let stage = InventoryOperation::Stage(revision("r2"));
        let outbox = InventoryOutbox::open(root.path(), &client).unwrap();
        assert!(outbox.enqueue(&publish).unwrap());
        assert!(!outbox.enqueue(&publish).unwrap());
        assert!(outbox.enqueue(&stage).unwrap());
        assert_eq!(outbox.drain(&client, 1).await.unwrap().pending, 1);
        drop(outbox);
        let rotated = InventoryClient::new(&url, "rotated-secret".into()).unwrap();
        let outbox = InventoryOutbox::open(root.path(), &rotated).unwrap();
        assert_eq!(outbox.drain(&rotated, 1).await.unwrap().failed, 1);
        assert_eq!(outbox.drain(&rotated, 1).await.unwrap().acknowledged, 1);
        assert_eq!(outbox.drain(&rotated, 1).await.unwrap().acknowledged, 1);
        assert_eq!(outbox.drain(&rotated, 1).await.unwrap().acknowledged, 0);
        assert!(!outbox.enqueue(&publish).unwrap());
        let mut changed = revision("r1");
        changed.coverage = InventoryCoverage::Missing;
        assert!(matches!(
            outbox.enqueue(&InventoryOperation::Publish(changed)),
            Err(OutboxError::Conflict)
        ));
        let other = InventoryClient::new("https://example.com", "secret".into()).unwrap();
        assert!(matches!(
            InventoryOutbox::open(root.path(), &other),
            Err(OutboxError::Destination)
        ));
        assert!(matches!(
            outbox.drain(&other, 1).await,
            Err(OutboxError::Destination)
        ));
        assert!(matches!(
            outbox.drain(&rotated, 0).await,
            Err(OutboxError::Invalid)
        ));
        worker.join().unwrap();
    }
}
