//! Optional APSS inventory transport. Durable callers retain work until acknowledged.
use serde::{de::DeserializeOwned, Serialize};
use session_capture::inventory::{
    InventoryManifestBatch, InventoryPublication, InventoryReceipt, InventoryRecord,
    InventoryRevision,
};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum InventoryUploadError {
    #[error("invalid inventory client configuration")]
    Configuration,
    #[error("invalid inventory request")]
    Invalid,
    #[error("inventory request failed")]
    Transport,
    #[error("inventory endpoint returned status {0}")]
    Status(u16),
    #[error("invalid inventory response")]
    Response,
}

/// No Debug implementation: configuration contains a namespace-scoped secret.
pub struct InventoryClient {
    http: reqwest::Client,
    base: reqwest::Url,
    token: String,
}
impl InventoryClient {
    pub(crate) fn destination(&self) -> String {
        self.base.as_str().trim_end_matches('/').to_owned()
    }

    pub fn new(base: &str, token: String) -> Result<Self, InventoryUploadError> {
        let base = reqwest::Url::parse(base).map_err(|_| InventoryUploadError::Configuration)?;
        if !matches!(base.scheme(), "http" | "https")
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || token.trim().is_empty()
            || reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).is_err()
        {
            return Err(InventoryUploadError::Configuration);
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| InventoryUploadError::Configuration)?;
        Ok(Self { http, base, token })
    }

    async fn post<T: Serialize>(
        &self,
        route: &str,
        body: &T,
    ) -> Result<Vec<u8>, InventoryUploadError> {
        let url = format!(
            "{}/v1/inventory/{route}",
            self.base.as_str().trim_end_matches('/')
        );
        let mut response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|_| InventoryUploadError::Transport)?;
        if !response.status().is_success() {
            return Err(InventoryUploadError::Status(response.status().as_u16()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| InventoryUploadError::Transport)?
        {
            if bytes.len() + chunk.len() > 4096 {
                return Err(InventoryUploadError::Response);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    pub async fn record(
        &self,
        record: &InventoryRecord,
    ) -> Result<InventoryReceipt, InventoryUploadError> {
        record
            .validate()
            .map_err(|_| InventoryUploadError::Invalid)?;
        decode(&self.post("records", record).await?)
    }
    pub async fn stage(
        &self,
        revision: &InventoryRevision,
    ) -> Result<InventoryReceipt, InventoryUploadError> {
        revision
            .validate()
            .map_err(|_| InventoryUploadError::Invalid)?;
        decode(&self.post("revisions", revision).await?)
    }
    pub async fn manifest(
        &self,
        batch: &InventoryManifestBatch,
    ) -> Result<(), InventoryUploadError> {
        batch
            .validate()
            .map_err(|_| InventoryUploadError::Invalid)?;
        if !self.post("manifests", batch).await?.is_empty() {
            return Err(InventoryUploadError::Response);
        }
        Ok(())
    }
    pub async fn publish(
        &self,
        revision: &InventoryRevision,
    ) -> Result<InventoryPublication, InventoryUploadError> {
        revision
            .validate()
            .map_err(|_| InventoryUploadError::Invalid)?;
        decode(&self.post("publish", revision).await?)
    }
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, InventoryUploadError> {
    serde_json::from_slice(bytes).map_err(|_| InventoryUploadError::Response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_capture::inventory::{InventoryCoverage, QualifiedRun};
    use std::io::{Read, Write};

    fn revision() -> InventoryRevision {
        InventoryRevision {
            run: QualifiedRun::new("installation".into(), "run".into()).unwrap(),
            revision_id: "r1".into(),
            parent_revision_id: None,
            revision_sequence: 1,
            producer_id: "producer".into(),
            sequence_high_watermark: 0,
            resolver_version: "v1".into(),
            coverage: InventoryCoverage::Unknown,
            expected_record_count: 0,
        }
    }

    #[test]
    fn rejects_credentials_and_header_injection_without_echoing_secrets() {
        for url in [
            "https://secret@example.com",
            "https://example.com?token=secret",
            "file:///secret",
        ] {
            let error = InventoryClient::new(url, "secret".into()).err().unwrap();
            assert!(!error.to_string().contains("secret"));
        }
        assert!(InventoryClient::new("https://example.com", "secret\r\nx: y".into()).is_err());
    }

    #[tokio::test]
    async fn publication_preserves_pending_outcomes_and_does_not_follow_redirects() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let worker = std::thread::spawn(move || {
            for (status, body) in [
                (200, "\"pending_parent\""),
                (200, "\"already_published\""),
                (307, "secret response"),
                (200, "invalid"),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut data = vec![];
                let mut buf = [0; 4096];
                loop {
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&buf[..n]);
                    if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&data[..i]).to_lowercase();
                        let length: usize = header
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if data.len() >= i + 4 + length {
                            break;
                        }
                    }
                }
                let request = String::from_utf8(data).unwrap();
                assert!(request.starts_with("POST /v1/inventory/publish "));
                assert!(request
                    .to_lowercase()
                    .contains("authorization: bearer secret"));
                write!(stream, "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nLocation: https://example.com\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let client = InventoryClient::new(&url, "secret".into()).unwrap();
        assert_eq!(
            client.publish(&revision()).await.unwrap(),
            InventoryPublication::PendingParent
        );
        assert_eq!(
            client.publish(&revision()).await.unwrap(),
            InventoryPublication::AlreadyPublished
        );
        assert!(matches!(
            client.publish(&revision()).await,
            Err(InventoryUploadError::Status(307))
        ));
        assert!(matches!(
            client.publish(&revision()).await,
            Err(InventoryUploadError::Response)
        ));
        worker.join().unwrap();
    }
}
