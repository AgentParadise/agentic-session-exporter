//! Optional qualified capture transport. Durable callers retain the envelope
//! until a receipt is validated against both identity and original content.
use session_capture::{
    content_hash_for,
    inventory::{CaptureReceipt, QualifiedTranscript},
    SessionEnvelope,
};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum CaptureUploadError {
    #[error("invalid capture client configuration")]
    Configuration,
    #[error("invalid qualified capture")]
    Invalid,
    #[error("capture request failed")]
    Transport,
    #[error("capture endpoint returned status {0}")]
    Status(u16),
    #[error("capture acknowledgement does not match the submitted transcript")]
    Receipt,
}

pub(crate) fn valid_content_hash(hash: &str) -> bool {
    hash.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// Intentionally no Debug: this owns the scoped capture credential.
pub struct QualifiedCaptureClient {
    http: reqwest::Client,
    endpoint: reqwest::Url,
    token: String,
}

impl QualifiedCaptureClient {
    pub(crate) fn destination(&self) -> &str {
        self.endpoint.as_str()
    }
    pub fn new(base: &str, token: String) -> Result<Self, CaptureUploadError> {
        let mut endpoint =
            reqwest::Url::parse(base).map_err(|_| CaptureUploadError::Configuration)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || token.is_empty()
            || token.len() > 4096
            || !token.bytes().all(|b| (33..=126).contains(&b))
        {
            return Err(CaptureUploadError::Configuration);
        }
        endpoint.set_path(&format!(
            "{}/v1/transcripts",
            endpoint.path().trim_end_matches('/')
        ));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| CaptureUploadError::Configuration)?;
        Ok(Self {
            http,
            endpoint,
            token,
        })
    }

    pub async fn delete(
        &self,
        identity: &QualifiedTranscript,
        content_hash: &str,
    ) -> Result<(), CaptureUploadError> {
        if !valid_content_hash(content_hash) {
            return Err(CaptureUploadError::Invalid);
        }
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("source_instance_id", identity.source_instance_id())
            .append_pair("harness", identity.harness())
            .append_pair("native_session_id", identity.native_session_id())
            .append_pair("content_hash", content_hash);
        let response = self
            .http
            .delete(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|_| CaptureUploadError::Transport)?;
        if response.status().as_u16() != 204 {
            return Err(CaptureUploadError::Status(response.status().as_u16()));
        }
        Ok(())
    }

    pub async fn upload(
        &self,
        identity: &QualifiedTranscript,
        envelope: &SessionEnvelope,
    ) -> Result<CaptureReceipt, CaptureUploadError> {
        if identity.native_session_id() != envelope.session_id {
            return Err(CaptureUploadError::Invalid);
        }
        let mut envelope = envelope.clone();
        envelope.content_hash = None;
        envelope
            .validate()
            .map_err(|_| CaptureUploadError::Invalid)?;
        let hash = content_hash_for(&envelope).map_err(|_| CaptureUploadError::Invalid)?;
        let bytes = serde_json::to_vec(&envelope).map_err(|_| CaptureUploadError::Invalid)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(CaptureUploadError::Invalid);
        }
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("source_instance_id", identity.source_instance_id())
            .append_pair("harness", identity.harness())
            .append_pair("native_session_id", identity.native_session_id());
        let mut response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes)
            .send()
            .await
            .map_err(|_| CaptureUploadError::Transport)?;
        if !response.status().is_success() {
            return Err(CaptureUploadError::Status(response.status().as_u16()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CaptureUploadError::Transport)?
        {
            if bytes.len() + chunk.len() > 4096 {
                return Err(CaptureUploadError::Receipt);
            }
            bytes.extend_from_slice(&chunk);
        }
        let receipt: CaptureReceipt =
            serde_json::from_slice(&bytes).map_err(|_| CaptureUploadError::Receipt)?;
        if !receipt.validates_capture(identity, &hash) {
            return Err(CaptureUploadError::Receipt);
        }
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn envelope() -> SessionEnvelope {
        SessionEnvelope {
            scs_version: "1.0".into(),
            origin: session_capture::Origin::new("test", "local"),
            agent: "codex".into(),
            source_format: "codex-rollout-jsonl".into(),
            session_id: "Native/雪 %2F".into(),
            parent_session_id: None,
            started_at: "2026-09-22T00:00:00Z".into(),
            last_activity_at: "2026-09-22T00:00:01Z".into(),
            content_hash: None,
            metadata: None,
            raw: serde_json::json!("verbatim\r\n"),
        }
    }

    #[test]
    fn configuration_errors_do_not_expose_credentials() {
        for url in [
            "https://secret@example.com",
            "file:///secret",
            "https://example.com?token=secret",
            "https://example.com#secret",
        ] {
            assert!(!QualifiedCaptureClient::new(url, "secret".into())
                .err()
                .unwrap()
                .to_string()
                .contains("secret"));
        }
        assert!(QualifiedCaptureClient::new("https://example.com", "secret\r\n".into()).is_err());
    }

    #[tokio::test]
    async fn only_matching_bounded_receipts_acknowledge_exact_capture() {
        let env = envelope();
        let identity =
            QualifiedTranscript::new("source".into(), "codex".into(), env.session_id.clone())
                .unwrap();
        let good = serde_json::json!({"storage_key":identity.storage_key(),"content_hash":content_hash_for(&env).unwrap(),
            "stored_content_hash":format!("sha256:{}","a".repeat(64)),"duplicate":false});
        let mut duplicate = good.clone();
        duplicate["duplicate"] = true.into();
        let mut wrong_key = good.clone();
        wrong_key["storage_key"] = "wrong".into();
        let mut wrong_hash = good.clone();
        wrong_hash["content_hash"] = "wrong".into();
        let mut invalid_hash = good.clone();
        invalid_hash["stored_content_hash"] = "wrong".into();
        let responses = vec![
            (200, good.to_string()),
            (200, duplicate.to_string()),
            (200, wrong_key.to_string()),
            (200, wrong_hash.to_string()),
            (200, invalid_hash.to_string()),
            (200, "x".repeat(4097)),
            (200, "invalid secret response".into()),
            (401, "secret".into()),
            (307, "secret".into()),
        ];
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let worker = std::thread::spawn(move || {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let count = stream.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                    if let Some(end) = data.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..end]);
                        let length: usize = headers
                            .to_lowercase()
                            .lines()
                            .find_map(|v| v.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if data.len() < end + 4 + length {
                            continue;
                        }
                        assert!(headers.starts_with("POST /v1/transcripts?source_instance_id=source&harness=codex&native_session_id="));
                        assert!(headers.contains("Native%2F%E9%9B%AA+%252F"));
                        assert!(headers
                            .to_lowercase()
                            .contains("authorization: bearer secret"));
                        let payload: serde_json::Value =
                            serde_json::from_slice(&data[end + 4..]).unwrap();
                        assert_eq!(payload["raw"], "verbatim\r\n");
                        assert_eq!(payload["session_id"], "Native/雪 %2F");
                        break;
                    }
                }
                write!(stream,"HTTP/1.1 {status} X\r\nContent-Length: {}\r\nLocation: https://example.com\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
            }
        });
        let client = QualifiedCaptureClient::new(&url, "secret".into()).unwrap();
        assert!(!client.upload(&identity, &env).await.unwrap().duplicate);
        assert!(client.upload(&identity, &env).await.unwrap().duplicate);
        for _ in 0..5 {
            assert!(matches!(
                client.upload(&identity, &env).await,
                Err(CaptureUploadError::Receipt)
            ));
        }
        for status in [401, 307] {
            assert!(
                matches!(client.upload(&identity,&env).await,Err(CaptureUploadError::Status(s)) if s==status)
            );
        }
        worker.join().unwrap();
        assert!(matches!(
            client
                .upload(
                    &QualifiedTranscript::new("source".into(), "codex".into(), "wrong".into())
                        .unwrap(),
                    &env
                )
                .await,
            Err(CaptureUploadError::Invalid)
        ));
    }
}

/// Read credentials are independent of capture-write authority. Both requests
/// are bounded and raw retrieval is pinned to the envelope's content version.
pub struct QualifiedCaptureReader {
    envelope: QualifiedCaptureClient,
    raw: QualifiedCaptureClient,
}

pub struct RetrievedCapture {
    pub envelope: SessionEnvelope,
    pub raw: Vec<u8>,
}

impl QualifiedCaptureReader {
    pub fn new(
        base: &str,
        read_token: String,
        raw_token: String,
    ) -> Result<Self, CaptureUploadError> {
        Ok(Self {
            envelope: QualifiedCaptureClient::new(base, read_token)?,
            raw: QualifiedCaptureClient::new(base, raw_token)?,
        })
    }

    pub async fn fetch(
        &self,
        identity: &QualifiedTranscript,
    ) -> Result<RetrievedCapture, CaptureUploadError> {
        use sha2::{Digest, Sha256};
        let mut url = self.envelope.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("source_instance_id", identity.source_instance_id())
            .append_pair("harness", identity.harness())
            .append_pair("native_session_id", identity.native_session_id());
        let response = self
            .envelope
            .http
            .get(url.clone())
            .bearer_auth(&self.envelope.token)
            .send()
            .await
            .map_err(|_| CaptureUploadError::Transport)?;
        let bytes = bounded_capture_body(response).await?;
        #[derive(serde::Deserialize)]
        struct Stored {
            #[serde(flatten)]
            envelope: SessionEnvelope,
            stored_content_hash: String,
        }
        let stored: Stored =
            serde_json::from_slice(&bytes).map_err(|_| CaptureUploadError::Invalid)?;
        if stored.envelope.session_id != identity.native_session_id() {
            return Err(CaptureUploadError::Invalid);
        }
        stored
            .envelope
            .validate()
            .map_err(|_| CaptureUploadError::Invalid)?;
        let hash = stored
            .envelope
            .content_hash
            .as_deref()
            .ok_or(CaptureUploadError::Invalid)?;
        url.set_path(&format!("{}/raw", url.path()));
        url.query_pairs_mut().append_pair("content_hash", hash);
        let response = self
            .raw
            .http
            .get(url)
            .bearer_auth(&self.raw.token)
            .send()
            .await
            .map_err(|_| CaptureUploadError::Transport)?;
        if !response.status().is_success() {
            return Err(CaptureUploadError::Status(response.status().as_u16()));
        }
        if response
            .headers()
            .get("X-Source-Format")
            .and_then(|v| v.to_str().ok())
            != Some(stored.envelope.source_format.as_str())
            || response
                .headers()
                .get("X-Stored-Content-Hash")
                .and_then(|v| v.to_str().ok())
                != Some(stored.stored_content_hash.as_str())
        {
            return Err(CaptureUploadError::Invalid);
        }
        let raw = bounded_capture_body(response).await?;
        if format!("sha256:{:x}", Sha256::digest(&raw)) != stored.stored_content_hash {
            return Err(CaptureUploadError::Invalid);
        }
        Ok(RetrievedCapture {
            envelope: stored.envelope,
            raw,
        })
    }
}

async fn bounded_capture_body(
    mut response: reqwest::Response,
) -> Result<Vec<u8>, CaptureUploadError> {
    if !response.status().is_success() {
        return Err(CaptureUploadError::Status(response.status().as_u16()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| CaptureUploadError::Transport)?
    {
        if bytes.len() + chunk.len() > 64 * 1024 * 1024 {
            return Err(CaptureUploadError::Invalid);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod reader_tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};

    #[tokio::test]
    async fn qualified_reads_pin_versions_separate_credentials_and_check_integrity() {
        for (raw_status, corrupt) in [(200, false), (200, true), (401, false), (307, false)] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let base = format!("http://{}/store", listener.local_addr().unwrap());
            let raw = "native\r\n雪\r\n";
            let stored_hash = format!("sha256:{:x}", Sha256::digest(raw.as_bytes()));
            let hash = format!("sha256:{}", "a".repeat(64));
            let envelope = serde_json::json!({
                "scs_version":"1.0","origin":{"host":"test","environment":"local"},
                "agent":"Codex","source_format":"codex-rollout-jsonl","session_id":"Native/雪 %2F",
                "started_at":"2026-09-22T00:00:00Z","last_activity_at":"2026-09-22T00:00:01Z",
                "raw":raw,"content_hash":hash,"stored_content_hash":stored_hash
            })
            .to_string();
            let server = std::thread::spawn(move || {
                for index in 0..2 {
                    let (mut socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0; 2048];
                    while !request.windows(4).any(|v| v == b"\r\n\r\n") {
                        let count = socket.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        request.extend_from_slice(&buffer[..count]);
                    }
                    let request = String::from_utf8(request).unwrap();
                    let target = request
                        .lines()
                        .next()
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap();
                    let url = reqwest::Url::parse(&format!("http://fixture{target}")).unwrap();
                    let pairs: std::collections::HashMap<_, _> = url.query_pairs().collect();
                    assert_eq!(pairs.get("source_instance_id").unwrap(), "source");
                    assert_eq!(pairs.get("harness").unwrap(), "codex");
                    assert_eq!(pairs.get("native_session_id").unwrap(), "Native/雪 %2F");
                    let (body, headers) = if index == 0 {
                        assert_eq!(url.path(), "/store/v1/transcripts");
                        assert!(!pairs.contains_key("content_hash"));
                        assert!(request.contains("Bearer envelope-token"));
                        (envelope.as_str(), String::new())
                    } else {
                        assert_eq!(url.path(), "/store/v1/transcripts/raw");
                        assert_eq!(pairs.get("content_hash").unwrap(), &hash);
                        assert!(request.contains("Bearer raw-token"));
                        (if corrupt { "tampered" } else { raw }, format!("X-Source-Format: codex-rollout-jsonl\r\nX-Stored-Content-Hash: {stored_hash}\r\n"))
                    };
                    let status = if index == 0 { 200 } else { raw_status };
                    write!(socket, "HTTP/1.1 {status} Response\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                }
            });
            let reader =
                QualifiedCaptureReader::new(&base, "envelope-token".into(), "raw-token".into())
                    .unwrap();
            let identity =
                QualifiedTranscript::new("source".into(), "codex".into(), "Native/雪 %2F".into())
                    .unwrap();
            let result = reader.fetch(&identity).await;
            if raw_status != 200 {
                assert!(
                    matches!(result, Err(CaptureUploadError::Status(status)) if status == raw_status)
                );
            } else if corrupt {
                assert!(matches!(result, Err(CaptureUploadError::Invalid)));
            } else {
                assert_eq!(result.unwrap().raw, raw.as_bytes());
            }
            server.join().unwrap();
        }
    }
}
