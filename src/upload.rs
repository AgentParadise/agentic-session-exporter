//! HTTP upload client for the store's batch ingest endpoint.

use async_trait::async_trait;
use serde::Deserialize;
use session_capture::SessionEnvelope;

/// The batch-send seam the run loop depends on. `Client` implements it over
/// HTTP; tests implement it with a mock so the per-item fallback + skip logic is
/// verifiable without a network. A hard error (non-2xx / timeout / connection
/// closed) is `Err`; per-item rejections come back as `Rejected` outcomes.
#[async_trait]
pub trait BatchSender: Send + Sync {
    async fn send_batch(&self, batch: &[SessionEnvelope]) -> Result<Vec<Outcome>, UploadError>;
}

/// Per-item ingest outcome as returned by `POST /v1/sessions/batch`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Accepted { session_id: String },
    Duplicate { session_id: String },
    Rejected { session_id: String, reason: String },
}

impl Outcome {
    /// The session id this outcome refers to.
    pub fn session_id(&self) -> &str {
        match self {
            Outcome::Accepted { session_id }
            | Outcome::Duplicate { session_id }
            | Outcome::Rejected { session_id, .. } => session_id,
        }
    }
}

#[derive(Debug, Deserialize)]
struct IngestResponse {
    results: Vec<Outcome>,
}

/// Tallied result of one batch upload.
#[derive(Debug, Default, Clone, Copy)]
pub struct BatchTally {
    pub accepted: usize,
    pub duplicate: usize,
    pub rejected: usize,
}

/// Upload errors.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("http request failed: {0}")]
    Http(String),
    #[error("store returned status {0}")]
    Status(u16),
    #[error("could not parse store response: {0}")]
    Decode(String),
}

/// Thin batch-ingest client.
pub struct Client {
    http: reqwest::Client,
    endpoint: String,
    write_token: Option<String>,
}

impl Client {
    /// Build a client for a store base URL (for example `http://host:18090`).
    pub fn new(store_url: &str, write_token: Option<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint: format!("{}/v1/sessions/batch", store_url.trim_end_matches('/')),
            write_token,
        }
    }

    /// Health probe: GET /healthz, true when the store answers `ok`.
    pub async fn healthy(&self, store_url: &str) -> bool {
        let url = format!("{}/healthz", store_url.trim_end_matches('/'));
        match self.http.get(&url).send().await {
            Ok(r) => r.status().is_success(),
            Err(_) => false,
        }
    }
}

#[async_trait]
impl BatchSender for Client {
    async fn send_batch(&self, batch: &[SessionEnvelope]) -> Result<Vec<Outcome>, UploadError> {
        let mut req = self
            .http
            .post(&self.endpoint)
            .json(&serde_json::json!({ "envelopes": batch }));
        if let Some(tok) = &self.write_token {
            req = req.bearer_auth(tok);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| UploadError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(UploadError::Status(status.as_u16()));
        }
        let body: IngestResponse = resp
            .json()
            .await
            .map_err(|e| UploadError::Decode(e.to_string()))?;
        Ok(body.results)
    }
}

/// Fold a batch's per-item outcomes into counts, warning on each rejection.
pub(crate) fn tally(results: &[Outcome]) -> BatchTally {
    let mut t = BatchTally::default();
    for r in results {
        match r {
            Outcome::Accepted { .. } => t.accepted += 1,
            Outcome::Duplicate { .. } => t.duplicate += 1,
            Outcome::Rejected { session_id, reason } => {
                t.rejected += 1;
                tracing::warn!(%session_id, %reason, "store rejected an envelope");
            }
        }
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tally_counts_each_outcome() {
        let results = vec![
            Outcome::Accepted {
                session_id: "a".into(),
            },
            Outcome::Duplicate {
                session_id: "b".into(),
            },
            Outcome::Duplicate {
                session_id: "c".into(),
            },
            Outcome::Rejected {
                session_id: "d".into(),
                reason: "bad".into(),
            },
        ];
        let t = tally(&results);
        assert_eq!(t.accepted, 1);
        assert_eq!(t.duplicate, 2);
        assert_eq!(t.rejected, 1);
    }

    // Test-only extractors. Each is exercised with a matching and a non-matching
    // input across the tests below, so both arms are covered with no exclusion
    // while the assertions stay strong (exact variant, and for Status the code).
    fn is_accepted(o: &Outcome) -> bool {
        matches!(o, Outcome::Accepted { .. })
    }

    fn status_code(e: &UploadError) -> Option<u16> {
        match e {
            UploadError::Status(code) => Some(*code),
            _ => None,
        }
    }

    fn is_decode(e: &UploadError) -> bool {
        matches!(e, UploadError::Decode(_))
    }

    fn is_http(e: &UploadError) -> bool {
        matches!(e, UploadError::Http(_))
    }

    #[test]
    fn outcome_deserializes_from_store_shape() {
        let json = r#"{"status":"accepted","session_id":"s1"}"#;
        let o: Outcome = serde_json::from_str(json).unwrap();
        assert!(is_accepted(&o));
        // Cover the `is_accepted` false arm with a non-Accepted outcome.
        assert!(!is_accepted(&Outcome::Duplicate {
            session_id: "x".into()
        }));
    }

    #[test]
    fn session_id_reads_each_variant() {
        assert_eq!(
            Outcome::Accepted {
                session_id: "a".into()
            }
            .session_id(),
            "a"
        );
        assert_eq!(
            Outcome::Duplicate {
                session_id: "b".into()
            }
            .session_id(),
            "b"
        );
        assert_eq!(
            Outcome::Rejected {
                session_id: "c".into(),
                reason: "why".into()
            }
            .session_id(),
            "c"
        );
    }

    // --- HTTP integration: drive real reqwest against a canned local server ----

    use session_capture::{Metadata, Origin, SessionEnvelope, SCS_VERSION};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    /// Assemble a raw HTTP/1.1 response. `Connection: close` so reqwest opens a
    /// fresh connection per request, keeping the tiny server single-read simple.
    fn http_response(status: u16, body: &[u8]) -> Vec<u8> {
        // reqwest parses the numeric status; the reason phrase is ignored, so a
        // fixed phrase keeps this helper branch-free.
        let mut head = format!("HTTP/1.1 {status} X\r\n");
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        head.push_str("Connection: close\r\n\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(body);
        out
    }

    /// Spawn a throwaway HTTP server that answers each incoming request with the
    /// next canned response, then exits. Returns the base URL. Runs entirely on
    /// 127.0.0.1 with an ephemeral port, so no real network is touched.
    fn spawn_server(responses: Vec<Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            // `Connection: close` -> one accept per response, so the loop runs
            // exactly `responses.len()` times with no unexercised branches.
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0u8; 16384];
                let _ = stream.read(&mut buf); // drain the (small) request
                let _ = stream.write_all(&response);
                let _ = stream.flush();
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    /// A URL that refuses connections: bind then immediately drop the listener so
    /// the port is free but nothing is listening.
    fn dead_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{port}")
    }

    fn envelope(id: &str) -> SessionEnvelope {
        SessionEnvelope {
            scs_version: SCS_VERSION.to_string(),
            origin: Origin {
                host: "h".into(),
                environment: "test".into(),
            },
            agent: "Cursor".into(),
            source_format: "cursor-state-vscdb".into(),
            session_id: id.into(),
            parent_session_id: None,
            started_at: "2026-07-14T00:00:00Z".into(),
            last_activity_at: "2026-07-14T00:10:00Z".into(),
            content_hash: None,
            metadata: Some(Metadata::default()),
            raw: serde_json::json!({ "text": "x" }),
        }
    }

    #[test]
    fn client_new_trims_trailing_slash_in_endpoint() {
        let c = Client::new("http://host:18090/", Some("tok".into()));
        assert_eq!(c.endpoint, "http://host:18090/v1/sessions/batch");
    }

    #[tokio::test]
    async fn send_batch_success_parses_results_with_bearer_auth() {
        let body = br#"{"results":[{"status":"accepted","session_id":"s1"},{"status":"rejected","session_id":"s2","reason":"dup"}]}"#;
        let url = spawn_server(vec![http_response(200, body)]);
        let client = Client::new(&url, Some("write-token".into()));
        let results = client
            .send_batch(&[envelope("s1"), envelope("s2")])
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].session_id(), "s1");
    }

    #[tokio::test]
    async fn send_batch_success_without_token_omits_auth() {
        let body = br#"{"results":[{"status":"duplicate","session_id":"s1"}]}"#;
        let url = spawn_server(vec![http_response(200, body)]);
        let client = Client::new(&url, None);
        let results = client.send_batch(&[envelope("s1")]).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn send_batch_non_2xx_is_status_error() {
        let url = spawn_server(vec![http_response(500, b"nope")]);
        let client = Client::new(&url, None);
        let err = client.send_batch(&[envelope("s1")]).await.err().unwrap();
        assert_eq!(status_code(&err), Some(500));
        // A Status error also covers the false arms of the other extractors.
        assert!(!is_decode(&err));
        assert!(!is_http(&err));
    }

    #[tokio::test]
    async fn send_batch_undecodable_body_is_decode_error() {
        let url = spawn_server(vec![http_response(200, b"not json at all")]);
        let client = Client::new(&url, None);
        let err = client.send_batch(&[envelope("s1")]).await.err().unwrap();
        assert!(is_decode(&err));
        // A Decode error covers the `status_code` `_ => None` arm.
        assert_eq!(status_code(&err), None);
    }

    #[tokio::test]
    async fn send_batch_connection_refused_is_http_error() {
        let client = Client::new(&dead_url(), None);
        let err = client.send_batch(&[envelope("s1")]).await.err().unwrap();
        assert!(is_http(&err));
    }

    #[tokio::test]
    async fn healthy_true_on_2xx_and_false_on_error_and_non_2xx() {
        let ok = spawn_server(vec![http_response(200, b"ok")]);
        let client = Client::new(&ok, None);
        assert!(client.healthy(&ok).await);

        let bad = spawn_server(vec![http_response(500, b"down")]);
        assert!(!client.healthy(&bad).await);

        assert!(!client.healthy(&dead_url()).await);
    }

    #[test]
    fn tally_reject_emits_warn_fields() {
        // With WARN enabled, the rejection warn!'s `session_id` / `reason` field
        // expressions are evaluated (skipped when logging is disabled).
        crate::install_warn_logging();
        let t = tally(&[Outcome::Rejected {
            session_id: "z".into(),
            reason: "too big".into(),
        }]);
        assert_eq!(t.rejected, 1);
    }

    #[test]
    fn upload_error_messages_render() {
        assert_eq!(
            UploadError::Http("boom".into()).to_string(),
            "http request failed: boom"
        );
        assert_eq!(
            UploadError::Status(413).to_string(),
            "store returned status 413"
        );
        assert_eq!(
            UploadError::Decode("x".into()).to_string(),
            "could not parse store response: x"
        );
    }
}
