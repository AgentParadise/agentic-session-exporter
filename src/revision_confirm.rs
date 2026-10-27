//! Revision confirmation: does the store hold one exact qualified revision?
//!
//! DRAFT CONTRACT. Every wire detail of revision confirmation lives in this one
//! module: the paths, query parameter names, the stored-hash header and the
//! capability keys. They follow the unratified APS-V1-0004 draft in
//! AgentParadise/agent-paradise-standards-system PR #154 (base 5.1.1 and 5.3.1,
//! inventory profile `HEAD /v1/transcripts`). The open owner questions there
//! are the spec version and the route shape (path or query form). When the
//! draft is ruled, this module is what changes; the CLI contract in
//! `docs/qualified-capture.md` does not.
//!
//! Every store answer maps to exactly one [`Outcome`] and none is folded into
//! another: base 5.3.1 says a client "MUST NOT fold any of them into absent".
//! A transport failure is `unavailable`. Anything the draft does not define,
//! including a 200 without a well-formed stored hash and any redirect, is
//! `unexpected`.
use crate::qualified_capture::{valid_content_hash, CaptureUploadError};
use serde::Serialize;
use session_capture::inventory::QualifiedTranscript;
use std::time::Duration;

// --- Draft wire contract (APSS PR #154). Change here, and only here. --------
const CAPABILITIES_PATH: &str = "v1/capabilities";
const CONFIRM_PATH: &str = "v1/transcripts";
const QUERY_SOURCE: &str = "source_instance_id";
const QUERY_HARNESS: &str = "harness";
const QUERY_NATIVE_ID: &str = "native_session_id";
const QUERY_CONTENT_HASH: &str = "content_hash";
const STORED_HASH_HEADER: &str = "x-stored-content-hash";
const CAPABILITY_CONFIRM: &str = "revision_confirm";
const CAPABILITY_VERIFIES_BODY: &str = "confirm_verifies_body";
// ----------------------------------------------------------------------------

/// Bound on a capability document. It is a handful of keys.
const MAX_CAPABILITIES_BYTES: usize = 64 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Committed durably; `stored_content_hash` and `body_verified` are set.
    Confirmed,
    /// Not held, including a superseded revision.
    Absent,
    /// 401 or 403, on the capability document or the confirmation.
    Unauthorized,
    /// 409: the stored body failed integrity verification.
    IntegrityFailure,
    /// 503, timeout or connection failure.
    Unavailable,
    /// No capability document, or `revision_confirm` is not `true`.
    NotAdvertised,
    /// Any answer outside the draft. Never absent, never confirmed.
    Unexpected,
}

/// The `--capture-confirm` stdout document (schema 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Confirmation {
    pub schema_version: u32,
    pub outcome: Outcome,
    /// The HTTP status that decided the outcome; null for transport failure.
    pub status: Option<u16>,
    pub stored_content_hash: Option<String>,
    pub body_verified: Option<bool>,
}

impl Confirmation {
    fn of(outcome: Outcome, status: Option<u16>) -> Self {
        Self {
            schema_version: 1,
            outcome,
            status,
            stored_content_hash: None,
            body_verified: None,
        }
    }
}

fn non_success(status: u16) -> Confirmation {
    let outcome = match status {
        401 | 403 => Outcome::Unauthorized,
        503 => Outcome::Unavailable,
        _ => Outcome::Unexpected,
    };
    Confirmation::of(outcome, Some(status))
}

/// Intentionally no Debug: this owns the read-scoped credential.
pub struct RevisionConfirmClient {
    http: reqwest::Client,
    base: reqwest::Url,
    token: String,
}

impl RevisionConfirmClient {
    pub fn new(base: &str, token: String) -> Result<Self, CaptureUploadError> {
        let mut base = reqwest::Url::parse(base).map_err(|_| CaptureUploadError::Configuration)?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || token.is_empty()
            || token.len() > 4096
            || !token.bytes().all(|b| (33..=126).contains(&b))
        {
            return Err(CaptureUploadError::Configuration);
        }
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| CaptureUploadError::Configuration)?;
        Ok(Self { http, base, token })
    }

    /// Ask now. Never an error: every answer, and every failure, is an outcome.
    pub async fn confirm(
        &self,
        identity: &QualifiedTranscript,
        content_hash: &str,
    ) -> Confirmation {
        let verifies_body = match self.discover().await {
            Ok(verifies_body) => verifies_body,
            Err(outcome) => return outcome,
        };
        let Ok(mut url) = self.base.join(CONFIRM_PATH) else {
            return Confirmation::of(Outcome::Unexpected, None);
        };
        url.query_pairs_mut()
            .append_pair(QUERY_SOURCE, identity.source_instance_id())
            .append_pair(QUERY_HARNESS, identity.harness())
            .append_pair(QUERY_NATIVE_ID, identity.native_session_id())
            .append_pair(QUERY_CONTENT_HASH, content_hash);
        let response = match self.http.head(url).bearer_auth(&self.token).send().await {
            Ok(response) => response,
            Err(_) => return Confirmation::of(Outcome::Unavailable, None),
        };
        let status = response.status().as_u16();
        match status {
            200 => {
                let stored = response
                    .headers()
                    .get(STORED_HASH_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| valid_content_hash(value));
                match stored {
                    Some(stored) => Confirmation {
                        schema_version: 1,
                        outcome: Outcome::Confirmed,
                        status: Some(200),
                        stored_content_hash: Some(stored.to_string()),
                        body_verified: Some(verifies_body),
                    },
                    None => Confirmation::of(Outcome::Unexpected, Some(200)),
                }
            }
            404 => Confirmation::of(Outcome::Absent, Some(404)),
            409 => Confirmation::of(Outcome::IntegrityFailure, Some(409)),
            other => non_success(other),
        }
    }

    /// `confirm_verifies_body` when confirmation is advertised, else the outcome.
    async fn discover(&self) -> Result<bool, Confirmation> {
        let url = self
            .base
            .join(CAPABILITIES_PATH)
            .map_err(|_| Confirmation::of(Outcome::Unexpected, None))?;
        let mut response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|_| Confirmation::of(Outcome::Unavailable, None))?;
        match response.status().as_u16() {
            200 => {}
            // No capability document: nothing optional is advertised.
            404 => return Err(Confirmation::of(Outcome::NotAdvertised, Some(404))),
            other => return Err(non_success(other)),
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    bytes.extend_from_slice(&chunk);
                    if bytes.len() > MAX_CAPABILITIES_BYTES {
                        return Err(Confirmation::of(Outcome::Unexpected, Some(200)));
                    }
                }
                Ok(None) => break,
                Err(_) => return Err(Confirmation::of(Outcome::Unavailable, None)),
            }
        }
        // Unknown keys are tolerated: the document grows additively (8.3).
        let document: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| Confirmation::of(Outcome::Unexpected, Some(200)))?;
        let Some(document) = document.as_object() else {
            return Err(Confirmation::of(Outcome::Unexpected, Some(200)));
        };
        match document.get(CAPABILITY_CONFIRM) {
            Some(serde_json::Value::Bool(true)) => {}
            None | Some(serde_json::Value::Bool(false)) => {
                return Err(Confirmation::of(Outcome::NotAdvertised, Some(200)))
            }
            Some(_) => return Err(Confirmation::of(Outcome::Unexpected, Some(200))),
        }
        // The draft requires both keys together.
        match document.get(CAPABILITY_VERIFIES_BODY) {
            Some(serde_json::Value::Bool(verifies)) => Ok(*verifies),
            _ => Err(Confirmation::of(Outcome::Unexpected, Some(200))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// One scripted reply per request: (status, extra headers, body).
    type Reply = (u16, &'static str, &'static str);

    /// Serves `replies` in order and returns each request's head line.
    fn serve(replies: Vec<Reply>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let worker = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for (status, headers, body) in replies {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                let mut buffer = [0u8; 4096];
                while !data.windows(4).any(|v| v == b"\r\n\r\n") {
                    let count = stream.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                }
                let head = String::from_utf8_lossy(&data).to_string();
                assert!(head
                    .to_lowercase()
                    .contains("authorization: bearer read-secret"));
                seen.push(head.lines().next().unwrap().to_string());
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            seen
        });
        (url, worker)
    }

    const ADVERTISED: Reply = (
        200,
        "",
        r#"{"revision_confirm":true,"confirm_verifies_body":true,"request_encodings":["gzip"]}"#,
    );
    const STORED: &str = "X-Stored-Content-Hash: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\r\n";

    fn identity() -> QualifiedTranscript {
        serde_json::from_value(serde_json::json!({
            "source_instance_id": "install-1",
            "harness": "claude",
            "native_session_id": "Native/雪"
        }))
        .unwrap()
    }

    fn hash() -> String {
        format!("sha256:{}", "a".repeat(64))
    }

    async fn ask(replies: Vec<Reply>) -> (Confirmation, Vec<String>) {
        let (url, worker) = serve(replies);
        let client = RevisionConfirmClient::new(&url, "read-secret".into()).unwrap();
        let outcome = client.confirm(&identity(), &hash()).await;
        (outcome, worker.join().unwrap())
    }

    #[tokio::test]
    async fn each_confirm_status_maps_to_exactly_one_outcome() {
        let cases: Vec<(Reply, Outcome, Option<u16>)> = vec![
            ((200, STORED, ""), Outcome::Confirmed, Some(200)),
            ((404, "", ""), Outcome::Absent, Some(404)),
            ((401, "", ""), Outcome::Unauthorized, Some(401)),
            ((403, "", ""), Outcome::Unauthorized, Some(403)),
            ((409, "", ""), Outcome::IntegrityFailure, Some(409)),
            ((503, "", ""), Outcome::Unavailable, Some(503)),
            ((200, "", ""), Outcome::Unexpected, Some(200)),
            (
                (200, "X-Stored-Content-Hash: sha256:short\r\n", ""),
                Outcome::Unexpected,
                Some(200),
            ),
            ((500, "", ""), Outcome::Unexpected, Some(500)),
            ((400, "", ""), Outcome::Unexpected, Some(400)),
            (
                (307, "Location: https://elsewhere.test/\r\n", ""),
                Outcome::Unexpected,
                Some(307),
            ),
        ];
        for (reply, outcome, status) in cases {
            let (answer, seen) = ask(vec![ADVERTISED, reply]).await;
            assert_eq!(
                (answer.outcome, answer.status),
                (outcome, status),
                "{reply:?}"
            );
            assert_eq!(seen[0], "GET /v1/capabilities HTTP/1.1");
            assert!(seen[1].starts_with(
                "HEAD /v1/transcripts?source_instance_id=install-1&harness=claude&native_session_id=Native%2F%E9%9B%AA&content_hash=sha256%3Aaaaa"
            ));
            if outcome == Outcome::Confirmed {
                assert_eq!(answer.body_verified, Some(true));
                assert_eq!(
                    answer.stored_content_hash.as_deref(),
                    Some(format!("sha256:{}", "b".repeat(64)).as_str())
                );
            } else {
                assert_eq!(
                    (answer.stored_content_hash, answer.body_verified),
                    (None, None)
                );
            }
        }
    }

    #[tokio::test]
    async fn capability_answer_decides_before_any_confirm_request() {
        let cases: Vec<(Reply, Outcome)> = vec![
            ((404, "", ""), Outcome::NotAdvertised),
            (
                (200, "", r#"{"request_encodings":["gzip"]}"#),
                Outcome::NotAdvertised,
            ),
            (
                (200, "", r#"{"revision_confirm":false}"#),
                Outcome::NotAdvertised,
            ),
            (
                (200, "", r#"{"revision_confirm":true}"#),
                Outcome::Unexpected,
            ),
            (
                (
                    200,
                    "",
                    r#"{"revision_confirm":"yes","confirm_verifies_body":true}"#,
                ),
                Outcome::Unexpected,
            ),
            ((200, "", "not json"), Outcome::Unexpected),
            ((200, "", "[]"), Outcome::Unexpected),
            ((503, "", ""), Outcome::Unavailable),
            ((401, "", ""), Outcome::Unauthorized),
        ];
        for (reply, outcome) in cases {
            let (answer, seen) = ask(vec![reply]).await;
            assert_eq!(answer.outcome, outcome, "{reply:?}");
            assert_eq!(seen.len(), 1, "no HEAD after {reply:?}");
        }
    }

    #[tokio::test]
    async fn body_verified_comes_from_the_capability_document() {
        let capabilities: Reply = (
            200,
            "",
            r#"{"revision_confirm":true,"confirm_verifies_body":false}"#,
        );
        let (answer, _) = ask(vec![capabilities, (200, STORED, "")]).await;
        assert_eq!(answer.outcome, Outcome::Confirmed);
        assert_eq!(answer.body_verified, Some(false));
    }

    #[tokio::test]
    async fn an_unreachable_store_is_unavailable_not_absent() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let client = RevisionConfirmClient::new(&url, "read-secret".into()).unwrap();
        let answer = client.confirm(&identity(), &hash()).await;
        assert_eq!(
            (answer.outcome, answer.status),
            (Outcome::Unavailable, None)
        );
    }

    #[test]
    fn rejects_unusable_configuration() {
        for (url, token) in [
            ("ftp://store", "t"),
            ("https://user:pw@store", "t"),
            ("https://store?x=1", "t"),
            ("https://store", ""),
            ("https://store", "has space"),
        ] {
            assert!(RevisionConfirmClient::new(url, token.into()).is_err());
        }
    }

    #[test]
    fn document_shape_is_stable() {
        let document =
            serde_json::to_value(Confirmation::of(Outcome::IntegrityFailure, Some(409))).unwrap();
        assert_eq!(
            document,
            serde_json::json!({"schema_version":1,"outcome":"integrity_failure","status":409,
                "stored_content_hash":null,"body_verified":null})
        );
    }
}
