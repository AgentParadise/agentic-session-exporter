//! Provider transcript readers that build SCS envelopes.
//!
//! These reuse the *reading* logic from the SeshMagic exporters
//! (`ws_packages/sesh-magic-adapters/src/exporters/{claude.rs,codex.rs}`): how
//! to locate the identity + header metadata inside a provider JSONL transcript.
//! The critical difference: the exporters flatten every row into a `messages`
//! array and throw the original away. Here we do the opposite: we keep the
//! original transcript bytes as `raw` (verbatim, as an SCS string), and derive
//! only header metadata during the read. The searchable text is derived
//! separately, by the store's server-side projection step. This is what makes
//! resume possible (SCS D1).
//!
//! Input is the transcript file's bytes. Metadata derivation skips malformed
//! lines (matching the exporters' resilience), but the Claude `raw` string
//! retains every original UTF-8 byte, including those lines.

use chrono::{DateTime, Utc};
use serde_json::Value;

use session_capture::{Metadata, Origin, SessionEnvelope, SCS_VERSION};

/// Errors from reading a provider transcript.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("transcript is empty (no well-formed rows)")]
    Empty,
    #[error("could not determine session id from transcript")]
    NoSessionId,
    #[error("Claude JSONL transcript is not valid UTF-8, so it cannot be preserved as an SCS raw string")]
    InvalidUtf8,
}

/// Parse the well-formed JSONL rows out of a transcript byte buffer. Blank and
/// malformed lines are dropped (as the exporters do). Order is preserved.
fn parse_rows(bytes: &[u8]) -> Vec<Value> {
    let text = String::from_utf8_lossy(bytes);
    let mut rows = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<Value>(line) {
            rows.push(v);
        }
    }
    rows
}

/// Read a Claude Code JSONL transcript into an envelope.
///
/// `native_id` is the file stem (`<sessionUuid>`), as the exporter derives it.
/// `origin` describes where this transcript was captured (host + environment).
pub fn read_claude(
    bytes: &[u8],
    native_id: &str,
    origin: Origin,
) -> Result<SessionEnvelope, ParseError> {
    // SCS permits an opaque raw string. Keep Claude JSONL as exactly that
    // string instead of reserializing parsed rows: JSON reserialization alters
    // whitespace and drops malformed lines, both of which break byte-exact
    // resume and make the canonical pre-sanitize hash depend on a parser.
    let raw = String::from_utf8(bytes.to_vec()).map_err(|_| ParseError::InvalidUtf8)?;
    let rows = parse_rows(raw.as_bytes());
    if rows.is_empty() {
        return Err(ParseError::Empty);
    }
    if native_id.trim().is_empty() {
        return Err(ParseError::NoSessionId);
    }

    let mut meta = Metadata::default();
    let mut message_count: u64 = 0;
    let mut first_ts: Option<DateTime<Utc>> = None;
    let mut last_ts: Option<DateTime<Utc>> = None;

    for row in &rows {
        // First-write-wins header stash, mirroring claude.rs::stash_metadata.
        if meta.cwd.is_none() {
            if let Some(cwd) = row.get("cwd").and_then(|x| x.as_str()) {
                meta.cwd = Some(cwd.to_string());
            }
        }
        if meta.project.is_none() {
            if let Some(branch) = row.get("gitBranch").and_then(|x| x.as_str()) {
                meta.project = Some(branch.to_string());
            }
        }
        if let Some(ts) = row.get("timestamp").and_then(|x| x.as_str()) {
            if let Ok(dt) = ts.parse::<DateTime<Utc>>() {
                first_ts.get_or_insert(dt);
                last_ts = Some(dt);
            }
        }
        // Count conversational message rows (type user/assistant).
        if matches!(
            row.get("type").and_then(|t| t.as_str()),
            Some("user") | Some("assistant")
        ) {
            message_count += 1;
        }
        let row_type = row.get("type").and_then(|x| x.as_str());
        let message_role = row
            .get("message")
            .and_then(|message| message.get("role"))
            .and_then(|x| x.as_str());
        if meta.model.is_none()
            && (row_type == Some("assistant") || message_role == Some("assistant"))
        {
            let model = row.get("model").and_then(|x| x.as_str()).or_else(|| {
                row.get("message")
                    .and_then(|message| message.get("model"))
                    .and_then(|x| x.as_str())
            });
            if let Some(model) = model {
                if !model.is_empty() {
                    meta.model = Some(model.to_string());
                }
            }
        }
    }
    meta.message_count = Some(message_count);

    let (started_at, last_activity_at) = time_bounds(first_ts, last_ts);
    Ok(SessionEnvelope {
        scs_version: SCS_VERSION.to_string(),
        origin,
        agent: "ClaudeCode".to_string(),
        source_format: "claude-code-jsonl".to_string(),
        session_id: native_id.to_string(),
        parent_session_id: None,
        started_at,
        last_activity_at,
        content_hash: None,
        metadata: Some(meta),
        raw: Value::String(raw),
    })
}

/// Read a Codex rollout JSONL transcript into an envelope.
///
/// `fallback_id` is used when the transcript carries no `session_meta.id` (the
/// exporter falls back to the filename stem the same way).
pub fn read_codex(
    bytes: &[u8],
    fallback_id: &str,
    origin: Origin,
) -> Result<SessionEnvelope, ParseError> {
    let rows = parse_rows(bytes);
    if rows.is_empty() {
        return Err(ParseError::Empty);
    }

    let mut session_id = fallback_id.to_string();
    let mut meta = Metadata::default();
    let mut message_count: u64 = 0;
    let mut first_ts: Option<DateTime<Utc>> = None;
    let mut last_ts: Option<DateTime<Utc>> = None;

    for row in &rows {
        if let Some(ts) = row.get("timestamp").and_then(|x| x.as_str()) {
            if let Ok(dt) = ts.parse::<DateTime<Utc>>() {
                first_ts.get_or_insert(dt);
                last_ts = Some(dt);
            }
        }
        match row.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "session_meta" => {
                if let Some(payload) = row.get("payload") {
                    if let Some(id) = payload.get("id").and_then(|x| x.as_str()) {
                        if !id.is_empty() {
                            session_id = id.to_string();
                        }
                    }
                    if let Some(cwd) = payload.get("cwd").and_then(|x| x.as_str()) {
                        meta.cwd = Some(cwd.to_string());
                    }
                    if let Some(m) = payload.get("model_provider").and_then(|x| x.as_str()) {
                        meta.model = Some(m.to_string());
                    }
                }
            }
            "response_item" => {
                if let Some(payload) = row.get("payload") {
                    if payload.get("type").and_then(|t| t.as_str()) == Some("message") {
                        message_count += 1;
                    }
                }
            }
            _ => {}
        }
    }
    meta.message_count = Some(message_count);

    if session_id.trim().is_empty() {
        return Err(ParseError::NoSessionId);
    }

    let (started_at, last_activity_at) = time_bounds(first_ts, last_ts);
    Ok(SessionEnvelope {
        scs_version: SCS_VERSION.to_string(),
        origin,
        agent: "Codex".to_string(),
        source_format: "codex-rollout-jsonl".to_string(),
        session_id,
        parent_session_id: None,
        started_at,
        last_activity_at,
        content_hash: None,
        metadata: Some(meta),
        raw: Value::Array(rows),
    })
}

/// Resolve the started/last-activity pair. If the transcript carries no
/// timestamps, fall back to now for both (a valid, if coarse, envelope).
fn time_bounds(first: Option<DateTime<Utc>>, last: Option<DateTime<Utc>>) -> (String, String) {
    // `first` and `last` are only ever set together (in the same loop step), so
    // the mixed `(Some, None)` / `(None, Some)` states are unreachable and fold
    // into the no-timestamp fallback below.
    match (first, last) {
        (Some(f), Some(l)) if l >= f => (f.to_rfc3339(), l.to_rfc3339()),
        (Some(f), Some(_)) => (f.to_rfc3339(), f.to_rfc3339()),
        _ => {
            let now = Utc::now();
            (now.to_rfc3339(), now.to_rfc3339())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin() -> Origin {
        Origin {
            host: "box".into(),
            environment: "vps".into(),
        }
    }

    #[test]
    fn claude_preserves_raw_verbatim_and_derives_header() {
        let jsonl = concat!(
            r#"{"type":"user","timestamp":"2026-07-01T00:00:00Z","cwd":"/work","gitBranch":"main","message":{"role":"user","content":"hi"}}"#,
            "\n",
            r#"{"type":"assistant","timestamp":"2026-07-01T00:05:00Z","model":"claude-3-5-sonnet","message":{"role":"assistant","content":"hello"}}"#,
            "\n",
        );
        let env = read_claude(jsonl.as_bytes(), "sess-uuid", origin()).unwrap();
        assert_eq!(env.agent, "ClaudeCode");
        assert_eq!(env.source_format, "claude-code-jsonl");
        assert_eq!(env.session_id, "sess-uuid");
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.cwd.as_deref(), Some("/work"));
        assert_eq!(metadata.project.as_deref(), Some("main"));
        assert_eq!(metadata.model.as_deref(), Some("claude-3-5-sonnet"));
        assert_eq!(metadata.message_count, Some(2));
        assert_eq!(env.started_at, "2026-07-01T00:00:00+00:00");
        assert_eq!(env.last_activity_at, "2026-07-01T00:05:00+00:00");
        // raw is the exact original JSONL string, not parsed/reserialized rows
        // and not a flattened messages array.
        assert_eq!(env.raw.as_str(), Some(jsonl));
    }

    #[test]
    fn claude_extracts_model_from_message_nested_field() {
        let jsonl = concat!(
            r#"{"type":"assistant","message":{"model":"claude-internal","role":"assistant","content":"hello"}}"#,
            "\n",
        );
        let env = read_claude(jsonl.as_bytes(), "sess-uuid", origin()).unwrap();
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.model.as_deref(), Some("claude-internal"));
    }

    #[test]
    fn claude_prefers_assistant_model_over_non_assistant_model() {
        let jsonl = concat!(
            r#"{"type":"user","message":{"model":"bogus-user-model","role":"user","content":"hello"}}"#,
            "\n",
            r#"{"type":"assistant","message":{"model":"claude-3-5-sonnet","role":"assistant","content":"hi"}}"#,
            "\n",
        );
        let env = read_claude(jsonl.as_bytes(), "sess-uuid", origin()).unwrap();
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.model.as_deref(), Some("claude-3-5-sonnet"));
    }

    #[test]
    fn claude_prefers_role_assistant_model_when_type_missing() {
        let jsonl = concat!(
            r#"{"message":{"role":"assistant","model":"claude-3-opus","content":"hello"}}"#,
            "\n",
        );
        let env = read_claude(jsonl.as_bytes(), "sess-uuid", origin()).unwrap();
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.model.as_deref(), Some("claude-3-opus"));
    }

    #[test]
    fn claude_ignores_empty_assistant_model() {
        let jsonl = concat!(
            r#"{"type":"assistant","message":{"model":"","role":"assistant","content":"hello"}}"#,
            "\n",
            r#"{"type":"assistant","message":{"model":"claude-3-5-sonnet","role":"assistant","content":"hi"}}"#,
            "\n",
        );
        let env = read_claude(jsonl.as_bytes(), "sess-uuid", origin()).unwrap();
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.model.as_deref(), Some("claude-3-5-sonnet"));
    }

    #[test]
    fn claude_skips_malformed_lines() {
        let jsonl =
            "not json\n{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"ok\"}}\n";
        let env = read_claude(jsonl.as_bytes(), "s", origin()).unwrap();
        assert_eq!(env.raw.as_str(), Some(jsonl));
    }

    // Test-only extractor mapping each `ParseError` variant to a stable kind
    // string. Exercised below across all three variants (Empty, NoSessionId,
    // InvalidUtf8), so every match arm is live with no exclusion, while the
    // assertions stay exact: each kind string uniquely names one variant.
    fn parse_err_kind(e: &ParseError) -> &'static str {
        match e {
            ParseError::Empty => "empty",
            ParseError::NoSessionId => "no-session-id",
            ParseError::InvalidUtf8 => "invalid-utf8",
        }
    }

    #[test]
    fn claude_empty_errors() {
        // Structured variant assertion (`ParseError::Empty`), restored from the
        // weaker Display-string check.
        let err = read_claude(b"\n\n", "s", origin()).unwrap_err();
        assert_eq!(parse_err_kind(&err), "empty");
        // A non-UTF-8 buffer yields a different variant (`InvalidUtf8`).
        let non_empty = read_claude(&[0xff, 0xfe, 0x00], "s", origin()).unwrap_err();
        assert_eq!(parse_err_kind(&non_empty), "invalid-utf8");
    }

    #[test]
    fn claude_rejects_non_utf8() {
        // Invalid UTF-8 cannot be preserved as an SCS raw string. Assert the
        // structured variant, not the Display string.
        let err = read_claude(&[0xff, 0xfe, 0x00], "s", origin()).unwrap_err();
        assert_eq!(parse_err_kind(&err), "invalid-utf8");
    }

    #[test]
    fn claude_tolerates_bad_timestamp_and_non_conversational_rows() {
        // A row with an unparseable timestamp (skipped) and a non-user/assistant
        // row (not counted): both header-derivation branches are exercised.
        let jsonl = concat!(
            r#"{"type":"summary","timestamp":"not-a-timestamp","summary":"x"}"#,
            "\n",
            r#"{"type":"user","message":{"content":"hi"}}"#,
            "\n",
        );
        let env = read_claude(jsonl.as_bytes(), "s", origin()).unwrap();
        // Only the user row counts; the summary row does not.
        assert_eq!(env.metadata.as_ref().unwrap().message_count, Some(1));
    }

    #[test]
    fn codex_skips_unparseable_timestamp() {
        let jsonl = concat!(
            r#"{"type":"session_meta","timestamp":"nope","payload":{"id":"x"}}"#,
            "\n",
            r#"{"type":"response_item","payload":{"type":"message","content":"go"}}"#,
            "\n",
        );
        let env = read_codex(jsonl.as_bytes(), "stem", origin()).unwrap();
        assert_eq!(env.session_id, "x");
        assert_eq!(env.metadata.as_ref().unwrap().message_count, Some(1));
    }

    #[test]
    fn codex_prefers_session_meta_id() {
        let jsonl = concat!(
            r#"{"type":"session_meta","timestamp":"2026-07-01T00:00:00Z","payload":{"id":"real-id","cwd":"/c","model_provider":"openai"}}"#,
            "\n",
            r#"{"type":"response_item","payload":{"type":"message","role":"user","content":"go"}}"#,
            "\n",
        );
        let env = read_codex(jsonl.as_bytes(), "stem-fallback", origin()).unwrap();
        assert_eq!(env.session_id, "real-id");
        assert_eq!(env.agent, "Codex");
        let metadata = env.metadata.as_ref().unwrap();
        assert_eq!(metadata.cwd.as_deref(), Some("/c"));
        assert_eq!(metadata.model.as_deref(), Some("openai"));
        assert_eq!(metadata.message_count, Some(1));
        assert_eq!(env.raw.as_array().unwrap().len(), 2);
    }

    #[test]
    fn codex_falls_back_to_stem_id() {
        let jsonl =
            r#"{"type":"response_item","payload":{"type":"message","role":"user","content":"go"}}"#;
        let env = read_codex(jsonl.as_bytes(), "stem-fallback", origin()).unwrap();
        assert_eq!(env.session_id, "stem-fallback");
    }

    #[test]
    fn no_timestamps_falls_back_to_now() {
        let jsonl = r#"{"type":"user","message":{"role":"user","content":"hi"}}"#;
        let env = read_claude(jsonl.as_bytes(), "s", origin()).unwrap();
        assert!(env.last_activity_at >= env.started_at);
    }

    #[test]
    fn claude_blank_native_id_errors() {
        // Well-formed rows but a blank file stem yields no session id.
        let jsonl = r#"{"type":"user","message":{"role":"user","content":"hi"}}"#;
        let err = read_claude(jsonl.as_bytes(), "   ", origin()).unwrap_err();
        assert_eq!(parse_err_kind(&err), "no-session-id");
    }

    #[test]
    fn codex_empty_errors() {
        let err = read_codex(b"\n\n", "stem", origin()).unwrap_err();
        assert_eq!(parse_err_kind(&err), "empty");
    }

    #[test]
    fn codex_blank_id_everywhere_errors() {
        // session_meta id is empty AND the fallback stem is blank: no session id.
        let jsonl = concat!(
            r#"{"type":"session_meta","payload":{"id":""}}"#,
            "\n",
            r#"{"type":"response_item","payload":{"type":"message","content":"x"}}"#,
            "\n",
        );
        let err = read_codex(jsonl.as_bytes(), "   ", origin()).unwrap_err();
        assert_eq!(parse_err_kind(&err), "no-session-id");
    }

    #[test]
    fn codex_tolerates_sparse_and_unknown_rows() {
        // session_meta present but carrying none of id/cwd/model_provider; a
        // response_item without a payload; a response_item whose payload is not a
        // message; and a wholly unknown row type. The read must still succeed via
        // the fallback id and count zero messages.
        let jsonl = concat!(
            r#"{"type":"session_meta"}"#, // session_meta with no payload at all
            "\n",
            r#"{"type":"session_meta","payload":{"other":"x"}}"#,
            "\n",
            r#"{"type":"response_item"}"#,
            "\n",
            r#"{"type":"response_item","payload":{"type":"reasoning"}}"#,
            "\n",
            r#"{"type":"event","payload":{"note":"ignored"}}"#,
            "\n",
        );
        let env = read_codex(jsonl.as_bytes(), "stem", origin()).unwrap();
        assert_eq!(env.session_id, "stem");
        let metadata = env.metadata.as_ref().unwrap();
        assert!(metadata.cwd.is_none());
        assert!(metadata.model.is_none());
        assert_eq!(metadata.message_count, Some(0));
    }

    #[test]
    fn out_of_order_timestamps_clamp_last_to_first() {
        // A later row bearing an earlier timestamp must not push last_activity_at
        // before started_at; the pair clamps to the first timestamp.
        let jsonl = concat!(
            r#"{"type":"user","timestamp":"2026-07-01T00:05:00Z","message":{"content":"a"}}"#,
            "\n",
            r#"{"type":"assistant","timestamp":"2026-07-01T00:00:00Z","message":{"content":"b"}}"#,
            "\n",
        );
        let env = read_claude(jsonl.as_bytes(), "s", origin()).unwrap();
        assert_eq!(env.started_at, "2026-07-01T00:05:00+00:00");
        assert_eq!(env.last_activity_at, env.started_at);
    }

    #[test]
    fn captured_envelope_validates_against_apss_crate() {
        let jsonl =
            r#"{"type":"user","timestamp":"2026-07-01T00:00:00Z","message":{"content":"hi"}}"#;
        let envelope = read_claude(jsonl.as_bytes(), "captured", origin()).unwrap();
        let standard_envelope: &session_capture::SessionEnvelope = &envelope;
        assert!(standard_envelope.validate().is_ok());
    }
}
