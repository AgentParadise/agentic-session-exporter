//! Exercise the shipped qualified restore command against an HTTP fixture.
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    process::Command,
    time::Duration,
};

#[test]
fn qualified_cli_restores_exact_bytes_without_native_resume() {
    let root = tempfile::tempdir().unwrap();
    let checkout = root.path().join("repos/acme/widget");
    std::fs::create_dir_all(&checkout).unwrap();
    for args in [
        vec!["init"],
        vec![
            "remote",
            "add",
            "origin",
            "https://github.com/acme/widget.git",
        ],
    ] {
        assert!(Command::new("git")
            .current_dir(&checkout)
            .args(args)
            .output()
            .unwrap()
            .status
            .success());
    }
    let raw = "native transcript\r\n雪\r\n";
    let stored_hash = format!("sha256:{:x}", Sha256::digest(raw.as_bytes()));
    let envelope = serde_json::json!({
        "scs_version":"1.0","origin":{"host":"original","environment":"workspace"},
        "agent":"ClaudeCode","source_format":"claude-code-jsonl","session_id":"session-123",
        "started_at":"2026-09-22T00:00:00Z","last_activity_at":"2026-09-22T00:00:01Z",
        "metadata":{"repo":"acme/widget","git_remote":"https://github.com/acme/widget.git","cwd":"/src/acme/widget"},
        "raw":raw,"content_hash":format!("sha256:{}", "a".repeat(64)),"stored_content_hash":stored_hash
    }).to_string();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        for index in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                let mut buffer = [0; 2048];
                let count = socket.read(&mut buffer).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
            }
            let request = String::from_utf8(bytes).unwrap();
            assert!(request.contains("source_instance_id=installation"));
            assert!(request.contains("harness=claude"));
            assert!(request.contains(if index == 0 {
                "Bearer envelope-secret"
            } else {
                "Bearer raw-secret"
            }));
            let (body, headers) = if index == 0 {
                (envelope.as_str(), String::new())
            } else {
                assert!(request.contains("content_hash=sha256%3A"));
                (raw, format!("X-Source-Format: claude-code-jsonl\r\nX-Stored-Content-Hash: {stored_hash}\r\n"))
            };
            write!(
                socket,
                "HTTP/1.1 200 OK\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    let projects = root.path().join("projects");
    let output = Command::new(env!("CARGO_BIN_EXE_apss-session-reconstitute"))
        .args([
            "session-123",
            "--source-instance-id",
            "installation",
            "--harness",
            "claude",
            "--no-resume",
        ])
        .env("SESSION_STORE_URL", url)
        .env("SESSIONS_READ_TOKEN", "envelope-secret")
        .env("SESSIONS_RAW_READ_TOKEN", "raw-secret")
        .env("CLAUDE_PROJECTS_ROOT", &projects)
        .env("RECONSTITUTION_REPOS_ROOT", root.path().join("repos"))
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    server.join().unwrap();
    let entries: Vec<_> = std::fs::read_dir(projects).unwrap().collect();
    assert_eq!(entries.len(), 1);
    let transcript = entries[0]
        .as_ref()
        .unwrap()
        .path()
        .join("session-123.jsonl");
    assert_eq!(std::fs::read(transcript).unwrap(), raw.as_bytes());
    for stream in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(stream);
        assert!(!text.contains("envelope-secret") && !text.contains("raw-secret"));
    }
}

#[test]
fn restore_cli_probes_and_usage_errors_do_not_need_configuration() {
    let root = tempfile::tempdir().unwrap();
    for (args, expected) in [
        (vec!["--help"], 0),
        (vec!["--version"], 0),
        (vec!["--unknown"], 2),
        (vec!["native", "--source-instance-id", "source"], 2),
        (vec!["native", "--harness", "claude"], 2),
        (vec!["native", "--harness"], 2),
        (
            vec!["native", "--harness", "claude", "--harness", "codex"],
            2,
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_apss-session-reconstitute"))
            .args(args)
            .env_clear()
            .envs(std::env::vars().filter(|(key, _)| key == "LLVM_PROFILE_FILE"))
            .current_dir(root.path())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(expected));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}

#[test]
fn qualified_restore_missing_credentials_never_creates_files_or_prints_tokens() {
    let root = tempfile::tempdir().unwrap();
    for read_present in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_apss-session-reconstitute"));
        command
            .env_clear()
            .envs(std::env::vars().filter(|(key, _)| key == "LLVM_PROFILE_FILE"))
            .env("HOME", root.path())
            .env("SESSION_STORE_URL", "http://127.0.0.1:1")
            .args([
                "native",
                "--source-instance-id",
                "source",
                "--harness",
                "claude",
                "--no-resume",
            ]);
        if read_present {
            command.env("SESSIONS_READ_TOKEN", "never-print-this-token");
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("never-print-this-token"));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
