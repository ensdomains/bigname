//! A log line that cannot be written is dropped: `serve` neither panics nor reports the failure on
//! stderr, whichever output failed.
//!
//! Each row starts the real binary. It logs its first line, then refuses an invalid setting without
//! touching the network, so exit code 1 shows the process got past the failed write.

use std::{
    fs::File,
    path::Path,
    process::{Command, Output, Stdio},
};

const DEV_FULL: &str = "/dev/full";
const FIRST_EVENT: &str = "logging configured";
const REFUSAL: &str = "BIGNAME_API_REQUEST_TIMEOUT_MS must be greater than zero";
const LIBRARY_REPORT: &str = "[tracing-subscriber]";

#[derive(Clone, Copy, PartialEq)]
enum Sink {
    DevFull,
    ClosedPipe,
    Captured,
}

impl Sink {
    fn stdio(self) -> Stdio {
        match self {
            Self::DevFull => File::options()
                .write(true)
                .open(DEV_FULL)
                .expect("open /dev/full")
                .into(),
            Self::ClosedPipe => {
                let (reader, writer) = std::io::pipe().expect("create a pipe");
                drop(reader);
                writer.into()
            }
            Self::Captured => Stdio::piped(),
        }
    }
}

fn serve(json: bool, stdout: Sink, stderr: Sink) -> Option<Output> {
    if [stdout, stderr].contains(&Sink::DevFull) && !Path::new(DEV_FULL).exists() {
        println!("skipped, this host has no {DEV_FULL}");
        return None;
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_bigname-api"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("BIGNAME_") {
            command.env_remove(key);
        }
    }
    if json {
        command.env("BIGNAME_LOG_JSON", "1");
    }
    let output = command
        .arg("serve")
        .env("BIGNAME_API_REQUEST_TIMEOUT_MS", "0")
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(stdout.stdio())
        .stderr(stderr.stdio())
        .output()
        .expect("run bigname-api");
    Some(output)
}

fn assert_refused(json: bool, stdout: Sink, stderr: Sink) {
    let Some(output) = serve(json, stdout, stderr) else {
        return;
    };
    let reported = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}, stderr: {reported}",
        output.status
    );
    if stderr == Sink::Captured {
        assert!(reported.contains(REFUSAL), "stderr: {reported}");
        assert!(!reported.contains(LIBRARY_REPORT), "stderr: {reported}");
    }
}

fn assert_first_event(json: bool) {
    let output = serve(json, Sink::Captured, Sink::Captured).expect("no device needed");
    assert_eq!(output.status.code(), Some(1), "{}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().next().expect("a first log line");
    let parsed = serde_json::from_str::<serde_json::Value>(line);
    if json {
        let event = parsed.expect("a JSON log line");
        assert_eq!(event["fields"]["message"], FIRST_EVENT, "line: {line}");
    } else {
        assert!(parsed.is_err(), "a JSON line in compact mode: {line}");
        assert!(line.contains(FIRST_EVENT), "line: {line}");
    }
    let reported = String::from_utf8_lossy(&output.stderr);
    assert!(reported.contains(REFUSAL), "stderr: {reported}");
}

#[test]
fn compact_both_dev_full() {
    assert_refused(false, Sink::DevFull, Sink::DevFull);
}

#[test]
fn json_both_dev_full() {
    assert_refused(true, Sink::DevFull, Sink::DevFull);
}

#[test]
fn compact_stdout_dev_full() {
    assert_refused(false, Sink::DevFull, Sink::Captured);
}

#[test]
fn json_stdout_dev_full() {
    assert_refused(true, Sink::DevFull, Sink::Captured);
}

#[test]
fn compact_both_closed_pipes() {
    assert_refused(false, Sink::ClosedPipe, Sink::ClosedPipe);
}

#[test]
fn json_both_closed_pipes() {
    assert_refused(true, Sink::ClosedPipe, Sink::ClosedPipe);
}

#[test]
fn compact_healthy_outputs() {
    assert_first_event(false);
}

#[test]
fn json_healthy_outputs() {
    assert_first_event(true);
}
