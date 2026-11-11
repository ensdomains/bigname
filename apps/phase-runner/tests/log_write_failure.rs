//! A log line that cannot be written is dropped: the runner neither panics nor reports the failure
//! on stderr, whichever output failed.
//!
//! This target has `harness = false` because the child must log and exit with nothing else writing
//! to its stdout and stderr, and libtest writes its report to both. `main` answers the `--list` and
//! `--exact` calls nextest makes, so each row is its own test there.

use std::{
    fs::File,
    path::Path,
    process::{Command, ExitCode, Stdio},
};

const CHILD_MARKER: &str = "PHASE_RUNNER_LOG_WRITE_FAILURE_CHILD";
const EVENTS: [&str; 2] = ["first probe event", "second probe event"];
const DEV_FULL: &str = "/dev/full";

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

struct Row {
    name: &'static str,
    json: bool,
    stdout: Sink,
    stderr: Sink,
}

const fn row(name: &'static str, json: bool, stdout: Sink, stderr: Sink) -> Row {
    Row {
        name,
        json,
        stdout,
        stderr,
    }
}

const ROWS: [Row; 8] = [
    row("compact_both_dev_full", false, Sink::DevFull, Sink::DevFull),
    row("json_both_dev_full", true, Sink::DevFull, Sink::DevFull),
    row(
        "compact_stdout_dev_full",
        false,
        Sink::DevFull,
        Sink::Captured,
    ),
    row("json_stdout_dev_full", true, Sink::DevFull, Sink::Captured),
    row(
        "compact_both_closed_pipes",
        false,
        Sink::ClosedPipe,
        Sink::ClosedPipe,
    ),
    row(
        "json_both_closed_pipes",
        true,
        Sink::ClosedPipe,
        Sink::ClosedPipe,
    ),
    row(
        "compact_healthy_outputs",
        false,
        Sink::Captured,
        Sink::Captured,
    ),
    row("json_healthy_outputs", true, Sink::Captured, Sink::Captured),
];

fn child() {
    phase_runner::logging::init();
    tracing::info!("{}", EVENTS[0]);
    tracing::warn!("{}", EVENTS[1]);
}

fn assert_events(stdout: &str, json: bool) {
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), EVENTS.len(), "stdout: {stdout}");
    for (line, message) in lines.into_iter().zip(EVENTS) {
        let parsed = serde_json::from_str::<serde_json::Value>(line);
        if json {
            let event = parsed.expect("a JSON log line");
            assert_eq!(event["fields"]["message"], message, "line: {line}");
        } else {
            assert!(parsed.is_err(), "a JSON line in compact mode: {line}");
            assert!(line.contains(message), "line: {line}");
        }
    }
}

fn run(row: &Row) {
    if [row.stdout, row.stderr].contains(&Sink::DevFull) && !Path::new(DEV_FULL).exists() {
        println!("{}: skipped, this host has no {DEV_FULL}", row.name);
        return;
    }
    let mut command = Command::new(std::env::current_exe().expect("find this test binary"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("BIGNAME_") {
            command.env_remove(key);
        }
    }
    if row.json {
        command.env("BIGNAME_LOG_JSON", "1");
    }
    let output = command
        .env(CHILD_MARKER, "1")
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(row.stdout.stdio())
        .stderr(row.stderr.stdio())
        .output()
        .expect("run the child");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}: {}, stderr: {stderr}",
        row.name,
        output.status
    );
    assert!(stderr.is_empty(), "{}: stderr: {stderr}", row.name);
    if row.stdout == Sink::Captured {
        assert_events(&String::from_utf8_lossy(&output.stdout), row.json);
    }
    println!("{}: ok", row.name);
}

fn main() -> ExitCode {
    if std::env::var_os(CHILD_MARKER).is_some() {
        child();
        return ExitCode::SUCCESS;
    }
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let flag = |name: &str| args.iter().any(|arg| arg == name);
    if flag("--ignored") {
        return ExitCode::SUCCESS;
    }
    if flag("--list") {
        for row in &ROWS {
            println!("{}: test", row.name);
        }
        return ExitCode::SUCCESS;
    }
    let filters = args
        .iter()
        .filter(|arg| !arg.starts_with("--"))
        .collect::<Vec<_>>();
    let selected = |row: &Row| {
        filters.is_empty()
            || filters.iter().any(|filter| {
                if flag("--exact") {
                    row.name == filter.as_str()
                } else {
                    row.name.contains(filter.as_str())
                }
            })
    };
    ROWS.iter().filter(|row| selected(row)).for_each(run);
    ExitCode::SUCCESS
}
