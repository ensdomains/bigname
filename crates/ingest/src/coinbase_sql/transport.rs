use std::{
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

use super::error::truncate_error_body;

pub(super) struct HttpResponse {
    pub status: StatusCode,
    pub body: String,
}

pub(super) async fn run(
    url: String,
    token: String,
    sql: String,
    timeout_secs: u64,
) -> Result<HttpResponse> {
    tokio::task::spawn_blocking(move || run_blocking(&url, &token, &sql, timeout_secs))
        .await
        .context("Coinbase SQL curl task failed to join")?
}

fn run_blocking(url: &str, token: &str, sql: &str, timeout_secs: u64) -> Result<HttpResponse> {
    let id = Uuid::new_v4().simple().to_string();
    let config = std::env::temp_dir().join(format!("bigname-coinbase-sql-{id}.curl"));
    let body = std::env::temp_dir().join(format!("bigname-coinbase-sql-{id}.json"));
    let result = run_with_files(url, token, sql, timeout_secs, &config, &body);
    let _ = fs::remove_file(config);
    let _ = fs::remove_file(body);
    result
}

fn run_with_files(
    url: &str,
    token: &str,
    sql: &str,
    timeout_secs: u64,
    config_path: &Path,
    body_path: &Path,
) -> Result<HttpResponse> {
    let mut config = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(config_path)
        .context("failed to create Coinbase SQL curl config")?;
    writeln!(config, "silent")?;
    writeln!(config, "show-error")?;
    writeln!(config, "request = \"POST\"")?;
    writeln!(config, "user-agent = \"bigname-ingest/0.1\"")?;
    writeln!(config, "url = \"{}\"", escape(url))?;
    writeln!(
        config,
        "header = \"Authorization: Bearer {}\"",
        escape(token)
    )?;
    writeln!(config, "header = \"Content-Type: application/json\"")?;
    drop(config);

    let request = serde_json::to_vec(&json!({ "sql": sql }))?;
    let mut child = Command::new("curl")
        .arg("--config")
        .arg(config_path)
        .arg("--data-binary")
        .arg("@-")
        .arg("--output")
        .arg(body_path)
        .arg("--write-out")
        .arg("%{http_code}")
        .arg("--max-time")
        .arg(timeout_secs.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn curl for Coinbase SQL")?;
    child
        .stdin
        .take()
        .context("failed to open Coinbase SQL curl stdin")?
        .write_all(&request)?;
    let output = child.wait_with_output()?;
    let status_text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let body = fs::read_to_string(body_path).unwrap_or_default();
    if !output.status.success() && status_text.is_empty() {
        bail!(
            "Coinbase SQL curl failed: {}",
            truncate_error_body(&String::from_utf8_lossy(&output.stderr))
        );
    }
    let status = StatusCode::from_u16(status_text.parse()?)?;
    Ok(HttpResponse { status, body })
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
