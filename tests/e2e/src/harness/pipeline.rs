use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::process::{Child, Command};

/// The pipeline binaries run as real subprocesses via `cargo run`, sharing
/// the root workspace target dir. `CARGO_TARGET_DIR` is left alone so repeat
/// runs reuse the build cache.
fn cargo() -> Command {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    Command::new(cargo)
}

async fn run_to_completion(mut command: Command, what: &str) -> Result<String> {
    let output = command
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("spawn {what}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{what} failed ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status
        );
    }
    Ok(stdout)
}

pub async fn indexer_backfill(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    chain_rpc_url: &str,
    from_block: u64,
    to_block: u64,
    idempotency_key: &str,
) -> Result<String> {
    let mut command = cargo();
    command
        .current_dir(repo_root)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            "apps/indexer/Cargo.toml",
            "--",
            "backfill",
            "--database-url",
            database_url,
            "--manifests-root",
        ])
        .arg(manifests_root)
        .args([
            "--chain-rpc-url",
            &format!("ethereum-mainnet={chain_rpc_url}"),
            "--chain",
            "ethereum-mainnet",
            "--from-block",
            &from_block.to_string(),
            "--to-block",
            &to_block.to_string(),
            "--idempotency-key",
            idempotency_key,
        ]);
    run_to_completion(command, "indexer backfill").await
}

/// Live intake session: the real `indexer run` poll loop against the local
/// chain. Unlike `backfill`, this is the path that promotes canonical chain
/// checkpoints, which snapshot-selecting API reads require. The process is
/// killed once the canonical checkpoint reaches the target block and the
/// caller's readiness query (a parameterless SQL statement returning one
/// boolean) is true — the latter guards against stopping intake before
/// adapters finish deriving the scenario's normalized events.
pub async fn indexer_run_until_checkpoint(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_url: &str,
    target_block: u64,
    extra_ready_sql: Option<&str>,
) -> Result<()> {
    let mut child = cargo()
        .current_dir(repo_root)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            "apps/indexer/Cargo.toml",
            "--",
            "run",
            "--database-url",
            database_url,
            "--manifests-root",
        ])
        .arg(manifests_root)
        .args([
            "--chain-rpc-url",
            &format!("ethereum-mainnet={chain_rpc_url}"),
            "--poll-interval-secs",
            "1",
        ])
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawn indexer run")?;

    // First iterations may sit behind a cargo build of the indexer crate.
    let deadline = std::time::Instant::now() + Duration::from_secs(600);
    loop {
        if let Some(status) = child.try_wait()? {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                use tokio::io::AsyncReadExt;
                let _ = pipe.read_to_string(&mut stderr).await;
            }
            bail!("indexer run exited early ({status}):\n{stderr}");
        }
        let checkpoint: Option<i64> = sqlx::query_scalar(
            "SELECT canonical_block_number FROM chain_checkpoints WHERE chain_id = 'ethereum-mainnet'",
        )
        .fetch_optional(pool)
        .await?
        .flatten();
        if checkpoint.is_some_and(|block| block >= target_block as i64) {
            let extra_ready = match extra_ready_sql {
                Some(sql) => sqlx::query_scalar::<_, bool>(sql).fetch_one(pool).await?,
                None => true,
            };
            if extra_ready {
                child.kill().await.ok();
                return Ok(());
            }
        }
        if std::time::Instant::now() > deadline {
            child.kill().await.ok();
            bail!("indexer run did not reach canonical checkpoint {target_block} within 600s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub async fn worker_replay_all_current_projections(
    repo_root: &Path,
    database_url: &str,
) -> Result<String> {
    let mut command = cargo();
    command.current_dir(repo_root).args([
        "run",
        "--quiet",
        "--manifest-path",
        "apps/worker/Cargo.toml",
        "--",
        "replay",
        "all-current-projections",
        "--database-url",
        database_url,
    ]);
    run_to_completion(command, "worker replay all-current-projections").await
}

/// The shipped API binary serving HTTP on a local port; killed on drop.
pub struct ApiServer {
    _child: Child,
    pub base_url: String,
    http: reqwest::Client,
}

impl ApiServer {
    pub async fn start(repo_root: &Path, database_url: &str) -> Result<Self> {
        let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let bind_addr = format!("127.0.0.1:{port}");
        let child = cargo()
            .current_dir(repo_root)
            .args([
                "run",
                "--quiet",
                "--manifest-path",
                "apps/api/Cargo.toml",
                "--",
                "serve",
                "--bind-addr",
                &bind_addr,
                "--database-url",
                database_url,
            ])
            .kill_on_drop(true)
            .spawn()
            .context("spawn bigname-api serve")?;
        let server = Self {
            _child: child,
            base_url: format!("http://{bind_addr}"),
            http: reqwest::Client::new(),
        };
        server.wait_healthy().await?;
        Ok(server)
    }

    async fn wait_healthy(&self) -> Result<()> {
        // First readiness poll may sit behind a cargo build of the API crate.
        for _ in 0..1200 {
            if let Ok(response) = self
                .http
                .get(format!("{}/healthz", self.base_url))
                .send()
                .await
                && response.status().is_success()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        bail!("API did not become healthy at {}", self.base_url)
    }

    pub async fn get_json(&self, path: &str) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let response = self
            .http
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .with_context(|| format!("GET {path}"))?;
        let status = response.status();
        let body = response
            .json()
            .await
            .with_context(|| format!("GET {path} body"))?;
        Ok((status, body))
    }
}
