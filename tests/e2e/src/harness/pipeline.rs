use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
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

fn indexer_log_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "bigname-e2e-indexer-{}-{label}.log",
        std::process::id()
    ))
}

/// A supervised `indexer run` process. The caller decides when the live session
/// has reached enough checkpoints/readiness and then stops it explicitly.
pub struct IndexerRunSession {
    child: Child,
    log_path: PathBuf,
}

impl IndexerRunSession {
    pub fn start(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_url: &str,
        log_label: &str,
    ) -> Result<Self> {
        // Output goes to a log file, not a pipe: the run loop can out-write an
        // undrained pipe buffer and block, deadlocking the session.
        let log_path = indexer_log_path(log_label);
        let log_file = std::fs::File::create(&log_path).context("create indexer log file")?;
        let child = cargo()
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
                // Scenario readiness often waits on a full-closure authority
                // sync round; the default 30s cadence just slows tests down.
                "--normalized-replay-catchup-poll-interval-secs",
                "2",
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()
            .context("spawn indexer run")?;

        Ok(Self { child, log_path })
    }

    pub async fn wait_for_first_checkpoint(&mut self, pool: &sqlx::PgPool) -> Result<i64> {
        let deadline = std::time::Instant::now() + Duration::from_secs(600);
        loop {
            self.bail_if_exited()?;
            let checkpoint = canonical_checkpoint(pool).await?;
            if let Some(block) = checkpoint {
                return Ok(block);
            }
            self.bail_if_timed_out(deadline, "indexer run did not write a canonical checkpoint")
                .await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn wait_for_checkpoint(
        &mut self,
        pool: &sqlx::PgPool,
        target_block: u64,
        extra_ready_sql: Option<&str>,
    ) -> Result<()> {
        // First iterations may sit behind a cargo build of the indexer crate.
        let deadline = std::time::Instant::now() + Duration::from_secs(600);
        loop {
            self.bail_if_exited()?;
            let checkpoint = canonical_checkpoint(pool).await?;
            if checkpoint.is_some_and(|block| block >= target_block as i64) {
                let extra_ready = match extra_ready_sql {
                    Some(sql) => sqlx::query_scalar::<_, bool>(sql).fetch_one(pool).await?,
                    None => true,
                };
                if extra_ready {
                    return Ok(());
                }
            }
            self.bail_if_timed_out(
                deadline,
                &format!("indexer run did not reach canonical checkpoint {target_block}"),
            )
            .await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn stop(mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill().await.ok();
        }
        Ok(())
    }

    fn bail_if_exited(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "indexer run exited early ({status}); log tail (reversed) from {:?}:\n{}",
                self.log_path,
                self.log_tail()
            );
        }
        Ok(())
    }

    async fn bail_if_timed_out(
        &mut self,
        deadline: std::time::Instant,
        message: &str,
    ) -> Result<()> {
        if std::time::Instant::now() > deadline {
            self.child.kill().await.ok();
            bail!(
                "{message} within 600s; log tail (reversed) from {:?}:\n{}",
                self.log_path,
                self.log_tail()
            );
        }
        Ok(())
    }

    fn log_tail(&self) -> String {
        let log = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        log.lines().rev().take(60).collect::<Vec<_>>().join("\n")
    }
}

async fn canonical_checkpoint(pool: &sqlx::PgPool) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT canonical_block_number FROM chain_checkpoints WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_optional(pool)
    .await?
    .flatten())
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
    let mut session = IndexerRunSession::start(
        repo_root,
        database_url,
        manifests_root,
        chain_rpc_url,
        &target_block.to_string(),
    )?;
    session
        .wait_for_checkpoint(pool, target_block, extra_ready_sql)
        .await?;
    session.stop().await
}

pub struct RestartCompletion {
    pub target_block: u64,
    pub extra_ready_sql: Option<String>,
}

pub async fn indexer_run_restart_after_first_checkpoint<F, Fut>(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_url: &str,
    after_first_checkpoint: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<RestartCompletion>>,
{
    let mut first_session = IndexerRunSession::start(
        repo_root,
        database_url,
        manifests_root,
        chain_rpc_url,
        "restart-first",
    )?;
    first_session.wait_for_first_checkpoint(pool).await?;
    first_session.stop().await?;

    let completion = after_first_checkpoint().await?;
    indexer_run_until_checkpoint(
        repo_root,
        database_url,
        pool,
        manifests_root,
        chain_rpc_url,
        completion.target_block,
        completion.extra_ready_sql.as_deref(),
    )
    .await
}

/// Full-range normalized-event replay from stored raw facts — the operator
/// command that rebuilds adapter state under a complete closure boundary.
/// The active corpus registers under the `mainnet` profile (the generated
/// root mirrors the shipped mainnet manifests), so that is the profile the
/// replay must name.
pub async fn indexer_replay_normalized_events(
    repo_root: &Path,
    database_url: &str,
    to_block: u64,
) -> Result<String> {
    let mut command = cargo();
    command.current_dir(repo_root).args([
        "run",
        "--quiet",
        "--manifest-path",
        "apps/indexer/Cargo.toml",
        "--",
        "replay",
        "normalized-events",
        "--database-url",
        database_url,
        "--deployment-profile",
        "mainnet",
        "--chain",
        "ethereum-mainnet",
        "--from-block",
        "0",
        "--to-block",
        &to_block.to_string(),
    ]);
    run_to_completion(command, "indexer replay normalized-events").await
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
