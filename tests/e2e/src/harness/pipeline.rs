use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::process::{Child, Command};

const PIPELINE_BINARY_SPECS: [(&str, &str); 3] = [
    ("bigname-api", "apps/api/Cargo.toml"),
    ("bigname-indexer", "apps/indexer/Cargo.toml"),
    ("bigname-worker", "apps/worker/Cargo.toml"),
];

#[derive(Debug)]
struct PipelineBinaries {
    api: PathBuf,
    indexer: PathBuf,
    worker: PathBuf,
}

static PIPELINE_BINARIES: OnceLock<std::result::Result<PipelineBinaries, String>> = OnceLock::new();

/// Build the three pipeline binaries once, then launch the exact artifacts
/// Cargo reports. Direct launches keep later scenario startup independent of
/// the workspace target lock while still honoring its effective target dir.
fn pipeline_binaries(repo_root: &Path) -> Result<&'static PipelineBinaries> {
    match PIPELINE_BINARIES
        .get_or_init(|| build_pipeline_binaries(repo_root).map_err(|error| format!("{error:#}")))
    {
        Ok(binaries) => Ok(binaries),
        Err(error) => bail!("{error}"),
    }
}

fn build_pipeline_binaries(repo_root: &Path) -> Result<PipelineBinaries> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = std::process::Command::new(cargo)
        .current_dir(repo_root)
        .args([
            "build",
            "--locked",
            "--message-format=json-render-diagnostics",
            "--package",
            "bigname-api",
            "--package",
            "bigname-indexer",
            "--package",
            "bigname-worker",
            "--bins",
        ])
        .output()
        .context("spawn one-time pipeline binary build")?;

    if !output.status.success() {
        bail!(
            "pipeline binary build failed ({}):\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    parse_pipeline_binaries(repo_root, &output.stdout)
}

fn normalize_cargo_path(repo_root: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn parse_pipeline_binaries(repo_root: &Path, stdout: &[u8]) -> Result<PipelineBinaries> {
    let mut api = None;
    let mut indexer = None;
    let mut worker = None;

    for (line_index, line) in stdout.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        let message: Value = serde_json::from_slice(line).with_context(|| {
            format!("parse Cargo JSON message on stdout line {}", line_index + 1)
        })?;
        if message.get("reason").and_then(Value::as_str) != Some("compiler-artifact") {
            continue;
        }
        let Some(target) = message.get("target") else {
            continue;
        };
        let is_binary = target
            .get("kind")
            .and_then(Value::as_array)
            .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some("bin")));
        if !is_binary {
            continue;
        }
        let Some(name) = target.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(manifest_path) = message.get("manifest_path").and_then(Value::as_str) else {
            continue;
        };
        let manifest_path = normalize_cargo_path(repo_root, manifest_path);
        let Some((_, expected_manifest)) = PIPELINE_BINARY_SPECS
            .iter()
            .find(|(expected, path)| name == *expected && manifest_path == repo_root.join(*path))
        else {
            continue;
        };

        let executable = match message.get("executable") {
            Some(Value::String(path)) => normalize_cargo_path(repo_root, path),
            Some(Value::Null) => {
                bail!("Cargo reported a null executable for {name} from {expected_manifest}")
            }
            Some(_) => {
                bail!("Cargo reported a non-string executable for {name} from {expected_manifest}")
            }
            None => bail!("Cargo omitted the executable for {name} from {expected_manifest}"),
        };

        let slot = match name {
            "bigname-api" => &mut api,
            "bigname-indexer" => &mut indexer,
            "bigname-worker" => &mut worker,
            _ => unreachable!("pipeline binary specification matched an unknown name"),
        };
        if slot.replace(executable).is_some() {
            bail!("Cargo reported duplicate executable artifacts for {name}");
        }
    }

    let mut missing = Vec::new();
    if api.is_none() {
        missing.push("bigname-api");
    }
    if indexer.is_none() {
        missing.push("bigname-indexer");
    }
    if worker.is_none() {
        missing.push("bigname-worker");
    }
    if !missing.is_empty() {
        bail!(
            "Cargo build did not report executable artifacts for {}",
            missing.join(", ")
        );
    }

    Ok(PipelineBinaries {
        api: api.expect("checked above"),
        indexer: indexer.expect("checked above"),
        worker: worker.expect("checked above"),
    })
}

fn pipeline_command(repo_root: &Path, executable: &Path) -> Command {
    let mut command = Command::new(executable);
    command.current_dir(repo_root);
    // E2e corpora are tiny, but every spawned binary defaults to a
    // 10-connection pool; under suite parallelism that exhausts the shared
    // test postgres (max_connections 100) and surfaces as pool-acquire
    // timeouts in unrelated tests.
    command.env("BIGNAME_DATABASE_MAX_CONNECTIONS", "4");
    command
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

fn worker_log_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "bigname-e2e-worker-{}-{label}.log",
        std::process::id()
    ))
}

fn process_log_tail(log_path: &Path) -> String {
    let log = std::fs::read_to_string(log_path).unwrap_or_default();
    log.lines().rev().take(60).collect::<Vec<_>>().join("\n")
}

async fn stop_supervised_child(child: Child, what: &str, log_path: &Path) -> Result<()> {
    stop_supervised_child_with_pre_kill_delay(child, what, log_path, None).await
}

async fn stop_supervised_child_with_pre_kill_delay(
    mut child: Child,
    what: &str,
    log_path: &Path,
    pre_kill_delay: Option<Duration>,
) -> Result<()> {
    if let Some(status) = child.try_wait()? {
        bail!(
            "{what} exited before requested stop ({status}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }

    if let Some(delay) = pre_kill_delay {
        tokio::time::sleep(delay).await;
    }
    if let Err(kill_error) = child.start_kill() {
        let status = child
            .wait()
            .await
            .with_context(|| format!("failed to reap {what} after stop failed"))?;
        bail!(
            "{what} exited while stop was requested ({status}; stop error: {kill_error}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }
    let status = child
        .wait()
        .await
        .with_context(|| format!("failed to reap {what} after stop"))?;
    if !exited_from_requested_kill(&status) {
        bail!(
            "{what} exited independently while stop was requested ({status}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }
    Ok(())
}

fn exited_from_requested_kill(status: &std::process::ExitStatus) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        // Tokio's start_kill is SIGKILL on Unix. An ordinary nonzero exit or
        // another fatal signal therefore remains distinguishable from the
        // harness-requested stop even if it races with start_kill.
        status.signal() == Some(9)
    }

    #[cfg(not(unix))]
    {
        !status.success()
    }
}

pub type ChainRpcUrl<'a> = (&'a str, &'a str);

pub struct ChainCheckpointTarget<'a> {
    pub chain_rpc_urls: &'a [ChainRpcUrl<'a>],
    pub chain: &'a str,
    pub target_block: u64,
    pub extra_ready_sql: Option<&'a str>,
}

pub struct ChainBackfillTarget<'a> {
    pub chain_rpc_urls: &'a [ChainRpcUrl<'a>],
    pub chain: &'a str,
    pub block_range: std::ops::RangeInclusive<u64>,
    pub idempotency_key: &'a str,
}

fn format_chain_rpc_urls(chain_rpc_urls: &[ChainRpcUrl<'_>]) -> String {
    chain_rpc_urls
        .iter()
        .map(|(chain, url)| format!("{chain}={url}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// A supervised `indexer run` process. The caller decides when the live session
/// has reached enough checkpoints/readiness and then stops it explicitly.
pub struct IndexerRunSession {
    child: Child,
    log_path: PathBuf,
}

/// A supervised production `worker run` process. Most scenarios use the
/// deterministic one-shot projection replay command; this session exists for
/// the smaller set that must prove bootstrap handoff and continuous
/// invalidation/apply behavior while intake and the API remain live.
pub struct WorkerRunSession {
    child: Child,
    log_path: PathBuf,
}

impl WorkerRunSession {
    pub fn start(repo_root: &Path, database_url: &str, log_label: &str) -> Result<Self> {
        let worker = &pipeline_binaries(repo_root)?.worker;
        let log_path = worker_log_path(log_label);
        let log_file = std::fs::File::create(&log_path).context("create worker log file")?;
        let child = pipeline_command(repo_root, worker)
            .args([
                "run",
                "--database-url",
                database_url,
                "--poll-interval-secs",
                "1",
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()
            .context("spawn worker run")?;
        Ok(Self { child, log_path })
    }

    pub async fn wait_for_sql(&mut self, pool: &sqlx::PgPool, sql: &str) -> Result<()> {
        let ready_timeout_secs = std::env::var("BIGNAME_E2E_READY_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(600);
        let deadline = std::time::Instant::now() + Duration::from_secs(ready_timeout_secs);
        loop {
            self.bail_if_exited()?;
            if sqlx::query_scalar::<_, bool>(sql).fetch_one(pool).await? {
                return Ok(());
            }
            if std::time::Instant::now() > deadline {
                self.child.kill().await.ok();
                bail!(
                    "worker run did not satisfy readiness SQL within {ready_timeout_secs}s; log tail (reversed) from {:?}:\n{}",
                    self.log_path,
                    self.log_tail()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn stop(self) -> Result<()> {
        stop_supervised_child(self.child, "worker run", &self.log_path).await
    }

    fn bail_if_exited(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "worker run exited early ({status}); log tail (reversed) from {:?}:\n{}",
                self.log_path,
                self.log_tail()
            );
        }
        Ok(())
    }

    fn log_tail(&self) -> String {
        process_log_tail(&self.log_path)
    }
}

impl IndexerRunSession {
    pub fn start(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_url: &str,
        log_label: &str,
    ) -> Result<Self> {
        Self::start_with_chain_rpc_urls(
            repo_root,
            database_url,
            manifests_root,
            &[("ethereum-mainnet", chain_rpc_url)],
            log_label,
        )
    }

    pub fn start_with_chain_rpc_urls(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
        log_label: &str,
    ) -> Result<Self> {
        Self::start_with_chain_rpc_urls_and_adapter_sync_mode(
            repo_root,
            database_url,
            manifests_root,
            chain_rpc_urls,
            log_label,
            None,
        )
    }

    /// Start the production loop with live poll adapter sync enabled from the
    /// first poll. This keeps automatic normalized replay catch-up out of the
    /// assertion path so a test cannot pass after a later repair cycle.
    pub fn start_with_live_poll_adapter_sync(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
        log_label: &str,
    ) -> Result<Self> {
        Self::start_with_chain_rpc_urls_and_adapter_sync_mode(
            repo_root,
            database_url,
            manifests_root,
            chain_rpc_urls,
            log_label,
            Some("auto"),
        )
    }

    fn start_with_chain_rpc_urls_and_adapter_sync_mode(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
        log_label: &str,
        adapter_sync_mode: Option<&str>,
    ) -> Result<Self> {
        let indexer = &pipeline_binaries(repo_root)?.indexer;
        // Output goes to a log file, not a pipe: the run loop can out-write an
        // undrained pipe buffer and block, deadlocking the session.
        let log_path = indexer_log_path(log_label);
        let log_file = std::fs::File::create(&log_path).context("create indexer log file")?;
        let chain_rpc_urls = format_chain_rpc_urls(chain_rpc_urls);
        let mut command = pipeline_command(repo_root, indexer);
        command
            .args(["run", "--database-url", database_url, "--manifests-root"])
            .arg(manifests_root)
            .args([
                "--chain-rpc-url",
                &chain_rpc_urls,
                "--poll-interval-secs",
                "1",
                // Scenario readiness often waits on a full-closure authority
                // sync round; the default 30s cadence just slows tests down.
                "--normalized-replay-catchup-poll-interval-secs",
                "2",
            ]);
        if let Some(adapter_sync_mode) = adapter_sync_mode {
            command.args(["--hash-pinned-adapter-sync", adapter_sync_mode]);
            command.env("BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_ENABLED", "false");
        }
        let child = command
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()
            .context("spawn indexer run")?;

        Ok(Self { child, log_path })
    }

    pub async fn wait_for_first_checkpoint(&mut self, pool: &sqlx::PgPool) -> Result<i64> {
        self.wait_for_first_chain_checkpoint(pool, "ethereum-mainnet")
            .await
    }

    pub async fn wait_for_first_chain_checkpoint(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
    ) -> Result<i64> {
        let deadline = std::time::Instant::now() + Duration::from_secs(600);
        loop {
            self.bail_if_exited()?;
            let checkpoint = canonical_checkpoint(pool, chain).await?;
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
        self.wait_for_chain_checkpoint(pool, "ethereum-mainnet", target_block, extra_ready_sql)
            .await
    }

    pub async fn wait_for_chain_checkpoint(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
        target_block: u64,
        extra_ready_sql: Option<&str>,
    ) -> Result<()> {
        // The binary is built before the session starts, so this deadline
        // measures intake readiness rather than workspace build-lock waits.
        let ready_timeout_secs = std::env::var("BIGNAME_E2E_READY_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(600);
        let deadline = std::time::Instant::now() + Duration::from_secs(ready_timeout_secs);
        loop {
            self.bail_if_exited()?;
            let checkpoint = canonical_checkpoint(pool, chain).await?;
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

    pub async fn stop(self) -> Result<()> {
        stop_supervised_child(self.child, "indexer run", &self.log_path).await
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
        process_log_tail(&self.log_path)
    }
}

async fn canonical_checkpoint(pool: &sqlx::PgPool, chain: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT canonical_block_number FROM chain_checkpoints WHERE chain_id = $1",
    )
    .bind(chain)
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
    let chain_rpc_urls = [("ethereum-mainnet", chain_rpc_url)];
    indexer_backfill_with_chain_rpc_urls(
        repo_root,
        database_url,
        manifests_root,
        ChainBackfillTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: "ethereum-mainnet",
            block_range: from_block..=to_block,
            idempotency_key,
        },
    )
    .await
}

pub async fn indexer_backfill_with_chain_rpc_urls(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    target: ChainBackfillTarget<'_>,
) -> Result<String> {
    let indexer = &pipeline_binaries(repo_root)?.indexer;
    let chain_rpc_urls = format_chain_rpc_urls(target.chain_rpc_urls);
    let from_block = target.block_range.start().to_string();
    let to_block = target.block_range.end().to_string();
    let mut command = pipeline_command(repo_root, indexer);
    command
        .args([
            "backfill",
            "--database-url",
            database_url,
            "--manifests-root",
        ])
        .arg(manifests_root)
        .args([
            "--chain-rpc-url",
            &chain_rpc_urls,
            "--chain",
            target.chain,
            "--from-block",
            &from_block,
            "--to-block",
            &to_block,
            "--idempotency-key",
            target.idempotency_key,
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
    let chain_rpc_urls = [("ethereum-mainnet", chain_rpc_url)];
    indexer_run_until_chain_checkpoint(
        repo_root,
        database_url,
        pool,
        manifests_root,
        ChainCheckpointTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: "ethereum-mainnet",
            target_block,
            extra_ready_sql,
        },
    )
    .await
}

pub async fn indexer_run_until_chain_checkpoint(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    target: ChainCheckpointTarget<'_>,
) -> Result<()> {
    let mut session = IndexerRunSession::start_with_chain_rpc_urls(
        repo_root,
        database_url,
        manifests_root,
        target.chain_rpc_urls,
        &target.target_block.to_string(),
    )?;
    session
        .wait_for_chain_checkpoint(
            pool,
            target.chain,
            target.target_block,
            target.extra_ready_sql,
        )
        .await?;
    session.stop().await
}

/// Run one live session over multiple chains and wait each chain's
/// canonical checkpoint sequentially; `extra_ready_sql` gates the final
/// wait.
pub async fn indexer_run_until_chain_checkpoints(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_urls: &[ChainRpcUrl<'_>],
    targets: &[(&str, u64)],
    extra_ready_sql: Option<&str>,
) -> Result<()> {
    let label = targets
        .iter()
        .map(|(_, block)| block.to_string())
        .collect::<Vec<_>>()
        .join("-");
    let mut session = IndexerRunSession::start_with_chain_rpc_urls(
        repo_root,
        database_url,
        manifests_root,
        chain_rpc_urls,
        &label,
    )?;
    for (index, (chain, target_block)) in targets.iter().enumerate() {
        let extra = if index + 1 == targets.len() {
            extra_ready_sql
        } else {
            None
        };
        session
            .wait_for_chain_checkpoint(pool, chain, *target_block, extra)
            .await?;
    }
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
    let indexer = &pipeline_binaries(repo_root)?.indexer;
    let mut command = pipeline_command(repo_root, indexer);
    command.args([
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
    let worker = &pipeline_binaries(repo_root)?.worker;
    let mut command = pipeline_command(repo_root, worker);
    command.args([
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
        Self::start_with_chain_rpc_urls(repo_root, database_url, &[]).await
    }

    pub async fn start_with_chain_rpc_urls(
        repo_root: &Path,
        database_url: &str,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
    ) -> Result<Self> {
        let api = &pipeline_binaries(repo_root)?.api;
        let _startup_guard = super::lock_local_server_start().await;
        // The free port is released before the API binds it, so a parallel
        // external process can still steal it in the window. Harness-managed
        // Anvil/API starts share the startup lock; retry if an external bind
        // wins and the child dies instead of becoming healthy.
        let mut last_error = None;
        for _ in 0..3 {
            match Self::try_start(repo_root, api, database_url, chain_rpc_urls).await {
                Ok(server) => return Ok(server),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("at least one API start attempt ran"))
    }

    async fn try_start(
        repo_root: &Path,
        api: &Path,
        database_url: &str,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
    ) -> Result<Self> {
        let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let bind_addr = format!("127.0.0.1:{port}");
        let mut command = pipeline_command(repo_root, api);
        command.args([
            "serve",
            "--bind-addr",
            &bind_addr,
            "--database-url",
            database_url,
        ]);
        if !chain_rpc_urls.is_empty() {
            let chain_rpc_urls = format_chain_rpc_urls(chain_rpc_urls);
            command.args(["--chain-rpc-url", &chain_rpc_urls]);
        }
        let child = command
            .kill_on_drop(true)
            .spawn()
            .context("spawn bigname-api serve")?;
        let mut server = Self {
            _child: child,
            base_url: format!("http://{bind_addr}"),
            http: reqwest::Client::new(),
        };
        server.wait_healthy().await?;
        Ok(server)
    }

    async fn wait_healthy(&mut self) -> Result<()> {
        // The binary is built before startup, so this loop measures process
        // health rather than workspace build-lock waits.
        for _ in 0..1200 {
            if let Some(status) = self._child.try_wait()? {
                bail!(
                    "API exited before becoming healthy at {} ({status})",
                    self.base_url
                );
            }
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

    pub async fn post_json(
        &self,
        path: &str,
        request: &serde_json::Value,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .json(request)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        let status = response.status();
        let body = response
            .json()
            .await
            .with_context(|| format!("POST {path} body"))?;
        Ok((status, body))
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::*;

    fn artifact_message(name: &str, manifest_path: &Path, executable: Option<Value>) -> String {
        let mut message = serde_json::json!({
            "reason": "compiler-artifact",
            "manifest_path": manifest_path,
            "target": {
                "kind": ["bin"],
                "name": name,
            },
        });
        if let Some(executable) = executable {
            message["executable"] = executable;
        }
        message.to_string()
    }

    fn expected_manifest(repo_root: &Path, name: &str) -> PathBuf {
        let (_, manifest) = PIPELINE_BINARY_SPECS
            .iter()
            .find(|(expected, _)| *expected == name)
            .expect("known pipeline binary");
        repo_root.join(*manifest)
    }

    #[test]
    fn parses_exact_pipeline_artifacts_and_normalizes_relative_executables() -> Result<()> {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-root");
        let custom_target = std::env::temp_dir()
            .join("custom target")
            .join("aarch64-unknown-linux-gnu")
            .join("debug");
        let api = custom_target.join("bigname-api");
        let indexer = custom_target.join("bigname-indexer");
        let relative_worker = PathBuf::from("relative-target/debug/bigname-worker");
        let messages = [
            // An exact binary name from another manifest must not be selected.
            artifact_message(
                "bigname-api",
                &repo_root.join("decoy/Cargo.toml"),
                Some(Value::Null),
            ),
            artifact_message(
                "bigname-api",
                &expected_manifest(&repo_root, "bigname-api"),
                Some(Value::String(api.to_string_lossy().into_owned())),
            ),
            artifact_message(
                "bigname-indexer",
                &expected_manifest(&repo_root, "bigname-indexer"),
                Some(Value::String(indexer.to_string_lossy().into_owned())),
            ),
            artifact_message(
                "bigname-worker",
                &expected_manifest(&repo_root, "bigname-worker"),
                Some(Value::String(
                    relative_worker.to_string_lossy().into_owned(),
                )),
            ),
            serde_json::json!({"reason": "build-finished", "success": true}).to_string(),
        ]
        .join("\n");

        let binaries = parse_pipeline_binaries(&repo_root, messages.as_bytes())?;
        assert_eq!(binaries.api, api);
        assert_eq!(binaries.indexer, indexer);
        assert_eq!(binaries.worker, repo_root.join(relative_worker));
        Ok(())
    }

    #[test]
    fn rejects_missing_pipeline_artifact() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-missing");
        let messages = [
            artifact_message(
                "bigname-api",
                &expected_manifest(&repo_root, "bigname-api"),
                Some(Value::String("api".to_string())),
            ),
            artifact_message(
                "bigname-indexer",
                &expected_manifest(&repo_root, "bigname-indexer"),
                Some(Value::String("indexer".to_string())),
            ),
        ]
        .join("\n");

        let error = parse_pipeline_binaries(&repo_root, messages.as_bytes())
            .expect_err("a missing worker artifact must fail");
        assert!(format!("{error:#}").contains("bigname-worker"));
    }

    #[test]
    fn rejects_null_or_omitted_pipeline_executable() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-null");
        for (executable, expected) in [
            (Some(Value::Null), "null executable"),
            (None, "omitted the executable"),
        ] {
            let message = artifact_message(
                "bigname-api",
                &expected_manifest(&repo_root, "bigname-api"),
                executable,
            );
            let error = parse_pipeline_binaries(&repo_root, message.as_bytes())
                .expect_err("a missing executable path must fail");
            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn rejects_duplicate_pipeline_artifacts() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-duplicate");
        let api = artifact_message(
            "bigname-api",
            &expected_manifest(&repo_root, "bigname-api"),
            Some(Value::String("api".to_string())),
        );
        let messages = format!("{api}\n{api}");

        let error = parse_pipeline_binaries(&repo_root, messages.as_bytes())
            .expect_err("duplicate API artifacts must fail");
        assert!(format!("{error:#}").contains("duplicate executable artifacts for bigname-api"));
    }

    #[test]
    fn pipeline_command_uses_exact_binary_cwd_and_pool_limit() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-command-root");
        let executable = std::env::temp_dir()
            .join("custom target")
            .join("debug")
            .join("bigname-api");
        let command = pipeline_command(&repo_root, &executable);
        let command = command.as_std();

        assert_eq!(command.get_program(), executable.as_os_str());
        assert_eq!(command.get_current_dir(), Some(repo_root.as_path()));
        assert_eq!(command.get_args().count(), 0);
        assert_eq!(
            command
                .get_envs()
                .find(|(name, _)| *name == OsStr::new("BIGNAME_DATABASE_MAX_CONNECTIONS"))
                .and_then(|(_, value)| value),
            Some(OsStr::new("4"))
        );
    }

    #[tokio::test]
    async fn stop_reports_a_child_that_crashed_after_readiness() -> Result<()> {
        let log_path =
            std::env::temp_dir().join(format!("bigname-e2e-stop-crash-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path)?;
        let child = Command::new("sh")
            .args(["-c", "echo deliberate-child-crash >&2; exit 17"])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let error = stop_supervised_child(child, "test child", &log_path)
            .await
            .expect_err("an already-crashed child must make stop fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited before requested stop"),
            "{message}"
        );
        assert!(message.contains("deliberate-child-crash"), "{message}");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stop_reports_a_child_that_crashes_between_status_check_and_kill() -> Result<()> {
        let log_path =
            std::env::temp_dir().join(format!("bigname-e2e-stop-race-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path)?;
        let child = Command::new("sh")
            .args([
                "-c",
                "sleep 0.2; echo deliberate-stop-race-crash >&2; exit 17",
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;

        let error = stop_supervised_child_with_pre_kill_delay(
            child,
            "test child",
            &log_path,
            Some(Duration::from_millis(500)),
        )
        .await
        .expect_err("a child crash racing with stop must not be accepted as a requested kill");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited independently while stop was requested"),
            "{message}"
        );
        assert!(message.contains("deliberate-stop-race-crash"), "{message}");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }
}
