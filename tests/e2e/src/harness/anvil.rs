use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};

use super::pipeline::{await_with_readiness_deadline, deadline_after, ready_timeout_secs};
use super::rpc::RpcClient;

static ANVIL_SEQ: AtomicU64 = AtomicU64::new(0);
const SPAWN_ATTEMPTS: usize = 5;

/// Fixed genesis timestamp so scenario time math is stable across runs.
pub const GENESIS_TIMESTAMP: u64 = 1_750_000_000;

/// A local anvil instance presented to the indexer under a provider label.
/// Chain admission is keyed by the provider label, not the numeric chain id,
/// but we still run realistic local ids so transaction receipts look familiar.
pub struct Anvil {
    child: Child,
    pub url: String,
    log_path: PathBuf,
}

impl Anvil {
    pub async fn spawn() -> Result<Self> {
        Self::spawn_with_chain_id(1).await
    }

    pub async fn spawn_base_mainnet() -> Result<Self> {
        Self::spawn_with_chain_id(8453).await
    }

    pub async fn spawn_ethereum_sepolia() -> Result<Self> {
        Self::spawn_with_chain_id(11155111).await
    }

    async fn spawn_with_chain_id(chain_id: u64) -> Result<Self> {
        let ready_timeout_secs = ready_timeout_secs()?;
        let deadline = deadline_after(ready_timeout_secs, "anvil readiness")?;
        let mut last_error = None;
        for attempt in 1..=SPAWN_ATTEMPTS {
            match Self::try_spawn_until_bound(chain_id, deadline, ready_timeout_secs).await {
                Ok(mut instance) => match instance
                    .wait_ready(chain_id, deadline, ready_timeout_secs)
                    .await
                {
                    Ok(()) => return Ok(instance),
                    Err(error) => {
                        last_error = Some(error.context(format!(
                            "anvil chain {chain_id} startup attempt {attempt}/{SPAWN_ATTEMPTS} failed after binding"
                        )));
                    }
                },
                Err(error) => {
                    last_error = Some(error.context(format!(
                        "anvil chain {chain_id} startup attempt {attempt}/{SPAWN_ATTEMPTS} failed"
                    )));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        let error = last_error.expect("at least one anvil startup attempt ran");
        if tokio::time::Instant::now() >= deadline {
            return Err(error.context(format!(
                "anvil chain {chain_id} did not become ready within the configured {ready_timeout_secs}s readiness deadline"
            )));
        }
        Err(error)
    }

    async fn try_spawn_until_bound(
        chain_id: u64,
        deadline: tokio::time::Instant,
        ready_timeout_secs: u64,
    ) -> Result<Self> {
        // Serialize only the probe-to-bind window. Once this child's listener
        // is observable, RPC readiness and any retry can proceed without
        // blocking unrelated Anvil or API starts.
        let _startup_guard = await_with_readiness_deadline(
            deadline,
            ready_timeout_secs,
            format!("anvil chain {chain_id} local-server startup lock wait"),
            super::lock_local_server_start(),
        )
        .await?;
        let port = free_port()?;
        let url = format!("http://127.0.0.1:{port}");
        let (log_path, log_file) = create_anvil_log_file(chain_id)?;
        let child = Command::new("anvil")
            .args([
                "--port",
                &port.to_string(),
                "--chain-id",
                &chain_id.to_string(),
                "--timestamp",
                &GENESIS_TIMESTAMP.to_string(),
                "--silent",
            ])
            .stdout(Stdio::from(log_file.try_clone()?))
            .stderr(Stdio::from(log_file))
            .spawn()
            .context("failed to spawn anvil; is foundry installed?")?;
        let mut instance = Self {
            child,
            url,
            log_path,
        };
        instance
            .wait_until_listener_bound(port, deadline, ready_timeout_secs)
            .await?;
        Ok(instance)
    }

    pub fn client(&self) -> RpcClient {
        RpcClient::new(self.url.clone())
    }

    async fn wait_until_listener_bound(
        &mut self,
        port: u16,
        deadline: tokio::time::Instant,
        ready_timeout_secs: u64,
    ) -> Result<()> {
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        loop {
            self.bail_if_exited()?;
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(self.readiness_deadline_error(
                    ready_timeout_secs,
                    "waiting for its listener to bind",
                ));
            }
            let connect_timeout = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(20));
            if std::net::TcpStream::connect_timeout(&address, connect_timeout).is_ok() {
                self.bail_if_exited()?;
                return Ok(());
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(10)),
            )
            .await;
        }
    }

    async fn wait_ready(
        &mut self,
        expected_chain_id: u64,
        deadline: tokio::time::Instant,
        ready_timeout_secs: u64,
    ) -> Result<()> {
        let client = self.client();
        let mut consecutive_matches = 0_u8;
        loop {
            self.bail_if_exited()?;
            if tokio::time::Instant::now() >= deadline {
                return Err(
                    self.readiness_deadline_error(ready_timeout_secs, "waiting for RPC readiness")
                );
            }
            let chain_id = await_with_readiness_deadline(
                deadline,
                ready_timeout_secs,
                format!("anvil chain-id RPC at {}", self.url),
                client.chain_id(),
            )
            .await
            .with_context(|| {
                format!(
                    "anvil log tail from {:?}:\n{}",
                    self.log_path,
                    self.log_tail()
                )
            })?;
            match chain_id {
                Ok(actual_chain_id) if actual_chain_id == expected_chain_id => {
                    // Harness starts are serialized, but an external process
                    // can still take the selected port after free_port releases
                    // it. Do not accept its endpoint after our child exits.
                    self.bail_if_exited()?;
                    consecutive_matches += 1;
                    if consecutive_matches >= 2 {
                        return Ok(());
                    }
                }
                Ok(actual_chain_id) => {
                    bail!(
                        "anvil at {} returned chain id {actual_chain_id}, expected {expected_chain_id}; log tail from {:?}:\n{}",
                        self.url,
                        self.log_path,
                        self.log_tail()
                    );
                }
                Err(_) => consecutive_matches = 0,
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(100)),
            )
            .await;
        }
    }

    fn readiness_deadline_error(&self, ready_timeout_secs: u64, phase: &str) -> anyhow::Error {
        anyhow!(
            "anvil did not become ready at {} within the configured {ready_timeout_secs}s readiness deadline while {phase}; log tail from {:?}:\n{}",
            self.url,
            self.log_path,
            self.log_tail()
        )
    }

    fn bail_if_exited(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "anvil exited before readiness ({status}); log tail from {:?}:\n{}",
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

impl Drop for Anvil {
    fn drop(&mut self) {
        let exited_before_drop = match self.child.try_wait() {
            Ok(Some(status)) => {
                eprintln!(
                    "anvil at {} exited before harness drop ({status}); retaining log {:?}",
                    self.url, self.log_path
                );
                true
            }
            Ok(None) => false,
            Err(error) => {
                eprintln!(
                    "failed to inspect anvil at {} during drop ({error}); retaining log {:?}",
                    self.url, self.log_path
                );
                true
            }
        };
        if !exited_before_drop {
            let killed = self.child.kill().is_ok();
            let reaped = self.child.wait().is_ok();
            if killed && reaped {
                std::fs::remove_file(&self.log_path).ok();
            }
        }
    }
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind for free port")?;
    Ok(listener.local_addr()?.port())
}

fn create_anvil_log_file(chain_id: u64) -> Result<(PathBuf, std::fs::File)> {
    for _ in 0..1000 {
        let sequence = ANVIL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bigname-e2e-anvil-{}-{chain_id}-{sequence}.log",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("create anvil log file at {path:?}"));
            }
        }
    }
    anyhow::bail!("could not allocate a unique anvil log path")
}
