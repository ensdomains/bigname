use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};

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
        // `free_port` must release its listener before Anvil can bind. Hold the
        // harness-wide startup lock through readiness so another local server
        // cannot take the selected port in that window.
        let _startup_guard = super::lock_local_server_start().await;
        let mut last_error = None;
        for attempt in 1..=SPAWN_ATTEMPTS {
            match Self::try_spawn_with_chain_id(chain_id).await {
                Ok(instance) => return Ok(instance),
                Err(error) => {
                    last_error = Some(error.context(format!(
                        "anvil chain {chain_id} startup attempt {attempt}/{SPAWN_ATTEMPTS} failed"
                    )));
                }
            }
        }
        Err(last_error.expect("at least one anvil startup attempt ran"))
    }

    async fn try_spawn_with_chain_id(chain_id: u64) -> Result<Self> {
        let port = free_port()?;
        let url = format!("http://127.0.0.1:{port}");
        let log_path = anvil_log_path(chain_id);
        let log_file = std::fs::File::create(&log_path)
            .with_context(|| format!("create anvil log file at {log_path:?}"))?;
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
        instance.wait_ready(chain_id).await?;
        Ok(instance)
    }

    pub fn client(&self) -> RpcClient {
        RpcClient::new(self.url.clone())
    }

    async fn wait_ready(&mut self, expected_chain_id: u64) -> Result<()> {
        let client = self.client();
        let mut consecutive_matches = 0_u8;
        for _ in 0..100 {
            self.bail_if_exited()?;
            match client.chain_id().await {
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
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        bail!(
            "anvil did not become ready within 10s at {}; log tail from {:?}:\n{}",
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

fn anvil_log_path(chain_id: u64) -> PathBuf {
    std::env::temp_dir().join(format!(
        "bigname-e2e-anvil-{}-{chain_id}-{}.log",
        std::process::id(),
        ANVIL_SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}
