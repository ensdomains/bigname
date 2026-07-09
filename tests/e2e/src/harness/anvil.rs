use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::json;

use super::rpc::RpcClient;

/// Fixed genesis timestamp so scenario time math is stable across runs.
pub const GENESIS_TIMESTAMP: u64 = 1_750_000_000;

/// A local anvil instance presented to the indexer under a provider label.
/// Chain admission is keyed by the provider label, not the numeric chain id,
/// but we still run realistic local ids so transaction receipts look familiar.
pub struct Anvil {
    child: Child,
    pub url: String,
}

impl Anvil {
    pub async fn spawn() -> Result<Self> {
        Self::spawn_with_chain_id(1).await
    }

    pub async fn spawn_base_mainnet() -> Result<Self> {
        Self::spawn_with_chain_id(8453).await
    }

    async fn spawn_with_chain_id(chain_id: u64) -> Result<Self> {
        let port = free_port()?;
        let url = format!("http://127.0.0.1:{port}");
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
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to spawn anvil; is foundry installed?")?;
        let instance = Self { child, url };
        instance.wait_ready().await?;
        Ok(instance)
    }

    pub fn client(&self) -> RpcClient {
        RpcClient::new(self.url.clone())
    }

    async fn wait_ready(&self) -> Result<()> {
        let client = self.client();
        for _ in 0..100 {
            if client.call("eth_chainId", json!([])).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        bail!("anvil did not become ready within 10s at {}", self.url)
    }
}

impl Drop for Anvil {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind for free port")?;
    Ok(listener.local_addr()?.port())
}
