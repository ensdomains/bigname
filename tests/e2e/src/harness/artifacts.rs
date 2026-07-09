use std::path::Path;

use alloy_primitives::{Address, U256, hex};
use anyhow::{Context, Result, anyhow};
use serde_json::Value;

use super::rpc::RpcClient;

/// Creation bytecode loaded from a pinned upstream hardhat-deploy artifact
/// under `.refs/ens_v1/deployments/<network>/<name>.json`. Deploying from
/// these artifacts means local test chains run the exact bytecode upstream
/// shipped, not a re-compilation.
pub struct Artifact {
    pub name: String,
    pub creation_code: Vec<u8>,
}

pub fn load_ens_v1_artifact(repo_root: &Path, network: &str, name: &str) -> Result<Artifact> {
    let path = repo_root
        .join(".refs/ens_v1/deployments")
        .join(network)
        .join(format!("{name}.json"));
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("missing pinned artifact {path:?}; run scripts/sync-refs"))?;
    let parsed: Value = serde_json::from_str(&raw).context("artifact json parse")?;
    let bytecode = parsed
        .get("bytecode")
        .and_then(Value::as_str)
        .filter(|code| code.len() > 2)
        .ok_or_else(|| anyhow!("artifact {name} on {network} has no creation bytecode"))?;
    Ok(Artifact {
        name: name.to_string(),
        creation_code: hex::decode(bytecode).context("artifact bytecode hex decode")?,
    })
}

pub struct Deployed {
    pub address: Address,
    pub block_number: u64,
}

pub async fn deploy(
    rpc: &RpcClient,
    from: Address,
    artifact: &Artifact,
    constructor_args: &[u8],
) -> Result<Deployed> {
    let mut payload = artifact.creation_code.clone();
    payload.extend_from_slice(constructor_args);
    let receipt = rpc
        .send_transaction(from, None, &payload, U256::ZERO)
        .await?;
    if !receipt.status_ok {
        anyhow::bail!(
            "deploy of {} reverted (tx {})",
            artifact.name,
            receipt.tx_hash
        );
    }
    let address = receipt
        .contract_address
        .ok_or_else(|| anyhow!("deploy of {} produced no contract address", artifact.name))?;
    Ok(Deployed {
        address,
        block_number: receipt.block_number,
    })
}
