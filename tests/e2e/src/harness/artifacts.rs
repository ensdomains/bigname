use std::path::Path;
use std::str::FromStr;

use alloy_primitives::{Address, B256, U256, hex};
use anyhow::{Context, Result, anyhow, bail};
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

/// Creation bytecode loaded from pinned ENSv2 hardhat-deploy artifacts under
/// `.refs/ens_v2/contracts/deployments/sepolia-dev/<name>.json`.
pub fn load_ens_v2_artifact(repo_root: &Path, name: &str) -> Result<Artifact> {
    let path = repo_root
        .join(".refs/ens_v2/contracts/deployments/sepolia-dev")
        .join(format!("{name}.json"));
    let raw = std::fs::read_to_string(&path).with_context(|| {
        format!("missing pinned ENSv2 artifact {path:?}; run scripts/sync-refs")
    })?;
    let parsed: Value = serde_json::from_str(&raw).context("ENSv2 artifact json parse")?;
    let bytecode = parsed
        .get("bytecode")
        .and_then(Value::as_str)
        .filter(|code| code.len() > 2)
        .ok_or_else(|| anyhow!("ENSv2 artifact {name} has no creation bytecode"))?;
    Ok(Artifact {
        name: format!("ens_v2:sepolia-dev:{name}"),
        creation_code: hex::decode(bytecode).context("ENSv2 artifact bytecode hex decode")?,
    })
}

/// Forge-built artifact from the pinned Basenames checkout. The committed
/// broadcast bytecode predates the pinned sources (its constructors differ),
/// so deployments build the pinned sources instead — the pin vendors every
/// forge lib, making `forge build` fully offline. Built at most once per
/// test process, on demand.
pub fn load_basenames_forge_artifact(repo_root: &Path, contract: &str) -> Result<Artifact> {
    ensure_basenames_built(repo_root)?;
    let path = repo_root
        .join(".refs/basenames/out")
        .join(format!("{contract}.sol"))
        .join(format!("{contract}.json"));
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("missing forge artifact {path:?}"))?;
    let parsed: Value = serde_json::from_str(&raw).context("forge artifact json parse")?;
    let bytecode = parsed
        .pointer("/bytecode/object")
        .and_then(Value::as_str)
        .filter(|code| code.len() > 2)
        .ok_or_else(|| anyhow!("forge artifact {contract} has no creation bytecode"))?;
    Ok(Artifact {
        name: format!("basenames:{contract}"),
        creation_code: hex::decode(bytecode).context("forge bytecode hex decode")?,
    })
}

fn ensure_basenames_built(repo_root: &Path) -> Result<()> {
    use std::sync::OnceLock;
    static BUILD: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    BUILD
        .get_or_init(|| {
            let root = repo_root.join(".refs/basenames");
            let required_artifacts = [
                "out/Registry.sol/Registry.json",
                "out/UpgradeableRegistrarController.sol/UpgradeableRegistrarController.json",
                "out/ERC1967Proxy.sol/ERC1967Proxy.json",
            ];
            if required_artifacts
                .iter()
                .all(|relative| root.join(relative).exists())
            {
                return Ok(());
            }
            let output = std::process::Command::new("forge")
                .arg("build")
                .current_dir(&root)
                .output()
                .map_err(|error| format!("spawn forge build: {error}"))?;
            if output.status.success() {
                Ok(())
            } else {
                Err(format!(
                    "forge build in {root:?} failed:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                ))
            }
        })
        .clone()
        .map_err(|message| anyhow!(message))
}

#[derive(Clone, Copy)]
pub enum BroadcastArgKind {
    Address,
    Bytes32,
    String,
    Uint256,
    Uint256Array,
}

enum BroadcastArg {
    Address(Address),
    Bytes32(B256),
    String(String),
    Uint256(U256),
    Uint256Array(Vec<U256>),
}

/// Load committed Forge broadcast creation calldata from
/// `.refs/basenames/broadcast/<script>/84532/run-latest.json` and split off
/// the constructor arguments recorded by Forge. The Basenames pin does not
/// include `out/` artifacts, so this keeps local deployments byte-exact to
/// the committed broadcast input while allowing scenario-specific constructor
/// arguments.
pub fn load_basenames_broadcast_artifact(
    repo_root: &Path,
    script: &str,
    contract_name: &str,
    arg_kinds: &[BroadcastArgKind],
) -> Result<Artifact> {
    let path = repo_root
        .join(".refs/basenames/broadcast")
        .join(script)
        .join("84532")
        .join("run-latest.json");
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("missing pinned broadcast artifact {path:?}"))?;
    let parsed: Value = serde_json::from_str(&raw).context("broadcast json parse")?;
    let tx = parsed
        .get("transactions")
        .and_then(Value::as_array)
        .and_then(|transactions| {
            transactions.iter().find(|tx| {
                tx.get("transactionType").and_then(Value::as_str) == Some("CREATE")
                    && tx.get("contractName").and_then(Value::as_str) == Some(contract_name)
            })
        })
        .ok_or_else(|| anyhow!("broadcast {script} has no CREATE for {contract_name}"))?;
    let input = tx
        .get("transaction")
        .and_then(|transaction| transaction.get("input"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("broadcast {script}/{contract_name} has no transaction.input"))?;
    let input = hex::decode(input).context("broadcast transaction.input hex decode")?;
    let raw_args = tx
        .get("arguments")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("broadcast {script}/{contract_name} has no arguments array"))?;
    if raw_args.len() != arg_kinds.len() {
        bail!(
            "broadcast {script}/{contract_name} constructor argument count changed: got {}, expected {}",
            raw_args.len(),
            arg_kinds.len()
        );
    }
    let args = raw_args
        .iter()
        .zip(arg_kinds)
        .map(|(value, kind)| parse_broadcast_arg(value, *kind))
        .collect::<Result<Vec<_>>>()?;
    let encoded_args = abi_encode_params(&args);
    if !input.ends_with(&encoded_args) {
        bail!(
            "broadcast {script}/{contract_name} constructor arguments did not match input suffix"
        );
    }
    Ok(Artifact {
        name: format!("basenames:{script}:{contract_name}"),
        creation_code: input[..input.len() - encoded_args.len()].to_vec(),
    })
}

fn parse_broadcast_arg(value: &Value, kind: BroadcastArgKind) -> Result<BroadcastArg> {
    let raw = value
        .as_str()
        .ok_or_else(|| anyhow!("broadcast constructor argument is not a string: {value}"))?;
    Ok(match kind {
        BroadcastArgKind::Address => BroadcastArg::Address(
            Address::from_str(raw).with_context(|| format!("parse address argument {raw}"))?,
        ),
        BroadcastArgKind::Bytes32 => BroadcastArg::Bytes32(
            B256::from_str(raw).with_context(|| format!("parse bytes32 argument {raw}"))?,
        ),
        BroadcastArgKind::String => BroadcastArg::String(clean_broadcast_string(raw)?),
        BroadcastArgKind::Uint256 => BroadcastArg::Uint256(parse_u256_decimal(raw)?),
        BroadcastArgKind::Uint256Array => BroadcastArg::Uint256Array(parse_u256_array(raw)?),
    })
}

fn clean_broadcast_string(raw: &str) -> Result<String> {
    if raw.starts_with('"') {
        serde_json::from_str(raw).with_context(|| format!("parse quoted string argument {raw}"))
    } else {
        Ok(raw.to_owned())
    }
}

fn parse_u256_array(raw: &str) -> Result<Vec<U256>> {
    let raw = raw.trim();
    let inner = raw
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| anyhow!("uint256[] argument is not bracketed: {raw}"))?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|value| parse_u256_decimal(value.trim()))
        .collect()
}

fn parse_u256_decimal(raw: &str) -> Result<U256> {
    let mut value = U256::ZERO;
    for byte in raw.bytes() {
        if !byte.is_ascii_digit() {
            bail!("uint256 argument is not decimal: {raw}");
        }
        value = value * U256::from(10_u8) + U256::from(byte - b'0');
    }
    Ok(value)
}

fn abi_encode_params(args: &[BroadcastArg]) -> Vec<u8> {
    let head_len = args.len() * 32;
    let mut head = Vec::with_capacity(head_len);
    let mut tail = Vec::new();
    let mut dynamic_offset = head_len;
    for arg in args {
        match arg {
            BroadcastArg::Address(value) => head.extend_from_slice(&address_word(*value)),
            BroadcastArg::Bytes32(value) => head.extend_from_slice(value.as_slice()),
            BroadcastArg::Uint256(value) => head.extend_from_slice(&u256_word(*value)),
            BroadcastArg::String(value) => {
                let encoded = dynamic_bytes(value.as_bytes());
                head.extend_from_slice(&u256_word(U256::from(dynamic_offset)));
                dynamic_offset += encoded.len();
                tail.extend_from_slice(&encoded);
            }
            BroadcastArg::Uint256Array(values) => {
                let encoded = dynamic_u256_array(values);
                head.extend_from_slice(&u256_word(U256::from(dynamic_offset)));
                dynamic_offset += encoded.len();
                tail.extend_from_slice(&encoded);
            }
        }
    }
    head.extend_from_slice(&tail);
    head
}

fn address_word(value: Address) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[12..].copy_from_slice(value.as_slice());
    word
}

fn u256_word(value: U256) -> [u8; 32] {
    value.to_be_bytes()
}

fn dynamic_bytes(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&u256_word(U256::from(value.len())));
    encoded.extend_from_slice(value);
    let padding = (32 - value.len() % 32) % 32;
    encoded.extend(std::iter::repeat_n(0, padding));
    encoded
}

fn dynamic_u256_array(values: &[U256]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(32 + values.len() * 32);
    encoded.extend_from_slice(&u256_word(U256::from(values.len())));
    for value in values {
        encoded.extend_from_slice(&u256_word(*value));
    }
    encoded
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
