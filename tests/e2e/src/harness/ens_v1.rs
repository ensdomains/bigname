use std::path::Path;

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Context, Result, bail};

use super::artifacts::{Deployed, deploy, load_ens_v1_artifact};
use super::rpc::RpcClient;

// Call fragments match the pinned upstream sources:
// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L39 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L23 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IETHRegistrarController.sol:L7 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L210 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IPriceOracle.sol:L5 @ ens_v1@91c966f)
sol! {
    function setSubnodeOwner(bytes32 node, bytes32 label, address owner) external returns (bytes32);
    function owner(bytes32 node) external view returns (address);
    function addController(address controller) external;

    struct Registration {
        string label;
        address owner;
        uint256 duration;
        bytes32 secret;
        address resolver;
        bytes[] data;
        uint8 reverseRecord;
        bytes32 referrer;
    }
    struct Price {
        uint256 base;
        uint256 premium;
    }
    function makeCommitment(Registration registration) external pure returns (bytes32 commitment);
    function commit(bytes32 commitment) external;
    function register(Registration registration) external payable;
    function rentPrice(string label, uint256 duration) external view returns (Price price);
}

pub fn labelhash(label: &str) -> B256 {
    keccak256(label.as_bytes())
}

pub fn namehash(name: &str) -> B256 {
    let mut node = B256::ZERO;
    if name.is_empty() {
        return node;
    }
    for label in name.rsplit('.') {
        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(node.as_slice());
        buf[32..].copy_from_slice(labelhash(label).as_slice());
        node = keccak256(buf);
    }
    node
}

/// The ENSv1 mainnet contract topology deployed onto a local chain from
/// pinned upstream artifacts: legacy registry + current registry (with
/// fallback), .eth base registrar, current registrar controller, price
/// oracle pair, reverse registrars, name wrapper, and public resolver.
pub struct EnsV1Deployment {
    pub deployer: Address,
    pub legacy_registry: Deployed,
    pub registry: Deployed,
    pub base_registrar: Deployed,
    pub controller: Deployed,
    pub public_resolver: Deployed,
    pub reverse_registrar: Deployed,
    pub name_wrapper: Deployed,
}

pub async fn deploy_ens_v1(rpc: &RpcClient, repo_root: &Path) -> Result<EnsV1Deployment> {
    let accounts = rpc.accounts().await?;
    let deployer = *accounts.first().context("anvil exposes no accounts")?;
    let load = |name: &str| load_ens_v1_artifact(repo_root, "sepolia", name);

    let legacy_registry = deploy(rpc, deployer, &load("LegacyENSRegistry")?, &[]).await?;
    let registry = deploy(
        rpc,
        deployer,
        &load("ENSRegistry")?,
        &(legacy_registry.address,).abi_encode_params(),
    )
    .await?;
    let eth_node = namehash("eth");
    let base_registrar = deploy(
        rpc,
        deployer,
        &load("BaseRegistrarImplementation")?,
        &(registry.address, eth_node).abi_encode_params(),
    )
    .await?;
    let dummy_oracle = deploy(
        rpc,
        deployer,
        &load("DummyOracle")?,
        &(U256::from(160_000_000_000u64),).abi_encode_params(),
    )
    .await?;
    // Rent prices and premium parameters mirror the pinned upstream deployment args
    // (.refs/ens_v1/deployments/sepolia/ExponentialPremiumPriceOracle.json).
    let rent_prices: Vec<U256> = [
        0u64,
        0,
        20_294_266_869_609,
        5_073_566_717_402,
        158_548_959_919,
    ]
    .into_iter()
    .map(U256::from)
    .collect();
    let price_oracle = deploy(
        rpc,
        deployer,
        &load("ExponentialPremiumPriceOracle")?,
        &(
            dummy_oracle.address,
            rent_prices,
            U256::from(10u8).pow(U256::from(26u8)),
            U256::from(21u8),
        )
            .abi_encode_params(),
    )
    .await?;
    let reverse_registrar = deploy(
        rpc,
        deployer,
        &load("ReverseRegistrar")?,
        &(registry.address,).abi_encode_params(),
    )
    .await?;
    let default_reverse_registrar =
        deploy(rpc, deployer, &load("DefaultReverseRegistrar")?, &[]).await?;
    // Upstream contracts deployed from here on may inherit ReverseClaimer,
    // whose constructor resolves the owner of `addr.reverse` and claims a
    // reverse record through it — the registry namespace wiring must exist
    // before they deploy.
    wire_registry_nodes(
        rpc,
        deployer,
        &registry,
        &base_registrar,
        &reverse_registrar,
    )
    .await?;

    let controller = deploy(
        rpc,
        deployer,
        &load("ETHRegistrarController")?,
        &(
            base_registrar.address,
            price_oracle.address,
            U256::from(60u8),
            U256::from(86_400u32),
            reverse_registrar.address,
            default_reverse_registrar.address,
            registry.address,
        )
            .abi_encode_params(),
    )
    .await?;
    let metadata_service = deploy(
        rpc,
        deployer,
        &load("StaticMetadataService")?,
        &("ens-metadata-service.appspot.com/name/0x{id}".to_string(),).abi_encode_params(),
    )
    .await?;
    let name_wrapper = deploy(
        rpc,
        deployer,
        &load("NameWrapper")?,
        &(
            registry.address,
            base_registrar.address,
            metadata_service.address,
        )
            .abi_encode_params(),
    )
    .await?;
    let public_resolver = deploy(
        rpc,
        deployer,
        &load("PublicResolver")?,
        &(
            registry.address,
            name_wrapper.address,
            controller.address,
            reverse_registrar.address,
        )
            .abi_encode_params(),
    )
    .await?;

    let deployment = EnsV1Deployment {
        deployer,
        legacy_registry,
        registry,
        base_registrar,
        controller,
        public_resolver,
        reverse_registrar,
        name_wrapper,
    };
    let controller_calls = [
        addControllerCall {
            controller: deployment.controller.address,
        }
        .abi_encode(),
        addControllerCall {
            controller: deployment.name_wrapper.address,
        }
        .abi_encode(),
    ];
    for data in controller_calls {
        send_checked(rpc, deployer, deployment.base_registrar.address, &data).await?;
    }
    Ok(deployment)
}

async fn wire_registry_nodes(
    rpc: &RpcClient,
    deployer: Address,
    registry: &Deployed,
    base_registrar: &Deployed,
    reverse_registrar: &Deployed,
) -> Result<()> {
    let root = B256::ZERO;
    let calls = [
        setSubnodeOwnerCall {
            node: root,
            label: labelhash("eth"),
            owner: base_registrar.address,
        }
        .abi_encode(),
        setSubnodeOwnerCall {
            node: root,
            label: labelhash("reverse"),
            owner: deployer,
        }
        .abi_encode(),
        setSubnodeOwnerCall {
            node: namehash("reverse"),
            label: labelhash("addr"),
            owner: reverse_registrar.address,
        }
        .abi_encode(),
    ];
    for data in calls {
        send_checked(rpc, deployer, registry.address, &data).await?;
    }
    Ok(())
}

async fn send_checked(rpc: &RpcClient, from: Address, to: Address, data: &[u8]) -> Result<()> {
    let receipt = rpc
        .send_transaction(from, Some(to), data, U256::ZERO)
        .await?;
    if !receipt.status_ok {
        bail!("call to {to} reverted (tx {})", receipt.tx_hash);
    }
    Ok(())
}

pub struct RegisteredName {
    pub label: String,
    pub owner: Address,
    pub commit_block: u64,
    pub register_block: u64,
}

/// Register `<label>.eth` through the pinned registrar controller's
/// commit/reveal flow, warping chain time past the minimum commitment age.
pub async fn register_eth_name(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    label: &str,
    owner: Address,
    duration_secs: u64,
    resolver: Address,
) -> Result<RegisteredName> {
    let registration = Registration {
        label: label.to_string(),
        owner,
        duration: U256::from(duration_secs),
        secret: B256::repeat_byte(0x42),
        resolver,
        data: vec![],
        reverseRecord: 0,
        referrer: B256::ZERO,
    };

    let commitment_raw = rpc
        .eth_call(
            d.controller.address,
            &makeCommitmentCall {
                registration: registration.clone(),
            }
            .abi_encode(),
        )
        .await?;
    let commitment =
        makeCommitmentCall::abi_decode_returns(&commitment_raw).context("makeCommitment decode")?;

    let commit_receipt = rpc
        .send_transaction(
            owner,
            Some(d.controller.address),
            &commitCall { commitment }.abi_encode(),
            U256::ZERO,
        )
        .await?;
    if !commit_receipt.status_ok {
        bail!("commit reverted (tx {})", commit_receipt.tx_hash);
    }

    rpc.increase_time(61).await?;

    let price_raw = rpc
        .eth_call(
            d.controller.address,
            &rentPriceCall {
                label: label.to_string(),
                duration: U256::from(duration_secs),
            }
            .abi_encode(),
        )
        .await?;
    let price = rentPriceCall::abi_decode_returns(&price_raw).context("rentPrice decode")?;

    let register_receipt = rpc
        .send_transaction(
            owner,
            Some(d.controller.address),
            &registerCall { registration }.abi_encode(),
            price.base + price.premium,
        )
        .await?;
    if !register_receipt.status_ok {
        bail!(
            "register of {label}.eth reverted (tx {})",
            register_receipt.tx_hash
        );
    }

    Ok(RegisteredName {
        label: label.to_string(),
        owner,
        commit_block: commit_receipt.block_number,
        register_block: register_receipt.block_number,
    })
}
