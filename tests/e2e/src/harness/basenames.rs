use std::path::Path;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Context, Result, bail};

use super::artifacts::{Deployed, deploy, load_ens_v1_artifact};
use super::ens_v1::{labelhash, namehash};
use super::rpc::RpcClient;

// ENSv1's Base L2ReverseRegistrar deployment declares Base coin type
// 2147492101.
// (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
pub const BASE_PRIMARY_COIN_TYPE: u64 = 2_147_492_101;

// Call fragments match the pinned Basenames and ENSv1 sources used by the
// scenario harness:
// (upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L208 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L226 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L327 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/RegistrarController.sol:L438 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/RegistrarController.sol:L395 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L125 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L48 @ basenames@1809bbc)
// (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L193 @ ens_v1@91c966f)
sol! {
    function setSubnodeOwner(bytes32 node, bytes32 label, address owner) external returns (bytes32);
    function setOwner(bytes32 node, address owner) external;
    function setResolver(address resolver) external;
    function addController(address controller) external;
    function setControllerApproval(address controller, bool approved) external;
    function transferFrom(address from, address to, uint256 tokenId) external;
    function reclaim(uint256 id, address owner) external;
    function setAddr(bytes32 node, address a) external;
    function setName(string name) external;
    function registerPrice(string name, uint256 duration) external view returns (uint256);

    struct RegisterRequest {
        string name;
        address owner;
        uint256 duration;
        address resolver;
        bytes[] data;
        bool reverseRecord;
    }
    function register(RegisterRequest request) external payable;
}

/// Local Basenames Base deployment, forge-built from the pinned sources
/// (the committed broadcast bytecode predates them and its constructors
/// differ), plus the ENSv1 Base L2ReverseRegistrar hardhat artifact.
/// (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
pub struct BasenamesDeployment {
    pub deployer: Address,
    pub registry: Deployed,
    pub base_registrar: Deployed,
    pub registrar_controller: Deployed,
    pub l2_resolver: Deployed,
    pub primary_reverse_registrar: Deployed,
    pub helper_reverse_registrar: Deployed,
}

pub struct RegisteredBasename {
    pub label: String,
    pub owner: Address,
    pub register_block: u64,
}

pub async fn deploy_basenames(rpc: &RpcClient, repo_root: &Path) -> Result<BasenamesDeployment> {
    let accounts = rpc.accounts().await?;
    let deployer = *accounts.first().context("anvil exposes no accounts")?;

    let registry = deploy(
        rpc,
        deployer,
        &forge(repo_root, "Registry")?,
        &(deployer,).abi_encode_params(),
    )
    .await?;
    let base_node = namehash("base.eth");
    let base_registrar = deploy(
        rpc,
        deployer,
        // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L189 @ basenames@1809bbc)
        &forge(repo_root, "BaseRegistrar")?,
        &(
            registry.address,
            deployer,
            base_node,
            "https://base-uri.invalid/".to_owned(),
            "https://collection-uri.invalid/".to_owned(),
        )
            .abi_encode_params(),
    )
    .await?;
    wire_base_namespace(rpc, deployer, registry.address, base_registrar.address).await?;

    let price_oracle = deploy(
        rpc,
        deployer,
        // (upstream: .refs/basenames/src/L2/ExponentialPremiumPriceOracle.sol:L13 @ basenames@1809bbc)
        &forge(repo_root, "ExponentialPremiumPriceOracle")?,
        &(
            vec![
                U256::from(317_097_919_837_u64),
                U256::from(31_709_791_983_u64),
                U256::from(3_170_979_198_u64),
                U256::from(317_097_919_u64),
                U256::from(31_709_791_u64),
                U256::from(3_170_979_u64),
            ],
            U256::from(500_u16) * U256::from(10_u8).pow(U256::from(18_u8)),
            U256::from(2_419_200_u64),
        )
            .abi_encode_params(),
    )
    .await?;

    // Constructor is (ENS registry_, address owner_, bytes32 reverseNode_)
    // (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L102 @ basenames@1809bbc);
    // the reverse node must match the `80002105.reverse` namespace wired
    // below — the controller's and resolver's constructor-time claim()
    // calls write under it.
    let helper_reverse_registrar = deploy(
        rpc,
        deployer,
        &forge(repo_root, "ReverseRegistrar")?,
        &(registry.address, deployer, namehash("80002105.reverse")).abi_encode_params(),
    )
    .await?;
    wire_base_reverse_namespace(
        rpc,
        deployer,
        registry.address,
        helper_reverse_registrar.address,
    )
    .await?;

    let registrar_controller = deploy(
        rpc,
        deployer,
        // Current 7-arg constructor including paymentReceiver_
        // (upstream: .refs/basenames/src/L2/RegistrarController.sol:L271 @ basenames@1809bbc)
        &forge(repo_root, "RegistrarController")?,
        &(
            base_registrar.address,
            price_oracle.address,
            helper_reverse_registrar.address,
            deployer,
            base_node,
            ".base.eth".to_owned(),
            deployer,
        )
            .abi_encode_params(),
    )
    .await?;
    send_checked(
        rpc,
        deployer,
        base_registrar.address,
        &addControllerCall {
            controller: registrar_controller.address,
        }
        .abi_encode(),
    )
    .await?;
    send_checked(
        rpc,
        deployer,
        helper_reverse_registrar.address,
        &setControllerApprovalCall {
            controller: registrar_controller.address,
            approved: true,
        }
        .abi_encode(),
    )
    .await?;

    let l2_resolver = deploy(
        rpc,
        deployer,
        // (upstream: .refs/basenames/src/L2/L2Resolver.sol:L113 @ basenames@1809bbc)
        &forge(repo_root, "L2Resolver")?,
        &(
            registry.address,
            registrar_controller.address,
            helper_reverse_registrar.address,
            deployer,
        )
            .abi_encode_params(),
    )
    .await?;
    send_checked(
        rpc,
        deployer,
        base_registrar.address,
        &setResolverCall {
            resolver: l2_resolver.address,
        }
        .abi_encode(),
    )
    .await?;

    let primary_reverse_registrar = deploy(
        rpc,
        deployer,
        &load_ens_v1_artifact(repo_root, "base", "L2ReverseRegistrar")?,
        &(
            U256::from(BASE_PRIMARY_COIN_TYPE),
            deployer,
            base_reverse_node(),
            Address::ZERO,
        )
            .abi_encode_params(),
    )
    .await?;

    Ok(BasenamesDeployment {
        deployer,
        registry,
        base_registrar,
        registrar_controller,
        l2_resolver,
        primary_reverse_registrar,
        helper_reverse_registrar,
    })
}

fn forge(repo_root: &Path, contract: &str) -> Result<super::artifacts::Artifact> {
    super::artifacts::load_basenames_forge_artifact(repo_root, contract)
}

async fn wire_base_namespace(
    rpc: &RpcClient,
    deployer: Address,
    registry: Address,
    base_registrar: Address,
) -> Result<()> {
    let calls = [
        setSubnodeOwnerCall {
            node: B256::ZERO,
            label: labelhash("eth"),
            owner: deployer,
        }
        .abi_encode(),
        setSubnodeOwnerCall {
            node: namehash("eth"),
            label: labelhash("base"),
            owner: base_registrar,
        }
        .abi_encode(),
    ];
    for data in calls {
        send_checked(rpc, deployer, registry, &data).await?;
    }
    Ok(())
}

async fn wire_base_reverse_namespace(
    rpc: &RpcClient,
    deployer: Address,
    registry: Address,
    helper_reverse_registrar: Address,
) -> Result<()> {
    let calls = [
        setSubnodeOwnerCall {
            node: B256::ZERO,
            label: labelhash("reverse"),
            owner: deployer,
        }
        .abi_encode(),
        setSubnodeOwnerCall {
            node: namehash("reverse"),
            label: labelhash("80002105"),
            owner: helper_reverse_registrar,
        }
        .abi_encode(),
    ];
    for data in calls {
        send_checked(rpc, deployer, registry, &data).await?;
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

pub async fn register_base_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    label: &str,
    owner: Address,
    duration_secs: u64,
) -> Result<RegisteredBasename> {
    let price_raw = rpc
        .eth_call(
            d.registrar_controller.address,
            &registerPriceCall {
                name: label.to_owned(),
                duration: U256::from(duration_secs),
            }
            .abi_encode(),
        )
        .await?;
    let price =
        registerPriceCall::abi_decode_returns(&price_raw).context("registerPrice decode")?;
    let request = RegisterRequest {
        name: label.to_owned(),
        owner,
        duration: U256::from(duration_secs),
        resolver: d.l2_resolver.address,
        data: Vec::<Bytes>::new(),
        reverseRecord: false,
    };
    let receipt = rpc
        .send_transaction(
            from,
            Some(d.registrar_controller.address),
            &registerCall { request }.abi_encode(),
            price,
        )
        .await?;
    if !receipt.status_ok {
        bail!(
            "register of {label}.base.eth reverted (tx {})",
            receipt.tx_hash
        );
    }
    Ok(RegisteredBasename {
        label: label.to_owned(),
        owner,
        register_block: receipt.block_number,
    })
}

pub async fn transfer_base_token(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    to: Address,
    label: &str,
) -> Result<()> {
    let token_id = U256::from_be_bytes(labelhash(label).0);
    send_checked(
        rpc,
        from,
        d.base_registrar.address,
        &transferFromCall {
            from,
            to,
            tokenId: token_id,
        }
        .abi_encode(),
    )
    .await
}

pub async fn reclaim_base_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    owner: Address,
    label: &str,
) -> Result<()> {
    let token_id = U256::from_be_bytes(labelhash(label).0);
    send_checked(
        rpc,
        from,
        d.base_registrar.address,
        &reclaimCall {
            id: token_id,
            owner,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_registry_owner(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    name: &str,
    owner: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.registry.address,
        &setOwnerCall {
            node: namehash(name),
            owner,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_addr_record(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    name: &str,
    target: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.l2_resolver.address,
        &setAddrCall {
            node: namehash(name),
            a: target,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_primary_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    name: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.primary_reverse_registrar.address,
        &setNameCall {
            name: name.to_owned(),
        }
        .abi_encode(),
    )
    .await
}

impl BasenamesDeployment {
    pub fn manifest_targets(&self) -> std::collections::HashMap<&'static str, (Address, u64)> {
        std::collections::HashMap::from([
            (
                "BasenamesRegistry",
                (self.registry.address, self.registry.block_number),
            ),
            (
                "registry",
                (self.registry.address, self.registry.block_number),
            ),
            (
                "registrar",
                (
                    self.base_registrar.address,
                    self.base_registrar.block_number,
                ),
            ),
            (
                "legacy_registrar_controller",
                (
                    self.registrar_controller.address,
                    self.registrar_controller.block_number,
                ),
            ),
            (
                "resolver",
                (self.l2_resolver.address, self.l2_resolver.block_number),
            ),
            (
                "reverse_registrar",
                (
                    self.primary_reverse_registrar.address,
                    self.primary_reverse_registrar.block_number,
                ),
            ),
        ])
    }
}

fn base_reverse_node() -> B256 {
    // Basenames scripts wire the ENSIP-19 Base reverse namespace as
    // `80002105.reverse`.
    // (upstream: .refs/basenames/src/util/Constants.sol:L10 @ basenames@1809bbc)
    namehash("80002105.reverse")
}
