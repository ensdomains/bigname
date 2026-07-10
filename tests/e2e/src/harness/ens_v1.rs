use std::path::Path;
use std::str::FromStr;

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Context, Result, bail};

use super::artifacts::{Deployed, deploy, load_ens_v1_artifact};
use super::rpc::RpcClient;

// Call fragments match the pinned upstream sources:
// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L39 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L45 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L112 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L23 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IETHRegistrarController.sol:L7 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L210 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L352 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IPriceOracle.sol:L5 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/root/Controllable.sol:L18 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L26 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/ContenthashResolver.sol:L14 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L15 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L98 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L20 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f)
sol! {
    function setSubnodeOwner(bytes32 node, bytes32 label, address owner) external returns (bytes32);
    function owner(bytes32 node) external view returns (address);
    function setResolver(bytes32 node, address resolver) external;
    function setApprovalForAll(address operator, bool approved) external;
    function addController(address controller) external;
    function setController(address controller, bool enabled) external;
    function reclaim(uint256 id, address owner) external;
    function transferFrom(address from, address to, uint256 tokenId) external;
    function renew(string label, uint256 duration, bytes32 referrer) external payable;
    function setAddr(bytes32 node, address a) external;
    function setContenthash(bytes32 node, bytes hash) external;
    function setText(bytes32 node, string key, string value) external;
    function setName(bytes32 node, string newName) external;
    function approve(bytes32 node, address delegate, bool approved) external;
    function clearRecords(bytes32 node) external;
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/ABIResolver.sol:L16 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/InterfaceResolver.sol:L17 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L51 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L136 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/PubkeyResolver.sol:L19 @ ens_v1@91c966f)
    function setABI(bytes32 node, uint256 contentType, bytes data) external;
    function setInterface(bytes32 node, bytes4 interfaceID, address implementer) external;
    function setDNSRecords(bytes32 node, bytes data) external;
    function setZonehash(bytes32 node, bytes hash) external;
    function setPubkey(bytes32 node, bytes32 x, bytes32 y) external;

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

mod multicoin_addr_calls {
    use alloy_sol_types::sol;

    // The coin-type overload must stay out of the main `sol!` block because
    // the generated Rust call type collides with `setAddr(bytes32,address)`.
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47 @ ens_v1@91c966f)
    sol! {
        function setAddr(bytes32 node, uint256 coinType, bytes addressBytes) external;
    }
}

mod legacy_registry_calls {
    use alloy_sol_types::sol;

    // Dedicated legacy-registry send bindings. The pinned artifact supplies
    // the input signatures; transaction calldata does not depend on return ABI.
    // (upstream: .refs/ens_v1/deployments/sepolia/LegacyENSRegistry.json:L274 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/deployments/sepolia/LegacyENSRegistry.json:L280 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/deployments/sepolia/LegacyENSRegistry.json:L297 @ ens_v1@91c966f)
    sol! {
        function setSubnodeOwner(bytes32 node, bytes32 label, address owner) external;
        function setResolver(bytes32 node, address resolver) external;
    }
}

mod registrar_token_calls {
    use alloy_sol_types::sol;

    // BaseRegistrarImplementation is an ERC721 and advertises
    // approve(address,uint256) in its interface id.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L5 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L24 @ ens_v1@91c966f)
    sol! {
        function approve(address to, uint256 tokenId) external;
    }
}

mod wrapper_calls {
    use alloy_sol_types::sol;

    // NameWrapper .eth wrapping moves the registrar token into the wrapper,
    // reclaims registry ownership to the wrapper, and mints a wrapped token.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L268 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L270 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1013 @ ens_v1@91c966f)
    //
    // Unwrap checks CANNOT_UNWRAP, burns wrapper state, restores registry
    // ownership, then returns the registrar token.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L391 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1023 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1028 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1029 @ ens_v1@91c966f)
    //
    // Fuse and wrapped-subname calls are owner-gated wrapper operations.
    // Burning owner-controlled fuses is only allowed through setFuses.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L427 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L434 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L596 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L612 @ ens_v1@91c966f)
    //
    // Fuse constants are imported by NameWrapper from INameWrapper.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L6 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L12 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L13 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18 @ ens_v1@91c966f)
    sol! {
        function wrapETH2LD(string label, address wrappedOwner, uint16 ownerControlledFuses, address resolver) external returns (uint64 expiry);
        function unwrapETH2LD(bytes32 labelhash, address registrant, address controller) external;
        function setFuses(bytes32 node, uint16 ownerControlledFuses) external returns (uint32 oldFuses);
        function setSubnodeOwner(bytes32 parentNode, string label, address owner, uint32 fuses, uint64 expiry) external returns (bytes32 node);
        function setSubnodeRecord(bytes32 parentNode, string label, address owner, address resolver, uint64 ttl, uint32 fuses, uint64 expiry) external returns (bytes32 node);
    }
}

mod reverse_calls {
    use alloy_sol_types::sol;

    // ReverseRegistrar.setName claims for msg.sender, then writes name() on
    // the configured resolver.
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L105 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L107 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L108 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
    // Claim-only and third-party claim entrypoints route through claimForAddr.
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L64 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L93 @ ens_v1@91c966f)
    // PublicResolver authorizes the trusted ReverseRegistrar to write NameResolver records.
    // (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L70 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L116 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L117 @ ens_v1@91c966f)
    sol! {
        function setDefaultResolver(address resolver) external;
        function setName(string name) external returns (bytes32 node);
        function claim(address owner) external returns (bytes32 node);
        function claimWithResolver(address owner, address resolver) external returns (bytes32 node);
        function setNameForAddr(address addr, address owner, address resolver, string name) external returns (bytes32 node);
    }
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

pub fn reverse_node(address: Address) -> B256 {
    namehash(&format!("{address:x}.addr.reverse"))
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

pub const EXECUTION_UNIVERSAL_RESOLVER_ADDRESS: &str = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L27 @ ens_v1@91c966f)
pub const REVERSE_RECORD_ETHEREUM: u8 = 1;

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
    let registrar_controller_calls = [
        addControllerCall {
            controller: deployment.controller.address,
        }
        .abi_encode(),
        addControllerCall {
            controller: deployment.name_wrapper.address,
        }
        .abi_encode(),
    ];
    for data in registrar_controller_calls {
        send_checked(rpc, deployer, deployment.base_registrar.address, &data).await?;
    }
    // (upstream: .refs/ens_v1/deploy/ethregistrar/04_deploy_eth_registrar_controller.ts:L78 @ ens_v1@91c966f)
    send_checked(
        rpc,
        deployer,
        deployment.reverse_registrar.address,
        &setControllerCall {
            controller: deployment.controller.address,
            enabled: true,
        }
        .abi_encode(),
    )
    .await?;
    Ok(deployment)
}

/// Deploy another byte-exact copy of the pinned PublicResolver artifact
/// without adding it to `manifest_targets`.
///
/// The constructor dependencies mirror the main deployment helper:
/// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L75 @ ens_v1@91c966f).
pub async fn deploy_extra_public_resolver(
    rpc: &RpcClient,
    repo_root: &Path,
    d: &EnsV1Deployment,
) -> Result<Deployed> {
    deploy(
        rpc,
        d.deployer,
        &load_ens_v1_artifact(repo_root, "sepolia", "PublicResolver")?,
        &(
            d.registry.address,
            d.name_wrapper.address,
            d.controller.address,
            d.reverse_registrar.address,
        )
            .abi_encode_params(),
    )
    .await
}

/// Deploy the pinned UniversalResolver with local constructor dependencies,
/// then install its runtime bytecode at the address currently used by the
/// execution crate for ENS verified resolution.
///
/// The constructor shape is pinned upstream:
/// (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L11 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L19 @ ens_v1@91c966f)
/// and the batch gateway provider returns the URL set supplied at deployment:
/// (upstream: .refs/ens_v1/contracts/ccipRead/GatewayProvider.sol:L11 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ccipRead/GatewayProvider.sol:L16 @ ens_v1@91c966f).
pub async fn install_local_universal_resolver(
    rpc: &RpcClient,
    repo_root: &Path,
    d: &EnsV1Deployment,
) -> Result<Deployed> {
    let gateway_provider = deploy(
        rpc,
        d.deployer,
        &load_ens_v1_artifact(repo_root, "sepolia", "BatchGatewayProvider")?,
        &(d.deployer, Vec::<String>::new()).abi_encode_params(),
    )
    .await?;
    let materialized = deploy(
        rpc,
        d.deployer,
        &load_ens_v1_artifact(repo_root, "sepolia", "UniversalResolver")?,
        &(d.deployer, d.registry.address, gateway_provider.address).abi_encode_params(),
    )
    .await?;
    let runtime_code = rpc.get_code(materialized.address).await.with_context(|| {
        format!(
            "read local UniversalResolver runtime at {:#x}",
            materialized.address
        )
    })?;
    if runtime_code.is_empty() {
        bail!(
            "local UniversalResolver deployment at {:#x} has no runtime code",
            materialized.address
        );
    }
    let execution_address = Address::from_str(EXECUTION_UNIVERSAL_RESOLVER_ADDRESS)
        .context("parse execution UniversalResolver address")?;
    rpc.set_code(execution_address, &runtime_code)
        .await
        .with_context(|| {
            format!(
                "install local UniversalResolver runtime at {EXECUTION_UNIVERSAL_RESOLVER_ADDRESS}"
            )
        })?;
    Ok(Deployed {
        address: execution_address,
        block_number: materialized.block_number,
    })
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

/// Create `<label>.<parent>` in the registry, owned by `owner`. `from` must
/// control the parent node.
pub async fn create_subname(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    parent: &str,
    label: &str,
    owner: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.registry.address,
        &setSubnodeOwnerCall {
            node: namehash(parent),
            label: labelhash(label),
            owner,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_registry_approval_for_all(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    owner: Address,
    operator: Address,
    approved: bool,
) -> Result<()> {
    send_checked(
        rpc,
        owner,
        d.registry.address,
        &setApprovalForAllCall { operator, approved }.abi_encode(),
    )
    .await
}

/// Create `<label>` below `parent_node` in the legacy registry. `from` must
/// control `parent_node` in that legacy registry state.
pub async fn create_legacy_subname(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    parent_node: B256,
    label: &str,
    owner: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.legacy_registry.address,
        &legacy_registry_calls::setSubnodeOwnerCall {
            node: parent_node,
            label: labelhash(label),
            owner,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_resolver(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    name: &str,
    resolver: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.registry.address,
        &setResolverCall {
            node: namehash(name),
            resolver,
        }
        .abi_encode(),
    )
    .await
}

/// Set a resolver on the legacy registry. `from` must control `node` in that
/// legacy registry state.
pub async fn set_legacy_resolver(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    node: B256,
    resolver: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.legacy_registry.address,
        &legacy_registry_calls::setResolverCall { node, resolver }.abi_encode(),
    )
    .await
}

pub async fn set_text_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setTextCall {
            node: namehash(name),
            key: key.to_string(),
            value: value.to_string(),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_abi_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    content_type: u64,
    data: &[u8],
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setABICall {
            node: namehash(name),
            contentType: U256::from(content_type),
            data: Bytes::copy_from_slice(data),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_interface_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    interface_id: [u8; 4],
    implementer: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setInterfaceCall {
            node: namehash(name),
            interfaceID: interface_id.into(),
            implementer,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_dns_records(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    data: &[u8],
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setDNSRecordsCall {
            node: namehash(name),
            data: Bytes::copy_from_slice(data),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_zonehash(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    hash: &[u8],
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setZonehashCall {
            node: namehash(name),
            hash: Bytes::copy_from_slice(hash),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_pubkey_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    x: B256,
    y: B256,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setPubkeyCall {
            node: namehash(name),
            x,
            y,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_name_record_for_node(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    node: B256,
    name: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setNameCall {
            node,
            newName: name.to_string(),
        }
        .abi_encode(),
    )
    .await
}

pub async fn approve_resolver_delegate(
    rpc: &RpcClient,
    resolver: Address,
    owner: Address,
    name: &str,
    delegate: Address,
    approved: bool,
) -> Result<()> {
    send_checked(
        rpc,
        owner,
        resolver,
        &approveCall {
            node: namehash(name),
            delegate,
            approved,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_addr_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    target: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setAddrCall {
            node: namehash(name),
            a: target,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_multicoin_addr_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    coin_type: u64,
    address_bytes: &[u8],
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &multicoin_addr_calls::setAddrCall {
            node: namehash(name),
            coinType: U256::from(coin_type),
            addressBytes: Bytes::copy_from_slice(address_bytes),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_contenthash_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    contenthash: &[u8],
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &setContenthashCall {
            node: namehash(name),
            hash: Bytes::copy_from_slice(contenthash),
        }
        .abi_encode(),
    )
    .await
}

pub async fn clear_records(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &clearRecordsCall {
            node: namehash(name),
        }
        .abi_encode(),
    )
    .await
}

/// Renew `<label>.eth` through the controller for `duration_secs`.
pub async fn renew_eth_name(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    label: &str,
    duration_secs: u64,
) -> Result<()> {
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
    let receipt = rpc
        .send_transaction(
            from,
            Some(d.controller.address),
            &renewCall {
                label: label.to_string(),
                duration: U256::from(duration_secs),
                referrer: B256::ZERO,
            }
            .abi_encode(),
            price.base + price.premium,
        )
        .await?;
    if !receipt.status_ok {
        bail!("renew of {label}.eth reverted (tx {})", receipt.tx_hash);
    }
    Ok(())
}

/// Transfer the registrar token for `<label>.eth` without reclaiming registry
/// ownership (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f).
pub async fn transfer_eth_name_without_reclaim(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
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

/// Transfer the registrar token for `<label>.eth` and reclaim registry
/// ownership for the new holder.
pub async fn transfer_eth_name(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    to: Address,
    label: &str,
) -> Result<()> {
    transfer_eth_name_without_reclaim(rpc, d, from, to, label).await?;
    let token_id = U256::from_be_bytes(labelhash(label).0);
    send_checked(
        rpc,
        to,
        d.base_registrar.address,
        &reclaimCall {
            id: token_id,
            owner: to,
        }
        .abi_encode(),
    )
    .await
}

mod registrar_direct {
    use alloy_sol_types::sol;

    sol! {
        function register(uint256 id, address owner, uint256 duration) external returns (uint256);
    }
}

/// Authorise an extra registrar controller from the registrar owner
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f).
pub async fn add_registrar_controller(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    controller: Address,
) -> Result<()> {
    send_checked(
        rpc,
        d.deployer,
        d.base_registrar.address,
        &addControllerCall { controller }.abi_encode(),
    )
    .await
}

/// Register `<label>.eth` directly on the registrar as an authorised
/// controller, bypassing the admitted string-label controllers
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110 @ ens_v1@91c966f).
pub async fn register_via_registrar(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    sender: Address,
    label: &str,
    owner: Address,
    duration_secs: u64,
) -> Result<()> {
    send_checked(
        rpc,
        sender,
        d.base_registrar.address,
        &registrar_direct::registerCall {
            id: U256::from_be_bytes(labelhash(label).0),
            owner,
            duration: U256::from(duration_secs),
        }
        .abi_encode(),
    )
    .await
}

/// Approve the wrapper for `<label>.eth` and wrap the live registrar lease.
pub async fn wrap_eth_2ld(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    label: &str,
    wrapped_owner: Address,
    owner_controlled_fuses: u16,
    resolver: Address,
) -> Result<()> {
    let token_id = U256::from_be_bytes(labelhash(label).0);
    send_checked(
        rpc,
        from,
        d.base_registrar.address,
        &registrar_token_calls::approveCall {
            to: d.name_wrapper.address,
            tokenId: token_id,
        }
        .abi_encode(),
    )
    .await?;
    send_checked(
        rpc,
        from,
        d.name_wrapper.address,
        &wrapper_calls::wrapETH2LDCall {
            label: label.to_string(),
            wrappedOwner: wrapped_owner,
            ownerControlledFuses: owner_controlled_fuses,
            resolver,
        }
        .abi_encode(),
    )
    .await
}

pub async fn unwrap_eth_2ld(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    label: &str,
    registrant: Address,
    controller: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.name_wrapper.address,
        &wrapper_calls::unwrapETH2LDCall {
            labelhash: labelhash(label),
            registrant,
            controller,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_wrapper_fuses(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    name: &str,
    owner_controlled_fuses: u16,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.name_wrapper.address,
        &wrapper_calls::setFusesCall {
            node: namehash(name),
            ownerControlledFuses: owner_controlled_fuses,
        }
        .abi_encode(),
    )
    .await
}

pub struct WrappedSubnodeOwner<'a> {
    pub parent: &'a str,
    pub label: &'a str,
    pub owner: Address,
    pub fuses: u32,
    pub expiry: u64,
}

pub async fn set_wrapped_subnode_owner(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    input: WrappedSubnodeOwner<'_>,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.name_wrapper.address,
        &wrapper_calls::setSubnodeOwnerCall {
            parentNode: namehash(input.parent),
            label: input.label.to_string(),
            owner: input.owner,
            fuses: input.fuses,
            expiry: input.expiry,
        }
        .abi_encode(),
    )
    .await
}

pub struct WrappedSubnodeRecord<'a> {
    pub parent: &'a str,
    pub label: &'a str,
    pub owner: Address,
    pub resolver: Address,
    pub fuses: u32,
    pub expiry: u64,
}

pub async fn set_wrapped_subnode_record(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    input: WrappedSubnodeRecord<'_>,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.name_wrapper.address,
        &wrapper_calls::setSubnodeRecordCall {
            parentNode: namehash(input.parent),
            label: input.label.to_string(),
            owner: input.owner,
            resolver: input.resolver,
            ttl: 0,
            fuses: input.fuses,
            expiry: input.expiry,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_reverse_name(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    name: &str,
) -> Result<()> {
    set_reverse_default_resolver(rpc, d, d.public_resolver.address).await?;
    send_checked(
        rpc,
        from,
        d.reverse_registrar.address,
        &reverse_calls::setNameCall {
            name: name.to_string(),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_reverse_default_resolver(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    resolver: Address,
) -> Result<()> {
    send_checked(
        rpc,
        d.deployer,
        d.reverse_registrar.address,
        &reverse_calls::setDefaultResolverCall { resolver }.abi_encode(),
    )
    .await
}

pub async fn claim_reverse(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    owner: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.reverse_registrar.address,
        &reverse_calls::claimCall { owner }.abi_encode(),
    )
    .await
}

pub async fn claim_reverse_with_resolver(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    owner: Address,
    resolver: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.reverse_registrar.address,
        &reverse_calls::claimWithResolverCall { owner, resolver }.abi_encode(),
    )
    .await
}

pub async fn set_reverse_name_for_addr(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    from: Address,
    address: Address,
    owner: Address,
    resolver: Address,
    name: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.reverse_registrar.address,
        &reverse_calls::setNameForAddrCall {
            addr: address,
            owner,
            resolver,
            name: name.to_string(),
        }
        .abi_encode(),
    )
    .await
}

impl EnsV1Deployment {
    /// Manifest patch targets for this deployment, keyed by `[[roots]].name`
    /// and `[[contracts]].role` (see `harness::manifests`).
    pub fn manifest_targets(&self) -> std::collections::HashMap<&'static str, (Address, u64)> {
        std::collections::HashMap::from([
            (
                "ENSRegistry",
                (self.registry.address, self.registry.block_number),
            ),
            (
                "registry",
                (self.registry.address, self.registry.block_number),
            ),
            (
                "registry_old",
                (
                    self.legacy_registry.address,
                    self.legacy_registry.block_number,
                ),
            ),
            (
                "ETHRegistrar",
                (
                    self.base_registrar.address,
                    self.base_registrar.block_number,
                ),
            ),
            (
                "registrar",
                (
                    self.base_registrar.address,
                    self.base_registrar.block_number,
                ),
            ),
            (
                "unwrapped_registrar_controller",
                (self.controller.address, self.controller.block_number),
            ),
            (
                "public_resolver",
                (
                    self.public_resolver.address,
                    self.public_resolver.block_number,
                ),
            ),
            (
                "reverse_registrar",
                (
                    self.reverse_registrar.address,
                    self.reverse_registrar.block_number,
                ),
            ),
            (
                "name_wrapper",
                (self.name_wrapper.address, self.name_wrapper.block_number),
            ),
        ])
    }
}

pub struct RegisteredName {
    pub label: String,
    pub owner: Address,
    pub commit_block: u64,
    pub register_block: u64,
    pub register_tx_hash: String,
}

#[derive(Default)]
pub struct RegistrationOptions {
    pub data: Vec<Bytes>,
    pub reverse_record: u8,
    pub referrer: B256,
}

pub fn registration_addr_record_data(name: &str, target: Address) -> Bytes {
    multicoin_addr_calls::setAddrCall {
        node: namehash(name),
        coinType: U256::from(60),
        addressBytes: Bytes::copy_from_slice(target.as_slice()),
    }
    .abi_encode()
    .into()
}

pub fn registration_text_record_data(name: &str, key: &str, value: &str) -> Bytes {
    setTextCall {
        node: namehash(name),
        key: key.to_owned(),
        value: value.to_owned(),
    }
    .abi_encode()
    .into()
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
    register_eth_name_with_options(
        rpc,
        d,
        label,
        owner,
        duration_secs,
        resolver,
        RegistrationOptions::default(),
    )
    .await
}

pub async fn register_eth_name_with_options(
    rpc: &RpcClient,
    d: &EnsV1Deployment,
    label: &str,
    owner: Address,
    duration_secs: u64,
    resolver: Address,
    options: RegistrationOptions,
) -> Result<RegisteredName> {
    let registration = Registration {
        label: label.to_string(),
        owner,
        duration: U256::from(duration_secs),
        secret: B256::repeat_byte(0x42),
        resolver,
        data: options.data,
        reverseRecord: options.reverse_record,
        referrer: options.referrer,
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
        register_tx_hash: register_receipt.tx_hash,
    })
}
