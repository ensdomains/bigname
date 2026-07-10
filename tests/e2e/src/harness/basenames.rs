use std::path::Path;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Context, Result, bail};

use super::artifacts::{Deployed, deploy, load_ens_v1_artifact};
use super::ens_v1::{labelhash, namehash};
use super::rpc::{RpcClient, TxReceipt};

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
    function renew(string name, uint256 duration) external payable;
    function registerPrice(string name, uint256 duration) external view returns (uint256);

    struct Price {
        uint256 base;
        uint256 premium;
    }
    function rentPrice(string name, uint256 duration) external view returns (Price price);

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

mod registry_calls {
    use alloy_sol_types::sol;

    // Registry mutations and reads are kept apart from BaseRegistrar's
    // one-argument setResolver overload.
    // (upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/Registry.sol:L165 @ basenames@1809bbc)
    sol! {
        function setSubnodeOwner(bytes32 node, bytes32 label, address owner) external returns (bytes32);
        function setResolver(bytes32 node, address resolver) external;
        function owner(bytes32 node) external view returns (address);
    }
}

mod resolver_calls {
    use alloy_sol_types::sol;

    // (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L17 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/NameResolver.sol:L15 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L22 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/ContentHashResolver.sol:L16 @ basenames@1809bbc)
    sol! {
        function setText(bytes32 node, string key, string value) external;
        function setName(bytes32 node, string newName) external;
        function clearRecords(bytes32 node) external;
        function setContenthash(bytes32 node, bytes hash) external;
    }
}

mod multicoin_addr_calls {
    use alloy_sol_types::sol;

    // (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L45 @ basenames@1809bbc)
    sol! {
        function setAddr(bytes32 node, uint256 coinType, bytes addressBytes) external;
    }
}

mod upgradeable_controller_calls {
    use alloy_sol_types::sol;

    // UpgradeableRegistrarController is initialized through its ERC1967
    // proxy and extends the legacy registration tuple with ENSIP-19 fields.
    // (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L300 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L515 @ basenames@1809bbc)
    sol! {
        function initialize(
            address base,
            address prices,
            address reverseRegistrar,
            address owner,
            bytes32 rootNode,
            string rootName,
            address paymentReceiver,
            address legacyRegistrarController,
            address legacyL2Resolver,
            address l2ReverseRegistrar
        ) external;

        struct RegisterRequest {
            string name;
            address owner;
            uint256 duration;
            address resolver;
            bytes[] data;
            bool reverseRecord;
            uint256[] coinTypes;
            uint256 signatureExpiry;
            bytes signature;
        }
        function register(RegisterRequest request) external payable;
        function setRegistrarController(address registrarController) external;
    }
}

mod reverse_calls {
    use alloy_sol_types::sol;

    // (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
    sol! {
        function claimForBaseAddr(address addr, address owner, address resolver) external returns (bytes32 node);
        function setNameForAddr(address addr, address owner, address resolver, string name) external returns (bytes32 node);
    }
}

mod direct_registrar_calls {
    use alloy_sol_types::sol;

    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L237 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L248 @ basenames@1809bbc)
    sol! {
        function register(uint256 id, address owner, uint256 duration) external returns (uint256);
        function registerOnly(uint256 id, address owner, uint256 duration) external returns (uint256);
    }
}

/// Local Basenames Base deployment, forge-built from the pinned sources
/// (the committed broadcast bytecode predates them and its constructors
/// differ), plus the ENSv1 Base L2ReverseRegistrar hardhat artifact.
/// (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
pub struct BasenamesDeployment {
    pub deployer: Address,
    pub registry: Deployed,
    pub base_registrar: Deployed,
    pub price_oracle: Deployed,
    pub registrar_controller: Deployed,
    pub upgradeable_registrar_controller: Option<Deployed>,
    pub upgradeable_registrar_controller_implementation: Option<Deployed>,
    pub l2_resolver: Deployed,
    pub primary_reverse_registrar: Deployed,
    pub helper_reverse_registrar: Deployed,
}

pub struct UnadmittedL2ResolverDeployment {
    pub registry: Deployed,
    pub resolver: Deployed,
}

pub struct RegisteredBasename {
    pub label: String,
    pub owner: Address,
    pub register_block: u64,
    pub register_tx_hash: String,
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
        price_oracle,
        registrar_controller,
        upgradeable_registrar_controller: None,
        upgradeable_registrar_controller_implementation: None,
        l2_resolver,
        primary_reverse_registrar,
        helper_reverse_registrar,
    })
}

fn forge(repo_root: &Path, contract: &str) -> Result<super::artifacts::Artifact> {
    super::artifacts::load_basenames_forge_artifact(repo_root, contract)
}

/// Deploy the pinned UpgradeableRegistrarController implementation behind
/// the vendored OpenZeppelin ERC1967Proxy, initialize proxy storage, and
/// authorize the proxy, and make it the L2Resolver's trusted controller.
/// (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L300 @ basenames@1809bbc)
/// (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/proxy/ERC1967/ERC1967Proxy.sol:L15 @ basenames@1809bbc)
/// (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/proxy/ERC1967/ERC1967Proxy.sol:L26 @ basenames@1809bbc)
/// (upstream: .refs/basenames/script/configure/EstablishController.s.sol:L15 @ basenames@1809bbc)
/// (upstream: .refs/basenames/script/configure/EstablishController.s.sol:L18 @ basenames@1809bbc)
/// (upstream: .refs/basenames/test/Integration/SwitchToUpgradeableRegistrarController.t.sol:L66 @ basenames@1809bbc)
pub async fn deploy_upgradeable_registrar_controller(
    rpc: &RpcClient,
    repo_root: &Path,
    d: &mut BasenamesDeployment,
) -> Result<()> {
    if d.upgradeable_registrar_controller.is_some()
        || d.upgradeable_registrar_controller_implementation.is_some()
    {
        bail!("UpgradeableRegistrarController already deployed");
    }

    let implementation = deploy(
        rpc,
        d.deployer,
        &forge(repo_root, "UpgradeableRegistrarController")?,
        &[],
    )
    .await?;
    let initialize = upgradeable_controller_calls::initializeCall {
        base: d.base_registrar.address,
        prices: d.price_oracle.address,
        reverseRegistrar: d.helper_reverse_registrar.address,
        owner: d.deployer,
        rootNode: namehash("base.eth"),
        rootName: ".base.eth".to_owned(),
        paymentReceiver: d.deployer,
        legacyRegistrarController: d.registrar_controller.address,
        legacyL2Resolver: d.l2_resolver.address,
        l2ReverseRegistrar: d.primary_reverse_registrar.address,
    }
    .abi_encode();
    let proxy = deploy(
        rpc,
        d.deployer,
        &forge(repo_root, "ERC1967Proxy")?,
        &(implementation.address, Bytes::copy_from_slice(&initialize)).abi_encode_params(),
    )
    .await?;

    send_checked(
        rpc,
        d.deployer,
        d.base_registrar.address,
        &addControllerCall {
            controller: proxy.address,
        }
        .abi_encode(),
    )
    .await?;
    send_checked(
        rpc,
        d.deployer,
        d.helper_reverse_registrar.address,
        &setControllerApprovalCall {
            controller: proxy.address,
            approved: true,
        }
        .abi_encode(),
    )
    .await?;
    send_checked(
        rpc,
        d.deployer,
        d.l2_resolver.address,
        &upgradeable_controller_calls::setRegistrarControllerCall {
            registrarController: proxy.address,
        }
        .abi_encode(),
    )
    .await?;

    d.upgradeable_registrar_controller = Some(proxy);
    d.upgradeable_registrar_controller_implementation = Some(implementation);
    Ok(())
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
    send_checked_receipt(rpc, from, to, data, U256::ZERO)
        .await
        .map(|_| ())
}

async fn send_checked_receipt(
    rpc: &RpcClient,
    from: Address,
    to: Address,
    data: &[u8],
    value: U256,
) -> Result<TxReceipt> {
    let receipt = rpc.send_transaction(from, Some(to), data, value).await?;
    if !receipt.status_ok {
        bail!("call to {to} reverted (tx {})", receipt.tx_hash);
    }
    Ok(receipt)
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
        register_tx_hash: receipt.tx_hash,
    })
}

pub async fn legacy_base_rent_price(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    label: &str,
    duration_secs: u64,
) -> Result<(U256, U256)> {
    controller_rent_price(rpc, d.registrar_controller.address, label, duration_secs).await
}

/// Renew a Basename through the legacy controller. Its three-argument
/// NameRenewed event is emitted by the controller while BaseRegistrar emits
/// its token-id lifecycle event.
/// (upstream: .refs/basenames/src/L2/RegistrarController.sol:L486 @ basenames@1809bbc)
pub async fn renew_base_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    label: &str,
    duration_secs: u64,
) -> Result<TxReceipt> {
    renew_through_controller(
        rpc,
        d.registrar_controller.address,
        from,
        label,
        duration_secs,
    )
    .await
}

pub async fn register_upgradeable_base_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    label: &str,
    owner: Address,
    duration_secs: u64,
) -> Result<RegisteredBasename> {
    let controller = upgradeable_controller(d)?;
    let price = controller_register_price(rpc, controller.address, label, duration_secs).await?;
    let request = upgradeable_controller_calls::RegisterRequest {
        name: label.to_owned(),
        owner,
        duration: U256::from(duration_secs),
        resolver: d.l2_resolver.address,
        data: Vec::<Bytes>::new(),
        reverseRecord: false,
        coinTypes: Vec::<U256>::new(),
        signatureExpiry: U256::ZERO,
        signature: Bytes::new(),
    };
    let receipt = send_checked_receipt(
        rpc,
        from,
        controller.address,
        &upgradeable_controller_calls::registerCall { request }.abi_encode(),
        price,
    )
    .await
    .with_context(|| format!("register {label}.base.eth through upgradeable controller"))?;
    Ok(RegisteredBasename {
        label: label.to_owned(),
        owner,
        register_block: receipt.block_number,
        register_tx_hash: receipt.tx_hash,
    })
}

pub async fn renew_upgradeable_base_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    label: &str,
    duration_secs: u64,
) -> Result<TxReceipt> {
    renew_through_controller(
        rpc,
        upgradeable_controller(d)?.address,
        from,
        label,
        duration_secs,
    )
    .await
}

fn upgradeable_controller(d: &BasenamesDeployment) -> Result<&Deployed> {
    d.upgradeable_registrar_controller
        .as_ref()
        .context("UpgradeableRegistrarController has not been deployed")
}

async fn controller_register_price(
    rpc: &RpcClient,
    controller: Address,
    label: &str,
    duration_secs: u64,
) -> Result<U256> {
    let raw = rpc
        .eth_call(
            controller,
            &registerPriceCall {
                name: label.to_owned(),
                duration: U256::from(duration_secs),
            }
            .abi_encode(),
        )
        .await?;
    registerPriceCall::abi_decode_returns(&raw).context("registerPrice decode")
}

async fn controller_rent_price(
    rpc: &RpcClient,
    controller: Address,
    label: &str,
    duration_secs: u64,
) -> Result<(U256, U256)> {
    let raw = rpc
        .eth_call(
            controller,
            &rentPriceCall {
                name: label.to_owned(),
                duration: U256::from(duration_secs),
            }
            .abi_encode(),
        )
        .await?;
    let price = rentPriceCall::abi_decode_returns(&raw).context("rentPrice decode")?;
    Ok((price.base, price.premium))
}

async fn renew_through_controller(
    rpc: &RpcClient,
    controller: Address,
    from: Address,
    label: &str,
    duration_secs: u64,
) -> Result<TxReceipt> {
    let (base, _) = controller_rent_price(rpc, controller, label, duration_secs).await?;
    send_checked_receipt(
        rpc,
        from,
        controller,
        &renewCall {
            name: label.to_owned(),
            duration: U256::from(duration_secs),
        }
        .abi_encode(),
        base,
    )
    .await
    .with_context(|| format!("renew {label}.base.eth through controller {controller}"))
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

pub async fn set_base_subnode_owner(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    parent_name: &str,
    label: &str,
    owner: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.registry.address,
        &registry_calls::setSubnodeOwnerCall {
            node: namehash(parent_name),
            label: labelhash(label),
            owner,
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_base_registry_resolver(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    name: &str,
    resolver: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        d.registry.address,
        &registry_calls::setResolverCall {
            node: namehash(name),
            resolver,
        }
        .abi_encode(),
    )
    .await
}

/// Deploy the same L2Resolver source with a distinct immutable registry and
/// deliberately omit it from the local manifest targets. The separate
/// Registry gives the scenario an honest authorization hierarchy without
/// making that registry authoritative.
/// (upstream: .refs/basenames/src/L2/Registry.sol:L63 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/L2Resolver.sol:L113 @ basenames@1809bbc)
pub async fn deploy_unadmitted_l2_resolver(
    rpc: &RpcClient,
    repo_root: &Path,
    d: &BasenamesDeployment,
    name: &str,
    owner: Address,
) -> Result<UnadmittedL2ResolverDeployment> {
    let registry = deploy(
        rpc,
        d.deployer,
        &forge(repo_root, "Registry")?,
        &(d.deployer,).abi_encode_params(),
    )
    .await?;
    for (parent, label, child_owner) in [
        ("", "eth", d.deployer),
        ("eth", "base", d.deployer),
        ("base.eth", base_label(name)?, owner),
    ] {
        send_checked(
            rpc,
            d.deployer,
            registry.address,
            &registry_calls::setSubnodeOwnerCall {
                node: namehash(parent),
                label: labelhash(label),
                owner: child_owner,
            }
            .abi_encode(),
        )
        .await?;
    }
    let resolver = deploy(
        rpc,
        d.deployer,
        &forge(repo_root, "L2Resolver")?,
        &(
            registry.address,
            d.registrar_controller.address,
            d.helper_reverse_registrar.address,
            d.deployer,
        )
            .abi_encode_params(),
    )
    .await?;
    Ok(UnadmittedL2ResolverDeployment { registry, resolver })
}

fn base_label(name: &str) -> Result<&str> {
    let label = name.strip_suffix(".base.eth").unwrap_or(name);
    if label.is_empty() || label.contains('.') {
        bail!("expected a Basenames 2LD label or <label>.base.eth, got {name}");
    }
    Ok(label)
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

pub async fn set_base_text_record(
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
        &resolver_calls::setTextCall {
            node: namehash(name),
            key: key.to_owned(),
            value: value.to_owned(),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_base_multicoin_addr_record(
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

pub async fn set_base_name_record(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
    value: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &resolver_calls::setNameCall {
            node: namehash(name),
            newName: value.to_owned(),
        }
        .abi_encode(),
    )
    .await
}

pub async fn clear_base_records(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    name: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &resolver_calls::clearRecordsCall {
            node: namehash(name),
        }
        .abi_encode(),
    )
    .await
}

pub async fn set_base_contenthash_record(
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
        &resolver_calls::setContenthashCall {
            node: namehash(name),
            hash: Bytes::copy_from_slice(contenthash),
        }
        .abi_encode(),
    )
    .await
}

pub async fn claim_legacy_base_reverse(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    address: Address,
    owner: Address,
    resolver: Address,
) -> Result<TxReceipt> {
    send_checked_receipt(
        rpc,
        from,
        d.helper_reverse_registrar.address,
        &reverse_calls::claimForBaseAddrCall {
            addr: address,
            owner,
            resolver,
        }
        .abi_encode(),
        U256::ZERO,
    )
    .await
}

pub async fn set_legacy_base_reverse_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    from: Address,
    address: Address,
    owner: Address,
    resolver: Address,
    name: &str,
) -> Result<TxReceipt> {
    send_checked_receipt(
        rpc,
        from,
        d.helper_reverse_registrar.address,
        &reverse_calls::setNameForAddrCall {
            addr: address,
            owner,
            resolver,
            name: name.to_owned(),
        }
        .abi_encode(),
        U256::ZERO,
    )
    .await
}

pub fn base_reverse_node_for(address: Address) -> B256 {
    namehash(&format!("{address:x}.80002105.reverse"))
}

pub async fn add_base_controller(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
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

pub async fn direct_register_base_name(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    controller: Address,
    label: &str,
    owner: Address,
    duration_secs: u64,
) -> Result<TxReceipt> {
    send_checked_receipt(
        rpc,
        controller,
        d.base_registrar.address,
        &direct_registrar_calls::registerCall {
            id: U256::from_be_bytes(labelhash(label).0),
            owner,
            duration: U256::from(duration_secs),
        }
        .abi_encode(),
        U256::ZERO,
    )
    .await
}

pub async fn direct_register_base_name_only(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    controller: Address,
    label: &str,
    owner: Address,
    duration_secs: u64,
) -> Result<TxReceipt> {
    send_checked_receipt(
        rpc,
        controller,
        d.base_registrar.address,
        &direct_registrar_calls::registerOnlyCall {
            id: U256::from_be_bytes(labelhash(label).0),
            owner,
            duration: U256::from(duration_secs),
        }
        .abi_encode(),
        U256::ZERO,
    )
    .await
}

pub async fn base_registry_owner(
    rpc: &RpcClient,
    d: &BasenamesDeployment,
    name: &str,
) -> Result<Address> {
    let raw = rpc
        .eth_call(
            d.registry.address,
            &registry_calls::ownerCall {
                node: namehash(name),
            }
            .abi_encode(),
        )
        .await?;
    registry_calls::ownerCall::abi_decode_returns(&raw).context("Registry.owner decode")
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
        let mut targets = std::collections::HashMap::from([
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
        ]);
        if let Some(controller) = &self.upgradeable_registrar_controller {
            targets.insert(
                "upgradeable_registrar_controller",
                (controller.address, controller.block_number),
            );
        }
        if let Some(implementation) = &self.upgradeable_registrar_controller_implementation {
            targets.insert(
                "upgradeable_registrar_controller_implementation",
                (implementation.address, implementation.block_number),
            );
        }
        targets
    }
}

fn base_reverse_node() -> B256 {
    // Basenames scripts wire the ENSIP-19 Base reverse namespace as
    // `80002105.reverse`.
    // (upstream: .refs/basenames/src/util/Constants.sol:L10 @ basenames@1809bbc)
    namehash("80002105.reverse")
}
