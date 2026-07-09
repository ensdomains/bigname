use std::path::Path;

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Context, Result, bail};

use super::artifacts::{Deployed, deploy, load_ens_v1_artifact};
use super::rpc::RpcClient;

// Call fragments match the pinned upstream sources:
// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L39 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L45 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L23 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IETHRegistrarController.sol:L7 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L210 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L352 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/IPriceOracle.sol:L5 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L26 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/ContenthashResolver.sol:L14 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L15 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L20 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f)
sol! {
    function setSubnodeOwner(bytes32 node, bytes32 label, address owner) external returns (bytes32);
    function owner(bytes32 node) external view returns (address);
    function setResolver(bytes32 node, address resolver) external;
    function addController(address controller) external;
    function reclaim(uint256 id, address owner) external;
    function transferFrom(address from, address to, uint256 tokenId) external;
    function renew(string label, uint256 duration, bytes32 referrer) external payable;
    function setAddr(bytes32 node, address a) external;
    function setContenthash(bytes32 node, bytes hash) external;
    function setText(bytes32 node, string key, string value) external;
    function clearRecords(bytes32 node) external;

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

/// Transfer the registrar token for `<label>.eth` and reclaim registry
/// ownership for the new holder.
pub async fn transfer_eth_name(
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
    .await?;
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
