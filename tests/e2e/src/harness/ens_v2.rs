use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::{Context, Result, bail};

use super::artifacts::{Deployed, deploy, load_ens_v2_artifact};
use super::rpc::{RpcClient, TxReceipt};

const MIN_COMMITMENT_AGE: u64 = 60;
const MAX_COMMITMENT_AGE: u64 = 86_400;
pub const MIN_REGISTER_DURATION: u64 = 2_419_200;

const ROLE_REGISTRAR: usize = 0;
pub const ROLE_UNREGISTER: usize = 12;
pub const ROLE_SET_PARENT: usize = 8;
pub const ROLE_RENEW: usize = 16;
pub const ROLE_SET_SUBREGISTRY: usize = 20;
pub const ROLE_SET_RESOLVER: usize = 24;

mod erc20_calls {
    use alloy_sol_types::sol;

    sol! {
        function mint(address to, uint256 amount) external;
        function approve(address spender, uint256 value) external returns (bool);
    }
}

mod registrar_calls {
    use alloy_sol_types::sol;

    sol! {
        function commit(bytes32 commitment) external;
        function makeCommitment(
            string label,
            address owner,
            bytes32 secret,
            address subregistry,
            address resolver,
            uint64 duration,
            bytes32 referrer
        ) external pure returns (bytes32);
        function register(
            string label,
            address owner,
            bytes32 secret,
            address subregistry,
            address resolver,
            uint64 duration,
            address paymentToken,
            bytes32 referrer
        ) external returns (uint256);
        function rentPrice(
            string label,
            address owner,
            uint64 duration,
            address paymentToken
        ) external view returns (uint256 base, uint256 premium);
    }
}

mod registry_calls {
    use alloy_sol_types::sol;

    sol! {
        function register(
            string label,
            address owner,
            address registry,
            address resolver,
            uint256 roleBitmap,
            uint64 expiry
        ) external returns (uint256);
        function unregister(uint256 anyId) external;
        function grantRoles(uint256 anyId, uint256 roleBitmap, address account) external returns (bool);
        function grantRootRoles(uint256 roleBitmap, address account) external returns (bool);
        function revokeRoles(uint256 anyId, uint256 roleBitmap, address account) external returns (bool);
        function revokeRootRoles(uint256 roleBitmap, address account) external returns (bool);
        function setSubregistry(uint256 anyId, address registry) external;
        function setResolver(uint256 anyId, address resolver) external;
        function setParent(address parent, string label) external;
        function getTokenId(uint256 anyId) external view returns (uint256);
        function getResource(uint256 anyId) external view returns (uint256);
    }
}

/// Local ENSv2 sepolia-dev deployment from pinned hardhat artifacts.
///
/// Constructor signatures are pinned in upstream sources:
/// - `SimpleRegistryMetadata(IHCAFactoryBasic)`
///   (upstream: .refs/ens_v2/contracts/src/registry/SimpleRegistryMetadata.sol:L37 @ ens_v2@554c309)
/// - `PermissionedRegistry(IHCAFactoryBasic,IRegistryMetadata,address,uint256)`
///   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L96 @ ens_v2@554c309)
/// - `MockERC20(string,uint8,IHCAFactoryBasic)`
///   (upstream: .refs/ens_v2/contracts/test/mocks/MockERC20.sol:L21 @ ens_v2@554c309)
/// - `StandardRentPriceOracle(address,IPermissionedRegistry,uint256[],DiscountPoint[],uint256,uint64,uint64,PaymentRatio[])`
///   (upstream: .refs/ens_v2/contracts/src/registrar/StandardRentPriceOracle.sol:L124 @ ens_v2@554c309)
/// - `ETHRegistrar(IPermissionedRegistry,IHCAFactoryBasic,address,uint64,uint64,uint64,IRentPriceOracle)`
///   (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L88 @ ens_v2@554c309)
pub struct EnsV2Deployment {
    pub deployer: Address,
    pub hca_factory: Deployed,
    pub metadata: Deployed,
    pub root_registry: Deployed,
    pub eth_registry: Deployed,
    pub eth_registrar: Deployed,
    pub mock_usdc: Deployed,
    pub mock_dai: Deployed,
}

pub struct RegisteredEnsV2Name {
    pub token_id: U256,
    pub resource_id: U256,
    pub register_block: u64,
}

pub struct RegisterEthName<'a> {
    pub from: Address,
    pub label: &'a str,
    pub owner: Address,
    pub duration_secs: u64,
    pub subregistry: Address,
    pub resolver: Address,
}

struct CommitmentRequest<'a> {
    label: &'a str,
    owner: Address,
    secret: B256,
    subregistry: Address,
    resolver: Address,
    duration_secs: u64,
    referrer: B256,
}

impl EnsV2Deployment {
    pub fn manifest_targets(&self) -> HashMap<&str, (Address, u64)> {
        HashMap::from([
            (
                "RootRegistry",
                (self.root_registry.address, self.root_registry.block_number),
            ),
            (
                "root_registry",
                (self.root_registry.address, self.root_registry.block_number),
            ),
            (
                "ETHRegistry",
                (self.eth_registry.address, self.eth_registry.block_number),
            ),
            (
                "registry",
                (self.eth_registry.address, self.eth_registry.block_number),
            ),
            (
                "ETHRegistrar",
                (self.eth_registrar.address, self.eth_registrar.block_number),
            ),
            (
                "registrar",
                (self.eth_registrar.address, self.eth_registrar.block_number),
            ),
        ])
    }
}

pub async fn deploy_ens_v2(rpc: &RpcClient, repo_root: &Path) -> Result<EnsV2Deployment> {
    let accounts = rpc.accounts().await?;
    let deployer = *accounts.first().context("anvil exposes no accounts")?;

    let hca_factory = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "HCAFactory")?,
        &[],
    )
    .await?;
    let metadata = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "SimpleRegistryMetadata")?,
        &(hca_factory.address,).abi_encode_params(),
    )
    .await?;

    let root_registry = deploy_registry(
        rpc,
        repo_root,
        deployer,
        "RootRegistry",
        hca_factory.address,
        metadata.address,
    )
    .await?;
    let eth_registry = deploy_registry(
        rpc,
        repo_root,
        deployer,
        "ETHRegistry",
        hca_factory.address,
        metadata.address,
    )
    .await?;

    let mock_usdc = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "MockUSDC")?,
        &("USDC".to_owned(), U256::from(6_u8), hca_factory.address).abi_encode_params(),
    )
    .await?;
    let mock_dai = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "MockDAI")?,
        &("DAI".to_owned(), U256::from(18_u8), hca_factory.address).abi_encode_params(),
    )
    .await?;

    let price_oracle = deploy_price_oracle(
        rpc,
        repo_root,
        deployer,
        eth_registry.address,
        mock_usdc.address,
        mock_dai.address,
    )
    .await?;
    let eth_registrar = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "ETHRegistrar")?,
        &(
            eth_registry.address,
            hca_factory.address,
            deployer,
            MIN_COMMITMENT_AGE,
            MAX_COMMITMENT_AGE,
            MIN_REGISTER_DURATION,
            price_oracle.address,
        )
            .abi_encode_params(),
    )
    .await?;

    // Root-resource grants use the dedicated entrypoint: grantRoles rejects
    // ROOT_RESOURCE directly
    // (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L30 @ ens_v2@554c309).
    send_checked(
        rpc,
        deployer,
        eth_registry.address,
        // Upstream grants REGISTRAR | RENEW so registrar renewals can move
        // registry expiry
        // (upstream: .refs/ens_v2/contracts/deploy/03_ETHRegistrar.ts:L45 @ ens_v2@554c309).
        &registry_calls::grantRootRolesCall {
            roleBitmap: role_bit(ROLE_REGISTRAR) | role_bit(ROLE_RENEW),
            account: eth_registrar.address,
        }
        .abi_encode(),
        "grant ENSv2 registrar root roles",
    )
    .await?;

    Ok(EnsV2Deployment {
        deployer,
        hca_factory,
        metadata,
        root_registry,
        eth_registry,
        eth_registrar,
        mock_usdc,
        mock_dai,
    })
}

/// Upstream's "grant all roles" bitmap: role bits occupy every fourth bit
/// (one nibble per role), so a full grant is the repeating 0x1 nibble — not
/// U256::MAX, whose off-pattern bits the access-control layer rejects
/// (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L11 @ ens_v2@554c309).
fn all_roles() -> U256 {
    U256::from_str_radix(
        "1111111111111111111111111111111111111111111111111111111111111111",
        16,
    )
    .expect("static role bitmap")
}

async fn deploy_registry(
    rpc: &RpcClient,
    repo_root: &Path,
    deployer: Address,
    artifact_name: &str,
    hca_factory: Address,
    metadata: Address,
) -> Result<Deployed> {
    deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, artifact_name)?,
        &(hca_factory, metadata, deployer, all_roles()).abi_encode_params(),
    )
    .await
}

async fn deploy_price_oracle(
    rpc: &RpcClient,
    repo_root: &Path,
    deployer: Address,
    eth_registry: Address,
    mock_usdc: Address,
    mock_dai: Address,
) -> Result<Deployed> {
    let base_rates = vec![
        U256::ZERO,
        U256::ZERO,
        U256::from(20_280_377_u64),
        U256::from(5_070_095_u64),
        U256::from(158_441_u64),
    ];
    let discount_points = vec![(31_557_600_u64, 0_u128); 6];
    let payment_ratios = vec![
        (mock_usdc, 1_u128, 1_000_000_u128),
        (mock_dai, 1_000_000_u128, 1_u128),
    ];
    deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "StandardRentPriceOracle")?,
        &(
            deployer,
            eth_registry,
            base_rates,
            discount_points,
            U256::from(100_u64) * U256::from(10_u8).pow(U256::from(18_u8)),
            86_400_u64,
            1_814_400_u64,
            payment_ratios,
        )
            .abi_encode_params(),
    )
    .await
}

pub async fn register_eth_name(
    rpc: &RpcClient,
    d: &EnsV2Deployment,
    request: RegisterEthName<'_>,
) -> Result<RegisteredEnsV2Name> {
    let RegisterEthName {
        from,
        label,
        owner,
        duration_secs,
        subregistry,
        resolver,
    } = request;
    let secret_material = format!("bigname-e2e-ens-v2:{label}:{owner:#x}");
    let secret = keccak256(secret_material.as_bytes());
    let referrer = B256::ZERO;
    let commitment = make_commitment(
        rpc,
        d.eth_registrar.address,
        &CommitmentRequest {
            label,
            owner,
            secret,
            subregistry,
            resolver,
            duration_secs,
            referrer,
        },
    )
    .await?;
    send_checked(
        rpc,
        from,
        d.eth_registrar.address,
        &registrar_calls::commitCall { commitment }.abi_encode(),
        &format!("commit {label}.eth"),
    )
    .await?;
    rpc.increase_time(MIN_COMMITMENT_AGE + 1).await?;

    let (base, premium) = rent_price(rpc, d, label, owner, duration_secs).await?;
    let cost = base + premium;
    let mint_amount = if cost == U256::ZERO {
        U256::from(1_000_000_u64)
    } else {
        cost * U256::from(2_u8)
    };
    send_checked(
        rpc,
        d.deployer,
        d.mock_usdc.address,
        &erc20_calls::mintCall {
            to: from,
            amount: mint_amount,
        }
        .abi_encode(),
        &format!("mint USDC for {label}.eth"),
    )
    .await?;
    send_checked(
        rpc,
        from,
        d.mock_usdc.address,
        &erc20_calls::approveCall {
            spender: d.eth_registrar.address,
            value: U256::MAX,
        }
        .abi_encode(),
        &format!("approve registrar payment for {label}.eth"),
    )
    .await?;

    let receipt = rpc
        .send_transaction(
            from,
            Some(d.eth_registrar.address),
            &registrar_calls::registerCall {
                label: label.to_owned(),
                owner,
                secret,
                subregistry,
                resolver,
                duration: duration_secs,
                paymentToken: d.mock_usdc.address,
                referrer,
            }
            .abi_encode(),
            U256::ZERO,
        )
        .await?;
    if !receipt.status_ok {
        bail!(
            "ENSv2 register {label}.eth reverted (tx {})",
            receipt.tx_hash
        );
    }
    let token_id = token_id(rpc, d.eth_registry.address, label_id(label)).await?;
    let resource_id = resource_id(rpc, d.eth_registry.address, label_id(label)).await?;
    Ok(RegisteredEnsV2Name {
        token_id,
        resource_id,
        register_block: receipt.block_number,
    })
}

async fn make_commitment(
    rpc: &RpcClient,
    registrar: Address,
    request: &CommitmentRequest<'_>,
) -> Result<B256> {
    let raw = rpc
        .eth_call(
            registrar,
            &registrar_calls::makeCommitmentCall {
                label: request.label.to_owned(),
                owner: request.owner,
                secret: request.secret,
                subregistry: request.subregistry,
                resolver: request.resolver,
                duration: request.duration_secs,
                referrer: request.referrer,
            }
            .abi_encode(),
        )
        .await?;
    registrar_calls::makeCommitmentCall::abi_decode_returns(&raw)
        .context("decode ETHRegistrar.makeCommitment return")
}

async fn rent_price(
    rpc: &RpcClient,
    d: &EnsV2Deployment,
    label: &str,
    owner: Address,
    duration: u64,
) -> Result<(U256, U256)> {
    let raw = rpc
        .eth_call(
            d.eth_registrar.address,
            &registrar_calls::rentPriceCall {
                label: label.to_owned(),
                owner,
                duration,
                paymentToken: d.mock_usdc.address,
            }
            .abi_encode(),
        )
        .await?;
    let decoded = registrar_calls::rentPriceCall::abi_decode_returns(&raw)
        .context("decode ETHRegistrar.rentPrice return")?;
    Ok((decoded.base, decoded.premium))
}

pub async fn deploy_child_registry(
    rpc: &RpcClient,
    repo_root: &Path,
    d: &EnsV2Deployment,
) -> Result<Deployed> {
    deploy(
        rpc,
        d.deployer,
        &load_ens_v2_artifact(repo_root, "ETHRegistry")?,
        &(
            d.hca_factory.address,
            d.metadata.address,
            d.deployer,
            all_roles(),
        )
            .abi_encode_params(),
    )
    .await
}

pub async fn register_in_registry(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    label: &str,
    owner: Address,
    expiry: u64,
) -> Result<U256> {
    let receipt = rpc
        .send_transaction(
            from,
            Some(registry),
            &registry_calls::registerCall {
                label: label.to_owned(),
                owner,
                registry: Address::ZERO,
                resolver: Address::ZERO,
                roleBitmap: role_bit(ROLE_SET_SUBREGISTRY)
                    | role_bit(ROLE_SET_RESOLVER)
                    | role_bit(ROLE_UNREGISTER),
                expiry,
            }
            .abi_encode(),
            U256::ZERO,
        )
        .await?;
    if !receipt.status_ok {
        bail!(
            "ENSv2 registry register {label} reverted (tx {})",
            receipt.tx_hash
        );
    }
    token_id(rpc, registry, label_id(label)).await
}

pub async fn attach_subregistry(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    any_id: U256,
    subregistry: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::setSubregistryCall {
            anyId: any_id,
            registry: subregistry,
        }
        .abi_encode(),
        "set ENSv2 subregistry",
    )
    .await
}

pub async fn set_parent(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    parent: Address,
    label: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::setParentCall {
            parent,
            label: label.to_owned(),
        }
        .abi_encode(),
        &format!("set ENSv2 parent for {label}"),
    )
    .await
}

pub async fn grant_roles(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    any_id: U256,
    role_bitmap: U256,
    account: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::grantRolesCall {
            anyId: any_id,
            roleBitmap: role_bitmap,
            account,
        }
        .abi_encode(),
        "grant ENSv2 registry roles",
    )
    .await
}

pub async fn revoke_roles(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    any_id: U256,
    role_bitmap: U256,
    account: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::revokeRolesCall {
            anyId: any_id,
            roleBitmap: role_bitmap,
            account,
        }
        .abi_encode(),
        "revoke ENSv2 registry roles",
    )
    .await
}

pub async fn unregister(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    any_id: U256,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::unregisterCall { anyId: any_id }.abi_encode(),
        "unregister ENSv2 label",
    )
    .await
}

pub async fn token_id(rpc: &RpcClient, registry: Address, any_id: U256) -> Result<U256> {
    let raw = rpc
        .eth_call(
            registry,
            &registry_calls::getTokenIdCall { anyId: any_id }.abi_encode(),
        )
        .await?;
    registry_calls::getTokenIdCall::abi_decode_returns(&raw)
        .context("decode PermissionedRegistry.getTokenId return")
}

pub async fn resource_id(rpc: &RpcClient, registry: Address, any_id: U256) -> Result<U256> {
    let raw = rpc
        .eth_call(
            registry,
            &registry_calls::getResourceCall { anyId: any_id }.abi_encode(),
        )
        .await?;
    registry_calls::getResourceCall::abi_decode_returns(&raw)
        .context("decode PermissionedRegistry.getResource return")
}

pub fn label_id(label: &str) -> U256 {
    U256::from_be_bytes(keccak256(label.as_bytes()).0)
}

pub fn role_bit(bit: usize) -> U256 {
    U256::from(1_u8) << bit
}

/// Admin-half counterpart of a role bit
/// (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L11 @ ens_v2@554c309).
pub fn admin_role_bit(bit: usize) -> U256 {
    role_bit(bit) << 128
}

mod erc1155_calls {
    use alloy_sol_types::sol;

    // The registry token is an ERC1155 singleton
    // (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L230 @ ens_v2@554c309).
    sol! {
        function safeTransferFrom(address from, address to, uint256 id, uint256 value, bytes data) external;
    }
}

mod renew_calls {
    use alloy_sol_types::sol;

    // Registrar renew pays and forwards; direct registry renew moves expiry
    // only and rejects reduction.
    // (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L196 @ ens_v2@554c309)
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249 @ ens_v2@554c309)
    sol! {
        function renew(string label, uint64 duration, address paymentToken, bytes32 referrer) external;
    }
}

mod registry_renew_calls {
    use alloy_sol_types::sol;

    sol! {
        function renew(uint256 anyId, uint64 newExpiry) external;
    }
}

/// Renew `<label>.eth` through the registrar, funding the payer with mock
/// USDC first.
pub async fn renew_eth_name(
    rpc: &RpcClient,
    d: &EnsV2Deployment,
    from: Address,
    label: &str,
    duration_secs: u64,
) -> Result<TxReceipt> {
    send_checked(
        rpc,
        d.deployer,
        d.mock_usdc.address,
        &erc20_calls::mintCall {
            to: from,
            amount: U256::from(10_u64).pow(U256::from(12_u8)),
        }
        .abi_encode(),
        &format!("mint USDC for {label}.eth renewal"),
    )
    .await?;
    send_checked(
        rpc,
        from,
        d.mock_usdc.address,
        &erc20_calls::approveCall {
            spender: d.eth_registrar.address,
            value: U256::MAX,
        }
        .abi_encode(),
        &format!("approve registrar renewal for {label}.eth"),
    )
    .await?;
    let receipt = rpc
        .send_transaction(
            from,
            Some(d.eth_registrar.address),
            &renew_calls::renewCall {
                label: label.to_owned(),
                duration: duration_secs,
                paymentToken: d.mock_usdc.address,
                referrer: B256::ZERO,
            }
            .abi_encode(),
            U256::ZERO,
        )
        .await?;
    if !receipt.status_ok {
        bail!(
            "ENSv2 registrar renew {label} reverted (tx {})",
            receipt.tx_hash
        );
    }
    Ok(receipt)
}

/// Direct registry renew; returns the receipt without asserting success so
/// callers can pin the reduction revert.
pub async fn renew_in_registry(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    any_id: U256,
    new_expiry: u64,
) -> Result<TxReceipt> {
    rpc.send_transaction(
        from,
        Some(registry),
        &registry_renew_calls::renewCall {
            anyId: any_id,
            newExpiry: new_expiry,
        }
        .abi_encode(),
        U256::ZERO,
    )
    .await
}

pub async fn set_resolver_in_registry(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    any_id: U256,
    resolver: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::setResolverCall {
            anyId: any_id,
            resolver,
        }
        .abi_encode(),
        "ENSv2 setResolver",
    )
    .await
}

pub async fn transfer_registry_token(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    to: Address,
    token_id: U256,
) -> Result<TxReceipt> {
    let receipt = rpc
        .send_transaction(
            from,
            Some(registry),
            &erc1155_calls::safeTransferFromCall {
                from,
                to,
                id: token_id,
                value: U256::from(1_u8),
                data: Bytes::new(),
            }
            .abi_encode(),
            U256::ZERO,
        )
        .await?;
    if !receipt.status_ok {
        bail!("ENSv2 token transfer reverted (tx {})", receipt.tx_hash);
    }
    Ok(receipt)
}

pub async fn grant_root_roles(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    role_bitmap: U256,
    account: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::grantRootRolesCall {
            roleBitmap: role_bitmap,
            account,
        }
        .abi_encode(),
        "ENSv2 grantRootRoles",
    )
    .await
}

/// Revoke root-scope roles
/// (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L158 @ ens_v2@554c309).
pub async fn revoke_root_roles(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    role_bitmap: U256,
    account: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        registry,
        &registry_calls::revokeRootRolesCall {
            roleBitmap: role_bitmap,
            account,
        }
        .abi_encode(),
        "ENSv2 revokeRootRoles",
    )
    .await
}

/// Direct registry register with explicit role bitmap, registry, resolver,
/// and expiry — reservations must pass owner=0 with an empty bitmap
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L184 @ ens_v2@554c309).
#[allow(clippy::too_many_arguments)]
pub async fn register_in_registry_with(
    rpc: &RpcClient,
    registry: Address,
    from: Address,
    label: &str,
    owner: Address,
    role_bitmap: U256,
    resolver: Address,
    expiry: u64,
) -> Result<TxReceipt> {
    rpc.send_transaction(
        from,
        Some(registry),
        &registry_calls::registerCall {
            label: label.to_owned(),
            owner,
            registry: Address::ZERO,
            resolver,
            roleBitmap: role_bitmap,
            expiry,
        }
        .abi_encode(),
        U256::ZERO,
    )
    .await
}

async fn send_checked(
    rpc: &RpcClient,
    from: Address,
    to: Address,
    data: &[u8],
    description: &str,
) -> Result<()> {
    let receipt = rpc
        .send_transaction(from, Some(to), data, U256::ZERO)
        .await?;
    if !receipt.status_ok {
        bail!("{description} reverted at {to:#x} (tx {})", receipt.tx_hash);
    }
    Ok(())
}

mod resolver_calls {
    use alloy_sol_types::sol;

    // Writable v2 resolver (UUPS impl deployed directly for tests):
    // constructor(hcaFactory) then initialize(admin, roleBitmap)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L223 @ ens_v2@554c309).
    sol! {
        function initialize(address admin, uint256 roleBitmap) external;
        function setText(bytes32 node, string key, string value) external;
        function clearRecords(bytes32 node) external;
    }
}

mod resolver_addr_calls {
    use alloy_sol_types::sol;

    sol! {
        function setAddr(bytes32 node, address addr_) external;
    }
}

mod factory_calls {
    use alloy_sol_types::sol;

    // User resolvers deploy behind VerifiableFactory proxies — the raw
    // implementation disables its initializers
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L177 @ ens_v2@554c309).
    sol! {
        function deployProxy(address implementation, uint256 salt, bytes data) external returns (address);
    }
}

/// Deploy the writable PermissionedResolver behind a VerifiableFactory
/// proxy, initializing the admin with the full role bitmap.
pub async fn deploy_permissioned_resolver(
    rpc: &RpcClient,
    repo_root: &Path,
    d: &EnsV2Deployment,
    admin: Address,
) -> Result<Deployed> {
    let implementation = deploy(
        rpc,
        d.deployer,
        &load_ens_v2_artifact(repo_root, "PermissionedResolverImpl")?,
        &(d.hca_factory.address,).abi_encode_params(),
    )
    .await?;
    let factory = deploy(
        rpc,
        d.deployer,
        &load_ens_v2_artifact(repo_root, "VerifiableFactory")?,
        &[],
    )
    .await?;
    let init_data = resolver_calls::initializeCall {
        admin,
        roleBitmap: all_roles(),
    }
    .abi_encode();
    let receipt = rpc
        .send_transaction(
            admin,
            Some(factory.address),
            &factory_calls::deployProxyCall {
                implementation: implementation.address,
                salt: U256::from(1_u8),
                data: init_data.into(),
            }
            .abi_encode(),
            U256::ZERO,
        )
        .await?;
    if !receipt.status_ok {
        bail!(
            "VerifiableFactory deployProxy reverted (tx {})",
            receipt.tx_hash
        );
    }
    let raw_receipt = rpc
        .call(
            "eth_getTransactionReceipt",
            serde_json::json!([receipt.tx_hash]),
        )
        .await?;
    let proxy_topic = format!(
        "{:#x}",
        keccak256("ProxyDeployed(address,address,uint256,address)".as_bytes())
    );
    let proxy = raw_receipt
        .get("logs")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|log| {
            let topics = log.get("topics")?.as_array()?;
            if topics.first()?.as_str()? != proxy_topic {
                return None;
            }
            let padded = topics.get(2)?.as_str()?;
            Address::parse_checksummed(format!("0x{}", &padded[padded.len() - 40..]), None)
                .ok()
                .or_else(|| format!("0x{}", &padded[padded.len() - 40..]).parse().ok())
        })
        .context("ProxyDeployed log missing from deployProxy receipt")?;
    Ok(Deployed {
        address: proxy,
        block_number: receipt.block_number,
    })
}

pub async fn set_resolver_text(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    node: B256,
    key: &str,
    value: &str,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &resolver_calls::setTextCall {
            node,
            key: key.to_owned(),
            value: value.to_owned(),
        }
        .abi_encode(),
        "v2 resolver setText",
    )
    .await
}

pub async fn set_resolver_addr(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    node: B256,
    addr: Address,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &resolver_addr_calls::setAddrCall { node, addr_: addr }.abi_encode(),
        "v2 resolver setAddr",
    )
    .await
}

pub async fn clear_resolver_records(
    rpc: &RpcClient,
    resolver: Address,
    from: Address,
    node: B256,
) -> Result<()> {
    send_checked(
        rpc,
        from,
        resolver,
        &resolver_calls::clearRecordsCall { node }.abi_encode(),
        "v2 resolver clearRecords",
    )
    .await
}
