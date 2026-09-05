use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::{Context, Result};

use super::artifacts::{Deployed, deploy, load_ens_v2_artifact};
use super::ens_v1::{self, EnsV1Deployment};
use super::ens_v2::{self, EnsV2Deployment};
use super::rpc::{RpcClient, TxReceipt};

// The archived controller requires ROLE_REGISTER_RESERVED, whose deployed
// generation defines it as 1 << 4.
// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/UnlockedMigrationController.sol:L18-L22 @ ens_v2_sepolia_20260629@ccaeb58)
// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/libraries/RegistryRolesLib.sol:L13-L16 @ ens_v2_sepolia_20260629@ccaeb58)
const ROLE_REGISTER_RESERVED_BIT: usize = 4;

mod calls {
    use alloy_sol_types::sol;

    sol! {
        struct MigrationData {
            string label;
            address owner;
            address subregistry;
            address resolver;
        }

        function clear(bytes[] names) external;
        function owner(bytes32 node) external view returns (address);
        function resolver(bytes32 node) external view returns (address);
        function getOwner(uint256 anyId) external view returns (address);
        function getSubregistry(string label) external view returns (address);
        function getExpiry(uint256 anyId) external view returns (uint64);
        function getWrappedNode() external view returns (bytes32);
    }
}

pub struct EnsV2MigrationDeployment {
    pub address_set: Deployed,
    pub verifiable_factory: Deployed,
    pub ens_v1_resolver: Deployed,
    pub graveyard: Deployed,
    pub wrapper_registry_implementation: Deployed,
    pub unlocked_migration_controller: Deployed,
    pub locked_migration_controller: Deployed,
}

/// Deploy the connected migration contracts from the admitted archived
/// Sepolia artifacts. Constructor wiring is pinned upstream.
/// (upstream: .refs/ens_v2/contracts/src/resolver/ENSV1Resolver.sol:L28-L30 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L73-L75 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L70-L89 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L56-L64 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L42-L57 @ ens_v2@a971bd64)
pub async fn deploy_ens_v2_migration(
    rpc: &RpcClient,
    repo_root: &Path,
    ens_v1: &EnsV1Deployment,
    ens_v2: &EnsV2Deployment,
) -> Result<EnsV2MigrationDeployment> {
    let deployer = ens_v2.deployer;
    let address_set = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "PublicResolverSet")?,
        &(deployer,).abi_encode_params(),
    )
    .await?;
    let verifiable_factory = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "VerifiableFactory")?,
        &[],
    )
    .await?;
    let ens_v1_resolver = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "ENSV1Resolver")?,
        &(Address::ZERO, address_set.address, ens_v1.registry.address).abi_encode_params(),
    )
    .await?;
    let graveyard = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "Graveyard")?,
        &(ens_v1.name_wrapper.address, address_set.address).abi_encode_params(),
    )
    .await?;
    let wrapper_registry_implementation = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "WrapperRegistryImpl")?,
        &(
            ens_v1.name_wrapper.address,
            graveyard.address,
            verifiable_factory.address,
            ens_v1_resolver.address,
            address_set.address,
            ens_v2.label_store.address,
            address_set.address,
            Address::ZERO,
            address_set.address,
        )
            .abi_encode_params(),
    )
    .await?;
    let unlocked_migration_controller = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "UnlockedMigrationController")?,
        &(
            ens_v1.name_wrapper.address,
            graveyard.address,
            ens_v2.eth_registry.address,
            address_set.address,
        )
            .abi_encode_params(),
    )
    .await?;
    let locked_migration_controller = deploy(
        rpc,
        deployer,
        &load_ens_v2_artifact(repo_root, "LockedMigrationController")?,
        &(
            ens_v1.name_wrapper.address,
            graveyard.address,
            ens_v2.eth_registry.address,
            verifiable_factory.address,
            wrapper_registry_implementation.address,
            address_set.address,
            Address::ZERO,
            address_set.address,
        )
            .abi_encode_params(),
    )
    .await?;

    let reserved_role = ens_v2::role_bit(ROLE_REGISTER_RESERVED_BIT);
    for controller in [
        unlocked_migration_controller.address,
        locked_migration_controller.address,
    ] {
        ens_v2::grant_root_roles(
            rpc,
            ens_v2.eth_registry.address,
            deployer,
            reserved_role,
            controller,
        )
        .await?;
    }
    ens_v1::add_registrar_controller(rpc, ens_v1, graveyard.address).await?;

    Ok(EnsV2MigrationDeployment {
        address_set,
        verifiable_factory,
        ens_v1_resolver,
        graveyard,
        wrapper_registry_implementation,
        unlocked_migration_controller,
        locked_migration_controller,
    })
}

pub fn migration_manifest_targets(
    deployment: &EnsV2MigrationDeployment,
) -> HashMap<&'static str, (Address, u64)> {
    HashMap::from([
        (
            "unlocked_migration_controller",
            (
                deployment.unlocked_migration_controller.address,
                deployment.unlocked_migration_controller.block_number,
            ),
        ),
        (
            "locked_migration_controller",
            (
                deployment.locked_migration_controller.address,
                deployment.locked_migration_controller.block_number,
            ),
        ),
        (
            "graveyard",
            (
                deployment.graveyard.address,
                deployment.graveyard.block_number,
            ),
        ),
        (
            "verifiable_factory",
            (
                deployment.verifiable_factory.address,
                deployment.verifiable_factory.block_number,
            ),
        ),
        (
            "wrapper_registry_implementation",
            (
                deployment.wrapper_registry_implementation.address,
                deployment.wrapper_registry_implementation.block_number,
            ),
        ),
        (
            "ens_v1_renewal_bridge",
            (placeholder("ens_v1_renewal_bridge"), 0),
        ),
        ("batch_registrar", (placeholder("batch_registrar"), 0)),
        ("migration_helper", (placeholder("migration_helper"), 0)),
    ])
}

pub fn migration_correlation_addresses(ens_v1: &EnsV1Deployment) -> HashMap<&'static str, Address> {
    HashMap::from([
        ("ens_v1_name_wrapper", ens_v1.name_wrapper.address),
        ("ens_v1_base_registrar", ens_v1.base_registrar.address),
    ])
}

fn placeholder(label: &str) -> Address {
    Address::from_slice(&keccak256(format!("bigname-e2e-placeholder:{label}"))[12..])
}

/// ABI encode the exact `LibMigration.Data` tuple.
/// (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L20-L31 @ ens_v2@a971bd64)
pub fn migration_data(label: &str, owner: Address) -> Bytes {
    Bytes::from(
        calls::MigrationData {
            label: label.to_owned(),
            owner,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        }
        .abi_encode(),
    )
}

pub async fn reserve_eth_label(
    rpc: &RpcClient,
    ens_v2: &EnsV2Deployment,
    label: &str,
    expiry: u64,
) -> Result<TxReceipt> {
    let receipt = ens_v2::register_in_registry_with(
        rpc,
        ens_v2.eth_registry.address,
        ens_v2.deployer,
        label,
        Address::ZERO,
        U256::ZERO,
        Address::ZERO,
        expiry,
    )
    .await?;
    anyhow::ensure!(receipt.status_ok, "ENSv2 reservation for {label} reverted");
    Ok(receipt)
}

pub async fn migrate_unlocked_wrapped(
    rpc: &RpcClient,
    ens_v1: &EnsV1Deployment,
    migration: &EnsV2MigrationDeployment,
    from: Address,
    label: &str,
    owner: Address,
) -> Result<TxReceipt> {
    ens_v1::transfer_wrapped_name_with_data(
        rpc,
        ens_v1,
        from,
        migration.unlocked_migration_controller.address,
        &format!("{label}.eth"),
        migration_data(label, owner),
    )
    .await
}

pub async fn migrate_locked_wrapped(
    rpc: &RpcClient,
    ens_v1: &EnsV1Deployment,
    migration: &EnsV2MigrationDeployment,
    from: Address,
    label: &str,
    owner: Address,
) -> Result<TxReceipt> {
    ens_v1::transfer_wrapped_name_with_data(
        rpc,
        ens_v1,
        from,
        migration.locked_migration_controller.address,
        &format!("{label}.eth"),
        migration_data(label, owner),
    )
    .await
}

pub async fn graveyard_clear(
    rpc: &RpcClient,
    migration: &EnsV2MigrationDeployment,
    from: Address,
    names: &[&str],
) -> Result<()> {
    let encoded = names
        .iter()
        .map(|name| ens_v1::dns_encode_name(name))
        .collect::<Result<Vec<_>>>()?;
    rpc.send_checked(
        from,
        migration.graveyard.address,
        &calls::clearCall { names: encoded }.abi_encode(),
        U256::ZERO,
        "Graveyard.clear",
    )
    .await?;
    Ok(())
}

pub async fn proxy_deployed_address(rpc: &RpcClient, receipt: &TxReceipt) -> Result<Address> {
    ens_v2::proxy_deployed_address(rpc, &receipt.tx_hash).await
}

pub async fn ens_v1_registry_owner(
    rpc: &RpcClient,
    ens_v1: &EnsV1Deployment,
    name: &str,
) -> Result<Address> {
    let raw = rpc
        .eth_call(
            ens_v1.registry.address,
            &calls::ownerCall {
                node: ens_v1::namehash(name),
            }
            .abi_encode(),
        )
        .await?;
    calls::ownerCall::abi_decode_returns(&raw).context("decode ENSv1 registry owner")
}

pub async fn ens_v1_registry_resolver(
    rpc: &RpcClient,
    ens_v1: &EnsV1Deployment,
    name: &str,
) -> Result<Address> {
    let raw = rpc
        .eth_call(
            ens_v1.registry.address,
            &calls::resolverCall {
                node: ens_v1::namehash(name),
            }
            .abi_encode(),
        )
        .await?;
    calls::resolverCall::abi_decode_returns(&raw).context("decode ENSv1 registry resolver")
}

pub async fn registry_owner(rpc: &RpcClient, registry: Address, label: &str) -> Result<Address> {
    let raw = rpc
        .eth_call(
            registry,
            &calls::getOwnerCall {
                anyId: ens_v2::label_id(label),
            }
            .abi_encode(),
        )
        .await?;
    calls::getOwnerCall::abi_decode_returns(&raw).context("decode ENSv2 registry owner")
}

pub async fn registry_subregistry(
    rpc: &RpcClient,
    registry: Address,
    label: &str,
) -> Result<Address> {
    let raw = rpc
        .eth_call(
            registry,
            &calls::getSubregistryCall {
                label: label.to_owned(),
            }
            .abi_encode(),
        )
        .await?;
    calls::getSubregistryCall::abi_decode_returns(&raw).context("decode ENSv2 registry subregistry")
}

pub async fn registry_expiry(rpc: &RpcClient, registry: Address, label: &str) -> Result<u64> {
    let raw = rpc
        .eth_call(
            registry,
            &calls::getExpiryCall {
                anyId: ens_v2::label_id(label),
            }
            .abi_encode(),
        )
        .await?;
    calls::getExpiryCall::abi_decode_returns(&raw).context("decode ENSv2 registry expiry")
}

pub async fn wrapper_registry_node(rpc: &RpcClient, registry: Address) -> Result<B256> {
    let raw = rpc
        .eth_call(registry, &calls::getWrappedNodeCall {}.abi_encode())
        .await?;
    calls::getWrappedNodeCall::abi_decode_returns(&raw).context("decode WrapperRegistry node")
}
