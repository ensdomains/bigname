use alloy_primitives::Address;
use anyhow::{Context, Result, ensure};
use bigname_adapters::{
    sync_block_derived_normalized_events, sync_ens_v1_unwrapped_authority, sync_ens_v2_permissions,
    sync_ens_v2_registrar, sync_ens_v2_registry_resource_surface, sync_ens_v2_resolver,
};
use serde_json::Value;
use sqlx::PgPool;

use super::support::{self, TempDir};
use crate::harness::{
    anvil::{self, Anvil},
    basenames, ens_v1, ens_v2, manifests,
};
use crate::harness::{db::HarnessDb, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

async fn sync_profile(pool: &PgPool, root: &std::path::Path) -> Result<()> {
    let repository = bigname_manifests::load_repository(root)?;
    bigname_manifests::sync_repository(pool, &repository).await?;
    Ok(())
}

async fn interpret_ens_v2(pool: &PgPool) -> Result<()> {
    sync_ens_v2_registry_resource_surface(pool, "ethereum-sepolia").await?;
    sync_ens_v2_registrar(pool, "ethereum-sepolia").await?;
    sync_ens_v2_resolver(pool, "ethereum-sepolia").await?;
    sync_ens_v2_permissions(pool, "ethereum-sepolia").await?;
    Ok(())
}

async fn active_edge_to(pool: &PgPool, edge_kind: &str, address: Address) -> Result<bool> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM discovery_edges edge
            JOIN contract_instance_addresses target
              ON target.contract_instance_id = edge.to_contract_instance_id
            WHERE edge.edge_kind = $1
              AND lower(target.address) = $2
              AND edge.active_to_block_number IS NULL
              AND edge.deactivated_at IS NULL
              AND target.deactivated_at IS NULL
        )
        "#,
    )
    .bind(edge_kind)
    .bind(format!("{address:#x}"))
    .fetch_one(pool)
    .await
    .context("load active discovery edge")
}

/// Restores the retired ENSv2 interpretation matrix using prefetched raw
/// facts. It covers role grant/revoke, a subregistry swap, unregister then
/// re-register, and a RegistryCreated instance with no parent link.
///
/// RegistryCreated is the constructor announcement
/// (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@ccaeb58).
/// Role changes emit EACRolesChanged
/// (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L274 @ ens_v2@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L308 @ ens_v2@ccaeb58).
#[tokio::test]
async fn ens_v2_prefetched_discovery_and_lifecycle_semantics() -> Result<()> {
    let chain = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = chain.client();
    let root = repo_root();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &root).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];
    let bob = accounts[2];

    let unlinked = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;

    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "roles",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let roles_label = ens_v2::label_id("roles");
    let role_bitmap = ens_v2::role_bit(ens_v2::ROLE_SET_RESOLVER);
    ens_v2::grant_roles(
        &rpc,
        deployment.eth_registry.address,
        alice,
        roles_label,
        role_bitmap,
        bob,
    )
    .await?;
    ens_v2::revoke_roles(
        &rpc,
        deployment.eth_registry.address,
        alice,
        roles_label,
        role_bitmap,
        bob,
    )
    .await?;

    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "tree",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let tree_label = ens_v2::label_id("tree");
    let child_a = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    ens_v2::attach_subregistry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        tree_label,
        child_a.address,
    )
    .await?;
    ens_v2::set_parent(
        &rpc,
        child_a.address,
        deployment.deployer,
        deployment.eth_registry.address,
        "tree",
    )
    .await?;
    ens_v2::register_in_registry(
        &rpc,
        child_a.address,
        deployment.deployer,
        "leaf",
        alice,
        anvil::GENESIS_TIMESTAMP + 5 * YEAR,
    )
    .await?;

    let database = HarnessDb::create().await?;
    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_sepolia_profile(
        scratch.path(),
        &root,
        &deployment.manifest_targets(),
    )?;
    sync_profile(&database.pool, &profile.root).await?;
    support::prefetch_raw_facts(&database.pool, &rpc, "ethereum-sepolia").await?;
    interpret_ens_v2(&database.pool).await?;

    let bob_permissions: Vec<(String, String)> = sqlx::query_as(
        "SELECT after_state->>'old_role_bitmap', after_state->>'role_bitmap' \
         FROM normalized_events \
         WHERE event_kind = 'PermissionChanged' \
           AND after_state->>'subject' = $1 \
           AND after_state->>'source_event' = 'EACRolesChanged' \
         ORDER BY block_number, log_index",
    )
    .bind(format!("{bob:#x}"))
    .fetch_all(&database.pool)
    .await?;
    let zero_bitmap = "0x0000000000000000000000000000000000000000000000000000000000000000";
    ensure!(
        bob_permissions.len() == 2
            && bob_permissions[0].0 == zero_bitmap
            && bob_permissions[0].1 != zero_bitmap
            && bob_permissions[1].0 != zero_bitmap
            && bob_permissions[1].1 == zero_bitmap,
        "grant/revoke history did not preserve both role bitmap transitions: {bob_permissions:?}"
    );

    ensure!(
        active_edge_to(&database.pool, "registry_announcement", unlinked.address).await?,
        "RegistryCreated must admit an unlinked registry instance"
    );
    ensure!(
        !active_edge_to(&database.pool, "subregistry", unlinked.address).await?,
        "RegistryCreated must not manufacture a parent-child link"
    );
    ensure!(
        active_edge_to(&database.pool, "subregistry", child_a.address).await?,
        "the first attached registry child must be present before the swap"
    );
    ensure!(
        normalized_name_event_exists(&database.pool, "ens:leaf.tree.eth", "RegistrationGranted")
            .await?,
        "the first attached registry's child must be interpreted before the swap"
    );

    let child_b = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    ens_v2::attach_subregistry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        tree_label,
        child_b.address,
    )
    .await?;
    ens_v2::set_parent(
        &rpc,
        child_b.address,
        deployment.deployer,
        deployment.eth_registry.address,
        "tree",
    )
    .await?;
    ens_v2::register_in_registry(
        &rpc,
        child_b.address,
        deployment.deployer,
        "newleaf",
        alice,
        anvil::GENESIS_TIMESTAMP + 5 * YEAR,
    )
    .await?;

    let first_cycle = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "cycle",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    ens_v2::unregister(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        ens_v2::label_id("cycle"),
    )
    .await?;
    rpc.increase_time(ens_v2::GRACE_PERIOD + 1).await?;
    let second_cycle = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: bob,
            label: "cycle",
            owner: bob,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    ensure!(
        first_cycle.resource_id != second_cycle.resource_id,
        "unregister then re-register must advance the resource"
    );
    ensure!(
        first_cycle.token_id != second_cycle.token_id,
        "unregister then re-register must advance the registry token"
    );

    support::prefetch_raw_facts(&database.pool, &rpc, "ethereum-sepolia").await?;
    interpret_ens_v2(&database.pool).await?;

    ensure!(
        !active_edge_to(&database.pool, "subregistry", child_a.address).await?,
        "the replaced registry child must be absent after the swap"
    );
    ensure!(
        active_edge_to(&database.pool, "subregistry", child_b.address).await?,
        "the replacement registry child must be present after the swap"
    );
    ensure!(
        normalized_name_event_exists(
            &database.pool,
            "ens:newleaf.tree.eth",
            "RegistrationGranted",
        )
        .await?,
        "the replacement registry's child must be interpreted after the swap"
    );

    let cycle_resources: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT resource_id)::BIGINT \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:cycle.eth' \
           AND event_kind = 'RegistrationGranted' \
           AND canonicality_state <> 'orphaned'",
    )
    .fetch_one(&database.pool)
    .await?;
    ensure!(
        cycle_resources == 2,
        "unregister then re-register must retain two resource epochs, saw {cycle_resources}"
    );
    let cycle_tokens: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT after_state->>'token_id')::BIGINT \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:cycle.eth' \
           AND event_kind = 'RegistrationGranted' \
           AND canonicality_state <> 'orphaned'",
    )
    .fetch_one(&database.pool)
    .await?;
    ensure!(
        cycle_tokens == 2,
        "unregister then re-register must retain two token epochs, saw {cycle_tokens}"
    );

    database.cleanup().await
}

/// The v1 resolver stream is selected by its ENS-specific signature even
/// when the emitting resolver has no registry pointer or discovery edge.
/// AddrChanged is declared by the pinned resolver interface
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f).
/// The pinned implementation emits both AddressChanged and AddrChanged for
/// an ETH address update
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L61 @ ens_v1@91c966f).
#[tokio::test]
async fn ens_v1_match_all_resolver_without_pointer_from_prefetched_raw_facts() -> Result<()> {
    let chain = Anvil::spawn().await?;
    let rpc = chain.client();
    let root = repo_root();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];
    ens_v1::register_eth_name(&rpc, &deployment, "unpointed", alice, YEAR, Address::ZERO).await?;
    let resolver = ens_v1::deploy_extra_public_resolver(&rpc, &root, &deployment).await?;
    ens_v1::set_addr_record(&rpc, resolver.address, alice, "unpointed.eth", alice).await?;

    let database = HarnessDb::create().await?;
    let scratch = TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    sync_profile(&database.pool, &profile.root).await?;
    support::prefetch_raw_facts(&database.pool, &rpc, "ethereum-mainnet").await?;
    let summary = sync_ens_v1_unwrapped_authority(&database.pool, "ethereum-mainnet").await?;

    let record_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM normalized_events \
         WHERE logical_name_id = 'ens:unpointed.eth' \
           AND event_kind = 'RecordChanged' \
           AND source_family = 'ens_v1_resolver_l1' \
           AND after_state->>'record_key' = 'addr:60'",
    )
    .fetch_one(&database.pool)
    .await?;
    let resolver_events: Vec<(String, Option<String>, Value)> = sqlx::query_as(
        "SELECT event_kind, logical_name_id, after_state \
         FROM normalized_events \
         WHERE source_family = 'ens_v1_resolver_l1' \
         ORDER BY block_number, log_index",
    )
    .fetch_all(&database.pool)
    .await?;
    ensure!(
        record_count == 2,
        "match-all resolver selection must retain the unpointed record; \
         summary={summary:?}; resolver_events={resolver_events:?}"
    );
    ensure!(
        !active_edge_to(&database.pool, "resolver", resolver.address).await?,
        "the resolver record must not depend on a discovered edge"
    );
    let pointer_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM normalized_events \
         WHERE logical_name_id = 'ens:unpointed.eth' \
           AND event_kind = 'ResolverChanged'",
    )
    .fetch_one(&database.pool)
    .await?;
    ensure!(
        pointer_count == 0,
        "the test name must remain unpointed while its resolver history is retained"
    );

    database.cleanup().await
}

/// A declared ERC-1967 proxy contributes its constructor Upgraded log as
/// normalized contract history. The event signature is canonical ERC-1967
/// (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/interfaces/IERC1967.sol:L13 @ basenames@1809bbc),
/// and the proxy constructor installs the initial implementation
/// (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/proxy/ERC1967/ERC1967Proxy.sol:L27 @ basenames@1809bbc).
#[tokio::test]
async fn declared_proxy_upgraded_history_from_prefetched_raw_facts() -> Result<()> {
    let chain = Anvil::spawn_base_mainnet().await?;
    let rpc = chain.client();
    let root = repo_root();
    let mut deployment = basenames::deploy_basenames(&rpc, &root).await?;
    basenames::deploy_upgradeable_registrar_controller(&rpc, &root, &mut deployment).await?;
    let proxy = deployment
        .upgradeable_registrar_controller
        .as_ref()
        .context("upgradeable controller proxy is missing")?;
    let implementation = deployment
        .upgradeable_registrar_controller_implementation
        .as_ref()
        .context("upgradeable controller implementation is missing")?;

    let database = HarnessDb::create().await?;
    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_basenames_profile(
        scratch.path(),
        &root,
        &deployment.manifest_targets(),
    )?;
    sync_profile(&database.pool, &profile.root).await?;
    let block_hashes = support::prefetch_raw_facts(&database.pool, &rpc, "base-mainnet").await?;
    sync_block_derived_normalized_events(&database.pool, "base-mainnet", &block_hashes, None)
        .await?;

    let observed: Option<String> = sqlx::query_scalar(
        "SELECT after_state->>'implementation' \
         FROM normalized_events \
         WHERE event_kind = 'Upgraded' \
           AND after_state->>'proxy_address' = $1 \
         ORDER BY block_number, log_index \
         LIMIT 1",
    )
    .bind(format!("{:#x}", proxy.address))
    .fetch_optional(&database.pool)
    .await?;
    let expected_implementation = format!("{:#x}", implementation.address);
    ensure!(
        observed.as_deref() == Some(expected_implementation.as_str()),
        "declared proxy Upgraded history has the wrong implementation: {observed:?}"
    );

    database.cleanup().await
}

async fn normalized_name_event_exists(
    pool: &PgPool,
    logical_name_id: &str,
    event_kind: &str,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = $1 AND event_kind = $2 \
           AND canonicality_state <> 'orphaned')",
    )
    .bind(logical_name_id)
    .bind(event_kind)
    .fetch_one(pool)
    .await
    .context("load normalized name event")
}
