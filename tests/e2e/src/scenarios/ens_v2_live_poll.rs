use alloy_primitives::Address;
use anyhow::Result;
use sqlx::types::Uuid;

use crate::harness::{anvil::Anvil, db::HarnessDb, ens_v2, manifests, pipeline, repo_root};

use super::support;

const CHAIN: &str = "ethereum-sepolia";
const LABEL: &str = "crosspoll";
const LOGICAL_NAME_ID: &str = "ens:crosspoll.eth";
const YEAR: u64 = 365 * 24 * 60 * 60;

#[tokio::test]
async fn ens_v2_registry_state_survives_distinct_live_polls() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let root = repo_root();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &root).await?;
    rpc.mine(2).await?;

    let scratch = support::TempDir::create()?;
    let profile = manifests::generate_local_sepolia_profile(
        scratch.path(),
        &root,
        &deployment.manifest_targets(),
    )?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [(CHAIN, anvil.url.as_str())];
    let mut indexer = pipeline::IndexerRunSession::start_with_live_poll_adapter_sync(
        &root,
        &db.url,
        &profile.root,
        &chain_rpc_urls,
        "ens-v2-cross-poll",
    )?;
    indexer
        .wait_for_first_chain_checkpoint(&db.pool, CHAIN)
        .await?;

    let accounts = rpc.accounts().await?;
    let alice = accounts[1];
    let bob = accounts[2];
    let registration = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: LABEL,
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    rpc.mine(1).await?;
    let registration_checkpoint = rpc.block_number().await?;
    indexer
        .wait_for_chain_checkpoint(
            &db.pool,
            CHAIN,
            registration_checkpoint,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:crosspoll.eth' \
                   AND event_kind = 'RegistrationGranted' \
                   AND canonicality_state IN ('canonical', 'safe', 'finalized'))",
            ),
        )
        .await?;

    let resolver = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    ens_v2::set_resolver_in_registry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        ens_v2::label_id(LABEL),
        resolver.address,
    )
    .await?;
    rpc.mine(1).await?;
    let resolver_checkpoint = rpc.block_number().await?;
    indexer
        .wait_for_chain_checkpoint(
            &db.pool,
            CHAIN,
            resolver_checkpoint,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:crosspoll.eth' \
                   AND event_kind = 'ResolverChanged' \
                   AND canonicality_state IN ('canonical', 'safe', 'finalized'))",
            ),
        )
        .await?;

    let child = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    ens_v2::attach_subregistry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        ens_v2::label_id(LABEL),
        child.address,
    )
    .await?;
    rpc.mine(1).await?;
    let subregistry_checkpoint = rpc.block_number().await?;
    indexer
        .wait_for_chain_checkpoint(
            &db.pool,
            CHAIN,
            subregistry_checkpoint,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:crosspoll.eth' \
                   AND event_kind = 'SubregistryChanged' \
                   AND canonicality_state IN ('canonical', 'safe', 'finalized'))",
            ),
        )
        .await?;

    ens_v2::grant_roles(
        &rpc,
        deployment.eth_registry.address,
        alice,
        ens_v2::label_id(LABEL),
        ens_v2::role_bit(ens_v2::ROLE_SET_RESOLVER),
        bob,
    )
    .await?;
    rpc.mine(1).await?;
    let regeneration_checkpoint = rpc.block_number().await?;
    indexer
        .wait_for_chain_checkpoint(
            &db.pool,
            CHAIN,
            regeneration_checkpoint,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:crosspoll.eth' \
                   AND event_kind = 'TokenRegenerated' \
                   AND canonicality_state IN ('canonical', 'safe', 'finalized'))",
            ),
        )
        .await?;

    ens_v2::unregister(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        ens_v2::label_id(LABEL),
    )
    .await?;
    rpc.mine(1).await?;
    let unregister_checkpoint = rpc.block_number().await?;
    indexer
        .wait_for_chain_checkpoint(
            &db.pool,
            CHAIN,
            unregister_checkpoint,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:crosspoll.eth' \
                   AND event_kind = 'RegistrationReleased' \
                   AND canonicality_state IN ('canonical', 'safe', 'finalized')) \
                 AND EXISTS (SELECT 1 FROM surface_bindings \
                 WHERE logical_name_id = 'ens:crosspoll.eth' \
                   AND active_to IS NOT NULL \
                   AND canonicality_state IN ('canonical', 'safe', 'finalized'))",
            ),
        )
        .await?;
    indexer.stop().await?;

    let lifecycle: Vec<(String, i64, Uuid)> = sqlx::query_as(
        "SELECT event_kind, block_number, resource_id FROM normalized_events \
         WHERE logical_name_id = $1 \
           AND event_kind IN ( \
             'ResolverChanged', 'SubregistryChanged', \
             'TokenRegenerated', 'RegistrationReleased' \
           ) \
           AND resource_id IS NOT NULL \
           AND canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY block_number, log_index, event_kind",
    )
    .bind(LOGICAL_NAME_ID)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        lifecycle
            .iter()
            .map(|row| row.0.as_str())
            .collect::<Vec<_>>(),
        vec![
            "ResolverChanged",
            "SubregistryChanged",
            "TokenRegenerated",
            "RegistrationReleased",
        ],
        "each later poll must retain enough registry state to normalize its lifecycle event: {lifecycle:?}"
    );
    assert!(
        lifecycle
            .iter()
            .all(|(_, block_number, _)| *block_number > registration.register_block as i64),
        "every mutation should be observed after the registration poll: {lifecycle:?}"
    );
    assert!(
        lifecycle
            .iter()
            .all(|(_, _, resource_id)| *resource_id == lifecycle[0].2),
        "resolver, subregistry, token, and unregister events must remain on one resource: {lifecycle:?}"
    );

    let expiry_event_blocks: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM normalized_events \
         WHERE logical_name_id = $1 \
           AND event_kind = 'ExpiryChanged' \
           AND canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY block_number, log_index",
    )
    .bind(LOGICAL_NAME_ID)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        expiry_event_blocks,
        vec![registration.register_block as i64],
        "resolver, subregistry, token, and unregister polls must not manufacture expiry changes"
    );

    let checkpoint: i64 = sqlx::query_scalar(
        "SELECT canonical_block_number FROM chain_checkpoints WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        checkpoint >= unregister_checkpoint as i64,
        "canonical checkpoint {checkpoint} did not advance through unregister block {unregister_checkpoint}"
    );

    let binding_closed: bool = sqlx::query_scalar(
        "SELECT count(*) = 1 AND bool_and(active_to IS NOT NULL) \
         FROM surface_bindings \
         WHERE logical_name_id = $1 \
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(LOGICAL_NAME_ID)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        binding_closed,
        "unregister must close the active name binding"
    );

    db.cleanup().await
}
