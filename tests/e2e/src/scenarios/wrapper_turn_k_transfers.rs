use std::collections::BTreeSet;

use alloy_primitives::Address;
use anyhow::{Context, Result};
use sqlx::types::Uuid;

use super::support;
use crate::harness::responses::{exact_name, pointer};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

async fn resource_token_lineage(
    run: &support::PipelineRun,
    resource_id: Uuid,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar("SELECT token_lineage_id FROM resources WHERE resource_id = $1")
        .bind(resource_id)
        .fetch_one(&run.db.pool)
        .await
        .context("resource token lineage lookup failed")
}

/// Renewing through the current controller extends the registrar lease
/// directly, while NameWrapper's separate renewal path is the one that
/// updates wrapper storage without emitting ExpiryExtended.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L366 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f)
#[tokio::test]
async fn wrapped_renewal_tracks_registrar_expiry_without_wrapper_event() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;
    let name = "renewwrapped.eth";

    ens_v1::register_eth_name(&rpc, &deployment, "renewwrapped", alice, YEAR, resolver).await?;
    ens_v1::wrap_eth_2ld(&rpc, &deployment, alice, "renewwrapped", bob, 0, resolver).await?;

    let wrapper_before = ens_v1::wrapped_name_data(&rpc, &deployment, name).await?;
    assert_eq!(wrapper_before.owner, bob);

    ens_v1::renew_eth_name(&rpc, &deployment, alice, "renewwrapped", YEAR).await?;

    let wrapper_after = ens_v1::wrapped_name_data(&rpc, &deployment, name).await?;
    assert_eq!(wrapper_after.owner, bob);
    assert_eq!(
        wrapper_after.fuses, wrapper_before.fuses,
        "current-controller renewal must not mutate wrapper fuses"
    );
    assert_eq!(
        wrapper_after.expiry, wrapper_before.expiry,
        "current-controller renewal touches the registrar, leaving wrapper storage stale"
    );

    let ready_sql =
        support::canonical_event_ready_sql("ens:renewwrapped.eth", "RegistrationRenewed", None);
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let (renewal_tx, renewal_expiry, registrar_resource): (String, i64, Uuid) = sqlx::query_as(
        "SELECT transaction_hash, (after_state->>'expiry')::BIGINT, resource_id \
             FROM normalized_events \
             WHERE logical_name_id = 'ens:renewwrapped.eth' \
               AND event_kind = 'RegistrationRenewed' \
               AND source_family = 'ens_v1_registrar_l1' \
               AND canonicality_state = 'canonical' \
             ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC \
             LIMIT 1",
    )
    .fetch_one(&run.db.pool)
    .await?;

    let renewal_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:renewwrapped.eth' \
           AND event_kind = 'RegistrationRenewed' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(renewal_count, 1, "one controller renew should derive once");

    let wrapper_expiry_in_renewal_tx: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND event_kind = 'ExpiryChanged' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&renewal_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        wrapper_expiry_in_renewal_tx, 0,
        "current-controller renewal must not invent a wrapper ExpiryChanged"
    );

    let last_wrapper_expiry: i64 = sqlx::query_scalar(
        "SELECT (after_state->>'expiry')::BIGINT FROM normalized_events \
         WHERE logical_name_id = 'ens:renewwrapped.eth' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND event_kind = 'ExpiryChanged' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC \
         LIMIT 1",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        last_wrapper_expiry,
        i64::try_from(wrapper_after.expiry)?,
        "the last wrapper expiry event should agree with unchanged onchain wrapper storage"
    );
    assert_ne!(
        renewal_expiry, last_wrapper_expiry,
        "a one-year renewal should distinguish registrar expiry from stale wrapper expiry"
    );

    let wrapper_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events \
         WHERE logical_name_id = 'ens:renewwrapped.eth' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND after_state->>'authority_kind' = 'wrapper' \
           AND resource_id IS NOT NULL \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index, normalized_event_id \
         LIMIT 1",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_ne!(
        wrapper_resource, registrar_resource,
        "renewal fact stays registrar-anchored while the surface stays wrapper-anchored"
    );
    let wrapper_lineage = resource_token_lineage(&run, wrapper_resource).await?;
    assert!(
        wrapper_lineage.is_some(),
        "active wrapper resource must retain a token lineage through renewal"
    );

    let body = exact_name(&run.api, "ens", name).await?;
    assert_eq!(
        pointer(&body, "/data/resource_id").as_str(),
        Some(wrapper_resource.to_string().as_str()),
        "renewal must not rotate the active wrapper resource; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/data/token_lineage_id").as_str(),
        wrapper_lineage.as_ref().map(ToString::to_string).as_deref(),
        "renewal must not rotate the wrapper token lineage; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{bob:#x}"),
        "wrapped holder remains the projected registrant; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/expiry"),
        renewal_expiry,
        "exact-name expiry should track the registrar renewal, not stale wrapper storage; body: {body}"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// NameWrapper's ERC1155 single and batch transfers mutate only wrapper-local
/// token ownership and emit TransferSingle/TransferBatch; they make no ENS
/// registry call.
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L281 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L303 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L172 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L187 @ ens_v1@91c966f)
#[tokio::test]
async fn wrapped_erc1155_single_and_batch_transfers_preserve_identity() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob, carol) = (accounts[1], accounts[2], accounts[3]);
    let resolver = deployment.public_resolver.address;
    let labels = ["singlemove", "batchmoveone", "batchmovetwo"];
    let names = ["singlemove.eth", "batchmoveone.eth", "batchmovetwo.eth"];

    for label in labels {
        ens_v1::register_eth_name(&rpc, &deployment, label, alice, YEAR, resolver).await?;
        ens_v1::wrap_eth_2ld(&rpc, &deployment, alice, label, alice, 0, resolver).await?;
    }

    let single_tx = ens_v1::transfer_wrapped_name(&rpc, &deployment, alice, bob, names[0]).await?;
    let batch_tx =
        ens_v1::batch_transfer_wrapped_names(&rpc, &deployment, alice, carol, &names[1..]).await?;

    assert_eq!(
        ens_v1::wrapped_name_data(&rpc, &deployment, names[0])
            .await?
            .owner,
        bob
    );
    for name in &names[1..] {
        assert_eq!(
            ens_v1::wrapped_name_data(&rpc, &deployment, name)
                .await?
                .owner,
            carol
        );
    }

    let ready_sql = format!(
        "SELECT \
           (SELECT count(*) FROM normalized_events \
            WHERE transaction_hash = '{single_tx}' \
              AND event_kind = 'TokenControlTransferred' \
              AND source_family = 'ens_v1_wrapper_l1' \
              AND canonicality_state = 'canonical') = 1 \
         AND \
           (SELECT count(*) FROM normalized_events \
            WHERE transaction_hash = '{batch_tx}' \
              AND event_kind = 'TokenControlTransferred' \
              AND source_family = 'ens_v1_wrapper_l1' \
              AND canonicality_state = 'canonical') = 2"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let single_transfers: Vec<(String, Uuid, i64, String, String, String)> = sqlx::query_as(
        "SELECT logical_name_id, resource_id, log_index, event_identity, \
                before_state->>'from', after_state->>'to' \
         FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND event_kind = 'TokenControlTransferred' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND canonicality_state = 'canonical' \
         ORDER BY logical_name_id",
    )
    .bind(&single_tx)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(single_transfers.len(), 1);
    assert_eq!(single_transfers[0].0, "ens:singlemove.eth");
    assert_eq!(single_transfers[0].4, format!("{alice:#x}"));
    assert_eq!(single_transfers[0].5, format!("{bob:#x}"));

    let batch_transfers: Vec<(String, Uuid, i64, String, String, String)> = sqlx::query_as(
        "SELECT logical_name_id, resource_id, log_index, event_identity, \
                before_state->>'from', after_state->>'to' \
         FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND event_kind = 'TokenControlTransferred' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND canonicality_state = 'canonical' \
         ORDER BY logical_name_id",
    )
    .bind(&batch_tx)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        batch_transfers.len(),
        2,
        "TransferBatch must fan out per id"
    );
    assert_eq!(
        batch_transfers
            .iter()
            .map(|row| row.0.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["ens:batchmoveone.eth", "ens:batchmovetwo.eth"])
    );
    assert_eq!(
        batch_transfers
            .iter()
            .map(|row| row.1)
            .collect::<BTreeSet<_>>()
            .len(),
        2,
        "each batch id must retain its own resource"
    );
    assert_eq!(
        batch_transfers
            .iter()
            .map(|row| row.2)
            .collect::<BTreeSet<_>>()
            .len(),
        1,
        "both derived rows must point to the one TransferBatch raw log"
    );
    assert_eq!(
        batch_transfers
            .iter()
            .map(|row| row.3.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        2,
        "per-id fanout must use collision-free normalized identities"
    );
    for transfer in &batch_transfers {
        assert_eq!(transfer.4, format!("{alice:#x}"));
        assert_eq!(transfer.5, format!("{carol:#x}"));
    }

    let transfer_blocks: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM raw_transactions \
         WHERE transaction_hash IN ($1, $2) \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number",
    )
    .bind(&single_tx)
    .bind(&batch_tx)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        transfer_blocks.len(),
        2,
        "both ERC1155 transactions must be retained as canonical raw facts"
    );
    let logical_name_ids = names
        .iter()
        .map(|name| format!("ens:{name}"))
        .collect::<Vec<_>>();

    let registry_derivations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE block_number = ANY($1::BIGINT[]) \
           AND logical_name_id = ANY($2::TEXT[]) \
           AND source_family = 'ens_v1_registry_l1' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&transfer_blocks)
    .bind(&logical_name_ids)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        registry_derivations, 0,
        "ERC1155 holder motion must derive no registry event"
    );

    let lifecycle_derivations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE block_number = ANY($1::BIGINT[]) \
           AND logical_name_id = ANY($2::TEXT[]) \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND event_kind IN ( \
               'ExpiryChanged', 'PermissionScopeChanged', \
               'AuthorityEpochChanged', 'SurfaceBound', 'SurfaceUnbound' \
           ) \
           AND canonicality_state = 'canonical'",
    )
    .bind(&transfer_blocks)
    .bind(&logical_name_ids)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        lifecycle_derivations, 0,
        "plain holder transfers must not invent wrapper lifecycle transitions"
    );

    assert_transferred_exact_shape(&run, names[0], bob, deployment.name_wrapper.address).await?;
    for name in &names[1..] {
        assert_transferred_exact_shape(&run, name, carol, deployment.name_wrapper.address).await?;
    }

    run.db.cleanup().await?;
    Ok(())
}

async fn assert_transferred_exact_shape(
    run: &support::PipelineRun,
    name: &str,
    holder: Address,
    wrapper: Address,
) -> Result<()> {
    let logical_name_id = format!("ens:{name}");
    let wrapper_control_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = $1 \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND event_kind = 'TokenControlTransferred' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&logical_name_id)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        wrapper_control_events, 2,
        "{name} should have one wrap grant and one holder transfer"
    );

    let resources: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT resource_id FROM normalized_events \
         WHERE logical_name_id = $1 \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND event_kind = 'TokenControlTransferred' \
           AND resource_id IS NOT NULL \
           AND canonicality_state = 'canonical'",
    )
    .bind(&logical_name_id)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        resources.len(),
        1,
        "holder rotation must not rotate {name}'s wrapper resource"
    );
    let resource = resources[0];
    let lineage = resource_token_lineage(run, resource).await?;
    assert!(
        lineage.is_some(),
        "wrapper resource must carry a token lineage"
    );

    let latest_registry_owner: String = sqlx::query_scalar(
        "SELECT after_state->>'owner' FROM normalized_events \
         WHERE logical_name_id = $1 \
           AND source_family = 'ens_v1_registry_l1' \
           AND event_kind = 'AuthorityTransferred' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC \
         LIMIT 1",
    )
    .bind(&logical_name_id)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(latest_registry_owner, format!("{wrapper:#x}"));

    let body = exact_name(&run.api, "ens", name).await?;
    assert_eq!(
        pointer(&body, "/data/resource_id").as_str(),
        Some(resource.to_string().as_str()),
        "exact-name surface should retain {name}'s wrapper resource; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/data/token_lineage_id").as_str(),
        lineage.as_ref().map(ToString::to_string).as_deref(),
        "exact-name surface should retain {name}'s wrapper lineage; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{holder:#x}"),
        "registration registrant should follow the ERC1155 holder; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/declared_state/control"),
        serde_json::json!({
            "status": "unsupported",
            "unsupported_reason": "ENSv1 wrapper effective control is not yet projected",
        }),
        "wrapper control must stay explicitly unsupported instead of leaking stale or holder-derived facets; body: {body}"
    );

    // The registration summary follows the ERC1155 holder, but wrapper
    // effective control is deliberately not inferred from that shared fact.
    let authority_key = pointer(&body, "/declared_state/registration/authority_key");
    assert!(
        authority_key
            .as_str()
            .is_some_and(|key| key.starts_with("registrar:")),
        "pinned stale registrar authority_key after wrapped holder rotation; body: {body}"
    );

    Ok(())
}
