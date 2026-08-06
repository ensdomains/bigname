use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::types::Uuid;

use super::support;
use crate::harness::responses::{exact_name, pointer};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;
const CANNOT_UNWRAP: u16 = 1;
const PARENT_CANNOT_CONTROL: u32 = 1 << 16;

async fn active_binding(
    pool: &sqlx::PgPool,
    logical_name_id: &str,
) -> Result<(Uuid, Option<Uuid>, String)> {
    sqlx::query_as(
        "SELECT binding.resource_id, resource.token_lineage_id, \
                (SELECT event.after_state->>'authority_kind' \
                 FROM normalized_events event \
                 WHERE event.resource_id = binding.resource_id \
                   AND event.event_kind = 'AuthorityEpochChanged' \
                   AND event.canonicality_state = 'canonical' \
                 ORDER BY event.block_number DESC, event.log_index DESC, \
                          event.normalized_event_id DESC LIMIT 1) \
         FROM surface_bindings binding \
         JOIN resources resource USING (resource_id) \
         JOIN name_current current \
           ON current.logical_name_id = binding.logical_name_id \
          AND current.resource_id = binding.resource_id \
         WHERE binding.logical_name_id = $1 \
           AND binding.active_to IS NULL \
           AND binding.canonicality_state = 'canonical' \
           AND resource.canonicality_state = 'canonical' \
         ORDER BY binding.active_from DESC, binding.surface_binding_id DESC \
         LIMIT 1",
    )
    .bind(logical_name_id)
    .fetch_one(pool)
    .await
    .with_context(|| format!("active binding missing for {logical_name_id}"))
}

/// The admitted mainnet wrapped controller calls NameWrapper's controller-only
/// registerAndWrapETH2LD path, which registers directly to the wrapper and
/// mints the wrapper token before the controller emits NameRegistered.
/// (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L281 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f)
/// Its renewal path calls NameWrapper, which stores registrar expiry plus grace
/// without emitting ExpiryExtended.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L333 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f)
#[tokio::test]
async fn born_wrapped_registration_retains_wrapper_authority() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];

    let registered = ens_v1::register_wrapped_eth_name(
        &rpc,
        &deployment,
        "bornwrapped",
        alice,
        YEAR,
        Address::ZERO,
        0,
    )
    .await?;
    let tx_hash = &registered.register_tx_hash;
    let wrapper_before = ens_v1::wrapped_name_data(&rpc, &deployment, "bornwrapped.eth").await?;
    let renewal_tx =
        ens_v1::renew_wrapped_eth_name(&rpc, &deployment, alice, "bornwrapped", YEAR).await?;
    let renewed_registrar_expiry =
        ens_v1::eth_name_expiry(&rpc, &deployment, "bornwrapped").await?;
    let wrapper_after = ens_v1::wrapped_name_data(&rpc, &deployment, "bornwrapped.eth").await?;
    assert_eq!(wrapper_after.owner, wrapper_before.owner);
    assert_eq!(wrapper_after.fuses, wrapper_before.fuses);
    assert!(wrapper_after.expiry > wrapper_before.expiry);
    assert_eq!(
        wrapper_after.expiry,
        renewed_registrar_expiry + GRACE_PERIOD
    );
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
             AND event_kind = 'RegistrationGranted' \
             AND source_family = 'ens_v1_registrar_l1' \
             AND transaction_hash = '{tx_hash}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
             AND event_kind = 'ExpiryChanged' \
             AND source_family = 'ens_v1_wrapper_l1' \
             AND transaction_hash = '{tx_hash}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
             AND event_kind = 'AuthorityTransferred' \
             AND lower(after_state->>'owner') = '{wrapper:#x}' \
             AND transaction_hash = '{tx_hash}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
             AND event_kind = 'ExpiryChanged' \
             AND source_family = 'ens_v1_registrar_l1' \
             AND after_state->>'authority_kind' = 'wrapper' \
             AND transaction_hash = '{renewal_tx}' \
             AND canonicality_state = 'canonical')",
        wrapper = deployment.name_wrapper.address,
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let registration: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
           AND event_kind = 'RegistrationGranted' \
           AND source_family = 'ens_v1_registrar_l1' \
           AND transaction_hash = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(registration["registrant"], format!("{alice:#x}"));
    assert_eq!(registration["authority_kind"], "registrar");
    let registrar_expiry = registration["expiry"]
        .as_i64()
        .context("born-wrapped registrar expiry missing")?;
    let transaction_to: Option<String> = sqlx::query_scalar(
        "SELECT to_address FROM raw_transactions raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE transaction_hash = $1 AND lineage.canonicality_state = 'canonical'",
    )
    .bind(tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        transaction_to.as_deref(),
        Some(format!("{:#x}", deployment.wrapped_controller.address).as_str()),
        "RegistrationGranted must come from the admitted wrapped-controller reveal"
    );

    let wrapper_expiry: i64 = sqlx::query_scalar(
        "SELECT (after_state->>'expiry')::BIGINT FROM normalized_events \
         WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
           AND event_kind = 'ExpiryChanged' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND transaction_hash = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        wrapper_expiry,
        registrar_expiry + GRACE_PERIOD as i64,
        "born-wrapped NameWrapped expiry should include registrar grace"
    );
    let renewal_wrapper_expiry: (i64, i64) = sqlx::query_as(
        "SELECT (after_state->>'registrar_expiry')::BIGINT, \
                (after_state->>'expiry')::BIGINT \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
           AND event_kind = 'ExpiryChanged' \
           AND source_family = 'ens_v1_registrar_l1' \
           AND after_state->>'authority_kind' = 'wrapper' \
           AND transaction_hash = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&renewal_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        renewal_wrapper_expiry,
        (renewed_registrar_expiry as i64, wrapper_after.expiry as i64)
    );

    // Both name-bearing logs retain their own observation, while the durable
    // labelhash-to-label fact remains deduplicated.
    let bornwrapped_labelhash = format!("{:#x}", ens_v1::labelhash("bornwrapped"));
    let preimage_observations: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT source_family, after_state->>'source_event', \
                after_state->>'raw_name', after_state->'raw_labels'->>0 \
         FROM normalized_events \
         WHERE event_kind = 'PreimageObserved' \
           AND transaction_hash = $1 \
           AND after_state->>'raw_name' = 'bornwrapped.eth' \
           AND canonicality_state = 'canonical' \
         ORDER BY source_family, log_index",
    )
    .bind(tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        preimage_observations,
        vec![
            (
                "ens_v1_registrar_l1".to_owned(),
                "NameRegistered".to_owned(),
                "bornwrapped.eth".to_owned(),
                "bornwrapped".to_owned(),
            ),
            (
                "ens_v1_wrapper_l1".to_owned(),
                "NameWrapped".to_owned(),
                "bornwrapped.eth".to_owned(),
                "bornwrapped".to_owned(),
            ),
        ],
        "the registrar and wrapper name-bearing logs must both retain the same verified label preimage"
    );
    let retained_label: (Vec<u8>, Option<String>, bool) = sqlx::query_as(
        "SELECT raw_label, decoded_label, normalized_under_version \
         FROM label_preimages WHERE labelhash = $1",
    )
    .bind(&bornwrapped_labelhash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        retained_label,
        (
            b"bornwrapped".to_vec(),
            Some("bornwrapped".to_owned()),
            true,
        ),
        "duplicate observations must converge on one verified label fact"
    );

    let registry_owner_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
           AND event_kind = 'AuthorityTransferred' \
           AND source_family = 'ens_v1_registry_l1' \
           AND lower(after_state->>'owner') = $1 \
           AND transaction_hash = $2 \
           AND canonicality_state = 'canonical'",
    )
    .bind(format!("{:#x}", deployment.name_wrapper.address))
    .bind(tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        registry_owner_events, 1,
        "registrar registration must set registry ownership to NameWrapper"
    );
    let registrar_holder_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
           AND event_kind = 'TokenControlTransferred' \
           AND source_family = 'ens_v1_registrar_l1' \
           AND transaction_hash = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        registrar_holder_events, 0,
        "zero-address registrar mint must not invent a pre-wrap holder transfer"
    );

    let wrapper_resources: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT resource_id FROM normalized_events \
         WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND after_state->>'authority_kind' = 'wrapper' \
           AND resource_id IS NOT NULL \
           AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        wrapper_resources.len(),
        1,
        "born-wrapped derivation should mint one wrapper resource"
    );
    let resource_shape: (i64, i64, i64) = sqlx::query_as(
        "SELECT \
           count(DISTINCT resource_id) FILTER (WHERE after_state->>'authority_kind' = 'registrar'), \
           count(DISTINCT resource_id) FILTER (WHERE after_state->>'authority_kind' = 'registry_only'), \
           count(DISTINCT resource_id) FILTER (WHERE after_state->>'authority_kind' = 'wrapper') \
         FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND resource_id IS NOT NULL \
           AND canonicality_state = 'canonical'",
    )
    .bind(tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        resource_shape,
        (1, 0, 1),
        "same-transaction registry setup must remain on the registrar epoch instead of minting a spurious registry-only epoch"
    );
    let setup_and_registration_resources: (Uuid, Uuid) = sqlx::query_as(
        "SELECT \
           (SELECT resource_id FROM normalized_events \
            WHERE transaction_hash = $1 AND source_family = 'ens_v1_registry_l1' \
              AND event_kind = 'AuthorityTransferred' \
              AND canonicality_state = 'canonical' LIMIT 1), \
           (SELECT resource_id FROM normalized_events \
            WHERE transaction_hash = $1 AND source_family = 'ens_v1_registrar_l1' \
              AND event_kind = 'RegistrationGranted' \
              AND canonicality_state = 'canonical' LIMIT 1)",
    )
    .bind(tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        setup_and_registration_resources.0, setup_and_registration_resources.1,
        "the registry ownership setup and registrar grant must share the registration resource"
    );
    let wrapper_bound: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141' \
           AND event_kind = 'SurfaceBound' \
           AND after_state->>'authority_kind' = 'wrapper' \
           AND resource_id = $1 \
           AND canonicality_state = 'canonical')",
    )
    .bind(wrapper_resources[0])
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        wrapper_bound,
        "wrapper resource must anchor the surface once"
    );

    // NameWrapped is earlier than the controller's NameRegistered, but both
    // observations belong to the same atomic registration. The later grant
    // must therefore retain the wrapper binding instead of treating it as a
    // stale wrapper from a previous registration epoch.
    let (active_resource, active_lineage, active_kind) = active_binding(
        &run.db.pool,
        "ens:0xb30b6bcb9454bce932c3121da769db8cd4a47747b30881b95661b967de6d6141",
    )
    .await?;
    assert_eq!(active_kind, "wrapper");
    assert!(active_lineage.is_some());
    assert_eq!(active_resource, wrapper_resources[0]);

    let body = exact_name(&run.api, "ens", "bornwrapped.eth").await?;
    let active_lineage_string = active_lineage.map(|lineage| lineage.to_string());
    assert_eq!(
        pointer(&body, "/data/resource_id"),
        active_resource.to_string()
    );
    assert_eq!(
        pointer(&body, "/data/token_lineage_id").as_str(),
        active_lineage_string.as_deref()
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{alice:#x}")
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/expiry"),
        renewed_registrar_expiry
    );
    assert_eq!(
        pointer(&body, "/declared_state/wrapper_state"),
        "emancipated"
    );
    assert_eq!(
        pointer(&body, "/declared_state/control/registrant"),
        format!("{alice:#x}"),
        "wrapped-flow reconciliation should route control to the wrapper holder"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/authority_kind"),
        "registrar"
    );
    assert!(
        pointer(&body, "/declared_state/registration/authority_key")
            .as_str()
            .is_some_and(|key| key.starts_with("registrar:")),
        "born-wrapped authority key missing: {body}"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// A parent must burn CANNOT_UNWRAP before it can burn a parent-controlled
/// fuse on a child; setChildFuses ORs the live bitmap, while extendExpiry
/// emits ExpiryExtended after normalising to the parent expiry.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L517 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L963 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L443 @ ens_v1@91c966f)
/// Unwrap retains the fuse and expiry data; an unexpired rewrap restores the
/// parent-controlled fuse and larger prior expiry even though NameWrapped
/// carries the wrapping call's arguments.
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L235 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L239 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L242 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L246 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L901 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L902 @ ens_v1@91c966f)
#[tokio::test]
async fn parent_burns_pcc_then_extends_existing_child_expiry() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob, carol) = (accounts[1], accounts[2], accounts[3]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "fuseparent", alice, YEAR, resolver).await?;
    let registrar_expiry = ens_v1::eth_name_expiry(&rpc, &deployment, "fuseparent").await?;
    ens_v1::wrap_eth_2ld(
        &rpc,
        &deployment,
        alice,
        "fuseparent",
        bob,
        CANNOT_UNWRAP,
        resolver,
    )
    .await?;
    let parent_data = ens_v1::wrapped_name_data(&rpc, &deployment, "fuseparent.eth").await?;
    assert_eq!(parent_data.owner, bob);
    assert_ne!(parent_data.fuses & CANNOT_UNWRAP as u32, 0);
    assert_eq!(parent_data.expiry, registrar_expiry + GRACE_PERIOD);

    ens_v1::set_wrapped_subnode_owner(
        &rpc,
        &deployment,
        bob,
        ens_v1::WrappedSubnodeOwner {
            parent: "fuseparent.eth",
            label: "transition",
            owner: carol,
            fuses: 0,
            expiry: registrar_expiry,
        },
    )
    .await?;
    let child_before =
        ens_v1::wrapped_name_data(&rpc, &deployment, "transition.fuseparent.eth").await?;
    assert_eq!(child_before.owner, carol);
    assert_eq!(child_before.fuses, 0);
    assert_eq!(child_before.expiry, registrar_expiry);

    let fuse_tx = ens_v1::set_child_fuses(
        &rpc,
        &deployment,
        bob,
        "fuseparent.eth",
        "transition",
        PARENT_CANNOT_CONTROL,
        registrar_expiry,
    )
    .await?;
    let extend_tx = ens_v1::extend_child_expiry(
        &rpc,
        &deployment,
        bob,
        "fuseparent.eth",
        "transition",
        u64::MAX,
    )
    .await?;
    let child_after =
        ens_v1::wrapped_name_data(&rpc, &deployment, "transition.fuseparent.eth").await?;
    assert_eq!(child_after.owner, carol);
    assert_eq!(child_after.fuses, PARENT_CANNOT_CONTROL);
    assert_eq!(child_after.expiry, parent_data.expiry);

    ens_v1::unwrap_registry_name(
        &rpc,
        &deployment,
        carol,
        "fuseparent.eth",
        "transition",
        carol,
    )
    .await?;
    ens_v1::set_registry_approval_for_all(
        &rpc,
        &deployment,
        carol,
        deployment.name_wrapper.address,
        true,
    )
    .await?;
    let child_name = "transition.fuseparent.eth";
    let rewrap_tx =
        ens_v1::wrap_registry_name(&rpc, &deployment, carol, child_name, carol, Address::ZERO)
            .await?;
    let rewrapped = ens_v1::wrapped_name_data(&rpc, &deployment, child_name).await?;
    assert_eq!(rewrapped.owner, carol);
    assert_eq!(rewrapped.fuses, PARENT_CANNOT_CONTROL);
    assert_eq!(rewrapped.expiry, parent_data.expiry);

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3' \
             AND event_kind = 'PermissionScopeChanged' \
             AND (after_state->>'fuses')::BIGINT = {pcc} \
             AND transaction_hash = '{fuse_tx}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3' \
             AND event_kind = 'ExpiryChanged' \
             AND (after_state->>'expiry')::BIGINT = {final_expiry} \
             AND transaction_hash = '{extend_tx}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3' \
             AND event_kind = 'PermissionScopeChanged' \
             AND (after_state->>'fuses')::BIGINT = {pcc} \
             AND (after_state->>'expiry')::BIGINT = {final_expiry} \
             AND transaction_hash = '{rewrap_tx}' \
             AND canonicality_state = 'canonical')",
        pcc = PARENT_CANNOT_CONTROL,
        final_expiry = parent_data.expiry,
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let fuse_transitions: Vec<(Option<i64>, i64)> = sqlx::query_as(
        "SELECT (before_state->>'fuses')::BIGINT, \
                (after_state->>'fuses')::BIGINT \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3' \
           AND event_kind = 'PermissionScopeChanged' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index, event_identity",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        fuse_transitions,
        vec![
            (None, 0),
            (Some(0), PARENT_CANNOT_CONTROL as i64),
            (None, PARENT_CANNOT_CONTROL as i64),
        ]
    );

    let expiry_transitions: Vec<(Option<i64>, i64)> = sqlx::query_as(
        "SELECT (before_state->>'expiry')::BIGINT, \
                (after_state->>'expiry')::BIGINT \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3' \
           AND event_kind = 'ExpiryChanged' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index, event_identity",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        expiry_transitions,
        vec![
            (None, registrar_expiry as i64),
            (Some(registrar_expiry as i64), parent_data.expiry as i64),
            (None, parent_data.expiry as i64),
        ]
    );
    let fuse_tx_expiry_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3' \
           AND event_kind = 'ExpiryChanged' \
           AND transaction_hash = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&fuse_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        fuse_tx_expiry_events, 0,
        "setChildFuses at the existing expiry must not emit ExpiryExtended"
    );

    let event_resources: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT resource_id FROM normalized_events \
         WHERE logical_name_id = 'ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3' \
           AND event_kind IN ('PermissionScopeChanged', 'ExpiryChanged') \
           AND resource_id IS NOT NULL \
           AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        event_resources.len(),
        2,
        "rewrap must rotate the wrapper resource"
    );
    let rewrap_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND event_kind = 'PermissionScopeChanged' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&rewrap_tx)
    .fetch_one(&run.db.pool)
    .await?;
    let (active_resource, active_lineage, active_kind) = active_binding(
        &run.db.pool,
        "ens:0xe4704a24a660012d3847315355c68cc41069cecb265cf1cf5e98ef53debb84a3",
    )
    .await?;
    assert_eq!(active_kind, "wrapper");
    assert_eq!(active_resource, rewrap_resource);
    assert!(active_lineage.is_some());

    let body = exact_name(&run.api, "ens", "transition.fuseparent.eth").await?;
    let active_lineage_string = active_lineage.map(|lineage| lineage.to_string());
    assert_eq!(
        pointer(&body, "/data/resource_id"),
        active_resource.to_string()
    );
    assert_eq!(
        pointer(&body, "/data/token_lineage_id").as_str(),
        active_lineage_string.as_deref()
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/authority_kind"),
        "wrapper"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{carol:#x}")
    );
    assert_eq!(
        pointer(&body, "/declared_state/control/registrant"),
        format!("{carol:#x}")
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/expiry"),
        serde_json::Value::Null,
        "a wrapper-only subname has no registrar lease expiry"
    );
    assert_eq!(
        pointer(&body, "/declared_state/wrapper_state"),
        "emancipated"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// Generic wrap consumes a DNS-encoded registry name, uses registry operator
/// approval for its setOwner call, and emits NameWrapped with the full name.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L342 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
#[tokio::test]
async fn wrap_existing_registry_subname_rotates_child_only() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob, carol) = (accounts[1], accounts[2], accounts[3]);

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "registryparent",
        alice,
        YEAR,
        Address::ZERO,
    )
    .await?;
    ens_v1::transfer_eth_name_without_reclaim(&rpc, &deployment, alice, bob, "registryparent")
        .await?;
    ens_v1::create_subname(
        &rpc,
        &deployment,
        alice,
        "registryparent.eth",
        "plainchild",
        alice,
    )
    .await?;
    ens_v1::set_registry_approval_for_all(
        &rpc,
        &deployment,
        alice,
        deployment.name_wrapper.address,
        true,
    )
    .await?;
    let child_name = "plainchild.registryparent.eth";
    let wrap_tx =
        ens_v1::wrap_registry_name(&rpc, &deployment, alice, child_name, carol, Address::ZERO)
            .await?;

    // Wrapping an existing placeholder child reveals its label via
    // NameWrapped. Replay the complete immutable corpus and assert the
    // resulting normalized events and projections directly.
    let run = support::replay_full_corpus_projections(&anvil, &deployment).await?;
    let (child_wrapper_epoch, parent_registry_epoch): (bool, bool) = sqlx::query_as(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = $1 \
               AND event_kind = 'AuthorityEpochChanged' \
               AND after_state->>'authority_kind' = 'wrapper' \
               AND canonicality_state = 'canonical'), \
           EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:0xfb43d46f1fd1b637140404515fcb87f1aaa2c42faef41bd7313aff9b912dda05' \
               AND event_kind = 'AuthorityEpochChanged' \
               AND after_state->>'authority_kind' = 'registry_only' \
               AND canonicality_state = 'canonical')",
    )
    .bind(support::schema_v2_logical_name_id(&format!(
        "ens:{child_name}"
    )))
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        child_wrapper_epoch,
        "child must rotate to wrapper authority"
    );
    assert!(
        parent_registry_epoch,
        "parent must stay registry-only anchored"
    );

    // ENSRegistry.setOwner emits Transfer, while setSubnodeOwner emits
    // NewOwner. The raw wrapping transaction must contain only the former.
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L71 @ ens_v1@91c966f)
    let transfer_topic = format!("{:#x}", keccak256(b"Transfer(bytes32,address)"));
    let new_owner_topic = format!("{:#x}", keccak256(b"NewOwner(bytes32,bytes32,address)"));
    let (raw_transfers, raw_new_owners): (i64, i64) = sqlx::query_as(
        "SELECT \
           count(*) FILTER (WHERE topics[1] = $1), \
           count(*) FILTER (WHERE topics[1] = $2) \
         FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE emitting_address = $3 AND transaction_hash = $4 \
           AND lineage.canonicality_state = 'canonical'",
    )
    .bind(&transfer_topic)
    .bind(&new_owner_topic)
    .bind(format!("{:#x}", deployment.registry.address))
    .bind(&wrap_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(raw_transfers, 1);
    assert_eq!(raw_new_owners, 0);

    let wrap_owner_state: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'AuthorityTransferred' \
           AND source_family = 'ens_v1_registry_l1' \
           AND transaction_hash = $1 \
           AND after_state->>'node' = '0xe6cd46d3f5db891144f288bc594dad25f1ab8c1febd784b53000b461d0dc290f' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&wrap_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        wrap_owner_state["owner"],
        format!("{:#x}", deployment.name_wrapper.address)
    );
    assert_eq!(
        wrap_owner_state["node"],
        "0xe6cd46d3f5db891144f288bc594dad25f1ab8c1febd784b53000b461d0dc290f",
        "the registry Transfer must retain the wrapped child's node even before NameWrapped reveals its surface"
    );
    assert_eq!(
        wrap_owner_state["labelhash"],
        Value::Null,
        "registry Transfer path should not invent a NewOwner labelhash"
    );
    let wrap_subregistry_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND transaction_hash = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&wrap_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(wrap_subregistry_events, 0);

    let preimage: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'PreimageObserved' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND after_state->>'source_event' = 'NameWrapped' \
           AND after_state->>'raw_name' = $1 \
           AND transaction_hash = $2 \
           AND canonicality_state = 'canonical'",
    )
    .bind(child_name)
    .bind(&wrap_tx)
    .fetch_one(&run.db.pool)
    .await?;
    let child_labelhash = format!("{:#x}", ens_v1::labelhash("plainchild"));
    assert_eq!(preimage["raw_labels"][0], "plainchild");
    let label_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM label_preimages WHERE labelhash = $1")
            .bind(&child_labelhash)
            .fetch_one(&run.db.pool)
            .await?;
    assert!(
        label_rows >= 1,
        "NameWrapped label preimage was not projected"
    );

    let (child_resource, child_lineage, child_kind) = active_binding(
        &run.db.pool,
        "ens:0xe6cd46d3f5db891144f288bc594dad25f1ab8c1febd784b53000b461d0dc290f",
    )
    .await?;
    assert_eq!(child_kind, "wrapper");
    assert!(child_lineage.is_some());
    // The pre-wrap placeholder interval minted a registry-only resource but
    // never a surface binding (placeholder children have no surfaces); the
    // wrap is the child's first and only binding.
    let (registry_resource, registry_lineage): (Uuid, Option<Uuid>) = sqlx::query_as(
        "SELECT DISTINCT event.resource_id, resource.token_lineage_id \
         FROM normalized_events event \
         JOIN resources resource USING (resource_id) \
         WHERE event.transaction_hash = $1 \
           AND event.after_state->>'node' = '0xe6cd46d3f5db891144f288bc594dad25f1ab8c1febd784b53000b461d0dc290f' \
           AND event.after_state->>'authority_kind' = 'registry_only' \
           AND event.canonicality_state = 'canonical'",
    )
    .bind(&wrap_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_ne!(registry_resource, child_resource);
    assert_eq!(registry_lineage, None);
    let child_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings \
         WHERE logical_name_id = 'ens:0xe6cd46d3f5db891144f288bc594dad25f1ab8c1febd784b53000b461d0dc290f' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(child_bindings, 1, "the wrap is the child's only binding");

    let (child_projected_resource, child_projected_lineage, child_summary): (
        Uuid,
        Option<Uuid>,
        Value,
    ) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id, declared_summary \
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(support::schema_v2_logical_name_id(&format!(
        "ens:{child_name}"
    )))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(child_projected_resource, child_resource);
    assert_eq!(child_projected_lineage, child_lineage);
    assert_eq!(child_summary["registration"]["authority_kind"], "wrapper");
    assert_eq!(
        child_summary["registration"]["registrant"],
        format!("{carol:#x}")
    );
    assert_eq!(
        child_summary["control"]["registrant"],
        format!("{carol:#x}")
    );

    let (parent_resource, parent_lineage, parent_kind) = active_binding(
        &run.db.pool,
        "ens:0xfb43d46f1fd1b637140404515fcb87f1aaa2c42faef41bd7313aff9b912dda05",
    )
    .await?;
    assert_eq!(parent_kind, "registry_only");
    assert_eq!(parent_lineage, None);
    let (parent_projected_resource, parent_projected_lineage, parent_summary): (
        Uuid,
        Option<Uuid>,
        Value,
    ) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id, declared_summary \
         FROM name_current WHERE logical_name_id = 'ens:0xfb43d46f1fd1b637140404515fcb87f1aaa2c42faef41bd7313aff9b912dda05'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(parent_projected_resource, parent_resource);
    assert_eq!(parent_projected_lineage, None);
    assert_eq!(
        parent_summary["registration"]["authority_kind"],
        "registry_only"
    );
    assert_eq!(
        parent_summary["registration"]["registrant"],
        format!("{bob:#x}")
    );
    assert_eq!(parent_summary["control"]["registrant"], format!("{bob:#x}"));
    let retained_registry_owner: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:0xfb43d46f1fd1b637140404515fcb87f1aaa2c42faef41bd7313aff9b912dda05' \
           AND event_kind = 'AuthorityTransferred' \
           AND source_family = 'ens_v1_registry_l1' \
           AND lower(after_state->>'owner') = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(format!("{alice:#x}"))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        retained_registry_owner, 1,
        "the registrar-token transfer must not rewrite the parent's registry owner"
    );
    let parent_wrapper_resources: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:0xfb43d46f1fd1b637140404515fcb87f1aaa2c42faef41bd7313aff9b912dda05' \
           AND after_state->>'authority_kind' = 'wrapper' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        parent_wrapper_resources, 0,
        "generic child wrap must not wrap or re-anchor the parent"
    );

    run.db.cleanup().await?;
    Ok(())
}
