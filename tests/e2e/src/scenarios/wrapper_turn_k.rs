use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::types::Uuid;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;
const CANNOT_UNWRAP: u16 = 1;
const PARENT_CANNOT_CONTROL: u32 = 1 << 16;

fn pointer(body: &Value, path: &str) -> Value {
    body.pointer(path).cloned().unwrap_or(Value::Null)
}

async fn exact_name(run: &support::PipelineRun, name: &str) -> Result<Value> {
    let (status, body) = run.api.get_json(&format!("/v1/names/ens/{name}")).await?;
    assert_eq!(status, 200, "exact-name lookup for {name} failed: {body}");
    Ok(body)
}

async fn active_binding(
    pool: &sqlx::PgPool,
    logical_name_id: &str,
) -> Result<(Uuid, Option<Uuid>, String)> {
    sqlx::query_as(
        "SELECT binding.resource_id, resource.token_lineage_id, \
                resource.provenance->>'authority_kind' \
         FROM surface_bindings binding \
         JOIN resources resource USING (resource_id) \
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
#[tokio::test]
async fn born_wrapped_registration_exposes_trailing_grant_rebind() -> Result<()> {
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
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:bornwrapped.eth' \
             AND event_kind = 'RegistrationGranted' \
             AND source_family = 'ens_v1_registrar_l1' \
             AND transaction_hash = '{tx_hash}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:bornwrapped.eth' \
             AND event_kind = 'ExpiryChanged' \
             AND source_family = 'ens_v1_wrapper_l1' \
             AND transaction_hash = '{tx_hash}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:bornwrapped.eth' \
             AND event_kind = 'AuthorityTransferred' \
             AND lower(after_state->>'owner') = '{wrapper:#x}' \
             AND transaction_hash = '{tx_hash}' \
             AND canonicality_state = 'canonical')",
        wrapper = deployment.name_wrapper.address,
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let registration: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE logical_name_id = 'ens:bornwrapped.eth' \
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
        "SELECT to_address FROM raw_transactions \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
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
         WHERE logical_name_id = 'ens:bornwrapped.eth' \
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

    // PreimageObserved is a repair event for already-known hash-only
    // identities; a born-wrapped registration introduces the name with its
    // label in the same round, so none derives anywhere in the corpus.
    let preimage_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind = 'PreimageObserved' AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        preimage_count, 0,
        "born-wrapped introduction needs no preimage repair"
    );

    let registry_owner_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:bornwrapped.eth' \
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
         WHERE logical_name_id = 'ens:bornwrapped.eth' \
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
         WHERE logical_name_id = 'ens:bornwrapped.eth' \
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
           count(*) FILTER (WHERE provenance->>'authority_kind' = 'registrar'), \
           count(*) FILTER (WHERE provenance->>'authority_kind' = 'registry_only'), \
           count(*) FILTER (WHERE provenance->>'authority_kind' = 'wrapper') \
         FROM resources \
         WHERE provenance->>'logical_name_id' = 'ens:bornwrapped.eth' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        resource_shape,
        (1, 1, 1),
        "pinned born-wrapped rebind should retain registrar, registry-only, and wrapper resources"
    );
    let wrapper_bound: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:bornwrapped.eth' \
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

    // REVIEW POINT (pinned observed implementation shape): NameWrapped is
    // earlier than the controller's NameRegistered. The adapter's stale-wrap
    // guard therefore clears the just-created wrapper and rebinds through
    // registry-only to registrar. Boundary events have no log index and sort
    // by identity after the raw grant, so exact-name's authority_kind/key end
    // on registry_only even while /data identifies the active registrar
    // resource. Born-wrapped registration therefore disproves the ledger's
    // earlier hypothesis that only the post-registration wrap window was
    // stale.
    let (active_resource, active_lineage, active_kind) =
        active_binding(&run.db.pool, "ens:bornwrapped.eth").await?;
    assert_eq!(active_kind, "registrar");
    assert!(active_lineage.is_some());
    assert_ne!(active_resource, wrapper_resources[0]);

    let body = exact_name(&run, "bornwrapped.eth").await?;
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
        registrar_expiry
    );
    assert_eq!(
        pointer(&body, "/declared_state/control/registry_owner"),
        format!("{:#x}", deployment.name_wrapper.address)
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/authority_kind"),
        "registry_only"
    );
    assert!(
        pointer(&body, "/declared_state/registration/authority_key")
            .as_str()
            .is_some_and(|key| key.starts_with("registry-only:")),
        "pinned mixed born-wrapped authority key missing: {body}"
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

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:transition.fuseparent.eth' \
             AND event_kind = 'PermissionScopeChanged' \
             AND (after_state->>'fuses')::BIGINT = {pcc} \
             AND transaction_hash = '{fuse_tx}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE logical_name_id = 'ens:transition.fuseparent.eth' \
             AND event_kind = 'ExpiryChanged' \
             AND (after_state->>'expiry')::BIGINT = {final_expiry} \
             AND transaction_hash = '{extend_tx}' \
             AND canonicality_state = 'canonical')",
        pcc = PARENT_CANNOT_CONTROL,
        final_expiry = parent_data.expiry,
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let fuse_transitions: Vec<(Option<i64>, i64)> = sqlx::query_as(
        "SELECT (before_state->>'fuses')::BIGINT, \
                (after_state->>'fuses')::BIGINT \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:transition.fuseparent.eth' \
           AND event_kind = 'PermissionScopeChanged' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index, event_identity",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        fuse_transitions,
        vec![(None, 0), (Some(0), PARENT_CANNOT_CONTROL as i64)]
    );

    let expiry_transitions: Vec<(Option<i64>, i64)> = sqlx::query_as(
        "SELECT (before_state->>'expiry')::BIGINT, \
                (after_state->>'expiry')::BIGINT \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:transition.fuseparent.eth' \
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
        ]
    );
    let fuse_tx_expiry_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:transition.fuseparent.eth' \
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
         WHERE logical_name_id = 'ens:transition.fuseparent.eth' \
           AND event_kind IN ('PermissionScopeChanged', 'ExpiryChanged') \
           AND resource_id IS NOT NULL \
           AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(event_resources.len(), 1, "child wrapper resource rotated");
    let (active_resource, active_lineage, active_kind) =
        active_binding(&run.db.pool, "ens:transition.fuseparent.eth").await?;
    assert_eq!(active_kind, "wrapper");
    assert_eq!(active_resource, event_resources[0]);
    assert!(active_lineage.is_some());

    let body = exact_name(&run, "transition.fuseparent.eth").await?;
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
        pointer(&body, "/declared_state/control/registry_owner"),
        format!("{:#x}", deployment.name_wrapper.address)
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/expiry"),
        parent_data.expiry
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
    // NameWrapped — the same live-intake reveal wedge pinned on the
    // registration path (chipped): the run loop hangs before checkpoint
    // promotion. Derivation and projections are pinned via backfill +
    // replay instead; API reads are impossible on this path.
    let run =
        support::backfill_and_replay_projections(&anvil, &deployment, "wrap-registry-subname")
            .await?;
    let (child_wrapper_epoch, parent_registry_epoch): (bool, bool) = sqlx::query_as(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = $1 \
               AND event_kind = 'AuthorityEpochChanged' \
               AND after_state->>'authority_kind' = 'wrapper' \
               AND canonicality_state = 'canonical'), \
           EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:registryparent.eth' \
               AND event_kind = 'AuthorityEpochChanged' \
               AND after_state->>'authority_kind' = 'registry_only' \
               AND canonicality_state = 'canonical')",
    )
    .bind(format!("ens:{child_name}"))
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
         FROM raw_logs \
         WHERE emitting_address = $3 AND transaction_hash = $4 \
           AND canonicality_state = 'canonical'",
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
         WHERE logical_name_id = 'ens:plainchild.registryparent.eth' \
           AND event_kind = 'AuthorityTransferred' \
           AND source_family = 'ens_v1_registry_l1' \
           AND transaction_hash = $1 \
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
        wrap_owner_state["labelhash"], "",
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
           AND after_state->>'decoded_name' = $1 \
           AND transaction_hash = $2 \
           AND canonicality_state = 'canonical'",
    )
    .bind(child_name)
    .bind(&wrap_tx)
    .fetch_one(&run.db.pool)
    .await?;
    let child_labelhash = format!("{:#x}", ens_v1::labelhash("plainchild"));
    assert_eq!(preimage["labelhashes"][0], child_labelhash);
    let label_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM label_preimages WHERE labelhash = $1")
            .bind(&child_labelhash)
            .fetch_one(&run.db.pool)
            .await?;
    assert!(
        label_rows >= 1,
        "NameWrapped label preimage was not projected"
    );

    let (child_resource, child_lineage, child_kind) =
        active_binding(&run.db.pool, "ens:plainchild.registryparent.eth").await?;
    assert_eq!(child_kind, "wrapper");
    assert!(child_lineage.is_some());
    // The pre-wrap placeholder interval minted a registry-only resource but
    // never a surface binding (placeholder children have no surfaces); the
    // wrap is the child's first and only binding.
    let (registry_resource, registry_lineage): (Uuid, Option<Uuid>) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id FROM resources \
         WHERE provenance->>'logical_name_id' = 'ens:plainchild.registryparent.eth' \
           AND provenance->>'authority_kind' = 'registry_only' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_ne!(registry_resource, child_resource);
    assert_eq!(registry_lineage, None);
    let child_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings \
         WHERE logical_name_id = 'ens:plainchild.registryparent.eth' \
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
    .bind(format!("ens:{child_name}"))
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
        child_summary["control"]["registry_owner"],
        format!("{:#x}", deployment.name_wrapper.address)
    );

    let (parent_resource, parent_lineage, parent_kind) =
        active_binding(&run.db.pool, "ens:registryparent.eth").await?;
    assert_eq!(parent_kind, "registry_only");
    assert_eq!(parent_lineage, None);
    let (parent_projected_resource, parent_projected_lineage, parent_summary): (
        Uuid,
        Option<Uuid>,
        Value,
    ) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id, declared_summary \
         FROM name_current WHERE logical_name_id = 'ens:registryparent.eth'",
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
    assert_eq!(
        parent_summary["control"]["registry_owner"],
        format!("{alice:#x}")
    );
    let parent_wrapper_resources: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resources \
         WHERE provenance->>'logical_name_id' = 'ens:registryparent.eth' \
           AND provenance->>'authority_kind' = 'wrapper' \
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
