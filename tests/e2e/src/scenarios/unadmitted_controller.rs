use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::responses::{exact_name, pointer};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

/// An owner-added controller registers directly on the registrar
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110 @ ens_v1@91c966f).
/// The admitted registrar event retains the authoritative lease even though the
/// unadmitted controller contributes no plaintext name. Exact-name and address-name
/// routes therefore remain empty.
#[tokio::test]
async fn unadmitted_controller_registration_retains_resource_keyed_registrar_lease() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (carol, registrant) = (accounts[3], accounts[4]);

    ens_v1::add_registrar_controller(&rpc, &deployment, carol).await?;
    ens_v1::register_via_registrar(&rpc, &deployment, carol, "shadow", registrant, YEAR).await?;

    let shadow_node = format!("{:#x}", ens_v1::namehash("shadow.eth"));
    let shadow_labelhash = format!("{:#x}", ens_v1::labelhash("shadow"));
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
         AND after_state->>'child_node' = '{shadow_node}' \
         AND canonicality_state = 'canonical')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let register_tx: String = sqlx::query_scalar(
        "SELECT transaction_hash FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE emitting_address = $1 AND topics[4] = $2 \
         AND lineage.canonicality_state = 'canonical' LIMIT 1",
    )
    .bind(format!("{:#x}", deployment.base_registrar.address))
    .bind(&shadow_labelhash)
    .fetch_one(&run.db.pool)
    .await?;

    // The BaseRegistrar logs persist raw: the ERC721 mint and the
    // uint256-id NameRegistered both live in the transaction's log set.
    let registrar_raw_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE emitting_address = $1 AND transaction_hash = $2 \
         AND lineage.canonicality_state = 'canonical'",
    )
    .bind(format!("{:#x}", deployment.base_registrar.address))
    .bind(&register_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        registrar_raw_logs >= 2,
        "expected registrar mint + NameRegistered raw logs, saw {registrar_raw_logs}"
    );

    // Schema-v2 retains both the registry observation and the authoritative
    // BaseRegistrar lifecycle. Those rows have `resource_id` but no
    // `logical_name_id`.
    let derived_kinds: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_kind, source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical' \
         ORDER BY block_number, transaction_index, log_index, normalized_event_id",
    )
    .bind(&register_tx)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        derived_kinds,
        vec![
            (
                "SubregistryChanged".to_owned(),
                "ens_v1_registry_l1".to_owned(),
            ),
            (
                "AuthorityTransferred".to_owned(),
                "ens_v1_registry_l1".to_owned(),
            ),
            (
                "PermissionChanged".to_owned(),
                "ens_v1_registry_l1".to_owned(),
            ),
            (
                "RegistrationGranted".to_owned(),
                "ens_v1_registrar_l1".to_owned(),
            ),
            ("ExpiryChanged".to_owned(), "ens_v1_registrar_l1".to_owned(),),
            (
                "PermissionChanged".to_owned(),
                "ens_v1_registrar_l1".to_owned(),
            ),
            (
                "AuthorityEpochChanged".to_owned(),
                "ens_v1_registrar_l1".to_owned(),
            ),
        ],
        "unadmitted-controller registration must retain registrar lifecycle facets"
    );
    let lease_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind IN ('RegistrationGranted', 'TokenControlTransferred', \
                              'ExpiryChanged', 'RegistrationRenewed') \
         AND (after_state->>'labelhash' = $1 \
              OR after_state->>'child_node' = $2 \
              OR logical_name_id = 'ens:0x71912a92f1d7b9f48a8ccc1e1a7bcc3ed43e88c682cb276692e6618bb96437ae') \
         AND canonicality_state = 'canonical'",
    )
    .bind(&shadow_labelhash)
    .bind(&shadow_node)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        lease_events, 2,
        "the registrar must retain grant and expiry facts"
    );
    let resource_keyed_lease_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash = $1 \
         AND event_kind IN ('RegistrationGranted', 'ExpiryChanged') \
         AND logical_name_id IS NULL AND resource_id IS NOT NULL \
         AND canonicality_state = 'canonical'",
    )
    .bind(&register_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(resource_keyed_lease_events, 2);

    // `children_current` is keyed by a routeable parent surface. The harness
    // has no `.eth` parent surface, so the registry fact remains normalized
    // evidence rather than becoming a child row.
    let child_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM children_current WHERE namehash = $1")
            .bind(&shadow_node)
            .fetch_one(&run.db.pool)
            .await?;
    assert_eq!(
        child_rows, 0,
        "a resource-keyed registration must not invent a child without a parent surface"
    );
    let surfaces: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_surfaces WHERE logical_name_id = $1")
            .bind("ens:0x71912a92f1d7b9f48a8ccc1e1a7bcc3ed43e88c682cb276692e6618bb96437ae")
            .fetch_one(&run.db.pool)
            .await?;
    assert_eq!(surfaces, 0, "no exact-name surface may be minted");
    let (status, body) = run.api.get_json("/v1/names/ens/shadow.eth").await?;
    assert_eq!(status, 404, "shadow.eth must stay unknown: {body}");

    let registrant_names: Value = {
        let (status, body) = run
            .api
            .get_json(&format!(
                "/v1/addresses/{registrant:#x}/names?namespace=ens&relation=registrant"
            ))
            .await?;
        assert_eq!(status, 200, "registrant collection failed: {body}");
        body
    };
    let entries = registrant_names
        .pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        entries.is_empty(),
        "a lease without a surface must not appear in a name collection: {entries:?}"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn later_wrap_exposes_unadmitted_controller_registrar_owner_and_expiry() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (controller, registrant, wrapped_owner) = (accounts[3], accounts[4], accounts[5]);

    ens_v1::add_registrar_controller(&rpc, &deployment, controller).await?;
    ens_v1::register_via_registrar(&rpc, &deployment, controller, "laterwrap", registrant, YEAR)
        .await?;
    let registrar_expiry = ens_v1::eth_name_expiry(&rpc, &deployment, "laterwrap").await?;
    ens_v1::wrap_eth_2ld(
        &rpc,
        &deployment,
        registrant,
        "laterwrap",
        wrapped_owner,
        0,
        deployment.public_resolver.address,
    )
    .await?;

    let logical_name_id = support::schema_v2_logical_name_id("ens:laterwrap.eth");
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{logical_name_id}' \
           AND event_kind = 'SurfaceBound' \
           AND source_family = 'ens_v1_wrapper_l1' \
           AND canonicality_state = 'canonical')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
    let body = exact_name(&run.api, "ens", "laterwrap.eth").await?;
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{registrant:#x}")
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/expiry"),
        registrar_expiry
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/authority_kind"),
        "wrapper"
    );
    run.db.cleanup().await?;
    Ok(())
}
