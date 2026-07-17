use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

/// An owner-added controller registers directly on the registrar
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110 @ ens_v1@91c966f).
/// The registrar-level uint256 events are outside every active manifest ABI,
/// so the pipeline sees only the registry-side normalized event. With no
/// routeable `.eth` parent surface, no child projection, lease facts, or
/// exact-name surface materializes.
#[tokio::test]
async fn unadmitted_controller_registration_derives_registry_side_only() -> Result<()> {
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
        "SELECT transaction_hash FROM raw_logs \
         WHERE emitting_address = $1 AND topics[4] = $2 \
         AND canonicality_state = 'canonical' LIMIT 1",
    )
    .bind(format!("{:#x}", deployment.base_registrar.address))
    .bind(&shadow_labelhash)
    .fetch_one(&run.db.pool)
    .await?;

    // The registrar-plane facts persist raw: the ERC721 mint and the
    // uint256-id NameRegistered both live in the transaction's log set.
    let registrar_raw_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE emitting_address = $1 AND transaction_hash = $2 \
         AND canonicality_state = 'canonical'",
    )
    .bind(format!("{:#x}", deployment.base_registrar.address))
    .bind(&register_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        registrar_raw_logs >= 2,
        "expected registrar mint + NameRegistered raw logs, saw {registrar_raw_logs}"
    );

    // Nothing lease-bearing derives: the only normalized event from the
    // transaction is the registry-side child edge.
    let derived_kinds: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_kind, source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&register_tx)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        derived_kinds,
        vec![(
            "SubregistryChanged".to_owned(),
            "ens_v1_registry_l1".to_owned(),
        )],
        "unadmitted-controller registration must derive exactly one registry-side event"
    );
    let lease_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind IN ('RegistrationGranted', 'TokenControlTransferred', \
                              'ExpiryChanged', 'RegistrationRenewed') \
         AND (after_state->>'labelhash' = $1 \
              OR after_state->>'child_node' = $2 \
              OR logical_name_id = 'ens:shadow.eth') \
         AND canonicality_state = 'canonical'",
    )
    .bind(&shadow_labelhash)
    .bind(&shadow_node)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(lease_events, 0, "no lease facts may derive for shadow.eth");

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
        "unadmitted registration must not invent a child without a parent surface"
    );
    let surfaces: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_surfaces WHERE logical_name_id = $1")
            .bind("ens:shadow.eth")
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
        "an unadmitted-controller lease must not appear as a registration: {entries:?}"
    );

    run.db.cleanup().await?;
    Ok(())
}
