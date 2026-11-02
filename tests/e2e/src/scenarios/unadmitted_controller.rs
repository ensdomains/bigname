use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, families, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

/// An owner-added controller registers directly on the registrar
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110 @ ens_v1@91c966f).
/// No admitted controller event follows the registrar's uint256 `NameRegistered`
/// in its transaction, so the Mainnet profile interprets it as the lease's grant
/// (TYR-449). The proven registry path names the child, and the lease and its
/// token lineage are the name's, keyed by the token id.
#[tokio::test]
async fn unadmitted_controller_registration_derives_the_registrar_lease() -> Result<()> {
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

    // The registrar-plane facts persist raw: the ERC721 mint and the
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

    // The registrar's own events derive the lease: exactly one grant, named.
    let grants: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT logical_name_id FROM normalized_events \
         WHERE transaction_hash = $1 AND event_kind = 'RegistrationGranted' \
         AND source_family = 'ens_v1_registrar_l1' AND canonicality_state = 'canonical'",
    )
    .bind(&register_tx)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        grants,
        vec![Some(
            "ens:0x71912a92f1d7b9f48a8ccc1e1a7bcc3ed43e88c682cb276692e6618bb96437ae".to_owned()
        )],
        "the registrar registration grants one named lease"
    );
    let lease_rows: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events \
         WHERE transaction_hash = $1 AND source_family = 'ens_v1_registrar_l1' \
         AND after_state->>'labelhash' = $2 AND canonicality_state = 'canonical'",
    )
    .bind(&register_tx)
    .bind(&shadow_labelhash)
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        lease_rows.iter().any(|kind| kind == "ExpiryChanged"),
        "the lease's expiry derives: {lease_rows:?}"
    );

    let child_rows = families::served_child_rows(&run.db.pool, &shadow_node).await?;
    assert_eq!(
        child_rows, 1,
        "the admitted registry child must be served under the proven parent"
    );
    let surfaces: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_surfaces WHERE logical_name_id = $1")
            .bind("ens:0x71912a92f1d7b9f48a8ccc1e1a7bcc3ed43e88c682cb276692e6618bb96437ae")
            .fetch_one(&run.db.pool)
            .await?;
    assert_eq!(surfaces, 1, "the registry path proves the name surface");
    let row = families::required_name(&run.db.pool, &format!("ens:{shadow_node}")).await?;
    assert!(row.token_lineage_id.is_some(), "{:?}", row.declared_summary);
    assert_ne!(row.declared_summary["registration"]["expiry"], Value::Null);
    assert_eq!(
        row.declared_summary["registration"]["authority_kind"],
        "registrar"
    );

    let registrant_names: Value = {
        let (status, body) = run
            .api
            .get_json(&format!(
                "/v1/addresses/{registrant:#x}/names?namespace=ens&relation=token_holder"
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
    assert_eq!(
        entries.len(),
        1,
        "the registrant holds the lease: {entries:?}"
    );
    assert_eq!(entries[0]["namehash"], shadow_node);
    assert!(entries[0]["token_lineage_id"].is_string(), "{entries:?}");

    run.db.cleanup().await?;
    Ok(())
}
