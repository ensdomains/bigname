use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, families, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

/// An owner-added controller registers directly on the registrar
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110 @ ens_v1@91c966f).
/// This Mainnet profile does not admit the registrar-level uint256 lifecycle,
/// so only registry facts derive. The proven registry path names the child and
/// its control binding, while the unadmitted registration creates no lease facts.
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

    // Nothing lease-bearing derives. Schema-v2 expands the registry-side
    // child edge into its named authority, binding and permission facets;
    // every derived event remains a registry-family fact.
    let mut derived_kinds: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_kind, source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&register_tx)
    .fetch_all(&run.db.pool)
    .await?;
    derived_kinds.sort_unstable();
    assert_eq!(
        derived_kinds,
        vec![
            (
                "AuthorityEpochChanged".to_owned(),
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
                "SubregistryChanged".to_owned(),
                "ens_v1_registry_l1".to_owned(),
            ),
            ("SurfaceBound".to_owned(), "ens_v1_registry_l1".to_owned(),),
        ],
        "unadmitted-controller registration must derive only registry-side facets"
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
    assert_eq!(lease_events, 0, "no lease facts may derive for shadow.eth");

    // The root-proven eth path makes this registry child addressable without
    // attributing the unadmitted registrar's lease or token lineage to it.
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
    assert_eq!(row.token_lineage_id, None);
    assert_eq!(row.declared_summary["registration"]["expiry"], Value::Null);
    assert_eq!(
        row.declared_summary["registration"]["authority_kind"],
        "registry_only"
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
        "the direct registry owner holds the named child: {entries:?}"
    );
    assert_eq!(entries[0]["namehash"], shadow_node);
    assert_eq!(entries[0]["token_lineage_id"], Value::Null);

    run.db.cleanup().await?;
    Ok(())
}
