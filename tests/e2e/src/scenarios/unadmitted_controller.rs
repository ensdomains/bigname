use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

/// An owner-added controller registers directly on the registrar
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110 @ ens_v1@91c966f).
/// No admitted controller event carries the label, so the registrar's own
/// uint256-id `NameRegistered` is the source of the registration and expiry
/// facts, flagged `controller_admitted = false` and carried by the resource
/// rather than a name. With no label and no routeable `.eth` parent surface,
/// no child projection or exact-name surface materializes.
#[tokio::test]
async fn unadmitted_controller_registration_derives_flagged_registrar_facts_without_a_surface()
-> Result<()> {
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

    // The registry-side child edge expands into its authority and permission
    // facets; the registrar's own NameRegistered supplies the lease facts.
    let derived_kinds: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_kind, source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical' \
         ORDER BY normalized_event_id",
    )
    .bind(&register_tx)
    .fetch_all(&run.db.pool)
    .await?;
    let owned = |kind: &str, family: &str| (kind.to_owned(), family.to_owned());
    assert_eq!(
        derived_kinds,
        vec![
            owned("SubregistryChanged", "ens_v1_registry_l1"),
            owned("AuthorityTransferred", "ens_v1_registry_l1"),
            owned("PermissionChanged", "ens_v1_registry_l1"),
            owned("RegistrationGranted", "ens_v1_registrar_l1"),
            owned("ExpiryChanged", "ens_v1_registrar_l1"),
            owned("PermissionChanged", "ens_v1_registrar_l1"),
            owned("AuthorityEpochChanged", "ens_v1_registrar_l1"),
        ],
        "unadmitted-controller registration derives registry facets plus flagged registrar facts"
    );
    // The lease facts are flagged, label-less, and carried by the resource:
    // nothing links them to a name identity.
    let lease_facts: Vec<(String, Option<String>, Value)> = sqlx::query_as(
        "SELECT event_kind, logical_name_id, after_state FROM normalized_events \
         WHERE event_kind IN ('RegistrationGranted', 'TokenControlTransferred', \
                              'ExpiryChanged', 'RegistrationRenewed') \
         AND (after_state->>'labelhash' = $1 \
              OR after_state->>'child_node' = $2 \
              OR logical_name_id = 'ens:0x71912a92f1d7b9f48a8ccc1e1a7bcc3ed43e88c682cb276692e6618bb96437ae') \
         AND canonicality_state = 'canonical' \
         ORDER BY normalized_event_id",
    )
    .bind(&shadow_labelhash)
    .bind(&shadow_node)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        lease_facts
            .iter()
            .map(|(kind, _, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["RegistrationGranted", "ExpiryChanged"],
        "{lease_facts:?}"
    );
    for (kind, logical_name_id, after_state) in &lease_facts {
        assert_eq!(
            logical_name_id.as_deref(),
            None,
            "{kind} must not name a surface"
        );
        assert_eq!(
            after_state["controller_admitted"], false,
            "{kind}: {after_state}"
        );
        assert_eq!(after_state["surface_known"], false, "{kind}: {after_state}");
        assert_eq!(
            after_state["labelhash"], shadow_labelhash,
            "{kind}: {after_state}"
        );
        assert_eq!(
            after_state["registrant"],
            format!("{registrant:#x}"),
            "{kind}: {after_state}"
        );
        assert!(after_state["expiry"].is_i64(), "{kind}: {after_state}");
    }

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
        "a label-less registration must not invent a child without a parent surface"
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
        "a label-less lease has no name to appear under: {entries:?}"
    );

    run.db.cleanup().await?;
    Ok(())
}
