use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::responses::{exact_name, pointer, primary_name};
use crate::harness::{anvil::Anvil, basenames, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

async fn records(run: &support::PipelineRun, name: &str) -> Result<Value> {
    let (status, body) = run
        .api
        .get_json(&format!(
            "/v1/names/basenames/{name}/records?coin_types=60&mode=declared"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "Basenames records lookup for {name} failed: {body}"
    );
    Ok(body)
}

fn assert_exact_control(body: &Value, registrant: &str, registry_owner: &str, latest: &str) {
    assert_eq!(
        pointer(body, "/declared_state/control/registrant"),
        registrant,
        "registrant facet should match; body: {body}"
    );
    assert_eq!(
        pointer(body, "/declared_state/control/registry_owner"),
        registry_owner,
        "registry owner facet should match; body: {body}"
    );
    // Control sections end on the epoch event after any authority
    // divergence dance (same shape as the ENSv1 scenarios); the transfer
    // events themselves stay visible as history heads.
    assert_eq!(
        pointer(body, "/declared_state/control/latest_event_kind"),
        latest,
        "control latest event should match; body: {body}"
    );
}

fn assert_declared_primary(body: &Value, expected_name: &str) {
    assert_eq!(
        pointer(body, "/declared_state/claimed_primary_name/status"),
        "success",
        "Basenames declared primary should be a candidate only; body: {body}"
    );
    assert_eq!(
        pointer(body, "/declared_state/claimed_primary_name/name"),
        expected_name,
        "Basenames declared primary should follow NameForAddrChanged; body: {body}"
    );
    assert_eq!(
        pointer(
            body,
            "/declared_state/claimed_primary_name/provenance/source_family"
        ),
        "basenames_base_primary",
        "Basenames primary claim should come from the Base primary source family; body: {body}"
    );
}

/// Basenames declared-state matrix on a local Base chain: label-bearing
/// registration, independent registrar-token and registry-owner control
/// facets, L2Resolver address changes, and Base coin-type primary set/unset.
#[tokio::test]
async fn basenames_declared_state_matrix_end_to_end() -> Result<()> {
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();

    let deployment = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];
    let bob = accounts[2];
    let resolved = accounts[3];
    let alice_path = format!("{alice:#x}");
    let bob_path = format!("{bob:#x}");
    let resolved_path = format!("{resolved:#x}");

    basenames::register_base_name(&rpc, &deployment, alice, "alice", alice, YEAR).await?;
    basenames::set_addr_record(&rpc, &deployment, alice, "alice.base.eth", resolved).await?;
    basenames::set_primary_name(&rpc, &deployment, alice, "alice.base.eth").await?;

    basenames::register_base_name(&rpc, &deployment, alice, "nftonly", alice, YEAR).await?;
    basenames::transfer_base_token(&rpc, &deployment, alice, bob, "nftonly").await?;

    basenames::register_base_name(&rpc, &deployment, alice, "mgmtonly", alice, YEAR).await?;
    basenames::set_registry_owner(&rpc, &deployment, alice, "mgmtonly.base.eth", bob).await?;

    basenames::register_base_name(&rpc, &deployment, alice, "fullxfer", alice, YEAR).await?;
    basenames::transfer_base_token(&rpc, &deployment, alice, bob, "fullxfer").await?;
    basenames::reclaim_base_name(&rpc, &deployment, bob, bob, "fullxfer").await?;

    let ready_sql = format!(
        "SELECT
           (SELECT COUNT(DISTINCT logical_name_id) >= 4 FROM normalized_events
            WHERE namespace = 'basenames'
              AND event_kind = 'RegistrationGranted'
              AND canonicality_state = 'canonical'
              AND logical_name_id IN (
                'basenames:0xb200d2bfac166c814e32bfb9f34bde9c866514a1501c47af54d0fffa03a6bfee',
                'basenames:0xe002a767598a48d137d7d6efb2d8552348b73aea05b8694250b6d7090c50304f',
                'basenames:0xb89f7a452c9b08a6c151db6cef7bfee3cd51e0b51eb34cc53f61b79dd05bef00',
                'basenames:0xc9e9183bd20413846f8340975867177d4348b75ac6ff88665295214b676c8594'
              ))
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'basenames:0xb200d2bfac166c814e32bfb9f34bde9c866514a1501c47af54d0fffa03a6bfee'
               AND source_family = 'basenames_base_resolver'
               AND event_kind = 'RecordChanged'
               AND canonicality_state = 'canonical'
               AND after_state->>'record_key' = 'addr:60'
               AND lower(after_state->>'value') = '{resolved_path}'
           )
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE source_family = 'basenames_base_primary'
               AND event_kind = 'RecordChanged'
               AND canonicality_state = 'canonical'
               AND after_state->>'raw_name' = 'alice.base.eth'
               AND lower(after_state->'primary_claim_source'->>'address') = '{alice_path}'
           )
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'basenames:0xe002a767598a48d137d7d6efb2d8552348b73aea05b8694250b6d7090c50304f'
               AND event_kind = 'TokenControlTransferred'
               AND canonicality_state = 'canonical'
           )
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'basenames:0xb89f7a452c9b08a6c151db6cef7bfee3cd51e0b51eb34cc53f61b79dd05bef00'
               AND event_kind = 'AuthorityTransferred'
               AND canonicality_state = 'canonical'
           )
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'basenames:0xc9e9183bd20413846f8340975867177d4348b75ac6ff88665295214b676c8594'
               AND event_kind = 'AuthorityTransferred'
               AND canonicality_state = 'canonical'
           )"
    );
    let run = support::ingest_basenames_and_serve(&base, &deployment, Some(&ready_sql)).await?;

    let raw_primary_logs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(format!(
                "{:#x}",
                deployment.primary_reverse_registrar.address
            ))
            .fetch_one(&run.db.pool)
            .await?;
    assert!(
        raw_primary_logs >= 1,
        "expected raw NameForAddrChanged logs from the Base primary registrar"
    );
    let registration_events: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT logical_name_id) FROM normalized_events
         WHERE namespace = 'basenames'
           AND event_kind = 'RegistrationGranted'
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(registration_events, 4);

    let alice_body = exact_name(&run.api, "basenames", "alice.base.eth").await?;
    assert_eq!(
        pointer(&alice_body, "/data/logical_name_id"),
        "basenames:0xb200d2bfac166c814e32bfb9f34bde9c866514a1501c47af54d0fffa03a6bfee"
    );
    assert_eq!(pointer(&alice_body, "/data/namespace"), "basenames");
    assert_eq!(
        pointer(&alice_body, "/data/binding_kind"),
        "declared_registry_path"
    );
    assert_eq!(pointer(&alice_body, "/coverage/status"), "projected");
    assert_eq!(
        pointer(&alice_body, "/declared_state/resolver/chain_id"),
        "base-mainnet"
    );
    assert_eq!(
        pointer(&alice_body, "/declared_state/resolver/address"),
        format!("{:#x}", deployment.l2_resolver.address)
    );
    // A fresh registration's control section ends on the epoch event, same
    // as the ENSv1 scenarios' observed shape.
    assert_eq!(
        pointer(&alice_body, "/declared_state/control/registrant"),
        alice_path
    );
    assert_eq!(
        pointer(&alice_body, "/declared_state/control/registry_owner"),
        Value::Null,
        "first-ownership setup is not a later control transfer"
    );
    assert_eq!(
        pointer(&alice_body, "/declared_state/control/latest_event_kind"),
        "AuthorityEpochChanged"
    );

    let alice_records = records(&run, "alice.base.eth").await?;
    assert_eq!(
        pointer(&alice_records, "/data/coin_addresses/60/status"),
        "success",
        "L2Resolver addr:60 should be exposed as a declared record; body: {alice_records}"
    );
    assert_eq!(
        pointer(&alice_records, "/data/coin_addresses/60/value"),
        resolved_path,
        "L2Resolver addr:60 should match the changed address; body: {alice_records}"
    );

    let nft_only = exact_name(&run.api, "basenames", "nftonly.base.eth").await?;
    assert_eq!(
        pointer(&nft_only, "/declared_state/control/registrant"),
        bob_path
    );
    assert_eq!(
        pointer(&nft_only, "/declared_state/control/registry_owner"),
        Value::Null,
        "the first registry-owner setup is not materialized as a later control transfer"
    );
    assert_eq!(
        pointer(&nft_only, "/declared_state/control/latest_event_kind"),
        "AuthorityEpochChanged"
    );

    let management_only = exact_name(&run.api, "basenames", "mgmtonly.base.eth").await?;
    assert_exact_control(
        &management_only,
        &alice_path,
        &bob_path,
        "AuthorityEpochChanged",
    );

    let full_transfer = exact_name(&run.api, "basenames", "fullxfer.base.eth").await?;
    assert_exact_control(
        &full_transfer,
        &bob_path,
        &bob_path,
        "AuthorityEpochChanged",
    );

    let declared = primary_name(
        &run.api,
        "basenames",
        basenames::BASE_PRIMARY_COIN_TYPE,
        &alice_path,
        "declared",
    )
    .await?;
    assert_declared_primary(&declared, "alice.base.eth");
    run.db.cleanup().await?;

    basenames::set_primary_name(&rpc, &deployment, alice, "").await?;
    let clear_ready_sql = format!(
        "SELECT EXISTS (
           SELECT 1 FROM normalized_events
           WHERE source_family = 'basenames_base_primary'
             AND event_kind = 'RecordChanged'
             AND canonicality_state = 'canonical'
             AND after_state->>'raw_name' = ''
             AND lower(after_state->'primary_claim_source'->>'address') = '{alice_path}'
        )"
    );
    let cleared =
        support::ingest_basenames_and_serve(&base, &deployment, Some(&clear_ready_sql)).await?;
    let cleared_body = primary_name(
        &cleared.api,
        "basenames",
        basenames::BASE_PRIMARY_COIN_TYPE,
        &alice_path,
        "declared",
    )
    .await?;
    assert_eq!(
        pointer(&cleared_body, "/declared_state/claimed_primary_name/status"),
        "not_found",
        "blank Base primary claim should clear the declared candidate; body: {cleared_body}"
    );
    let stored_clear_status: String = sqlx::query_scalar(
        "SELECT claim_status::TEXT
         FROM primary_names_current
         WHERE address = $1
           AND namespace = 'basenames'
           AND coin_type = '2147492101'
           AND claim_status = 'not_found'",
    )
    .bind(&alice_path)
    .fetch_one(&cleared.db.pool)
    .await
    .context("cleared Basenames primary row should persist not_found status")?;
    assert_eq!(stored_clear_status, "not_found");

    cleared.db.cleanup().await?;
    Ok(())
}
