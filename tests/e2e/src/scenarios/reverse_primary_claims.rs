use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::responses::{pointer, primary_name};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

fn assert_declared_not_found(body: &Value) {
    assert_eq!(
        pointer(body, "/declared_state/claimed_primary_name/status"),
        "not_found",
        "claim without an admitted name record must not mint a candidate; body: {body}"
    );
    assert!(
        body.pointer("/declared_state/claimed_primary_name/name")
            .is_none(),
        "not_found claim must not carry a candidate name; body: {body}"
    );
}

async fn assert_persisted_not_found(run: &support::PipelineRun, address: &str) -> Result<()> {
    let row: (String, Option<String>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name FROM primary_names_current \
         WHERE address = $1 AND namespace = 'ens' AND coin_type = '60'",
    )
    .bind(address)
    .fetch_one(&run.db.pool)
    .await
    .with_context(|| format!("load persisted primary-name tuple for {address}"))?;
    assert_eq!(row.0, "not_found");
    assert_eq!(row.1, None);
    Ok(())
}

/// `claim` routes through `claimForAddr` and updates the registry without
/// invoking a resolver name setter.
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L64 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
#[tokio::test]
async fn claim_without_name_record_keeps_candidate_absent() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let claimant = rpc.accounts().await?[1];
    let claimant_path = format!("{claimant:#x}");
    let reverse_node = format!("{:#x}", ens_v1::reverse_node(claimant));

    ens_v1::set_reverse_default_resolver(&rpc, &deployment, deployment.public_resolver.address)
        .await?;
    ens_v1::claim_reverse(&rpc, &deployment, claimant, claimant).await?;

    let ready_sql = format!(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE event_kind = 'ReverseChanged' \
              AND lower(after_state->>'address') = '{claimant_path}' \
              AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
            WHERE event_kind = 'SubregistryChanged' \
              AND after_state->>'child_node' = '{reverse_node}' \
              AND canonicality_state = 'canonical')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let claim_tx: String = sqlx::query_scalar(
        "SELECT transaction_hash FROM normalized_events \
         WHERE event_kind = 'ReverseChanged' \
           AND lower(after_state->>'address') = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&claimant_path)
    .fetch_one(&run.db.pool)
    .await?;

    let claim_events: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_kind, source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&claim_tx)
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        claim_events
            .iter()
            .any(|(kind, family)| kind == "SubregistryChanged" && family == "ens_v1_registry_l1"),
        "claim transaction should derive the reverse-node registry edge: {claim_events:?}"
    );
    assert_eq!(
        claim_events
            .iter()
            .filter(|(kind, _)| kind == "ReverseChanged")
            .count(),
        1,
        "registry NewOwner must not be decoded as another reverse claim: {claim_events:?}"
    );
    assert!(
        claim_events
            .iter()
            .any(|(kind, family)| kind == "ReverseChanged" && family == "ens_v1_reverse_l1")
    );
    assert!(
        claim_events.iter().all(|(kind, _)| kind != "RecordChanged"),
        "claim-only transaction must not derive NameChanged: {claim_events:?}"
    );

    let resolver_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE transaction_hash = $1 AND lower(emitting_address) = $2 \
           AND lineage.canonicality_state = 'canonical'",
    )
    .bind(&claim_tx)
    .bind(format!("{:#x}", deployment.public_resolver.address))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(resolver_logs, 0, "claim-only should not call the resolver");

    assert_persisted_not_found(&run, &claimant_path).await?;
    let body = primary_name(&run.api, "ens", 60, &claimant_path, "declared").await?;
    assert_declared_not_found(&body);

    run.db.cleanup().await?;
    Ok(())
}

/// Third-party authorization is checked against the claimed address, and
/// `setNameForAddr` passes that address into `claimForAddr`.
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L44 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
#[tokio::test]
async fn authorised_third_party_generic_name_record_does_not_key_claim() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (claimed_address, operator) = (accounts[1], accounts[2]);
    let claimed_path = format!("{claimed_address:#x}");
    let operator_path = format!("{operator:#x}");
    let reverse_node = format!("{:#x}", ens_v1::reverse_node(claimed_address));

    ens_v1::set_registry_approval_for_all(&rpc, &deployment, claimed_address, operator, true)
        .await?;
    ens_v1::set_reverse_name_for_addr(
        &rpc,
        &deployment,
        operator,
        claimed_address,
        claimed_address,
        deployment.public_resolver.address,
        "thirdparty.eth",
    )
    .await?;

    let ready_sql = "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
           AND after_state->>'raw_name' = 'thirdparty.eth' \
           AND NOT (after_state ? 'primary_claim_source') \
           AND canonicality_state = 'canonical')";
    let run = support::ingest_and_serve(&anvil, &deployment, Some(ready_sql)).await?;

    let reverse: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'ReverseChanged' \
           AND lower(after_state->>'address') = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&claimed_path)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(reverse["address"], claimed_path);
    assert_eq!(reverse["reverse_node"], reverse_node);

    let (name_tx, name_state): (String, Value) = sqlx::query_as(
        "SELECT transaction_hash, after_state FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
           AND after_state->>'raw_name' = 'thirdparty.eth' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        name_state.get("primary_claim_source").is_none(),
        "generic resolver NameChanged must not become a primary claim: {name_state}"
    );
    let sender: String = sqlx::query_scalar(
        "SELECT from_address FROM raw_transactions raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE transaction_hash = $1 AND lineage.canonicality_state = 'canonical'",
    )
    .bind(&name_tx)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(sender, operator_path);
    assert_ne!(sender, claimed_path);

    let claimed = primary_name(&run.api, "ens", 60, &claimed_path, "declared").await?;
    assert_declared_not_found(&claimed);
    let operator_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM primary_names_current \
         WHERE address = $1 AND namespace = 'ens' AND coin_type = '60'",
    )
    .bind(&operator_path)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(operator_rows, 0, "tx sender must not become the claim key");

    run.db.cleanup().await?;
    Ok(())
}

/// `claimWithResolver` accepts the resolver address supplied by the caller.
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L93 @ ens_v1@91c966f)
#[tokio::test]
async fn unadmitted_reverse_resolver_keeps_candidate_absent() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let unadmitted = ens_v1::deploy_owned_resolver(&rpc, &root, &deployment).await?;
    let claimant = rpc.accounts().await?[1];
    let claimant_path = format!("{claimant:#x}");
    let unadmitted_path = format!("{:#x}", unadmitted.address);
    let reverse_node = ens_v1::reverse_node(claimant);

    ens_v1::claim_reverse_with_resolver(&rpc, &deployment, claimant, claimant, unadmitted.address)
        .await?;
    ens_v1::set_name_record_for_node(
        &rpc,
        unadmitted.address,
        deployment.deployer,
        reverse_node,
        "hidden.eth",
    )
    .await?;

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'ReverseChanged' \
           AND lower(after_state->>'address') = '{claimant_path}' \
           AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events ne \
          JOIN raw_logs rl \
            ON rl.chain_id = ne.chain_id \
           AND rl.block_hash = ne.block_hash \
           AND rl.transaction_hash = ne.transaction_hash \
           AND rl.log_index = ne.log_index \
          WHERE ne.event_kind = 'RecordChanged' \
            AND ne.after_state->>'raw_name' = 'hidden.eth' \
            AND lower(rl.emitting_address) = '{unadmitted_path}' \
            AND ne.canonicality_state = 'canonical')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let observed_names: Vec<(String, Option<String>, Option<String>, Value)> = sqlx::query_as(
        "SELECT ne.event_identity, ne.logical_name_id, ne.resource_id::TEXT, ne.after_state \
         FROM normalized_events ne \
         JOIN raw_logs rl \
           ON rl.chain_id = ne.chain_id \
          AND rl.block_hash = ne.block_hash \
          AND rl.transaction_hash = ne.transaction_hash \
          AND rl.log_index = ne.log_index \
         WHERE ne.event_kind = 'RecordChanged' \
           AND ne.after_state->>'raw_name' = 'hidden.eth' \
           AND lower(rl.emitting_address) = $1 \
           AND ne.canonicality_state = 'canonical' \
         ORDER BY ne.event_identity",
    )
    .bind(&unadmitted_path)
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        (1..=2).contains(&observed_names.len()),
        "match-all intake must retain the unanchored NameChanged observation, with at most one later unsupported-profile boundary"
    );
    for (_, _, _, after_state) in &observed_names {
        assert_eq!(after_state["record_key"], "name");
        assert_eq!(after_state["raw_name"], "hidden.eth");
        assert!(
            after_state.get("primary_claim_source").is_none(),
            "an unadmitted resolver observation must not become primary-name identity: {after_state}"
        );
    }

    assert_persisted_not_found(&run, &claimant_path).await?;
    let body = primary_name(&run.api, "ens", 60, &claimant_path, "declared").await?;
    assert_declared_not_found(&body);

    run.db.cleanup().await?;
    Ok(())
}

/// Reverse name setting and forward address setting are independent writes.
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L105 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L26 @ ens_v1@91c966f)
#[tokio::test]
async fn forward_mismatch_keeps_generic_name_record_unadmitted() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let accounts = rpc.accounts().await?;
    let (claimant, different_target) = (accounts[1], accounts[2]);
    assert_ne!(claimant, different_target);
    let claimant_path = format!("{claimant:#x}");

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "primarymismatch",
        claimant,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::set_addr_record(
        &rpc,
        deployment.public_resolver.address,
        claimant,
        "primarymismatch.eth",
        different_target,
    )
    .await?;
    ens_v1::set_reverse_name(&rpc, &deployment, claimant, "primarymismatch.eth").await?;

    let ready_sql = format!(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:0xc7a82548d35271d2a5afc514f7250f90f03bec28061693ba42807217ff26b8a0' \
              AND event_kind = 'RecordChanged' \
              AND after_state->>'record_key' = 'addr:60' \
              AND lower(after_state->>'value') = '{different_target:#x}' \
              AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
            WHERE event_kind = 'RecordChanged' \
              AND after_state->>'raw_name' = 'primarymismatch.eth' \
              AND NOT (after_state ? 'primary_claim_source') \
              AND canonicality_state = 'canonical')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let (records_status, records) = run
        .api
        .get_json("/v1/names/ens/primarymismatch.eth/records?coin_types=60&mode=declared&meta=full")
        .await?;
    assert_eq!(
        records_status, 200,
        "forward records lookup failed: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/60/status"),
        "success"
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/60/value"),
        format!("{different_target:#x}")
    );
    assert_ne!(
        pointer(&records, "/data/coin_addresses/60/value"),
        claimant_path
    );

    let declared = primary_name(&run.api, "ens", 60, &claimant_path, "declared").await?;
    assert_declared_not_found(&declared);

    run.db.cleanup().await?;
    Ok(())
}
