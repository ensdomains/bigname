use std::collections::BTreeSet;
use std::str::FromStr;

use alloy_primitives::{B256, LogData, U256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::responses::pointer;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

sol! {
    #[derive(Debug)]
    event NameRegistered(
        string label,
        bytes32 indexed labelhash,
        address indexed owner,
        uint256 baseCost,
        uint256 premium,
        uint256 expires,
        bytes32 referrer
    );
}

/// Controller registration writes node-checked resolver data and the Ethereum
/// reverse record before emitting its label-bearing registration event
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L307 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L319 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L333 @ ens_v1@91c966f).
#[tokio::test]
async fn registration_with_records_reverse_and_referrer_derives_single_burst() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, record_target) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;
    let referrer = B256::repeat_byte(0xa5);

    let registered = ens_v1::register_eth_name_with_options(
        &rpc,
        &deployment,
        "burst",
        alice,
        YEAR,
        resolver,
        ens_v1::RegistrationOptions {
            data: vec![
                ens_v1::registration_addr_record_data("burst.eth", record_target),
                ens_v1::registration_text_record_data("burst.eth", "com.twitter", "burst"),
            ],
            reverse_record: ens_v1::REVERSE_RECORD_ETHEREUM,
            referrer,
        },
    )
    .await?;

    let ready_sql = format!(
        "SELECT \
           (SELECT count(DISTINCT source_family) = 4 FROM normalized_events \
            WHERE transaction_hash = '{tx_hash}' \
            AND source_family IN ('ens_v1_registrar_l1', 'ens_v1_registry_l1', \
                                  'ens_v1_resolver_l1', 'ens_v1_reverse_l1') \
            AND canonicality_state = 'canonical') \
         AND \
           (SELECT count(DISTINCT after_state->>'record_key') >= 2 FROM normalized_events \
            WHERE (logical_name_id = 'ens:0xff8b5f8209f6197db09fe13cdf9395c8ed39d5e0546c071e44a7d51ca50d1854' OR after_state->>'node' = '0xff8b5f8209f6197db09fe13cdf9395c8ed39d5e0546c071e44a7d51ca50d1854') \
            AND event_kind = 'RecordChanged' \
            AND after_state->>'record_key' IN ('addr:60', 'text:com.twitter') \
            AND transaction_hash = '{tx_hash}' \
            AND canonicality_state = 'canonical')",
        tx_hash = registered.register_tx_hash,
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
    let burst_id = support::schema_v2_logical_name_id(
        "ens:0xff8b5f8209f6197db09fe13cdf9395c8ed39d5e0546c071e44a7d51ca50d1854",
    );

    let source_families: BTreeSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .fetch_all(&run.db.pool)
    .await?
    .into_iter()
    .collect();
    assert_eq!(
        source_families,
        BTreeSet::from([
            "ens_v1_registrar_l1".to_owned(),
            "ens_v1_registry_l1".to_owned(),
            "ens_v1_resolver_l1".to_owned(),
            "ens_v1_reverse_l1".to_owned(),
        ]),
        "registration transaction should derive across four admitted families"
    );

    let registration: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE logical_name_id = $2 \
         AND event_kind = 'PreimageObserved' \
         AND source_family = 'ens_v1_registrar_l1' \
         AND transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .bind(&burst_id)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        registration["referrer"],
        format!("{referrer:#x}"),
        "the controller referrer must survive interpretation: {registration}"
    );

    let forward_records: Vec<Value> = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE (logical_name_id = $2 OR namespace || ':' || after_state->>'node' = $2) \
         AND event_kind = 'RecordChanged' \
         AND source_family = 'ens_v1_resolver_l1' \
         AND transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .bind(&burst_id)
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        forward_records.iter().any(|state| {
            state.get("record_key") == Some(&json!("addr:60"))
                && state.get("value") == Some(&json!(format!("{record_target:#x}")))
        }),
        "addr:60 record missing from burst: {forward_records:?}"
    );
    assert!(
        forward_records.iter().any(|state| {
            state.get("record_key") == Some(&json!("text:com.twitter"))
                && state.get("value") == Some(&json!("burst"))
        }),
        "text record missing from burst: {forward_records:?}"
    );
    assert!(
        forward_records
            .iter()
            .all(|state| state.get("writer").is_none()),
        "record state must not invent a writer field: {forward_records:?}"
    );
    let forward_resolver_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE emitting_address = $1 AND transaction_hash = $2 AND topics[2] = $3",
    )
    .bind(format!("{resolver:#x}"))
    .bind(&registered.register_tx_hash)
    .bind(format!("{:#x}", ens_v1::namehash("burst.eth")))
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        forward_resolver_logs >= 2,
        "expected resolver-emitted addr/text logs in the controller transaction"
    );
    let (transaction_from, transaction_to): (String, Option<String>) = sqlx::query_as(
        "SELECT transaction.from_address, transaction.to_address
         FROM raw_transactions transaction
         JOIN chain_lineage lineage
           ON lineage.chain_id = transaction.chain_id
          AND lineage.block_hash = transaction.block_hash
          AND lineage.block_number = transaction.block_number
         WHERE transaction.transaction_hash = $1
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(transaction_from, format!("{alice:#x}"));
    assert_eq!(
        transaction_to.as_deref(),
        Some(format!("{:#x}", deployment.controller.address).as_str())
    );

    let reverse: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'ReverseChanged' \
         AND source_family = 'ens_v1_reverse_l1' \
         AND transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(reverse["address"], format!("{alice:#x}"));
    assert_eq!(reverse["coin_type"], "60");
    assert_eq!(reverse["source_event"], "ReverseClaimed");
    assert_eq!(
        reverse["claim_provenance"]["emitting_address"],
        format!("{:#x}", deployment.reverse_registrar.address)
    );

    let reverse_name_records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
         AND source_family = 'ens_v1_resolver_l1' \
         AND logical_name_id IS NULL AND resource_id IS NULL \
         AND after_state->>'raw_name' = 'burst.eth' \
         AND NOT (after_state ? 'primary_claim_source') \
         AND transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(reverse_name_records, 1);

    let current_resource: String = sqlx::query_scalar(
        "SELECT resource_id::text FROM normalized_events \
         WHERE event_kind = 'RegistrationGranted' AND transaction_hash = $1 \
         AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    let records_by_resource: Vec<(String, i64)> = sqlx::query_as(
        "SELECT resource_id::text, count(*) FROM normalized_events \
         WHERE event_kind = 'RecordChanged' AND logical_name_id = $2 \
         AND transaction_hash = $1 AND canonicality_state = 'canonical' \
         GROUP BY resource_id",
    )
    .bind(&registered.register_tx_hash)
    .bind(&burst_id)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        records_by_resource,
        vec![(current_resource.clone(), 3)],
        "same-transaction record writes must reconcile onto the registrar resource"
    );
    let last_owner_subject: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'AuthorityTransferred' AND transaction_hash = $1 \
         AND canonicality_state = 'canonical' \
         ORDER BY log_index DESC LIMIT 1",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        last_owner_subject["owner"],
        format!("{alice:#x}"),
        "normalized layer holds the post-setRecord owner: {last_owner_subject}"
    );

    let (projected_resource, declared): (String, Value) = sqlx::query_as(
        "SELECT resource_id::text, declared_summary
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&burst_id)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(projected_resource, current_resource);
    assert_eq!(
        pointer(&declared, "/resolver/address"),
        format!("{resolver:#x}"),
        "resolver must survive same-transaction reconciliation"
    );
    assert_eq!(
        pointer(&declared, "/registration/registrant"),
        format!("{alice:#x}")
    );
    let (selectors, entries): (Value, Value) = sqlx::query_as(
        "SELECT selectors, entries FROM record_inventory_current
         WHERE resource_id::text = $1",
    )
    .bind(&current_resource)
    .fetch_one(&run.db.pool)
    .await?;
    let selector_keys = selectors
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry["record_key"].as_str())
        .collect::<BTreeSet<_>>();
    assert!(selector_keys.contains("addr:60"));
    assert!(selector_keys.contains("text:com.twitter"));
    assert!(entries.as_array().is_some_and(|entries| {
        entries.iter().any(|entry| {
            entry["record_key"] == "addr:60" && entry["value"] == format!("{record_target:#x}")
        }) && entries
            .iter()
            .any(|entry| entry["record_key"] == "text:com.twitter" && entry["value"] == "burst")
    }));

    // The controller calls `setNameForAddr` for the reverse-record bit
    // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L320 @ ens_v1@91c966f).
    // The reverse registrar emits `ReverseClaimed`, then calls the resolver's
    // `setName` (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f),
    // which emits `NameChanged`
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f).
    // Schema-v2 deliberately keeps that resolver observation raw-only; it
    // does not synthesize the v1 route-local fallback into the persisted
    // primary projection.
    let primary: (String, Option<String>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(format!("{alice:#x}"))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(primary, ("not_found".to_owned(), None));

    let (topics, data): (Vec<String>, Vec<u8>) = sqlx::query_as(
        "SELECT log.topics, log.data FROM raw_logs log
         JOIN chain_lineage lineage
           ON lineage.chain_id = log.chain_id
          AND lineage.block_hash = log.block_hash
          AND lineage.block_number = log.block_number
         WHERE log.emitting_address = $1 AND log.transaction_hash = $2
           AND log.topics[1] = $3
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(format!("{:#x}", deployment.controller.address))
    .bind(&registered.register_tx_hash)
    .bind(format!("{:#x}", NameRegistered::SIGNATURE_HASH))
    .fetch_one(&run.db.pool)
    .await?;
    let topics = topics
        .iter()
        .map(|topic| B256::from_str(topic).with_context(|| format!("invalid topic {topic}")))
        .collect::<Result<Vec<_>>>()?;
    let log_data =
        LogData::new(topics, data.into()).context("controller log has too many topics")?;
    let decoded = NameRegistered::decode_log_data_validate(&log_data)
        .context("decode controller NameRegistered")?;
    assert_eq!(decoded.label, "burst");
    assert_eq!(decoded.owner, alice);
    assert_eq!(decoded.referrer, referrer);
    assert!(decoded.expires > U256::ZERO);

    run.db.cleanup().await?;

    // Later writes must remain on the same registrar resource and replace the
    // same current inventory entries idempotently.
    ens_v1::set_addr_record(&rpc, resolver, alice, "burst.eth", record_target).await?;
    ens_v1::set_text_record(&rpc, resolver, alice, "burst.eth", "com.twitter", "burst").await?;
    let recovery_ready = format!(
        "SELECT count(*) >= 2 FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
         AND logical_name_id = 'ens:0xff8b5f8209f6197db09fe13cdf9395c8ed39d5e0546c071e44a7d51ca50d1854' \
         AND transaction_hash <> '{tx_hash}' \
         AND canonicality_state = 'canonical'",
        tx_hash = registered.register_tx_hash,
    );
    let recovered = support::ingest_and_serve(&anvil, &deployment, Some(&recovery_ready)).await?;
    let recovered_id = support::schema_v2_logical_name_id(
        "ens:0xff8b5f8209f6197db09fe13cdf9395c8ed39d5e0546c071e44a7d51ca50d1854",
    );
    let recovered_resource: String =
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id = $1")
            .bind(&recovered_id)
            .fetch_one(&recovered.db.pool)
            .await?;
    let recovered_entries: Value = sqlx::query_scalar(
        "SELECT entries FROM record_inventory_current WHERE resource_id::text = $1",
    )
    .bind(&recovered_resource)
    .fetch_one(&recovered.db.pool)
    .await?;
    assert!(recovered_entries.as_array().is_some_and(|entries| {
        entries.iter().any(|entry| {
            entry["record_key"] == "addr:60" && entry["value"] == format!("{record_target:#x}")
        }) && entries
            .iter()
            .any(|entry| entry["record_key"] == "text:com.twitter" && entry["value"] == "burst")
    }));

    recovered.db.cleanup().await?;
    Ok(())
}
