use std::collections::BTreeSet;
use std::str::FromStr;

use alloy_primitives::{B256, LogData, U256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::responses::{pointer, selector_keys};
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
            WHERE logical_name_id = 'ens:burst.eth' \
            AND event_kind = 'RecordChanged' \
            AND after_state->>'record_key' IN ('addr:60', 'text:com.twitter') \
            AND transaction_hash = '{tx_hash}' \
            AND canonicality_state = 'canonical')",
        tx_hash = registered.register_tx_hash,
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

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
         WHERE logical_name_id = 'ens:burst.eth' \
         AND event_kind = 'RegistrationGranted' \
         AND source_family = 'ens_v1_registrar_l1' \
         AND transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        registration.get("referrer").is_none(),
        "normalized registration currently carries no referrer field: {registration}"
    );

    let forward_records: Vec<Value> = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE logical_name_id = 'ens:burst.eth' \
         AND event_kind = 'RecordChanged' \
         AND source_family = 'ens_v1_resolver_l1' \
         AND transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
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
        "SELECT from_address, to_address FROM raw_transactions \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
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
         AND lower(after_state->'primary_claim_source'->>'address') = $1 \
         AND transaction_hash = $2 AND canonicality_state = 'canonical'",
    )
    .bind(format!("{alice:#x}"))
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(reverse_name_records, 1);

    let (status, primary) = run
        .api
        .get_json(&format!(
            "/v1/primary-names/{alice:#x}?namespace=ens&coin_type=60&mode=declared"
        ))
        .await?;
    assert_eq!(status, 200, "primary-name lookup failed: {primary}");
    assert_eq!(
        pointer(&primary, "/declared_state/claimed_primary_name/status"),
        "success"
    );
    assert_eq!(
        pointer(&primary, "/declared_state/claimed_primary_name/name"),
        "burst.eth"
    );
    assert_eq!(
        pointer(
            &primary,
            "/declared_state/claimed_primary_name/provenance/source_family"
        ),
        "ens_v1_reverse_l1"
    );

    // REVIEW POINT (reproduced defect): the burst's record writes derive only under
    // the transient registry-only anchor; the same-tx RegistrationGranted
    // rebinds the surface to the registrar resource and carries the
    // resolver across, but neither the records nor the registry-owner
    // facet. Exact-name therefore serves no selectors and the controller as
    // registry_owner even though the normalized layer holds the records and
    // the alice owner observation.
    // The rebind pair (SurfaceUnbound/SurfaceBound) is synthetic — no
    // transaction anchor — so the superseded resource comes from the
    // log-anchored registry ResolverChanged in the same transaction.
    let superseded_resource: String = sqlx::query_scalar(
        "SELECT resource_id::text FROM normalized_events \
         WHERE event_kind = 'ResolverChanged' \
         AND source_family = 'ens_v1_registry_l1' \
         AND logical_name_id = 'ens:burst.eth' \
         AND transaction_hash = $1 \
         AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    let rebind_pair: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'SurfaceUnbound' AND resource_id::text = $1 \
           AND logical_name_id = 'ens:burst.eth' \
           AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'SurfaceBound' \
           AND source_family = 'ens_v1_registrar_l1' \
           AND logical_name_id = 'ens:burst.eth' \
           AND canonicality_state = 'canonical')",
    )
    .bind(&superseded_resource)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(rebind_pair, "registration must rebind the burst surface");
    let current_resource: String = sqlx::query_scalar(
        "SELECT resource_id::text FROM normalized_events \
         WHERE event_kind = 'RegistrationGranted' AND transaction_hash = $1 \
         AND canonicality_state = 'canonical'",
    )
    .bind(&registered.register_tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_ne!(superseded_resource, current_resource);
    let records_by_resource: Vec<(String, i64)> = sqlx::query_as(
        "SELECT resource_id::text, count(*) FROM normalized_events \
         WHERE event_kind = 'RecordChanged' AND logical_name_id = 'ens:burst.eth' \
         AND transaction_hash = $1 AND canonicality_state = 'canonical' \
         GROUP BY resource_id",
    )
    .bind(&registered.register_tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        records_by_resource,
        vec![(superseded_resource.clone(), 3)],
        "burst records must derive only under the superseded registry-only anchor"
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

    let (status, exact) = run.api.get_json("/v1/names/ens/burst.eth").await?;
    assert_eq!(status, 200, "burst.eth exact-name lookup failed: {exact}");
    assert!(
        selector_keys(&exact).is_empty(),
        "burst-written selectors must stay invisible after the anchor rebind: {exact}"
    );
    let gap_families: BTreeSet<String> = exact
        .pointer("/declared_state/record_inventory/explicit_gaps")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|gap| gap["gap_reason"] == "not_observed_on_current_resolver")
        .filter_map(|gap| gap["record_family"].as_str().map(str::to_owned))
        .collect();
    assert!(
        gap_families.contains("addr") && gap_families.contains("text"),
        "burst families must report explicit gaps: {exact}"
    );
    assert_eq!(
        pointer(&exact, "/declared_state/resolver/address"),
        format!("{resolver:#x}"),
        "resolver is carried across the rebind"
    );
    assert_eq!(
        pointer(&exact, "/declared_state/control/registrant"),
        format!("{alice:#x}")
    );
    assert_eq!(
        pointer(&exact, "/declared_state/control/registry_owner"),
        format!("{:#x}", deployment.controller.address),
        "registry_owner facet stays at the mid-burst controller observation"
    );
    let (status, records) = run
        .api
        .get_json(
            "/v1/names/ens/burst.eth/records?include=resolver_address,coins&coin_types=60\
             &texts=com.twitter&mode=declared&meta=full",
        )
        .await?;
    assert_eq!(status, 200, "burst.eth records lookup failed: {records}");
    assert_eq!(
        pointer(&records, "/data/coin_addresses/60/status"),
        "not_found",
        "burst-written addr must not serve from the current anchor: {records}"
    );
    assert!(
        records.pointer("/data/coin_addresses/60/value").is_none(),
        "not_found addr must omit value: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/text_records/com.twitter/status"),
        "not_found",
        "burst-written text must not serve from the current anchor: {records}"
    );
    assert!(
        records
            .pointer("/data/text_records/com.twitter/value")
            .is_none(),
        "not_found text must omit value: {records}"
    );

    let (topics, data): (Vec<String>, Vec<u8>) = sqlx::query_as(
        "SELECT topics, data FROM raw_logs \
         WHERE emitting_address = $1 AND transaction_hash = $2 \
         AND topics[1] = $3 AND canonicality_state = 'canonical'",
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

    // Recovery: later plain writes derive under the current registrar
    // resource, and the inventory becomes whole while the stale
    // registry_owner facet persists (record writes move no authority facet).
    ens_v1::set_addr_record(&rpc, resolver, alice, "burst.eth", record_target).await?;
    ens_v1::set_text_record(&rpc, resolver, alice, "burst.eth", "com.twitter", "burst").await?;
    let recovery_ready = format!(
        "SELECT count(*) >= 2 FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
         AND logical_name_id = 'ens:burst.eth' \
         AND transaction_hash <> '{tx_hash}' \
         AND canonicality_state = 'canonical'",
        tx_hash = registered.register_tx_hash,
    );
    let recovered = support::ingest_and_serve(&anvil, &deployment, Some(&recovery_ready)).await?;
    let (status, exact) = recovered.api.get_json("/v1/names/ens/burst.eth").await?;
    assert_eq!(status, 200, "recovered exact-name lookup failed: {exact}");
    let selectors = selector_keys(&exact);
    for expected in ["addr:60", "text:com.twitter"] {
        assert!(
            selectors.contains(expected),
            "missing recovered selector {expected}: {exact}"
        );
    }
    assert_eq!(
        pointer(&exact, "/declared_state/control/registry_owner"),
        format!("{:#x}", deployment.controller.address)
    );
    let (status, records) = recovered
        .api
        .get_json(
            "/v1/names/ens/burst.eth/records?include=resolver_address,coins&coin_types=60\
             &texts=com.twitter&mode=declared&meta=full",
        )
        .await?;
    assert_eq!(status, 200, "recovered records lookup failed: {records}");
    assert_eq!(
        pointer(&records, "/data/coin_addresses/60/value"),
        format!("{record_target:#x}")
    );
    assert_eq!(
        pointer(&records, "/data/text_records/com.twitter/value"),
        "burst"
    );

    recovered.db.cleanup().await?;
    Ok(())
}
