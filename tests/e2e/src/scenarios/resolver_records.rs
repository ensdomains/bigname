use std::collections::BTreeSet;

use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const MULTICOIN_TYPE: u64 = 0;
const MULTICOIN_BYTES: &[u8] = &[0xde, 0xad, 0xbe, 0xef];
const CONTENTHASH_BYTES: &[u8] = &[0xe3, 0x01, 0x01, 0x70, 0x12, 0x20];
const MULTICOIN_HEX: &str = "0xdeadbeef";
const CONTENTHASH_HEX: &str = "0xe30101701220";

fn pointer(body: &Value, path: &str) -> Value {
    body.pointer(path).cloned().unwrap_or(Value::Null)
}

fn selector_keys(body: &Value) -> BTreeSet<String> {
    body.pointer("/declared_state/record_inventory/selectors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("record_key").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn boundary(body: &Value) -> Result<Value> {
    body.pointer("/declared_state/record_inventory/record_version_boundary")
        .cloned()
        .context("exact-name response is missing record_version_boundary")
}

async fn exact_name(run: &support::PipelineRun, name: &str) -> Result<Value> {
    let (status, body) = run.api.get_json(&format!("/v1/names/ens/{name}")).await?;
    assert_eq!(status, 200, "exact-name lookup for {name} failed: {body}");
    Ok(body)
}

async fn compact_records(run: &support::PipelineRun, name: &str, query: &str) -> Result<Value> {
    let (status, body) = run
        .api
        .get_json(&format!("/v1/names/ens/{name}/records{query}"))
        .await?;
    assert_eq!(status, 200, "records lookup for {name} failed: {body}");
    Ok(body)
}

fn assert_resolver(body: &Value, resolver: Address) {
    assert_eq!(
        pointer(body, "/declared_state/resolver/address"),
        format!("{resolver:#x}"),
        "declared resolver should match current registry binding; body: {body}"
    );
    assert_eq!(
        pointer(body, "/declared_state/resolver/chain_id"),
        "ethereum-mainnet"
    );
    assert_eq!(
        pointer(body, "/declared_state/resolver/latest_event_kind"),
        "ResolverChanged"
    );
}

fn assert_no_resolver(body: &Value) {
    assert_eq!(
        pointer(body, "/declared_state/resolver/address"),
        Value::Null,
        "zero resolver should use the supported null resolver shape; body: {body}"
    );
    assert_eq!(
        pointer(body, "/declared_state/resolver/chain_id"),
        Value::Null
    );
}

fn assert_compact_record_not_success(body: &Value, path: &str, old_value: Value) {
    let status = pointer(body, &format!("{path}/status"));
    assert_ne!(
        status, "success",
        "old resolver cache must not remain successful at {path}; body: {body}"
    );
    assert_ne!(
        pointer(body, &format!("{path}/value")),
        old_value,
        "old resolver value must not be attributed to the current resolver at {path}; body: {body}"
    );
}

#[tokio::test]
async fn resolver_changes_follow_registry_and_zero_releases() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let second_resolver = ens_v1::deploy_extra_public_resolver(&rpc, &root, &deployment).await?;
    let alice = rpc.accounts().await?[1];
    let first_resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "flip", alice, YEAR, first_resolver).await?;

    {
        let run = support::ingest_and_serve(
            &anvil,
            &deployment,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:flip.eth' AND event_kind = 'ResolverChanged' \
                 AND canonicality_state = 'canonical')",
            ),
        )
        .await?;
        let body = exact_name(&run, "flip.eth").await?;
        assert_resolver(&body, first_resolver);
        run.db.cleanup().await?;
    }

    ens_v1::set_resolver(
        &rpc,
        &deployment,
        alice,
        "flip.eth",
        second_resolver.address,
    )
    .await?;

    {
        let ready_sql = format!(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:flip.eth' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical' \
             AND lower(after_state->>'resolver') = '{:#x}')",
            second_resolver.address
        );
        let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
        let body = exact_name(&run, "flip.eth").await?;
        assert_resolver(&body, second_resolver.address);
        run.db.cleanup().await?;
    }

    ens_v1::set_resolver(&rpc, &deployment, alice, "flip.eth", Address::ZERO).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT count(*) >= 3 FROM normalized_events \
             WHERE logical_name_id = 'ens:flip.eth' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical'",
        ),
    )
    .await?;
    let body = exact_name(&run, "flip.eth").await?;
    assert_no_resolver(&body);

    let records = compact_records(
        &run,
        "flip.eth",
        "?include=resolver_address,content_hash,coins&coin_types=60&content_hash=true",
    )
    .await?;
    assert_eq!(
        pointer(&records, "/data/resolver_address"),
        Value::Null,
        "records route should expose the same null resolver shape after zeroing; body: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/60/status"),
        "not_found"
    );
    assert_eq!(pointer(&records, "/data/content_hash/status"), "not_found");

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn records_route_values_and_version_boundaries_follow_current_resolver() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let replacement_resolver =
        ens_v1::deploy_extra_public_resolver(&rpc, &root, &deployment).await?;
    let alice = rpc.accounts().await?[1];
    let resolver_a = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "records", alice, YEAR, resolver_a).await?;
    ens_v1::set_multicoin_addr_record(
        &rpc,
        resolver_a,
        alice,
        "records.eth",
        MULTICOIN_TYPE,
        MULTICOIN_BYTES,
    )
    .await?;
    ens_v1::set_contenthash_record(&rpc, resolver_a, alice, "records.eth", CONTENTHASH_BYTES)
        .await?;

    ens_v1::register_eth_name(&rpc, &deployment, "clearable", alice, YEAR, resolver_a).await?;
    ens_v1::set_text_record(
        &rpc,
        resolver_a,
        alice,
        "clearable.eth",
        "com.twitter",
        "before-clear",
    )
    .await?;

    let initial = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT \
               (SELECT count(DISTINCT after_state->>'record_key') >= 2 FROM normalized_events \
                WHERE logical_name_id = 'ens:records.eth' AND event_kind = 'RecordChanged' \
                AND canonicality_state = 'canonical' \
                AND after_state->>'record_key' IN ('addr:0', 'contenthash')) \
             AND \
               EXISTS (SELECT 1 FROM normalized_events \
                WHERE logical_name_id = 'ens:clearable.eth' AND event_kind = 'RecordChanged' \
                AND canonicality_state = 'canonical' \
                AND after_state->>'record_key' = 'text:com.twitter')",
        ),
    )
    .await?;

    let records_exact = exact_name(&initial, "records.eth").await?;
    let selectors = selector_keys(&records_exact);
    for expected in ["addr:0", "contenthash"] {
        assert!(
            selectors.contains(expected),
            "expected selector {expected} in records.eth inventory; body: {records_exact}"
        );
    }
    let initial_records_boundary = boundary(&records_exact)?;

    // `include` replaces the default section set (which is just
    // resolver_address), so it must be named explicitly alongside the
    // record sections.
    let records = compact_records(
        &initial,
        "records.eth",
        "?include=resolver_address,content_hash,coins&content_hash=true&coin_types=0&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&records, "/data/resolver_address"),
        format!("{resolver_a:#x}")
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/0/status"),
        "success"
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/0/value"),
        json!({
            "encoding": "hex",
            "bytes": MULTICOIN_HEX,
        })
    );
    assert_eq!(pointer(&records, "/data/content_hash/status"), "success");
    assert_eq!(
        pointer(&records, "/data/content_hash/value"),
        json!({
            "encoding": "hex",
            "bytes": CONTENTHASH_HEX,
        })
    );

    let clearable_records = compact_records(
        &initial,
        "clearable.eth",
        "?texts=com.twitter&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&clearable_records, "/data/text_records/com.twitter/value"),
        "before-clear",
        "clearable.eth should have a cached text value before clearRecords; body: {clearable_records}"
    );
    let clearable_exact = exact_name(&initial, "clearable.eth").await?;
    let initial_clearable_boundary = boundary(&clearable_exact)?;
    initial.db.cleanup().await?;

    ens_v1::set_resolver(
        &rpc,
        &deployment,
        alice,
        "records.eth",
        replacement_resolver.address,
    )
    .await?;
    ens_v1::clear_records(&rpc, resolver_a, alice, "clearable.eth").await?;

    let replacement_addr = format!("{:#x}", replacement_resolver.address);
    let ready_sql = format!(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:records.eth' AND event_kind = 'ResolverChanged' \
            AND canonicality_state = 'canonical' \
            AND lower(after_state->>'resolver') = '{replacement_addr}') \
         AND \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:clearable.eth' AND event_kind = 'RecordVersionChanged' \
            AND canonicality_state = 'canonical')"
    );
    let current = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let replaced_exact = exact_name(&current, "records.eth").await?;
    assert_resolver(&replaced_exact, replacement_resolver.address);
    // The wire boundary object carries only its chain position; the
    // event-identity fields (event_kind, normalized_event_id) are null.
    // Boundary movement is asserted positionally.
    let replacement_boundary = boundary(&replaced_exact)?;
    assert_ne!(
        replacement_boundary, initial_records_boundary,
        "resolver replacement must move the record-version boundary; body: {replaced_exact}"
    );
    let boundary_block = |value: &Value| -> i64 {
        value
            .pointer("/chain_position/block_number")
            .and_then(Value::as_i64)
            .unwrap_or_default()
    };
    assert!(
        boundary_block(&replacement_boundary) > boundary_block(&initial_records_boundary),
        "replacement boundary should move to a later block; body: {replaced_exact}"
    );

    let replaced_records = compact_records(
        &current,
        "records.eth",
        "?include=resolver_address,content_hash,coins&content_hash=true&coin_types=0&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&replaced_records, "/data/resolver_address"),
        replacement_addr
    );
    assert_compact_record_not_success(
        &replaced_records,
        "/data/coin_addresses/0",
        json!(MULTICOIN_HEX),
    );
    assert_compact_record_not_success(
        &replaced_records,
        "/data/content_hash",
        json!({
            "encoding": "hex",
            "bytes": CONTENTHASH_HEX,
        }),
    );

    let cleared_exact = exact_name(&current, "clearable.eth").await?;
    let cleared_boundary = boundary(&cleared_exact)?;
    assert!(
        boundary_block(&cleared_boundary) > boundary_block(&initial_clearable_boundary),
        "clearRecords must move the record-version boundary to a later block; body: {cleared_exact}"
    );
    let cleared_records = compact_records(
        &current,
        "clearable.eth",
        "?texts=com.twitter&mode=declared&meta=full",
    )
    .await?;
    assert_compact_record_not_success(
        &cleared_records,
        "/data/text_records/com.twitter",
        json!("before-clear"),
    );

    current.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn unadmitted_custom_resolver_observes_facts_but_keeps_profile_gated() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let custom_resolver = ens_v1::deploy_extra_public_resolver(&rpc, &root, &deployment).await?;
    let alice = rpc.accounts().await?[1];

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "custom",
        alice,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::set_resolver(
        &rpc,
        &deployment,
        alice,
        "custom.eth",
        custom_resolver.address,
    )
    .await?;
    ens_v1::set_text_record(
        &rpc,
        custom_resolver.address,
        alice,
        "custom.eth",
        "description",
        "custom resolver text",
    )
    .await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        // Registry-side facts (the rebind to the custom resolver) derive
        // regardless of resolver-profile admission; resolver-local facts on
        // the unadmitted instance do not produce RecordChanged events, so
        // readiness gates on the registry side only.
        Some(
            "SELECT count(*) >= 2 FROM normalized_events \
             WHERE logical_name_id = 'ens:custom.eth' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical'",
        ),
    )
    .await?;

    let record_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:custom.eth' AND event_kind = 'RecordChanged' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;

    // Pinned gating shape for a name bound to an unadmitted resolver
    // generation with a record written on it: no RecordChanged normalized
    // events derive (resolver-local facts require generation admission), the
    // inventory publishes no selectors, and every family reports an explicit
    // `not_observed_on_current_resolver` gap. Note the asymmetry with a
    // record-free unadmitted binding, which reports the families as
    // `resolver_family_pending` under unsupported_families instead — both
    // shapes are pinned by this file.
    assert_eq!(
        record_events, 0,
        "unadmitted-generation resolver writes must not derive RecordChanged events"
    );
    let exact = exact_name(&run, "custom.eth").await?;
    assert_resolver(&exact, custom_resolver.address);
    assert_eq!(
        pointer(&exact, "/declared_state/record_inventory/selectors"),
        json!([]),
        "unadmitted resolver must not publish inventory selectors; body: {exact}"
    );
    let gaps = pointer(&exact, "/declared_state/record_inventory/explicit_gaps");
    let gap_families: Vec<&str> = gaps
        .as_array()
        .into_iter()
        .flatten()
        .filter(|gap| {
            gap.get("gap_reason").and_then(Value::as_str)
                == Some("not_observed_on_current_resolver")
        })
        .filter_map(|gap| gap.get("record_family").and_then(Value::as_str))
        .collect();
    for family in ["addr", "contenthash", "text"] {
        assert!(
            gap_families.contains(&family),
            "expected explicit not_observed_on_current_resolver gap for {family}; body: {exact}"
        );
    }

    // Declared reads never serve the unadmitted write: the requested text
    // reports not_found (no value is fabricated from unadmitted facts) and
    // known-text enumeration stays supported-but-empty.
    let records = compact_records(
        &run,
        "custom.eth",
        "?texts=description&known_text_keys=true&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&records, "/data/text_records/description/status"),
        "not_found",
        "unadmitted-resolver writes must not surface as declared values; body: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/text_records/description/value"),
        Value::Null,
        "no value may accompany the not_found status; body: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/known_text_keys"),
        json!({ "keys": [], "status": "supported" }),
        "known-text enumeration stays supported-but-empty; body: {records}"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn shared_resolver_keeps_per_name_records_and_overview_fan_in_unsupported() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "sharedone", alice, YEAR, resolver).await?;
    ens_v1::register_eth_name(&rpc, &deployment, "sharedtwo", bob, YEAR, resolver).await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        alice,
        "sharedone.eth",
        "description",
        "one record",
    )
    .await?;
    ens_v1::set_addr_record(&rpc, resolver, bob, "sharedtwo.eth", bob).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT count(*) >= 2 FROM normalized_events \
             WHERE logical_name_id IN ('ens:sharedone.eth', 'ens:sharedtwo.eth') \
             AND event_kind = 'RecordChanged' AND canonicality_state = 'canonical'",
        ),
    )
    .await?;

    let one = compact_records(
        &run,
        "sharedone.eth",
        "?texts=description&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&one, "/data/text_records/description/value"),
        "one record",
        "sharedone.eth text record should stay scoped by node; body: {one}"
    );
    assert_eq!(
        pointer(&one, "/data/resolver_address"),
        format!("{resolver:#x}")
    );

    let two = compact_records(
        &run,
        "sharedtwo.eth",
        "?coin_types=60&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&two, "/data/coin_addresses/60/value"),
        format!("{bob:#x}"),
        "sharedtwo.eth addr record should stay scoped by node; body: {two}"
    );
    assert_eq!(
        pointer(&two, "/data/resolver_address"),
        format!("{resolver:#x}")
    );

    let (status, overview) = run
        .api
        .get_json(&format!(
            "/v1/resolvers/ethereum-mainnet/{resolver:#x}/overview?include=nodes&meta=full"
        ))
        .await?;
    assert_eq!(status, 200, "resolver overview failed: {overview}");
    assert_eq!(
        pointer(&overview, "/data/nodes"),
        Value::Null,
        "resolver overview should not enumerate shared-resolver fan-in; body: {overview}"
    );
    assert!(
        pointer(&overview, "/meta/unsupported_fields")
            .as_array()
            .into_iter()
            .flatten()
            .any(|field| field.as_str() == Some("nodes")),
        "resolver overview should mark nodes unsupported; body: {overview}"
    );
    assert_eq!(
        pointer(&overview, "/meta/coverage/unsupported_reason"),
        "resolver_binding_enumeration_not_projected",
        "resolver overview should report the documented fan-in unsupported reason; body: {overview}"
    );

    run.db.cleanup().await?;
    Ok(())
}
