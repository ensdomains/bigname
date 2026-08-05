use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::responses::{exact_name, pointer, selector_keys};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const MULTICOIN_TYPE: u64 = 0;
const MULTICOIN_BYTES: &[u8] = &[0xde, 0xad, 0xbe, 0xef];
const CONTENTHASH_BYTES: &[u8] = &[0xe3, 0x01, 0x01, 0x70, 0x12, 0x20];
const MULTICOIN_HEX: &str = "0xdeadbeef";
const CONTENTHASH_HEX: &str = "0xe30101701220";

fn boundary(body: &Value) -> Result<Value> {
    body.pointer("/declared_state/record_inventory/record_version_boundary")
        .cloned()
        .context("exact-name response is missing record_version_boundary")
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

fn assert_compact_record_not_success(body: &Value, path: &str) {
    assert_eq!(
        pointer(body, &format!("{path}/status")),
        "not_found",
        "old resolver cache must become not_found at {path}; body: {body}"
    );
    assert!(
        body.pointer(&format!("{path}/value")).is_none(),
        "not_found record must omit value at {path}; body: {body}"
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
        let ready_sql = support::canonical_event_ready_sql(
            "ens:0x1973cc0d7ca356c07f68eae6cb7ca41dc66e9d1552a607d2fe446fd0d3fc9804",
            "ResolverChanged",
            None,
        );
        let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
        let body = exact_name(&run.api, "ens", "flip.eth").await?;
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
             WHERE logical_name_id = 'ens:0x1973cc0d7ca356c07f68eae6cb7ca41dc66e9d1552a607d2fe446fd0d3fc9804' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical' \
             AND lower(after_state->>'resolver') = '{:#x}')",
            second_resolver.address
        );
        let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
        let body = exact_name(&run.api, "ens", "flip.eth").await?;
        assert_resolver(&body, second_resolver.address);
        run.db.cleanup().await?;
    }

    ens_v1::set_resolver(&rpc, &deployment, alice, "flip.eth", Address::ZERO).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT count(*) >= 3 FROM normalized_events \
             WHERE logical_name_id = 'ens:0x1973cc0d7ca356c07f68eae6cb7ca41dc66e9d1552a607d2fe446fd0d3fc9804' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical'",
        ),
    )
    .await?;
    let body = exact_name(&run.api, "ens", "flip.eth").await?;
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
                WHERE logical_name_id = 'ens:0x9407a4f27b24ccf343caeb964a24d93fe04d7851e8fa0813a35c1c3b9eda8574' AND event_kind = 'RecordChanged' \
                AND canonicality_state = 'canonical' \
                AND after_state->>'record_key' IN ('addr:0', 'contenthash')) \
             AND \
               EXISTS (SELECT 1 FROM normalized_events \
                WHERE logical_name_id = 'ens:0x129efdee8c82c635f3d70c6f1b7b36923b7419ade3f0c5eb4c1223435cc277dd' AND event_kind = 'RecordChanged' \
                AND canonicality_state = 'canonical' \
                AND after_state->>'record_key' = 'text:com.twitter')",
        ),
    )
    .await?;

    let records_exact = exact_name(&initial.api, "ens", "records.eth").await?;
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
    let clearable_exact = exact_name(&initial.api, "ens", "clearable.eth").await?;
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
            WHERE logical_name_id = 'ens:0x9407a4f27b24ccf343caeb964a24d93fe04d7851e8fa0813a35c1c3b9eda8574' AND event_kind = 'ResolverChanged' \
            AND canonicality_state = 'canonical' \
            AND lower(after_state->>'resolver') = '{replacement_addr}') \
         AND \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:0x129efdee8c82c635f3d70c6f1b7b36923b7419ade3f0c5eb4c1223435cc277dd' AND event_kind = 'RecordVersionChanged' \
            AND canonicality_state = 'canonical')"
    );
    let current = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let replaced_exact = exact_name(&current.api, "ens", "records.eth").await?;
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
    assert_compact_record_not_success(&replaced_records, "/data/coin_addresses/0");
    assert_compact_record_not_success(&replaced_records, "/data/content_hash");

    let cleared_exact = exact_name(&current.api, "ens", "clearable.eth").await?;
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
    assert_compact_record_not_success(&cleared_records, "/data/text_records/com.twitter");

    current.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "retired: byte-identical resolver admission by observed code hash was replaced by declared-list classification"]
async fn byte_identical_public_resolver_copy_converges_to_admitted_profile() -> Result<()> {
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

    let resolver_profile_ready = support::resolver_code_hash_comparison_sql(
        custom_resolver.address,
        deployment.public_resolver.address,
        true,
    );
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:0xa2fabe46600ed54c2ddf6013c7588548f91faf8f5a89ca514e89b32a3a69d612' AND event_kind = 'RecordChanged' \
         AND after_state->>'record_key' = 'text:description' \
         AND canonicality_state = 'canonical') \
         AND {resolver_profile_ready}",
    );

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        // The copied runtime is dynamically admitted by matching the pinned
        // PublicResolver seed. Wait for both the observed record and that
        // code-hash match so the assertions cannot capture the transient
        // pre-profile state.
        Some(&ready_sql),
    )
    .await?;

    let record_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:0xa2fabe46600ed54c2ddf6013c7588548f91faf8f5a89ca514e89b32a3a69d612' AND event_kind = 'RecordChanged' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;

    let text_changed_topic = format!(
        "{:#x}",
        alloy_primitives::keccak256("TextChanged(bytes32,string,string,string)".as_bytes())
    );
    let raw_text_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE emitting_address = $1 AND topics[1] = $2 \
         AND canonicality_state = 'canonical'",
    )
    .bind(format!("{:#x}", custom_resolver.address))
    .bind(&text_changed_topic)
    .fetch_one(&run.db.pool)
    .await?;

    // The write is retained and derives as an observed selector. Because this
    // is a byte-identical PublicResolver deployment, code-hash matching admits
    // the same declared families as the manifest seed.
    assert_eq!(
        raw_text_logs, 1,
        "the copied resolver write must remain in raw intake"
    );
    assert_eq!(
        record_events, 1,
        "the admitted copied resolver write must derive"
    );
    let exact = exact_name(&run.api, "ens", "custom.eth").await?;
    assert_resolver(&exact, custom_resolver.address);
    assert_eq!(
        pointer(&exact, "/declared_state/record_inventory/selectors"),
        json!([{
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "cacheable": true
        }]),
        "the admitted copied resolver must publish its observed selector; body: {exact}"
    );
    assert_eq!(
        pointer(&exact, "/declared_state/record_inventory/explicit_gaps"),
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "gap_reason": "not_observed_on_current_resolver"
            },
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver"
            }
        ]),
        "admitted families must report explicit absence for unobserved records; body: {exact}"
    );
    assert_eq!(
        pointer(
            &exact,
            "/declared_state/record_inventory/unsupported_families"
        ),
        json!([]),
        "byte-identical resolver families must be admitted by code hash; body: {exact}"
    );

    // The admitted profile serves the observed cache value and can enumerate
    // the observed text-key set.
    let records = compact_records(
        &run,
        "custom.eth",
        "?texts=description&known_text_keys=true&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&records, "/data/text_records/description/status"),
        "success",
        "the admitted current-resolver value must surface; body: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/text_records/description/value"),
        "custom resolver text",
        "the declared value must come from the admitted selector cache; body: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/known_text_keys"),
        json!({ "keys": ["description"], "status": "supported" }),
        "known-text enumeration must reflect the admitted profile; body: {records}"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn shared_resolver_keeps_per_name_records_and_projection_marks_fan_in_unsupported()
-> Result<()> {
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
             WHERE logical_name_id IN ('ens:0x139602c6817eb66c83e55c315be8b13b69f89cf04eae8a9815295e737729eea7', 'ens:0x7e8051d3a865d1f28e73883a890c8a7a8ebfcd91a3e1012ebabcee0d3fa85ed6') \
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

    let overview: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = 'ethereum-mainnet' AND resolver_address = $1",
    )
    .bind(format!("{resolver:#x}"))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        pointer(&overview, "/bindings/status"),
        "unsupported",
        "schema-v2 must not claim ENSv1 resolver-binding enumeration: {overview}"
    );
    assert_eq!(
        pointer(&overview, "/bindings/unsupported_reason"),
        "resolver_binding_enumeration_not_projected",
        "schema-v2 should persist the fan-in unsupported reason: {overview}"
    );
    assert_eq!(pointer(&overview, "/coverage/status"), "projected");
    assert_eq!(
        pointer(&overview, "/coverage/exhaustiveness"),
        "not_asserted"
    );

    run.db.cleanup().await?;
    Ok(())
}
