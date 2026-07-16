use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::responses::{exact_name, pointer, selector_keys};
use crate::harness::{anvil::Anvil, db::HarnessDb, ens_v1, manifests, pipeline, repo_root};

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
        let resolver_profile_ready = support::resolver_code_hash_comparison_sql(
            second_resolver.address,
            first_resolver,
            true,
        );
        let ready_sql = format!(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:flip.eth' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical' \
             AND lower(after_state->>'resolver') = '{:#x}') \
             AND {resolver_profile_ready}",
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
             WHERE logical_name_id = 'ens:flip.eth' AND event_kind = 'ResolverChanged' \
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
    let replacement_profile_ready =
        support::resolver_code_hash_comparison_sql(replacement_resolver.address, resolver_a, true);
    let ready_sql = format!(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:records.eth' AND event_kind = 'ResolverChanged' \
            AND canonicality_state = 'canonical' \
            AND lower(after_state->>'resolver') = '{replacement_addr}') \
         AND \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:clearable.eth' AND event_kind = 'RecordVersionChanged' \
            AND canonicality_state = 'canonical') \
         AND {replacement_profile_ready}"
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
    assert_compact_record_not_success(
        &cleared_records,
        "/data/text_records/com.twitter",
        json!("before-clear"),
    );

    current.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
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
         WHERE logical_name_id = 'ens:custom.eth' AND event_kind = 'RecordChanged' \
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
         WHERE logical_name_id = 'ens:custom.eth' AND event_kind = 'RecordChanged' \
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

/// Keep the production indexer, projection worker, and API live while one
/// resolver's effective code hash moves supported -> unsupported -> supported.
/// Bootstrap replay completes before the tested name exists; no normalized-
/// event or projection full replay runs after that point, so both transitions
/// must cross the durable resolver-profile queue.
#[tokio::test]
async fn live_code_hash_profile_transition_orphans_and_reactivates_records() -> Result<()> {
    const NAME: &str = "profiletransition.eth";
    const LOGICAL_NAME_ID: &str = "ens:profiletransition.eth";
    const TEXT_KEY: &str = "description";
    const TEXT_RECORD_KEY: &str = "text:description";
    const TEXT_VALUE: &str = "live profile transition";

    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let resolver = ens_v1::deploy_extra_public_resolver(&rpc, &root, &deployment).await?;
    let owner = rpc.accounts().await?[1];
    rpc.mine(2).await?;
    let deployment_head = rpc.block_number().await?;

    let scratch = support::TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    let db = HarnessDb::create().await?;
    let mut bootstrap_indexer = pipeline::IndexerRunSession::start(
        &root,
        &db.url,
        &profile.root,
        &anvil.url,
        "resolver-profile-bootstrap",
    )
    .await?;
    let first_checkpoint = bootstrap_indexer
        .wait_for_first_checkpoint(&db.pool)
        .await?;
    assert!(
        first_checkpoint >= deployment_head as i64,
        "initial indexer checkpoint {first_checkpoint} did not cover deployment-only head {deployment_head}"
    );

    let mut worker =
        pipeline::WorkerRunSession::start(&root, &db.url, "resolver-profile-transition").await?;
    worker
        .wait_for_sql(
            &db.pool,
            "SELECT EXISTS (SELECT 1 FROM projection_apply_cursors \
             WHERE cursor_name = 'normalized_events_to_projection_invalidations')",
        )
        .await?;
    bootstrap_indexer.stop().await?;

    let chain_rpc_urls = [("ethereum-mainnet", anvil.url.as_str())];
    let mut indexer = pipeline::IndexerRunSession::start_with_live_poll_adapter_sync(
        &root,
        &db.url,
        &profile.root,
        &chain_rpc_urls,
        "resolver-profile-transition",
    )
    .await?;
    rpc.mine(1).await?;
    let live_poll_head = rpc.block_number().await?;
    indexer
        .wait_for_checkpoint(&db.pool, live_poll_head, None)
        .await?;

    // The name and record arrive only after both production loops have
    // completed their initial handoff and the bootstrap indexer has been
    // replaced by live-poll adapter sync. Advancing one empty block proves the
    // replacement process is active. Every assertion below therefore uses
    // live adapter reconciliation and continuous projection apply.
    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "profiletransition",
        owner,
        YEAR,
        resolver.address,
    )
    .await?;
    ens_v1::set_text_record(&rpc, resolver.address, owner, NAME, TEXT_KEY, TEXT_VALUE).await?;
    rpc.mine(2).await?;
    let initial_head = rpc.block_number().await?;

    let resolver_address = format!("{:#x}", resolver.address);
    let seed_address = deployment.public_resolver.address;
    let profile_match =
        support::resolver_code_hash_comparison_sql(resolver.address, seed_address, true);
    let initial_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{LOGICAL_NAME_ID}' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = '{TEXT_RECORD_KEY}' \
           AND after_state->>'value' = '{TEXT_VALUE}' \
           AND canonicality_state = 'canonical') \
         AND {profile_match} \
         AND EXISTS (SELECT 1 FROM resolver_profile_input_changes \
          WHERE chain_id = 'ethereum-mainnet' \
            AND contract_address = '{resolver_address}' \
            AND processed_generation = generation)"
    );
    indexer
        .wait_for_checkpoint(&db.pool, initial_head, Some(&initial_ready_sql))
        .await?;

    let (normalized_event_id, event_identity, resource_id): (i64, String, String) = sqlx::query_as(
        "SELECT normalized_event_id, event_identity, resource_id::TEXT \
             FROM normalized_events \
             WHERE logical_name_id = $1 \
               AND event_kind = 'RecordChanged' \
               AND after_state->>'record_key' = $2 \
               AND after_state->>'value' = $3 \
               AND canonicality_state = 'canonical'",
    )
    .bind(LOGICAL_NAME_ID)
    .bind(TEXT_RECORD_KEY)
    .bind(TEXT_VALUE)
    .fetch_one(&db.pool)
    .await?;
    let (raw_code_hash_id, original_code_hash, code_byte_length): (i64, String, i64) =
        sqlx::query_as(
            "SELECT raw_code_hash_id, lower(code_hash), code_byte_length \
             FROM raw_code_hashes \
             WHERE chain_id = 'ethereum-mainnet' \
               AND contract_address = $1 \
               AND canonicality_state <> 'orphaned' \
             ORDER BY block_number DESC, \
               CASE canonicality_state \
                 WHEN 'finalized' THEN 4 WHEN 'safe' THEN 3 \
                 WHEN 'canonical' THEN 2 WHEN 'observed' THEN 1 ELSE 0 \
               END DESC, raw_code_hash_id DESC \
             LIMIT 1",
        )
        .bind(&resolver_address)
        .fetch_one(&db.pool)
        .await?;
    let (initial_generation, initial_processed_generation, queued_code_hash): (
        i64,
        i64,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT generation, processed_generation, current_code_hash \
         FROM resolver_profile_input_changes \
         WHERE chain_id = 'ethereum-mainnet' AND contract_address = $1",
    )
    .bind(&resolver_address)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(initial_processed_generation, initial_generation);
    assert_eq!(
        queued_code_hash.as_deref(),
        Some(original_code_hash.as_str())
    );

    let initial_projection_ready = format!(
        "SELECT EXISTS (SELECT 1 \
          FROM record_inventory_current inventory \
          CROSS JOIN LATERAL jsonb_array_elements(inventory.selectors) selector \
          WHERE inventory.resource_id = '{resource_id}'::UUID \
            AND selector->>'record_key' = '{TEXT_RECORD_KEY}' \
            AND inventory.unsupported_families = '[]'::JSONB)"
    );
    worker
        .wait_for_sql(&db.pool, &initial_projection_ready)
        .await?;
    let api = pipeline::ApiServer::start(&root, &db.url).await?;

    let initial_exact = exact_name(&api, "ens", NAME).await?;
    assert_resolver(&initial_exact, resolver.address);
    assert!(
        selector_keys(&initial_exact).contains(TEXT_RECORD_KEY),
        "the admitted profile must publish the text selector before perturbation: {initial_exact}"
    );
    let (status, initial_records) = api
        .get_json(&format!(
            "/v1/names/ens/{NAME}/records?texts={TEXT_KEY}&known_text_keys=true&mode=declared&meta=full"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "initial records lookup failed: {initial_records}"
    );
    assert_eq!(
        pointer(
            &initial_records,
            &format!("/data/text_records/{TEXT_KEY}/value")
        ),
        TEXT_VALUE
    );
    assert_eq!(
        pointer(&initial_records, "/data/known_text_keys/status"),
        "supported"
    );

    let unsupported_code_hash = format!(
        "{:#x}",
        alloy_primitives::keccak256(b"bigname-e2e-unsupported-resolver-profile")
    );
    assert_ne!(unsupported_code_hash, original_code_hash);
    let unsupported_correction = bigname_storage::RawCodeHashCorrectionUpdate {
        raw_code_hash_id,
        stored_code_hash: original_code_hash.clone(),
        stored_code_byte_length: code_byte_length,
        corrected_code_hash: unsupported_code_hash.clone(),
        corrected_code_byte_length: code_byte_length,
    };
    let correction = bigname_storage::apply_raw_code_hash_corrections(
        &db.pool,
        std::slice::from_ref(&unsupported_correction),
    )
    .await?;
    assert_eq!(correction.corrected_count, 1);

    rpc.mine(1).await?;
    let unsupported_head = rpc.block_number().await?;
    let unsupported_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE normalized_event_id = {normalized_event_id} \
           AND canonicality_state = 'orphaned') \
         AND EXISTS (SELECT 1 FROM resolver_profile_input_changes \
          WHERE chain_id = 'ethereum-mainnet' \
            AND contract_address = '{resolver_address}' \
            AND generation > {initial_generation} \
            AND processed_generation = generation \
            AND current_code_hash = '{unsupported_code_hash}')"
    );
    indexer
        .wait_for_checkpoint(&db.pool, unsupported_head, Some(&unsupported_ready_sql))
        .await?;

    let unsupported_projection_ready = format!(
        "SELECT EXISTS (SELECT 1 FROM record_inventory_current inventory \
         WHERE inventory.resource_id = '{resource_id}'::UUID \
           AND inventory.selectors = '[]'::JSONB \
           AND inventory.coverage->>'unsupported_reason' = 'resolver_family_unsupported' \
           AND EXISTS (SELECT 1 \
             FROM jsonb_array_elements(inventory.unsupported_families) family \
             WHERE family->>'record_family' = 'text' \
               AND family->>'unsupported_reason' = 'resolver_family_unsupported')) \
         AND NOT EXISTS (SELECT 1 FROM projection_invalidations \
          WHERE (projection = 'record_inventory_current' \
                   AND projection_key = '{resource_id}') \
             OR (projection = 'resolver_current' \
                   AND projection_key = 'ethereum-mainnet:{resolver_address}'))"
    );
    worker
        .wait_for_sql(&db.pool, &unsupported_projection_ready)
        .await?;

    let (orphaned_identity, orphaned_state): (String, String) = sqlx::query_as(
        "SELECT event_identity, canonicality_state::TEXT \
         FROM normalized_events WHERE normalized_event_id = $1",
    )
    .bind(normalized_event_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(orphaned_identity, event_identity);
    assert_eq!(orphaned_state, "orphaned");

    let unsupported_exact = exact_name(&api, "ens", NAME).await?;
    assert_resolver(&unsupported_exact, resolver.address);
    assert!(
        !selector_keys(&unsupported_exact).contains(TEXT_RECORD_KEY),
        "an unsupported profile must not publish the stale selector: {unsupported_exact}"
    );
    let unsupported_families = unsupported_exact
        .pointer("/declared_state/record_inventory/unsupported_families")
        .and_then(Value::as_array)
        .context("unsupported profile must publish explicit unsupported families")?;
    assert!(
        unsupported_families.iter().any(|family| {
            family.get("record_family").and_then(Value::as_str) == Some("text")
                && family.get("unsupported_reason").and_then(Value::as_str)
                    == Some("resolver_family_unsupported")
        }),
        "the text family must become explicitly unsupported: {unsupported_exact}"
    );
    let (status, unsupported_records) = api
        .get_json(&format!(
            "/v1/names/ens/{NAME}/records?texts={TEXT_KEY}&known_text_keys=true&mode=declared&meta=full"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "unsupported records lookup failed: {unsupported_records}"
    );
    assert_eq!(
        pointer(
            &unsupported_records,
            &format!("/data/text_records/{TEXT_KEY}/status")
        ),
        "unsupported"
    );
    assert_ne!(
        pointer(
            &unsupported_records,
            &format!("/data/text_records/{TEXT_KEY}/value")
        ),
        TEXT_VALUE,
        "the stale declared value must disappear while the profile is unsupported"
    );
    assert_eq!(
        pointer(&unsupported_records, "/data/known_text_keys/status"),
        "unsupported"
    );

    let unsupported_generation: i64 = sqlx::query_scalar(
        "SELECT generation FROM resolver_profile_input_changes \
         WHERE chain_id = 'ethereum-mainnet' AND contract_address = $1 \
           AND processed_generation = generation",
    )
    .bind(&resolver_address)
    .fetch_one(&db.pool)
    .await?;
    assert!(unsupported_generation > initial_generation);

    let restored_correction = bigname_storage::RawCodeHashCorrectionUpdate {
        raw_code_hash_id,
        stored_code_hash: unsupported_code_hash,
        stored_code_byte_length: code_byte_length,
        corrected_code_hash: original_code_hash.clone(),
        corrected_code_byte_length: code_byte_length,
    };
    let restoration = bigname_storage::apply_raw_code_hash_corrections(
        &db.pool,
        std::slice::from_ref(&restored_correction),
    )
    .await?;
    assert_eq!(restoration.corrected_count, 1);

    rpc.mine(1).await?;
    let restored_head = rpc.block_number().await?;
    let restored_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE normalized_event_id = {normalized_event_id} \
           AND event_identity = '{event_identity}' \
           AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM resolver_profile_input_changes \
          WHERE chain_id = 'ethereum-mainnet' \
            AND contract_address = '{resolver_address}' \
            AND generation > {unsupported_generation} \
            AND processed_generation = generation \
            AND current_code_hash = '{original_code_hash}')"
    );
    indexer
        .wait_for_checkpoint(&db.pool, restored_head, Some(&restored_ready_sql))
        .await?;

    let restored_projection_ready = format!(
        "SELECT EXISTS (SELECT 1 \
         FROM record_inventory_current inventory \
         CROSS JOIN LATERAL jsonb_array_elements(inventory.selectors) selector \
         WHERE inventory.resource_id = '{resource_id}'::UUID \
           AND selector->>'record_key' = '{TEXT_RECORD_KEY}' \
           AND inventory.unsupported_families = '[]'::JSONB) \
         AND NOT EXISTS (SELECT 1 FROM projection_invalidations \
          WHERE (projection = 'record_inventory_current' \
                   AND projection_key = '{resource_id}') \
             OR (projection = 'resolver_current' \
                   AND projection_key = 'ethereum-mainnet:{resolver_address}'))"
    );
    worker
        .wait_for_sql(&db.pool, &restored_projection_ready)
        .await?;

    let restored_exact = exact_name(&api, "ens", NAME).await?;
    assert_resolver(&restored_exact, resolver.address);
    assert!(
        selector_keys(&restored_exact).contains(TEXT_RECORD_KEY),
        "restoring the admitted hash must reactivate the selector: {restored_exact}"
    );
    let (status, restored_records) = api
        .get_json(&format!(
            "/v1/names/ens/{NAME}/records?texts={TEXT_KEY}&known_text_keys=true&mode=declared&meta=full"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "restored records lookup failed: {restored_records}"
    );
    assert_eq!(
        pointer(
            &restored_records,
            &format!("/data/text_records/{TEXT_KEY}/status")
        ),
        "success"
    );
    assert_eq!(
        pointer(
            &restored_records,
            &format!("/data/text_records/{TEXT_KEY}/value")
        ),
        TEXT_VALUE
    );
    assert_eq!(
        pointer(&restored_records, "/data/known_text_keys"),
        json!({ "keys": [TEXT_KEY], "status": "supported" })
    );

    worker.stop().await?;
    indexer.stop().await?;
    drop(api);
    db.cleanup().await?;
    drop(scratch);
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
