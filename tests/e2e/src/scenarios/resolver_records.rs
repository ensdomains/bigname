use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::responses::{exact_name, pointer, selector_keys};
use crate::harness::{anvil::Anvil, db::HarnessDb, ens_v1, manifests, pipeline, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const MULTICOIN_TYPE: u64 = 0;
const ENSIP19_DEFAULT_COIN_TYPE: u64 = 1 << 31;
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

pub(super) async fn start_split_replay(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    target: u64,
    ready_sql: &str,
) -> Result<(
    HarnessDb,
    support::TempDir,
    pipeline::SequentialFixtureReplay,
)> {
    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [("ethereum-mainnet", anvil.url.as_str())];
    let mut replay = pipeline::SequentialFixtureReplay::start_with_chain_rpc_urls(
        &root,
        &db.url,
        &profile.root,
        &chain_rpc_urls,
    )
    .await?;
    replay
        .replay_chain_through(&db.pool, "ethereum-mainnet", target, Some(ready_sql))
        .await?;
    Ok((db, scratch, replay))
}

pub(super) async fn materialize_wrapped_surface(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    owner: Address,
    name: &str,
    previous_target: u64,
    db: &HarnessDb,
    replay: &mut pipeline::SequentialFixtureReplay,
) -> Result<u64> {
    let rpc = anvil.client();
    ens_v1::set_name_record_for_node(
        &rpc,
        deployment.public_resolver.address,
        owner,
        ens_v1::namehash(name),
        name,
    )
    .await?;
    ens_v1::set_registry_approval_for_all(
        &rpc,
        deployment,
        owner,
        deployment.name_wrapper.address,
        true,
    )
    .await?;
    ens_v1::wrap_registry_name(&rpc, deployment, owner, name, owner, Address::ZERO).await?;
    rpc.mine(1).await?;
    let target = rpc.block_number().await?;
    replay
        .replay_chain_range(
            &db.pool,
            "ethereum-mainnet",
            previous_target + 1,
            target,
            Some(&format!(
                "SELECT EXISTS (SELECT 1 FROM name_surfaces WHERE logical_name_id = '{}')",
                support::schema_v2_logical_name_id(&format!("ens:{name}"))
            )),
        )
        .await?;
    Ok(target)
}

pub(super) async fn select_wrapped_resolver(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    owner: Address,
    selection: (&str, Address),
    previous_target: u64,
    db: &HarnessDb,
    replay: &mut pipeline::SequentialFixtureReplay,
) -> Result<u64> {
    let (name, resolver) = selection;
    let rpc = anvil.client();
    ens_v1::set_wrapped_resolver(&rpc, deployment, owner, name, resolver).await?;
    rpc.mine(1).await?;
    let target = rpc.block_number().await?;
    replay
        .replay_chain_range(
            &db.pool,
            "ethereum-mainnet",
            previous_target + 1,
            target,
            None,
        )
        .await?;
    Ok(target)
}

async fn materialize_and_select_wrapped_resolver(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    owner: Address,
    selection: (&str, Address),
    previous_target: u64,
    db: &HarnessDb,
    replay: &mut pipeline::SequentialFixtureReplay,
) -> Result<u64> {
    let surface_target = materialize_wrapped_surface(
        anvil,
        deployment,
        owner,
        selection.0,
        previous_target,
        db,
        replay,
    )
    .await?;
    select_wrapped_resolver(
        anvil,
        deployment,
        owner,
        selection,
        surface_target,
        db,
        replay,
    )
    .await
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
async fn pre_surface_newowner_record_serves_after_late_surface() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let alice = rpc.accounts().await?[1];
    let resolver = deployment.public_resolver.address;
    let name = "before.known.eth";
    let node = format!("{:#x}", ens_v1::namehash(name));

    ens_v1::register_eth_name(&rpc, &deployment, "known", alice, YEAR, resolver).await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "known.eth", "before", alice).await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        alice,
        name,
        "description",
        "written before the surface",
    )
    .await?;
    let null_link_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:description' \
           AND lower(after_state->>'node') = lower('{node}') \
           AND logical_name_id IS NULL)"
    );
    let record_target = rpc.block_number().await?;
    let (db, scratch, mut replay) =
        start_split_replay(&anvil, &deployment, record_target, &null_link_ready_sql).await?;
    materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        alice,
        (name, resolver),
        record_target,
        &db,
        &mut replay,
    )
    .await?;
    let run = support::serve_existing_db(db, scratch, &anvil).await?;

    let (linked_name, retained_node, emitting_resolver): (Option<String>, String, String) =
        sqlx::query_as(
            "SELECT logical_name_id, after_state->>'node', \
                    COALESCE(after_state->>'resolver', raw_fact_ref->>'emitting_address') \
             FROM normalized_events \
             WHERE event_kind = 'RecordChanged' \
               AND after_state->>'record_key' = 'text:description' \
               AND lower(after_state->>'node') = lower($1)",
        )
        .bind(&node)
        .fetch_one(&run.db.pool)
        .await?;
    assert_eq!(
        linked_name, None,
        "the interpretation-time name link must stay null"
    );
    assert_eq!(retained_node.to_lowercase(), node);
    assert_eq!(emitting_resolver.to_lowercase(), format!("{resolver:#x}"));

    let exact = exact_name(&run.api, "ens", name).await?;
    assert!(
        selector_keys(&exact).contains("text:description"),
        "late surface must recover the earlier record in inventory: {exact}"
    );
    let records = compact_records(&run, name, "?texts=description&mode=declared&meta=full").await?;
    assert_eq!(
        pointer(&records, "/data/text_records/description/value"),
        "written before the surface"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pre_surface_record_history_follows_current_resolver_and_version_boundary() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let resolver_two =
        ens_v1::deploy_extra_public_resolver(&rpc, &repo_root(), &deployment).await?;
    let owner = rpc.accounts().await?[1];
    let resolver_one = deployment.public_resolver.address;
    let name = "version.history.eth";
    let node = format!("{:#x}", ens_v1::namehash(name));

    ens_v1::register_eth_name(&rpc, &deployment, "history", owner, YEAR, resolver_one).await?;
    ens_v1::create_subname(&rpc, &deployment, owner, "history.eth", "version", owner).await?;
    ens_v1::set_text_record(&rpc, resolver_one, owner, name, "legacy", "before-version").await?;
    ens_v1::clear_records(&rpc, resolver_one, owner, name).await?;
    ens_v1::set_text_record(&rpc, resolver_one, owner, name, "active", "resolver-one").await?;
    ens_v1::set_text_record(
        &rpc,
        resolver_two.address,
        owner,
        name,
        "active",
        "resolver-two",
    )
    .await?;

    let record_target = rpc.block_number().await?;
    let null_ready = format!(
        "SELECT count(*) >= 4 FROM normalized_events \
         WHERE logical_name_id IS NULL \
           AND event_kind IN ('RecordChanged', 'RecordVersionChanged') \
           AND lower(after_state->>'node') = lower('{node}')"
    );
    let (db, scratch, mut replay) =
        start_split_replay(&anvil, &deployment, record_target, &null_ready).await?;
    let resolver_one_target = materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name, resolver_one),
        record_target,
        &db,
        &mut replay,
    )
    .await?;
    let run = support::serve_existing_db(db, scratch, &anvil).await?;

    let resolver_one_records =
        compact_records(&run, name, "?texts=legacy,active&mode=declared&meta=full").await?;
    assert_compact_record_not_success(&resolver_one_records, "/data/text_records/legacy");
    assert_eq!(
        pointer(&resolver_one_records, "/data/text_records/active/value"),
        "resolver-one"
    );

    let resolver_two_target = select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name, resolver_two.address),
        resolver_one_target,
        &run.db,
        &mut replay,
    )
    .await?;
    let resolver_two_records =
        compact_records(&run, name, "?texts=active&mode=declared&meta=full").await?;
    assert_eq!(
        pointer(&resolver_two_records, "/data/text_records/active/value"),
        "resolver-two"
    );

    select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name, resolver_one),
        resolver_two_target,
        &run.db,
        &mut replay,
    )
    .await?;
    let restored = compact_records(&run, name, "?texts=active&mode=declared&meta=full").await?;
    assert_eq!(
        pointer(&restored, "/data/text_records/active/value"),
        "resolver-one",
        "selecting the first resolver again must restore its pre-pointer value"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pre_surface_record_attribution_is_node_scoped_and_never_materializes_unknown_names()
-> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let owner = rpc.accounts().await?[1];
    let resolver = deployment.public_resolver.address;
    let (name_one, name_two, unknown_name) = ("one.scope.eth", "two.scope.eth", "orphan");

    ens_v1::register_eth_name(&rpc, &deployment, "scope", owner, YEAR, resolver).await?;
    ens_v1::create_subname(&rpc, &deployment, owner, "scope.eth", "one", owner).await?;
    ens_v1::create_subname(&rpc, &deployment, owner, "scope.eth", "two", owner).await?;
    ens_v1::create_subname(
        &rpc,
        &deployment,
        deployment.deployer,
        "",
        unknown_name,
        owner,
    )
    .await?;
    ens_v1::set_text_record(&rpc, resolver, owner, name_one, "description", "one-only").await?;
    ens_v1::set_text_record(&rpc, resolver, owner, name_two, "description", "two-only").await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        owner,
        unknown_name,
        "description",
        "never-served",
    )
    .await?;

    let record_target = rpc.block_number().await?;
    let nodes =
        [name_one, name_two, unknown_name].map(|name| format!("{:#x}", ens_v1::namehash(name)));
    let null_ready = format!(
        "SELECT count(*) = 3 FROM normalized_events \
         WHERE logical_name_id IS NULL AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:description' \
           AND lower(after_state->>'node') IN (lower('{}'), lower('{}'), lower('{}'))",
        nodes[0], nodes[1], nodes[2]
    );
    let (db, scratch, mut replay) =
        start_split_replay(&anvil, &deployment, record_target, &null_ready).await?;
    let one_pointer = materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name_one, resolver),
        record_target,
        &db,
        &mut replay,
    )
    .await?;
    materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name_two, resolver),
        one_pointer,
        &db,
        &mut replay,
    )
    .await?;
    let run = support::serve_existing_db(db, scratch, &anvil).await?;

    for (name, expected) in [(name_one, "one-only"), (name_two, "two-only")] {
        let records =
            compact_records(&run, name, "?texts=description&mode=declared&meta=full").await?;
        assert_eq!(
            pointer(&records, "/data/text_records/description/value"),
            expected,
            "shared-resolver attribution leaked across nodes: {records}"
        );
    }

    let (surface_count, name_count, child_count, inventory_count, discovery_count): (
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        "SELECT
           (SELECT count(*) FROM name_surfaces WHERE namehash = $1),
           (SELECT count(*) FROM name_current WHERE namehash = $1),
           (SELECT count(*) FROM children_current WHERE namehash = $1),
           (SELECT count(DISTINCT inventory.resource_id)
            FROM record_inventory_current inventory
            JOIN normalized_events event USING (resource_id)
            WHERE lower(event.after_state->>'node') = lower($1)),
           (SELECT count(*) FROM discovery_edges
            WHERE lower(provenance::text) LIKE '%' || lower($1) || '%')",
    )
    .bind(&nodes[2])
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        (
            surface_count,
            name_count,
            child_count,
            inventory_count,
            discovery_count
        ),
        (0, 0, 0, 0, 0),
        "unknown-node history must remain audit-only"
    );

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
    ens_v1::set_multicoin_addr_record(
        &rpc,
        resolver_a,
        alice,
        "records.eth",
        ENSIP19_DEFAULT_COIN_TYPE,
        alice.as_slice(),
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
               (SELECT count(DISTINCT after_state->>'record_key') >= 3 FROM normalized_events \
                WHERE logical_name_id = 'ens:0x9407a4f27b24ccf343caeb964a24d93fe04d7851e8fa0813a35c1c3b9eda8574' AND event_kind = 'RecordChanged' \
                AND canonicality_state = 'canonical' \
                AND after_state->>'record_key' IN ('addr:0', 'addr:2147483648', 'contenthash')) \
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
    for expected in ["addr:0", "addr:2147483648", "contenthash"] {
        assert!(
            selectors.contains(expected),
            "expected selector {expected} in records.eth inventory; body: {records_exact}"
        );
    }
    let initial_records_boundary = boundary(&records_exact)?;
    let (classification, provenance): (Value, Value) = sqlx::query_as(
        "SELECT resolver.declared_summary->'classification', inventory.provenance
         FROM resolver_current resolver
         JOIN record_inventory_current inventory
           ON inventory.provenance->>'resolver_address' = resolver.resolver_address
         WHERE resolver.chain_id = 'ethereum-mainnet'
           AND resolver.resolver_address = $1
         ORDER BY inventory.resource_id
         LIMIT 1",
    )
    .bind(format!("{resolver_a:#x}"))
    .fetch_one(&initial.db.pool)
    .await?;
    assert_eq!(
        classification["read_features"],
        json!(["ensip19_default_address"])
    );
    assert_eq!(
        provenance["read_rules"],
        json!([{
            "kind": "ensip19_default_address",
            "source_record_key": "addr:2147483648"
        }])
    );

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
        json!(MULTICOIN_HEX)
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
