use alloy_primitives::{Address, B256};
use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const DURATION_SECS: u64 = 365 * 24 * 60 * 60;

fn path_name(name: &str) -> String {
    name.replace('[', "%5B").replace(']', "%5D")
}

fn child_entries(body: &Value) -> Vec<Value> {
    body.pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

async fn children_for(run: &support::PipelineRun, parent: &str) -> Result<Vec<Value>> {
    let (status, body) = run
        .api
        .get_json(&format!("/v1/names/ens/{}/children", path_name(parent)))
        .await?;
    assert_eq!(status, 200, "children lookup for {parent} failed: {body}");
    Ok(child_entries(&body))
}

fn account(accounts: &[Address], index: usize) -> Result<Address> {
    accounts
        .get(index)
        .copied()
        .with_context(|| format!("anvil account {index} is missing"))
}

#[tokio::test]
async fn registry_migration_legacy_to_current_semantics() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let deployer = deployment.deployer;
    let parent_owner = account(&accounts, 1)?;
    let legacy_only_owner = account(&accounts, 2)?;
    let still_legacy_owner = account(&accounts, 3)?;
    let migrate_current_owner = account(&accounts, 4)?;
    let suppressed_legacy_owner = account(&accounts, 5)?;
    let suppressed_legacy_resolver = account(&accounts, 6)?;
    let legacy_2ld_owner = account(&accounts, 7)?;
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "legacyparent",
        parent_owner,
        DURATION_SECS,
        resolver,
    )
    .await?;

    ens_v1::create_legacy_subname(&rpc, &deployment, deployer, B256::ZERO, "eth", deployer).await?;
    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployer,
        ens_v1::namehash("eth"),
        "legacyparent",
        deployer,
    )
    .await?;
    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployer,
        ens_v1::namehash("eth"),
        "legacyonly",
        legacy_2ld_owner,
    )
    .await?;
    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployer,
        ens_v1::namehash("eth"),
        "migrate",
        deployer,
    )
    .await?;
    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployer,
        ens_v1::namehash("legacyparent.eth"),
        "legacyonly",
        legacy_only_owner,
    )
    .await?;

    // Phase 1: ingest the purely-legacy chain state. The prior legacy owner
    // is observable before the current-registry write supersedes it, without
    // admitting that owner address as a registry contract.
    let legacy_registry = format!("{:#x}", deployment.legacy_registry.address);
    let eth_node = format!("{:#x}", ens_v1::namehash("eth"));
    let migrate_labelhash = format!("{:#x}", ens_v1::labelhash("migrate"));
    let deployer_hex = format!("{deployer:#x}");
    let phase1_ready = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
         AND source_family = 'ens_v1_registry_l1' \
         AND canonicality_state = 'canonical' \
         AND lower(after_state->>'node') = '{eth_node}' \
         AND lower(after_state->>'labelhash') = '{migrate_labelhash}' \
         AND lower(after_state->>'owner') = '{deployer_hex}' \
         AND lower(raw_fact_ref->>'emitting_address') = '{legacy_registry}')"
    );
    let legacy_run = support::ingest_and_serve(&anvil, &deployment, Some(&phase1_ready)).await?;
    assert_prior_legacy_migrate_state(&legacy_run, &deployment, deployer).await?;
    legacy_run.db.cleanup().await?;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "migrate",
        migrate_current_owner,
        DURATION_SECS,
        resolver,
    )
    .await?;
    ens_v1::set_legacy_resolver(
        &rpc,
        &deployment,
        deployer,
        ens_v1::namehash("migrate.eth"),
        suppressed_legacy_resolver,
    )
    .await?;
    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployer,
        ens_v1::namehash("eth"),
        "migrate",
        suppressed_legacy_owner,
    )
    .await?;

    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployer,
        ens_v1::namehash("legacyparent.eth"),
        "stilllegacy",
        still_legacy_owner,
    )
    .await?;

    let legacy_parent_node = format!("{:#x}", ens_v1::namehash("legacyparent.eth"));
    let legacy_2ld_labelhash = format!("{:#x}", ens_v1::labelhash("legacyonly"));
    let legacy_only_labelhash = format!("{:#x}", ens_v1::labelhash("legacyonly"));
    let still_legacy_labelhash = format!("{:#x}", ens_v1::labelhash("stilllegacy"));
    let current_resolver = format!("{resolver:#x}");
    let migrate_current_owner_hex = format!("{migrate_current_owner:#x}");
    let ready_sql = format!(
        "SELECT \
         EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'SubregistryChanged' \
           AND source_family = 'ens_v1_registry_l1' \
           AND canonicality_state = 'canonical' \
           AND lower(after_state->>'node') = '{eth_node}' \
           AND lower(after_state->>'labelhash') = '{legacy_2ld_labelhash}' \
           AND lower(after_state->>'owner') = '{legacy_2ld_owner:#x}' \
           AND lower(raw_fact_ref->>'emitting_address') = '{legacy_registry}') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'SubregistryChanged' \
           AND source_family = 'ens_v1_registry_l1' \
           AND canonicality_state = 'canonical' \
           AND lower(after_state->>'node') = '{legacy_parent_node}' \
           AND lower(after_state->>'labelhash') = '{legacy_only_labelhash}' \
           AND lower(after_state->>'owner') = '{legacy_only_owner:#x}' \
           AND lower(raw_fact_ref->>'emitting_address') = '{legacy_registry}') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'SubregistryChanged' \
           AND source_family = 'ens_v1_registry_l1' \
           AND canonicality_state = 'canonical' \
           AND lower(after_state->>'node') = '{legacy_parent_node}' \
           AND lower(after_state->>'labelhash') = '{still_legacy_labelhash}' \
           AND lower(after_state->>'owner') = '{still_legacy_owner:#x}' \
           AND lower(raw_fact_ref->>'emitting_address') = '{legacy_registry}') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE (logical_name_id = 'ens:0x47841aea2cf37e6ff19afa53e6181c9ca34542bee4bc5201ea9936bb884412ac' OR after_state->>'node' = '0x47841aea2cf37e6ff19afa53e6181c9ca34542bee4bc5201ea9936bb884412ac') \
           AND event_kind = 'ResolverChanged' \
           AND source_family = 'ens_v1_registry_l1' \
           AND canonicality_state = 'canonical' \
           AND lower(after_state->>'resolver') = '{current_resolver}') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE (logical_name_id = 'ens:0x47841aea2cf37e6ff19afa53e6181c9ca34542bee4bc5201ea9936bb884412ac' OR after_state->>'node' = '0x47841aea2cf37e6ff19afa53e6181c9ca34542bee4bc5201ea9936bb884412ac') \
           AND event_kind = 'AuthorityTransferred' \
           AND source_family = 'ens_v1_registry_l1' \
           AND canonicality_state = 'canonical' \
           AND lower(after_state->>'owner') = '{migrate_current_owner_hex}')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    assert_legacy_subregistry_observed_without_owner_admission(
        &run,
        &deployment,
        "eth",
        "legacyonly",
        legacy_2ld_owner,
    )
    .await?;
    assert_legacy_2ld_public_state(&run, "legacyonly.eth").await?;
    assert_legacy_subregistry_observed_without_owner_admission(
        &run,
        &deployment,
        "legacyparent.eth",
        "legacyonly",
        legacy_only_owner,
    )
    .await?;
    assert_legacy_subregistry_observed_without_owner_admission(
        &run,
        &deployment,
        "legacyparent.eth",
        "stilllegacy",
        still_legacy_owner,
    )
    .await?;
    assert_current_migrate_state(
        &run,
        migrate_current_owner,
        resolver,
        suppressed_legacy_owner,
        suppressed_legacy_resolver,
    )
    .await?;

    let children = children_for(&run, "legacyparent.eth").await?;
    assert_placeholder_child(
        &children,
        "legacyonly",
        "legacyparent.eth",
        legacy_only_owner,
    );
    assert_placeholder_child(
        &children,
        "stilllegacy",
        "legacyparent.eth",
        still_legacy_owner,
    );

    assert_exact_name_not_minted(&run, "legacyonly.legacyparent.eth").await?;
    assert_exact_name_not_minted(&run, "stilllegacy.legacyparent.eth").await?;

    let (status, migrate_body) = run.api.get_json("/v1/names/ens/migrate.eth").await?;
    assert_eq!(
        status, 200,
        "current-registry migrated exact-name lookup failed: {migrate_body}"
    );
    assert_eq!(
        migrate_body
            .pointer("/declared_state/resolver/address")
            .cloned()
            .unwrap_or(Value::Null),
        format!("{resolver:#x}"),
        "current-registry resolver should stand after later legacy resolver write; body: {migrate_body}"
    );
    assert_eq!(
        migrate_body
            .pointer("/declared_state/control/registry_owner")
            .cloned()
            .unwrap_or(Value::Null),
        format!("{migrate_current_owner:#x}"),
        "the current-registry owner must stand after the later legacy write: {migrate_body}"
    );

    run.db.cleanup().await?;
    Ok(())
}

async fn assert_legacy_subregistry_observed_without_owner_admission(
    run: &support::PipelineRun,
    deployment: &ens_v1::EnsV1Deployment,
    parent: &str,
    label: &str,
    owner: Address,
) -> Result<()> {
    let parent_node = format!("{:#x}", ens_v1::namehash(parent));
    let labelhash = format!("{:#x}", ens_v1::labelhash(label));
    let child_node = format!("{:#x}", ens_v1::namehash(&format!("{label}.{parent}")));
    let owner_hex = format!("{owner:#x}");
    let legacy_registry = format!("{:#x}", deployment.legacy_registry.address);

    let events = subregistry_events_for_child(run, &child_node).await?;
    let event_rows = events.as_array().cloned().unwrap_or_default();
    let observed = event_rows.iter().any(|event| {
        event.pointer("/source_family").and_then(Value::as_str) == Some("ens_v1_registry_l1")
            && event.pointer("/after_state/node").and_then(Value::as_str)
                == Some(parent_node.as_str())
            && event
                .pointer("/after_state/labelhash")
                .and_then(Value::as_str)
                == Some(labelhash.as_str())
            && event.pointer("/after_state/owner").and_then(Value::as_str)
                == Some(owner_hex.as_str())
            && event
                .pointer("/raw_fact_ref/emitting_address")
                .and_then(Value::as_str)
                == Some(legacy_registry.as_str())
    });
    assert!(
        observed,
        "expected legacy-registry SubregistryChanged for {label}.{parent}; saw {events}"
    );

    let owner_edge_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM discovery_edges edge \
         JOIN contract_instance_addresses target \
           ON target.contract_instance_id = edge.to_contract_instance_id \
          AND target.chain_id = edge.chain_id \
         WHERE edge.edge_kind = 'subregistry' \
           AND lower(target.address) = $1",
    )
    .bind(&owner_hex)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        owner_edge_count, 0,
        "legacy owner {owner_hex} must remain a leaf rather than a discovered registry"
    );

    Ok(())
}

async fn assert_prior_legacy_migrate_state(
    run: &support::PipelineRun,
    deployment: &ens_v1::EnsV1Deployment,
    owner: Address,
) -> Result<()> {
    let migrate_node = format!("{:#x}", ens_v1::namehash("migrate.eth"));
    let legacy_registry = format!("{:#x}", deployment.legacy_registry.address);
    let owner_hex = format!("{owner:#x}");
    let events = subregistry_events_for_child(run, &migrate_node).await?;
    let event_rows = events.as_array().cloned().unwrap_or_default();
    let found = event_rows.iter().any(|event| {
        event
            .pointer("/raw_fact_ref/emitting_address")
            .and_then(Value::as_str)
            == Some(legacy_registry.as_str())
            && event.pointer("/after_state/owner").and_then(Value::as_str)
                == Some(owner_hex.as_str())
    });
    assert!(
        found,
        "expected migrate.eth to have prior observed legacy owner state before current migration; saw {events}"
    );
    Ok(())
}

async fn assert_current_migrate_state(
    run: &support::PipelineRun,
    current_owner: Address,
    current_resolver: Address,
    suppressed_owner: Address,
    suppressed_resolver: Address,
) -> Result<()> {
    let current_owner = format!("{current_owner:#x}");
    let current_resolver = format!("{current_resolver:#x}");
    let suppressed_owner = format!("{suppressed_owner:#x}");
    let suppressed_resolver = format!("{suppressed_resolver:#x}");
    let migrate_node = format!("{:#x}", ens_v1::namehash("migrate.eth"));

    let migrate_events = sqlx::query_scalar::<_, Value>(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object( \
             'event_kind', event_kind, \
             'source_family', source_family, \
             'block_number', block_number, \
             'log_index', log_index, \
             'after_state', after_state) \
           ORDER BY block_number, log_index), '[]'::jsonb) \
         FROM normalized_events \
         WHERE (logical_name_id = 'ens:0x47841aea2cf37e6ff19afa53e6181c9ca34542bee4bc5201ea9936bb884412ac' OR after_state->>'node' = '0x47841aea2cf37e6ff19afa53e6181c9ca34542bee4bc5201ea9936bb884412ac') \
           AND source_family = 'ens_v1_registry_l1' \
           AND event_kind IN ('AuthorityTransferred', 'ResolverChanged') \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    let migrate_events_array = migrate_events.as_array().cloned().unwrap_or_default();
    let has_current_owner = migrate_events_array.iter().any(|event| {
        event.pointer("/event_kind").and_then(Value::as_str) == Some("AuthorityTransferred")
            && event.pointer("/after_state/owner").and_then(Value::as_str)
                == Some(current_owner.as_str())
    });
    let has_current_resolver = migrate_events_array.iter().any(|event| {
        event.pointer("/event_kind").and_then(Value::as_str) == Some("ResolverChanged")
            && event
                .pointer("/after_state/resolver")
                .and_then(Value::as_str)
                == Some(current_resolver.as_str())
    });
    assert!(
        has_current_owner && has_current_resolver,
        "expected current-registry owner and resolver events for migrate.eth; saw {migrate_events}"
    );

    let suppressed_events = sqlx::query_scalar::<_, Value>(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object( \
             'event_kind', event_kind, \
             'logical_name_id', logical_name_id, \
             'block_number', block_number, \
             'log_index', log_index, \
             'after_state', after_state, \
             'raw_fact_ref', raw_fact_ref) \
           ORDER BY block_number, log_index), '[]'::jsonb) \
         FROM normalized_events \
         WHERE source_family = 'ens_v1_registry_l1' \
           AND canonicality_state = 'canonical' \
           AND ( \
             (event_kind = 'ResolverChanged' \
              AND lower(after_state->>'resolver') = $1) \
             OR (event_kind = 'SubregistryChanged' \
              AND lower(after_state->>'child_node') = $2 \
              AND lower(after_state->>'owner') = $3) \
             OR (event_kind = 'AuthorityTransferred' \
              AND logical_name_id = 'ens:0x47841aea2cf37e6ff19afa53e6181c9ca34542bee4bc5201ea9936bb884412ac' \
              AND lower(after_state->>'owner') = $3) \
           )",
    )
    .bind(&suppressed_resolver)
    .bind(&migrate_node)
    .bind(&suppressed_owner)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        suppressed_events.as_array().is_some_and(Vec::is_empty),
        "later legacy resolver/owner writes for migrated migrate.eth should be suppressed; saw {suppressed_events}"
    );

    Ok(())
}

async fn assert_legacy_2ld_public_state(run: &support::PipelineRun, name: &str) -> Result<()> {
    assert_exact_name_not_minted(run, name).await?;

    let child_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM children_current WHERE namehash = $1")
            .bind(format!("{:#x}", ens_v1::namehash(name)))
            .fetch_one(&run.db.pool)
            .await?;
    assert_eq!(
        child_rows, 0,
        "legacy-only 2LD {name} should derive SubregistryChanged but no children_current row because eth has no exact parent surface"
    );

    let (status, body) = run.api.get_json("/v1/names/ens/eth/children").await?;
    assert_eq!(
        status, 404,
        "eth parent should not expose a children route without an exact parent surface; body: {body}"
    );

    Ok(())
}

async fn subregistry_events_for_child(
    run: &support::PipelineRun,
    child_node: &str,
) -> Result<Value> {
    sqlx::query_scalar::<_, Value>(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object( \
             'event_identity', event_identity, \
             'source_family', source_family, \
             'block_number', block_number, \
             'log_index', log_index, \
             'after_state', after_state, \
             'raw_fact_ref', raw_fact_ref) \
           ORDER BY block_number, log_index), '[]'::jsonb) \
         FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND lower(after_state->>'child_node') = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(child_node)
    .fetch_one(&run.db.pool)
    .await
    .with_context(|| {
        format!("failed to load SubregistryChanged events for child node {child_node}")
    })
}

fn assert_placeholder_child(children: &[Value], label: &str, parent: &str, owner: Address) {
    let labelhash = format!("{:#x}", ens_v1::labelhash(label));
    let namehash = format!("{:#x}", ens_v1::namehash(&format!("{label}.{parent}")));
    let owner = format!("{owner:#x}");
    let Some(child) = children
        .iter()
        .find(|child| child.get("labelhash").and_then(Value::as_str) == Some(labelhash.as_str()))
    else {
        panic!(
            "expected placeholder child {label}.{parent} with labelhash {labelhash}; saw {children:?}"
        );
    };
    assert_eq!(
        child.get("namehash").and_then(Value::as_str),
        Some(namehash.as_str()),
        "placeholder child namehash mismatch for {label}.{parent}: {child}"
    );
    assert_eq!(
        child.get("owner").and_then(Value::as_str),
        Some(owner.as_str()),
        "placeholder child owner mismatch for {label}.{parent}: {child}"
    );
    let normalized_name = child
        .get("normalized_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        normalized_name.starts_with('[') && normalized_name.ends_with(parent),
        "expected bracketed placeholder normalized_name for {label}.{parent}; child: {child}"
    );
}

async fn assert_exact_name_not_minted(run: &support::PipelineRun, name: &str) -> Result<()> {
    let (status, body) = run.api.get_json(&format!("/v1/names/ens/{name}")).await?;
    assert_eq!(
        status, 404,
        "raw registry setSubnodeOwner should not mint exact-name surface for {name}; body: {body}"
    );
    Ok(())
}
