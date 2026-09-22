//! Plan-shape tests for the address history route (`GET /v1/addresses/{address}/history`, and
//! `GET /v1/names/{name}/history?scope=both`, which reads through the same selector). The
//! fixture is small, so `enable_seqscan` is off to stand in for a large `normalized_events`: the
//! assertions are about which access paths the planner can use at all, not about costs.

use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::super::attribution::{
    push_empty_mirror_writes_for_test, push_exact_node_mirror_writes_for_test,
    push_pointer_window_attribution_for_test,
};
use super::super::{
    EventHistoryReadFilter,
    columns::push_history_select,
    duplicates::push_product_history_duplicate_filter,
    paging::{push_history_filters, push_history_order},
    selectors::HistorySelector,
    summary::push_history_count_query,
};
use super::push_historical_address_matches_query;
use crate::address_names::push_address_names_current_query;

const TARGET: &str = "0x0000000000000000000000000000000000000a11";
const TARGET_RESOLVER: &str = "0x0000000000000000000000000000000000000c11";
const UNRELATED_NAMES: i64 = 300;
/// The target's rows: a registrant grant and a registry owner transfer on its name, a token
/// transfer on its resource with no name, a resolver pointer on both, and one node-keyed record
/// write that pointer attributes to its resource.
const TARGET_ROWS: i64 = 5;
const ANCHOR_INDEXES: [&str; 3] = [
    "normalized_events_address_registrant_match_idx",
    "normalized_events_address_token_holder_match_idx",
    "normalized_events_address_registry_owner_match_idx",
];
const SELECTOR_INDEXES: [&str; 3] = [
    "normalized_events_name_history_idx",
    "normalized_events_resource_history_idx",
    "normalized_events_pkey",
];

#[tokio::test]
async fn address_history_selector_plans_use_history_indexes() -> Result<()> {
    with_fixture("address_history_selector_plan", check_selector_plans).await
}

#[tokio::test]
async fn address_history_anchor_plan_uses_address_match_indexes() -> Result<()> {
    with_fixture("address_history_anchor_plan", check_anchor_plan).await
}

#[tokio::test]
async fn bounded_record_attribution_plans_do_not_scan_normalized_events() -> Result<()> {
    with_fixture("bounded_attribution_plan", check_attribution_plans).await
}

#[tokio::test]
async fn bounded_mirror_registry_probe_uses_the_addressed_node_index() -> Result<()> {
    with_fixture(
        "bounded_mirror_registry_plan",
        check_mirror_registry_probe_plan,
    )
    .await
}

#[tokio::test]
async fn bounded_current_relation_plan_probes_the_cited_resource() -> Result<()> {
    with_fixture(
        "bounded_current_relation_plan",
        check_bounded_current_relation_plan,
    )
    .await
}

async fn with_fixture(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_fixture(&mut connection).await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// The count and the page read the selector the anchors produce: the target's name and
/// resource, as load_address_history_selector builds them for `scope=both`.
async fn check_selector_plans(connection: &mut PgConnection) -> Result<()> {
    let filter = EventHistoryReadFilter {
        selectors: vec![HistorySelector::logical_names_or_resources(
            vec![target_name()],
            vec![target_resource()],
        )],
        ..EventHistoryReadFilter::default()
    }
    .with_attributed_records(&mut *connection)
    .await?;
    ensure!(
        filter.attributed_records.event_ids().len() == 1,
        "the target's pointer must attribute its record write: {:?}",
        filter.attributed_records
    );
    // Plan failures are collected so one run reports every statement that regressed.
    let mut plan_failures = Vec::new();

    let push_count = |builder: &mut _| {
        push_history_count_query(builder, &filter, true, Some(10_001));
    };
    for plan in explain_both(connection, push_count).await? {
        if let Err(error) = assert_selector_uses_history_indexes("count", &plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut count = QueryBuilder::<Postgres>::new("");
    push_count(&mut count);
    let total: i64 = count
        .build_query_scalar()
        .fetch_one(&mut *connection)
        .await?;
    ensure!(total == TARGET_ROWS, "count returned {total}");

    let push_page = |builder: &mut _| {
        push_history_select(builder, &filter, true, false, false);
        push_history_filters(builder, &filter, true);
        push_product_history_duplicate_filter(builder, &filter, true);
        push_history_order(builder, filter.order);
        builder.push(" LIMIT ");
        builder.push_bind(51_i64);
    };
    for plan in explain_both(connection, push_page).await? {
        if let Err(error) = assert_selector_uses_history_indexes("page", &plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut page = QueryBuilder::<Postgres>::new("");
    push_page(&mut page);
    let rows = page.build().fetch_all(&mut *connection).await?;
    let identities = rows
        .iter()
        .map(|row| row.try_get::<String, _>("event_identity"))
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        identities
            == [
                "target:pointer",
                "target:record",
                "target:owner",
                "target:transfer",
                "target:grant"
            ],
        "page returned {identities:?}"
    );
    ensure!(
        plan_failures.is_empty(),
        "address history plans:\n{}",
        plan_failures.join("\n\n")
    );
    Ok(())
}

/// The anchor lookup must reach the target's rows through the three address-match indexes,
/// not by reading every grant and transfer, and must find the target's name and resource.
async fn check_anchor_plan(connection: &mut PgConnection) -> Result<()> {
    let push_anchors = |builder: &mut _| {
        push_historical_address_matches_query(builder, TARGET, None, None, true, false, None);
    };
    let mut plan_failures = Vec::new();
    for plan in explain_both(connection, push_anchors).await? {
        if let Err(error) = assert_anchor_uses_address_match_indexes(&plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut anchors = QueryBuilder::<Postgres>::new("");
    push_anchors(&mut anchors);
    let rows = anchors.build().fetch_all(&mut *connection).await?;
    let anchors = rows
        .iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect::<Result<BTreeSet<(Option<String>, Option<Uuid>)>>>()?;
    let expected = BTreeSet::from([
        (Some(target_name()), Some(target_resource())),
        (None, Some(target_resource())),
        (Some(target_name()), None),
    ]);
    ensure!(anchors == expected, "anchor lookup returned {anchors:?}");
    ensure!(
        plan_failures.is_empty(),
        "address history anchor plans:\n{}",
        plan_failures.join("\n\n")
    );
    Ok(())
}

/// The bounded attribution reader's two statements: the pointer-window attribution for the
/// target's resource, and the mirror substitution with an empty walk (no mirror pointer, the
/// common case on Mainnet). Neither may read `normalized_events` sequentially. The ENSv2
/// declared-resolver arm and the `ResolverRecordLinked` scan have no dedicated index
/// (docs/storage.md), so this checks only that each read is an index read. The mirror registry
/// lookup, which an empty walk never runs, is checked by `check_mirror_registry_probe_plan`.
async fn check_attribution_plans(connection: &mut PgConnection) -> Result<()> {
    let resource_ids: &'static [Uuid] = Box::leak(Box::new([target_resource()]));
    let published =
        std::collections::BTreeMap::from([("ethereum-mainnet".to_owned(), UNRELATED_NAMES + 10)]);
    let mut plan_failures = Vec::new();
    let push_attribution = |builder: &mut QueryBuilder<'static, Postgres>| {
        push_pointer_window_attribution_for_test(builder, resource_ids, Some(&published));
    };
    for plan in explain_both(connection, push_attribution).await? {
        if let Err(error) = assert_no_event_seq_scan("attribution", &plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut attribution = QueryBuilder::<Postgres>::new("");
    push_attribution(&mut attribution);
    let rows = attribution.build().fetch_all(&mut *connection).await?;
    ensure!(rows.len() == 1, "attribution returned {} rows", rows.len());

    let push_mirror = |builder: &mut QueryBuilder<'static, Postgres>| {
        push_empty_mirror_writes_for_test(builder, Some(&published));
    };
    for plan in explain_both(connection, push_mirror).await? {
        if let Err(error) = assert_no_event_seq_scan("mirror", &plan) {
            plan_failures.push(error.to_string());
        }
    }
    ensure!(
        plan_failures.is_empty(),
        "bounded attribution plans:\n{}",
        plan_failures.join("\n\n")
    );
    Ok(())
}

/// The mirror substitution with a walk over the target's own node, so the ENSv1 registry pointer
/// lookup runs. That lookup keys each `ResolverChanged` by the node it addresses (`child_node`,
/// then `namehash`, then `node`) and must read it through
/// `normalized_events_project_v1_pointer_addressed_node_idx` in both the generic and the bound
/// plan (docs/storage.md), not through a broader index filtered by that expression.
async fn check_mirror_registry_probe_plan(connection: &mut PgConnection) -> Result<()> {
    const INDEX: &str = "normalized_events_project_v1_pointer_addressed_node_idx";
    fn registry_reads<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
        // The planner renames the lateral subquery's scan (`registry_1`).
        if node["Relation Name"] == "normalized_events"
            && node["Alias"]
                .as_str()
                .is_some_and(|alias| alias.starts_with("registry"))
        {
            output.push(node);
        }
        for child in node["Plans"].as_array().into_iter().flatten() {
            registry_reads(child, output);
        }
    }
    let published =
        std::collections::BTreeMap::from([("ethereum-mainnet".to_owned(), UNRELATED_NAMES + 10)]);
    let target_hash = format!("0x{:064x}", 0xa11);
    let push_mirror = |builder: &mut QueryBuilder<'static, Postgres>| {
        push_exact_node_mirror_writes_for_test(
            builder,
            target_resource(),
            &target_hash,
            &["target", "eth"],
            Some(&published),
        );
    };
    let mut plan_failures = Vec::new();
    for plan in explain_both(connection, push_mirror).await? {
        let mut reads = Vec::new();
        registry_reads(&plan[0]["Plan"], &mut reads);
        let keyed = !reads.is_empty()
            && reads.iter().all(|read| {
                let mut indexes = read["Index Name"].as_str().into_iter().collect::<Vec<_>>();
                bitmap_index_names(read, &mut indexes);
                indexes == [INDEX]
            });
        if !keyed {
            plan_failures.push(format!("registry probe does not use {INDEX}: {plan}"));
        }
        if let Err(error) = assert_no_event_seq_scan("mirror registry", &plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut mirror = QueryBuilder::<Postgres>::new("");
    push_mirror(&mut mirror);
    mirror.build().fetch_all(&mut *connection).await?;
    ensure!(
        plan_failures.is_empty(),
        "mirror registry plans:\n{}",
        plan_failures.join("\n\n")
    );
    Ok(())
}

/// The bounded current-row read (`load_address_names_current_at_bound`) for a token-holder row
/// that Project cites at a self-transfer above the bound. The name is granted to the target below
/// the bound and transferred from the target to itself twice above it, while most of the chain's
/// events lie above the bound too. The cited event is read by primary key and the range between
/// the bound and it from the resource history index; no read of `normalized_events` is
/// sequential, the attachment probe reads `surface_bindings` by name, and the row is admitted.
/// Both read paths are checked: the canonical read and the read that includes noncanonical
/// identity rows.
async fn check_bounded_current_relation_plan(connection: &mut PgConnection) -> Result<()> {
    let bound = 10;
    let cited = UNRELATED_NAMES + 7;
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
             block_number, canonicality_state)
        VALUES ('ens:{held_hash}', 'ens', 'held.eth', ARRAY['held', 'eth'], '\x00'::bytea,
                '{held_hash}', ARRAY['{held_hash}', 'eth'], 'test', 'active',
                'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state);

        INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
        VALUES ('{held_resource}', 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state);

        INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
             active_from, chain_id, block_hash, block_number, canonicality_state)
        VALUES ('{binding}', 'ens:{held_hash}', '{held_resource}', 'declared_registry_path',
                'ens_v2', to_timestamp(1), 'ethereum-mainnet', 'block-1', 1,
                'canonical'::canonicality_state);

        INSERT INTO normalized_events
            (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family,
             manifest_version, chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, before_state,
             after_state)
        SELECT identity, 'ens', 'ens:{held_hash}', '{held_resource}'::uuid, kind,
               'ens_v2_registry_l1', 1, 'ethereum-mainnet', 'block-' || block, block,
               'tx-' || identity, 0, 0, 'ens_v2_registry_resource_surface',
               'canonical'::canonicality_state, before, after
        FROM (VALUES
            ('held:grant', 'RegistrationGranted', 5, '{{}}'::jsonb,
             jsonb_build_object('registrant', '{TARGET}')),
            ('held:self-1', 'TokenControlTransferred', 200,
             jsonb_build_object('from', '{TARGET}'), jsonb_build_object('to', '{TARGET}')),
            ('held:self-2', 'TokenControlTransferred', {cited},
             jsonb_build_object('from', '{TARGET}'), jsonb_build_object('to', '{TARGET}'))
        ) held(identity, kind, block, before, after);

        INSERT INTO address_names_current
            (address, logical_name_id, relation, namespace, raw_name, namehash,
             surface_binding_id, resource_id, binding_kind, support_status, provenance,
             chain_positions, canonicality_summary, manifest_version)
        SELECT '{TARGET}', 'ens:{held_hash}', 'token_holder', 'ens', 'held.eth', '{held_hash}',
               '{binding}', '{held_resource}', 'declared_registry_path', 'supported',
               jsonb_build_object('chain_id', 'ethereum-mainnet',
                                  'normalized_event_id', normalized_event_id),
               jsonb_build_object('block_number', {cited}, 'block_hash', 'block-{cited}',
                                  'target_block_number', {cited},
                                  'target_block_hash', 'block-{cited}'),
               jsonb_build_object('state', 'canonical_lineage'), 1
        FROM normalized_events
        WHERE event_identity = 'held:self-2';

        ANALYZE;
        "#,
        held_hash = format!("0x{:064x}", 0xa12),
        held_resource = Uuid::from_u128(0xa12),
        binding = Uuid::from_u128(0xa12b),
    ))
    .execute(&mut *connection)
    .await?;
    let published: &'static std::collections::BTreeMap<String, i64> = Box::leak(Box::new(
        std::collections::BTreeMap::from([("ethereum-mainnet".to_owned(), bound)]),
    ));
    let mut plan_failures = Vec::new();
    // Both read paths: the canonical read with its identity joins and the read that includes
    // noncanonical identity rows, which has none.
    for include_noncanonical in [false, true] {
        let push_current = move |builder: &mut QueryBuilder<'static, Postgres>| {
            push_address_names_current_query(
                builder,
                TARGET,
                None,
                None,
                include_noncanonical,
                Some(published),
            );
        };
        for plan in explain_both(connection, push_current).await? {
            if let Err(error) = assert_cited_probe_is_keyed(&plan)
                .and_then(|()| assert_attachment_probe_is_keyed(&plan))
            {
                plan_failures.push(format!(
                    "include_noncanonical={include_noncanonical}: {error}"
                ));
            }
        }
        let mut current = QueryBuilder::<Postgres>::new("");
        push_current(&mut current);
        let rows = current.build().fetch_all(&mut *connection).await?;
        ensure!(
            rows.len() == 1,
            "include_noncanonical={include_noncanonical}: the row held at the bound returned {} \
             rows",
            rows.len()
        );
    }
    ensure!(
        plan_failures.is_empty(),
        "bounded current relation plans:\n{}",
        plan_failures.join("\n\n")
    );
    Ok(())
}

/// The attachment probe reads `surface_bindings` through an index keyed by the name or the
/// resource, never sequentially. `surface_bindings_no_overlap` is the canonical bindings'
/// exclusion index, keyed by chain and name first.
fn assert_attachment_probe_is_keyed(plan: &Value) -> Result<()> {
    fn walk<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
        output.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, output);
        }
    }
    let mut nodes = Vec::new();
    walk(&plan[0]["Plan"], &mut nodes);
    let mut indexes = Vec::new();
    for node in nodes
        .iter()
        .filter(|node| node["Relation Name"] == "surface_bindings" && node["Alias"] == "attached")
    {
        ensure!(
            node["Node Type"] != "Seq Scan",
            "the attachment probe reads surface_bindings sequentially: {plan}"
        );
        indexes.extend(node["Index Name"].as_str());
        bitmap_index_names(node, &mut indexes);
    }
    ensure!(
        !indexes.is_empty()
            && indexes.iter().all(|index| {
                matches!(
                    *index,
                    "surface_bindings_name_idx"
                        | "surface_bindings_no_overlap"
                        | "surface_bindings_chain_name_history_idx"
                        | "surface_bindings_resource_idx"
                )
            }),
        "the attachment probe is not keyed by name or resource ({indexes:?}): {plan}"
    );
    Ok(())
}

/// The cited event is read through the primary key, the range through the resource history
/// index, and nothing reads `normalized_events` sequentially.
fn assert_cited_probe_is_keyed(plan: &Value) -> Result<()> {
    assert_no_event_seq_scan("bounded current relation", plan)?;
    fn walk<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
        output.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, output);
        }
    }
    let mut nodes = Vec::new();
    walk(&plan[0]["Plan"], &mut nodes);
    let indexes_for = |alias: &str| {
        let mut indexes = Vec::new();
        for node in nodes
            .iter()
            .filter(|node| node["Relation Name"] == "normalized_events" && node["Alias"] == alias)
        {
            indexes.extend(node["Index Name"].as_str());
            bitmap_index_names(node, &mut indexes);
        }
        indexes
    };
    let cited = indexes_for("cited");
    ensure!(
        cited == ["normalized_events_pkey"],
        "the cited event is not read by primary key ({cited:?}): {plan}"
    );
    let moved = indexes_for("moved");
    ensure!(
        moved == ["normalized_events_resource_history_idx"],
        "the range probe does not use the resource history index ({moved:?}): {plan}"
    );
    Ok(())
}

/// Every read of `normalized_events`, sub-plans included, is an index read.
fn assert_no_event_seq_scan(statement: &str, plan: &Value) -> Result<()> {
    fn walk<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
        output.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, output);
        }
    }
    let mut nodes = Vec::new();
    walk(&plan[0]["Plan"], &mut nodes);
    let event_reads = nodes
        .iter()
        .filter(|node| node["Relation Name"] == "normalized_events")
        .map(|node| {
            let mut indexes = node["Index Name"].as_str().into_iter().collect::<Vec<_>>();
            bitmap_index_names(node, &mut indexes);
            format!(
                "{} {} {}",
                node["Alias"].as_str().unwrap_or("?"),
                node["Node Type"].as_str().unwrap_or("?"),
                indexes.join("+"),
            )
        })
        .collect::<Vec<_>>();
    for node in nodes {
        ensure!(
            node["Relation Name"] != "normalized_events" || node["Node Type"] != "Seq Scan",
            "{statement} reads normalized_events sequentially ({event_reads:?}): {node}\n{plan}"
        );
    }
    Ok(())
}

fn assert_anchor_uses_address_match_indexes(plan: &Value) -> Result<()> {
    let nodes = main_plan_nodes(plan);
    let indexes = index_names(&nodes);
    // The keyed-scan check runs first so it alone catches a regressed shape.
    assert_no_unkeyed_event_scan("anchor", &nodes, plan, &ANCHOR_INDEXES)?;
    for expected in ANCHOR_INDEXES {
        ensure!(
            indexes.contains(&expected),
            "anchor lookup does not use {expected}: {plan}"
        );
    }
    Ok(())
}

/// The selector ORs names, resources and attributed record writes. Every branch must be an
/// index condition, so the planner combines the name history index, the resource history
/// index and the primary key instead of filtering a full scan.
fn assert_selector_uses_history_indexes(statement: &str, plan: &Value) -> Result<()> {
    let nodes = main_plan_nodes(plan);
    let indexes = index_names(&nodes);
    // The keyed-scan check runs first so it alone catches a regressed shape.
    assert_no_unkeyed_event_scan(statement, &nodes, plan, &SELECTOR_INDEXES)?;
    for expected in SELECTOR_INDEXES {
        ensure!(
            indexes.contains(&expected),
            "{statement} does not use {expected}: {plan}"
        );
    }
    ensure!(
        !indexes.contains(&"normalized_events_projection_idx"),
        "{statement} scans normalized_events_projection_idx: {plan}"
    );
    Ok(())
}

/// Every read of `ne` must be keyed by the statement's own indexes. A bitmap heap scan counts
/// only when every index under it (through BitmapAnd and BitmapOr) is one of `keyed_indexes`, or
/// its recheck condition compares a row key; a bitmap scan of an unrelated index with the
/// selector left as a filter is still a scan of every candidate row.
fn assert_no_unkeyed_event_scan(
    statement: &str,
    nodes: &[&Value],
    plan: &Value,
    keyed_indexes: &[&str],
) -> Result<()> {
    const ROW_KEYS: [&str; 3] = ["normalized_event_id", "logical_name_id", "resource_id"];
    let references_row_key = |condition: &Value| {
        condition
            .as_str()
            .is_some_and(|condition| ROW_KEYS.iter().any(|key| condition.contains(key)))
    };
    for node in nodes {
        if node["Relation Name"] != "normalized_events" || node["Alias"] != "ne" {
            continue;
        }
        let keyed = if node["Node Type"] == "Bitmap Heap Scan" {
            let mut bitmap_indexes = Vec::new();
            bitmap_index_names(node, &mut bitmap_indexes);
            (!bitmap_indexes.is_empty()
                && bitmap_indexes
                    .iter()
                    .all(|index| keyed_indexes.contains(index)))
                || references_row_key(&node["Recheck Cond"])
        } else {
            node.get("Index Cond").is_some()
                && (node["Index Name"]
                    .as_str()
                    .is_some_and(|index| keyed_indexes.contains(&index))
                    || references_row_key(&node["Index Cond"]))
        };
        ensure!(
            keyed,
            "{statement} reads normalized_events without an index key: {node}\n{plan}"
        );
    }
    Ok(())
}

fn bitmap_index_names<'a>(node: &'a Value, output: &mut Vec<&'a str>) {
    for child in node["Plans"].as_array().into_iter().flatten() {
        match child["Node Type"].as_str() {
            Some("Bitmap Index Scan") => output.extend(child["Index Name"].as_str()),
            Some("BitmapAnd" | "BitmapOr") => bitmap_index_names(child, output),
            _ => {}
        }
    }
}

/// The reverted `IN (SELECT ...)` selector can plan as a bitmap heap scan of an unrelated index
/// with the selector as a hashed sub-plan filter; that is not keyed. A bitmap OR of the
/// selector's own indexes is.
#[test]
fn bitmap_heap_scan_is_keyed_only_by_the_statement_indexes() {
    let scan = |bitmap: Value, recheck: &str| {
        serde_json::json!([{"Plan": {
            "Node Type": "Bitmap Heap Scan",
            "Relation Name": "normalized_events",
            "Alias": "ne",
            "Recheck Cond": recheck,
            "Filter": "((logical_name_id = ANY ($1)) OR (hashed SubPlan 1))",
            "Plans": [bitmap],
        }}])
    };
    let index =
        |name: &str| serde_json::json!({"Node Type": "Bitmap Index Scan", "Index Name": name});
    let unkeyed = scan(
        index("normalized_events_projection_idx"),
        "(canonicality_state = ANY ('{canonical,safe,finalized}'))",
    );
    let nodes = main_plan_nodes(&unkeyed);
    assert!(assert_no_unkeyed_event_scan("probe", &nodes, &unkeyed, &SELECTOR_INDEXES).is_err());

    let keyed = scan(
        serde_json::json!({"Node Type": "BitmapOr", "Plans": [
            index("normalized_events_name_history_idx"),
            index("normalized_events_resource_history_idx"),
            index("normalized_events_pkey"),
        ]}),
        "((logical_name_id = ANY ($1)) OR (resource_id = ANY ($2)))",
    );
    let nodes = main_plan_nodes(&keyed);
    assert!(assert_no_unkeyed_event_scan("probe", &nodes, &keyed, &SELECTOR_INDEXES).is_ok());
}

/// Plan the statement twice: as PostgreSQL's generic prepared-statement plan, which sqlx's
/// statement cache can reach, and with the values bound.
async fn explain_both<'a>(
    connection: &mut PgConnection,
    push: impl Fn(&mut QueryBuilder<'a, Postgres>),
) -> Result<[Value; 2]> {
    let mut plain = QueryBuilder::<Postgres>::new("");
    push(&mut plain);
    let generic_sql = format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {}", plain.sql());
    let generic = sqlx::raw_sql(&generic_sql)
        .fetch_one(&mut *connection)
        .await
        .context("generic plan")?
        .try_get::<Value, _>(0)?;
    let mut bound = QueryBuilder::<Postgres>::new("EXPLAIN (FORMAT JSON) ");
    push(&mut bound);
    let bound = bound
        .build_query_scalar::<Value>()
        .fetch_one(&mut *connection)
        .await
        .context("bound plan")?;
    Ok([generic, bound])
}

/// Every node outside correlated sub-plans; those run per output row and are keyed there.
fn main_plan_nodes(plan: &Value) -> Vec<&Value> {
    fn walk<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
        if node["Parent Relationship"] == "SubPlan" {
            return;
        }
        output.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, output);
        }
    }
    let mut output = Vec::new();
    walk(&plan[0]["Plan"], &mut output);
    output
}

fn index_names<'a>(nodes: &[&'a Value]) -> Vec<&'a str> {
    nodes
        .iter()
        .filter_map(|node| node["Index Name"].as_str())
        .collect()
}

fn target_name() -> String {
    format!("ens:0x{:064x}", 0xa11)
}

fn target_resource() -> Uuid {
    Uuid::from_u128(0xa11)
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    // Unrelated names each hold a resource and one grant, token transfer or registry owner
    // transfer to another address, plus a resolver pointer and a node-keyed record write that
    // pointer attributes to their resource. The target (name 0xa11, resource 0xa11) holds
    // TARGET_ROWS rows.
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'::canonicality_state
        FROM generate_series(1, {blocks}) n;

        INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
             block_number, canonicality_state)
        SELECT 'ens:' || hash, 'ens', 'name-' || n || '.eth', ARRAY['name-' || n, 'eth'],
               '\x00'::bytea, hash, ARRAY[hash, 'eth'], 'test', 'active',
               'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
        FROM generate_series(1, {names}) n,
             LATERAL (SELECT '0x' || lpad(to_hex(n), 64, '0') AS hash) h
        UNION ALL
        SELECT '{target_name}', 'ens', 'target.eth', ARRAY['target', 'eth'], '\x00'::bytea,
               '{target_hash}', ARRAY['{target_hash}', 'eth'], 'test', 'active',
               'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state;

        INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
        SELECT lpad(to_hex(n), 32, '0')::uuid, 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
        FROM generate_series(1, {names}) n
        UNION ALL
        SELECT '{target_resource}', 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state;

        INSERT INTO normalized_events
            (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family,
             manifest_version, chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, after_state)
        SELECT 'unrelated:' || n, 'ens',
               'ens:0x' || lpad(to_hex(n), 64, '0'), lpad(to_hex(n), 32, '0')::uuid,
               (ARRAY['RegistrationGranted', 'TokenControlTransferred',
                      'AuthorityTransferred'])[1 + n % 3],
               'ens_v2_registry_l1', 1, 'ethereum-mainnet', 'block-' || n, n, 'tx-' || n,
               0, 0, 'ens_v2_registry_resource_surface', 'canonical'::canonicality_state,
               jsonb_build_object('registrant', holder, 'to', holder, 'owner', holder)
        FROM generate_series(1, {names}) n,
             LATERAL (SELECT '0x' || lpad(to_hex(n), 40, '0') AS holder) h
        UNION ALL
        SELECT 'unrelated:record:' || n, 'ens', NULL, NULL, 'RecordChanged',
               'ens_v1_resolver_l1', 1, 'ethereum-mainnet', 'block-' || n, n,
               'tx-record-' || n, 0, 0, 'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
               jsonb_build_object('node', '0x' || lpad(to_hex(n), 64, '0'),
                                  'resolver', '0x' || lpad(to_hex(n), 40, 'e'))
        FROM generate_series(1, {names}) n
        UNION ALL
        SELECT 'unrelated:pointer:' || n, 'ens', 'ens:0x' || lpad(to_hex(n), 64, '0'),
               lpad(to_hex(n), 32, '0')::uuid, 'ResolverChanged', 'ens_v1_registry_l1', 1,
               'ethereum-mainnet', 'block-' || n, n, 'tx-pointer-' || n, 0, 0,
               'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
               jsonb_build_object('node', '0x' || lpad(to_hex(n), 64, '0'),
                                  'resolver', '0x' || lpad(to_hex(n), 40, 'e'))
        FROM generate_series(1, {names}) n
        UNION ALL
        SELECT identity, 'ens', name, resource, kind, 'ens_v2_registry_l1', 1,
               'ethereum-mainnet', 'block-' || block, block, 'tx-' || identity, 0, 0,
               'ens_v2_registry_resource_surface', 'canonical'::canonicality_state, state
        FROM (VALUES
            ('target:grant', '{target_name}', '{target_resource}'::uuid,
             'RegistrationGranted', {first}, jsonb_build_object('registrant', upper('{TARGET}'))),
            ('target:transfer', NULL, '{target_resource}'::uuid,
             'TokenControlTransferred', {second}, jsonb_build_object('to', '{TARGET}')),
            ('target:owner', '{target_name}', NULL,
             'AuthorityTransferred', {third}, jsonb_build_object('owner', '{TARGET}'))
        ) target(identity, name, resource, kind, block, state)
        UNION ALL
        SELECT 'target:record', 'ens', NULL, NULL, 'RecordChanged', 'ens_v1_resolver_l1', 1,
               'ethereum-mainnet', 'block-{fourth}', {fourth}, 'tx-target-record', 0, 0,
               'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
               jsonb_build_object('node', '{target_hash}', 'resolver', '{TARGET_RESOLVER}')
        UNION ALL
        SELECT 'target:pointer', 'ens', '{target_name}', '{target_resource}'::uuid,
               'ResolverChanged', 'ens_v1_registry_l1', 1, 'ethereum-mainnet',
               'block-{fifth}', {fifth}, 'tx-target-pointer', 0, 0,
               'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
               jsonb_build_object('node', '{target_hash}', 'resolver', '{TARGET_RESOLVER}');

        ANALYZE;
        SET enable_seqscan = off;
        SET jit = off;
        "#,
        blocks = UNRELATED_NAMES + 10,
        names = UNRELATED_NAMES,
        target_name = target_name(),
        target_hash = format!("0x{:064x}", 0xa11),
        target_resource = target_resource(),
        first = UNRELATED_NAMES + 1,
        second = UNRELATED_NAMES + 2,
        third = UNRELATED_NAMES + 3,
        fourth = UNRELATED_NAMES + 4,
        fifth = UNRELATED_NAMES + 5,
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
