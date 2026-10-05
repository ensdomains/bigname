//! Disposable physical-layout screen. Compare the previous and revised production builders
//! on identical retained facts, then inspect their real custom and generic access paths.

use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{Connection, Execute, PgConnection, Postgres, QueryBuilder, Row};

use super::{AddressRead, catalogue_count, catalogue_source};
use crate::history::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryOrder, HistoryScope,
};

#[tokio::test]
#[ignore = "manual read-only comparison of the preserved and candidate catalogue layouts"]
async fn address_history_catalogue_layout_screen() -> Result<()> {
    let mut candidate =
        PgConnection::connect(&std::env::var("BIGNAME_HISTORY_LAYOUT_DATABASE_URL")?).await?;
    let mut original =
        PgConnection::connect(&std::env::var("BIGNAME_HISTORY_ORIGINAL_DATABASE_URL")?).await?;
    let directory = std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR")?;
    let directory = Path::new(&directory);
    std::fs::create_dir_all(directory)?;
    for connection in [&mut candidate, &mut original] {
        sqlx::raw_sql("SET search_path=bigname_phase,public; SET jit=off; SET statement_timeout='120s'; BEGIN READ ONLY")
            .execute(connection).await?;
    }
    let block: i64 = sqlx::query_scalar(
        "SELECT block_number FROM project_history_catalogue_marker WHERE chain_id='ethereum-mainnet'",
    ).fetch_one(&mut candidate).await?;
    let published = BTreeMap::from([("ethereum-mainnet".to_owned(), block)]);
    let relations = [crate::AddressNameRelation::TokenHolder];
    let mut receipts = Vec::new();
    for namespace in [Some("ens"), None] {
        let read = AddressRead {
            address: "0x000000000000000000000000000000000000a235",
            namespace,
            relations: Some(&relations),
            scope: HistoryScope::Both,
            canonical_only: true,
            published: Some(&published),
            catalogue: true,
        };
        for order in [HistoryOrder::Asc, HistoryOrder::Desc] {
            let filter = retained_filter(block, order);
            let case = format!(
                "{}-{}",
                namespace.unwrap_or("all-namespaces"),
                order.as_str()
            );
            let mut query = QueryBuilder::<Postgres>::new("");
            catalogue_source::push_seek_query(&mut query, &read, &filter, None);
            let initial = compare_query(
                &mut candidate,
                Some(&mut original),
                Some(directory),
                &format!("{case}-seek-initial"),
                query,
                true,
            )
            .await?;
            let bucket = initial[0][if order == HistoryOrder::Asc {
                "min"
            } else {
                "max"
            }]
            .as_i64()
            .context("retained catalogue has an initial bucket")?;
            let mut query = QueryBuilder::<Postgres>::new("");
            catalogue_source::push_seek_query(&mut query, &read, &filter, Some(bucket));
            compare_query(
                &mut candidate,
                Some(&mut original),
                Some(directory),
                &format!("{case}-seek-next"),
                query,
                true,
            )
            .await?;
            let mut query = QueryBuilder::<Postgres>::new("");
            catalogue_source::push_candidate_query(
                &mut query, &read, &filter, None, bucket, None, 2,
            );
            let page = compare_query(
                &mut candidate,
                Some(&mut original),
                Some(directory),
                &format!("{case}-page-prefix"),
                query,
                true,
            )
            .await?;
            ensure!(
                !page.as_array().context("page result array")?.is_empty(),
                "retained page is empty"
            );
            receipts.push(json!({"case":case, "initial_bucket":bucket, "page_witnesses":page.as_array().unwrap().len()}));
        }
        let filter = retained_filter(block, HistoryOrder::Desc);
        let case = namespace.unwrap_or("all-namespaces");
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_count::push_names_query(&mut query, &read, &filter, None);
        let names = compare_query(
            &mut candidate,
            Some(&mut original),
            Some(directory),
            &format!("{case}-proof-names-first"),
            query,
            true,
        )
        .await?;
        let names: Vec<String> = names
            .as_array()
            .context("proof result array")?
            .iter()
            .map(|row| row["anchor_id"].as_str().unwrap().to_owned())
            .collect();
        ensure!(
            names.len() == 256,
            "fixed proof batch must contain 256 names"
        );
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_count::push_names_query(
            &mut query,
            &read,
            &filter,
            names.last().map(String::as_str),
        );
        compare_query(
            &mut candidate,
            Some(&mut original),
            Some(directory),
            &format!("{case}-proof-names-next"),
            query,
            true,
        )
        .await?;
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_count::push_proof_query(&mut query, &read, &filter, &names, 10_001);
        let proof = compare_query(
            &mut candidate,
            Some(&mut original),
            Some(directory),
            &format!("{case}-proof-events"),
            query,
            true,
        )
        .await?;
        ensure!(
            proof[0]["count"].as_i64().is_some_and(|n| n > 0),
            "proof must witness direct events"
        );
        // The retained fixture has no handoffs. Its large-table generic plan still checks
        // the membership lookup; the small SQL fixture below supplies actual peer rows.
        let groups = json!([{"chain":"ethereum-mainnet", "block":1,
            "hash":"perf-1", "node":"layout-node", "origin":"layout-handoff"}]);
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_handoff_query(&mut query, &read, &filter, &groups);
        let peers = compare_query(
            &mut candidate,
            Some(&mut original),
            Some(directory),
            &format!("{case}-handoff-membership"),
            query,
            true,
        )
        .await?;
        ensure!(peers == json!([]), "the retained fixture has no handoffs");
    }
    std::fs::write(
        directory.join("cases.json"),
        serde_json::to_vec_pretty(&receipts)?,
    )?;
    for connection in [&mut candidate, &mut original] {
        sqlx::raw_sql("ROLLBACK").execute(connection).await?;
    }
    let mut failures = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("-check.json"))
        {
            let check: Value = serde_json::from_slice(&std::fs::read(path)?)?;
            if check["accepted"] == false {
                failures.push(check);
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "bounded layout plan failures: {}",
        json!(failures)
    );
    Ok(())
}

fn retained_filter(block: i64, order: HistoryOrder) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        order,
        event_kinds: [
            "AuthorityEpochChanged",
            "AuthorityTransferred",
            "EACRolesChanged",
            "ExpiryChanged",
            "LabelRegistered",
            "MigrationApplied",
            "PermissionChanged",
            "PermissionScopeChanged",
            "RecordChanged",
            "RecordVersionChanged",
            "RegistrationGranted",
            "RegistrationReleased",
            "RegistrationRenewed",
            "ResolverChanged",
            "ReverseChanged",
            "RolesChanged",
            "RootPermissionChanged",
            "SubregistryChanged",
            "TokenControlTransferred",
        ]
        .map(str::to_owned)
        .to_vec(),
        block_window: Some(HistoryBlockWindow {
            ranges: vec![ChainBlockRange {
                chain_id: "ethereum-mainnet".to_owned(),
                from_block: None,
                to_block: Some(block),
            }],
        }),
        ..Default::default()
    }
}

/// Reconstruct only the three accepted SQL changes to compare the previous query semantics.
/// All parameters, filtering, candidate ordering and selected-event/witness logic are shared.
fn previous_sql(sql: &str) -> String {
    let old = sql.replace(
        "FROM catalogue_anchors anchor CROSS JOIN LATERAL (SELECT source.chain_id, source.source_kind, source.source_key, source.resolver_address FROM bigname_phase.project_history_source source WHERE",
        "FROM catalogue_anchors anchor JOIN bigname_phase.project_history_source source ON",
    ).replace(" OFFSET 0) source", "");
    let mut old = old.replace(
        "source.chain_id = anchor.chain_id AND source.source_kind = anchor.anchor_kind AND source.source_key = anchor.anchor_id AND source.resolver_address = ''",
        "source.source_kind = anchor.anchor_kind AND source.source_key = anchor.anchor_id AND (anchor.anchor_kind = 0 OR source.chain_id = anchor.chain_id) WHERE TRUE",
    ).replace(" AND anchor.chain_id = ne.chain_id AND ((anchor.anchor_kind", " AND ((anchor.anchor_kind");
    for (bound, direction) in [("first_bucket", "ASC"), ("last_bucket", "DESC")] {
        old = old.replace(&format!(" ORDER BY anchor.{bound} {direction} LIMIT 1"),
            &format!(" ORDER BY anchor.{bound} {direction}, anchor.anchor_kind {direction}, anchor.anchor_id {direction}, anchor.chain_id {direction} LIMIT 1"));
    }
    old
}

pub(super) async fn compare_query(
    candidate: &mut PgConnection,
    original: Option<&mut PgConnection>,
    directory: Option<&Path>,
    label: &str,
    mut builder: QueryBuilder<'_, Postgres>,
    bounded: bool,
) -> Result<Value> {
    let mut query = builder.build();
    let sql = query.sql().to_owned();
    let old_sql = previous_sql(&sql);
    let arguments = query
        .take_arguments()
        .map_err(|error| anyhow::anyhow!("query arguments: {error}"))?
        .context("bound production query")?;
    let wrap = |sql: &str| {
        format!("SELECT coalesce(jsonb_agg(to_jsonb(result)), '[]'::jsonb) FROM ({sql}) result")
    };
    let expected: Value = sqlx::query_scalar_with(&wrap(&old_sql), arguments.clone())
        .fetch_one(original.unwrap_or(&mut *candidate))
        .await
        .with_context(|| format!("previous result {label}"))?;
    let actual: Value = sqlx::query_scalar_with(&wrap(&sql), arguments.clone())
        .fetch_one(&mut *candidate)
        .await
        .with_context(|| format!("candidate result {label}"))?;
    ensure!(
        actual == expected,
        "{label}: revised result differs: old={expected}, new={actual}"
    );
    if let Some(directory) = directory {
        std::fs::create_dir_all(directory)?;
        std::fs::write(directory.join(format!("{label}.sql")), &sql)?;
        std::fs::write(directory.join(format!("{label}-previous.sql")), old_sql)?;
        std::fs::write(
            directory.join(format!("{label}-result.json")),
            serde_json::to_vec_pretty(&actual)?,
        )?;
    }
    let plan: Value = sqlx::query_scalar_with(
        &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"),
        arguments,
    )
    .fetch_one(&mut *candidate)
    .await
    .with_context(|| format!("custom plan {label}"))?;
    let generic: Value = sqlx::raw_sql(&format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql}"))
        .fetch_one(&mut *candidate)
        .await
        .with_context(|| format!("generic plan {label}"))?
        .get(0);
    let mut scans = Vec::new();
    collect_scans(&plan[0]["Plan"], &mut scans);
    let mut generic_scans = Vec::new();
    collect_scans(&generic[0]["Plan"], &mut generic_scans);
    if let Some(directory) = directory {
        std::fs::write(
            directory.join(format!("{label}-custom.json")),
            serde_json::to_vec_pretty(&plan)?,
        )?;
        std::fs::write(
            directory.join(format!("{label}-generic.json")),
            serde_json::to_vec_pretty(&generic)?,
        )?;
    }
    println!(
        "layout {label}: {}",
        json!({"equivalent":true,"planning_ms":plan[0]["Planning Time"],
        "execution_ms":plan[0]["Execution Time"],"rows":actual.as_array().map(Vec::len),"scans":scans,"generic_scans":generic_scans})
    );
    if bounded {
        let tables = [
            "normalized_events",
            "project_address_history_anchor",
            "project_history_source",
            "project_history_source_edge",
        ];
        let custom_bounded = scans.iter().all(|scan| {
            !tables.contains(&scan["relation"].as_str().unwrap_or_default())
                || scan["type"] != "Seq Scan"
                || scan["loops"].as_u64().unwrap_or(0) == 0
        });
        let generic_bounded = generic_scans.iter().all(|scan| {
            !tables.contains(&scan["relation"].as_str().unwrap_or_default())
                || scan["type"] != "Seq Scan"
        });
        let check = json!({"case":label, "accepted":custom_bounded && generic_bounded,
            "custom_bounded":custom_bounded, "generic_bounded":generic_bounded});
        if let Some(directory) = directory {
            std::fs::write(
                directory.join(format!("{label}-check.json")),
                serde_json::to_vec_pretty(&check)?,
            )?;
        } else {
            ensure!(
                custom_bounded && generic_bounded,
                "{label}: whole-table candidate scan"
            );
        }
    }
    Ok(actual)
}

fn collect_scans(node: &Value, scans: &mut Vec<Value>) {
    if node["Relation Name"].is_string() {
        scans.push(
            json!({"relation":node["Relation Name"], "type":node["Node Type"],
            "index":node["Index Name"], "condition":node["Index Cond"], "rows":node["Actual Rows"],
            "removed":node["Rows Removed by Filter"], "loops":node["Actual Loops"],
            "shared_hit":node["Shared Hit Blocks"], "shared_read":node["Shared Read Blocks"]}),
        );
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        collect_scans(child, scans);
    }
}
