//! Explicit read-only plan capture of the production statements on the retained first gate.

use std::{collections::BTreeMap, path::Path};

use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder};

use super::{AddressRead, catalogue_count, catalogue_source};
use crate::history::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryOrder, HistoryScope,
};

#[tokio::test]
#[ignore = "manual read-only plans of the Project-built retained first performance gate"]
async fn address_history_retained_catalogue_plans() -> Result<()> {
    let url = std::env::var("BIGNAME_HISTORY_RETAINED_DATABASE_URL")?;
    let directory = std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR")?;
    let directory = Path::new(&directory);
    std::fs::create_dir_all(directory)?;
    let mut connection = PgConnection::connect(&url).await?;
    sqlx::raw_sql(
        "SET search_path=bigname_phase,public; SET jit=off; SET statement_timeout='120s'; BEGIN READ ONLY",
    )
    .execute(&mut connection)
    .await?;
    let published = BTreeMap::from([("ethereum-mainnet".to_owned(), 40_821)]);
    let relations = [crate::AddressNameRelation::TokenHolder];
    let read = AddressRead {
        address: "0x000000000000000000000000000000000000a235",
        namespace: Some("ens"),
        relations: Some(&relations),
        scope: HistoryScope::Both,
        canonical_only: true,
        published: Some(&published),
        catalogue: true,
    };
    let filter = EventHistoryReadFilter {
        order: HistoryOrder::Desc,
        // The route's default product kinds. Keep in step with product_history_event_kinds.
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
                to_block: Some(40_821),
            }],
        }),
        ..Default::default()
    };
    let prefix = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ";
    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_source::push_seek_query(&mut query, &read, &filter, None);
    explain(&mut connection, query, directory, "initial-seek").await?;
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_source::push_seek_query(&mut query, &read, &filter, None);
    let bucket: Option<i64> = query
        .build_query_scalar()
        .fetch_one(&mut connection)
        .await?;
    let bucket = bucket.expect("the retained first-gate owner has history");

    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_source::push_candidate_query(&mut query, &read, &filter, None, bucket, None, 2);
    explain(&mut connection, query, directory, "page-one-prefix").await?;

    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_count::push_names_query(&mut query, &read, &filter, None);
    explain(&mut connection, query, directory, "count-names-first").await?;
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_count::push_names_query(&mut query, &read, &filter, None);
    let names: Vec<String> = query
        .build_query_scalar()
        .fetch_all(&mut connection)
        .await?;
    ensure!(
        names.len() == 256,
        "expected a fixed complete proof-name batch"
    );
    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_count::push_proof_query(&mut query, &read, &filter, &names, 10_001);
    explain(&mut connection, query, directory, "count-proof-first").await?;
    sqlx::raw_sql("ROLLBACK").execute(&mut connection).await?;
    Ok(())
}

async fn explain(
    connection: &mut PgConnection,
    mut query: QueryBuilder<'_, Postgres>,
    directory: &Path,
    label: &str,
) -> Result<()> {
    std::fs::write(directory.join(format!("{label}.sql")), query.sql())?;
    let plan: Value = query
        .build_query_scalar()
        .persistent(false)
        .fetch_one(connection)
        .await?;
    std::fs::write(
        directory.join(format!("{label}.json")),
        serde_json::to_vec_pretty(&plan)?,
    )?;
    let mut scans = Vec::new();
    collect_scans(&plan[0]["Plan"], &mut scans);
    println!(
        "{label}: {}",
        json!({"planning_ms":plan[0]["Planning Time"], "execution_ms":plan[0]["Execution Time"], "scans":scans})
    );
    ensure!(
        scans
            .iter()
            .all(|scan| scan["relation"] != "normalized_events"
                || scan["type"] != "Seq Scan"
                || scan["loops"].as_u64().unwrap_or(0) == 0),
        "{label} performed a whole normalized-event scan"
    );
    Ok(())
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
