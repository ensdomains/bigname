//! The real page/count builders must use the emitter bound, including with prepared plans.
use super::super::super::contract_count::contract_count_filter;
use super::super::super::summary::push_history_count_query;
use super::*;

const CONTRACT_FIXTURE: &str = r#"
    INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'
    FROM generate_series(1, 11200) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, raw_fact_ref, after_state)
    SELECT 'contract-plan:' || n, 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1,
           'ethereum-mainnet', 'block-' || (1 + (n - 1) / 10), 1 + (n - 1) / 10,
           'tx-' || n, (n - 1) % 10, 0,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('kind', 'raw_log', 'emitting_address',
               CASE WHEN n <= 12000 THEN '0x0000000000000000000000000000000000000076'
                    ELSE '0x' || lpad(to_hex(n), 40, '0') END),
           jsonb_build_object('record_key', CASE WHEN n % 2 = 0 THEN 'addr:60' ELSE 'text:avatar' END)
    FROM generate_series(1, 112000) n;
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

// Mirrors the sorted product_history_event_kinds() result in apps/api/src/v2/history.rs:
// the public default does not use a single-kind equality predicate.
const PRODUCT_KINDS: &[&str] = &[
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
    "SubregistryChanged",
    "TokenControlTransferred",
];
const RECORD_KINDS: &[&str] = &["RecordChanged", "RecordVersionChanged"];

#[tokio::test]
async fn contract_count_and_page_use_emitter_index_on_large_history() -> Result<()> {
    let database = phase_database("history_contract_count", CONTRACT_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        for mode in [PlanMode::Unprepared, PlanMode::Generic] {
            for (case, key, kinds, count) in [
                ("public-default", None, PRODUCT_KINDS, 12000),
                ("record-key-default", Some("addr:60"), RECORD_KINDS, 6000),
                ("missing-key-default", Some("absent"), RECORD_KINDS, 0),
                ("explicit-write-kind", None, &["RecordChanged"][..], 12000),
                ("explicit-write-key", Some("addr:60"), &["RecordChanged"][..], 6000),
                ("explicit-missing-key", Some("absent"), &["RecordChanged"][..], 0),
                ("explicit-missing-kind", None, &["RecordVersionChanged"][..], 0),
            ] {
                // The registry overview's `counts.events` statement, plus the feed's record key.
                let kinds: Vec<String> = kinds.iter().map(|kind| (*kind).to_owned()).collect();
                let filter = EventHistoryReadFilter {
                    record_key: key.map(str::to_owned),
                    ..contract_count_filter(
                        "ethereum-mainnet",
                        "0x0000000000000000000000000000000000000076",
                        &kinds,
                        Some(11200),
                    )
                };
                let mut count_query = QueryBuilder::<Postgres>::new("");
                push_history_count_query(&mut count_query, &filter, true, None);
                let actual: i64 = count_query.build_query_scalar().fetch_one(&mut *connection).await?;
                assert_eq!(actual, count);
                count_query.reset();
                push_history_count_query(&mut count_query, &filter, true, None);
                let count_plan = explain_page(&mut connection, count_query, mode).await?;
                let mut page = QueryBuilder::<Postgres>::new("");
                push_history_page_query(&mut page, &filter, true, None, false, 21);
                let page_plan = explain_page(&mut connection, page, mode).await?;
                for (label, plan) in [("count", count_plan), ("page", page_plan)] {
                    // Preserve the complete per-node evidence, including auxiliary subplans.
                    eprintln!("contract_plan {}", serde_json::json!({
                        "mode": format!("{mode:?}"), "case": case, "operation": label,
                        "record_key": key, "kinds": kinds, "expected_count": count, "plan": plan,
                    }));
                    let mut scans = Vec::new();
                    page_scans(&plan[0]["Plan"], &mut scans);
                    ensure!(scans.len() == 1, "{label} {mode:?}: {plan}");
                    let scan = scans[0];
                    // This metric is only the principal normalized-event scan. It excludes
                    // auxiliary subplans, joins, and work outside this SQL statement.
                    let visited = (scan["Actual Rows"].as_f64().unwrap_or(0.0)
                        + scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                        * scan["Actual Loops"].as_f64().unwrap_or(1.0);
                    let encoded = plan.to_string();
                    ensure!(encoded.contains("normalized_events_emitter_history_idx") || (count == 0 && visited == 0.0), "{label} {mode:?}: {plan}");
                    ensure!(visited <= 12000.0, "{label} scanned unrelated history: {plan}");
                    eprintln!("contract {label} {mode:?} case={case}: count={count}; principal_event_rows_visited={visited}; execution_ms={}; shared_hit_blocks={}", plan[0]["Execution Time"], plan[0]["Plan"]["Shared Hit Blocks"]);
                }
            }
        }
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}
