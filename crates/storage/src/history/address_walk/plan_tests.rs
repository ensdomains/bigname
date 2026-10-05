//! Actual address-candidate statements under ordinary planner settings. Large variants are
//! manual because they intentionally seed hundreds of thousands of unrelated rows.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use super::{AddressRead, source::push_candidate_query};
use crate::history::{ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryScope};

const ADDRESS: &str = "0x0000000000000000000000000000000000000a11";
const EMPTY_ADDRESS: &str = "0x0000000000000000000000000000000000000bad";

#[tokio::test]
async fn address_history_candidate_plans_are_address_keyed() -> Result<()> {
    candidate_plans(32, 3_200).await
}

#[tokio::test]
#[ignore = "manual realistic-cardinality candidate plan gate"]
async fn address_history_representative_candidate_plans() -> Result<()> {
    let target = std::env::var("BIGNAME_HISTORY_PLAN_NAMES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9_221);
    let unrelated = std::env::var("BIGNAME_HISTORY_PLAN_UNRELATED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(target * 10);
    candidate_plans(target, unrelated).await
}

async fn candidate_plans(target: usize, unrelated: usize) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("address_walk_plans").pool_max_connections(1))
            .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install(&mut connection, target, unrelated).await?;
        let published = BTreeMap::from([("ethereum-mainnet".to_owned(), (target + unrelated + 1) as i64)]);
        let mut failures = Vec::new();
        for (label, address, absent_key) in [("heavy", ADDRESS, false), ("sparse", ADDRESS, true), ("empty", EMPTY_ADDRESS, false)] {
            let read = AddressRead { address, namespace: Some("ens"), relations: None, scope: HistoryScope::Both, canonical_only: true, published: Some(&published) };
            let filter = EventHistoryReadFilter {
                block_window: Some(HistoryBlockWindow { ranges: vec![ChainBlockRange { chain_id: "ethereum-mainnet".to_owned(), from_block: None, to_block: Some((target + unrelated + 1) as i64) }] }),
                record_key: absent_key.then(|| "missing-key".to_owned()),
                ..Default::default()
            };
            for mode in ["auto", "force_custom_plan", "force_generic_plan"] {
                sqlx::raw_sql(&format!("SET plan_cache_mode={mode}; SET statement_timeout='120s'; SET jit=off; SET enable_seqscan=on")).execute(&mut *connection).await?;
                // DECLARE uses a nonpersistent request statement. Also inspect a true generic
                // plan explicitly: merely setting plan_cache_mode around EXPLAIN ANALYZE
                // does not prove the inner statement was planned with unknown parameters.
                let generic = mode == "force_generic_plan";
                let mut query = QueryBuilder::<Postgres>::new(if generic {
                    "EXPLAIN (GENERIC_PLAN, FORMAT JSON) "
                } else { "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " });
                push_candidate_query(&mut query, &read, &filter, None);
                let sql = query.sql().to_owned();
                let plan: Value = if generic {
                    // Do not bind concrete values: GENERIC_PLAN accepts unresolved SQL
                    // parameters and infers their types from the production predicates.
                    sqlx::raw_sql(&sql).fetch_one(&mut *connection).await.map(|row| row.get(0))
                } else { query.build_query_scalar().fetch_one(&mut *connection).await }
                    .with_context(|| format!("candidate plan {label}/{mode}"))?;
                if generic { ensure!(plan.to_string().contains('$'), "generic plan substituted concrete parameters"); }
                let scans = event_scans(&plan[0]["Plan"]);
                println!("{label}/{mode}: {}", json!({"target_names":target,"unrelated_names":unrelated,"execution_ms":plan[0]["Execution Time"],"planning_ms":plan[0]["Planning Time"],"rows":plan[0]["Plan"]["Actual Rows"],"event_scans":scans}));
                if let Ok(directory) = std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR") {
                    let directory = PathBuf::from(directory);
                    std::fs::create_dir_all(&directory)?;
                    let name = format!("{target}-{unrelated}-{label}-{mode}");
                    std::fs::write(directory.join(format!("{name}.json")), serde_json::to_vec_pretty(&plan)?)?;
                    std::fs::write(directory.join(format!("{name}.sql")), sql)?;
                }
                for scan in scans {
                    if scan["type"] == "Seq Scan" && (generic || scan["loops"].as_u64().unwrap_or(0) > 0) {
                        failures.push(format!("{label}/{mode} scanned normalized_events: {scan}"));
                    }
                }
                let expected = if label == "heavy" { target * 12 } else { 0 };
                ensure!(generic || plan[0]["Plan"]["Actual Rows"].as_u64() == Some(expected as u64), "{label}/{mode}: expected {expected} distinct witnesses, plan={plan}");
            }
        }
        let resources: Vec<uuid::Uuid> = (1..=target.min(128)).map(|n| uuid::Uuid::from_u128(n as u128)).collect();
        let identities: Vec<String> = (1..=target.min(128)).map(|n| format!("plan:id-record:{n}")).collect();
        let event_ids: Vec<i64> = sqlx::query_scalar("SELECT normalized_event_id FROM normalized_events WHERE event_identity = ANY($1) ORDER BY substring(event_identity FROM '[0-9]+$')::bigint")
            .bind(&identities).fetch_all(&mut *connection).await?;
        let mut paired = QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ");
        crate::history::attribution::push_paired_attribution_for_test(&mut paired, &resources, &event_ids, Some(&published));
        let paired_sql = paired.sql().to_owned();
        let plan: Value = paired.build_query_scalar().fetch_one(&mut *connection).await.context("paired attribution plan")?;
        let scans = event_scans(&plan[0]["Plan"]);
        println!("paired: {}", json!({"execution_ms":plan[0]["Execution Time"],"rows":plan[0]["Plan"]["Actual Rows"],"event_scans":scans}));
        if let Ok(directory) = std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR") {
            std::fs::write(PathBuf::from(&directory).join(format!("{target}-{unrelated}-paired.json")), serde_json::to_vec_pretty(&plan)?)?;
            std::fs::write(PathBuf::from(directory).join(format!("{target}-{unrelated}-paired.sql")), paired_sql)?;
        }
        ensure!(plan[0]["Plan"]["Actual Rows"].as_u64() == Some(resources.len() as u64), "paired output must contain exactly the requested valid pairs: {plan}");
        for scan in scans {
            if scan["type"] == "Seq Scan" && scan["loops"].as_u64().unwrap_or(0) > 0 { failures.push(format!("paired attribution scanned normalized_events: {scan}")); }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

fn event_scans(node: &Value) -> Vec<Value> {
    let mut scans = Vec::new();
    if node["Relation Name"] == "normalized_events" {
        scans.push(json!({"type":node["Node Type"],"alias":node["Alias"],"index":node["Index Name"],"condition":node["Index Cond"],"rows":node["Actual Rows"],"removed":node["Rows Removed by Filter"],"loops":node["Actual Loops"],"shared_hit":node["Shared Hit Blocks"],"shared_read":node["Shared Read Blocks"]}));
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scans.extend(event_scans(child));
    }
    scans
}

async fn install(connection: &mut PgConnection, target: usize, unrelated: usize) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../schema/baseline/01_chain.sql"),
        include_str!("../../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../../schema/baseline/03_identity.sql"),
        include_str!("../../../schema/baseline/04_manifests.sql"),
        include_str!("../../../schema/baseline/05_normalized_events.sql"),
        include_str!("../../../schema/baseline/06_projections.sql"),
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    let total = target + unrelated;
    // Family index rows are deliberately shaped here only for access-plan evidence. Route
    // semantic tests build their projections through Project; this fixture proves no serving
    // semantics of hand-written family rows.
    sqlx::raw_sql(&format!(r#"
      INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
      SELECT 'ethereum-mainnet','block-'||n,n,to_timestamp(n),'canonical'::canonicality_state FROM generate_series(1,{total}+1) n;
      INSERT INTO name_surfaces(logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
      SELECT 'ens:0x'||lpad(to_hex(n),64,'0'),'ens','plan-'||n||'.eth',ARRAY['plan-'||n,'eth'],'\x00'::bytea,'0x'||lpad(to_hex(n),64,'0'),ARRAY['0x'||lpad(to_hex(n),64,'0'),'eth'],'test','active','ethereum-mainnet','block-1',1,'canonical'::canonicality_state FROM generate_series(1,{total}) n;
      INSERT INTO resources(resource_id,chain_id,block_hash,block_number,canonicality_state)
      SELECT lpad(to_hex(n),32,'0')::uuid,'ethereum-mainnet','block-1',1,'canonical'::canonicality_state FROM generate_series(1,{total}) n;
      INSERT INTO surface_bindings(surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state)
      SELECT lpad(to_hex(n),32,'0')::uuid,'ens:0x'||lpad(to_hex(n),64,'0'),lpad(to_hex(n),32,'0')::uuid,'declared_registry_path','ens_v2',to_timestamp(1),'ethereum-mainnet','block-1',1,'canonical'::canonicality_state FROM generate_series(1,{total}) n;
      INSERT INTO project_address_name_index(address,logical_name_id,relation,chain_id)
      SELECT CASE WHEN n<={target} THEN '{ADDRESS}' ELSE '0x'||lpad(to_hex(n),40,'e') END,'ens:0x'||lpad(to_hex(n),64,'0'),'token_holder','ethereum-mainnet' FROM generate_series(1,{total}) n;
      INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
      SELECT 'plan:grant:'||n,'ens','ens:0x'||lpad(to_hex(n),64,'0'),lpad(to_hex(n),32,'0')::uuid,'RegistrationGranted','ens_v2_registry_l1',1,'ethereum-mainnet','block-'||n,n,'tx-'||n,0,0,'ens_v2_registry_resource_surface','canonical'::canonicality_state,jsonb_build_object('registrant',CASE WHEN n<={target} THEN '{ADDRESS}' ELSE '0x'||lpad(to_hex(n),40,'e') END) FROM generate_series(1,{total}) n
      UNION ALL
      SELECT 'plan:pointer:'||n,'ens','ens:0x'||lpad(to_hex(n),64,'0'),lpad(to_hex(n),32,'0')::uuid,'ResolverChanged','ens_v1_registry_l1',1,'ethereum-mainnet','block-'||n,n,'tx-'||n,0,1,'ens_v1_unwrapped_authority','canonical'::canonicality_state,jsonb_build_object('resolver','0x'||lpad(to_hex(n),40,'d'),'node','0x'||lpad(to_hex(n),64,'0')) FROM generate_series(1,{total}) n
      UNION ALL
      SELECT 'plan:record:'||n,'ens',NULL,NULL,'RecordChanged',CASE WHEN n%2=0 THEN 'ens_v1_resolver_l1' ELSE 'ens_v2_resolver_l1' END,1,'ethereum-mainnet','block-'||n,n,'tx-'||n,0,2,'ens_v1_unwrapped_authority','canonical'::canonicality_state,jsonb_build_object('resolver','0x'||lpad(to_hex(n),40,'d'),'node','0x'||lpad(to_hex(n),64,'0'),'record_key','text:description','value','plan fixture') FROM generate_series(1,{total}) n
      UNION ALL
      SELECT 'plan:link:'||n,'ens',NULL,NULL,'ResolverRecordLinked','ens_v2_resolver_l1',1,'ethereum-mainnet','block-'||n,n,'tx-'||n,0,3,'ens_v2_resolver','canonical'::canonicality_state,jsonb_build_object('resolver','0x'||lpad(to_hex(n),40,'d'),'node','0x'||lpad(to_hex(n),64,'0'),'storage_model','resolver_record_id','resolver_record_id','record-'||n) FROM generate_series(1,{total}) n
      UNION ALL
      SELECT 'plan:id-record:'||n,'ens',NULL,NULL,'RecordChanged','ens_v2_resolver_l1',1,'ethereum-mainnet','block-'||n,n,'tx-'||n,0,4,'ens_v2_resolver','canonical'::canonicality_state,jsonb_build_object('resolver','0x'||lpad(to_hex(n),40,'d'),'storage_model','resolver_record_id','resolver_record_id','record-'||n,'record_key','text:description','value','plan fixture') FROM generate_series(1,{total}) n;
      ANALYZE;
    "#)).execute(&mut *connection).await?;
    Ok(())
}
