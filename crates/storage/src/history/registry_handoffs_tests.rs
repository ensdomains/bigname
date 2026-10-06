use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::PgConnection;

use super::*;

const CHAIN: &str = "ethereum-mainnet";
const OLD: &str = "0x0000000000000000000000000000000000000001";
const CURRENT: &str = "0x0000000000000000000000000000000000000002";

#[tokio::test]
async fn handoff_uses_earliest_exact_owner_log_and_readable_earlier_old_witness() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("history_registry_handoff")).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install(&mut connection).await?;
        // Equal owners are still an explicit registry handoff. Ordering distinguishes the
        // two writes in the same block and transaction, independent of page order.
        let old = insert(&mut connection, 1, "registry_old", 10, 0, 1, false).await?;
        let first = insert(&mut connection, 1, "registry", 20, 1, 2, true).await?;
        let later = insert(&mut connection, 1, "registry", 20, 1, 3, false).await?;
        let current_only = insert(&mut connection, 2, "registry", 20, 0, 0, false).await?;
        let late_old = insert(&mut connection, 3, "registry", 20, 0, 0, false).await?;
        insert(&mut connection, 3, "registry_old", 30, 0, 0, false).await?;
        let retracted_old = insert(&mut connection, 4, "registry_old", 10, 0, 0, false).await?;
        sqlx::query("UPDATE normalized_events SET canonicality_state='orphaned' WHERE normalized_event_id=$1")
            .bind(retracted_old.normalized_event_id).execute(&mut *connection).await?;
        let unproven = insert(&mut connection, 4, "registry", 20, 0, 0, false).await?;
        insert(&mut connection, 0, "registry_old", 10, 0, 0, false).await?;
        let root = insert(&mut connection, 0, "registry", 20, 0, 0, false).await?;
        let pointer_old = insert(&mut connection, 5, "registry_old", 10, 0, 0, false).await?;
        sqlx::query("UPDATE normalized_events SET event_kind='ResolverChanged',after_state=jsonb_set(after_state,'{source_event}','\"NewResolver\"') WHERE normalized_event_id=$1")
            .bind(pointer_old.normalized_event_id).execute(&mut *connection).await?;
        let pointer_only = insert(&mut connection, 5, "registry", 20, 0, 0, false).await?;
        let incomplete_old = insert(&mut connection, 6, "registry_old", 10, 0, 0, false).await?;
        sqlx::query("UPDATE normalized_events SET raw_fact_ref='{}' WHERE normalized_event_id=$1")
            .bind(incomplete_old.normalized_event_id).execute(&mut *connection).await?;
        let incomplete = insert(&mut connection, 6, "registry", 20, 0, 0, false).await?;
        let page = [later.clone(), current_only, first.clone(), late_old, unproven, root, pointer_only, incomplete];
        let bounds = BTreeMap::from([(CHAIN.to_owned(), 20)]);
        let evidence = load_history_registry_handoffs(&mut connection, &page, &bounds).await?;
        assert_eq!(evidence, BTreeMap::from([(first.normalized_event_id, HistoryRegistryHandoff {
            from_registry: OLD.into(), to_registry: CURRENT.into(), old_event_id: old.normalized_event_id,
        })]));
        assert!(load_history_registry_handoffs(&mut connection, &page, &BTreeMap::from([(CHAIN.to_owned(), 19)])).await?.is_empty());
        sqlx::query("UPDATE normalized_events SET consumer_visibility='candidate',migration_correlation_ids=ARRAY['handoff-read-test'] WHERE normalized_event_id=$1")
            .bind(old.normalized_event_id).execute(&mut *connection).await?;
        assert!(load_history_registry_handoffs(&mut connection, &page, &bounds).await?.is_empty());
        sqlx::query("UPDATE normalized_events SET consumer_visibility='activated' WHERE normalized_event_id=$1")
            .bind(old.normalized_event_id).execute(&mut *connection).await?;
        // A retracted first log cannot classify an unrelated later page using its stale id.
        sqlx::query("UPDATE normalized_events SET canonicality_state='orphaned' WHERE normalized_event_id=$1")
            .bind(first.normalized_event_id).execute(&mut *connection).await?;
        let evidence = load_history_registry_handoffs(&mut connection, &page, &bounds).await?;
        assert_eq!(evidence.keys().copied().collect::<Vec<_>>(), vec![later.normalized_event_id]);
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn handoff_probes_are_indexed_by_chain_and_exact_node() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("history_registry_handoff_plan")).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install(&mut connection).await?;
        insert(&mut connection, 1, "registry_old", 10, 0, 0, false).await?;
        let first = insert(&mut connection, 1, "registry", 20, 0, 1, true).await?;
        sqlx::raw_sql("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref)
            SELECT 'noise-'||n,'ens',CASE WHEN n%2=0 THEN 'AuthorityTransferred' ELSE 'SubregistryChanged' END,'ens_v1_registry_l1',1,'ethereum-mainnet','block-10',10,'noise',0,n,'ens_v1_unwrapped_authority','canonical',jsonb_build_object('source_event',CASE WHEN n%2=0 THEN 'Transfer' ELSE 'NewOwner' END,'emitter_role','registry_old',CASE WHEN n%2=0 THEN 'node' ELSE 'child_node' END,'0x'||lpad(to_hex(n+100),64,'0')),'{}'::jsonb FROM generate_series(1,12000) n; ANALYZE normalized_events; ANALYZE chain_lineage; SET jit=off;")
            .execute(&mut *connection).await?;
        let bounds = BTreeMap::from([(CHAIN.to_owned(), 20)]);
        let targets = json!([target(&first, &bounds).unwrap()]);
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {}",handoff_sql()))
            .bind(targets).fetch_one(&mut *connection).await?;
        let mut scans = Vec::new();
        normalized_scans(&plan[0]["Plan"], &mut scans);
        assert_eq!(plan[0]["Plan"]["Actual Rows"], 1);
        assert!(!scans.is_empty());
        for scan in scans {
            assert_ne!(scan["Node Type"], "Seq Scan", "unbounded owner observation scan: {scan}");
        }
        if let Ok(directory) = std::env::var("BIGNAME_HISTORY_ACTIONS_PLAN_DIR") {
            std::fs::create_dir_all(&directory)?;
            std::fs::write(std::path::Path::new(&directory).join("registry-handoff-plan.json"), serde_json::to_vec_pretty(&plan)?)?;
        }
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

fn normalized_scans<'a>(node: &'a Value, scans: &mut Vec<&'a Value>) {
    if node["Relation Name"] == "normalized_events" {
        scans.push(node);
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        normalized_scans(child, scans);
    }
}

async fn install(connection: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path=bigname_phase,public")
        .execute(&mut *connection)
        .await?;
    for schema in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(schema).execute(&mut *connection).await?;
    }
    sqlx::raw_sql("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) SELECT 'ethereum-mainnet','block-'||n,n,to_timestamp(n),'canonical' FROM unnest(ARRAY[10,20,30]) n")
        .execute(connection).await?;
    Ok(())
}

async fn insert(
    connection: &mut PgConnection,
    node: u64,
    role: &str,
    block: i64,
    tx: i64,
    log: i64,
    new_owner: bool,
) -> Result<HistoryEvent> {
    let kind = if new_owner {
        "SubregistryChanged"
    } else {
        "AuthorityTransferred"
    };
    let source = if new_owner { "NewOwner" } else { "Transfer" };
    let node = format!("0x{node:064x}");
    let mut after = json!({"source_event":source,"emitter_role":role,"node":node,"owner":"0x0000000000000000000000000000000000000011"});
    if new_owner {
        after["child_node"] = json!(node);
    }
    let raw = json!({"emitting_address":if role == "registry" { CURRENT } else { OLD }});
    let identity = format!("{role}:{node}:{block}:{tx}:{log}");
    let block_hash = format!("block-{block}");
    let id: i64 = sqlx::query_scalar("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES($1,'ens',$2,'ens_v1_registry_l1',1,$3,$4,$5,'tx',$6,$7,'ens_v1_unwrapped_authority','canonical',$8,$9) RETURNING normalized_event_id")
        .bind(&identity).bind(kind).bind(CHAIN).bind(&block_hash).bind(block).bind(tx).bind(log).bind(&after).bind(&raw).fetch_one(connection).await?;
    Ok(HistoryEvent {
        normalized_event_id: id,
        event_identity: identity,
        namespace: "ens".into(),
        logical_name_id: None,
        resource_id: None,
        registration_id: None,
        event_kind: kind.into(),
        source_family: "ens_v1_registry_l1".into(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(CHAIN.into()),
        block_number: Some(block),
        block_hash: Some(block_hash),
        block_timestamp: None,
        transaction_hash: Some("tx".into()),
        transaction_index: Some(tx),
        log_index: Some(log),
        raw_fact_ref: raw,
        derivation_kind: "ens_v1_unwrapped_authority".into(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: after,
        migration_correlation_ids: vec![],
        consumer_visibility: "activated".into(),
        migration_associations: json!([]),
        provenance: json!({}),
        coverage: json!({}),
    })
}
