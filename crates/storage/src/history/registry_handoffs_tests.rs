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
async fn handoff_requires_original_keys_without_advancing_past_retargeted_rows() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("history_registry_original_owner")).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install(&mut connection).await?;
        insert(&mut connection, 1, "registry_old", 10, 0, 0, false).await?;
        let mut first = insert(&mut connection, 1, "registry", 20, 0, 0, false).await?;
        let later = insert(&mut connection, 1, "registry", 30, 0, 0, false).await?;
        let original = first.after_state.clone();
        let bounds = BTreeMap::from([(CHAIN.to_owned(), 30)]);
        for missing in [
            vec!["authority_kind"],
            vec!["authority_key"],
            vec!["authority_kind", "authority_key"],
        ] {
            first.after_state = original.clone();
            for key in &missing {
                first.after_state.as_object_mut().unwrap().remove(*key);
            }
            sqlx::query("UPDATE normalized_events SET after_state=$2 WHERE normalized_event_id=$1")
                .bind(first.normalized_event_id)
                .bind(&first.after_state)
                .execute(&mut *connection)
                .await?;
            assert!(
                load_history_registry_handoffs(
                    &mut connection,
                    &[first.clone(), later.clone()],
                    &bounds
                )
                .await?
                .is_empty(),
                "missing {missing:?} must not move the marker to the later row"
            );
        }
        first.after_state = original;
        first.after_state["authority_kind"] = Value::Null;
        first.after_state["authority_key"] = Value::Null;
        sqlx::query("UPDATE normalized_events SET after_state=$2 WHERE normalized_event_id=$1")
            .bind(first.normalized_event_id)
            .bind(&first.after_state)
            .execute(&mut *connection)
            .await?;
        let evidence =
            load_history_registry_handoffs(&mut connection, &[first.clone(), later], &bounds)
                .await?;
        assert_eq!(
            evidence.keys().copied().collect::<Vec<_>>(),
            vec![first.normalized_event_id],
            "present-null authority fields are original owner evidence"
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn handoff_clear_veto_requires_earlier_readable_exact_ownership_evidence() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_registry_clear")).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install(&mut connection).await?;
        insert(&mut connection, 1, "registry_old", 10, 0, 0, false).await?;
        let first = insert(&mut connection, 1, "registry", 20, 1, 2, true).await?;
        let clear = insert_clear(&mut connection, 1, 20, 1, 2).await?;
        let page = [first];
        let bounds = BTreeMap::from([(CHAIN.to_owned(), 20)]);
        assert_eq!(load_history_registry_handoffs(&mut connection, &page, &bounds).await?.len(), 1, "a same-position clear cannot veto the original owner row");
        sqlx::query("UPDATE normalized_events SET log_index=1 WHERE normalized_event_id=$1")
            .bind(clear).execute(&mut *connection).await?;
        assert!(load_history_registry_handoffs(&mut connection, &page, &bounds).await?.is_empty(), "an earlier ownership clear must veto");
        // These are guards on the existing witness, not extra families of evidence.
        for (field, replacement) in [("emitter_role", "registry_old"), ("source_event", "NewResolver"), ("child_node", "0x0000000000000000000000000000000000000000000000000000000000000002")] {
            let saved: Value = sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE normalized_event_id=$1")
                .bind(clear).fetch_one(&mut *connection).await?;
            let mut changed = saved.clone();
            changed[field] = json!(replacement);
            sqlx::query("UPDATE normalized_events SET after_state=$2 WHERE normalized_event_id=$1")
                .bind(clear).bind(changed).execute(&mut *connection).await?;
            assert_eq!(load_history_registry_handoffs(&mut connection, &page, &bounds).await?.len(), 1, "unmatched {field} must not veto");
            sqlx::query("UPDATE normalized_events SET after_state=$2 WHERE normalized_event_id=$1")
                .bind(clear).bind(saved).execute(&mut *connection).await?;
        }
        sqlx::query("UPDATE normalized_events SET raw_fact_ref=jsonb_build_object('emitting_address',$2::text) WHERE normalized_event_id=$1")
            .bind(clear).bind(OLD).execute(&mut *connection).await?;
        assert_eq!(load_history_registry_handoffs(&mut connection, &page, &bounds).await?.len(), 1);
        sqlx::query("UPDATE normalized_events SET raw_fact_ref=jsonb_build_object('emitting_address',$2::text),canonicality_state='orphaned' WHERE normalized_event_id=$1")
            .bind(clear).bind(CURRENT).execute(&mut *connection).await?;
        assert_eq!(load_history_registry_handoffs(&mut connection, &page, &bounds).await?.len(), 1);
        sqlx::query("UPDATE normalized_events SET canonicality_state='canonical',consumer_visibility='candidate',migration_correlation_ids=ARRAY['handoff-clear-test'] WHERE normalized_event_id=$1")
            .bind(clear).execute(&mut *connection).await?;
        assert_eq!(load_history_registry_handoffs(&mut connection, &page, &bounds).await?.len(), 1);
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
        insert_clear(&mut connection, 1, 20, 0, 1).await?;
        insert(&mut connection, 2, "registry_old", 10, 0, 0, false).await?;
        let vetoed = insert(&mut connection, 2, "registry", 20, 0, 2, true).await?;
        insert_clear(&mut connection, 2, 20, 0, 1).await?;
        sqlx::raw_sql("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref)
            SELECT 'noise-'||n,'ens',CASE WHEN n%3=0 THEN 'ResolverChanged' WHEN n%3=1 THEN 'AuthorityTransferred' ELSE 'SubregistryChanged' END,'ens_v1_registry_l1',1,'ethereum-mainnet','block-10',10,'noise',0,n,'ens_v1_unwrapped_authority','canonical',jsonb_build_object('source_event',CASE WHEN n%3=1 THEN 'Transfer' ELSE 'NewOwner' END,'registry_fallback_handoff',true,'emitter_role',CASE WHEN n%3=0 THEN 'registry' ELSE 'registry_old' END,CASE WHEN n%3=1 THEN 'node' ELSE 'child_node' END,'0x'||lpad(to_hex(n+100),64,'0')),jsonb_build_object('emitting_address','0x0000000000000000000000000000000000000002') FROM generate_series(1,12000) n; ANALYZE normalized_events; ANALYZE chain_lineage; SET jit=off;")
            .execute(&mut *connection).await?;
        let bounds = BTreeMap::from([(CHAIN.to_owned(), 20)]);
        let first_target = target(&first, &bounds).unwrap();
        let vetoed_target = target(&vetoed, &bounds).unwrap();
        let evidence = load_history_registry_handoffs(&mut connection, &[first.clone(), vetoed], &bounds).await?;
        assert_eq!(evidence.keys().copied().collect::<Vec<_>>(), vec![first.normalized_event_id]);
        for (name, targets, expected_rows) in [
            ("registry-handoff", json!([first_target.clone(), vetoed_target.clone()]), 1),
            ("registry-handoff-positive", json!([first_target]), 1),
            ("registry-handoff-veto", json!([vetoed_target]), 0),
        ] {
            let target_count = targets.as_array().unwrap().len();
            let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {}",handoff_sql()))
                .bind(targets).fetch_one(&mut *connection).await?;
            if let Ok(directory) = std::env::var("BIGNAME_HISTORY_ACTIONS_PLAN_DIR") {
                std::fs::create_dir_all(&directory)?;
                std::fs::write(std::path::Path::new(&directory).join(format!("{name}-plan.json")), serde_json::to_vec_pretty(&plan)?)?;
            }
            let mut scans = Vec::new();
            normalized_scans(&plan[0]["Plan"], &mut scans);
            assert_eq!(plan[0]["Plan"]["Actual Rows"], expected_rows, "{name}");
            assert!(!scans.is_empty());
            let clear_scan = scans.iter().find(|scan| scan["Alias"] == "cleared").expect("complete query must plan the clear veto");
            assert_eq!(clear_scan["Index Name"], "normalized_events_v1_direct_node_probe_idx", "{name}: {clear_scan}");
            let condition = clear_scan["Index Cond"].as_str().unwrap();
            assert!(condition.contains("chain_id = target.chain") && condition.contains("target.name_key"), "{condition}");
            assert_eq!(clear_scan["Actual Loops"], target_count);
            for scan in scans {
                assert_ne!(scan["Node Type"], "Seq Scan", "unbounded owner observation scan: {scan}");
            }
        }
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

async fn insert_clear(
    connection: &mut PgConnection,
    node: u64,
    block: i64,
    tx: i64,
    log: i64,
) -> Result<i64> {
    // NewOwner's child takes precedence over the parent node in the indexed name expression.
    sqlx::query_scalar("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES($1,'ens','ResolverChanged','ens_v1_registry_l1',1,$2,$3,$4,'tx',$5,$6,'ens_v1_unwrapped_authority','canonical',$7,$8) RETURNING normalized_event_id")
        .bind(format!("clear:{node}:{block}:{tx}:{log}")).bind(CHAIN).bind(format!("block-{block}")).bind(block).bind(tx).bind(log)
        .bind(json!({"source_event":"NewOwner","emitter_role":"registry","registry_fallback_handoff":true,"child_node":format!("0x{node:064x}"),"node":format!("0x{:064x}",99),"resolver":"0x0000000000000000000000000000000000000000"}))
        .bind(json!({"emitting_address":CURRENT})).fetch_one(connection).await.map_err(Into::into)
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
    let mut after = json!({"source_event":source,"emitter_role":role,"node":node,"owner":"0x0000000000000000000000000000000000000011","authority_kind":"registry_only","authority_key":node});
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
