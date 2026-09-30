use std::{sync::Arc, time::Duration};

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};

use super::*;
use crate::test_chain::{self, Tamper, TestChain, TestLog, WATCHED_ADDRESS};

const CHAIN: &str = "test-chain";
const TRANSFER: &str = "Transfer(address,address,uint256)";

fn topic(signature: &str) -> String {
    alloy_primitives::keccak256(signature).to_string()
}

async fn database(name: &str, family: &str, events: Value) -> Result<TestDatabase> {
    let db = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for schema in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
    ] {
        sqlx::raw_sql(schema).execute(db.pool()).await?;
    }
    let payload = json!({
        "manifest_version":1,"namespace":"test","source_family":family,
        "chain":CHAIN,"deployment_epoch":"fixture","rollout_status":"active",
        "normalizer_version":"test","capability_flags":{},
        "contracts":[],"roots":[],"discovery_rules":[],
        "abi":{"events":events,"calls":[]}
    });
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,
         deployment_label,rollout_status,normalizer_version,file_path,manifest_payload)
         VALUES (1,'test',$1,$2,'fixture','active','test','watch-plan-test',$3) RETURNING manifest_id",
    ).bind(family).bind(CHAIN).bind(payload).fetch_one(db.pool()).await?;
    let instance = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind,provenance) VALUES ($1,$2,'contract','{}')")
        .bind(instance).bind(CHAIN).execute(db.pool()).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,source_manifest_id,provenance) VALUES ($1,$2,$3,0,$4,'{}')")
        .bind(instance).bind(CHAIN).bind(WATCHED_ADDRESS).bind(manifest).execute(db.pool()).await?;
    sqlx::query("INSERT INTO manifest_contract_instances (manifest_id,chain_id,declaration_kind,declaration_name,contract_instance_id,declared_address,role,proxy_kind,start_block_number) VALUES ($1,$2,'contract','fixture',$3,$4,'fixture','none',0)")
        .bind(manifest).bind(CHAIN).bind(instance).bind(WATCHED_ADDRESS).execute(db.pool()).await?;
    Ok(db)
}

async fn declared_database(name: &str) -> Result<TestDatabase> {
    database(name, "test_events", json!([{
        "name":"Transfer", "fragment":"event Transfer(address indexed from,address indexed to,uint256 value)",
        "emitter_roles":[],"normalized_events":[]
    }])).await
}

fn transfer_chain(count: i64) -> TestChain {
    let mut chain = TestChain::synthetic(0, count, 64);
    for block in &mut chain.blocks {
        for transaction in &mut block.transactions {
            for log in &mut transaction.logs {
                if log.address == WATCHED_ADDRESS {
                    log.topics[0] = topic(TRANSFER);
                }
            }
        }
    }
    chain
}

async fn engine(db: &TestDatabase, chain: TestChain) -> Result<(Engine, BatchRequest)> {
    let end = chain.blocks.last().unwrap().number;
    let node = test_chain::serve(chain, Tamper::None).await?;
    let engine = Engine::new(db.pool().clone());
    let source = SourceDescriptor {
        key: "rpc".into(),
        kind: "rpc".into(),
        start_block: 0,
        endpoint: "test-node".into(),
    };
    engine.providers.lock().await.insert(
        super::super::provider_key(CHAIN, &source),
        Arc::new(node.provider),
    );
    Ok((
        engine,
        BatchRequest {
            chain_id: CHAIN.into(),
            sources: vec![source],
            cursors: vec![],
            redo_range: Some((0, end)),
            resume_current: None,
        },
    ))
}

#[tokio::test]
async fn configured_windows_keep_normal_and_redo_progress_and_facts_complete() -> Result<()> {
    let db = declared_database("ingest_configured_windows").await?;
    let node = test_chain::serve(transfer_chain(8), Tamper::None).await?;
    let config = crate::IngestConfig::new(3, 2, 2)?;
    let engine = Engine::with_config(db.pool().clone(), config);
    let source = SourceDescriptor {
        key: "rpc".into(),
        kind: "rpc".into(),
        start_block: 0,
        endpoint: node.endpoint.clone(),
    };
    let initial = BatchRequest {
        chain_id: CHAIN.into(),
        sources: vec![source],
        cursors: vec![],
        redo_range: None,
        resume_current: None,
    };
    let mut request = initial.clone();
    for to in [2, 5, 7] {
        let outcome = engine.run_batch(request.clone()).await?;
        assert_eq!(outcome.current.number, to);
        assert_eq!(outcome.target.number, 7);
        assert_eq!(outcome.complete, to == 7);
        request.cursors = vec![crate::SourceCursor {
            key: "rpc".into(),
            next_block: to + 1,
            target_block: Some(7),
            last_processed: Some(outcome.current),
            redo_loaded_boundary: None,
        }];
    }
    // Both ordinary and prepared redo paths must use the same configured window,
    // repeat every block, and retain identical immutable raw facts.
    for prepared in [false, true] {
        let mut request = initial.clone();
        request.redo_range = Some((0, 7));
        for to in [2, 5, 7] {
            let outcome = if prepared {
                engine.run_redo_attempt_batch(request.clone(), 1).await?
            } else {
                engine.run_batch(request.clone()).await?
            };
            assert_eq!(outcome.current.number, to);
            assert_eq!(outcome.complete, to == 7);
            request.resume_current = Some(outcome.current);
        }
    }
    let blocks: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM chain_lineage WHERE chain_id=$1 ORDER BY block_number",
    )
    .bind(CHAIN)
    .fetch_all(db.pool())
    .await?;
    assert_eq!(blocks, (0..8).collect::<Vec<_>>());
    let logs: Vec<i64> =
        sqlx::query_scalar("SELECT log_index FROM raw_logs WHERE chain_id=$1 ORDER BY log_index")
            .bind(CHAIN)
            .fetch_all(db.pool())
            .await?;
    assert_eq!(logs, vec![1, 2, 3]);
    assert!(engine.redo_watch_plans.lock().await.is_empty());
    db.cleanup().await
}

#[tokio::test]
async fn adjacent_redo_batches_reuse_the_plan_and_keep_address_interval_boundaries() -> Result<()> {
    let db = declared_database("redo_watch_reuse").await?;
    sqlx::query(
        "UPDATE contract_instance_addresses SET active_to_block_number=400 WHERE chain_id=$1",
    )
    .bind(CHAIN)
    .execute(db.pool())
    .await?;
    let (engine, mut request) = engine(&db, transfer_chain(512)).await?;
    let first = engine.run_redo_attempt_batch(request.clone(), 1).await?;
    assert_eq!(first.current.number, 255);
    assert!(!first.complete);
    request.resume_current = Some(first.current);

    // Prevent either expensive table scan. The ordinary planner must block, while the next
    // real fetch/write batch must complete using the plan prepared above.
    let mut lock = db.pool().begin().await?;
    sqlx::query("LOCK TABLE discovery_edges, contract_instance_addresses IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await?;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(150),
            load_watch_filter(db.pool(), CHAIN, 256, 511)
        )
        .await
        .is_err()
    );
    let second = tokio::time::timeout(
        Duration::from_secs(10),
        engine.run_redo_attempt_batch(request, 1),
    )
    .await??;
    assert!(second.complete);
    assert_eq!(second.current.number, 511);
    lock.rollback().await?;
    let blocks: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT block_number FROM raw_logs WHERE chain_id=$1 ORDER BY 1",
    )
    .bind(CHAIN)
    .fetch_all(db.pool())
    .await?;
    assert_eq!(blocks, vec![0, 64, 128, 192, 256, 320, 384]);
    assert!(engine.redo_watch_plans.lock().await.is_empty());
    db.cleanup().await
}

#[tokio::test]
async fn a_resumed_attempt_reloads_changed_watch_intervals() -> Result<()> {
    let db = declared_database("redo_watch_resume").await?;
    let (engine, mut request) = engine(&db, transfer_chain(768)).await?;
    let first = engine.run_redo_attempt_batch(request.clone(), 7).await?;
    request.resume_current = Some(first.current);
    // A manifest update between attempts can retire an address without changing this range.
    sqlx::query(
        "UPDATE contract_instance_addresses SET active_to_block_number=300 WHERE chain_id=$1",
    )
    .bind(CHAIN)
    .execute(db.pool())
    .await?;
    let second = engine.run_redo_attempt_batch(request, 8).await?;
    assert_eq!(second.current.number, 511);
    let later: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT block_number FROM raw_logs WHERE chain_id=$1 AND block_number>=256 ORDER BY 1")
        .bind(CHAIN).fetch_all(db.pool()).await?;
    assert_eq!(later, vec![256]);
    db.cleanup().await
}

#[tokio::test]
async fn creation_discovered_during_redo_is_available_to_the_next_cached_window() -> Result<()> {
    let db = database("redo_watch_creation", "ens_v2_resolver_l1", json!([
        {"name":"ResolverCreated","fragment":"event ResolverCreated()","emitter_roles":[],"normalized_events":["ContractDiscovered"]},
        {"name":"TextUpdated","fragment":"event TextUpdated(uint256 indexed recordId,string indexed keyHash,string key,string value)","emitter_roles":[],"normalized_events":["RecordChanged"]}
    ])).await?;
    // This resolver is discovered from its creation log, never from a declared address.
    let created_address = "0x3333333333333333333333333333333333333333";
    let mut chain = TestChain::synthetic(0, 512, 1);
    for block in &mut chain.blocks {
        block.transactions.truncate(1);
        block.transactions[0].logs.clear();
        if matches!(block.number, 10 | 300) {
            block.transactions[0].logs.push(TestLog {
                log_index: 0,
                address: created_address.into(),
                topics: vec![if block.number == 10 {
                    bigname_manifests::resolver_creation_topic0()
                } else {
                    topic("TextUpdated(uint256,string,string,string)")
                }],
                data: "0x".into(),
            });
        }
    }
    // Redo revisits retained canonical blocks while fetching a newly widened event set.
    let creation = &chain.blocks[10];
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,10,to_timestamp(1700000010),'canonical')")
        .bind(CHAIN).bind(&creation.hash).bind(&creation.parent_hash).execute(db.pool()).await?;
    let (engine, mut request) = engine(&db, chain).await?;
    let first = engine.run_redo_attempt_batch(request.clone(), 1).await?;
    request.resume_current = Some(first.current);
    engine.run_redo_attempt_batch(request, 1).await?;
    let blocks: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM raw_logs WHERE chain_id=$1 AND emitting_address=$2 ORDER BY 1",
    )
    .bind(CHAIN)
    .bind(created_address)
    .fetch_all(db.pool())
    .await?;
    assert_eq!(blocks, vec![10, 300]);
    db.cleanup().await
}
