use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::Connection;

const CHAIN: &str = "ethereum-sepolia";

#[tokio::test]
async fn history_payment_values_require_unique_action_evidence_and_bounded_probes() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_payment_values")).await?;
    let result = check(database.pool()).await;
    database.cleanup().await?;
    result
}

async fn check(pool: &sqlx::PgPool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    let index = include_str!("../../../schema/baseline/05_normalized_events.sql")
        .split("CREATE INDEX IF NOT EXISTS normalized_events_block_idx")
        .nth(1)
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    sqlx::raw_sql(&format!(r#"
        CREATE SCHEMA bigname_phase;
        SET search_path=bigname_phase;
        CREATE TABLE chain_lineage(chain_id text,block_hash text,block_number bigint,canonicality_state text,PRIMARY KEY(chain_id,block_hash));
        INSERT INTO chain_lineage VALUES ('{CHAIN}','10',10,'canonical');
        CREATE TABLE manifest_contract_instances(manifest_id bigint,chain_id text,declaration_kind text,role text,declared_address text,start_block_number bigint);
        INSERT INTO manifest_contract_instances VALUES (1,'{CHAIN}','contract','registrar','registrar',0),(1,'{CHAIN}','contract','wrapped_registrar_controller','controller',0);
        CREATE TABLE normalized_events(normalized_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,event_identity text,namespace text,logical_name_id text,
            resource_id uuid,source_manifest_id bigint,manifest_version bigint,chain_id text,block_number bigint,block_hash text,transaction_hash text,
            transaction_index bigint,log_index bigint,source_family text,event_kind text,consumer_visibility text,canonicality_state text,after_state jsonb,raw_fact_ref jsonb);
        CREATE INDEX normalized_events_block_idx {index};
        INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash,
            transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref)
        SELECT 'target-'||n,'ens','ens:name-'||n,NULL,1,2,'{CHAIN}',10,'10','tx-'||n,n,0,'ens_v2_registry_l1','RegistrationGranted','activated','canonical',
            jsonb_build_object('token_id','0x'||lpad(to_hex(n),64,'0'),'source_event','LabelRegistered','sender','registrar'), '{{"emitting_address":"registry"}}'
        FROM generate_series(1,200) n;
        INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash,
            transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref)
        SELECT 'link-'||n,'ens','ens:name-'||n,lpad(to_hex(n),32,'0')::uuid,1,2,'{CHAIN}',10,'10','tx-'||n,n,1,'ens_v2_registry_l1','TokenResourceLinked','activated','canonical',
            jsonb_build_object('current_token_id','0x'||lpad(to_hex(n),64,'0')), '{{"emitting_address":"registry"}}'
        FROM generate_series(1,200) n;
        INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash,
            transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref)
        SELECT 'payment-'||n,'ens','ens:name-'||n,lpad(to_hex(n),32,'0')::uuid,2,2,'{CHAIN}',10,'10','tx-'||n,n,2,'ens_v2_registrar_l1','RegistrarNameRegistered','activated','canonical',
            jsonb_build_object('token_id','0x'||lpad(to_hex(n),64,'0'),'source_event','NameRegistered','base',n::text,'premium','0'), '{{"emitting_address":"registrar"}}'
        FROM generate_series(1,200) n;
        INSERT INTO normalized_events(event_identity,namespace,logical_name_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash,
            transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref)
        SELECT 'noise-'||n,'ens','ens:noise',1,2,'{CHAIN}',10,'10','noise-'||n,1000+n,0,'ens_v2_registry_l1','PermissionChanged','activated','canonical','{{}}','{{}}'
        FROM generate_series(1,100000) n;
        ANALYZE;
        PREPARE history_payments(jsonb,jsonb) AS {PAYMENT_VALUES_SQL};
    "#)).execute(&mut *conn).await?;
    let targets:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('event_id',normalized_event_id,'event_identity',event_identity,'chain_id',chain_id,'block_number',block_number,'block_hash',block_hash,'transaction_hash',transaction_hash,'transaction_index',transaction_index,'log_index',log_index) FROM normalized_events WHERE event_identity LIKE 'target-%' ORDER BY normalized_event_id")
        .fetch_all(&mut *conn).await?;
    let bounds = json!({CHAIN:10});
    assert_plans(
        &mut conn,
        &targets,
        &bounds,
        "100k unrelated, separate transactions",
    )
    .await?;
    // Pack all 200 independent payments into one dense transaction. Adding more unrelated
    // transactions must still leave every evidence probe constrained to this transaction.
    let mut dense = conn.begin().await?;
    sqlx::raw_sql("UPDATE normalized_events SET log_index=(transaction_index-1)*3+log_index,transaction_index=1,transaction_hash='dense' WHERE transaction_index<=200")
        .execute(&mut *dense).await?;
    let dense_targets = targets
        .iter()
        .enumerate()
        .map(|(n, target)| {
            let mut target = target.clone();
            target["transaction_index"] = json!(1);
            target["transaction_hash"] = json!("dense");
            target["log_index"] = json!(n * 3);
            target
        })
        .collect::<Vec<_>>();
    assert_plans(
        &mut dense,
        &dense_targets,
        &bounds,
        "100k unrelated, dense transaction",
    )
    .await?;
    sqlx::raw_sql("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref) SELECT event_identity||'-more',namespace,logical_name_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash||'-more',transaction_index+100000,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref FROM normalized_events WHERE event_identity LIKE 'noise-%'; ANALYZE normalized_events;")
        .execute(&mut *dense).await?;
    assert_plans(
        &mut dense,
        &dense_targets,
        &bounds,
        "200k unrelated, dense transaction",
    )
    .await?;
    dense.rollback().await?;
    // Each physical transaction owns its price even when many actions are in one block.
    // Ambiguous, early, mismatched, candidate and orphaned observations never guess a price.
    let original: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events WHERE event_identity='payment-1'",
    )
    .fetch_one(&mut *conn)
    .await?;
    for mutation in [
        "log_index=-1",
        "transaction_hash='wrong'",
        "transaction_index=999",
        "block_hash='fork'",
        "logical_name_id='ens:other'",
        "resource_id=lpad('999',32,'0')::uuid",
        "consumer_visibility='candidate'",
        "canonicality_state='orphaned'",
        "after_state=jsonb_set(after_state,'{token_id}','\"0x999\"')",
        "raw_fact_ref='{}'::jsonb",
    ] {
        let mut tx = conn.begin().await?;
        sqlx::raw_sql(&format!(
            "UPDATE normalized_events SET {mutation} WHERE event_identity='payment-1'"
        ))
        .execute(&mut *tx)
        .await?;
        let values: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
            .bind(json!(&targets[..1]))
            .bind(&bounds)
            .fetch_all(&mut *tx)
            .await?;
        assert!(values.is_empty(), "{mutation}: {values:?}");
        tx.rollback().await?;
    }
    for mutation in [
        "event_kind='Other'",
        "log_index=3",
        "resource_id=lpad('999',32,'0')::uuid",
        "source_manifest_id=99",
        "raw_fact_ref='{}'::jsonb",
    ] {
        let mut tx = conn.begin().await?;
        sqlx::raw_sql(&format!(
            "UPDATE normalized_events SET {mutation} WHERE event_identity='link-1'"
        ))
        .execute(&mut *tx)
        .await?;
        let values: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
            .bind(json!(&targets[..1]))
            .bind(&bounds)
            .fetch_all(&mut *tx)
            .await?;
        assert!(values.is_empty(), "{mutation}: {values:?}");
        tx.rollback().await?;
    }
    for kind in ["payment", "link"] {
        let mut tx = conn.begin().await?;
        sqlx::raw_sql(&format!("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref) SELECT event_identity,namespace,logical_name_id,resource_id,source_manifest_id,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state,raw_fact_ref FROM normalized_events WHERE event_identity='{kind}-1'"))
            .execute(&mut *tx).await?;
        let values: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
            .bind(json!(&targets[..1]))
            .bind(&bounds)
            .fetch_all(&mut *tx)
            .await?;
        assert!(values.is_empty(), "duplicate {kind}: {values:?}");
        tx.rollback().await?;
    }
    assert_eq!(original["base"], "1");
    check_renewal(&mut conn, &targets[0], &bounds).await?;
    Ok(())
}

async fn check_renewal(
    conn: &mut sqlx::PgConnection,
    target: &Value,
    bounds: &Value,
) -> Result<()> {
    sqlx::raw_sql("UPDATE normalized_events SET source_family='ens_v1_registrar_l1',event_kind='RegistrationRenewed',raw_fact_ref='{\"emitting_address\":\"registrar\"}',after_state='{\"source_event\":\"NameRenewed\",\"namehash\":\"name\",\"labelhash\":\"label\",\"expiry\":\"99\"}' WHERE event_identity='target-1'; UPDATE normalized_events SET source_family='ens_v1_registrar_l1',source_manifest_id=1,event_kind='PreimageObserved',raw_fact_ref='{\"emitting_address\":\"controller\"}',after_state='{\"source_event\":\"NameRenewed\",\"namehash\":\"name\",\"labelhash\":\"label\",\"expiry\":\"99\",\"cost\":\"0\"}' WHERE event_identity='payment-1';")
        .execute(&mut *conn).await?;
    let read = |target: &Value| json!([target]);
    let values: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
        .bind(read(target))
        .bind(bounds)
        .fetch_all(&mut *conn)
        .await?;
    assert_eq!(values, vec![(1, json!({"cost":"0"}))]);
    for mutation in [
        "source_manifest_id=99",
        "manifest_version=99",
        "raw_fact_ref='{\"emitting_address\":\"undeclared\"}'::jsonb",
        "after_state=jsonb_set(after_state,'{expiry}','\"100\"')",
    ] {
        let mut tx = conn.begin().await?;
        sqlx::raw_sql(&format!(
            "UPDATE normalized_events SET {mutation} WHERE event_identity='payment-1'"
        ))
        .execute(&mut *tx)
        .await?;
        let values: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
            .bind(read(target))
            .bind(bounds)
            .fetch_all(&mut *tx)
            .await?;
        assert!(values.is_empty(), "{mutation}: {values:?}");
        tx.rollback().await?;
    }
    // A second numeric renewal is the nearest preceding action for the controller log.
    sqlx::raw_sql("UPDATE normalized_events SET source_family='ens_v1_registrar_l1',source_manifest_id=1,event_kind='RegistrationRenewed',raw_fact_ref='{\"emitting_address\":\"registrar\"}',after_state='{\"source_event\":\"NameRenewed\",\"labelhash\":\"label\"}' WHERE event_identity='link-1'")
        .execute(&mut *conn).await?;
    let values: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
        .bind(read(target))
        .bind(bounds)
        .fetch_all(&mut *conn)
        .await?;
    assert!(values.is_empty());
    Ok(())
}

fn walk<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
    nodes.push(node);
    for child in node["Plans"].as_array().into_iter().flatten() {
        walk(child, nodes);
    }
}

async fn assert_plans(
    conn: &mut sqlx::PgConnection,
    targets: &[Value],
    bounds: &Value,
    label: &str,
) -> Result<()> {
    for mode in ["force_generic_plan", "force_custom_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode={mode}"))
            .execute(&mut *conn)
            .await?;
        for count in [1, 200] {
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE history_payments('{}','{bounds}')",
                serde_json::to_string(&targets[..count])?
            ))
            .fetch_one(&mut *conn)
            .await?;
            let mut nodes = Vec::new();
            walk(&plan[0]["Plan"], &mut nodes);
            let probes = nodes
                .iter()
                .filter(|n| n["Index Name"] == "normalized_events_block_idx")
                .collect::<Vec<_>>();
            assert!(!probes.is_empty(), "{plan}");
            assert!(
                probes.iter().all(|n| n["Index Cond"]
                    .as_str()
                    .unwrap()
                    .contains("transaction_index =")),
                "{plan}"
            );
            assert!(
                !nodes
                    .iter()
                    .any(|n| n["Node Type"] == "Seq Scan"
                        && n["Relation Name"] == "normalized_events"),
                "{plan}"
            );
            let values: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
                .bind(json!(&targets[..count]))
                .bind(bounds)
                .fetch_all(&mut *conn)
                .await?;
            assert_eq!(values.len(), count);
            for (id, value) in values {
                assert_eq!(value, json!({"base":id.to_string(),"premium":"0"}));
            }
            println!("history payment plan {label}, {mode}, {count} targets: {plan}");
        }
    }
    Ok(())
}
