use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

const CHAIN: &str = "ethereum-sepolia";

#[tokio::test]
async fn history_token_ids_follow_event_positions_and_index_seek() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_token_ids")).await?;
    let result = check(database.pool()).await;
    database.cleanup().await?;
    result
}

async fn check(pool: &sqlx::PgPool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    let index = include_str!("../../../schema/baseline/05_normalized_events.sql")
        .split("CREATE INDEX IF NOT EXISTS normalized_events_registry_token_idx")
        .nth(1)
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    sqlx::raw_sql(&format!(r#"
        CREATE SCHEMA bigname_phase;
        SET search_path=bigname_phase;
        CREATE TABLE manifest_versions (manifest_id bigint PRIMARY KEY, chain_id text,
            source_family text, manifest_version bigint, deployment_label text);
        INSERT INTO manifest_versions VALUES (1,'{CHAIN}','ens_v2_registry_l1',2,'ens_v2_sepolia_20261001');
        CREATE TABLE chain_lineage (chain_id text, block_hash text, block_number bigint,
            canonicality_state text, PRIMARY KEY(chain_id,block_hash));
        INSERT INTO chain_lineage VALUES ('{CHAIN}','10',10,'canonical'),('{CHAIN}','other',10,'canonical');
        CREATE TABLE normalized_events (normalized_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source_manifest_id bigint, manifest_version bigint, resource_id uuid, chain_id text,
            block_hash text, block_number bigint, transaction_index bigint, log_index bigint,
            source_family text, event_kind text, consumer_visibility text, canonicality_state text, after_state jsonb);
        CREATE INDEX normalized_events_registry_token_idx {index};
        INSERT INTO normalized_events (source_manifest_id,manifest_version,resource_id,chain_id,block_hash,
            block_number,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state)
        SELECT 1,2,lpad(to_hex(r),32,'0')::uuid,'{CHAIN}','10',10,0,n*2,'ens_v2_registry_l1',
            'TokenRegenerated','activated','canonical',jsonb_build_object('new_token_id',(4294967296+n)::text)
        FROM generate_series(1,2000) r CROSS JOIN generate_series(1,50) n;
        INSERT INTO normalized_events (source_manifest_id,manifest_version,resource_id,chain_id,block_hash,
            block_number,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state)
        SELECT 1,2,lpad(to_hex(1),32,'0')::uuid,'{CHAIN}','10',10,0,n,'ens_v2_registry_l1',
            'TokenRegenerated','activated','canonical','{{"new_token_id":"999"}}'
        FROM generate_series(200,20200) n;
        ANALYZE;
        PREPARE history_tokens(jsonb,jsonb) AS {TOKEN_IDS_SQL};
    "#)).execute(&mut *conn).await?;
    let mut rows = Vec::new();
    for resource in 1..=200 {
        let id: i64 = sqlx::query_scalar("INSERT INTO normalized_events (source_manifest_id,manifest_version,resource_id,chain_id,block_hash,block_number,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state) VALUES (1,2,$1,$2,'10',10,0,21,'ens_v2_registry_l1','PermissionChanged','activated','canonical','{}') RETURNING normalized_event_id")
            .bind(Uuid::from_u128(resource)).bind(CHAIN).fetch_one(&mut *conn).await?;
        rows.push(row(id, resource, 0, 21));
    }
    let bounds = BTreeMap::from([(CHAIN.to_owned(), 10)]);
    for mode in ["force_generic_plan", "force_custom_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode={mode}"))
            .execute(&mut *conn)
            .await?;
        for count in [1, 200] {
            let targets = rows[..count].iter().map(target).collect::<Vec<_>>();
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE history_tokens('{}','{{\"{CHAIN}\":10}}')",
                serde_json::to_string(&targets)?)).fetch_one(&mut *conn).await?;
            let mut nodes = Vec::new();
            walk(&plan[0]["Plan"], &mut nodes);
            let probe = nodes
                .iter()
                .find(|node| node["Index Name"] == "normalized_events_registry_token_idx")
                .with_context(|| format!("missing token probe: {plan}"))?;
            let condition = probe["Index Cond"].as_str().unwrap();
            assert!(
                condition.contains("resource_id =") && condition.contains("ROW("),
                "{plan}"
            );
            assert_eq!(probe["Actual Loops"], count as u64, "{plan}");
            assert!(probe["Actual Rows"].as_f64().unwrap() <= 1.0, "{plan}");
            assert!(
                !nodes.iter().any(|node| node["Node Type"] == "Sort"),
                "{plan}"
            );
            let values = load_history_token_ids(&mut *conn, &rows[..count], &bounds).await?;
            assert_eq!(values.len(), count);
            assert!(values.values().all(|value| value == "4294967306"));
            println!("history token plan {mode}, {count} targets: {plan}");
        }
    }
    // Same resource at a later log selects its distinct event-time token.
    let mut later = rows[0].clone();
    later.normalized_event_id = sqlx::query_scalar("INSERT INTO normalized_events (source_manifest_id,manifest_version,resource_id,chain_id,block_hash,block_number,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state) SELECT source_manifest_id,manifest_version,resource_id,chain_id,block_hash,block_number,0,31,source_family,event_kind,consumer_visibility,canonicality_state,after_state FROM normalized_events WHERE normalized_event_id=$1 RETURNING normalized_event_id")
        .bind(rows[0].normalized_event_id).fetch_one(&mut *conn).await?;
    later.log_index = Some(31);
    let values = load_history_token_ids(
        &mut *conn,
        &[rows[0].clone(), later.clone(), rows[0].clone()],
        &bounds,
    )
    .await?;
    assert_eq!(values.len(), 2);
    assert_eq!(values[&rows[0].normalized_event_id], "4294967306");
    assert_eq!(values[&later.normalized_event_id], "4294967311");

    // A fork at the same height, candidate or unreadable evidence cannot win.
    for (hash, visibility, canonicality) in [
        ("other", "activated", "canonical"),
        ("10", "candidate", "canonical"),
        ("10", "activated", "orphaned"),
    ] {
        sqlx::query("INSERT INTO normalized_events (resource_id,chain_id,block_hash,block_number,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state) VALUES ($1,$2,$3,10,0,20,'ens_v2_registry_l1','TokenRegenerated',$4,$5,'{\"new_token_id\":\"777\"}')")
            .bind(Uuid::from_u128(1)).bind(CHAIN).bind(hash).bind(visibility).bind(canonicality).execute(&mut *conn).await?;
    }
    assert_eq!(
        load_history_token_ids(&mut *conn, &rows[..1], &bounds).await?
            [&rows[0].normalized_event_id],
        "4294967306"
    );
    assert!(
        load_history_token_ids(&mut *conn, &rows[..1], &BTreeMap::from([(CHAIN.into(), 9)]))
            .await?
            .is_empty()
    );
    for label in ["ens_v2_sepolia_dev", "unknown"] {
        sqlx::query("UPDATE manifest_versions SET deployment_label=$1")
            .bind(label)
            .execute(&mut *conn)
            .await?;
        assert!(
            load_history_token_ids(&mut *conn, &rows[..1], &bounds)
                .await?
                .is_empty()
        );
    }
    sqlx::query("UPDATE manifest_versions SET deployment_label='ens_v2_sepolia_post_audit'")
        .execute(&mut *conn)
        .await?;
    assert_eq!(
        load_history_token_ids(&mut *conn, &rows[..1], &bounds)
            .await?
            .len(),
        1
    );
    // Legacy marker token_id is accepted. Missing/malformed latest data never falls back.
    for (payload, valid) in [
        (json!({"token_id":"0x10000000a"}), true),
        (json!({}), false),
        (json!({"token_id":"wrong"}), false),
        (json!({"token_id":42}), false),
        (json!({"token_id":"1_000"}), false),
        (json!({"token_id":format!("0x1{}","0".repeat(64))}), false),
    ] {
        sqlx::query("UPDATE normalized_events SET event_kind='TokenResourceLinked',after_state=$1 WHERE resource_id=$2 AND log_index=20 AND block_hash='10' AND consumer_visibility='activated' AND canonicality_state='canonical'")
            .bind(payload).bind(Uuid::from_u128(1)).execute(&mut *conn).await?;
        assert_eq!(
            load_history_token_ids(&mut *conn, &rows[..1], &bounds)
                .await
                .is_ok(),
            valid
        );
    }
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE block_hash='10'")
        .execute(&mut *conn)
        .await?;
    assert!(
        load_history_token_ids(&mut *conn, &rows[..1], &bounds)
            .await?
            .is_empty()
    );

    // A direct grant before linkage needs neither resource nor marker/manifest evidence.
    // Dropping both sources is a SQL tripwire: these direct-only requests must issue no
    // predecessor/correlation query, rather than obtaining the right value by an extra read.
    sqlx::raw_sql(
        "DROP TABLE normalized_events; DROP TABLE manifest_versions; DROP TABLE chain_lineage;",
    )
    .execute(&mut *conn)
    .await?;
    let mut direct = rows[0].clone();
    direct.resource_id = None;
    direct.event_kind = "RegistrationGranted".into();
    direct.after_state = json!({"token_id":format!("0x{:064x}", U256::MAX)});
    assert_eq!(
        load_history_token_ids(&mut *conn, &[direct.clone()], &bounds).await?
            [&direct.normalized_event_id],
        U256::MAX.to_string()
    );
    direct.source_family = "ens_v1_registrar_l1".into();
    direct.after_state = json!({"source_event":"NameRegistered","cost":"0"});
    assert!(
        crate::load_history_payment_values(&mut *conn, std::slice::from_ref(&direct), &bounds)
            .await?
            .is_empty()
    );
    assert!(
        load_history_token_ids(&mut *conn, &[direct], &bounds)
            .await?
            .is_empty()
    );
    Ok(())
}

fn target(row: &HistoryEvent) -> Value {
    json!({"event_id":row.normalized_event_id,"chain_id":row.chain_id,"resource_id":row.resource_id,
        "block_number":row.block_number,"block_hash":row.block_hash,"transaction_index":row.transaction_index,"log_index":row.log_index})
}

fn row(id: i64, resource: u128, tx: i64, log: i64) -> HistoryEvent {
    HistoryEvent {
        normalized_event_id: id,
        event_identity: id.to_string(),
        namespace: "ens".into(),
        logical_name_id: None,
        resource_id: Some(Uuid::from_u128(resource)),
        registration_id: None,
        event_kind: "PermissionChanged".into(),
        source_family: "ens_v2_registry_l1".into(),
        manifest_version: 2,
        source_manifest_id: Some(1),
        chain_id: Some(CHAIN.into()),
        block_number: Some(10),
        block_hash: Some("10".into()),
        block_timestamp: None,
        transaction_hash: Some("tx".into()),
        transaction_index: Some(tx),
        log_index: Some(log),
        raw_fact_ref: json!({}),
        derivation_kind: "direct".into(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({}),
        migration_correlation_ids: vec![],
        consumer_visibility: "activated".into(),
        migration_associations: json!([]),
        provenance: json!({}),
        coverage: json!({}),
    }
}

fn walk<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
    nodes.push(node);
    for child in node["Plans"].as_array().into_iter().flatten() {
        walk(child, nodes);
    }
}
