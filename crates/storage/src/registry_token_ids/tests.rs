use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};

const CHAIN: &str = "ethereum-sepolia";

#[tokio::test]
async fn registry_tokens_are_bounded_and_probe_token_index_for_small_and_page_batches() -> Result<()>
{
    let database = TestDatabase::create(TestDatabaseConfig::new("registry_token_ids")).await?;
    let result = check(database.pool()).await;
    database.cleanup().await?;
    result
}

async fn check(pool: &sqlx::PgPool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    // The production index, including its partial predicate and ordering. Unrelated histories
    // make a table scan an actual bad choice, without disabling sequential scans.
    let schema = include_str!("../../schema/baseline/05_normalized_events.sql");
    let index = schema
        .split("CREATE INDEX IF NOT EXISTS normalized_events_registry_token_idx")
        .nth(1)
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    sqlx::raw_sql(&format!(r#"CREATE SCHEMA bigname_phase;
        SET search_path=bigname_phase;
        CREATE TABLE resources (resource_id uuid PRIMARY KEY, chain_id text NOT NULL);
        CREATE TABLE project_family_marker (chain_id text PRIMARY KEY, current_block_number bigint);
        CREATE TABLE chain_lineage (chain_id text, block_hash text, block_number bigint, canonicality_state text,
                                    PRIMARY KEY(chain_id, block_hash));
        CREATE TABLE normalized_events (normalized_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            resource_id uuid, chain_id text, block_hash text, block_number bigint,
            transaction_index bigint, log_index bigint, source_family text, event_kind text,
            consumer_visibility text, canonicality_state text, after_state jsonb);
        CREATE INDEX normalized_events_registry_token_idx {index};
        INSERT INTO resources SELECT lpad(to_hex(n),32,'0')::uuid, '{CHAIN}' FROM generate_series(1,10000) n;
        INSERT INTO project_family_marker VALUES ('{CHAIN}',10);
        INSERT INTO chain_lineage SELECT '{CHAIN}', n::text, n, 'canonical' FROM generate_series(1,13) n;
        INSERT INTO normalized_events (resource_id,chain_id,block_hash,block_number,transaction_index,
            log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state)
        SELECT resource_id,'{CHAIN}', b::text,b,0,0,'ens_v2_registry_l1',
            CASE WHEN b=1 THEN 'TokenResourceLinked' WHEN b=9 THEN 'TokenRegenerated' ELSE 'RecordChanged' END,
            'activated','canonical',CASE WHEN b=1 THEN '{{"token_id":"4294967296"}}'::jsonb
            WHEN b=9 THEN '{{"new_token_id":"0x100000001"}}'::jsonb ELSE '{{}}'::jsonb END
        FROM resources CROSS JOIN generate_series(1,10) b;
        ANALYZE;
        PREPARE registry_tokens(uuid[],jsonb) AS {TOKEN_IDS_SQL};"#))
        .execute(&mut *conn).await?;

    for mode in ["force_generic_plan", "force_custom_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode={mode}"))
            .execute(&mut *conn)
            .await?;
        for count in [1, 200] {
            let ids = (1..=count).map(Uuid::from_u128).collect::<Vec<_>>();
            let literal = ids
                .iter()
                .map(Uuid::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE registry_tokens('{{{literal}}}', '{{\"{CHAIN}\":10}}')"))
                .fetch_one(&mut *conn).await?;
            let mut nodes = Vec::new();
            walk(&plan[0]["Plan"], &mut nodes);
            let probes = nodes
                .iter()
                .filter(|node| node["Relation Name"] == "normalized_events")
                .collect::<Vec<_>>();
            assert_eq!(probes.len(), 1, "{plan}");
            let probe = probes[0];
            assert_eq!(
                probe["Index Name"], "normalized_events_registry_token_idx",
                "{plan}"
            );
            assert!(
                probe["Index Cond"]
                    .as_str()
                    .unwrap()
                    .contains("resource_id ="),
                "{plan}"
            );
            assert!(
                probe["Index Cond"]
                    .as_str()
                    .unwrap()
                    .contains("block_number <="),
                "{plan}"
            );
            assert_eq!(probe["Actual Loops"], count as u64, "{plan}");
            assert!(probe["Actual Rows"].as_f64().unwrap() <= 1.0, "{plan}");
            let rows =
                load_ens_v2_token_ids(&mut *conn, &ids, &BTreeMap::from([(CHAIN.into(), 10)]))
                    .await?;
            assert_eq!(rows.len(), count as usize);
            assert!(rows.values().all(|word| word == "4294967297"));
            println!(
                "registry tokens {mode}, {count} resources: {} ms; probe {probe}",
                plan[0]["Execution Time"]
            );
        }
    }

    let resource = Uuid::from_u128(1);
    assert_eq!(
        load_ens_v2_token_ids(
            &mut *conn,
            &[resource],
            &BTreeMap::from([(CHAIN.into(), 8)])
        )
        .await?[&resource],
        "4294967296"
    );
    // Later activated events cannot pass Project's publication bound. Once published, candidate
    // or orphaned evidence, including a canonical event on orphaned lineage, still cannot win.
    sqlx::raw_sql(&format!(r#"INSERT INTO normalized_events
        (resource_id,chain_id,block_hash,block_number,transaction_index,log_index,source_family,
         event_kind,consumer_visibility,canonicality_state,after_state) VALUES
        ('{resource}','{CHAIN}','11',11,0,0,'ens_v2_registry_l1','TokenRegenerated','activated','canonical','{{"new_token_id":"4294967298"}}'),
        ('{resource}','{CHAIN}','12',12,0,0,'ens_v2_registry_l1','TokenRegenerated','candidate','canonical','{{"new_token_id":"999"}}'),
        ('{resource}','{CHAIN}','12',12,0,1,'ens_v2_registry_l1','TokenRegenerated','activated','orphaned','{{"new_token_id":"999"}}'),
        ('{resource}','{CHAIN}','13',13,0,0,'ens_v2_registry_l1','TokenRegenerated','activated','canonical','{{"new_token_id":"999"}}');
        UPDATE chain_lineage SET canonicality_state='orphaned' WHERE block_hash='13';"#))
        .execute(&mut *conn).await?;
    let bounds = BTreeMap::from([(CHAIN.into(), 13)]);
    assert_eq!(
        load_ens_v2_token_ids(&mut *conn, &[resource], &bounds).await?[&resource],
        "4294967297"
    );
    sqlx::query("UPDATE bigname_phase.project_family_marker SET current_block_number=13")
        .execute(&mut *conn)
        .await?;
    assert_eq!(
        load_ens_v2_token_ids(&mut *conn, &[resource], &bounds).await?[&resource],
        "4294967298"
    );
    assert!(
        load_ens_v2_token_ids(
            &mut *conn,
            &[resource],
            &BTreeMap::from([("base-sepolia".into(), 13)])
        )
        .await?
        .is_empty()
    );
    // Historical payloads need only their actual token word, not a current-pin field name.
    sqlx::query("UPDATE bigname_phase.normalized_events SET event_kind='TokenResourceLinked',after_state=$1 WHERE resource_id=$2 AND block_number=11")
        .bind(json!({"token_id":"0x100000002"})).bind(resource).execute(&mut *conn).await?;
    assert_eq!(
        load_ens_v2_token_ids(&mut *conn, &[resource], &bounds).await?[&resource],
        "4294967298"
    );
    // A stale cached grant or another source family is not a competing token authority.
    sqlx::query("INSERT INTO normalized_events (resource_id,chain_id,block_hash,block_number,transaction_index,log_index,source_family,event_kind,consumer_visibility,canonicality_state,after_state) VALUES ($1,$2,'12',12,0,2,'ens_v2_registry_l1','RegistrationGranted','activated','canonical','{\"token_id\":\"999\"}'), ($1,$2,'12',12,0,3,'ens_v2_registrar_l1','TokenRegenerated','activated','canonical','{\"new_token_id\":\"999\"}')")
        .bind(resource).bind(CHAIN).execute(&mut *conn).await?;
    assert_eq!(
        load_ens_v2_token_ids(&mut *conn, &[resource], &bounds).await?[&resource],
        "4294967298"
    );
    // A latest event with a missing or malformed token fails closed; the reader never searches
    // back to the prior valid token. Missing evidence, however, legitimately omits the field.
    for payload in [
        json!({}),
        json!({"token_id":"not-a-token"}),
        json!({"token_id":"0x1".to_owned()+&"0".repeat(64)}),
    ] {
        sqlx::query(
            "UPDATE normalized_events SET after_state=$1 WHERE resource_id=$2 AND block_number=11",
        )
        .bind(payload)
        .bind(resource)
        .execute(&mut *conn)
        .await?;
        assert!(
            load_ens_v2_token_ids(&mut *conn, &[resource], &bounds)
                .await
                .is_err()
        );
    }
    sqlx::query("DELETE FROM normalized_events WHERE resource_id=$1 AND event_kind IN ('TokenResourceLinked','TokenRegenerated')")
        .bind(resource).execute(&mut *conn).await?;
    assert!(
        load_ens_v2_token_ids(&mut *conn, &[resource], &bounds)
            .await?
            .is_empty()
    );
    // Deduplication and internal chunking also hold across the 200-key boundary.
    let ids = (2..=402)
        .chain(2..=402)
        .map(Uuid::from_u128)
        .collect::<Vec<_>>();
    assert_eq!(
        load_ens_v2_token_ids(&mut *conn, &ids, &bounds)
            .await?
            .len(),
        401
    );
    Ok(())
}

fn walk<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
    nodes.push(node);
    for child in node["Plans"].as_array().into_iter().flatten() {
        walk(child, nodes);
    }
}
