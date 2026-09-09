use super::{Row, restore_statement};
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, STATE_SCOPE_KEY, SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, PgPool};

type Result<T = ()> = anyhow::Result<T>;

// Frozen original selection from 9fbdd70f, independent of the candidate builder.
fn reference_statement() -> String {
    format!(
        "
        WITH ranked AS (
            SELECT event.*,
                   live_lineage.block_timestamp AS retained_block_timestamp,
                   row_number() OVER (
                       PARTITION BY
                           event.raw_fact_ref ? '{INTERPRETER_STATE_KEY}',
                           public.digest(
                               COALESCE(
                                   event.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
                                   event.event_identity
                               ),
                               'sha256'
                           ),
                           COALESCE(
                               event.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
                               event.event_identity
                           ),
                           event.after_state ? '{SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY}'
                       ORDER BY event.block_number DESC,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS state_rank
            FROM normalized_events event
            JOIN chain_lineage live_lineage
              ON live_lineage.chain_id = event.chain_id
             AND live_lineage.block_hash = event.block_hash
             AND live_lineage.block_number = event.block_number
             AND live_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            WHERE event.chain_id = $1
              AND event.block_number < $2
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        SELECT ranked.chain_id,
               ranked.namespace,
               ranked.logical_name_id,
               ranked.resource_id,
               ranked.event_kind,
               ranked.source_family,
               ranked.manifest_version,
               ranked.source_manifest_id,
               ranked.raw_fact_ref ->> 'emitting_address',
               ranked.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
               ranked.event_identity,
               ranked.raw_fact_ref ->> '{STATE_SCOPE_KEY}',
               ranked.block_number,
               ranked.block_hash,
               ranked.retained_block_timestamp,
               ranked.after_state
        FROM ranked
        WHERE ranked.state_rank = 1
        ORDER BY ranked.block_number, ranked.normalized_event_id
        "
    )
}

async fn database() -> Result<TestDatabase> {
    let db = TestDatabase::create(TestDatabaseConfig::new("restore_narrow_winners")).await?;
    for sql in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(sql).execute(db.pool()).await?;
    }
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) SELECT 'restore-test','block-'||n,n,to_timestamp(n),'canonical' FROM generate_series(1,4) n")
        .execute(db.pool()).await?;
    Ok(db)
}

async fn seed(
    pool: &PgPool,
    identity: &str,
    key: Value,
    after: Value,
    block: i64,
    position: Option<i64>,
) -> Result {
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','RecordChanged','test',1,'restore-test',$2,'block-'||$2::text,'tx',$3,$3,$4,'ens_v2_resolver','canonical',$5)")
        .bind(identity).bind(block).bind(position).bind(key).bind(after).execute(pool).await?;
    Ok(())
}

async fn compare(pool: &PgPool, cutoff: i64) -> Result<Vec<Row>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let original = sqlx::query_as::<_, Row>(&reference_statement())
        .bind("restore-test")
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await?;
    let candidate = sqlx::query_as::<_, Row>(&restore_statement())
        .bind("restore-test")
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(original.len(), candidate.len());
    for (left, right) in original.iter().zip(&candidate) {
        assert_eq!(left.0, right.0);
        assert_eq!(left.1, right.1);
        assert_eq!(left.2, right.2);
        assert_eq!(left.3, right.3);
        assert_eq!(left.4, right.4);
        assert_eq!(left.5, right.5);
        assert_eq!(left.6, right.6);
        assert_eq!(left.7, right.7);
        assert_eq!(left.8, right.8);
        assert_eq!(left.9, right.9);
        assert_eq!(left.10, right.10);
        assert_eq!(left.11, right.11);
        assert_eq!(left.12, right.12);
        assert_eq!(left.13, right.13);
        assert_eq!(left.14, right.14);
        assert_eq!(left.15, right.15);
    }
    tx.commit().await?;
    Ok(candidate)
}

#[tokio::test]
async fn exact_partitions_positions_and_cutoff_keep_expected_winners() -> Result {
    let db = database().await?;
    seed(db.pool(), "fallback", json!({}), json!({}), 1, None).await?;
    seed(
        db.pool(),
        "explicit",
        json!({INTERPRETER_STATE_KEY:"fallback"}),
        json!({}),
        1,
        None,
    )
    .await?;
    seed(
        db.pool(),
        "null-key",
        json!({INTERPRETER_STATE_KEY:null}),
        json!({}),
        1,
        None,
    )
    .await?;
    seed(
        db.pool(),
        "empty-old",
        json!({INTERPRETER_STATE_KEY:""}),
        json!({}),
        1,
        None,
    )
    .await?;
    seed(
        db.pool(),
        "empty-new",
        json!({INTERPRETER_STATE_KEY:""}),
        json!({}),
        2,
        None,
    )
    .await?;
    seed(
        db.pool(),
        "ordinary-old",
        json!({INTERPRETER_STATE_KEY:"shared"}),
        json!({}),
        1,
        None,
    )
    .await?;
    seed(
        db.pool(),
        "ordinary-null-position",
        json!({INTERPRETER_STATE_KEY:"shared"}),
        json!({}),
        2,
        None,
    )
    .await?;
    seed(
        db.pool(),
        "ordinary-position",
        json!({INTERPRETER_STATE_KEY:"shared"}),
        json!({}),
        2,
        Some(0),
    )
    .await?;
    seed(
        db.pool(),
        "ordinary-id-tie",
        json!({INTERPRETER_STATE_KEY:"shared"}),
        json!({}),
        2,
        Some(0),
    )
    .await?;
    for (id, marker) in [
        ("clear-null", Value::Null),
        ("clear-false", json!(false)),
        ("clear-empty", json!([])),
    ] {
        seed(
            db.pool(),
            id,
            json!({INTERPRETER_STATE_KEY:"shared"}),
            json!({SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY:marker}),
            2,
            Some(0),
        )
        .await?;
    }
    seed(
        db.pool(),
        "cutoff",
        json!({INTERPRETER_STATE_KEY:"shared"}),
        json!({}),
        3,
        Some(1),
    )
    .await?;
    let rows = compare(db.pool(), 3).await?;
    assert_eq!(
        rows.iter().map(|r| r.10.as_str()).collect::<Vec<_>>(),
        [
            "fallback",
            "explicit",
            "null-key",
            "empty-new",
            "ordinary-id-tie",
            "clear-empty"
        ]
    );
    seed(
        db.pool(),
        "explicit-null-fallback",
        json!({INTERPRETER_STATE_KEY:"null-key"}),
        json!({}),
        2,
        Some(0),
    )
    .await?;
    let grouped = compare(db.pool(), 3).await?;
    assert!(!grouped.iter().any(|row| row.10 == "null-key"));
    assert!(grouped.iter().any(|row| row.10 == "explicit-null-fallback"));
    assert!(compare(db.pool(), 1).await?.is_empty());
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ordered_wide_winners_cross_application_chunk_boundaries() -> Result {
    let db = database().await?;
    for n in 0..2050 {
        // Insert later blocks first so ID order alone would be wrong.
        seed(
            db.pool(),
            &format!("wide-{n}"),
            json!({INTERPRETER_STATE_KEY:format!("key-{n}")}),
            json!({"payload":"x".repeat(2048)}),
            if n < 1025 { 2 } else { 1 },
            Some(0),
        )
        .await?;
    }
    let rows = compare(db.pool(), 3).await?;
    assert_eq!(rows.len(), 2050);
    assert_eq!(rows[0].10, "wide-1025");
    assert_eq!(rows[1024].10, "wide-2049");
    assert_eq!(rows[1025].10, "wide-0");
    db.cleanup().await?;
    Ok(())
}

#[test]
fn original_text_remains_in_the_window_partition() {
    let query = restore_statement();
    let partition = query
        .split("PARTITION BY")
        .nth(1)
        .unwrap()
        .split("ORDER BY")
        .next()
        .unwrap();
    assert_eq!(partition.matches("COALESCE(").count(), 2);
    assert!(partition.contains("event.event_identity"));
}

#[tokio::test]
async fn snapshot_and_lineage_eligibility_preserve_the_old_winner() -> Result {
    let db = database().await?;
    seed(
        db.pool(),
        "old",
        json!({INTERPRETER_STATE_KEY:"k"}),
        json!({"v":1}),
        1,
        None,
    )
    .await?;
    seed(
        db.pool(),
        "unreadable-event",
        json!({INTERPRETER_STATE_KEY:"k"}),
        json!({}),
        2,
        None,
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET canonicality_state='orphaned' WHERE event_identity='unreadable-event'").execute(db.pool()).await?;
    seed(
        db.pool(),
        "unreadable-lineage",
        json!({INTERPRETER_STATE_KEY:"k"}),
        json!({}),
        3,
        None,
    )
    .await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE block_number=3")
        .execute(db.pool())
        .await?;
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ('other-chain','other-block',2,to_timestamp(2),'canonical')").execute(db.pool()).await?;
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ('other-chain-event','ens','RecordChanged','test',1,'other-chain',2,'other-block',jsonb_build_object($1::text,'k'),'ens_v2_resolver','canonical','{}')").bind(INTERPRETER_STATE_KEY).execute(db.pool()).await?;
    let rows = compare(db.pool(), 4).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].10, "old");
    let mut tx = db.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let before = sqlx::query_as::<_, Row>(&reference_statement())
        .bind("restore-test")
        .bind(4_i64)
        .fetch_all(&mut *tx)
        .await?;
    // A newer event and an orphaned old lineage commit outside the established snapshot.
    seed(
        db.pool(),
        "new",
        json!({INTERPRETER_STATE_KEY:"k"}),
        json!({"v":2}),
        2,
        Some(0),
    )
    .await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE block_number=1")
        .execute(db.pool())
        .await?;
    let after = sqlx::query_as::<_, Row>(&restore_statement())
        .bind("restore-test")
        .bind(4_i64)
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(before.len(), after.len());
    assert_eq!(after[0].15, json!({"v":1}));
    tx.commit().await?;
    assert_eq!(compare(db.pool(), 4).await?[0].15, json!({"v":2}));
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn exact_result_boundaries_and_all_readable_states() -> Result {
    let db = database().await?;
    for count in [0_i64, 1, 1024, 1025] {
        sqlx::query("DELETE FROM normalized_events")
            .execute(db.pool())
            .await?;
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,raw_fact_ref,derivation_kind,canonicality_state,after_state) SELECT 'boundary-'||n,'ens','RecordChanged','test',1,'restore-test',1,'block-1',jsonb_build_object($2::text,'key-'||n),'ens_v2_resolver',(CASE WHEN n%3=0 THEN 'canonical' WHEN n%3=1 THEN 'safe' ELSE 'finalized' END)::canonicality_state,jsonb_build_object('n',n) FROM generate_series(1,$1) n")
            .bind(count).bind(INTERPRETER_STATE_KEY).execute(db.pool()).await?;
        assert_eq!(compare(db.pool(), 2).await?.len(), count as usize);
    }
    for state in ["safe", "finalized"] {
        sqlx::query("UPDATE chain_lineage SET canonicality_state=$1::canonicality_state WHERE block_number=1").bind(state).execute(db.pool()).await?;
        assert_eq!(compare(db.pool(), 2).await?.len(), 1025);
    }
    db.cleanup().await?;
    Ok(())
}

// Explicitly invoked under a disposable PostgreSQL allocation; no production data.
#[tokio::test]
#[ignore = "bounded restore resource fixture"]
async fn wide_history_streams_identically_under_prepared_plan_modes() -> Result {
    let db = database().await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,raw_fact_ref,derivation_kind,canonicality_state,after_state) SELECT 'resource-'||n,'ens','RecordChanged','test',1,'restore-test',1+n%4,'block-'||(1+n%4),jsonb_build_object($1::text,repeat('k',512)||(n/4)), 'ens_v2_resolver','canonical',jsonb_build_object('payload',(SELECT string_agg(md5((n+j)::text),'') FROM generate_series(1,128) j)) FROM generate_series(1,16384) n")
        .bind(INTERPRETER_STATE_KEY).execute(db.pool()).await?;
    sqlx::query("ANALYZE normalized_events")
        .execute(db.pool())
        .await?;
    for distinct in [false, true] {
        if distinct {
            sqlx::query("UPDATE normalized_events SET raw_fact_ref=jsonb_build_object($1::text,repeat('k',512)||event_identity)")
                .bind(INTERPRETER_STATE_KEY).execute(db.pool()).await?;
            sqlx::query("ANALYZE normalized_events")
                .execute(db.pool())
                .await?;
        }
        for mode in ["force_custom_plan", "force_generic_plan", "auto"] {
            // A detached connection closes on error instead of returning named statements
            // to the pool. Each mode and distribution starts with a fresh session.
            let mut connection = db.pool().acquire().await?.detach();
            let mut tx = connection.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SET LOCAL work_mem='1MB'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SET LOCAL temp_file_limit='256MB'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SET LOCAL statement_timeout='5min'")
                .execute(&mut *tx)
                .await?;
            sqlx::query(&format!("SET LOCAL plan_cache_mode='{mode}'"))
                .execute(&mut *tx)
                .await?;
            let mut expected = None;
            for (name, statement) in [
                ("original", reference_statement()),
                ("candidate", restore_statement()),
            ] {
                let prepared = format!("restore_fixture_{name}");
                sqlx::raw_sql(&format!("PREPARE {prepared} (text, bigint) AS {statement}"))
                    .execute(&mut *tx)
                    .await?;
                assert_eq!(plan_counts(&mut tx, &prepared).await?, (0, 0));
                let execute = format!("EXECUTE {prepared}('restore-test', 5)");
                // Auto starts with five custom choices, then makes its cost-based choice.
                // Every EXPLAIN ANALYZE EXECUTE counts as an execution in that history.
                for execution in 1..=if mode == "auto" { 6 } else { 1 } {
                    let before = plan_counts(&mut tx, &prepared).await?;
                    let plan: Value = sqlx::query_scalar(&format!(
                        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON) {execute}"
                    ))
                    .persistent(false)
                    .fetch_one(&mut *tx)
                    .await?;
                    let after = plan_counts(&mut tx, &prepared).await?;
                    let generic = assert_plan_choice(before, after, mode, execution);
                    let expressions = plan[0]["Plan"].to_string();
                    assert_eq!(expressions.contains("$1"), generic, "{plan}");
                    assert_eq!(expressions.contains("$2"), generic, "{plan}");
                    assert_eq!(
                        has_wide_global_store(&plan[0]["Plan"]),
                        name == "original",
                        "{name} distinct={distinct} mode={mode}: {plan}"
                    );
                    eprintln!(
                        "restore-plan distinct={distinct} mode={mode} query={name} execution={execution} before={before:?} after={after:?} generic={generic} json={plan}"
                    );
                }
                let before = plan_counts(&mut tx, &prepared).await?;
                let result = stream_digest(&mut tx, &execute, false).await?;
                let after = plan_counts(&mut tx, &prepared).await?;
                let generic =
                    assert_plan_choice(before, after, mode, if mode == "auto" { 7 } else { 2 });
                // Retain the original bound SQLx stream as separate semantic evidence.
                // Its SQL-text cache entry is not the named statement inspected above.
                let bound = stream_digest(&mut tx, &statement, true).await?;
                assert_eq!(result, bound);
                assert_eq!(result.0, if distinct { 16384 } else { 4097 });
                eprintln!(
                    "restore-resource distinct={distinct} mode={mode} query={name} rows={} digest={} named_stream_before={before:?} named_stream_after={after:?} generic={generic}",
                    result.0, result.1
                );
                sqlx::raw_sql(&format!("DEALLOCATE {prepared}"))
                    .execute(&mut *tx)
                    .await?;
                let remaining: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM pg_prepared_statements WHERE name=$1")
                        .bind(&prepared)
                        .fetch_one(&mut *tx)
                        .await?;
                assert_eq!(remaining, 0);
                if let Some(ref expected) = expected {
                    assert_eq!(&result, expected);
                } else {
                    expected = Some(result);
                }
            }
            tx.commit().await?;
            connection.close().await?;
        }
    }
    db.cleanup().await?;
    Ok(())
}

fn has_wide_global_store(node: &Value) -> bool {
    let storing = matches!(
        node["Node Type"].as_str(),
        Some("Sort" | "Incremental Sort" | "Materialize" | "Hash")
    );
    let wide = node["Output"].as_array().is_some_and(|output| {
        output
            .iter()
            .filter_map(Value::as_str)
            .any(|field| field.ends_with(".after_state") || field.ends_with(".raw_fact_ref"))
    });
    (storing && wide)
        || node["Plans"]
            .as_array()
            .is_some_and(|children| children.iter().any(has_wide_global_store))
}

// Counters belong to the SQL-level SELECT, not SQLx's utility-statement wrappers.
async fn plan_counts(connection: &mut PgConnection, name: &str) -> Result<(i64, i64)> {
    let (custom, generic, from_sql, types): (i64, i64, bool, Vec<String>) = sqlx::query_as(
        "SELECT custom_plans, generic_plans, from_sql, parameter_types::text[] FROM pg_prepared_statements WHERE name=$1",
    ).bind(name).fetch_one(connection).await?;
    assert!(from_sql);
    assert_eq!(types, ["text", "bigint"]);
    Ok((custom, generic))
}

fn assert_plan_choice(before: (i64, i64), after: (i64, i64), mode: &str, execution: i64) -> bool {
    let delta = (after.0 - before.0, after.1 - before.1);
    assert!(matches!(delta, (1, 0) | (0, 1)), "{before:?} -> {after:?}");
    assert_eq!(after.0 + after.1, execution);
    match mode {
        "force_custom_plan" => assert_eq!(delta, (1, 0)),
        "force_generic_plan" => assert_eq!(delta, (0, 1)),
        "auto" if execution <= 5 => assert_eq!(delta, (1, 0)),
        "auto" => {}
        _ => unreachable!(),
    }
    delta == (0, 1)
}

async fn stream_digest(
    connection: &mut PgConnection,
    statement: &str,
    bound: bool,
) -> Result<(usize, alloy_primitives::B256)> {
    use futures_util::TryStreamExt;
    let started = std::time::Instant::now();
    let query = sqlx::query_as::<_, Row>(statement);
    let query = if bound {
        query.bind("restore-test").bind(5_i64)
    } else {
        query.persistent(false)
    };
    let mut stream = query.fetch(connection);
    let mut page = Vec::with_capacity(1024);
    let mut count = 0;
    let mut high_water = 0;
    let mut digest = alloy_primitives::Keccak256::new();
    while let Some(row) = stream.try_next().await? {
        page.push(row);
        high_water = high_water.max(page.len());
        if page.len() == 1024 {
            for row in page.drain(..) {
                digest_row(&mut digest, &row);
                count += 1;
            }
        }
    }
    for row in page.drain(..) {
        digest_row(&mut digest, &row);
        count += 1;
    }
    assert_eq!(high_water, 1024);
    eprintln!(
        "restore-stream bound={bound} rows={count} page_high_water={high_water} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    Ok((count, digest.finalize()))
}

fn digest_row(digest: &mut alloy_primitives::Keccak256, row: &Row) {
    digest.update(
        format!(
            "{:?}",
            (
                &row.0, &row.1, &row.2, &row.3, &row.4, &row.5, &row.6, &row.7
            )
        )
        .as_bytes(),
    );
    digest.update(
        format!(
            "{:?}",
            (
                &row.8, &row.9, &row.10, &row.11, &row.12, &row.13, &row.14, &row.15
            )
        )
        .as_bytes(),
    );
}
