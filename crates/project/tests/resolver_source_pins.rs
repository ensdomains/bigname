use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const CHAIN: &str = "ethereum-mainnet";
const RESOLVER: &str = "0x1111111111111111111111111111111111111111";

fn production_order(source: &str, selection: &str) -> String {
    let normalized = source.split_whitespace().collect::<Vec<_>>().join(" ");
    let selection_start = normalized.find(selection).expect("production selection");
    let selection = &normalized[selection_start..];
    let order_start = selection.find("ORDER BY").expect("selection ordering");
    let order_end = selection[order_start..]
        .find("LIMIT 1")
        .expect("selection limit");
    selection[order_start..order_start + order_end]
        .trim()
        .to_owned()
}

fn quote_identifier(identifier: &str) -> String {
    format!(r#""{}""#, identifier.replace('"', r#""""#))
}

async fn migrated_pool() -> TestResult<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "project_resolver_declaration_alignment",
    ))
    .await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE {} SET search_path TO bigname_phase, public",
        quote_identifier(&database_name)
    ))
    .execute(&mut *transaction)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    Ok((database, pool))
}

async fn seed_duplicate_declarations(pool: &PgPool) -> TestResult {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES
             ($1, $2, 20, '2026-08-01T00:00:20Z', 'canonical'),
             ($1, $3, 30, '2026-08-01T00:00:30Z', 'canonical')",
    )
    .bind(CHAIN)
    .bind(format!("0x{:064x}", 20_u64))
    .bind(format!("0x{:064x}", 30_u64))
    .execute(pool)
    .await?;
    let payload = json!({
        "deployment_epoch": "test",
        "contracts": [
            {
                "role": "old_resolver",
                "address": RESOLVER,
                "proxy_kind": "none",
                "read_features": ["ensip19_default_address"],
                "start_block": 10
            },
            {
                "role": "first_at_target",
                "address": RESOLVER,
                "proxy_kind": "none",
                "read_features": ["ensip19_default_address"],
                "start_block": 20
            },
            {
                "role": "latest_at_target",
                "address": RESOLVER,
                "proxy_kind": "none",
                "read_features": ["ensip19_default_address"],
                "start_block": 20
            },
            {
                "role": "future_resolver",
                "address": RESOLVER,
                "proxy_kind": "none",
                "read_features": ["ensip19_default_address"],
                "start_block": 30
            }
        ]
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'test', 'active',
                   'test', 'test/resolver.toml', $2)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(payload.clone())
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, derivation_kind, canonicality_state, after_state
         ) VALUES (
             'manifest:test', 'ens', 'SourceManifestUpdated', 'ens_v1_resolver_l1', 1,
             $1, $2, 'manifest_sync', 'canonical', $3
         )",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(json!({
        "rollout_status": "active",
        "normalizer_version": "test",
        "manifest_payload": payload
    }))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             'alias:test', 'ens', 'AliasChanged', 'ens_v1_resolver_l1', 1,
             $1, $2, 20, $3, 'raw_log_preimage_observation', 'canonical', $4
         )",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(format!("0x{:064x}", 20_u64))
    .bind(json!({"resolver": RESOLVER, "active": false}))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn duplicate_declarations_project_latest_role_and_features_together() -> TestResult {
    let resolver = include_str!("../src/builders/resolver.rs");
    let features = include_str!("../src/builders/resolver/read_features.rs");
    assert_eq!(
        production_order(resolver, "SELECT declaration ->> 'role'"),
        "ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC, declaration_ordinality DESC"
    );
    assert_eq!(
        production_order(features, "SELECT declaration -> 'read_features'"),
        "ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC, declaration_ordinality DESC"
    );
    assert_eq!(
        production_order(resolver, "SELECT implementation ->> 'role'"),
        "ORDER BY implementation_ordinality DESC"
    );
    assert_eq!(
        production_order(features, "SELECT admitted -> 'read_features'"),
        "ORDER BY admitted_ordinality DESC"
    );

    let (database, pool) = migrated_pool().await?;
    seed_duplicate_declarations(&pool).await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 20,
            affected_from_block: 20,
            affected_to_block: 20,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let classification: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'classification'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(&pool)
    .await?;
    assert_eq!(classification["role"], json!("latest_at_target"));
    assert_eq!(
        classification["read_features"],
        json!(["ensip19_default_address"])
    );
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 30,
            affected_from_block: 21,
            affected_to_block: 30,
            resume_current: Some(Marker {
                number: 20,
                hash: format!("0x{:064x}", 20_u64),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    let resumed_classification: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'classification'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(&pool)
    .await?;
    assert_eq!(resumed_classification["role"], json!("future_resolver"));
    assert_eq!(
        resumed_classification["read_features"],
        json!(["ensip19_default_address"])
    );
    database.cleanup().await?;
    Ok(())
}
