use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::raw_sql;

use super::REGISTRY_RESOLVER_PARENT_SCOPE_SQL;

const CHAIN: &str = "registry-resolver-parent-scope-plan";
const CHILD_NAMEHASH: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const PARENT_NAMEHASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHILD_LOGICAL_NAME_ID: &str =
    "ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const AFTER_CHILD_INDEX: &str = "normalized_events_v1_subregistry_after_child_scope_idx";

const BASELINE: &[&str] = &[
    include_str!("../../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    include_str!("../../../../schema-v2/baseline/06_projections.sql"),
    include_str!("../../../../schema-v2/baseline/07_labels.sql"),
    include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
    include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
    include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
];
const AFTER_CHILD_MIGRATION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../migrations/20260827130100_normalized_events_v1_after_child_scope_idx.sql"
));

#[tokio::test]
async fn registry_resolver_parent_scope_uses_after_child_partial_index() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "registry_resolver_parent_scope_plan",
    ))
    .await?;
    let mut transaction = database.pool().begin().await?;
    raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    sqlx::query(&format!("DROP INDEX {AFTER_CHILD_INDEX}"))
        .execute(&mut *transaction)
        .await?;
    raw_sql(AFTER_CHILD_MIGRATION)
        .execute(&mut *transaction)
        .await?;

    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES
             ($1, '0x10', 10, '2026-08-01T00:00:10Z', 'canonical'),
             ($1, '0x11', 11, '2026-08-01T00:00:11Z', 'canonical')",
    )
    .bind(CHAIN)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'child.eth', ARRAY['child', 'eth'], decode('00', 'hex'),
             $2, ARRAY[$2, $2], 'test', 'active', $3, '0x10', 10, 'canonical'
         )",
    )
    .bind(CHILD_LOGICAL_NAME_ID)
    .bind(CHILD_NAMEHASH)
    .bind(CHAIN)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, consumer_visibility, after_state
         ) VALUES
             ('plan:edge', 'ens', NULL, 'SubregistryChanged',
              'ens_v1_registry_l1', 1, $1, 10, '0x10',
              'raw_log_preimage_observation', 'canonical', 'activated',
              jsonb_build_object('node', $2::text, 'child_node', $3::text)),
             ('plan:owner', 'ens', $4, 'AuthorityTransferred',
              'ens_v1_registry_l1', 1, $1, 11, '0x11',
              'raw_log_preimage_observation', 'canonical', 'activated',
              jsonb_build_object('owner_getter', $5::text))",
    )
    .bind(CHAIN)
    .bind(PARENT_NAMEHASH)
    .bind(CHILD_NAMEHASH)
    .bind(CHILD_LOGICAL_NAME_ID)
    .bind(ZERO_ADDRESS)
    .execute(&mut *transaction)
    .await?;
    raw_sql(
        "CREATE TEMP TABLE project_scope_names (
             logical_name_id text PRIMARY KEY
         ) ON COMMIT DROP;
         CREATE TEMP TABLE project_changed_events (
             chain_id text NOT NULL,
             namespace text NOT NULL,
             logical_name_id text,
             resource_id uuid,
             event_kind text NOT NULL,
             source_family text NOT NULL
         ) ON COMMIT DROP;
         INSERT INTO project_changed_events VALUES (
             'registry-resolver-parent-scope-plan', 'ens',
             'ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
             NULL, 'ResolverChanged', 'ens_v1_registry_l1'
         );
         ANALYZE normalized_events;
         ANALYZE project_changed_events;
         SET LOCAL enable_seqscan = off",
    )
    .execute(&mut *transaction)
    .await?;

    let plan = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (COSTS OFF) {REGISTRY_RESOLVER_PARENT_SCOPE_SQL}"
    ))
    .bind(CHAIN)
    .bind(20_i64)
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    eprintln!("{plan}");
    assert!(
        plan.contains(AFTER_CHILD_INDEX),
        "registry-resolver parent scope must use {AFTER_CHILD_INDEX}:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on normalized_events edge"),
        "registry-resolver parent scope must not scan all normalized events:\n{plan}"
    );
    sqlx::query(REGISTRY_RESOLVER_PARENT_SCOPE_SQL)
        .bind(CHAIN)
        .bind(20_i64)
        .execute(&mut *transaction)
        .await?;
    let scoped_names = sqlx::query_scalar::<_, String>(
        "SELECT logical_name_id FROM project_scope_names ORDER BY logical_name_id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    assert_eq!(scoped_names, vec![format!("ens:{PARENT_NAMEHASH}")]);

    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
