use std::path::Path;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use phase_runner::{database::RunnerDatabase, schema::initialize_schema_v2};

#[tokio::test]
async fn schema_migrations_apply_to_an_empty_database_before_the_phase_baseline() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_empty_migrations_then_baseline")
            .pool_max_connections(2)
            .parse_context("failed to parse empty schema-migration test database URL")
            .admin_connect_context("failed to connect empty schema-migration test admin pool")
            .pool_connect_context("failed to connect empty schema-migration test pool"),
    )
    .await?;

    bigname_storage::MIGRATOR.run(database.pool()).await?;
    let audit_before_baseline: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.manifest_authority_attestations') IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(!audit_before_baseline);

    initialize_schema_v2(database.pool()).await?;
    let audit_after_baseline: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.manifest_authority_attestations') IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(audit_after_baseline);

    database.cleanup().await
}

#[tokio::test]
async fn audit_schema_migration_applies_on_top_of_the_pre_audit_phase_baseline() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_pre_audit_baseline_migration")
            .pool_max_connections(2)
            .parse_context("failed to parse baseline schema-migration test database URL")
            .admin_connect_context("failed to connect baseline schema-migration test admin pool")
            .pool_connect_context("failed to connect baseline schema-migration test pool"),
    )
    .await?;
    let mut transaction = database.pool().begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for sql in [
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
        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;

    let legacy_fingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let legacy_marker = format!("manifest-authority:{legacy_fingerprint}");
    sqlx::query(
        "INSERT INTO bigname_phase.chain_phase_state (
             chain_id, phase_name, input_content_hash
         ) VALUES
             ('legacy-authority-marker', 'interpret', $1),
             ('legacy-authority-marker', 'project', $1)",
    )
    .bind(&legacy_marker)
    .execute(database.pool())
    .await?;

    bigname_storage::MIGRATOR.run(database.pool()).await?;
    let audit_table_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.manifest_authority_attestations') IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(audit_table_exists);
    let upgraded_markers: Vec<String> = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM bigname_phase.chain_phase_state
         WHERE chain_id = 'legacy-authority-marker'
         ORDER BY phase_name",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(upgraded_markers.len(), 2);
    let expected_prefix = format!("{legacy_marker}:");
    assert!(
        upgraded_markers
            .iter()
            .all(|marker| marker.starts_with(&expected_prefix))
    );
    assert_eq!(
        upgraded_markers[0], upgraded_markers[1],
        "matching legacy markers on one chain must receive one upgrade generation"
    );

    database.cleanup().await
}

#[tokio::test]
async fn bootstrap_after_legacy_schema_drop_reaches_manifest_sync() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_supported_bootstrap")
            .pool_max_connections(4)
            .parse_context("failed to parse phase-runner bootstrap test database URL")
            .admin_connect_context("failed to connect phase-runner bootstrap admin pool")
            .pool_connect_context("failed to connect phase-runner bootstrap pool"),
    )
    .await?;
    initialize_schema_v2(database.pool()).await?;
    let phase_structure_before = load_phase_schema_structure(database.pool()).await?;
    bigname_storage::MIGRATOR.run(database.pool()).await?;
    let phase_structure_after = load_phase_schema_structure(database.pool()).await?;
    assert_eq!(
        phase_structure_after, phase_structure_before,
        "legacy public-schema deletion changed the installed phase schema"
    );
    let residual_public_objects = sqlx::query_scalar::<_, String>(
        r#"
        SELECT object_kind || ':' || object_name
        FROM (
            SELECT 'function' AS object_kind,
                   format(
                       '%I.%I(%s)',
                       namespace.nspname,
                       procedure.proname,
                       pg_get_function_identity_arguments(procedure.oid)
                   ) AS object_name
            FROM pg_proc procedure
            JOIN pg_namespace namespace ON namespace.oid = procedure.pronamespace
            WHERE namespace.nspname = 'public'
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_depend dependency
                  JOIN pg_extension extension ON extension.oid = dependency.refobjid
                  WHERE dependency.classid = 'pg_proc'::regclass
                    AND dependency.objid = procedure.oid
                    AND dependency.refclassid = 'pg_extension'::regclass
                    AND dependency.deptype = 'e'
              )

            UNION ALL

            SELECT 'sequence', format('%I.%I', namespace.nspname, relation.relname)
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relkind = 'S'
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_depend dependency
                  JOIN pg_extension extension ON extension.oid = dependency.refobjid
                  WHERE dependency.classid = 'pg_class'::regclass
                    AND dependency.objid = relation.oid
                    AND dependency.refclassid = 'pg_extension'::regclass
                    AND dependency.deptype = 'e'
              )

            UNION ALL

            SELECT 'enum', format('%I.%I', namespace.nspname, enum_type.typname)
            FROM pg_type enum_type
            JOIN pg_namespace namespace ON namespace.oid = enum_type.typnamespace
            WHERE namespace.nspname = 'public'
              AND enum_type.typtype = 'e'
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_depend dependency
                  JOIN pg_extension extension ON extension.oid = dependency.refobjid
                  WHERE dependency.classid = 'pg_type'::regclass
                    AND dependency.objid = enum_type.oid
                    AND dependency.refclassid = 'pg_extension'::regclass
                    AND dependency.deptype = 'e'
              )

            UNION ALL

            SELECT 'relation', format('%I.%I', namespace.nspname, relation.relname)
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relname NOT IN ('_sqlx_migrations', '_sqlx_migrations_pkey')
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_depend dependency
                  JOIN pg_extension extension ON extension.oid = dependency.refobjid
                  WHERE dependency.classid = 'pg_class'::regclass
                    AND dependency.objid = relation.oid
                    AND dependency.refclassid = 'pg_extension'::regclass
                    AND dependency.deptype = 'e'
              )
        ) residual
        ORDER BY object_kind, object_name
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert!(
        residual_public_objects.is_empty(),
        "legacy public-schema objects survived migration: {residual_public_objects:?}"
    );
    let runner =
        RunnerDatabase::connect_with_options(database.pool().connect_options().as_ref().clone(), 4)
            .await?;
    let manifests_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = bigname_manifests::load_repository(&manifests_root)?;
    let summary = bigname_manifests::sync_schema_v2_repository(runner.pool(), &repository).await?;
    assert!(summary.manifest_count > 0);

    let (legacy_table_exists, phase_table_exists): (bool, bool) = sqlx::query_as(
        "SELECT to_regclass('public.manifest_versions') IS NOT NULL, \
                to_regclass('bigname_phase.chain_phase_state') IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(!legacy_table_exists);
    assert!(phase_table_exists);
    let audit_table_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.manifest_authority_attestations') IS NOT NULL",
    )
    .fetch_one(runner.pool())
    .await?;
    assert!(audit_table_exists);
    let active_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(runner.pool())
        .await?;
    assert_eq!(active_schema, "bigname_phase");

    runner.pool().close().await;
    database.cleanup().await
}

async fn load_phase_schema_structure(pool: &sqlx::PgPool) -> Result<Vec<String>> {
    sqlx::query_scalar(
        r#"
        SELECT object_identity
        FROM (
            SELECT format('relation:%s:%s', relation.relkind, relation.relname)
                       AS object_identity
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase'

            UNION ALL

            SELECT format(
                       'column:%s:%s:%s:%s:%s:%s',
                       relation.relname,
                       attribute.attnum,
                       attribute.attname,
                       pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
                       attribute.attnotnull,
                       COALESCE(pg_get_expr(default_value.adbin, default_value.adrelid), '')
                   )
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            JOIN pg_attribute attribute ON attribute.attrelid = relation.oid
            LEFT JOIN pg_attrdef default_value
              ON default_value.adrelid = relation.oid
             AND default_value.adnum = attribute.attnum
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relkind IN ('r', 'p', 'v', 'm')
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped

            UNION ALL

            SELECT format(
                       'constraint:%s:%s:%s',
                       relation.relname,
                       constraint_row.conname,
                       pg_get_constraintdef(constraint_row.oid)
                   )
            FROM pg_constraint constraint_row
            JOIN pg_class relation ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase'

            UNION ALL

            SELECT format('index:%s', pg_get_indexdef(index_row.indexrelid))
            FROM pg_index index_row
            JOIN pg_class relation ON relation.oid = index_row.indrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase'

            UNION ALL

            SELECT format(
                       'function:%s(%s):%s',
                       procedure.proname,
                       pg_get_function_identity_arguments(procedure.oid),
                       pg_get_functiondef(procedure.oid)
                   )
            FROM pg_proc procedure
            JOIN pg_namespace namespace ON namespace.oid = procedure.pronamespace
            WHERE namespace.nspname = 'bigname_phase'

            UNION ALL

            SELECT format('type:%s:%s', type_row.typtype, type_row.typname)
            FROM pg_type type_row
            JOIN pg_namespace namespace ON namespace.oid = type_row.typnamespace
            WHERE namespace.nspname = 'bigname_phase'
        ) structure
        ORDER BY object_identity
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

#[tokio::test]
async fn bootstrap_rejects_structural_drift_in_an_existing_phase_schema() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_bootstrap_structural_drift")
            .pool_max_connections(4)
            .parse_context("failed to parse phase-runner bootstrap test database URL")
            .admin_connect_context("failed to connect phase-runner bootstrap admin pool")
            .pool_connect_context("failed to connect phase-runner bootstrap pool"),
    )
    .await?;
    initialize_schema_v2(database.pool()).await?;
    sqlx::query(
        "ALTER TABLE bigname_phase.chain_phase_state \
         DROP CONSTRAINT chain_phase_state_verification_phase_check",
    )
    .execute(database.pool())
    .await?;

    let error = initialize_schema_v2(database.pool())
        .await
        .expect_err("a nonempty phase schema must require a reviewed upgrade or rebuild");
    assert!(
        error
            .to_string()
            .contains("requires an empty bigname_phase schema")
    );
    let constraint_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_constraint constraint_row
        JOIN pg_class relation ON relation.oid = constraint_row.conrelid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'bigname_phase'
          AND relation.relname = 'chain_phase_state'
          AND constraint_row.conname = 'chain_phase_state_verification_phase_check'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        constraint_count, 0,
        "rejection must not repair or rewrite drift"
    );

    database.cleanup().await
}
