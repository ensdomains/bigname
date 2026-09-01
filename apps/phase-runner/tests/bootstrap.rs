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
async fn expiry_scope_index_migration_repairs_an_initialized_phase_schema() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_expiry_scope_index_migration")
            .pool_max_connections(2)
            .parse_context("failed to parse expiry index schema-migration test database URL")
            .admin_connect_context(
                "failed to connect expiry index schema-migration test admin pool",
            )
            .pool_connect_context("failed to connect expiry index schema-migration test pool"),
    )
    .await?;
    initialize_schema_v2(database.pool()).await?;
    sqlx::query("DROP INDEX bigname_phase.normalized_events_v2_expiry_scope_idx")
        .execute(database.pool())
        .await?;
    let absent_before: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.normalized_events_v2_expiry_scope_idx') IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(absent_before);

    bigname_storage::MIGRATOR.run(database.pool()).await?;
    let ready_and_valid: bool = sqlx::query_scalar(
        "SELECT COALESCE(bool_and(index_state.indisready AND index_state.indisvalid), false)
         FROM pg_index index_state
         WHERE index_state.indexrelid =
               to_regclass('bigname_phase.normalized_events_v2_expiry_scope_idx')",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(ready_and_valid);

    database.cleanup().await
}

#[tokio::test]
async fn manifest_change_counter_migrates_existing_history_with_baseline_parity() -> Result<()> {
    let migrated = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_manifest_change_counter_migration")
            .pool_max_connections(2)
            .parse_context("failed to parse manifest counter schema-migration database URL")
            .admin_connect_context("failed to connect manifest counter schema-migration admin pool")
            .pool_connect_context("failed to connect manifest counter schema-migration pool"),
    )
    .await?;
    initialize_schema_v2(migrated.pool()).await?;
    sqlx::raw_sql(
        "ALTER TABLE bigname_phase.manifest_versions
             DROP CONSTRAINT manifest_versions_applied_change_count_check,
             DROP COLUMN applied_change_count",
    )
    .execute(migrated.pool())
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO bigname_phase.manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (
             1, 'test', 'migration_counter', 'migration-counter-chain', 'fixture',
             'deprecated', 'test-normalizer', 'test/migration_counter/v1.toml', '{}'::jsonb
         )
         RETURNING manifest_id",
    )
    .fetch_one(migrated.pool())
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, derivation_kind, canonicality_state
         ) VALUES
             ('manifest-counter-1', 'test', 'SourceManifestUpdated',
              'migration_counter', 1, $1, 'migration-counter-chain',
              'manifest_sync', 'finalized'),
             ('manifest-counter-2', 'test', 'SourceManifestUpdated',
              'migration_counter', 1, $1, 'migration-counter-chain',
              'manifest_sync', 'finalized')",
    )
    .bind(manifest_id)
    .execute(migrated.pool())
    .await?;

    let migration =
        include_str!("../../../migrations/20260826120100_manifest_applied_change_count.sql");
    sqlx::raw_sql(migration).execute(migrated.pool()).await?;
    sqlx::raw_sql(migration).execute(migrated.pool()).await?;
    let applied_change_count: i64 = sqlx::query_scalar(
        "SELECT applied_change_count
         FROM bigname_phase.manifest_versions
         WHERE manifest_id = $1",
    )
    .bind(manifest_id)
    .fetch_one(migrated.pool())
    .await?;
    assert_eq!(
        applied_change_count, 2,
        "migration backfill must count existing manifest history once"
    );
    let migrated_structure = load_manifest_counter_structure(migrated.pool()).await?;

    let installed = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_manifest_change_counter_baseline")
            .pool_max_connections(2)
            .parse_context("failed to parse manifest counter baseline database URL")
            .admin_connect_context("failed to connect manifest counter baseline admin pool")
            .pool_connect_context("failed to connect manifest counter baseline pool"),
    )
    .await?;
    initialize_schema_v2(installed.pool()).await?;
    let installed_structure = load_manifest_counter_structure(installed.pool()).await?;
    assert_eq!(
        migrated_structure, installed_structure,
        "the schema migration and phase baseline must define the same manifest counter"
    );

    installed.cleanup().await?;
    migrated.cleanup().await
}

#[tokio::test]
async fn reverse_hydration_attempt_state_migrates_an_initialized_phase_schema() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_reverse_hydration_attempt_migration")
            .pool_max_connections(2)
            .parse_context("failed to parse reverse hydration schema-migration database URL")
            .admin_connect_context(
                "failed to connect reverse hydration schema-migration admin pool",
            )
            .pool_connect_context("failed to connect reverse hydration schema-migration pool"),
    )
    .await?;
    initialize_schema_v2(database.pool()).await?;
    sqlx::raw_sql(
        "ALTER TABLE bigname_phase.primary_names_current
             DROP CONSTRAINT primary_names_current_reverse_hydration_attempt_check,
             DROP COLUMN reverse_hydration_attempted_block_number,
             DROP COLUMN reverse_hydration_attempted_block_hash,
             DROP COLUMN reverse_hydration_attempt_ordinal;
         DROP SEQUENCE bigname_phase.reverse_hydration_attempt_ordinal_seq;",
    )
    .execute(database.pool())
    .await?;

    bigname_storage::MIGRATOR.run(database.pool()).await?;

    let sequence_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass(
             'bigname_phase.reverse_hydration_attempt_ordinal_seq'
         ) IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(sequence_exists);
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'bigname_phase'
           AND table_name = 'primary_names_current'
           AND column_name IN (
               'reverse_hydration_attempted_block_number',
               'reverse_hydration_attempted_block_hash',
               'reverse_hydration_attempt_ordinal'
           )
         ORDER BY column_name",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        columns,
        vec![
            "reverse_hydration_attempt_ordinal",
            "reverse_hydration_attempted_block_hash",
            "reverse_hydration_attempted_block_number",
        ]
    );
    let constraint_is_valid: bool = sqlx::query_scalar(
        "SELECT constraint_row.convalidated
         FROM pg_constraint constraint_row
         WHERE constraint_row.conrelid =
                 'bigname_phase.primary_names_current'::regclass
           AND constraint_row.conname =
                 'primary_names_current_reverse_hydration_attempt_check'",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(constraint_is_valid);
    sqlx::query(
        "SELECT reverse_hydration_attempted_block_number,
                reverse_hydration_attempted_block_hash,
                reverse_hydration_attempt_ordinal
         FROM bigname_phase.primary_names_current
         LIMIT 0",
    )
    .execute(database.pool())
    .await?;
    let attempt_ordinal: i64 = sqlx::query_scalar(
        "SELECT nextval(
             'bigname_phase.reverse_hydration_attempt_ordinal_seq'
         )",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(attempt_ordinal > 0);

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
    let added = phase_structure_after
        .iter()
        .filter(|object| !phase_structure_before.contains(object))
        .collect::<Vec<_>>();
    let removed = phase_structure_before
        .iter()
        .filter(|object| !phase_structure_after.contains(object))
        .collect::<Vec<_>>();
    assert!(
        added.is_empty() && removed.is_empty(),
        "legacy public-schema deletion changed the installed phase schema; added={added:?}; removed={removed:?}"
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

#[tokio::test]
async fn generation_failure_audit_matches_between_baseline_and_schema_migration() -> Result<()> {
    let migrated = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_generation_failure_migrated")
            .pool_max_connections(2)
            .parse_context("failed to parse migrated failure-audit database URL")
            .admin_connect_context("failed to connect migrated failure-audit admin pool")
            .pool_connect_context("failed to connect migrated failure-audit pool"),
    )
    .await?;
    let mut transaction = migrated.pool().begin().await?;
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
        include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
    ] {
        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_phase_state (
             chain_id, phase_name, input_content_hash,
             current_block_number, current_block_hash
         ) VALUES (
             'failure-audit-resume', 'project', 'resume-marker', 41, 'resume-hash'
         )",
    )
    .execute(migrated.pool())
    .await?;
    let absent_before: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.project_generation_failures') IS NULL",
    )
    .fetch_one(migrated.pool())
    .await?;
    assert!(absent_before);

    bigname_storage::MIGRATOR.run(migrated.pool()).await?;
    let migrated_structure =
        load_table_structure(migrated.pool(), "project_generation_failures").await?;
    let resume: (i64, String) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash
         FROM bigname_phase.chain_phase_state
         WHERE chain_id = 'failure-audit-resume' AND phase_name = 'project'",
    )
    .fetch_one(migrated.pool())
    .await?;
    assert_eq!(
        resume,
        (41, "resume-hash".to_owned()),
        "the resume cursor survives the schema migration"
    );

    assert_failure_kind_vocabulary(migrated.pool()).await?;

    let installed = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_generation_failure_baseline")
            .pool_max_connections(2)
            .parse_context("failed to parse baseline failure-audit database URL")
            .admin_connect_context("failed to connect baseline failure-audit admin pool")
            .pool_connect_context("failed to connect baseline failure-audit pool"),
    )
    .await?;
    initialize_schema_v2(installed.pool()).await?;
    let installed_structure =
        load_table_structure(installed.pool(), "project_generation_failures").await?;

    assert!(
        !installed_structure.is_empty(),
        "the baseline installs the failure-audit table"
    );
    assert_failure_kind_vocabulary(installed.pool()).await?;
    assert_eq!(
        migrated_structure, installed_structure,
        "the schema-migration and the baseline define one identical table"
    );

    installed.cleanup().await?;
    migrated.cleanup().await
}

#[tokio::test]
async fn interpret_decode_skip_audit_matches_between_baseline_and_schema_migration() -> Result<()> {
    let migrated = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_decode_skip_migrated")
            .pool_max_connections(2)
            .parse_context("failed to parse migrated decode-skip database URL")
            .admin_connect_context("failed to connect migrated decode-skip admin pool")
            .pool_connect_context("failed to connect migrated decode-skip pool"),
    )
    .await?;
    let mut transaction = migrated.pool().begin().await?;
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
        include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../schema-v2/baseline/12_project_generation_failures.sql"),
    ] {
        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    let absent_before: bool =
        sqlx::query_scalar("SELECT to_regclass('bigname_phase.interpret_decode_skips') IS NULL")
            .fetch_one(migrated.pool())
            .await?;
    assert!(absent_before);

    bigname_storage::MIGRATOR.run(migrated.pool()).await?;
    let migrated_structure =
        load_table_structure(migrated.pool(), "interpret_decode_skips").await?;

    let installed = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_decode_skip_baseline")
            .pool_max_connections(2)
            .parse_context("failed to parse baseline decode-skip database URL")
            .admin_connect_context("failed to connect baseline decode-skip admin pool")
            .pool_connect_context("failed to connect baseline decode-skip pool"),
    )
    .await?;
    initialize_schema_v2(installed.pool()).await?;
    let installed_structure =
        load_table_structure(installed.pool(), "interpret_decode_skips").await?;

    assert!(
        !installed_structure.is_empty(),
        "the baseline installs the interpretation decode-skip table"
    );
    assert_eq!(
        migrated_structure, installed_structure,
        "the schema migration and the baseline define one identical table"
    );

    installed.cleanup().await?;
    migrated.cleanup().await
}

/// Both installation paths admit the exact-name kind slice 2E records and the
/// child kind slice 3B adds, and refuse anything else.
async fn assert_failure_kind_vocabulary(pool: &sqlx::PgPool) -> Result<()> {
    for kind in [
        "dual_current_exact_name_authority",
        "dual_current_child_authority",
    ] {
        insert_failure_kind(pool, kind).await?;
    }
    let rejected = insert_failure_kind(pool, "dual_current_unknown_authority").await;
    assert!(
        rejected.is_err(),
        "the failure-kind vocabulary is closed to {}",
        "dual_current_unknown_authority"
    );
    let recorded: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM bigname_phase.project_generation_failures
         WHERE chain_id = 'failure-kind-vocabulary'",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(recorded, 2, "each admitted kind records one row");
    sqlx::query(
        "DELETE FROM bigname_phase.project_generation_failures
         WHERE chain_id = 'failure-kind-vocabulary'",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_failure_kind(pool: &sqlx::PgPool, kind: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_generation_failures (
             chain_id, target_block_number, target_block_hash,
             interpreter_content_hash, failure_kind, failure_fingerprint,
             logical_name_id, evidence
         ) VALUES ('failure-kind-vocabulary', 1, '0x01', 'hash', $1,
                   repeat('a', 64), 'ens:0x01', '{}'::jsonb)",
    )
    .bind(kind)
    .execute(pool)
    .await?;
    Ok(())
}

async fn load_table_structure(pool: &sqlx::PgPool, table: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        r#"
        SELECT object_identity
        FROM (
            SELECT format(
                       'column:%s:%s:%s:%s:%s',
                       attribute.attnum,
                       attribute.attname,
                       pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
                       attribute.attnotnull,
                       COALESCE(pg_get_expr(default_value.adbin, default_value.adrelid), '')
                   ) AS object_identity
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            JOIN pg_attribute attribute ON attribute.attrelid = relation.oid
            LEFT JOIN pg_attrdef default_value
              ON default_value.adrelid = relation.oid
             AND default_value.adnum = attribute.attnum
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relname = $1
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped

            UNION ALL

            SELECT format(
                       'constraint:%s:%s',
                       constraint_row.conname,
                       pg_get_constraintdef(constraint_row.oid)
                   )
            FROM pg_constraint constraint_row
            JOIN pg_class relation ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relname = $1

            UNION ALL

            SELECT format('index:%s', pg_get_indexdef(index_row.indexrelid))
            FROM pg_index index_row
            JOIN pg_class relation ON relation.oid = index_row.indrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relname = $1

            UNION ALL

            SELECT format(
                       'comment:%s:%s',
                       COALESCE(attribute.attname, '<table>'),
                       description.description
                   )
            FROM pg_description description
            JOIN pg_class relation ON relation.oid = description.objoid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            LEFT JOIN pg_attribute attribute
              ON attribute.attrelid = relation.oid
             AND attribute.attnum = description.objsubid
             AND description.objsubid > 0
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relname = $1
        ) structure
        ORDER BY object_identity
        "#,
    )
    .bind(table)
    .fetch_all(pool)
    .await?)
}

async fn load_manifest_counter_structure(pool: &sqlx::PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        r#"
        SELECT object_identity
        FROM (
            SELECT format(
                       'column:%s:%s:%s',
                       pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
                       attribute.attnotnull,
                       COALESCE(pg_get_expr(default_value.adbin, default_value.adrelid), '')
                   ) AS object_identity
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            JOIN pg_attribute attribute ON attribute.attrelid = relation.oid
            LEFT JOIN pg_attrdef default_value
              ON default_value.adrelid = relation.oid
             AND default_value.adnum = attribute.attnum
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relname = 'manifest_versions'
              AND attribute.attname = 'applied_change_count'
              AND NOT attribute.attisdropped

            UNION ALL

            SELECT format(
                       'constraint:%s:%s:%s',
                       constraint_row.conname,
                       constraint_row.convalidated,
                       pg_get_constraintdef(constraint_row.oid)
                   )
            FROM pg_constraint constraint_row
            JOIN pg_class relation ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relname = 'manifest_versions'
              AND constraint_row.conname =
                  'manifest_versions_applied_change_count_check'

            UNION ALL

            SELECT format('comment:%s', description.description)
            FROM pg_description description
            JOIN pg_class relation ON relation.oid = description.objoid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            JOIN pg_attribute attribute
              ON attribute.attrelid = relation.oid
             AND attribute.attnum = description.objsubid
            WHERE namespace.nspname = 'bigname_phase'
              AND relation.relname = 'manifest_versions'
              AND attribute.attname = 'applied_change_count'
        ) structure
        ORDER BY object_identity
        "#,
    )
    .fetch_all(pool)
    .await?)
}
