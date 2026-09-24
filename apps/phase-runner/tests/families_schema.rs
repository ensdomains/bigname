//! The owned key family tables (docs/projections.md, "Owned key families"): `init-schema` installs
//! them from the baseline, each schema-migration creates the same table on an existing phase
//! schema, and a schema-migration applied over a baseline that already has them changes nothing.
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use phase_runner::schema::initialize_schema_v2;

const FAMILY_TABLES: &[&str] = &[
    "project_family_marker",
    "project_family_undo",
    "project_repair_record",
    "project_name_state",
    "project_binding_candidate",
    "project_lifecycle_key_state",
    "project_lifecycle_triple_summary",
    "project_lifecycle_association",
    "project_lifecycle_event",
    "project_child_registration_state",
    "project_wrapper_state",
    "project_registry_node_state",
    "project_registry_binding_observation",
    "project_resolver_classification",
    "project_registry_pointer",
    "project_resource_pointer",
];

const BASELINE: &[&str] = &[
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
    include_str!("../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
    include_str!("../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
];

async fn database(prefix: &str) -> Result<TestDatabase> {
    TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(2)).await
}

/// Install the baseline files directly, as a phase schema created before these tables would have
/// had them, then drop the family tables when `without_families` asks for the older shape.
async fn install_baseline(database: &TestDatabase, without_families: bool) -> Result<()> {
    let mut transaction = database.pool().begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for sql in BASELINE {
        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
    }
    if without_families {
        for table in FAMILY_TABLES {
            sqlx::raw_sql(&format!("DROP TABLE bigname_phase.{table}"))
                .execute(&mut *transaction)
                .await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn schema_migrations_create_the_family_tables_the_baseline_installs() -> Result<()> {
    let installed = database("families_schema_installed").await?;
    initialize_schema_v2(installed.pool()).await?;
    for table in FAMILY_TABLES {
        let key: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_constraintdef(constraint_row.oid)
             FROM pg_constraint constraint_row
             JOIN pg_class relation ON relation.oid = constraint_row.conrelid
             JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = 'bigname_phase' AND relation.relname = $1
               AND constraint_row.contype = 'p'",
        )
        .bind(table)
        .fetch_optional(installed.pool())
        .await?;
        assert!(key.is_some(), "init-schema installs {table} with a primary key");
    }

    let migrated = database("families_schema_migrated").await?;
    install_baseline(&migrated, true).await?;
    bigname_storage::MIGRATOR.run(migrated.pool()).await?;
    for table in FAMILY_TABLES {
        let from_migration = load_table_structure(migrated.pool(), table).await?;
        assert!(!from_migration.is_empty(), "the schema-migration creates {table}");
        assert_eq!(
            from_migration,
            load_table_structure(installed.pool(), table).await?,
            "the schema-migration and the baseline define one identical {table}"
        );
    }

    let current = database("families_schema_current").await?;
    install_baseline(&current, false).await?;
    let before = structures(&current).await?;
    bigname_storage::MIGRATOR.run(current.pool()).await?;
    assert_eq!(
        structures(&current).await?,
        before,
        "the schema-migrations change nothing on a baseline that already has the tables"
    );

    installed.cleanup().await?;
    migrated.cleanup().await?;
    current.cleanup().await
}

async fn structures(database: &TestDatabase) -> Result<Vec<Vec<String>>> {
    let mut structures = Vec::new();
    for table in FAMILY_TABLES {
        structures.push(load_table_structure(database.pool(), table).await?);
    }
    Ok(structures)
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
            SELECT format('constraint:%s:%s', constraint_row.conname,
                          pg_get_constraintdef(constraint_row.oid))
            FROM pg_constraint constraint_row
            JOIN pg_class relation ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase' AND relation.relname = $1
            UNION ALL
            SELECT format('index:%s', pg_get_indexdef(index_row.indexrelid))
            FROM pg_index index_row
            JOIN pg_class relation ON relation.oid = index_row.indrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'bigname_phase' AND relation.relname = $1
            UNION ALL
            SELECT format('comment:%s:%s', COALESCE(attribute.attname, '<table>'),
                          description.description)
            FROM pg_description description
            JOIN pg_class relation ON relation.oid = description.objoid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            LEFT JOIN pg_attribute attribute
              ON attribute.attrelid = relation.oid
             AND attribute.attnum = description.objsubid
             AND description.objsubid > 0
            WHERE namespace.nspname = 'bigname_phase' AND relation.relname = $1
        ) structure
        ORDER BY object_identity
        "#,
    )
    .bind(table)
    .fetch_all(pool)
    .await?)
}
