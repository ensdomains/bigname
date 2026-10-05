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
    "project_registry_owner_event",
    "project_registry_binding_observation",
    "project_resolver_classification",
    "project_registry_pointer",
    "project_resource_pointer",
    "project_named_resource_pointer",
    "project_universal_resolver_proxy",
    "project_node_record_partition",
    "project_node_record_value",
    "project_text_hydration_work",
    "project_record_id_value",
    "project_resolver_link",
    "project_grant",
    "project_resource_admin_aggregate",
    "project_account_approval",
    "project_ens_v2_entry_owner",
    "project_ens_v2_registry_parent",
    "project_child_edge_candidate",
    "project_parent_subregistry",
    "project_reverse_tuple",
    "project_reverse_hydration_work",
    "project_reverse_node_claim",
    "project_claim_normalization",
    "project_address_name_fold",
    "project_address_controller_candidate",
    "project_address_name_index",
    "project_address_record_node_index",
    "project_address_record_id_index",
    "project_name_history",
    "project_name_summary",
];

const BASELINE: &[&str] = &[
    include_str!("../../../crates/storage/schema/baseline/01_chain.sql"),
    include_str!("../../../crates/storage/schema/baseline/02_raw_facts.sql"),
    include_str!("../../../crates/storage/schema/baseline/03_identity.sql"),
    include_str!("../../../crates/storage/schema/baseline/04_manifests.sql"),
    include_str!("../../../crates/storage/schema/baseline/05_normalized_events.sql"),
    include_str!("../../../crates/storage/schema/baseline/06_projections.sql"),
    include_str!("../../../crates/storage/schema/baseline/07_labels.sql"),
    include_str!("../../../crates/storage/schema/baseline/08_heartbeats.sql"),
    include_str!("../../../crates/storage/schema/baseline/09_divergence.sql"),
    include_str!("../../../crates/storage/schema/baseline/10_phase_state.sql"),
    include_str!("../../../crates/storage/schema/baseline/11_manifest_authority_attestations.sql"),
    include_str!("../../../crates/storage/schema/baseline/12_project_generation_failures.sql"),
    include_str!("../../../crates/storage/schema/baseline/13_interpret_decode_skips.sql"),
    include_str!("../../../crates/storage/schema/baseline/14_discovery_watch_admissions.sql"),
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
    for (index, current) in BASELINE.iter().enumerate() {
        let sql = if without_families {
            match index {
                4 => include_str!(
                    "../../../crates/storage/schema/fixtures/pre-7c/05_normalized_events.sql"
                ),
                5 => include_str!(
                    "../../../crates/storage/schema/fixtures/pre-7c/06_projections.sql"
                ),
                8 => {
                    include_str!("../../../crates/storage/schema/fixtures/pre-7c/09_divergence.sql")
                }
                11 => include_str!(
                    "../../../crates/storage/schema/fixtures/pre-7c/12_project_generation_failures.sql"
                ),
                _ => current,
            }
        } else {
            current
        };
        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
    }
    if without_families {
        // The historical fixture predates the newest family tables, such as
        // `project_universal_resolver_proxy`, which only the schema-migrations create.
        for table in FAMILY_TABLES {
            sqlx::raw_sql(&format!("DROP TABLE IF EXISTS bigname_phase.{table}"))
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
        assert!(
            key.is_some(),
            "init-schema installs {table} with a primary key"
        );
    }

    let migrated = database("families_schema_migrated").await?;
    install_baseline(&migrated, true).await?;
    bigname_storage::MIGRATOR.run(migrated.pool()).await?;
    for table in FAMILY_TABLES {
        let from_migration = load_table_structure(migrated.pool(), table).await?;
        assert!(
            !from_migration.is_empty(),
            "the schema-migration creates {table}"
        );
        assert_eq!(
            from_migration,
            load_table_structure(installed.pool(), table).await?,
            "the schema-migration and the baseline define one identical {table}"
        );
    }

    let current = database("families_schema_current").await?;
    install_baseline(&current, false).await?;
    let before = structures(&current).await?;
    // A fresh initializer already contains historical changes. The forward removal and
    // hydration-work upgrades also support an already-current baseline without changing it.
    sqlx::raw_sql(include_str!(
        "../../../migrations/20260929160000_remove_served_projections.sql"
    ))
    .execute(current.pool())
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../migrations/20260930220000_project_hydration_work.sql"
    ))
    .execute(current.pool())
    .await?;
    assert_eq!(
        structures(&current).await?,
        before,
        "the forward upgrades change nothing on the fresh family baseline"
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

const OPTIONAL_RAW_EVIDENCE: &str =
    include_str!("../../../migrations/20261005130000_name_surfaces_optional_raw_evidence.sql");

/// `name_surfaces` without column positions: the test rebuilds the predecessor shape by
/// dropping the new column, which a database that never had it does not do.
async fn name_surfaces_structure(pool: &sqlx::PgPool) -> Result<Vec<String>> {
    let mut structure = load_table_structure(pool, "name_surfaces")
        .await?
        .into_iter()
        .map(|object| match object.strip_prefix("column:") {
            Some(column) => format!(
                "column:{}",
                column.split_once(':').map_or(column, |(_, rest)| rest)
            ),
            None => object,
        })
        .collect::<Vec<_>>();
    structure.sort();
    Ok(structure)
}

/// A populated phase schema from before raw label bytes became optional gains the baseline's
/// `name_surfaces`, keeps every row's raw labels, and names each row's preimage witness.
#[tokio::test]
async fn optional_raw_evidence_upgrade_matches_the_baseline_on_a_populated_schema() -> Result<()> {
    let installed = database("name_surfaces_schema_installed").await?;
    initialize_schema_v2(installed.pool()).await?;

    let upgraded = database("name_surfaces_schema_upgraded").await?;
    install_baseline(&upgraded, false).await?;
    sqlx::raw_sql(
        "BEGIN;
         SET LOCAL search_path TO bigname_phase, public;
         ALTER TABLE name_surfaces
             DROP CONSTRAINT name_surfaces_raw_evidence_check,
             DROP COLUMN preimage_event_identity,
             ALTER COLUMN raw_name SET NOT NULL,
             ALTER COLUMN raw_labels SET NOT NULL,
             ALTER COLUMN dns_encoded_name SET NOT NULL,
             ADD CHECK (cardinality(raw_labels) = cardinality(labelhashes));
         INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('upgrade', '0x01', 1, to_timestamp(1), 'canonical'),
                ('upgrade', '0x02', 2, to_timestamp(2), 'canonical'),
                ('upgrade', '0x0f', 2, to_timestamp(2), 'orphaned');
         INSERT INTO name_surfaces
             (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
              labelhashes, normalizer_version, visibility_state, deactivation_reason,
              deactivated_at, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('ens:0xnamed', 'ens', 'named.eth', ARRAY['named', 'eth'], '\\x05', '0xnamed',
                 ARRAY['0xa', '0xeth'], 'test', 'active', NULL, NULL, 'upgrade', '0x01', 1,
                 'canonical'),
                ('ens:0xbytes', 'ens', '', '{}', '', '0xbytes', '{}', 'test', 'shadow',
                 'normalization_gate', to_timestamp(2), 'upgrade', '0x02', 2, 'canonical'),
                ('ens:0xgone', 'ens', 'gone.eth', ARRAY['gone', 'eth'], '\\x04', '0xgone',
                 ARRAY['0xb', '0xeth'], 'test', 'active', NULL, NULL, 'upgrade', '0x0f', 2,
                 'orphaned');
         INSERT INTO normalized_events
             (event_identity, namespace, logical_name_id, event_kind, source_family,
              manifest_version, chain_id, block_number, block_hash, transaction_hash,
              transaction_index, log_index, derivation_kind, canonicality_state)
         VALUES ('named-later', 'ens', 'ens:0xnamed', 'PreimageObserved', 'ens_v1_registrar_l1',
                 1, 'upgrade', 2, '0x02', '0xtx2', 0, 0, 'raw_log_preimage_observation',
                 'canonical'),
                ('named-first', 'ens', 'ens:0xnamed', 'PreimageObserved', 'ens_v1_registrar_l1',
                 1, 'upgrade', 1, '0x01', '0xtx1', 0, 4, 'raw_log_preimage_observation',
                 'canonical'),
                ('named-other', 'ens', 'ens:0xnamed', 'RegistrationGranted',
                 'ens_v1_registrar_l1', 1, 'upgrade', 1, '0x01', '0xtx1', 0, 1,
                 'ens_v1_unwrapped_authority', 'canonical'),
                ('bytes-first', 'ens', 'ens:0xbytes', 'PreimageObserved', 'ens_v1_wrapper_l1',
                 1, 'upgrade', 2, '0x02', '0xtx2', 0, 2, 'raw_log_preimage_observation',
                 'canonical'),
                ('gone-orphaned', 'ens', 'ens:0xgone', 'PreimageObserved', 'ens_v1_wrapper_l1',
                 1, 'upgrade', 2, '0x0f', '0xtx3', 0, 0, 'raw_log_preimage_observation',
                 'orphaned');
         COMMIT;",
    )
    .execute(upgraded.pool())
    .await?;
    assert_ne!(
        name_surfaces_structure(upgraded.pool()).await?,
        name_surfaces_structure(installed.pool()).await?,
        "the fixture restores the predecessor shape"
    );

    for _ in 0..2 {
        sqlx::raw_sql(OPTIONAL_RAW_EVIDENCE)
            .execute(upgraded.pool())
            .await?;
        assert_eq!(
            name_surfaces_structure(upgraded.pool()).await?,
            name_surfaces_structure(installed.pool()).await?,
            "the schema-migration and the baseline define one identical name_surfaces"
        );
        let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT logical_name_id, raw_name, preimage_event_identity
             FROM bigname_phase.name_surfaces ORDER BY logical_name_id",
        )
        .fetch_all(upgraded.pool())
        .await?;
        assert_eq!(
            rows,
            vec![
                (
                    "ens:0xbytes".to_owned(),
                    Some(String::new()),
                    Some("bytes-first".to_owned())
                ),
                ("ens:0xgone".to_owned(), Some("gone.eth".to_owned()), None),
                (
                    "ens:0xnamed".to_owned(),
                    Some("named.eth".to_owned()),
                    Some("named-first".to_owned())
                ),
            ]
        );
    }

    for pool in [installed.pool(), upgraded.pool()] {
        sqlx::raw_sql(
            "INSERT INTO bigname_phase.chain_lineage
                 (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ('upgrade', '0x03', 3, to_timestamp(3), 'canonical');
             INSERT INTO bigname_phase.name_surfaces
                 (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
                  visibility_state, chain_id, block_hash, block_number, canonicality_state)
             VALUES ('ens:0xchild', 'ens', '0xchild', ARRAY['0xc', '0xa', '0xeth'], 'test',
                     'active', 'upgrade', '0x03', 3, 'canonical')",
        )
        .execute(pool)
        .await?;
    }

    installed.cleanup().await?;
    upgraded.cleanup().await
}
