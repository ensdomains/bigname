use std::collections::BTreeMap;

use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_storage_migration_indexes_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for migration-index tests")
            .admin_connect_context("failed to connect admin pool for migration-index tests")
            .pool_connect_context("failed to connect migration-index test pool"),
        &crate::MIGRATOR,
        "failed to apply migrations for migration-index tests",
    )
    .await
}

async fn normalized_event_index_definitions(pool: &PgPool) -> Result<BTreeMap<String, String>> {
    let mut definitions = BTreeMap::new();
    for index in DEFERRED_NORMALIZED_EVENT_INDEXES {
        let definition = sqlx::query_scalar::<_, String>("SELECT pg_get_indexdef(to_regclass($1))")
            .bind(index)
            .fetch_one(pool)
            .await
            .with_context(|| {
                format!("failed to inspect migrated normalized-event index definition {index}")
            })?;
        definitions.insert((*index).to_owned(), definition);
    }
    Ok(definitions)
}

async fn required_runtime_index_definitions(pool: &PgPool) -> Result<BTreeMap<String, String>> {
    let mut definitions = BTreeMap::new();
    for index in REQUIRED_RUNTIME_INDEXES {
        let definition = sqlx::query_scalar::<_, String>("SELECT pg_get_indexdef(to_regclass($1))")
            .bind(index.name)
            .fetch_one(pool)
            .await
            .with_context(|| {
                format!(
                    "failed to inspect migrated required runtime index definition {}",
                    index.name
                )
            })?;
        definitions.insert(index.name.to_owned(), definition);
    }
    Ok(definitions)
}

#[test]
fn migration_error_has_priority_over_guard_release_error() {
    let error = prioritize_operation_error::<()>(
        Err(anyhow::anyhow!("actionable migration failure")),
        Err(anyhow::anyhow!("secondary guard release failure")),
    )
    .expect_err("the migration failure must remain actionable");

    assert_eq!(error.to_string(), "actionable migration failure");
}

#[test]
fn guard_release_error_is_returned_after_successful_migration() {
    let error = prioritize_operation_error(Ok(()), Err(anyhow::anyhow!("guard release failure")))
        .expect_err("a failed guard release must fail migration setup");

    assert_eq!(error.to_string(), "guard release failure");
}

#[test]
fn successful_migration_returns_its_value_after_successful_release() {
    let value = prioritize_operation_error(Ok(42), Ok(()))
        .expect("successful migration setup must return the operation value");

    assert_eq!(value, 42);
}

#[tokio::test]
async fn runtime_rebuild_matches_all_migrated_deferred_index_definitions() -> Result<()> {
    let database = test_database().await?;
    let migrated_definitions = normalized_event_index_definitions(database.pool()).await?;
    assert_eq!(
        count_unready_normalized_event_indexes(database.pool(), DEFERRED_NORMALIZED_EVENT_INDEXES,)
            .await?,
        0,
        "checked-in migrations must install every deferred normalized-event index ready"
    );

    let mut guard = acquire_normalized_replay_index_ddl_guard(database.pool()).await?;
    guard.defer_deferred_normalized_event_indexes().await?;
    assert_eq!(
        count_unready_normalized_event_indexes(database.pool(), DEFERRED_NORMALIZED_EVENT_INDEXES,)
            .await?,
        i64::try_from(DEFERRED_NORMALIZED_EVENT_INDEXES.len())
            .expect("deferred normalized-event index count must fit in i64"),
        "deferral must make every deferred normalized-event index unready"
    );
    guard
        .ensure_deferred_normalized_event_indexes_ready()
        .await?;
    guard.release().await?;

    let rebuilt_definitions = normalized_event_index_definitions(database.pool()).await?;
    assert_eq!(
        rebuilt_definitions, migrated_definitions,
        "the storage runtime descriptors and checked-in migrations must install the same catalog definitions"
    );
    assert_eq!(
        count_unready_normalized_event_indexes(database.pool(), DEFERRED_NORMALIZED_EVENT_INDEXES,)
            .await?,
        0,
        "the runtime rebuild must restore deferred-index readiness"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn migrate_repairs_missing_and_unready_required_runtime_indexes() -> Result<()> {
    let database = test_database().await?;
    let migrated_definitions = required_runtime_index_definitions(database.pool()).await?;
    sqlx::query(&format!(
        "DROP INDEX {}",
        EXECUTION_CACHE_OUTCOMES_REQUEST_LOOKUP_INDEX
    ))
    .execute(database.pool())
    .await?;
    for index in REQUIRED_RUNTIME_INDEXES
        .iter()
        .filter(|index| index.name != EXECUTION_CACHE_OUTCOMES_REQUEST_LOOKUP_INDEX)
    {
        sqlx::query(
            r#"
            UPDATE pg_index
            SET indisvalid = FALSE,
                indisready = FALSE
            WHERE indexrelid = to_regclass($1)
            "#,
        )
        .bind(index.name)
        .execute(database.pool())
        .await?;
    }
    {
        let mut connection = database.pool().acquire().await?;
        for index in REQUIRED_RUNTIME_INDEXES {
            assert!(
                !required_index_ready(&mut connection, index.name, index.table).await?,
                "test setup must make required index {} missing or unready",
                index.name
            );
        }
    }

    crate::migrate(database.pool()).await?;

    {
        let mut connection = database.pool().acquire().await?;
        for index in REQUIRED_RUNTIME_INDEXES {
            assert!(
                required_index_ready(&mut connection, index.name, index.table).await?,
                "migrate must repair required runtime index {}",
                index.name
            );
        }
    }
    assert_eq!(
        required_runtime_index_definitions(database.pool()).await?,
        migrated_definitions,
        "runtime recovery descriptors must recreate the checked-in migration definitions"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn watched_address_lookup_indexes_cover_active_and_historical_rows() -> Result<()> {
    let database = test_database().await?;
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;

    let active_plan = sqlx::query_scalar::<_, String>(
        r#"
        EXPLAIN (FORMAT TEXT)
        SELECT 1
        FROM contract_instance_addresses
        WHERE chain_id = 'eip155:1'
          AND deactivated_at IS NULL
          AND LOWER(address) = '0x0000000000000000000000000000000000000001'
        "#,
    )
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    assert!(
        active_plan.contains(ACTIVE_CONTRACT_ADDRESS_INDEX),
        "active watched-address lookup must use its expression index:\n{active_plan}"
    );

    let historical_plan = sqlx::query_scalar::<_, String>(
        r#"
        EXPLAIN (FORMAT TEXT)
        SELECT 1
        FROM contract_instance_addresses
        WHERE chain_id = 'eip155:1'
          AND deactivated_at IS NOT NULL
          AND active_to_block_number IS NOT NULL
          AND LOWER(address) = '0x0000000000000000000000000000000000000001'
        "#,
    )
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    assert!(
        historical_plan.contains(HISTORICAL_CONTRACT_ADDRESS_INDEX),
        "historical watched-address lookup must use its expression index:\n{historical_plan}"
    );

    let raw_code_plan = sqlx::query_scalar::<_, String>(
        r#"
        EXPLAIN (FORMAT TEXT)
        SELECT 1
        FROM raw_code_hashes
        WHERE chain_id = 'eip155:1'
          AND LOWER(contract_address) = '0x0000000000000000000000000000000000000001'
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    assert!(
        raw_code_plan.contains(NON_ORPHANED_RAW_CODE_LOWER_ADDRESS_INDEX),
        "raw-code baseline lookup must use its expression index:\n{raw_code_plan}"
    );

    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn compat_trigger_tuple_invalidation_uses_request_lookup_index() -> Result<()> {
    let database = test_database().await?;
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(&mut *transaction)
        .await?;

    let plan = sqlx::query_scalar::<_, String>(
        r#"
        EXPLAIN (FORMAT TEXT)
        DELETE FROM execution_cache_outcomes AS outcome
        WHERE outcome.request_type = 'verified_primary_name'
          AND outcome.namespace = $1
          AND outcome.request_key = $2
        "#,
    )
    .bind("ens")
    .bind("ens:0x0000000000000000000000000000000000000001:60")
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    eprintln!("compat trigger tuple invalidation plan:\n{plan}");
    assert!(
        plan.contains(&format!(
            "Index Scan using {EXECUTION_CACHE_OUTCOMES_REQUEST_LOOKUP_INDEX}"
        )),
        "compat trigger tuple invalidation must use its request lookup index:\n{plan}"
    );

    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[test]
fn raw_log_revision_migration_fences_writers_before_backfill() {
    let migration =
        include_str!("../../../../migrations/20260714120000_raw_log_staging_input_revisions.sql");
    let lock = migration
        .find("LOCK TABLE public.raw_logs IN SHARE ROW EXCLUSIVE MODE")
        .expect("raw-log revision cutover must exclude writers");
    let revision_backfill = migration
        .find("INSERT INTO public.raw_log_staging_input_revisions")
        .expect("raw-log revision cutover must seed its chain ledger");
    let trigger = migration
        .find("CREATE TRIGGER raw_logs_staging_revision_insert")
        .expect("raw-log revision cutover must install its insert trigger");

    assert!(
        lock < revision_backfill && revision_backfill < trigger,
        "the raw_logs write fence must precede revision backfill and remain held through trigger installation"
    );
}
