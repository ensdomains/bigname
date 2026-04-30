use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::info;

use super::{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, NormalizedReplayCursor};

const CURRENT_PROJECTION_TABLES: &[&str] = &[
    "address_names_current",
    "children_current",
    "name_current",
    "permissions_current",
    "primary_names_current",
    "record_inventory_current",
    "resolver_current",
];

const DEFERRED_NORMALIZED_EVENT_INDEXES: &[&str] = &[
    "normalized_events_namespace_idx",
    "normalized_events_kind_idx",
    "normalized_events_manifest_idx",
    "normalized_events_chain_position_idx",
    "normalized_events_name_projection_replay_idx",
    "normalized_events_resource_projection_replay_idx",
    "normalized_events_name_relevant_projection_idx",
    "normalized_events_record_inventory_resource_replay_idx",
];

const TEMPORARY_REPLAY_INDEXES: &[&str] = &[
    "normalized_events_replay_latest_resolver_tmp_idx",
    "normalized_events_replay_latest_record_version_tmp_idx",
];

pub(super) async fn prepare_deferred_projection_indexes_for_fresh_replay(
    pool: &PgPool,
    cursor: &NormalizedReplayCursor,
) -> Result<()> {
    if cursor.next_block_number > cursor.target_block_number {
        return Ok(());
    }

    let already_deferred = any_index_missing(pool, DEFERRED_NORMALIZED_EVENT_INDEXES).await?
        || any_index_exists(pool, TEMPORARY_REPLAY_INDEXES).await?;
    let projection_tables_empty = current_projection_tables_empty(pool).await?;
    if !already_deferred && !projection_tables_empty {
        return Ok(());
    }

    ensure_temporary_replay_indexes(pool).await?;
    drop_deferred_projection_indexes(pool).await?;

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        range_start_block_number = cursor.range_start_block_number,
        next_block_number = cursor.next_block_number,
        target_block_number = cursor.target_block_number,
        projection_tables_empty,
        already_deferred,
        "normalized replay projection indexes deferred for fresh catch-up"
    );
    Ok(())
}

pub(super) async fn ensure_projection_indexes_after_catchup(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<()> {
    if chains.is_empty()
        || !all_configured_cursors_complete(pool, deployment_profile, chains).await?
    {
        return Ok(());
    }
    if !any_index_missing(pool, DEFERRED_NORMALIZED_EVENT_INDEXES).await?
        && !any_index_exists(pool, TEMPORARY_REPLAY_INDEXES).await?
    {
        return Ok(());
    }

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile,
        chain_count = chains.len(),
        "normalized replay complete; rebuilding deferred projection indexes"
    );

    create_deferred_projection_indexes(pool).await?;
    drop_temporary_replay_indexes(pool).await?;

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile,
        chain_count = chains.len(),
        "deferred normalized replay projection indexes are ready"
    );
    Ok(())
}

async fn current_projection_tables_empty(pool: &PgPool) -> Result<bool> {
    for table in CURRENT_PROJECTION_TABLES {
        if !relation_exists(pool, table).await? {
            continue;
        }
        let has_rows = sqlx::query_scalar::<_, bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1)"
        ))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to inspect current projection table {table}"))?;
        if has_rows {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn all_configured_cursors_complete(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<bool> {
    let complete = sqlx::query_scalar::<_, Option<bool>>(
        r#"
        WITH configured_chains AS (
            SELECT DISTINCT UNNEST($3::TEXT[]) AS chain_id
        ),
        chain_completion AS (
            SELECT
                configured_chains.chain_id,
                cursor.next_block_number > cursor.target_block_number AS cursor_complete,
                EXISTS (
                    SELECT 1
                    FROM raw_logs
                    JOIN chain_lineage AS lineage
                      ON lineage.chain_id = raw_logs.chain_id
                     AND lineage.block_hash = raw_logs.block_hash
                    WHERE raw_logs.chain_id = configured_chains.chain_id
                      AND lineage.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                      AND raw_logs.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                    LIMIT 1
                ) AS has_canonical_raw_logs
            FROM configured_chains
            LEFT JOIN normalized_replay_cursors AS cursor
              ON cursor.deployment_profile = $1
             AND cursor.cursor_kind = $2
             AND cursor.chain_id = configured_chains.chain_id
        )
        SELECT BOOL_AND(COALESCE(cursor_complete, NOT has_canonical_raw_logs))
        FROM chain_completion
        "#,
    )
    .bind(deployment_profile)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .bind(chains)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to inspect normalized replay cursor completion for {deployment_profile}")
    })?;

    Ok(complete.unwrap_or(false))
}

async fn relation_exists(pool: &PgPool, relation: &str) -> Result<bool> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to inspect relation {relation}"))?;
    Ok(exists)
}

async fn any_index_missing(pool: &PgPool, indexes: &[&str]) -> Result<bool> {
    for index in indexes {
        if !relation_exists(pool, index).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn any_index_exists(pool: &PgPool, indexes: &[&str]) -> Result<bool> {
    for index in indexes {
        if relation_exists(pool, index).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn ensure_temporary_replay_indexes(pool: &PgPool) -> Result<()> {
    execute_ddl(
        pool,
        "CREATE INDEX IF NOT EXISTS normalized_events_replay_latest_resolver_tmp_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             log_index DESC NULLS LAST,
             normalized_event_id DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND event_kind = 'ResolverChanged'
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX IF NOT EXISTS normalized_events_replay_latest_record_version_tmp_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             log_index DESC NULLS LAST,
             normalized_event_id DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND event_kind = 'RecordVersionChanged'
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await
}

async fn drop_deferred_projection_indexes(pool: &PgPool) -> Result<()> {
    for index in DEFERRED_NORMALIZED_EVENT_INDEXES {
        execute_ddl(pool, &format!("DROP INDEX IF EXISTS {index}")).await?;
    }
    Ok(())
}

async fn create_deferred_projection_indexes(pool: &PgPool) -> Result<()> {
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_namespace_idx
         ON normalized_events (namespace, normalized_event_id DESC)",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_kind_idx
         ON normalized_events (event_kind, normalized_event_id DESC)",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_manifest_idx
         ON normalized_events (source_manifest_id, event_kind, normalized_event_id DESC)
         WHERE source_manifest_id IS NOT NULL",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_chain_position_idx
         ON normalized_events (chain_id, block_number DESC, normalized_event_id DESC)
         WHERE block_number IS NOT NULL",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_projection_replay_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             chain_id ASC NULLS LAST,
             block_hash DESC NULLS LAST,
             transaction_hash DESC NULLS LAST,
             log_index DESC NULLS LAST,
             event_identity DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resource_projection_replay_idx
         ON normalized_events (
             resource_id,
             block_number DESC NULLS LAST,
             chain_id ASC NULLS LAST,
             block_hash DESC NULLS LAST,
             transaction_hash DESC NULLS LAST,
             log_index DESC NULLS LAST,
             event_identity DESC
         )
         WHERE resource_id IS NOT NULL
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_relevant_projection_idx
         ON normalized_events (
             logical_name_id,
             block_number ASC NULLS FIRST,
             log_index ASC NULLS LAST,
             event_identity ASC
         )
         WHERE logical_name_id IS NOT NULL
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_inventory_resource_replay_idx
         ON normalized_events (
             resource_id,
             block_number ASC,
             log_index ASC NULLS FIRST,
             normalized_event_id ASC
         )
         WHERE resource_id IS NOT NULL
           AND logical_name_id IS NOT NULL
           AND chain_id IS NOT NULL
           AND block_number IS NOT NULL
           AND block_hash IS NOT NULL
           AND derivation_kind IN (
               'ens_v1_unwrapped_authority',
               'ens_v2_resolver'
           )
           AND event_kind IN (
               'RecordChanged',
               'RecordVersionChanged',
               'ResolverChanged'
           )
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await
}

async fn drop_temporary_replay_indexes(pool: &PgPool) -> Result<()> {
    for index in TEMPORARY_REPLAY_INDEXES {
        execute_ddl(pool, &format!("DROP INDEX IF EXISTS {index}")).await?;
    }
    Ok(())
}

async fn execute_ddl(pool: &PgPool, statement: &str) -> Result<()> {
    sqlx::query(statement)
        .execute(pool)
        .await
        .with_context(|| format!("failed to execute normalized replay index DDL: {statement}"))?;
    Ok(())
}
