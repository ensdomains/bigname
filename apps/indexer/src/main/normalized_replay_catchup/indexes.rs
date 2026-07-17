use anyhow::{Context, Result};
use bigname_storage::{
    DEFERRED_NORMALIZED_EVENT_INDEXES, NormalizedReplayIndexDdlGuard,
    TEMPORARY_NORMALIZED_REPLAY_INDEXES, acquire_normalized_replay_index_ddl_guard,
    count_unready_normalized_event_indexes,
};
use sqlx::{PgConnection, PgPool};
use tracing::info;

use crate::reconciliation::guard_release::prioritize_operation_error;

use super::{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, NormalizedReplayCursor, cursors};

const CURRENT_PROJECTION_TABLES: &[&str] = &[
    "address_names_current",
    "children_current",
    "name_current",
    "permissions_current",
    "primary_names_current",
    "record_inventory_current",
    "resolver_current",
];

pub(super) async fn prepare_deferred_projection_indexes_for_fresh_replay(
    pool: &PgPool,
    cursor: &NormalizedReplayCursor,
) -> Result<()> {
    if cursor.next_block_number > cursor.target_block_number {
        return Ok(());
    }

    let mut ddl_guard = acquire_normalized_replay_index_ddl_guard(pool).await?;
    let preparation = async {
        let already_deferred = ddl_guard
            .count_unready_normalized_event_indexes(DEFERRED_NORMALIZED_EVENT_INDEXES)
            .await?
            > 0
            || any_index_exists_on_connection(
                ddl_guard.connection_mut(),
                TEMPORARY_NORMALIZED_REPLAY_INDEXES,
            )
            .await?;
        let projection_tables_empty =
            current_projection_tables_empty(ddl_guard.connection_mut()).await?;
        if !should_defer_projection_indexes(cursor, already_deferred, projection_tables_empty) {
            return Ok(None);
        }

        ensure_temporary_replay_indexes(ddl_guard.connection_mut()).await?;
        ddl_guard.defer_deferred_normalized_event_indexes().await?;
        Ok(Some((projection_tables_empty, already_deferred)))
    }
    .await;
    let Some((projection_tables_empty, already_deferred)) =
        finish_normalized_replay_index_ddl(ddl_guard, preparation).await?
    else {
        return Ok(());
    };

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
    if !projection_indexes_need_restore(pool).await? {
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

    ensure_deferred_projection_indexes_ready(pool, deployment_profile, chains).await?;

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

pub(super) async fn restore_deferred_projection_indexes(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<()> {
    if !projection_indexes_need_restore(pool).await? {
        return Ok(());
    }

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile,
        chain_count = chains.len(),
        "normalized replay selected closure/dependency adapters; restoring deferred projection indexes before failing closed"
    );

    ensure_deferred_projection_indexes_ready(pool, deployment_profile, chains).await?;

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

async fn ensure_deferred_projection_indexes_ready(
    pool: &PgPool,
    _deployment_profile: &str,
    _chains: &[String],
) -> Result<()> {
    let mut ddl_guard = acquire_normalized_replay_index_ddl_guard(pool).await?;
    let restoration = async {
        if !projection_indexes_need_restore_while_guarded(&mut ddl_guard).await? {
            return Ok(());
        }

        ddl_guard
            .ensure_deferred_normalized_event_indexes_ready()
            .await?;
        if ddl_guard
            .count_unready_normalized_event_indexes(DEFERRED_NORMALIZED_EVENT_INDEXES)
            .await?
            > 0
        {
            anyhow::bail!(
                "deferred normalized replay projection indexes remain unready after rebuild"
            );
        }
        drop_temporary_replay_indexes(ddl_guard.connection_mut()).await
    }
    .await;
    finish_normalized_replay_index_ddl(ddl_guard, restoration).await
}

async fn finish_normalized_replay_index_ddl<T>(
    guard: NormalizedReplayIndexDdlGuard,
    operation: Result<T>,
) -> Result<T> {
    let release = guard.release().await;
    prioritize_operation_error(operation, release)
}

async fn projection_indexes_need_restore(pool: &PgPool) -> Result<bool> {
    Ok(
        any_index_not_ready(pool, DEFERRED_NORMALIZED_EVENT_INDEXES).await?
            || any_index_exists(pool, TEMPORARY_NORMALIZED_REPLAY_INDEXES).await?,
    )
}

async fn projection_indexes_need_restore_while_guarded(
    ddl_guard: &mut NormalizedReplayIndexDdlGuard,
) -> Result<bool> {
    Ok(ddl_guard
        .count_unready_normalized_event_indexes(DEFERRED_NORMALIZED_EVENT_INDEXES)
        .await?
        > 0
        || any_index_exists_on_connection(
            ddl_guard.connection_mut(),
            TEMPORARY_NORMALIZED_REPLAY_INDEXES,
        )
        .await?)
}

fn should_defer_projection_indexes(
    cursor: &NormalizedReplayCursor,
    already_deferred: bool,
    projection_tables_empty: bool,
) -> bool {
    already_deferred || (projection_tables_empty && cursor.last_replayed_at.is_none())
}

async fn current_projection_tables_empty(connection: &mut PgConnection) -> Result<bool> {
    for table in CURRENT_PROJECTION_TABLES {
        if !relation_exists_on_connection(&mut *connection, table).await? {
            continue;
        }
        let has_rows = sqlx::query_scalar::<_, bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1)"
        ))
        .fetch_one(&mut *connection)
        .await
        .with_context(|| format!("failed to inspect current projection table {table}"))?;
        if has_rows {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
// These focused index-readiness tests stay beside the helper they exercise.
#[expect(clippy::items_after_test_module)]
mod tests {
    use sqlx::types::time::OffsetDateTime;

    use super::*;

    fn cursor(last_replayed_at: Option<OffsetDateTime>) -> NormalizedReplayCursor {
        NormalizedReplayCursor {
            range_start_block_number: 1,
            next_block_number: 10,
            target_block_number: 20,
            last_replayed_at,
            raw_log_input_revision: 0,
            raw_log_retention_generation: 0,
        }
    }

    #[test]
    fn defers_indexes_when_already_deferred() {
        assert!(should_defer_projection_indexes(
            &cursor(Some(OffsetDateTime::UNIX_EPOCH)),
            true,
            false
        ));
    }

    #[test]
    fn defers_indexes_for_initial_empty_projection_replay() {
        assert!(should_defer_projection_indexes(&cursor(None), false, true));
    }

    #[test]
    fn keeps_projection_indexes_after_initial_replay_completed() {
        assert!(!should_defer_projection_indexes(
            &cursor(Some(OffsetDateTime::UNIX_EPOCH)),
            false,
            true
        ));
    }
}

pub(super) async fn all_configured_cursors_complete(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<bool> {
    cursors::all_configured_cursors_complete(pool, deployment_profile, chains).await
}

async fn relation_exists(pool: &PgPool, relation: &str) -> Result<bool> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to inspect relation {relation}"))?;
    Ok(exists)
}

async fn relation_exists_on_connection(
    connection: &mut PgConnection,
    relation: &str,
) -> Result<bool> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(connection)
        .await
        .with_context(|| format!("failed to inspect relation {relation}"))?;
    Ok(exists)
}

async fn any_index_not_ready(pool: &PgPool, indexes: &[&str]) -> Result<bool> {
    Ok(count_unready_normalized_event_indexes(pool, indexes).await? > 0)
}

async fn any_index_exists(pool: &PgPool, indexes: &[&str]) -> Result<bool> {
    for index in indexes {
        if relation_exists(pool, index).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn any_index_exists_on_connection(
    connection: &mut PgConnection,
    indexes: &[&str],
) -> Result<bool> {
    for index in indexes {
        if relation_exists_on_connection(&mut *connection, index).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn ensure_temporary_replay_indexes(connection: &mut PgConnection) -> Result<()> {
    execute_ddl(
        &mut *connection,
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
        &mut *connection,
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
    .await?;
    execute_ddl(
        &mut *connection,
        "CREATE INDEX IF NOT EXISTS normalized_events_replay_latest_registrar_tmp_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             log_index DESC NULLS LAST,
             normalized_event_id DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND event_kind IN (
               'RegistrationGranted',
               'RegistrationRenewed',
               'ExpiryChanged',
               'TokenControlTransferred'
           )
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await
}

async fn drop_temporary_replay_indexes(connection: &mut PgConnection) -> Result<()> {
    for index in TEMPORARY_NORMALIZED_REPLAY_INDEXES {
        execute_ddl(&mut *connection, &format!("DROP INDEX IF EXISTS {index}")).await?;
    }
    Ok(())
}

async fn execute_ddl(connection: &mut PgConnection, statement: &str) -> Result<()> {
    sqlx::query(statement)
        .execute(connection)
        .await
        .with_context(|| format!("failed to execute normalized replay index DDL: {statement}"))?;
    Ok(())
}
