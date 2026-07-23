mod apply;
mod apply_locks;
mod dead_letters;
mod derive;
mod derive_queries;
mod manifest_queries;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::info;

use crate::primary_name::rebuild_heartbeat::LoopHeartbeat;
use crate::record_inventory;

pub(crate) use derive::{
    ProjectionInvalidationDeriveSummary,
    capture_normalized_event_change_watermark as load_normalized_event_change_watermark,
    capture_normalized_event_change_watermark_in_transaction, completed_projection_sources_changed,
    normalized_event_cursor_exists, seed_normalized_event_cursor_if_absent_in_transaction,
};

const NORMALIZED_EVENT_CURSOR: &str = "normalized_events_to_projection_invalidations";
const NORMALIZED_EVENT_DERIVE_BATCH_LIMIT: i64 = 5_000;
pub(crate) const NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT: i64 = 250;
const PROJECTION_APPLY_BATCH_LIMIT: i64 = 25;
const FAILURE_RETRY_DELAY: &str = "60 seconds";
const CLAIM_RETRY_DELAY: &str = "5 minutes";
const PRIMARY_NAMES_CURRENT_PROJECTION: &str = "primary_names_current";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedEventChangeCursor {
    pub(crate) change_id: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProjectionApplyIterationSummary {
    pub(crate) scanned_event_count: i64,
    pub(crate) enqueued_invalidation_count: u64,
    pub(crate) claimed_invalidation_count: usize,
    pub(crate) applied_invalidation_count: usize,
    pub(crate) failed_invalidation_count: usize,
}

impl ProjectionApplyIterationSummary {
    pub(crate) fn made_progress(&self) -> bool {
        self.scanned_event_count > 0
            || self.claimed_invalidation_count > 0
            || self.applied_invalidation_count > 0
            || self.failed_invalidation_count > 0
    }
}

pub(crate) async fn run_once(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<ProjectionApplyIterationSummary> {
    let derived = derive_once_with_heartbeat(pool, loop_heartbeat).await?;
    let applied = apply::apply_pending_invalidations_with_heartbeat(
        pool,
        PROJECTION_APPLY_BATCH_LIMIT,
        text_hydration_config,
        loop_heartbeat,
    )
    .await?;

    let summary = ProjectionApplyIterationSummary {
        scanned_event_count: derived.scanned_event_count,
        enqueued_invalidation_count: derived.enqueued_invalidation_count,
        claimed_invalidation_count: applied.claimed_invalidation_count,
        applied_invalidation_count: applied.applied_invalidation_count,
        failed_invalidation_count: applied.failed_invalidation_count,
    };

    if summary.made_progress() {
        info!(
            service = "worker",
            projection_apply = true,
            scanned_event_count = summary.scanned_event_count,
            enqueued_invalidation_count = summary.enqueued_invalidation_count,
            claimed_invalidation_count = summary.claimed_invalidation_count,
            applied_invalidation_count = summary.applied_invalidation_count,
            failed_invalidation_count = summary.failed_invalidation_count,
            "continuous projection apply iteration completed"
        );
    }

    Ok(summary)
}

#[cfg(test)]
pub(crate) async fn derive_once(
    pool: &PgPool,
) -> Result<derive::ProjectionInvalidationDeriveSummary> {
    derive_once_inner(pool, None).await
}

pub(crate) async fn derive_once_with_heartbeat(
    pool: &PgPool,
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<derive::ProjectionInvalidationDeriveSummary> {
    derive_once_inner(pool, Some(loop_heartbeat)).await
}

async fn derive_once_inner(
    pool: &PgPool,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<derive::ProjectionInvalidationDeriveSummary> {
    let complete_upper = derive::capture_normalized_event_change_watermark(pool).await?;
    let mut remaining = NORMALIZED_EVENT_DERIVE_BATCH_LIMIT;
    let mut summary = derive::ProjectionInvalidationDeriveSummary::default();

    while remaining > 0 {
        let derived = derive::derive_normalized_event_invalidations_through(
            pool,
            remaining.min(NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT),
            complete_upper,
        )
        .await?;
        if derived.scanned_event_count == 0 {
            break;
        }

        remaining -= derived.scanned_event_count;
        summary.scanned_event_count += derived.scanned_event_count;
        summary.enqueued_invalidation_count += derived.enqueued_invalidation_count;
        if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
            loop_heartbeat.record_if_due(pool).await;
        }
    }

    Ok(summary)
}

pub(crate) async fn has_primary_hydration_blocking_work(pool: &PgPool) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        WITH cursor AS (
            SELECT COALESCE((
                SELECT last_change_id
                FROM projection_apply_cursors
                WHERE cursor_name = $1
            ), 0) AS last_change_id
        ),
        watermark AS (
            SELECT COALESCE(MAX(change_id), 0) AS max_change_id
            FROM projection_normalized_event_changes
        )
        SELECT
            EXISTS (
                SELECT 1
                FROM projection_invalidations
                WHERE state = 'pending'::projection_invalidation_state
                  AND (
                    (
                        claim_token IS NOT NULL
                        AND claimed_at >= now() - $2::INTERVAL
                    )
                    OR (
                        (
                            claim_token IS NULL
                            OR claimed_at < now() - $2::INTERVAL
                        )
                        AND (
                            last_failure_at IS NULL
                            OR last_failure_at < now() - $3::INTERVAL
                        )
                    )
                    OR (
                        projection = $4
                        AND last_failure_at >= now() - $3::INTERVAL
                    )
                  )
            )
            OR cursor.last_change_id < watermark.max_change_id
        FROM cursor
        CROSS JOIN watermark
        "#,
    )
    .bind(NORMALIZED_EVENT_CURSOR)
    .bind(CLAIM_RETRY_DELAY)
    .bind(FAILURE_RETRY_DELAY)
    .bind(PRIMARY_NAMES_CURRENT_PROJECTION)
    .fetch_one(pool)
    .await
    .context("failed to inspect projection apply work before primary-name hydration")
}

pub(crate) async fn load_chain_checkpoint_max_block(pool: &PgPool) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT NULLIF(MAX(GREATEST(
            COALESCE(canonical_block_number, -1),
            COALESCE(safe_block_number, -1),
            COALESCE(finalized_block_number, -1)
        )), -1)
        FROM chain_checkpoints
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to load chain-checkpoint projection replay target block")
}

#[cfg(test)]
pub(crate) mod heartbeat_tests {
    use std::time::Duration;

    use anyhow::Context;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use sqlx::{Postgres, Transaction};
    use tokio::time::{sleep, timeout};

    use super::*;

    pub(crate) async fn seed_blocked_later_progress_unit(
        database: &TestDatabase,
    ) -> Result<(i64, Transaction<'static, Postgres>)> {
        sqlx::query(
            r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                event_kind,
                source_family,
                manifest_version,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state,
                observed_at
            )
            SELECT
                'projection-derive-heartbeat-' || series::TEXT,
                'ens',
                CASE WHEN series = $1 + 1 THEN 'ens:blocked.eth' END,
                'HeartbeatProgress',
                'test',
                1,
                '{}'::jsonb,
                'heartbeat_progress_test',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{}'::jsonb,
                clock_timestamp()
            FROM generate_series(1::BIGINT, $1 + 1) AS series
            "#,
        )
        .bind(NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT)
        .execute(database.pool())
        .await?;
        let first_progress_change_id = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT change_id
            FROM projection_normalized_event_changes
            ORDER BY change_id
            LIMIT 1 OFFSET $1
            "#,
        )
        .bind(NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT - 1)
        .fetch_one(database.pool())
        .await?;

        sqlx::query(
            r#"
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload
            )
            VALUES (
                'name_current',
                'ens:blocked.eth',
                '{"logical_name_id":"ens:blocked.eth"}'::jsonb
            )
            "#,
        )
        .execute(database.pool())
        .await?;
        let mut later_unit_blocker = database.pool().begin().await?;
        sqlx::query(
            r#"
            UPDATE projection_invalidations
            SET generation = generation
            WHERE projection = 'name_current'
              AND projection_key = 'ens:blocked.eth'
            "#,
        )
        .execute(&mut *later_unit_blocker)
        .await?;

        Ok((first_progress_change_id, later_unit_blocker))
    }

    pub(crate) async fn wait_for_derive_cursor(
        database: &TestDatabase,
        expected: i64,
    ) -> Result<()> {
        loop {
            let cursor = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT last_change_id
                FROM projection_apply_cursors
                WHERE cursor_name = $1
                "#,
            )
            .bind(NORMALIZED_EVENT_CURSOR)
            .fetch_optional(database.pool())
            .await?;
            if cursor == Some(expected) {
                return Ok(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub(crate) async fn wait_for_fresh_worker_heartbeat(
        database: &TestDatabase,
        instance_id: &str,
    ) -> Result<()> {
        loop {
            let heartbeat = bigname_storage::load_service_loop_heartbeat(
                database.pool(),
                bigname_storage::WORKER_SERVICE_NAME,
                instance_id,
            )
            .await?
            .context("projection derive must retain its registered heartbeat")?;
            if heartbeat.age_seconds <= 1 {
                return Ok(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn detached_derive_commits_before_a_later_progress_unit_finishes() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_worker_detached_projection_derive_progress_test")
                .pool_max_connections(5),
            &bigname_storage::MIGRATOR,
            "failed to migrate detached projection derive progress test database",
        )
        .await?;
        let (first_progress_change_id, later_unit_blocker) =
            seed_blocked_later_progress_unit(&database).await?;

        let derive_pool = database.pool().clone();
        let derive = tokio::spawn(async move { derive_once(&derive_pool).await });
        match timeout(
            Duration::from_secs(2),
            wait_for_derive_cursor(&database, first_progress_change_id),
        )
        .await
        {
            Ok(result) => result?,
            Err(error) => {
                derive.abort();
                later_unit_blocker.rollback().await?;
                let _ = derive.await;
                database.cleanup().await?;
                return Err(error)
                    .context("detached derive did not commit a bounded progress unit");
            }
        }
        assert!(
            !derive.is_finished(),
            "the later derive unit must still be blocked after earlier progress commits"
        );

        later_unit_blocker.commit().await?;
        let summary = timeout(Duration::from_secs(10), derive)
            .await
            .context("detached derive did not finish after the later unit was released")?
            .context("detached derive task failed")??;
        assert_eq!(
            summary.scanned_event_count,
            NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT + 1
        );
        database.cleanup().await
    }

    #[tokio::test]
    async fn large_derive_batch_beats_before_a_later_progress_unit_finishes() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_worker_projection_derive_heartbeat_test")
                .pool_max_connections(5),
            &bigname_storage::MIGRATOR,
            "failed to migrate projection derive heartbeat test database",
        )
        .await?;
        let instance_id = "projection-derive-progress-test";
        bigname_storage::register_service_loop(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE service_loop_heartbeats
            SET started_at = clock_timestamp() - INTERVAL '2 minutes',
                heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
            WHERE service_name = 'worker'
              AND instance_id = $1
            "#,
        )
        .bind(instance_id)
        .execute(database.pool())
        .await?;
        let (first_progress_change_id, later_unit_blocker) =
            seed_blocked_later_progress_unit(&database).await?;

        let derive_pool = database.pool().clone();
        let derive = tokio::spawn(async move {
            let mut heartbeat = LoopHeartbeat::new(instance_id.to_owned(), Duration::ZERO);
            derive_once_with_heartbeat(&derive_pool, &mut heartbeat).await
        });
        timeout(
            Duration::from_secs(10),
            wait_for_derive_cursor(&database, first_progress_change_id),
        )
        .await
        .context("first bounded derive unit did not commit")??;
        timeout(
            Duration::from_secs(10),
            wait_for_fresh_worker_heartbeat(&database, instance_id),
        )
        .await
        .context("first bounded derive unit did not record a heartbeat")??;
        assert!(
            !derive.is_finished(),
            "the later derive unit must still be blocked when the heartbeat is inspected"
        );

        later_unit_blocker.commit().await?;
        let summary = timeout(Duration::from_secs(10), derive)
            .await
            .context("derive did not finish after the later unit was released")?
            .context("derive task failed")??;
        assert_eq!(
            summary.scanned_event_count,
            NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT + 1
        );
        database.cleanup().await
    }
}
