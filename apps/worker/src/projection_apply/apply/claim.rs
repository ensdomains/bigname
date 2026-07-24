use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use tokio::{
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use uuid::Uuid;

use crate::projection_apply::{CLAIM_RETRY_DELAY, FAILURE_RETRY_DELAY};

use super::ClaimedInvalidation;

const CLAIM_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

pub(super) fn spawn_claim_heartbeats(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
) -> Vec<JoinHandle<()>> {
    invalidations
        .iter()
        .cloned()
        .map(|invalidation| spawn_claim_heartbeat(pool.clone(), invalidation))
        .collect()
}

fn spawn_claim_heartbeat(pool: PgPool, invalidation: ClaimedInvalidation) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut heartbeat = interval(CLAIM_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            heartbeat.tick().await;
            if let Err(error) = refresh_claimed_invalidation_claim(&pool, &invalidation).await {
                tracing::warn!(
                    projection = %invalidation.projection,
                    projection_key = %invalidation.projection_key,
                    error = %error,
                    "failed to refresh projection invalidation claim heartbeat"
                );
            }
        }
    })
}

pub(super) async fn stop_claim_heartbeats(mut heartbeats: Vec<JoinHandle<()>>) {
    while let Some(heartbeat) = heartbeats.pop() {
        heartbeat.abort();
        let _ = heartbeat.await;
    }
}

pub(super) async fn refresh_claimed_invalidation_claim(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open projection invalidation claim-heartbeat transaction")?;
    bigname_storage::projection_staging::lock_current_projection_replay_version_for_projection_write_in_transaction(
        &mut transaction,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET claimed_at = now()
        WHERE projection = $1
          AND projection_key = $2
          AND claim_token = $3
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.claim_token)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!(
            "failed to refresh projection invalidation claim {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    transaction
        .commit()
        .await
        .context("failed to commit projection invalidation claim heartbeat")
}

pub(super) async fn claim_pending_invalidations(
    pool: &PgPool,
    batch_limit: i64,
    claim_token: Uuid,
) -> Result<Vec<ClaimedInvalidation>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open projection invalidation claim transaction")?;
    bigname_storage::projection_staging::lock_current_projection_replay_version_for_projection_write_in_transaction(
        &mut transaction,
    )
    .await?;
    let rows = sqlx::query(
        r#"
        WITH unclaimed_candidates AS (
            SELECT
                projection,
                projection_key,
                CASE projection
                    WHEN 'name_current' THEN 10
                    WHEN 'children_current' THEN 20
                    WHEN 'permissions_current' THEN 30
                    WHEN 'record_inventory_current' THEN 40
                    WHEN 'resolver_current' THEN 50
                    WHEN 'address_names_current' THEN 60
                    WHEN 'primary_names_current' THEN 70
                    ELSE 1000
                END AS projection_priority,
                CASE
                    WHEN projection = 'name_current'
                     AND projection_key LIKE 'basenames:%' THEN 0
                    ELSE 1
                END AS namespace_priority,
                last_changed_at
            FROM projection_invalidations
            WHERE claim_token IS NULL
              AND state = 'pending'::projection_invalidation_state
              AND (
                  last_failure_at IS NULL
                  OR last_failure_at < now() - $2::INTERVAL
              )
            ORDER BY
                CASE projection
                    WHEN 'name_current' THEN 10
                    WHEN 'children_current' THEN 20
                    WHEN 'permissions_current' THEN 30
                    WHEN 'record_inventory_current' THEN 40
                    WHEN 'resolver_current' THEN 50
                    WHEN 'address_names_current' THEN 60
                    WHEN 'primary_names_current' THEN 70
                    ELSE 1000
                END,
                CASE
                    WHEN projection = 'name_current'
                     AND projection_key LIKE 'basenames:%' THEN 0
                    ELSE 1
                END,
                last_changed_at ASC,
                projection_key ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        ),
        stale_claim_candidates AS (
            SELECT
                projection,
                projection_key,
                CASE projection
                    WHEN 'name_current' THEN 10
                    WHEN 'children_current' THEN 20
                    WHEN 'permissions_current' THEN 30
                    WHEN 'record_inventory_current' THEN 40
                    WHEN 'resolver_current' THEN 50
                    WHEN 'address_names_current' THEN 60
                    WHEN 'primary_names_current' THEN 70
                    ELSE 1000
                END AS projection_priority,
                CASE
                    WHEN projection = 'name_current'
                     AND projection_key LIKE 'basenames:%' THEN 0
                    ELSE 1
                END AS namespace_priority,
                last_changed_at
            FROM projection_invalidations
            WHERE claim_token IS NOT NULL
              AND claimed_at < now() - $3::INTERVAL
              AND state = 'pending'::projection_invalidation_state
              AND (
                  last_failure_at IS NULL
                  OR last_failure_at < now() - $2::INTERVAL
              )
            ORDER BY
                projection_priority ASC,
                namespace_priority ASC,
                claimed_at ASC,
                projection_key ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        ),
        candidates AS (
            SELECT projection, projection_key
            FROM (
                SELECT * FROM unclaimed_candidates
                UNION ALL
                SELECT * FROM stale_claim_candidates
            ) candidate
            ORDER BY
                projection_priority ASC,
                namespace_priority ASC,
                last_changed_at ASC,
                projection_key ASC
            LIMIT $1
        )
        UPDATE projection_invalidations invalidation
        SET
            claim_token = $4,
            claimed_at = now()
        FROM candidates
        WHERE invalidation.projection = candidates.projection
          AND invalidation.projection_key = candidates.projection_key
        RETURNING
            invalidation.projection,
            invalidation.projection_key,
            invalidation.key_payload,
            invalidation.generation,
            invalidation.claim_token,
            invalidation.attempt_count
        "#,
    )
    .bind(batch_limit)
    .bind(FAILURE_RETRY_DELAY)
    .bind(CLAIM_RETRY_DELAY)
    .bind(claim_token)
    .fetch_all(&mut *transaction)
    .await
    .context("failed to claim projection invalidations")?;
    transaction
        .commit()
        .await
        .context("failed to commit projection invalidation claim")?;

    rows.into_iter()
        .map(|row| {
            Ok(ClaimedInvalidation {
                projection: row.try_get("projection")?,
                projection_key: row.try_get("projection_key")?,
                key_payload: row.try_get("key_payload")?,
                generation: row.try_get("generation")?,
                claim_token: row.try_get("claim_token")?,
                attempt_count: row.try_get("attempt_count")?,
            })
        })
        .collect()
}
