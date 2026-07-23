use anyhow::{Context, Result, ensure};
use bigname_adapters::StartupAdapterProgress;
use sqlx::PgConnection;

#[path = "invalidations/capture.rs"]
mod capture;

pub(super) use capture::{
    stage_resolver_profile_projection_invalidations,
    stage_resolver_profile_projection_invalidations_with_progress,
};

const INVALIDATION_PAGE_SIZE: usize = 1_000;

/// Publish and remove staged key pages in the same transaction that commits
/// the adapter's matching chain-context reconciliation.
pub(super) async fn publish_resolver_profile_projection_invalidations(
    connection: &mut PgConnection,
    chain: &str,
) -> Result<u64> {
    publish_resolver_profile_projection_invalidations_inner(connection, chain, None).await
}

pub(super) async fn publish_resolver_profile_projection_invalidations_with_progress(
    pool: &sqlx::PgPool,
    connection: &mut PgConnection,
    chain: &str,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<u64> {
    publish_resolver_profile_projection_invalidations_inner(
        connection,
        chain,
        Some((pool, progress)),
    )
    .await
}

async fn publish_resolver_profile_projection_invalidations_inner(
    connection: &mut PgConnection,
    chain: &str,
    mut progress: Option<(&sqlx::PgPool, &mut dyn StartupAdapterProgress)>,
) -> Result<u64> {
    let mut published_count = 0u64;
    loop {
        let (page_count, deleted_count) = sqlx::query_as::<_, (i64, i64)>(
            r#"
            WITH candidate_page AS MATERIALIZED (
                SELECT projection, projection_key, key_payload
                FROM resolver_profile_reconciliation_invalidation_keys
                WHERE chain_id = $1
                ORDER BY projection, projection_key
                LIMIT $2
            ),
            upserted AS (
                INSERT INTO projection_invalidations (
                    projection,
                    projection_key,
                    key_payload,
                    invalidated_at,
                    last_changed_at
                )
                SELECT projection, projection_key, key_payload, now(), now()
                FROM candidate_page
                ON CONFLICT (projection, projection_key)
                DO UPDATE SET
                    key_payload = EXCLUDED.key_payload,
                    generation = projection_invalidations.generation + 1,
                    invalidated_at = EXCLUDED.invalidated_at,
                    last_changed_at = EXCLUDED.last_changed_at,
                    claim_token = NULL,
                    claimed_at = NULL,
                    last_failure_reason = NULL,
                    last_failure_at = NULL
                RETURNING 1
            ),
            deleted AS (
                DELETE FROM resolver_profile_reconciliation_invalidation_keys staged
                USING candidate_page candidate
                WHERE staged.chain_id = $1
                  AND staged.projection = candidate.projection
                  AND staged.projection_key = candidate.projection_key
                RETURNING 1
            )
            SELECT
                (SELECT COUNT(*)::BIGINT FROM upserted),
                (SELECT COUNT(*)::BIGINT FROM deleted)
            "#,
        )
        .bind(chain)
        .bind(i64::try_from(INVALIDATION_PAGE_SIZE)?)
        .fetch_one(&mut *connection)
        .await
        .context("failed to publish staged resolver-profile projection invalidation page")?;
        ensure!(
            page_count == deleted_count,
            "resolver-profile invalidation publication staged {page_count} rows but deleted {deleted_count}"
        );
        if page_count == 0 {
            return Ok(published_count);
        }
        published_count = published_count
            .checked_add(u64::try_from(page_count)?)
            .context("resolver-profile invalidation count overflowed u64")?;
        if let Some((pool, progress)) = progress.as_mut() {
            progress.record(pool).await?;
        }
    }
}
