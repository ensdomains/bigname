pub(crate) mod replay {
    pub(crate) use super::replay_staging as staging;
}

#[allow(dead_code)]
#[path = "../../worker/src/replay/staging.rs"]
pub(crate) mod replay_staging;

pub(crate) mod projection_apply {
    use anyhow::{Context, Result};
    use serde_json::Value;
    use sqlx::{Postgres, Transaction};

    #[derive(Clone, Copy)]
    pub(crate) enum CompletedProjectionSourceRange<'a> {
        Through(&'a Value),
        Full,
    }

    #[derive(Clone, Copy)]
    pub(crate) struct ProjectionStagingInputWatermark {
        pub(crate) normalized_change_id: i64,
        pub(crate) direct_invalidation_revision: i64,
        pub(crate) permissions_resource_revision: i64,
    }

    pub(crate) async fn capture_projection_staging_input_watermark_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<ProjectionStagingInputWatermark> {
        let normalized_change_id = sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_normalized_event_change_watermark()",
        )
        .fetch_one(&mut **transaction)
        .await
        .context("failed to capture complete normalized-event projection change watermark")?;
        let permissions_resource_revision = sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_permissions_resource_input_watermark()",
        )
        .fetch_one(&mut **transaction)
        .await
        .context("failed to capture complete permissions resource-input watermark")?;
        let direct_invalidation_revision = sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_direct_invalidation_watermark()",
        )
        .fetch_one(&mut **transaction)
        .await
        .context("failed to capture complete direct projection invalidation watermark")?;
        Ok(ProjectionStagingInputWatermark {
            normalized_change_id,
            direct_invalidation_revision,
            permissions_resource_revision,
        })
    }

    pub(crate) async fn completed_projection_sources_changed(
        transaction: &mut Transaction<'_, Postgres>,
        projection: &str,
        lower: ProjectionStagingInputWatermark,
        upper: ProjectionStagingInputWatermark,
        completed_range: CompletedProjectionSourceRange<'_>,
    ) -> Result<bool> {
        if let CompletedProjectionSourceRange::Through(last_source_key) = completed_range {
            let _ = last_source_key;
        }
        if upper.normalized_change_id <= lower.normalized_change_id
            && upper.direct_invalidation_revision <= lower.direct_invalidation_revision
            && upper.permissions_resource_revision <= lower.permissions_resource_revision
        {
            return Ok(false);
        }
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM projection_normalized_event_changes
                WHERE change_id > $1
                  AND change_id <= $2
            )
            OR EXISTS (
                SELECT 1
                FROM projection_direct_invalidation_revisions
                WHERE projection = $3
                  AND revision > $4
                  AND revision <= $5
            )
            OR (
                $3 = 'permissions_current'
                AND EXISTS (
                    SELECT 1
                    FROM projection_permissions_resource_input_revisions
                    WHERE revision > $6
                      AND revision <= $7
                )
            )
            "#,
        )
        .bind(lower.normalized_change_id)
        .bind(upper.normalized_change_id)
        .bind(projection)
        .bind(lower.direct_invalidation_revision)
        .bind(upper.direct_invalidation_revision)
        .bind(lower.permissions_resource_revision)
        .bind(upper.permissions_resource_revision)
        .fetch_one(&mut **transaction)
        .await
        .context("failed to conservatively fence API fixture projection staging")
    }
}
