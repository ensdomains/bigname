use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionStagingInputWatermark {
    pub(crate) normalized_change_id: i64,
    pub(crate) direct_invalidation_revision: i64,
    pub(crate) permissions_resource_revision: i64,
}

pub(crate) async fn capture_projection_staging_input_watermark_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<ProjectionStagingInputWatermark> {
    // Preserve the established normalized -> direct journal order. The resource journal sits
    // between them because storage repairs can update a resource after recording their normalized
    // change, while ordinary adapters commit identity output before their later event write.
    let normalized_change_id =
        super::capture_normalized_event_change_watermark_in_transaction(transaction)
            .await?
            .change_id;
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

pub(super) const DIRECT_INVALIDATION_REVISIONS_PREFIX: &str = r#"
WITH candidate_keys AS (
    SELECT projection, projection_key, key_payload
    FROM projection_direct_invalidation_revisions
    WHERE revision > $1
      AND revision <= $2
)
"#;

pub(super) const PERMISSIONS_RESOURCE_INPUT_REVISIONS_PREFIX: &str = r#"
WITH candidate_keys AS (
    SELECT
        'permissions_current'::TEXT AS projection,
        resource_id::TEXT AS projection_key,
        jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
    FROM projection_permissions_resource_input_revisions
    WHERE revision > $1
      AND revision <= $2
)
"#;

pub(super) async fn children_parent_changed_requires_full_restage(
    transaction: &mut Transaction<'_, Postgres>,
    lower_change_id: i64,
    upper_change_id: i64,
) -> Result<bool> {
    if upper_change_id <= lower_change_id {
        return Ok(false);
    }
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM projection_normalized_event_changes change
            JOIN normalized_events event
              ON event.normalized_event_id = change.normalized_event_id
            WHERE change.change_id > $1
              AND change.change_id <= $2
              AND event.event_kind = 'ParentChanged'
        )
        "#,
    )
    .bind(lower_change_id)
    .bind(upper_change_id)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to detect ParentChanged drift requiring children_current restaging")
}
