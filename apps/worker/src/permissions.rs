mod canonicality;
mod json;
mod load;
mod project;
mod resource_summary;
#[allow(clippy::duplicate_mod)]
#[path = "staged_rebuild.rs"]
mod staged_rebuild;
#[cfg(test)]
mod tests;
mod types;

use anyhow::{Context, Result};
use bigname_storage::replace_permissions_current_resource_projection;
use futures_util::{StreamExt, TryStreamExt, pin_mut, stream};
use load::stream_target_resource_ids_after;
use project::build_resource_projection;
pub(crate) use project::{mask_effective_powers_for_fuse_state, scope_fuse_state_from_after_state};
use sqlx::PgPool;
use uuid::Uuid;

use crate::primary_name::rebuild_heartbeat::{
    LoopHeartbeat, record_rebuild_progress, run_rebuild_phase,
};

use staged_rebuild::{
    count_rows, publish_permissions_current_stage_tables,
    stage_permissions_current_resource_summaries, stage_permissions_current_rows,
};

const EVENT_KIND_AUTHORITY_EPOCH_CHANGED: &str = "AuthorityEpochChanged";
const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_ROOT_PERMISSION_CHANGED: &str = "RootPermissionChanged";
const EVENT_KIND_PERMISSION_SCOPE_CHANGED: &str = "PermissionScopeChanged";
const EVENT_KIND_REGISTRATION_GRANTED: &str = "RegistrationGranted";
const EVENT_KIND_TOKEN_RESOURCE_LINKED: &str = "TokenResourceLinked";
const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const PERMISSIONS_CURRENT_DERIVATION_KIND: &str = "permissions_current_rebuild";
const PERMISSIONS_ENUMERATION_BASIS: &str = "resource_permissions";
#[cfg(not(test))]
const PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE: usize = 2_000;
#[cfg(test)]
const PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE: usize = 1;
const PERMISSIONS_CURRENT_REBUILD_CONCURRENCY: usize = 8;
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PermissionsCurrentRebuildSummary {
    pub requested_resource_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_permissions_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<PermissionsCurrentRebuildSummary> {
    rebuild_permissions_current_inner(pool, resource_id, None).await
}

async fn rebuild_permissions_current_inner(
    pool: &PgPool,
    resource_id: Option<&str>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<PermissionsCurrentRebuildSummary> {
    match resource_id {
        Some(resource_id) => rebuild_one_resource(pool, resource_id, loop_heartbeat).await,
        None => {
            let summary = rebuild_all_resources(pool, None, loop_heartbeat).await?;
            crate::replay::staging::cleanup_projection_checkpoint(pool, "permissions_current")
                .await?;
            Ok(summary)
        }
    }
}

pub(crate) async fn rebuild_permissions_current_for_replay(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<PermissionsCurrentRebuildSummary> {
    rebuild_all_resources(pool, normalized_target_block, loop_heartbeat).await
}

async fn rebuild_all_resources(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<PermissionsCurrentRebuildSummary> {
    let previous_row_count = run_rebuild_phase(
        pool,
        &mut loop_heartbeat,
        "permissions_current.count_existing",
        count_rows(pool, "permissions_current", None),
    )
    .await?;
    let mut checkpoint = crate::replay::staging::ProjectionStagingCheckpoint::load_or_start(
        pool,
        "permissions_current",
        normalized_target_block,
    )
    .await?;
    if !checkpoint.staging_complete() {
        loop {
            let input_fence = checkpoint.prepare_next_batch(pool).await?;
            let after_resource_id = checkpoint
                .last_source_key()
                .and_then(serde_json::Value::as_str)
                .map(Uuid::parse_str)
                .transpose()
                .context("permissions_current staging source key must be a UUID")?;
            let resource_ids = stream_target_resource_ids_after(
                pool,
                after_resource_id,
                i64::try_from(PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE)?,
            );
            pin_mut!(resource_ids);
            let mut page = Vec::with_capacity(PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE);
            while page.len() < PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE {
                let Some(resource_id) = resource_ids.try_next().await? else {
                    break;
                };
                page.push(resource_id);
            }
            if page.is_empty() {
                checkpoint.mark_staging_complete(pool, input_fence).await?;
                break;
            }
            let last_source_key = serde_json::Value::String(
                page.last()
                    .expect("permissions_current staging page must not be empty")
                    .to_string(),
            );
            let projections = build_permissions_page(pool, &page, &mut loop_heartbeat).await?;
            let mut rows = Vec::new();
            let mut summaries = Vec::with_capacity(projections.len());
            for projection in projections {
                rows.extend(projection.rows);
                if let Some(summary) = projection.summary {
                    summaries.push(summary);
                }
            }
            let mut transaction = pool.begin().await?;
            let staged_rows =
                stage_permissions_current_rows(&mut transaction, checkpoint.stage_table(0)?, &rows)
                    .await?;
            let staged_summaries = stage_permissions_current_resource_summaries(
                &mut transaction,
                checkpoint.stage_table(1)?,
                &summaries,
            )
            .await?;
            let progress = checkpoint.progress_after_batch(
                page.len(),
                last_source_key,
                staged_rows,
                staged_summaries,
            )?;
            checkpoint
                .persist_progress(&mut transaction, &progress, &input_fence)
                .await?;
            transaction.commit().await?;
            checkpoint.accept_progress(progress, input_fence);
            let completed_resource_count = checkpoint.completed_source_count()?;
            if completed_resource_count.is_multiple_of(5_000) {
                tracing::info!(
                    projection = "permissions_current",
                    completed_resource_count,
                    upserted_row_count = checkpoint.staged_row_count()?,
                    "permissions_current rebuild resources processed"
                );
            }
        }
    }
    let requested_resource_count = checkpoint.completed_source_count()?;
    let upserted_row_count = checkpoint.staged_row_count()?;
    let staged_summary_count = checkpoint.staged_aux_row_count()?;
    let (_deleted_row_count, published_row_count, published_summary_count) = run_rebuild_phase(
        pool,
        &mut loop_heartbeat,
        "permissions_current.publish",
        publish_permissions_current_stage_tables(
            pool,
            checkpoint.stage_table(0)?,
            checkpoint.stage_table(1)?,
            checkpoint.full_replay_input_revision(),
        ),
    )
    .await?;
    debug_assert_eq!(published_row_count as usize, upserted_row_count);
    debug_assert_eq!(published_summary_count as usize, staged_summary_count);

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count,
        upserted_row_count,
        deleted_row_count: previous_row_count,
    })
}

async fn build_permissions_page(
    pool: &PgPool,
    resource_ids: &[Uuid],
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<Vec<types::ProjectedPermissionsResource>> {
    let projections = stream::iter(resource_ids.iter().copied())
        .map(|resource_id| {
            let pool = pool.clone();
            async move { build_resource_projection(&pool, resource_id).await }
        })
        .buffer_unordered(PERMISSIONS_CURRENT_REBUILD_CONCURRENCY);
    pin_mut!(projections);
    let mut completed = Vec::with_capacity(resource_ids.len());
    while let Some(projection) = projections.try_next().await? {
        completed.push(projection);
        record_rebuild_progress(pool, loop_heartbeat).await;
    }
    Ok(completed)
}

async fn rebuild_one_resource(
    pool: &PgPool,
    resource_id: &str,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<PermissionsCurrentRebuildSummary> {
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let projection = build_resource_projection(pool, resource_id).await?;
    record_rebuild_progress(pool, &mut loop_heartbeat).await;
    let (upserted_row_count, deleted_row_count) = replace_permissions_current_resource_projection(
        pool,
        resource_id,
        &projection.rows,
        projection.summary.as_ref(),
    )
    .await?;
    record_rebuild_progress(pool, &mut loop_heartbeat).await;

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}
