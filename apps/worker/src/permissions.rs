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
use futures_util::{TryStreamExt, pin_mut};
use load::stream_target_resource_ids;
use project::build_resource_projection;
pub(crate) use project::{mask_effective_powers_for_fuse_state, scope_fuse_state_from_after_state};
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

use staged_rebuild::{
    count_rows, create_stage_table, drop_stage_table, publish_permissions_current_stage_tables,
    stage_permissions_current_resource_summaries, stage_permissions_current_rows,
};

const EVENT_KIND_AUTHORITY_EPOCH_CHANGED: &str = "AuthorityEpochChanged";
const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_ROOT_PERMISSION_CHANGED: &str = "RootPermissionChanged";
const EVENT_KIND_PERMISSION_SCOPE_CHANGED: &str = "PermissionScopeChanged";
const EVENT_KIND_REGISTRATION_GRANTED: &str = "RegistrationGranted";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const PERMISSIONS_CURRENT_DERIVATION_KIND: &str = "permissions_current_rebuild";
const PERMISSIONS_ENUMERATION_BASIS: &str = "resource_permissions";
const PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE: usize = 2_000;
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
    match resource_id {
        Some(resource_id) => rebuild_one_resource(pool, resource_id).await,
        None => rebuild_all_resources(pool).await,
    }
}

async fn rebuild_all_resources(pool: &PgPool) -> Result<PermissionsCurrentRebuildSummary> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire permissions_current staging connection")?;
    let stage_table = create_stage_table(&mut conn, "permissions_current").await?;
    let summary_stage_table =
        create_stage_table(&mut conn, "permissions_current_resource_summary").await?;
    let previous_row_count = count_rows(&mut conn, "permissions_current", None).await?;
    let mut rows = Vec::with_capacity(PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE);
    let mut summaries = Vec::with_capacity(PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE);
    let mut requested_resource_count = 0usize;
    let mut completed_resource_count = 0usize;
    let mut upserted_row_count = 0usize;
    let mut staged_summary_count = 0usize;

    let resource_ids = stream_target_resource_ids(pool);
    pin_mut!(resource_ids);
    let mut tasks = JoinSet::new();

    while tasks.len() < PERMISSIONS_CURRENT_REBUILD_CONCURRENCY {
        let Some(resource_id) = resource_ids.try_next().await? else {
            break;
        };
        requested_resource_count += 1;
        spawn_permissions_rebuild_task(&mut tasks, pool, resource_id);
    }

    while let Some(result) = tasks.join_next().await {
        completed_resource_count += 1;
        let projection = result??;
        rows.extend(projection.rows);
        if let Some(summary) = projection.summary {
            summaries.push(summary);
        }
        if rows.len() >= PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE
            || summaries.len() >= PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE
        {
            upserted_row_count +=
                stage_permissions_current_rows(&mut conn, &stage_table, &rows).await? as usize;
            rows.clear();
            staged_summary_count += stage_permissions_current_resource_summaries(
                &mut conn,
                &summary_stage_table,
                &summaries,
            )
            .await? as usize;
            summaries.clear();
        }

        if completed_resource_count % 5_000 == 0 {
            tracing::info!(
                projection = "permissions_current",
                queued_resource_count = requested_resource_count,
                completed_resource_count,
                upserted_row_count,
                "permissions_current rebuild resources processed"
            );
        }

        while tasks.len() < PERMISSIONS_CURRENT_REBUILD_CONCURRENCY {
            let Some(resource_id) = resource_ids.try_next().await? else {
                break;
            };
            requested_resource_count += 1;
            spawn_permissions_rebuild_task(&mut tasks, pool, resource_id);
        }
    }

    if !rows.is_empty() {
        upserted_row_count +=
            stage_permissions_current_rows(&mut conn, &stage_table, &rows).await? as usize;
    }
    if !summaries.is_empty() {
        staged_summary_count += stage_permissions_current_resource_summaries(
            &mut conn,
            &summary_stage_table,
            &summaries,
        )
        .await? as usize;
    }
    let (_deleted_row_count, published_row_count, published_summary_count) =
        publish_permissions_current_stage_tables(&mut conn, &stage_table, &summary_stage_table)
            .await?;
    drop_stage_table(&mut conn, &stage_table).await?;
    drop_stage_table(&mut conn, &summary_stage_table).await?;
    debug_assert_eq!(published_row_count as usize, upserted_row_count);
    debug_assert_eq!(published_summary_count as usize, staged_summary_count);

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count,
        upserted_row_count,
        deleted_row_count: previous_row_count,
    })
}

fn spawn_permissions_rebuild_task(
    tasks: &mut JoinSet<Result<types::ProjectedPermissionsResource>>,
    pool: &PgPool,
    resource_id: Uuid,
) {
    let pool = pool.clone();
    tasks.spawn(async move { build_resource_projection(&pool, resource_id).await });
}

async fn rebuild_one_resource(
    pool: &PgPool,
    resource_id: &str,
) -> Result<PermissionsCurrentRebuildSummary> {
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let projection = build_resource_projection(pool, resource_id).await?;
    let (upserted_row_count, deleted_row_count) = replace_permissions_current_resource_projection(
        pool,
        resource_id,
        &projection.rows,
        projection.summary.as_ref(),
    )
    .await?;

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}
