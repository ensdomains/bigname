mod canonicality;
mod json;
mod load;
mod persist;
mod project;
#[cfg(test)]
mod tests;
mod types;

use anyhow::{Context, Result};
use bigname_storage::{clear_permissions_current, upsert_permissions_current_rows};
use futures_util::{TryStreamExt, pin_mut};
use load::stream_target_resource_ids;
use persist::delete_stale_permissions_current_rows_for_resource;
use project::build_rows;
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
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
    let deleted_row_count = clear_permissions_current(pool).await?;
    let mut rows = Vec::with_capacity(PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE);
    let mut requested_resource_count = 0usize;
    let mut completed_resource_count = 0usize;
    let mut upserted_row_count = 0usize;

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
        rows.extend(result??);
        if rows.len() >= PERMISSIONS_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count += upsert_permissions_current_rows(pool, &rows).await?.len();
            rows.clear();
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
        upserted_row_count += upsert_permissions_current_rows(pool, &rows).await?.len();
    }

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count,
        upserted_row_count,
        deleted_row_count,
    })
}

fn spawn_permissions_rebuild_task(
    tasks: &mut JoinSet<Result<Vec<bigname_storage::PermissionsCurrentRow>>>,
    pool: &PgPool,
    resource_id: Uuid,
) {
    let pool = pool.clone();
    tasks.spawn(async move { build_rows(&pool, std::slice::from_ref(&resource_id)).await });
}

async fn rebuild_one_resource(
    pool: &PgPool,
    resource_id: &str,
) -> Result<PermissionsCurrentRebuildSummary> {
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let rows = build_rows(pool, &[resource_id]).await?;
    let upserted_row_count = upsert_permissions_current_rows(pool, &rows).await?.len();
    let deleted_row_count =
        delete_stale_permissions_current_rows_for_resource(pool, resource_id, &rows).await?;

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}
