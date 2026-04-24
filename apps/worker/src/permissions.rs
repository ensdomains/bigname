mod canonicality;
mod json;
mod load;
mod persist;
mod project;
#[cfg(test)]
mod tests;
mod types;

use anyhow::{Context, Result};
use bigname_storage::upsert_permissions_current_rows;
use load::load_target_resource_ids;
use persist::{
    delete_stale_permissions_current_rows, delete_stale_permissions_current_rows_for_resource,
};
use project::build_rows;
use sqlx::PgPool;
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const PERMISSIONS_CURRENT_DERIVATION_KIND: &str = "permissions_current_rebuild";
const PERMISSIONS_ENUMERATION_BASIS: &str = "resource_permissions";
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
    let resource_ids = load_target_resource_ids(pool).await?;
    let rows = build_rows(pool, &resource_ids).await?;
    let upserted_row_count = upsert_permissions_current_rows(pool, &rows).await?.len();
    let deleted_row_count = delete_stale_permissions_current_rows(pool, &rows).await?;

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count: resource_ids.len(),
        upserted_row_count,
        deleted_row_count,
    })
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
