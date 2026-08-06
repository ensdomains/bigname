use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    canonicality::CURRENT_PERMISSION_SUMMARY_READ_FILTER, types::PermissionsCurrentResourceSummary,
};

const SUMMARY_SELECT_COLUMNS: &str = r#"
    summary.resource_id,
    summary.authority_kind,
    summary.root_resource_id,
    CASE
        WHEN summary.support_status = 'supported' THEN jsonb_build_object(
            'status', 'full',
            'exhaustiveness', 'authoritative',
            'source_classes_considered', jsonb_build_array('permissions_current'),
            'enumeration_basis', 'resource_permissions',
            'unsupported_reason', NULL
        )
        ELSE jsonb_build_object(
            'status', 'unsupported',
            'exhaustiveness', 'not_applicable',
            'source_classes_considered', jsonb_build_array('permissions_current'),
            'enumeration_basis', 'resource_permissions',
            'unsupported_reason', CASE
                WHEN summary.unsupported_reason = 'ensv1_wrapper_holder_permissions_not_projected'
                    THEN 'ensv1_wrapper_holder_permissions_not_projected'
                ELSE 'resource_permission_authority_not_projected'
            END
        )
    END AS coverage,
    summary.provenance,
    summary.chain_positions,
    summary.canonicality_summary,
    summary.manifest_version,
    summary.last_recomputed_at
"#;
pub async fn load_permissions_current_resource_summary(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Option<PermissionsCurrentResourceSummary>> {
    sqlx::query_as::<_, PermissionsCurrentResourceSummary>(&format!(
        "SELECT {SUMMARY_SELECT_COLUMNS} \
         FROM bigname_phase.permissions_current_resource_summary summary \
         WHERE summary.resource_id = $1 AND {CURRENT_PERMISSION_SUMMARY_READ_FILTER}"
    ))
    .bind(resource_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load permissions_current resource summary for resource_id {resource_id}")
    })
}

pub async fn load_permissions_current_resource_summaries(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, PermissionsCurrentResourceSummary>> {
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query_as::<_, PermissionsCurrentResourceSummary>(&format!(
        "SELECT {SUMMARY_SELECT_COLUMNS} \
         FROM bigname_phase.permissions_current_resource_summary summary \
         WHERE summary.resource_id = ANY($1::UUID[]) \
           AND {CURRENT_PERMISSION_SUMMARY_READ_FILTER} \
         ORDER BY summary.resource_id"
    ))
    .bind(resource_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load permissions_current resource summaries for {} resource ids",
            resource_ids.len()
        )
    })?;
    Ok(rows.into_iter().map(|row| (row.resource_id, row)).collect())
}
