use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    canonicality::CURRENT_PERMISSION_SUMMARY_READ_FILTER, types::PermissionsCurrentResourceSummary,
};

/// The phase table stores `support_status`/`unsupported_reason`, so the typed coverage is
/// synthesized here. Every branch must reproduce one of the combinations
/// `ResourcePermissionCoverage::validate` accepts; anything else fails JSON decoding and turns
/// one such row into a failed page read.
const SUMMARY_SELECT_COLUMNS: &str = r#"
    summary.resource_id,
    summary.authority_kind,
    summary.root_resource_id,
    CASE
        WHEN summary.support_status = 'supported'
         AND summary.unsupported_reason IS NULL
        THEN jsonb_build_object(
            'status', 'full',
            'exhaustiveness', 'authoritative',
            'source_classes_considered', jsonb_build_array('permissions_current'),
            'enumeration_basis', 'resource_permissions',
            'unsupported_reason', NULL
        )
        WHEN summary.unsupported_reason = 'operator_approval_surfaces_not_ingested'
        THEN jsonb_build_object(
            'status', 'partial',
            'exhaustiveness', 'best_effort',
            'source_classes_considered', jsonb_build_array('permissions_current'),
            'enumeration_basis', 'resource_permissions',
            'unsupported_reason', 'operator_approval_surfaces_not_ingested'
        )
        WHEN summary.unsupported_reason = 'wrapper_parent_and_resolver_delegation_not_projected'
        THEN jsonb_build_object(
            'status', 'partial',
            'exhaustiveness', 'best_effort',
            'source_classes_considered', jsonb_build_array(
                'permissions_current', 'ens_v1_wrapper_l1'
            ),
            'enumeration_basis', 'resource_permissions',
            'unsupported_reason', 'wrapper_parent_and_resolver_delegation_not_projected'
        )
        ELSE jsonb_build_object(
            'status', 'partial',
            'exhaustiveness', 'best_effort',
            'source_classes_considered', jsonb_build_array('permissions_current'),
            'enumeration_basis', 'resource_permissions',
            'unsupported_reason', 'resource_permission_authority_not_projected'
        )
    END AS coverage,
    summary.resource_restrictions,
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

/// Namespace membership for resource audit reads, including registrations with no current name.
/// Whether `resource_id` is a NameWrapper resource whose canonical `NameWrapped` row recorded the
/// BaseRegistrar lease it wrapped. Such a resource is never a public registration handle: the
/// lease is the registration, also after the name was unwrapped, released or registered again.
/// History rejects the same value by the same link.
pub async fn resource_wrapped_a_registrar_lease(pool: &PgPool, resource_id: Uuid) -> Result<bool> {
    sqlx::query_scalar(
        r#"SELECT EXISTS (
            SELECT 1 FROM bigname_phase.normalized_events ne
            LEFT JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
            WHERE ne.resource_id = $1
              AND ne.source_family = 'ens_v1_wrapper_l1'
              AND ne.event_kind = 'SurfaceBound'
              AND ne.after_state ->> 'wrapped_registrar_resource_id' IS NOT NULL
              AND ne.consumer_visibility = 'activated'
              AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND (ne.block_hash IS NULL
                   OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
        )"#,
    )
    .bind(resource_id)
    .fetch_one(pool)
    .await
    .context("failed to check whether a resource wrapped a registrar lease")
}

pub async fn permission_resource_matches_namespace(
    pool: &PgPool,
    resource_id: Uuid,
    namespace: &str,
) -> Result<bool> {
    sqlx::query_scalar(
        r#"SELECT EXISTS (
            SELECT 1 FROM bigname_phase.normalized_events ne
            JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
            WHERE ne.resource_id = $1 AND ne.namespace = $2
              AND ne.consumer_visibility = 'activated'
              AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        )"#,
    )
    .bind(resource_id)
    .bind(namespace)
    .fetch_one(pool)
    .await
    .context("failed to check permission resource namespace")
}
