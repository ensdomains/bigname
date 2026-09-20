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

/// Whether retained activated events prove this resource wrapped a registrar lease: a recorded
/// lease link, or a registrar grant for the same name in the wrap's transaction. The latter
/// covers controller-derived registration after NameWrapped. This classification survives the
/// current name row so obsolete wrapper handles cannot become registration audit handles.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L305 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
pub async fn resource_wrapped_a_registrar_lease(pool: &PgPool, resource_id: Uuid) -> Result<bool> {
    sqlx::query_scalar(
        r#"SELECT EXISTS (
            SELECT 1 FROM bigname_phase.normalized_events ne
            LEFT JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
            WHERE ne.resource_id = $1
              AND ne.source_family = 'ens_v1_wrapper_l1'
              AND ne.event_kind = 'SurfaceBound'
              AND (ne.after_state ->> 'wrapped_registrar_resource_id' IS NOT NULL
                   OR EXISTS (
                       SELECT 1 FROM bigname_phase.normalized_events grant_event
                       JOIN bigname_phase.chain_lineage grant_lineage
                         ON grant_lineage.chain_id = grant_event.chain_id
                        AND grant_lineage.block_hash = grant_event.block_hash
                       WHERE grant_event.chain_id = ne.chain_id
                         AND grant_event.block_hash = ne.block_hash
                         AND grant_event.transaction_hash = ne.transaction_hash
                         AND grant_event.logical_name_id = ne.logical_name_id
                         AND grant_event.source_family = 'ens_v1_registrar_l1'
                         AND grant_event.event_kind = 'RegistrationGranted'
                         AND grant_event.resource_id <> ne.resource_id
                         AND grant_event.consumer_visibility = 'activated'
                         AND grant_event.canonicality_state IN ('canonical', 'safe', 'finalized')
                         AND grant_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                   ))
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

/// Retained activated registry-only authority is control of a name, never its registration.
pub async fn resource_is_registry_only(pool: &PgPool, resource_id: Uuid) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM bigname_phase.normalized_events ne
            JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
            WHERE ne.resource_id = $1
              AND ne.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1',
                  'basenames_base_registry', 'basenames_base_registrar')
              AND ne.event_kind IN ('AuthorityEpochChanged', 'SurfaceBound')
              AND ne.after_state ->> 'authority_kind' = 'registry_only'
              AND ne.consumer_visibility = 'activated'
              AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
    )
    .bind(resource_id)
    .fetch_one(pool)
    .await
    .context("failed to classify historical registry control resource")
}
