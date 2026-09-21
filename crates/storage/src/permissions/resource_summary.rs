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

/// Registry-only control is not a registration handle when the same name has a distinct
/// activated registrar lease. Ordinary registry-owned subnames have no such lease and keep
/// their own resource handle, including historical resource audits.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
pub async fn resource_is_registry_control_for_registrar_lease(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM bigname_phase.normalized_events ne
            JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
            WHERE ne.resource_id = $1
              AND ne.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1',
                  'basenames_base_registry', 'basenames_base_registrar')
              AND ne.event_kind IN ('AuthorityTransferred', 'AuthorityEpochChanged', 'SurfaceBound')
              AND ne.after_state ->> 'authority_kind' = 'registry_only'
              AND EXISTS (
                  SELECT 1 FROM bigname_phase.normalized_events grant_event
                  JOIN bigname_phase.chain_lineage grant_lineage
                    ON grant_lineage.chain_id = grant_event.chain_id
                   AND grant_lineage.block_hash = grant_event.block_hash
                  WHERE grant_event.namespace = ne.namespace
                    AND grant_event.chain_id = ne.chain_id
                    AND grant_event.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
                    AND grant_event.event_kind = 'RegistrationGranted'
                    AND grant_event.resource_id <> ne.resource_id
                    AND CASE WHEN
                        COALESCE(ne.after_state ->> 'child_node',
                            ne.after_state ->> 'namehash', ne.after_state ->> 'node') IS NOT NULL
                        AND COALESCE(grant_event.after_state ->> 'child_node',
                            grant_event.after_state ->> 'namehash', grant_event.after_state ->> 'node') IS NOT NULL
                    THEN lower(COALESCE(ne.after_state ->> 'child_node',
                             ne.after_state ->> 'namehash', ne.after_state ->> 'node')) =
                         lower(COALESCE(grant_event.after_state ->> 'child_node',
                             grant_event.after_state ->> 'namehash', grant_event.after_state ->> 'node'))
                    ELSE grant_event.logical_name_id = ne.logical_name_id OR EXISTS (
                        SELECT 1 FROM bigname_phase.name_surfaces surface
                        WHERE surface.logical_name_id = ne.logical_name_id
                          AND surface.namespace = ne.namespace
                          AND surface.chain_id = ne.chain_id
                          AND surface.namehash = grant_event.after_state ->> 'namehash'
                    ) END
                    AND grant_event.consumer_visibility = 'activated'
                    AND grant_event.canonicality_state IN ('canonical', 'safe', 'finalized')
                    AND grant_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              )
              AND ne.consumer_visibility = 'activated'
              AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
    )
    .bind(resource_id)
    .fetch_one(pool)
    .await
    .context("failed to classify historical registry control resource")
}

/// Resolve pre-materialization registry control and registrar leases in either direction.
/// Both reads use one publication-bounded relation: the latest same-node authority must still
/// be registry control, and the latest grant must not have been released. In particular, an
/// earlier lease cannot become current again during a release gap or after a successor grant.
/// Current-name projection takes precedence at the API; this is its nameless evidence fallback.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
pub async fn load_registry_permission_registration_map(
    pool: &PgPool,
    resource_ids: &[Uuid],
    registration_id: Option<Uuid>,
    publication_block_bounds: &BTreeMap<String, i64>,
) -> Result<BTreeMap<Uuid, Uuid>> {
    if publication_block_bounds.is_empty() || (resource_ids.is_empty() && registration_id.is_none())
    {
        return Ok(BTreeMap::new());
    }
    // Select a batch of page resources, or controls of the explicitly requested lease. The
    // reverse lookup is only a candidate restriction; the latest-grant check below still
    // excludes obsolete leases. Direct nodes take precedence over possibly stale name links.
    let same_authority = registry_permission_same_node("candidate", "authority");
    let same_grant = registry_permission_same_node("candidate", "grant_event");
    let node_candidates = registry_permission_node_candidates();
    let anchor_identity = registry_permission_token_identity("anchor");
    let authority_candidates = registry_permission_authority_candidates(node_candidates);
    let query = format!(
        r#"WITH eligible AS NOT MATERIALIZED (
            SELECT ne.*,
                lower(COALESCE(ne.after_state ->> 'child_node',
                    ne.after_state ->> 'namehash', ne.after_state ->> 'node')) AS node,
                COALESCE(ne.namespace || ':' || lower(COALESCE(
                    ne.after_state ->> 'child_node', ne.after_state ->> 'namehash',
                    ne.after_state ->> 'node', ne.after_state #>> '{{grant_source,node}}',
                    ne.after_state #>> '{{revocation_source,node}}')), ne.logical_name_id) AS node_key
            FROM bigname_phase.normalized_events ne
            JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
             AND lineage.block_number = ne.block_number
            WHERE ne.block_number <= ($3::jsonb ->> ne.chain_id)::bigint
              AND ne.consumer_visibility = 'activated'
              AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ), anchors AS (
            SELECT DISTINCT anchor.namespace, anchor.chain_id,
                COALESCE(anchor.node, recovered.node) AS node, anchor.logical_name_id,
                (anchor.source_family LIKE 'basenames\_%') AS basenames
            FROM eligible anchor
            LEFT JOIN LATERAL ({anchor_identity}) recovered ON true
            WHERE (anchor.resource_id = ANY($1::uuid[])
                AND anchor.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1',
                    'basenames_base_registry', 'basenames_base_registrar')
                AND anchor.event_kind IN ('AuthorityTransferred', 'AuthorityEpochChanged', 'SurfaceBound')
                AND anchor.after_state ->> 'authority_kind' = 'registry_only')
              OR (anchor.resource_id = $2::uuid AND anchor.event_kind = 'RegistrationGranted'
                AND anchor.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar'))
        ), candidates AS (
            SELECT anchor.*,
                array_remove(ARRAY[anchor.namespace || ':' || anchor.node, anchor.logical_name_id]
                    || ARRAY(SELECT anchor.namespace || ':' || lower(surface.namehash)
                        FROM bigname_phase.name_surfaces surface
                        WHERE surface.namespace = anchor.namespace AND surface.chain_id = anchor.chain_id
                          AND surface.logical_name_id = anchor.logical_name_id), NULL) AS node_keys,
                array_remove(ARRAY[anchor.logical_name_id]
                    || ARRAY(SELECT surface.logical_name_id FROM bigname_phase.name_surfaces surface
                        WHERE surface.namespace = anchor.namespace AND surface.chain_id = anchor.chain_id
                          AND lower(surface.namehash) = anchor.node), NULL) AS logical_ids
            FROM anchors anchor
        )
        SELECT DISTINCT control.resource_id, lease.resource_id AS registration_id
        FROM candidates candidate
        JOIN LATERAL (
            SELECT authority.resource_id, authority.after_state, authority.event_kind
            FROM ({authority_candidates}) authority
            WHERE {same_authority}
              AND authority.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1',
                  'ens_v1_wrapper_l1', 'basenames_base_registry', 'basenames_base_registrar')
              AND authority.event_kind IN ('AuthorityTransferred', 'AuthorityEpochChanged',
                  'SurfaceBound', 'SurfaceUnbound')
              AND NOT (COALESCE(authority.after_state ->> 'surface_materialization', 'false') = 'true'
                  AND authority.after_state ->> 'authority_key' IS NULL)
            ORDER BY authority.block_number DESC, authority.log_index DESC NULLS LAST,
                authority.normalized_event_id DESC
            LIMIT 1
        ) control ON control.after_state ->> 'authority_kind' = 'registry_only'
            AND control.event_kind <> 'SurfaceUnbound'
            AND ($2::uuid IS NOT NULL OR control.resource_id = ANY($1::uuid[]))
        JOIN LATERAL (
            SELECT grant_event.* FROM ({node_candidates}) grant_event
            WHERE {same_grant}
              AND grant_event.event_kind = 'RegistrationGranted'
              AND grant_event.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
            ORDER BY grant_event.block_number DESC, grant_event.log_index DESC NULLS LAST,
                grant_event.normalized_event_id DESC
            LIMIT 1
        ) lease ON lease.resource_id <> control.resource_id
        WHERE ($2::uuid IS NULL OR lease.resource_id = $2::uuid)
          AND NOT EXISTS (
              SELECT 1 FROM eligible release
              WHERE release.resource_id = lease.resource_id
                AND release.namespace = lease.namespace AND release.chain_id = lease.chain_id
                AND release.event_kind = 'RegistrationReleased'
                AND (release.block_number, COALESCE(release.log_index, -1))
                    >= (lease.block_number, COALESCE(lease.log_index, -1))
          )"#
    );
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(&query)
        .bind(resource_ids)
        .bind(registration_id)
        .bind(serde_json::to_value(publication_block_bounds)?)
        .fetch_all(pool)
        .await
        .context("failed to resolve published registry permission registrations")?;
    let mut mapping = BTreeMap::new();
    for (resource, registration) in rows {
        if let Some(previous) = mapping.insert(resource, registration) {
            anyhow::ensure!(
                previous == registration,
                "ambiguous registry permission registration"
            );
        }
    }
    Ok(mapping)
}

fn registry_permission_same_node(left: &str, right: &str) -> String {
    format!(
        "{left}.namespace = {right}.namespace AND {left}.chain_id = {right}.chain_id
         AND CASE WHEN {left}.node IS NOT NULL AND {right}.node IS NOT NULL
             THEN {left}.node = {right}.node
             ELSE {left}.logical_name_id = {right}.logical_name_id OR EXISTS (
                 SELECT 1 FROM bigname_phase.name_surfaces surface
                 WHERE surface.logical_name_id = {left}.logical_name_id
                   AND surface.namespace = {left}.namespace AND surface.chain_id = {left}.chain_id
                   AND lower(surface.namehash) = {right}.node
             ) OR EXISTS (
                 SELECT 1 FROM bigname_phase.name_surfaces surface
                 WHERE surface.logical_name_id = {right}.logical_name_id
                   AND surface.namespace = {right}.namespace AND surface.chain_id = {right}.chain_id
                   AND lower(surface.namehash) = {left}.node
             ) END"
    )
}

// Seed each probe from the requested resource's indexed history. ENS direct nodes use the
// exact expression and literal predicate of normalized_events_v1_direct_node_probe_idx.
// Legacy logical-name evidence uses the name-history index; its final identity check still
// rejects conflicting direct nodes. Basenames remains admitted through its family branch.
fn registry_permission_node_candidates() -> &'static str {
    r"SELECT probe.* FROM eligible probe
       WHERE NOT candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'ens\_v1\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'basenames\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.logical_name_id = ANY(candidate.logical_ids)"
}

// A pre-surface token transfer can emit an epoch without node/name fields. Its token
// companion supplies only identity; the epoch still supplies authority, resource and order.
// Probe the exact raw log through normalized_events_block_idx, never arbitrary resource history.
fn registry_permission_same_raw_log(left: &str, right: &str) -> String {
    format!(
        "{left}.chain_id = {right}.chain_id AND {left}.namespace = {right}.namespace
         AND {left}.source_family = {right}.source_family
         AND {left}.manifest_version = {right}.manifest_version
         AND {left}.source_manifest_id IS NOT DISTINCT FROM {right}.source_manifest_id
         AND {left}.block_hash = {right}.block_hash AND {left}.block_number = {right}.block_number
         AND {left}.transaction_hash = {right}.transaction_hash
         AND {left}.transaction_index = {right}.transaction_index AND {left}.log_index = {right}.log_index
         AND lower({left}.raw_fact_ref ->> 'emitting_address') = lower({right}.raw_fact_ref ->> 'emitting_address')
         AND {left}.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',{left}.chain_id,
             'block_hash',{left}.block_hash,'block_number',{left}.block_number,
             'transaction_hash',{left}.transaction_hash,'transaction_index',{left}.transaction_index,'log_index',{left}.log_index)
         AND {right}.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',{right}.chain_id,
             'block_hash',{right}.block_hash,'block_number',{right}.block_number,
             'transaction_hash',{right}.transaction_hash,'transaction_index',{right}.transaction_index,'log_index',{right}.log_index)"
    )
}

fn registry_permission_token_identity(epoch: &str) -> String {
    let same_log = registry_permission_same_raw_log(epoch, "companion");
    format!(
        "SELECT min(companion.node) AS node FROM eligible companion
         WHERE {epoch}.event_kind = 'AuthorityEpochChanged'
           AND {epoch}.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
           AND {epoch}.after_state ->> 'source_event' = 'Transfer'
           AND {epoch}.node_key IS NULL
           AND companion.event_kind = 'TokenControlTransferred'
           AND companion.after_state ->> 'source_event' = 'Transfer'
           AND companion.node IS NOT NULL AND {same_log}
         HAVING count(DISTINCT companion.node) = 1"
    )
}

fn registry_permission_authority_candidates(node_candidates: &str) -> String {
    let columns = "resource_id,after_state,event_kind,namespace,chain_id,source_family,block_number,log_index,normalized_event_id,logical_name_id";
    let direct_columns = columns
        .split(',')
        .map(|c| format!("probe.{c}"))
        .collect::<Vec<_>>()
        .join(",");
    let epoch_columns = columns
        .split(',')
        .map(|c| format!("epoch.{c}"))
        .collect::<Vec<_>>()
        .join(",");
    let same_log = registry_permission_same_raw_log("token", "epoch");
    let identity = registry_permission_token_identity("epoch");
    format!(
        "SELECT {direct_columns}, probe.node FROM ({node_candidates}) probe
         UNION ALL
         SELECT {epoch_columns}, recovered.node FROM ({node_candidates}) token
         JOIN eligible epoch ON {same_log}
         JOIN LATERAL ({identity}) recovered ON recovered.node = token.node
         WHERE token.event_kind = 'TokenControlTransferred'
           AND token.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')"
    )
}
