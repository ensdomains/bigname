use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

/// Per-resource permission summary, including the `resource_restrictions` block. ENSv2
/// `locked_roles` reads the registry root from the identity table and the admin rows from the
/// staged rows for in-scope resources plus the live rows for every other resource, so an
/// incremental build sees a root that its own window never touched; a full rebuild has every
/// row staged.
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    let resource_summary_query = [
        r#"
        WITH target_time AS (
            SELECT extract(epoch FROM lineage.block_timestamp) AS epoch_seconds
            FROM chain_lineage lineage
            WHERE lineage.chain_id = $1
              AND lineage.block_number = $2
              AND lineage.block_hash = $3
        ),
        resource_event_candidates AS (
            SELECT event.resource_id,
                   candidate.summary_kind,
                   candidate.authority_kind,
                   candidate.raw_fact_ref,
                   candidate.block_number,
                   candidate.block_hash, candidate.manifest_version, event.normalized_event_id,
                   row_number() OVER (
                       PARTITION BY event.resource_id, candidate.summary_kind
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS latest_rank
            FROM project_events event
            CROSS JOIN LATERAL (VALUES
                (
                    CASE WHEN event.after_state ->> 'authority_kind' IS NOT NULL
                         THEN 'direct' END,
                    event.after_state ->> 'authority_kind',
                    NULL::jsonb, NULL::bigint, NULL::text, NULL::bigint
                ),
                (
                    CASE WHEN event.after_state -> 'scope' ->> 'kind' = 'resource'
                           AND COALESCE(
                               event.after_state -> 'grant_source' ->> 'authority_kind',
                               event.after_state -> 'revocation_source' ->> 'authority_kind'
                           ) IS NOT NULL
                         THEN 'scoped' END,
                    COALESCE(
                        event.after_state -> 'grant_source' ->> 'authority_kind',
                        event.after_state -> 'revocation_source' ->> 'authority_kind'
                    ),
                    NULL::jsonb, NULL::bigint, NULL::text, NULL::bigint
                ),
                (
                    CASE WHEN event.event_kind IN (
                        'AuthorityEpochChanged', 'RegistrationGranted',
                        'PermissionChanged', 'RootPermissionChanged'
                    ) THEN 'latest' END,
                    NULL::text, event.raw_fact_ref, event.block_number,
                    event.block_hash, event.manifest_version
                )
            ) candidate(
                summary_kind, authority_kind, raw_fact_ref, block_number,
                block_hash, manifest_version
            )
            WHERE event.resource_id IS NOT NULL
              AND candidate.summary_kind IS NOT NULL
        ),
        resource_event_summaries AS (
            SELECT resource_id,
                   max(authority_kind) FILTER (
                       WHERE summary_kind = 'direct' AND latest_rank = 1
                   ) AS direct_authority_kind,
                   max(authority_kind) FILTER (
                       WHERE summary_kind = 'scoped' AND latest_rank = 1
                   ) AS scoped_authority_kind,
                   (array_agg(raw_fact_ref) FILTER (
                       WHERE summary_kind = 'latest' AND latest_rank = 1
                   ))[1] AS raw_fact_ref,
                   max(block_number) FILTER (
                       WHERE summary_kind = 'latest' AND latest_rank = 1
                   ) AS authority_block_number,
                   max(block_hash) FILTER (
                       WHERE summary_kind = 'latest' AND latest_rank = 1
                   ) AS authority_block_hash,
                   max(manifest_version) FILTER (
                       WHERE summary_kind = 'latest' AND latest_rank = 1
                   ) AS authority_manifest_version,
                   max(normalized_event_id) FILTER (WHERE summary_kind = 'latest' AND latest_rank = 1) AS authority_event_id
            FROM resource_event_candidates
            GROUP BY resource_id
        ),
        wrapper_modifiers AS (
            SELECT DISTINCT ON (event.resource_id) event.resource_id, event.normalized_event_id,
                   event.block_number, event.block_hash,
                   CASE WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric BETWEEN 0 AND 9223372036854775807
                           THEN (event.after_state ->> 'fuses')::bigint END AS fuses,
                   CASE event.after_state ->> 'wrapper_state'
                       WHEN 'wrapped' THEN 'wrapped'
                       WHEN 'emancipated' THEN 'emancipated'
                       WHEN 'locked' THEN 'locked'
                   END AS wrapper_state
            FROM project_events event
            WHERE event.event_kind = 'PermissionScopeChanged' AND event.source_family = 'ens_v1_wrapper_l1'
              AND event.resource_id IS NOT NULL
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST, event.normalized_event_id DESC
        ),
        wrapper_expiries AS (
            SELECT DISTINCT ON (event.resource_id) event.resource_id, event.normalized_event_id,
                   event.block_number, event.block_hash,
                   CASE WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric BETWEEN 0 AND 18446744073709551615
                           THEN (event.after_state ->> 'expiry')::numeric END AS expiry_seconds
            FROM project_events event
            WHERE event.event_kind = 'ExpiryChanged' AND event.resource_id IS NOT NULL
              AND (
                    event.source_family = 'ens_v1_wrapper_l1'
                 OR (
                        event.source_family = 'ens_v1_registrar_l1'
                    AND event.after_state ->> 'source_event' = 'NameRenewed'
                    AND event.after_state ->> 'authority_kind' = 'wrapper'
                 )
              )
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        -- `NameWrapped` mints the token (recorded as the wrapper `TokenControlTransferred`) and
        -- `NameUnwrapped` closes the wrapper authority epoch; the wrapper restrictions block is
        -- served only while the latest of the two is the mint.
        -- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L893-L902 @ ens_v1@91c966f)
        -- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
        wrapper_lifecycles AS (
            SELECT DISTINCT ON (event.resource_id) event.resource_id,
                   event.after_state ->> 'source_event' = 'NameUnwrapped' AS unwrapped
            FROM project_events event
            WHERE event.source_family = 'ens_v1_wrapper_l1' AND event.resource_id IS NOT NULL
              AND (
                    (event.event_kind = 'TokenControlTransferred'
                     AND event.after_state ->> 'source_event' = 'NameWrapped')
                 OR (event.event_kind IN ('AuthorityEpochChanged', 'SurfaceUnbound')
                     AND event.after_state ->> 'source_event' = 'NameUnwrapped')
              )
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        registry_roots AS (
            SELECT root.resource_id, root.provenance ->> 'registry_contract_instance_id' AS registry
            FROM resources root
            JOIN chain_lineage lineage
              ON lineage.chain_id = root.chain_id
             AND lineage.block_hash = root.block_hash
             AND lineage.block_number = root.block_number
            WHERE root.chain_id = $1
              AND root.block_number <= $2
              AND root.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND root.provenance ->> 'upstream_resource' =
                  '0x0000000000000000000000000000000000000000000000000000000000000000'
              AND root.provenance ->> 'registry_contract_instance_id' IS NOT NULL
        ),
        "#,
        super::expiry_retirement::V2_RESOURCE_REVIVALS_CTE,
        ",",
        super::expiry_retirement::CTE,
        r#",
        resource_authority AS (
            SELECT resource.*,
                   CASE COALESCE(
                       summary.direct_authority_kind, summary.scoped_authority_kind, resource.provenance ->> 'authority_kind',
                       CASE
                           WHEN COALESCE(resource.provenance ->> 'source_family', resource.provenance ->> 'binding_source_family')
                                IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                               THEN 'ens_v2_registry'
                       END
                   )
                       WHEN 'name_wrapper' THEN 'wrapper'
                       ELSE COALESCE(
                           summary.direct_authority_kind, summary.scoped_authority_kind, resource.provenance ->> 'authority_kind',
                           CASE
                               WHEN COALESCE(resource.provenance ->> 'source_family', resource.provenance ->> 'binding_source_family')
                                    IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                                   THEN 'ens_v2_registry'
                           END
                       )
                   END AS authority_kind,
                   summary.raw_fact_ref, summary.authority_block_number, summary.authority_block_hash,
                   summary.authority_manifest_version, summary.authority_event_id, modifier.fuses AS wrapper_fuses,
                   modifier.wrapper_state AS wrapper_state,
                   modifier.normalized_event_id AS wrapper_modifier_event_id, modifier.block_number AS wrapper_modifier_block_number,
                   modifier.block_hash AS wrapper_modifier_block_hash, expiry.expiry_seconds AS wrapper_expiry_seconds,
                   expiry.normalized_event_id AS wrapper_expiry_event_id, expiry.block_number AS wrapper_expiry_block_number,
                   expiry.block_hash AS wrapper_expiry_block_hash, retirement.normalized_event_id AS expiry_retirement_event_id,
                   retirement.source_manifest_id AS expiry_retirement_source_manifest_id, retirement.source_family AS expiry_retirement_source_family,
                   retirement.manifest_version AS expiry_retirement_manifest_version, retirement.block_number AS expiry_retirement_block_number,
                   retirement.block_hash AS expiry_retirement_block_hash, retirement.transaction_index AS expiry_retirement_transaction_index,
                   retirement.log_index AS expiry_retirement_log_index,
                   COALESCE(lifecycle.unwrapped, false) AS wrapper_unwrapped
            FROM project_resources resource
            LEFT JOIN resource_event_summaries summary USING (resource_id)
            LEFT JOIN wrapper_modifiers modifier USING (resource_id)
            LEFT JOIN wrapper_expiries expiry USING (resource_id)
            LEFT JOIN wrapper_lifecycles lifecycle USING (resource_id)
            LEFT JOIN expiry_retirements retirement USING (resource_id)
        ),
        admin_rows AS (
            SELECT staged.resource_id, staged.scope_kind, staged.effective_powers
            FROM project_stage_permissions_current staged
            UNION ALL
            SELECT live.resource_id, live.scope_kind, live.effective_powers
            FROM permissions_current live
            WHERE NOT $4
              AND live.provenance ->> 'chain_id' = $1
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_resources scope
                  WHERE scope.resource_id = live.resource_id
              )
        ),
        v2_admin_powers AS (
            SELECT row.resource_id, array_agg(DISTINCT power.value) AS admins
            FROM admin_rows row
            CROSS JOIN LATERAL jsonb_array_elements_text(row.effective_powers) power
            WHERE row.scope_kind IN ('registry', 'root')
              AND (power.value LIKE 'admin\_%' OR power.value = 'can_transfer_admin')
            GROUP BY row.resource_id
        )
        INSERT INTO project_stage_permissions_current_resource_summary (
            resource_id, authority_kind, root_resource_id, resource_restrictions, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT resource.resource_id,
               resource.authority_kind,
               root_resource.resource_id,
               CASE
                   WHEN resource.authority_kind = 'wrapper'
                    AND NOT resource.wrapper_unwrapped
                    AND effective_wrapper.wrapper_state IS NOT NULL
                       THEN jsonb_build_object(
                           'kind', 'ens_v1_wrapper',
                           'wrapper_state', effective_wrapper.wrapper_state,
                           'fuses', effective_wrapper.fuses,
                           'expiry_seconds', resource.wrapper_expiry_seconds
                       )
                   WHEN resource.authority_kind = 'ens_v2_registry'
                    AND EXISTS (
                        SELECT 1 FROM project_stage_permissions_current live
                        WHERE live.resource_id = resource.resource_id
                    )
                       THEN jsonb_build_object(
                           'kind', 'ens_v2_registry',
                           'locked_roles', locks.locked_roles
                       )
               END,
               'unsupported',
               CASE
                   WHEN resource.authority_kind = 'wrapper'
                       THEN 'wrapper_parent_and_resolver_delegation_not_projected'
                   WHEN resource.authority_kind IN (
                       'registrar', 'registry', 'registry_only',
                       'registry_owner', 'registrant', 'resolver',
                       'ens_v2_registry'
                   ) THEN 'operator_approval_surfaces_not_ingested'
                   ELSE 'resource_permission_authority_not_projected'
               END,
               COALESCE(resource.raw_fact_ref, resource.provenance) || jsonb_strip_nulls(jsonb_build_object(
                   'chain_id', $1, 'authority_event_id', resource.authority_event_id,
                   'expiry_retirement_event_id', resource.expiry_retirement_event_id, 'expiry_retirement_source_manifest_id', resource.expiry_retirement_source_manifest_id,
                   'expiry_retirement_source_family', resource.expiry_retirement_source_family, 'expiry_retirement_manifest_version', resource.expiry_retirement_manifest_version,
                   'expiry_retirement_chain_position', CASE WHEN resource.expiry_retirement_event_id IS NOT NULL THEN
                       jsonb_strip_nulls(jsonb_build_object('block_number', resource.expiry_retirement_block_number,
                           'block_hash', resource.expiry_retirement_block_hash, 'transaction_index',
                           resource.expiry_retirement_transaction_index, 'log_index', resource.expiry_retirement_log_index)) END,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               )) || CASE
                   WHEN resource.wrapper_fuses IS NOT NULL
                    AND resource.wrapper_expiry_seconds IS NOT NULL
                       THEN jsonb_build_object(
                           'wrapper_expiry_boundary', jsonb_build_object(
                               'fuses', resource.wrapper_fuses,
                               'expiry_seconds', resource.wrapper_expiry_seconds,
                               'fuses_event_id', resource.wrapper_modifier_event_id,
                               'expiry_event_id', resource.wrapper_expiry_event_id
                           )
                       )
                   ELSE '{}'::jsonb
               END,
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', NULLIF(GREATEST(
                       COALESCE(resource.authority_block_number, -1),
                       COALESCE(resource.wrapper_modifier_block_number, -1),
                       COALESCE(resource.wrapper_expiry_block_number, -1)), -1),
                   'block_hash', CASE
                       WHEN COALESCE(resource.wrapper_expiry_block_number, -1) >= GREATEST(
                            COALESCE(resource.wrapper_modifier_block_number, -1),
                            COALESCE(resource.authority_block_number, -1))
                           THEN resource.wrapper_expiry_block_hash
                       WHEN COALESCE(resource.wrapper_modifier_block_number, -1) >=
                            COALESCE(resource.authority_block_number, -1)
                           THEN resource.wrapper_modifier_block_hash
                       ELSE resource.authority_block_hash
                   END,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               COALESCE(
                   resource.authority_manifest_version,
                   NULLIF(resource.provenance ->> 'manifest_version', '')::bigint,
                   NULLIF(resource.provenance ->> 'binding_manifest_version', '')::bigint,
                   1
               )
        FROM resource_authority resource
        LEFT JOIN registry_roots root_resource
          ON resource.authority_kind = 'ens_v2_registry'
         AND root_resource.registry = resource.provenance ->> 'registry_contract_instance_id'
        LEFT JOIN target_time ON TRUE
        CROSS JOIN LATERAL (
            SELECT CASE
                       WHEN resource.wrapper_fuses IS NULL
                         OR resource.wrapper_state IS NULL
                         OR resource.wrapper_expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN resource.wrapper_expiry_seconds < target_time.epoch_seconds THEN 0
                       ELSE resource.wrapper_fuses
                   END AS fuses,
                   CASE
                       WHEN resource.wrapper_fuses IS NULL
                         OR resource.wrapper_state IS NULL
                         OR resource.wrapper_expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN resource.wrapper_expiry_seconds < target_time.epoch_seconds
                        AND resource.wrapper_state IN ('emancipated', 'locked') THEN NULL
                       ELSE resource.wrapper_state
                   END AS wrapper_state
        ) effective_wrapper
        -- A token-scoped role can change only through a held admin role on the registration or
        -- its root, and a registration cannot re-grant an admin role.
        -- (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L418-L424 @ ens_v2@a971bd64)
        -- (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L453-L455 @ ens_v2@a971bd64)
        -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L560-L572 @ ens_v2@a971bd64)
        -- (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L24-L45 @ ens_v2@a971bd64)
        CROSS JOIN LATERAL (
            SELECT COALESCE(jsonb_agg(to_jsonb(role.name) ORDER BY role.ordinality), '[]'::jsonb)
                       AS locked_roles
            FROM (VALUES
                (1, 'unregister', 'admin_unregister'),
                (2, 'renew', 'admin_renew'),
                (3, 'set_subregistry', 'admin_set_subregistry'),
                (4, 'set_resolver', 'admin_set_resolver'),
                (5, 'transfer', 'can_transfer_admin')
            ) role(ordinality, name, admin)
            WHERE NOT EXISTS (
                SELECT 1 FROM v2_admin_powers admins
                WHERE admins.resource_id IN (resource.resource_id, root_resource.resource_id)
                  AND role.admin = ANY(admins.admins)
            )
        ) locks
        ORDER BY resource.resource_id
        "#,
    ]
    .concat();
    sqlx::query(&resource_summary_query)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .bind(full_rebuild)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build resource permissions", error))?;
    Ok(())
}
