use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH target_time AS (
            SELECT extract(epoch FROM lineage.block_timestamp) AS epoch_seconds
            FROM chain_lineage lineage
            WHERE lineage.chain_id = $1
              AND lineage.block_number = $2
              AND lineage.block_hash = $3
        ),
        wrapper_constants AS (SELECT 131072::bigint AS is_dot_eth, 7776000::numeric AS grace_period_seconds),
        decoded AS (
            SELECT event.*,
                   lower(event.after_state ->> 'subject') AS subject,
                   event.after_state -> 'scope' AS scope_detail,
                   CASE event.after_state -> 'scope' ->> 'kind'
                       WHEN 'root' THEN 'root'
                       WHEN 'registry_root' THEN 'root'
                       WHEN 'registry' THEN 'registry'
                       WHEN 'resource' THEN 'resource'
                       WHEN 'resolver' THEN 'resolver'
                       WHEN 'record_manager' THEN 'record_manager'
                       WHEN 'migration_derived' THEN 'migration_derived'
                       WHEN 'transport_derived' THEN 'transport_derived'
                   END AS scope_kind,
                   CASE event.after_state -> 'scope' ->> 'kind'
                       WHEN 'root' THEN 'root'
                       WHEN 'registry_root' THEN 'root'
                       WHEN 'registry' THEN 'registry'
                       WHEN 'resource' THEN 'resource'
                       WHEN 'resolver' THEN concat(
                           'resolver:',
                           event.after_state -> 'scope' ->> 'chain_id',
                           ':',
                           lower(event.after_state -> 'scope' ->> 'resolver_address')
                       )
                       WHEN 'record_manager' THEN concat(
                           'record_manager:',
                           event.after_state -> 'scope' ->> 'chain_id',
                           ':',
                           lower(event.after_state -> 'scope' ->> 'manager_address')
                       )
                       WHEN 'migration_derived' THEN concat(
                           'migration_derived:',
                           event.after_state -> 'scope' ->> 'predecessor_resource_id'
                       )
                       WHEN 'transport_derived' THEN concat(
                           'transport_derived:',
                           event.after_state -> 'scope' ->> 'transport'
                       )
                   END AS scope
            FROM project_events event
            WHERE event.event_kind IN ('PermissionChanged', 'RootPermissionChanged')
              AND event.resource_id IS NOT NULL
              AND event.after_state ->> 'subject' IS NOT NULL
              AND btrim(event.after_state ->> 'subject') <> ''
              AND jsonb_typeof(event.after_state -> 'scope') = 'object'
              AND jsonb_typeof(event.after_state -> 'effective_powers') = 'array'
        ),
        latest AS (
            SELECT DISTINCT ON (event.resource_id, event.subject, event.scope)
                   event.*
            FROM decoded event
            WHERE event.scope IS NOT NULL AND btrim(event.scope) <> ''
            ORDER BY event.resource_id, event.subject, event.scope,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        modifiers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.*,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric >= 0
                        AND (event.after_state ->> 'fuses')::numeric <= 9223372036854775807
                           THEN (event.after_state ->> 'fuses')::bigint
                   END AS fuses,
                   CASE event.after_state ->> 'wrapper_state'
                       WHEN 'wrapped' THEN 'wrapped'
                       WHEN 'emancipated' THEN 'emancipated'
                       WHEN 'locked' THEN 'locked'
                   END AS wrapper_state
            FROM project_events event
            WHERE event.event_kind = 'PermissionScopeChanged'
              AND event.resource_id IS NOT NULL
              AND event.source_family = 'ens_v1_wrapper_l1'
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        wrapper_expiries AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.*,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric >= 0
                        AND (event.after_state ->> 'expiry')::numeric <= 18446744073709551615
                           THEN (event.after_state ->> 'expiry')::numeric
                   END AS expiry_seconds
            FROM project_events event
            WHERE event.event_kind = 'ExpiryChanged'
              AND event.resource_id IS NOT NULL
              AND (
                    event.source_family = 'ens_v1_wrapper_l1'
                 OR (
                        event.source_family = 'ens_v1_registrar_l1'
                    AND event.after_state ->> 'source_event' = 'NameRenewed'
                    AND event.after_state ->> 'authority_kind' = 'wrapper'
                 )
              )
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        )
        INSERT INTO project_stage_permissions_current (
            resource_id, subject, scope, scope_kind, scope_detail,
            effective_powers, grant_source, revocation_source,
            inheritance_path, transfer_behavior, provenance,
            chain_positions, canonicality_summary, manifest_version
        )
        SELECT event.resource_id,
               event.subject,
               event.scope,
               event.scope_kind,
               event.scope_detail,
               masked.effective_powers,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'grant_source') = 'object'
                       THEN event.after_state -> 'grant_source'
                   ELSE '{}'::jsonb
               END,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'revocation_source') = 'object'
                       THEN event.after_state -> 'revocation_source'
                   ELSE NULL
               END,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'inheritance_path') = 'array'
                       THEN event.after_state -> 'inheritance_path'
                   ELSE '[]'::jsonb
               END,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'transfer_behavior') = 'object'
                       THEN event.after_state -> 'transfer_behavior'
                   ELSE jsonb_build_object(
                       'mode', event.after_state -> 'transfer_behavior'
                   )
               END,
               jsonb_build_object(
                   'normalized_event_ids', evidence.event_ids || CASE
                       WHEN modifier.normalized_event_id IS NOT NULL
                        AND masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(modifier.normalized_event_id)
                       ELSE '[]'::jsonb
                   END || CASE
                       WHEN wrapper_expiry.normalized_event_id IS NOT NULL AND
                            masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(wrapper_expiry.normalized_event_id)
                       ELSE '[]'::jsonb
                   END,
                   'raw_fact_refs', evidence.raw_fact_refs || CASE
                       WHEN modifier.normalized_event_id IS NOT NULL
                        AND masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(modifier.raw_fact_ref)
                       ELSE '[]'::jsonb
                   END || CASE
                       WHEN wrapper_expiry.normalized_event_id IS NOT NULL AND
                            masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(wrapper_expiry.raw_fact_ref)
                       ELSE '[]'::jsonb
                   END,
                   'manifest_versions', evidence.manifest_versions || CASE
                       WHEN modifier.normalized_event_id IS NOT NULL
                        AND masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(jsonb_build_object(
                               'source_manifest_id', modifier.source_manifest_id,
                               'source_family', modifier.source_family,
                               'manifest_version', modifier.manifest_version
                           ))
                       ELSE '[]'::jsonb
                   END || CASE
                       WHEN wrapper_expiry.normalized_event_id IS NOT NULL AND
                            masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(jsonb_build_object(
                               'source_manifest_id', wrapper_expiry.source_manifest_id,
                               'source_family', wrapper_expiry.source_family,
                               'manifest_version', wrapper_expiry.manifest_version
                           ))
                       ELSE '[]'::jsonb
                   END,
                   'derivation_kind', 'permissions_current_rebuild',
                   'chain_id', $1,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', GREATEST(
                       event.block_number,
                       modifier.block_number,
                       wrapper_expiry.block_number
                   ),
                   'block_hash', CASE
                       WHEN wrapper_expiry.block_number = GREATEST(
                           event.block_number,
                           modifier.block_number,
                           wrapper_expiry.block_number
                       ) THEN wrapper_expiry.block_hash
                       WHEN modifier.block_number > event.block_number
                           THEN modifier.block_hash
                       ELSE event.block_hash
                   END,
                   'transaction_index', event.transaction_index,
                   'log_index', event.log_index,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', event.canonicality_state,
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(
                   evidence.manifest_version,
                   CASE
                       WHEN masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN modifier.manifest_version
                   END,
                   CASE
                       WHEN masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN wrapper_expiry.manifest_version
                   END,
                   event.manifest_version
               )
        FROM latest event
        LEFT JOIN modifiers modifier USING (resource_id)
        LEFT JOIN wrapper_expiries wrapper_expiry USING (resource_id)
        LEFT JOIN target_time ON TRUE
        CROSS JOIN wrapper_constants
        CROSS JOIN LATERAL (
            SELECT CASE
                       WHEN modifier.fuses IS NULL
                         OR modifier.wrapper_state IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds THEN 0
                       ELSE modifier.fuses
                   END AS fuses,
                   CASE
                       WHEN modifier.fuses IS NULL
                         OR modifier.wrapper_state IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds
                        AND modifier.wrapper_state IN ('emancipated', 'locked') THEN NULL
                       ELSE modifier.wrapper_state
                   END AS wrapper_state
        ) effective_wrapper
        CROSS JOIN LATERAL (SELECT COALESCE(
                       (effective_wrapper.fuses & wrapper_constants.is_dot_eth) <> 0
                       AND wrapper_expiry.expiry_seconds - wrapper_constants.grace_period_seconds
                           < target_time.epoch_seconds,
                       false
                   ) AS in_grace
        ) grace
        CROSS JOIN LATERAL (
            SELECT CASE
                WHEN modifier.normalized_event_id IS NULL
                    THEN event.after_state -> 'effective_powers'
                WHEN effective_wrapper.fuses IS NULL
                  OR effective_wrapper.wrapper_state IS NULL
                    THEN '[]'::jsonb
                ELSE COALESCE((
                    SELECT jsonb_agg(to_jsonb(power.value) ORDER BY power.ordinality)
                    FROM jsonb_array_elements_text(event.after_state -> 'effective_powers')
                        WITH ORDINALITY AS power(value, ordinality)
                    WHERE (NOT grace.in_grace OR power.value IN ('approve', 'approve_wrapper'))
                      AND NOT CASE power.value
                        WHEN 'resource_control' THEN
                            effective_wrapper.wrapper_state = 'locked'
                        WHEN 'resolver_control' THEN
                            (effective_wrapper.fuses & 8) <> 0
                        WHEN 'set_resolver' THEN
                            (effective_wrapper.fuses & 8) <> 0
                        WHEN 'set_ttl' THEN
                            (effective_wrapper.fuses & 16) <> 0
                        WHEN 'create_subnames' THEN
                            (effective_wrapper.fuses & 32) <> 0
                        WHEN 'create_subdomain' THEN
                            (effective_wrapper.fuses & 32) <> 0
                        WHEN 'transfer' THEN
                            (effective_wrapper.fuses & 4) <> 0
                        WHEN 'transfer_name' THEN
                            (effective_wrapper.fuses & 4) <> 0
                        WHEN 'unwrap' THEN
                            (effective_wrapper.fuses & 1) <> 0
                        WHEN 'burn_fuses' THEN
                            (effective_wrapper.fuses & 2) <> 0
                        WHEN 'approve' THEN
                            (effective_wrapper.fuses & 64) <> 0
                        WHEN 'approve_wrapper' THEN
                            (effective_wrapper.fuses & 64) <> 0
                        ELSE false
                    END
                ), '[]'::jsonb)
            END AS effective_powers
        ) masked
        LEFT JOIN LATERAL (
            SELECT COALESCE(
                       jsonb_agg(to_jsonb(prior.normalized_event_id)
                                 ORDER BY prior.normalized_event_id),
                       '[]'::jsonb
                   ) AS event_ids,
                   COALESCE(
                       jsonb_agg(prior.raw_fact_ref ORDER BY prior.normalized_event_id),
                       '[]'::jsonb
                   ) AS raw_fact_refs,
                   COALESCE(
                       jsonb_agg(jsonb_build_object(
                           'source_manifest_id', prior.source_manifest_id,
                           'source_family', prior.source_family,
                           'manifest_version', prior.manifest_version
                       ) ORDER BY prior.normalized_event_id),
                       '[]'::jsonb
                   ) AS manifest_versions,
                   max(prior.manifest_version) AS manifest_version
            FROM decoded prior
            WHERE prior.resource_id = event.resource_id
              AND prior.subject = event.subject
              AND prior.scope = event.scope
        ) evidence ON TRUE
        WHERE jsonb_array_length(masked.effective_powers) > 0
        ORDER BY event.resource_id, event.subject, event.scope
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build permissions_current", error))?;

    sqlx::query(
        r#"
        WITH resource_authority AS (
            SELECT resource.*,
                   CASE COALESCE(
                       direct.authority_kind,
                       scoped.authority_kind,
                       resource.provenance ->> 'authority_kind',
                       CASE
                           WHEN COALESCE(
                               resource.provenance ->> 'source_family',
                               resource.provenance ->> 'binding_source_family'
                           ) IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                               THEN 'ens_v2_registry'
                       END
                   )
                       WHEN 'name_wrapper' THEN 'wrapper'
                       ELSE COALESCE(
                           direct.authority_kind,
                           scoped.authority_kind,
                           resource.provenance ->> 'authority_kind',
                           CASE
                               WHEN COALESCE(
                                   resource.provenance ->> 'source_family',
                                   resource.provenance ->> 'binding_source_family'
                               ) IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                                   THEN 'ens_v2_registry'
                           END
                       )
                   END AS authority_kind,
                   latest.raw_fact_ref,
                   latest.block_number AS authority_block_number,
                   latest.block_hash AS authority_block_hash,
                   latest.manifest_version AS authority_manifest_version
            FROM project_resources resource
            LEFT JOIN LATERAL (
                SELECT event.after_state ->> 'authority_kind' AS authority_kind
                FROM project_events event
                WHERE event.resource_id = resource.resource_id
                  AND event.after_state ->> 'authority_kind' IS NOT NULL
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) direct ON TRUE
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                           event.after_state -> 'grant_source' ->> 'authority_kind',
                           event.after_state -> 'revocation_source' ->> 'authority_kind'
                       ) AS authority_kind
                FROM project_events event
                WHERE event.resource_id = resource.resource_id
                  AND event.after_state -> 'scope' ->> 'kind' = 'resource'
                  AND COALESCE(
                      event.after_state -> 'grant_source' ->> 'authority_kind',
                      event.after_state -> 'revocation_source' ->> 'authority_kind'
                  ) IS NOT NULL
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) scoped ON TRUE
            LEFT JOIN LATERAL (
                SELECT event.raw_fact_ref,
                       event.block_number,
                       event.block_hash,
                       event.manifest_version
                FROM project_events event
                WHERE event.resource_id = resource.resource_id
                  AND event.event_kind IN (
                      'AuthorityEpochChanged', 'RegistrationGranted',
                      'PermissionChanged', 'RootPermissionChanged'
                  )
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) latest ON TRUE
        )
        INSERT INTO project_stage_permissions_current_resource_summary (
            resource_id, authority_kind, root_resource_id, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT resource.resource_id,
               resource.authority_kind,
               root_resource.resource_id,
               CASE
                   WHEN resource.authority_kind IN (
                       'registrar', 'registry', 'registry_only',
                       'registry_owner', 'registrant', 'resolver',
                       'ens_v2_registry'
                   ) THEN 'supported'
                   ELSE 'unsupported'
               END,
               CASE
                   WHEN resource.authority_kind = 'wrapper'
                       THEN 'ensv1_wrapper_holder_permissions_not_projected'
                   WHEN resource.authority_kind IN (
                       'registrar', 'registry', 'registry_only',
                       'registry_owner', 'registrant', 'resolver',
                       'ens_v2_registry'
                   ) THEN NULL
                   ELSE 'resource_permission_authority_not_projected'
               END,
               COALESCE(resource.raw_fact_ref, resource.provenance) || jsonb_build_object(
                   'chain_id', $1,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', resource.authority_block_number,
                   'block_hash', resource.authority_block_hash,
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
        LEFT JOIN project_resources root_resource
          ON resource.authority_kind = 'ens_v2_registry'
         AND root_resource.provenance ->> 'registry_contract_instance_id' =
             resource.provenance ->> 'registry_contract_instance_id'
         AND root_resource.provenance ->> 'upstream_resource' =
             '0x0000000000000000000000000000000000000000000000000000000000000000'
        ORDER BY resource.resource_id
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to build permissions_current_resource_summary",
            error,
        )
    })?;
    Ok(())
}
