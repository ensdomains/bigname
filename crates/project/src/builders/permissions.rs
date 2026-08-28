use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    // Permission rows fold resource-keyed history; no output follows historical resolver pointers.
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
                   END AS scope
            FROM project_events event
            WHERE event.event_kind IN ('PermissionChanged', 'RootPermissionChanged')
              AND event.resource_id IS NOT NULL
              AND event.after_state ->> 'subject' IS NOT NULL
              AND btrim(event.after_state ->> 'subject') <> ''
              AND jsonb_typeof(event.after_state -> 'scope') = 'object'
              AND jsonb_typeof(event.after_state -> 'effective_powers') = 'array'
        ),
        ranked AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.resource_id, event.subject, event.scope
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS latest_rank,
                   jsonb_agg(to_jsonb(event.normalized_event_id)) OVER evidence AS event_ids,
                   jsonb_agg(event.raw_fact_ref) OVER evidence AS raw_fact_refs,
                   jsonb_agg(jsonb_build_object(
                       'source_manifest_id', event.source_manifest_id,
                       'source_family', event.source_family,
                       'manifest_version', event.manifest_version
                   )) OVER evidence AS manifest_versions,
                   max(event.manifest_version) OVER (
                       PARTITION BY event.resource_id, event.subject, event.scope
                   ) AS evidence_manifest_version
            FROM decoded event
            WHERE event.scope IS NOT NULL AND btrim(event.scope) <> ''
            WINDOW evidence AS (
                PARTITION BY event.resource_id, event.subject, event.scope
                ORDER BY event.normalized_event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        ),
        latest AS (
            SELECT * FROM ranked WHERE latest_rank = 1
        ),
        v2_registration_current AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id, event.event_kind, event.after_state
            FROM project_events event
            WHERE event.resource_id IS NOT NULL
              AND (
                  (
                      event.event_kind IN (
                          'RegistrationGranted', 'RegistrationReserved'
                      )
                      AND event.source_family IN (
                          'ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1'
                      )
                  )
                  OR (
                      event.event_kind = 'RegistrationReleased'
                      AND event.after_state ->> 'source_event' = 'RegistryPathExpired'
                      AND event.after_state ->> 'derived_from' = 'interpreter_state'
                      AND event.after_state ->> 'terminal_reason' =
                          'registry_name_binding_expired'
                  )
              )
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
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
                   'normalized_event_ids', event.event_ids || CASE
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
                   'permission_manifest_versions', event.manifest_versions,
                   'raw_fact_refs', event.raw_fact_refs || CASE
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
                   'manifest_versions', event.manifest_versions || CASE
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
                   event.evidence_manifest_version,
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
        LEFT JOIN v2_registration_current registration USING (resource_id)
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
        WHERE jsonb_array_length(masked.effective_powers) > 0
          AND NOT COALESCE(
              registration.event_kind = 'RegistrationReleased'
              AND registration.after_state ->> 'source_event' = 'RegistryPathExpired'
              AND registration.after_state ->> 'derived_from' = 'interpreter_state'
              AND registration.after_state ->> 'terminal_reason' =
                  'registry_name_binding_expired',
              FALSE
          )
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
        WITH resource_event_candidates AS (
            SELECT event.resource_id,
                   candidate.summary_kind,
                   candidate.authority_kind,
                   candidate.raw_fact_ref,
                   candidate.block_number,
                   candidate.block_hash,
                   candidate.manifest_version,
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
                   ) AS authority_manifest_version
            FROM resource_event_candidates
            GROUP BY resource_id
        ),
        wrapper_modifiers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id, event.normalized_event_id,
                   event.block_number, event.block_hash,
                   CASE WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric BETWEEN 0 AND 9223372036854775807
                           THEN (event.after_state ->> 'fuses')::bigint
                   END AS fuses
            FROM project_events event
            WHERE event.event_kind = 'PermissionScopeChanged'
              AND event.source_family = 'ens_v1_wrapper_l1'
              AND event.resource_id IS NOT NULL
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        wrapper_expiries AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id, event.normalized_event_id,
                   event.block_number, event.block_hash,
                   CASE WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric
                            BETWEEN 0 AND 18446744073709551615
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
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        resource_authority AS (
            SELECT resource.*,
                   CASE COALESCE(
                       summary.direct_authority_kind,
                       summary.scoped_authority_kind,
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
                           summary.direct_authority_kind,
                           summary.scoped_authority_kind,
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
                   summary.raw_fact_ref, summary.authority_block_number,
                   summary.authority_block_hash, summary.authority_manifest_version,
                   modifier.fuses AS wrapper_fuses,
                   modifier.normalized_event_id AS wrapper_modifier_event_id,
                   modifier.block_number AS wrapper_modifier_block_number,
                   modifier.block_hash AS wrapper_modifier_block_hash,
                   expiry.expiry_seconds AS wrapper_expiry_seconds,
                   expiry.normalized_event_id AS wrapper_expiry_event_id,
                   expiry.block_number AS wrapper_expiry_block_number,
                   expiry.block_hash AS wrapper_expiry_block_hash
            FROM project_resources resource
            LEFT JOIN resource_event_summaries summary USING (resource_id)
            LEFT JOIN wrapper_modifiers modifier USING (resource_id)
            LEFT JOIN wrapper_expiries expiry USING (resource_id)
        )
        INSERT INTO project_stage_permissions_current_resource_summary (
            resource_id, authority_kind, root_resource_id, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT resource.resource_id,
               resource.authority_kind,
               root_resource.resource_id,
               'unsupported',
               CASE
                   WHEN resource.authority_kind = 'wrapper'
                       THEN 'ensv1_wrapper_holder_permissions_not_projected'
                   WHEN resource.authority_kind IN (
                       'registrar', 'registry', 'registry_only',
                       'registry_owner', 'registrant', 'resolver',
                       'ens_v2_registry'
                   ) THEN 'operator_approval_surfaces_not_ingested'
                   ELSE 'resource_permission_authority_not_projected'
               END,
               COALESCE(resource.raw_fact_ref, resource.provenance) || jsonb_build_object(
                   'chain_id', $1,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ) || CASE
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
    .map_err(|error| ProjectError::database("failed to build resource permissions", error))?;
    Ok(())
}
