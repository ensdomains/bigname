use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO project_stage_name_current (logical_name_id, namespace, raw_name, namehash,
            surface_binding_id, resource_id, serving_resource_id,
            token_lineage_id, binding_kind,
            declared_summary, support_status, unsupported_reason, provenance,
            chain_positions, canonicality_summary, manifest_version
        )
        SELECT surface.logical_name_id, surface.namespace, surface.raw_name,
               surface.namehash, binding.surface_binding_id, binding.resource_id,
               serving.serving_resource_id,
               resource.token_lineage_id, binding.binding_kind,
               jsonb_build_object(
                   'registration', jsonb_build_object(
                       'status', CASE
                           WHEN selected_authority.known_ownerless_registry
                               THEN 'unregistered'
                           ELSE CASE registration.event_kind
                               WHEN 'RegistrationReleased' THEN 'released'
                               WHEN 'RegistrationReserved' THEN 'reserved'
                               WHEN 'RegistrationGranted' THEN 'active'
                               WHEN 'RegistrationRenewed' THEN 'active'
                               ELSE CASE WHEN binding.resource_id IS NOT NULL
                                   THEN 'active' ELSE NULL END
                           END
                       END,
                       'authority_kind', authority_context.authority_kind,
                       'authority_key', authority_context.authority_key,
                       'registrant', registrant.registrant,
                       'expiry', to_jsonb(expiry.expiry_seconds),
                       'registered_at', registration_grant.block_timestamp,
                       'created_at', created.block_timestamp,
                       'released_at', registration.after_state -> 'released_at',
                       'latest_event_kind', registration_latest.event_kind
                   ) || CASE
                       WHEN selected_authority.known_ownerless_registry
                           THEN jsonb_build_object(
                               'authority_kind', NULL, 'authority_key', NULL,
                               'registrant', NULL, 'expiry', NULL
                           )
                       WHEN registration.event_kind = 'RegistrationReleased' AND selected_authority.selected_authority_arm = 'ens_v2'
                       THEN jsonb_build_object('authority_kind', NULL, 'authority_key', NULL,
                           'registrant', NULL, 'expiry', NULL) ELSE '{}'::jsonb END,
                   'control', CASE
                       WHEN selected_authority.known_ownerless_registry
                           THEN jsonb_build_object('status', 'unregistered')
                       WHEN registration.event_kind = 'RegistrationReleased' AND
                            selected_authority.selected_authority_arm = 'ens_v2'
                           THEN jsonb_build_object('status', 'unregistered')
                       WHEN COALESCE(
                           resource.provenance ->> 'authority_kind',
                           registration_grant.after_state ->> 'authority_kind'
                       ) IN ('wrapper', 'name_wrapper')
                           THEN jsonb_build_object(
                               'status', 'unsupported',
                               'unsupported_reason',
                                   'ENSv1 wrapper effective control is not yet projected'
                           )
                       ELSE jsonb_build_object(
                           'status', status.after_state ->> 'status',
                           'expiry', CASE
                               WHEN expiry.expiry_seconds IS NULL THEN NULL
                               ELSE to_jsonb(to_char(
                                   to_timestamp(expiry.expiry_seconds)
                                       AT TIME ZONE 'UTC',
                                   'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                               ))
                           END,
                           'registrant', registrant.registrant,
                           'registry_owner', control_owner.registry_owner,
                           'latest_event_kind', control.latest_event_kind
                       )
                   END,
                   'resolver', jsonb_build_object(
                       'chain_id', CASE
                           WHEN resolver.resolver_address IS NOT NULL
                            AND resolver.resolver_address <> '0x0000000000000000000000000000000000000000'
                            AND NOT (COALESCE(registration.event_kind, '') = 'RegistrationReleased' AND selected_authority.selected_authority_arm = 'ens_v2')
                               THEN resolver.chain_id
                           ELSE NULL
                       END,
                       'address', CASE
                           WHEN resolver.resolver_address IS NOT NULL
                            AND resolver.resolver_address <> '0x0000000000000000000000000000000000000000'
                            AND NOT (COALESCE(registration.event_kind, '') = 'RegistrationReleased' AND selected_authority.selected_authority_arm = 'ens_v2')
                               THEN resolver.resolver_address
                           ELSE NULL
                       END,
                       'latest_event_kind', resolver.event_kind
                   ),
                   'record_inventory', jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason',
                           'record_inventory remains unsupported in the ENSv1 name_current rebuild'
                   ),
                   'history', jsonb_build_object(
                       'surface_head', surface_history.pointer,
                       'resource_head', resource_history.pointer
                   ),
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted',
                       'source_classes_considered', CASE
                           WHEN corpus.has_ens_v2 THEN jsonb_build_array(
                               'ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1'
                           )
                           WHEN surface.namespace IN ('ens', 'basenames')
                               THEN jsonb_build_array('ensv1_registry_path')
                           ELSE '[]'::jsonb
                       END,
                       'unsupported_reason', to_jsonb(support.unsupported_reason),
                       'enumeration_basis', CASE
                           WHEN serving.serving_resource_id IS NOT NULL
                               THEN 'event_linked_registry_resolver'
                           WHEN corpus.has_ens_v2 THEN 'exact_name_profile'
                           ELSE 'exact_name'
                       END
                   )
               ) || CASE
                   WHEN effective_wrapper.wrapper_state IS NOT NULL
                       THEN jsonb_build_object(
                           'wrapper_state', effective_wrapper.wrapper_state,
                           'wrapper_fuses', jsonb_build_object(
                               'fuses', effective_wrapper.fuses,
                               'cannot_unwrap', (effective_wrapper.fuses & 1) <> 0,
                               'cannot_burn_fuses', (effective_wrapper.fuses & 2) <> 0,
                               'cannot_transfer', (effective_wrapper.fuses & 4) <> 0,
                               'cannot_set_resolver', (effective_wrapper.fuses & 8) <> 0,
                               'cannot_set_ttl', (effective_wrapper.fuses & 16) <> 0,
                               'cannot_create_subdomain', (effective_wrapper.fuses & 32) <> 0,
                               'cannot_approve', (effective_wrapper.fuses & 64) <> 0,
                               'parent_cannot_control', (effective_wrapper.fuses & 65536) <> 0,
                               'is_dot_eth', (effective_wrapper.fuses & 131072) <> 0,
                               'can_extend_expiry', (effective_wrapper.fuses & 262144) <> 0
                           )
                       )
                   ELSE '{}'::jsonb
               END,
               support.support_status,
               support.unsupported_reason,
               jsonb_build_object(
                   'chain_id', $1,
                   'surface_block_number', surface.block_number,
                   'selected_event_ids', COALESCE(evidence.event_ids, '[]'::jsonb),
                   'raw_fact_refs', COALESCE(evidence.raw_fact_refs, '[]'::jsonb),
                   'manifest_versions', COALESCE(
                       evidence.manifest_versions, '[]'::jsonb
                   ),
                   'derivation_kind', 'name_current_rebuild',
                   'authority_selection', jsonb_strip_nulls(jsonb_build_object(
                       'authority_arm', selected_authority.selected_authority_arm,
                       'surface_binding_id', selected_authority.selected_binding_id,
                       'resource_id', selected_authority.selected_resource_id,
                       'epoch_start_position', selected_authority.authority_epoch_start_position,
                       'proof_kind', selected_authority.authority_proof_kind,
                       'proof_event_id', selected_authority.authority_proof_event_id,
                       'proof_event_identity', selected_authority.authority_proof_event_identity,
                       'transition_id', selected_authority.authority_transition_id,
                       'lifecycle_state', selected_authority.lifecycle_state,
                       'deployment_profile', selected_authority.deployment_profile,
                       'resource_authority_context', selected_authority.resource_authority_context,
                       'unsupported_reason', selected_authority.unsupported_reason
                   )),
                   'read_reachability', jsonb_strip_nulls(jsonb_build_object(
                       'serving_resource_id', serving.serving_resource_id,
                       'basis', serving.read_reachability_basis,
                       'owner_getter_reason', serving.owner_getter_reason,
                       'pointer_event_id', serving.pointer_event_id,
                       'pointer_event_identity', serving.pointer_event_identity
                   ))
               ) || jsonb_strip_nulls(jsonb_build_object(
                   'resolver_pointer_source_family', resolver.source_family
               )),
               jsonb_build_object(
                   CASE $1
                       WHEN 'ethereum-mainnet' THEN 'ethereum'
                       WHEN 'base-mainnet' THEN 'base'
                       ELSE $1
                   END,
                   jsonb_build_object(
                       'chain_id', $1,
                       'block_number', $2,
                       'block_hash', $3,
                       'timestamp', (
                           SELECT lineage.block_timestamp
                           FROM chain_lineage lineage
                           WHERE lineage.chain_id = $1
                             AND lineage.block_number = $2
                             AND lineage.block_hash = $3
                       )
                   )
               ),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(
                   COALESCE(registration.manifest_version, 1),
                   COALESCE(authority.manifest_version, 1),
                   COALESCE(resolver.manifest_version, 1),
                   COALESCE(evidence.manifest_version, 1),
                   COALESCE(
                       NULLIF(surface.provenance ->> 'manifest_version', '')::bigint,
                       NULLIF(binding.provenance ->> 'manifest_version', '')::bigint,
                       NULLIF(resource.provenance ->> 'manifest_version', '')::bigint,
                       1
                   )
               )
        FROM project_surfaces surface
        LEFT JOIN project_name_authority selected_authority USING (logical_name_id)
        LEFT JOIN project_name_serving serving USING (logical_name_id)
        LEFT JOIN project_bindings binding USING (logical_name_id)
        LEFT JOIN project_resources resource ON resource.resource_id = binding.resource_id
        LEFT JOIN LATERAL (
            SELECT event.* FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
                  'RegistrationReserved'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) registration ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.event_kind
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
                  'RegistrationReserved',
                  'ExpiryChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) registration_latest ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.*, lineage.block_timestamp
            FROM project_authority_events event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind = 'RegistrationGranted'
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) registration_grant ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.after_state ->> 'authority_kind' AS authority_kind,
                   event.after_state ->> 'authority_key' AS authority_key
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'RegistrationGranted', 'AuthorityEpochChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) authority_context ON TRUE
        LEFT JOIN LATERAL (
            SELECT lower(CASE event.event_kind WHEN 'TokenControlTransferred' THEN event.after_state ->> 'to'
                       WHEN 'RegistrationReleased' THEN event.before_state ->> 'registrant'
                       ELSE event.after_state ->> 'registrant'
                   END) AS registrant
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationReleased', 'TokenControlTransferred'
              )
              AND CASE event.event_kind WHEN 'TokenControlTransferred' THEN event.after_state ->> 'to' WHEN 'RegistrationReleased' THEN event.before_state ->> 'registrant' ELSE event.after_state ->> 'registrant' END IS NOT NULL
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) registrant ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric =
                            trunc((event.after_state ->> 'expiry')::numeric)
                        AND (event.after_state ->> 'expiry')::numeric BETWEEN
                            -377705116800 AND 253402300799
                           THEN (event.after_state ->> 'expiry')::bigint
                       ELSE NULL
                   END AS expiry_seconds
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
              )
              AND NOT (
                  event.event_kind = 'ExpiryChanged'
                  AND (
                      event.source_family = 'ens_v1_wrapper_l1'
                      OR (
                          event.source_family = 'ens_v1_registrar_l1'
                          AND COALESCE(event.after_state ->> 'source_event', '') =
                              'NameRenewed'
                          AND COALESCE(event.after_state ->> 'authority_kind', '') =
                              'wrapper'
                      )
                  )
              )
              AND (
                  event.event_kind = 'RegistrationGranted'
                  OR jsonb_typeof(event.after_state -> 'expiry') = 'number'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) expiry ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE event.after_state ->> 'wrapper_state'
                       WHEN 'wrapped' THEN 'wrapped'
                       WHEN 'emancipated' THEN 'emancipated'
                       WHEN 'locked' THEN 'locked'
                   END AS wrapper_state,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric >= 0
                        AND (event.after_state ->> 'fuses')::numeric <= 4294967295
                           THEN (event.after_state ->> 'fuses')::bigint
                   END AS fuses
            FROM project_authority_events event
            WHERE event.resource_id = binding.resource_id
              AND event.event_kind = 'PermissionScopeChanged'
              AND event.source_family = 'ens_v1_wrapper_l1'
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) wrapper ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric >= 0
                        AND (event.after_state ->> 'expiry')::numeric <=
                            18446744073709551615
                           THEN (event.after_state ->> 'expiry')::numeric
                   END AS expiry_seconds
            FROM project_authority_events event
            WHERE event.resource_id = binding.resource_id
              AND event.event_kind = 'ExpiryChanged'
              AND (
                    event.source_family = 'ens_v1_wrapper_l1'
                 OR (
                        event.source_family = 'ens_v1_registrar_l1'
                    AND event.after_state ->> 'source_event' = 'NameRenewed'
                    AND event.after_state ->> 'authority_kind' = 'wrapper'
                 )
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) wrapper_expiry ON TRUE
        LEFT JOIN LATERAL (
            SELECT extract(epoch FROM lineage.block_timestamp) AS epoch_seconds
            FROM chain_lineage lineage
            WHERE lineage.chain_id = $1
              AND lineage.block_number = $2
              AND lineage.block_hash = $3
        ) target_time ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE
                       WHEN wrapper.wrapper_state IS NULL
                         OR wrapper.fuses IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds
                        AND wrapper.wrapper_state IN ('emancipated', 'locked') THEN NULL
                       ELSE wrapper.wrapper_state
                   END AS wrapper_state,
                   CASE
                       WHEN wrapper.fuses IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds THEN 0
                       ELSE wrapper.fuses
                   END AS fuses
        ) effective_wrapper ON TRUE
        LEFT JOIN LATERAL (
            SELECT lineage.block_timestamp
            FROM project_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id
            ORDER BY event.block_number,
                     event.transaction_index NULLS FIRST,
                     event.log_index NULLS FIRST,
                     event.normalized_event_id
            LIMIT 1
        ) created ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.*
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.after_state ? 'status'
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) status ON TRUE
        LEFT JOIN LATERAL (
            SELECT lower(CASE
                       WHEN event.after_state ->> 'owner_word_unmasked' = 'true'
                           THEN NULL
                       ELSE COALESCE(
                           event.after_state ->> 'registry_owner',
                           event.after_state ->> 'owner'
                       )
                   END) AS registry_owner
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'AuthorityTransferred', 'AuthorityEpochChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) control_owner ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.event_kind AS latest_event_kind
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'TokenControlTransferred', 'AuthorityTransferred',
                  'AuthorityEpochChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) control ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.* FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind IN (
                  'AuthorityTransferred', 'TokenControlTransferred',
                  'AuthorityEpochChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) authority ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.*,
                   lower(event.after_state ->> 'resolver') AS resolver_address
            FROM (
                SELECT selected.* FROM project_authority_events selected
                UNION ALL
                SELECT pointer.* FROM project_events pointer
                WHERE pointer.normalized_event_id = serving.pointer_event_id
                  AND NOT EXISTS (
                      SELECT 1 FROM project_authority_events selected
                      WHERE selected.normalized_event_id = pointer.normalized_event_id
                  )
            ) event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind = 'ResolverChanged'
              AND (
                  binding.resource_id IS NULL
                  OR event.resource_id = binding.resource_id
                  OR event.normalized_event_id = serving.pointer_event_id
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) resolver ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.block_number,
                   jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_kind', event.event_kind,
                       'chain_position', jsonb_strip_nulls(jsonb_build_object(
                           'chain_id', event.chain_id,
                           'block_number', event.block_number,
                           'block_hash', event.block_hash,
                           'timestamp', lineage.block_timestamp
                       ))
                   ) AS pointer
            FROM project_events event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) surface_history ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.block_number,
                   jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_kind', event.event_kind,
                       'chain_position', jsonb_strip_nulls(jsonb_build_object(
                           'chain_id', event.chain_id,
                           'block_number', event.block_number,
                           'block_hash', event.block_hash,
                           'timestamp', lineage.block_timestamp
                       ))
                   ) AS pointer
            FROM project_authority_events event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.resource_id = binding.resource_id
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) resource_history ON TRUE
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(to_jsonb(event.normalized_event_id)
                             ORDER BY event.normalized_event_id) AS event_ids,
                   jsonb_agg(event.raw_fact_ref
                             ORDER BY event.normalized_event_id) AS raw_fact_refs,
                   jsonb_agg(jsonb_build_object(
                       'source_manifest_id', event.source_manifest_id,
                       'source_family', event.source_family,
                       'manifest_version', event.manifest_version
                   ) ORDER BY event.normalized_event_id) AS manifest_versions,
                   max(event.manifest_version) AS manifest_version
            FROM project_events event
            WHERE event.logical_name_id = surface.logical_name_id
        ) evidence ON TRUE
        LEFT JOIN LATERAL (
            SELECT COALESCE(bool_or(
                       event.source_family IN (
                           'ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1'
                       )
                   ), false) AS has_ens_v2,
                   COALESCE(bool_or(
                       event.source_family LIKE 'ens_v1_%'
                   ), false) AS has_ens_v1
            FROM project_events event
            WHERE event.logical_name_id = surface.logical_name_id
        ) corpus ON TRUE
        LEFT JOIN LATERAL (
            SELECT EXISTS (
                       SELECT 1
                       FROM project_events event
                       JOIN project_manifests manifest
                         ON manifest.manifest_id = event.source_manifest_id
                        AND manifest.manifest_version = event.manifest_version
                        AND manifest.source_family = event.source_family
                       WHERE event.logical_name_id = surface.logical_name_id
                         AND event.source_family = 'ens_v2_registry_l1'
                         AND manifest.namespace = 'ens'
                         AND manifest.chain_id = 'ethereum-sepolia'
                         AND manifest.deployment_label =
                             'ens_v2_sepolia_post_audit'
                   )
                   AND EXISTS (
                       SELECT 1
                       FROM project_events event
                       JOIN project_manifests manifest
                         ON manifest.manifest_id = event.source_manifest_id
                        AND manifest.manifest_version = event.manifest_version
                        AND manifest.source_family = event.source_family
                       WHERE event.logical_name_id = surface.logical_name_id
                         AND event.source_family = 'ens_v2_registrar_l1'
                         AND manifest.namespace = 'ens'
                         AND manifest.chain_id = 'ethereum-sepolia'
                         AND manifest.deployment_label =
                             'ens_v2_sepolia_post_audit'
                         AND manifest.manifest_payload
                             -> 'capability_flags'
                             -> 'exact_name_profile'
                             ->> 'status' = 'supported'
                   ) AS supported
        ) ens_v2_profile ON TRUE
        CROSS JOIN LATERAL (
            SELECT CASE
                       WHEN selected_authority.unsupported_reason IS NOT NULL
                           THEN 'unsupported'
                       WHEN selected_authority.selected_authority_arm = 'ens_v2'
                        AND NOT ens_v2_profile.supported
                           THEN 'unsupported'
                       ELSE 'supported'
                   END AS support_status,
                   CASE
                       WHEN selected_authority.unsupported_reason IS NOT NULL
                           THEN selected_authority.unsupported_reason
                       WHEN selected_authority.selected_authority_arm = 'ens_v2'
                        AND NOT ens_v2_profile.supported
                           THEN 'ensv2_exact_name_profile_shadow'
                       ELSE NULL
                   END AS unsupported_reason
        ) support
        WHERE surface.visibility_state = 'active'
          AND surface.raw_name <> ''
        ORDER BY surface.logical_name_id
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build name_current", error))?;
    Ok(())
}
