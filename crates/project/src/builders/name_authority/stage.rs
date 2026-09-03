use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn prepare(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    bind_resource_events(transaction).await?;
    ownerless_registry(transaction).await
}

async fn bind_resource_events(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "UPDATE project_events event SET logical_name_id = binding.logical_name_id
         FROM project_binding_candidates binding JOIN project_surfaces surface
           ON surface.logical_name_id = binding.logical_name_id
         WHERE event.logical_name_id IS NULL AND event.resource_id = binding.resource_id
           AND lower(surface.namehash) = lower(COALESCE(event.after_state->>'namehash',
               event.after_state->>'child_node', event.after_state->>'node'))",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to bind resource-keyed events", error))?;
    Ok(())
}

async fn ownerless_registry(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_latest_registry_owner ON COMMIT DROP AS
         SELECT latest.logical_name_id, latest.resource_id, latest.owner_getter,
                latest.owner_getter_reason
         FROM (
             SELECT DISTINCT ON (COALESCE(
                        event.logical_name_id,
                        linked.logical_name_id,
                        surface.logical_name_id
                    ))
                    COALESCE(
                        event.logical_name_id,
                        linked.logical_name_id,
                        surface.logical_name_id
                    ) AS logical_name_id,
                    event.resource_id,
                    event.after_state ->> 'owner_getter' AS owner_getter,
                    event.after_state ->> 'owner_getter_reason' AS owner_getter_reason
             FROM project_events event
             LEFT JOIN LATERAL (
                 SELECT candidate.logical_name_id
                 FROM project_events candidate
                 WHERE event.logical_name_id IS NULL
                   AND candidate.logical_name_id IS NOT NULL
                   AND candidate.resource_id = event.resource_id
                   AND candidate.source_family = event.source_family
                 ORDER BY candidate.block_number DESC NULLS LAST,
                          candidate.transaction_index DESC NULLS LAST,
                          candidate.log_index DESC NULLS LAST,
                          candidate.event_identity DESC
                 LIMIT 1
             ) linked ON TRUE
             LEFT JOIN project_surfaces surface
               ON event.logical_name_id IS NULL
              AND surface.namespace = event.namespace
              AND surface.visibility_state = 'active'
              AND lower(surface.namehash) = lower(COALESCE(
                      NULLIF(event.after_state ->> 'child_node', ''),
                      NULLIF(event.after_state ->> 'node', '')
                  ))
             WHERE event.event_kind = 'AuthorityTransferred'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'basenames_base_registry'
               )
               AND COALESCE(
                       event.logical_name_id,
                       linked.logical_name_id,
                       surface.logical_name_id
                   ) IS NOT NULL
             ORDER BY COALESCE(
                          event.logical_name_id,
                          linked.logical_name_id,
                          surface.logical_name_id
                      ),
                      event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
         ) latest
         WHERE latest.owner_getter =
               '0x0000000000000000000000000000000000000000'",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage ownerless registry names", error))?;
    Ok(())
}

pub(super) async fn build(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        "ALTER TABLE project_name_authority ADD PRIMARY KEY (logical_name_id)",
        "CREATE TEMP TABLE project_bindings ON COMMIT DROP AS
         SELECT candidate.*
         FROM project_name_authority authority
         JOIN project_binding_candidates candidate
           ON candidate.surface_binding_id = authority.selected_binding_id",
        "CREATE INDEX ON project_bindings (logical_name_id)",
        "CREATE TEMP TABLE project_authority_events ON COMMIT DROP AS
         SELECT DISTINCT ON (event.normalized_event_id)
                event.*, authority.logical_name_id AS selected_logical_name_id
         FROM project_events event
         JOIN project_name_authority authority
           ON authority.logical_name_id = event.logical_name_id
           OR (event.logical_name_id IS NULL
               AND ((event.resource_id = authority.selected_resource_id
                     AND event.source_family = 'ens_v1_registrar_l1'
                     AND event.event_kind IN (
                         'RegistrationGranted', 'RegistrationRenewed',
                         'RegistrationReleased', 'ExpiryChanged'
                     )
                     AND EXISTS (
                         SELECT 1 FROM project_surfaces selected_surface
                         WHERE selected_surface.logical_name_id =
                               authority.logical_name_id
                           AND lower(selected_surface.namehash) =
                               lower(event.after_state ->> 'namehash')
                     ))
                    OR (event.source_family = 'ens_v1_registrar_l1'
                        AND event.event_kind IN (
                            'RegistrationGranted', 'RegistrationRenewed',
                            'ExpiryChanged', 'TokenControlTransferred'
                        )
                        AND EXISTS (
                            SELECT 1 FROM project_events selected_wrapper
                            WHERE selected_wrapper.logical_name_id =
                                  authority.logical_name_id
                              AND selected_wrapper.resource_id =
                                  authority.selected_resource_id
                              AND selected_wrapper.source_family =
                                  'ens_v1_wrapper_l1'
                              AND selected_wrapper.event_kind = 'SurfaceBound'
                              AND (
                                  event.event_kind <> 'TokenControlTransferred'
                                  OR event.transaction_hash IS DISTINCT FROM
                                     selected_wrapper.transaction_hash
                              )
                              AND selected_wrapper.after_state ->>
                                  'wrapped_registrar_resource_id' =
                                  event.resource_id::text
                              AND lower(selected_wrapper.after_state ->> 'node') =
                                  lower(event.after_state ->> 'namehash')
                        ))))
         WHERE (
               (
                   authority.unsupported_reason IS NULL
                   AND (
                       event.resource_id = authority.selected_resource_id
                       OR (
                           event.resource_id IS NULL
                           AND CASE
                               WHEN event.source_family LIKE 'ens_v1_%' THEN 'ens_v1'
                               WHEN event.source_family LIKE 'ens_v2_%' THEN 'ens_v2'
                               WHEN event.source_family LIKE 'basenames_%' THEN 'basenames'
                           END = authority.selected_authority_arm
                       )
                       OR (
                           authority.selected_authority_arm = 'ens_v1'
                           AND event.event_kind IN (
                               'RegistrationGranted', 'RegistrationRenewed',
                               'ExpiryChanged', 'TokenControlTransferred'
                           )
                           AND event.source_family = 'ens_v1_registrar_l1'
                           AND COALESCE(
                               NULLIF(event.after_state ->> 'authority_kind', ''),
                               'registrar'
                           ) = 'registrar'
                           AND (
                               EXISTS (
                                   SELECT 1
                                   FROM project_bindings selected_binding
                                   JOIN LATERAL (
                                       SELECT predecessor.*
                                       FROM project_binding_candidates predecessor
                                       WHERE predecessor.logical_name_id =
                                             selected_binding.logical_name_id
                                         AND predecessor.authority_arm = 'ens_v1'
                                         AND (
                                             predecessor.block_number,
                                             COALESCE(
                                                 (predecessor.provenance ->> 'transaction_index')::bigint,
                                                 -1
                                             ),
                                             COALESCE(
                                                 (predecessor.provenance ->> 'log_index')::bigint, -1
                                             )
                                         ) < (
                                             selected_binding.block_number,
                                             COALESCE(
                                                 (selected_binding.provenance ->> 'transaction_index')::bigint,
                                                 -1
                                             ),
                                             COALESCE(
                                                 (selected_binding.provenance ->> 'log_index')::bigint,
                                                 -1
                                             )
                                         )
                                       ORDER BY predecessor.block_number DESC,
                                                COALESCE(
                                                    (predecessor.provenance ->> 'transaction_index')::bigint,
                                                    -1
                                                ) DESC,
                                                COALESCE(
                                                    (predecessor.provenance ->> 'log_index')::bigint,
                                                    -1
                                                ) DESC,
                                                predecessor.surface_binding_id DESC
                                       LIMIT 1
                                   ) predecessor ON TRUE
                                   WHERE selected_binding.logical_name_id =
                                         authority.logical_name_id
                                     AND predecessor.resource_id = event.resource_id
                               )
                               OR EXISTS (
                                   SELECT 1
                                   FROM project_events selected_wrapper
                                   JOIN project_events registration
                                     ON registration.resource_id = event.resource_id
                                    AND registration.source_family =
                                        'ens_v1_registrar_l1'
                                    AND registration.event_kind = 'RegistrationGranted'
                                    AND selected_wrapper.after_state ->>
                                        'wrapped_registrar_resource_id' =
                                        registration.resource_id::text
                                    AND (
                                        event.event_kind <> 'TokenControlTransferred'
                                        OR event.transaction_hash IS DISTINCT FROM
                                           selected_wrapper.transaction_hash
                                    )
                                    AND (registration.logical_name_id =
                                         selected_wrapper.logical_name_id
                                         OR (registration.logical_name_id IS NULL
                                             AND lower(registration.after_state ->> 'namehash') =
                                                 lower(selected_wrapper.after_state ->> 'node')))
                                   WHERE selected_wrapper.logical_name_id =
                                         authority.logical_name_id
                                     AND selected_wrapper.resource_id =
                                         authority.selected_resource_id
                                     AND selected_wrapper.source_family =
                                         'ens_v1_wrapper_l1'
                                     AND selected_wrapper.event_kind = 'SurfaceBound'
                               )
                           )
                           AND EXISTS (
                               SELECT 1 FROM project_events wrapper
                               WHERE wrapper.logical_name_id = authority.logical_name_id
                                 AND wrapper.resource_id = authority.selected_resource_id
                                 AND wrapper.source_family = 'ens_v1_wrapper_l1'
                                 AND wrapper.event_kind = 'PermissionScopeChanged'
                           )
                       )
                       OR (
                           authority.selected_authority_arm IN ('ens_v1', 'basenames')
                           AND event.event_kind IN (
                               'RegistrationGranted', 'RegistrationRenewed',
                               'RegistrationReleased', 'RegistrationReserved',
                               'ExpiryChanged', 'TokenControlTransferred'
                           )
                           AND EXISTS (
                               SELECT 1 FROM project_events fallback
                               WHERE fallback.logical_name_id = authority.logical_name_id
                                 AND fallback.resource_id = authority.selected_resource_id
                                 AND fallback.event_kind = 'AuthorityEpochChanged'
                                 AND fallback.after_state ->> 'authority_kind' = 'registry_only'
                           )
                           AND EXISTS (
                               SELECT 1
                               FROM project_bindings selected_binding
                               JOIN LATERAL (
                                   SELECT predecessor.*
                                   FROM project_binding_candidates predecessor
                                   WHERE predecessor.logical_name_id =
                                         selected_binding.logical_name_id
                                     AND predecessor.authority_arm =
                                         authority.selected_authority_arm
                                     AND (
                                         predecessor.block_number,
                                         COALESCE(
                                             (predecessor.provenance ->> 'transaction_index')::bigint,
                                             -1
                                         ),
                                         COALESCE(
                                             (predecessor.provenance ->> 'log_index')::bigint, -1
                                         )
                                     ) < (
                                         selected_binding.block_number,
                                         COALESCE(
                                             (selected_binding.provenance ->> 'transaction_index')::bigint,
                                             -1
                                         ),
                                         COALESCE(
                                             (selected_binding.provenance ->> 'log_index')::bigint,
                                             -1
                                         )
                                     )
                                   ORDER BY predecessor.block_number DESC,
                                            COALESCE(
                                                (predecessor.provenance ->> 'transaction_index')::bigint,
                                                -1
                                            ) DESC,
                                            COALESCE(
                                                (predecessor.provenance ->> 'log_index')::bigint,
                                                -1
                                            ) DESC,
                                            predecessor.surface_binding_id DESC
                                   LIMIT 1
                               ) predecessor ON TRUE
                               WHERE selected_binding.logical_name_id =
                                     authority.logical_name_id
                                 AND predecessor.resource_id = event.resource_id
                                 AND (
                                     event.block_number,
                                     COALESCE(event.transaction_index, -1),
                                     COALESCE(event.log_index, -1)
                                 ) >= (
                                     predecessor.block_number,
                                     COALESCE(
                                         (predecessor.provenance ->> 'transaction_index')::bigint,
                                         -1
                                     ),
                                     COALESCE(
                                         (predecessor.provenance ->> 'log_index')::bigint, -1
                                     )
                                 )
                                 AND (
                                     event.block_number,
                                     COALESCE(event.transaction_index, -1),
                                     COALESCE(event.log_index, -1)
                                 ) <= (
                                     selected_binding.block_number,
                                     COALESCE(
                                         (selected_binding.provenance ->> 'transaction_index')::bigint,
                                         -1
                                     ),
                                     COALESCE(
                                         (selected_binding.provenance ->> 'log_index')::bigint, -1
                                     )
                                 )
                           )
                       )
                   )
               )
               OR (
                   authority.unsupported_reason = 'current_authority_not_projected'
                   AND ((event.event_kind = 'ResolverChanged' AND event.resource_id IS NULL)
                        OR (event.source_family = 'ens_v1_registrar_l1'
                            AND event.event_kind IN ('RegistrationGranted',
                                'RegistrationRenewed', 'RegistrationReleased',
                                'ExpiryChanged', 'TokenControlTransferred')))
               )
           )
           AND (
               authority.authority_proof_event_id IS NULL
               OR (
                   event.block_number,
                   COALESCE(event.transaction_index, -1),
                   COALESCE(event.log_index, -1)
               ) >= (
                   (authority.authority_epoch_start_position ->> 'block_number')::bigint,
                   COALESCE(
                       (authority.authority_epoch_start_position ->> 'transaction_index')::bigint,
                       -1
                   ),
                   COALESCE(
                       (authority.authority_epoch_start_position ->> 'log_index')::bigint, -1
                   )
               )
               OR (
                   event.logical_name_id IS NULL
                   AND event.source_family = 'ens_v1_registrar_l1'
                   AND event.event_kind IN (
                       'RegistrationGranted', 'RegistrationRenewed', 'ExpiryChanged',
                       'TokenControlTransferred'
                   )
                   AND EXISTS (
                       SELECT 1 FROM project_events selected_wrapper
                       WHERE selected_wrapper.logical_name_id =
                             authority.logical_name_id
                         AND selected_wrapper.resource_id =
                             authority.selected_resource_id
                         AND selected_wrapper.source_family = 'ens_v1_wrapper_l1'
                         AND selected_wrapper.event_kind = 'SurfaceBound'
                         AND (
                             event.event_kind <> 'TokenControlTransferred'
                             OR event.transaction_hash IS DISTINCT FROM
                                selected_wrapper.transaction_hash
                         )
                         AND selected_wrapper.after_state ->>
                             'wrapped_registrar_resource_id' = event.resource_id::text
                         AND lower(selected_wrapper.after_state ->> 'node') =
                             lower(event.after_state ->> 'namehash')
                   )
               )
           )
         ORDER BY event.normalized_event_id",
        "UPDATE project_authority_events
         SET logical_name_id = selected_logical_name_id
         WHERE logical_name_id IS NULL",
        "ALTER TABLE project_authority_events DROP COLUMN selected_logical_name_id",
        "CREATE INDEX ON project_authority_events (logical_name_id, normalized_event_id)",
        "CREATE INDEX ON project_authority_events (resource_id, normalized_event_id)",
        "CREATE TEMP TABLE project_registration_events ON COMMIT DROP AS
         SELECT event.*
         FROM project_authority_events event
         WHERE event.event_kind IN (
             'RegistrationGranted', 'RegistrationReleased', 'TokenControlTransferred'
         )
           AND NOT (
               (
                   event.event_kind = 'TokenControlTransferred'
                   AND event.source_family = 'ens_v1_wrapper_l1'
                   AND COALESCE(event.after_state ->> 'source_event', '') = 'NameWrapped'
                   AND EXISTS (
                       SELECT 1
                       FROM project_authority_events wrapper_binding
                       JOIN project_authority_events registrar_grant
                         ON registrar_grant.resource_id::text =
                            wrapper_binding.after_state ->> 'wrapped_registrar_resource_id'
                        AND registrar_grant.source_family = 'ens_v1_registrar_l1'
                        AND registrar_grant.event_kind = 'RegistrationGranted'
                        AND registrar_grant.transaction_hash IS DISTINCT FROM
                            wrapper_binding.transaction_hash
                       WHERE wrapper_binding.logical_name_id = event.logical_name_id
                         AND wrapper_binding.resource_id = event.resource_id
                         AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
                         AND wrapper_binding.event_kind = 'SurfaceBound'
                   )
               ) OR (
                   event.event_kind = 'TokenControlTransferred'
                   AND event.source_family = 'ens_v1_registrar_l1'
                   AND EXISTS (
                       SELECT 1
                       FROM project_authority_events wrapper_binding
                       JOIN project_authority_events registrar_grant
                         ON registrar_grant.resource_id = event.resource_id
                        AND registrar_grant.source_family = 'ens_v1_registrar_l1'
                        AND registrar_grant.event_kind = 'RegistrationGranted'
                        AND registrar_grant.transaction_hash IS DISTINCT FROM
                            wrapper_binding.transaction_hash
                       WHERE wrapper_binding.logical_name_id = event.logical_name_id
                         AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
                         AND wrapper_binding.event_kind = 'SurfaceBound'
                         AND wrapper_binding.transaction_hash = event.transaction_hash
                         AND wrapper_binding.after_state ->>
                             'wrapped_registrar_resource_id' = event.resource_id::text
                   )
               )
           )",
        "CREATE INDEX ON project_registration_events (logical_name_id, normalized_event_id)",
        "CREATE TEMP TABLE project_name_serving ON COMMIT DROP AS
         SELECT authority.logical_name_id,
                pointer.resource_id AS serving_resource_id,
                pointer.chain_id AS resolver_chain_id,
                lower(pointer.after_state ->> 'resolver') AS resolver_address,
                pointer.normalized_event_id AS pointer_event_id,
                pointer.event_identity AS pointer_event_identity,
                pointer.block_number AS pointer_block_number,
                pointer.transaction_index AS pointer_transaction_index,
                pointer.log_index AS pointer_log_index,
                'retained_registry_resolver_pointer'::text AS read_reachability_basis,
                authority.owner_getter_reason
         FROM project_name_authority authority
         JOIN LATERAL (
             SELECT event.*
             FROM project_events event
             WHERE event.logical_name_id = authority.logical_name_id
               AND event.event_kind = 'ResolverChanged'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'basenames_base_registry'
               )
               AND event.resource_id = authority.ownerless_registry_resource_id
             ORDER BY event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
             LIMIT 1
         ) pointer ON TRUE
         JOIN project_resources resource
           ON resource.resource_id = pointer.resource_id
          AND resource.token_lineage_id IS NULL
         WHERE authority.known_ownerless_registry
           AND NULLIF(lower(pointer.after_state ->> 'resolver'), '') IS NOT NULL
           AND lower(pointer.after_state ->> 'resolver') <>
               '0x0000000000000000000000000000000000000000'",
        "CREATE UNIQUE INDEX ON project_name_serving (logical_name_id)",
        "CREATE INDEX ON project_name_serving (serving_resource_id)",
        "CREATE INDEX ON project_name_serving (resolver_chain_id, resolver_address)",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to stage selected name authority", error)
            })?;
    }
    Ok(())
}
