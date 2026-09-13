use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn ownerless_registry(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
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
         SELECT DISTINCT ON (event.normalized_event_id) event.*
         FROM project_events event
         JOIN project_name_authority authority
           ON authority.logical_name_id = event.logical_name_id
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
                               'ExpiryChanged'
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
                                     ON registration.logical_name_id =
                                        selected_wrapper.logical_name_id
                                    AND registration.transaction_hash =
                                        selected_wrapper.transaction_hash
                                    AND registration.resource_id = event.resource_id
                                    AND registration.source_family =
                                        'ens_v1_registrar_l1'
                                    AND registration.event_kind = 'RegistrationGranted'
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
                   AND event.event_kind = 'ResolverChanged'
                   AND event.resource_id IS NULL
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
           )
         ORDER BY event.normalized_event_id",
        "CREATE INDEX ON project_authority_events (logical_name_id, normalized_event_id)",
        "CREATE INDEX ON project_authority_events (resource_id, normalized_event_id)",
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
               '0x0000000000000000000000000000000000000000'
         UNION ALL
         -- An ENSv2 root-registry TLD whose registration was never observed has a token resource
         -- and a resolver pointer but no surface binding, so no authority is selected. The root
         -- registry keys the pointer by token and returns it while the label is unexpired
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150-L155 @ ens_v2@a971bd64)
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64);
         -- the pointer serves as the TLD's serving resource without projecting authority. The
         -- state-derived expiry clear and release name the resource but no logical name, so the
         -- resource is followed from the name-linked pointer; a reservation or release on that
         -- resource keeps the pointer out of serving.
         SELECT authority.logical_name_id,
                pointer.resource_id AS serving_resource_id,
                pointer.chain_id AS resolver_chain_id,
                lower(pointer.after_state ->> 'resolver') AS resolver_address,
                pointer.normalized_event_id AS pointer_event_id,
                pointer.event_identity AS pointer_event_identity,
                pointer.block_number AS pointer_block_number,
                pointer.transaction_index AS pointer_transaction_index,
                pointer.log_index AS pointer_log_index,
                'root_registry_resolver_pointer'::text AS read_reachability_basis,
                NULL::text AS owner_getter_reason
         FROM project_name_authority authority
         JOIN LATERAL (
             SELECT event.*
             FROM project_events event
             WHERE event.event_kind = 'ResolverChanged'
               AND event.source_family = 'ens_v2_root_l1'
               AND event.resource_id IS NOT NULL
               AND (
                   event.logical_name_id = authority.logical_name_id
                   OR (
                       event.logical_name_id IS NULL
                       AND EXISTS (
                           SELECT 1 FROM project_events linked
                           WHERE linked.logical_name_id = authority.logical_name_id
                             AND linked.resource_id = event.resource_id
                             AND linked.source_family = 'ens_v2_root_l1'
                             AND linked.event_kind = 'ResolverChanged'
                       )
                   )
               )
             ORDER BY event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
             LIMIT 1
         ) pointer ON TRUE
         WHERE authority.unsupported_reason = 'current_authority_not_projected'
           AND authority.selected_binding_id IS NULL
           AND NULLIF(lower(pointer.after_state ->> 'resolver'), '') IS NOT NULL
           AND lower(pointer.after_state ->> 'resolver') <>
               '0x0000000000000000000000000000000000000000'
           AND NOT EXISTS (
               SELECT 1 FROM project_events lifecycle
               WHERE lifecycle.resource_id = pointer.resource_id
                 AND lifecycle.source_family = 'ens_v2_root_l1'
                 AND lifecycle.event_kind IN (
                     'RegistrationReserved', 'RegistrationReleased'
                 )
           )",
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
