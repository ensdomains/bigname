use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

mod stage;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_name_authority ON COMMIT DROP AS
        WITH target_time AS (
            SELECT block_timestamp + interval '1 second' AS cutoff
            FROM chain_lineage
            WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
        ), open_bindings AS (
            SELECT binding.*
            FROM project_binding_candidates binding
            CROSS JOIN target_time
            WHERE binding.active_from < target_time.cutoff
              AND (binding.active_to IS NULL OR binding.active_to >= target_time.cutoff)
        ), arm_summary AS (
            SELECT logical_name_id,
                   count(DISTINCT authority_arm) AS arm_count,
                   min(authority_arm) AS sole_arm,
                   bool_or(authority_arm = 'ens_v1') AS has_ens_v1,
                   bool_or(authority_arm = 'ens_v2') AS has_ens_v2
            FROM open_bindings
            GROUP BY logical_name_id
        ), event_arms AS (
            SELECT event.logical_name_id, event.normalized_event_id,
                   CASE
                       WHEN event.source_family LIKE 'ens_v1_%' THEN 'ens_v1'
                       WHEN event.source_family IN (
                           'ens_v2_root_l1', 'ens_v2_registry_l1',
                           'ens_v2_registrar_l1'
                       ) THEN 'ens_v2'
                       WHEN event.source_family LIKE 'basenames_%' THEN 'basenames'
                   END AS authority_arm
            FROM project_events event
            WHERE event.logical_name_id IS NOT NULL
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed',
                  'RegistrationReleased', 'RegistrationReserved',
                  'ExpiryChanged', 'AuthorityTransferred',
                  'TokenControlTransferred', 'AuthorityEpochChanged'
              )
        ), event_arm_summary AS (
            SELECT logical_name_id,
                   count(DISTINCT authority_arm) AS arm_count,
                   min(authority_arm) AS sole_arm,
                   bool_or(authority_arm = 'ens_v1') AS has_ens_v1,
                   bool_or(authority_arm = 'ens_v2') AS has_ens_v2
            FROM event_arms
            WHERE authority_arm IS NOT NULL
            GROUP BY logical_name_id
        ), latest_v2_binding AS (
            SELECT DISTINCT ON (binding.logical_name_id)
                   binding.logical_name_id, binding.resource_id
            FROM project_binding_candidates binding
            WHERE binding.authority_arm = 'ens_v2'
            ORDER BY binding.logical_name_id, binding.block_number DESC,
                     COALESCE(
                         (binding.provenance ->> 'transaction_index')::bigint, -1
                     ) DESC,
                     COALESCE(
                         (binding.provenance ->> 'log_index')::bigint, -1
                     ) DESC,
                     binding.surface_binding_id DESC
        ), latest_v2_lifecycle AS (
            SELECT DISTINCT ON (event.logical_name_id)
                   event.logical_name_id, event.resource_id, event.event_kind,
                   event.block_number, event.transaction_index, event.log_index
            FROM project_events event
            JOIN latest_v2_binding binding
              ON binding.logical_name_id = event.logical_name_id
             AND binding.resource_id = event.resource_id
            WHERE event.logical_name_id IS NOT NULL
              AND event.resource_id IS NOT NULL
              AND event.source_family IN (
                  'ens_v2_root_l1', 'ens_v2_registry_l1',
                  'ens_v2_registrar_l1'
              )
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed',
                  'RegistrationReleased', 'RegistrationReserved'
              )
            ORDER BY event.logical_name_id, event.block_number DESC,
                     event.transaction_index DESC, event.log_index DESC,
                     event.normalized_event_id DESC
        ), released_v2_authority AS (
            SELECT lifecycle.logical_name_id,
                   lifecycle.resource_id AS released_v2_resource_id
            FROM latest_v2_lifecycle lifecycle
            WHERE lifecycle.event_kind = 'RegistrationReleased'
              AND EXISTS (
                  SELECT 1 FROM project_binding_candidates binding
                  WHERE binding.logical_name_id = lifecycle.logical_name_id
                    AND binding.authority_arm = 'ens_v2'
                    AND binding.resource_id = lifecycle.resource_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM open_bindings binding
                  WHERE binding.logical_name_id = lifecycle.logical_name_id
                    AND binding.authority_arm = 'ens_v2'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_binding_candidates predecessor
                  WHERE predecessor.logical_name_id = lifecycle.logical_name_id
                    AND predecessor.authority_arm = 'ens_v1'
                    AND (
                        predecessor.block_number,
                        COALESCE(
                            (predecessor.provenance ->> 'transaction_index')::bigint, -1
                        ),
                        COALESCE(
                            (predecessor.provenance ->> 'log_index')::bigint, -1
                        )
                    ) <= (
                        lifecycle.block_number,
                        COALESCE(lifecycle.transaction_index, -1),
                        COALESCE(lifecycle.log_index, -1)
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_events predecessor
                  WHERE predecessor.logical_name_id = lifecycle.logical_name_id
                    AND predecessor.source_family LIKE 'ens_v1_%'
                    AND predecessor.event_kind IN (
                        'RegistrationGranted', 'RegistrationRenewed',
                        'RegistrationReleased', 'RegistrationReserved',
                        'ExpiryChanged', 'AuthorityTransferred',
                        'TokenControlTransferred', 'AuthorityEpochChanged'
                    )
                    AND (
                        predecessor.block_number,
                        COALESCE(predecessor.transaction_index, -1),
                        COALESCE(predecessor.log_index, -1)
                    ) <= (
                        lifecycle.block_number,
                        COALESCE(lifecycle.transaction_index, -1),
                        COALESCE(lifecycle.log_index, -1)
                    )
              )
        ), released_v2_regime AS (
            -- Regime entry keys on a qualifying release's presence in epoch
            -- history, never on it being the latest v2 lifecycle row: v1 facts
            -- at or before the release suppress entry, later ones are residue.
            SELECT release.logical_name_id
            FROM project_events release
            WHERE release.logical_name_id IS NOT NULL
              AND release.resource_id IS NOT NULL
              AND release.source_family IN (
                  'ens_v2_root_l1', 'ens_v2_registry_l1',
                  'ens_v2_registrar_l1'
              )
              AND release.event_kind = 'RegistrationReleased'
              AND EXISTS (
                  SELECT 1 FROM project_binding_candidates binding
                  WHERE binding.logical_name_id = release.logical_name_id
                    AND binding.authority_arm = 'ens_v2'
                    AND binding.resource_id = release.resource_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_binding_candidates predecessor
                  WHERE predecessor.logical_name_id = release.logical_name_id
                    AND predecessor.authority_arm = 'ens_v1'
                    AND (
                        predecessor.block_number,
                        COALESCE(
                            (predecessor.provenance ->> 'transaction_index')::bigint, -1
                        ),
                        COALESCE(
                            (predecessor.provenance ->> 'log_index')::bigint, -1
                        )
                    ) <= (
                        release.block_number,
                        COALESCE(release.transaction_index, -1),
                        COALESCE(release.log_index, -1)
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_events predecessor
                  WHERE predecessor.logical_name_id = release.logical_name_id
                    AND predecessor.source_family LIKE 'ens_v1_%'
                    AND predecessor.event_kind IN (
                        'RegistrationGranted', 'RegistrationRenewed',
                        'RegistrationReleased', 'RegistrationReserved',
                        'ExpiryChanged', 'AuthorityTransferred',
                        'TokenControlTransferred', 'AuthorityEpochChanged'
                    )
                    AND (
                        predecessor.block_number,
                        COALESCE(predecessor.transaction_index, -1),
                        COALESCE(predecessor.log_index, -1)
                    ) <= (
                        release.block_number,
                        COALESCE(release.transaction_index, -1),
                        COALESCE(release.log_index, -1)
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_events predecessor_grant
                  WHERE predecessor_grant.logical_name_id = release.logical_name_id
                    AND predecessor_grant.source_family IN (
                        'ens_v2_root_l1', 'ens_v2_registry_l1',
                        'ens_v2_registrar_l1'
                    )
                    AND predecessor_grant.event_kind = 'RegistrationGranted'
                    AND predecessor_grant.resource_id IS NOT NULL
                    AND predecessor_grant.resource_id <> release.resource_id
                    AND (
                        predecessor_grant.block_number,
                        COALESCE(predecessor_grant.transaction_index, -1),
                        COALESCE(predecessor_grant.log_index, -1)
                    ) <= (
                        release.block_number,
                        COALESCE(release.transaction_index, -1),
                        COALESCE(release.log_index, -1)
                    )
              )
              AND EXISTS (
                  SELECT 1 FROM project_events regrant
                  WHERE regrant.logical_name_id = release.logical_name_id
                    AND regrant.source_family IN (
                        'ens_v2_root_l1', 'ens_v2_registry_l1',
                        'ens_v2_registrar_l1'
                    )
                    AND regrant.event_kind = 'RegistrationGranted'
                    AND (
                        regrant.block_number,
                        COALESCE(regrant.transaction_index, -1),
                        COALESCE(regrant.log_index, -1)
                    ) > (
                        release.block_number,
                        COALESCE(release.transaction_index, -1),
                        COALESCE(release.log_index, -1)
                    )
              )
            GROUP BY release.logical_name_id
        ), transition_proof AS (
            SELECT DISTINCT ON (event.logical_name_id)
                   event.logical_name_id,
                   event.normalized_event_id AS proof_event_id,
                   event.event_identity AS proof_event_identity,
                   event.migration_correlation_ids[1] AS transition_id,
                   event.block_number AS epoch_block_number,
                   event.transaction_index AS epoch_transaction_index,
                   event.log_index AS epoch_log_index,
                   (event.after_state #>> '{successor_binding,binding_id}')::uuid
                       AS successor_binding_id,
                   (event.after_state #>> '{successor_binding,resource_id}')::uuid
                       AS successor_resource_id
            FROM project_events event
            WHERE event.event_kind = 'MigrationApplied'
            ORDER BY event.logical_name_id, event.block_number DESC,
                     event.transaction_index DESC, event.log_index DESC,
                     event.normalized_event_id DESC
        ), child_proof AS (
            SELECT DISTINCT ON (registration.logical_name_id)
                   registration.logical_name_id,
                   registration.normalized_event_id AS proof_event_id,
                   registration.event_identity AS proof_event_identity,
                   registration.block_number AS epoch_block_number,
                   registration.transaction_index AS epoch_transaction_index,
                   registration.log_index AS epoch_log_index,
                   registration.resource_id AS successor_resource_id
            FROM project_events registration
            JOIN project_surfaces child
              ON child.logical_name_id = registration.logical_name_id
            JOIN migration_discovery_associations migration_registry
              ON migration_registry.chain_id = registration.chain_id
             AND migration_registry.registry_contract_instance_id::text =
                 registration.after_state ->> 'registry_contract_instance_id'
             AND migration_registry.correlation_kind = 'migration_registry_creation'
            JOIN discovery_edges registry_edge
              ON registry_edge.chain_id = migration_registry.chain_id
             AND registry_edge.edge_kind = 'registry_announcement'
             AND registry_edge.to_contract_instance_id =
                 migration_registry.registry_contract_instance_id
             AND registry_edge.source_manifest_id = migration_registry.source_manifest_id
             AND registry_edge.active_from_block_number = migration_registry.block_number
             AND registry_edge.active_from_block_hash = migration_registry.block_hash
             AND (registry_edge.provenance ->> 'transaction_index')::bigint =
                 migration_registry.transaction_index
             AND (registry_edge.provenance ->> 'log_index')::bigint =
                 migration_registry.log_index
            JOIN contract_instance_addresses registry_address
              ON registry_address.chain_id = migration_registry.chain_id
             AND registry_address.contract_instance_id =
                 migration_registry.registry_contract_instance_id
             AND lower(registry_address.address) =
                 lower(migration_registry.registry_address)
            JOIN chain_lineage migration_registry_lineage
              ON migration_registry_lineage.chain_id = migration_registry.chain_id
             AND migration_registry_lineage.block_hash = migration_registry.block_hash
             AND migration_registry_lineage.block_number = migration_registry.block_number
            WHERE registration.event_kind = 'RegistrationGranted'
              AND registration.source_family = 'ens_v2_registry_l1'
              AND registration.resource_id IS NOT NULL
              AND registration.after_state ->> 'status' = 'registered'
              AND EXISTS (
                  SELECT 1
                  FROM project_events parent_boundary
                  JOIN name_surfaces parent
                    ON parent.logical_name_id = parent_boundary.logical_name_id
                   AND parent.chain_id = parent_boundary.chain_id
                  JOIN project_events parent_registry
                    ON parent_registry.chain_id = parent_boundary.chain_id
                   AND parent_registry.logical_name_id = parent_boundary.logical_name_id
                   AND parent_registry.event_kind = 'SubregistryChanged'
                   AND parent_registry.source_family IN (
                       'ens_v2_root_l1', 'ens_v2_registry_l1'
                   )
                  JOIN chain_lineage parent_surface_lineage
                    ON parent_surface_lineage.chain_id = parent.chain_id
                   AND parent_surface_lineage.block_hash = parent.block_hash
                   AND parent_surface_lineage.block_number = parent.block_number
                  JOIN chain_lineage parent_lineage
                    ON parent_lineage.chain_id = parent_boundary.chain_id
                   AND parent_lineage.block_hash = parent_boundary.block_hash
                   AND parent_lineage.block_number = parent_boundary.block_number
                  JOIN chain_lineage parent_registry_lineage
                    ON parent_registry_lineage.chain_id = parent_registry.chain_id
                   AND parent_registry_lineage.block_hash = parent_registry.block_hash
                   AND parent_registry_lineage.block_number = parent_registry.block_number
                  WHERE parent_boundary.chain_id = $1
                    AND parent_boundary.block_number <= $2
                    AND parent_boundary.event_kind = 'MigrationApplied'
                    AND parent_boundary.consumer_visibility = 'activated'
                    AND parent_boundary.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND parent.block_number <= $2
                    AND parent.canonicality_state IN ('canonical', 'safe', 'finalized')
                    AND parent_lineage.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND parent_surface_lineage.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND migration_registry.block_number <= registration.block_number
                    AND migration_registry.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND migration_registry_lineage.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND registry_edge.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND registry_edge.active_from_block_number <=
                        registration.block_number
                    AND (
                        registry_edge.active_to_block_number IS NULL
                        OR registry_edge.active_to_block_number >=
                           registration.block_number
                    )
                    AND COALESCE(registry_address.active_from_block_number, 0) <=
                        registration.block_number
                    AND (
                        registry_address.active_to_block_number IS NULL
                        OR registry_address.active_to_block_number >=
                           registration.block_number
                    )
                    AND lower(parent_registry.after_state ->> 'subregistry') =
                        lower(migration_registry.registry_address)
                    AND parent_registry.block_number <= registration.block_number
                    AND parent_registry.consumer_visibility = 'activated'
                    AND parent_registry.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND parent_registry_lineage.canonicality_state IN (
                        'canonical', 'safe', 'finalized'
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM project_events later_registry
                        WHERE later_registry.chain_id = parent_registry.chain_id
                          AND later_registry.logical_name_id =
                              parent_registry.logical_name_id
                          AND later_registry.event_kind = 'SubregistryChanged'
                          AND later_registry.source_family IN (
                              'ens_v2_root_l1', 'ens_v2_registry_l1'
                          )
                          AND (
                              later_registry.block_number,
                              COALESCE(later_registry.transaction_index, -1),
                              COALESCE(later_registry.log_index, -1),
                              later_registry.normalized_event_id
                          ) > (
                              parent_registry.block_number,
                              COALESCE(parent_registry.transaction_index, -1),
                              COALESCE(parent_registry.log_index, -1),
                              parent_registry.normalized_event_id
                          )
                          AND (
                              later_registry.block_number,
                              COALESCE(later_registry.transaction_index, -1),
                              COALESCE(later_registry.log_index, -1)
                          ) <= (
                              registration.block_number,
                              COALESCE(registration.transaction_index, -1),
                              COALESCE(registration.log_index, -1)
                          )
                    )
                    AND cardinality(child.labelhashes) =
                        cardinality(parent.labelhashes) + 1
                    AND child.labelhashes[2:cardinality(child.labelhashes)] =
                        parent.labelhashes
                    AND (
                        migration_registry.block_number,
                        migration_registry.transaction_index,
                        migration_registry.log_index
                    ) <= (
                        parent_registry.block_number,
                        COALESCE(parent_registry.transaction_index, -1),
                        COALESCE(parent_registry.log_index, -1)
                    )
                    AND (
                        parent_boundary.block_number,
                        COALESCE(parent_boundary.transaction_index, -1),
                        COALESCE(parent_boundary.log_index, -1)
                    ) <= (
                        parent_registry.block_number,
                        COALESCE(parent_registry.transaction_index, -1),
                        COALESCE(parent_registry.log_index, -1)
                    )
                    AND (
                        parent_registry.block_number,
                        COALESCE(parent_registry.transaction_index, -1),
                        COALESCE(parent_registry.log_index, -1)
                    ) <= (
                        registration.block_number,
                        COALESCE(registration.transaction_index, -1),
                        COALESCE(registration.log_index, -1)
                    )
              )
            ORDER BY registration.logical_name_id, registration.block_number,
                     registration.transaction_index, registration.log_index,
                     registration.normalized_event_id
        ), proof AS (
            SELECT logical_name_id, 'migration_authority_transition'::text AS proof_kind,
                   proof_event_id, proof_event_identity, transition_id,
                   epoch_block_number, epoch_transaction_index, epoch_log_index,
                   successor_binding_id, successor_resource_id
            FROM transition_proof
            UNION ALL
            SELECT child.logical_name_id,
                   'positive_v2_child_registration'::text,
                   child.proof_event_id, child.proof_event_identity, NULL::text,
                   child.epoch_block_number, child.epoch_transaction_index,
                   child.epoch_log_index, NULL::uuid, child.successor_resource_id
            FROM child_proof child
            WHERE NOT EXISTS (
                SELECT 1 FROM transition_proof transition
                WHERE transition.logical_name_id = child.logical_name_id
            )
        ), decision AS (
            SELECT surface.logical_name_id,
                   CASE
                       WHEN proof.logical_name_id IS NOT NULL THEN 'ens_v2'
                       WHEN released.logical_name_id IS NOT NULL THEN 'ens_v2'
                       WHEN regime.logical_name_id IS NOT NULL THEN 'ens_v2'
                       WHEN (
                           COALESCE(summary.has_ens_v1, false)
                           OR COALESCE(event_summary.has_ens_v1, false)
                       ) AND (
                           COALESCE(summary.has_ens_v2, false)
                           OR COALESCE(event_summary.has_ens_v2, false)
                       ) THEN NULL
                       WHEN summary.arm_count = 1 THEN summary.sole_arm
                       WHEN summary.logical_name_id IS NULL
                        AND event_summary.arm_count = 1 THEN event_summary.sole_arm
                   END AS selected_authority_arm,
                   proof.proof_kind, proof.proof_event_id,
                   proof.proof_event_identity, proof.transition_id,
                   proof.epoch_block_number, proof.epoch_transaction_index,
                   proof.epoch_log_index, proof.successor_binding_id,
                   proof.successor_resource_id,
                   released.released_v2_resource_id,
                   COALESCE(summary.has_ens_v1, false)
                       OR COALESCE(event_summary.has_ens_v1, false) AS has_ens_v1,
                   COALESCE(summary.has_ens_v2, false)
                       OR COALESCE(event_summary.has_ens_v2, false) AS has_ens_v2,
                   COALESCE(
                       proof.logical_name_id IS NULL
                           AND summary.logical_name_id IS NULL
                           AND event_summary.arm_count = 1,
                       false
                   ) AS bindingless_event_authority,
                   CASE
                       WHEN $1 = 'ethereum-sepolia' OR EXISTS (
                           SELECT 1 FROM project_manifests manifest
                           WHERE manifest.namespace = 'ens'
                             AND manifest.deployment_label =
                                 'ens_v2_sepolia_post_audit'
                       ) THEN 'sepolia'
                       ELSE 'mainnet'
                   END AS deployment_profile
            FROM project_surfaces surface
            LEFT JOIN arm_summary summary USING (logical_name_id)
            LEFT JOIN event_arm_summary event_summary USING (logical_name_id)
            LEFT JOIN proof USING (logical_name_id)
            LEFT JOIN released_v2_authority released USING (logical_name_id)
            LEFT JOIN released_v2_regime regime USING (logical_name_id)
        ), selected AS (
            SELECT decision.*, binding.surface_binding_id AS selected_binding_id,
                   binding.resource_id AS selected_resource_id,
                   binding.binding_kind AS selected_binding_kind,
                   COALESCE(
                       decision.epoch_block_number, binding.block_number
                   ) AS selected_epoch_block_number,
                   COALESCE(
                       decision.epoch_transaction_index,
                       (binding.provenance ->> 'transaction_index')::bigint
                   ) AS selected_epoch_transaction_index,
                   COALESCE(
                       decision.epoch_log_index,
                       (binding.provenance ->> 'log_index')::bigint
                   ) AS selected_epoch_log_index
            FROM decision
            LEFT JOIN LATERAL (
                SELECT candidate.*
                FROM project_binding_candidates candidate
                CROSS JOIN target_time
                WHERE candidate.logical_name_id = decision.logical_name_id
                  AND candidate.authority_arm = decision.selected_authority_arm
                  AND EXISTS (
                      SELECT 1 FROM project_resources resource
                      WHERE resource.resource_id = candidate.resource_id
                  )
                  AND (
                      decision.released_v2_resource_id IS NULL
                      OR candidate.resource_id = decision.released_v2_resource_id
                  )
                  AND (
                      decision.proof_event_id IS NOT NULL
                      OR decision.released_v2_resource_id IS NOT NULL
                      OR (
                          candidate.active_from < target_time.cutoff
                          AND (
                              candidate.active_to IS NULL
                              OR candidate.active_to >= target_time.cutoff
                          )
                      )
                  )
                  AND (
                      decision.epoch_block_number IS NULL
                      OR candidate.surface_binding_id = decision.successor_binding_id
                      OR (
                          decision.successor_binding_id IS NULL
                          AND candidate.resource_id = decision.successor_resource_id
                          AND (
                              candidate.block_number,
                              COALESCE(
                                  (candidate.provenance ->> 'transaction_index')::bigint, -1
                              ),
                              COALESCE(
                                  (candidate.provenance ->> 'log_index')::bigint, -1
                              )
                          ) = (
                              decision.epoch_block_number,
                              COALESCE(decision.epoch_transaction_index, -1),
                              COALESCE(decision.epoch_log_index, -1)
                          )
                      )
                      OR (
                          candidate.block_number,
                          COALESCE(
                              (candidate.provenance ->> 'transaction_index')::bigint, -1
                          ),
                          COALESCE(
                              (candidate.provenance ->> 'log_index')::bigint, -1
                          )
                      ) > (
                          decision.epoch_block_number,
                          COALESCE(decision.epoch_transaction_index, -1),
                          COALESCE(decision.epoch_log_index, -1)
                      )
                  )
                ORDER BY candidate.block_number DESC,
                         COALESCE(
                             (candidate.provenance ->> 'transaction_index')::bigint, -1
                         ) DESC,
                         COALESCE(
                             (candidate.provenance ->> 'log_index')::bigint, -1
                         ) DESC,
                         candidate.surface_binding_id DESC
                LIMIT 1
            ) binding ON TRUE
        )
        SELECT selected.logical_name_id, selected.selected_authority_arm,
               selected.selected_resource_id, selected.selected_binding_id,
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', selected.selected_epoch_block_number,
                   'transaction_index', selected.selected_epoch_transaction_index,
                   'log_index', selected.selected_epoch_log_index
               )) AS authority_epoch_start_position,
               selected.proof_kind AS authority_proof_kind,
               selected.proof_event_id AS authority_proof_event_id,
               selected.proof_event_identity AS authority_proof_event_identity,
               selected.transition_id AS authority_transition_id,
               CASE lifecycle.event_kind
                   WHEN 'RegistrationReleased' THEN 'unregistered'
                   WHEN 'RegistrationReserved' THEN 'reserved'
                   WHEN 'RegistrationGranted' THEN 'registered'
                   WHEN 'RegistrationRenewed' THEN 'registered'
                   ELSE CASE
                       WHEN selected.selected_binding_id IS NULL THEN 'unregistered'
                       ELSE 'registered'
                   END
               END AS lifecycle_state,
               CASE
                   WHEN selected.selected_authority_arm IS NULL
                    AND selected.has_ens_v1 AND selected.has_ens_v2
                    AND selected.deployment_profile = 'sepolia'
                       THEN 'independent_ens_deployments_overlap'
                   WHEN selected.selected_authority_arm IS NULL
                    AND selected.has_ens_v1 AND selected.has_ens_v2
                       THEN 'conflicting_current_ens_authority'
                   WHEN selected.selected_binding_id IS NULL
                       THEN 'current_authority_not_projected'
               END AS unsupported_reason,
               selected.deployment_profile,
               jsonb_strip_nulls(jsonb_build_object(
                   'authority_arm', selected.selected_authority_arm,
                   'binding_kind', selected.selected_binding_kind,
                   'resource_id', selected.selected_resource_id,
                   'surface_binding_id', selected.selected_binding_id
               )) AS resource_authority_context
        FROM selected
        LEFT JOIN LATERAL (
            SELECT event.event_kind
            FROM project_events event
            WHERE event.logical_name_id = selected.logical_name_id
              AND (
                  event.resource_id = selected.selected_resource_id
                  OR (
                      selected.bindingless_event_authority
                      AND CASE
                          WHEN event.source_family LIKE 'ens_v1_%' THEN 'ens_v1'
                          WHEN event.source_family LIKE 'ens_v2_%' THEN 'ens_v2'
                          WHEN event.source_family LIKE 'basenames_%' THEN 'basenames'
                      END = selected.selected_authority_arm
                  )
              )
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed',
                  'RegistrationReleased', 'RegistrationReserved'
              )
              AND (
                  selected.selected_epoch_block_number IS NULL
                  OR (
                      event.block_number,
                      COALESCE(event.transaction_index, -1),
                      COALESCE(event.log_index, -1)
                  ) >= (
                      selected.selected_epoch_block_number,
                      COALESCE(selected.selected_epoch_transaction_index, -1),
                      COALESCE(selected.selected_epoch_log_index, -1)
                  )
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) lifecycle ON TRUE
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to select name authority", error))?;

    stage::build(transaction).await
}
