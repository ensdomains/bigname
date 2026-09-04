use sqlx::{Postgres, QueryBuilder};

pub(super) fn push_product_event_kind_predicate(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        "ne.event_kind IN (
            'RegistrationGranted', 'LabelRegistered', 'RegistrationRenewed',
            'RegistrationReleased', 'ExpiryChanged', 'TokenControlTransferred',
            'AuthorityTransferred', 'AuthorityEpochChanged', 'ResolverChanged',
            'RecordChanged', 'RecordVersionChanged', 'ReverseChanged',
            'PermissionChanged', 'PermissionScopeChanged', 'RolesChanged',
            'EACRolesChanged'
        )",
    );
}

pub(super) fn push_product_registration_id(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
        CASE
            WHEN ne.resource_id IS NULL THEN NULL::uuid
            ELSE COALESCE(
                (
                    SELECT born_wrapper.resource_id
                    FROM bigname_phase.normalized_events registrar_grant
                    JOIN bigname_phase.normalized_events born_wrapper
                      ON born_wrapper.chain_id = registrar_grant.chain_id
                     AND born_wrapper.logical_name_id = COALESCE(
                            ne.logical_name_id,
                            (
                                SELECT surface.logical_name_id
                                FROM bigname_phase.name_surfaces surface
                                LEFT JOIN bigname_phase.chain_lineage surface_lineage
                                  ON surface_lineage.chain_id = surface.chain_id
                                 AND surface_lineage.block_hash = surface.block_hash
                                WHERE surface.chain_id = ne.chain_id
                                  AND surface.namespace = ne.namespace
                                  AND surface.namehash = COALESCE(
                                      ne.after_state ->> 'namehash',
                                      ne.after_state ->> 'child_node',
                                      ne.after_state ->> 'node'
                                  )
                                  AND surface.canonicality_state IN (
                                      'canonical', 'safe', 'finalized'
                                  )
                                  AND (
                                      surface.block_hash IS NULL
                                      OR surface_lineage.canonicality_state IN (
                                          'canonical', 'safe', 'finalized'
                                      )
                                  )
                                ORDER BY surface.logical_name_id
                                LIMIT 1
                            )
                         )
                     AND born_wrapper.transaction_hash = registrar_grant.transaction_hash
                     AND (
                         born_wrapper.after_state ->>
                             'wrapped_registrar_resource_id'
                     )::uuid = registrar_grant.resource_id
                    LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
                      ON wrapper_lineage.chain_id = born_wrapper.chain_id
                     AND wrapper_lineage.block_hash = born_wrapper.block_hash
                    LEFT JOIN bigname_phase.chain_lineage grant_lineage
                      ON grant_lineage.chain_id = registrar_grant.chain_id
                     AND grant_lineage.block_hash = registrar_grant.block_hash
                    WHERE registrar_grant.resource_id = COALESCE(
                              (
                                  SELECT (
                                      current_wrapper.after_state ->>
                                          'wrapped_registrar_resource_id'
                                  )::uuid
                                  FROM bigname_phase.normalized_events current_wrapper
                                  LEFT JOIN bigname_phase.chain_lineage current_lineage
                                    ON current_lineage.chain_id = current_wrapper.chain_id
                                   AND current_lineage.block_hash = current_wrapper.block_hash
                                  WHERE current_wrapper.resource_id = ne.resource_id
                                    AND current_wrapper.event_kind = 'SurfaceBound'
                                    AND current_wrapper.source_family = 'ens_v1_wrapper_l1'
                                    AND current_wrapper.consumer_visibility = 'activated'
                                    AND current_wrapper.after_state ->>
                                          'wrapped_registrar_resource_id' IS NOT NULL
                                    AND current_wrapper.canonicality_state IN (
                                        'canonical', 'safe', 'finalized'
                                    )
                                    AND (
                                        current_wrapper.block_hash IS NULL
                                        OR current_lineage.canonicality_state IN (
                                            'canonical', 'safe', 'finalized'
                                        )
                                    )
                                  ORDER BY current_wrapper.normalized_event_id DESC
                                  LIMIT 1
                              ),
                              ne.resource_id
                          )
                      AND registrar_grant.event_kind = 'RegistrationGranted'
                      AND registrar_grant.source_family = 'ens_v1_registrar_l1'
                      AND registrar_grant.consumer_visibility = 'activated'
                      AND registrar_grant.canonicality_state IN (
                          'canonical', 'safe', 'finalized'
                      )
                      AND born_wrapper.event_kind = 'SurfaceBound'
                      AND born_wrapper.source_family = 'ens_v1_wrapper_l1'
                      AND born_wrapper.consumer_visibility = 'activated'
                      AND born_wrapper.canonicality_state IN (
                          'canonical', 'safe', 'finalized'
                      )
                      AND (
                          born_wrapper.block_hash IS NULL
                          OR wrapper_lineage.canonicality_state IN (
                              'canonical', 'safe', 'finalized'
                          )
                      )
                      AND (
                          registrar_grant.block_hash IS NULL
                          OR grant_lineage.canonicality_state IN (
                              'canonical', 'safe', 'finalized'
                          )
                      )
                    ORDER BY born_wrapper.normalized_event_id
                    LIMIT 1
                ),
                (
                    SELECT
                        (wrapper_binding.after_state ->>
                            'wrapped_registrar_resource_id')::uuid
                    FROM bigname_phase.normalized_events wrapper_binding
                    LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
                      ON wrapper_lineage.chain_id = wrapper_binding.chain_id
                     AND wrapper_lineage.block_hash = wrapper_binding.block_hash
                    WHERE wrapper_binding.resource_id = ne.resource_id
                      AND wrapper_binding.logical_name_id = ne.logical_name_id
                      AND wrapper_binding.event_kind = 'SurfaceBound'
                      AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
                      AND wrapper_binding.consumer_visibility = 'activated'
                      AND wrapper_binding.after_state ->>
                            'wrapped_registrar_resource_id' IS NOT NULL
                      AND wrapper_binding.canonicality_state IN (
                          'canonical'::bigname_phase.canonicality_state,
                          'safe'::bigname_phase.canonicality_state,
                          'finalized'::bigname_phase.canonicality_state
                      )
                      AND (
                          wrapper_binding.block_hash IS NULL
                          OR wrapper_lineage.canonicality_state IN (
                              'canonical'::bigname_phase.canonicality_state,
                              'safe'::bigname_phase.canonicality_state,
                              'finalized'::bigname_phase.canonicality_state
                          )
                      )
                    ORDER BY wrapper_binding.normalized_event_id DESC
                    LIMIT 1
                ),
                CASE
                    WHEN ne.source_family IN (
                        'ens_v1_registry_l1', 'basenames_base_registry'
                    ) AND (
                        (
                            ne.event_kind IN (
                                'AuthorityTransferred', 'AuthorityEpochChanged'
                            )
                            AND lower(COALESCE(ne.after_state ->> 'owner_getter', '')) =
                                '0x0000000000000000000000000000000000000000'
                        )
                        OR (
                            ne.event_kind = 'ResolverChanged'
                            AND NOT EXISTS (
                                SELECT 1
                                FROM bigname_phase.resources event_resource
                                WHERE event_resource.resource_id = ne.resource_id
                                  AND event_resource.token_lineage_id IS NOT NULL
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                FROM bigname_phase.surface_bindings binding
                                JOIN bigname_phase.chain_lineage binding_lineage
                                  ON binding_lineage.chain_id = binding.chain_id
                                 AND binding_lineage.block_hash = binding.block_hash
                                 AND binding_lineage.block_number = binding.block_number
                                WHERE binding.resource_id = ne.resource_id
                                  AND binding.chain_id = ne.chain_id
                                  AND binding.active_from <= rb.block_timestamp
                                      + GREATEST(COALESCE(ne.log_index, 0), 0)
                                        * interval '1 microsecond'
                                  AND (
                                      binding.active_to IS NULL
                                      OR binding.active_to > rb.block_timestamp
                                          + GREATEST(COALESCE(ne.log_index, 0), 0)
                                            * interval '1 microsecond'
                                  )
                                  AND binding.canonicality_state IN (
                                      'canonical'::bigname_phase.canonicality_state,
                                      'safe'::bigname_phase.canonicality_state,
                                      'finalized'::bigname_phase.canonicality_state
                                  )
                                  AND binding_lineage.canonicality_state IN (
                                      'canonical'::bigname_phase.canonicality_state,
                                      'safe'::bigname_phase.canonicality_state,
                                      'finalized'::bigname_phase.canonicality_state
                                  )
                            )
                        )
                    ) THEN NULL::uuid
                    ELSE ne.resource_id
                END
            )
        END
        "#,
    );
}
