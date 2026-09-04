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
