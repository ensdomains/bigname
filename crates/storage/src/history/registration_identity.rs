use sqlx::{Postgres, QueryBuilder};

pub(super) fn push_product_registration_id(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
        CASE
            WHEN ne.resource_id IS NULL THEN NULL::uuid
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
        "#,
    );
}
