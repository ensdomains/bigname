//! Canonicality read filter for `address_records_current`: the projection target, name surface,
//! optional authority resource, record-serving resource, and optional binding must all sit on
//! canonical lineage. Shared by the single-coin and `coin_type=evm` reads.

pub(super) const ADDRESS_RECORDS_CURRENT_READ_FILTER: &str = r#"
              AND arc.canonicality_summary ->> 'state' = 'canonical_lineage'
              AND EXISTS (
                  SELECT 1
                  FROM bigname_phase.chain_lineage projection_lineage
                  WHERE projection_lineage.chain_id = arc.provenance ->> 'chain_id'
                    AND projection_lineage.block_hash = arc.chain_positions ->> 'target_block_hash'
                    AND projection_lineage.canonicality_state IN (
                        'canonical'::bigname_phase.canonicality_state,
                        'safe'::bigname_phase.canonicality_state,
                        'finalized'::bigname_phase.canonicality_state
                    )
              )
              AND surface.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND surface_lineage.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND (arc.resource_id IS NULL OR (resource.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND resource_lineage.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              ))
              AND record_resource.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND record_resource_lineage.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND (arc.surface_binding_id IS NULL OR (binding.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND binding_lineage.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              ))
"#;
