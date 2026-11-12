//! The two statements as they read before the lost-witness sets: each read every surface of the
//! chain. Kept verbatim, as the oracle the bounded statements must match.
use bigname_adapters::schema_v2::seam::{
    NAME_IDENTITY_OBSERVED_KEY, PREIMAGE_OBSERVATION_EVENT_KIND,
};

pub(super) fn redo_names() -> String {
    format!(
        "/* interpret:identity_search.redo_names */ SELECT logical_name_id FROM name_surfaces surface
         WHERE surface.chain_id=$1 AND (
             surface.block_number BETWEEN $2 AND $3
             OR (surface.raw_name IS NOT NULL AND (
                 surface.preimage_event_identity IS NULL OR NOT EXISTS (
                     SELECT 1 FROM normalized_events witness JOIN chain_lineage lineage
                       ON lineage.chain_id=witness.chain_id AND lineage.block_hash=witness.block_hash
                      AND lineage.block_number=witness.block_number
                     WHERE witness.chain_id=surface.chain_id
                       AND witness.event_identity=surface.preimage_event_identity
                       AND witness.canonicality_state IN ('canonical','safe','finalized')
                       AND lineage.canonicality_state IN ('canonical','safe','finalized'))))
             OR EXISTS (SELECT 1 FROM normalized_events event
                WHERE event.chain_id=$1 AND event.block_number BETWEEN $2 AND $3
                  AND event.logical_name_id=surface.logical_name_id
                  AND event.event_kind='{PREIMAGE_OBSERVATION_EVENT_KIND}'))
         ORDER BY logical_name_id",
    )
}

pub(super) fn repair_targets() -> String {
    format!(
        "
            SELECT surface.logical_name_id
            FROM name_surfaces surface
            WHERE surface.chain_id = $1
              AND surface.raw_name IS NOT NULL
              AND (
                  surface.preimage_event_identity IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM normalized_events witness
                      JOIN chain_lineage lineage
                        ON lineage.chain_id = witness.chain_id
                       AND lineage.block_hash = witness.block_hash
                       AND lineage.block_number = witness.block_number
                      WHERE witness.chain_id = surface.chain_id
                        AND witness.event_identity = surface.preimage_event_identity
                        AND witness.canonicality_state IN ('canonical', 'safe', 'finalized')
                        AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  )
              )
            UNION
            SELECT event.logical_name_id
            FROM normalized_events event
            WHERE event.chain_id = $1
              AND event.block_number BETWEEN $2 AND $3
              AND event.logical_name_id IS NOT NULL
              AND event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        "
    )
}

pub(super) fn repair() -> String {
    format!(
        "
        WITH targets AS ({targets}),
        repaired AS (
            SELECT target.logical_name_id, earliest.event_identity, earliest.block_timestamp
            FROM targets target
            LEFT JOIN LATERAL (
                SELECT witness.event_identity, lineage.block_timestamp
                FROM normalized_events witness
                JOIN chain_lineage lineage
                  ON lineage.chain_id = witness.chain_id
                 AND lineage.block_hash = witness.block_hash
                 AND lineage.block_number = witness.block_number
                WHERE witness.chain_id = $1
                  AND witness.logical_name_id = target.logical_name_id
                  AND witness.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
                  AND witness.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                ORDER BY witness.block_number,
                         witness.transaction_index NULLS FIRST,
                         witness.log_index NULLS FIRST,
                         witness.event_identity
                LIMIT 1
            ) earliest ON true
        )
        UPDATE name_surfaces surface
        SET preimage_event_identity = repaired.event_identity,
            raw_name = CASE WHEN repaired.event_identity IS NULL THEN NULL ELSE surface.raw_name END,
            raw_labels = CASE
                WHEN repaired.event_identity IS NULL THEN NULL
                ELSE surface.raw_labels
            END,
            dns_encoded_name = CASE
                WHEN repaired.event_identity IS NULL THEN NULL
                ELSE surface.dns_encoded_name
            END,
            visibility_state = CASE
                WHEN repaired.event_identity IS NULL THEN 'active'
                ELSE surface.visibility_state
            END,
            normalization_errors = CASE
                WHEN repaired.event_identity IS NULL THEN '[]'::jsonb
                ELSE surface.normalization_errors
            END,
            deactivation_reason = CASE
                WHEN repaired.event_identity IS NULL THEN NULL
                ELSE surface.deactivation_reason
            END,
            deactivated_at = CASE
                WHEN repaired.event_identity IS NULL THEN NULL
                WHEN surface.visibility_state = 'shadow' THEN repaired.block_timestamp
                ELSE surface.deactivated_at
            END,
            observed_at = now()
        FROM repaired
        WHERE surface.chain_id = $1
          AND surface.logical_name_id = repaired.logical_name_id
          AND surface.raw_name IS NOT NULL
          AND (
              (
                  repaired.event_identity IS NOT NULL
                  AND (
                      surface.preimage_event_identity IS DISTINCT FROM repaired.event_identity
                      OR (surface.visibility_state = 'shadow'
                          AND surface.deactivated_at IS DISTINCT FROM repaired.block_timestamp)
                  )
              )
              OR (
                  repaired.event_identity IS NULL
                  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND EXISTS (
                      SELECT 1
                      FROM normalized_events observation
                      JOIN chain_lineage lineage
                        ON lineage.chain_id = observation.chain_id
                       AND lineage.block_hash = observation.block_hash
                       AND lineage.block_number = observation.block_number
                      WHERE observation.chain_id = surface.chain_id
                        AND observation.logical_name_id = surface.logical_name_id
                        AND observation.after_state
                            @> jsonb_build_object('{NAME_IDENTITY_OBSERVED_KEY}', true)
                        AND observation.canonicality_state IN ('canonical', 'safe', 'finalized')
                        AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  )
              )
          )
        ",
        targets = repair_targets(),
    )
}
