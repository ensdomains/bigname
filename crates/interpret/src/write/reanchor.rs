use bigname_adapters::schema_v2::seam::{
    NAME_IDENTITY_OBSERVED_KEY, PREIMAGE_OBSERVATION_EVENT_KIND, TOKEN_LINEAGE_ID_KEY,
};
use sqlx::{Postgres, Transaction};

use crate::Result;

pub(super) async fn stable_identities(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let token_lineage_join =
        format!("event.after_state ->> '{TOKEN_LINEAGE_ID_KEY}' = identity.token_lineage_id::text");
    for (table, identity_join) in [
        (
            "name_surfaces",
            "event.logical_name_id = identity.logical_name_id",
        ),
        ("resources", "event.resource_id = identity.resource_id"),
        ("token_lineages", token_lineage_join.as_str()),
    ] {
        let identity_column = match table {
            "name_surfaces" => "logical_name_id",
            "resources" => "resource_id",
            "token_lineages" => TOKEN_LINEAGE_ID_KEY,
            _ => unreachable!("fixed stable identity table"),
        };
        let candidate_filter = if table == "name_surfaces" {
            // Only an observation that establishes the surface can anchor it: one that re-states
            // its body, or one that proves the identity from its label-hash path. A surviving
            // reference of any other kind must not move the anchor or deactivated_at.
            format!(
                "AND (
                     event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
                     OR event.after_state @> jsonb_build_object('{NAME_IDENTITY_OBSERVED_KEY}', true)
                 )"
            )
        } else {
            String::new()
        };
        // A shadow surface was deactivated when its raw labels were first observed, which a
        // label-hash-path observation does not do.
        let evidence_timestamp = if table == "name_surfaces" {
            format!(
                ",
                       first_value(lineage.block_timestamp) OVER (
                           PARTITION BY identity.logical_name_id
                           ORDER BY event.event_kind <> '{PREIMAGE_OBSERVATION_EVENT_KIND}',
                                    event.block_number,
                                    event.transaction_index NULLS FIRST,
                                    event.log_index NULLS FIRST,
                                    event.normalized_event_id
                       ) AS evidence_block_timestamp"
            )
        } else {
            String::new()
        };
        let deactivation_assignment = if table == "name_surfaces" {
            ",
                deactivated_at = CASE
                    WHEN identity.visibility_state = 'shadow'
                        THEN candidate.evidence_block_timestamp
                    ELSE NULL
                END"
        } else {
            ""
        };
        let statement = format!(
            "
            WITH candidates AS (
                SELECT identity.{identity_column} AS identity_id,
                       event.block_hash,
                       event.block_number,
                       event.raw_fact_ref AS provenance,
                       lineage.canonicality_state,
                       row_number() OVER (
                           PARTITION BY identity.{identity_column}
                           ORDER BY event.block_number,
                                    event.transaction_index NULLS FIRST,
                                    event.log_index NULLS FIRST,
                                    event.normalized_event_id
                       ) AS candidate_rank{evidence_timestamp}
                FROM {table} identity
                JOIN normalized_events event
                  ON event.chain_id = identity.chain_id
                 AND {identity_join}
                JOIN chain_lineage lineage
                  ON lineage.chain_id = event.chain_id
                 AND lineage.block_hash = event.block_hash
                 AND lineage.block_number = event.block_number
                WHERE identity.chain_id = $1
                  AND identity.block_number BETWEEN $2 AND $3
                  AND identity.canonicality_state = 'orphaned'
                  AND event.block_number IS NOT NULL
                  AND event.block_number NOT BETWEEN $2 AND $3
                  {candidate_filter}
                  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            )
            UPDATE {table} identity
            SET block_hash = candidate.block_hash,
                block_number = candidate.block_number,
                provenance = candidate.provenance,
                canonicality_state = candidate.canonicality_state{deactivation_assignment},
                observed_at = now()
            FROM candidates candidate
            WHERE candidate.candidate_rank = 1
              AND identity.{identity_column} = candidate.identity_id
            "
        );
        sqlx::query(&statement)
            .bind(chain_id)
            .bind(from_block)
            .bind(to_block)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                crate::InterpretError::database(
                    format!("failed to reanchor {table} before redo"),
                    error,
                )
            })?;
    }
    repair_preimage_witnesses(transaction, chain_id, from_block, to_block).await
}

/// Before a redo deletes its range's events, release every preimage witness among them. Raw
/// evidence keeps its bytes until the redo either re-observes them or, at completion, finds no
/// surviving witness. This covers evidence attached to a surface anchored before the range.
pub(super) async fn release_preimage_witnesses(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(&format!(
        "
        UPDATE name_surfaces surface
        SET preimage_event_identity = NULL,
            observed_at = now()
        FROM normalized_events event
        WHERE event.chain_id = $1
          AND event.block_number BETWEEN $2 AND $3
          AND event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
          AND surface.chain_id = event.chain_id
          AND surface.logical_name_id = event.logical_name_id
          AND surface.preimage_event_identity = event.event_identity
        "
    ))
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database(
            "failed to release name-surface preimage witnesses before redo",
            error,
        )
    })?;
    Ok(())
}

/// At redo completion, point each surface whose witness was released, or whose preimage the
/// range observed, at its earliest surviving preimage observation. A canonical surface left
/// without one loses the raw evidence when a surviving label-hash-path observation still
/// establishes its identity; any other surface is left as it is.
async fn repair_preimage_witnesses(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let statement = format!(
        "
        WITH targets AS (
            SELECT surface.logical_name_id
            FROM name_surfaces surface
            WHERE surface.chain_id = $1
              AND surface.raw_name IS NOT NULL
              AND surface.preimage_event_identity IS NULL
            UNION
            SELECT event.logical_name_id
            FROM normalized_events event
            WHERE event.chain_id = $1
              AND event.block_number BETWEEN $2 AND $3
              AND event.logical_name_id IS NOT NULL
              AND event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        ),
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
                         witness.normalized_event_id
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
                WHEN surface.visibility_state = 'shadow'
                 AND repaired.block_timestamp > surface.deactivated_at
                    THEN repaired.block_timestamp
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
                  AND surface.preimage_event_identity IS DISTINCT FROM repaired.event_identity
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
        "
    );
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            crate::InterpretError::database(
                "failed to repair name-surface preimage witnesses after redo",
                error,
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod tests;
