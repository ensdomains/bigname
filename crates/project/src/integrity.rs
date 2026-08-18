use sqlx::{Postgres, Row, Transaction};

use crate::{Marker, ProjectError, Result};

pub const DUAL_CURRENT_EXACT_NAME_AUTHORITY: &str = "dual_current_exact_name_authority";

/// Structured evidence for a projection-blocking invariant failure.
///
/// The phase runner persists this after the generation transaction rolls back;
/// the assertion itself writes nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationFailureEvidence {
    pub failure_kind: String,
    pub failure_fingerprint: String,
    pub logical_name_id: String,
    pub target_block_number: i64,
    pub target_block_hash: String,
    pub payload: serde_json::Value,
}

/// Fail the generation when a Mainnet name still holds current bindings on both
/// authority arms after its proven activated boundary.
///
/// Bindings are evaluated at end-of-target-block, which is the transaction- and
/// block-level reconciliation tolerance: transients inside one migration
/// transaction or block are invisible here by construction.
pub(crate) async fn assert_publishable(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    let conflict = sqlx::query(
        r#"
        WITH target_time AS (
            SELECT block_timestamp + interval '1 second' AS cutoff,
                   canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
        ), conflict AS (
            SELECT authority.logical_name_id,
                   predecessor.surface_binding_id AS predecessor_binding_id,
                   predecessor.resource_id AS predecessor_resource_id,
                   predecessor.block_number AS predecessor_block_number,
                   COALESCE(
                       (predecessor.provenance ->> 'transaction_index')::bigint, -1
                   ) AS predecessor_transaction_index,
                   COALESCE(
                       (predecessor.provenance ->> 'log_index')::bigint, -1
                   ) AS predecessor_log_index,
                   predecessor.canonicality_state AS predecessor_canonicality_state,
                   successor.surface_binding_id AS successor_binding_id,
                   successor.resource_id AS successor_resource_id,
                   successor.block_number AS successor_block_number,
                   COALESCE(
                       (successor.provenance ->> 'transaction_index')::bigint, -1
                   ) AS successor_transaction_index,
                   COALESCE(
                       (successor.provenance ->> 'log_index')::bigint, -1
                   ) AS successor_log_index,
                   successor.canonicality_state AS successor_canonicality_state,
                   authority.authority_proof_event_identity AS boundary_event_identity,
                   boundary.block_number AS boundary_block_number,
                   boundary.block_hash AS boundary_block_hash,
                   COALESCE(boundary.transaction_index, -1) AS boundary_transaction_index,
                   COALESCE(boundary.log_index, -1) AS boundary_log_index,
                   boundary.canonicality_state AS boundary_canonicality_state,
                   target_time.canonicality_state AS target_canonicality_state
            FROM project_name_authority authority
            CROSS JOIN target_time
            JOIN project_events boundary
              ON boundary.normalized_event_id = authority.authority_proof_event_id
            JOIN LATERAL (
                SELECT candidate.*
                FROM project_binding_candidates candidate
                WHERE candidate.logical_name_id = authority.logical_name_id
                  AND candidate.authority_arm = 'ens_v1'
                  AND candidate.active_from < target_time.cutoff
                  AND (
                      candidate.active_to IS NULL
                      OR candidate.active_to >= target_time.cutoff
                  )
                ORDER BY candidate.block_number DESC, candidate.surface_binding_id DESC
                LIMIT 1
            ) predecessor ON TRUE
            JOIN LATERAL (
                SELECT candidate.*
                FROM project_binding_candidates candidate
                WHERE candidate.logical_name_id = authority.logical_name_id
                  AND candidate.authority_arm = 'ens_v2'
                  AND candidate.active_from < target_time.cutoff
                  AND (
                      candidate.active_to IS NULL
                      OR candidate.active_to >= target_time.cutoff
                  )
                ORDER BY candidate.block_number DESC, candidate.surface_binding_id DESC
                LIMIT 1
            ) successor ON TRUE
            WHERE authority.deployment_profile = 'mainnet'
              AND authority.authority_proof_kind = 'migration_authority_transition'
              AND authority.authority_proof_event_identity IS NOT NULL
              AND boundary.block_number <= $2
        )
        SELECT logical_name_id,
               encode(
                   sha256(
                       convert_to(
                           concat_ws(
                               '|', logical_name_id,
                               predecessor_binding_id::text, predecessor_resource_id::text,
                               predecessor_block_number::text,
                               predecessor_transaction_index::text,
                               predecessor_log_index::text,
                               successor_binding_id::text, successor_resource_id::text,
                               successor_block_number::text,
                               successor_transaction_index::text,
                               successor_log_index::text,
                               boundary_event_identity, boundary_block_number::text,
                               boundary_transaction_index::text, boundary_log_index::text
                           ),
                           'UTF8'
                       )
                   ),
                   'hex'
               ) AS failure_fingerprint,
               jsonb_build_object(
                   'predecessor', jsonb_build_object(
                       'authority_arm', 'ens_v1',
                       'surface_binding_id', predecessor_binding_id,
                       'resource_id', predecessor_resource_id,
                       'block_number', predecessor_block_number,
                       'transaction_index', predecessor_transaction_index,
                       'log_index', predecessor_log_index,
                       'canonicality_state', predecessor_canonicality_state
                   ),
                   'successor', jsonb_build_object(
                       'authority_arm', 'ens_v2',
                       'surface_binding_id', successor_binding_id,
                       'resource_id', successor_resource_id,
                       'block_number', successor_block_number,
                       'transaction_index', successor_transaction_index,
                       'log_index', successor_log_index,
                       'canonicality_state', successor_canonicality_state
                   ),
                   'boundary', jsonb_build_object(
                       'event_identity', boundary_event_identity,
                       'block_number', boundary_block_number,
                       'block_hash', boundary_block_hash,
                       'transaction_index', boundary_transaction_index,
                       'log_index', boundary_log_index,
                       'canonicality_state', boundary_canonicality_state
                   ),
                   'target', jsonb_build_object(
                       'block_number', $2::bigint,
                       'block_hash', $3::text,
                       'canonicality_state', target_canonicality_state
                   )
               ) AS payload
        FROM conflict
        ORDER BY logical_name_id
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to assert exact-name authority integrity", error)
    })?;

    let Some(row) = conflict else {
        return Ok(());
    };
    let logical_name_id: String = row
        .try_get("logical_name_id")
        .map_err(|error| ProjectError::database("failed to read integrity failure name", error))?;
    let failure_fingerprint: String = row.try_get("failure_fingerprint").map_err(|error| {
        ProjectError::database("failed to read integrity failure fingerprint", error)
    })?;
    let payload: serde_json::Value = row.try_get("payload").map_err(|error| {
        ProjectError::database("failed to read integrity failure evidence", error)
    })?;

    Err(ProjectError::generation_failure(
        format!(
            "chain {chain_id} name {logical_name_id} holds current bindings on both \
             authority arms after its activated migration boundary; \
             projection generation for block {} is not publishable",
            target.number
        ),
        GenerationFailureEvidence {
            failure_kind: DUAL_CURRENT_EXACT_NAME_AUTHORITY.into(),
            failure_fingerprint,
            logical_name_id,
            target_block_number: target.number,
            target_block_hash: target.hash.clone(),
            payload,
        },
    ))
}
