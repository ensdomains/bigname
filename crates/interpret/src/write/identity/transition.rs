use bigname_adapters::schema_v2::{BatchOutput, MigrationAuthorityTransition, seam};
use sqlx::{Postgres, Transaction, types::Uuid};

use crate::{InterpretError, Result};

mod registrar;
mod selector;
use selector::{PredecessorCleanup, PredecessorSelector, REGISTRAR_ANCHOR_KIND, validate};

pub(super) fn validate_boundaries(output: &BatchOutput) -> Result<()> {
    for transition in &output.migration_authority_transitions {
        let matching = output
            .normalized_events
            .iter()
            .filter(|event| exact_boundary(transition, event))
            .count();
        if matching != 1 {
            return Err(InterpretError::data_integrity(format!(
                "migration authority transition {} has {matching} exact activated MigrationApplied boundaries; expected one",
                transition.boundary_event_identity
            )));
        }
    }
    for event in output.normalized_events.iter().filter(|event| {
        event.event_kind == seam::MIGRATION_APPLIED_EVENT_KIND
            && event.consumer_visibility == "activated"
            && matches!(
                event.canonicality_state.as_str(),
                "canonical" | "safe" | "finalized"
            )
    }) {
        let matching = output
            .migration_authority_transitions
            .iter()
            .filter(|transition| exact_boundary(transition, event))
            .count();
        if matching != 1 {
            return Err(InterpretError::data_integrity(format!(
                "activated MigrationApplied boundary {} has {matching} exact authority transitions; expected one",
                event.event_identity,
            )));
        }
    }
    Ok(())
}

fn exact_boundary(
    transition: &MigrationAuthorityTransition,
    event: &bigname_adapters::schema_v2::NormalizedEvent,
) -> bool {
    event.event_identity == transition.boundary_event_identity
        && event.event_kind == seam::MIGRATION_APPLIED_EVENT_KIND
        && event.consumer_visibility == "activated"
        && event.migration_correlation_ids == [transition.migration_correlation_id.clone()]
        && event.logical_name_id.as_deref() == Some(transition.logical_name_id.as_str())
        && event.chain_id == transition.chain_id
        && event.block_number == Some(transition.block_number)
        && event.transaction_index == Some(transition.transaction_index)
        && event.log_index == Some(transition.log_index)
        && matches!(
            event.canonicality_state.as_str(),
            "canonical" | "safe" | "finalized"
        )
        && event.after_state["predecessor_binding"] == transition.predecessor_selector
        && event.after_state["successor_binding"]["binding_id"]
            == transition.successor_surface_binding_id.to_string()
        && event.after_state["successor_binding"]["resource_id"]
            == transition.successor_resource_id.to_string()
        && event.after_state["successor_binding"]["authority_epoch"] == transition.successor_arm
}

/// Applies each activated boundary: locks its ENSv2 successor, resolves the instant its ENSv1
/// predecessor ended, then closes the ENSv1 side according to the selector's anchor.
///
/// A `wrapper_backed_control` or child anchor names one NameWrapper binding that must still be
/// open at that instant, and closes exactly it. A `registrar_backed_registration` anchor names a
/// BaseRegistrar lease: the token, not any binding. The token is found by its own evidence and
/// every ENSv1 binding of the name still open at the cleanup is closed there
/// ([`registrar::close_lease_predecessor`]).
pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    transitions: &[MigrationAuthorityTransition],
) -> Result<()> {
    for transition in transitions {
        let selector = validate(transition)?;
        let boundary_time = lock_successor(transaction, transition).await?;
        // A child or unlocked second-level predecessor resolves and closes at its recorded ENSv1
        // cleanup. A locked-wrapped second-level predecessor resolves and closes at the boundary.
        let (predecessor_at, predecessor_time) = match &selector.cleanup {
            Some(cleanup) => (
                (
                    cleanup.block_number,
                    cleanup.transaction_index,
                    cleanup.log_index,
                ),
                resolve_cleanup_time(transaction, transition, &selector, cleanup).await?,
            ),
            None => (
                (
                    transition.block_number,
                    transition.transaction_index,
                    transition.log_index,
                ),
                boundary_time,
            ),
        };
        if selector.anchor_kind == REGISTRAR_ANCHOR_KIND {
            registrar::close_lease_predecessor(
                transaction,
                transition,
                &selector,
                predecessor_at,
                predecessor_time,
            )
            .await?;
        } else {
            close_wrapper_predecessor(
                transaction,
                transition,
                &selector,
                predecessor_at,
                predecessor_time,
            )
            .await?;
        }
    }
    Ok(())
}

/// Locks the exact ENSv2 successor binding the boundary names and returns the boundary instant.
async fn lock_successor(
    transaction: &mut Transaction<'_, Postgres>,
    transition: &MigrationAuthorityTransition,
) -> Result<time::OffsetDateTime> {
    let boundary_time: Option<time::OffsetDateTime> = sqlx::query_scalar(&format!(
        "SELECT lineage.block_timestamp + $8 * interval '1 microsecond'
         FROM surface_bindings binding
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.surface_binding_id = $1
           AND binding.logical_name_id = $2
           AND binding.resource_id = $3
           AND binding.authority_arm = $4
           AND binding.chain_id = $5
           AND binding.block_number = $6
           AND COALESCE((binding.provenance ->> '{}')::bigint, -1) = $7
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         FOR UPDATE OF binding",
        seam::TRANSACTION_INDEX_KEY,
    ))
    .bind(transition.successor_surface_binding_id)
    .bind(&transition.logical_name_id)
    .bind(transition.successor_resource_id)
    .bind(&transition.successor_arm)
    .bind(&transition.chain_id)
    .bind(transition.block_number)
    .bind(transition.transaction_index)
    .bind(transition.log_index)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to lock migration successor binding", error)
    })?;
    boundary_time.ok_or_else(|| {
        InterpretError::data_integrity(format!(
            "activated migration boundary {} has no exact ENSv2 successor binding",
            transition.boundary_event_identity
        ))
    })
}

/// Closes the one NameWrapper binding a wrapper or child anchor names: the binding of the name
/// whose own resource carries the wrapper's evidence for the namehash, positioned before the
/// predecessor instant and still open at it.
async fn close_wrapper_predecessor(
    transaction: &mut Transaction<'_, Postgres>,
    transition: &MigrationAuthorityTransition,
    selector: &PredecessorSelector,
    predecessor_at: (i64, i64, i64),
    predecessor_time: time::OffsetDateTime,
) -> Result<()> {
    let predecessors: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT surface_binding_id
         FROM surface_bindings
         WHERE chain_id = $1
           AND logical_name_id = $2
           AND authority_arm = $3
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (
               block_number,
               COALESCE((provenance ->> '{}')::bigint, -1),
               COALESCE((provenance ->> '{}')::bigint, -1)
           ) < ($4, $5, $6)
           AND active_from < $7
           AND (active_to IS NULL OR active_to >= $7)
           AND EXISTS (
               SELECT 1
               FROM normalized_events evidence
               WHERE evidence.chain_id = surface_bindings.chain_id
                 AND evidence.logical_name_id = surface_bindings.logical_name_id
                 AND evidence.resource_id = surface_bindings.resource_id
                 AND evidence.consumer_visibility = 'activated'
                 AND evidence.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND (
                     evidence.block_number,
                     COALESCE(evidence.transaction_index, -1),
                     COALESCE(evidence.log_index, -1)
                 ) < ($4, $5, $6)
                 AND EXISTS (
                     SELECT 1
                     FROM chain_lineage evidence_lineage
                     WHERE evidence_lineage.chain_id = evidence.chain_id
                       AND evidence_lineage.block_hash = evidence.block_hash
                       AND evidence_lineage.block_number = evidence.block_number
                       AND evidence_lineage.canonicality_state IN (
                           'canonical', 'safe', 'finalized'
                       )
                 )
                 AND evidence.after_state ->> 'authority_kind' = 'wrapper'
                 AND evidence.after_state ->> 'node' = $8
                 AND lower(evidence.raw_fact_ref ->> 'emitting_address') = lower($9)
           )
         ORDER BY surface_binding_id
         FOR UPDATE",
        seam::TRANSACTION_INDEX_KEY,
        seam::LOG_INDEX_KEY,
    ))
    .bind(&transition.chain_id)
    .bind(&transition.logical_name_id)
    .bind(&transition.expected_predecessor_arm)
    .bind(predecessor_at.0)
    .bind(predecessor_at.1)
    .bind(predecessor_at.2)
    .bind(predecessor_time)
    .bind(&selector.identity)
    .bind(&selector.contract_address)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to lock migration predecessor binding", error)
    })?;
    if predecessors.len() != 1 {
        return Err(InterpretError::data_integrity(format!(
            "activated migration boundary {} has {} active ENSv1 predecessors matching its resource selector; expected exactly one",
            transition.boundary_event_identity,
            predecessors.len()
        )));
    }
    sqlx::query(
        "UPDATE surface_bindings
         SET active_to = $2, observed_at = now()
         WHERE surface_binding_id = $1",
    )
    .bind(predecessors[0])
    .bind(predecessor_time)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to apply migration authority transition", error)
    })?;
    Ok(())
}

/// Resolves the recorded ENSv1 cleanup to the instant its authority ended.
///
/// A child boundary names its wrapper cleanup. Both unlocked `.eth` second-level paths name the
/// BaseRegistrar transfer that precedes the ENSv2 registration. The recorded event must exist
/// exactly as described — same identity, name, position, source event, and admitted emitter — on
/// readable canonical lineage. "Some earlier cleanup" is not equivalent evidence.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@a971bd64)
async fn resolve_cleanup_time(
    transaction: &mut Transaction<'_, Postgres>,
    transition: &MigrationAuthorityTransition,
    selector: &PredecessorSelector,
    cleanup: &PredecessorCleanup,
) -> Result<time::OffsetDateTime> {
    // The cleanup kind must match its anchor, so an unrelated same-position event cannot stand in
    // for the recorded transfer.
    let resolved: Option<time::OffsetDateTime> = sqlx::query_scalar(
        "SELECT lineage.block_timestamp + $6 * interval '1 microsecond'
         FROM normalized_events event
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.event_identity = $2
           AND event.logical_name_id = $3
           AND event.block_number = $4
           AND COALESCE(event.transaction_index, -1) = $5
           AND COALESCE(event.log_index, -1) = $6
           AND event.after_state ->> 'source_event' = $7
           AND (
               (
                   $9 = 'registrar_backed_registration'
                   AND event.event_kind = $10
                   AND EXISTS (
                       SELECT 1
                       FROM contract_instance_addresses address
                       WHERE address.chain_id = event.chain_id
                         AND address.contract_instance_id = $11
                         AND lower(address.address) = lower(
                             event.raw_fact_ref ->> 'emitting_address'
                         )
                         AND (
                             address.active_from_block_number IS NULL
                             OR address.active_from_block_number <= event.block_number
                         )
                         AND (
                             address.active_to_block_number IS NULL
                             OR address.active_to_block_number >= event.block_number
                         )
                   )
               )
               OR (
                   $9 = 'wrapper_backed_control'
                   AND event.event_kind = ANY($12)
                   AND lower(event.raw_fact_ref ->> 'emitting_address') = lower($8)
               )
           )
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(&transition.chain_id)
    .bind(&cleanup.event_identity)
    .bind(&transition.logical_name_id)
    .bind(cleanup.block_number)
    .bind(cleanup.transaction_index)
    .bind(cleanup.log_index)
    .bind(&cleanup.source_event)
    .bind(&selector.contract_address)
    .bind(&selector.anchor_kind)
    .bind(seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
    .bind(selector.contract_instance_id)
    .bind(
        seam::CHILD_CLEANUP_EVENT_KINDS
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to resolve migration predecessor cleanup", error)
    })?;
    resolved.ok_or_else(|| {
        InterpretError::data_integrity(format!(
            "activated migration boundary {} has no exact ENSv1 predecessor cleanup",
            transition.boundary_event_identity
        ))
    })
}
