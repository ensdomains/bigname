//! The ENSv1 predecessor of a `.eth` second-level migration whose selector names the
//! BaseRegistrar lease (`registrar_backed_registration`).
//!
//! The lease is the token. The unlocked migration controller receives the token from whoever
//! holds it, reclaims the ENSv1 registry record for itself, writes the Graveyard as owner with an
//! empty resolver and TTL, parks the token in the Graveyard and only then registers the name in
//! ENSv2. The registry-owner record and the token are independent: `reclaim` lets the token
//! holder overwrite whatever owner the registry names, and a token transfer never touches the
//! registry by itself.
//! (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L121 @ ens_v2@a971bd6)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
//!
//! So the predecessor is found by the token's own evidence, never through a registry-owner
//! binding. A transfer without `reclaim` (a
//! [registry-only handoff](../../../../../../docs/glossary.md#registry-only-handoff)) closes the
//! lease binding and binds the name to a registry-only resource while the lease goes on under it;
//! that closed lease binding is still the predecessor. What the boundary closes is whatever ENSv1
//! binding of the name is still open at the cleanup: the lease binding itself, or the
//! registry-only binding that stood in for it, or nothing when ordinary ENSv1 interpretation of
//! the same transaction already closed it.

use bigname_adapters::schema_v2::{MigrationAuthorityTransition, seam};
use sqlx::{Postgres, Transaction, types::Uuid};

use super::selector::PredecessorSelector;
use crate::{InterpretError, Result};

/// The token's own lifecycle events. Authority-boundary events (`SurfaceBound`, `SurfaceUnbound`,
/// `AuthorityEpochChanged`, permission and resolver changes) also carry the registrar observation
/// that caused them, token id included, but they land on the resource that gained or lost the
/// name, which for a registry-only handoff is not the lease.
const LEASE_EVIDENCE_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "RegistrationRenewed",
    "ExpiryChanged",
    seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
];

/// Resolves the lease and closes the ENSv1 side of the name at `predecessor_time`.
///
/// The lease is the one resource of the name that carries an activated canonical event with
/// `after_state.token_id` equal to the selector's labelhash, emitted by the selector's
/// BaseRegistrar instance, positioned before the cleanup (or at it: a registrar identity
/// materialized at `NameUnwrapped` has the cleanup transfer as its first evidence); that once had
/// an `ens_v1` binding positioned no later than the same rule allows; and whose registration was
/// not released before the cleanup. Zero or several such resources are integrity errors, exactly
/// as before: the entry point accepts the token only from the BaseRegistrar, whose `ownerOf`
/// rejects an expired token, so a supported migration with no live lease means the ENSv1
/// interpretation is corrupt.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L100-L102 @ ens_v2@a971bd6)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
pub(super) async fn close_lease_predecessor(
    transaction: &mut Transaction<'_, Postgres>,
    transition: &MigrationAuthorityTransition,
    selector: &PredecessorSelector,
    predecessor_at: (i64, i64, i64),
    predecessor_time: time::OffsetDateTime,
) -> Result<()> {
    // A fallback registrar binding is effective from NameUnwrapped, but its confirming evidence
    // is the cleanup transfer, so a lease whose binding is positioned at the cleanup may use the
    // cleanup itself as evidence. Every other lease needs evidence strictly before it: the cleanup
    // transfer alone does not vouch for a binding opened earlier.
    let allow_cleanup_evidence = selector.cleanup.is_some();
    let leases: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT evidence.resource_id
         FROM normalized_events evidence
         WHERE evidence.chain_id = $1
           AND evidence.logical_name_id = $2
           AND evidence.resource_id IS NOT NULL
           AND evidence.event_kind = ANY($10)
           AND evidence.consumer_visibility = 'activated'
           AND evidence.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND evidence.after_state ->> 'token_id' = $7
           AND (
               (
                   evidence.block_number,
                   COALESCE(evidence.transaction_index, -1),
                   COALESCE(evidence.log_index, -1)
               ) < ($4, $5, $6)
               OR (
                   $9
                   AND (
                       evidence.block_number,
                       COALESCE(evidence.transaction_index, -1),
                       COALESCE(evidence.log_index, -1)
                   ) = ($4, $5, $6)
                   AND EXISTS (
                       SELECT 1
                       FROM surface_bindings fallback
                       WHERE fallback.chain_id = evidence.chain_id
                         AND fallback.logical_name_id = evidence.logical_name_id
                         AND fallback.resource_id = evidence.resource_id
                         AND fallback.authority_arm = $3
                         AND fallback.canonicality_state IN ('canonical', 'safe', 'finalized')
                         AND (
                             fallback.block_number,
                             COALESCE((fallback.provenance ->> '{transaction_index}')::bigint, -1),
                             COALESCE((fallback.provenance ->> '{log_index}')::bigint, -1)
                         ) = ($4, $5, $6)
                   )
               )
           )
           AND EXISTS (
               SELECT 1
               FROM chain_lineage evidence_lineage
               WHERE evidence_lineage.chain_id = evidence.chain_id
                 AND evidence_lineage.block_hash = evidence.block_hash
                 AND evidence_lineage.block_number = evidence.block_number
                 AND evidence_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           )
           AND EXISTS (
               SELECT 1
               FROM contract_instance_addresses address
               WHERE address.chain_id = evidence.chain_id
                 AND address.contract_instance_id = $8
                 AND lower(address.address) = lower(evidence.raw_fact_ref ->> 'emitting_address')
                 AND (
                     address.active_from_block_number IS NULL
                     OR address.active_from_block_number <= evidence.block_number
                 )
                 AND (
                     address.active_to_block_number IS NULL
                     OR address.active_to_block_number >= evidence.block_number
                 )
           )
           AND EXISTS (
               SELECT 1
               FROM surface_bindings lease
               WHERE lease.chain_id = evidence.chain_id
                 AND lease.logical_name_id = evidence.logical_name_id
                 AND lease.resource_id = evidence.resource_id
                 AND lease.authority_arm = $3
                 AND lease.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND (
                     (
                         lease.block_number,
                         COALESCE((lease.provenance ->> '{transaction_index}')::bigint, -1),
                         COALESCE((lease.provenance ->> '{log_index}')::bigint, -1)
                     ) < ($4, $5, $6)
                     OR ($9 AND (
                         lease.block_number,
                         COALESCE((lease.provenance ->> '{transaction_index}')::bigint, -1),
                         COALESCE((lease.provenance ->> '{log_index}')::bigint, -1)
                     ) = ($4, $5, $6))
                 )
           )
           AND NOT EXISTS (
               SELECT 1
               FROM normalized_events released
               WHERE released.chain_id = evidence.chain_id
                 AND released.resource_id = evidence.resource_id
                 AND released.event_kind = 'RegistrationReleased'
                 AND released.consumer_visibility = 'activated'
                 AND released.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND (
                     released.block_number,
                     COALESCE(released.transaction_index, -1),
                     COALESCE(released.log_index, -1)
                 ) < ($4, $5, $6)
                 AND EXISTS (
                     SELECT 1
                     FROM chain_lineage released_lineage
                     WHERE released_lineage.chain_id = released.chain_id
                       AND released_lineage.block_hash = released.block_hash
                       AND released_lineage.block_number = released.block_number
                       AND released_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                 )
           )
         ORDER BY evidence.resource_id",
        transaction_index = seam::TRANSACTION_INDEX_KEY,
        log_index = seam::LOG_INDEX_KEY,
    ))
    .bind(&transition.chain_id)
    .bind(&transition.logical_name_id)
    .bind(&transition.expected_predecessor_arm)
    .bind(predecessor_at.0)
    .bind(predecessor_at.1)
    .bind(predecessor_at.2)
    .bind(&selector.identity)
    .bind(selector.contract_instance_id)
    .bind(allow_cleanup_evidence)
    .bind(
        LEASE_EVIDENCE_EVENT_KINDS
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to resolve the migration predecessor lease", error)
    })?;
    if leases.len() != 1 {
        return Err(InterpretError::data_integrity(format!(
            "activated migration boundary {} has {} active ENSv1 predecessors matching its resource selector; expected exactly one (lease resources {leases:?})",
            transition.boundary_event_identity,
            leases.len()
        )));
    }

    // Close every ENSv1 binding of the name still open at the cleanup. The overlap exclusion on
    // `surface_bindings` allows at most one, and ordinary interpretation of the same transaction
    // may already have closed it, so this is zero or one row.
    sqlx::query(&format!(
        "UPDATE surface_bindings
         SET active_to = $7, observed_at = now()
         WHERE chain_id = $1
           AND logical_name_id = $2
           AND authority_arm = $3
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (
               (
                   block_number,
                   COALESCE((provenance ->> '{transaction_index}')::bigint, -1),
                   COALESCE((provenance ->> '{log_index}')::bigint, -1)
               ) < ($4, $5, $6)
               OR ($8 AND (
                   block_number,
                   COALESCE((provenance ->> '{transaction_index}')::bigint, -1),
                   COALESCE((provenance ->> '{log_index}')::bigint, -1)
               ) = ($4, $5, $6))
           )
           AND active_from < $7
           AND (active_to IS NULL OR active_to > $7)",
        transaction_index = seam::TRANSACTION_INDEX_KEY,
        log_index = seam::LOG_INDEX_KEY,
    ))
    .bind(&transition.chain_id)
    .bind(&transition.logical_name_id)
    .bind(&transition.expected_predecessor_arm)
    .bind(predecessor_at.0)
    .bind(predecessor_at.1)
    .bind(predecessor_at.2)
    .bind(predecessor_time)
    .bind(allow_cleanup_evidence)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to apply migration authority transition", error)
    })?;

    // An ENSv1 binding opened at the cleanup instant itself cannot be closed there (a binding is
    // never empty), so it would outlive the boundary. Ordinary interpretation of a complete
    // migration transaction opens none; if one is present the ENSv1 side is not consistent with
    // the boundary and the batch must stop rather than publish two current authorities.
    let open_after: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT surface_binding_id
         FROM surface_bindings
         WHERE chain_id = $1
           AND logical_name_id = $2
           AND authority_arm = $3
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (
               block_number,
               COALESCE((provenance ->> '{transaction_index}')::bigint, -1),
               COALESCE((provenance ->> '{log_index}')::bigint, -1)
           ) <= ($4, $5, $6)
           AND (active_to IS NULL OR active_to > $7)
         ORDER BY surface_binding_id",
        transaction_index = seam::TRANSACTION_INDEX_KEY,
        log_index = seam::LOG_INDEX_KEY,
    ))
    .bind(&transition.chain_id)
    .bind(&transition.logical_name_id)
    .bind(&transition.expected_predecessor_arm)
    .bind(predecessor_at.0)
    .bind(predecessor_at.1)
    .bind(predecessor_at.2)
    .bind(predecessor_time)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to verify the migration predecessor close", error)
    })?;
    if !open_after.is_empty() {
        return Err(InterpretError::data_integrity(format!(
            "activated migration boundary {} leaves {} ENSv1 bindings open at its predecessor cleanup (bindings {open_after:?})",
            transition.boundary_event_identity,
            open_after.len()
        )));
    }
    Ok(())
}
