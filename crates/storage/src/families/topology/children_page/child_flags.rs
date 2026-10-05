//! The per-child flags the children relation (`children_page::push_children`) projects for a
//! child with no name row: its lifecycle shadow, the NameWrapper holding it, its released lease
//! and its lease's holder, each a scalar the planner drops when a statement reads no column of
//! it (the child counts).

use crate::families::name::rendered::composed_surface_sql;

/// Whether the child has no name surface at the clock (the joined surface fails
/// `registry_children::published_surface_exists`, whose canonicality tests
/// [`CHILD_SURFACE_FILTER`] already applies) and the newest retained ENSv1 or Basenames
/// registrar lifecycle event of its node at or below the clock is a `RegistrationReleased`.
/// Interpret releases a lease at the first block past its grace, unnamed while no surface names
/// the node (crates/adapters/src/schema_v2.rs, `settle_block_boundary`), and orders it before
/// every transaction of that block, so a re-registration there is newer. A child with a name
/// surface serves its name row's own registration. The probe reads
/// `project_lifecycle_event_namehash_idx`.
pub(super) fn released_lease() -> String {
    format!(
        "CASE WHEN {composed}
              AND child_surface.block_number <= clock.block_number THEN FALSE
         ELSE COALESCE((
             SELECT lease.event_kind = 'RegistrationReleased'
             FROM bigname_phase.project_lifecycle_event lease
             WHERE lease.chain_id = clock.chain_id
               AND lease.namehash = lower(selected.namehash)
               AND lease.state_kind = 'resource'
               AND lease.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
               AND lease.block_number <= clock.block_number
             ORDER BY lease.block_number DESC, lease.transaction_index DESC NULLS LAST,
                      lease.log_index DESC NULLS LAST, lease.event_identity COLLATE \"C\" DESC
             LIMIT 1), FALSE)
    END",
        composed = composed_surface_sql("child_surface")
    )
}

/// The holder of the child's registrar lease when it has no name surface at the clock (as in
/// [`released_lease`]): of the newest retained ENSv1 or Basenames registrar lifecycle event of its
/// node at or below the clock among the rows that name a holder, a `TokenControlTransferred`'s
/// recipient or a `RegistrationGranted`'s registrant, and none after a `RegistrationReleased`
/// or for a zero registrant (a grant synthesised from a bare renewal). These are the rows the
/// named path's registrant reads (crates/project/src/families/lifecycle/registrant.rs). The
/// token moves without the registry record until `reclaim`
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f).
/// The probe reads `project_lifecycle_event_namehash_idx`.
pub(super) fn token_holder() -> String {
    format!(
        "CASE WHEN {composed}
              AND child_surface.block_number <= clock.block_number THEN NULL
         ELSE (
             SELECT NULLIF(CASE lease.event_kind
                               WHEN 'TokenControlTransferred' THEN lease.to_address
                               WHEN 'RegistrationGranted' THEN lease.registrant
                           END, '0x0000000000000000000000000000000000000000')
             FROM bigname_phase.project_lifecycle_event lease
             WHERE lease.chain_id = clock.chain_id
               AND lease.namehash = lower(selected.namehash)
               AND lease.state_kind = 'resource'
               AND lease.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
               AND lease.event_kind IN ('RegistrationGranted', 'TokenControlTransferred',
                                        'RegistrationReleased')
               AND lease.block_number <= clock.block_number
             ORDER BY lease.block_number DESC, lease.transaction_index DESC NULLS LAST,
                      lease.log_index DESC NULLS LAST, lease.event_identity COLLATE \"C\" DESC
             LIMIT 1)
    END",
        composed = composed_surface_sql("child_surface")
    )
}

/// Whether the child's only surface is a shadow one at or below the clock that a lifecycle
/// observer named: the NameWrapper (`ens_v1_wrapper_l1`, `NameWrapped`) or the ENSv1 registrar
/// and its controllers (`ens_v1_registrar_l1`, `NameRegistered` and `NameRenewed`). Each shadow
/// observation writes a `PreimageObserved` event under the observer's source family
/// (crates/adapters/src/schema_v2/identity.rs, `materialize`), and the surface row keeps only the
/// earliest observation's provenance, so the events, not the row, tell the observers apart. The
/// lookup reads `normalized_events_name_history_idx`.
pub(super) const LIFECYCLE_SHADOW: &str = "COALESCE(child_surface.visibility_state = 'shadow'
         AND child_surface.block_number <= clock.block_number
         AND EXISTS (
             SELECT 1 FROM bigname_phase.normalized_events observed
             WHERE observed.logical_name_id = selected.child_logical_name_id
               AND observed.chain_id = clock.chain_id
               AND observed.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND observed.block_number <= clock.block_number
               AND observed.event_kind = 'PreimageObserved'
               AND observed.consumer_visibility = 'activated'
               AND observed.source_family IN ('ens_v1_wrapper_l1', 'ens_v1_registrar_l1')),
         FALSE)";

/// Whether the child's only surface is a shadow one at or below the clock that a NameWrapper
/// observed, and its served registry owner is that NameWrapper. NameWrapper takes the registry
/// record of a child it creates or wraps
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579-L581 @ ens_v1@91c966f), and
/// an unwrap or a parent's registry `setSubnodeOwner` moves it out again
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f).
pub(super) const WRAPPER_HELD: &str = "COALESCE(child_surface.visibility_state = 'shadow'
         AND child_surface.block_number <= clock.block_number
         AND EXISTS (
             SELECT 1 FROM bigname_phase.normalized_events observed
             WHERE observed.logical_name_id = selected.child_logical_name_id
               AND observed.chain_id = clock.chain_id
               AND observed.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND observed.block_number <= clock.block_number
               AND observed.event_kind = 'PreimageObserved'
               AND observed.consumer_visibility = 'activated'
               AND observed.source_family = 'ens_v1_wrapper_l1'
               AND lower(observed.raw_fact_ref ->> 'emitting_address') = selected.owner),
         FALSE)";
