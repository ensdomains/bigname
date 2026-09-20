use super::super::lineage::same_fork_as;

// A registry-only resource is retained across registrar leases. Its identity is established
// by an activated authority row, independently of the name's current projection.
pub(super) fn is_registry_only(resource: &str, event: &str, canonical_only: bool) -> String {
    let fork = same_fork_as("registry_epoch", &[event], canonical_only);
    let canonical = canonical_row("registry_epoch", "registry_epoch_lineage", canonical_only);
    format!(
        r#"EXISTS (
            SELECT 1 FROM bigname_phase.normalized_events registry_epoch
            LEFT JOIN bigname_phase.chain_lineage registry_epoch_lineage
              ON registry_epoch_lineage.chain_id = registry_epoch.chain_id
             AND registry_epoch_lineage.block_hash = registry_epoch.block_hash
            WHERE registry_epoch.resource_id = {resource}
              AND registry_epoch.chain_id = {event}.chain_id
              AND registry_epoch.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1')
              AND registry_epoch.event_kind IN (
                  'AuthorityTransferred', 'AuthorityEpochChanged', 'SurfaceBound'
              )
              AND registry_epoch.after_state ->> 'authority_kind' = 'registry_only'
              AND registry_epoch.consumer_visibility = 'activated'
              AND (registry_epoch.block_number, COALESCE(registry_epoch.log_index, -1)) <=
                  ({event}.block_number, COALESCE({event}.log_index, -1))
              AND {fork} AND {canonical}
        )"#
    )
}

// The grant's node identifies the name even when the grant predates plaintext discovery.
// Choosing the last grant first, then testing its release, preserves gaps between leases.
// All evidence is at or before the event, hence also below its publication bound.
pub(super) fn lease_at_event(event: &str, canonical_only: bool) -> String {
    let grant_fork = same_fork_as("registry_lease_grant", &[event], canonical_only);
    let release_fork = same_fork_as("registry_lease_release", &[event], canonical_only);
    let grant_canonical = canonical_row(
        "registry_lease_grant",
        "registry_grant_lineage",
        canonical_only,
    );
    let release_canonical = canonical_row(
        "registry_lease_release",
        "registry_release_lineage",
        canonical_only,
    );
    format!(
        r#"(
            SELECT registry_lease.resource_id FROM (
                SELECT registry_lease_grant.resource_id,
                       registry_lease_grant.block_number, registry_lease_grant.log_index
                FROM bigname_phase.normalized_events registry_lease_grant
                LEFT JOIN bigname_phase.chain_lineage registry_grant_lineage
                  ON registry_grant_lineage.chain_id = registry_lease_grant.chain_id
                 AND registry_grant_lineage.block_hash = registry_lease_grant.block_hash
                WHERE registry_lease_grant.chain_id = {event}.chain_id
                  AND registry_lease_grant.namespace = {event}.namespace
                  AND registry_lease_grant.source_family LIKE 'ens\_v1\_%'
                  AND registry_lease_grant.source_family = 'ens_v1_registrar_l1'
                  AND registry_lease_grant.event_kind = 'RegistrationGranted'
                  AND registry_lease_grant.consumer_visibility = 'activated'
                  AND registry_lease_grant.resource_id IS NOT NULL
                  AND COALESCE(
                      registry_lease_grant.namespace || ':' || lower(COALESCE(
                          registry_lease_grant.after_state ->> 'child_node',
                          registry_lease_grant.after_state ->> 'namehash',
                          registry_lease_grant.after_state ->> 'node',
                          registry_lease_grant.after_state #>> '{{grant_source,node}}',
                          registry_lease_grant.after_state #>> '{{revocation_source,node}}'
                      )), registry_lease_grant.logical_name_id
                  ) = COALESCE({event}.logical_name_id,
                      {event}.namespace || ':' || lower(COALESCE(
                          {event}.after_state ->> 'child_node',
                          {event}.after_state ->> 'namehash',
                          {event}.after_state ->> 'node'
                      )))
                  AND (registry_lease_grant.block_number, COALESCE(registry_lease_grant.log_index, -1)) <=
                      ({event}.block_number, COALESCE({event}.log_index, -1))
                  AND {grant_fork} AND {grant_canonical}
                ORDER BY registry_lease_grant.block_number DESC,
                         registry_lease_grant.log_index DESC NULLS LAST,
                         registry_lease_grant.normalized_event_id DESC
                LIMIT 1
            ) registry_lease
            WHERE NOT EXISTS (
                SELECT 1 FROM bigname_phase.normalized_events registry_lease_release
                LEFT JOIN bigname_phase.chain_lineage registry_release_lineage
                  ON registry_release_lineage.chain_id = registry_lease_release.chain_id
                 AND registry_release_lineage.block_hash = registry_lease_release.block_hash
                WHERE registry_lease_release.resource_id = registry_lease.resource_id
                  AND registry_lease_release.chain_id = {event}.chain_id
                  AND registry_lease_release.event_kind = 'RegistrationReleased'
                  AND registry_lease_release.consumer_visibility = 'activated'
                  AND (registry_lease_release.block_number, COALESCE(registry_lease_release.log_index, -1)) >=
                      (registry_lease.block_number, COALESCE(registry_lease.log_index, -1))
                  AND (registry_lease_release.block_number, COALESCE(registry_lease_release.log_index, -1)) <=
                      ({event}.block_number, COALESCE({event}.log_index, -1))
                  AND {release_fork} AND {release_canonical}
            )
        )"#
    )
}

fn canonical_row(event: &str, lineage: &str, canonical_only: bool) -> String {
    if !canonical_only {
        return "TRUE".to_owned();
    }
    format!(
        "{event}.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND ({event}.block_hash IS NULL
              OR {lineage}.canonicality_state IN ('canonical', 'safe', 'finalized'))"
    )
}
