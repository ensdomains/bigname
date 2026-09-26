/* project:scope.v2_release_names */
-- Authority selection and the registration section count a release Interpret writes without a
-- name, on the resource a name was last bound to, as that name's release
-- (`builders/name_authority/build.sql`, `latest_v2_lifecycle`). Such a release names no one, so
-- the changed-event seed does not bring the name into scope. This brings in every name with an
-- ENSv2 binding on the release's resource at or before the target, closed bindings included; the
-- selector decides which of them the release decides once each is rebuilt.
INSERT INTO project_scope_names
SELECT DISTINCT binding.logical_name_id
FROM project_changed_events release
JOIN surface_bindings binding
  ON binding.resource_id = release.resource_id
 AND binding.chain_id = release.chain_id
JOIN chain_lineage lineage
  ON lineage.chain_id = binding.chain_id
 AND lineage.block_hash = binding.block_hash
 AND lineage.block_number = binding.block_number
WHERE release.logical_name_id IS NULL
  AND release.resource_id IS NOT NULL
  AND release.event_kind = 'RegistrationReleased'
  AND release.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
  AND binding.authority_arm = 'ens_v2'
  AND binding.block_number <= $1
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
ON CONFLICT DO NOTHING
