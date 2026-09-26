/* project:scope.v2_release_names.retracted */
-- The redo mirror of `v2_release_names.sql`. A release without a name that a reorg retracts is
-- not a changed event, and no name cites it, so nothing else brings back the name it decided.
-- Interpret deletes such a release during redo and keeps its resource in
-- `project_redo_expiry_roots`; a release left on an orphaned block is still in
-- `normalized_events`. Both bring in every name with an ENSv2 binding on that resource at or
-- before the target, closed bindings included, so the redo rebuilds the name without it.
WITH released_resources AS (
    SELECT root.resource_id
    FROM project_redo_expiry_roots root
    WHERE root.chain_id = $1
      AND root.block_number BETWEEN $2 AND $3
      AND root.resource_id IS NOT NULL
    UNION
    SELECT release.resource_id
    FROM normalized_events release
    JOIN chain_lineage release_lineage
      ON release_lineage.chain_id = release.chain_id
     AND release_lineage.block_hash = release.block_hash
     AND release_lineage.block_number = release.block_number
    WHERE release.chain_id = $1
      AND release.block_number BETWEEN $2 AND $3
      AND release.logical_name_id IS NULL
      AND release.resource_id IS NOT NULL
      AND release.event_kind = 'RegistrationReleased'
      AND release.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
      AND (release.canonicality_state = 'orphaned'
           OR release_lineage.canonicality_state = 'orphaned')
)
INSERT INTO project_scope_names
SELECT DISTINCT binding.logical_name_id
FROM released_resources released
JOIN surface_bindings binding
  ON binding.resource_id = released.resource_id
 AND binding.chain_id = $1
JOIN chain_lineage lineage
  ON lineage.chain_id = binding.chain_id
 AND lineage.block_hash = binding.block_hash
 AND lineage.block_number = binding.block_number
WHERE binding.authority_arm = 'ens_v2'
  AND binding.block_number <= $4
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
ON CONFLICT DO NOTHING
