CREATE TEMP TABLE project_mirror_seen_resources(resource_id uuid PRIMARY KEY) ON COMMIT DROP;
CREATE TEMP TABLE project_mirror_seen_names(logical_name_id text PRIMARY KEY) ON COMMIT DROP;
CREATE TEMP TABLE project_mirror_changed_nodes ON COMMIT DROP AS
SELECT DISTINCT event.namespace, lower(event.after_state ->> 'node') AS namehash, event.resource_id
FROM project_changed_events changed
JOIN normalized_events event USING(normalized_event_id)
JOIN chain_lineage lineage ON lineage.chain_id = event.chain_id
 AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
WHERE event.chain_id = $1 AND event.block_number <= $2
  AND event.event_kind = 'ResolverChanged'
  AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
  AND event.after_state ->> 'node' IS NOT NULL
  AND event.consumer_visibility = 'activated'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized');
