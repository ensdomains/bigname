/* project:scope.mirror.create_mirror_seen_resources */
CREATE TEMP TABLE project_mirror_seen_resources(resource_id uuid PRIMARY KEY) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_seen_names */
CREATE TEMP TABLE project_mirror_seen_names(logical_name_id text PRIMARY KEY) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_changed_nodes */
CREATE TEMP TABLE project_mirror_changed_nodes ON COMMIT DROP AS
SELECT DISTINCT event.namespace, lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node')) AS namehash, event.resource_id
FROM project_changed_events changed
JOIN normalized_events event USING(normalized_event_id)
JOIN chain_lineage lineage ON lineage.chain_id = event.chain_id
 AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
WHERE event.chain_id = $1 AND event.block_number <= $2
  AND event.event_kind = 'ResolverChanged'
  AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
  AND COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node') IS NOT NULL
  AND event.consumer_visibility = 'activated'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized');

/* project:scope.mirror.create_mirror_seen_seeds */
-- Labels are chain data of any length, so no btree key holds the label array itself: an
-- entry over about 2.7 KB would fail the insert. Seeds are keyed by a 64-bit hash of the
-- array, and every probe also compares the labels, so a collision never changes a result.
CREATE TEMP TABLE project_mirror_seen_seeds(namespace text, raw_labels text[]) ON COMMIT DROP;
/* project:scope.mirror.index_mirror_seen_seeds_namespace */
CREATE INDEX ON project_mirror_seen_seeds(namespace, hash_array_extended(raw_labels, 0));
/* project:scope.mirror.create_mirror_seen_pointers */
CREATE TEMP TABLE project_mirror_seen_pointers(resource_id uuid, logical_name_id text, PRIMARY KEY(resource_id, logical_name_id)) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_links */
CREATE TEMP TABLE project_mirror_links(mirror_resource_id uuid, consulted_logical_name_id text,
 namespace text, namehash text, PRIMARY KEY(mirror_resource_id,consulted_logical_name_id,namespace,namehash)) ON COMMIT DROP;
/* project:scope.mirror.index_mirror_links_consulted_logical_name_id */
CREATE INDEX ON project_mirror_links(consulted_logical_name_id);
/* project:scope.mirror.index_mirror_links_namespace */
CREATE INDEX ON project_mirror_links(namespace,namehash);
/* project:scope.mirror.create_mirror_seen_nodes */
CREATE TEMP TABLE project_mirror_seen_nodes(namespace text, namehash text, PRIMARY KEY(namespace, namehash)) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_cached_nodes */
CREATE TEMP TABLE project_mirror_cached_nodes(namespace text, namehash text, resource_id uuid,
    UNIQUE NULLS NOT DISTINCT(namespace, namehash, resource_id)) ON COMMIT DROP;
/* project:scope.mirror.index_mirror_cached_nodes_resource_id */
CREATE INDEX ON project_mirror_cached_nodes(resource_id);

/* project:scope.mirror.create_mirror_frontier_resources */
-- Per-pass work tables for mirror_bulk.sql. They are created once per publication and
-- truncated after each pass instead of being dropped and recreated: PostgreSQL holds the
-- lock of every relation created or dropped until the transaction ends, so recreating them
-- on every closure pass would grow the lock footprint with the hop count.
CREATE TEMP TABLE IF NOT EXISTS project_mirror_frontier_resources(resource_id uuid) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_frontier_names */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_frontier_names(logical_name_id text) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_frontier_changed */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_frontier_changed(namespace text, namehash text, resource_id uuid) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_resource_nodes */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_resource_nodes(namespace text, namehash text) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_seeds */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_seeds(namespace text, raw_labels text[]) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_queried_names */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_queried_names(logical_name_id text) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_pointer_candidates */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_pointer_candidates(resource_id uuid, logical_name_id text) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_new_pointers */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_new_pointers(resource_id uuid, logical_name_id text) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_walks */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_walks(mirror_resource_id uuid, namespace text, suffix text[]) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_suffixes */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_suffixes(namespace text, suffix text[]) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_surfaces */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_surfaces(suffix text[], logical_name_id text, namespace text, namehash text) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_consulted */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_consulted(mirror_resource_id uuid, logical_name_id text, namespace text, namehash text) ON COMMIT DROP;
/* project:scope.mirror.create_mirror_wanted */
CREATE TEMP TABLE IF NOT EXISTS project_mirror_wanted(namespace text, namehash text) ON COMMIT DROP;
