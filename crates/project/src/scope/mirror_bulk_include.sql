/* project:scope.mirror_bulk_include */
WITH full_links AS MATERIALIZED (
 SELECT link.* FROM project_mirror_frontier_resources scope JOIN project_mirror_links link
   ON link.mirror_resource_id = scope.resource_id
 WHERE EXISTS (SELECT 1 FROM project_mirror_cached_nodes node
               WHERE node.namespace=link.namespace AND node.namehash=link.namehash)
 UNION
 SELECT link.* FROM project_mirror_frontier_names scope JOIN project_mirror_links link
   ON link.consulted_logical_name_id = scope.logical_name_id
 WHERE EXISTS (SELECT 1 FROM project_mirror_cached_nodes node
               WHERE node.namespace=link.namespace AND node.namehash=link.namehash)
), full_nodes AS MATERIALIZED (
 SELECT DISTINCT namespace, namehash FROM full_links
), changed_nodes AS MATERIALIZED (
 SELECT node.* FROM project_mirror_frontier_changed changed JOIN project_mirror_cached_nodes node
   ON node.namespace=changed.namespace AND node.namehash=changed.namehash
  AND node.resource_id IS NOT DISTINCT FROM changed.resource_id
), partial_nodes AS MATERIALIZED (
 SELECT node.namespace, node.namehash FROM project_mirror_frontier_resources scope
 JOIN project_mirror_cached_nodes node USING(resource_id)
 UNION SELECT namespace, namehash FROM changed_nodes
), selected_links AS MATERIALIZED (
 SELECT mirror_resource_id, consulted_logical_name_id FROM full_links
 UNION ALL
 SELECT link.mirror_resource_id, link.consulted_logical_name_id FROM partial_nodes node
 JOIN project_mirror_links link USING(namespace,namehash)
), inserted_names AS (
 INSERT INTO project_scope_names SELECT DISTINCT consulted_logical_name_id FROM selected_links
 ON CONFLICT DO NOTHING
)
INSERT INTO project_scope_resources
SELECT mirror_resource_id FROM selected_links
UNION
SELECT node.resource_id FROM full_nodes full_node JOIN project_mirror_cached_nodes node
 USING(namespace,namehash) WHERE node.resource_id IS NOT NULL
UNION
SELECT node.resource_id FROM changed_nodes node WHERE node.resource_id IS NOT NULL
 AND EXISTS(SELECT 1 FROM project_mirror_links link
            WHERE link.namespace=node.namespace AND link.namehash=node.namehash)
ON CONFLICT DO NOTHING;
