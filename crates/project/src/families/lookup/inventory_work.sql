/* project:families.lookup.inventory_work */
INSERT INTO pg_temp.bigname_lookup_inventory_work
WITH changed AS MATERIALIZED (
    SELECT family, key::jsonb AS key FROM project_family_undo
    WHERE chain_id=$1 AND block_number=$2
      AND family IN ('project_resource_pointer', 'project_resolver_classification',
        'project_registry_pointer', 'project_node_record_partition', 'project_node_record_value',
        'project_resolver_link', 'project_record_id_value')
), inputs AS (
    SELECT 'resource_pointer'::text AS kind, key ->> 1 AS key1, ''::text AS key2, ''::text AS key3
    FROM changed WHERE family='project_resource_pointer'
    UNION SELECT 'classification', key ->> 1, '', '' FROM changed
      WHERE family='project_resolver_classification'
    UNION SELECT 'registry_node', key ->> 1, key ->> 2, '' FROM changed
      WHERE family='project_registry_pointer'
    UNION SELECT 'partition', key ->> 1, key ->> 2, key ->> 3 FROM changed
      WHERE family IN ('project_node_record_partition', 'project_node_record_value')
    UNION SELECT 'link', key ->> 1, key ->> 2, '' FROM changed WHERE family='project_resolver_link'
    UNION SELECT 'record_id', key ->> 1, key ->> 2, '' FROM changed WHERE family='project_record_id_value'
    UNION SELECT 'registry_node', namespace, lower(namehash), '' FROM name_surfaces
      WHERE chain_id=$1 AND block_number>$3 AND block_number<=$2
    UNION SELECT 'identity', logical_name_id, '', '' FROM name_surfaces
      WHERE chain_id=$1 AND block_number>$3 AND block_number<=$2
)
SELECT DISTINCT dependency.resource_id, true FROM inputs
JOIN project_lookup_dependency dependency
  ON dependency.chain_id=$1 AND dependency.kind=inputs.kind
 AND dependency.key1=inputs.key1 AND dependency.key2=inputs.key2 AND dependency.key3=inputs.key3
ON CONFLICT (resource_id) DO UPDATE SET refresh=true
