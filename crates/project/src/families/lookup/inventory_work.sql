/* project:families.lookup.inventory_work */
WITH changed AS MATERIALIZED (
    SELECT family, key::jsonb AS key, before_image FROM project_family_undo
    WHERE chain_id=$1 AND block_number=$2
      AND family IN ('project_resource_pointer', 'project_resolver_classification',
        'project_registry_pointer', 'project_node_record_partition', 'project_node_record_value',
        'project_resolver_link', 'project_record_id_value')
), inputs AS (
    SELECT 'resource_pointer'::text AS kind, key ->> 1 AS key1, ''::text AS key2, ''::text AS key3
    FROM changed WHERE family='project_resource_pointer'
    UNION SELECT 'classification', old.key ->> 1, '', '' FROM changed old
      LEFT JOIN project_resolver_classification current
        ON current.chain_id=$1 AND current.resolver_address=old.key ->> 1
      WHERE old.family='project_resolver_classification'
        AND ((old.before_image IS NULL OR old.before_image='null'::jsonb)
              IS DISTINCT FROM (current.chain_id IS NULL)
          OR COALESCE(old.before_image -> 'classification', 'null'::jsonb)
              IS DISTINCT FROM COALESCE(current.classification, 'null'::jsonb)
          OR old.before_image ->> 'support_status' IS DISTINCT FROM current.support_status
          OR old.before_image ->> 'unsupported_reason' IS DISTINCT FROM current.unsupported_reason
          OR COALESCE(old.before_image -> 'manifest_id', 'null'::jsonb)
              IS DISTINCT FROM COALESCE(to_jsonb(current.manifest_id), 'null'::jsonb))
    UNION SELECT 'registry_node', key ->> 1, key ->> 2, '' FROM changed
      WHERE family='project_registry_pointer'
    UNION SELECT 'partition', old.key ->> 1, old.key ->> 2, old.key ->> 3 FROM changed old
      LEFT JOIN project_node_record_partition current
        ON current.chain_id=$1 AND current.resolver_address=old.key ->> 1
       AND current.arm=old.key ->> 2 AND current.arm_identity=old.key ->> 3
      WHERE old.family='project_node_record_partition'
        AND ((old.before_image IS NULL OR old.before_image='null'::jsonb)
              IS DISTINCT FROM (current.chain_id IS NULL)
          OR COALESCE(old.before_image -> 'version_position', 'null'::jsonb)
              IS DISTINCT FROM COALESCE(current.version_position, 'null'::jsonb))
    UNION SELECT 'link', key ->> 1, key ->> 2, '' FROM changed WHERE family='project_resolver_link'
    UNION SELECT 'registry_node', namespace, lower(namehash), '' FROM name_surfaces
      WHERE chain_id=$1 AND block_number>$3 AND block_number<=$2
    UNION SELECT 'identity', logical_name_id, '', '' FROM name_surfaces
      WHERE chain_id=$1 AND block_number>$3 AND block_number<=$2
), value_inputs AS (
    SELECT 'partition'::text AS kind, key ->> 1 AS key1, key ->> 2 AS key2,
           key ->> 3 AS key3, key ->> 4 AS record_key FROM changed
      WHERE family='project_node_record_value'
    UNION SELECT 'record_id', key ->> 1, key ->> 2, '', key ->> 3 FROM changed
      WHERE family='project_record_id_value'
), record_work AS (
    INSERT INTO pg_temp.bigname_lookup_record_work
    SELECT DISTINCT dependency.resource_id, value_inputs.record_key FROM value_inputs
    JOIN project_lookup_dependency dependency
      ON dependency.chain_id=$1 AND dependency.kind=value_inputs.kind
     AND dependency.key1=value_inputs.key1 AND dependency.key2=value_inputs.key2
     AND dependency.key3=value_inputs.key3
    ON CONFLICT DO NOTHING RETURNING resource_id
), work AS (
    SELECT dependency.resource_id, true AS refresh FROM inputs
    JOIN project_lookup_dependency dependency
      ON dependency.chain_id=$1 AND dependency.kind=inputs.kind
     AND dependency.key1=inputs.key1 AND dependency.key2=inputs.key2 AND dependency.key3=inputs.key3
    UNION ALL SELECT resource_id, false FROM record_work
)
INSERT INTO pg_temp.bigname_lookup_inventory_work
SELECT resource_id, bool_or(refresh) FROM work GROUP BY resource_id
ON CONFLICT (resource_id) DO UPDATE
SET refresh=pg_temp.bigname_lookup_inventory_work.refresh OR EXCLUDED.refresh
