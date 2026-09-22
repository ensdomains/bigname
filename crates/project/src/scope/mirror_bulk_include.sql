WITH rebuilt AS MATERIALIZED (
 SELECT pair.* FROM project_scope_resources scope JOIN project_mirror_pairs pair
 ON pair.mirror_resource_id=scope.resource_id
 UNION
 SELECT pair.* FROM project_scope_resources scope JOIN project_mirror_pairs pair
 ON pair.consulted_resource_id=scope.resource_id
 UNION
 SELECT pair.* FROM project_scope_names scope JOIN project_mirror_pairs pair
 ON pair.consulted_logical_name_id=scope.logical_name_id
 UNION
 SELECT * FROM project_mirror_pairs WHERE changed
), scoped_names AS (
 INSERT INTO project_scope_names SELECT DISTINCT consulted_logical_name_id FROM rebuilt
 ON CONFLICT DO NOTHING
)
INSERT INTO project_scope_resources
SELECT mirror_resource_id FROM rebuilt
UNION SELECT consulted_resource_id FROM rebuilt WHERE consulted_resource_id IS NOT NULL
ON CONFLICT DO NOTHING;
