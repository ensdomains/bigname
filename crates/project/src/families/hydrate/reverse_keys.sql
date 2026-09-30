/* project:families.hydrate.reverse.keys */
-- Each changed dependency probes its own keys; keep generic plans from reading retained
-- tuple/claim history before discovering that a JSON change list is empty.
WITH tuples_changed AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_reverse_tuple, $2)
), registry_changed AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_registry_pointer, $3)
), resources_changed AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_resource_pointer, $4)
), resource_nodes AS (
    SELECT namespace, namehash FROM resources_changed
    UNION SELECT p.namespace, p.namehash FROM resources_changed c CROSS JOIN LATERAL (
        SELECT p.namespace, p.namehash FROM project_resource_pointer p
        WHERE (p.chain_id, p.resource_id) = (c.chain_id, c.resource_id) OFFSET 0
    ) p
), nodes_changed AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_reverse_node_claim, $5)
), normalization_changed AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_claim_normalization, $6)
), affected AS (
    SELECT address, coin_type, namespace FROM tuples_changed
    UNION
    SELECT t.address, t.coin_type, t.namespace
    FROM registry_changed p CROSS JOIN LATERAL (
        SELECT t.* FROM project_reverse_tuple t
        WHERE t.chain_id = $1 AND t.namespace = p.namespace AND t.reverse_node = p.node OFFSET 0
    ) t
    UNION
    SELECT t.address, t.coin_type, t.namespace
    FROM resource_nodes p CROSS JOIN LATERAL (
        SELECT t.* FROM project_reverse_tuple t
        WHERE t.chain_id = $1 AND t.namespace = p.namespace AND t.reverse_node = p.namehash OFFSET 0
    ) t
    UNION
    SELECT t.address, t.coin_type, t.namespace
    FROM nodes_changed p CROSS JOIN LATERAL (
        SELECT t.* FROM project_reverse_tuple t
        WHERE t.chain_id = $1 AND t.namespace = p.namespace AND t.reverse_node = p.reverse_node OFFSET 0
    ) t
    UNION
    SELECT t.address, t.coin_type, t.namespace
    FROM normalization_changed p CROSS JOIN LATERAL (
        SELECT t.* FROM project_reverse_tuple t
        WHERE t.chain_id = $1 AND t.claim_event_identity = p.claim_event_identity OFFSET 0
    ) t
    UNION
    SELECT t.address, t.coin_type, t.namespace
    FROM normalization_changed p CROSS JOIN LATERAL (
        SELECT n.* FROM project_reverse_node_claim n
        WHERE n.chain_id = $1 AND n.event_identity = p.claim_event_identity OFFSET 0
    ) n CROSS JOIN LATERAL (
        SELECT t.* FROM project_reverse_tuple t
        WHERE t.chain_id = $1 AND t.namespace = n.namespace AND t.reverse_node = n.reverse_node OFFSET 0
    ) t
)
SELECT to_jsonb(affected) FROM affected
