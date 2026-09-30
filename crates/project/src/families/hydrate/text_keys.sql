/* project:families.hydrate.text.keys */
-- Keep dependency changes ahead of their indexed fan-out. OFFSET 0 preserves the lateral
-- key probes even when PostgreSQL estimates a JSON parameter as many more rows than it holds.
WITH values_changed AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_node_record_value, $2)
), partitions_changed AS MATERIALIZED (
    SELECT change.* FROM jsonb_populate_recordset(NULL::project_node_record_partition, $3) change
    LEFT JOIN LATERAL (
        SELECT stored.version_position FROM project_node_record_partition stored
        WHERE (stored.chain_id, stored.resolver_address, stored.arm, stored.arm_identity) =
            (change.chain_id, change.resolver_address, change.arm, change.arm_identity) OFFSET 0
    ) stored ON true
    WHERE change.version_position IS DISTINCT FROM stored.version_position
), admissions_changed AS MATERIALIZED (
    SELECT change.* FROM jsonb_populate_recordset(NULL::project_resolver_classification, $4) change
    LEFT JOIN LATERAL (
        SELECT stored.* FROM project_resolver_classification stored
        WHERE (stored.chain_id, stored.resolver_address) = (change.chain_id, change.resolver_address)
        OFFSET 0
    ) stored ON true
    WHERE (change.classification, change.support_status, change.unsupported_reason, change.manifest_id)
        IS DISTINCT FROM (stored.classification, stored.support_status, stored.unsupported_reason, stored.manifest_id)
), affected AS (
    SELECT chain_id, resolver_address, arm, arm_identity, record_key FROM values_changed
    UNION
    SELECT v.chain_id, v.resolver_address, v.arm, v.arm_identity, v.record_key
    FROM partitions_changed p CROSS JOIN LATERAL (
        SELECT v.* FROM project_node_record_value v
        WHERE (v.chain_id, v.resolver_address, v.arm, v.arm_identity) =
            (p.chain_id, p.resolver_address, p.arm, p.arm_identity) OFFSET 0
    ) v
    UNION
    SELECT v.chain_id, v.resolver_address, v.arm, v.arm_identity, v.record_key
    FROM admissions_changed a CROSS JOIN LATERAL (
        SELECT v.* FROM project_node_record_value v
        WHERE (v.chain_id, v.resolver_address) = (a.chain_id, a.resolver_address) OFFSET 0
    ) v
)
SELECT to_jsonb(affected) FROM affected WHERE chain_id = $1
