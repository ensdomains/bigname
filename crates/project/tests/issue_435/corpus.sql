INSERT INTO chain_lineage (
    chain_id, block_hash, block_number, block_timestamp, canonicality_state
) VALUES ('issue-435-measurement', '0x435', 435, to_timestamp(435), 'canonical') ON CONFLICT DO NOTHING;
WITH parameters AS (
    SELECT __START__::bigint AS start, __V1_ROWS__::bigint AS total,
           __FRONTIER__::bigint AS frontier, __DEPTH__::bigint AS depth
), generated AS (
    SELECT start + local AS ordinal, local, start, total, frontier, depth,
           (total * 4 / 5) AS qualifying
    FROM parameters, generate_series(1, total) local
), endpoints AS (
    SELECT generated.*,
           CASE WHEN start = 0 AND local <= frontier * depth
                THEN ((local - 1) / depth) * (depth + 1) + ((local - 1) % depth) + 1
                ELSE 1000000000000 + ordinal * 2 END AS parent_number
    FROM generated
)
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    consumer_visibility, migration_correlation_ids, before_state, after_state
)
SELECT
    'issue-435:' || ordinal,
    'ens',
    CASE WHEN local <= qualifying OR local % 4 <> 0
         THEN 'SubregistryChanged' ELSE 'ResolverChanged' END,
    CASE WHEN local <= qualifying
         THEN CASE WHEN local % 20 = 0
                   THEN 'basenames_base_registry' ELSE 'ens_v1_registry_l1' END
         WHEN local % 4 = 1 THEN 'decoy_family'
         ELSE 'ens_v1_registry_l1' END,
    1,
    'issue-435-measurement',
    435,
    '0x435',
    'ens_v1_unwrapped_authority',
    CASE WHEN local > qualifying AND local % 4 = 3
         THEN 'orphaned'::canonicality_state ELSE 'canonical'::canonicality_state END,
    CASE WHEN local > qualifying AND local % 4 = 2
         THEN 'candidate' ELSE 'activated' END,
    CASE WHEN local > qualifying AND local % 4 = 2
         THEN ARRAY['issue-435-candidate']::text[] ELSE ARRAY[]::text[] END,
    CASE WHEN local <= frontier * depth OR local % 2 = 0 THEN jsonb_build_object(
        'node', '0x' || lpad(to_hex(parent_number), 64, '0'),
        'child_node', '0x' || lpad(to_hex(parent_number + 1), 64, '0')
    ) ELSE '{}'::jsonb END,
    jsonb_build_object(
        'node', '0x' || lpad(to_hex(parent_number), 64, '0'),
        'child_node', '0x' || lpad(to_hex(parent_number + 1), 64, '0')
    )
FROM endpoints;
