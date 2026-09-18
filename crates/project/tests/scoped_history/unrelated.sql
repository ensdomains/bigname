INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    before_state, after_state
)
SELECT 'unrelated:' || kind || ':' || value, 'ens', kind, 'ens_v1_registry_l1', 1,
       'project-history-test', 10, '0x10', 'ens_v1_unwrapped_authority',
       'canonical'::canonicality_state,
       jsonb_build_object('to_logical_name_id', 'ens:unrelated:' || value,
                         'address', 'unrelated:' || value, 'coin_type', '60', 'namespace', 'ens',
                         'primary_claim_source', jsonb_build_object(
                             'address', 'unrelated:' || value, 'coin_type', '60', 'namespace', 'ens')),
       jsonb_build_object('node', 'unrelated:' || value, 'child_node', 'unrelated:' || value,
                         'to_logical_name_id', 'ens:unrelated:' || value,
                         'address', 'unrelated:' || value, 'coin_type', '60', 'namespace', 'ens',
                         'primary_claim_source', jsonb_build_object(
                             'address', 'unrelated:' || value, 'coin_type', '60', 'namespace', 'ens'))
FROM generate_series($1::bigint, $2::bigint) value
CROSS JOIN (VALUES ('SubregistryChanged'), ('RecordChanged')) kinds(kind)
