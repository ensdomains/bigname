CREATE TEMP TABLE project_scope_names (logical_name_id text PRIMARY KEY) ON COMMIT DROP;
CREATE TEMP TABLE project_scope_children (logical_name_id text PRIMARY KEY) ON COMMIT DROP;
CREATE TEMP TABLE project_scope_primary (
    address text, coin_type text, namespace text,
    PRIMARY KEY (address, coin_type, namespace)
) ON COMMIT DROP;
INSERT INTO project_scope_names VALUES ('ens:0xaa'), ('ens:'), (''), ('ens:CaseSensitive');
INSERT INTO project_scope_children VALUES ('ens:0xaa'), ('basenames:0xbb');
INSERT INTO project_scope_primary VALUES
    ('0xabc', '60', 'ens'), ('0xabc', '0', 'basenames'), ('', '', ''), ('42', '60', 'ens');

INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT chain_id, '0x' || block_number, block_number,
       '2026-09-17T00:00:00Z'::timestamptz, 'canonical'::canonicality_state
FROM (VALUES ('project-history-test'), ('project-history-other')) chains(chain_id)
CROSS JOIN generate_series(9, 11) block_number;

CREATE TEMP TABLE history_fixture (
    event_identity text, namespace text, event_kind text, source_family text,
    chain_id text, block_number bigint, canonicality_state text, consumer_visibility text,
    before_state jsonb, after_state jsonb, expected_name boolean, expected_primary boolean
) ON COMMIT DROP;
INSERT INTO history_fixture VALUES
    ('name:node', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:child', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"child_node":"0XAA"}', TRUE, FALSE),
    ('name:after-target', 'ens', 'AliasChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"to_logical_name_id":"ens:0xaa"}', TRUE, FALSE),
    ('name:before-target', 'ens', 'AliasChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{"to_logical_name_id":"ens:0xaa"}', '{}', TRUE, FALSE),
    ('name:four-matches', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{"to_logical_name_id":"ens:0xaa"}', '{"node":"0XAA","child_node":"0XAA","to_logical_name_id":"ens:0xaa"}', TRUE, FALSE),
    ('name:empty-node', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":""}', TRUE, FALSE),
    ('name:empty-target', 'ens', 'AliasChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"to_logical_name_id":""}', TRUE, FALSE),
    ('name:exact-case', 'ens', 'AliasChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"to_logical_name_id":"ens:CaseSensitive"}', TRUE, FALSE),
    ('name:case-not-folded', 'ens', 'AliasChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"to_logical_name_id":"ENS:CASESENSITIVE"}', FALSE, FALSE),
    ('name:basenames-child', 'basenames', 'SubregistryChanged', 'basenames_base_registry', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"child_node":"0XBB"}', TRUE, FALSE),
    ('name:any-subregistry-family', 'ens', 'SubregistryChanged', 'ens_v2_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:any-alias-family', 'ens', 'AliasChanged', 'other-family', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:authority:ens_v1_registry_l1', 'ens', 'AuthorityTransferred', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:authority:basenames_base_registry', 'ens', 'AuthorityTransferred', 'basenames_base_registry', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:authority:ens_v2_registry_l1', 'ens', 'AuthorityTransferred', 'ens_v2_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:wrong-kind', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:before-node-not-selected', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{"node":"0XAA","child_node":"0XAA"}', '{}', FALSE, FALSE),
    ('name:missing-keys', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{}', FALSE, FALSE),
    ('name:json-null', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":null,"to_logical_name_id":null}', FALSE, FALSE),
    ('name:namespace-mismatch', 'basenames', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:state:canonical', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:state:safe', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'safe', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:state:finalized', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'finalized', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:state:observed', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'observed', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:state:orphaned', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'orphaned', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:before-boundary', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 9, 'canonical', 'activated', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('name:after-boundary', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 11, 'canonical', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:null-block', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', NULL, 'canonical', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:other-chain', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-other', 10, 'canonical', 'activated', '{}', '{"node":"0XAA"}', FALSE, FALSE),
    ('name:candidate-visible-to-scope', 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'candidate', '{}', '{"node":"0XAA"}', TRUE, FALSE),
    ('primary:after', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, TRUE),
    ('primary:before', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', '{}', FALSE, TRUE),
    ('primary:after-source', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"primary_claim_source":{"address":"0XABC","coin_type":"60","namespace":"ens"}}', FALSE, TRUE),
    ('primary:before-source', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{"primary_claim_source":{"address":"0XABC","coin_type":"60","namespace":"ens"}}', '{}', FALSE, TRUE),
    ('primary:four-matches', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{"address":"0XABC","coin_type":"60","namespace":"ens","primary_claim_source":{"address":"0XABC","coin_type":"60","namespace":"ens"}}', '{"address":"0XABC","coin_type":"60","namespace":"ens","primary_claim_source":{"address":"0XABC","coin_type":"60","namespace":"ens"}}', FALSE, TRUE),
    ('primary:empty-tuple', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"","coin_type":"","namespace":""}', FALSE, TRUE),
    ('primary:json-number', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":42,"coin_type":60,"namespace":"ens"}', FALSE, TRUE),
    ('primary:other-admitted-tuple', 'ens', 'ReverseChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"0","namespace":"basenames"}', FALSE, TRUE),
    ('primary:any-record-family', 'basenames', 'RecordChanged', 'other-family', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, TRUE),
    ('primary:state:canonical', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, TRUE),
    ('primary:state:safe', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'safe', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, TRUE),
    ('primary:state:finalized', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'finalized', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, TRUE),
    ('primary:state:observed', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'observed', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, FALSE),
    ('primary:state:orphaned', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'orphaned', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, FALSE),
    ('primary:wrong-coin', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"61","namespace":"ens"}', FALSE, FALSE),
    ('primary:namespace-case', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ENS"}', FALSE, FALSE),
    ('primary:missing-coin', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","namespace":"ens"}', FALSE, FALSE),
    ('primary:null-address', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":null,"coin_type":"60","namespace":"ens"}', FALSE, FALSE),
    ('primary:mixed-tuple', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"0","namespace":"ens"}', FALSE, FALSE),
    ('primary:no-cross-state-tuple', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{"coin_type":"60","namespace":"ens"}', '{"address":"0XABC"}', FALSE, FALSE),
    ('primary:wrong-kind', 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, FALSE),
    ('primary:before-boundary', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 9, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, TRUE),
    ('primary:after-boundary', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 11, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, FALSE),
    ('primary:null-block', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', NULL, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, FALSE),
    ('primary:other-chain', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-other', 10, 'canonical', 'activated', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, FALSE),
    ('primary:candidate-visible-to-scope', 'ens', 'RecordChanged', 'ens_v1_registry_l1', 'project-history-test', 10, 'canonical', 'candidate', '{}', '{"address":"0XABC","coin_type":"60","namespace":"ens"}', FALSE, TRUE);
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    consumer_visibility, before_state, after_state, migration_correlation_ids
)
SELECT event_identity, namespace, event_kind, source_family, 1,
       chain_id, block_number, CASE WHEN block_number IS NOT NULL THEN '0x' || block_number END,
       'ens_v1_unwrapped_authority', canonicality_state::canonicality_state,
       consumer_visibility, before_state, after_state,
       CASE WHEN consumer_visibility = 'candidate' THEN ARRAY['candidate'] ELSE ARRAY[]::text[] END
FROM history_fixture;
