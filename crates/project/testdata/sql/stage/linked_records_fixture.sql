CREATE TEMP TABLE normalized_events(
    normalized_event_id bigint PRIMARY KEY, chain_id text, event_kind text,
    block_number bigint, block_hash text, canonicality_state text,
    consumer_visibility text, after_state jsonb, raw_fact_ref jsonb
);
CREATE TEMP TABLE chain_lineage(
    chain_id text, block_number bigint, block_hash text, canonicality_state text,
    PRIMARY KEY(chain_id, block_hash)
);
CREATE TEMP TABLE project_scope_resolvers(resolver_address text PRIMARY KEY);
CREATE TEMP TABLE project_events (LIKE normalized_events);
CREATE TEMP TABLE linked_input_calls(value integer);
-- A statement trigger detects accidental double execution even though the INSERT is idempotent.
CREATE FUNCTION pg_temp.count_linked_input_calls() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO linked_input_calls VALUES(1); RETURN NULL; END $$;
CREATE TRIGGER linked_input_call AFTER INSERT ON project_events
    FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.count_linked_input_calls();
INSERT INTO project_scope_resolvers VALUES('0xSHARED'), ('0xshared');
INSERT INTO chain_lineage VALUES
    ('bench', 9, 'old', 'safe'), ('bench', 10, 'current', 'finalized'),
    ('bench', 11, 'future', 'canonical'), ('other', 10, 'current', 'canonical'),
    ('bench', 10, 'orphan', 'orphaned');
INSERT INTO normalized_events
SELECT id, 'bench', kind, 10, 'current', 'canonical', 'activated',
    jsonb_build_object('resolver', '0xShared', 'storage_model', 'resolver_record_id',
                      'record_id', 'retained-id', 'value', 'value-' || id),
    jsonb_build_object('transaction_hash', 'tx-' || id, 'log_index', id)
FROM (VALUES
    (1, 'ResolverRecordLinked'), (2, 'ResolverPermissionArgument'), (3, 'RecordChanged'),
    (4, 'RecordChanged'), (5, 'RecordVersionChanged'), (6, 'ResolverRecordLinked'),
    (7, 'ResolverRecordLinked'), (8, 'ResolverRecordLinked'), (9, 'ResolverRecordLinked'),
    (10, 'ResolverRecordLinked'), (11, 'ResolverRecordLinked'), (12, 'ResolverRecordLinked'),
    (13, 'ResolverRecordLinked'), (14, 'ResolverRecordLinked'), (15, 'ResolverRecordLinked'),
    (16, 'ResolverRecordLinked'), (17, 'ResolverRecordLinked'), (18, 'ResolverRecordLinked'),
    (19, 'RecordChanged'), (20, 'ResolverRecordLinked')
) fixture(id, kind);
UPDATE normalized_events SET after_state = after_state || '{"storage_model":"node"}' WHERE normalized_event_id = 4;
UPDATE normalized_events SET block_number = 9, block_hash = 'old', canonicality_state = 'safe' WHERE normalized_event_id = 6;
UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE normalized_event_id = 7;
UPDATE normalized_events SET consumer_visibility = 'pending' WHERE normalized_event_id = 8;
UPDATE normalized_events SET block_number = 11, block_hash = 'future' WHERE normalized_event_id = 9;
UPDATE normalized_events SET block_hash = 'missing' WHERE normalized_event_id = 10;
UPDATE normalized_events SET block_hash = 'orphan' WHERE normalized_event_id = 11;
UPDATE normalized_events SET chain_id = 'other' WHERE normalized_event_id = 12;
UPDATE normalized_events SET after_state = after_state || '{"resolver":"0xunscoped"}' WHERE normalized_event_id = 13;
UPDATE normalized_events SET block_number = 9 WHERE normalized_event_id = 14;
UPDATE normalized_events SET block_number = NULL, block_hash = NULL WHERE normalized_event_id = 15;
UPDATE normalized_events SET after_state = after_state - 'resolver' WHERE normalized_event_id = 16;
UPDATE normalized_events SET after_state = after_state || '{"linked":false}' WHERE normalized_event_id = 17;
UPDATE normalized_events SET canonicality_state = 'finalized' WHERE normalized_event_id = 18;
UPDATE normalized_events SET after_state = after_state || '{"value":null}' WHERE normalized_event_id = 19;
-- An existing row suppresses both case-equivalent resolver-scope matches.
INSERT INTO project_events SELECT * FROM normalized_events WHERE normalized_event_id = 20;
TRUNCATE linked_input_calls;
CREATE TEMP TABLE project_staged_event_ids(normalized_event_id bigint PRIMARY KEY);
INSERT INTO project_staged_event_ids VALUES(20);
