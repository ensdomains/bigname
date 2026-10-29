-- Existing schema-v2 databases gain the history catalogue build of a rebuild's composition pass:
-- project_compose_name_work records which names the names stream composed (composed, with a
-- partial index over the names still to compose) and the current address relations of each
-- composed name (relations), and project_compose_history_work holds the build's source keys and
-- their per-span partial aggregates. Both tables hold rows only while a pass runs. The pass end
-- and a family reset clear them, and nothing served reads them. A pass under way when this
-- release starts never resumes on it: the projection content hash rotates, so the rebuild starts
-- again and its reset clears the work list. Every family run of this release writes the new
-- columns, so this schema-migration must run first. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner init-schema installs
-- the same columns, index and table. Applied again, it changes nothing.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_compose_name_work') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
ALTER TABLE bigname_phase.project_compose_name_work
    ADD COLUMN IF NOT EXISTS composed boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS relations jsonb
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_compose_name_work_uncomposed_idx
    ON bigname_phase.project_compose_name_work (chain_id, serial) WHERE NOT composed
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_name_work.composed IS
    'This value is true once the names stream composed the id, in the transaction that composed it. The history catalogue build starts only when every row is.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_name_work.relations IS
    'This value is the current address relations the names stream found for a composed name, as a JSON array of address, namespace, resource_id and relation, which the history catalogue build reads. It is null below reach 2 and for a name with none.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_compose_history_work (
    chain_id text NOT NULL,
    source_kind smallint NOT NULL,
    source_key text NOT NULL,
    resolver_address text NOT NULL DEFAULT '',
    part smallint NOT NULL,
    key_partition smallint NOT NULL,
    work boolean NOT NULL,
    first_bucket bigint,
    last_bucket bigint,
    event_mask bigint,
    key_bloom bit(256),
    PRIMARY KEY (chain_id, source_kind, source_key, resolver_address, part),
    CHECK (source_kind BETWEEN 0 AND 3),
    CHECK (source_kind = 3 OR resolver_address = ''),
    CHECK (part >= -1),
    CHECK ((first_bucket IS NULL) = (last_bucket IS NULL))
)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_compose_history_work_part_idx
    ON bigname_phase.project_compose_history_work (chain_id, part)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_compose_history_work_partition_idx
    ON bigname_phase.project_compose_history_work (chain_id, key_partition, source_kind)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_compose_history_work IS
    'Project-owned work table of a rebuild''s history catalogue build: every catalogue source key the composition pass reaches, with partial aggregates of its events per block span. The build merges them into the catalogue. The pass end and a family reset clear the chain''s rows.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.chain_id IS
    'This value is the chain whose rebuild builds the catalogue.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.source_kind IS
    'This value is the catalogue source kind: 0 logical name, 1 resource, 2 node records, 3 resolver record-ID writes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.source_key IS
    'This value is the source key as project_history_source holds it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.resolver_address IS
    'This value is the lower-case resolver of a record-ID source, empty for other kinds.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.part IS
    'This value is the block span whose events this row aggregates, from 0, or -1 for a key the work list reaches with no event of its own.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.key_partition IS
    'This value is the key partition of the source key, its hashtext under the build''s partition mask.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.work IS
    'This value is true when a resource key also gets resolver edges and an address membership. It is read for resources only.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.first_bucket IS
    'This value is the earliest bucket of the span''s events the catalogue reads, block divided by 256 or -1 for unpositioned events, and null when the span has none.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.last_bucket IS
    'This value is the latest bucket of the span''s events the catalogue reads, null when the span has none.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.event_mask IS
    'This value is the event-kind bit union of the span''s events the catalogue reads, null when the span has none.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_history_work.key_bloom IS
    'This value is the record-key summary union of the span''s events the catalogue reads, null when the span has none.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_stream IS
    'This value is the composition pass stream under way: 0 names, 1 record nodes, 2 record ids, 3 history catalogue build; null outside a pass.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_cursor IS
    'This value is the last key the composition pass committed in its stream, a work list serial in the names stream and the last step and partition with the partition counts and server version in the history catalogue build; null at a stream''s start and outside a pass.'
$ddl$;
END
$migration$;
