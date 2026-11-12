-- Existing schema-v2 databases gain the work table of the composition pass's event count stream:
-- project_compose_event_count_work holds the per-key event counts of each block span the stream
-- read, which it then sums into project_event_count by key partition. The table holds rows only
-- while a pass runs: the pass's beginning, the stream's last partition, the pass end and a family
-- reset clear it, and nothing served reads it. Every family run of this release writes it, so
-- this schema-migration must run first. A pass under way when this release starts never resumes
-- on it: the projection content hash rotates, so the rebuild starts again and its reset clears
-- the work tables. An empty schema-migration database has no phase baseline yet, so this
-- migration is a no-op there and phase-runner init-schema installs the same table and index.
-- Applied again, it changes nothing.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_repair_record') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_compose_event_count_work (
    chain_id text NOT NULL,
    part smallint NOT NULL,
    key_partition smallint NOT NULL,
    anchor_kind smallint NOT NULL,
    anchor text NOT NULL,
    namespace text NOT NULL,
    event_kind text NOT NULL,
    delta bigint NOT NULL,
    PRIMARY KEY (chain_id, part, anchor_kind, anchor, namespace, event_kind),
    CHECK (part >= 0),
    CHECK (key_partition >= 0),
    CHECK (anchor_kind BETWEEN 0 AND 2),
    CHECK (delta > 0)
)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_compose_event_count_work_partition_idx
    ON bigname_phase.project_compose_event_count_work (chain_id, key_partition)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_compose_event_count_work IS
    'Project-owned work table of a rebuild''s event count stream: the per-key counts of the events of each block span at or below the pass block, as each block counts its own range. The stream sums them into project_event_count by key partition. The pass''s beginning, the stream''s last partition, the pass end and a family reset clear the chain''s rows.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.chain_id IS
    'This value is the chain whose rebuild counts the events.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.part IS
    'This value is the block span whose events this row counts, from 0.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.key_partition IS
    'This value is the key partition of the anchor, its hashtext under the stream''s partition mask.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.anchor_kind IS
    'This value is how the anchor reads the events, as project_event_count.anchor_kind holds it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.anchor IS
    'This value is the lower-case contract address the anchor kind names.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.namespace IS
    'This value is the namespace of the counted events.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.event_kind IS
    'This value is the event kind of the counted events.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_event_count_work.delta IS
    'This value is the number of the span''s events the key covers.'
$ddl$;
END
$migration$;
