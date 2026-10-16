-- Existing schema-v2 databases gain the composition pass of a rebuild in ranges on
-- project_repair_record: the state composing, the block the pass composed at (composed_block),
-- the pass's stream and cursor (compose_stream, compose_cursor) and its committed chunk count
-- (compose_chunks), with the pass's name work list in project_compose_name_work. The ranges of a
-- rebuild at or below its switch point write family rows only; the pass then composes them once,
-- and every rebuild generation above composed_block composes and journals as before. Every family
-- run of this release reads the new record columns, so this schema-migration must run first. The
-- columns and the table are additive and unread by every served path. An empty schema-migration
-- database has no phase baseline yet, so this migration is a no-op there and phase-runner
-- init-schema installs the same columns, constraints and table.
--
-- The compose CHECK only requires a stream while composing. A runner of the release before this
-- one does not know the columns, so a record it moves off composing keeps them, and the CHECK
-- lets it.
--
-- The state CHECK and the rebuilding trusted-base CHECK were created unnamed, so each is found
-- by the columns it reads (state alone, and state with trusted_base_number, which no other CHECK
-- of the table reads) and replaced. The rebuilding one is replaced by a named constraint that also
-- covers composing, as the baseline names it. Applied again, it finds its own constraints and
-- replaces them unchanged.
DO $migration$
DECLARE
    old_constraint text;
BEGIN
IF to_regclass('bigname_phase.project_repair_record') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
ALTER TABLE bigname_phase.project_repair_record
    ADD COLUMN IF NOT EXISTS composed_block bigint,
    ADD COLUMN IF NOT EXISTS compose_stream smallint,
    ADD COLUMN IF NOT EXISTS compose_cursor text,
    ADD COLUMN IF NOT EXISTS compose_chunks bigint NOT NULL DEFAULT 0
$ddl$;

FOR old_constraint IN
    SELECT constraint_row.conname FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid = 'bigname_phase.project_repair_record'::regclass
      AND constraint_row.contype = 'c'
      AND (SELECT array_agg(attribute.attname::text ORDER BY attribute.attname::text)
           FROM pg_attribute attribute
           WHERE attribute.attrelid = constraint_row.conrelid
             AND attribute.attnum = ANY (constraint_row.conkey))
          IN (ARRAY['state'], ARRAY['state', 'trusted_base_number'])
LOOP
    EXECUTE format('ALTER TABLE bigname_phase.project_repair_record DROP CONSTRAINT %I',
        old_constraint);
END LOOP;

EXECUTE $ddl$
ALTER TABLE bigname_phase.project_repair_record
    DROP CONSTRAINT IF EXISTS project_repair_record_state_check,
    DROP CONSTRAINT IF EXISTS project_repair_record_rebuild_base_check,
    DROP CONSTRAINT IF EXISTS project_repair_record_compose_check,
    ADD CONSTRAINT project_repair_record_state_check
        CHECK (state IN ('undoing', 'replaying', 'rebuilding', 'composing', 'complete')),
    ADD CONSTRAINT project_repair_record_rebuild_base_check
        CHECK (state NOT IN ('rebuilding', 'composing') OR trusted_base_number IS NULL),
    ADD CONSTRAINT project_repair_record_compose_check
        CHECK (state <> 'composing' OR compose_stream IS NOT NULL)
$ddl$;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.state IS
    'This value is undoing, replaying, rebuilding, composing or complete. A rebuild in ranges is composing while its composition pass runs.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.composed_block IS
    'This value is the block a rebuild''s composition pass composed at, -1 when it composed nothing; null before the pass and for an undo-then-replay. Every rebuild generation above it composes and journals.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_stream IS
    'This value is the composition pass stream under way: 0 names, 1 record nodes, 2 record ids, 3 history windows; null outside a pass.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_cursor IS
    'This value is the last key the composition pass committed in its stream, a work list serial in the names stream; null at a stream''s start and outside a pass.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_chunks IS
    'This value is how many chunks the latest composition pass committed; zero before a pass. The pass refreshes the planner statistics of the composed tables after 1, 2, 4, 8, ... chunks.'
$ddl$;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_compose_name_work (
    chain_id text NOT NULL,
    serial bigint NOT NULL,
    logical_name_id text NOT NULL,
    reach smallint NOT NULL,
    PRIMARY KEY (chain_id, serial),
    CHECK (serial > 0),
    CHECK (reach IN (0, 1, 2))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_compose_name_work IS
    'Project-owned work list of a rebuild''s composition pass: every name id the pass composes or indexes, numbered in COLLATE "C" order once when the pass begins. The names stream reads it in serial order. The pass end and a family reset clear the chain''s rows.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_name_work.chain_id IS
    'This value is the chain whose rebuild composes the id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_name_work.serial IS
    'This value is the id''s position in the work list, from 1.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_name_work.logical_name_id IS
    'This value is the name id the pass composes or indexes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_compose_name_work.reach IS
    'This value is what the pass composes for the id: 0 the address name index, 1 the lookup name row as well, 2 also the summary and the catalogue name source.'
$ddl$;
END
$migration$;
