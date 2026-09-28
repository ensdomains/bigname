-- TYR-36 step 7b: record, per chain, the block the served tables stopped at when the
-- publication switch first ran Project. The switch skips the served engine while the Project
-- row follows the family marker, so a switch-off start refuses a chain whose served tables are
-- behind until a Project redo over the gap replays them (docs/deployment.md, Publication switch).
-- Additive; a database that ran the switch before this migration has no row to refuse on.
-- SQLx migrations precede fresh phase initialization: no phase schema is a no-op.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_served_stop (
    chain_id text NOT NULL,
    block_number bigint,
    PRIMARY KEY (chain_id),
    CHECK (block_number IS NULL OR block_number >= 0)
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_served_stop IS
    'Project-owned record, per chain, that the publication switch ran Project (TYR-36 step 7b). The served engine does not run under the switch, so the served tables stop while the Project row follows the family marker. With the switch off, the phase runner and the API refuse a chain whose served tables stopped short of the Project row until a Project redo over the gap replays them and deletes the row. Step 7c removes the switch and this table.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_served_stop.chain_id IS
    'This value is the chain the switch ran Project on.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_served_stop.block_number IS
    'This value is the Project row''s block when the switch first ran Project, the last block the served tables applied; null when Project had applied no block.'
$ddl$;
END
$migration$;
