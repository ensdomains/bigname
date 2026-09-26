-- Existing schema-v2 databases gain the Project-owned owned key family tables
-- project_family_marker, project_family_undo, project_repair_record (TYR-36
-- step 2). The tables are additive and unread by every served path; the
-- family loop fills them block by block after each Project batch commits. An
-- empty schema-migration database has no phase baseline yet, so this
-- migration is a no-op there and phase-runner init-schema installs the same
-- tables.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_family_marker (
    chain_id text NOT NULL,
    current_block_number bigint,
    current_block_hash text,
    block_timestamp timestamptz,
    input_content_hash text,
    sequence bigint NOT NULL DEFAULT 0,
    interpret_input_content_hash text,
    interpret_redo_attempt bigint,
    state text NOT NULL,
    PRIMARY KEY (chain_id),
    CHECK ((current_block_number IS NULL) = (current_block_hash IS NULL)),
    CHECK (state IN ('live', 'bootstrap_pending')),
    CHECK (sequence >= 0)
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_family_marker IS
    'Project-owned shadow marker of the owned key families: the last block the family loop applied on each chain, the generation every family block and family undo advances, and the input revision it read. It is not the served marker; chain_phase_state keeps that role. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.chain_id IS
    'This value is the chain the marker belongs to.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.current_block_number IS
    'This value is the last block whose facts the families hold; null before the first block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.current_block_hash IS
    'This value is the readable hash that block had when it was applied.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.block_timestamp IS
    'This value is that block''s timestamp from chain_lineage, the block clock the family reads will use.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.input_content_hash IS
    'This value is the interpreter content hash of the binary that applied the block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.sequence IS
    'This value counts every family block and every family undo applied on the chain; it only grows. It is the explicit publication generation of the design, named so because schema-v2 reserves generation for authorised columns.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.interpret_input_content_hash IS
    'This value is the Interpret row''s input_content_hash read before the block, the first half of the input revision; null while Interpret was in redo.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.interpret_redo_attempt IS
    'This value is the Interpret row''s redo_attempt_generation read before the block, the second half of the input revision; null while Interpret was in redo.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.state IS
    'This value is live when the marker follows the served publication and bootstrap_pending while a rebuild is populating the families.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_family_undo (
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    family text NOT NULL,
    key text NOT NULL,
    before_image jsonb,
    PRIMARY KEY (chain_id, block_number, family, key),
    CHECK (btrim(block_hash) <> '')
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_family_undo IS
    'Project-owned undo record of the owned key families: per applied block, the image each family row had before the block first changed it, plus the prior marker under family marker. Undoing a block restores these images; rows below the retained depth are pruned as the marker advances. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_undo.chain_id IS
    'This value is the chain of the block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_undo.block_number IS
    'This value is the block whose change the row undoes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_undo.block_hash IS
    'This value is the readable hash the block had when it was applied.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_undo.family IS
    'This value names the family table of the row, or marker for the prior family marker.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_undo.key IS
    'This value is the row''s primary key as a JSON array in key column order, or the chain id for the marker.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_undo.before_image IS
    'This value is to_jsonb of the row before the block, or null when the row did not exist.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_repair_record (
    chain_id text NOT NULL,
    attempt bigint NOT NULL,
    reason text NOT NULL,
    trusted_base_number bigint,
    trusted_base_hash text,
    replay_target_number bigint NOT NULL,
    replay_target_hash text NOT NULL,
    state text NOT NULL,
    prefix_interpret_input_content_hash text,
    prefix_interpret_redo_attempt bigint,
    invalidation_from bigint,
    pending_undo_target bigint,
    completed_sequence bigint,
    completed_marker_number bigint,
    completed_marker_hash text,
    completed_input_hash text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id),
    CHECK (reason IN ('required_redo_range', 'orphaned_lineage', 'content_hash_rebuild', 'operator_redo')),
    CHECK (state IN ('undoing', 'replaying', 'rebuilding', 'complete')),
    CHECK ((state = 'complete') = (completed_sequence IS NOT NULL AND completed_marker_number IS NOT NULL AND completed_marker_hash IS NOT NULL AND completed_input_hash IS NOT NULL)),
    CHECK (state = 'complete' OR (completed_sequence IS NULL AND completed_marker_number IS NULL AND completed_marker_hash IS NULL AND completed_input_hash IS NULL)),
    CHECK (state <> 'undoing' OR (prefix_interpret_input_content_hash IS NULL AND prefix_interpret_redo_attempt IS NULL)),
    CHECK ((trusted_base_number IS NULL) = (trusted_base_hash IS NULL)),
    CHECK (state <> 'rebuilding' OR trusted_base_number IS NULL)
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_repair_record IS
    'Project-owned repair record: the durable description of the latest family undo-then-replay or rebuild of a chain, its attempt, reason, trusted base, replay target, state, input revision and completion identity. Undo never rewrites it. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.chain_id IS
    'This value is the chain under repair.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.attempt IS
    'This value is the Project row''s redo_attempt_generation when the repair began.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.reason IS
    'This value says why the families are repaired: a required redo range, an orphaned lineage, a content-hash rebuild or an operator redo.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.trusted_base_number IS
    'This value is the block below the repaired range whose facts stay; null for a rebuild.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.trusted_base_hash IS
    'This value is the trusted base''s readable hash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.replay_target_number IS
    'This value is the block the replay must reach, captured before the first undo.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.replay_target_hash IS
    'This value is the replay target''s hash when captured.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.state IS
    'This value is undoing, replaying, rebuilding or complete.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.prefix_interpret_input_content_hash IS
    'This value is the Interpret input_content_hash of the input revision the replay started from; null while undoing.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.prefix_interpret_redo_attempt IS
    'This value is the Interpret redo_attempt_generation of that input revision; null while undoing.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.invalidation_from IS
    'This value is the lowest block a stamp invalidated while the repair was active; step 2 never sets it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.pending_undo_target IS
    'This value is the block the undo must reach before replay may start; null once replay starts.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.completed_sequence IS
    'This value is the family marker sequence the completing block produced; null until complete.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.completed_marker_number IS
    'This value is the family marker block when the repair completed; null until complete.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.completed_marker_hash IS
    'This value is the family marker hash when the repair completed; null until complete.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.completed_input_hash IS
    'This value is the interpreter content hash the completing loop ran under; null until complete.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.updated_at IS
    'This value is when the record last changed.'
$ddl$;
END
$migration$;
