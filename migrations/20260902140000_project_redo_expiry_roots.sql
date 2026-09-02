-- A schema-migration database can exist before phase-runner installs the phase
-- baseline. Leave that empty path untouched; init-schema creates this table from
-- the baseline. Existing initialized schemas receive the additive object here.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE TABLE IF NOT EXISTS bigname_phase.project_redo_expiry_roots (
        chain_id text NOT NULL,
        event_identity text NOT NULL,
        block_number bigint NOT NULL,
        logical_name_id text NOT NULL,
        recorded_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (chain_id, event_identity),
        CHECK (block_number >= 0),
        CHECK (btrim(logical_name_id) <> '')
    );

    CREATE INDEX IF NOT EXISTS project_redo_expiry_roots_range_idx
        ON bigname_phase.project_redo_expiry_roots (chain_id, block_number);

    COMMENT ON TABLE bigname_phase.project_redo_expiry_roots IS
        'Interpret preserves logical names from deleted state-derived ENSv2 path-expiry releases here until Project follows their surviving canonical subregistry edges and publishes the redo.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.chain_id IS
        'This value identifies the chain whose Interpret redo replaced the event range.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.event_identity IS
        'This value identifies the pre-redo path-expiry release without depending on its sequence-assigned row ID.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.block_number IS
        'This value anchors the removed path-expiry release in the active redo range.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.logical_name_id IS
        'This value seeds bounded traversal from the name whose deleted path-expiry release removed descendant projections.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.recorded_at IS
        'This time records the Interpret redo that first captured the path-expiry logical name for pending Project repair.';
END
$migration$;
