-- Preserve bounded Project scope from entry-creating registry events before redo deletion.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE TABLE IF NOT EXISTS bigname_phase.project_redo_child_registration_history (
        chain_id text NOT NULL, event_identity text NOT NULL,
        block_number bigint NOT NULL, event_kind text NOT NULL,
        logical_name_id text NOT NULL, registry_contract_instance_id uuid NOT NULL,
        recorded_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (chain_id, event_identity),
        CHECK (block_number >= 0),
        CHECK (event_kind IN ('RegistrationReserved', 'RegistrationGranted', 'RegistrationRenewed')),
        CHECK (btrim(logical_name_id) <> '')
    );

    CREATE INDEX IF NOT EXISTS project_redo_child_registration_history_range_idx ON bigname_phase.project_redo_child_registration_history (chain_id, block_number);

    COMMENT ON TABLE bigname_phase.project_redo_child_registration_history IS 'Interpret preserves child identifiers from removed entry-creating events in ENSv1→ENSv2 migration registries until Project publishes a covering redo.';
    COMMENT ON COLUMN bigname_phase.project_redo_child_registration_history.chain_id IS 'This value identifies the chain whose Interpret redo replaced the event range.';
    COMMENT ON COLUMN bigname_phase.project_redo_child_registration_history.event_identity IS 'This value identifies the pre-redo normalized event without depending on its sequence-assigned row ID.';
    COMMENT ON COLUMN bigname_phase.project_redo_child_registration_history.block_number IS 'This value anchors the removed entry-creating event in the active redo range.';
    COMMENT ON COLUMN bigname_phase.project_redo_child_registration_history.event_kind IS 'This value identifies the entry-creating registry operation removed by redo.';
    COMMENT ON COLUMN bigname_phase.project_redo_child_registration_history.logical_name_id IS 'This value identifies the child whose parent reachability must be rebuilt.';
    COMMENT ON COLUMN bigname_phase.project_redo_child_registration_history.registry_contract_instance_id IS 'This value identifies the ENSv1→ENSv2 migration registry whose historical entry made the child ineligible.';
    COMMENT ON COLUMN bigname_phase.project_redo_child_registration_history.recorded_at IS 'This time records the Interpret redo that first captured the event for pending Project repair.';
END
$migration$;
