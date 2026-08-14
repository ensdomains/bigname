-- A schema-migration database can exist before phase-runner installs the phase
-- baseline. Leave that empty path untouched; init-schema creates this table from
-- the baseline. Existing initialized schemas receive the additive object here.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE TABLE IF NOT EXISTS bigname_phase.project_redo_resolver_evidence (
        chain_id text NOT NULL,
        event_identity text NOT NULL,
        block_number bigint NOT NULL,
        event_kind text NOT NULL,
        source_family text NOT NULL,
        resource_id uuid,
        before_resolver_address text,
        after_resolver_address text,
        recorded_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (chain_id, event_identity),
        CHECK (block_number >= 0),
        CHECK (event_kind IN ('PermissionChanged', 'ResolverChanged', 'AliasChanged')),
        CHECK (
            before_resolver_address IS NOT NULL
            OR after_resolver_address IS NOT NULL
        )
    );

    CREATE INDEX IF NOT EXISTS project_redo_resolver_evidence_range_idx
        ON bigname_phase.project_redo_resolver_evidence (chain_id, block_number);

    COMMENT ON TABLE bigname_phase.project_redo_resolver_evidence IS
        'Interpret inserts this pre-delete redo handoff once and preserves it across retries; Project compares it with re-derived events and consumes it after selecting affected projection rows.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.chain_id IS
        'This value identifies the chain whose Interpret redo replaced the event range.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.event_identity IS
        'This value identifies the pre-redo normalized event without depending on its sequence-assigned row ID.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.block_number IS
        'This value anchors the removed event in the active redo range.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.event_kind IS
        'This value states whether the pre-redo row changed a permission, resolver pointer, or alias.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.source_family IS
        'This value preserves the pre-redo event family so Project can select a replacement from the same family and event kind.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.resource_id IS
        'This value identifies the permission resource whose current projection must be rebuilt when its event disappears.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.before_resolver_address IS
        'This value is the resolver referenced by the pre-redo event before state.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.after_resolver_address IS
        'This value is the resolver referenced by the pre-redo event after state.';
    COMMENT ON COLUMN bigname_phase.project_redo_resolver_evidence.recorded_at IS
        'This time records the Interpret redo that first captured the event for the pending Project repair.';
END
$migration$;
