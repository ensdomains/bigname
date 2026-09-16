-- Resolver-record membership does not require a current owner or registration.
-- Retain real authority IDs when present, but allow the serving resource to stand alone.
-- Existing rows remain valid. Project rebuilds add previously omitted names; operators must
-- replay Project for retained history when upgrading an already indexed database.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.address_records_current') IS NULL THEN
        RETURN;
    END IF;
    ALTER TABLE bigname_phase.address_records_current
        ALTER COLUMN surface_binding_id DROP NOT NULL,
        ALTER COLUMN resource_id DROP NOT NULL,
        ALTER COLUMN binding_kind DROP NOT NULL;
    COMMENT ON TABLE bigname_phase.address_records_current IS
        'Project-owned reverse index over current addr:<coin_type> resolver records: one row per address a record resolves to, coin type, and current name. Rebuilt from record_inventory_current; not serving truth for forward record values.';
    COMMENT ON COLUMN bigname_phase.address_records_current.surface_binding_id IS
        'Binding selected by Project, absent when only a serving resource is known.';
    COMMENT ON COLUMN bigname_phase.address_records_current.resource_id IS
        'Registration resource referenced by the selected name binding, absent without authority.';
    COMMENT ON COLUMN bigname_phase.address_records_current.binding_kind IS
        'Kind of the selected name binding, absent without authority.';
END
$migration$;
