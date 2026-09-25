-- Existing schema-v2 databases gain the access path the owned key family loop
-- (TYR-36 step 2) uses to re-read a name's retained registrar grants when an
-- AuthorityEpochChanged turns an existing binding candidate registry-only, so
-- a grant retained before the epoch can become the handoff lease. Rows named
-- by their original name are read through this index and rows named later
-- through project_lifecycle_event_decoded_name_idx. The index only adds a read
-- path and changes no row. An empty schema-migration database has no phase
-- baseline yet, so this migration is a no-op there and phase-runner
-- init-schema installs the same index.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_lifecycle_event_registrar_grant_name_idx
    ON bigname_phase.project_lifecycle_event (chain_id, original_logical_name_id)
    WHERE source_family = 'ens_v1_registrar_l1' AND event_kind = 'RegistrationGranted'
$ddl$;
END
$migration$;
