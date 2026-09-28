-- Existing schema-v2 databases gain the expiry indexes the composed expiring
-- listing reads (TYR-36 step 7b, GET /v1/names under the publication switch):
-- the retained lifecycle events and the NameWrapper states by integral expiry,
-- and the retained events whose expiry is a JSON number that is not an integral
-- second, which the listing always considers. Indexes only; no column or row
-- changes. An empty schema-migration database has no phase baseline yet, so
-- this migration is a no-op there and phase-runner init-schema installs the
-- same indexes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_lifecycle_event_expiry_idx
    ON bigname_phase.project_lifecycle_event (expiry_seconds)
    WHERE expiry_seconds IS NOT NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_lifecycle_event_inexact_expiry_idx
    ON bigname_phase.project_lifecycle_event (chain_id)
    WHERE expiry_seconds IS NULL AND jsonb_typeof(expiry) = 'number'
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_wrapper_state_expiry_idx
    ON bigname_phase.project_wrapper_state (expiry_seconds)
    WHERE expiry_seconds IS NOT NULL
$ddl$;
END
$migration$;
