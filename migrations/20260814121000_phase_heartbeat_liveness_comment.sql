-- Existing phase schemas retain the original work-completion wording unless
-- the column comment is updated in place. An empty schema-migration database
-- has no phase baseline yet, so this step is a no-op there.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.service_heartbeats') IS NOT NULL THEN
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.service_heartbeats.heartbeat_at IS
                'This time records runner liveness, including refreshes during storage-capacity waits.'
        $ddl$;
    END IF;
END
$migration$;
