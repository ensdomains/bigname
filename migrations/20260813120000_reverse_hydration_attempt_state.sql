-- Add fair reverse-name polling selection state to initialized phase schemas.
-- Empty schema-migration databases have no phase baseline yet, so this is a no-op there;
-- the baseline creates the same objects when phase-runner initializes the schema.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.primary_names_current') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE $ddl$
        CREATE SEQUENCE IF NOT EXISTS
            bigname_phase.reverse_hydration_attempt_ordinal_seq AS bigint
    $ddl$;
    EXECUTE $ddl$
        ALTER TABLE bigname_phase.primary_names_current
            ADD COLUMN IF NOT EXISTS
                reverse_hydration_attempted_block_number bigint,
            ADD COLUMN IF NOT EXISTS
                reverse_hydration_attempted_block_hash text,
            ADD COLUMN IF NOT EXISTS
                reverse_hydration_attempt_ordinal bigint
    $ddl$;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid =
                'bigname_phase.primary_names_current'::regclass
          AND constraint_row.conname =
                'primary_names_current_reverse_hydration_attempt_check'
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE bigname_phase.primary_names_current
            ADD CONSTRAINT primary_names_current_reverse_hydration_attempt_check CHECK (
                (
                    reverse_hydration_attempted_block_number IS NULL
                    AND reverse_hydration_attempted_block_hash IS NULL
                    AND reverse_hydration_attempt_ordinal IS NULL
                )
                OR (
                    reverse_hydration_attempted_block_number IS NOT NULL
                    AND reverse_hydration_attempted_block_number >= 0
                    AND reverse_hydration_attempted_block_hash IS NOT NULL
                    AND btrim(reverse_hydration_attempted_block_hash) <> ''
                    AND reverse_hydration_attempt_ordinal IS NOT NULL
                    AND reverse_hydration_attempt_ordinal > 0
                )
            ) NOT VALID
        $ddl$;
    END IF;

    EXECUTE $ddl$
        COMMENT ON COLUMN
            bigname_phase.primary_names_current.reverse_hydration_attempted_block_number IS
            'This internal reverse-name polling selection value identifies the head height of the latest attempt. Readers never use it as serving data.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN
            bigname_phase.primary_names_current.reverse_hydration_attempted_block_hash IS
            'This internal reverse-name polling selection value identifies the head hash of the latest attempt. Readers never use it as serving data.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN
            bigname_phase.primary_names_current.reverse_hydration_attempt_ordinal IS
            'This internal value orders reverse-name polling attempts for fair rolling selection. It never records or validates a provider result.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON SEQUENCE
            bigname_phase.reverse_hydration_attempt_ordinal_seq IS
            'This sequence assigns durable order to reverse-name polling batches; its values are not serving data.'
    $ddl$;
END
$migration$;
