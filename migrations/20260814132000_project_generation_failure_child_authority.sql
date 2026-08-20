-- The projection-blocking failure vocabulary gains the child publication
-- invariant's kind. The installed CHECK is unnamed in both the baseline and the
-- migration that created the table, so it carries PostgreSQL's generated name;
-- this step replaces that exact constraint and leaves every recorded row in
-- place, because the existing kind stays admitted.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.project_generation_failures') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        JOIN pg_class relation ON relation.oid = constraint_row.conrelid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'bigname_phase'
          AND relation.relname = 'project_generation_failures'
          AND constraint_row.conname = 'project_generation_failures_failure_kind_check'
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE bigname_phase.project_generation_failures
                DROP CONSTRAINT project_generation_failures_failure_kind_check
        $ddl$;
    END IF;

    EXECUTE $ddl$
        ALTER TABLE bigname_phase.project_generation_failures
            ADD CONSTRAINT project_generation_failures_failure_kind_check
            CHECK (failure_kind IN (
                'dual_current_exact_name_authority',
                'dual_current_child_authority'
            ))
    $ddl$;

    -- The evidence comment described only the exact-name payload. An upgraded database
    -- must read the same description the baseline now installs, because bootstrap parity
    -- compares column comments across the two install paths.
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.project_generation_failures.evidence IS
            'This payload carries both conflicting sides of the failed invariant — binding and resource identities for an exact name, parent and child relation evidence for a child — the identity of the event proving the selected ENSv2 authority, which is an activated migration boundary or a positive ENSv2 child registration, each block, transaction, and log position, and the canonicality observed at failure.'
    $ddl$;
END
$migration$;
