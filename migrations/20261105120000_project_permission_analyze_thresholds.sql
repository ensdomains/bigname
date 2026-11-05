-- Lower autovacuum's analyze threshold on the three small tables the ENSv2 permission reads join.
-- By default autovacuum refreshes a table's planner statistics after changes to 10% of its rows
-- plus 50. On a small table a new kind of row can be most of what a read matches and still stay
-- under that, so the planner goes on estimating it at about none. These tables now analyze after
-- changes to 2% of their rows plus 20. No rows, phase markers, catalogue version or content-hash
-- inputs change, and no redo is required.
--
-- ALTER TABLE takes SHARE UPDATE EXCLUSIVE for these two parameters, so it blocks no read and no
-- write. It waits behind a VACUUM, an ANALYZE or an index build on the table. Applied again, it
-- sets the same values.
--
-- An empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there, and phase-runner init-schema installs the same parameters.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.project_account_approval') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.project_account_approval
        SET (autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 20);
    ALTER TABLE bigname_phase.project_ens_v2_entry_owner
        SET (autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 20);
    ALTER TABLE bigname_phase.project_ens_v2_registry_parent
        SET (autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 20);
END
$migration$;
