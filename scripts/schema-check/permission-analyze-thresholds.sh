# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261105120000_project_permission_analyze_thresholds.sql must give each table the baseline's
# storage parameters from the shape before it, and a rerun must leave them unchanged.
analyze_thresholds_migration="$ROOT/migrations/20261105120000_project_permission_analyze_thresholds.sql"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TEMP TABLE expected_analyze_thresholds AS
SELECT oid, reloptions FROM pg_class
WHERE oid IN ('project_account_approval'::regclass, 'project_ens_v2_entry_owner'::regclass,
              'project_ens_v2_registry_parent'::regclass);
BEGIN;
ALTER TABLE project_account_approval
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
ALTER TABLE project_ens_v2_entry_owner
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
ALTER TABLE project_ens_v2_registry_parent
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
SQL
    emit_phase_migration "$analyze_thresholds_migration" preceding-shape
    emit_phase_migration "$analyze_thresholds_migration" baseline-first
    cat <<'SQL'
DO $$ BEGIN
    IF (SELECT count(*) FROM expected_analyze_thresholds
        WHERE reloptions = ARRAY['autovacuum_analyze_scale_factor=0.02',
                                 'autovacuum_analyze_threshold=20']) <> 3 THEN
        RAISE EXCEPTION 'baseline lacks the analyze thresholds';
    END IF;
    IF EXISTS (SELECT 1 FROM expected_analyze_thresholds expected
               JOIN pg_class table_row USING (oid)
               WHERE table_row.reloptions IS DISTINCT FROM expected.reloptions) THEN
        RAISE EXCEPTION 'analyze thresholds upgrade differs from baseline';
    END IF;
END $$;
ROLLBACK;
DROP TABLE expected_analyze_thresholds;
SQL
} | run_psql
assert_migration_context_count "$analyze_thresholds_migration" preceding-shape 1
