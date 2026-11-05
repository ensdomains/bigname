# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261105120000_project_permission_analyze_thresholds.sql must set both storage parameters on
# each table from the shape before it, and a rerun must leave them unchanged. The removal path's
# catalog comparison holds it to the baseline's values.
analyze_thresholds_assert="DO \$\$ BEGIN
    IF (SELECT count(*) FROM pg_class
        WHERE oid IN ('project_account_approval'::regclass,
                      'project_ens_v2_entry_owner'::regclass,
                      'project_ens_v2_registry_parent'::regclass)
          AND reloptions = ARRAY['autovacuum_analyze_scale_factor=0.02',
                                 'autovacuum_analyze_threshold=20']) <> 3 THEN
        RAISE EXCEPTION 'analyze thresholds are not set on all three tables';
    END IF;
END \$\$;"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
BEGIN;
ALTER TABLE project_account_approval
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
ALTER TABLE project_ens_v2_entry_owner
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
ALTER TABLE project_ens_v2_registry_parent
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
SQL
    emit_phase_migration "$analyze_thresholds_migration" preceding-shape
    printf '%s\n' "$analyze_thresholds_assert"
    emit_phase_migration "$analyze_thresholds_migration" baseline-first
    printf '%s\n' "$analyze_thresholds_assert"
    printf 'ROLLBACK;\n'
} | run_psql
assert_migration_context_count "$analyze_thresholds_migration" preceding-shape 1
