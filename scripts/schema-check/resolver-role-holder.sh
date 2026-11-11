# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261112140000_project_grant_resolver_role_holder_index.sql must build the baseline's
# definition on an empty table, refuse a populated table without the prebuild, adopt the
# ops/resolver-role-holder-index/install.sql prebuild by OID with every row unchanged, rerun as
# a no-op, leave the caller's search_path and quote_all_identifiers as it found them, and refuse
# every shape CREATE INDEX IF NOT EXISTS would skip. install.sql must build the baseline's
# definition, rerun as a no-op and refuse every shape its own check names.
role_holder_migration="$ROOT/migrations/20261112140000_project_grant_resolver_role_holder_index.sql"
role_holder_install="$ROOT/ops/resolver-role-holder-index/install.sql"
role_holder_readme=ops/resolver-role-holder-index/README.md
role_holder_index=project_grant_resolver_role_holder_idx
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_role_holder_index AS SELECT pg_get_indexdef('$role_holder_index'::regclass) AS definition;" \
        "BEGIN; DELETE FROM project_grant; DROP INDEX $role_holder_index;"
    emit_phase_migration "$role_holder_migration" preceding-shape
    emit_phase_migration "$role_holder_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF pg_get_indexdef('$role_holder_index'::regclass) <> (SELECT definition FROM expected_role_holder_index) THEN
        RAISE EXCEPTION 'resolver role holder index empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
DROP TABLE expected_role_holder_index;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$role_holder_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$role_holder_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
assert_concurrent_index_installer resolver-role-holder \
    "$role_holder_index" "$role_holder_install" "$role_holder_readme" project_grant \
    "chain_id, subject"
# A stored grant makes this a populated released predecessor. Refusal must preserve data and
# the surrounding transaction. The helper rolls the mutation back.
role_holder_seed="INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
    event_identity, event_kind, scope_kind, effective_powers, grant_source, revoked)
VALUES ('schema-v2-check', '00000000-0000-0000-0000-000000000001',
    '0x0000000000000000000000000000000000000001',
    'resolver:schema-v2-check:0x0000000000000000000000000000000000000002', 1,
    'resolver-role-holder-upgrade-proof', 'PermissionChanged', 'resolver', '[\"set_addr\"]',
    '{\"kind\": \"raw_log\", \"source_event\": \"EACRolesChanged\"}', false);"
assert_migration_refusal resolver-role-holder-missing-populated-prebuild "$role_holder_migration" \
    "missing prebuilt index $role_holder_index on populated project_grant; prebuild it with ops/resolver-role-holder-index/install.sql, as $role_holder_readme describes, before this schema-migration" <<SQL
DROP INDEX $role_holder_index;
$role_holder_seed
SQL
# The install.sql prebuild over the same populated predecessor, run outside a transaction, is
# adopted by OID with every grant and phase marker unchanged, and a repeat application is a
# no-op. The index then still has the baseline's definition.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_role_holder_index AS SELECT pg_get_indexdef('$role_holder_index'::regclass) AS definition;" \
        "$role_holder_seed" "DROP INDEX $role_holder_index;"
    render_phase_migration "$role_holder_install"
    cat <<SQL
CREATE TEMP TABLE role_holder_upgrade_before AS SELECT
    '$role_holder_index'::regclass::oid AS index_oid,
    (SELECT jsonb_agg(to_jsonb(g) ORDER BY chain_id, resource_id, subject, scope) FROM project_grant g) AS grants,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$role_holder_migration" specialized
    emit_phase_migration "$role_holder_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM role_holder_upgrade_before b WHERE
        b.index_oid <> '$role_holder_index'::regclass::oid OR
        b.grants IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(g) ORDER BY chain_id, resource_id, subject, scope) FROM project_grant g) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'resolver role holder index adoption changed its index, grants or phase markers';
    END IF;
    IF pg_get_indexdef('$role_holder_index'::regclass) <> (SELECT definition FROM expected_role_holder_index) THEN
        RAISE EXCEPTION 'resolver role holder index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE role_holder_upgrade_before;
DROP TABLE expected_role_holder_index;
DELETE FROM project_grant WHERE event_identity = 'resolver-role-holder-upgrade-proof';
SQL
} | run_psql
role_holder_recovery="follow the recovery steps in $role_holder_readme, then run the schema-migrations again"
for role_holder_flag in indisvalid indisready; do
    assert_migration_refusal "resolver-role-holder-$role_holder_flag" "$role_holder_migration" \
        "$role_holder_index exists but is not a valid and ready index on $scratch_schema.project_grant; $role_holder_recovery" <<SQL
UPDATE pg_index SET $role_holder_flag=false WHERE indexrelid='$role_holder_index'::regclass;
SQL
done
for role_holder_kind in table view; do
    if [ "$role_holder_kind" = table ]; then role_holder_ddl="CREATE TABLE $role_holder_index ();"
    else role_holder_ddl="CREATE VIEW $role_holder_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "resolver-role-holder-$role_holder_kind-collision" "$role_holder_migration" \
        "$scratch_schema.$role_holder_index is a $role_holder_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $role_holder_index;
$role_holder_ddl
SQL
done
assert_migration_refusal resolver-role-holder-other-table "$role_holder_migration" \
    "$role_holder_index exists but is not a valid and ready index on $scratch_schema.project_grant; $role_holder_recovery" <<SQL
DROP INDEX $role_holder_index;
CREATE INDEX $role_holder_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so the
# table carries the schema name.
role_holder_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        if [ -n "$1" ]; then
            printf '%s\n' "DROP INDEX $role_holder_index;" "CREATE INDEX $role_holder_index ON project_grant $1;"
        fi
        printf '%s\n' 'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$role_holder_index'::regclass);" 'ROLLBACK;'
    } | run_psql
}
role_holder_definition="$(role_holder_printed_definition '')"
# The reviewed keys without the predicate, the reviewed keys without the EAC source condition,
# which would hold every derived ENSv1 resolver row, and the keys in the other order. Each is a
# valid, ready index under the name.
for role_holder_wrong in \
    '(subject COLLATE "C", scope COLLATE "C")' \
    "(subject COLLATE \"C\", scope COLLATE \"C\") WHERE scope_kind = 'resolver' AND NOT revoked" \
    "(scope COLLATE \"C\", subject COLLATE \"C\") WHERE scope_kind = 'resolver' AND NOT revoked AND grant_source ->> 'source_event' = 'EACRolesChanged'"; do
    role_holder_found="$(role_holder_printed_definition "$role_holder_wrong")"
    assert_migration_refusal "resolver-role-holder-wrong-definition-$role_holder_wrong" "$role_holder_migration" \
        "$role_holder_index exists but does not have the reviewed definition; found \"$role_holder_found\", expected \"$role_holder_definition\"; $role_holder_recovery" <<SQL
DROP INDEX $role_holder_index;
CREATE INDEX $role_holder_index ON project_grant $role_holder_wrong;
SQL
done
assert_migration_context_count "$role_holder_migration" empty-schema 1
assert_migration_context_count "$role_holder_migration" preceding-shape 3
assert_migration_context_count "$role_holder_migration" specialized 1
assert_migration_context_count "$role_holder_migration" baseline-first 6
