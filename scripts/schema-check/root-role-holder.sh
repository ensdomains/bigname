# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261112170000_project_grant_root_role_holder_index.sql must build the baseline's
# definition on an empty table, refuse a populated table without the prebuild, adopt the
# ops/root-role-holder-index/install.sql prebuild by OID with every row unchanged, rerun as
# a no-op, leave the caller's search_path and quote_all_identifiers as it found them, and refuse
# every shape CREATE INDEX IF NOT EXISTS would skip. install.sql must build the baseline's
# definition, rerun as a no-op and refuse every shape its own check names.
root_role_holder_migration="$ROOT/migrations/20261112170000_project_grant_root_role_holder_index.sql"
root_role_holder_install="$ROOT/ops/root-role-holder-index/install.sql"
root_role_holder_readme=ops/root-role-holder-index/README.md
root_role_holder_index=project_grant_root_role_holder_idx
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_root_role_holder_index AS SELECT pg_get_indexdef('$root_role_holder_index'::regclass) AS definition;" \
        "BEGIN; DELETE FROM project_grant; DROP INDEX $root_role_holder_index;"
    emit_phase_migration "$root_role_holder_migration" preceding-shape
    emit_phase_migration "$root_role_holder_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF pg_get_indexdef('$root_role_holder_index'::regclass) <> (SELECT definition FROM expected_root_role_holder_index) THEN
        RAISE EXCEPTION 'root role holder index empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
DROP TABLE expected_root_role_holder_index;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$root_role_holder_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$root_role_holder_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
assert_concurrent_index_installer root-role-holder \
    "$root_role_holder_index" "$root_role_holder_install" "$root_role_holder_readme" project_grant \
    "chain_id, subject"
# A stored grant makes this a populated released predecessor. Refusal must preserve data and
# the surrounding transaction. The helper rolls the mutation back.
root_role_holder_seed="INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
    event_identity, event_kind, scope_kind, scope_detail, effective_powers, grant_source, revoked)
VALUES ('schema-v2-check', '00000000-0000-0000-0000-000000000001',
    '0x0000000000000000000000000000000000000001', 'root', 1,
    'root-role-holder-upgrade-proof', 'RootPermissionChanged', 'root',
    '{\"kind\": \"root\", \"chain_id\": \"schema-v2-check\", \"registry_address\": \"0x0000000000000000000000000000000000000002\"}',
    '[\"registrar\"]', '{\"kind\": \"raw_log\", \"source_event\": \"EACRolesChanged\"}', false);"
assert_migration_refusal root-role-holder-missing-populated-prebuild "$root_role_holder_migration" \
    "missing prebuilt index $root_role_holder_index on populated project_grant; prebuild it with ops/root-role-holder-index/install.sql, as $root_role_holder_readme describes, before this schema-migration" <<SQL
DROP INDEX $root_role_holder_index;
$root_role_holder_seed
SQL
# The install.sql prebuild over the same populated predecessor, run outside a transaction, is
# adopted by OID with every grant and phase marker unchanged, and a repeat application is a
# no-op. The index then still has the baseline's definition.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_root_role_holder_index AS SELECT pg_get_indexdef('$root_role_holder_index'::regclass) AS definition;" \
        "$root_role_holder_seed" "DROP INDEX $root_role_holder_index;"
    render_phase_migration "$root_role_holder_install"
    cat <<SQL
CREATE TEMP TABLE root_role_holder_upgrade_before AS SELECT
    '$root_role_holder_index'::regclass::oid AS index_oid,
    (SELECT jsonb_agg(to_jsonb(g) ORDER BY chain_id, resource_id, subject, scope) FROM project_grant g) AS grants,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$root_role_holder_migration" specialized
    emit_phase_migration "$root_role_holder_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM root_role_holder_upgrade_before b WHERE
        b.index_oid <> '$root_role_holder_index'::regclass::oid OR
        b.grants IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(g) ORDER BY chain_id, resource_id, subject, scope) FROM project_grant g) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'root role holder index adoption changed its index, grants or phase markers';
    END IF;
    IF pg_get_indexdef('$root_role_holder_index'::regclass) <> (SELECT definition FROM expected_root_role_holder_index) THEN
        RAISE EXCEPTION 'root role holder index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE root_role_holder_upgrade_before;
DROP TABLE expected_root_role_holder_index;
DELETE FROM project_grant WHERE event_identity = 'root-role-holder-upgrade-proof';
SQL
} | run_psql
root_role_holder_recovery="follow the recovery steps in $root_role_holder_readme, then run the schema-migrations again"
for root_role_holder_flag in indisvalid indisready; do
    assert_migration_refusal "root-role-holder-$root_role_holder_flag" "$root_role_holder_migration" \
        "$root_role_holder_index exists but is not a valid and ready index on $scratch_schema.project_grant; $root_role_holder_recovery" <<SQL
UPDATE pg_index SET $root_role_holder_flag=false WHERE indexrelid='$root_role_holder_index'::regclass;
SQL
done
for root_role_holder_kind in table view; do
    if [ "$root_role_holder_kind" = table ]; then root_role_holder_ddl="CREATE TABLE $root_role_holder_index ();"
    else root_role_holder_ddl="CREATE VIEW $root_role_holder_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "root-role-holder-$root_role_holder_kind-collision" "$root_role_holder_migration" \
        "$scratch_schema.$root_role_holder_index is a $root_role_holder_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $root_role_holder_index;
$root_role_holder_ddl
SQL
done
assert_migration_refusal root-role-holder-other-table "$root_role_holder_migration" \
    "$root_role_holder_index exists but is not a valid and ready index on $scratch_schema.project_grant; $root_role_holder_recovery" <<SQL
DROP INDEX $root_role_holder_index;
CREATE INDEX $root_role_holder_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so the
# table carries the schema name.
root_role_holder_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        if [ -n "$1" ]; then
            printf '%s\n' "DROP INDEX $root_role_holder_index;" "CREATE INDEX $root_role_holder_index ON project_grant $1;"
        fi
        printf '%s\n' 'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$root_role_holder_index'::regclass);" 'ROLLBACK;'
    } | run_psql
}
root_role_holder_definition="$(root_role_holder_printed_definition '')"
# The reviewed keys without the predicate, the reviewed keys without NOT revoked, which would
# hold every revoked root grant, and the address key without lower(). Each is a valid, ready
# index under the name.
for root_role_holder_wrong in \
    "(subject COLLATE \"C\", chain_id COLLATE \"C\", lower(scope_detail ->> 'registry_address') COLLATE \"C\")" \
    "(subject COLLATE \"C\", chain_id COLLATE \"C\", lower(scope_detail ->> 'registry_address') COLLATE \"C\") WHERE scope = 'root'" \
    "(subject COLLATE \"C\", chain_id COLLATE \"C\", (scope_detail ->> 'registry_address') COLLATE \"C\") WHERE scope = 'root' AND NOT revoked"; do
    root_role_holder_found="$(root_role_holder_printed_definition "$root_role_holder_wrong")"
    assert_migration_refusal "root-role-holder-wrong-definition-$root_role_holder_wrong" "$root_role_holder_migration" \
        "$root_role_holder_index exists but does not have the reviewed definition; found \"$root_role_holder_found\", expected \"$root_role_holder_definition\"; $root_role_holder_recovery" <<SQL
DROP INDEX $root_role_holder_index;
CREATE INDEX $root_role_holder_index ON project_grant $root_role_holder_wrong;
SQL
done
assert_migration_context_count "$root_role_holder_migration" empty-schema 1
assert_migration_context_count "$root_role_holder_migration" preceding-shape 3
assert_migration_context_count "$root_role_holder_migration" specialized 1
assert_migration_context_count "$root_role_holder_migration" baseline-first 6
