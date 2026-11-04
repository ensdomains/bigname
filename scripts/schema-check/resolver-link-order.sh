# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261103130000_project_resolver_link_order_index.sql must build the baseline's definition on
# an empty table, refuse a populated table without a prebuild, adopt the docs/deployment.md
# prebuild by OID with every row unchanged, rerun as a no-op, leave the caller's search_path
# and quote_all_identifiers as it found them, and refuse every shape CREATE INDEX IF NOT EXISTS
# would skip.
resolver_link_order_migration="$ROOT/migrations/20261103130000_project_resolver_link_order_index.sql"
resolver_link_order_index=project_resolver_link_order_idx
# The prebuild statement exactly as docs/deployment.md gives it.
resolver_link_order_prebuild="$(
    sed -n "/^CREATE INDEX CONCURRENTLY IF NOT EXISTS $resolver_link_order_index\$/,/;\$/p" \
        "$ROOT/docs/deployment.md"
)"
if [ "$(printf '%s\n' "$resolver_link_order_prebuild" | grep -c '^CREATE INDEX CONCURRENTLY')" -ne 1 ]; then
    printf '%s\n' "docs/deployment.md does not give exactly one prebuild statement for $resolver_link_order_index" >&2
    exit 1
fi
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_resolver_link_order_index AS SELECT pg_get_indexdef('$resolver_link_order_index'::regclass) AS definition;" \
        "BEGIN; DELETE FROM project_resolver_link; DROP INDEX $resolver_link_order_index;"
    emit_phase_migration "$resolver_link_order_migration" preceding-shape
    emit_phase_migration "$resolver_link_order_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF pg_get_indexdef('$resolver_link_order_index'::regclass) <> (SELECT definition FROM expected_resolver_link_order_index) THEN
        RAISE EXCEPTION 'resolver link order index empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$resolver_link_order_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$resolver_link_order_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
# A stored link makes this a populated released predecessor. Refusal must preserve data and
# the surrounding transaction. The helper rolls the mutation back.
resolver_link_order_seed="INSERT INTO project_resolver_link (chain_id, resolver_address, node,
    block_number, event_identity, record_id)
VALUES ('schema-v2-check', '0x0000000000000000000000000000000000000001',
    '0x0000000000000000000000000000000000000000000000000000000000000001', 1,
    'resolver-link-order-upgrade-proof', '1');"
assert_migration_refusal resolver-link-order-missing-populated-prebuild "$resolver_link_order_migration" \
    "missing prebuilt index $resolver_link_order_index on populated project_resolver_link; prebuild it as docs/deployment.md describes before this schema-migration" <<SQL
DROP INDEX $resolver_link_order_index;
$resolver_link_order_seed
SQL
# The docs/deployment.md prebuild over the same populated predecessor, run outside a
# transaction, is adopted by OID with every link and phase marker unchanged, and a repeat
# application is a no-op. The index then still has the baseline's definition.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_resolver_link_order_index AS SELECT pg_get_indexdef('$resolver_link_order_index'::regclass) AS definition;" \
        "$resolver_link_order_seed" "DROP INDEX $resolver_link_order_index;"
    printf '%s\n' "$resolver_link_order_prebuild" | sed "s/bigname_phase/$scratch_schema/g"
    cat <<SQL
CREATE TEMP TABLE resolver_link_order_upgrade_before AS SELECT
    '$resolver_link_order_index'::regclass::oid AS index_oid,
    (SELECT jsonb_agg(to_jsonb(l) ORDER BY chain_id, resolver_address, node) FROM project_resolver_link l) AS links,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$resolver_link_order_migration" specialized
    emit_phase_migration "$resolver_link_order_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM resolver_link_order_upgrade_before b WHERE
        b.index_oid <> '$resolver_link_order_index'::regclass::oid OR
        b.links IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(l) ORDER BY chain_id, resolver_address, node) FROM project_resolver_link l) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'resolver link order index adoption changed its index, links or phase markers';
    END IF;
    IF pg_get_indexdef('$resolver_link_order_index'::regclass) <> (SELECT definition FROM expected_resolver_link_order_index) THEN
        RAISE EXCEPTION 'resolver link order index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE resolver_link_order_upgrade_before;
DROP TABLE expected_resolver_link_order_index;
DELETE FROM project_resolver_link WHERE event_identity = 'resolver-link-order-upgrade-proof';
SQL
} | run_psql
resolver_link_order_recovery="follow the recovery steps in docs/deployment.md, then run the schema-migrations again"
for resolver_link_order_flag in indisvalid indisready; do
    assert_migration_refusal "resolver-link-order-$resolver_link_order_flag" "$resolver_link_order_migration" \
        "$resolver_link_order_index exists but is not a valid and ready index on $scratch_schema.project_resolver_link; $resolver_link_order_recovery" <<SQL
UPDATE pg_index SET $resolver_link_order_flag=false WHERE indexrelid='$resolver_link_order_index'::regclass;
SQL
done
for resolver_link_order_kind in table view; do
    if [ "$resolver_link_order_kind" = table ]; then resolver_link_order_ddl="CREATE TABLE $resolver_link_order_index ();"
    else resolver_link_order_ddl="CREATE VIEW $resolver_link_order_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "resolver-link-order-$resolver_link_order_kind-collision" "$resolver_link_order_migration" \
        "$scratch_schema.$resolver_link_order_index is a $resolver_link_order_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $resolver_link_order_index;
$resolver_link_order_ddl
SQL
done
assert_migration_refusal resolver-link-order-other-table "$resolver_link_order_migration" \
    "$resolver_link_order_index exists but is not a valid and ready index on $scratch_schema.project_resolver_link; $resolver_link_order_recovery" <<SQL
DROP INDEX $resolver_link_order_index;
CREATE INDEX $resolver_link_order_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so the
# table carries the schema name.
resolver_link_order_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        if [ -n "$1" ]; then
            printf '%s\n' "DROP INDEX $resolver_link_order_index;" "CREATE INDEX $resolver_link_order_index ON project_resolver_link $1;"
        fi
        printf '%s\n' 'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$resolver_link_order_index'::regclass);" 'ROLLBACK;'
    } | run_psql
}
resolver_link_order_definition="$(resolver_link_order_printed_definition '')"
# The primary key's columns, the reviewed keys without the non-zero record condition, and the
# record id unpadded under the reviewed condition. Each is a valid, ready index under the name.
for resolver_link_order_wrong in \
    "(chain_id, resolver_address, node)" \
    "(chain_id, resolver_address, lpad(record_id, 78, '0'), node)" \
    "(chain_id, resolver_address, record_id, node) WHERE record_id <> '0'"; do
    resolver_link_order_found="$(resolver_link_order_printed_definition "$resolver_link_order_wrong")"
    assert_migration_refusal "resolver-link-order-wrong-definition-$resolver_link_order_wrong" "$resolver_link_order_migration" \
        "$resolver_link_order_index exists but does not have the reviewed definition; found \"$resolver_link_order_found\", expected \"$resolver_link_order_definition\"; $resolver_link_order_recovery" <<SQL
DROP INDEX $resolver_link_order_index;
CREATE INDEX $resolver_link_order_index ON project_resolver_link $resolver_link_order_wrong;
SQL
done
assert_migration_context_count "$resolver_link_order_migration" empty-schema 1
assert_migration_context_count "$resolver_link_order_migration" preceding-shape 3
assert_migration_context_count "$resolver_link_order_migration" specialized 1
assert_migration_context_count "$resolver_link_order_migration" baseline-first 6
