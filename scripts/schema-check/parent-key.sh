# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261104120000_name_surfaces_project_parent_key_index.sql must build the baseline's definition
# on an empty table, refuse a populated table without a prebuild, adopt the docs/deployment.md
# prebuild by OID with every row unchanged, rerun as a no-op, leave the caller's search_path
# and quote_all_identifiers as it found them, and refuse every shape CREATE INDEX IF NOT EXISTS
# would skip. The definition docs/deployment.md prints is the one the baseline builds.
parent_key_migration="$ROOT/migrations/20261104120000_name_surfaces_project_parent_key_index.sql"
parent_key_index=name_surfaces_project_parent_key_idx
# The prebuild statement exactly as docs/deployment.md gives it.
parent_key_prebuild="$(
    sed -n "/^CREATE INDEX CONCURRENTLY IF NOT EXISTS $parent_key_index\$/,/;\$/p" \
        "$ROOT/docs/deployment.md"
)"
if [ "$(printf '%s\n' "$parent_key_prebuild" | grep -c '^CREATE INDEX CONCURRENTLY')" -ne 1 ]; then
    printf '%s\n' "docs/deployment.md does not give exactly one prebuild statement for $parent_key_index" >&2
    exit 1
fi
# The definition line docs/deployment.md says the schema-migration compares.
parent_key_documented="$(grep -x "CREATE INDEX $parent_key_index ON bigname_phase\.name_surfaces .*" "$ROOT/docs/deployment.md")"
if [ "$(printf '%s\n' "$parent_key_documented" | grep -c .)" -ne 1 ]; then
    printf '%s\n' "docs/deployment.md does not give exactly one definition line for $parent_key_index" >&2
    exit 1
fi
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_parent_key_index AS SELECT pg_get_indexdef('$parent_key_index'::regclass) AS definition;" \
        "BEGIN; TRUNCATE name_surfaces CASCADE; DROP INDEX $parent_key_index;"
    emit_phase_migration "$parent_key_migration" preceding-shape
    emit_phase_migration "$parent_key_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF pg_get_indexdef('$parent_key_index'::regclass) <> (SELECT definition FROM expected_parent_key_index) THEN
        RAISE EXCEPTION 'parent key index empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$parent_key_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$parent_key_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
# Printed as the schema-migration reads it, the baseline's definition is the documented line.
parent_key_baseline_definition="$(
    {
        printf 'BEGIN;\nSET LOCAL search_path TO pg_catalog;\n'
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        printf '%s\n' "SELECT pg_get_indexdef('$scratch_schema.$parent_key_index'::regclass);" 'ROLLBACK;'
    } | run_psql
)"
if [ "$parent_key_baseline_definition" != "${parent_key_documented//bigname_phase/$scratch_schema}" ]; then
    printf '%s\n' "docs/deployment.md prints another definition for $parent_key_index than the baseline builds:" \
        "  documented: $parent_key_documented" "  built:      $parent_key_baseline_definition" >&2
    exit 1
fi
# A stored surface makes this a populated released predecessor. Refusal must preserve data and
# the surrounding transaction. The helper rolls the mutation back.
parent_key_seed="INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
    canonicality_state)
VALUES ('schema-v2-parent-key', '0x01', 1, to_timestamp(1), 'canonical');
INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
    namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
    block_number, canonicality_state)
VALUES ('schema-v2-parent-key:0xchild', 'schema-v2-parent-key', 'child.parent',
    ARRAY['child', 'parent'], '\\x00'::bytea, '0xchild', ARRAY['0xchild', '0xparent'], 'test',
    'active', 'schema-v2-parent-key', '0x01', 1, 'canonical');"
assert_migration_refusal parent-key-missing-populated-prebuild "$parent_key_migration" \
    "missing prebuilt index $parent_key_index on populated name_surfaces; prebuild it as docs/deployment.md describes before this schema-migration" <<SQL
DROP INDEX $parent_key_index;
$parent_key_seed
SQL
# The docs/deployment.md prebuild over the same populated predecessor, run outside a
# transaction, is adopted by OID with every surface and phase marker unchanged, and a repeat
# application is a no-op. The index then still has the baseline's definition.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_parent_key_index AS SELECT pg_get_indexdef('$parent_key_index'::regclass) AS definition;" \
        "$parent_key_seed" "DROP INDEX $parent_key_index;"
    printf '%s\n' "$parent_key_prebuild" | sed "s/bigname_phase/$scratch_schema/g"
    cat <<SQL
CREATE TEMP TABLE parent_key_upgrade_before AS SELECT
    '$parent_key_index'::regclass::oid AS index_oid,
    (SELECT jsonb_agg(to_jsonb(s) ORDER BY logical_name_id) FROM name_surfaces s) AS surfaces,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$parent_key_migration" specialized
    emit_phase_migration "$parent_key_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM parent_key_upgrade_before b WHERE
        b.index_oid <> '$parent_key_index'::regclass::oid OR
        b.surfaces IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(s) ORDER BY logical_name_id) FROM name_surfaces s) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'parent key index adoption changed its index, surfaces or phase markers';
    END IF;
    IF pg_get_indexdef('$parent_key_index'::regclass) <> (SELECT definition FROM expected_parent_key_index) THEN
        RAISE EXCEPTION 'parent key index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE parent_key_upgrade_before;
DROP TABLE expected_parent_key_index;
DELETE FROM name_surfaces WHERE logical_name_id = 'schema-v2-parent-key:0xchild';
DELETE FROM chain_lineage WHERE chain_id = 'schema-v2-parent-key';
SQL
} | run_psql
parent_key_recovery="follow the recovery steps in docs/deployment.md, then run the schema-migrations again"
for parent_key_flag in indisvalid indisready; do
    assert_migration_refusal "parent-key-$parent_key_flag" "$parent_key_migration" \
        "$parent_key_index exists but is not a valid and ready index on $scratch_schema.name_surfaces; $parent_key_recovery" <<SQL
UPDATE pg_index SET $parent_key_flag=false WHERE indexrelid='$parent_key_index'::regclass;
SQL
done
for parent_key_kind in table view; do
    if [ "$parent_key_kind" = table ]; then parent_key_ddl="CREATE TABLE $parent_key_index ();"
    else parent_key_ddl="CREATE VIEW $parent_key_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "parent-key-$parent_key_kind-collision" "$parent_key_migration" \
        "$scratch_schema.$parent_key_index is a $parent_key_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $parent_key_index;
$parent_key_ddl
SQL
done
assert_migration_refusal parent-key-other-table "$parent_key_migration" \
    "$parent_key_index exists but is not a valid and ready index on $scratch_schema.name_surfaces; $parent_key_recovery" <<SQL
DROP INDEX $parent_key_index;
CREATE INDEX $parent_key_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so the
# table and function carry the schema name.
parent_key_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        if [ -n "$1" ]; then
            printf '%s\n' "DROP INDEX $parent_key_index;" "CREATE INDEX $parent_key_index ON name_surfaces $1;"
        fi
        printf '%s\n' 'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$parent_key_index'::regclass);" 'ROLLBACK;'
    } | run_psql
}
parent_key_definition="$(parent_key_printed_definition '')"
# The reviewed key over every surface, the whole path's key under the reviewed condition, and
# the reviewed key without the root-name condition. Each is a valid, ready index under the
# name.
for parent_key_wrong in \
    "(namespace, hash_array_extended(entry_key_hashes(labelhashes[2:]), 0))" \
    "(namespace, hash_array_extended(entry_key_hashes(labelhashes), 0)) WHERE visibility_state = 'active' AND raw_name IS DISTINCT FROM ''" \
    "(namespace, hash_array_extended(entry_key_hashes(labelhashes[2:]), 0)) WHERE visibility_state = 'active'"; do
    parent_key_found="$(parent_key_printed_definition "$parent_key_wrong")"
    assert_migration_refusal "parent-key-wrong-definition-$parent_key_wrong" "$parent_key_migration" \
        "$parent_key_index exists but does not have the reviewed definition; found \"$parent_key_found\", expected \"$parent_key_definition\"; $parent_key_recovery" <<SQL
DROP INDEX $parent_key_index;
CREATE INDEX $parent_key_index ON name_surfaces $parent_key_wrong;
SQL
done
assert_migration_context_count "$parent_key_migration" empty-schema 1
assert_migration_context_count "$parent_key_migration" preceding-shape 1
assert_migration_context_count "$parent_key_migration" specialized 1
assert_migration_context_count "$parent_key_migration" baseline-first 6
