# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261102140000_normalized_events_registry_roles_index.sql must build the baseline's
# definition on an empty table, refuse a populated table without a prebuild, adopt the
# docs/deployment.md prebuild by OID with every row unchanged, rerun as a no-op, leave the
# caller's search_path and quote_all_identifiers as it found them, and refuse every shape
# CREATE INDEX IF NOT EXISTS would skip.
registry_roles_migration="$ROOT/migrations/20261102140000_normalized_events_registry_roles_index.sql"
registry_roles_index=normalized_events_registry_roles_idx
# The prebuild statement exactly as docs/deployment.md gives it.
registry_roles_prebuild="$(
    sed -n "/^CREATE INDEX CONCURRENTLY IF NOT EXISTS $registry_roles_index\$/,/;\$/p" \
        "$ROOT/docs/deployment.md"
)"
if [ "$(printf '%s\n' "$registry_roles_prebuild" | grep -c '^CREATE INDEX CONCURRENTLY')" -ne 1 ]; then
    printf '%s\n' "docs/deployment.md does not give exactly one prebuild statement for $registry_roles_index" >&2
    exit 1
fi
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_registry_roles_index AS SELECT pg_get_indexdef('$registry_roles_index'::regclass) AS definition;" \
        "BEGIN; DELETE FROM normalized_events; DROP INDEX $registry_roles_index;"
    emit_phase_migration "$registry_roles_migration" preceding-shape
    emit_phase_migration "$registry_roles_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF pg_get_indexdef('$registry_roles_index'::regclass) <> (SELECT definition FROM expected_registry_roles_index) THEN
        RAISE EXCEPTION 'registry roles index empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$registry_roles_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$registry_roles_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
# A valid stored event makes this a populated released predecessor. Refusal must
# preserve data and the surrounding transaction. The helper rolls the mutation back.
registry_roles_seed="INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind)
VALUES ('registry-roles-upgrade-proof', 'schema-v2-check', 'ResolverRecordLinked', 'ens_v2_resolver_l1',
    1, 'schema-v2-check', 'ens_v2_resolver');"
assert_migration_refusal registry-roles-missing-populated-prebuild "$registry_roles_migration" \
    "missing prebuilt index $registry_roles_index on populated normalized_events; prebuild it as docs/deployment.md describes, or with ops/walk-index-set/install.sql, before this schema-migration" <<SQL
DROP INDEX $registry_roles_index;
$registry_roles_seed
SQL
# The docs/deployment.md prebuild over the same populated predecessor, run outside a
# transaction, is adopted by OID with every event and phase marker unchanged, and a
# repeat application is a no-op. The index then still has the baseline's definition.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_registry_roles_index AS SELECT pg_get_indexdef('$registry_roles_index'::regclass) AS definition;" \
        "$registry_roles_seed" "DROP INDEX $registry_roles_index;"
    printf '%s\n' "$registry_roles_prebuild" | sed "s/bigname_phase/$scratch_schema/g"
    cat <<SQL
CREATE TEMP TABLE registry_roles_upgrade_before AS SELECT
    '$registry_roles_index'::regclass::oid AS index_oid,
    (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) AS events,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$registry_roles_migration" specialized
    emit_phase_migration "$registry_roles_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM registry_roles_upgrade_before b WHERE
        b.index_oid <> '$registry_roles_index'::regclass::oid OR
        b.events IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'registry roles index adoption changed its index, events or phase markers';
    END IF;
    IF pg_get_indexdef('$registry_roles_index'::regclass) <> (SELECT definition FROM expected_registry_roles_index) THEN
        RAISE EXCEPTION 'registry roles index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE registry_roles_upgrade_before;
DROP TABLE expected_registry_roles_index;
DELETE FROM normalized_events WHERE event_identity = 'registry-roles-upgrade-proof';
SQL
} | run_psql
registry_roles_recovery="follow the recovery steps in docs/deployment.md, then run the schema-migrations again"
for registry_roles_flag in indisvalid indisready; do
    assert_migration_refusal "registry-roles-$registry_roles_flag" "$registry_roles_migration" \
        "$registry_roles_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $registry_roles_recovery" <<SQL
UPDATE pg_index SET $registry_roles_flag=false WHERE indexrelid='$registry_roles_index'::regclass;
SQL
done
for registry_roles_kind in table view; do
    if [ "$registry_roles_kind" = table ]; then registry_roles_ddl="CREATE TABLE $registry_roles_index ();"
    else registry_roles_ddl="CREATE VIEW $registry_roles_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "registry-roles-$registry_roles_kind-collision" "$registry_roles_migration" \
        "$scratch_schema.$registry_roles_index is a $registry_roles_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $registry_roles_index;
$registry_roles_ddl
SQL
done
assert_migration_refusal registry-roles-other-table "$registry_roles_migration" \
    "$registry_roles_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $registry_roles_recovery" <<SQL
DROP INDEX $registry_roles_index;
CREATE INDEX $registry_roles_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so
# the table and the enum type carry the schema name.
registry_roles_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        if [ -n "$1" ]; then
            printf '%s\n' "DROP INDEX $registry_roles_index;" "CREATE INDEX $registry_roles_index ON normalized_events $1;"
        fi
        printf '%s\n' 'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$registry_roles_index'::regclass);" 'ROLLBACK;'
    } | run_psql
}
registry_roles_definition="$(registry_roles_printed_definition '')"
# Other keys, the reviewed keys without the EACRolesChanged condition, and the narrower
# keys without the DISTINCT ON columns under the reviewed predicate. Each is a valid, ready
# index under the name.
registry_roles_predicate="WHERE source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1') AND event_kind IN ('PermissionChanged', 'RootPermissionChanged') AND after_state ->> 'source_event' = 'EACRolesChanged' AND resource_id IS NOT NULL AND after_state ->> 'subject' IS NOT NULL AND consumer_visibility = 'activated' AND canonicality_state IN ('canonical', 'safe', 'finalized')"
for registry_roles_wrong in \
    "(chain_id, block_number)" \
    "(chain_id, lower(raw_fact_ref ->> 'emitting_address'), resource_id, lower(after_state ->> 'subject'), block_number DESC, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1') AND event_kind IN ('PermissionChanged', 'RootPermissionChanged') AND resource_id IS NOT NULL AND after_state ->> 'subject' IS NOT NULL AND consumer_visibility = 'activated' AND canonicality_state IN ('canonical', 'safe', 'finalized')" \
    "(chain_id, lower(raw_fact_ref ->> 'emitting_address'), block_number DESC, log_index DESC NULLS LAST, normalized_event_id DESC) $registry_roles_predicate"; do
    registry_roles_found="$(registry_roles_printed_definition "$registry_roles_wrong")"
    assert_migration_refusal "registry-roles-wrong-definition-$registry_roles_wrong" "$registry_roles_migration" \
        "$registry_roles_index exists but does not have the reviewed definition; found \"$registry_roles_found\", expected \"$registry_roles_definition\"; $registry_roles_recovery" <<SQL
DROP INDEX $registry_roles_index;
CREATE INDEX $registry_roles_index ON normalized_events $registry_roles_wrong;
SQL
done
assert_migration_context_count "$registry_roles_migration" empty-schema 1
assert_migration_context_count "$registry_roles_migration" preceding-shape 3
assert_migration_context_count "$registry_roles_migration" specialized 1
assert_migration_context_count "$registry_roles_migration" baseline-first 6
