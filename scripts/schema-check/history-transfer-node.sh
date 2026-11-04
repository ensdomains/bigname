# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261102130000_normalized_events_v1_transfer_node_index.sql must build the baseline's
# definition on an empty table, refuse a populated table without a prebuild, adopt the
# docs/deployment.md prebuild by OID with every row unchanged, rerun as a no-op, leave the
# caller's search_path and quote_all_identifiers as it found them, and refuse every shape
# CREATE INDEX IF NOT EXISTS would skip.
transfer_node_migration="$ROOT/migrations/20261102130000_normalized_events_v1_transfer_node_index.sql"
transfer_node_index=normalized_events_v1_transfer_node_idx
# The prebuild statement exactly as docs/deployment.md gives it.
transfer_node_prebuild="$(
    sed -n "/^CREATE INDEX CONCURRENTLY IF NOT EXISTS $transfer_node_index\$/,/;\$/p" \
        "$ROOT/docs/deployment.md"
)"
if [ "$(printf '%s\n' "$transfer_node_prebuild" | grep -c '^CREATE INDEX CONCURRENTLY')" -ne 1 ]; then
    printf '%s\n' "docs/deployment.md does not give exactly one prebuild statement for $transfer_node_index" >&2
    exit 1
fi
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_transfer_node_index AS SELECT pg_get_indexdef('$transfer_node_index'::regclass) AS definition;" \
        "BEGIN; DELETE FROM normalized_events; DROP INDEX $transfer_node_index;"
    emit_phase_migration "$transfer_node_migration" preceding-shape
    emit_phase_migration "$transfer_node_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF pg_get_indexdef('$transfer_node_index'::regclass) <> (SELECT definition FROM expected_transfer_node_index) THEN
        RAISE EXCEPTION 'transfer node index empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$transfer_node_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$transfer_node_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
# A valid stored event makes this a populated released predecessor. Refusal must
# preserve data and the surrounding transaction. The helper rolls the mutation back.
transfer_node_seed="INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind)
VALUES ('transfer-node-upgrade-proof', 'schema-v2-check', 'ResolverRecordLinked', 'ens_v2_resolver_l1',
    1, 'schema-v2-check', 'ens_v2_resolver');"
assert_migration_refusal transfer-node-missing-populated-prebuild "$transfer_node_migration" \
    "missing prebuilt index $transfer_node_index on populated normalized_events; prebuild it as docs/deployment.md describes before this schema-migration" <<SQL
DROP INDEX $transfer_node_index;
$transfer_node_seed
SQL
# The docs/deployment.md prebuild over the same populated predecessor, run outside a
# transaction, is adopted by OID with every event and phase marker unchanged, and a
# repeat application is a no-op. The index then still has the baseline's definition.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_transfer_node_index AS SELECT pg_get_indexdef('$transfer_node_index'::regclass) AS definition;" \
        "$transfer_node_seed" "DROP INDEX $transfer_node_index;"
    printf '%s\n' "$transfer_node_prebuild" | sed "s/bigname_phase/$scratch_schema/g"
    cat <<SQL
CREATE TEMP TABLE transfer_node_upgrade_before AS SELECT
    '$transfer_node_index'::regclass::oid AS index_oid,
    (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) AS events,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$transfer_node_migration" specialized
    emit_phase_migration "$transfer_node_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM transfer_node_upgrade_before b WHERE
        b.index_oid <> '$transfer_node_index'::regclass::oid OR
        b.events IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'transfer node index adoption changed its index, events or phase markers';
    END IF;
    IF pg_get_indexdef('$transfer_node_index'::regclass) <> (SELECT definition FROM expected_transfer_node_index) THEN
        RAISE EXCEPTION 'transfer node index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE transfer_node_upgrade_before;
DROP TABLE expected_transfer_node_index;
DELETE FROM normalized_events WHERE event_identity = 'transfer-node-upgrade-proof';
SQL
} | run_psql
transfer_node_recovery="follow the recovery steps in docs/deployment.md, then run the schema-migrations again"
for transfer_node_flag in indisvalid indisready; do
    assert_migration_refusal "transfer-node-$transfer_node_flag" "$transfer_node_migration" \
        "$transfer_node_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $transfer_node_recovery" <<SQL
UPDATE pg_index SET $transfer_node_flag=false WHERE indexrelid='$transfer_node_index'::regclass;
SQL
done
for transfer_node_kind in table view; do
    if [ "$transfer_node_kind" = table ]; then transfer_node_ddl="CREATE TABLE $transfer_node_index ();"
    else transfer_node_ddl="CREATE VIEW $transfer_node_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "transfer-node-$transfer_node_kind-collision" "$transfer_node_migration" \
        "$scratch_schema.$transfer_node_index is a $transfer_node_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $transfer_node_index;
$transfer_node_ddl
SQL
done
assert_migration_refusal transfer-node-other-table "$transfer_node_migration" \
    "$transfer_node_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $transfer_node_recovery" <<SQL
DROP INDEX $transfer_node_index;
CREATE INDEX $transfer_node_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so
# the table and the enum type carry the schema name.
transfer_node_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        if [ -n "$1" ]; then
            printf '%s\n' "DROP INDEX $transfer_node_index;" "CREATE INDEX $transfer_node_index ON normalized_events $1;"
        fi
        printf '%s\n' 'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$transfer_node_index'::regclass);" 'ROLLBACK;'
    } | run_psql
}
transfer_node_definition="$(transfer_node_printed_definition '')"
# Other keys, the generic node index's predicate without the Transfer condition, and the
# reviewed predicate without the INCLUDE column. Each is a valid, ready index under the name.
for transfer_node_wrong in \
    "(chain_id, block_number)" \
    "(chain_id, (namespace || ':' || lower(after_state ->> 'node')), block_number) INCLUDE (normalized_event_id) WHERE event_kind = 'AuthorityTransferred' AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry') AND canonicality_state IN ('canonical', 'safe', 'finalized')" \
    "(chain_id, (namespace || ':' || lower(after_state ->> 'node')), block_number) WHERE event_kind = 'AuthorityTransferred' AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry') AND after_state ->> 'source_event' = 'Transfer' AND canonicality_state IN ('canonical', 'safe', 'finalized')"; do
    transfer_node_found="$(transfer_node_printed_definition "$transfer_node_wrong")"
    assert_migration_refusal "transfer-node-wrong-definition-$transfer_node_wrong" "$transfer_node_migration" \
        "$transfer_node_index exists but does not have the reviewed definition; found \"$transfer_node_found\", expected \"$transfer_node_definition\"; $transfer_node_recovery" <<SQL
DROP INDEX $transfer_node_index;
CREATE INDEX $transfer_node_index ON normalized_events $transfer_node_wrong;
SQL
done
assert_migration_context_count "$transfer_node_migration" empty-schema 1
assert_migration_context_count "$transfer_node_migration" preceding-shape 3
assert_migration_context_count "$transfer_node_migration" specialized 1
assert_migration_context_count "$transfer_node_migration" baseline-first 6
